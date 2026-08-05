//! The `indexes` layer: pre-hashed archive index records.
//!
//! Two record shapes share the layer, told apart by a leading discriminant:
//!
//! - tags — `[0, dimension: tstr, key_hash: bytes(8), slot]`
//! - exact — `[1, kind: tstr, key: bytes, slot]`
//!
//! ## Pre-hashed, and why that is the whole point
//!
//! The index stores keep only `xxh3_64` of a tag's logical key, so the logical
//! key is not recoverable from disk. Rather than resolve historical transaction
//! inputs to rebuild it at publish time — the most expensive machinery in the
//! original design — the hashing scheme is promoted into the profile and the
//! stored form travels. This codec therefore *never hashes anything*: it copies
//! [`dolos_core::KeyHash`] bytes through in both directions. The one dimension
//! whose stored form is not a hash ([`dolos_core::VERBATIM_KEY_DIMENSION`],
//! whose keys are already `u64` labels) needs no special case here for exactly
//! that reason — it is opaque bytes like every other dimension, and
//! [`dolos_core::key_hash`] remains the only place in the tree that knows the
//! difference.
//!
//! ## Order: tags, then exact
//!
//! ADR-004 gives the two shapes separate orders — `(dimension, key_hash, slot)`
//! and `(kind, key)` — and a leading discriminant that sorts `0` before `1`.
//! The layer therefore carries every tag record first, then every exact record,
//! which is also the shape the two source iterators hand over
//! ([`dolos_core::IndexStore::iter_archive_tags`] then
//! [`dolos_core::IndexStore::iter_exact_records`]). Both runs are strictly
//! ascending, so "sorted, deduped" is one rule rather than two.
//!
//! Exact records dedupe on `(kind, key)` and not on the slot behind it: a block
//! hash names one slot, so two records sharing a key are a contradiction, not a
//! pair.

use dolos_cardano::indexes::archive_dimensions;
use dolos_core::{
    BlockSlot, ExactKind, ExactRecord, IndexRecord, KeyHash, TagDimension, TagRecord,
};
use stelae::frame::{self, CanonicalCbor};

use super::{blob, close, fixed, open, text, uint};
use crate::{Error, INDEXES};

/// Leading discriminant of a tag record.
pub const TAG_DISCRIMINANT: u64 = 0;

/// Leading discriminant of an exact-match record.
pub const EXACT_DISCRIMINANT: u64 = 1;

/// Resolve a dimension read off the wire to the `&'static str` the stores use.
///
/// Fails closed against `dolos_cardano`'s archive registry, which is the only
/// list of dimensions there is: stores keep a hash of the name rather than the
/// name, so an unrecognised dimension cannot be looked up, restored or even
/// traversed later.
pub fn resolve_dimension(name: &str) -> Result<TagDimension, Error> {
    archive_dimensions::ALL
        .into_iter()
        .find(|known| *known == name)
        .ok_or_else(|| Error::UnknownDimension(name.to_owned()))
}

/// Resolve an exact-record kind read off the wire.
///
/// The wire string is the stored one — [`ExactKind::as_str`] — so this is that
/// type's own [`std::str::FromStr`] and not a mapping table beside it.
pub fn resolve_exact_kind(name: &str) -> Result<ExactKind, Error> {
    name.parse()
        .map_err(|_| Error::UnknownExactKind(name.to_owned()))
}

pub fn encode(record: &IndexRecord) -> Result<CanonicalCbor, Error> {
    match record {
        IndexRecord::Tag(tag) => encode_tag(tag),
        IndexRecord::Exact(exact) => encode_exact(exact),
    }
}

fn encode_tag(record: &TagRecord) -> Result<CanonicalCbor, Error> {
    // Resolved rather than trusted: a `TagRecord` decoded from an outside source
    // carries an owned dimension string, and shipping one no store has a table
    // for would produce a layer that restores cleanly and can never be queried.
    let dimension = resolve_dimension(record.dimension())?;

    Ok(frame::encode(|e| {
        e.array(4)?
            .u64(TAG_DISCRIMINANT)?
            .str(dimension)?
            .bytes(&record.key_hash)?
            .u64(record.slot)?;
        Ok(())
    })?)
}

fn encode_exact(record: &ExactRecord) -> Result<CanonicalCbor, Error> {
    Ok(frame::encode(|e| {
        e.array(4)?
            .u64(EXACT_DISCRIMINANT)?
            .str(record.kind.as_str())?
            .bytes(record.key())?
            .u64(record.slot)?;
        Ok(())
    })?)
}

pub fn decode(bytes: &[u8]) -> Result<IndexRecord, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    open(INDEXES, &mut decoder, 4)?;

    let discriminant = uint(INDEXES, "discriminant", &mut decoder)?;

    let record = match discriminant {
        TAG_DISCRIMINANT => {
            let dimension = resolve_dimension(text(INDEXES, "dimension", &mut decoder)?)?;
            let key_hash: KeyHash = fixed(INDEXES, "key_hash", &mut decoder)?;
            let slot = uint(INDEXES, "slot", &mut decoder)?;

            IndexRecord::Tag(TagRecord::new(dimension, key_hash, slot))
        }
        EXACT_DISCRIMINANT => {
            let kind = resolve_exact_kind(text(INDEXES, "kind", &mut decoder)?)?;
            let key = blob(INDEXES, "key", &mut decoder)?;
            let slot = uint(INDEXES, "slot", &mut decoder)?;

            // The width rule belongs to `ExactRecord`, which is where the delta
            // path already enforces it. Surfacing its refusal keeps one
            // validation site: a second one here could disagree, and the entry
            // it let through would be one no lookup could ever reach.
            IndexRecord::Exact(ExactRecord::new(kind, key, slot)?)
        }
        other => {
            return Err(Error::malformed(
                INDEXES,
                format!("unknown record discriminant {other}"),
            ))
        }
    };

    close(INDEXES, &decoder, bytes)?;

    Ok(record)
}

/// Every tag record, strictly ascending; then every exact record, likewise.
#[derive(Debug, Default, Clone)]
pub struct OrderCheck {
    last_tag: Option<TagRecord>,
    last_exact: Option<ExactRecord>,
}

impl OrderCheck {
    pub fn check(&mut self, record: &IndexRecord) -> Result<(), Error> {
        match record {
            IndexRecord::Tag(tag) => self.check_tag(tag),
            IndexRecord::Exact(exact) => self.check_exact(exact),
        }
    }

    fn check_tag(&mut self, record: &TagRecord) -> Result<(), Error> {
        if self.last_exact.is_some() {
            return Err(Error::out_of_order(
                INDEXES,
                "a tag record after an exact record; every tag record comes first",
            ));
        }

        if let Some(previous) = &self.last_tag {
            // `TagRecord`'s own ordering is `(dimension, key_hash, slot)`, so
            // "strictly ascending" is both the sort rule and the dedup rule.
            if record <= previous {
                return Err(Error::out_of_order(
                    INDEXES,
                    format!("tag {record:?} follows {previous:?}"),
                ));
            }
        }

        self.last_tag = Some(record.clone());

        Ok(())
    }

    fn check_exact(&mut self, record: &ExactRecord) -> Result<(), Error> {
        if let Some(previous) = &self.last_exact {
            if (record.kind, record.key()) <= (previous.kind, previous.key()) {
                return Err(Error::out_of_order(
                    INDEXES,
                    format!("exact {record:?} follows {previous:?}"),
                ));
            }
        }

        self.last_exact = Some(*record);

        Ok(())
    }
}

/// The slot of either record shape, for callers planning an epoch range.
pub fn slot_of(record: &IndexRecord) -> BlockSlot {
    match record {
        IndexRecord::Tag(tag) => tag.slot,
        IndexRecord::Exact(exact) => exact.slot,
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::{key_hash, MAX_EXACT_KEY_LEN, VERBATIM_KEY_DIMENSION};

    use super::*;

    fn tag(dimension: TagDimension, hash: u8, slot: BlockSlot) -> IndexRecord {
        TagRecord::new(dimension, [hash; 8], slot).into()
    }

    fn exact(kind: ExactKind, byte: u8, slot: BlockSlot) -> IndexRecord {
        ExactRecord::new(kind, &vec![byte; kind.key_len()], slot)
            .unwrap()
            .into()
    }

    #[test]
    fn every_dimension_round_trips() {
        for dimension in archive_dimensions::ALL {
            let original = tag(dimension, 0x7f, 42);
            let encoded = encode(&original).unwrap();

            assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
        }
    }

    #[test]
    fn every_exact_kind_round_trips() {
        for kind in ExactKind::ALL {
            let original = exact(kind, 0x11, 7);
            let encoded = encode(&original).unwrap();

            assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
        }
    }

    /// The dimension whose stored key is a verbatim `u64` label rather than a
    /// hash. Nothing in this codec treats it specially — that is the property
    /// under test.
    #[test]
    fn the_verbatim_dimension_carries_its_label_unchanged() {
        let label = 674u64;
        let stored = key_hash(VERBATIM_KEY_DIMENSION, &label.to_be_bytes()).unwrap();

        assert_eq!(stored, label.to_be_bytes(), "the label is kept, not hashed");

        let original: IndexRecord = TagRecord::new(VERBATIM_KEY_DIMENSION, stored, 900).into();
        let encoded = encode(&original).unwrap();
        let decoded = decode(encoded.as_bytes()).unwrap();

        assert_eq!(decoded, original);

        let IndexRecord::Tag(tag) = decoded else {
            panic!("expected a tag record");
        };
        assert_eq!(u64::from_be_bytes(tag.key_hash), label);
    }

    #[test]
    fn an_unknown_dimension_is_refused_in_both_directions() {
        let wire = frame::encode(|e| {
            e.array(4)?
                .u64(TAG_DISCRIMINANT)?
                .str("not_a_dimension")?
                .bytes(&[0u8; 8])?
                .u64(1)?;
            Ok(())
        })
        .unwrap();

        let err = decode(wire.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::UnknownDimension(_)), "{err:?}");

        let smuggled = TagRecord {
            dimension: std::borrow::Cow::Owned("not_a_dimension".to_owned()),
            key_hash: [0u8; 8],
            slot: 1,
        };
        let err = encode(&smuggled.into()).unwrap_err();
        assert!(matches!(err, Error::UnknownDimension(_)), "{err:?}");
    }

    /// `"block_number"` is the deliberate one: it is the plausible spelling of
    /// a kind that really exists, and `dolos-core` pins `block_num`
    /// precisely because that rename looks cosmetic and orphans every
    /// stored entry. If `ExactKind::as_str` ever moves to it, this case is
    /// *supposed* to fail — the wire literal is the stored form, and the
    /// goldens would move with it.
    #[test]
    fn an_unknown_exact_kind_is_refused() {
        for name in ["block_number", "not_an_exact_kind", "", "TxHash"] {
            let wire = frame::encode(|e| {
                e.array(4)?
                    .u64(EXACT_DISCRIMINANT)?
                    .str(name)?
                    .bytes(&[0u8; 8])?
                    .u64(1)?;
                Ok(())
            })
            .unwrap();

            let err = decode(wire.as_bytes()).unwrap_err();
            assert!(
                matches!(err, Error::UnknownExactKind(_)),
                "{name:?}: {err:?}"
            );
        }
    }

    /// A key wider than any kind's — and wider than the inline buffer — is
    /// refused by `ExactRecord::new`, whose verdict this codec surfaces rather
    /// than duplicating.
    #[test]
    fn an_over_wide_exact_key_is_refused_by_the_store_type() {
        for width in [MAX_EXACT_KEY_LEN + 1, 64, 0] {
            let wire = frame::encode(|e| {
                e.array(4)?
                    .u64(EXACT_DISCRIMINANT)?
                    .str(ExactKind::TxHash.as_str())?
                    .bytes(&vec![0u8; width])?
                    .u64(1)?;
                Ok(())
            })
            .unwrap();

            let err = decode(wire.as_bytes()).unwrap_err();
            assert!(matches!(err, Error::Index(_)), "{width}: {err:?}");
        }
    }

    #[test]
    fn an_unknown_discriminant_is_refused() {
        let wire = frame::encode(|e| {
            e.array(4)?
                .u64(2)?
                .str("address")?
                .bytes(&[0u8; 8])?
                .u64(1)?;
            Ok(())
        })
        .unwrap();

        let err = decode(wire.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
    }

    #[test]
    fn tags_then_exact_is_accepted() {
        let mut order = OrderCheck::default();

        for record in [
            tag(archive_dimensions::ADDRESS, 0x00, 1),
            tag(archive_dimensions::ADDRESS, 0x00, 2),
            tag(archive_dimensions::ADDRESS, 0x01, 0),
            tag(archive_dimensions::ASSET, 0x00, 0),
            exact(ExactKind::BlockHash, 0x00, 5),
            exact(ExactKind::BlockHash, 0x01, 4),
            exact(ExactKind::BlockNumber, 0x00, 3),
            exact(ExactKind::TxHash, 0x00, 2),
        ] {
            order.check(&record).unwrap();
        }
    }

    #[test]
    fn out_of_order_and_duplicate_records_are_refused() {
        let cases: [(&str, Vec<IndexRecord>); 5] = [
            (
                "a tag after an exact",
                vec![
                    exact(ExactKind::BlockHash, 0x00, 1),
                    tag(archive_dimensions::ADDRESS, 0x00, 1),
                ],
            ),
            (
                "a repeated tag",
                vec![
                    tag(archive_dimensions::ADDRESS, 0x00, 1),
                    tag(archive_dimensions::ADDRESS, 0x00, 1),
                ],
            ),
            (
                "a tag slot going backwards",
                vec![
                    tag(archive_dimensions::ADDRESS, 0x00, 2),
                    tag(archive_dimensions::ADDRESS, 0x00, 1),
                ],
            ),
            (
                "a dimension going backwards",
                vec![
                    tag(archive_dimensions::ASSET, 0x00, 1),
                    tag(archive_dimensions::ADDRESS, 0x00, 1),
                ],
            ),
            (
                "an exact key repeated under a different slot",
                vec![
                    exact(ExactKind::BlockHash, 0x00, 1),
                    exact(ExactKind::BlockHash, 0x00, 2),
                ],
            ),
        ];

        for (why, records) in cases {
            let mut order = OrderCheck::default();
            let mut refused = None;

            for record in &records {
                if let Err(e) = order.check(record) {
                    refused = Some(e);
                    break;
                }
            }

            let err = refused.unwrap_or_else(|| panic!("{why} was accepted"));
            assert!(matches!(err, Error::OutOfOrder { .. }), "{why}: {err:?}");
        }
    }
}
