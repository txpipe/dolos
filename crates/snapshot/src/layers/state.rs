//! The `state-{ns}` layers: the ledger tip, one namespace per kind.
//!
//! `[key: bytes, value: bytes]`, ordered by `key`, with the shard given by the
//! first nibble of `key[0]` for a sixteen-way namespace and fixed at 0 for a
//! single-blob one.
//!
//! The namespace is not in the record: it is the layer's *kind*, one per state
//! namespace (`crate::STATE_KINDS`), so a shape change to one namespace's
//! records costs that namespace's kind a media-type move and leaves the others
//! alone — and a namespace a reader does not know is skippable at the
//! transport instead of poisoning one shared layer. Which is also why this
//! module has one codec for every kind — the kinds differ in what they carry,
//! never in how a record is written — and why its refusals name the record
//! shape, `state`, rather than a layer.
//!
//! ## One record shape, including for UTxOs
//!
//! ADR-004 treats the UTxO set as namespace [`crate::UTXOS`] beside the entity
//! namespaces, rather than as a special layer kind. That is
//! what keeps the format's state vocabulary to a single record, and it makes
//! the planned refactor folding UTxOs into the entity system (#1042) invisible
//! from outside: the day `utxos` becomes an ordinary namespace, nothing in this
//! file changes.
//!
//! The namespace still governs the *codec parameters* — the key kind above
//! all — so [`encode`] and [`decode`] take it as an argument, derived by the
//! caller from the layer's kind. It is per-layer configuration now, not
//! per-record content.
//!
//! ## Keys
//!
//! A key is the stored bytes of the namespace's typed key (decision 0051), of
//! whatever length that kind has — an empty one for a singleton — and the
//! codec refuses one that does not decode exactly as that kind
//! ([`namespaces::check_state_key`]).
//!
//! ## The one place bytes are built rather than carried
//!
//! Entity values are the stored minicbor, verbatim. UTxO values are not: the
//! state store hands out a [`UtxoValue`], which is a Rust value rather than a
//! byte string, so this codec composes `[era, body, slot, tx_index]` — or
//! `[era, body]` for a UTxO no block produced — itself. It composes it through
//! [`stelae::frame::encode`] — the same validator every record goes through —
//! so the one transformed field in the whole profile is canonical by
//! construction rather than by inspection.
//!
//! ## Sharding
//!
//! The chain-scale namespaces (`utxos`, `accounts`, `assets`, `datums`)
//! split sixteen ways by the first nibble of the first key byte. Keys are
//! hash-derived (transaction hashes, credentials, script hashes), so the split
//! is uniform without a hash of its own; shards can be fetched in parallel and
//! stay far from registry size limits as state grows. Every other namespace is
//! a single blob — shard 0 — because sixteen slivers of a kilobyte-scale
//! population would be all overhead. The counts are fixed by the profile
//! specification (`crate::STATE_KINDS`), never by data or configuration.

use dolos_core::{
    EntityKey, EntityValue, Era, EraCbor, Namespace, TxoRef, UtxoPosition, UtxoValue,
};
use stelae::frame::{self, CanonicalCbor};

use crate::{namespaces, Error};
use stelae::codec::{blob, close, open, uint};

pub use crate::namespaces::UTXO_KEY_LEN;

/// The name this codec refuses under.
///
/// One record shape serves every `state-{ns}` kind, so an error names the
/// shape rather than a layer — the layer is already in the message the
/// caller wraps it in.
const STATE: &str = "state";

/// The shard a state key belongs to under a kind published in `shards` shards:
/// the first nibble of its first byte for a sixteen-way namespace, 0 for a
/// single blob.
///
/// An empty key only exists in a single-blob namespace (the singletons), so
/// the fallback for one is unreachable rather than a policy.
pub fn shard_of(key: &[u8], shards: u8) -> u8 {
    if shards <= 1 {
        return 0;
    }

    key.first().copied().unwrap_or(0) >> 4
}

/// One state entry, as the layer carries it.
///
/// No namespace: the layer's kind is the namespace, and a record that repeated
/// it could disagree with the layer it sits in.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateRecord {
    /// The namespace's typed key, stored, or [`UTXO_KEY_LEN`] bytes under
    /// [`crate::UTXOS`].
    pub key: Vec<u8>,
    /// The stored `EntityValue` verbatim, or CBOR `[era, body, slot, tx_index]`
    /// (`[era, body]` without a position) under [`crate::UTXOS`].
    pub value: EntityValue,
}

/// A record for an entity, whose value is carried verbatim.
pub fn entity(key: &EntityKey, value: &EntityValue) -> StateRecord {
    StateRecord {
        key: key.as_ref().to_vec(),
        value: value.clone(),
    }
}

/// A record for one UTxO, composing its `[era, body, slot, tx_index]` value.
pub fn utxo(txo: &TxoRef, value: &UtxoValue) -> Result<StateRecord, Error> {
    let mut key = Vec::with_capacity(UTXO_KEY_LEN);
    key.extend_from_slice(txo.0.as_ref());
    key.extend_from_slice(&txo.1.to_be_bytes());

    Ok(StateRecord {
        key,
        value: encode_utxo_value(value)?.into_bytes(),
    })
}

/// Read a record back as an entity. [`decode`] already held its key to the
/// namespace's key kind.
pub fn as_entity(record: &StateRecord) -> EntityKey {
    EntityKey::from_raw(&record.key)
}

/// Read a record back as a UTxO. Refuses a key that is not UTxO-width — an
/// entity record, above all.
pub fn as_utxo(record: &StateRecord) -> Result<(TxoRef, UtxoValue), Error> {
    let key: [u8; UTXO_KEY_LEN] = record.key.as_slice().try_into().map_err(|_| {
        Error::malformed(
            STATE,
            format!(
                "utxo key: expected {UTXO_KEY_LEN} bytes, found {}",
                record.key.len()
            ),
        )
    })?;

    let (hash, index) = key.split_at(32);
    let hash: [u8; 32] = hash.try_into().expect("split at the hash");
    let index = u32::from_be_bytes(index.try_into().expect("the remaining four bytes"));

    Ok((
        TxoRef(hash.into(), index),
        decode_utxo_value(&record.value)?,
    ))
}

/// The `utxos` value: `[era: uint, body: bytes, slot: uint, tx_index: uint]`,
/// the creation position last. A UTxO no block produced has no position and
/// is `[era: uint, body: bytes]`.
pub fn encode_utxo_value(value: &UtxoValue) -> Result<CanonicalCbor, Error> {
    let EraCbor(era, body) = &value.output;

    Ok(frame::encode(|e| {
        match value.position {
            Some(UtxoPosition { slot, tx_index }) => {
                e.array(4)?
                    .u64(u64::from(*era))?
                    .bytes(body)?
                    .u64(slot)?
                    .u64(u64::from(tx_index))?;
            }
            None => {
                e.array(2)?.u64(u64::from(*era))?.bytes(body)?;
            }
        }
        Ok(())
    })?)
}

pub fn decode_utxo_value(bytes: &[u8]) -> Result<UtxoValue, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    let fields = decoder
        .array()
        .map_err(|e| Error::malformed(STATE, format!("expected an array: {e}")))?
        .ok_or_else(|| Error::malformed(STATE, "indefinite-length array"))?;

    if fields != 2 && fields != 4 {
        return Err(Error::malformed(
            STATE,
            format!("expected 2 or 4 fields, found {fields}"),
        ));
    }

    let era = uint(STATE, "era", &mut decoder)?;
    let era = Era::try_from(era)
        .map_err(|_| Error::malformed(STATE, format!("era {era} does not fit a u16")))?;
    let body = blob(STATE, "body", &mut decoder)?.to_vec();

    let position = if fields == 4 {
        let slot = uint(STATE, "slot", &mut decoder)?;
        let tx_index = uint(STATE, "tx_index", &mut decoder)?;
        let position = u16::try_from(tx_index)
            .ok()
            .and_then(|tx_index| UtxoPosition::checked(slot, tx_index))
            .ok_or_else(|| {
                Error::malformed(
                    STATE,
                    format!("tx_index {tx_index} is past {}", UtxoPosition::MAX_TX_INDEX),
                )
            })?;

        Some(position)
    } else {
        None
    };

    close(STATE, &decoder, bytes)?;

    Ok(UtxoValue::new(EraCbor(era, body), position))
}

/// Encode one record of `ns`'s state layer.
///
/// The namespace is a parameter rather than a field: it comes from the layer's
/// kind, and it decides the key kind this refuses under.
pub fn encode(ns: Namespace, record: &StateRecord) -> Result<CanonicalCbor, Error> {
    let ns = namespaces::resolve(ns)?;

    namespaces::check_state_key(ns, &record.key)?;

    Ok(frame::encode(|e| {
        e.array(2)?.bytes(&record.key)?.bytes(&record.value)?;
        Ok(())
    })?)
}

/// Decode one record of `ns`'s state layer, holding the key to the kind the
/// namespace stores.
pub fn decode(ns: Namespace, bytes: &[u8]) -> Result<StateRecord, Error> {
    let ns = namespaces::resolve(ns)?;

    let mut decoder = minicbor::Decoder::new(bytes);

    open(STATE, &mut decoder, 2)?;

    let key = blob(STATE, "key", &mut decoder)?.to_vec();
    let value = blob(STATE, "value", &mut decoder)?.to_vec();

    close(STATE, &decoder, bytes)?;

    namespaces::check_state_key(ns, &key)?;

    Ok(StateRecord { key, value })
}

/// Strictly ascending `key`, optionally within one shard.
///
/// One namespace per layer, so the key alone orders it — the ordering rule the
/// store's own iterator already yields.
#[derive(Debug, Default, Clone)]
pub struct OrderCheck {
    /// `(shard, shards)`: which shard every record must belong to, under the
    /// kind's shard count.
    shard: Option<(u8, u8)>,
    last: Option<Vec<u8>>,
}

impl OrderCheck {
    /// A check that also insists every record belongs to `shard` of `shards`.
    ///
    /// Worth having beside the ordering rule: a record in the wrong shard layer
    /// still restores — the write path dispatches on the kind, not on the
    /// shard — so nothing downstream would ever notice, and a client fetching
    /// shards selectively would silently miss it.
    pub fn for_shard(shard: u8, shards: u8) -> Self {
        Self {
            shard: Some((shard, shards)),
            last: None,
        }
    }

    pub fn check(&mut self, record: &StateRecord) -> Result<(), Error> {
        if let Some((shard, shards)) = self.shard {
            if shard_of(&record.key, shards) != shard {
                return Err(Error::malformed(
                    STATE,
                    format!(
                        "{} belongs to shard {}, not shard {shard}",
                        hex::encode(&record.key),
                        shard_of(&record.key, shards),
                    ),
                ));
            }
        }

        if let Some(previous) = &self.last {
            if record.key <= *previous {
                return Err(Error::out_of_order(
                    STATE,
                    format!(
                        "{} follows {}",
                        hex::encode(&record.key),
                        hex::encode(previous),
                    ),
                ));
            }
        }

        self.last = Some(record.key.clone());

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dolos_cardano::model::{
        AccountAddressChunk, AccountAddressSet, AccountEpochLog, AccountState, AccountStats,
        AddressStats, AssetKey, AssetMintChunk, AssetMintSet, AssetState, DRepState, EpochState,
        EraState, FirstMintSet, FixedNamespace, GovState, MetadataLabelState, PaymentAddressSet,
        PaymentStats, PoolState, ProposalState,
    };

    use super::*;
    use crate::UTXOS;

    /// An asset key: 32 bytes, any of them, so a byte picks its shard.
    fn entity_key(byte: u8) -> EntityKey {
        EntityKey::of(&AssetKey([byte; 32].into()))
    }

    fn txo(byte: u8, index: u32) -> TxoRef {
        TxoRef([byte; 32].into(), index)
    }

    fn keyed(ns: Namespace, key: &[u8]) -> Result<StateRecord, Error> {
        let wire = frame::encode(|e| {
            e.array(2)?.bytes(key)?.bytes(&[])?;
            Ok(())
        })
        .unwrap();

        decode(ns, wire.as_bytes())
    }

    #[test]
    fn an_entity_record_round_trips() {
        let original = entity(&entity_key(0x5a), &vec![0x82, 0x01, 0x02]);

        let encoded = encode(AssetState::NS, &original).unwrap();
        let decoded = decode(AssetState::NS, encoded.as_bytes()).unwrap();

        assert_eq!(decoded, original);
        assert_eq!(as_entity(&decoded), entity_key(0x5a));
    }

    /// Each namespace takes exactly the keys of its kind, whatever their
    /// width: an empty one for a singleton, 29 bytes or 1 for a DRep, and so
    /// on.
    #[test]
    fn keys_are_held_to_their_namespace_kind() {
        let credential = [[0x82, 0x00, 0x58, 0x1c].as_slice(), &[7; 28]].concat();

        for (ns, len) in [
            (EpochState::NS, 0),
            (GovState::NS, 0),
            (EraState::NS, 2),
            (MetadataLabelState::NS, 8),
            (DRepState::NS, 1),
            (PoolState::NS, 28),
            (ProposalState::NS, 36),
            (FirstMintSet::NS, 68),
            (AssetMintSet::NS, 52),
        ] {
            keyed(ns, &vec![1; len]).unwrap_or_else(|err| panic!("{ns}/{len}: {err:?}"));
        }

        keyed(AccountState::NS, &credential).unwrap();
        keyed(DRepState::NS, &[[0x22].as_slice(), &[7; 28]].concat()).unwrap();

        // an address: an account and a base address's header and payment
        // hash, or a tag and the address whole
        let staked = [credential.as_slice(), &[0x01], &[1; 28]].concat();
        let unstaked = [[0x00].as_slice(), &[1; 29]].concat();

        // an account address: the account, the epoch, then the address's key,
        // last
        let staged = |address: &[u8]| [credential.as_slice(), &[0; 8], address].concat();
        keyed(AccountAddressSet::NS, &staged(&staked)).unwrap();
        keyed(AccountAddressSet::NS, &staged(&unstaked)).unwrap();

        // unsealed stats: an address's and an account's are keyed epoch
        // first, a payment credential's set and stats group first
        let epoch = [0u8; 8];
        let address = [epoch.as_slice(), &staked].concat();
        keyed(AddressStats::NS, &address).unwrap();
        keyed(AddressStats::NS, &[epoch.as_slice(), &unstaked].concat()).unwrap();
        keyed(
            PaymentAddressSet::NS,
            &[[1; 28].as_slice(), &epoch, &[1; 29]].concat(),
        )
        .unwrap();
        keyed(PaymentStats::NS, &[[1; 28].as_slice(), &epoch].concat()).unwrap();
        keyed(AccountStats::NS, &[epoch.as_slice(), &credential].concat()).unwrap();

        for (ns, key) in [
            (EpochState::NS, vec![0x30]),
            (EpochState::NS, [0x30].into_iter().chain([0; 31]).collect()),
            (PoolState::NS, vec![1; 32]),
            (ProposalState::NS, vec![1; 32]),
            (MetadataLabelState::NS, vec![1; 7]),
            (MetadataLabelState::NS, vec![1; 9]),
            (MetadataLabelState::NS, vec![1; 32]),
            (DRepState::NS, vec![0x22; 32]),
            (AccountState::NS, vec![1; 32]),
            (AccountEpochLog::NS, vec![1; 32]),
            (AccountAddressSet::NS, staged(&[])),
            (AccountAddressSet::NS, staged(&[1; 57])),
            (AccountAddressSet::NS, credential.clone()),
            (AccountAddressChunk::NS, vec![1; 32]),
            (AssetMintSet::NS, vec![1; 48]),
            (AssetMintChunk::NS, vec![1; 36]),
            // an archive-only log holds no state, even under its log key
            (AccountAddressChunk::NS, credential.clone()),
            (AddressStats::NS, [epoch.as_slice(), &credential].concat()),
            (AddressStats::NS, [epoch.as_slice(), &staked, &[1]].concat()),
            (
                AddressStats::NS,
                [epoch.as_slice(), &[0x02], &[1; 29]].concat(),
            ),
            // a sealed row's key, without the epoch
            (AddressStats::NS, staked.clone()),
            (PaymentAddressSet::NS, [[1; 28].as_slice(), &epoch].concat()),
            (
                PaymentStats::NS,
                [[1; 28].as_slice(), &epoch, &[1]].concat(),
            ),
            (AccountStats::NS, credential.clone()),
            (AccountStats::NS, [credential.as_slice(), &epoch].concat()),
        ] {
            let err = keyed(ns, &key).unwrap_err();
            assert!(
                matches!(err, Error::MalformedRecord { .. }),
                "{ns}/{}: {err:?}",
                key.len()
            );
        }
    }

    fn unpositioned(era: Era, body: Vec<u8>) -> UtxoValue {
        UtxoValue::new(EraCbor(era, body), None)
    }

    #[test]
    fn a_utxo_record_round_trips() {
        let ref_ = txo(0x30, 7);

        for position in [None, Some(UtxoPosition::new(91_000_000, 12))] {
            let value = UtxoValue::new(EraCbor(6, vec![0xa0, 0x01]), position);

            let original = utxo(&ref_, &value).unwrap();

            assert_eq!(original.key.len(), UTXO_KEY_LEN);

            let encoded = encode(UTXOS, &original).unwrap();
            let decoded = decode(UTXOS, encoded.as_bytes()).unwrap();

            assert_eq!(decoded, original);
            assert_eq!(as_utxo(&decoded).unwrap(), (ref_.clone(), value));
        }
    }

    /// The composed field, checked against the encoding profile it has to obey
    /// rather than against itself.
    #[test]
    fn the_utxo_value_is_canonical_by_construction() {
        let positions = [
            None,
            Some(UtxoPosition::new(0, 0)),
            Some(UtxoPosition::new(23, 24)),
            Some(UtxoPosition::new(u64::MAX, UtxoPosition::MAX_TX_INDEX)),
        ];

        for era in [0u16, 23, 24, 255, 256, u16::MAX] {
            for position in positions {
                let value = UtxoValue::new(EraCbor(era, vec![0xff; 3]), position);
                let composed = encode_utxo_value(&value).unwrap();

                // `CanonicalCbor::new` is the validator; re-running it over
                // the bytes proves the composition did not merely happen to
                // pass once.
                CanonicalCbor::new(composed.as_bytes().to_vec()).unwrap();
                assert_eq!(decode_utxo_value(composed.as_bytes()).unwrap(), value);
            }
        }
    }

    /// The store codec (`UtxoValue::encode_cbor`) and the record's composed
    /// value are the same bytes.
    #[test]
    fn the_record_value_is_the_stored_value() {
        for position in [None, Some(UtxoPosition::new(4_492_800, 3))] {
            let value = UtxoValue::new(EraCbor(6, vec![0xa0, 0x01]), position);

            assert_eq!(
                encode_utxo_value(&value).unwrap().as_bytes(),
                value.encode_cbor().as_slice()
            );
        }
    }

    /// A position is both fields or neither, and the index is at most
    /// `UtxoPosition::MAX_TX_INDEX`: past it, the tag order a restore builds
    /// from the record does not exist.
    #[test]
    fn a_malformed_position_is_refused() {
        let three = frame::encode(|e| {
            e.array(3)?.u64(6)?.bytes(&[])?.u64(10)?;
            Ok(())
        })
        .unwrap();

        let index = |tx_index: u64| {
            frame::encode(|e| {
                e.array(4)?.u64(6)?.bytes(&[])?.u64(10)?.u64(tx_index)?;
                Ok(())
            })
            .unwrap()
        };

        for wire in [
            three,
            index(u64::from(u16::MAX)),
            index(u64::from(u16::MAX) + 1),
        ] {
            let err = decode_utxo_value(wire.as_bytes()).unwrap_err();
            assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
        }
    }

    /// The era is a `u16` in the stores and a CBOR uint on the wire, so the
    /// widths disagree and the decoder narrows. `encode_utxo_value` cannot
    /// reach the refusal — a `UtxoValue` already holds a `u16` — so the wire
    /// bytes are built directly, which is the only way an out-of-range era
    /// arrives at all.
    #[test]
    fn an_out_of_range_era_is_refused() {
        for era in [u64::from(u16::MAX) + 1, u64::MAX] {
            let wire = frame::encode(|e| {
                e.array(2)?.u64(era)?.bytes(&[])?;
                Ok(())
            })
            .unwrap();

            let err = decode_utxo_value(wire.as_bytes()).unwrap_err();
            assert!(
                matches!(err, Error::MalformedRecord { .. }),
                "{era}: {err:?}"
            );
        }
    }

    /// The key kinds tell the two record shapes apart, now that no namespace
    /// field does: each namespace refuses the other's record.
    #[test]
    fn the_two_readers_refuse_each_others_records() {
        let entity_record = entity(&entity_key(1), &Vec::new());
        let utxo_record = utxo(&txo(1, 0), &unpositioned(1, Vec::new())).unwrap();

        assert!(as_utxo(&entity_record).is_err());
        assert!(encode(AssetState::NS, &utxo_record).is_err());
        assert!(encode(UTXOS, &entity_record).is_err());
    }

    #[test]
    fn a_wrong_width_key_is_refused() {
        for (ns, width) in [
            (AssetState::NS, UTXO_KEY_LEN),
            (AssetState::NS, 0),
            (UTXOS, 32),
            (UTXOS, UTXO_KEY_LEN + 1),
        ] {
            let wire = frame::encode(|e| {
                e.array(2)?.bytes(&vec![0u8; width])?.bytes(&[])?;
                Ok(())
            })
            .unwrap();

            let err = decode(ns, wire.as_bytes()).unwrap_err();
            assert!(
                matches!(err, Error::MalformedRecord { .. }),
                "{ns}/{width}: {err:?}"
            );
        }
    }

    /// The shape the `state` kind carried before the split, offered to the
    /// decoder of the kinds that replaced it.
    ///
    /// It has to be refused, and refused on the arity: a decoder that read the
    /// first two elements and stopped would take the namespace string for a
    /// key, and a decoder that tolerated the extra element would restore v1
    /// records into a namespace decided by the layer rather than by the record
    /// — silently, and only for the namespaces that happen to agree.
    #[test]
    fn the_pre_split_three_element_record_is_refused() {
        let wire = frame::encode(|e| {
            e.array(3)?.str("accounts")?.bytes(&[0u8; 32])?.bytes(&[])?;
            Ok(())
        })
        .unwrap();

        let err = decode(AccountState::NS, wire.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
    }

    /// A namespace this profile does not define is refused by the codec itself,
    /// not left to a later stage.
    #[test]
    fn an_unknown_namespace_is_refused_by_both_directions() {
        let record = entity(&entity_key(1), &Vec::new());

        assert!(encode("receipts", &record).is_err());

        let wire = encode(AssetState::NS, &record).unwrap();
        assert!(decode("receipts", wire.as_bytes()).is_err());
    }

    /// Hash-derived keys spread evenly over the sixteen shards, which is the
    /// premise the shard split rests on — and a single-blob namespace is
    /// always shard 0, whatever its keys.
    #[test]
    fn sharding_is_the_first_nibble_or_the_single_blob() {
        assert_eq!(shard_of(&[0x00], 16), 0);
        assert_eq!(shard_of(&[0x0f], 16), 0);
        assert_eq!(shard_of(&[0x10], 16), 1);
        assert_eq!(shard_of(&[0xff], 16), 15);

        let mut counts = [0usize; 16];
        for byte in 0u8..=255 {
            counts[shard_of(&[byte], 16) as usize] += 1;
        }

        assert!(counts.iter().all(|c| *c == 16), "{counts:?}");

        for byte in [0x00, 0x0f, 0x10, 0xff] {
            assert_eq!(shard_of(&[byte], 1), 0, "{byte:#x}");
        }
    }

    /// The two key widths shard the same way, so one rule covers every kind.
    #[test]
    fn both_key_widths_shard_identically() {
        let entity_record = entity(&entity_key(0xc3), &Vec::new());
        let utxo_record = utxo(&txo(0xc3, 0), &unpositioned(1, Vec::new())).unwrap();

        assert_eq!(shard_of(&entity_record.key, 16), 12);
        assert_eq!(shard_of(&utxo_record.key, 16), 12);
    }

    #[test]
    fn ordering_is_by_key() {
        let mut order = OrderCheck::default();

        for record in [
            entity(&entity_key(0x00), &Vec::new()),
            entity(&entity_key(0x01), &Vec::new()),
            utxo(&txo(0x01, 0), &unpositioned(1, Vec::new())).unwrap(),
        ] {
            order.check(&record).unwrap();
        }

        let err = order
            .check(&entity(&entity_key(0x01), &Vec::new()))
            .unwrap_err();
        assert!(matches!(err, Error::OutOfOrder { .. }), "{err:?}");
    }

    #[test]
    fn a_record_from_another_shard_is_refused() {
        let mut order = OrderCheck::for_shard(0, 16);

        order
            .check(&entity(&entity_key(0x0a), &Vec::new()))
            .unwrap();

        let err = order
            .check(&entity(&entity_key(0xa0), &Vec::new()))
            .unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");

        // Under a single-blob count the same keys are all shard 0.
        let mut single = OrderCheck::for_shard(0, 1);
        single
            .check(&entity(&entity_key(0x0a), &Vec::new()))
            .unwrap();
        single
            .check(&entity(&entity_key(0xa0), &Vec::new()))
            .unwrap();
    }
}
