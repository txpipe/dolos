//! The `log-{ns}` layers: epoch-boundary ledger logs, one layer per namespace.
//!
//! `[log_key: bytes, value: bytes]`, ordered by `log_key`.
//!
//! The namespace is not in the record: it is the layer's *kind*, one per log
//! namespace (`crate::LOG_KINDS`), so a shape change to one namespace's logs
//! costs a backfill of that namespace's blobs and leaves the others alone.
//! Which is also why this module has one codec rather than one per kind — the
//! kinds differ in what they carry, never in how a record is written — and why
//! its refusals name the record shape, `log`, rather than a layer.
//!
//! The namespace still decides the key kind, so [`encode`] and [`decode`] take
//! it as an argument, as the state codec does. A log key is the 8-byte
//! big-endian slot followed by the namespace's typed key (decision 0051), and
//! the codec refuses one whose part after the slot does not decode exactly as
//! that kind ([`namespaces::check_log_key`]).
//!
//! ## Why these are shipped rather than derived
//!
//! Reward and stake logs are products of ledger computation over a whole epoch.
//! Deriving them at restore time means replaying the ledger, which is the thing
//! a snapshot exists to avoid — so they travel, and the value is the stored
//! minicbor of the entity, byte for byte. This codec never decodes an entity: a
//! log value is an opaque byte string here. The layer's encoding rules govern
//! the record's own CBOR, and the value is a byte string within it.
//!
//! Determinism therefore rests on the entity encoders being deterministic — an
//! ADR-004 limitation, and an audit that belongs to the export slice, not here.

use dolos_core::{EntityValue, LogKey, Namespace};
use stelae::frame::{self, CanonicalCbor};

use crate::{namespaces, Error};
use stelae::codec::{blob, close, open};

/// The name this codec refuses under.
///
/// One record shape serves every `log-{ns}` kind, so an error names the
/// shape rather than a layer — the layer is already in the message the caller
/// wraps it in.
const LOG: &str = "log";

/// One epoch-boundary log entry, as the layer carries it.
///
/// No namespace: the layer's kind is the namespace, and a record that repeated
/// it could disagree with the layer it sits in.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub key: LogKey,
    /// The stored `EntityValue`, verbatim.
    pub value: EntityValue,
}

impl LogRecord {
    pub fn new(key: LogKey, value: impl Into<EntityValue>) -> Self {
        Self {
            key,
            value: value.into(),
        }
    }
}

/// Encode one record of `ns`'s log layer.
pub fn encode(ns: Namespace, record: &LogRecord) -> Result<CanonicalCbor, Error> {
    namespaces::check_log_key(ns, record.key.entity())?;

    Ok(frame::encode(|e| {
        e.array(2)?
            .bytes(record.key.as_ref())?
            .bytes(&record.value)?;
        Ok(())
    })?)
}

/// Decode one record of `ns`'s log layer, holding the key to the kind the
/// namespace logs under.
pub fn decode(ns: Namespace, bytes: &[u8]) -> Result<LogRecord, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    open(LOG, &mut decoder, 2)?;

    let key = blob(LOG, "log_key", &mut decoder)?;
    let key = LogKey::from_raw(key).map_err(|err| Error::malformed(LOG, format!("{err}")))?;
    let value = blob(LOG, "value", &mut decoder)?.to_vec();

    close(LOG, &decoder, bytes)?;

    namespaces::check_log_key(ns, key.entity())?;

    Ok(LogRecord { key, value })
}

/// Strictly ascending `log_key`.
///
/// One namespace per layer, so the key alone orders it — the ordering rule the
/// store's own iterator already yields.
#[derive(Debug, Default, Clone)]
pub struct OrderCheck {
    last: Option<LogKey>,
}

impl OrderCheck {
    pub fn check(&mut self, record: &LogRecord) -> Result<(), Error> {
        if let Some(previous) = &self.last {
            if record.key <= *previous {
                return Err(Error::out_of_order(
                    LOG,
                    format!(
                        "{} follows {}",
                        hex::encode(record.key.as_ref()),
                        hex::encode(previous.as_ref()),
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
        AccountAddressChunk, AccountStats, AddressStats, AssetMintChunk, DRepEpochLog, DRepState,
        EpochState, FirstMintChunk, FirstScriptChunk, FixedNamespace as _, PaymentAddressChunk,
        PolicyKey, PolicyMintChunk, PoolEpochLog, PoolKey, PoolState, ProposalState,
    };

    use super::*;

    /// A `stakes` key: the slot and a 28-byte pool.
    fn key(slot: u64, pool: u8) -> LogKey {
        LogKey::of(slot, &PoolKey([pool; 28].into()))
    }

    fn wire(key: &[u8]) -> CanonicalCbor {
        frame::encode(|e| {
            e.array(2)?.bytes(key)?.bytes(&[])?;
            Ok(())
        })
        .unwrap()
    }

    #[test]
    fn round_trips() {
        let original = LogRecord::new(key(432_000, 0xab), vec![0x82, 0x01]);
        let encoded = encode(PoolEpochLog::NS, &original).unwrap();

        assert_eq!(
            decode(PoolEpochLog::NS, encoded.as_bytes()).unwrap(),
            original
        );
    }

    /// Each log takes exactly the keys of its kind after the slot: none for
    /// the epoch log, a chunk index for first mints and first scripts, a policy
    /// and a chunk for the per-policy list, an asset key and a chunk for an
    /// asset's mints, a pool, a proposal or a DRep for its final row.
    #[test]
    fn keys_are_held_to_their_namespace_kind() {
        let slot = 7u64.to_be_bytes();

        for (ns, len) in [
            (EpochState::NS, 0),
            (FirstMintChunk::NS, 4),
            (FirstScriptChunk::NS, 4),
            (PolicyMintChunk::NS, 32),
            (AssetMintChunk::NS, 36),
            (PoolEpochLog::NS, 28),
            (PoolState::NS, 28),
            (ProposalState::NS, 36),
        ] {
            let key = [slot.as_slice(), &vec![1; len]].concat();
            decode(ns, wire(&key).as_bytes()).unwrap_or_else(|err| panic!("{ns}: {err:?}"));
        }

        let policy = LogKey::of(7, &(PolicyKey([1; 28].into()), 3u32));
        assert_eq!(policy.entity().len(), 32);

        // account address chunks take an account: a credential header and its hash
        let account = [slot.as_slice(), &[0x82, 0x01, 0x58, 0x1c], &[1; 28]].concat();
        decode(AccountAddressChunk::NS, wire(&account).as_bytes()).unwrap();
        decode(AccountStats::NS, wire(&account).as_bytes()).unwrap();

        // address stats take an account and a base address's header and
        // payment hash, or a tag and the address whole
        let staked = [account.as_slice(), &[0x01], &[1; 28]].concat();
        decode(AddressStats::NS, wire(&staked).as_bytes()).unwrap();
        let unstaked = [slot.as_slice(), &[0x00], &[1; 29]].concat();
        decode(AddressStats::NS, wire(&unstaked).as_bytes()).unwrap();
        decode(
            PaymentAddressChunk::NS,
            wire(&[slot.as_slice(), &[1; 28]].concat()).as_bytes(),
        )
        .unwrap();

        // drep epochs and a DRep's final row take a drep: a credential
        // header and its hash, or the one-byte tag of Abstain or NoConfidence
        for ns in [DRepEpochLog::NS, DRepState::NS] {
            for drep in [&[0x22][..], &[0x23], &[0x00], &[0x01]] {
                let hash: &[u8] = if drep[0] > 1 { &[1; 28] } else { &[] };
                let key = [slot.as_slice(), drep, hash].concat();
                decode(ns, wire(&key).as_bytes()).unwrap_or_else(|err| panic!("{ns}: {err:?}"));
            }

            for drep in [&[][..], &[0x22; 28], &[0x22; 30], &[0x00, 0x01], &[0x02]] {
                let key = [slot.as_slice(), drep].concat();
                let err = decode(ns, wire(&key).as_bytes()).unwrap_err();
                assert!(
                    matches!(err, Error::MalformedRecord { .. }),
                    "{ns}/{drep:02x?}: {err:?}"
                );
            }
        }

        for (ns, len) in [
            (EpochState::NS, 32),
            (FirstMintChunk::NS, 32),
            (FirstScriptChunk::NS, 32),
            (AssetMintChunk::NS, 32),
            (AccountAddressChunk::NS, 32),
            (AccountStats::NS, 32),
            (AddressStats::NS, 1),
            (AddressStats::NS, 32),
            (AddressStats::NS, 33),
            (PaymentAddressChunk::NS, 32),
            (PoolEpochLog::NS, 32),
            (PoolEpochLog::NS, 0),
            (PoolState::NS, 32),
            (ProposalState::NS, 32),
            (ProposalState::NS, 37),
        ] {
            let key = [slot.as_slice(), &vec![1; len]].concat();
            let err = decode(ns, wire(&key).as_bytes()).unwrap_err();
            assert!(
                matches!(err, Error::MalformedRecord { .. }),
                "{ns}/{len}: {err:?}"
            );
        }
    }

    /// The shape the `logs` kind carried before the split, offered to the
    /// decoder of the kinds that replaced it.
    ///
    /// It has to be refused, and refused on the arity: a decoder that read the
    /// first two elements and stopped would take the namespace string for a log
    /// key, and a decoder that tolerated the extra element would restore v1
    /// records into a namespace decided by the layer rather than by the record
    /// — silently, and only for the namespaces that happen to agree.
    #[test]
    fn the_pre_split_three_element_record_is_refused() {
        let wire = frame::encode(|e| {
            e.array(3)?
                .str("pool-epochs")?
                .bytes(key(1, 0).as_ref())?
                .bytes(&[])?;
            Ok(())
        })
        .unwrap();

        let err = decode(PoolEpochLog::NS, wire.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
    }

    /// A key too short to hold its slot.
    #[test]
    fn a_key_shorter_than_a_slot_is_refused() {
        for width in [0, 7] {
            let err = decode(EpochState::NS, wire(&vec![0u8; width]).as_bytes()).unwrap_err();
            assert!(
                matches!(err, Error::MalformedRecord { .. }),
                "{width}: {err:?}"
            );
        }
    }

    #[test]
    fn ordering_is_by_key() {
        let mut order = OrderCheck::default();

        for record in [
            LogRecord::new(key(0, 0x00), Vec::new()),
            LogRecord::new(key(0, 0x01), Vec::new()),
            LogRecord::new(key(1, 0x00), Vec::new()),
        ] {
            order.check(&record).unwrap();
        }

        for backwards in [
            LogRecord::new(key(1, 0x00), Vec::new()),
            LogRecord::new(key(0, 0xff), Vec::new()),
        ] {
            let err = order.check(&backwards).unwrap_err();
            assert!(matches!(err, Error::OutOfOrder { .. }), "{err:?}");
        }
    }
}
