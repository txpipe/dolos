//! The `log-{ns}` layers: epoch-boundary ledger logs, one layer per namespace.
//!
//! `[log_key: bytes(40), value: bytes]`, ordered by `log_key`.
//!
//! The namespace is not in the record: it is the layer's *kind*, one per log
//! namespace (`crate::LOG_KINDS`), so a shape change to one namespace's logs
//! costs a backfill of that namespace's blobs and leaves the other five alone.
//! Which is also why this module has one codec rather than six — the six kinds
//! differ in what they carry, never in how a record is written — and why its
//! refusals name the record shape, `log`, rather than a layer.
//!
//! ## Why these are shipped rather than derived
//!
//! Reward and stake logs are products of ledger computation over a whole epoch.
//! Deriving them at restore time means replaying the ledger, which is the thing
//! a snapshot exists to avoid — so they travel, and the value is the stored
//! minicbor of the entity, byte for byte. This codec never decodes an entity: a
//! log value is an opaque byte string here, which is also why an entity holding
//! a float (`StakeLog::relative_size`) does not collide with the layer's
//! no-floats encoding rule. The rule governs the record's own CBOR, and the
//! value is a byte string within it.
//!
//! Determinism therefore rests on the entity encoders being deterministic — an
//! ADR-004 limitation, and an audit that belongs to the export slice, not here.

use dolos_core::{EntityValue, LogKey};
use stelae::frame::{self, CanonicalCbor};

use crate::Error;
use stelae::codec::{blob, close, fixed, open};

/// The name this codec refuses under.
///
/// One record shape serves all six `log-{ns}` kinds, so an error names the
/// shape rather than a layer — the layer is already in the message the caller
/// wraps it in.
const LOG: &str = "log";

/// Width of a [`LogKey`]: an 8-byte big-endian slot followed by a 32-byte
/// entity key.
///
/// Spelled out because `dolos-core` keeps the two halves private, and pinned
/// against the type by `log_key_len_matches_the_store_type` — a `LogKey` built
/// from a slice zero-pads or truncates to fit, so a decoder that took the wire
/// length on trust would turn a short key into a plausible, unreachable one.
pub const LOG_KEY_LEN: usize = 40;

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

pub fn encode(record: &LogRecord) -> Result<CanonicalCbor, Error> {
    Ok(frame::encode(|e| {
        e.array(2)?
            .bytes(record.key.as_ref())?
            .bytes(&record.value)?;
        Ok(())
    })?)
}

pub fn decode(bytes: &[u8]) -> Result<LogRecord, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    open(LOG, &mut decoder, 2)?;

    let key: [u8; LOG_KEY_LEN] = fixed(LOG, "log_key", &mut decoder)?;
    let value = blob(LOG, "value", &mut decoder)?.to_vec();

    close(LOG, &decoder, bytes)?;

    Ok(LogRecord {
        key: LogKey::from(key.as_slice()),
        value,
    })
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
    use super::*;

    fn key(slot: u64, entity: u8) -> LogKey {
        let mut raw = [0u8; LOG_KEY_LEN];
        raw[..8].copy_from_slice(&slot.to_be_bytes());
        raw[8..].fill(entity);
        LogKey::from(raw.as_slice())
    }

    #[test]
    fn log_key_len_matches_the_store_type() {
        assert_eq!(LOG_KEY_LEN, LogKey::full_range().start.as_ref().len());
    }

    #[test]
    fn round_trips() {
        let original = LogRecord::new(key(432_000, 0xab), vec![0x82, 0x01]);
        let encoded = encode(&original).unwrap();

        assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
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
                .str("stakes")?
                .bytes(&[0u8; LOG_KEY_LEN])?
                .bytes(&[])?;
            Ok(())
        })
        .unwrap();

        let err = decode(wire.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::MalformedRecord { .. }), "{err:?}");
    }

    #[test]
    fn a_wrong_width_log_key_is_refused() {
        for width in [8, 32, 39, 41] {
            let wire = frame::encode(|e| {
                e.array(2)?.bytes(&vec![0u8; width])?.bytes(&[])?;
                Ok(())
            })
            .unwrap();

            let err = decode(wire.as_bytes()).unwrap_err();
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
