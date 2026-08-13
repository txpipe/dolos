//! The `logs` layer: epoch-boundary ledger logs.
//!
//! `[ns: tstr, log_key: bytes(40), value: bytes]`, ordered by `(ns, log_key)`.
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

use dolos_core::{EntityValue, LogKey, Namespace};
use stelae::frame::{self, CanonicalCbor};

use super::{blob, close, fixed, open, text};
use crate::{namespaces, Error, LOGS};

/// Width of a [`LogKey`]: an 8-byte big-endian slot followed by a 32-byte
/// entity key.
///
/// Spelled out because `dolos-core` keeps the two halves private, and pinned
/// against the type by `log_key_len_matches_the_store_type` — a `LogKey` built
/// from a slice zero-pads or truncates to fit, so a decoder that took the wire
/// length on trust would turn a short key into a plausible, unreachable one.
pub const LOG_KEY_LEN: usize = 40;

/// One epoch-boundary log entry, as the layer carries it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogRecord {
    pub ns: Namespace,
    pub key: LogKey,
    /// The stored `EntityValue`, verbatim.
    pub value: EntityValue,
}

impl LogRecord {
    pub fn new(ns: Namespace, key: LogKey, value: impl Into<EntityValue>) -> Self {
        Self {
            ns,
            key,
            value: value.into(),
        }
    }
}

pub fn encode(record: &LogRecord) -> Result<CanonicalCbor, Error> {
    let ns = namespaces::resolve(record.ns)?;

    Ok(frame::encode(|e| {
        e.array(3)?
            .str(ns)?
            .bytes(record.key.as_ref())?
            .bytes(&record.value)?;
        Ok(())
    })?)
}

pub fn decode(bytes: &[u8]) -> Result<LogRecord, Error> {
    let mut decoder = minicbor::Decoder::new(bytes);

    open(LOGS, &mut decoder, 3)?;

    let ns = namespaces::resolve(text(LOGS, "ns", &mut decoder)?)?;
    let key: [u8; LOG_KEY_LEN] = fixed(LOGS, "log_key", &mut decoder)?;
    let value = blob(LOGS, "value", &mut decoder)?.to_vec();

    close(LOGS, &decoder, bytes)?;

    Ok(LogRecord {
        ns,
        key: LogKey::from(key.as_slice()),
        value,
    })
}

/// Strictly ascending `(ns, log_key)`.
#[derive(Debug, Default, Clone)]
pub struct OrderCheck {
    last: Option<(Namespace, LogKey)>,
}

impl OrderCheck {
    pub fn check(&mut self, record: &LogRecord) -> Result<(), Error> {
        let current = (record.ns, record.key.clone());

        if let Some(previous) = &self.last {
            if current <= *previous {
                return Err(Error::out_of_order(
                    LOGS,
                    format!(
                        "{}/{} follows {}/{}",
                        current.0,
                        hex::encode(current.1.as_ref()),
                        previous.0,
                        hex::encode(previous.1.as_ref()),
                    ),
                ));
            }
        }

        self.last = Some(current);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use dolos_cardano::model::{FixedNamespace, LeaderRewardLog, StakeLog};

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
        let original = LogRecord::new(LeaderRewardLog::NS, key(432_000, 0xab), vec![0x82, 0x01]);
        let encoded = encode(&original).unwrap();

        assert_eq!(decode(encoded.as_bytes()).unwrap(), original);
    }

    #[test]
    fn an_unknown_namespace_is_refused_in_both_directions() {
        let wire = frame::encode(|e| {
            e.array(3)?
                .str("not_a_namespace")?
                .bytes(&[0u8; LOG_KEY_LEN])?
                .bytes(&[])?;
            Ok(())
        })
        .unwrap();

        let err = decode(wire.as_bytes()).unwrap_err();
        assert!(matches!(err, Error::UnknownNamespace(_)), "{err:?}");

        let smuggled = LogRecord::new("not_a_namespace", key(1, 0), Vec::new());
        let err = encode(&smuggled).unwrap_err();
        assert!(matches!(err, Error::UnknownNamespace(_)), "{err:?}");
    }

    #[test]
    fn a_wrong_width_log_key_is_refused() {
        for width in [8, 32, 39, 41] {
            let wire = frame::encode(|e| {
                e.array(3)?
                    .str(StakeLog::NS)?
                    .bytes(&vec![0u8; width])?
                    .bytes(&[])?;
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
    fn ordering_is_namespace_then_key() {
        let mut order = OrderCheck::default();

        for record in [
            LogRecord::new(LeaderRewardLog::NS, key(0, 0x00), Vec::new()),
            LogRecord::new(LeaderRewardLog::NS, key(0, 0x01), Vec::new()),
            LogRecord::new(LeaderRewardLog::NS, key(1, 0x00), Vec::new()),
            LogRecord::new(StakeLog::NS, key(0, 0x00), Vec::new()),
        ] {
            order.check(&record).unwrap();
        }

        for backwards in [
            LogRecord::new(StakeLog::NS, key(0, 0x00), Vec::new()),
            LogRecord::new(LeaderRewardLog::NS, key(9, 0x00), Vec::new()),
        ] {
            let err = order.check(&backwards).unwrap_err();
            assert!(matches!(err, Error::OutOfOrder { .. }), "{err:?}");
        }
    }
}
