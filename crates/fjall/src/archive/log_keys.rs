//! Log key encoding for the unified archive-logs keyspace.
//!
//! All log namespaces share a single keyspace with namespace hash prefixes,
//! the same layout lesson the state store's entities keyspace records:
//! per-namespace LSM trees multiply segment files and blow the file-descriptor
//! limit under heavy compaction.
//!
//! ## Key Format
//!
//! ```text
//! Key:   [ns_hash:8][slot:8][entity_key:var]
//! Value: entity value bytes (CBOR encoded)
//! ```
//!
//! The part after the namespace hash is core's [`LogKey`]: an 8-byte
//! big-endian slot followed by the typed entity key, of whatever length its
//! kind has. Within one namespace, lexicographic order of the prefixed key
//! equals `LogKey` order, which is what `iter_logs` range scans rely on.

use dolos_core::{LogKey, Namespace};

use crate::{
    state::entity_keys::{hash_namespace, prefixed, StoredKey, NS_HASH_SIZE},
    Error,
};

pub use crate::state::entity_keys::{build_range, namespace_end};

/// Shortest stored log key: the namespace hash and the slot.
pub const MIN_PREFIXED_LOG_KEY_SIZE: usize = NS_HASH_SIZE + LogKey::SLOT_LEN;

/// Build a log key: `[ns_hash:8][log_key]`
pub fn build_log_key(ns: Namespace, key: &LogKey) -> StoredKey {
    prefixed(ns, key.as_ref())
}

/// Decode the [`LogKey`] out of a stored key.
pub fn decode_log_key(key: &[u8]) -> Result<LogKey, Error> {
    let log_key = key.get(NS_HASH_SIZE..).ok_or_else(|| {
        Error::Codec(format!(
            "malformed archive log key: no namespace hash in {} bytes",
            key.len()
        ))
    })?;

    LogKey::from_raw(log_key).map_err(|e| Error::Codec(format!("malformed archive log key: {e}")))
}

/// Build a temporal bound within a namespace: `[ns_hash:8][slot:8]`.
///
/// Compared lexicographically against stored keys, this 16-byte bound sorts
/// at or before every key of the namespace whose slot is `>= slot` and after
/// every key whose slot is `< slot` — the boundary prune and truncate draw.
pub fn build_temporal_bound(ns: Namespace, slot: u64) -> StoredKey {
    prefixed(ns, &slot.to_be_bytes())
}

/// Build the inclusive start of a namespace's key range: the bare 8-byte
/// namespace hash, which sorts before every stored key of the namespace.
pub fn namespace_start(ns: Namespace) -> StoredKey {
    StoredKey::from_slice(&hash_namespace(ns))
}

#[cfg(test)]
mod tests {
    use std::ops::{Bound, RangeBounds};

    use dolos_core::KeyRange;

    use super::*;

    fn log_key(slot: u64, entity: &[u8]) -> LogKey {
        LogKey::from_raw(&[slot.to_be_bytes().as_slice(), entity].concat()).unwrap()
    }

    #[test]
    fn prefixed_key_roundtrip() {
        for len in [0, 4, 28, 32, 100] {
            let key = log_key(42, &vec![0xab; len]);
            let prefixed = build_log_key("account-epochs", &key);
            assert_eq!(prefixed.len(), MIN_PREFIXED_LOG_KEY_SIZE + len);
            assert_eq!(decode_log_key(&prefixed).unwrap(), key);
        }
    }

    #[test]
    fn short_stored_key_is_an_error() {
        let err = decode_log_key(&[0; NS_HASH_SIZE + 3]).unwrap_err();
        assert!(
            err.to_string().contains("key needs 8 bytes, found 3"),
            "{err}"
        );

        assert!(decode_log_key(&[0; NS_HASH_SIZE - 1]).is_err());
    }

    #[test]
    fn namespace_isolation() {
        let key = log_key(42, &[0xab; 32]);
        let a = build_log_key("account-epochs", &key);
        let b = build_log_key("pool-epochs", &key);
        assert_ne!(a[..NS_HASH_SIZE], b[..NS_HASH_SIZE]);
        assert_eq!(a[NS_HASH_SIZE..], b[NS_HASH_SIZE..]);
    }

    #[test]
    fn key_order_matches_log_key_order() {
        let earlier = build_log_key("pool-epochs", &log_key(1, &[0xff; 40]));
        let later = build_log_key("pool-epochs", &log_key(2, &[]));
        assert!(earlier < later);
    }

    #[test]
    fn temporal_bound_splits_at_slot() {
        let ns = "pool-epochs";
        let bound = build_temporal_bound(ns, 5);
        let below = build_log_key(ns, &log_key(4, &[0xff; 50]));
        let at = build_log_key(ns, &log_key(5, &[]));
        assert!(below.as_slice() < bound.as_slice());
        assert!(at.as_slice() >= bound.as_slice());
    }

    #[test]
    fn namespace_bounds_cover_all_keys() {
        let ns = "epochs";
        let start = namespace_start(ns);
        let end = namespace_end(ns);
        let min = build_log_key(ns, &log_key(0, &[]));
        let max = build_log_key(ns, &log_key(u64::MAX, &[0xff; 100]));
        assert!(start.as_slice() < min.as_slice());

        let range = (Bound::Included(start), end);
        assert!(range.contains(&max));

        let whole = build_range(ns, &KeyRange::all());
        assert!(whole.contains(&min));
        assert!(whole.contains(&max));
    }
}
