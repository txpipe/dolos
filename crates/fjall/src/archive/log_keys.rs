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
//! Key:   [ns_hash:8][log_key:40]  (48 bytes total)
//! Value: entity value bytes (CBOR encoded)
//! ```
//!
//! The `log_key` is core's 40-byte [`LogKey`]: an 8-byte big-endian slot
//! followed by a 32-byte entity key. Within one namespace, lexicographic
//! order of the prefixed key equals `LogKey` order, which is what
//! `iter_logs` range scans rely on.

use dolos_core::{LogKey, Namespace};

use crate::state::entity_keys::hash_namespace;

/// Size of namespace hash prefix: 8 bytes (xxh3 truncated)
pub const NS_HASH_SIZE: usize = 8;

/// Size of core's log key: 8-byte temporal prefix + 32-byte entity key
pub const LOG_KEY_SIZE: usize = 40;

/// Total size of a prefixed log key: 48 bytes
pub const PREFIXED_LOG_KEY_SIZE: usize = NS_HASH_SIZE + LOG_KEY_SIZE;

/// Build a log key: `[ns_hash:8][log_key:40]`
pub fn build_log_key(ns: Namespace, key: &LogKey) -> [u8; PREFIXED_LOG_KEY_SIZE] {
    let mut result = [0u8; PREFIXED_LOG_KEY_SIZE];
    result[..NS_HASH_SIZE].copy_from_slice(&hash_namespace(ns));
    result[NS_HASH_SIZE..].copy_from_slice(key.as_ref());
    result
}

/// Decode the 40-byte [`LogKey`] portion out of a stored key.
///
/// The input must be at least `PREFIXED_LOG_KEY_SIZE` bytes.
pub fn decode_log_key(key: &[u8]) -> LogKey {
    debug_assert!(key.len() >= PREFIXED_LOG_KEY_SIZE);
    LogKey::from(&key[NS_HASH_SIZE..PREFIXED_LOG_KEY_SIZE])
}

/// Build a temporal bound within a namespace: `[ns_hash:8][slot:8]`.
///
/// Compared lexicographically against 48-byte stored keys, this 16-byte
/// bound sorts before every key of the namespace whose temporal prefix is
/// `>= slot` and after every key whose prefix is `< slot` — the exact
/// boundary semantics redb gets from comparing 40-byte keys against an
/// 8-byte `TemporalKey` bound, which prune and truncate must reproduce.
pub fn build_temporal_bound(ns: Namespace, slot: u64) -> Vec<u8> {
    let mut result = Vec::with_capacity(NS_HASH_SIZE + 8);
    result.extend_from_slice(&hash_namespace(ns));
    result.extend_from_slice(&slot.to_be_bytes());
    result
}

/// Build the inclusive start of a namespace's key range: the bare 8-byte
/// namespace hash, which sorts before every stored key of the namespace.
pub fn namespace_start(ns: Namespace) -> Vec<u8> {
    hash_namespace(ns).to_vec()
}

/// Build an exclusive end bound covering the whole namespace:
/// `[ns_hash:8][0xff:41]`.
///
/// One byte longer than any stored key, so even a log key of all `0xff`
/// sorts before it, while every key of the next namespace prefix sorts
/// after it.
pub fn namespace_end(ns: Namespace) -> Vec<u8> {
    let mut result = Vec::with_capacity(PREFIXED_LOG_KEY_SIZE + 1);
    result.extend_from_slice(&hash_namespace(ns));
    result.extend_from_slice(&[0xff; LOG_KEY_SIZE + 1]);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_key(slot: u64, entity: u8) -> LogKey {
        let mut bytes = [entity; LOG_KEY_SIZE];
        bytes[..8].copy_from_slice(&slot.to_be_bytes());
        LogKey::from(bytes.as_slice())
    }

    #[test]
    fn prefixed_key_roundtrip() {
        let key = log_key(42, 0xab);
        let prefixed = build_log_key("account-epochs", &key);
        assert_eq!(prefixed.len(), PREFIXED_LOG_KEY_SIZE);
        assert_eq!(decode_log_key(&prefixed), key);
    }

    #[test]
    fn namespace_isolation() {
        let key = log_key(42, 0xab);
        let a = build_log_key("account-epochs", &key);
        let b = build_log_key("stakes", &key);
        assert_ne!(a[..NS_HASH_SIZE], b[..NS_HASH_SIZE]);
        assert_eq!(a[NS_HASH_SIZE..], b[NS_HASH_SIZE..]);
    }

    #[test]
    fn key_order_matches_log_key_order() {
        let earlier = build_log_key("stakes", &log_key(1, 0xff));
        let later = build_log_key("stakes", &log_key(2, 0x00));
        assert!(earlier < later);
    }

    #[test]
    fn temporal_bound_splits_at_slot() {
        let ns = "stakes";
        let bound = build_temporal_bound(ns, 5);
        let below = build_log_key(ns, &log_key(4, 0xff));
        let at = build_log_key(ns, &log_key(5, 0x00));
        assert!(below.as_slice() < bound.as_slice());
        assert!(at.as_slice() > bound.as_slice());
    }

    #[test]
    fn namespace_bounds_cover_all_keys() {
        let ns = "epochs";
        let start = namespace_start(ns);
        let end = namespace_end(ns);
        let min = build_log_key(ns, &LogKey::from([0u8; LOG_KEY_SIZE].as_slice()));
        let max = build_log_key(ns, &LogKey::from([0xff; LOG_KEY_SIZE].as_slice()));
        assert!(start.as_slice() < min.as_slice());
        assert!(max.as_slice() < end.as_slice());
    }
}
