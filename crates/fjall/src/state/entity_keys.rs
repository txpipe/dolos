//! Entity key encoding for the unified state-entities keyspace.
//!
//! All entity types share a single keyspace with namespace hash prefixes.
//! This reduces the number of LSM-tree segment files compared to separate
//! keyspaces per entity type.
//!
//! ## Key Format
//!
//! ```text
//! Key:   [ns_hash:8][entity_key:32]  (40 bytes total)
//! Value: entity value bytes (CBOR encoded)
//! ```
//!
//! The `ns_hash` is the first 8 bytes of xxh3 hash of the namespace string
//! (e.g., "accounts", "pools"). This provides:
//! - Deterministic IDs without hardcoded mapping
//! - Extensibility for new entity types without code changes
//! - Consistent ordering within each namespace (entities with same namespace are grouped)

use dolos_core::{EntityKey, Namespace};

use crate::keys::hash_key;

/// Size of namespace hash prefix: 8 bytes (xxh3 truncated)
pub const NS_HASH_SIZE: usize = 8;

/// Size of entity key: 32 bytes
pub const ENTITY_KEY_SIZE: usize = 32;

/// Total size of prefixed entity key: 40 bytes
pub const PREFIXED_KEY_SIZE: usize = NS_HASH_SIZE + ENTITY_KEY_SIZE;

/// Hash a namespace string to an 8-byte prefix using xxh3.
///
/// The hash is stored as big-endian bytes for consistent lexicographic ordering.
pub fn hash_namespace(ns: Namespace) -> [u8; NS_HASH_SIZE] {
    let hash = hash_key(ns.as_bytes());
    hash.to_be_bytes()
}

/// Build entity key: `[ns_hash:8][entity_key:32]`
pub fn build_entity_key(ns: Namespace, key: &EntityKey) -> [u8; PREFIXED_KEY_SIZE] {
    let mut result = [0u8; PREFIXED_KEY_SIZE];
    let ns_hash = hash_namespace(ns);
    result[..NS_HASH_SIZE].copy_from_slice(&ns_hash);
    result[NS_HASH_SIZE..].copy_from_slice(key.as_ref());
    result
}

/// Build prefix for namespace iteration: `[ns_hash:8]`
pub fn build_namespace_prefix(ns: Namespace) -> [u8; NS_HASH_SIZE] {
    hash_namespace(ns)
}

/// Build range start key for entity iteration within a namespace.
///
/// Returns: `[ns_hash:8][range_start:32]`
pub fn build_range_start(ns: Namespace, start: &EntityKey) -> [u8; PREFIXED_KEY_SIZE] {
    build_entity_key(ns, start)
}

/// Build range end key for entity iteration within a namespace.
///
/// Returns: `[ns_hash:8][range_end:32]`
pub fn build_range_end(ns: Namespace, end: &EntityKey) -> [u8; PREFIXED_KEY_SIZE] {
    build_entity_key(ns, end)
}

/// Decode entity key from stored key (extracts the 32-byte EntityKey portion).
///
/// The input key must be at least `PREFIXED_KEY_SIZE` bytes.
pub fn decode_entity_key(key: &[u8]) -> EntityKey {
    debug_assert!(key.len() >= PREFIXED_KEY_SIZE);
    let mut entity_key = [0u8; ENTITY_KEY_SIZE];
    entity_key.copy_from_slice(&key[NS_HASH_SIZE..NS_HASH_SIZE + ENTITY_KEY_SIZE]);
    EntityKey::from(&entity_key)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_namespace_hash_deterministic() {
        let hash1 = hash_namespace("accounts");
        let hash2 = hash_namespace("accounts");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_namespace_hash_different() {
        let hash1 = hash_namespace("accounts");
        let hash2 = hash_namespace("pools");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_entity_key_roundtrip() {
        let ns = "accounts";
        let original_key = EntityKey::from(&[0xab; 32]);

        let prefixed = build_entity_key(ns, &original_key);
        assert_eq!(prefixed.len(), PREFIXED_KEY_SIZE);

        let decoded = decode_entity_key(&prefixed);
        assert_eq!(original_key, decoded);
    }

    #[test]
    fn test_entity_key_structure() {
        let ns = "accounts";
        let entity_key = EntityKey::from(&[0xcd; 32]);

        let prefixed = build_entity_key(ns, &entity_key);

        // First 8 bytes should be namespace hash
        let expected_prefix = hash_namespace(ns);
        assert_eq!(&prefixed[..NS_HASH_SIZE], &expected_prefix);

        // Remaining 32 bytes should be entity key
        assert_eq!(&prefixed[NS_HASH_SIZE..], entity_key.as_ref());
    }

    #[test]
    fn test_namespace_prefix() {
        let ns = "pools";
        let prefix = build_namespace_prefix(ns);
        let entity_key = EntityKey::from(&[0x12; 32]);
        let full_key = build_entity_key(ns, &entity_key);

        // Full key should start with namespace prefix
        assert!(full_key.starts_with(&prefix));
    }

    #[test]
    fn test_namespace_isolation() {
        // Keys from different namespaces should not collide
        let key_bytes = [0xaa; 32];
        let entity_key = EntityKey::from(&key_bytes);

        let accounts_key = build_entity_key("accounts", &entity_key);
        let pools_key = build_entity_key("pools", &entity_key);

        // Same entity key, different namespace -> different prefixed keys
        assert_ne!(accounts_key, pools_key);

        // But entity key portion is the same
        assert_eq!(&accounts_key[NS_HASH_SIZE..], &pools_key[NS_HASH_SIZE..]);
    }

    #[test]
    fn test_range_keys() {
        let ns = "epochs";
        let start = EntityKey::from(&[0x00; 32]);
        let end = EntityKey::from(&[0xff; 32]);

        let start_key = build_range_start(ns, &start);
        let end_key = build_range_end(ns, &end);

        // Both should have same namespace prefix
        assert_eq!(&start_key[..NS_HASH_SIZE], &end_key[..NS_HASH_SIZE]);

        // Start should be less than end lexicographically
        assert!(start_key < end_key);
    }
}
