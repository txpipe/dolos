//! Entity key encoding for the unified state-entities keyspace.
//!
//! All entity types share a single keyspace with namespace hash prefixes.
//! This reduces the number of LSM-tree segment files compared to separate
//! keyspaces per entity type.
//!
//! ## Key Format
//!
//! ```text
//! Key:   [ns_hash:8][entity_key:var]
//! Value: entity value bytes (CBOR encoded)
//! ```
//!
//! The `ns_hash` is the first 8 bytes of xxh3 hash of the namespace string
//! (e.g., "accounts", "pools"). This provides:
//! - Deterministic IDs without hardcoded mapping
//! - Extensibility for new entity types without code changes
//! - Consistent ordering within each namespace (entities with same namespace
//!   are grouped)
//!
//! The entity key is the typed key's encoding, of whatever length its kind
//! has. A namespace's keys sort after its bare hash and before the hash's
//! successor, which bounds a range left open at its end.

use std::ops::Bound;

use dolos_core::{EntityKey, KeyRange, Namespace};

use crate::{keys::hash_key, Error};

/// Size of namespace hash prefix: 8 bytes (xxh3 truncated)
pub const NS_HASH_SIZE: usize = 8;

/// Hash a namespace string to an 8-byte prefix using xxh3.
///
/// The hash is stored as big-endian bytes for consistent lexicographic
/// ordering.
pub fn hash_namespace(ns: Namespace) -> [u8; NS_HASH_SIZE] {
    let hash = hash_key(ns.as_bytes());
    hash.to_be_bytes()
}

/// `[ns_hash:8][bytes]`
pub fn prefixed(ns: Namespace, bytes: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(NS_HASH_SIZE + bytes.len());
    result.extend_from_slice(&hash_namespace(ns));
    result.extend_from_slice(bytes);
    result
}

/// Build entity key: `[ns_hash:8][entity_key]`
pub fn build_entity_key(ns: Namespace, key: &EntityKey) -> Vec<u8> {
    prefixed(ns, key.as_ref())
}

/// The exclusive end of a namespace: the successor of its hash, which sorts
/// after every key of the namespace and before every key of the next.
/// Unbounded only for the all-`0xff` hash.
pub fn namespace_end(ns: Namespace) -> Bound<Vec<u8>> {
    match dolos_core::keys::successor(&hash_namespace(ns)) {
        Some(end) => Bound::Excluded(end.to_vec()),
        None => Bound::Unbounded,
    }
}

/// The stored bounds of `range` within a namespace.
pub fn build_range(ns: Namespace, range: &KeyRange) -> (Bound<Vec<u8>>, Bound<Vec<u8>>) {
    let start = Bound::Included(prefixed(ns, &range.start));

    let end = match &range.end {
        Some(end) => Bound::Excluded(prefixed(ns, end)),
        None => namespace_end(ns),
    };

    (start, end)
}

/// Decode the entity key out of a stored key.
pub fn decode_entity_key(key: &[u8]) -> Result<EntityKey, Error> {
    match key.get(NS_HASH_SIZE..) {
        Some(entity) => Ok(EntityKey::from_raw(entity)),
        None => Err(Error::Codec(format!(
            "malformed state entity key of {} bytes",
            key.len()
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::ops::RangeBounds;

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
        for len in [0, 2, 28, 32, 68] {
            let original_key = EntityKey::from_raw(&vec![0xab; len]);

            let prefixed = build_entity_key("accounts", &original_key);
            assert_eq!(prefixed.len(), NS_HASH_SIZE + len);

            let decoded = decode_entity_key(&prefixed).unwrap();
            assert_eq!(original_key, decoded);
        }
    }

    #[test]
    fn test_short_stored_key_is_an_error() {
        assert!(decode_entity_key(&[0; NS_HASH_SIZE - 1]).is_err());
    }

    #[test]
    fn test_entity_key_structure() {
        let ns = "accounts";
        let entity_key = EntityKey::from_raw(&[0xcd; 32]);

        let prefixed = build_entity_key(ns, &entity_key);

        // First 8 bytes should be namespace hash
        let expected_prefix = hash_namespace(ns);
        assert_eq!(&prefixed[..NS_HASH_SIZE], &expected_prefix);

        // Remaining bytes should be entity key
        assert_eq!(&prefixed[NS_HASH_SIZE..], entity_key.as_ref());
    }

    #[test]
    fn test_namespace_isolation() {
        // Keys from different namespaces should not collide
        let entity_key = EntityKey::from_raw(&[0xaa; 32]);

        let accounts_key = build_entity_key("accounts", &entity_key);
        let pools_key = build_entity_key("pools", &entity_key);

        // Same entity key, different namespace -> different prefixed keys
        assert_ne!(accounts_key, pools_key);

        // But entity key portion is the same
        assert_eq!(&accounts_key[NS_HASH_SIZE..], &pools_key[NS_HASH_SIZE..]);
    }

    #[test]
    fn test_open_range_covers_the_namespace_only() {
        let ns = "epochs";
        let range = build_range(ns, &KeyRange::all());

        let empty = build_entity_key(ns, &EntityKey::from_raw(&[]));
        let long = build_entity_key(ns, &EntityKey::from_raw(&[0xff; 100]));
        assert!(range.contains(&empty));
        assert!(range.contains(&long));

        let next = dolos_core::keys::successor(&hash_namespace(ns))
            .unwrap()
            .to_vec();
        assert!(!range.contains(&next));
    }
}
