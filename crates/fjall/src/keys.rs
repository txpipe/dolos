//! Key and value encoding utilities for fjall storage.
//!
//! All multi-byte integers are encoded as big-endian to ensure correct
//! lexicographic ordering in the LSM tree.
//!
//! ## Index Store Keys
//!
//! All index keys use dimension hashing for chain-agnostic storage:
//! - **UTxO tag**: `[dim_hash:8][lookup_key:var][txo_ref:36]`
//! - **Block tag**: `[dim_hash:8][xxh3(tag_key):8][slot:8]`
//! - **Exact lookup**: `[dim_hash:8][key_data:var]`
//!
//! ## State Store Keys
//!
//! - **UTxOs**: `txhash[32] ++ idx[4]` (36 bytes) -> `era[2] ++ cbor[...]`
//! - **Entities**: `[ns_hash:8][key:32]` (40 bytes) -> value bytes

use dolos_core::{Era, TxoRef};
use std::hash::Hasher;
use xxhash_rust::xxh3::{xxh3_64, Xxh3};

/// Size of encoded TxoRef: 32-byte tx hash + 4-byte index
pub const TXO_REF_SIZE: usize = 36;

/// Size of encoded slot: 8-byte u64
pub const SLOT_SIZE: usize = 8;

/// Size of encoded hash key: 8-byte u64
pub const HASH_KEY_SIZE: usize = 8;

/// Size of dimension hash: 8 bytes
pub const DIM_HASH_SIZE: usize = 8;

// ============================================================================
// Dimension Hashing (Chain-Agnostic Index Keys)
// ============================================================================

/// Internal dimension prefix constants for index store.
///
/// These prefixes are combined with dimension strings to create unique
/// hashes that distinguish between different index types (UTxO tags,
/// block tags, exact lookups) even when they share the same dimension name.
pub mod dim_prefix {
    /// Prefix for UTxO tag dimensions (current state filters)
    pub const UTXO: &str = "utxo";
    /// Prefix for block tag dimensions (historical approximate lookups)
    pub const BLOCK: &str = "block";
    /// Prefix for exact lookup dimensions (point queries)
    pub const EXACT: &str = "exact";
}

/// Hash a qualified dimension string to 8 bytes.
///
/// Combines prefix and dimension with ":" separator, then hashes with xxh3.
/// This ensures dimensions with the same name but different types don't collide.
///
/// # Examples
///
/// ```ignore
/// // These produce different hashes:
/// hash_dimension("utxo", "address")   // hashes "utxo:address"
/// hash_dimension("block", "address")  // hashes "block:address"
/// ```
pub fn hash_dimension(prefix: &str, dim: &str) -> [u8; DIM_HASH_SIZE] {
    let mut hasher = Xxh3::new();
    hasher.write(prefix.as_bytes());
    hasher.write(b":");
    hasher.write(dim.as_bytes());
    hasher.finish().to_be_bytes()
}

/// Encode a TxoRef as 36 bytes: tx_hash (32) + index_be (4)
pub fn encode_txo_ref(txo: &TxoRef) -> [u8; TXO_REF_SIZE] {
    let mut result = [0u8; TXO_REF_SIZE];
    result[..32].copy_from_slice(txo.0.as_ref());
    result[32..].copy_from_slice(&txo.1.to_be_bytes());
    result
}

/// Decode a TxoRef from 36 bytes
pub fn decode_txo_ref(bytes: &[u8]) -> TxoRef {
    debug_assert!(bytes.len() >= TXO_REF_SIZE);
    let mut hash = [0u8; 32];
    hash.copy_from_slice(&bytes[..32]);
    let index = u32::from_be_bytes(bytes[32..36].try_into().unwrap());
    TxoRef(hash.into(), index)
}

/// Encode a slot as big-endian u64
pub fn encode_slot(slot: u64) -> [u8; SLOT_SIZE] {
    slot.to_be_bytes()
}

/// Decode a slot from big-endian bytes
pub fn decode_slot(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() >= SLOT_SIZE);
    u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

/// Encode a u64 as big-endian bytes
pub fn encode_u64(value: u64) -> [u8; 8] {
    value.to_be_bytes()
}

/// Decode a u64 from big-endian bytes
pub fn decode_u64(bytes: &[u8]) -> u64 {
    debug_assert!(bytes.len() >= 8);
    u64::from_be_bytes(bytes[..8].try_into().unwrap())
}

/// Build a composite key by concatenating prefix and suffix
pub fn composite_key(prefix: &[u8], suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(prefix.len() + suffix.len());
    key.extend_from_slice(prefix);
    key.extend_from_slice(suffix);
    key
}

/// Build a composite key for UTxO indexes: lookup_key ++ txo_ref
pub fn utxo_composite_key(lookup_key: &[u8], txo: &TxoRef) -> Vec<u8> {
    let txo_bytes = encode_txo_ref(txo);
    composite_key(lookup_key, &txo_bytes)
}

/// Build a composite key for archive indexes: xxh3_hash ++ slot
pub fn archive_composite_key(hash: u64, slot: u64) -> [u8; 16] {
    let mut key = [0u8; 16];
    key[..8].copy_from_slice(&hash.to_be_bytes());
    key[8..].copy_from_slice(&slot.to_be_bytes());
    key
}

/// Extract slot from the end of a composite key (last 8 bytes)
pub fn decode_slot_from_suffix(key: &[u8]) -> u64 {
    debug_assert!(key.len() >= SLOT_SIZE);
    let start = key.len() - SLOT_SIZE;
    decode_slot(&key[start..])
}

/// Extract TxoRef from the end of a composite key (last 36 bytes)
pub fn decode_txo_ref_from_suffix(key: &[u8]) -> TxoRef {
    debug_assert!(key.len() >= TXO_REF_SIZE);
    let start = key.len() - TXO_REF_SIZE;
    decode_txo_ref(&key[start..])
}

/// Hash variable-length data to a fixed u64 using xxh3
pub fn hash_key(data: &[u8]) -> u64 {
    xxh3_64(data)
}

/// Build prefix for archive index queries (just the hash portion)
pub fn archive_prefix(hash: u64) -> [u8; HASH_KEY_SIZE] {
    hash.to_be_bytes()
}

// ============================================================================
// State Store Encodings
// ============================================================================

/// Size of era encoding: 2-byte u16
pub const ERA_SIZE: usize = 2;

/// Size of datum reference count: 8-byte u64
pub const REFCOUNT_SIZE: usize = 8;

/// Encode a UTxO value: era (2 bytes BE) + cbor
pub fn encode_utxo_value(era: Era, cbor: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(ERA_SIZE + cbor.len());
    value.extend_from_slice(&era.to_be_bytes());
    value.extend_from_slice(cbor);
    value
}

/// Decode a UTxO value into (era, cbor)
pub fn decode_utxo_value(bytes: &[u8]) -> Option<(Era, Vec<u8>)> {
    if bytes.len() < ERA_SIZE {
        return None;
    }
    let era = u16::from_be_bytes(bytes[..ERA_SIZE].try_into().ok()?);
    let cbor = bytes[ERA_SIZE..].to_vec();
    Some((era, cbor))
}

/// Encode a datum value: refcount (8 bytes BE) + datum bytes
pub fn encode_datum_value(refcount: u64, datum_bytes: &[u8]) -> Vec<u8> {
    let mut value = Vec::with_capacity(REFCOUNT_SIZE + datum_bytes.len());
    value.extend_from_slice(&refcount.to_be_bytes());
    value.extend_from_slice(datum_bytes);
    value
}

/// Decode a datum value into (refcount, datum_bytes)
pub fn decode_datum_value(bytes: &[u8]) -> Option<(u64, Vec<u8>)> {
    if bytes.len() < REFCOUNT_SIZE {
        return None;
    }
    let refcount = u64::from_be_bytes(bytes[..REFCOUNT_SIZE].try_into().ok()?);
    let datum_bytes = bytes[REFCOUNT_SIZE..].to_vec();
    Some((refcount, datum_bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_txo_ref_roundtrip() {
        let txo = TxoRef([0xab; 32].into(), 42);
        let encoded = encode_txo_ref(&txo);
        let decoded = decode_txo_ref(&encoded);
        assert_eq!(txo.0, decoded.0);
        assert_eq!(txo.1, decoded.1);
    }

    #[test]
    fn test_slot_roundtrip() {
        let slot = 141868807u64;
        let encoded = encode_slot(slot);
        let decoded = decode_slot(&encoded);
        assert_eq!(slot, decoded);
    }

    #[test]
    fn test_archive_key_ordering() {
        // Keys with same hash should be ordered by slot
        let hash = hash_key(b"test_address");
        let key1 = archive_composite_key(hash, 100);
        let key2 = archive_composite_key(hash, 200);
        let key3 = archive_composite_key(hash, 50);

        assert!(key1 < key2);
        assert!(key3 < key1);
    }

    #[test]
    fn test_decode_slot_from_suffix() {
        let hash = hash_key(b"test");
        let slot = 12345u64;
        let key = archive_composite_key(hash, slot);
        assert_eq!(decode_slot_from_suffix(&key), slot);
    }

    #[test]
    fn test_utxo_composite_key() {
        let address = b"addr_test1qz...";
        let txo = TxoRef([0xcd; 32].into(), 7);
        let key = utxo_composite_key(address, &txo);

        assert_eq!(key.len(), address.len() + TXO_REF_SIZE);
        assert_eq!(&key[..address.len()], address);

        let decoded_txo = decode_txo_ref_from_suffix(&key);
        assert_eq!(txo.0, decoded_txo.0);
        assert_eq!(txo.1, decoded_txo.1);
    }

    #[test]
    fn test_utxo_value_roundtrip() {
        let era: Era = 6;
        let cbor = vec![0x82, 0x00, 0x01, 0x02, 0x03];
        let encoded = encode_utxo_value(era, &cbor);
        let (decoded_era, decoded_cbor) = decode_utxo_value(&encoded).unwrap();
        assert_eq!(era, decoded_era);
        assert_eq!(cbor, decoded_cbor);
    }

    #[test]
    fn test_datum_value_roundtrip() {
        let refcount = 42u64;
        let datum_bytes = vec![0xd8, 0x79, 0x9f, 0x01, 0x02, 0xff];
        let encoded = encode_datum_value(refcount, &datum_bytes);
        let (decoded_refcount, decoded_bytes) = decode_datum_value(&encoded).unwrap();
        assert_eq!(refcount, decoded_refcount);
        assert_eq!(datum_bytes, decoded_bytes);
    }

    #[test]
    fn test_empty_cbor_utxo_value() {
        let era: Era = 1;
        let cbor: Vec<u8> = vec![];
        let encoded = encode_utxo_value(era, &cbor);
        assert_eq!(encoded.len(), ERA_SIZE);
        let (decoded_era, decoded_cbor) = decode_utxo_value(&encoded).unwrap();
        assert_eq!(era, decoded_era);
        assert!(decoded_cbor.is_empty());
    }

    #[test]
    fn test_hash_dimension_deterministic() {
        let hash1 = hash_dimension("utxo", "address");
        let hash2 = hash_dimension("utxo", "address");
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_hash_dimension_different_dims() {
        let hash1 = hash_dimension("utxo", "address");
        let hash2 = hash_dimension("utxo", "payment");
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_dimension_different_prefixes() {
        // Same dimension name, different prefix -> different hash
        let utxo_hash = hash_dimension("utxo", "address");
        let block_hash = hash_dimension("block", "address");
        assert_ne!(utxo_hash, block_hash);
    }

    #[test]
    fn test_hash_dimension_size() {
        let hash = hash_dimension("exact", "block_hash");
        assert_eq!(hash.len(), DIM_HASH_SIZE);
    }
}
