//! Tag key encoding for the index-tags keyspace (chain-agnostic).
//!
//! Both UTxO tags (current state) and block tags (historical) share a single keyspace.
//! Keys are prefixed with an 8-byte hash of the qualified dimension string, making
//! the storage layer fully chain-agnostic.
//!
//! ## Key Format
//!
//! | Type | Key Format | Value |
//! |------|------------|-------|
//! | UTxO Tag | `[dim_hash:8][lookup_key:var][txo_ref:36]` | empty |
//! | Block Tag | `[dim_hash:8][xxh3(tag_key):8][slot:8]` | empty |
//!
//! The `dim_hash` is computed as `xxh3("utxo:" + dimension)` or `xxh3("block:" + dimension)`,
//! ensuring that UTxO and block tags with the same dimension name don't collide.

use dolos_core::TxoRef;

use crate::keys::{
    decode_slot, decode_txo_ref, dim_prefix, encode_txo_ref, hash_dimension, hash_key,
    DIM_HASH_SIZE, HASH_KEY_SIZE, SLOT_SIZE, TXO_REF_SIZE,
};

// ============================================================================
// UTxO Tag Key Encoding
// ============================================================================

/// Build a UTxO tag key: `[dim_hash:8][lookup_key:var][txo_ref:36]`
pub fn build_utxo_tag_key(dimension: &str, lookup_key: &[u8], txo: &TxoRef) -> Vec<u8> {
    let dim_hash = hash_dimension(dim_prefix::UTXO, dimension);
    let txo_bytes = encode_txo_ref(txo);
    let mut key = Vec::with_capacity(DIM_HASH_SIZE + lookup_key.len() + TXO_REF_SIZE);
    key.extend_from_slice(&dim_hash);
    key.extend_from_slice(lookup_key);
    key.extend_from_slice(&txo_bytes);
    key
}

/// Build prefix for UTxO tag queries: `[dim_hash:8][lookup_key:var]`
pub fn build_utxo_tag_prefix(dimension: &str, lookup_key: &[u8]) -> Vec<u8> {
    let dim_hash = hash_dimension(dim_prefix::UTXO, dimension);
    let mut prefix = Vec::with_capacity(DIM_HASH_SIZE + lookup_key.len());
    prefix.extend_from_slice(&dim_hash);
    prefix.extend_from_slice(lookup_key);
    prefix
}

/// Decode TxoRef from UTxO tag key (last 36 bytes)
pub fn decode_utxo_tag_txo(key: &[u8]) -> TxoRef {
    debug_assert!(key.len() >= DIM_HASH_SIZE + TXO_REF_SIZE);
    let start = key.len() - TXO_REF_SIZE;
    decode_txo_ref(&key[start..])
}

// ============================================================================
// Block Tag Key Encoding
// ============================================================================

/// Fixed size of block tag key: dim_hash(8) + tag_hash(8) + slot(8) = 24 bytes
pub const BLOCK_TAG_KEY_SIZE: usize = DIM_HASH_SIZE + HASH_KEY_SIZE + SLOT_SIZE;

/// Build a block tag key: `[dim_hash:8][xxh3(tag_key):8][slot:8]`
pub fn build_block_tag_key(dimension: &str, tag_key: &[u8], slot: u64) -> [u8; BLOCK_TAG_KEY_SIZE] {
    let tag_hash = hash_key(tag_key);
    build_block_tag_key_hashed(dimension, tag_hash, slot)
}

/// Build a block tag key with pre-computed tag hash: `[dim_hash:8][tag_hash:8][slot:8]`
pub fn build_block_tag_key_hashed(
    dimension: &str,
    tag_hash: u64,
    slot: u64,
) -> [u8; BLOCK_TAG_KEY_SIZE] {
    let dim_hash = hash_dimension(dim_prefix::BLOCK, dimension);
    let mut key = [0u8; BLOCK_TAG_KEY_SIZE];
    key[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    key[DIM_HASH_SIZE..DIM_HASH_SIZE + HASH_KEY_SIZE].copy_from_slice(&tag_hash.to_be_bytes());
    key[DIM_HASH_SIZE + HASH_KEY_SIZE..].copy_from_slice(&slot.to_be_bytes());
    key
}

/// Build prefix for block tag queries: `[dim_hash:8][tag_hash:8]`
pub fn build_block_tag_prefix_hashed(
    dimension: &str,
    tag_hash: u64,
) -> [u8; DIM_HASH_SIZE + HASH_KEY_SIZE] {
    let dim_hash = hash_dimension(dim_prefix::BLOCK, dimension);
    let mut prefix = [0u8; DIM_HASH_SIZE + HASH_KEY_SIZE];
    prefix[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    prefix[DIM_HASH_SIZE..].copy_from_slice(&tag_hash.to_be_bytes());
    prefix
}

/// Decode slot from block tag key (last 8 bytes)
pub fn decode_block_tag_slot(key: &[u8]) -> u64 {
    debug_assert!(key.len() >= BLOCK_TAG_KEY_SIZE);
    let start = key.len() - SLOT_SIZE;
    decode_slot(&key[start..])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utxo_tag_key_roundtrip() {
        let lookup_key = b"addr_test1qz...";
        let txo = TxoRef([0xab; 32].into(), 42);
        let key = build_utxo_tag_key("address", lookup_key, &txo);

        // Verify size: dim_hash(8) + lookup_key + txo_ref(36)
        assert_eq!(key.len(), DIM_HASH_SIZE + lookup_key.len() + TXO_REF_SIZE);

        // Verify TxoRef decoding
        let decoded_txo = decode_utxo_tag_txo(&key);
        assert_eq!(txo.0, decoded_txo.0);
        assert_eq!(txo.1, decoded_txo.1);
    }

    #[test]
    fn test_utxo_tag_prefix() {
        let lookup_key = b"addr_test1qz...";
        let txo = TxoRef([0xab; 32].into(), 42);
        let key = build_utxo_tag_key("address", lookup_key, &txo);
        let prefix = build_utxo_tag_prefix("address", lookup_key);

        // Key should start with prefix
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn test_block_tag_key_roundtrip() {
        let tag_key = b"some_address_bytes";
        let slot = 141868807u64;
        let key = build_block_tag_key("address", tag_key, slot);

        assert_eq!(key.len(), BLOCK_TAG_KEY_SIZE);

        let decoded_slot = decode_block_tag_slot(&key);
        assert_eq!(slot, decoded_slot);
    }

    #[test]
    fn test_block_tag_ordering() {
        let tag_key = b"test_address";
        let key1 = build_block_tag_key("address", tag_key, 100);
        let key2 = build_block_tag_key("address", tag_key, 200);
        let key3 = build_block_tag_key("address", tag_key, 50);

        // Same prefix, ordered by slot
        assert!(key1 < key2);
        assert!(key3 < key1);
    }

    #[test]
    fn test_block_tag_prefix() {
        let tag_key = b"test_address";
        let key = build_block_tag_key("address", tag_key, 12345);
        let tag_hash = hash_key(tag_key);
        let prefix = build_block_tag_prefix_hashed("address", tag_hash);

        // Key should start with prefix
        assert!(key.starts_with(&prefix));
    }

    #[test]
    fn test_utxo_block_separation() {
        // UTxO and block tags with same dimension name should have different prefixes
        let utxo_key = build_utxo_tag_key("address", b"addr", &TxoRef([0; 32].into(), 0));
        let block_key = build_block_tag_key("address", b"addr", 0);

        // First 8 bytes (dim_hash) should be different
        assert_ne!(&utxo_key[..DIM_HASH_SIZE], &block_key[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_dimension_separation() {
        // Different dimensions should have different prefixes
        let key1 = build_utxo_tag_key("address", b"test", &TxoRef([0; 32].into(), 0));
        let key2 = build_utxo_tag_key("payment", b"test", &TxoRef([0; 32].into(), 0));

        // First 8 bytes (dim_hash) should be different
        assert_ne!(&key1[..DIM_HASH_SIZE], &key2[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_any_dimension_works() {
        // Any dimension string should work (chain-agnostic)
        let key1 = build_utxo_tag_key("some_custom_dimension", b"data", &TxoRef([0; 32].into(), 0));
        let key2 = build_block_tag_key("another_dimension", b"data", 12345);

        // Keys should be valid sizes
        assert!(key1.len() > DIM_HASH_SIZE);
        assert_eq!(key2.len(), BLOCK_TAG_KEY_SIZE);
    }
}
