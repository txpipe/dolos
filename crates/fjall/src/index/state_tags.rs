//! State tag index operations for the `state-tags` keyspace (chain-agnostic).
//!
//! This module handles key encoding, batch writes (apply/undo), and read queries
//! for the `state-tags` keyspace. These indexes map lookup keys (addresses, policies,
//! assets) to sets of TxoRefs.
//!
//! Key format: `[dim_hash:8][lookup_key:var][txo_ref:36]` with empty value.
//! Queries use prefix scanning to find all TxoRefs for a given lookup key.
//!
//! The `dim_hash` is computed as `xxh3("utxo:" + dimension)`, making the
//! storage layer fully chain-agnostic.

use std::collections::HashSet;

use dolos_core::{IndexDelta, TxoRef, UtxoSet};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use crate::keys::{
    decode_txo_ref, dim_prefix, encode_txo_ref, hash_dimension, DIM_HASH_SIZE, TXO_REF_SIZE,
};
use crate::Error;

// ============================================================================
// Key Encoding
// ============================================================================

/// Build a UTxO tag key: `[dim_hash:8][lookup_key:var][txo_ref:36]`
fn build_utxo_tag_key(dimension: &str, lookup_key: &[u8], txo: &TxoRef) -> Vec<u8> {
    let dim_hash = hash_dimension(dim_prefix::UTXO, dimension);
    let txo_bytes = encode_txo_ref(txo);
    let mut key = Vec::with_capacity(DIM_HASH_SIZE + lookup_key.len() + TXO_REF_SIZE);
    key.extend_from_slice(&dim_hash);
    key.extend_from_slice(lookup_key);
    key.extend_from_slice(&txo_bytes);
    key
}

/// Build prefix for UTxO tag queries: `[dim_hash:8][lookup_key:var]`
fn build_utxo_tag_prefix(dimension: &str, lookup_key: &[u8]) -> Vec<u8> {
    let dim_hash = hash_dimension(dim_prefix::UTXO, dimension);
    let mut prefix = Vec::with_capacity(DIM_HASH_SIZE + lookup_key.len());
    prefix.extend_from_slice(&dim_hash);
    prefix.extend_from_slice(lookup_key);
    prefix
}

/// Decode TxoRef from UTxO tag key (last 36 bytes)
fn decode_utxo_tag_txo(key: &[u8]) -> TxoRef {
    debug_assert!(key.len() >= DIM_HASH_SIZE + TXO_REF_SIZE);
    let start = key.len() - TXO_REF_SIZE;
    decode_txo_ref(&key[start..])
}

// ============================================================================
// Operations
// ============================================================================

/// Insert a UTxO tag entry
fn insert_entry(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    txo: &TxoRef,
) {
    let key = build_utxo_tag_key(dimension, lookup_key, txo);
    batch.insert(keyspace, key, []);
}

/// Remove a UTxO tag entry
fn remove_entry(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    txo: &TxoRef,
) {
    let key = build_utxo_tag_key(dimension, lookup_key, txo);
    batch.remove(keyspace, key);
}

/// Apply UTxO tag changes from an IndexDelta to the state-tags keyspace
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Insert produced UTxOs
    for (txo_ref, tags) in &delta.utxo.produced {
        for tag in tags {
            insert_entry(batch, keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    // Remove consumed UTxOs
    for (txo_ref, tags) in &delta.utxo.consumed {
        for tag in tags {
            remove_entry(batch, keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    Ok(())
}

/// Undo UTxO tag changes from an IndexDelta (for rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Remove produced UTxOs (undo insertion)
    for (txo_ref, tags) in &delta.utxo.produced {
        for tag in tags {
            remove_entry(batch, keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    // Restore consumed UTxOs (undo removal)
    for (txo_ref, tags) in &delta.utxo.consumed {
        for tag in tags {
            insert_entry(batch, keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    Ok(())
}

// ============================================================================
// Queries
// ============================================================================

/// Get all TxoRefs for a given dimension and lookup key using prefix scanning.
///
/// The dimension string is passed directly (chain-agnostic).
///
/// Uses the `Readable` trait to support both direct keyspace access and snapshot-based
/// reads. Snapshot-based reads avoid potential deadlocks with concurrent writes by using
/// MVCC (Multi-Version Concurrency Control).
pub fn get_by_key<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
) -> Result<UtxoSet, Error> {
    let mut result = HashSet::new();

    // Build prefix: [dim_hash:8][lookup_key]
    let prefix = build_utxo_tag_prefix(dimension, lookup_key);

    // Prefix scan: all keys starting with prefix
    for guard in readable.prefix(keyspace, prefix) {
        let key = guard.key()?;

        // Key format: [dim_hash:8][lookup_key:var][txo_ref:36]
        // Minimum length: DIM_HASH_SIZE + lookup_key.len() + TXO_REF_SIZE
        if key.len() >= DIM_HASH_SIZE + lookup_key.len() + TXO_REF_SIZE {
            let txo_ref = decode_utxo_tag_txo(&key);
            result.insert(txo_ref);
        }
    }

    Ok(result)
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
        let key = build_utxo_tag_key("some_custom_dimension", b"data", &TxoRef([0; 32].into(), 0));

        // Key should be valid size
        assert!(key.len() > DIM_HASH_SIZE);
    }
}
