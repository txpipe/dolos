//! UTxO tag index operations for fjall (chain-agnostic).
//!
//! These indexes map lookup keys (addresses, policies, assets) to sets of TxoRefs.
//! All entries are stored in the `index-tags` keyspace with dimension hash prefixes.
//! Key format: `[dim_hash:8][lookup_key:var][txo_ref:36]` with empty value.
//! Queries use prefix scanning to find all TxoRefs for a given lookup key.
//!
//! The dimension hash is computed as `xxh3("utxo:" + dimension)`, making the
//! storage layer fully chain-agnostic.

use std::collections::HashSet;

use dolos_core::{IndexDelta, TxoRef, UtxoSet};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use super::tag_keys::{build_utxo_tag_key, build_utxo_tag_prefix, decode_utxo_tag_txo};
use crate::keys::{DIM_HASH_SIZE, TXO_REF_SIZE};
use crate::Error;

/// Insert a UTxO tag entry into the tags keyspace
fn insert_entry(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    txo: &TxoRef,
) {
    let key = build_utxo_tag_key(dimension, lookup_key, txo);
    batch.insert(tags_keyspace, key, []);
}

/// Remove a UTxO tag entry from the tags keyspace
fn remove_entry(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    txo: &TxoRef,
) {
    let key = build_utxo_tag_key(dimension, lookup_key, txo);
    batch.remove(tags_keyspace, key);
}

/// Apply UTxO tag changes from an IndexDelta to the tags keyspace
pub fn apply(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Insert produced UTxOs
    for (txo_ref, tags) in &delta.utxo.produced {
        for tag in tags {
            insert_entry(batch, tags_keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    // Remove consumed UTxOs
    for (txo_ref, tags) in &delta.utxo.consumed {
        for tag in tags {
            remove_entry(batch, tags_keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    Ok(())
}

/// Undo UTxO tag changes from an IndexDelta (for rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Remove produced UTxOs (undo insertion)
    for (txo_ref, tags) in &delta.utxo.produced {
        for tag in tags {
            remove_entry(batch, tags_keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    // Restore consumed UTxOs (undo removal)
    for (txo_ref, tags) in &delta.utxo.consumed {
        for tag in tags {
            insert_entry(batch, tags_keyspace, tag.dimension, &tag.key, txo_ref);
        }
    }

    Ok(())
}

/// Get all TxoRefs for a given dimension and lookup key using prefix scanning.
///
/// The dimension string is passed directly (chain-agnostic).
///
/// Uses the `Readable` trait to support both direct keyspace access and snapshot-based
/// reads. Snapshot-based reads avoid potential deadlocks with concurrent writes by using
/// MVCC (Multi-Version Concurrency Control).
pub fn get_by_key<R: Readable>(
    readable: &R,
    tags_keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
) -> Result<UtxoSet, Error> {
    let mut result = HashSet::new();

    // Build prefix: [dim_hash:8][lookup_key]
    let prefix = build_utxo_tag_prefix(dimension, lookup_key);

    // Prefix scan: all keys starting with prefix
    for guard in readable.prefix(tags_keyspace, prefix) {
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
