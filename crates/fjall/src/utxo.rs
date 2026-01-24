//! UTxO filter index operations for fjall.
//!
//! These indexes map lookup keys (addresses, policies, assets) to sets of TxoRefs.
//! Each entry is stored as a composite key: `lookup_key ++ txo_ref` with an empty value.
//! Queries use prefix scanning to find all TxoRefs for a given lookup key.

use std::collections::HashSet;

use dolos_core::{IndexDelta, TxoRef, UtxoSet};
use fjall::{Keyspace, OwnedWriteBatch};

use crate::keys::{decode_txo_ref_from_suffix, utxo_composite_key, TXO_REF_SIZE};
use crate::{utxo_dimensions, Error};

/// References to all UTxO filter keyspaces
pub struct UtxoKeyspaces<'a> {
    pub address: &'a Keyspace,
    pub payment: &'a Keyspace,
    pub stake: &'a Keyspace,
    pub policy: &'a Keyspace,
    pub asset: &'a Keyspace,
}

impl<'a> UtxoKeyspaces<'a> {
    /// Get keyspace for a tag dimension
    fn keyspace_for_dimension(&self, dimension: &str) -> Option<&'a Keyspace> {
        match dimension {
            utxo_dimensions::ADDRESS => Some(self.address),
            utxo_dimensions::PAYMENT => Some(self.payment),
            utxo_dimensions::STAKE => Some(self.stake),
            utxo_dimensions::POLICY => Some(self.policy),
            utxo_dimensions::ASSET => Some(self.asset),
            _ => None,
        }
    }
}

/// Insert a UTxO entry into a keyspace
fn insert_entry(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, key: &[u8], txo: &TxoRef) {
    let composite = utxo_composite_key(key, txo);
    batch.insert(keyspace, composite, []);
}

/// Remove a UTxO entry from a keyspace
fn remove_entry(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, key: &[u8], txo: &TxoRef) {
    let composite = utxo_composite_key(key, txo);
    batch.remove(keyspace, composite);
}

/// Apply UTxO filter changes from an IndexDelta
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspaces: &UtxoKeyspaces,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Insert produced UTxOs
    for (txo_ref, tags) in &delta.utxo.produced {
        for tag in tags {
            if let Some(keyspace) = keyspaces.keyspace_for_dimension(tag.dimension) {
                insert_entry(batch, keyspace, &tag.key, txo_ref);
            }
        }
    }

    // Remove consumed UTxOs
    for (txo_ref, tags) in &delta.utxo.consumed {
        for tag in tags {
            if let Some(keyspace) = keyspaces.keyspace_for_dimension(tag.dimension) {
                remove_entry(batch, keyspace, &tag.key, txo_ref);
            }
        }
    }

    Ok(())
}

/// Undo UTxO filter changes from an IndexDelta (for rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    keyspaces: &UtxoKeyspaces,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Remove produced UTxOs (undo insertion)
    for (txo_ref, tags) in &delta.utxo.produced {
        for tag in tags {
            if let Some(keyspace) = keyspaces.keyspace_for_dimension(tag.dimension) {
                remove_entry(batch, keyspace, &tag.key, txo_ref);
            }
        }
    }

    // Restore consumed UTxOs (undo removal)
    for (txo_ref, tags) in &delta.utxo.consumed {
        for tag in tags {
            if let Some(keyspace) = keyspaces.keyspace_for_dimension(tag.dimension) {
                insert_entry(batch, keyspace, &tag.key, txo_ref);
            }
        }
    }

    Ok(())
}

/// Get all TxoRefs for a given lookup key using prefix scanning
pub fn get_by_key(keyspace: &Keyspace, lookup_key: &[u8]) -> Result<UtxoSet, Error> {
    let mut result = HashSet::new();

    // Prefix scan: all keys starting with lookup_key
    // fjall's prefix() returns an iterator of Guard items
    // Guard::key() consumes the guard and returns Result<UserKey>
    for guard in keyspace.prefix(lookup_key) {
        let key = guard.key()?;

        // Key format: lookup_key ++ txo_ref
        // We need to extract txo_ref from the suffix
        if key.len() >= lookup_key.len() + TXO_REF_SIZE {
            let txo_ref = decode_txo_ref_from_suffix(&key);
            result.insert(txo_ref);
        }
    }

    Ok(result)
}
