//! Live-UTxO tag operations for the `state-tags` keyspace (chain-agnostic).
//!
//! This module handles key encoding, batch writes (apply/undo), and read
//! queries for the `state-tags` keyspace of the state store. These tags map
//! lookup keys (addresses, policies, assets) to sets of TxoRefs, and are
//! written in the same batch as the UTxO set they project.
//!
//! Key format: `[dim_hash:8][lookup_key:var][tag_order:10][txo_ref:36]` with
//! empty value. `tag_order` is the UTxO's creation position as
//! `UtxoPosition::tag_order` writes it, so the keys of one lookup key sort in
//! creation order, then by ref. Queries use prefix scanning to find the
//! TxoRefs for a given lookup key, forward or in reverse.
//!
//! The `dim_hash` is computed as `xxh3("utxo:" + dimension)`, making the
//! storage layer fully chain-agnostic.

use std::collections::HashSet;
use std::ops::Bound;

use dolos_core::{
    TagEntry, TagOrder, TagStart, TxoRef, UtxoIndexDelta, UtxoPosition, UtxoSet, TAG_ORDER_LEN,
};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use crate::keys::{
    decode_txo_ref, dim_prefix, encode_txo_ref, hash_dimension, DIM_HASH_SIZE, TXO_REF_SIZE,
};
use crate::Error;

// ============================================================================
// Key Encoding
// ============================================================================

/// Build a UTxO tag key:
/// `[dim_hash:8][lookup_key:var][tag_order:10][txo_ref:36]`
fn build_utxo_tag_key(
    dimension: &str,
    lookup_key: &[u8],
    position: Option<UtxoPosition>,
    txo: &TxoRef,
) -> Vec<u8> {
    build_entry_key(dimension, lookup_key, &TagEntry::new(position, txo.clone()))
}

/// The tag key of `entry` under `lookup_key`.
fn build_entry_key(dimension: &str, lookup_key: &[u8], entry: &TagEntry) -> Vec<u8> {
    let dim_hash = hash_dimension(dim_prefix::UTXO, dimension);
    let txo_bytes = encode_txo_ref(&entry.txo);
    let mut key = Vec::with_capacity(tag_key_len(lookup_key));
    key.extend_from_slice(&dim_hash);
    key.extend_from_slice(lookup_key);
    key.extend_from_slice(&entry.order);
    key.extend_from_slice(&txo_bytes);
    key
}

/// The length of every tag key under `lookup_key`. A longer key under the same
/// prefix belongs to a longer lookup key that starts with this one (an asset
/// name extending another), so reads keep only keys of exactly this length.
fn tag_key_len(lookup_key: &[u8]) -> usize {
    DIM_HASH_SIZE + lookup_key.len() + TAG_ORDER_LEN + TXO_REF_SIZE
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
    debug_assert!(key.len() >= DIM_HASH_SIZE + TAG_ORDER_LEN + TXO_REF_SIZE);
    let start = key.len() - TXO_REF_SIZE;
    decode_txo_ref(&key[start..])
}

/// Decode the entry from a UTxO tag key (the last 46 bytes)
fn decode_utxo_tag_entry(key: &[u8]) -> TagEntry {
    debug_assert!(key.len() >= DIM_HASH_SIZE + TAG_ORDER_LEN + TXO_REF_SIZE);
    let start = key.len() - TXO_REF_SIZE - TAG_ORDER_LEN;
    let mut order = [0; TAG_ORDER_LEN];
    order.copy_from_slice(&key[start..start + TAG_ORDER_LEN]);

    TagEntry {
        order,
        txo: decode_utxo_tag_txo(key),
    }
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
    position: Option<UtxoPosition>,
    txo: &TxoRef,
) {
    let key = build_utxo_tag_key(dimension, lookup_key, position, txo);
    batch.insert(keyspace, key, []);
}

/// Remove a UTxO tag entry
fn remove_entry(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    position: Option<UtxoPosition>,
    txo: &TxoRef,
) {
    let key = build_utxo_tag_key(dimension, lookup_key, position, txo);
    batch.remove(keyspace, key);
}

/// Apply UTxO tag changes from a UtxoIndexDelta to the state-tags keyspace
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    delta: &UtxoIndexDelta,
) -> Result<(), Error> {
    // Insert produced UTxOs
    for (txo_ref, position, tags) in &delta.produced {
        for tag in tags {
            insert_entry(batch, keyspace, tag.dimension, &tag.key, *position, txo_ref);
        }
    }

    // Remove consumed UTxOs
    for (txo_ref, position, tags) in &delta.consumed {
        for tag in tags {
            remove_entry(batch, keyspace, tag.dimension, &tag.key, *position, txo_ref);
        }
    }

    Ok(())
}

/// Undo UTxO tag changes from a UtxoIndexDelta (for rollback), the steps of
/// [`apply`] undone in reverse. The last write to a key wins in a batch, so a
/// ref in both halves (an output its own block spent) ends with no tag, as
/// [`apply`] left it.
pub fn undo(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    delta: &UtxoIndexDelta,
) -> Result<(), Error> {
    // Restore consumed UTxOs (undo removal)
    for (txo_ref, position, tags) in &delta.consumed {
        for tag in tags {
            insert_entry(batch, keyspace, tag.dimension, &tag.key, *position, txo_ref);
        }
    }

    // Remove produced UTxOs (undo insertion)
    for (txo_ref, position, tags) in &delta.produced {
        for tag in tags {
            remove_entry(batch, keyspace, tag.dimension, &tag.key, *position, txo_ref);
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
/// Uses the `Readable` trait to support both direct keyspace access and
/// snapshot-based reads. Snapshot-based reads avoid potential deadlocks with
/// concurrent writes by using MVCC (Multi-Version Concurrency Control).
pub fn get_by_key<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
) -> Result<UtxoSet, Error> {
    let mut result = HashSet::new();

    // Build prefix: [dim_hash:8][lookup_key]
    let prefix = build_utxo_tag_prefix(dimension, lookup_key);
    let key_len = tag_key_len(lookup_key);

    // Prefix scan: all keys starting with prefix
    for guard in readable.prefix(keyspace, prefix) {
        let key = guard.key()?;

        if key.len() == key_len {
            result.insert(decode_utxo_tag_txo(&key));
        }
    }

    Ok(result)
}

/// One page of the entries for a dimension and lookup key, in key order
/// (creation position, then ref) or its reverse, from `start`.
///
/// The scan stops once it has passed the start and collected `count` entries.
/// Keys of another length under the prefix belong to a longer lookup key and
/// are stepped over, uncounted.
pub fn get_page_by_key<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    order: TagOrder,
    start: TagStart,
    count: usize,
) -> Result<Vec<TagEntry>, Error> {
    let prefix = build_utxo_tag_prefix(dimension, lookup_key);
    let key_len = tag_key_len(lookup_key);

    let (offset, scan) = match start {
        TagStart::Skip(offset) => (offset, readable.prefix(keyspace, &prefix)),
        TagStart::After(entry) => {
            let after = build_entry_key(dimension, lookup_key, &entry);

            let range = match order {
                TagOrder::Asc => (Bound::Excluded(after), Bound::Unbounded),
                TagOrder::Desc => (Bound::Included(prefix.clone()), Bound::Excluded(after)),
            };

            (0, readable.range(keyspace, range))
        }
    };

    let scan: Box<dyn Iterator<Item = fjall::Guard>> = match order {
        TagOrder::Asc => Box::new(scan),
        TagOrder::Desc => Box::new(scan.rev()),
    };

    let mut page = Vec::with_capacity(count.min(1024));
    let mut skipped = 0;

    for guard in scan {
        if page.len() == count {
            break;
        }

        let key = guard.key()?;

        // an ascending range from `after` runs on past the prefix
        if !key.starts_with(&prefix) {
            break;
        }

        if key.len() != key_len {
            continue;
        }

        if skipped < offset {
            skipped += 1;
            continue;
        }

        page.push(decode_utxo_tag_entry(&key));
    }

    Ok(page)
}

/// The entries of `entries` whose tag key under the dimension and lookup key
/// exists, in the order given: one point read each.
pub fn filter_by_key<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    entries: Vec<TagEntry>,
) -> Result<Vec<TagEntry>, Error> {
    let mut kept = Vec::with_capacity(entries.len());

    for entry in entries {
        if readable.contains_key(keyspace, build_entry_key(dimension, lookup_key, &entry))? {
            kept.push(entry);
        }
    }

    Ok(kept)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_utxo_tag_key_roundtrip() {
        let lookup_key = b"addr_test1qz...";
        let txo = TxoRef([0xab; 32].into(), 42);
        let position = Some(UtxoPosition::new(1_000, 3));
        let key = build_utxo_tag_key("address", lookup_key, position, &txo);

        assert_eq!(key.len(), tag_key_len(lookup_key));
        assert_eq!(
            key.len(),
            DIM_HASH_SIZE + lookup_key.len() + TAG_ORDER_LEN + TXO_REF_SIZE
        );

        let order_start = DIM_HASH_SIZE + lookup_key.len();
        assert_eq!(
            key[order_start..order_start + TAG_ORDER_LEN],
            UtxoPosition::tag_order(position)
        );

        let decoded_txo = decode_utxo_tag_txo(&key);
        assert_eq!(txo.0, decoded_txo.0);
        assert_eq!(txo.1, decoded_txo.1);
    }

    #[test]
    fn test_utxo_tag_prefix() {
        let lookup_key = b"addr_test1qz...";
        let txo = TxoRef([0xab; 32].into(), 42);
        let key = build_utxo_tag_key("address", lookup_key, None, &txo);
        let prefix = build_utxo_tag_prefix("address", lookup_key);

        // Key should start with prefix
        assert!(key.starts_with(&prefix));
    }

    /// Byte order is creation order: slot, then tx index, then the ref, with
    /// the UTxOs that have no position first.
    #[test]
    fn tag_keys_sort_in_creation_order() {
        let key = |position: Option<(u64, u16)>, hash: u8, index: u32| {
            build_utxo_tag_key(
                "address",
                b"addr",
                position.map(|(slot, tx)| UtxoPosition::new(slot, tx)),
                &TxoRef([hash; 32].into(), index),
            )
        };

        let ordered = [
            key(None, 0x01, 0),
            key(None, 0xff, 0),
            key(Some((0, 0)), 0x00, 0),
            key(Some((0, 1)), 0x00, 0),
            key(Some((1, 0)), 0xff, 0),
            key(Some((1, 0)), 0xff, 1),
            key(Some((1, 2)), 0x00, 0),
            key(Some((256, 0)), 0x00, 0),
        ];

        for pair in ordered.windows(2) {
            assert!(pair[0] < pair[1], "{:?} !< {:?}", pair[0], pair[1]);
        }
    }

    #[test]
    fn test_dimension_separation() {
        // Different dimensions should have different prefixes
        let key1 = build_utxo_tag_key("address", b"test", None, &TxoRef([0; 32].into(), 0));
        let key2 = build_utxo_tag_key("payment", b"test", None, &TxoRef([0; 32].into(), 0));

        // First 8 bytes (dim_hash) should be different
        assert_ne!(&key1[..DIM_HASH_SIZE], &key2[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_any_dimension_works() {
        // Any dimension string should work (chain-agnostic)
        let key = build_utxo_tag_key(
            "some_custom_dimension",
            b"data",
            None,
            &TxoRef([0; 32].into(), 0),
        );

        // Key should be valid size
        assert!(key.len() > DIM_HASH_SIZE);
    }
}
