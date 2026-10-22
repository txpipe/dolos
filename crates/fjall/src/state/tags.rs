//! Live-UTxO tag operations for the tag keyspaces of the state store
//! (chain-agnostic).
//!
//! This module handles key encoding, batch writes (apply/undo), and read
//! queries for the three keyspaces that project the UTxO set onto its tags.
//! The tags map lookup keys (addresses, policies, assets) to sets of TxoRefs,
//! and are written in the same batch as the UTxO set they project.
//!
//! - `state-tags`: `[dim_hash:8][lookup_key:var][txo_ref:36]` with empty value,
//!   one entry per tag of every live UTxO. Queries use prefix scanning to find
//!   all TxoRefs for a given lookup key.
//! - `state-tags-by-slot`: `[dim_hash:8][key_len:2][lookup_key:var][slot:8]
//!   [txo_ref:36]` with empty value, the same tags for the UTxOs that have a
//!   producing slot, sorted by that slot under each key so one tag can be
//!   walked in chain order. The length prefix keeps a lookup key's range from
//!   running into the entries of a longer key it is a prefix of.
//! - `state-utxo-slots`: `[txo_ref:36] -> [slot:8]`, the producing slot of
//!   every positioned live UTxO, read back when it is consumed so the consuming
//!   delta can name the ordered entries to take out.
//!
//! The `dim_hash` is computed as `xxh3("utxo:" + dimension)`, making the
//! storage layer fully chain-agnostic.

use std::collections::{HashMap, HashSet};
use std::ops::Bound;

use dolos_core::{BlockSlot, SlotOrder, SlotScan, Tag, TxoRef, UtxoIndexDelta, UtxoSet};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use crate::keys::{
    decode_slot, decode_txo_ref, dim_prefix, encode_slot, encode_txo_ref, hash_dimension,
    DIM_HASH_SIZE, SLOT_SIZE, TXO_REF_SIZE,
};
use crate::Error;

/// The three keyspaces one tag write touches.
#[derive(Clone, Copy)]
pub struct Keyspaces<'a> {
    /// `state-tags`
    pub tags: &'a Keyspace,
    /// `state-tags-by-slot`
    pub by_slot: &'a Keyspace,
    /// `state-utxo-slots`
    pub slots: &'a Keyspace,
}

/// Width of the length prefix of a lookup key in an ordered tag key.
const KEY_LEN_SIZE: usize = 2;

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

/// Build the prefix of one tag in the ordered keyspace:
/// `[dim_hash:8][key_len:2][lookup_key:var]`.
///
/// A lookup key longer than the length prefix can express is refused rather
/// than truncated: the ordered entries of such a key could never be looked
/// up again.
fn build_ordered_tag_prefix(dimension: &str, lookup_key: &[u8]) -> Result<Vec<u8>, Error> {
    let key_len = u16::try_from(lookup_key.len())
        .map_err(|_| Error::Codec(format!("tag key of {} bytes is too long", lookup_key.len())))?;

    let dim_hash = hash_dimension(dim_prefix::UTXO, dimension);
    let mut prefix = Vec::with_capacity(DIM_HASH_SIZE + KEY_LEN_SIZE + lookup_key.len());
    prefix.extend_from_slice(&dim_hash);
    prefix.extend_from_slice(&key_len.to_be_bytes());
    prefix.extend_from_slice(lookup_key);

    Ok(prefix)
}

/// Build an ordered tag key:
/// `[dim_hash:8][key_len:2][lookup_key:var][slot:8][txo_ref:36]`.
fn build_ordered_tag_key(
    dimension: &str,
    lookup_key: &[u8],
    slot: BlockSlot,
    txo: &TxoRef,
) -> Result<Vec<u8>, Error> {
    let mut key = build_ordered_tag_prefix(dimension, lookup_key)?;
    key.extend_from_slice(&encode_slot(slot));
    key.extend_from_slice(&encode_txo_ref(txo));

    Ok(key)
}

/// Decode the `(slot, txo_ref)` tail of an ordered tag key (last 44 bytes).
fn decode_ordered_tag(key: &[u8]) -> (BlockSlot, TxoRef) {
    debug_assert!(key.len() >= DIM_HASH_SIZE + KEY_LEN_SIZE + SLOT_SIZE + TXO_REF_SIZE);
    let txo_start = key.len() - TXO_REF_SIZE;
    let slot_start = txo_start - SLOT_SIZE;

    (
        decode_slot(&key[slot_start..txo_start]),
        decode_txo_ref(&key[txo_start..]),
    )
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

/// Put in (`insert`) or take out the entries of one tagged ref: its plain
/// tags, and when it has a producing slot its ordered tags and its slot row.
fn write_ref(
    batch: &mut OwnedWriteBatch,
    keyspaces: Keyspaces<'_>,
    txo: &TxoRef,
    tags: &[Tag],
    slot: Option<BlockSlot>,
    insert: bool,
) -> Result<(), Error> {
    for tag in tags {
        if insert {
            insert_entry(batch, keyspaces.tags, tag.dimension, &tag.key, txo);
        } else {
            remove_entry(batch, keyspaces.tags, tag.dimension, &tag.key, txo);
        }

        if let Some(slot) = slot {
            let key = build_ordered_tag_key(tag.dimension, &tag.key, slot, txo)?;

            if insert {
                batch.insert(keyspaces.by_slot, key, []);
            } else {
                batch.remove(keyspaces.by_slot, key);
            }
        }
    }

    if let Some(slot) = slot {
        if insert {
            batch.insert(keyspaces.slots, encode_txo_ref(txo), encode_slot(slot));
        } else {
            batch.remove(keyspaces.slots, encode_txo_ref(txo));
        }
    }

    Ok(())
}

/// The entries of one side of a delta, put in or taken out.
fn write_side(
    batch: &mut OwnedWriteBatch,
    keyspaces: Keyspaces<'_>,
    side: &[(TxoRef, Vec<Tag>)],
    slots: &HashMap<TxoRef, BlockSlot>,
    insert: bool,
) -> Result<(), Error> {
    for (txo, tags) in side {
        write_ref(batch, keyspaces, txo, tags, slots.get(txo).copied(), insert)?;
    }

    Ok(())
}

/// Apply UTxO tag changes from a UtxoIndexDelta: the produced refs go in,
/// the consumed refs go out, each with its ordered entries and slot row when
/// the delta positions it.
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspaces: Keyspaces<'_>,
    delta: &UtxoIndexDelta,
) -> Result<(), Error> {
    write_side(batch, keyspaces, &delta.produced, &delta.slots, true)?;
    write_side(batch, keyspaces, &delta.consumed, &delta.slots, false)?;

    Ok(())
}

/// Undo UTxO tag changes from a UtxoIndexDelta (for rollback): the consumed
/// refs come back in, then the produced refs go out.
///
/// That order, the one `utxos::apply_delta` puts and deletes in, decides the
/// fate of a ref on both sides — one a block produced and consumed — because
/// the last write to a key wins within one batch: it ends up out, as it is
/// out of the set.
pub fn undo(
    batch: &mut OwnedWriteBatch,
    keyspaces: Keyspaces<'_>,
    delta: &UtxoIndexDelta,
) -> Result<(), Error> {
    write_side(batch, keyspaces, &delta.consumed, &delta.slots, true)?;
    write_side(batch, keyspaces, &delta.produced, &delta.slots, false)?;

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
///
/// Lookup keys vary in length and one may be a prefix of another (an asset
/// with an empty name and every asset of its policy, say), so the prefix
/// scan also meets the longer keys' entries: only an entry of exactly the
/// looked-up key's length belongs to it.
pub fn get_by_key<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
) -> Result<UtxoSet, Error> {
    let mut result = HashSet::new();

    // Build prefix: [dim_hash:8][lookup_key]
    let prefix = build_utxo_tag_prefix(dimension, lookup_key);
    let entry_len = DIM_HASH_SIZE + lookup_key.len() + TXO_REF_SIZE;

    // Prefix scan: all keys starting with prefix
    for guard in readable.prefix(keyspace, prefix) {
        let key = guard.key()?;

        // Key format: [dim_hash:8][lookup_key:var][txo_ref:36]
        if key.len() == entry_len {
            let txo_ref = decode_utxo_tag_txo(&key);
            result.insert(txo_ref);
        }
    }

    Ok(result)
}

/// The producing slot of each of `refs` that has a slot row.
pub fn get_utxo_slots<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    refs: &[TxoRef],
) -> Result<HashMap<TxoRef, BlockSlot>, Error> {
    let mut out = HashMap::with_capacity(refs.len());

    for txo in refs {
        if let Some(value) = readable.get(keyspace, encode_txo_ref(txo))? {
            if value.len() == SLOT_SIZE {
                out.insert(txo.clone(), decode_slot(&value));
            }
        }
    }

    Ok(out)
}

/// One bounded read of a tag's ordered entries, as [`SlotScan`] describes.
///
/// One range over the ordered keyspace, bounded on both sides by the
/// lookup key's prefix and the scan's slots and resumed past `scan.after`
/// when it has one; walked from the front ascending, from the back
/// descending, and stopped at `scan.limit` rows, so the keys it passes are
/// the rows it returns and, until compaction, the tombstones of spent ones.
pub fn get_by_key_in_slots<R: Readable>(
    readable: &R,
    keyspace: &Keyspace,
    dimension: &str,
    lookup_key: &[u8],
    scan: &SlotScan,
) -> Result<Vec<(BlockSlot, TxoRef)>, Error> {
    let mut out = Vec::new();

    let (start, end) = (*scan.slots.start(), *scan.slots.end());

    if start > end || scan.limit == 0 {
        return Ok(out);
    }

    let prefix = build_ordered_tag_prefix(dimension, lookup_key)?;
    let entry_len = prefix.len() + SLOT_SIZE + TXO_REF_SIZE;

    let lowest = build_ordered_tag_key(dimension, lookup_key, start, &lowest_txo())?;
    let highest = build_ordered_tag_key(dimension, lookup_key, end, &highest_txo())?;

    // the resume point narrows the end the walk starts from; one past the
    // other end selects nothing, one before the start is not reached yet
    let bounds = match (scan.order, &scan.after) {
        (_, None) => (Bound::Included(lowest), Bound::Included(highest)),
        (SlotOrder::Asc, Some((slot, txo))) => {
            let after = build_ordered_tag_key(dimension, lookup_key, *slot, txo)?;

            if after >= highest {
                return Ok(out);
            } else if after < lowest {
                (Bound::Included(lowest), Bound::Included(highest))
            } else {
                (Bound::Excluded(after), Bound::Included(highest))
            }
        }
        (SlotOrder::Desc, Some((slot, txo))) => {
            let after = build_ordered_tag_key(dimension, lookup_key, *slot, txo)?;

            if after <= lowest {
                return Ok(out);
            } else if after > highest {
                (Bound::Included(lowest), Bound::Included(highest))
            } else {
                (Bound::Included(lowest), Bound::Excluded(after))
            }
        }
    };

    let mut iter = readable.range(keyspace, bounds);

    loop {
        let guard = match scan.order {
            SlotOrder::Asc => iter.next(),
            SlotOrder::Desc => iter.next_back(),
        };

        let Some(guard) = guard else {
            break;
        };

        let key = guard.key()?;

        if key.len() == entry_len {
            out.push(decode_ordered_tag(&key));

            if out.len() == scan.limit {
                break;
            }
        }
    }

    Ok(out)
}

/// The first ref in key order.
fn lowest_txo() -> TxoRef {
    TxoRef(pallas::crypto::hash::Hash::new([0; 32]), 0)
}

/// The last ref in key order.
fn highest_txo() -> TxoRef {
    TxoRef(pallas::crypto::hash::Hash::new([0xff; 32]), u32::MAX)
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

    /// The ordered key decodes back to its slot and ref, and sorts by slot
    /// first, ref second.
    #[test]
    fn ordered_tag_key_roundtrips_and_sorts_by_slot_then_ref() {
        let low = TxoRef([0x01; 32].into(), 7);
        let high = TxoRef([0x02; 32].into(), 0);

        let key = build_ordered_tag_key("asset", b"policy.name", 42, &low).unwrap();
        assert_eq!(
            key.len(),
            DIM_HASH_SIZE + KEY_LEN_SIZE + b"policy.name".len() + SLOT_SIZE + TXO_REF_SIZE
        );
        assert_eq!(decode_ordered_tag(&key), (42, low.clone()));

        let later_slot = build_ordered_tag_key("asset", b"policy.name", 43, &low).unwrap();
        let later_ref = build_ordered_tag_key("asset", b"policy.name", 42, &high).unwrap();
        assert!(key < later_ref);
        assert!(later_ref < later_slot);
    }

    /// A lookup key that prefixes another does not share its ordered
    /// range: the length prefix parts them before the key bytes do.
    #[test]
    fn ordered_tag_prefixes_part_a_key_from_the_keys_it_prefixes() {
        let short = build_ordered_tag_prefix("asset", &[1, 1]).unwrap();
        let long = build_ordered_tag_prefix("asset", &[1, 1, 2]).unwrap();

        assert!(!long.starts_with(&short));
        assert_eq!(short[DIM_HASH_SIZE..DIM_HASH_SIZE + KEY_LEN_SIZE], [0, 2]);
        assert_eq!(long[DIM_HASH_SIZE..DIM_HASH_SIZE + KEY_LEN_SIZE], [0, 3]);
    }

    /// A lookup key too long for the length prefix is refused, never
    /// truncated into a key nothing could look up.
    #[test]
    fn ordered_tag_prefix_refuses_an_oversized_key() {
        let key = vec![0u8; usize::from(u16::MAX) + 1];

        assert!(build_ordered_tag_prefix("asset", &key).is_err());
        assert!(build_ordered_tag_prefix("asset", &key[..usize::from(u16::MAX)]).is_ok());
    }
}
