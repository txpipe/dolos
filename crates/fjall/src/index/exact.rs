//! Exact-match index operations for the `index-exact` keyspace
//! (chain-agnostic).
//!
//! This module handles key encoding, batch writes (apply/undo), and read
//! queries for the `index-exact` keyspace:
//! - Block hash -> slot
//! - Transaction hash -> slot
//! - Block number -> slot
//!
//! ## Key Format
//!
//! | Type | Key Format | Value |
//! |------|------------|-------|
//! | Block Hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
//! | Tx Hash | `[dim_hash:8][hash:32]` | `[slot:8]` |
//! | Block Number | `[dim_hash:8][num:8]` | `[slot:8]` |
//!
//! The `dim_hash` is computed as `xxh3("exact:" + dimension)`.

use std::ops::Range;

use dolos_core::{ArchiveIndexDelta, BlockSlot, ExactKind, ExactRecord, IndexDelta, IndexError};
use fjall::{Keyspace, OwnedWriteBatch, Readable, Snapshot};

use crate::keys::{decode_slot, dim_prefix, encode_slot, hash_dimension, DIM_HASH_SIZE, SLOT_SIZE};
use crate::Error;

// ============================================================================
// Internal Dimension Names
// ============================================================================
//
// These are derived from `ExactKind` rather than spelled out, so the names
// hashed into on-disk keys and the names carried by exported records cannot
// drift apart.

/// Internal dimension name for block hash lookups
const DIM_BLOCK_HASH: &str = ExactKind::BlockHash.as_str();

/// Internal dimension name for block number lookups
const DIM_BLOCK_NUM: &str = ExactKind::BlockNumber.as_str();

/// Internal dimension name for transaction hash lookups
const DIM_TX_HASH: &str = ExactKind::TxHash.as_str();

// ============================================================================
// Key Encoding
// ============================================================================

/// Build an exact lookup key: `[dim_hash:8][key_data:var]`
fn build_exact_key(dimension: &str, key_data: &[u8]) -> Vec<u8> {
    let dim_hash = hash_dimension(dim_prefix::EXACT, dimension);
    let mut key = Vec::with_capacity(DIM_HASH_SIZE + key_data.len());
    key.extend_from_slice(&dim_hash);
    key.extend_from_slice(key_data);
    key
}

/// Build an exact lookup key for block number: `[dim_hash:8][blocknum:8]`
fn build_exact_key_blocknum(block_number: u64) -> [u8; DIM_HASH_SIZE + 8] {
    let dim_hash = hash_dimension(dim_prefix::EXACT, DIM_BLOCK_NUM);
    let mut key = [0u8; DIM_HASH_SIZE + 8];
    key[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    key[DIM_HASH_SIZE..].copy_from_slice(&block_number.to_be_bytes());
    key
}

/// Encode slot as value for exact lookups
fn encode_slot_value(slot: u64) -> [u8; SLOT_SIZE] {
    encode_slot(slot)
}

/// Decode slot from value
fn decode_slot_value(value: &[u8]) -> u64 {
    decode_slot(value)
}

// ============================================================================
// Block Processing
// ============================================================================

/// Apply exact indexes for a single block delta
fn apply_block(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    let slot = block.slot;

    // Exact lookup: block hash -> slot
    if !block.block_hash.is_empty() {
        let key = build_exact_key(DIM_BLOCK_HASH, &block.block_hash);
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Exact lookup: block number -> slot
    if let Some(number) = block.block_number {
        let key = build_exact_key_blocknum(number);
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Exact lookup: tx hashes -> slot
    for tx_hash in &block.tx_hashes {
        let key = build_exact_key(DIM_TX_HASH, tx_hash.as_slice());
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    Ok(())
}

/// Undo exact indexes for a single block delta (rollback)
fn undo_block(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    if !block.block_hash.is_empty() {
        let key = build_exact_key(DIM_BLOCK_HASH, &block.block_hash);
        batch.remove(exact_keyspace, key);
    }

    if let Some(number) = block.block_number {
        let key = build_exact_key_blocknum(number);
        batch.remove(exact_keyspace, key);
    }

    for tx_hash in &block.tx_hashes {
        let key = build_exact_key(DIM_TX_HASH, tx_hash.as_slice());
        batch.remove(exact_keyspace, key);
    }

    Ok(())
}

/// Apply exact indexes from an IndexDelta
pub fn apply(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in &delta.archive {
        apply_block(batch, exact_keyspace, block)?;
    }
    Ok(())
}

/// Undo exact indexes from an IndexDelta (rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in delta.archive.iter().rev() {
        undo_block(batch, exact_keyspace, block)?;
    }
    Ok(())
}

/// Insert an exact entry from an already-decoded record.
///
/// The write mirror of [`ExactRecordIterator`]. Exact keys are stored verbatim,
/// so this is the same encoding `apply` performs — only the kind arrives as a
/// value instead of being implied by the call site.
///
/// `dim_hash` is `hash_dimension(dim_prefix::EXACT, record.kind.as_str())`,
/// passed in rather than computed here: records arrive grouped by kind, so the
/// caller hashes each kind once per group instead of once per record.
///
/// The key length is validated against [`ExactKind::key_len`]: the read paths
/// build fixed-width keys per kind, so a wrong-width key (possible only from an
/// external source — the delta path is type-enforced) would land as a
/// permanently unreadable entry that re-exports as if it were valid.
pub fn insert_prehashed(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    record: &ExactRecord,
    dim_hash: [u8; DIM_HASH_SIZE],
) -> Result<(), Error> {
    let expected = record.kind.key_len();
    if record.key.len() != expected {
        return Err(Error::Codec(format!(
            "exact record of kind {} has a {}-byte key, expected {expected}",
            record.kind,
            record.key.len(),
        )));
    }

    let mut key = Vec::with_capacity(DIM_HASH_SIZE + record.key.len());
    key.extend_from_slice(&dim_hash);
    key.extend_from_slice(&record.key);
    batch.insert(exact_keyspace, key, encode_slot_value(record.slot));
    Ok(())
}

// ============================================================================
// Queries
// ============================================================================

/// Get slot by block hash (exact lookup).
pub fn get_by_block_hash<R: Readable>(
    readable: &R,
    exact_keyspace: &Keyspace,
    block_hash: &[u8],
) -> Result<Option<BlockSlot>, Error> {
    let key = build_exact_key(DIM_BLOCK_HASH, block_hash);
    match readable.get(exact_keyspace, key).map_err(Error::Fjall)? {
        Some(value) => {
            let slot = decode_slot_value(value.as_ref());
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

/// Get slot by block number (exact lookup).
pub fn get_by_block_number<R: Readable>(
    readable: &R,
    exact_keyspace: &Keyspace,
    number: u64,
) -> Result<Option<BlockSlot>, Error> {
    let key = build_exact_key_blocknum(number);
    match readable.get(exact_keyspace, key).map_err(Error::Fjall)? {
        Some(value) => {
            let slot = decode_slot_value(value.as_ref());
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

/// Get slot by tx hash (exact lookup).
pub fn get_by_tx_hash<R: Readable>(
    readable: &R,
    exact_keyspace: &Keyspace,
    tx_hash: &[u8],
) -> Result<Option<BlockSlot>, Error> {
    let key = build_exact_key(DIM_TX_HASH, tx_hash);
    match readable.get(exact_keyspace, key).map_err(Error::Fjall)? {
        Some(value) => {
            let slot = decode_slot_value(value.as_ref());
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

// ============================================================================
// ExactRecordIterator
// ============================================================================

/// Lazy iterator over exact-match records, one kind prefix at a time.
///
/// ## Why it is driven by the kind list
///
/// Same reason as the tag iterator: the stored key is
/// `[dim_hash:8][key_data:var]` with `dim_hash = xxh3("exact:" + kind)`, so the
/// kind name is not on disk. `ExactKind::ALL` is the closed list, already in
/// ascending name order, which makes the `(kind, key)` contract fall out of
/// walking it in order — within a prefix, fjall's lexicographic order over
/// `key_data` is the rest of it.
///
/// ## Cost
///
/// The slot of an exact entry is its *value*, not part of its key, so a
/// slot-bounded traversal cannot seek at all: every exact entry in the store is
/// visited and its value read, however narrow the range. In absolute terms the
/// tag scan still dominates a per-epoch slice, simply because there are more
/// tag records than exact ones — but this one has no seekable structure to
/// exploit even in principle.
///
/// ## Lifetime and errors
///
/// Same contract as `TagRecordIterator`: the held MVCC snapshot pins fjall's
/// GC watermark for the iterator's lifetime, and errors — read failures or
/// malformed entries — are terminal (the iterator fuses rather than resuming
/// past a fault).
pub struct ExactRecordIterator {
    snapshot: Snapshot,
    keyspace: Keyspace,
    kinds: std::array::IntoIter<ExactKind, 3>,
    current: Option<(ExactKind, fjall::Iter)>,
    slots: Range<BlockSlot>,
    done: bool,
}

impl ExactRecordIterator {
    /// Create a new iterator over every exact record in the slot range.
    ///
    /// Returns immediately without reading any data.
    pub fn new(snapshot: Snapshot, keyspace: &Keyspace, slots: Range<BlockSlot>) -> Self {
        Self {
            snapshot,
            keyspace: keyspace.clone(),
            kinds: ExactKind::ALL.into_iter(),
            current: None,
            // An empty range matches nothing: skip the prefix scans entirely
            // instead of decoding every entry to filter everything out.
            done: slots.is_empty(),
            slots,
        }
    }

    /// Open a prefix scan for the next kind, if any is left.
    fn open_next(&mut self) -> Option<()> {
        let kind = self.kinds.next()?;
        let prefix = hash_dimension(dim_prefix::EXACT, kind.as_str());
        let iter = self.snapshot.prefix(&self.keyspace, prefix);
        self.current = Some((kind, iter));
        Some(())
    }
}

impl Iterator for ExactRecordIterator {
    type Item = Result<ExactRecord, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let Some((kind, iter)) = self.current.as_mut() else {
                self.open_next()?;
                continue;
            };

            let kind = *kind;

            let Some(guard) = iter.next() else {
                self.current = None;
                continue;
            };

            let (key, value) = match guard.into_inner() {
                Ok(pair) => pair,
                Err(e) => {
                    self.done = true;
                    return Some(Err(Error::Fjall(e).into()));
                }
            };

            if key.len() <= DIM_HASH_SIZE || value.len() < SLOT_SIZE {
                // A malformed entry cannot come from this module's write path:
                // it is corruption, and this stream becomes a signed layer —
                // error out (terminally) rather than skip.
                self.done = true;
                return Some(Err(IndexError::CodecError(format!(
                    "malformed exact index entry of kind {kind}: \
                     key {} bytes, value {} bytes",
                    key.len(),
                    value.len(),
                ))));
            }

            let slot = decode_slot_value(&value);

            if !self.slots.contains(&slot) {
                continue;
            }

            return Some(Ok(ExactRecord {
                kind,
                key: key[DIM_HASH_SIZE..].to_vec(),
                slot,
            }));
        }
    }
}

impl std::iter::FusedIterator for ExactRecordIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_key_block_hash() {
        let block_hash = [0xcd; 32];
        let key = build_exact_key(DIM_BLOCK_HASH, &block_hash);

        // First 8 bytes are dim_hash, rest is key_data
        assert_eq!(key.len(), DIM_HASH_SIZE + 32);
        assert_eq!(&key[DIM_HASH_SIZE..], &block_hash);
    }

    #[test]
    fn test_exact_key_tx_hash() {
        let tx_hash = [0xab; 32];
        let key = build_exact_key(DIM_TX_HASH, &tx_hash);

        assert_eq!(key.len(), DIM_HASH_SIZE + 32);
        assert_eq!(&key[DIM_HASH_SIZE..], &tx_hash);
    }

    #[test]
    fn test_exact_key_blocknum() {
        let block_num = 12345678u64;
        let key = build_exact_key_blocknum(block_num);

        assert_eq!(key.len(), DIM_HASH_SIZE + 8);

        // Verify big-endian encoding of block number
        let decoded = u64::from_be_bytes(key[DIM_HASH_SIZE..].try_into().unwrap());
        assert_eq!(decoded, block_num);
    }

    #[test]
    fn test_slot_value_roundtrip() {
        let slot = 141868807u64;
        let encoded = encode_slot_value(slot);
        let decoded = decode_slot_value(&encoded);
        assert_eq!(slot, decoded);
    }

    #[test]
    fn test_dimension_separation() {
        // Ensure keys from different dimensions don't overlap
        let hash = [0xab; 32];
        let block_key = build_exact_key(DIM_BLOCK_HASH, &hash);
        let tx_key = build_exact_key(DIM_TX_HASH, &hash);

        // First 8 bytes (dim_hash) should be different
        assert_ne!(&block_key[..DIM_HASH_SIZE], &tx_key[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_any_dimension_works() {
        // Any dimension string should work (chain-agnostic)
        let key = build_exact_key("custom_lookup", &[0x11; 20]);
        assert_eq!(key.len(), DIM_HASH_SIZE + 20);
    }

    /// A record written by `insert_prehashed` has to land on the exact key the
    /// delta path would have written, otherwise a restored store answers
    /// lookups differently from the one it was exported from.
    #[test]
    fn test_prehashed_key_matches_delta_key() {
        let block_hash = [0xcd; 32];
        let from_delta = build_exact_key(DIM_BLOCK_HASH, &block_hash);
        let record = ExactRecord::new(ExactKind::BlockHash, block_hash, 42);
        let from_record = build_exact_key(record.kind.as_str(), &record.key);
        assert_eq!(from_delta, from_record);

        let number = 12345678u64;
        let from_delta = build_exact_key_blocknum(number);
        let record = ExactRecord::new(ExactKind::BlockNumber, number.to_be_bytes(), 42);
        let from_record = build_exact_key(record.kind.as_str(), &record.key);
        assert_eq!(from_delta.as_slice(), from_record.as_slice());
    }
}
