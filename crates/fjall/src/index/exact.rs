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

use dolos_core::{
    ArchiveIndexDelta, BlockSlot, ExactKind, ExactRecord, IndexDelta, IndexError, MAX_EXACT_KEY_LEN,
};
use fjall::{Keyspace, OwnedWriteBatch, Readable, Snapshot};

use crate::index::scan::{DimensionScan, ScanTarget};
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

/// The widest exact key this keyspace holds: `[dim_hash:8][key_data:<=32]`.
const EXACT_KEY_BUF: usize = DIM_HASH_SIZE + MAX_EXACT_KEY_LEN;

/// An exact lookup key, built on the stack.
///
/// Every key in this keyspace fits, so nothing here allocates — and a bulk
/// restore writes one of these per record into a batch that copies out of it
/// immediately.
struct ExactKeyBuf {
    buf: [u8; EXACT_KEY_BUF],
    len: usize,
}

impl ExactKeyBuf {
    fn as_slice(&self) -> &[u8] {
        &self.buf[..self.len]
    }
}

/// Build an exact lookup key: `[dim_hash:8][key_data:var]`
///
/// `None` for key data wider than any kind stores. No such entry can exist, so
/// a query for one is a miss; the callers that build keys from a record are
/// already width-checked by [`ExactRecord::new`].
fn build_exact_key(dimension: &str, key_data: &[u8]) -> Option<ExactKeyBuf> {
    if key_data.len() > MAX_EXACT_KEY_LEN {
        return None;
    }

    let dim_hash = hash_dimension(dim_prefix::EXACT, dimension);
    let mut buf = [0u8; EXACT_KEY_BUF];
    buf[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    buf[DIM_HASH_SIZE..DIM_HASH_SIZE + key_data.len()].copy_from_slice(key_data);

    Some(ExactKeyBuf {
        buf,
        len: DIM_HASH_SIZE + key_data.len(),
    })
}

/// [`build_exact_key`] for the delta paths, where an over-wide key is a
/// malformed block rather than a lookup that cannot match.
fn exact_key_or_codec_error(dimension: &str, key_data: &[u8]) -> Result<ExactKeyBuf, Error> {
    build_exact_key(dimension, key_data).ok_or_else(|| {
        Error::Codec(format!(
            "{dimension} key is {} bytes, wider than the {MAX_EXACT_KEY_LEN} an exact key can hold",
            key_data.len(),
        ))
    })
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
        let key = exact_key_or_codec_error(DIM_BLOCK_HASH, &block.block_hash)?;
        batch.insert(exact_keyspace, key.as_slice(), encode_slot_value(slot));
    }

    // Exact lookup: block number -> slot
    if let Some(number) = block.block_number {
        let key = build_exact_key_blocknum(number);
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Exact lookup: tx hashes -> slot
    for tx_hash in &block.tx_hashes {
        let key = exact_key_or_codec_error(DIM_TX_HASH, tx_hash.as_slice())?;
        batch.insert(exact_keyspace, key.as_slice(), encode_slot_value(slot));
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
        let key = exact_key_or_codec_error(DIM_BLOCK_HASH, &block.block_hash)?;
        batch.remove(exact_keyspace, key.as_slice());
    }

    if let Some(number) = block.block_number {
        let key = build_exact_key_blocknum(number);
        batch.remove(exact_keyspace, key);
    }

    for tx_hash in &block.tx_hashes {
        let key = exact_key_or_codec_error(DIM_TX_HASH, tx_hash.as_slice())?;
        batch.remove(exact_keyspace, key.as_slice());
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
/// The key needs no width check here: [`ExactRecord`] cannot be constructed
/// with a key that does not match its kind, which is what would otherwise land
/// as a permanently unreadable entry that re-exports as if it were valid.
pub fn insert_prehashed(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    record: &ExactRecord,
    dim_hash: [u8; DIM_HASH_SIZE],
) {
    let stored = record.key();

    let mut key = [0u8; EXACT_KEY_BUF];
    key[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    key[DIM_HASH_SIZE..DIM_HASH_SIZE + stored.len()].copy_from_slice(stored);

    batch.insert(
        exact_keyspace,
        &key[..DIM_HASH_SIZE + stored.len()],
        encode_slot_value(record.slot),
    );
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
    let Some(key) = build_exact_key(DIM_BLOCK_HASH, block_hash) else {
        return Ok(None);
    };
    match readable
        .get(exact_keyspace, key.as_slice())
        .map_err(Error::Fjall)?
    {
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
    let Some(key) = build_exact_key(DIM_TX_HASH, tx_hash) else {
        return Ok(None);
    };
    match readable
        .get(exact_keyspace, key.as_slice())
        .map_err(Error::Fjall)?
    {
        Some(value) => {
            let slot = decode_slot_value(value.as_ref());
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

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
/// GC watermark for the iterator's lifetime, and errors are terminal per the
/// policy in [`crate::index::scan`].
pub struct ExactRecordIterator(DimensionScan<ExactScan, std::array::IntoIter<ExactKind, 3>>);

/// The exact-match half of the shared prefix walk.
pub struct ExactScan;

impl ScanTarget for ExactScan {
    type Label = ExactKind;
    type Record = ExactRecord;

    fn prefix(kind: ExactKind) -> [u8; DIM_HASH_SIZE] {
        hash_dimension(dim_prefix::EXACT, kind.as_str())
    }

    /// Both halves are read: an exact entry keeps its slot in the *value*, so
    /// unlike the tag scan there is no way to range-filter from the key alone.
    fn decode(
        kind: ExactKind,
        guard: fjall::Guard,
        slots: &Range<BlockSlot>,
    ) -> Result<Option<ExactRecord>, IndexError> {
        let (key, value) = guard.into_inner().map_err(Error::Fjall)?;

        if key.len() <= DIM_HASH_SIZE || value.len() < SLOT_SIZE {
            return Err(IndexError::CodecError(format!(
                "malformed exact index entry of kind {kind}: \
                 key {} bytes, value {} bytes",
                key.len(),
                value.len(),
            )));
        }

        let slot = decode_slot_value(&value);

        if !slots.contains(&slot) {
            return Ok(None);
        }

        // A stored key of the wrong width for its kind is a malformed entry,
        // and `ExactRecord::new` is where that is decided. `Err` fuses the
        // scan, which is the policy for a malformed entry either way.
        ExactRecord::new(kind, &key[DIM_HASH_SIZE..], slot).map(Some)
    }
}

impl ExactRecordIterator {
    /// Create a new iterator over every exact record in the slot range.
    ///
    /// Returns immediately without reading any data.
    pub fn new(snapshot: Snapshot, keyspace: &Keyspace, slots: Range<BlockSlot>) -> Self {
        Self(DimensionScan::new(
            snapshot,
            keyspace,
            ExactKind::ALL.into_iter(),
            slots,
        ))
    }
}

impl Iterator for ExactRecordIterator {
    type Item = Result<ExactRecord, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl std::iter::FusedIterator for ExactRecordIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The key bytes for a dimension and key data, for tests that know the
    /// data fits.
    fn key_bytes(dimension: &str, key_data: &[u8]) -> Vec<u8> {
        build_exact_key(dimension, key_data)
            .expect("test keys fit the inline buffer")
            .as_slice()
            .to_vec()
    }

    #[test]
    fn test_exact_key_block_hash() {
        let block_hash = [0xcd; 32];
        let key = key_bytes(DIM_BLOCK_HASH, &block_hash);

        // First 8 bytes are dim_hash, rest is key_data
        assert_eq!(key.len(), DIM_HASH_SIZE + 32);
        assert_eq!(&key[DIM_HASH_SIZE..], &block_hash);
    }

    #[test]
    fn test_exact_key_tx_hash() {
        let tx_hash = [0xab; 32];
        let key = key_bytes(DIM_TX_HASH, &tx_hash);

        assert_eq!(key.len(), DIM_HASH_SIZE + 32);
        assert_eq!(&key[DIM_HASH_SIZE..], &tx_hash);
    }

    /// Nothing in this keyspace is wider than the widest kind, so a key that
    /// is cannot match an entry — the lookup misses rather than erroring.
    #[test]
    fn test_exact_key_wider_than_any_kind_has_no_key() {
        assert!(build_exact_key(DIM_BLOCK_HASH, &[0u8; MAX_EXACT_KEY_LEN]).is_some());
        assert!(build_exact_key(DIM_BLOCK_HASH, &[0u8; MAX_EXACT_KEY_LEN + 1]).is_none());
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
        let block_key = key_bytes(DIM_BLOCK_HASH, &hash);
        let tx_key = key_bytes(DIM_TX_HASH, &hash);

        // First 8 bytes (dim_hash) should be different
        assert_ne!(&block_key[..DIM_HASH_SIZE], &tx_key[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_any_dimension_works() {
        // Any dimension string should work (chain-agnostic)
        let key = key_bytes("custom_lookup", &[0x11; 20]);
        assert_eq!(key.len(), DIM_HASH_SIZE + 20);
    }

    /// A record written by `insert_prehashed` has to land on the exact key the
    /// delta path would have written, otherwise a restored store answers
    /// lookups differently from the one it was exported from.
    #[test]
    fn test_prehashed_key_matches_delta_key() {
        let block_hash = [0xcd; 32];
        let from_delta = key_bytes(DIM_BLOCK_HASH, &block_hash);
        let record = ExactRecord::new(ExactKind::BlockHash, &block_hash, 42).unwrap();
        let from_record = key_bytes(record.kind.as_str(), record.key());
        assert_eq!(from_delta, from_record);

        let number = 12345678u64;
        let from_delta = build_exact_key_blocknum(number);
        let record = ExactRecord::new(ExactKind::BlockNumber, &number.to_be_bytes(), 42).unwrap();
        let from_record = key_bytes(record.kind.as_str(), record.key());
        assert_eq!(from_delta.as_slice(), from_record.as_slice());
    }
}
