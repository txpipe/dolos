//! Archive tag index operations for the `archive-tags` keyspace
//! (chain-agnostic).
//!
//! This module handles key encoding, batch writes (apply/undo), read queries,
//! and the `SlotIterator` for the `archive-tags` keyspace. Block tags are
//! append-only entries that record which blocks contain specific data.
//!
//! ## Key Format
//!
//! ```text
//! Key:   [dim_hash:8][xxh3(tag_key):8][slot:8]
//! Value: (empty)
//! ```
//!
//! The `dim_hash` is computed as `xxh3("block:" + dimension)`.

use std::borrow::Cow;
use std::ops::Range;

use dolos_core::{
    ArchiveIndexDelta, BlockSlot, IndexDelta, IndexError, KeyHash, Tag, TagDimension, TagRecord,
};
use fjall::{Keyspace, OwnedWriteBatch, Readable, Snapshot};

use crate::keys::{
    decode_slot, dim_prefix, hash_dimension, hash_key, DIM_HASH_SIZE, HASH_KEY_SIZE, SLOT_SIZE,
};
use crate::Error;

// ============================================================================
// Key Encoding
// ============================================================================

/// Fixed size of block tag key: dim_hash(8) + tag_hash(8) + slot(8) = 24 bytes
const BLOCK_TAG_KEY_SIZE: usize = DIM_HASH_SIZE + HASH_KEY_SIZE + SLOT_SIZE;

/// Build a block tag key: `[dim_hash:8][xxh3(tag_key):8][slot:8]`
fn build_block_tag_key(dimension: &str, tag_key: &[u8], slot: u64) -> [u8; BLOCK_TAG_KEY_SIZE] {
    let tag_hash = hash_key(tag_key);
    build_block_tag_key_hashed(dimension, tag_hash, slot)
}

/// Build a block tag key with pre-computed tag hash:
/// `[dim_hash:8][tag_hash:8][slot:8]`
fn build_block_tag_key_hashed(
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
fn build_block_tag_prefix_hashed(
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
fn decode_block_tag_slot(key: &[u8]) -> u64 {
    debug_assert!(key.len() >= BLOCK_TAG_KEY_SIZE);
    let start = key.len() - SLOT_SIZE;
    decode_slot(&key[start..])
}

/// Decode the stored tag key hash from a block tag key (bytes 8..16)
fn decode_block_tag_key_hash(key: &[u8]) -> KeyHash {
    debug_assert!(key.len() >= BLOCK_TAG_KEY_SIZE);
    let mut hash = [0u8; HASH_KEY_SIZE];
    hash.copy_from_slice(&key[DIM_HASH_SIZE..DIM_HASH_SIZE + HASH_KEY_SIZE]);
    hash
}

// ============================================================================
// Operations
// ============================================================================

/// Insert a block tag entry (multimap style)
fn insert_block_tag(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    dimension: &str,
    data: &[u8],
    slot: BlockSlot,
) {
    let key = build_block_tag_key(dimension, data, slot);
    batch.insert(keyspace, key, []);
}

/// Insert a block tag entry with pre-hashed key
fn insert_block_tag_hashed(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    dimension: &str,
    hash: u64,
    slot: BlockSlot,
) {
    let key = build_block_tag_key_hashed(dimension, hash, slot);
    batch.insert(keyspace, key, []);
}

/// Remove a block tag entry (multimap style)
fn remove_block_tag(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    dimension: &str,
    data: &[u8],
    slot: BlockSlot,
) {
    let key = build_block_tag_key(dimension, data, slot);
    batch.remove(keyspace, key);
}

/// Remove a block tag entry with pre-hashed key
fn remove_block_tag_hashed(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    dimension: &str,
    hash: u64,
    slot: BlockSlot,
) {
    let key = build_block_tag_key_hashed(dimension, hash, slot);
    batch.remove(keyspace, key);
}

/// Insert a tag into the archive-tags keyspace.
///
/// For "metadata" dimension, the key is already the u64 hash value (8 bytes).
/// For all other dimensions, the key is hashed internally.
fn insert_tag(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, tag: &Tag, slot: BlockSlot) {
    // Metadata is special - the key is already the u64 hash value
    if tag.dimension == "metadata" {
        if let Ok(hash_bytes) = tag.key.as_slice().try_into() {
            let hash = u64::from_be_bytes(hash_bytes);
            insert_block_tag_hashed(batch, keyspace, tag.dimension, hash, slot);
        }
        return;
    }

    insert_block_tag(batch, keyspace, tag.dimension, &tag.key, slot);
}

/// Remove a tag from the archive-tags keyspace.
///
/// For "metadata" dimension, the key is already the u64 hash value (8 bytes).
/// For all other dimensions, the key is hashed internally.
fn remove_tag(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, tag: &Tag, slot: BlockSlot) {
    // Metadata is special - the key is already the u64 hash value
    if tag.dimension == "metadata" {
        if let Ok(hash_bytes) = tag.key.as_slice().try_into() {
            let hash = u64::from_be_bytes(hash_bytes);
            remove_block_tag_hashed(batch, keyspace, tag.dimension, hash, slot);
        }
        return;
    }

    remove_block_tag(batch, keyspace, tag.dimension, &tag.key, slot);
}

/// Insert a tag entry from a record that already carries its stored key hash.
///
/// This is the write mirror of [`TagRecordIterator`]: the 8 hash bytes go in
/// verbatim, so a record read out of one store lands byte-identical in another.
/// It is deliberately *not* routed through [`insert_tag`] — that function
/// hashes (and special-cases `metadata`) because it starts from a logical key,
/// which a restore does not have.
///
/// `dim_hash` is `hash_dimension(dim_prefix::BLOCK, record.dimension())`,
/// passed in rather than computed here: records arrive grouped by dimension,
/// so the caller hashes each dimension once per group instead of once per
/// record.
pub fn insert_prehashed(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    record: &TagRecord,
    dim_hash: [u8; DIM_HASH_SIZE],
) {
    let mut key = [0u8; BLOCK_TAG_KEY_SIZE];
    key[..DIM_HASH_SIZE].copy_from_slice(&dim_hash);
    key[DIM_HASH_SIZE..DIM_HASH_SIZE + HASH_KEY_SIZE].copy_from_slice(&record.key_hash);
    key[DIM_HASH_SIZE + HASH_KEY_SIZE..].copy_from_slice(&record.slot.to_be_bytes());
    batch.insert(keyspace, key, []);
}

/// Apply archive tag indexes for a single block delta
fn apply_block(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    for tag in &block.tags {
        insert_tag(batch, keyspace, tag, block.slot);
    }
    Ok(())
}

/// Undo archive tag indexes for a single block delta (rollback)
fn undo_block(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    for tag in &block.tags {
        remove_tag(batch, keyspace, tag, block.slot);
    }
    Ok(())
}

/// Apply archive tag indexes from an IndexDelta
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in &delta.archive {
        apply_block(batch, keyspace, block)?;
    }
    Ok(())
}

/// Undo archive tag indexes from an IndexDelta (rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in delta.archive.iter().rev() {
        undo_block(batch, keyspace, block)?;
    }
    Ok(())
}

// ============================================================================
// SlotIterator
// ============================================================================

/// Slot iterator for block tag queries.
/// Wraps a fjall prefix iterator and filters by slot range.
pub struct SlotIterator {
    /// Collected slots from prefix scan
    slots: Vec<BlockSlot>,
    /// Current position for forward iteration
    front: usize,
    /// Current position for backward iteration
    back: usize,
}

impl SlotIterator {
    /// Create a new slot iterator from an archive-tags keyspace prefix scan.
    ///
    /// The dimension string is passed directly (chain-agnostic).
    pub fn new<R: Readable>(
        readable: &R,
        keyspace: &Keyspace,
        dimension: &str,
        data: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self, Error> {
        let hash = hash_key(data);
        Self::from_hash(readable, keyspace, dimension, hash, start_slot, end_slot)
    }

    /// Create from a pre-computed hash (for metadata labels).
    ///
    /// The dimension string is passed directly (chain-agnostic).
    pub fn from_hash<R: Readable>(
        readable: &R,
        keyspace: &Keyspace,
        dimension: &str,
        hash: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self, Error> {
        let prefix = build_block_tag_prefix_hashed(dimension, hash);
        let mut slots = Vec::new();

        // Using Readable::prefix() enables snapshot-based iteration
        for guard in readable.prefix(keyspace, prefix) {
            let key = guard.key()?;

            if key.len() >= BLOCK_TAG_KEY_SIZE {
                let slot = decode_block_tag_slot(&key);

                if slot >= start_slot && slot <= end_slot {
                    slots.push(slot);
                }
            }
        }

        // Slots are already sorted because keys are sorted lexicographically
        // and we use big-endian encoding
        let len = slots.len();
        Ok(Self {
            slots,
            front: 0,
            back: len,
        })
    }
}

impl Iterator for SlotIterator {
    type Item = Result<BlockSlot, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            let slot = self.slots[self.front];
            self.front += 1;
            Some(Ok(slot))
        } else {
            None
        }
    }
}

impl DoubleEndedIterator for SlotIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.front < self.back {
            self.back -= 1;
            let slot = self.slots[self.back];
            Some(Ok(slot))
        } else {
            None
        }
    }
}

/// Lazy iterator over archive tag records, one dimension prefix at a time.
///
/// ## Why it is driven by a dimension list
///
/// The stored key is `[dim_hash:8][tag_hash:8][slot:8]` where
/// `dim_hash = xxh3("block:" + dimension)`. The dimension *string* is nowhere
/// on disk, so a blind scan of the keyspace could not label the records it
/// produced. Traversal therefore takes the known dimension list and seeks one
/// prefix at a time, which supplies the dimension as a side effect.
///
/// ## Ordering
///
/// On-disk order is by `dim_hash`, which is unrelated to the dimension string's
/// order. The contract is `(dimension, key_hash, slot)`, so the dimension list
/// is sorted (and deduplicated) up front and walked in that order; within one
/// prefix, fjall's lexicographic order over `[tag_hash][slot]` is already the
/// rest of the contract, both being big-endian.
///
/// ## Cost
///
/// `slot` is the *last* key component, so a slot range is not a seekable
/// prefix: each dimension is scanned in full and filtered. This is the cost
/// centre of a per-epoch slice. Memory stays O(1) — entries are matched one at
/// a time (tag values are empty, so each entry is just its 24-byte key).
///
/// ## Lifetime
///
/// The iterator holds an MVCC snapshot for as long as it lives, which pins
/// fjall's GC watermark: superseded versions written while it is alive are
/// retained until it drops. Drain or drop it promptly on a live store.
///
/// ## Errors
///
/// Errors are terminal: after yielding an `Err` — a read failure or a
/// malformed entry — the iterator is fused and yields `None` forever. The
/// record set feeds a signed snapshot layer, so resuming past a fault (or
/// silently skipping a bad entry) would let a corrupt store produce a
/// clean-looking, truncated layer.
pub struct TagRecordIterator {
    snapshot: Snapshot,
    keyspace: Keyspace,
    dimensions: std::vec::IntoIter<TagDimension>,
    current: Option<(TagDimension, fjall::Iter)>,
    slots: Range<BlockSlot>,
    done: bool,
}

impl TagRecordIterator {
    /// Create a new iterator over the tag records of the given dimensions.
    ///
    /// Returns immediately without reading any data.
    pub fn new(
        snapshot: Snapshot,
        keyspace: &Keyspace,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Self {
        // Sorting here rather than trusting the caller makes the ordering
        // contract unconditional; the list is a dozen entries at most.
        let mut dimensions = dimensions.to_vec();
        dimensions.sort_unstable();
        dimensions.dedup();

        Self {
            snapshot,
            keyspace: keyspace.clone(),
            dimensions: dimensions.into_iter(),
            current: None,
            // An empty range matches nothing; skip the prefix scans entirely.
            done: slots.is_empty(),
            slots,
        }
    }

    /// Open a prefix scan for the next dimension, if any is left.
    fn open_next(&mut self) -> Option<()> {
        let dimension = self.dimensions.next()?;
        let prefix = hash_dimension(dim_prefix::BLOCK, dimension);
        let iter = self.snapshot.prefix(&self.keyspace, prefix);
        self.current = Some((dimension, iter));
        Some(())
    }
}

impl Iterator for TagRecordIterator {
    type Item = Result<TagRecord, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        loop {
            let Some((dimension, iter)) = self.current.as_mut() else {
                self.open_next()?;
                continue;
            };

            let dimension = *dimension;

            let Some(guard) = iter.next() else {
                self.current = None;
                continue;
            };

            let key = match guard.key() {
                Ok(key) => key,
                Err(e) => {
                    self.done = true;
                    return Some(Err(Error::Fjall(e).into()));
                }
            };

            if key.len() < BLOCK_TAG_KEY_SIZE {
                self.done = true;
                return Some(Err(IndexError::CodecError(format!(
                    "malformed archive tag key in dimension {dimension}: \
                     {} bytes, expected {BLOCK_TAG_KEY_SIZE}",
                    key.len(),
                ))));
            }

            let slot = decode_block_tag_slot(&key);

            if !self.slots.contains(&slot) {
                continue;
            }

            return Some(Ok(TagRecord {
                dimension: Cow::Borrowed(dimension),
                key_hash: decode_block_tag_key_hash(&key),
                slot,
            }));
        }
    }
}

impl std::iter::FusedIterator for TagRecordIterator {}

#[cfg(test)]
mod tests {
    use super::*;

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
        let utxo_dim_hash = hash_dimension(dim_prefix::UTXO, "address");
        let block_key = build_block_tag_key("address", b"addr", 0);

        // dim_hash for utxo vs block should be different
        assert_ne!(&utxo_dim_hash, &block_key[..DIM_HASH_SIZE]);
    }

    #[test]
    fn test_any_dimension_works() {
        // Any dimension string should work (chain-agnostic)
        let key = build_block_tag_key("another_dimension", b"data", 12345);

        // Key should be valid size
        assert_eq!(key.len(), BLOCK_TAG_KEY_SIZE);
    }

    /// The key hash a record carries has to rebuild the exact key it came from,
    /// otherwise an export/restore round trip silently loses entries.
    #[test]
    fn test_prehashed_key_matches_written_key() {
        let slot = 42u64;
        let written = build_block_tag_key("address", b"some_address_bytes", slot);

        let record = TagRecord::new("address", decode_block_tag_key_hash(&written), slot);
        let rebuilt = build_block_tag_key_hashed(
            record.dimension(),
            u64::from_be_bytes(record.key_hash),
            record.slot,
        );

        assert_eq!(written, rebuilt);
    }

    /// `metadata` is the one dimension whose stored key bytes are not a hash:
    /// the logical key is already a u64 label and `insert_tag` writes it
    /// verbatim. A record must carry those bytes through unchanged rather than
    /// re-deriving them.
    #[test]
    fn test_prehashed_key_carries_metadata_label_verbatim() {
        let label = 674u64;
        let slot = 42u64;
        let written = build_block_tag_key_hashed("metadata", label, slot);

        let stored = decode_block_tag_key_hash(&written);
        assert_eq!(u64::from_be_bytes(stored), label);
        assert_ne!(
            u64::from_be_bytes(stored),
            hash_key(&label.to_be_bytes()),
            "the metadata label is stored raw, not hashed"
        );

        let record = TagRecord::new("metadata", stored, slot);
        let rebuilt = build_block_tag_key_hashed(
            record.dimension(),
            u64::from_be_bytes(record.key_hash),
            record.slot,
        );

        assert_eq!(written, rebuilt);
    }
}
