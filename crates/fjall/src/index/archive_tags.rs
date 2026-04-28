//! Archive tag index operations for the `archive-tags` keyspace (chain-agnostic).
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

use dolos_core::{ArchiveIndexDelta, BlockSlot, IndexDelta, IndexError, Tag};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

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

/// Build a block tag key with pre-computed tag hash: `[dim_hash:8][tag_hash:8][slot:8]`
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
}
