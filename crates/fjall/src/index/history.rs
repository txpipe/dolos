//! Historical index operations for fjall (chain-agnostic).
//!
//! These indexes support queries over archived block data:
//! 1. Exact lookups (in `index-exact` keyspace): block_hash -> slot, tx_hash -> slot, block_number -> slot
//! 2. Block tags (in `index-tags` keyspace): xxh3(data) ++ slot -> [] (multimap via prefix scan)
//!
//! All keys use dimension hashing for chain-agnostic storage:
//! - Exact lookups: `[dim_hash:8][key_data:var]` -> `[slot:8]`
//! - Block tags: `[dim_hash:8][xxh3(tag_key):8][slot:8]` -> empty

use dolos_core::{ArchiveIndexDelta, BlockSlot, IndexDelta, IndexError, Tag};
use fjall::{Keyspace, OwnedWriteBatch, Readable};

use super::exact_keys::{
    build_exact_key, build_exact_key_blocknum, decode_slot_value, encode_slot_value,
    DIM_BLOCK_HASH, DIM_TX_HASH,
};
use super::tag_keys::{
    build_block_tag_key, build_block_tag_key_hashed, build_block_tag_prefix_hashed,
    decode_block_tag_slot, BLOCK_TAG_KEY_SIZE,
};
use crate::keys::hash_key;
use crate::Error;

// ============================================================================
// Block Tag Operations (index-tags keyspace)
// ============================================================================

/// Insert a block tag entry (multimap style)
fn insert_block_tag(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    dimension: &str,
    data: &[u8],
    slot: BlockSlot,
) {
    let key = build_block_tag_key(dimension, data, slot);
    batch.insert(tags_keyspace, key, []);
}

/// Insert a block tag entry with pre-hashed key
fn insert_block_tag_hashed(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    dimension: &str,
    hash: u64,
    slot: BlockSlot,
) {
    let key = build_block_tag_key_hashed(dimension, hash, slot);
    batch.insert(tags_keyspace, key, []);
}

/// Remove a block tag entry (multimap style)
fn remove_block_tag(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    dimension: &str,
    data: &[u8],
    slot: BlockSlot,
) {
    let key = build_block_tag_key(dimension, data, slot);
    batch.remove(tags_keyspace, key);
}

/// Remove a block tag entry with pre-hashed key
fn remove_block_tag_hashed(
    batch: &mut OwnedWriteBatch,
    tags_keyspace: &Keyspace,
    dimension: &str,
    hash: u64,
    slot: BlockSlot,
) {
    let key = build_block_tag_key_hashed(dimension, hash, slot);
    batch.remove(tags_keyspace, key);
}

/// Insert a tag into the tags keyspace.
///
/// For "metadata" dimension, the key is already the u64 hash value (8 bytes).
/// For all other dimensions, the key is hashed internally.
fn insert_tag(batch: &mut OwnedWriteBatch, tags_keyspace: &Keyspace, tag: &Tag, slot: BlockSlot) {
    // Metadata is special - the key is already the u64 hash value
    if tag.dimension == "metadata" {
        if let Ok(hash_bytes) = tag.key.as_slice().try_into() {
            let hash = u64::from_be_bytes(hash_bytes);
            insert_block_tag_hashed(batch, tags_keyspace, tag.dimension, hash, slot);
        }
        return;
    }

    insert_block_tag(batch, tags_keyspace, tag.dimension, &tag.key, slot);
}

/// Remove a tag from the tags keyspace.
///
/// For "metadata" dimension, the key is already the u64 hash value (8 bytes).
/// For all other dimensions, the key is hashed internally.
fn remove_tag(batch: &mut OwnedWriteBatch, tags_keyspace: &Keyspace, tag: &Tag, slot: BlockSlot) {
    // Metadata is special - the key is already the u64 hash value
    if tag.dimension == "metadata" {
        if let Ok(hash_bytes) = tag.key.as_slice().try_into() {
            let hash = u64::from_be_bytes(hash_bytes);
            remove_block_tag_hashed(batch, tags_keyspace, tag.dimension, hash, slot);
        }
        return;
    }

    remove_block_tag(batch, tags_keyspace, tag.dimension, &tag.key, slot);
}

// ============================================================================
// Block Processing
// ============================================================================

/// Apply archive indexes for a single block delta
fn apply_block(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    tags_keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    let slot = block.slot;

    // Exact lookup: block hash -> slot (in exact keyspace)
    if !block.block_hash.is_empty() {
        let key = build_exact_key(DIM_BLOCK_HASH, &block.block_hash);
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Exact lookup: block number -> slot (in exact keyspace)
    if let Some(number) = block.block_number {
        let key = build_exact_key_blocknum(number);
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Exact lookup: tx hashes -> slot (in exact keyspace)
    for tx_hash in &block.tx_hashes {
        let key = build_exact_key(DIM_TX_HASH, tx_hash.as_slice());
        batch.insert(exact_keyspace, key, encode_slot_value(slot));
    }

    // Block tags (in tags keyspace)
    for tag in &block.tags {
        insert_tag(batch, tags_keyspace, tag, slot);
    }

    Ok(())
}

/// Undo archive indexes for a single block delta (rollback)
fn undo_block(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    tags_keyspace: &Keyspace,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    let slot = block.slot;

    // Remove exact lookups (from exact keyspace)
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

    // Remove block tags (from tags keyspace)
    for tag in &block.tags {
        remove_tag(batch, tags_keyspace, tag, slot);
    }

    Ok(())
}

/// Apply archive indexes from an IndexDelta
pub fn apply(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    tags_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in &delta.archive {
        apply_block(batch, exact_keyspace, tags_keyspace, block)?;
    }
    Ok(())
}

/// Undo archive indexes from an IndexDelta (rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    exact_keyspace: &Keyspace,
    tags_keyspace: &Keyspace,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Undo in reverse order
    for block in delta.archive.iter().rev() {
        undo_block(batch, exact_keyspace, tags_keyspace, block)?;
    }
    Ok(())
}

// ============================================================================
// Exact Lookup Queries (index-exact keyspace)
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
// Block Tag Iterator (index-tags keyspace)
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
    /// Create a new slot iterator from a tags keyspace prefix scan.
    ///
    /// The dimension string is passed directly (chain-agnostic).
    pub fn new<R: Readable>(
        readable: &R,
        tags_keyspace: &Keyspace,
        dimension: &str,
        data: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self, Error> {
        let hash = hash_key(data);
        Self::from_hash(
            readable,
            tags_keyspace,
            dimension,
            hash,
            start_slot,
            end_slot,
        )
    }

    /// Create from a pre-computed hash (for metadata labels).
    ///
    /// The dimension string is passed directly (chain-agnostic).
    pub fn from_hash<R: Readable>(
        readable: &R,
        tags_keyspace: &Keyspace,
        dimension: &str,
        hash: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self, Error> {
        let prefix = build_block_tag_prefix_hashed(dimension, hash);
        let mut slots = Vec::new();

        // Using Readable::prefix() enables snapshot-based iteration
        for guard in readable.prefix(tags_keyspace, prefix) {
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
