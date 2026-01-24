//! Archive index operations for fjall.
//!
//! Archive indexes support two patterns:
//! 1. Exact lookups: block_hash -> slot, tx_hash -> slot, block_number -> slot
//! 2. Approximate lookups: xxh3(data) ++ slot -> [] (multimap via prefix scan)
//!
//! For approximate lookups, variable-length data is hashed to u64 using xxh3.
//! Slots are stored as part of the key (big-endian) for efficient range queries.

use dolos_core::{ArchiveIndexDelta, BlockSlot, IndexDelta, IndexError, Tag};
use fjall::{Keyspace, OwnedWriteBatch};

use crate::keys::{
    archive_composite_key, archive_prefix, decode_slot_from_suffix, encode_slot, encode_u64,
    hash_key, HASH_KEY_SIZE, SLOT_SIZE,
};
use crate::{archive_dimensions, Error};

/// References to all archive index keyspaces
pub struct ArchiveKeyspaces<'a> {
    // Exact lookups
    pub blockhash: &'a Keyspace,
    pub blocknum: &'a Keyspace,
    pub txhash: &'a Keyspace,
    // Approximate lookups (multimap)
    pub address: &'a Keyspace,
    pub payment: &'a Keyspace,
    pub stake: &'a Keyspace,
    pub asset: &'a Keyspace,
    pub policy: &'a Keyspace,
    pub datum: &'a Keyspace,
    pub spenttxo: &'a Keyspace,
    pub account: &'a Keyspace,
    pub metadata: &'a Keyspace,
    pub script: &'a Keyspace,
}

impl<'a> ArchiveKeyspaces<'a> {
    /// Get keyspace for a tag dimension
    fn keyspace_for_dimension(&self, dimension: &str) -> Option<&'a Keyspace> {
        match dimension {
            archive_dimensions::ADDRESS => Some(self.address),
            archive_dimensions::PAYMENT => Some(self.payment),
            archive_dimensions::STAKE => Some(self.stake),
            archive_dimensions::ASSET => Some(self.asset),
            archive_dimensions::POLICY => Some(self.policy),
            archive_dimensions::DATUM => Some(self.datum),
            archive_dimensions::SPENT_TXO => Some(self.spenttxo),
            archive_dimensions::ACCOUNT_CERTS => Some(self.account),
            archive_dimensions::METADATA => Some(self.metadata),
            archive_dimensions::SCRIPT => Some(self.script),
            _ => None,
        }
    }
}

/// Insert an approximate index entry (multimap style)
fn insert_approx(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, data: &[u8], slot: BlockSlot) {
    let hash = hash_key(data);
    let key = archive_composite_key(hash, slot);
    batch.insert(keyspace, key, []);
}

/// Insert an approximate index entry with pre-hashed key
fn insert_approx_hashed(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    hash: u64,
    slot: BlockSlot,
) {
    let key = archive_composite_key(hash, slot);
    batch.insert(keyspace, key, []);
}

/// Remove an approximate index entry (multimap style)
fn remove_approx(batch: &mut OwnedWriteBatch, keyspace: &Keyspace, data: &[u8], slot: BlockSlot) {
    let hash = hash_key(data);
    let key = archive_composite_key(hash, slot);
    batch.remove(keyspace, key);
}

/// Remove an approximate index entry with pre-hashed key
fn remove_approx_hashed(
    batch: &mut OwnedWriteBatch,
    keyspace: &Keyspace,
    hash: u64,
    slot: BlockSlot,
) {
    let key = archive_composite_key(hash, slot);
    batch.remove(keyspace, key);
}

/// Insert a tag into the appropriate keyspace
fn insert_tag(
    batch: &mut OwnedWriteBatch,
    keyspaces: &ArchiveKeyspaces,
    tag: &Tag,
    slot: BlockSlot,
) {
    // Metadata is special - the key is already the u64 hash value
    if tag.dimension == archive_dimensions::METADATA {
        if let Ok(hash_bytes) = tag.key.as_slice().try_into() {
            let hash = u64::from_be_bytes(hash_bytes);
            insert_approx_hashed(batch, keyspaces.metadata, hash, slot);
        }
        return;
    }

    if let Some(keyspace) = keyspaces.keyspace_for_dimension(tag.dimension) {
        insert_approx(batch, keyspace, &tag.key, slot);
    }
}

/// Remove a tag from the appropriate keyspace
fn remove_tag(
    batch: &mut OwnedWriteBatch,
    keyspaces: &ArchiveKeyspaces,
    tag: &Tag,
    slot: BlockSlot,
) {
    // Metadata is special - the key is already the u64 hash value
    if tag.dimension == archive_dimensions::METADATA {
        if let Ok(hash_bytes) = tag.key.as_slice().try_into() {
            let hash = u64::from_be_bytes(hash_bytes);
            remove_approx_hashed(batch, keyspaces.metadata, hash, slot);
        }
        return;
    }

    if let Some(keyspace) = keyspaces.keyspace_for_dimension(tag.dimension) {
        remove_approx(batch, keyspace, &tag.key, slot);
    }
}

/// Apply archive indexes for a single block delta
fn apply_block(
    batch: &mut OwnedWriteBatch,
    keyspaces: &ArchiveKeyspaces,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    let slot = block.slot;

    // Exact lookup: block hash -> slot
    if !block.block_hash.is_empty() {
        batch.insert(keyspaces.blockhash, &block.block_hash, encode_slot(slot));
    }

    // Exact lookup: block number -> slot
    if let Some(number) = block.block_number {
        batch.insert(keyspaces.blocknum, encode_u64(number), encode_slot(slot));
    }

    // Exact lookup: tx hashes -> slot
    for tx_hash in &block.tx_hashes {
        batch.insert(keyspaces.txhash, tx_hash.as_slice(), encode_slot(slot));
    }

    // Tag-based approximate lookups
    for tag in &block.tags {
        insert_tag(batch, keyspaces, tag, slot);
    }

    Ok(())
}

/// Undo archive indexes for a single block delta (rollback)
fn undo_block(
    batch: &mut OwnedWriteBatch,
    keyspaces: &ArchiveKeyspaces,
    block: &ArchiveIndexDelta,
) -> Result<(), Error> {
    let slot = block.slot;

    // Remove exact lookups
    if !block.block_hash.is_empty() {
        batch.remove(keyspaces.blockhash, &block.block_hash);
    }

    if let Some(number) = block.block_number {
        batch.remove(keyspaces.blocknum, encode_u64(number));
    }

    for tx_hash in &block.tx_hashes {
        batch.remove(keyspaces.txhash, tx_hash.as_slice());
    }

    // Remove approximate lookups
    for tag in &block.tags {
        remove_tag(batch, keyspaces, tag, slot);
    }

    Ok(())
}

/// Apply archive indexes from an IndexDelta
pub fn apply(
    batch: &mut OwnedWriteBatch,
    keyspaces: &ArchiveKeyspaces,
    delta: &IndexDelta,
) -> Result<(), Error> {
    for block in &delta.archive {
        apply_block(batch, keyspaces, block)?;
    }
    Ok(())
}

/// Undo archive indexes from an IndexDelta (rollback)
pub fn undo(
    batch: &mut OwnedWriteBatch,
    keyspaces: &ArchiveKeyspaces,
    delta: &IndexDelta,
) -> Result<(), Error> {
    // Undo in reverse order
    for block in delta.archive.iter().rev() {
        undo_block(batch, keyspaces, block)?;
    }
    Ok(())
}

/// Get slot by block hash (exact lookup)
pub fn get_by_block_hash(
    keyspace: &Keyspace,
    block_hash: &[u8],
) -> Result<Option<BlockSlot>, Error> {
    match keyspace
        .get(block_hash)
        .map_err(|e| Error::Fjall(e.into()))?
    {
        Some(value) => {
            let slot = u64::from_be_bytes(
                value
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::Codec("invalid slot encoding".to_string()))?,
            );
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

/// Get slot by block number (exact lookup)
pub fn get_by_block_number(keyspace: &Keyspace, number: u64) -> Result<Option<BlockSlot>, Error> {
    let key = encode_u64(number);
    match keyspace.get(key).map_err(|e| Error::Fjall(e.into()))? {
        Some(value) => {
            let slot = u64::from_be_bytes(
                value
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::Codec("invalid slot encoding".to_string()))?,
            );
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

/// Get slot by tx hash (exact lookup)
pub fn get_by_tx_hash(keyspace: &Keyspace, tx_hash: &[u8]) -> Result<Option<BlockSlot>, Error> {
    match keyspace.get(tx_hash).map_err(|e| Error::Fjall(e.into()))? {
        Some(value) => {
            let slot = u64::from_be_bytes(
                value
                    .as_ref()
                    .try_into()
                    .map_err(|_| Error::Codec("invalid slot encoding".to_string()))?,
            );
            Ok(Some(slot))
        }
        None => Ok(None),
    }
}

/// Get slots for a given lookup key (approximate, filtered by slot range)
pub fn get_slots_by_key(
    keyspace: &Keyspace,
    data: &[u8],
    start_slot: BlockSlot,
    end_slot: BlockSlot,
) -> Result<Vec<BlockSlot>, Error> {
    let hash = hash_key(data);
    get_slots_by_hash(keyspace, hash, start_slot, end_slot)
}

/// Get slots for a given hash (for metadata labels which are already u64)
pub fn get_slots_by_hash(
    keyspace: &Keyspace,
    hash: u64,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
) -> Result<Vec<BlockSlot>, Error> {
    let prefix = archive_prefix(hash);
    let mut slots = Vec::new();

    // fjall's prefix() returns an iterator of Guard items
    // Guard::key() consumes the guard and returns Result<UserKey>
    for guard in keyspace.prefix(prefix) {
        let key = guard.key()?;

        if key.len() >= HASH_KEY_SIZE + SLOT_SIZE {
            let slot = decode_slot_from_suffix(&key);

            if slot >= start_slot && slot <= end_slot {
                slots.push(slot);
            }
        }
    }

    Ok(slots)
}

/// Slot iterator for archive index queries.
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
    /// Create a new slot iterator from a keyspace prefix scan
    pub fn new(
        keyspace: &Keyspace,
        data: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self, Error> {
        let hash = hash_key(data);
        Self::from_hash(keyspace, hash, start_slot, end_slot)
    }

    /// Create from a pre-computed hash (for metadata labels)
    pub fn from_hash(
        keyspace: &Keyspace,
        hash: u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self, Error> {
        let prefix = archive_prefix(hash);
        let mut slots = Vec::new();

        // fjall's prefix() returns an iterator of Guard items
        // Guard::key() consumes the guard and returns Result<UserKey>
        for guard in keyspace.prefix(prefix) {
            let key = guard.key()?;

            if key.len() >= HASH_KEY_SIZE + SLOT_SIZE {
                let slot = decode_slot_from_suffix(&key);

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
