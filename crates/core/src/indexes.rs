//! Index store trait for cross-cutting indexes.
//!
//! The `IndexStore` provides lookups that return primitive index values (slots, hashes)
//! rather than full block data. To get block data, use the `QueryHelpers` trait from
//! the `query` module which combines index lookups with archive fetches.

use thiserror::Error;

use crate::{BlockSlot, ChainPoint, SlotTags, UtxoSet, UtxoSetDelta};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index db error: {0}")]
    DbError(String),

    #[error("codec error: {0}")]
    CodecError(String),

    #[error("schema error: {0}")]
    SchemaError(String),
}

/// Writer for batched index operations.
///
/// This trait provides transactional write operations for indexes. Multiple
/// operations can be batched together and committed atomically.
pub trait IndexWriter: Send + Sync + 'static {
    /// Apply UTxO set changes to the filter indexes.
    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), IndexError>;

    /// Apply archive indexes for a block.
    fn apply_archive(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError>;

    /// Undo archive indexes for a block (rollback).
    fn undo_archive(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError>;

    /// Set the cursor position after applying indexes.
    ///
    /// This should be called after all index operations for a batch are complete,
    /// before committing. The cursor tracks the last applied chain point for
    /// synchronization with other stores.
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), IndexError>;

    /// Commit the batched operations.
    fn commit(self) -> Result<(), IndexError>;
}

/// Index store trait for cross-cutting indexes.
///
/// This trait provides pure index lookups that return primitive values like
/// `BlockSlot` rather than full block data. For high-level queries that also
/// fetch block data, use the `QueryHelpers` trait.
#[trait_variant::make(Send)]
pub trait IndexStore: Clone + Send + Sync + 'static {
    /// Writer type for batched write operations.
    type Writer: IndexWriter;

    /// Iterator type for sparse slot queries.
    type SlotIter: Iterator<Item = Result<BlockSlot, IndexError>> + DoubleEndedIterator;

    /// Start a new writer for batched operations.
    fn start_writer(&self) -> Result<Self::Writer, IndexError>;

    /// Initialize the index schema (create tables, etc.).
    fn initialize_schema(&self) -> Result<(), IndexError>;

    /// Copy all index data to another store.
    fn copy(&self, target: &Self) -> Result<(), IndexError>;

    /// Read the current cursor position.
    ///
    /// Returns the last chain point that was indexed, or None if no indexes
    /// have been applied yet. This is used for synchronization verification
    /// with other stores (state, archive).
    fn read_cursor(&self) -> Result<Option<ChainPoint>, IndexError>;

    // UTxO filter index queries (these return UTxO sets, not slots)

    /// Get UTxOs by full address.
    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, IndexError>;

    /// Get UTxOs by payment credential.
    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, IndexError>;

    /// Get UTxOs by stake credential.
    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, IndexError>;

    /// Get UTxOs by policy ID.
    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, IndexError>;

    /// Get UTxOs by asset (policy + name).
    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, IndexError>;

    // Archive index queries (these return slots, not block data)

    /// Get the slot for a block by its hash.
    fn slot_for_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError>;

    /// Get the slot for a block by its number (height).
    fn slot_for_block_number(&self, number: &u64) -> Result<Option<BlockSlot>, IndexError>;

    /// Get the slot containing a transaction by its hash.
    fn slot_for_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError>;

    /// Get slots containing blocks with a specific datum hash.
    fn slots_for_datum_hash(
        &self,
        datum_hash: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, IndexError>;

    /// Get slots containing blocks that spent a specific UTxO.
    fn slots_for_spent_txo(
        &self,
        spent_txo: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Vec<BlockSlot>, IndexError>;

    // Sparse slot iterators for archive indexes

    /// Iterate over slots of blocks containing transactions involving an address.
    fn slots_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;

    /// Iterate over slots of blocks containing transactions involving an asset.
    fn slots_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;

    /// Iterate over slots of blocks containing transactions involving a payment credential.
    fn slots_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;

    /// Iterate over slots of blocks containing transactions involving a stake credential.
    fn slots_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;

    /// Iterate over slots of blocks containing certificates for an account.
    fn slots_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;

    /// Iterate over slots of blocks containing transactions with a specific metadata label.
    fn slots_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;
}
