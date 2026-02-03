//! Index store trait for cross-cutting indexes.
//!
//! The `IndexStore` provides lookups that return primitive index values (slots, UTxO refs)
//! rather than full block data. To get block data, use `AsyncQueryFacade` from the
//! `async_query` module which combines index lookups with archive fetches.
//!
//! This module defines a chain-agnostic indexing system based on "tags" - associations
//! between entities (blocks, transactions, UTxOs) and dimension keys. Chain-specific
//! code (e.g., Cardano) defines the dimensions and provides extension traits for
//! convenient access.

use thiserror::Error;

use crate::{BlockSlot, ChainPoint, TxoRef, UtxoSet};

/// A dimension name identifying an index table.
///
/// Dimensions are static strings that identify the type of index.
/// Examples: "address", "payment", "stake", "policy", "asset"
///
/// Each dimension corresponds to a separate index table in the storage backend.
pub type TagDimension = &'static str;

/// A tag associating data with a dimension.
///
/// Tags are the fundamental unit of indexing. They associate an entity
/// (block, transaction, or UTxO) with a searchable key within a dimension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tag {
    pub dimension: TagDimension,
    pub key: Vec<u8>,
}

impl Tag {
    pub fn new(dimension: TagDimension, key: impl Into<Vec<u8>>) -> Self {
        Self {
            dimension,
            key: key.into(),
        }
    }
}

/// Delta for UTxO filter indexes (current state).
///
/// UTxO filter indexes track the current set of UTxOs matching various tags.
/// They are updated as UTxOs are produced and consumed.
#[derive(Debug, Clone, Default)]
pub struct UtxoIndexDelta {
    /// UTxOs to add to filter indexes: (txo_ref, tags)
    pub produced: Vec<(TxoRef, Vec<Tag>)>,
    /// UTxOs to remove from filter indexes: (txo_ref, tags)
    pub consumed: Vec<(TxoRef, Vec<Tag>)>,
}

/// Delta for archive indexes (historical).
///
/// Archive indexes track which slots contain data matching various tags.
/// They enable historical queries like "find all blocks with transactions
/// involving this address".
#[derive(Debug, Clone, Default)]
pub struct ArchiveIndexDelta {
    pub slot: BlockSlot,
    pub block_hash: Vec<u8>,
    pub block_number: Option<u64>,
    pub tx_hashes: Vec<Vec<u8>>,
    pub tags: Vec<Tag>,
}

/// Unified index delta for a batch of operations.
///
/// This structure contains all index changes for a batch of blocks,
/// including both UTxO filter updates and archive index entries.
#[derive(Debug, Clone)]
pub struct IndexDelta {
    /// Cursor position after applying this delta.
    pub cursor: ChainPoint,
    /// UTxO filter index changes.
    pub utxo: UtxoIndexDelta,
    /// Archive index changes (one per block in batch).
    pub archive: Vec<ArchiveIndexDelta>,
}

impl Default for IndexDelta {
    fn default() -> Self {
        Self {
            cursor: ChainPoint::Origin,
            utxo: UtxoIndexDelta::default(),
            archive: Vec::new(),
        }
    }
}

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index db error: {0}")]
    DbError(String),

    #[error("codec error: {0}")]
    CodecError(String),

    #[error("schema error: {0}")]
    SchemaError(String),

    #[error("dimension not found: {0}")]
    DimensionNotFound(String),
}

/// Writer for batched index operations.
///
/// This trait provides transactional write operations for indexes. Multiple
/// operations can be batched together and committed atomically.
pub trait IndexWriter: Send + Sync + 'static {
    /// Apply index changes from a delta.
    ///
    /// This applies all UTxO filter changes and archive index entries
    /// contained in the delta. The cursor is set internally from `delta.cursor`.
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError>;

    /// Undo index changes from a delta (rollback).
    ///
    /// This reverses the changes made by `apply()`:
    /// - UTxOs in `produced` are removed from filter indexes
    /// - UTxOs in `consumed` are restored to filter indexes
    /// - Archive index entries are removed
    fn undo(&self, delta: &IndexDelta) -> Result<(), IndexError>;

    /// Commit the batched operations.
    fn commit(self) -> Result<(), IndexError>;
}

/// Index store trait for cross-cutting indexes.
///
/// This trait provides pure index lookups that return primitive values like
/// `BlockSlot` or `UtxoSet` rather than full block data. For high-level queries
/// that also fetch block data, use `AsyncQueryFacade`.
///
/// The trait is chain-agnostic, using dimension strings to identify index types.
/// Chain-specific code should provide extension traits with convenient methods.
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
    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError>;

    // ============ UTxO Filter Queries ============

    /// Query UTxOs by tag dimension and key.
    ///
    /// Returns all UTxO references that have been tagged with the given
    /// dimension and key and have not been consumed.
    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, IndexError>;

    // ============ Archive Queries (Exact Lookups) ============

    /// Get the slot for a block by its hash (exact lookup).
    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError>;

    /// Get the slot for a block by its number/height (exact lookup).
    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError>;

    /// Get the slot containing a transaction by its hash (exact lookup).
    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError>;

    // ============ Archive Queries (Tag-Based Range Queries) ============

    /// Query slots by tag dimension and key within a slot range.
    ///
    /// Returns an iterator over slots that contain data tagged with the given
    /// dimension and key. The iterator is lazy and supports bidirectional
    /// iteration for efficient pagination.
    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;
}
