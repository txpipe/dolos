//! Index store trait for cross-cutting indexes.
//!
//! The `IndexStore` provides lookups that return primitive index values (slots,
//! UTxO refs) rather than full block data. To get block data, use
//! `AsyncQueryFacade` from the `async_query` module which combines index
//! lookups with archive fetches.
//!
//! This module defines a chain-agnostic indexing system based on "tags" -
//! associations between entities (blocks, transactions, UTxOs) and dimension
//! keys. Chain-specific code (e.g., Cardano) defines the dimensions and
//! provides extension traits for convenient access.

use std::{borrow::Cow, ops::Range};

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

    /// The operation is part of the trait but the concrete backend does not
    /// implement it.
    ///
    /// Used by backends outside the live set (see the record iteration and
    /// pre-hashed append below) so that a caller reaching for an unimplemented
    /// capability gets an error it can handle instead of a panic.
    #[error("{0} is not supported on this storage backend")]
    Unsupported(&'static str),
}

// ============================================================================
// Pre-hashed archive index records
// ============================================================================

/// Size of the hashed portion of an archive tag key.
pub const KEY_HASH_SIZE: usize = 8;

/// The stored, fixed-width form of a tag key.
///
/// For every dimension but `metadata` this is `xxh3_64(logical key)` in
/// big-endian byte order. `metadata` is an exception: its logical key is
/// already a `u64` label and the store keeps that label verbatim (big-endian)
/// rather than hashing it. Consumers must treat these bytes as opaque and copy
/// them through unchanged — recomputing a hash from the logical key is not a
/// valid way to reconstruct them for every dimension.
pub type KeyHash = [u8; KEY_HASH_SIZE];

/// One archive tag entry, carrying the stored key hash instead of the logical
/// key.
///
/// The logical key is not recoverable from the index store — only its hash is
/// kept on disk — so this is the most a reader can produce and the least a
/// writer needs.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TagRecord {
    /// The logical dimension name. Borrowed when read from a store (the
    /// dimension list drives the traversal), owned when decoded from an
    /// external source.
    pub dimension: Cow<'static, str>,

    /// The stored key hash. See [`KeyHash`] for the `metadata` caveat.
    pub key_hash: KeyHash,

    pub slot: BlockSlot,
}

impl TagRecord {
    pub fn new(dimension: TagDimension, key_hash: KeyHash, slot: BlockSlot) -> Self {
        Self {
            dimension: Cow::Borrowed(dimension),
            key_hash,
            slot,
        }
    }

    pub fn dimension(&self) -> &str {
        self.dimension.as_ref()
    }
}

/// The kinds of exact-match archive lookup an index store keeps.
///
/// This is a closed set: the store hashes the kind name into the key, so it
/// cannot be recovered from disk and traversal has to be driven by the known
/// list.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ExactKind {
    BlockHash,
    BlockNumber,
    TxHash,
}

impl ExactKind {
    /// Every kind, in ascending [`ExactKind::as_str`] order.
    pub const ALL: [ExactKind; 3] = [Self::BlockHash, Self::BlockNumber, Self::TxHash];

    /// The stored dimension name for this kind.
    ///
    /// These strings are hashed into on-disk keys — changing one is a storage
    /// migration, not a rename.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::BlockHash => "block_hash",
            Self::BlockNumber => "block_num",
            Self::TxHash => "tx_hash",
        }
    }

    /// The exact byte length of a stored key of this kind.
    ///
    /// Exact keys are fixed-width per kind: a 32-byte hash for
    /// [`ExactKind::BlockHash`] and [`ExactKind::TxHash`], an 8-byte big-endian
    /// number for [`ExactKind::BlockNumber`]. Writers validate against this so
    /// a wrong-width key from an external source cannot land as a permanently
    /// unreadable entry.
    pub const fn key_len(&self) -> usize {
        match self {
            Self::BlockHash | Self::TxHash => 32,
            Self::BlockNumber => 8,
        }
    }
}

impl std::str::FromStr for ExactKind {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::ALL
            .into_iter()
            .find(|kind| kind.as_str() == s)
            .ok_or_else(|| format!("unknown exact kind: {s}"))
    }
}

impl std::fmt::Display for ExactKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One exact-match archive entry.
///
/// Unlike tags, exact keys are stored verbatim (a 32-byte hash, or an 8-byte
/// big-endian block number), so the record is lossless.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ExactRecord {
    pub kind: ExactKind,
    pub key: Vec<u8>,
    pub slot: BlockSlot,
}

impl ExactRecord {
    pub fn new(kind: ExactKind, key: impl Into<Vec<u8>>, slot: BlockSlot) -> Self {
        Self {
            kind,
            key: key.into(),
            slot,
        }
    }
}

/// Either archive record, as consumed by
/// [`IndexWriter::append_prehashed`].
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum IndexRecord {
    Tag(TagRecord),
    Exact(ExactRecord),
}

impl From<TagRecord> for IndexRecord {
    fn from(value: TagRecord) -> Self {
        Self::Tag(value)
    }
}

impl From<ExactRecord> for IndexRecord {
    fn from(value: ExactRecord) -> Self {
        Self::Exact(value)
    }
}

/// Iterator used by backends that do not implement
/// [`IndexStore::iter_archive_tags`].
///
/// Never constructed — those backends return [`IndexError::Unsupported`], so
/// the alias only exists to satisfy the associated type.
pub type EmptyTagIter = std::iter::Empty<Result<TagRecord, IndexError>>;

/// Iterator used by backends that do not implement
/// [`IndexStore::iter_exact_records`].
///
/// Never constructed — those backends return [`IndexError::Unsupported`], so
/// the alias only exists to satisfy the associated type.
pub type EmptyExactIter = std::iter::Empty<Result<ExactRecord, IndexError>>;

/// Writer for batched index operations.
///
/// This trait provides transactional write operations for indexes. Multiple
/// operations can be batched together and committed atomically.
pub trait IndexWriter: Send + Sync + 'static {
    /// Apply index changes from a delta.
    ///
    /// This applies all UTxO filter changes and archive index entries
    /// contained in the delta. The cursor is set internally from
    /// `delta.cursor`.
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError>;

    /// Undo index changes from a delta (rollback).
    ///
    /// This reverses the changes made by `apply()`:
    /// - UTxOs in `produced` are removed from filter indexes
    /// - UTxOs in `consumed` are restored to filter indexes
    /// - Archive index entries are removed
    fn undo(&self, delta: &IndexDelta) -> Result<(), IndexError>;

    /// Append archive records that already carry their stored key form.
    ///
    /// This is the write mirror of [`IndexStore::iter_archive_tags`] and
    /// [`IndexStore::iter_exact_records`]: records that came out of one store
    /// go into another byte-for-byte, with no logical key in between (there is
    /// none to recover — see [`KeyHash`]).
    ///
    /// Records must arrive sorted, since the backing stores are append
    /// oriented. The cursor is *not* touched: a restore owns cursor placement,
    /// and places it with an otherwise-empty delta once the records are in:
    /// `writer.apply(&IndexDelta { cursor, ..Default::default() })`. A restore
    /// that skips this leaves `cursor()` at `None` and bootstrap will treat the
    /// store as never indexed.
    ///
    /// Backends that do not implement this return
    /// [`IndexError::Unsupported`].
    fn append_prehashed(&self, records: &[IndexRecord]) -> Result<(), IndexError>;

    /// Commit the batched operations.
    fn commit(self) -> Result<(), IndexError>;
}

/// Index store trait for cross-cutting indexes.
///
/// This trait provides pure index lookups that return primitive values like
/// `BlockSlot` or `UtxoSet` rather than full block data. For high-level queries
/// that also fetch block data, use `AsyncQueryFacade`.
///
/// The trait is chain-agnostic, using dimension strings to identify index
/// types. Chain-specific code should provide extension traits with convenient
/// methods.
#[trait_variant::make(Send)]
pub trait IndexStore: Clone + Send + Sync + 'static {
    /// Writer type for batched write operations.
    type Writer: IndexWriter;

    /// Iterator type for sparse slot queries.
    type SlotIter: Iterator<Item = Result<BlockSlot, IndexError>> + DoubleEndedIterator;

    /// Iterator type for archive tag record traversal.
    type TagIter: Iterator<Item = Result<TagRecord, IndexError>>;

    /// Iterator type for exact-match record traversal.
    type ExactIter: Iterator<Item = Result<ExactRecord, IndexError>>;

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
    ///
    /// Both `start` and `end` are **inclusive** — unlike the record traversal
    /// methods below, which take a half-open [`Range`]. Reusing one `(start,
    /// end)` pair across both conventions drops or double-counts the record at
    /// `end`.
    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError>;

    // ============ Record Traversal (Bulk Export) ============

    /// Iterate every archive tag record whose slot falls in `slots`.
    ///
    /// `slots` is **half-open** (`start..end`), unlike
    /// [`IndexStore::slots_by_tag`], whose bounds are both inclusive.
    ///
    /// `dimensions` is the closed list of dimensions to traverse. It has to be
    /// supplied by the caller: stores keep a hash of the dimension name, not
    /// the name, so the set of dimensions is not discoverable from disk.
    /// Duplicates are ignored.
    ///
    /// **Order is part of the contract.** Records come out sorted by
    /// `(dimension, key_hash, slot)` — `dimension` compared as a string,
    /// independently of the order `dimensions` was given in. Consumers of this
    /// iteration (snapshot layers) make that order their content, so an
    /// unordered iterator is a wrong one.
    ///
    /// **Errors are terminal.** A malformed on-disk entry or a read failure is
    /// yielded as `Err` and the iterator is fused from then on: it never
    /// resumes past a fault, so a consumer that collects into
    /// `Result<Vec<_>, _>` cannot receive a silently truncated record set.
    ///
    /// The iterator must be lazy in the size of the store. Each call opens its
    /// own point-in-time view: records from separate calls (or from
    /// [`IndexStore::iter_exact_records`]) are only mutually consistent if the
    /// store is quiescent across the calls.
    ///
    /// Backends that do not implement this return
    /// [`IndexError::Unsupported`].
    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, IndexError>;

    /// Iterate every exact-match record whose slot falls in `slots`.
    ///
    /// `slots` is **half-open** (`start..end`), unlike
    /// [`IndexStore::slots_by_tag`], whose bounds are both inclusive.
    ///
    /// **Order is part of the contract**: records come out sorted by
    /// `(kind, key)`, kinds in ascending [`ExactKind::as_str`] order.
    ///
    /// **Errors are terminal** — same policy as
    /// [`IndexStore::iter_archive_tags`].
    ///
    /// Note that the slot is the stored *value* of an exact entry, not part of
    /// its key, so a slot-bounded traversal is a scan of every exact entry in
    /// the store rather than a seek. The iterator is still lazy in memory.
    ///
    /// Backends that do not implement this return
    /// [`IndexError::Unsupported`].
    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, IndexError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_kind_all_is_sorted_by_name() {
        let names: Vec<&str> = ExactKind::ALL.iter().map(|k| k.as_str()).collect();

        let mut sorted = names.clone();
        sorted.sort_unstable();

        assert_eq!(
            names, sorted,
            "ExactKind::ALL must be in ascending name order: the exact record \
             contract sorts by (kind, key) and the traversal walks ALL in order"
        );
    }

    #[test]
    fn exact_kind_ord_matches_name_ord() {
        for pair in ExactKind::ALL.windows(2) {
            assert!(pair[0] < pair[1]);
            assert!(pair[0].as_str() < pair[1].as_str());
        }
    }

    #[test]
    fn exact_kind_roundtrips_through_name() {
        for kind in ExactKind::ALL {
            assert_eq!(kind.as_str().parse::<ExactKind>(), Ok(kind));
        }

        assert!("nope".parse::<ExactKind>().is_err());
    }

    /// Pins the literal strings: they are hashed into on-disk keys, so a
    /// rename that looks cosmetic (e.g. `block_num` -> `block_number`) orphans
    /// every existing entry of that kind. Every other test compares `as_str`
    /// against itself and would survive such a rename — this one exists to
    /// fail it.
    #[test]
    fn exact_kind_names_are_storage_format() {
        assert_eq!(ExactKind::BlockHash.as_str(), "block_hash");
        assert_eq!(ExactKind::BlockNumber.as_str(), "block_num");
        assert_eq!(ExactKind::TxHash.as_str(), "tx_hash");
    }

    #[test]
    fn tag_record_ord_is_dimension_then_hash_then_slot() {
        let a = TagRecord::new("address", [0xff; 8], 10);
        let b = TagRecord::new("asset", [0x00; 8], 0);
        let c = TagRecord::new("asset", [0x00; 8], 1);
        let d = TagRecord::new("asset", [0x01; 8], 0);

        assert!(a < b, "dimension dominates");
        assert!(b < c, "slot breaks ties within a key hash");
        assert!(c < d, "key hash dominates slot");
    }
}
