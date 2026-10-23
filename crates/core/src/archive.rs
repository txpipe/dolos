use std::{marker::PhantomData, ops::Range};

use thiserror::Error;

use crate::{
    indexes::{ArchiveIndexDelta, ExactRecord, IndexRecord, TagDimension, TagRecord},
    keys::{KeyCodec, KeyError, KeyRange, LogKey, LogRangeOf},
    BlockBody, BlockSlot, BrokenInvariant, ChainPoint, Entity, EntityValue, FixedNamespace,
    Namespace, RawBlock,
};

/// An entity logged in one archive namespace. Its rows are keyed
/// `slot ‖ Key`.
pub trait LogEntity: Entity + FixedNamespace {
    type Key: KeyCodec;
}

/// Log rows of one namespace with their keys decoded, in key order.
pub struct LogIterTyped<A: ArchiveStore, E: LogEntity> {
    inner: A::LogIter,
    _marker: PhantomData<E>,
}

impl<A: ArchiveStore, E: LogEntity> LogIterTyped<A, E> {
    pub fn new(inner: A::LogIter) -> Self {
        Self {
            inner,
            _marker: PhantomData,
        }
    }
}

impl<A: ArchiveStore, E: LogEntity> Iterator for LogIterTyped<A, E> {
    type Item = Result<(BlockSlot, E::Key, E), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped = next.and_then(|(key, value)| {
            let (slot, key) = key.decode::<E::Key>()?;

            E::decode_entity(E::NS, &value)
                .map(|v| (slot, key, v))
                .map_err(|x| ArchiveError::EntityDecodingError(x.to_string()))
        });

        Some(mapped)
    }
}

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("broken invariant")]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("storage error")]
    InternalError(String),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("block decoding error")]
    BlockDecodingError(#[from] pallas::ledger::traverse::Error),

    #[error("entity decoding error")]
    EntityDecodingError(String),

    #[error(transparent)]
    KeyDecodingError(#[from] KeyError),

    #[error("namespace {0} not found")]
    NamespaceNotFound(Namespace),

    /// The operation is part of the trait but the concrete backend does not
    /// implement it.
    ///
    /// The no-op archive answers the index record traversals and the
    /// pre-hashed append with this, so a restore into a store that discards
    /// records fails instead of reporting success.
    #[error("{0} is not supported on this storage backend")]
    Unsupported(&'static str),

    #[error(transparent)]
    IncompatibleStore(#[from] crate::compat::IncompatibleStore),
}

/// Iterator used by backends that do not implement
/// [`ArchiveStore::iter_archive_tags`].
///
/// Never constructed — those backends return [`ArchiveError::Unsupported`],
/// so the alias only exists to satisfy the associated type.
pub type EmptyTagIter = std::iter::Empty<Result<TagRecord, ArchiveError>>;

/// Iterator used by backends that do not implement
/// [`ArchiveStore::iter_exact_records`].
///
/// Never constructed — those backends return [`ArchiveError::Unsupported`],
/// so the alias only exists to satisfy the associated type.
pub type EmptyExactIter = std::iter::Empty<Result<ExactRecord, ArchiveError>>;

pub trait ArchiveWriter: Send + Sync + 'static {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError>;

    /// Write the index entries the blocks of this batch project: their tags
    /// and their exact lookups.
    ///
    /// Separate from [`ArchiveWriter::apply`] on purpose. A restore writes
    /// the `blocks` layer without deltas (the entries arrive pre-hashed in
    /// the `indexes` layer, see [`ArchiveWriter::append_prehashed`]), while
    /// the sync pipeline and the WAL catch-up derive the deltas from the
    /// blocks they are about to write and land both in the same commit.
    fn apply_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError>;

    /// Remove the index entries [`ArchiveWriter::apply_index`] wrote for
    /// these blocks (rollback).
    fn undo_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError>;

    /// Append archive records that already carry their stored key form.
    ///
    /// This is the write mirror of [`ArchiveStore::iter_archive_tags`] and
    /// [`ArchiveStore::iter_exact_records`]: records that came out of one
    /// store go into another byte-for-byte, with no logical key in between
    /// (there is none to recover — see [`crate::indexes::KeyHash`]).
    ///
    /// Records must arrive sorted, since the backing stores are append
    /// oriented.
    ///
    /// ## Chunking
    ///
    /// The argument is an iterator so a restore can pipe its wire decoder
    /// straight in, rather than materializing a slice beside the write
    /// batch's own encoded copy. It is consumed lazily and fully; a backend
    /// never collects it.
    ///
    /// This call is *not* the batch boundary — the writer accumulates until
    /// [`ArchiveWriter::commit`], so an unbounded iterator builds an
    /// unbounded batch. Chunking is the caller's: feed one writer a run of
    /// records, commit it, start the next. The sort order has to hold across
    /// the whole restore, not merely within a chunk.
    ///
    /// Backends that do not implement this return
    /// [`ArchiveError::Unsupported`].
    fn append_prehashed(
        &self,
        records: impl IntoIterator<Item = IndexRecord>,
    ) -> Result<(), ArchiveError>;

    fn write_log(
        &self,
        ns: Namespace,
        key: &LogKey,
        value: &EntityValue,
    ) -> Result<(), ArchiveError>;

    fn write_log_typed<E: LogEntity>(
        &self,
        slot: BlockSlot,
        key: &E::Key,
        entity: &E,
    ) -> Result<(), ArchiveError> {
        let (ns, raw) = E::encode_entity(entity);

        self.write_log(ns, &LogKey::of(slot, key), &raw)
    }

    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError>;

    fn commit(self) -> Result<(), ArchiveError>;
}

/// An iterator that supports efficient skipping without materializing items.
///
/// Storage backends can implement this to skip entries by traversing only index
/// keys, avoiding expensive data reads for items that will be discarded during
/// pagination.
pub trait Skippable {
    /// Advance the iterator forward by `n` entries without reading item data.
    fn skip_forward(&mut self, n: usize);
    /// Advance the iterator backward by `n` entries without reading item data.
    fn skip_backward(&mut self, n: usize);
}

pub trait ArchiveStore: Clone + Send + Sync + 'static {
    type BlockIter<'a>: Iterator<Item = (BlockSlot, BlockBody)>
        + DoubleEndedIterator
        + Skippable
        + 'a;
    type Writer: ArchiveWriter;
    type LogIter: Iterator<Item = Result<(LogKey, EntityValue), ArchiveError>>;
    type EntityValueIter: Iterator<Item = Result<EntityValue, ArchiveError>>;

    /// Iterator type for sparse slot queries.
    type SlotIter: Iterator<Item = Result<BlockSlot, ArchiveError>> + DoubleEndedIterator;

    /// Iterator type for archive tag record traversal.
    type TagIter: Iterator<Item = Result<TagRecord, ArchiveError>>;

    /// Iterator type for exact-match record traversal.
    type ExactIter: Iterator<Item = Result<ExactRecord, ArchiveError>>;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError>;

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError>;

    /// Log rows of `ns` whose key falls in `range`, in key order.
    fn iter_logs(&self, ns: Namespace, range: KeyRange) -> Result<Self::LogIter, ArchiveError>;

    fn read_logs_typed<E: LogEntity>(
        &self,
        keys: &[(BlockSlot, E::Key)],
    ) -> Result<Vec<Option<E>>, ArchiveError> {
        let keys: Vec<LogKey> = keys
            .iter()
            .map(|(slot, key)| LogKey::of(*slot, key))
            .collect();

        let raw = self.read_logs(E::NS, &keys.iter().collect::<Vec<_>>())?;

        let decoded = raw
            .into_iter()
            .map(|x| {
                x.map(|v| {
                    E::decode_entity(E::NS, &v)
                        .map_err(|x| ArchiveError::EntityDecodingError(x.to_string()))
                })
            })
            .map(|x| x.transpose())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(decoded)
    }

    fn read_log_typed<E: LogEntity>(
        &self,
        slot: BlockSlot,
        key: &E::Key,
    ) -> Result<Option<E>, ArchiveError> {
        let raw = self.read_logs(E::NS, &[&LogKey::of(slot, key)])?;

        raw.into_iter()
            .next()
            .unwrap()
            .map(|v| {
                E::decode_entity(E::NS, &v)
                    .map_err(|x| ArchiveError::EntityDecodingError(x.to_string()))
            })
            .transpose()
    }

    fn iter_logs_typed<E: LogEntity>(
        &self,
        range: Option<LogRangeOf<E::Key>>,
    ) -> Result<LogIterTyped<Self, E>, ArchiveError> {
        let range = range.unwrap_or_else(LogRangeOf::all);

        let inner = self.iter_logs(E::NS, range.into_raw())?;

        Ok(LogIterTyped::<Self, E>::new(inner))
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError>;

    /// Every block the archive holds at `slot`, in chain order.
    ///
    /// A slot is not always a unique chain position — a Byron epoch-boundary
    /// block shares its slot with the first main block of the epoch it opens —
    /// and [`Self::get_block_by_slot`] answers with only the block the slot
    /// resolves to. A caller that reached the slot from a block hash needs the
    /// candidates instead. A store that cannot hold two blocks at one slot
    /// inherits the single-block answer.
    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        Ok(self.get_block_by_slot(slot)?.into_iter().collect())
    }
    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError>;

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError>;

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError>;

    /// Prune history outside the retained slot window.
    ///
    /// `max_slots` is the retained window, from the first block to the tip.
    /// `max_prune_slots` caps slot advancement per call.
    /// `max_prune_index_rows` caps the index rows examined per call by
    /// backends that clean indexes apart from their blocks; other backends
    /// ignore it. `true` means block pruning and all active or currently due
    /// index maintenance are complete. A capped index budget can leave
    /// expired entries until a later pass is due.
    /// Repeat capped calls until `true` to finish active or currently due work.
    /// A `None` limit completes that part of the cleanup synchronously.
    fn prune_history(
        &self,
        max_slots: u64,
        max_prune_slots: Option<u64>,
        max_prune_index_rows: Option<u64>,
    ) -> Result<bool, ArchiveError>;

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError>;

    /// Get the slot for a block by its hash (exact lookup).
    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError>;

    /// Get the slot for a block by its number/height (exact lookup).
    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError>;

    /// Get the slot containing a transaction by its hash (exact lookup).
    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError>;

    /// Query slots by tag dimension and key within a slot range.
    ///
    /// This method returns a lazy iterator over the slots that contain data
    /// with the given dimension and key. Forward iteration gives the slots
    /// in ascending order. Reverse iteration gives the slots in descending
    /// order.
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
    ) -> Result<Self::SlotIter, ArchiveError>;

    /// Iterate every archive tag record whose slot falls in `slots`.
    ///
    /// `slots` is **half-open** (`start..end`), unlike
    /// [`ArchiveStore::slots_by_tag`], whose bounds are both inclusive.
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
    /// [`ArchiveStore::iter_exact_records`]) are only mutually consistent if
    /// the store is quiescent across the calls.
    ///
    /// Backends that do not implement this return
    /// [`ArchiveError::Unsupported`].
    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError>;

    /// Iterate every exact-match record whose slot falls in `slots`.
    ///
    /// `slots` is **half-open** (`start..end`), unlike
    /// [`ArchiveStore::slots_by_tag`], whose bounds are both inclusive.
    ///
    /// **Order is part of the contract**: records come out sorted by
    /// `(kind, key)`, kinds in ascending [`crate::indexes::ExactKind::as_str`]
    /// order.
    ///
    /// **Errors are terminal** — same policy as
    /// [`ArchiveStore::iter_archive_tags`].
    ///
    /// Note that the slot is the stored *value* of an exact entry, not part of
    /// its key, so a slot-bounded traversal is a scan of every exact entry in
    /// the store rather than a seek. The iterator is still lazy in memory.
    ///
    /// Backends that do not implement this return
    /// [`ArchiveError::Unsupported`].
    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, ArchiveError>;
}
