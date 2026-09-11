use std::{marker::PhantomData, ops::Range};

use thiserror::Error;

use crate::{
    indexes::{ArchiveIndexDelta, ExactRecord, IndexRecord, TagDimension, TagRecord},
    state::KEY_SIZE,
    BlockBody, BlockSlot, BrokenInvariant, ChainPoint, Entity, EntityKey, EntityValue, Namespace,
    RawBlock,
};

const TEMPORAL_KEY_SIZE: usize = 8;
const LOG_KEY_SIZE: usize = TEMPORAL_KEY_SIZE + KEY_SIZE;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TemporalKey([u8; TEMPORAL_KEY_SIZE]);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct LogKey([u8; LOG_KEY_SIZE]);

impl AsRef<[u8]> for TemporalKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl AsRef<[u8]> for LogKey {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<&[u8]> for LogKey {
    fn from(value: &[u8]) -> Self {
        let mut key = [0u8; LOG_KEY_SIZE];
        let len = value.len().min(LOG_KEY_SIZE);
        key[..len].copy_from_slice(&value[..len]);
        LogKey(key)
    }
}

impl From<Vec<u8>> for LogKey {
    fn from(value: Vec<u8>) -> Self {
        value.as_slice().into()
    }
}

impl From<LogKey> for TemporalKey {
    fn from(value: LogKey) -> Self {
        // Safe to unwrap, we know the length matches.
        let bytes: [u8; TEMPORAL_KEY_SIZE] =
            value.as_ref()[..TEMPORAL_KEY_SIZE].try_into().unwrap();
        TemporalKey(bytes)
    }
}

impl From<LogKey> for EntityKey {
    fn from(value: LogKey) -> Self {
        EntityKey::from(&value.as_ref()[TEMPORAL_KEY_SIZE..])
    }
}

impl From<&ChainPoint> for TemporalKey {
    fn from(value: &ChainPoint) -> Self {
        value.slot().into()
    }
}

impl From<u64> for TemporalKey {
    fn from(value: u64) -> Self {
        TemporalKey(value.to_be_bytes())
    }
}

impl From<(TemporalKey, EntityKey)> for LogKey {
    fn from((temporal, entity): (TemporalKey, EntityKey)) -> Self {
        // Safe to unwrap, we know the length matches.
        let bytes: [u8; LOG_KEY_SIZE] = [temporal.as_ref(), entity.as_ref()]
            .concat()
            .try_into()
            .unwrap();
        Self(bytes)
    }
}

impl From<TemporalKey> for LogKey {
    fn from(value: TemporalKey) -> Self {
        // Safe to unwrap, we know the length matches. We extend the key with 0 to match
        // length.
        let bytes: [u8; LOG_KEY_SIZE] = [value.as_ref(), &[0; KEY_SIZE]]
            .concat()
            .try_into()
            .unwrap();
        Self(bytes)
    }
}

impl From<&ChainPoint> for LogKey {
    fn from(value: &ChainPoint) -> Self {
        let temporal: TemporalKey = value.into();
        temporal.into()
    }
}

impl LogKey {
    pub fn full_range() -> Range<LogKey> {
        Range {
            start: LogKey([0u8; LOG_KEY_SIZE]),
            end: LogKey([255u8; LOG_KEY_SIZE]),
        }
    }
}

pub struct LogIterTyped<A: ArchiveStore, E: Entity> {
    inner: A::LogIter,
    ns: Namespace,
    _marker: PhantomData<E>,
}

impl<A: ArchiveStore, E: Entity> LogIterTyped<A, E> {
    pub fn new(inner: A::LogIter, ns: Namespace) -> Self {
        Self {
            inner,
            ns,
            _marker: PhantomData,
        }
    }
}

impl<A: ArchiveStore, E: Entity> Iterator for LogIterTyped<A, E> {
    type Item = Result<(LogKey, E), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        let next = self.inner.next()?;

        let mapped = next.and_then(|(key, value)| {
            E::decode_entity(self.ns, &value)
                .map(|v| (key, v))
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

    fn write_log_typed<E: Entity>(&self, key: &LogKey, entity: &E) -> Result<(), ArchiveError> {
        let (ns, raw) = E::encode_entity(entity);

        self.write_log(ns, key, &raw)
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

    fn iter_logs(&self, ns: Namespace, range: Range<LogKey>)
        -> Result<Self::LogIter, ArchiveError>;

    fn read_logs_typed<E: Entity>(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<E>>, ArchiveError> {
        let raw = self.read_logs(ns, keys)?;

        let decoded = raw
            .into_iter()
            .map(|x| {
                x.map(|v| {
                    E::decode_entity(ns, &v)
                        .map_err(|x| ArchiveError::EntityDecodingError(x.to_string()))
                })
            })
            .map(|x| x.transpose())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(decoded)
    }

    fn read_log_typed<E: Entity>(
        &self,
        ns: Namespace,
        key: &LogKey,
    ) -> Result<Option<E>, ArchiveError> {
        let raw = self.read_logs_typed(ns, &[key])?;

        let first = raw.into_iter().next().unwrap();

        Ok(first)
    }

    fn iter_logs_typed<E: Entity>(
        &self,
        ns: Namespace,
        range: Option<Range<LogKey>>,
    ) -> Result<LogIterTyped<Self, E>, ArchiveError> {
        let range = range.unwrap_or_else(LogKey::full_range);

        let inner = self.iter_logs(ns, range)?;

        Ok(LogIterTyped::<Self, E>::new(inner, ns))
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

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError>;

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

    /// Read one page of the stake address log: the addresses seen under the
    /// stake credential, ordered by first on-chain appearance.
    ///
    /// `offset` and `limit` window the ordered list; `reverse` reads the
    /// exact reverse of it. Returns `None` when the log is not authoritative
    /// on this store: the backend does not maintain it, or the store was not
    /// synced from genesis with the log in place (a stele restore, or a store
    /// that predates the log). Callers fall back to an archive scan then.
    ///
    /// Entries come from `ArchiveIndexDelta::stake_addresses` through
    /// [`ArchiveWriter::apply_index`]. [`ArchiveWriter::undo_index`] removes a
    /// pair only when the undone block is its stored first appearance.
    fn addresses_by_stake_log(
        &self,
        stake: &[u8],
        offset: usize,
        limit: usize,
        reverse: bool,
    ) -> Result<Option<Vec<Vec<u8>>>, ArchiveError>;

    /// Declare the stake address log complete from genesis.
    ///
    /// Genesis bootstrap calls this once on a fresh store, before it writes
    /// the state cursor that marks genesis as done. Until it runs,
    /// [`ArchiveStore::addresses_by_stake_log`] answers `None`. Backends that
    /// do not maintain the log treat this as a no-op.
    fn mark_stake_log_ready(&self) -> Result<(), ArchiveError>;

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
