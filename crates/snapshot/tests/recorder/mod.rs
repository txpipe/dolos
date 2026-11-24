//! The real store pair, wrapped to log every commit and persist in order and,
//! when asked, to stop the restore at one of them.
//!
//! Each logged call carries the progress file as it stood when the call
//! returned. A journal made with [`Journal::crashing_at`] refuses its chosen
//! call and every one after it without reaching the store, which is a
//! process that died there: what committed before stays, and the batch in
//! flight is dropped. One made with [`Journal::failing_persist`] fails a
//! single persist and nothing else, as an fsync that returned an IO error,
//! and one made with [`Journal::failing_settle`] fails the settle's join.

// Each integration test binary compiles this module in full, so the parts one
// binary does not reach look dead to it. They are not.
#![allow(dead_code)]

use std::{
    ops::Range,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

use dolos_core::{
    ArchiveError, ArchiveIndexDelta, ArchiveIngest, ArchiveStore, ArchiveWriter, BatchIngest,
    BlockBody, BlockSlot, ChainPoint, EntityKey, EntityKind, EntityValue, IndexRecord, KeyRange,
    LogKey, Namespace, RawBlock, Settling, StateError, StateStore, StateWriter, TagDimension,
    TagEntry, TagOrder, TagStart, TxoRef, UtxoIndexDelta, UtxoMap, UtxoSet, UtxoSetDelta,
};
use stelae::{plan::RestoreProgress, Digest};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Op {
    ArchiveCommit,
    /// A finished [`ArchiveIngest`]; refused, the ingest is dropped with its
    /// runs unfinished.
    ArchiveIngest,
    ArchivePersist,
    /// The archive's settle, run to its end inside the call so its entry
    /// keeps its place among the others; refused, it never starts.
    ArchiveSettle,
    StateCommit,
    StatePersist,
}

/// One returned call, and the layers the progress file recorded at that
/// moment (`None`: there was no progress file).
#[derive(Debug)]
pub struct Entry {
    pub op: Op,
    pub recorded: Option<Vec<Digest>>,
}

#[derive(Clone)]
pub struct Journal {
    progress: PathBuf,
    entries: Arc<Mutex<Vec<Entry>>>,
    /// The commit or persist, counted from one across both stores, that
    /// fails, as every later one does: the process died there.
    crash_at: Option<usize>,
    calls: Arc<AtomicUsize>,
    /// The persist of this op, counted from one, that fails alone.
    fail_persist: Option<(Op, usize)>,
    archive_persists: Arc<AtomicUsize>,
    state_persists: Arc<AtomicUsize>,
    /// Whether the archive writes ingested layers through its own writer,
    /// one commit per chunk, rather than the inner store's ingest.
    batched: bool,
    /// Whether the settle's join reports an error after the settle ran.
    fail_settle: bool,
}

impl Journal {
    pub fn new(progress: PathBuf) -> Self {
        Self {
            progress,
            entries: Arc::default(),
            crash_at: None,
            calls: Arc::default(),
            fail_persist: None,
            archive_persists: Arc::default(),
            state_persists: Arc::default(),
            batched: false,
            fail_settle: false,
        }
    }

    /// The same journal, over an archive that writes every layer in batches.
    pub fn batched(self) -> Self {
        Self {
            batched: true,
            ..self
        }
    }

    /// A journal whose `nth` persist of `op` (counted from one) fails without
    /// reaching the store, while every commit and every other persist goes
    /// through.
    pub fn failing_persist(progress: PathBuf, op: Op, nth: usize) -> Self {
        Self {
            fail_persist: Some((op, nth)),
            ..Self::new(progress)
        }
    }

    /// A journal whose settle runs, then fails when the restore joins it, as
    /// a major compaction that ran out of space.
    pub fn failing_settle(progress: PathBuf) -> Self {
        Self {
            fail_settle: true,
            ..Self::new(progress)
        }
    }

    /// A journal whose `call`-th commit or persist fails, and every one after
    /// it, without reaching the store.
    pub fn crashing_at(progress: PathBuf, call: usize) -> Self {
        Self {
            crash_at: Some(call),
            ..Self::new(progress)
        }
    }

    /// Count a commit or persist, and refuse it once the process has died.
    fn gate(&self) -> Result<(), String> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst) + 1;

        match self.crash_at {
            Some(crash_at) if call >= crash_at => Err(format!("stopped at call {crash_at}")),
            _ => Ok(()),
        }
    }

    /// Count a persist of `op`, and refuse it if it is the one chosen to fail.
    fn gate_persist(&self, op: Op) -> Result<(), String> {
        self.gate()?;

        let counter = match op {
            Op::ArchivePersist => &self.archive_persists,
            Op::StatePersist => &self.state_persists,
            Op::ArchiveCommit | Op::ArchiveIngest | Op::ArchiveSettle | Op::StateCommit => {
                unreachable!("{op:?} is not a persist")
            }
        };
        let nth = counter.fetch_add(1, Ordering::SeqCst) + 1;

        match self.fail_persist {
            Some(chosen) if chosen == (op, nth) => Err(format!("{op:?} {nth} failed")),
            _ => Ok(()),
        }
    }

    fn log(&self, op: Op) {
        let recorded = RestoreProgress::load(&self.progress)
            .unwrap()
            .map(|progress| progress.completed.into_iter().collect());

        self.entries.lock().unwrap().push(Entry { op, recorded });
    }

    pub fn take(&self) -> Vec<Entry> {
        std::mem::take(&mut self.entries.lock().unwrap())
    }
}

#[derive(Clone)]
pub struct RecordedArchive<A> {
    pub inner: A,
    pub journal: Journal,
}

pub struct RecordedArchiveWriter<W> {
    inner: W,
    journal: Journal,
}

impl<W: ArchiveWriter> ArchiveWriter for RecordedArchiveWriter<W> {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError> {
        self.inner.apply(point, block)
    }

    fn apply_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        self.inner.apply_index(deltas)
    }

    fn undo_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        self.inner.undo_index(deltas)
    }

    fn append_prehashed(
        &self,
        records: impl IntoIterator<Item = IndexRecord>,
    ) -> Result<(), ArchiveError> {
        self.inner.append_prehashed(records)
    }

    fn write_log(
        &self,
        ns: Namespace,
        key: &LogKey,
        value: &EntityValue,
    ) -> Result<(), ArchiveError> {
        self.inner.write_log(ns, key, value)
    }

    fn delete_log(&self, ns: Namespace, key: &LogKey) -> Result<(), ArchiveError> {
        self.inner.delete_log(ns, key)
    }

    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError> {
        self.inner.undo(point)
    }

    fn write_entity(
        &self,
        kind: EntityKind,
        key: &[u8],
        slot: BlockSlot,
    ) -> Result<(), ArchiveError> {
        self.inner.write_entity(kind, key, slot)
    }

    fn commit(self) -> Result<(), ArchiveError> {
        self.journal.gate().map_err(ArchiveError::InternalError)?;
        self.inner.commit()?;
        self.journal.log(Op::ArchiveCommit);

        Ok(())
    }
}

pub struct RecordedIngest<'a> {
    inner: Box<dyn ArchiveIngest + 'a>,
    journal: Journal,
}

impl ArchiveIngest for RecordedIngest<'_> {
    fn blocks(&mut self, blocks: Vec<(ChainPoint, RawBlock)>) -> Result<(), ArchiveError> {
        self.inner.blocks(blocks)
    }

    fn logs(
        &mut self,
        ns: Namespace,
        rows: Vec<(LogKey, EntityValue)>,
    ) -> Result<(), ArchiveError> {
        self.inner.logs(ns, rows)
    }

    fn finish(self: Box<Self>) -> Result<(), ArchiveError> {
        self.journal.gate().map_err(ArchiveError::InternalError)?;
        self.inner.finish()?;
        self.journal.log(Op::ArchiveIngest);

        Ok(())
    }
}

impl<A: ArchiveStore> ArchiveStore for RecordedArchive<A> {
    type BlockIter<'a> = A::BlockIter<'a>;
    type Writer = RecordedArchiveWriter<A::Writer>;
    type LogIter = A::LogIter;
    type EntityValueIter = A::EntityValueIter;
    type SlotIter = A::SlotIter;
    type TagIter = A::TagIter;
    type ExactIter = A::ExactIter;
    type EntityRecordIter = A::EntityRecordIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        Ok(RecordedArchiveWriter {
            inner: self.inner.start_writer()?,
            journal: self.journal.clone(),
        })
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError> {
        self.inner.read_logs(ns, keys)
    }

    fn iter_logs(&self, ns: Namespace, range: KeyRange) -> Result<Self::LogIter, ArchiveError> {
        self.inner.iter_logs(ns, range)
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        self.inner.get_block_by_slot(slot)
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        self.inner.get_range(from, to)
    }

    fn blocks_after<'a>(
        &self,
        point: &ChainPoint,
    ) -> Result<Option<Self::BlockIter<'a>>, ArchiveError> {
        self.inner.blocks_after(point)
    }

    fn blocks_before<'a>(
        &self,
        point: &ChainPoint,
    ) -> Result<Option<Self::BlockIter<'a>>, ArchiveError> {
        self.inner.blocks_before(point)
    }

    fn block_after(
        &self,
        point: &ChainPoint,
    ) -> Result<Option<Option<(BlockSlot, BlockBody)>>, ArchiveError> {
        self.inner.block_after(point)
    }

    fn block_before(
        &self,
        point: &ChainPoint,
    ) -> Result<Option<Option<(BlockSlot, BlockBody)>>, ArchiveError> {
        self.inner.block_before(point)
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        self.inner.find_intersect(intersect)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        self.inner.get_tip()
    }

    fn prune_history(
        &self,
        max_slots: u64,
        max_prune_slots: Option<u64>,
        max_prune_index_rows: Option<u64>,
    ) -> Result<bool, ArchiveError> {
        self.inner
            .prune_history(max_slots, max_prune_slots, max_prune_index_rows)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        self.inner.truncate_front(after)
    }

    fn persist(&self) -> Result<(), ArchiveError> {
        self.journal
            .gate_persist(Op::ArchivePersist)
            .map_err(ArchiveError::InternalError)?;
        self.inner.persist()?;
        self.journal.log(Op::ArchivePersist);

        Ok(())
    }

    fn start_ingest(&self) -> Result<Box<dyn ArchiveIngest + '_>, ArchiveError> {
        if self.journal.batched {
            return Ok(Box::new(BatchIngest::new(self)));
        }

        Ok(Box::new(RecordedIngest {
            inner: self.inner.start_ingest()?,
            journal: self.journal.clone(),
        }))
    }

    fn settle_ingested(&self) -> Result<Settling, ArchiveError> {
        self.journal.gate().map_err(ArchiveError::InternalError)?;
        self.inner.settle_ingested()?.join()?;
        self.journal.log(Op::ArchiveSettle);

        // What the restore joins carries only the outcome.
        let fail = self.journal.fail_settle;
        let outcome = std::thread::spawn(move || match fail {
            true => Err(ArchiveError::InternalError("the settle failed".into())),
            false => Ok(()),
        });

        Ok(Settling::of(vec![outcome]))
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        self.inner.slot_by_block_hash(hash)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        self.inner.slot_by_block_number(number)
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        self.inner.slot_by_tx_hash(hash)
    }

    fn entity_slots(
        &self,
        kind: EntityKind,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<BlockSlot>>, ArchiveError> {
        self.inner.entity_slots(kind, keys)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        self.inner.slots_by_tag(dimension, key, start, end)
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
        self.inner.iter_archive_tags(dimensions, slots)
    }

    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, ArchiveError> {
        self.inner.iter_exact_records(slots)
    }

    fn iter_entity_records(
        &self,
        kinds: &[EntityKind],
        slots: Range<BlockSlot>,
    ) -> Result<Self::EntityRecordIter, ArchiveError> {
        self.inner.iter_entity_records(kinds, slots)
    }
}

#[derive(Clone)]
pub struct RecordedState<S> {
    pub inner: S,
    pub journal: Journal,
}

pub struct RecordedStateWriter<W> {
    inner: W,
    journal: Journal,
}

impl<W: StateWriter> StateWriter for RecordedStateWriter<W> {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError> {
        self.inner.set_cursor(cursor)
    }

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError> {
        self.inner.write_entity(ns, key, value)
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        self.inner.delete_entity(ns, key)
    }

    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), StateError> {
        self.inner.apply_utxoset(delta)
    }

    fn apply_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        self.inner.apply_utxo_tags(delta)
    }

    fn undo_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        self.inner.undo_utxo_tags(delta)
    }

    fn commit(self) -> Result<(), StateError> {
        self.journal
            .gate()
            .map_err(StateError::InternalStoreError)?;
        self.inner.commit()?;
        self.journal.log(Op::StateCommit);

        Ok(())
    }
}

impl<S: StateStore> StateStore for RecordedState<S> {
    type EntityIter = S::EntityIter;
    type EntityValueIter = S::EntityValueIter;
    type UtxoIter = S::UtxoIter;
    type Writer = RecordedStateWriter<S::Writer>;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        self.inner.read_cursor()
    }

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError> {
        self.inner.read_entities(ns, keys)
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        Ok(RecordedStateWriter {
            inner: self.inner.start_writer()?,
            journal: self.journal.clone(),
        })
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: KeyRange,
    ) -> Result<Self::EntityIter, StateError> {
        self.inner.iter_entities(ns, range)
    }

    fn iter_entity_values(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        self.inner.iter_entity_values(ns, key)
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        self.inner.get_utxos(refs)
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, StateError> {
        self.inner.utxos_by_tag(dimension, key)
    }

    fn utxos_by_tag_page(
        &self,
        dimension: TagDimension,
        key: &[u8],
        order: TagOrder,
        start: TagStart,
        count: usize,
    ) -> Result<Vec<TagEntry>, StateError> {
        self.inner
            .utxos_by_tag_page(dimension, key, order, start, count)
    }

    fn utxos_with_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        entries: Vec<TagEntry>,
    ) -> Result<Vec<TagEntry>, StateError> {
        self.inner.utxos_with_tag(dimension, key, entries)
    }

    fn iter_utxos(&self) -> Result<Self::UtxoIter, StateError> {
        self.inner.iter_utxos()
    }

    fn persist(&self) -> Result<(), StateError> {
        self.journal
            .gate_persist(Op::StatePersist)
            .map_err(StateError::InternalStoreError)?;
        self.inner.persist()?;
        self.journal.log(Op::StatePersist);

        Ok(())
    }
}

/// One digest over everything a restore writes: the cursor, the entities of
/// every namespace, the UTxO set and its live tags' pages, the blocks, the
/// logs, the archive tags, the exact records and the entity index.
///
/// Two store sets with the same digest hold the same node, so a resumed
/// restore is held against an uninterrupted one in one comparison.
pub fn contents_digest<A: ArchiveStore, S: StateStore>(archive: &A, state: &S) -> Digest {
    use dolos_cardano::indexes::{archive_dimensions, utxo_index_delta_from_utxo_delta};
    use dolos_snapshot::{ENTITY_INDEX_KINDS, LOG_NAMESPACES, NAMESPACES, UTXOS};

    let mut seen = Vec::new();
    let mut add = |what: &str, value: String| {
        seen.push(format!("{what}: {value}"));
    };

    add("cursor", format!("{:?}", state.read_cursor().unwrap()));

    for ns in NAMESPACES.iter().filter(|ns| **ns != UTXOS) {
        let entities: Vec<_> = state
            .iter_entities(ns, KeyRange::all())
            .unwrap()
            .map(Result::unwrap)
            .collect();

        add(ns, format!("{entities:?}"));
    }

    let utxos: std::collections::BTreeMap<_, _> =
        state.iter_utxos().unwrap().map(Result::unwrap).collect();

    let produced = utxos
        .iter()
        .map(|(txo, value)| (txo.clone(), Arc::new(value.clone())))
        .collect();

    let tags = utxo_index_delta_from_utxo_delta(&UtxoSetDelta {
        produced_utxo: produced,
        ..Default::default()
    });

    for (_, _, tags) in &tags.produced {
        for tag in tags {
            let page = state
                .utxos_by_tag_page(
                    tag.dimension,
                    &tag.key,
                    TagOrder::Asc,
                    TagStart::Skip(0),
                    usize::MAX,
                )
                .unwrap();

            add(tag.dimension, format!("{:?} {page:?}", tag.key));
        }
    }

    add("utxos", format!("{utxos:?}"));

    let blocks: Vec<_> = archive.get_range(None, None).unwrap().collect();
    add("blocks", format!("{blocks:?}"));

    for ns in LOG_NAMESPACES {
        let logs: Vec<_> = archive
            .iter_logs(ns, KeyRange::all())
            .unwrap()
            .map(Result::unwrap)
            .collect();

        add(ns, format!("{logs:?}"));
    }

    let mut tags: Vec<_> = archive
        .iter_archive_tags(&archive_dimensions::ALL, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    tags.sort();
    add("archive tags", format!("{tags:?}"));

    let mut exact: Vec<_> = archive
        .iter_exact_records(0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    exact.sort_by_key(|record| (record.kind, record.key().to_vec()));
    add("exact records", format!("{exact:?}"));

    let entity_index: Vec<_> = archive
        .iter_entity_records(&ENTITY_INDEX_KINDS, 0..u64::MAX)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    add("entity index", format!("{entity_index:?}"));

    // Each line is labelled, so their order carries nothing, and the tag
    // delta's isn't stable.
    seen.sort();

    Digest::compute(seen.join("\n"))
}
