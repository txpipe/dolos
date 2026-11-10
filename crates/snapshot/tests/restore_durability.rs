//! A restore records a layer only once what it wrote is durable.
//!
//! The stores commit to buffered journals, so a commit that returned survives
//! a killed process but can be lost to a power loss or an OS crash. The
//! progress file must therefore never name a layer whose writes were not yet
//! synced, or a resume after a power loss would skip a layer the stores no
//! longer hold.
//!
//! The stores here are the real fjall pair, each wrapped by a recorder that
//! logs every commit and persist in order, with the progress file as it stood
//! when the call returned. The assertions read the order off that log.

mod node;

use std::{
    ops::Range,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use dolos_core::{
    ArchiveError, ArchiveIndexDelta, ArchiveStore, ArchiveWriter, BlockBody, BlockSlot, ChainPoint,
    EntityKey, EntityKind, EntityValue, IndexRecord, KeyRange, LogKey, Namespace, RawBlock,
    StateError, StateStore, StateWriter, TagDimension, TagEntry, TagOrder, TagStart, TxoRef,
    UtxoIndexDelta, UtxoMap, UtxoSet, UtxoSetDelta,
};
use dolos_snapshot::{
    restore::{self, default_budget, progress_path_in, Checkpoint, Target},
    ENTITY_KINDS,
};
use dolos_testing::toy_domain::{FjallStores, ToyDomain};
use node::{export_to, harness, Blank};
use stelae::{dir::SteleDir, plan::RestoreProgress, progress::Observer, Digest, SteleReader as _};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Op {
    ArchiveCommit,
    ArchivePersist,
    StateCommit,
    StatePersist,
}

/// One returned call, and the layers the progress file recorded at that
/// moment (`None`: there was no progress file).
#[derive(Debug)]
struct Entry {
    op: Op,
    recorded: Option<Vec<Digest>>,
}

#[derive(Clone)]
struct Journal {
    progress: PathBuf,
    entries: Arc<Mutex<Vec<Entry>>>,
}

impl Journal {
    fn new(progress: PathBuf) -> Self {
        Self {
            progress,
            entries: Arc::default(),
        }
    }

    fn log(&self, op: Op) {
        let recorded = RestoreProgress::load(&self.progress)
            .unwrap()
            .map(|progress| progress.completed.into_iter().collect());

        self.entries.lock().unwrap().push(Entry { op, recorded });
    }

    fn take(&self) -> Vec<Entry> {
        std::mem::take(&mut self.entries.lock().unwrap())
    }
}

#[derive(Clone)]
struct RecordedArchive<A> {
    inner: A,
    journal: Journal,
}

struct RecordedArchiveWriter<W> {
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
        self.inner.commit()?;
        self.journal.log(Op::ArchiveCommit);

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
        self.inner.persist()?;
        self.journal.log(Op::ArchivePersist);

        Ok(())
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
struct RecordedState<S> {
    inner: S,
    journal: Journal,
}

struct RecordedStateWriter<W> {
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
        self.inner.persist()?;
        self.journal.log(Op::StatePersist);

        Ok(())
    }
}

/// Restore `root` into the recorded fjall pair, checkpointing into `storage`,
/// and return the journal of the run with the epoch layers it checkpoints.
fn recorded_restore(root: &Path, storage: &Path, magic: u64) -> (Vec<Entry>, Vec<Digest>) {
    let blank = Blank::<FjallStores>::open();
    let journal = Journal::new(progress_path_in(storage));

    let archive = RecordedArchive {
        inner: blank.archive.clone(),
        journal: journal.clone(),
    };
    let state = RecordedState {
        inner: blank.state().clone(),
        journal: journal.clone(),
    };

    let stele = SteleDir::open(root).unwrap();
    let identity = stele.read_inscription().unwrap().digest().unwrap();
    let plan = restore::plan(&stele, magic, None).unwrap();
    let index = stele.blob_index().unwrap();
    let mut checkpoint = Checkpoint::open(progress_path_in(storage), identity, false).unwrap();

    let epoch_layers: Vec<Digest> = plan
        .layers()
        .map(|layer| layer.diff_id)
        .filter(|diff_id| !plan.tip_layers().any(|tip| tip.diff_id == *diff_id))
        .collect();

    restore::restore(
        &stele,
        &index,
        &plan,
        Target::new(&archive, &state),
        default_budget(),
        &mut checkpoint,
        &Observer::silent(),
    )
    .unwrap();

    (journal.take(), epoch_layers)
}

/// Entries as a seal writes them, straight into `domain`'s archive, so the
/// stele carries entity layers beside the blocks, logs and indexes.
fn seed_entities(domain: &ToyDomain<FjallStores>) {
    let writer = dolos_core::Domain::archive(domain).start_writer().unwrap();

    for (byte, (slot, (_, kind))) in [100, 200].into_iter().zip(&ENTITY_KINDS).enumerate() {
        let key = vec![byte as u8 + 1; kind.key_len()];
        writer.write_entity(*kind, &key, slot).unwrap();
    }

    writer.commit().unwrap();
}

/// Ruling 12: each epoch layer's `diffId` first appears in the progress file
/// only after an archive persist that returned after that layer's last archive
/// commit; and the state and archive are persisted after the cursor commit and
/// before the progress file is removed.
#[test]
fn a_layer_is_recorded_only_after_its_writes_are_persisted() {
    let domain: ToyDomain<FjallStores> = harness();
    let magic = u64::from(dolos_core::Domain::genesis(&domain).network_magic());
    seed_entities(&domain);

    let stele = tempfile::tempdir().unwrap();
    let inscription = export_to(stele.path(), &domain);
    let storage = tempfile::tempdir().unwrap();

    assert!(
        inscription
            .layers
            .iter()
            .any(|layer| layer.kind.starts_with("entity-")),
        "the fixture needs an entity layer, or one layer family goes unchecked"
    );

    let (entries, epoch_layers) = recorded_restore(stele.path(), storage.path(), magic);

    assert!(
        epoch_layers.len() >= 2,
        "the fixture needs several epoch layers for the order to mean anything"
    );

    for layer in &epoch_layers {
        let recorded_at = entries
            .iter()
            .position(|entry| {
                entry
                    .recorded
                    .as_ref()
                    .is_some_and(|set| set.contains(layer))
            })
            .unwrap_or_else(|| panic!("layer {layer:?} was never recorded"));

        let last_commit = entries[..recorded_at]
            .iter()
            .rposition(|entry| entry.op == Op::ArchiveCommit)
            .unwrap_or_else(|| panic!("layer {layer:?} was recorded before any archive commit"));

        assert!(
            entries[last_commit + 1..recorded_at]
                .iter()
                .any(|entry| entry.op == Op::ArchivePersist),
            "layer {layer:?} was recorded without an archive persist after its last commit: {:?}",
            &entries[last_commit..=recorded_at],
        );
    }

    let cursor_commit = entries
        .iter()
        .rposition(|entry| entry.op == Op::StateCommit)
        .expect("the restore committed no state");

    for op in [Op::StatePersist, Op::ArchivePersist] {
        assert!(
            entries[cursor_commit + 1..]
                .iter()
                .any(|entry| entry.op == op && entry.recorded.is_some()),
            "no {op:?} after the cursor commit while the progress file was still there: {:?}",
            &entries[cursor_commit..],
        );
    }

    assert_eq!(
        RestoreProgress::load(&progress_path_in(storage.path())).unwrap(),
        None,
        "a finished restore left its progress file behind"
    );
}
