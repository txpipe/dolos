use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc,
};

use dolos_core::{
    builtin::{MemoryArchiveStore, MemoryStateStore},
    ArchiveError, ArchiveIndexDelta, ArchiveStore, ArchiveWriter, BlockBody, BlockSlot, ChainPoint,
    Domain, DomainError, IndexRecord, LogEntry, LogKey, LogValue, Namespace, RawBlock, StateError,
    StateStore, StateWriter, TagDimension, TipEvent, UtxoIndexDelta, WalError, WalStore,
};

use crate::toy_domain::{Mempool, TipSubscription, ToyDomain};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TestFault {
    #[default]
    None,
    StateStoreError,
    ArchiveStoreError,
    /// Only [`StateWriter::apply_utxo_tags`] fails; every other state call
    /// succeeds.
    ///
    /// The narrow one, for a caller that has to reach a specific write and
    /// would never get there if opening the store failed too — a stele restore
    /// above all, which writes tags alongside each incoming UTxO batch.
    StateTagsApplyError,
    WalStoreError,
    GenesisError,
    /// The process dies after this many store commits: the first `n` state or
    /// archive commits land, and every later one fails without writing.
    ///
    /// Counted across both stores of one [`FaultyToyDomain`], so a test can
    /// stop a work unit between the two commits of a phase, whichever store
    /// that phase commits first.
    CrashAfterCommits(u32),
}

/// Whether a commit under `fault` comes after the crash, counting it if not.
fn past_crash(fault: TestFault, commits: &AtomicU32) -> bool {
    let TestFault::CrashAfterCommits(limit) = fault else {
        return false;
    };

    commits.fetch_add(1, Ordering::SeqCst) >= limit
}

#[derive(Clone)]
pub struct FaultyToyDomain {
    inner: ToyDomain,
    genesis_override: Option<Arc<dolos_core::Genesis>>,
    state: FaultyStateStore,
    archive: FaultyArchiveStore,
    wal: FaultyWalStore,
}

impl FaultyToyDomain {
    pub fn new(inner: ToyDomain, fault: TestFault) -> Self {
        let commits = Arc::new(AtomicU32::new(0));
        let state = FaultyStateStore::counting(inner.state().clone(), fault, commits.clone());
        let archive = FaultyArchiveStore::counting(inner.archive().clone(), fault, commits);
        let wal = FaultyWalStore::new(inner.wal().clone(), fault);
        let genesis_override = match fault {
            TestFault::GenesisError => {
                let mut genesis = (*inner.genesis()).clone();
                genesis.shelley.system_start = Some("invalid-date".to_string());
                Some(Arc::new(genesis))
            }
            _ => None,
        };
        Self {
            inner,
            genesis_override,
            state,
            archive,
            wal,
        }
    }
}

#[derive(Clone)]
pub struct FaultyStateStore {
    inner: MemoryStateStore,
    fault: TestFault,
    commits: Arc<AtomicU32>,
}

impl FaultyStateStore {
    pub fn new(inner: MemoryStateStore, fault: TestFault) -> Self {
        Self::counting(inner, fault, Default::default())
    }

    fn counting(inner: MemoryStateStore, fault: TestFault, commits: Arc<AtomicU32>) -> Self {
        Self {
            inner,
            fault,
            commits,
        }
    }

    fn should_fault_ns(&self, _ns: dolos_core::Namespace) -> bool {
        self.should_fault()
    }

    fn should_fault(&self) -> bool {
        matches!(self.fault, TestFault::StateStoreError)
    }

    fn fault_err(&self) -> StateError {
        StateError::InternalStoreError("fault injection: state store".into())
    }
}

impl StateStore for FaultyStateStore {
    type EntityIter = <MemoryStateStore as StateStore>::EntityIter;
    type EntityValueIter = <MemoryStateStore as StateStore>::EntityValueIter;
    type UtxoIter = <MemoryStateStore as StateStore>::UtxoIter;
    type Writer = FaultyStateWriter;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.read_cursor()
    }

    fn read_entities(
        &self,
        ns: dolos_core::Namespace,
        keys: &[&dolos_core::EntityKey],
    ) -> Result<Vec<Option<dolos_core::EntityValue>>, StateError> {
        if self.should_fault_ns(ns) {
            return Err(self.fault_err());
        }
        self.inner.read_entities(ns, keys)
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        Ok(FaultyStateWriter {
            inner: self.inner.start_writer()?,
            fault: self.fault,
            commits: self.commits.clone(),
        })
    }

    fn iter_entities(
        &self,
        ns: dolos_core::Namespace,
        range: std::ops::Range<dolos_core::EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        if self.should_fault_ns(ns) {
            return Err(self.fault_err());
        }
        self.inner.iter_entities(ns, range)
    }

    fn iter_entity_values(
        &self,
        ns: dolos_core::Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        if self.should_fault_ns(ns) {
            return Err(self.fault_err());
        }
        self.inner.iter_entity_values(ns, key)
    }

    fn get_utxos(&self, refs: Vec<dolos_core::TxoRef>) -> Result<dolos_core::UtxoMap, StateError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.get_utxos(refs)
    }

    fn utxos_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
    ) -> Result<dolos_core::UtxoSet, StateError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.utxos_by_tag(dimension, key)
    }

    fn iter_utxos(&self) -> Result<Self::UtxoIter, StateError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.iter_utxos()
    }
}

pub struct FaultyStateWriter {
    inner: <MemoryStateStore as StateStore>::Writer,
    fault: TestFault,
    commits: Arc<AtomicU32>,
}

impl StateWriter for FaultyStateWriter {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError> {
        self.inner.set_cursor(cursor)
    }

    fn write_entity(
        &self,
        ns: Namespace,
        key: &dolos_core::EntityKey,
        value: &dolos_core::EntityValue,
    ) -> Result<(), StateError> {
        self.inner.write_entity(ns, key, value)
    }

    fn delete_entity(&self, ns: Namespace, key: &dolos_core::EntityKey) -> Result<(), StateError> {
        self.inner.delete_entity(ns, key)
    }

    fn apply_utxoset(&self, delta: &dolos_core::UtxoSetDelta) -> Result<(), StateError> {
        self.inner.apply_utxoset(delta)
    }

    fn apply_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        if matches!(self.fault, TestFault::StateTagsApplyError) {
            return Err(StateError::InternalStoreError(
                "fault injection: state tags apply".into(),
            ));
        }
        self.inner.apply_utxo_tags(delta)
    }

    fn undo_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        self.inner.undo_utxo_tags(delta)
    }

    fn commit(self) -> Result<(), StateError> {
        if past_crash(self.fault, &self.commits) {
            return Err(StateError::InternalStoreError(
                "fault injection: crashed before state commit".into(),
            ));
        }
        self.inner.commit()
    }
}

#[derive(Clone)]
pub struct FaultyArchiveStore {
    inner: MemoryArchiveStore,
    fault: TestFault,
    commits: Arc<AtomicU32>,
}

impl FaultyArchiveStore {
    pub fn new(inner: MemoryArchiveStore, fault: TestFault) -> Self {
        Self::counting(inner, fault, Default::default())
    }

    fn counting(inner: MemoryArchiveStore, fault: TestFault, commits: Arc<AtomicU32>) -> Self {
        Self {
            inner,
            fault,
            commits,
        }
    }

    fn should_fault(&self) -> bool {
        matches!(self.fault, TestFault::ArchiveStoreError)
    }

    fn fault_err(&self) -> ArchiveError {
        ArchiveError::InternalError("fault injection: archive store".into())
    }
}

impl ArchiveStore for FaultyArchiveStore {
    type BlockIter<'a> = <MemoryArchiveStore as ArchiveStore>::BlockIter<'a>;
    type Writer = FaultyArchiveWriter;
    type LogIter = <MemoryArchiveStore as ArchiveStore>::LogIter;
    type EntityValueIter = <MemoryArchiveStore as ArchiveStore>::EntityValueIter;
    type SlotIter = <MemoryArchiveStore as ArchiveStore>::SlotIter;
    type TagIter = <MemoryArchiveStore as ArchiveStore>::TagIter;
    type ExactIter = <MemoryArchiveStore as ArchiveStore>::ExactIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        Ok(FaultyArchiveWriter {
            inner: self.inner.start_writer()?,
            fault: self.fault,
            commits: self.commits.clone(),
        })
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<dolos_core::EntityValue>>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.read_logs(ns, keys)
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: std::ops::Range<LogKey>,
    ) -> Result<Self::LogIter, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.iter_logs(ns, range)
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.get_block_by_slot(slot)
    }

    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.get_blocks_by_slot(slot)
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.get_range(from, to)
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.find_intersect(intersect)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.get_tip()
    }

    fn prune_history(
        &self,
        max_slots: u64,
        max_prune_slots: Option<u64>,
        max_prune_index_rows: Option<u64>,
    ) -> Result<bool, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner
            .prune_history(max_slots, max_prune_slots, max_prune_index_rows)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.truncate_front(after)
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.slot_by_block_hash(hash)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.slot_by_block_number(number)
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.slot_by_tx_hash(hash)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.slots_by_tag(dimension, key, start, end)
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: std::ops::Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.iter_archive_tags(dimensions, slots)
    }

    fn iter_exact_records(
        &self,
        slots: std::ops::Range<BlockSlot>,
    ) -> Result<Self::ExactIter, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.iter_exact_records(slots)
    }
}

pub struct FaultyArchiveWriter {
    inner: <MemoryArchiveStore as ArchiveStore>::Writer,
    fault: TestFault,
    commits: Arc<AtomicU32>,
}

impl ArchiveWriter for FaultyArchiveWriter {
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
        value: &dolos_core::EntityValue,
    ) -> Result<(), ArchiveError> {
        self.inner.write_log(ns, key, value)
    }

    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError> {
        self.inner.undo(point)
    }

    fn commit(self) -> Result<(), ArchiveError> {
        if past_crash(self.fault, &self.commits) {
            return Err(ArchiveError::InternalError(
                "fault injection: crashed before archive commit".into(),
            ));
        }
        self.inner.commit()
    }
}

#[derive(Clone)]
pub struct FaultyWalStore {
    inner: dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta>,
    fault: TestFault,
}

impl FaultyWalStore {
    pub fn new(
        inner: dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta>,
        fault: TestFault,
    ) -> Self {
        Self { inner, fault }
    }

    fn should_fault(&self) -> bool {
        matches!(self.fault, TestFault::WalStoreError)
    }

    fn fault_err(&self) -> WalError {
        WalError::internal("fault injection: wal store")
    }
}

impl WalStore for FaultyWalStore {
    type Delta = dolos_cardano::CardanoDelta;
    type LogIterator<'a> =
        <dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta> as WalStore>::LogIterator<'a>;
    type BlockIterator<'a> =
        <dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta> as WalStore>::BlockIterator<
            'a,
        >;

    fn reset_to(&self, point: &ChainPoint) -> Result<(), WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.reset_to(point)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.truncate_front(after)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner
            .prune_history(max_slots, max_prune)
            .map_err(WalError::from)
    }

    fn locate_point(&self, around: BlockSlot) -> Result<Option<ChainPoint>, WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.locate_point(around)
    }

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<Self::Delta>>, WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.read_entry(key)
    }

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.iter_logs(start, end)
    }

    fn iter_blocks<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::BlockIterator<'a>, WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.iter_blocks(start, end)
    }

    fn append_entries(&self, logs: Vec<LogEntry<Self::Delta>>) -> Result<(), WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.append_entries(&logs).map_err(WalError::from)
    }

    fn remove_entries(&mut self, after: &ChainPoint) -> Result<(), WalError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.remove_entries(after)
    }
}

impl Domain for FaultyToyDomain {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type Wal = FaultyWalStore;
    type Archive = FaultyArchiveStore;
    type State = FaultyStateStore;
    type Chain = dolos_cardano::CardanoLogic;
    type WorkUnit = dolos_cardano::CardanoWorkUnit;
    type TipSubscription = TipSubscription;
    type Mempool = Mempool;

    fn storage_config(&self) -> &dolos_core::config::StorageConfig {
        self.inner.storage_config()
    }

    fn sync_config(&self) -> &dolos_core::config::SyncConfig {
        self.inner.sync_config()
    }

    fn genesis(&self) -> Arc<dolos_core::Genesis> {
        self.genesis_override
            .as_ref()
            .cloned()
            .unwrap_or_else(|| self.inner.genesis())
    }

    fn read_chain(&self) -> std::sync::RwLockReadGuard<'_, Self::Chain> {
        self.inner.read_chain()
    }

    fn write_chain(&self) -> std::sync::RwLockWriteGuard<'_, Self::Chain> {
        self.inner.write_chain()
    }

    fn wal(&self) -> &Self::Wal {
        &self.wal
    }

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn archive(&self) -> &Self::Archive {
        &self.archive
    }

    fn mempool(&self) -> &Self::Mempool {
        self.inner.mempool()
    }

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError> {
        self.inner.watch_tip(from)
    }

    fn notify_tip(&self, tip: TipEvent) {
        self.inner.notify_tip(tip)
    }
}
