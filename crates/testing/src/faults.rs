use std::sync::Arc;

use dolos_core::{
    ArchiveError, ArchiveStore, BlockBody, BlockSlot, ChainPoint, Domain, DomainError, IndexError,
    IndexStore, LogEntry, LogKey, LogValue, Namespace, StateError, StateStore, TagDimension,
    TipEvent, WalError, WalStore,
};

use crate::toy_domain::{Mempool, TipSubscription, ToyDomain};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TestFault {
    #[default]
    None,
    StateStoreError,
    ArchiveStoreError,
    IndexStoreError,
    WalStoreError,
    GenesisError,
}

#[derive(Clone)]
pub struct FaultyToyDomain {
    inner: ToyDomain,
    genesis_override: Option<Arc<dolos_core::Genesis>>,
    state: FaultyStateStore,
    archive: FaultyArchiveStore,
    indexes: FaultyIndexStore,
    wal: FaultyWalStore,
}

impl FaultyToyDomain {
    pub fn new(inner: ToyDomain, fault: TestFault) -> Self {
        let state = FaultyStateStore::new(inner.state().clone(), fault);
        let archive = FaultyArchiveStore::new(inner.archive().clone(), fault);
        let indexes = FaultyIndexStore::new(inner.indexes().clone(), fault);
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
            indexes,
            wal,
        }
    }
}

#[derive(Clone)]
pub struct FaultyStateStore {
    inner: dolos_redb3::state::StateStore,
    fault: TestFault,
}

impl FaultyStateStore {
    pub fn new(inner: dolos_redb3::state::StateStore, fault: TestFault) -> Self {
        Self { inner, fault }
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
    type EntityIter = <dolos_redb3::state::StateStore as StateStore>::EntityIter;
    type EntityValueIter = <dolos_redb3::state::StateStore as StateStore>::EntityValueIter;
    type Writer = <dolos_redb3::state::StateStore as StateStore>::Writer;

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
        self.inner.start_writer()
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
}

#[derive(Clone)]
pub struct FaultyArchiveStore {
    inner: dolos_redb3::archive::ArchiveStore,
    fault: TestFault,
}

impl FaultyArchiveStore {
    pub fn new(inner: dolos_redb3::archive::ArchiveStore, fault: TestFault) -> Self {
        Self { inner, fault }
    }

    fn should_fault(&self) -> bool {
        matches!(self.fault, TestFault::ArchiveStoreError)
    }

    fn fault_err(&self) -> ArchiveError {
        ArchiveError::InternalError("fault injection: archive store".into())
    }
}

impl ArchiveStore for FaultyArchiveStore {
    type BlockIter<'a> = <dolos_redb3::archive::ArchiveStore as ArchiveStore>::BlockIter<'a>;
    type Writer = <dolos_redb3::archive::ArchiveStore as ArchiveStore>::Writer;
    type LogIter = <dolos_redb3::archive::ArchiveStore as ArchiveStore>::LogIter;
    type EntityValueIter = <dolos_redb3::archive::ArchiveStore as ArchiveStore>::EntityValueIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.start_writer().map_err(ArchiveError::from)
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
        self.inner
            .get_block_by_slot(slot)
            .map_err(ArchiveError::from)
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.get_range(from, to).map_err(ArchiveError::from)
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner
            .find_intersect(intersect)
            .map_err(ArchiveError::from)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.get_tip().map_err(ArchiveError::from)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner
            .prune_history(max_slots, max_prune)
            .map_err(ArchiveError::from)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.truncate_front(after)
    }
}

#[derive(Clone)]
pub struct FaultyIndexStore {
    inner: dolos_redb3::indexes::IndexStore,
    fault: TestFault,
}

impl FaultyIndexStore {
    pub fn new(inner: dolos_redb3::indexes::IndexStore, fault: TestFault) -> Self {
        Self { inner, fault }
    }

    fn should_fault(&self) -> bool {
        matches!(self.fault, TestFault::IndexStoreError)
    }

    fn fault_err(&self) -> IndexError {
        IndexError::DbError("fault injection: index store".into())
    }
}

impl IndexStore for FaultyIndexStore {
    type Writer = <dolos_redb3::indexes::IndexStore as IndexStore>::Writer;
    type SlotIter = <dolos_redb3::indexes::IndexStore as IndexStore>::SlotIter;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.start_writer()
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.initialize_schema()
    }

    fn copy(&self, target: &Self) -> Result<(), IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.copy(&target.inner)
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.cursor()
    }

    fn utxos_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
    ) -> Result<dolos_core::UtxoSet, IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.utxos_by_tag(dimension, key)
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.slot_by_block_hash(hash)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.slot_by_block_number(number)
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
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
    ) -> Result<Self::SlotIter, IndexError> {
        if self.should_fault() {
            return Err(self.fault_err());
        }
        self.inner.slots_by_tag(dimension, key, start, end)
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
    type Indexes = FaultyIndexStore;
    type Mempool = Mempool;

    fn storage_config(&self) -> &dolos_core::config::StorageConfig {
        self.inner.storage_config()
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

    fn indexes(&self) -> &Self::Indexes {
        &self.indexes
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
