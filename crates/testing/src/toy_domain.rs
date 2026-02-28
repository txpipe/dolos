use crate::{make_custom_utxo_delta, TestAddress, UtxoGenerator};
use dolos_cardano::indexes::index_delta_from_utxo_delta;
use dolos_core::{
    config::{CardanoConfig, StorageConfig},
    sync::execute_work_unit,
    BootstrapExt, LogKey, TemporalKey, *,
};
use std::sync::Arc;
use std::sync::RwLock;

pub fn seed_random_memory_store(utxo_generator: impl UtxoGenerator) -> impl StateStore {
    let store =
        dolos_redb3::state::StateStore::in_memory(dolos_cardano::model::build_schema()).unwrap();

    let everyone = TestAddress::everyone();
    let utxos_per_address = 2..4;

    let delta = make_custom_utxo_delta(everyone, utxos_per_address, utxo_generator);

    let writer = store.start_writer().unwrap();
    writer.apply_utxoset(&delta).unwrap();
    writer.commit().unwrap();

    store
}

#[derive(Clone)]
pub struct Mempool {
    pending: Arc<RwLock<Vec<MempoolTx>>>,
}

pub struct EmptyMempoolStream;

impl futures_core::Stream for EmptyMempoolStream {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(None)
    }
}

impl dolos_core::MempoolStore for Mempool {
    type Stream = EmptyMempoolStream;

    async fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        let mut pending = self.pending.write().map_err(|_| {
            MempoolError::Internal(Box::new(std::io::Error::other("mempool lock poisoned")))
        })?;
        pending.push(tx);
        Ok(())
    }

    async fn has_pending(&self) -> bool {
        self.pending
            .read()
            .map(|p| !p.is_empty())
            .unwrap_or(false)
    }

    async fn peek_pending(&self, limit: usize) -> Vec<MempoolTx> {
        self.pending
            .read()
            .map(|p| p.iter().take(limit).cloned().collect())
            .unwrap_or_default()
    }

    async fn mark_inflight(&self, _hashes: &[TxHash]) -> Result<(), MempoolError> {
        Ok(())
    }

    async fn mark_acknowledged(&self, _hashes: &[TxHash]) -> Result<(), MempoolError> {
        Ok(())
    }

    async fn find_inflight(&self, _tx_hash: &TxHash) -> Option<MempoolTx> {
        None
    }

    async fn peek_inflight(&self, _limit: usize) -> Vec<MempoolTx> {
        vec![]
    }

    async fn confirm(&self, _point: &ChainPoint, _seen_txs: &[TxHash], _unseen_txs: &[TxHash], _finalize_threshold: u32, _drop_threshold: u32) -> Result<(), MempoolError> {
        Ok(())
    }

    async fn check_status(&self, tx_hash: &TxHash) -> TxStatus {
        let stage = if let Ok(pending) = self.pending.read() {
            if pending.iter().any(|tx| &tx.hash == tx_hash) {
                MempoolTxStage::Pending
            } else {
                MempoolTxStage::Unknown
            }
        } else {
            MempoolTxStage::Unknown
        };

        TxStatus {
            stage,
            confirmations: 0,
            non_confirmations: 0,
            confirmed_at: None,
        }
    }

    async fn dump_finalized(&self, _cursor: u64, _limit: usize) -> dolos_core::MempoolPage {
        dolos_core::MempoolPage { items: vec![], next_cursor: None }
    }

    fn subscribe(&self) -> Self::Stream {
        EmptyMempoolStream
    }
}

#[derive(Clone)]
pub struct ToyDomain {
    wal: dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta>,
    chain: Arc<RwLock<dolos_cardano::CardanoLogic>>,
    state: dolos_redb3::state::StateStore,
    archive: dolos_redb3::archive::ArchiveStore,
    indexes: dolos_redb3::indexes::IndexStore,
    mempool: Mempool,
    storage_config: StorageConfig,
    genesis: Arc<dolos_core::Genesis>,
    tip_broadcast: tokio::sync::broadcast::Sender<TipEvent>,
    submit_lock: Arc<tokio::sync::Mutex<()>>,
}

impl ToyDomain {
    /// Create a new MockDomain with the provided state implementation
    pub async fn new(initial_delta: Option<UtxoSetDelta>, storage_config: Option<StorageConfig>) -> Self {
        let genesis = Arc::new(dolos_cardano::include::devnet::load());
        Self::new_with_genesis(genesis, initial_delta, storage_config).await
    }

    pub async fn new_with_genesis(
        genesis: Arc<dolos_core::Genesis>,
        initial_delta: Option<UtxoSetDelta>,
        storage_config: Option<StorageConfig>,
    ) -> Self {
        Self::new_with_genesis_and_config(
            genesis,
            CardanoConfig::default(),
            initial_delta,
            storage_config,
        ).await
    }

    pub async fn new_with_genesis_and_config(
        genesis: Arc<dolos_core::Genesis>,
        config: CardanoConfig,
        initial_delta: Option<UtxoSetDelta>,
        storage_config: Option<StorageConfig>,
    ) -> Self {
        let state = dolos_redb3::state::StateStore::in_memory(dolos_cardano::model::build_schema())
            .unwrap();

        let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

        let archive =
            dolos_redb3::archive::ArchiveStore::in_memory(dolos_cardano::model::build_schema())
                .unwrap();

        let indexes = dolos_redb3::indexes::IndexStore::in_memory().unwrap();

        let chain =
            dolos_cardano::CardanoLogic::initialize::<Self>(config.clone(), &state, &genesis)
                .unwrap();

        // Create the domain first (genesis work unit needs it for execution)
        let domain = Self {
            state,
            wal: dolos_redb3::wal::RedbWalStore::memory().unwrap(),
            chain: Arc::new(RwLock::new(chain)),
            archive,
            indexes,
            mempool: Mempool {
                pending: Arc::new(RwLock::new(Vec::new())),
            },
            storage_config: storage_config.unwrap_or_default(),
            genesis: genesis.clone(),
            tip_broadcast,
            submit_lock: Arc::new(tokio::sync::Mutex::new(())),
        };

        // Apply genesis state using the work unit pattern.
        // Note: We're bypassing the normal pop_work flow here, so we need to
        // manually trigger the cache refresh that would normally happen.
        let mut genesis_work = dolos_cardano::CardanoWorkUnit::Genesis(Box::new(
            dolos_cardano::genesis::GenesisWorkUnit::new(config, genesis),
        ));
        execute_work_unit(&domain, &mut genesis_work).await.unwrap();

        // Manually refresh the chain cache after genesis since we bypassed pop_work.
        // In normal operation, the cache refresh happens automatically via the
        // needs_cache_refresh flag in CardanoLogic::pop_work.
        {
            let mut chain = domain.chain.write().expect("chain lock poisoned");
            chain.refresh_cache::<Self>(&domain.state).unwrap();
        }

        domain.bootstrap().await.unwrap();

        // Ensure the current epoch state is available as an archive log entry.
        let chain = dolos_cardano::eras::load_era_summary::<Self>(&domain.state).unwrap();
        let epoch = dolos_cardano::load_epoch::<Self>(&domain.state).unwrap();
        let epoch_start = chain.epoch_start(epoch.number);
        let log_key = LogKey::from(TemporalKey::from(epoch_start));
        let writer = domain.archive.start_writer().unwrap();
        writer.write_log_typed(&log_key, &epoch).unwrap();
        writer.commit().unwrap();

        if let Some(delta) = initial_delta {
            let writer = domain.state.start_writer().unwrap();
            let index_writer = domain.indexes.start_writer().unwrap();
            writer.apply_utxoset(&delta).unwrap();

            // Build index delta from UTxO delta using Cardano-specific helper
            let cursor = domain
                .state
                .read_cursor()
                .unwrap()
                .unwrap_or(ChainPoint::Origin);
            let index_delta = index_delta_from_utxo_delta(cursor, &delta);
            index_writer.apply(&index_delta).unwrap();

            writer.commit().unwrap();
            index_writer.commit().unwrap();
        }

        domain
    }
}

pub struct TipSubscription {
    replay: Vec<(ChainPoint, RawBlock)>,
    receiver: tokio::sync::broadcast::Receiver<TipEvent>,
}

impl dolos_core::TipSubscription for TipSubscription {
    async fn next_tip(&mut self) -> TipEvent {
        if !self.replay.is_empty() {
            let (point, block) = self.replay.pop().unwrap();
            dbg!(&point, "running replay");
            return TipEvent::Apply(point, block);
        }

        self.receiver.recv().await.unwrap()
    }
}

impl dolos_core::Domain for ToyDomain {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type Wal = dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta>;
    type Archive = dolos_redb3::archive::ArchiveStore;
    type State = dolos_redb3::state::StateStore;
    type Chain = dolos_cardano::CardanoLogic;
    type WorkUnit = dolos_cardano::CardanoWorkUnit;
    type TipSubscription = TipSubscription;
    type Indexes = dolos_redb3::indexes::IndexStore;
    type Mempool = Mempool;

    fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    fn genesis(&self) -> Arc<dolos_core::Genesis> {
        self.genesis.clone()
    }

    fn read_chain(&self) -> std::sync::RwLockReadGuard<'_, Self::Chain> {
        self.chain.read().expect("chain lock poisoned")
    }

    fn write_chain(&self) -> std::sync::RwLockWriteGuard<'_, Self::Chain> {
        self.chain.write().expect("chain lock poisoned")
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
        &self.mempool
    }

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError> {
        let receiver = self.tip_broadcast.subscribe();

        let replay = self
            .wal()
            .iter_blocks(from.clone(), None)?
            .filter(|(point, _)| match from.as_ref() {
                Some(from) => from != point,
                None => true,
            })
            .collect::<Vec<_>>();

        Ok(TipSubscription { replay, receiver })
    }

    fn notify_tip(&self, tip: TipEvent) {
        if self.tip_broadcast.receiver_count() > 0 {
            self.tip_broadcast.send(tip).unwrap();
        }
    }

    async fn acquire_submit_lock(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.submit_lock.lock().await
    }
}

impl pallas::interop::utxorpc::LedgerContext for ToyDomain {
    fn get_utxos(
        &self,
        _refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        None
    }

    fn get_slot_timestamp(&self, _slot: u64) -> Option<u64> {
        None
    }
}
