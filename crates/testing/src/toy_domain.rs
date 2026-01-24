use crate::{make_custom_utxo_delta, TestAddress, UtxoGenerator};
use dolos_core::{
    config::{CardanoConfig, StorageConfig},
    sync::execute_work_unit,
    BootstrapExt, *,
};
use futures_util::stream::StreamExt;
use std::sync::Arc;
use tokio::sync::RwLock;

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
pub struct Mempool {}

pub struct MempoolStream {
    inner: tokio_stream::wrappers::BroadcastStream<MempoolEvent>,
}

impl futures_core::Stream for MempoolStream {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.inner.poll_next_unpin(cx) {
            std::task::Poll::Ready(Some(x)) => match x {
                Ok(x) => std::task::Poll::Ready(Some(Ok(x))),
                Err(err) => {
                    std::task::Poll::Ready(Some(Err(MempoolError::Internal(Box::new(err)))))
                }
            },
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}

impl dolos_core::MempoolStore for Mempool {
    type Stream = MempoolStream;

    fn receive(&self, _tx: MempoolTx) -> Result<(), MempoolError> {
        todo!()
    }

    fn apply(&self, _seen_txs: &[TxHash], _unseen_txs: &[TxHash]) {
        // do nothing for now
    }

    fn check_stage(&self, _tx_hash: &TxHash) -> MempoolTxStage {
        todo!()
    }

    fn subscribe(&self) -> Self::Stream {
        todo!()
    }

    fn pending(&self) -> Vec<(TxHash, EraCbor)> {
        vec![]
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
}

impl ToyDomain {
    /// Create a new MockDomain with the provided state implementation
    pub async fn new(
        initial_delta: Option<UtxoSetDelta>,
        storage_config: Option<StorageConfig>,
    ) -> Self {
        let state = dolos_redb3::state::StateStore::in_memory(dolos_cardano::model::build_schema())
            .unwrap();

        let genesis = Arc::new(dolos_cardano::include::devnet::load());
        let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

        let archive =
            dolos_redb3::archive::ArchiveStore::in_memory(dolos_cardano::model::build_schema())
                .unwrap();

        let indexes = dolos_redb3::indexes::IndexStore::in_memory().unwrap();

        let config = CardanoConfig::default();

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
            mempool: Mempool {},
            storage_config: storage_config.unwrap_or_default(),
            genesis: genesis.clone(),
            tip_broadcast,
        };

        // Apply genesis state using the work unit pattern.
        // Note: We're bypassing the normal pop_work flow here, so we need to
        // manually trigger the cache refresh that would normally happen.
        let mut genesis_work = dolos_cardano::CardanoWorkUnit::Genesis(
            dolos_cardano::genesis::GenesisWorkUnit::new(config, genesis),
        );
        execute_work_unit(&domain, &mut genesis_work).unwrap();

        // Manually refresh the chain cache after genesis since we bypassed pop_work.
        // In normal operation, the cache refresh happens automatically via the
        // needs_cache_refresh flag in CardanoLogic::pop_work.
        {
            let mut chain = domain.chain.write().await;
            chain.refresh_cache::<Self>(&domain.state).unwrap();
        }

        domain.bootstrap().await.unwrap();

        if let Some(delta) = initial_delta {
            let writer = domain.state.start_writer().unwrap();
            let index_writer = domain.indexes.start_writer().unwrap();
            writer.apply_utxoset(&delta).unwrap();
            index_writer.apply_utxoset(&delta).unwrap();
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

    async fn read_chain(&self) -> tokio::sync::RwLockReadGuard<'_, Self::Chain> {
        self.chain.read().await
    }

    async fn write_chain(&self) -> tokio::sync::RwLockWriteGuard<'_, Self::Chain> {
        self.chain.write().await
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

    fn get_historical_utxos(
        &self,
        _refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        None
    }
}
