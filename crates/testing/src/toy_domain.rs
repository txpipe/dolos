use std::sync::Arc;

use crate::{make_custom_utxo_delta, TestAddress, UtxoGenerator};
use dolos_core::*;
use dolos_redb::state::LedgerStore;
use futures_util::stream::StreamExt;

pub fn seed_random_memory_store(utxo_generator: impl UtxoGenerator) -> impl StateStore {
    let store = LedgerStore::in_memory_v2().unwrap();

    let everyone = TestAddress::everyone();
    let utxos_per_address = 2..4;

    let delta = make_custom_utxo_delta(1, everyone, utxos_per_address, utxo_generator);

    store.apply(&[delta]).unwrap();

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

    fn receive_raw(&self, _cbor: &[u8]) -> Result<TxHash, MempoolError> {
        todo!()
    }

    fn apply(&self, _deltas: &[UtxoSetDelta]) {
        // do nothing for now
    }

    fn check_stage(&self, _tx_hash: &TxHash) -> MempoolTxStage {
        todo!()
    }

    fn subscribe(&self) -> Self::Stream {
        todo!()
    }

    fn evaluate_raw(&self, _cbor: &[u8]) -> Result<EvalReport, MempoolError> {
        todo!()
    }
}

#[derive(Clone)]
pub struct ToyDomain {
    state: dolos_redb::state::LedgerStore,
    wal: dolos_redb::wal::RedbWalStore<dolos_cardano::CardanoDelta>,
    chain: dolos_cardano::CardanoLogic,
    archive: dolos_redb::archive::ChainStore,
    mempool: Mempool,
    storage_config: dolos_core::StorageConfig,
    genesis: Arc<dolos_core::Genesis>,
    state3: dolos_redb3::StateStore,
    tip_broadcast: tokio::sync::broadcast::Sender<TipEvent>,
}

impl ToyDomain {
    /// Create a new MockDomain with the provided state implementation
    pub fn new(initial_delta: Option<UtxoSetDelta>, storage_config: Option<StorageConfig>) -> Self {
        let state = dolos_redb::state::LedgerStore::in_memory_v2().unwrap();

        if let Some(delta) = initial_delta {
            state.apply(&[delta]).unwrap();
        }

        let state3 =
            dolos_redb3::StateStore::in_memory(dolos_cardano::model::build_schema()).unwrap();

        let (tip_broadcast, _) = tokio::sync::broadcast::channel(100);

        Self {
            state,
            wal: dolos_redb::wal::RedbWalStore::memory().unwrap(),
            chain: dolos_cardano::CardanoLogic::new(dolos_cardano::Config::default()),
            archive: dolos_redb::archive::ChainStore::in_memory_v1().unwrap(),
            mempool: Mempool {},
            storage_config: storage_config.unwrap_or_default(),
            genesis: Arc::new(dolos_cardano::include::devnet::load()),
            tip_broadcast,
            state3,
        }
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
            return TipEvent::Apply(point, block);
        }

        self.receiver.recv().await.unwrap()
    }
}

impl dolos_core::Domain for ToyDomain {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type State = dolos_redb::state::LedgerStore;
    type Wal = dolos_redb::wal::RedbWalStore<dolos_cardano::CardanoDelta>;
    type Archive = dolos_redb::archive::ChainStore;
    type Mempool = Mempool;
    type Chain = dolos_cardano::CardanoLogic;
    type TipSubscription = TipSubscription;
    type State3 = dolos_redb3::StateStore;

    fn storage_config(&self) -> &dolos_core::StorageConfig {
        &self.storage_config
    }

    fn genesis(&self) -> &dolos_core::Genesis {
        &self.genesis
    }

    fn chain(&self) -> &Self::Chain {
        &self.chain
    }

    fn wal(&self) -> &Self::Wal {
        &self.wal
    }

    fn state(&self) -> &Self::State {
        &self.state
    }

    fn state3(&self) -> &Self::State3 {
        &self.state3
    }

    fn archive(&self) -> &Self::Archive {
        &self.archive
    }

    fn mempool(&self) -> &Self::Mempool {
        &self.mempool
    }

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError> {
        let receiver = self.tip_broadcast.subscribe();

        let replay = self
            .wal()
            .iter_blocks(from, None)
            .map_err(WalError::from)?
            .collect::<Vec<_>>();

        Ok(TipSubscription { replay, receiver })
    }

    fn notify_tip(&self, tip: TipEvent) {
        if !self.tip_broadcast.receiver_count() == 0 {
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
}
