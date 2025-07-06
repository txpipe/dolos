use std::sync::Arc;

use crate::{TestAddress, UtxoGenerator, make_custom_utxo_delta};
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

    fn receive_raw(&self, cbor: &[u8]) -> Result<TxHash, MempoolError> {
        todo!()
    }

    fn apply(&self, deltas: &[LedgerDelta]) {
        todo!()
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        todo!()
    }

    fn subscribe(&self) -> Self::Stream {
        todo!()
    }

    fn evaluate_raw(&self, cbor: &[u8]) -> Result<EvalReport, MempoolError> {
        todo!()
    }
}

#[derive(Clone)]
pub struct ToyDomain {
    state: dolos_redb::state::LedgerStore,
    wal: dolos_redb::wal::RedbWalStore,
    archive: dolos_redb::archive::ChainStore,
    mempool: Mempool,
    storage_config: dolos_core::StorageConfig,
    genesis: Arc<dolos_core::Genesis>,
}

impl ToyDomain {
    /// Create a new MockDomain with the provided state implementation
    pub fn new() -> Self {
        Self {
            state: dolos_redb::state::LedgerStore::in_memory_v2().unwrap(),
            wal: dolos_redb::wal::RedbWalStore::memory().unwrap(),
            archive: dolos_redb::archive::ChainStore::in_memory_v1().unwrap(),
            mempool: Mempool {},
            storage_config: dolos_core::StorageConfig::default(),
            genesis: Arc::new(dolos_cardano::include::preprod::load()),
        }
    }
}

impl dolos_core::Domain for ToyDomain {
    type State = dolos_redb::state::LedgerStore;
    type Wal = dolos_redb::wal::RedbWalStore;
    type Archive = dolos_redb::archive::ChainStore;
    type Mempool = Mempool;
    type Chain = dolos_cardano::ChainLogic;

    fn storage_config(&self) -> &dolos_core::StorageConfig {
        &self.storage_config
    }

    fn genesis(&self) -> &dolos_core::Genesis {
        &self.genesis
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
        &self.mempool
    }
}
