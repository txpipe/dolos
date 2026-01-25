use std::sync::Arc;

use dolos_cardano::CardanoLogic;
use dolos_core::{config::StorageConfig, *};

use crate::storage::{IndexStoreBackend, StateStoreBackend};

// We can hardcode the WAL since we don't expect multiple types of
// implementations
pub type WalAdapter = dolos_redb3::wal::RedbWalStore<dolos_cardano::CardanoDelta>;

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

#[derive(Clone)]
pub struct DomainAdapter {
    pub storage_config: Arc<StorageConfig>,
    pub genesis: Arc<Genesis>,
    pub wal: WalAdapter,
    pub chain: Arc<std::sync::RwLock<CardanoLogic>>,
    pub state: StateStoreBackend,
    pub archive: dolos_redb3::archive::ArchiveStore,
    pub indexes: IndexStoreBackend,
    pub mempool: crate::mempool::Mempool,
    pub tip_broadcast: tokio::sync::broadcast::Sender<TipEvent>,
}

impl DomainAdapter {
    /// Gracefully shutdown all storage backends.
    ///
    /// This method should be called before the DomainAdapter goes out of scope,
    /// especially after heavy write operations like bulk imports. This ensures
    /// that storage backends complete any pending background work before being
    /// dropped.
    pub fn shutdown(&self) -> Result<(), DomainError> {
        tracing::info!("domain adapter: starting graceful shutdown");

        self.wal
            .shutdown()
            .map_err(|e| DomainError::WalError(e.into()))?;
        self.state.shutdown().map_err(DomainError::StateError)?;
        self.archive
            .shutdown()
            .map_err(|e| DomainError::ArchiveError(e.into()))?;
        self.indexes.shutdown().map_err(DomainError::IndexError)?;

        tracing::info!("domain adapter: graceful shutdown complete");
        Ok(())
    }
}

impl Domain for DomainAdapter {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type Chain = CardanoLogic;
    type WorkUnit = dolos_cardano::CardanoWorkUnit;
    type Wal = WalAdapter;
    type State = StateStoreBackend;
    type Archive = dolos_redb3::archive::ArchiveStore;
    type Indexes = IndexStoreBackend;
    type Mempool = crate::mempool::Mempool;
    type TipSubscription = TipSubscription;

    fn genesis(&self) -> Arc<Genesis> {
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

    fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }

    fn watch_tip(&self, from: Option<ChainPoint>) -> Result<Self::TipSubscription, DomainError> {
        // TODO: do a more thorough analysis to understand if this approach is
        // susceptible to race conditions. Things to explore:
        // - a mutex to block the sending of events while gathering the replay.
        // - storing the previous block hash in the db to use for consistency checks.

        // We first create the receiver so that the subscriber internal ring-buffer
        // position is defined.
        let receiver = self.tip_broadcast.subscribe();

        // We then collect any gap between the from point and the current tip. This
        // assumes that no event will be sent between the creation of the receiver and
        // the collection of the replay.
        let replay = self.wal().iter_blocks(from, None)?.collect::<Vec<_>>();

        Ok(TipSubscription { replay, receiver })
    }

    fn notify_tip(&self, tip: TipEvent) {
        if self.tip_broadcast.receiver_count() > 0 {
            self.tip_broadcast.send(tip).unwrap();
        }
    }
}

impl pallas::interop::utxorpc::LedgerContext for DomainAdapter {
    fn get_utxos(
        &self,
        refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        let refs: Vec<_> = refs.iter().map(|x| TxoRef::from(*x)).collect();

        let some = dolos_core::StateStore::get_utxos(self.state(), refs)
            .ok()?
            .into_iter()
            .map(|(k, v)| {
                let era = v.0.try_into().expect("era out of range");
                (k.into(), (era, v.1.clone()))
            })
            .collect();

        Some(some)
    }

    fn get_historical_utxos(
        &self,
        refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        if refs.is_empty() {
            return Some(Default::default());
        }

        let mut result = std::collections::HashMap::new();
        let refs_set: std::collections::HashSet<_> =
            refs.iter().copied().map(TxoRef::from).collect();

        let iter = self.wal().iter_logs(None, None).ok()?;
        for (_, log) in iter.rev() {
            for (txo_ref, era_cbor) in &log.inputs {
                if refs_set.contains(txo_ref) {
                    let era = era_cbor.0.try_into().expect("era out of range");
                    result.insert(txo_ref.clone().into(), (era, era_cbor.1.clone()));
                }
            }

            if result.len() == refs.len() {
                break;
            }
        }

        if result.is_empty() {
            None
        } else {
            Some(result)
        }
    }

    fn get_slot_timestamp(&self, slot: u64) -> Option<u64> {
        let time = dolos_cardano::eras::load_era_summary::<Self>(self.state())
            .ok()?
            .slot_time(slot);

        Some(time)
    }
}
