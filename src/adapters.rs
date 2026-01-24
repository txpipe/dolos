use std::sync::Arc;

use dolos_cardano::CardanoLogic;
use dolos_core::{config::StorageConfig, *};

// we can hardcode the WAL since we don't expect multiple types of
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
    pub chain: Arc<tokio::sync::RwLock<CardanoLogic>>,
    pub state: dolos_redb3::state::StateStore,
    pub archive: dolos_redb3::archive::ArchiveStore,
    pub indexes: dolos_redb3::indexes::IndexStore,
    pub mempool: crate::mempool::Mempool,
    pub tip_broadcast: tokio::sync::broadcast::Sender<TipEvent>,
}

impl Domain for DomainAdapter {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type Chain = CardanoLogic;
    type Wal = WalAdapter;
    type State = dolos_redb3::state::StateStore;
    type Archive = dolos_redb3::archive::ArchiveStore;
    type Indexes = dolos_redb3::indexes::IndexStore;
    type Mempool = crate::mempool::Mempool;
    type TipSubscription = TipSubscription;

    fn genesis(&self) -> Arc<Genesis> {
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
