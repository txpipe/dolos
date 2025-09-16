use std::sync::Arc;

use dolos_cardano::CardanoLogic;
use dolos_core::*;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChainConfig {
    Cardano(dolos_cardano::Config),
}

impl Default for ChainConfig {
    fn default() -> Self {
        Self::Cardano(dolos_cardano::Config::default())
    }
}

/// A persistent store for ledger state
#[derive(Clone)]
#[non_exhaustive]
pub enum ArchiveAdapter {
    Redb(dolos_redb::archive::ChainStore),
}

impl ArchiveStore for ArchiveAdapter {
    type BlockIter<'a> = ArchiveRangeBlockIter;
    type SparseBlockIter = ArchiveSparseBlockIter;
    type Writer = ArchiveStoreWriter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => ArchiveStoreWriter::Redb(x.start_writer()?),
        };

        Ok(out)
    }

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_block_by_hash(block_hash)?,
        };

        Ok(out)
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_block_by_slot(slot)?,
        };

        Ok(out)
    }

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_block_by_number(number)?,
        };

        Ok(out)
    }

    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_block_with_tx(tx_hash)?,
        };

        Ok(out)
    }

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_tx(tx_hash)?,
        };

        Ok(out)
    }

    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_slot_for_tx(tx_hash)?,
        };

        Ok(out)
    }

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.iter_blocks_with_address(address)?,
        };

        Ok(out.into())
    }

    fn iter_blocks_with_asset(&self, asset: &[u8]) -> Result<Self::SparseBlockIter, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.iter_blocks_with_asset(asset)?,
        };

        Ok(out.into())
    }

    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.iter_blocks_with_payment(payment)?,
        };

        Ok(out.into())
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_range(from, to)?.into(),
        };

        Ok(out)
    }

    fn find_intersect<'a>(
        &self,
        intersect: &[ChainPoint],
    ) -> Result<Option<ChainPoint>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.find_intersect(intersect)?,
        };

        Ok(out)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        let out = match self {
            ArchiveAdapter::Redb(x) => x.get_tip()?,
        };

        Ok(out)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        let done = match self {
            ArchiveAdapter::Redb(x) => x.prune_history(max_slots, max_prune)?,
        };

        Ok(done)
    }
}

impl From<dolos_redb::archive::ChainStore> for ArchiveAdapter {
    fn from(value: dolos_redb::archive::ChainStore) -> Self {
        Self::Redb(value)
    }
}

pub enum ArchiveStoreWriter {
    Redb(dolos_redb::archive::ChainStoreWriter),
}

impl ArchiveWriter for ArchiveStoreWriter {
    fn apply(
        &self,
        point: &ChainPoint,
        block: &RawBlock,
        tags: &SlotTags,
    ) -> Result<(), ArchiveError> {
        match self {
            ArchiveStoreWriter::Redb(x) => x.apply(point, block, tags)?,
        };

        Ok(())
    }

    fn undo(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), ArchiveError> {
        match self {
            ArchiveStoreWriter::Redb(x) => x.undo(point, tags)?,
        };

        Ok(())
    }

    fn commit(self) -> Result<(), ArchiveError> {
        match self {
            ArchiveStoreWriter::Redb(x) => x.commit()?,
        };

        Ok(())
    }
}

pub enum ArchiveRangeBlockIter {
    Redb(dolos_redb::archive::ChainRangeIter),
}

impl Iterator for ArchiveRangeBlockIter {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ArchiveRangeBlockIter::Redb(chainiter) => chainiter.next(),
        }
    }
}

impl DoubleEndedIterator for ArchiveRangeBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            ArchiveRangeBlockIter::Redb(chainiter) => chainiter.next_back(),
        }
    }
}

impl From<dolos_redb::archive::ChainRangeIter> for ArchiveRangeBlockIter {
    fn from(value: dolos_redb::archive::ChainRangeIter) -> Self {
        Self::Redb(value)
    }
}

pub struct ArchiveSparseBlockIter(dolos_redb::archive::ChainSparseIter);

impl Iterator for ArchiveSparseBlockIter {
    type Item = Result<(BlockSlot, Option<BlockBody>), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

impl DoubleEndedIterator for ArchiveSparseBlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0.next_back()
    }
}

impl From<dolos_redb::archive::ChainSparseIter> for ArchiveSparseBlockIter {
    fn from(value: dolos_redb::archive::ChainSparseIter) -> Self {
        Self(value)
    }
}

// we can hardcode the WAL since we don't expect multiple types of
// implementations
pub type WalAdapter = dolos_redb::wal::RedbWalStore<dolos_cardano::CardanoDelta>;

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
    pub chain: CardanoLogic,
    pub state: dolos_redb3::StateStore,
    pub archive: ArchiveAdapter,
    pub mempool: crate::mempool::Mempool,
    pub tip_broadcast: tokio::sync::broadcast::Sender<TipEvent>,
}

impl Domain for DomainAdapter {
    type Entity = dolos_cardano::CardanoEntity;
    type EntityDelta = dolos_cardano::CardanoDelta;
    type Chain = CardanoLogic;
    type Wal = WalAdapter;
    type State = dolos_redb3::StateStore;
    type Archive = ArchiveAdapter;
    type Mempool = crate::mempool::Mempool;
    type TipSubscription = TipSubscription;

    fn genesis(&self) -> &Genesis {
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

    fn archive(&self) -> &Self::Archive {
        &self.archive
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

    fn get_slot_timestamp(&self, slot: u64) -> Option<u64> {
        let time = dolos_cardano::eras::load_era_summary(self)
            .ok()?
            .slot_time(slot);

        Some(time)
    }
}
