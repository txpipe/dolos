use std::sync::Arc;

use dolos_core::*;

/// A persistent store for ledger state
#[derive(Clone)]
#[non_exhaustive]
pub enum StateAdapter {
    Redb(dolos_redb::state::LedgerStore),
}

impl StateStore for StateAdapter {
    fn start(&self) -> Result<Option<ChainPoint>, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.start()?,
        };

        Ok(out)
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.cursor()?,
        };

        Ok(out)
    }

    fn is_empty(&self) -> Result<bool, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.is_empty()?,
        };

        Ok(out)
    }

    fn get_pparams(&self, until: BlockSlot) -> Result<Vec<EraCbor>, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.get_pparams(until)?,
        };

        Ok(out)
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.get_utxos(refs)?,
        };

        Ok(out)
    }

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.get_utxo_by_address(address)?,
        };

        Ok(out)
    }

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.get_utxo_by_payment(payment)?,
        };

        Ok(out)
    }

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.get_utxo_by_stake(stake)?,
        };

        Ok(out)
    }

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.get_utxo_by_policy(policy)?,
        };

        Ok(out)
    }

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => x.get_utxo_by_asset(asset)?,
        };

        Ok(out)
    }

    fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), StateError> {
        match self {
            StateAdapter::Redb(x) => x.apply(deltas)?,
        };

        Ok(())
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, StateError> {
        let done = match self {
            StateAdapter::Redb(x) => x.prune_history(max_slots, max_prune)?,
        };

        Ok(done)
    }

    fn upgrade(self) -> Result<Self, StateError> {
        let out = match self {
            StateAdapter::Redb(x) => StateAdapter::Redb(x.upgrade()?),
        };

        Ok(out)
    }

    fn copy(&self, target: &Self) -> Result<(), StateError> {
        match (self, target) {
            (Self::Redb(x), Self::Redb(target)) => x.copy(target)?,
        }

        Ok(())
    }
}

impl From<dolos_redb::state::LedgerStore> for StateAdapter {
    fn from(value: dolos_redb::state::LedgerStore) -> Self {
        Self::Redb(value)
    }
}

impl TryFrom<StateAdapter> for dolos_redb::state::LedgerStore {
    type Error = StateError;

    fn try_from(value: StateAdapter) -> Result<Self, Self::Error> {
        match value {
            StateAdapter::Redb(x) => Ok(x),
        }
    }
}

impl pallas::interop::utxorpc::LedgerContext for StateAdapter {
    fn get_utxos<'a>(
        &self,
        refs: &[pallas::interop::utxorpc::TxoRef],
    ) -> Option<pallas::interop::utxorpc::UtxoMap> {
        let refs: Vec<_> = refs.iter().map(|x| TxoRef::from(*x)).collect();

        let some = dolos_core::StateStore::get_utxos(self, refs)
            .ok()?
            .into_iter()
            .map(|(k, v)| {
                let era = v.0.try_into().expect("era out of range");
                (k.into(), (era, v.1))
            })
            .collect();

        Some(some)
    }
}

#[derive(Clone)]
pub enum WalAdapter {
    Redb(dolos_redb::wal::RedbWalStore),
}

impl WalStore for WalAdapter {
    type LogIterator<'a> = WalIter<'a>;

    async fn tip_change(&self) {
        match self {
            WalAdapter::Redb(x) => x.tip_change().await,
        }
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError> {
        match self {
            WalAdapter::Redb(x) => WalStore::prune_history(x, max_slots, max_prune),
        }
    }

    fn crawl_range<'a>(
        &self,
        start: LogSeq,
        end: LogSeq,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        match self {
            WalAdapter::Redb(x) => Ok(WalIter::Redb(x.crawl_range(start, end)?)),
        }
    }

    fn crawl_from<'a>(&self, start: Option<LogSeq>) -> Result<Self::LogIterator<'a>, WalError> {
        match self {
            WalAdapter::Redb(x) => Ok(WalIter::Redb(x.crawl_from(start)?)),
        }
    }

    fn locate_point(&self, point: &ChainPoint) -> Result<Option<LogSeq>, WalError> {
        match self {
            WalAdapter::Redb(x) => x.locate_point(point),
        }
    }

    fn append_entries(&self, logs: impl Iterator<Item = LogValue>) -> Result<(), WalError> {
        match self {
            WalAdapter::Redb(x) => x.append_entries(logs),
        }
    }
}

impl From<dolos_redb::wal::RedbWalStore> for WalAdapter {
    fn from(value: dolos_redb::wal::RedbWalStore) -> Self {
        Self::Redb(value)
    }
}

pub enum WalIter<'a> {
    Redb(dolos_redb::wal::WalIter<'a>),
}

impl Iterator for WalIter<'_> {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            WalIter::Redb(chainiter) => chainiter.next(),
        }
    }
}

impl DoubleEndedIterator for WalIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            WalIter::Redb(chainiter) => chainiter.next_back(),
        }
    }
}

impl<'a> From<dolos_redb::wal::WalIter<'a>> for WalIter<'a> {
    fn from(value: dolos_redb::wal::WalIter<'a>) -> Self {
        Self::Redb(value)
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

    fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), ArchiveError> {
        match self {
            ArchiveAdapter::Redb(x) => x.apply(deltas)?,
        };

        Ok(())
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

impl From<dolos_redb::archive::ChainSparseIter> for ArchiveSparseBlockIter {
    fn from(value: dolos_redb::archive::ChainSparseIter) -> Self {
        Self(value)
    }
}

#[derive(Clone)]
pub struct DomainAdapter {
    pub storage_config: Arc<StorageConfig>,
    pub genesis: Arc<Genesis>,
    pub wal: WalAdapter,
    pub state: StateAdapter,
    pub archive: ArchiveAdapter,
    pub mempool: crate::mempool::Mempool,
}

impl Domain for DomainAdapter {
    type Wal = WalAdapter;
    type State = StateAdapter;
    type Archive = ArchiveAdapter;
    type Mempool = crate::mempool::Mempool;
    type Chain = dolos_cardano::ChainLogic;

    fn genesis(&self) -> &Genesis {
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

    fn storage_config(&self) -> &StorageConfig {
        &self.storage_config
    }
}
