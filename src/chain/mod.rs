use crate::ledger::LedgerDelta;
use crate::model::{BlockBody, BlockSlot};

pub mod error;
pub mod redb;

pub use error::ChainError;

/// A persistent store for ledger state
#[derive(Clone)]
#[non_exhaustive]
pub enum ChainStore {
    Redb(redb::ChainStore),
}

impl ChainStore {
    pub fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_block_by_hash(block_hash),
        }
    }

    pub fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_block_by_slot(slot),
        }
    }

    pub fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_block_by_number(number),
        }
    }

    pub fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<Vec<u8>>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_tx(tx_hash),
        }
    }

    pub fn get_tx_with_block_data(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, Vec<u8>)>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_tx_with_block_data(tx_hash),
        }
    }

    pub fn get_range(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<ChainIter, ChainError> {
        match self {
            ChainStore::Redb(x) => Ok(x.get_range(from, to)?.into()),
        }
    }
    pub fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_tip(),
        }
    }

    pub fn apply(&self, deltas: &[LedgerDelta]) -> Result<(), ChainError> {
        match self {
            ChainStore::Redb(x) => x.apply(deltas),
        }
    }

    pub fn housekeeping(&mut self) -> Result<(), ChainError> {
        match self {
            ChainStore::Redb(x) => x.housekeeping(),
        }
    }

    pub fn finalize(&self, until: BlockSlot) -> Result<(), ChainError> {
        match self {
            ChainStore::Redb(x) => x.finalize(until),
        }
    }
}

impl From<redb::ChainStore> for ChainStore {
    fn from(value: redb::ChainStore) -> Self {
        Self::Redb(value)
    }
}

pub enum ChainIter<'a> {
    Redb(redb::ChainIter<'a>),
}
impl Iterator for ChainIter<'_> {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ChainIter::Redb(chainiter) => chainiter.next(),
        }
    }
}

impl DoubleEndedIterator for ChainIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            ChainIter::Redb(chainiter) => chainiter.next_back(),
        }
    }
}

impl<'a> From<redb::ChainIter<'a>> for ChainIter<'a> {
    fn from(value: redb::ChainIter<'a>) -> Self {
        Self::Redb(value)
    }
}
