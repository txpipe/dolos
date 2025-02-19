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

    pub fn finalize(&self, until: BlockSlot) -> Result<(), ChainError> {
        match self {
            ChainStore::Redb(x) => x.finalize(until),
        }
    }

    pub fn copy(&self, target: &Self) -> Result<(), ChainError> {
        match (self, target) {
            (Self::Redb(x), Self::Redb(target)) => x.copy(target),
        }
    }
}

impl From<redb::ChainStore> for ChainStore {
    fn from(value: redb::ChainStore) -> Self {
        Self::Redb(value)
    }
}
