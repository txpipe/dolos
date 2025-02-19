use pallas::ledger::traverse::MultiEraBlock;
use thiserror::Error;

use crate::ledger::{BrokenInvariant, LedgerDelta};
use crate::model::{BlockBody, BlockSlot};

pub mod redb;

#[derive(Debug, Error)]
pub enum ChainError {
    #[error("broken invariant")]
    BrokenInvariant(#[source] BrokenInvariant),

    #[error("storage error")]
    StorageError(#[source] ::redb::Error),

    #[error("address decoding error")]
    AddressDecoding(pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[source] pallas::codec::minicbor::decode::Error),

    #[error("block decoding error")]
    BlockDecodingError(#[source] pallas::ledger::traverse::Error),
}

impl From<::redb::TableError> for ChainError {
    fn from(value: ::redb::TableError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<::redb::CommitError> for ChainError {
    fn from(value: ::redb::CommitError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<::redb::StorageError> for ChainError {
    fn from(value: ::redb::StorageError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<::redb::TransactionError> for ChainError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self::StorageError(value.into())
    }
}

impl From<pallas::ledger::addresses::Error> for ChainError {
    fn from(value: pallas::ledger::addresses::Error) -> Self {
        Self::AddressDecoding(value)
    }
}

/// A persistent store for ledger state
#[derive(Clone)]
#[non_exhaustive]
pub enum ChainStore {
    Redb(redb::ChainStore),
}

impl ChainStore {
    pub fn get_possible_block_slots_by_address(
        &self,
        address: &[u8],
    ) -> Result<Vec<BlockSlot>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_possible_block_slots_by_address(address),
        }
    }

    pub fn get_possible_block_slots_by_tx_hash(
        &self,
        tx_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_possible_block_slots_by_tx_hash(tx_hash),
        }
    }

    pub fn get_possible_block_slots_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Vec<BlockSlot>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_possible_block_slots_by_block_hash(block_hash),
        }
    }

    pub fn get_possible_blocks_by_address(
        &self,
        address: &[u8],
    ) -> Result<Vec<BlockBody>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_possible_blocks_by_address(address),
        }
    }

    pub fn get_possible_blocks_by_tx_hash(
        &self,
        tx_hash: &[u8],
    ) -> Result<Vec<BlockBody>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_possible_blocks_by_tx_hash(tx_hash),
        }
    }

    pub fn get_possible_blocks_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Vec<BlockBody>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_possible_blocks_by_block_hash(block_hash),
        }
    }

    pub fn get_block_by_block_hash(
        &self,
        block_hash: &[u8],
    ) -> Result<Option<BlockBody>, ChainError> {
        let possible = self.get_possible_blocks_by_block_hash(block_hash)?;
        for raw in possible {
            let block = MultiEraBlock::decode(&raw).map_err(ChainError::BlockDecodingError)?;
            if *block.hash() == *block_hash {
                return Ok(Some(raw));
            }
        }
        Ok(None)
    }

    pub fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ChainError> {
        match self {
            ChainStore::Redb(x) => x.get_block_by_slot(slot),
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
