//! Query helpers that combine index lookups with archive data fetching.
//!
//! This module provides the `QueryHelpers` trait which extends `Domain` with
//! high-level query methods that join index lookups (returning slots) with
//! archive fetches (returning block data).
//!
//! The helpers in this module are chain-agnostic. Chain-specific query helpers
//! (e.g., for Cardano-specific lookups like datum resolution) should be defined
//! in their respective chain crates as extension traits.

use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    ArchiveError, ArchiveStore, BlockBody, BlockSlot, ChainError, Domain, DomainError, EraCbor,
    IndexError, IndexStore, TagDimension, TxOrder,
};

/// Extension trait providing high-level query helpers that combine
/// index lookups with archive data fetching.
///
/// This trait is automatically implemented for all types that implement `Domain`.
/// It provides chain-agnostic query methods. Chain-specific queries should be
/// implemented in extension traits in their respective crates.
pub trait QueryHelpers: Domain {
    /// Get a block by its hash.
    fn block_by_hash(&self, hash: &[u8]) -> Result<Option<BlockBody>, DomainError>;

    /// Get a block by its number (height).
    fn block_by_number(&self, number: u64) -> Result<Option<BlockBody>, DomainError>;

    /// Get a block containing a transaction, along with the transaction's index in the block.
    fn block_by_tx_hash(&self, tx_hash: &[u8])
        -> Result<Option<(BlockBody, TxOrder)>, DomainError>;

    /// Alias for `block_by_tx_hash` (backward compatibility).
    fn block_with_tx(&self, tx_hash: &[u8]) -> Result<Option<(BlockBody, TxOrder)>, DomainError> {
        self.block_by_tx_hash(tx_hash)
    }

    /// Get a transaction's CBOR encoding by its hash.
    fn tx_cbor(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, DomainError>;

    /// Iterate over blocks matching a tag in the given slot range.
    ///
    /// Returns a sparse iterator that lazily fetches block data from the archive
    /// as slots are yielded from the index.
    fn blocks_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError>;
}

impl<D: Domain> QueryHelpers for D {
    fn block_by_hash(&self, hash: &[u8]) -> Result<Option<BlockBody>, DomainError> {
        let slot = self.indexes().slot_by_block_hash(hash)?;
        match slot {
            Some(slot) => Ok(self.archive().get_block_by_slot(&slot)?),
            None => Ok(None),
        }
    }

    fn block_by_number(&self, number: u64) -> Result<Option<BlockBody>, DomainError> {
        let slot = self.indexes().slot_by_block_number(number)?;
        match slot {
            Some(slot) => Ok(self.archive().get_block_by_slot(&slot)?),
            None => Ok(None),
        }
    }

    fn block_by_tx_hash(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, DomainError> {
        let slot = self.indexes().slot_by_tx_hash(tx_hash)?;
        let Some(slot) = slot else {
            return Ok(None);
        };

        let raw = self.archive().get_block_by_slot(&slot)?;
        let Some(raw) = raw else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice())
            .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
        if let Some((idx, _)) = block
            .txs()
            .iter()
            .enumerate()
            .find(|(_, tx)| tx.hash().to_vec() == tx_hash)
        {
            return Ok(Some((raw, idx)));
        }

        Ok(None)
    }

    fn tx_cbor(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, DomainError> {
        let slot = self.indexes().slot_by_tx_hash(tx_hash)?;
        let Some(slot) = slot else {
            return Ok(None);
        };

        let raw = self.archive().get_block_by_slot(&slot)?;
        let Some(raw) = raw else {
            return Ok(None);
        };

        let block = MultiEraBlock::decode(raw.as_slice())
            .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
        if let Some(tx) = block.txs().iter().find(|x| x.hash().to_vec() == tx_hash) {
            return Ok(Some(EraCbor(block.era().into(), tx.encode())));
        }

        Ok(None)
    }

    fn blocks_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        let slots = self
            .indexes()
            .slots_by_tag(dimension, key, start_slot, end_slot)?;
        Ok(SparseBlockIter::new(slots, self.archive().clone()))
    }
}

/// Error type for sparse block iteration.
#[derive(Debug)]
pub enum SparseBlockError {
    Index(IndexError),
    Archive(ArchiveError),
}

impl From<IndexError> for SparseBlockError {
    fn from(e: IndexError) -> Self {
        SparseBlockError::Index(e)
    }
}

impl From<ArchiveError> for SparseBlockError {
    fn from(e: ArchiveError) -> Self {
        SparseBlockError::Archive(e)
    }
}

impl From<SparseBlockError> for DomainError {
    fn from(e: SparseBlockError) -> Self {
        match e {
            SparseBlockError::Index(e) => DomainError::IndexError(e),
            SparseBlockError::Archive(e) => DomainError::ArchiveError(e),
        }
    }
}

/// Lazy iterator that wraps a slot iterator and fetches blocks on demand.
///
/// This iterator yields `(BlockSlot, Option<BlockBody>)` pairs, fetching
/// block data from the archive only when `next()` or `next_back()` is called.
pub struct SparseBlockIter<I: IndexStore, A: ArchiveStore> {
    slots: I::SlotIter,
    archive: A,
}

impl<I: IndexStore, A: ArchiveStore> SparseBlockIter<I, A> {
    /// Create a new sparse block iterator.
    pub fn new(slots: I::SlotIter, archive: A) -> Self {
        Self { slots, archive }
    }
}

impl<I: IndexStore, A: ArchiveStore> Iterator for SparseBlockIter<I, A> {
    type Item = Result<(BlockSlot, Option<BlockBody>), SparseBlockError>;

    fn next(&mut self) -> Option<Self::Item> {
        let slot = self.slots.next()?;
        match slot {
            Ok(slot) => {
                let block = self.archive.get_block_by_slot(&slot);
                Some(block.map(|b| (slot, b)).map_err(SparseBlockError::from))
            }
            Err(e) => Some(Err(SparseBlockError::from(e))),
        }
    }
}

impl<I: IndexStore, A: ArchiveStore> DoubleEndedIterator for SparseBlockIter<I, A> {
    fn next_back(&mut self) -> Option<Self::Item> {
        let slot = self.slots.next_back()?;
        match slot {
            Ok(slot) => {
                let block = self.archive.get_block_by_slot(&slot);
                Some(block.map(|b| (slot, b)).map_err(SparseBlockError::from))
            }
            Err(e) => Some(Err(SparseBlockError::from(e))),
        }
    }
}
