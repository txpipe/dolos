//! Query helpers that combine index lookups with archive data fetching.
//!
//! This module provides the `QueryHelpers` trait which extends `Domain` with
//! high-level query methods that join index lookups (returning slots) with
//! archive fetches (returning block data).

use pallas::{
    crypto::hash::Hash,
    ledger::{
        primitives::conway::{DatumOption, PlutusData},
        traverse::{ComputeHash, MultiEraBlock, OriginalHash},
    },
};

use crate::{
    ArchiveError, ArchiveStore, BlockBody, BlockSlot, ChainError, Domain, DomainError, EraCbor,
    IndexError, IndexStore, TxHash, TxOrder,
};

/// Extension trait providing high-level query helpers that combine
/// index lookups with archive data fetching.
///
/// This trait is automatically implemented for all types that implement `Domain`.
pub trait QueryHelpers: Domain {
    /// Get a block by its hash.
    fn block_by_hash(&self, hash: &[u8]) -> Result<Option<BlockBody>, DomainError>;

    /// Get a block by its number (height).
    fn block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, DomainError>;

    /// Get a block containing a transaction, along with the transaction's index in the block.
    fn block_with_tx(&self, tx_hash: &[u8]) -> Result<Option<(BlockBody, TxOrder)>, DomainError>;

    /// Get a transaction's CBOR encoding by its hash.
    fn tx_cbor(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, DomainError>;

    /// Get Plutus data by its datum hash.
    fn plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, DomainError>;

    /// Get the transaction hash that spent a given UTxO.
    fn tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, DomainError>;

    /// Iterate over blocks containing transactions involving an address.
    fn blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError>;

    /// Iterate over blocks containing transactions involving an asset.
    fn blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError>;

    /// Iterate over blocks containing transactions involving a payment credential.
    fn blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError>;

    /// Iterate over blocks containing transactions involving a stake credential.
    fn blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError>;

    /// Iterate over blocks containing certificates for an account.
    fn blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError>;

    /// Iterate over blocks containing transactions with a specific metadata label.
    fn blocks_with_metadata(
        &self,
        label: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError>;
}

impl<D: Domain> QueryHelpers for D {
    fn block_by_hash(&self, hash: &[u8]) -> Result<Option<BlockBody>, DomainError> {
        let slot = self.indexes().slot_for_block_hash(hash)?;
        match slot {
            Some(slot) => Ok(self.archive().get_block_by_slot(&slot)?),
            None => Ok(None),
        }
    }

    fn block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, DomainError> {
        let slot = self.indexes().slot_for_block_number(number)?;
        match slot {
            Some(slot) => Ok(self.archive().get_block_by_slot(&slot)?),
            None => Ok(None),
        }
    }

    fn block_with_tx(&self, tx_hash: &[u8]) -> Result<Option<(BlockBody, TxOrder)>, DomainError> {
        let slot = self.indexes().slot_for_tx_hash(tx_hash)?;
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
        let slot = self.indexes().slot_for_tx_hash(tx_hash)?;
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

    fn plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, DomainError> {
        let end_slot = self
            .archive()
            .get_tip()?
            .map(|(slot, _)| slot)
            .unwrap_or_default();

        let slots = self
            .indexes()
            .slots_for_datum_hash(datum_hash.as_slice(), 0, end_slot)?;

        for slot in slots {
            let Some(raw) = self.archive().get_block_by_slot(&slot)? else {
                continue;
            };

            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
            for tx in block.txs() {
                if let Some(plutus_data) = tx.find_plutus_data(datum_hash) {
                    return Ok(Some(plutus_data.clone().unwrap()));
                }

                for (_, output) in tx.produces() {
                    if let Some(DatumOption::Data(data)) = output.datum() {
                        if &data.original_hash() == datum_hash {
                            return Ok(Some(data.clone().unwrap().unwrap()));
                        }
                    }
                }

                for redeemer in tx.redeemers() {
                    if &redeemer.data().compute_hash() == datum_hash {
                        return Ok(Some(redeemer.data().clone()));
                    }
                }
            }
        }

        Ok(None)
    }

    fn tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, DomainError> {
        let end_slot = self
            .archive()
            .get_tip()?
            .map(|(slot, _)| slot)
            .unwrap_or_default();

        let slots = self.indexes().slots_for_spent_txo(spent_txo, 0, end_slot)?;

        for slot in slots {
            let Some(raw) = self.archive().get_block_by_slot(&slot)? else {
                continue;
            };

            let block = MultiEraBlock::decode(raw.as_slice())
                .map_err(|e| DomainError::ChainError(ChainError::DecodingError(e)))?;
            for tx in block.txs().iter() {
                for input in tx.inputs() {
                    let bytes: Vec<u8> = crate::TxoRef::from(&input).into();
                    if bytes.as_slice() == spent_txo {
                        return Ok(Some(tx.hash()));
                    }
                }
            }
        }

        Ok(None)
    }

    fn blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        let slots = self
            .indexes()
            .slots_with_address(address, start_slot, end_slot)?;
        Ok(SparseBlockIter::new(slots, self.archive().clone()))
    }

    fn blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        let slots = self
            .indexes()
            .slots_with_asset(asset, start_slot, end_slot)?;
        Ok(SparseBlockIter::new(slots, self.archive().clone()))
    }

    fn blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        let slots = self
            .indexes()
            .slots_with_payment(payment, start_slot, end_slot)?;
        Ok(SparseBlockIter::new(slots, self.archive().clone()))
    }

    fn blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        let slots = self
            .indexes()
            .slots_with_stake(stake, start_slot, end_slot)?;
        Ok(SparseBlockIter::new(slots, self.archive().clone()))
    }

    fn blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        let slots = self
            .indexes()
            .slots_with_account_certs(account, start_slot, end_slot)?;
        Ok(SparseBlockIter::new(slots, self.archive().clone()))
    }

    fn blocks_with_metadata(
        &self,
        label: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<SparseBlockIter<Self::Indexes, Self::Archive>, DomainError> {
        let slots = self
            .indexes()
            .slots_with_metadata(label, start_slot, end_slot)?;
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
