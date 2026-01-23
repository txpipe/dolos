use dolos_core::{
    ArchiveError, BlockBody, BlockSlot, EraCbor, IndexStore as CoreIndexStore, StateError, TxHash,
    TxOrder, UtxoSet,
};
use pallas::{crypto::hash::Hash, ledger::primitives::conway::PlutusData};
use redb::ReadableDatabase;

use crate::{archive, state, Error};

#[derive(Clone)]
pub struct IndexStore {
    state: state::StateStore,
    archive: archive::ArchiveStore,
}

impl IndexStore {
    pub fn new(state: state::StateStore, archive: archive::ArchiveStore) -> Self {
        Self { state, archive }
    }
}

impl CoreIndexStore for IndexStore {
    type SparseBlockIter = archive::ArchiveSparseIter;

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError> {
        let rx = self.state.db().begin_read().map_err(Error::from)?;
        let out = state::utxoset::FilterIndexes::get_by_address(&rx, address)?;
        Ok(out)
    }

    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, StateError> {
        let rx = self.state.db().begin_read().map_err(Error::from)?;
        let out = state::utxoset::FilterIndexes::get_by_payment(&rx, payment)?;
        Ok(out)
    }

    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, StateError> {
        let rx = self.state.db().begin_read().map_err(Error::from)?;
        let out = state::utxoset::FilterIndexes::get_by_stake(&rx, stake)?;
        Ok(out)
    }

    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError> {
        let rx = self.state.db().begin_read().map_err(Error::from)?;
        let out = state::utxoset::FilterIndexes::get_by_policy(&rx, policy)?;
        Ok(out)
    }

    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError> {
        let rx = self.state.db().begin_read().map_err(Error::from)?;
        let out = state::utxoset::FilterIndexes::get_by_asset(&rx, asset)?;
        Ok(out)
    }

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError> {
        self.archive
            .get_block_by_hash(block_hash)
            .map_err(ArchiveError::from)
    }

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError> {
        self.archive
            .get_block_by_number(number)
            .map_err(ArchiveError::from)
    }

    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, ArchiveError> {
        self.archive
            .get_block_with_tx(tx_hash)
            .map_err(ArchiveError::from)
    }

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, ArchiveError> {
        self.archive.get_tx(tx_hash).map_err(ArchiveError::from)
    }

    fn get_plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, ArchiveError> {
        self.archive
            .get_plutus_data(datum_hash)
            .map_err(ArchiveError::from)
    }

    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        self.archive
            .get_slot_for_tx(tx_hash)
            .map_err(ArchiveError::from)
    }

    fn get_tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, ArchiveError> {
        self.archive
            .get_tx_by_spent_txo(spent_txo)
            .map_err(ArchiveError::from)
    }

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        self.archive
            .iter_possible_blocks_with_address(address, start_slot, end_slot)
            .map_err(ArchiveError::from)
    }

    fn iter_blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        self.archive
            .iter_possible_blocks_with_asset(asset, start_slot, end_slot)
            .map_err(ArchiveError::from)
    }

    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        self.archive
            .iter_possible_blocks_with_payment(payment, start_slot, end_slot)
            .map_err(ArchiveError::from)
    }

    fn iter_blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        self.archive
            .iter_possible_blocks_with_stake(stake, start_slot, end_slot)
            .map_err(ArchiveError::from)
    }

    fn iter_blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        self.archive
            .iter_possible_blocks_with_account_certs(account, start_slot, end_slot)
            .map_err(ArchiveError::from)
    }

    fn iter_blocks_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError> {
        self.archive
            .iter_possible_blocks_with_metadata(metadata, start_slot, end_slot)
            .map_err(ArchiveError::from)
    }
}
