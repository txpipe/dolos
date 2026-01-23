use pallas::{crypto::hash::Hash, ledger::primitives::conway::PlutusData};

use crate::{ArchiveError, BlockBody, BlockSlot, EraCbor, StateError, TxHash, TxOrder, UtxoSet};

#[trait_variant::make(Send)]
pub trait IndexStore: Clone + Send + Sync + 'static {
    type SparseBlockIter: Iterator<Item = Result<(BlockSlot, Option<BlockBody>), ArchiveError>>
        + DoubleEndedIterator;

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, StateError>;
    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, StateError>;
    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, StateError>;
    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, StateError>;
    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, StateError>;

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError>;
    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError>;
    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, ArchiveError>;
    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, ArchiveError>;
    fn get_plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, ArchiveError>;
    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError>;
    fn get_tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, ArchiveError>;

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;
    fn iter_blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;
    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;
    fn iter_blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;
    fn iter_blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;
    fn iter_blocks_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, ArchiveError>;
}
