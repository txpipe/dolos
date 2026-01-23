use pallas::{crypto::hash::Hash, ledger::primitives::conway::PlutusData};
use thiserror::Error;

use crate::{
    BlockBody, BlockSlot, ChainPoint, EraCbor, SlotTags, TxHash, TxOrder, UtxoSet, UtxoSetDelta,
};

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("index db error: {0}")]
    DbError(String),

    #[error("codec error: {0}")]
    CodecError(String),

    #[error("schema error: {0}")]
    SchemaError(String),
}

#[trait_variant::make(Send)]
pub trait IndexStore: Clone + Send + Sync + 'static {
    type SparseBlockIter: Iterator<Item = Result<(BlockSlot, Option<BlockBody>), IndexError>>
        + DoubleEndedIterator;

    fn initialize_schema(&self) -> Result<(), IndexError>;
    fn copy(&self, target: &Self) -> Result<(), IndexError>;

    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), IndexError>;
    fn apply_archive_indexes(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError>;
    fn undo_archive_indexes(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), IndexError>;

    fn get_utxo_by_address(&self, address: &[u8]) -> Result<UtxoSet, IndexError>;
    fn get_utxo_by_payment(&self, payment: &[u8]) -> Result<UtxoSet, IndexError>;
    fn get_utxo_by_stake(&self, stake: &[u8]) -> Result<UtxoSet, IndexError>;
    fn get_utxo_by_policy(&self, policy: &[u8]) -> Result<UtxoSet, IndexError>;
    fn get_utxo_by_asset(&self, asset: &[u8]) -> Result<UtxoSet, IndexError>;

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, IndexError>;
    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, IndexError>;
    fn get_block_with_tx(&self, tx_hash: &[u8])
        -> Result<Option<(BlockBody, TxOrder)>, IndexError>;
    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, IndexError>;
    fn get_plutus_data(&self, datum_hash: &Hash<32>) -> Result<Option<PlutusData>, IndexError>;
    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError>;
    fn get_tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, IndexError>;

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError>;
    fn iter_blocks_with_asset(
        &self,
        asset: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError>;
    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError>;
    fn iter_blocks_with_stake(
        &self,
        stake: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError>;
    fn iter_blocks_with_account_certs(
        &self,
        account: &[u8],
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError>;
    fn iter_blocks_with_metadata(
        &self,
        metadata: &u64,
        start_slot: BlockSlot,
        end_slot: BlockSlot,
    ) -> Result<Self::SparseBlockIter, IndexError>;
}
