use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{
    BlockBody, BlockSlot, BrokenInvariant, ChainPoint, EraCbor, RawBlock, TxHash, TxOrder,
};

#[derive(Debug, Error)]
pub enum ArchiveError {
    #[error("broken invariant")]
    BrokenInvariant(#[from] BrokenInvariant),

    #[error("storage error")]
    InternalError(#[from] Box<dyn std::error::Error + Send + Sync>),

    #[error("address decoding error")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("query not supported")]
    QueryNotSupported,

    #[error("invalid store version")]
    InvalidStoreVersion,

    #[error("decoding error")]
    DecodingError(#[from] pallas::codec::minicbor::decode::Error),

    #[error("block decoding error")]
    BlockDecodingError(#[from] pallas::ledger::traverse::Error),
}

pub type OpaqueTag = Vec<u8>;

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct SlotTags {
    pub number: Option<u64>,
    pub tx_hashes: Vec<OpaqueTag>,
    pub scripts: Vec<OpaqueTag>,
    pub datums: Vec<OpaqueTag>,
    pub policies: Vec<OpaqueTag>,
    pub assets: Vec<OpaqueTag>,
    pub full_addresses: Vec<OpaqueTag>,
    pub payment_addresses: Vec<OpaqueTag>,
    pub stake_addresses: Vec<OpaqueTag>,
    pub spent_txo: Vec<OpaqueTag>,
    pub account_certs: Vec<OpaqueTag>,
}

pub trait ArchiveWriter: Send + Sync + 'static {
    fn apply(
        &self,
        point: &ChainPoint,
        block: &RawBlock,
        tags: &SlotTags,
    ) -> Result<(), ArchiveError>;

    fn undo(&self, point: &ChainPoint, tags: &SlotTags) -> Result<(), ArchiveError>;

    fn commit(self) -> Result<(), ArchiveError>;
}

pub trait ArchiveStore: Clone + Send + Sync + 'static {
    type BlockIter<'a>: Iterator<Item = (BlockSlot, BlockBody)> + DoubleEndedIterator + 'a;
    type SparseBlockIter: Iterator<Item = Result<(BlockSlot, Option<BlockBody>), ArchiveError>>
        + DoubleEndedIterator;
    type Writer: ArchiveWriter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError>;

    fn get_block_by_hash(&self, block_hash: &[u8]) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_by_number(&self, number: &u64) -> Result<Option<BlockBody>, ArchiveError>;

    fn get_block_with_tx(
        &self,
        tx_hash: &[u8],
    ) -> Result<Option<(BlockBody, TxOrder)>, ArchiveError>;

    fn get_tx(&self, tx_hash: &[u8]) -> Result<Option<EraCbor>, ArchiveError>;

    fn get_slot_for_tx(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError>;

    fn get_tx_by_spent_txo(&self, spent_txo: &[u8]) -> Result<Option<TxHash>, ArchiveError>;

    fn iter_blocks_with_address(
        &self,
        address: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_asset(&self, asset: &[u8]) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_payment(
        &self,
        payment: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_stake(&self, stake: &[u8]) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn iter_blocks_with_account(
        &self,
        account: &[u8],
    ) -> Result<Self::SparseBlockIter, ArchiveError>;

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError>;

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError>;

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError>;

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError>;

    fn truncate_front(&self, after: BlockSlot) -> Result<(), ArchiveError>;
}
