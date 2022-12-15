use pallas::{crypto::hash::Hash, network::miniprotocols::Point};

pub type BlockSlot = u64;
pub type BlockHash = Hash<32>;
pub type RawBlock = Vec<u8>;

#[derive(Debug, Clone)]
pub enum ChainSyncEvent {
    RollForward(BlockSlot, BlockHash),
    Rollback(Point),
}

#[derive(Debug, Clone)]
pub enum BlockFetchEvent {
    RollForward(BlockSlot, BlockHash, RawBlock),
    Rollback(Point),
}
