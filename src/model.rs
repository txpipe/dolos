use pallas::{crypto::hash::Hash, network::miniprotocols::Point};

pub type BlockSlot = u64;
pub type BlockHash = Hash<32>;

pub enum ChainSyncEvent {
    RollForward(BlockSlot, BlockHash),
    Rollback(Point),
}
