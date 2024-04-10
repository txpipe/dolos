use pallas::{crypto::hash::Hash, network::miniprotocols::Point};
use serde::Deserialize;

pub type BlockSlot = u64;
pub type BlockHeight = u64;
pub type BlockHash = Hash<32>;
pub type RawBlock = Vec<u8>;
pub type TxHash = Hash<32>;
pub type OutputIdx = u64;
pub type UtxoBody = (u16, Vec<u8>);

// #[derive(Debug, Clone)]
// pub struct BlockWithContext {
//     slot: BlockSlot,
//     hash: BlockHash,
//     raw: RawBlock,
//     utxos: HashMap<UtxoRef, UtxoBody>,
// }

#[derive(Debug, Clone)]
pub enum PullEvent {
    RollForward(BlockSlot, BlockHash, RawBlock),
    Rollback(Point),
}

#[derive(Debug, Clone)]
pub enum RollEvent {
    Apply(BlockSlot, BlockHash, RawBlock),
    Undo(BlockSlot, BlockHash, RawBlock),
    Origin,
}

#[derive(Deserialize)]
pub struct UpstreamConfig {
    pub peer_address: String,
    pub network_magic: u64,
}
