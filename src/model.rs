use pallas::{crypto::hash::Hash, ledger::traverse::Era, network::miniprotocols::Point};

pub type BlockSlot = u64;
pub type BlockHash = Hash<32>;
pub type RawBlock = Vec<u8>;
pub type TxHash = Hash<32>;
pub type OutputIdx = u64;
pub type UtxoBody = (Era, Vec<u8>);

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
    Reset(Point),
}
