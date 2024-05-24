use itertools::Itertools;
use pallas::network::miniprotocols::Point as PallasPoint;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod reader;
mod stream;
mod writer;

pub type BlockSlot = u64;
pub type BlockHash = pallas::crypto::hash::Hash<32>;
pub type BlockEra = pallas::ledger::traverse::Era;
pub type ChainTip = pallas::network::miniprotocols::chainsync::Tip;
pub type BlockBody = Vec<u8>;
pub type BlockHeader = Vec<u8>;
pub type LogSeq = u64;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ChainPoint {
    Origin,
    Specific(BlockSlot, BlockHash),
}

impl From<PallasPoint> for ChainPoint {
    fn from(value: PallasPoint) -> Self {
        match value {
            PallasPoint::Origin => ChainPoint::Origin,
            PallasPoint::Specific(s, h) => ChainPoint::Specific(s, h.as_slice().into()),
        }
    }
}

impl From<ChainPoint> for PallasPoint {
    fn from(value: ChainPoint) -> Self {
        match value {
            ChainPoint::Origin => PallasPoint::Origin,
            ChainPoint::Specific(s, h) => PallasPoint::Specific(s, h.to_vec()),
        }
    }
}

impl From<&LogValue> for ChainPoint {
    fn from(value: &LogValue) -> Self {
        match value {
            LogValue::Apply(RawBlock { slot, hash, .. }) => ChainPoint::Specific(*slot, *hash),
            LogValue::Undo(RawBlock { slot, hash, .. }) => ChainPoint::Specific(*slot, *hash),
            LogValue::Mark(x) => x.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RawBlock {
    pub slot: BlockSlot,
    pub hash: BlockHash,
    pub era: BlockEra,
    pub body: BlockBody,
}

impl From<&RawBlock> for ChainPoint {
    fn from(value: &RawBlock) -> Self {
        let RawBlock { slot, hash, .. } = value;
        ChainPoint::Specific(*slot, *hash)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogValue {
    //Origin,
    Apply(RawBlock),
    Undo(RawBlock),
    Mark(ChainPoint),
}

pub type LogEntry = (LogSeq, LogValue);

#[derive(Debug, Error)]
pub enum WalError {
    #[error("point not found in chain {0:?}")]
    PointNotFound(ChainPoint),

    #[error("IO error")]
    IO(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub use reader::{ReadUtils, WalReader};
pub use stream::WalStream;
pub use writer::WalWriter;

pub mod redb;
