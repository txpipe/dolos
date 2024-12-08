use crate::ledger;
use itertools::Itertools;
use pallas::network::miniprotocols::Point as PallasPoint;
use serde::{Deserialize, Serialize};
use thiserror::Error;

mod reader;
mod stream;
mod writer;

// A concrete implementation of the WAL using Redb
pub mod redb;

#[cfg(test)]
pub mod testing;

pub type BlockSlot = u64;
pub type BlockHash = pallas::crypto::hash::Hash<32>;
pub type BlockEra = pallas::ledger::traverse::Era;
pub type ChainTip = pallas::network::miniprotocols::chainsync::Tip;
pub type BlockBody = Vec<u8>;
pub type BlockHeader = Vec<u8>;
pub type LogSeq = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ChainPoint {
    Origin,
    Specific(BlockSlot, BlockHash),
}

impl PartialEq for ChainPoint {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Specific(l0, l1), Self::Specific(r0, r1)) => l0 == r0 && l1 == r1,
            (Self::Origin, Self::Origin) => true,
            _ => false,
        }
    }
}

impl From<ledger::ChainPoint> for ChainPoint {
    fn from(value: ledger::ChainPoint) -> Self {
        crate::wal::ChainPoint::Specific(value.0, value.1)
    }
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

impl From<&RawBlock> for ChainPoint {
    fn from(value: &RawBlock) -> Self {
        let RawBlock { slot, hash, .. } = value;
        ChainPoint::Specific(*slot, *hash)
    }
}

impl From<&LogValue> for ChainPoint {
    fn from(value: &LogValue) -> Self {
        match value {
            LogValue::Apply(x) => ChainPoint::from(x),
            LogValue::Undo(x) => ChainPoint::from(x),
            LogValue::Mark(x) => x.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RawBlock {
    pub slot: BlockSlot,
    pub hash: BlockHash,
    pub era: BlockEra,
    pub body: BlockBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogValue {
    Apply(RawBlock),
    Undo(RawBlock),
    Mark(ChainPoint),
}

impl PartialEq for LogValue {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Apply(l0), Self::Apply(r0)) => l0 == r0,
            (Self::Undo(l0), Self::Undo(r0)) => l0 == r0,
            (Self::Mark(l0), Self::Mark(r0)) => l0 == r0,
            _ => false,
        }
    }
}

pub type LogEntry = (LogSeq, LogValue);

#[derive(Debug, Error)]
pub enum WalError {
    #[error("wal is not empty")]
    NotEmpty,

    #[error("point not found in chain {0:?}")]
    PointNotFound(ChainPoint),

    #[error("slot not found in chain {0}")]
    SlotNotFound(BlockSlot),

    #[error("IO error")]
    IO(#[source] Box<dyn std::error::Error + Send + Sync>),
}

pub use reader::{ReadUtils, WalReader};
pub use stream::WalStream;
pub use writer::WalWriter;

#[cfg(test)]
mod tests {
    use super::*;

    fn slot_to_hash(slot: u64) -> BlockHash {
        let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
        hasher.input(&(slot as i32).to_le_bytes());
        hasher.finalize()
    }

    #[test]
    fn chainpoint_partial_eq() {
        assert_eq!(ChainPoint::Origin, ChainPoint::Origin);

        assert_eq!(
            ChainPoint::Specific(20, slot_to_hash(20)),
            ChainPoint::Specific(20, slot_to_hash(20))
        );

        assert_ne!(
            ChainPoint::Origin,
            ChainPoint::Specific(20, slot_to_hash(20))
        );

        assert_ne!(
            ChainPoint::Specific(20, slot_to_hash(20)),
            ChainPoint::Specific(50, slot_to_hash(50)),
        );

        assert_ne!(
            ChainPoint::Specific(50, slot_to_hash(20)),
            ChainPoint::Specific(50, slot_to_hash(50)),
        );
    }
}
