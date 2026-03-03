use serde::{Deserialize, Serialize};

use crate::prelude::*;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalSummary {
    pub start_slot: Option<BlockSlot>,
    pub tip_slot: Option<BlockSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveSummary {
    // TODO: archive interface needs a way to query from the start
    // pub start: Option<ChainPoint>,
    pub tip_slot: Option<BlockSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateSummary {
    pub tip_slot: Option<BlockSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSummary {
    pub tip_slot: Option<BlockSlot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataSummary {
    pub wal: WalSummary,
    pub archive: ArchiveSummary,
    pub state: StateSummary,
    pub indexes: IndexSummary,
}
