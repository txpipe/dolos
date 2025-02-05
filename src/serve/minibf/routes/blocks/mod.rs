use pallas::ledger::traverse::MultiEraBlock;
use serde::{Deserialize, Serialize};

use crate::wal::RawBlock;

pub mod latest;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Block {
    pub slot: Option<u64>,
    pub hash: String,
    pub tx_count: u64,
    pub size: u64,

    pub time: u64,
    pub height: Option<u64>,
    pub epoch: Option<u64>,
    pub epoch_slot: Option<u64>,
    pub slot_leader: String,
    pub output: Option<String>,
    pub fees: Option<String>,
    pub block_vrf: Option<String>,
    pub op_cert: Option<String>,
    pub op_cert_counter: Option<String>,
    pub previous_block: Option<String>,
    pub next_block: Option<String>,
    pub confirmations: u64,
}

impl From<&RawBlock> for Block {
    fn from(raw_block: &RawBlock) -> Self {
        let block = MultiEraBlock::decode(&raw_block.body).unwrap();
        Self {
            slot: Some(block.slot()),
            hash: block.hash().to_string(),
            tx_count: block.tx_count() as u64,
            size: block.size() as u64,
            // height: Some(block.epoch(genesis) as u64),
            ..Default::default()
        }
    }
}
