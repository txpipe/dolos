use pallas::ledger::traverse::{wellknown::GenesisValues, MultiEraBlock};
use rocket::http::Status;
use serde::{Deserialize, Serialize};

use crate::wal::{redb::WalStore, ReadUtils, WalReader};

pub mod hash_or_number;
pub mod latest;
pub mod slot;

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

impl Block {
    pub fn find_in_wal(
        wal: &WalStore,
        hash_or_number: &str,
        genesis: &GenesisValues,
    ) -> Result<Option<Block>, Status> {
        let iterator = wal
            .crawl_from(None)
            .map_err(|_| Status::ServiceUnavailable)?
            .rev()
            .into_blocks();

        let mut curr = None;
        let mut next = None;
        let mut confirmations = 0;

        // Scan the iterator, if found set the current block and continue to set next and count
        // confirmations.
        for raw in iterator.flatten() {
            let block = MultiEraBlock::decode(&raw.body).map_err(|_| Status::ServiceUnavailable)?;
            if block.hash().to_string() == hash_or_number
                || block.number().to_string() == hash_or_number
            {
                curr = Some(raw.body);
                break;
            } else {
                next = Some(hex::encode(raw.hash));
                confirmations += 1;
            }
        }
        match curr {
            Some(bytes) => {
                // Decode the block due to lifetime headaches.
                let block =
                    MultiEraBlock::decode(&bytes).map_err(|_| Status::ServiceUnavailable)?;

                let header = block.header();
                let prev = header.previous_hash().map(|h| h.to_string());
                let block_vrf = match header.vrf_vkey() {
                    Some(v) => Some(
                        bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("vrf_vk").unwrap(), v)
                            .map_err(|_| Status::ServiceUnavailable)?,
                    ),
                    None => None,
                };
                let (epoch, epoch_slot) = block.epoch(genesis);
                Ok(Some(Self {
                    slot: Some(block.slot()),
                    hash: block.hash().to_string(),
                    tx_count: block.tx_count() as u64,
                    size: block.body_size().unwrap_or(0) as u64,
                    epoch: Some(epoch),
                    epoch_slot: Some(epoch_slot),
                    height: Some(block.number()),
                    previous_block: prev.clone(),
                    next_block: next.clone(),
                    time: block.wallclock(genesis),
                    confirmations,
                    block_vrf,
                    output: match block.tx_count() {
                        0 => None,
                        _ => Some(
                            block
                                .txs()
                                .iter()
                                .map(|tx| {
                                    tx.outputs().iter().map(|o| o.value().coin()).sum::<u64>()
                                })
                                .sum::<u64>()
                                .to_string(),
                        ),
                    },
                    fees: match block.tx_count() {
                        0 => None,
                        _ => Some(
                            block
                                .txs()
                                .iter()
                                .map(|tx| tx.fee().unwrap_or(0))
                                .sum::<u64>()
                                .to_string(),
                        ),
                    },
                    ..Default::default()
                }))
            }
            _ => Err(Status::ServiceUnavailable),
        }
    }
}
