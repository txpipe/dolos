use pallas::ledger::traverse::{MultiEraBlock, MultiEraHeader, MultiEraUpdate};
use rocket::http::Status;
use serde::{Deserialize, Serialize};

use crate::{
    ledger::pparams::{self, EraSummary, Genesis},
    state::LedgerStore,
    wal::{redb::WalStore, ReadUtils, WalReader},
};

pub mod hash_or_number;
pub mod latest;
pub mod slot;

pub type BlockHeaderFields = (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
);

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
        ledger: &LedgerStore,
        hash_or_number: &str,
        genesis: &Genesis,
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
                let slot = block.slot();

                let tip = ledger.cursor().map_err(|_| Status::InternalServerError)?;
                let updates = ledger
                    .get_pparams(tip.map(|t| t.0).unwrap_or_default())
                    .map_err(|_| Status::InternalServerError)?
                    .into_iter()
                    .map(|eracbor| {
                        MultiEraUpdate::try_from(eracbor).map_err(|_| Status::InternalServerError)
                    })
                    .collect::<Result<Vec<MultiEraUpdate>, Status>>()?;
                let summary = pparams::fold_with_hacks(genesis, &updates, slot);

                let (previous_block, block_vrf, op_cert, op_cert_counter) =
                    Self::extract_from_header(&block.header())?;
                let (epoch, epoch_slot, time) =
                    Self::resolve_time_from_genesis(&slot, summary.era_for_slot(slot));
                Ok(Some(Self {
                    slot: Some(block.slot()),
                    hash: block.hash().to_string(),
                    tx_count: block.tx_count() as u64,
                    size: block.body_size().unwrap_or(0) as u64,
                    epoch: Some(epoch),
                    epoch_slot: Some(epoch_slot),
                    height: Some(block.number()),
                    next_block: next.clone(),
                    time,
                    confirmations,
                    previous_block,
                    block_vrf,
                    op_cert,
                    op_cert_counter,
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

    /// Resolve epoch, epoch slot and block time using Genesis values.
    pub fn resolve_time_from_genesis(slot: &u64, summary: &EraSummary) -> (u64, u64, u64) {
        let era_slot = slot - summary.start.slot;
        let era_epoch = era_slot / summary.pparams.epoch_length();
        let epoch_slot = era_slot % summary.pparams.epoch_length();
        let epoch = summary.start.epoch + era_epoch;
        let time = summary.start.timestamp.timestamp() as u64
            + (slot - summary.start.slot) * summary.pparams.slot_length();
        (epoch, epoch_slot, time)
    }

    pub fn extract_from_header(header: &MultiEraHeader) -> Result<BlockHeaderFields, Status> {
        let prev = header.previous_hash().map(|h| h.to_string());
        let block_vrf = match header.vrf_vkey() {
            Some(v) => Some(
                bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("vrf_vk").unwrap(), v)
                    .map_err(|_| Status::ServiceUnavailable)?,
            ),
            None => None,
        };

        let (op_cert, op_cert_counter) = match header {
            MultiEraHeader::ShelleyCompatible(x) => (
                Some(hex::encode(
                    x.header_body.operational_cert_hot_vkey.as_slice(),
                )),
                Some(x.header_body.operational_cert_sequence_number.to_string()),
            ),
            MultiEraHeader::BabbageCompatible(x) => (
                Some(hex::encode(
                    x.header_body
                        .operational_cert
                        .operational_cert_hot_vkey
                        .as_slice(),
                )),
                Some(
                    x.header_body
                        .operational_cert
                        .operational_cert_sequence_number
                        .to_string(),
                ),
            ),
            _ => (None, None),
        };
        Ok((prev, block_vrf, op_cert, op_cert_counter))
    }
}
