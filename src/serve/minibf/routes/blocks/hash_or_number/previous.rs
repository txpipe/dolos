use pallas::ledger::traverse::{wellknown::GenesisValues, MultiEraBlock};
use rocket::{get, http::Status, State};

use crate::{
    serve::minibf::common::{Order, Pagination},
    wal::{redb::WalStore, ReadUtils, WalReader},
};

use super::Block;

#[get("/blocks/<hash_or_number>/previous?<count>&<page>&<order>", rank = 2)]
pub fn route(
    hash_or_number: String,
    count: Option<u8>,
    page: Option<u64>,
    order: Option<Order>,
    genesis: &State<GenesisValues>,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Vec<Block>>, Status> {
    let pagination = Pagination::try_new(count, page, order)?;

    let tip_number = match wal.find_tip().map_err(|_| Status::ServiceUnavailable)? {
        None => return Err(Status::ServiceUnavailable),
        Some((_, point)) => {
            let raw = wal
                .read_block(&point)
                .map_err(|_| Status::ServiceUnavailable)?;

            MultiEraBlock::decode(&raw.body)
                .map_err(|_| Status::ServiceUnavailable)?
                .number()
        }
    };

    let iterator = wal
        .crawl_from(None)
        .map_err(|_| Status::ServiceUnavailable)?
        .rev()
        .into_blocks();

    let mut found = false;
    let mut i = 0;
    let mut output = vec![];
    for raw in iterator.flatten() {
        let block = MultiEraBlock::decode(&raw.body).map_err(|_| Status::ServiceUnavailable)?;

        if found {
            i += 1;
            if pagination.includes(i) {
                let block =
                    MultiEraBlock::decode(&raw.body).map_err(|_| Status::ServiceUnavailable)?;
                let header = block.header();
                let block_vrf = match header.vrf_vkey() {
                    Some(v) => Some(
                        bech32::encode::<bech32::Bech32>(bech32::Hrp::parse("vrf_vk").unwrap(), v)
                            .map_err(|_| Status::ServiceUnavailable)?,
                    ),
                    None => None,
                };

                let (epoch, epoch_slot) = block.epoch(genesis);
                output.push(Block {
                    slot: Some(block.slot()),
                    hash: block.hash().to_string(),
                    tx_count: block.tx_count() as u64,
                    size: block.body_size().unwrap_or(0) as u64,
                    epoch: Some(epoch),
                    epoch_slot: Some(epoch_slot),
                    height: Some(block.number()),
                    previous_block: block.header().previous_hash().map(hex::encode),
                    time: block.wallclock(genesis),
                    confirmations: tip_number - block.number(),
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
                })
            } else if i > pagination.to() {
                break;
            }
        }

        if block.hash().to_string() == hash_or_number
            || block.number().to_string() == hash_or_number
        {
            found = true;
        }
    }

    if found {
        let output = match pagination.order {
            Order::Asc => output.into_iter().rev().collect(),
            Order::Desc => output,
        };
        Ok(rocket::serde::json::Json(output))
    } else {
        Err(Status::NotFound)
    }
}
