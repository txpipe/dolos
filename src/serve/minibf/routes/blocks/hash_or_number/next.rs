use pallas::ledger::traverse::{MultiEraBlock, MultiEraUpdate};
use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    ledger::pparams::{self, Genesis},
    serve::minibf::common::{Order, Pagination},
    state::LedgerStore,
    wal::{redb::WalStore, ChainPoint, ReadUtils, WalReader},
};

use super::Block;

#[get("/blocks/<hash_or_number>/next?<count>&<page>&<order>", rank = 2)]
pub fn route(
    hash_or_number: String,
    count: Option<u8>,
    page: Option<u64>,
    order: Option<Order>,
    genesis: &State<Arc<Genesis>>,
    wal: &State<WalStore>,
    ledger: &State<LedgerStore>,
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

    let mut logseq = None;
    for raw in iterator.flatten() {
        let block = MultiEraBlock::decode(&raw.body).map_err(|_| Status::ServiceUnavailable)?;
        if block.hash().to_string() == hash_or_number
            || block.number().to_string() == hash_or_number
        {
            logseq = wal
                .locate_point(&ChainPoint::Specific(block.slot(), block.hash()))
                .map_err(|_| Status::InternalServerError)?;
            break;
        }
    }

    let mut iterator = match logseq {
        Some(logseq) => wal
            .crawl_from(Some(logseq))
            .map_err(|_| Status::ServiceUnavailable)?
            .into_blocks(),
        None => return Err(Status::BadRequest),
    };
    let _ = iterator.next(); // Discard first.

    let mut output = vec![];

    // Now we found it, continue forward
    let mut i = 0;
    for raw in iterator.flatten() {
        i += 1;
        if pagination.includes(i) {
            let block = MultiEraBlock::decode(&raw.body).map_err(|_| Status::ServiceUnavailable)?;
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
                Block::extract_from_header(&block.header())?;
            let (epoch, epoch_slot, block_time) =
                Block::resolve_time_from_genesis(&slot, summary.era_for_slot(slot));
            output.push(Block {
                slot: Some(block.slot()),
                hash: block.hash().to_string(),
                tx_count: block.tx_count() as u64,
                size: block.body_size().unwrap_or(0) as u64,
                epoch: Some(epoch),
                epoch_slot: Some(epoch_slot),
                height: Some(block.number()),
                time: block_time,
                confirmations: tip_number - block.number(),
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
                            .map(|tx| tx.outputs().iter().map(|o| o.value().coin()).sum::<u64>())
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
    let output = match pagination.order {
        Order::Asc => output,
        Order::Desc => output.into_iter().rev().collect(),
    };

    Ok(rocket::serde::json::Json(output))
}
