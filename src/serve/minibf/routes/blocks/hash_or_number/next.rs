use pallas::ledger::traverse::{MultiEraBlock, MultiEraUpdate};
use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    chain::ChainStore,
    ledger::pparams::{self, Genesis},
    serve::minibf::{
        common::{Order, Pagination},
        routes::blocks::hash_or_number_to_body,
    },
    state::LedgerStore,
};

use super::Block;

#[get("/blocks/<hash_or_number>/next?<count>&<page>&<order>", rank = 2)]
pub fn route(
    hash_or_number: String,
    count: Option<u8>,
    page: Option<u64>,
    order: Option<Order>,
    genesis: &State<Arc<Genesis>>,
    chain: &State<ChainStore>,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Vec<Block>>, Status> {
    let pagination = Pagination::try_new(count, page, order)?;

    let tip_number = match chain.get_tip().map_err(|_| Status::ServiceUnavailable)? {
        None => return Err(Status::ServiceUnavailable),
        Some((_, body)) => MultiEraBlock::decode(&body)
            .map_err(|_| Status::ServiceUnavailable)?
            .number(),
    };

    let body = hash_or_number_to_body(&hash_or_number, chain)?;
    let curr = MultiEraBlock::decode(&body).map_err(|_| Status::ServiceUnavailable)?;

    let mut iterator = chain
        .get_range(Some(curr.slot()), None)
        .map_err(|_| Status::ServiceUnavailable)?;
    let _ = iterator.next(); // Discard first.

    let mut output = vec![];

    // Now we found it, continue forward
    let mut i = 0;
    for (_, body) in iterator {
        i += 1;
        if pagination.includes(i) {
            let block = MultiEraBlock::decode(&body).map_err(|_| Status::ServiceUnavailable)?;
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
