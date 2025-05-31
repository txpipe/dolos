use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraUpdate};

use dolos_cardano::pparams;

use crate::serve::minibf::{
    common::{Order, Pagination, PaginationParameters},
    routes::blocks::{hash_or_number_to_body, BlockHeaderFields},
    SharedState,
};

use super::Block;

pub async fn route(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(state): State<SharedState>,
) -> Result<Json<Vec<Block>>, StatusCode> {
    let pagination = Pagination::try_from(params)?;
    let tip_number = match state
        .chain
        .get_tip()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?
    {
        None => return Err(StatusCode::SERVICE_UNAVAILABLE),
        Some((_, body)) => MultiEraBlock::decode(&body)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .number(),
    };

    let body = hash_or_number_to_body(&hash_or_number, &state.chain)?;
    let curr = MultiEraBlock::decode(&body).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let mut iterator = state
        .chain
        .get_range(Some(curr.slot()), None)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let _ = iterator.next(); // Discard first.

    let mut output = vec![];

    // Now we found it, continue forward
    let mut i = 0;
    for (_, body) in iterator {
        i += 1;
        if pagination.includes(i) {
            let block =
                MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let slot = block.slot();
            let tip = state
                .ledger
                .cursor()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let updates = state
                .ledger
                .get_pparams(tip.map(|t| t.0).unwrap_or_default())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .into_iter()
                .map(|eracbor| {
                    MultiEraUpdate::try_from(eracbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
                })
                .collect::<Result<Vec<MultiEraUpdate>, StatusCode>>()?;
            let summary = pparams::fold_with_hacks(&state.genesis, &updates, slot);

            let BlockHeaderFields {
                previous_block,
                block_vrf,
                op_cert,
                op_cert_counter,
                slot_leader,
            } = Block::extract_from_header(&block.header())?;
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
                slot_leader,
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

    Ok(Json(output))
}
