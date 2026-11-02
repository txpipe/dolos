use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::indexes::SlotOrder;
use dolos_core::Domain;
use futures_util::StreamExt;

use crate::{
    error::Error,
    inputs::InputDeps,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{blocks_for_address_stream, find_txs};

pub async fn txs<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let (stream, address) = blocks_for_address_stream(
        &domain,
        &address,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    )?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = Vec::new();
    let mut deps = InputDeps::default();

    let mut stream = Box::pin(stream);
    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let Some(block) = block else {
            continue;
        };

        let mut txs = find_txs(&domain, &mut deps, &address, &chain, &pagination, block)
            .await
            .map_err(Error::Code)?;
        matches.append(&mut txs);

        if matches.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let transactions = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .map(|x| x.tx_hash)
        .collect();

    Ok(Json(transactions))
}
