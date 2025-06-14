use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use pallas::ledger::traverse::MultiEraBlock;

use dolos_core::Domain;

use crate::{Facade, routes::blocks::hash_or_number_to_body};

pub async fn route<D: Domain>(
    Path(hash_or_number): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let body = hash_or_number_to_body(&hash_or_number, domain.archive())
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        block.txs().iter().map(|tx| tx.hash().to_string()).collect(),
    ))
}
