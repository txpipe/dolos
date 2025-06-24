use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use pallas::ledger::traverse::MultiEraBlock;

use dolos_core::Domain;

use crate::{routes::blocks::hash_or_number_to_body, Facade};

pub async fn route<D: Domain>(
    Path(hash_or_number): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let body = hash_or_number_to_body(&hash_or_number, domain.archive())?;
    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        block.txs().iter().map(|tx| tx.hash().to_string()).collect(),
    ))
}
