use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use pallas::ledger::traverse::MultiEraBlock;

use crate::serve::minibf::{routes::blocks::hash_or_number_to_body, SharedState};

pub async fn route(
    Path(hash_or_number): Path<String>,
    State(state): State<SharedState>,
) -> Result<Json<Vec<String>>, StatusCode> {
    let body = hash_or_number_to_body(&hash_or_number, &state.chain)
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        block.txs().iter().map(|tx| tx.hash().to_string()).collect(),
    ))
}
