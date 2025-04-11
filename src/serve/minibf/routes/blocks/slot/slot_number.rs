use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

use crate::serve::minibf::{routes::blocks::Block, SharedState};

pub async fn route(
    Path(slot_number): Path<u64>,
    State(state): State<SharedState>,
) -> Result<Json<Block>, StatusCode> {
    let body = state
        .chain
        .get_block_by_slot(&slot_number)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match body {
        Some(body) => Ok(Json(Block::from_body(
            &body,
            &state.chain,
            &state.ledger,
            &state.genesis,
        )?)),
        _ => Err(StatusCode::NOT_FOUND),
    }
}
