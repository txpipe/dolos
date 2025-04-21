pub mod txs;

use axum::{extract::State, http::StatusCode, Json};

use crate::serve::minibf::{routes::blocks::Block, SharedState};

pub async fn route(State(state): State<SharedState>) -> Result<Json<Block>, StatusCode> {
    let tip = state
        .chain
        .get_tip()
        .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    match tip {
        None => Err(StatusCode::SERVICE_UNAVAILABLE),
        Some((_, body)) => Ok(Json(Block::from_body(
            &body,
            &state.chain,
            &state.ledger,
            &state.genesis,
        )?)),
    }
}
