use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

use crate::serve::minibf::SharedState;

use super::Block;

pub mod addresses;
pub mod next;
pub mod previous;
pub mod txs;

pub async fn route(
    Path(hash_or_number): Path<String>,
    State(state): State<SharedState>,
) -> Result<Json<Block>, StatusCode> {
    Ok(Json(Block::find_in_chain(
        &state.chain,
        &state.ledger,
        &hash_or_number,
        &state.genesis,
    )?))
}
