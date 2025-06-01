use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use dolos_core::Domain;

use super::Block;

pub mod addresses;
pub mod next;
pub mod previous;
pub mod txs;

pub async fn route<D: Domain>(
    Path(hash_or_number): Path<String>,
    State(domain): State<D>,
) -> Result<Json<Block>, StatusCode> {
    Ok(Json(Block::find_in_chain(&domain, &hash_or_number)?))
}
