use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};

use dolos_core::{ArchiveStore as _, Domain};

use crate::{Facade, routes::blocks::Block};

pub async fn route<D: Domain>(
    Path(slot_number): Path<u64>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Block>, StatusCode> {
    let body = domain
        .archive()
        .get_block_by_slot(&slot_number)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    match body {
        Some(body) => Ok(Json(Block::from_body(&body, &*domain)?)),
        _ => Err(StatusCode::NOT_FOUND),
    }
}
