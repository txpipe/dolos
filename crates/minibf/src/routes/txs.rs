use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use blockfrost_openapi::models::{tx_content::TxContent, tx_content_cbor::TxContentCbor};
use dolos_core::{ArchiveStore as _, Domain};

use crate::{Facade, mapping::IntoModel as _};

pub async fn tx_hash<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContent>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let raw = domain
        .archive()
        .get_block_with_tx(&hash)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let ctx = domain.get_context()?;

    raw.into_response(&ctx)
}

pub async fn tx_hash_cbor<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContentCbor>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let raw = domain
        .archive()
        .get_block_with_tx(&hash)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let ctx = domain.get_context()?;

    raw.into_response(&ctx)
}
