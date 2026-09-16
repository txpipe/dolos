use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{ArchiveStore as _, Domain};

use crate::Facade;

use super::single_block_content;

pub async fn by_slot<D>(
    Path(slot_number): Path<u64>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = domain
        .archive()
        .get_block_by_slot(&slot_number)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let chain = domain.get_chain_summary()?;

    let model = single_block_content(&domain, &block, &chain).await?;

    Ok(Json(model))
}
