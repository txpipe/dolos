use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{ArchiveStore as _, Domain};

use crate::Facade;

use super::{single_block_content, tip_block};

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

    let tip = tip_block(&domain)?;
    let model = single_block_content(&domain, &block, &tip, &chain).await?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::assert_status;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn blocks_by_slot_happy_path() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");

        let (status, bytes) = app.get_bytes(&format!("/blocks/slot/{}", block.slot)).await;
        assert_eq!(status, StatusCode::OK);

        let model: BlockContent = serde_json::from_slice(&bytes).expect("failed to parse block");
        assert_eq!(model.hash, block.block_hash);
        assert_eq!(model.slot, Some(block.slot as i32));
    }

    #[tokio::test]
    async fn blocks_by_slot_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/blocks/slot/x", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn blocks_by_slot_not_found() {
        let app = TestApp::new();
        assert_status(&app, "/blocks/slot/1", StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_by_slot_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(
            &app,
            "/blocks/slot/172800",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
