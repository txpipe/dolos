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

/// `GET /blocks/epoch/{epoch_number}/slot/{slot_number}`: the block at an
/// epoch-relative slot.
///
/// Blockfrost matches the pair against the epoch and epoch-slot columns it
/// stores per block. Dolos stores blocks by absolute slot, so the handler
/// turns the pair into an absolute slot with the chain summary and looks
/// that up.
pub async fn by_epoch_slot<D>(
    Path((epoch_number, slot_number)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    // Blockfrost bounds both numbers to a positive signed 32-bit range and
    // rejects anything else as a bad request.
    let in_range =
        |raw: &str| -> Option<u64> { raw.parse::<u64>().ok().filter(|x| *x <= i32::MAX as u64) };

    let epoch = in_range(&epoch_number).ok_or(StatusCode::BAD_REQUEST)?;
    let slot = in_range(&slot_number).ok_or(StatusCode::BAD_REQUEST)?;

    let chain = domain.get_chain_summary()?;

    // A slot past the end of the epoch names no block; without this guard
    // the absolute slot would land in a later epoch.
    let epoch_length = chain.epoch_start(epoch + 1) - chain.epoch_start(epoch);
    if slot >= epoch_length {
        return Err(StatusCode::NOT_FOUND);
    }

    let absolute_slot = chain.epoch_start(epoch) + slot;

    let block = domain
        .archive()
        .get_block_by_slot(&absolute_slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let model = single_block_content(&domain, &block, &chain).await?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::assert_status;
    use crate::test_support::{TestApp, TestFault};

    /// The block behind `/blocks/{hash}`, which carries its own epoch and
    /// epoch-slot fields — the pair the endpoint under test resolves.
    async fn block_by_hash(app: &TestApp, hash: &str) -> BlockContent {
        let (status, bytes) = app.get_bytes(&format!("/blocks/{hash}")).await;
        assert_eq!(status, StatusCode::OK);
        serde_json::from_slice(&bytes).expect("failed to parse block")
    }

    #[tokio::test]
    async fn blocks_by_epoch_slot_happy_path() {
        let app = TestApp::new();
        let expected = block_by_hash(&app, &app.vectors().block_hash).await;

        let epoch = expected.epoch.expect("block has no epoch");
        let epoch_slot = expected.epoch_slot.expect("block has no epoch slot");

        let path = format!("/blocks/epoch/{epoch}/slot/{epoch_slot}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let model: BlockContent = serde_json::from_slice(&bytes).expect("failed to parse block");
        assert_eq!(model, expected);
    }

    #[tokio::test]
    async fn blocks_by_epoch_slot_not_found() {
        let app = TestApp::new();
        let block = block_by_hash(&app, &app.vectors().block_hash).await;
        let epoch = block.epoch.expect("block has no epoch");

        // an empty slot inside the epoch
        let path = format!("/blocks/epoch/{epoch}/slot/80000");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // a slot past the end of the epoch must not roll into the next one
        let path = format!("/blocks/epoch/{epoch}/slot/2000000000");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // an epoch with no blocks
        let path = "/blocks/epoch/500/slot/0";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_by_epoch_slot_bad_request() {
        let app = TestApp::new();

        for (epoch, slot) in [
            ("x", "0"),
            ("2", "x"),
            ("-1", "0"),
            ("2", "-5"),
            // past the positive signed 32-bit range Blockfrost accepts
            ("2147483648", "0"),
            ("2", "2147483648"),
        ] {
            let path = format!("/blocks/epoch/{epoch}/slot/{slot}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn blocks_by_epoch_slot_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(
            &app,
            "/blocks/epoch/2/slot/0",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
