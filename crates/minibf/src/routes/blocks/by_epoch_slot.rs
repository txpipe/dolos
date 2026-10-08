use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{ArchiveStore as _, Domain};

use crate::{error::Error, routes::PathInteger, Facade};

use super::{single_block_content, tip_block};

/// Dolos stores blocks by absolute slot, so the epoch-relative pair is
/// converted with the chain summary first.
pub async fn by_epoch_slot<D>(
    Path((epoch_number, slot_number)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    // Blockfrost checks the type of both numbers before the range of either.
    let epoch = PathInteger::parse(&epoch_number).ok_or(Error::EpochNumberNotInteger)?;
    let slot = PathInteger::parse(&slot_number).ok_or(Error::SlotNumberNotInteger)?;
    let epoch = epoch.in_range().ok_or(Error::InvalidEpochNumber)?;
    let slot = slot.in_range().ok_or(Error::InvalidSlotNumber)?;

    let chain = domain.get_chain_summary()?;

    // a slot past the epoch end must not roll into the next epoch
    let epoch_length = chain.epoch_start(epoch + 1) - chain.epoch_start(epoch);
    if slot >= epoch_length {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let absolute_slot = chain.epoch_start(epoch) + slot;

    let block = domain
        .archive()
        .get_block_by_slot(&absolute_slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tip = tip_block(&domain)?;
    let model = single_block_content(&domain, &block, &tip, &chain).await?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::{assert_status, error_mismatch};
    use crate::test_support::{TestApp, TestFault};

    /// `/blocks/{hash}` gives the epoch and epoch-slot the test resolves.
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

        // empty slot inside the epoch
        let path = format!("/blocks/epoch/{epoch}/slot/80000");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // slot past the epoch end
        let path = format!("/blocks/epoch/{epoch}/slot/2000000000");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        // epoch with no blocks
        let path = "/blocks/epoch/500/slot/0";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_by_epoch_slot_bad_request() {
        let app = TestApp::new();
        let epoch_not_integer = "params/epoch_number must be integer";
        let epoch_out_of_range = "Missing, out of range or malformed epoch_number.";
        let slot_not_integer = "params/slot_number must be integer";
        let slot_out_of_range = "Missing, out of range or malformed slot_number.";
        let mut mismatches = Vec::new();

        for (epoch, slot, message) in [
            ("x", "0", epoch_not_integer),
            ("1.5", "0", epoch_not_integer),
            ("2", "x", slot_not_integer),
            ("-1", "0", epoch_out_of_range),
            ("2", "-5", slot_out_of_range),
            ("2147483648", "0", epoch_out_of_range),
            ("2", "2147483648", slot_out_of_range),
            // Blockfrost checks both types before either range.
            ("x", "x", epoch_not_integer),
            ("-1", "x", slot_not_integer),
            ("-1", "-1", epoch_out_of_range),
        ] {
            let path = format!("/blocks/epoch/{epoch}/slot/{slot}");
            mismatches.extend(error_mismatch(&app, &path, StatusCode::BAD_REQUEST, message).await);
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
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
