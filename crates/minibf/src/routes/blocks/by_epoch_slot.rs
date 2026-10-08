use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::Domain;

use crate::{error::Error, Facade};

use super::{main_block_at_slot, single_block_content, tip_block, PathNumber};

/// Dolos stores blocks by absolute slot, so the epoch-relative pair is
/// converted with the chain summary first.
pub async fn by_epoch_slot<D>(
    Path((epoch_number, slot_number)): Path<(String, String)>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let epoch = PathNumber::parse(&epoch_number);
    let slot = PathNumber::parse(&slot_number);

    // Blockfrost checks that both are integers before it checks either range
    let (epoch, slot) = match (epoch, slot) {
        (PathNumber::NotInteger, _) => return Err(Error::EpochNumberNotInteger),
        (_, PathNumber::NotInteger) => return Err(Error::SlotNumberNotInteger),
        (PathNumber::OutOfRange, _) => return Err(Error::InvalidEpochNumber),
        (_, PathNumber::OutOfRange) => return Err(Error::InvalidSlotNumber),
        (PathNumber::Value(epoch), PathNumber::Value(slot)) => (epoch, slot),
    };

    let chain = domain.get_chain_summary()?;

    // a slot past the epoch end must not roll into the next epoch
    let epoch_length = chain.epoch_start(epoch + 1) - chain.epoch_start(epoch);
    if slot >= epoch_length {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let absolute_slot = chain.epoch_start(epoch) + slot;

    let block = main_block_at_slot(&domain, absolute_slot)?.ok_or(StatusCode::NOT_FOUND)?;

    let tip = tip_block(&domain)?;
    let model = single_block_content(&domain, &block, &tip, &chain).await?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::*;
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

        for (epoch, slot) in [
            ("x", "0"),
            ("2", "x"),
            ("-1", "0"),
            ("2", "-5"),
            // past i32::MAX
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

    #[tokio::test]
    async fn blocks_by_epoch_slot_skips_boundary_blocks() {
        let chain = mainnet_byron_chain(true);

        for (epoch, slot) in [(0, 0), (1, 21600)] {
            let path = format!("/blocks/epoch/{epoch}/slot/0");
            let block = get_block(&chain.app, &path).await;
            assert_eq!(block.hash, chain.main[&slot].hash);
        }

        // a boundary block alone at its slot leaves the slot empty
        let chain = mainnet_byron_chain(false);
        assert_status(&chain.app, "/blocks/epoch/0/slot/0", StatusCode::NOT_FOUND).await;
    }

    /// Blockfrost checks that both numbers are integers before it checks
    /// either range. Bodies checked against live Blockfrost on 2026-10-08.
    #[tokio::test]
    async fn blocks_by_epoch_slot_errors_match_blockfrost() {
        let app = TestApp::new();
        let bad = StatusCode::BAD_REQUEST;
        let epoch_not_integer = "params/epoch_number must be integer";
        let bad_epoch = "Missing, out of range or malformed epoch_number.";

        let cases = [
            ("abc", "abc", epoch_not_integer),
            ("abc", "-1", epoch_not_integer),
            ("-1", "abc", SLOT_NOT_INTEGER),
            ("-1", "-1", bad_epoch),
            ("2147483648", "2147483648", bad_epoch),
            ("0", "2147483648", BAD_SLOT),
        ];

        for (epoch, slot, message) in cases {
            let path = format!("/blocks/epoch/{epoch}/slot/{slot}");
            assert_error(&app, &path, bad, message).await;
        }

        for path in ["/blocks/epoch/0/slot/1", "/blocks/epoch/0/slot/432000"] {
            assert_error(&app, path, StatusCode::NOT_FOUND, NOT_FOUND).await;
        }
    }
}
