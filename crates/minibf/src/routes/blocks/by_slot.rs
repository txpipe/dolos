use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{ArchiveStore as _, Domain};

use crate::{error::Error, routes::PathInteger, Facade};

use super::{single_block_content, tip_block};

pub async fn by_slot<D>(
    Path(slot_number): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let slot = PathInteger::parse(&slot_number)
        .ok_or(Error::SlotNumberNotInteger)?
        .in_range()
        .ok_or(Error::InvalidSlotNumber)?;

    let block = domain
        .archive()
        .get_block_by_slot(&slot)
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
    use crate::routes::blocks::testing::{assert_status, error_mismatch};
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
        let not_integer = "params/slot_number must be integer";
        let out_of_range = "Missing, out of range or malformed slot_number.";
        let mut mismatches = Vec::new();

        for (slot, message) in [
            ("x", not_integer),
            ("1.5", not_integer),
            ("-1", out_of_range),
            ("2147483648", out_of_range),
            ("99999999999999999999", out_of_range),
        ] {
            let path = format!("/blocks/slot/{slot}");
            mismatches.extend(error_mismatch(&app, &path, StatusCode::BAD_REQUEST, message).await);
        }

        assert!(mismatches.is_empty(), "{mismatches:#?}");
    }

    #[tokio::test]
    async fn blocks_by_slot_not_found() {
        let app = TestApp::new();
        let mismatch = error_mismatch(
            &app,
            "/blocks/slot/1",
            StatusCode::NOT_FOUND,
            "The requested component has not been found.",
        )
        .await;

        assert_eq!(mismatch, None);
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
