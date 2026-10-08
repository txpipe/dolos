use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::Domain;

use crate::{error::Error, Facade};

use super::{main_block_at_slot, single_block_content, tip_block, PathNumber};

pub async fn by_slot<D>(
    Path(slot_number): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let slot_number = match PathNumber::parse(&slot_number) {
        PathNumber::NotInteger => return Err(Error::SlotNumberNotInteger),
        PathNumber::OutOfRange => return Err(Error::InvalidSlotNumber),
        PathNumber::Value(x) => x,
    };

    let block = main_block_at_slot(&domain, slot_number)?.ok_or(StatusCode::NOT_FOUND)?;

    let chain = domain.get_chain_summary()?;

    let tip = tip_block(&domain)?;
    let model = single_block_content(&domain, &block, &tip, &chain).await?;

    Ok(Json(model))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::*;
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

    #[tokio::test]
    async fn blocks_by_slot_skips_boundary_blocks() {
        let chain = mainnet_byron_chain(true);

        for slot in [0, 21600] {
            let block = get_block(&chain.app, &format!("/blocks/slot/{slot}")).await;
            assert_eq!(block.hash, chain.main[&slot].hash);
        }

        // a boundary block alone at its slot leaves the slot empty
        let chain = mainnet_byron_chain(false);
        assert_status(&chain.app, "/blocks/slot/0", StatusCode::NOT_FOUND).await;
    }

    /// Bodies checked against live Blockfrost on 2026-10-08.
    #[tokio::test]
    async fn blocks_by_slot_errors_match_blockfrost() {
        let app = TestApp::new();
        let bad = StatusCode::BAD_REQUEST;

        assert_error(&app, "/blocks/slot/abc", bad, SLOT_NOT_INTEGER).await;
        assert_error(&app, "/blocks/slot/1.5", bad, SLOT_NOT_INTEGER).await;
        assert_error(&app, "/blocks/slot/-1", bad, BAD_SLOT).await;
        assert_error(&app, "/blocks/slot/2147483648", bad, BAD_SLOT).await;
        assert_error(&app, "/blocks/slot/99999999999999999999", bad, BAD_SLOT).await;
        assert_error(&app, "/blocks/slot/1", StatusCode::NOT_FOUND, NOT_FOUND).await;
    }

    /// A genesis delegate keeps minting after the first Shelley epoch, until
    /// the decentralisation parameter reaches zero. Leaders checked against
    /// live Blockfrost on 2026-10-08.
    #[tokio::test]
    async fn blocks_by_slot_names_genesis_delegates_as_slot_leaders() {
        let blocks: Vec<dolos_core::RawBlock> =
            include_str!("../../../testdata/preview-shelley-blocks.txt")
                .lines()
                .filter(|line| !line.starts_with('#'))
                .map(|line| {
                    let (_, cbor) = line.split_once(' ').unwrap();
                    std::sync::Arc::new(hex::decode(cbor).unwrap())
                })
                .collect();

        let app =
            TestApp::new_with_archived_blocks(dolos_cardano::include::preview::load(), &blocks);

        let block = get_block(&app, "/blocks/slot/86400").await;
        assert_eq!(block.epoch, Some(1));
        assert_eq!(block.slot_leader, "ShelleyGenesis-7c54a168c731f2f4");

        let block = get_block(&app, "/blocks/slot/172836").await;
        assert_eq!(
            block.slot_leader,
            "pool1grvqd4eu354qervmr62uew0nsrjqedx5kglldeqr4c29vv59rku"
        );
    }
}
