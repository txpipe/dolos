use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::Domain;
use itertools::Either;

use crate::{error::Error, Facade};

use super::{
    genesis, load_block_by_hash_or_number, parse_hash_or_number, single_block_content, tip_block,
};

pub async fn by_hash_or_number<D>(
    Path(hash_or_number): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;

    let block = match load_block_by_hash_or_number(&domain, &hash_or_number).await {
        Ok(block) => block,
        Err(Error::Code(StatusCode::NOT_FOUND)) => {
            if let Either::Left(hash) = &hash_or_number {
                if genesis::is_genesis_hash(&domain, hash).map_err(Error::Code)? {
                    if let Some(block) = genesis::genesis_block(&domain).map_err(Error::Code)? {
                        return Ok(Json(block));
                    }
                }
            }

            return Err(Error::Code(StatusCode::NOT_FOUND));
        }
        Err(e) => return Err(e),
    };

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
    async fn blocks_by_hash_or_number_happy_path() {
        let app = TestApp::new();
        let block_hash = app.vectors().block_hash.as_str();
        let path = format!("/blocks/{block_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: BlockContent =
            serde_json::from_slice(&bytes).expect("failed to parse block content");
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_bad_request() {
        let app = TestApp::new();
        let path = format!("/blocks/{}", invalid_block());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_not_found() {
        let app = TestApp::new();
        let path = format!("/blocks/{}", missing_block());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = "/blocks/1".to_string();
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_genesis() {
        let app = TestApp::new();
        let first = app.vectors().blocks.first().expect("missing block vectors");

        let (status, bytes) = app.get_bytes(&format!("/blocks/{GENESIS_HASH}")).await;
        assert_eq!(status, StatusCode::OK);

        let genesis: BlockContent =
            serde_json::from_slice(&bytes).expect("failed to parse genesis block");
        assert_eq!(genesis.hash, GENESIS_HASH);
        assert_eq!(genesis.height, None);
        assert_eq!(genesis.previous_block, None);

        // the genesis block has no number, so block 0 is only ever a real block
        assert_status(&app, "/blocks/0", StatusCode::NOT_FOUND).await;

        // the first real block links back to genesis
        let block = get_blocks(&app, &format!("/blocks/{}/next", GENESIS_HASH)).await;
        assert_eq!(block[0].hash, first.block_hash);
        assert_eq!(block[0].previous_block.as_deref(), Some(GENESIS_HASH));
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_byron_slots_and_heights() {
        let chain = mainnet_byron_chain(true);

        // a main block keeps a zero slot and epoch slot
        let block = get_block(&chain.app, &format!("/blocks/{}", chain.main[&0].hash)).await;
        assert_eq!(block.slot, Some(0));
        assert_eq!(block.epoch_slot, Some(0));
        assert_eq!(block.height, Some(1));

        // a boundary block has none, wherever it sits
        for ebb in [&chain.ebb_0, &chain.ebb_1] {
            let block = get_block(&chain.app, &format!("/blocks/{}", ebb.hash)).await;
            assert_eq!(block.slot, None);
            assert_eq!(block.epoch_slot, None);
            assert_eq!(block.height, None);
            assert_eq!(block.slot_leader, "Epoch boundary slot leader");
        }
    }

    #[tokio::test]
    async fn blocks_by_number_skips_boundary_blocks() {
        let chain = mainnet_byron_chain(true);

        // boundary block 0 holds number 0, which db-sync gives no block
        assert_status(&chain.app, "/blocks/0", StatusCode::NOT_FOUND).await;

        let block = get_block(&chain.app, "/blocks/1").await;
        assert_eq!(block.hash, chain.main[&0].hash);

        // boundary block 1 repeats the number of the block before it
        let last = &chain.main[&21599];
        assert_eq!(chain.ebb_1.number, last.number);

        let block = get_block(&chain.app, &format!("/blocks/{}", last.number)).await;
        assert_eq!(block.hash, last.hash);

        let block = get_block(&chain.app, &format!("/blocks/{}", last.number + 1)).await;
        assert_eq!(block.hash, chain.main[&21600].hash);
    }
}
