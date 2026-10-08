use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use dolos_core::{archive::Skippable as _, ArchiveStore as _, Domain};
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::epoch_slot_range;

pub async fn by_number_blocks<D: Domain>(
    Path(epoch): Path<u64>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error> {
    let chain = domain.get_chain_summary()?;
    let pagination = Pagination::try_from(params)?;
    let (start, end) = epoch_slot_range(&chain, epoch);

    let mut iter = domain
        .archive()
        .get_range(Some(start), Some(end))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Skip past pages using key-only traversal (no block data read).
    match pagination.order {
        Order::Asc => iter.skip_forward(pagination.skip()),
        Order::Desc => iter.skip_backward(pagination.skip()),
    }

    let decode = |(_slot, body): (_, Vec<u8>)| -> Result<String, StatusCode> {
        let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(block.hash().to_string())
    };

    Ok(Json(match pagination.order {
        Order::Asc => iter
            .take(pagination.count)
            .map(decode)
            .collect::<Result<_, StatusCode>>()?,
        Order::Desc => iter
            .rev()
            .take(pagination.count)
            .map(decode)
            .collect::<Result<_, _>>()?,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    /// Regression test: a block minted on the last slot of an epoch must
    /// appear when the endpoint lists that epoch's blocks.
    ///
    /// The bug: `get_range(from, to)` excludes `to`. The old code passed the
    /// epoch's last slot as `to`, so a block on that exact slot was dropped.
    /// A live comparison against Blockfrost caught this (preview, epoch 16).
    ///
    /// Setup: build 3 blocks on consecutive slots, placed so the last block
    /// lands exactly on the last slot of epoch 2.
    #[tokio::test]
    async fn epochs_blocks_include_the_epochs_final_slot() {
        use dolos_testing::synthetic::SyntheticBlockConfig;

        let boundary = TestApp::new().epoch_start(3);
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            slot: boundary - 3,
            block_count: 3,
            ..Default::default()
        });

        let final_slot = boundary - 1;
        let (status, bytes) = app.get_bytes(&format!("/blocks/slot/{final_slot}")).await;
        assert_eq!(status, StatusCode::OK, "expected a block on the final slot");
        let block: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        let boundary_hash = block["hash"].as_str().unwrap().to_string();

        let (_, bytes) = app.get_bytes("/epochs/2/blocks?count=100").await;
        let all: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert!(all.contains(&boundary_hash));

        let pool = toy_issuer_pool();
        let (_, bytes) = app
            .get_bytes(&format!("/epochs/2/blocks/{pool}?count=100"))
            .await;
        let by_pool: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert!(by_pool.contains(&boundary_hash));
    }

    #[tokio::test]
    async fn epochs_blocks_happy_path() {
        let app = TestApp::new();
        let hashes: Vec<String> =
            get_ok(&app, &format!("/epochs/{}/blocks", app.tip_epoch())).await;

        // The synthetic chain mints every block in the tip epoch.
        let expected: Vec<String> = app
            .vectors()
            .blocks
            .iter()
            .map(|x| x.block_hash.clone())
            .collect();
        assert_eq!(hashes, expected);
    }

    #[tokio::test]
    async fn epochs_blocks_paginated() {
        let app = TestApp::new();
        let path = format!("/epochs/{}/blocks", app.tip_epoch());
        let all: Vec<String> = get_ok(&app, &path).await;

        let mut paged = Vec::new();
        for page in 1..=3 {
            let hashes: Vec<String> = get_ok(&app, &format!("{path}?count=2&page={page}")).await;
            paged.extend(hashes);
        }

        assert_eq!(paged, all);
    }

    #[tokio::test]
    async fn epochs_blocks_desc_is_reversed_asc() {
        let app = TestApp::new();
        let path = format!("/epochs/{}/blocks", app.tip_epoch());
        let asc: Vec<String> = get_ok(&app, &format!("{path}?order=asc")).await;
        let mut desc: Vec<String> = get_ok(&app, &format!("{path}?order=desc")).await;

        desc.reverse();
        assert!(!asc.is_empty());
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn epochs_blocks_empty_epoch() {
        let app = TestApp::new();
        let hashes: Vec<String> = get_ok(&app, "/epochs/0/blocks").await;
        assert!(hashes.is_empty());
    }

    #[tokio::test]
    async fn epochs_blocks_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/not-a-number/blocks", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/epochs/0/blocks?count=0", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_blocks_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/epochs/0/blocks", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
