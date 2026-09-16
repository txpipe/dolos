use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    block_content::BlockContent, block_content_txs_cbor_inner::BlockContentTxsCborInner,
};
use dolos_core::Domain;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{paged_block_txs, single_block_content, tip_block};

pub async fn latest<D>(State(domain): State<Facade<D>>) -> Result<Json<BlockContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tip = tip_block(&domain)?;
    let chain = domain.get_chain_summary()?;

    let model = single_block_content(&domain, &tip, &chain).await?;

    Ok(Json(model))
}

pub async fn latest_txs<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let tip = tip_block(&domain)?;

    Ok(Json(paged_block_txs(&tip, &pagination)?))
}

pub async fn latest_txs_cbor<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContentTxsCborInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let tip = tip_block(&domain)?;

    Ok(Json(paged_block_txs(&tip, &pagination)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::assert_status;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn blocks_latest_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/blocks/latest").await;

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
    async fn blocks_latest_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/blocks/latest", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn blocks_latest_txs_order_asc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs?order=asc").await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asc txs");

        assert_eq!(txs, block.tx_hashes);
    }

    #[tokio::test]
    async fn blocks_latest_txs_order_desc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs?order=desc").await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse desc txs");

        let mut reversed = block.tx_hashes.clone();
        reversed.reverse();
        assert_eq!(txs, reversed);
    }

    #[tokio::test]
    async fn blocks_latest_txs_cbor_happy_path() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs/cbor").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let txs: Vec<BlockContentTxsCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse latest txs cbor");

        let hashes: Vec<String> = txs.iter().map(|tx| tx.tx_hash.clone()).collect();
        assert_eq!(hashes, block.tx_hashes);

        for tx in txs {
            let cbor = hex::decode(&tx.cbor).expect("cbor is not valid hex");
            let decoded = pallas::ledger::traverse::MultiEraTx::decode(&cbor)
                .expect("cbor is not a decodable tx");
            assert_eq!(decoded.hash().to_string(), tx.tx_hash);
        }
    }

    #[tokio::test]
    async fn blocks_latest_txs_cbor_order_desc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs/cbor?order=desc").await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<BlockContentTxsCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse desc latest txs cbor");

        let hashes: Vec<String> = txs.iter().map(|tx| tx.tx_hash.clone()).collect();
        let mut reversed = block.tx_hashes.clone();
        reversed.reverse();
        assert_eq!(hashes, reversed);
    }

    #[tokio::test]
    async fn blocks_latest_txs_cbor_paginated() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app
            .get_bytes("/blocks/latest/txs/cbor?count=1&page=2")
            .await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<BlockContentTxsCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse paginated latest txs cbor");

        let expected: Vec<String> = block.tx_hashes.iter().skip(1).take(1).cloned().collect();
        let hashes: Vec<String> = txs.iter().map(|tx| tx.tx_hash.clone()).collect();
        assert_eq!(hashes, expected);
    }

    #[tokio::test]
    async fn blocks_latest_txs_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(
            &app,
            "/blocks/latest/txs/cbor",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
