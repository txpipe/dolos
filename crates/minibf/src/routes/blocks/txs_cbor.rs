use axum::{
    extract::{Path, Query, State},
    Json,
};
use blockfrost_openapi::models::block_content_txs_cbor_inner::BlockContentTxsCborInner;
use dolos_core::Domain;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{load_block_by_hash_or_number, paged_block_txs, parse_hash_or_number};

pub async fn by_hash_or_number_txs_cbor<D>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContentTxsCborInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
    let block = load_block_by_hash_or_number(&domain, &hash_or_number).await?;

    Ok(Json(paged_block_txs(&block, &pagination)?))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::{assert_status, invalid_block, missing_block};
    use crate::test_support::{TestApp, TestFault};
    use axum::http::StatusCode;

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_cbor_happy_path() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/blocks/{}/txs/cbor", block.block_hash);
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let txs: Vec<BlockContentTxsCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse txs cbor");

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
    async fn blocks_by_hash_or_number_txs_cbor_order_desc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/blocks/{}/txs/cbor?order=desc", block.block_hash);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<BlockContentTxsCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse desc txs cbor");

        let hashes: Vec<String> = txs.iter().map(|tx| tx.tx_hash.clone()).collect();
        let mut reversed = block.tx_hashes.clone();
        reversed.reverse();
        assert_eq!(hashes, reversed);
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_cbor_paginated() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/blocks/{}/txs/cbor?count=1&page=2", block.block_hash);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<BlockContentTxsCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse paginated txs cbor");

        let expected: Vec<String> = block.tx_hashes.iter().skip(1).take(1).cloned().collect();
        let hashes: Vec<String> = txs.iter().map(|tx| tx.tx_hash.clone()).collect();
        assert_eq!(hashes, expected);
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_cbor_bad_request() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/txs/cbor", invalid_block());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_cbor_not_found() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/txs/cbor", missing_block());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(
            &app,
            "/blocks/1/txs/cbor",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
