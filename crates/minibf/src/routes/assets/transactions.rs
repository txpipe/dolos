use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset_transactions_inner::AssetTransactionsInner;
use dolos_cardano::indexes::{AsyncCardanoQueryExt, SlotOrder};
use dolos_core::Domain;
use futures_util::StreamExt;

use crate::{
    error::Error,
    inputs::InputDeps,
    mapping::assets::resolve_asset,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::find_txs;

pub async fn by_subject_transactions<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetTransactionsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (subject, _) = resolve_asset(&domain, &subject).await?;

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let stream = domain.query().blocks_by_asset_stream(
        &subject,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = Vec::new();
    let mut deps = InputDeps::default();
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let mut txs = find_txs(&domain, &mut deps, &subject, &chain, &pagination, block)
            .await
            .map_err(Error::Code)?;
        matches.append(&mut txs);

        if matches.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let transactions = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(transactions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::assets::testing::*,
        test_support::{TestApp, TestFault},
    };

    #[tokio::test]
    async fn assets_by_subject_transactions_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let items: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset transactions");
        assert!(!items.is_empty());

        // every tx must sit exactly where the synthetic chain placed it
        for item in items {
            let (block_number, tx_index) = app.vectors().tx_position(&item.tx_hash);
            assert_eq!(item.block_height as u64, block_number);
            assert_eq!(item.tx_index as usize, tx_index);
        }
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_slot_constrained() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/assets/{asset}/transactions?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset transactions");
        assert!(!items.is_empty());
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_paginated() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path_page_1 = format!("/assets/{asset}/transactions?page=1&count=2");
        let path_page_2 = format!("/assets/{asset}/transactions?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse transactions page 1");
        let page_2: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse transactions page 2");

        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 2);

        let page_1_hashes: std::collections::HashSet<_> =
            page_1.into_iter().map(|x| x.tx_hash).collect();
        let page_2_hashes: std::collections::HashSet<_> =
            page_2.into_iter().map(|x| x.tx_hash).collect();
        assert!(page_1_hashes.is_disjoint(&page_2_hashes));
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_order_asc() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions asc");
        assert!(!asc.is_empty());
        let asc_pos: Vec<_> = asc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] < w[1]));
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_order_desc() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions desc");
        assert!(!desc.is_empty());
        let desc_pos: Vec<_> = desc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] > w[1]));
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/transactions");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/transactions", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_transactions_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/transactions");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
