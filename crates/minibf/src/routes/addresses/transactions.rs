use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::address_transactions_content_inner::AddressTransactionsContentInner;
use dolos_cardano::indexes::SlotOrder;
use dolos_core::Domain;
use futures_util::StreamExt;

use crate::{
    error::Error,
    inputs::InputDeps,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{blocks_for_address_stream, find_txs, is_address_in_chain};

pub async fn transactions<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressTransactionsContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let address_str = address.clone();
    let (stream, address) = blocks_for_address_stream(
        &domain,
        &address,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    )?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = Vec::new();
    let mut deps = InputDeps::default();

    let mut stream = Box::pin(stream);
    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

        let Some(block) = block else {
            continue;
        };

        let mut txs = find_txs(&domain, &mut deps, &address, &chain, &pagination, block)
            .await
            .map_err(Error::Code)?;
        matches.append(&mut txs);

        if matches.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let transactions: Vec<AddressTransactionsContentInner> = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    if transactions.is_empty() {
        let exists = is_address_in_chain(&domain, &address_str).await?;

        if !exists {
            return Err(StatusCode::NOT_FOUND.into());
        }
    }

    Ok(Json(transactions))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::addresses::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::address_transactions_content_inner::AddressTransactionsContentInner;

    #[tokio::test]
    async fn addresses_transactions_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address transactions");
    }

    #[tokio::test]
    async fn addresses_transactions_slot_constrained() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/addresses/{address}/transactions?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address transactions");
        assert!(!items.is_empty());
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn addresses_transactions_paginated() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path_page_1 = format!("/addresses/{address}/transactions?page=1&count=2");
        let path_page_2 = format!("/addresses/{address}/transactions?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse transactions page 1");
        let page_2: Vec<AddressTransactionsContentInner> =
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
    async fn addresses_transactions_order_asc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn addresses_transactions_order_desc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn addresses_transactions_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/transactions", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_transactions_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/transactions", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_transactions_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
