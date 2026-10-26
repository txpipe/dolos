use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_withdrawal_content_inner::AccountWithdrawalContentInner;
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::{AccountState, ProposalState},
    ChainSummary,
};
use dolos_core::Domain;
use futures_util::StreamExt;
use pallas::ledger::{addresses::StakeAddress, traverse::MultiEraBlock};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{account_exists, parse_account_key_param};

fn find_withdrawals_in_block(
    stake_address: &StakeAddress,
    chain: &ChainSummary,
    pagination: &Pagination,
    block: &[u8],
) -> Result<Vec<AccountWithdrawalContentInner>, StatusCode> {
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let account = stake_address.to_vec();

    let mut matches = vec![];

    for (idx, tx) in block.txs().iter().enumerate() {
        if pagination.should_skip(block.number(), idx) {
            continue;
        }

        let withdrawals = tx.withdrawals();
        let withdrawals: Vec<_> = withdrawals.collect();

        if let Some(amount) = withdrawals
            .into_iter()
            .find_map(|(address, amount)| (address == account.as_slice()).then_some(amount))
        {
            matches.push(AccountWithdrawalContentInner {
                tx_hash: tx.hash().to_string(),
                amount: amount.to_string(),
                tx_slot: block.slot() as i32,
                block_time: chain.slot_time(block.slot()) as i32,
                block_height: block.number() as i32,
            });
        }
    }

    if matches!(pagination.order, Order::Desc) {
        matches.reverse();
    }

    Ok(matches)
}

pub async fn by_stake_withdrawals<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountWithdrawalContentInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;
    if !account_exists(&domain, &account_key.account)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stream = domain.query().blocks_by_account_withdrawals_stream(
        &account_key.address.to_vec(),
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut items = Vec::new();
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let mut withdrawals =
            find_withdrawals_in_block(&account_key.address, &chain, &pagination, &block)
                .map_err(Error::Code)?;
        items.append(&mut withdrawals);

        if items.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let items = items
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_withdrawal_content_inner::AccountWithdrawalContentInner;

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account withdrawals");

        let expected = &app.vectors().account_withdrawals;
        assert_eq!(items.len(), expected.len());
        for (item, expected) in items.iter().zip(expected.iter()) {
            assert_eq!(item.tx_hash, expected.tx_hash);
            assert_eq!(item.amount, expected.amount.to_string());
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/withdrawals?page=1&count=1");
        let path_page_2 = format!("/accounts/{stake_address}/withdrawals?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse account withdrawals page 1");
        let page_2: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse account withdrawals page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].tx_hash, page_2[0].tx_hash);
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse withdrawals asc");
        if items.len() < 2 {
            return;
        }

        let positions: Vec<_> = items
            .iter()
            .map(|x| app.vectors().tx_position(&x.tx_hash))
            .collect();
        assert!(positions.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse withdrawals desc");
        if items.len() < 2 {
            return;
        }

        let positions: Vec<_> = items
            .iter()
            .map(|x| app.vectors().tx_position(&x.tx_hash))
            .collect();
        assert!(positions.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/withdrawals?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountWithdrawalContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account withdrawals");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/withdrawals", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/withdrawals", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_withdrawals_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/withdrawals");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
