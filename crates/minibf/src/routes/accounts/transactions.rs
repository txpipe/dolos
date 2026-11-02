use std::collections::BTreeSet;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_transactions_content_inner::AccountTransactionsContentInner;
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::{AccountState, ProposalState},
    ChainSummary,
};
use dolos_core::Domain;
use futures_util::StreamExt;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use crate::{
    error::Error,
    inputs::{for_each_touched_output, InputDeps, InputResolver},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{account_exists, address_belongs_to_account, parse_account_key_param};

fn account_addresses_in_tx(
    resolver: &mut InputResolver<'_>,
    account: &[u8],
    tx: &MultiEraTx<'_>,
) -> Result<BTreeSet<String>, StatusCode> {
    let mut addresses = BTreeSet::new();

    // never stops early: every touched output that belongs to the account
    // contributes its address to the set.
    for_each_touched_output(resolver, tx, |output| {
        let candidate = output
            .address()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if address_belongs_to_account(&candidate, account) {
            addresses.insert(candidate.to_string());
        }

        Ok(false)
    })?;

    Ok(addresses)
}

async fn find_account_txs_in_block<D>(
    domain: &Facade<D>,
    deps: &mut InputDeps,
    account: &[u8],
    chain: &ChainSummary,
    pagination: &Pagination,
    block: &[u8],
) -> Result<Vec<AccountTransactionsContentInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let txs = block.txs();

    // only the txs that will actually be scanned contribute dependencies
    let scanned = txs
        .iter()
        .enumerate()
        .filter(|(idx, _)| !pagination.should_skip(block.number(), *idx))
        .map(|(_, tx)| tx);

    let mut resolver = deps.prepare(domain, scanned).await?;

    let mut matches = vec![];

    for (idx, tx) in txs.iter().enumerate() {
        if pagination.should_skip(block.number(), idx) {
            continue;
        }

        let addresses = account_addresses_in_tx(&mut resolver, account, tx)?;

        let tx_hash = hex::encode(tx.hash().as_slice());
        let block_height = block.number() as i32;
        let block_time = chain.slot_time(block.slot()) as i32;

        for address in addresses {
            matches.push(AccountTransactionsContentInner {
                address,
                tx_hash: tx_hash.clone(),
                tx_index: idx as i32,
                block_height,
                block_time,
            });
        }
    }

    if matches!(pagination.order, Order::Desc) {
        matches.reverse();
    }

    Ok(matches)
}

pub async fn by_stake_transactions<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountTransactionsContentInner>>, Error>
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

    let account = account_key.address.to_vec();

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let stream = domain.query().blocks_by_stake_stream(
        &account,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );

    let mut matches = Vec::new();
    let mut deps = InputDeps::default();
    let mut stream = Box::pin(stream);

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let Some(block) = block else {
            continue;
        };

        let mut txs =
            find_account_txs_in_block(&domain, &mut deps, &account, &chain, &pagination, &block)
                .await?;
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
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_transactions_content_inner::AccountTransactionsContentInner;

    #[tokio::test]
    async fn accounts_by_stake_transactions_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let items: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account transactions");
        assert!(!items.is_empty());
        for item in &items {
            assert!(!item.address.is_empty());
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_slot_constrained() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/accounts/{stake_address}/transactions?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account transactions");
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_paginated() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path_page_1 = format!("/accounts/{stake_address}/transactions?page=1&count=2");
        let path_page_2 = format!("/accounts/{stake_address}/transactions?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse transactions page 1");
        let page_2: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse transactions page 2");

        let key = |x: &AccountTransactionsContentInner| (x.tx_hash.clone(), x.address.clone());
        let page_1_keys: std::collections::HashSet<_> = page_1.iter().map(key).collect();
        let page_2_keys: std::collections::HashSet<_> = page_2.iter().map(key).collect();
        assert!(page_1_keys.is_disjoint(&page_2_keys));
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_order_asc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_order_desc() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AccountTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/transactions", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/transactions", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/transactions");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
