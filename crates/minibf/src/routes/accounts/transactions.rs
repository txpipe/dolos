use std::collections::HashMap;

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
use dolos_core::{Domain, UtxoPosition};
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
    position: UtxoPosition,
) -> Result<Vec<String>, StatusCode> {
    let mut addresses = HashMap::new();

    for_each_touched_output(resolver, tx, |output, output_position| {
        let candidate = output
            .address()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if address_belongs_to_account(&candidate, account) {
            let position = output_position.in_tx(position);
            addresses
                .entry(candidate.to_string())
                .and_modify(|earliest| *earliest = position.min(*earliest))
                .or_insert(position);
        }

        Ok(false)
    })?;

    let mut addresses: Vec<_> = addresses.into_iter().collect();
    addresses.sort_unstable_by_key(|(_, position)| *position);
    Ok(addresses.into_iter().map(|(address, _)| address).collect())
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

        let addresses = account_addresses_in_tx(
            &mut resolver,
            account,
            tx,
            UtxoPosition::of_tx(block.slot(), idx),
        )?;

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
        test_support::{stake_address_accounts, TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_transactions_content_inner::AccountTransactionsContentInner;
    use dolos_core::{ArchiveIndexDelta, ArchiveStore, ArchiveWriter, ChainPoint, TxHash};
    use dolos_testing::blocks::make_conway_block_with_txs_at;
    use pallas::{
        codec::utils::{KeepRaw, NonEmptySet},
        ledger::{
            addresses::Address,
            primitives::conway::{
                PostAlonzoTransactionOutput, TransactionBody, TransactionInput, TransactionOutput,
                Value,
            },
            traverse::ComputeHash,
        },
    };

    fn output(address: &[u8]) -> TransactionOutput<'static> {
        TransactionOutput::PostAlonzo(KeepRaw::from(PostAlonzoTransactionOutput {
            address: address.to_vec().into(),
            value: Value::Coin(1_000_000),
            datum_option: None,
            script_ref: None,
        }))
    }

    fn tx_body(addresses: &[Vec<u8>]) -> TransactionBody<'static> {
        TransactionBody {
            inputs: vec![].into(),
            outputs: addresses.iter().map(|address| output(address)).collect(),
            fee: 200_000,
            ttl: None,
            certificates: None,
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: None,
            required_signers: None,
            network_id: None,
            collateral_return: None,
            total_collateral: None,
            reference_inputs: None,
            voting_procedures: None,
            proposal_procedures: None,
            treasury_value: None,
            donation: None,
        }
    }

    async fn assert_output_position_order(valid: bool) {
        let app = TestApp::new();
        let domain = app.facade();
        let account = Address::from_bech32(&app.vectors().stake_address)
            .unwrap()
            .to_vec();
        let mut addresses: Vec<_> = (0..8)
            .map(|payment| {
                stake_address_accounts::base(&stake_address_accounts::primary(), payment)
            })
            .collect();
        addresses.sort_by_key(|address| Address::from_bytes(address).unwrap().to_string());
        addresses.reverse();
        let [a, b, c, d, e, f, reference_only, inactive] = addresses.as_slice() else {
            unreachable!()
        };
        let input = |transaction_id: TxHash, index| TransactionInput {
            transaction_id,
            index,
        };

        let oldest = tx_body(&[
            a.clone(),
            a.clone(),
            b.clone(),
            reference_only.clone(),
            inactive.clone(),
        ]);
        let oldest_hash = oldest.compute_hash();
        let mut collateral_source = tx_body(std::slice::from_ref(inactive));
        collateral_source.collateral_return = Some(output(c));
        let collateral_hash = collateral_source.compute_hash();
        let (_, older) = make_conway_block_with_txs_at(
            200_000,
            10,
            None,
            vec![(oldest, true), (collateral_source, false)],
            None,
        );

        let same_block = tx_body(std::slice::from_ref(d));
        let same_block_hash = same_block.compute_hash();
        let consumed = vec![
            input(same_block_hash, 0),
            input(collateral_hash, 1),
            input(oldest_hash, 2),
            input(oldest_hash, 1),
            input(oldest_hash, 0),
            input([0xff; 32].into(), 0),
        ];
        let foreign = stake_address_accounts::base(&stake_address_accounts::other(), 9);
        let mut spending = tx_body(&[e.clone(), a.clone(), f.clone(), e.clone(), foreign]);
        spending.reference_inputs = NonEmptySet::from_vec(vec![input(oldest_hash, 3)]);
        spending.collateral_return = Some(output(if valid { inactive } else { e }));
        if valid {
            spending.inputs = consumed.into();
            spending.collateral = NonEmptySet::from_vec(vec![input(oldest_hash, 4)]);
        } else {
            spending.inputs = vec![input(oldest_hash, 4)].into();
            spending.collateral = NonEmptySet::from_vec(consumed);
        }
        let spending_hash = spending.compute_hash().to_string();
        let (_, current) = make_conway_block_with_txs_at(
            200_010,
            11,
            None,
            vec![(same_block, true), (spending, valid)],
            None,
        );

        let writer = domain.archive().start_writer().unwrap();
        for raw in [&older, &current] {
            let block = MultiEraBlock::decode(raw).unwrap();
            writer
                .apply(&ChainPoint::Specific(block.slot(), block.hash()), raw)
                .unwrap();
            writer
                .apply_index(&[ArchiveIndexDelta {
                    slot: block.slot(),
                    block_hash: block.hash().to_vec(),
                    block_number: Some(block.number()),
                    tx_hashes: block.txs().iter().map(|tx| tx.hash().to_vec()).collect(),
                    tags: vec![],
                }])
                .unwrap();
        }
        writer.commit().unwrap();

        let expected: Vec<_> = addresses[..if valid { 6 } else { 5 }]
            .iter()
            .map(|address| Address::from_bytes(address).unwrap().to_string())
            .collect();
        let chain = domain.get_chain_summary().unwrap();
        let mut deps = InputDeps::default();
        for order in [Order::Asc, Order::Desc] {
            let pagination = Pagination {
                order,
                from: Some(crate::pagination::PaginationNumberAndIndex {
                    number: 11,
                    index: Some(1),
                }),
                ..Default::default()
            };
            let rows = find_account_txs_in_block(
                &domain,
                &mut deps,
                &account,
                &chain,
                &pagination,
                &current,
            )
            .await
            .unwrap();
            assert!(rows
                .iter()
                .all(|row| row.tx_hash == spending_hash && row.tx_index == 1));
            let actual: Vec<_> = rows.into_iter().map(|row| row.address).collect();
            let mut expected = expected.clone();
            if matches!(order, Order::Desc) {
                expected.reverse();
            }
            assert_eq!(actual, expected, "order={order:?}, valid={valid}");
            assert_eq!(deps.fetches(), 4);
        }
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_orders_addresses_by_output_position() {
        assert_output_position_order(true).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_transactions_orders_only_consumed_and_produced_collateral() {
        assert_output_position_order(false).await;
    }

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
