use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    asset::Asset, asset_addresses_inner::AssetAddressesInner, asset_policy_inner::AssetPolicyInner,
    asset_transactions_inner::AssetTransactionsInner,
};
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, CardanoStateIndexExt, SlotOrder},
    model::AssetState,
    periods::first_mints,
    ChainSummary,
};
use dolos_core::{
    ArchiveStore as _, BlockBody, BlockSlot, Domain, DomainError, StateStore as _, TxoRef,
};
use futures_util::StreamExt;
use itertools::Itertools;
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{MultiEraOutput, MultiEraTx},
};

use crate::{
    error::Error,
    inputs::{match_block_txs, tx_touches_output, InputDeps, InputResolver},
    mapping::assets::{load_asset_builder, output_has_subject, resolve_asset_state},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

mod all;
mod history;
pub use all::all;
pub use history::by_subject_history;

pub async fn by_subject<D>(
    Path(unit): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Asset>, Error>
where
    Option<AssetState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let model = load_asset_builder(&domain, unit).await?;

    Ok(Json(model.into_model(&domain).await?))
}

/// UTxOs of an asset read, decoded and located per blocking task. Each one
/// also costs an archive index lookup, so one chunk holds its permit for a
/// few milliseconds.
const ASSET_ADDRESS_CHUNK: usize = 256;

/// Per address: the earliest chain position of a UTxO holding the asset, and
/// the quantity held.
type AssetHoldings = HashMap<String, ((BlockSlot, u32), u128)>;

pub async fn by_subject_addresses<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetAddressesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let (asset, _) = resolve_asset_state(&domain, &subject)?;

    let addresses = asset_holdings(&domain, asset, ASSET_ADDRESS_CHUNK).await?;

    let mut items = addresses
        .into_iter()
        .sorted_by_key(|(_, (sort, _))| *sort)
        .map(|(address, (_, amount))| AssetAddressesInner {
            address,
            quantity: amount.to_string(),
        })
        .collect_vec();

    if matches!(pagination.order, Order::Desc) {
        items.reverse();
    }

    let sorted = items
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect_vec();

    Ok(Json(sorted))
}

/// The holdings of every address with a UTxO that holds `asset`, `chunk`
/// UTxOs per blocking task.
async fn asset_holdings<D>(
    domain: &Facade<D>,
    asset: Vec<u8>,
    chunk: usize,
) -> Result<AssetHoldings, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let asset = Arc::new(asset);

    let utxoset = {
        let asset = asset.clone();
        domain
            .blocking(move |domain| {
                domain
                    .state()
                    .utxos_by_asset(&asset)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .await?
    };

    let mut refs = utxoset.into_iter().collect_vec();
    refs.sort();

    let mut addresses = AssetHoldings::new();

    for refs in refs.chunks(chunk.max(1)) {
        let refs = refs.to_vec();
        let asset = asset.clone();

        let holdings = domain
            .blocking(move |domain| chunk_holdings(&domain, refs, &asset))
            .await?;

        for (address, (sort, amount)) in holdings {
            addresses
                .entry(address)
                .and_modify(|entry| {
                    entry.0 = entry.0.min(sort);
                    entry.1 += amount;
                })
                .or_insert((sort, amount));
        }
    }

    Ok(addresses)
}

fn chunk_holdings<D>(
    domain: &Facade<D>,
    refs: Vec<TxoRef>,
    asset: &[u8],
) -> Result<AssetHoldings, StatusCode>
where
    D: Domain,
{
    let utxos = domain
        .state()
        .get_utxos(refs)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut addresses = AssetHoldings::new();
    for (txoref, eracbor) in utxos {
        let sort = (
            domain
                .archive()
                .slot_by_tx_hash(txoref.0.as_slice())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
            txoref.1,
        );

        let utxo = MultiEraOutput::decode(eracbor.era().try_into().unwrap(), eracbor.cbor())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let amount = utxo
            .value()
            .assets()
            .iter()
            .flat_map(|x| {
                let subject = x.policy().to_vec();
                x.assets()
                    .iter()
                    .find(|x| [subject.as_slice(), x.name()].concat() == asset)
                    .map(|x| x.any_coin() as u128)
            })
            .sum();

        addresses
            .entry(
                utxo.address()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                    .to_string(),
            )
            .and_modify(|entry| {
                entry.0 = entry.0.min(sort);
                entry.1 += amount;
            })
            .or_insert((sort, amount));
    }

    Ok(addresses)
}

fn tx_has_subject(
    resolver: &mut InputResolver<'_>,
    subject: &[u8],
    tx: &MultiEraTx<'_>,
) -> Result<bool, StatusCode> {
    tx_touches_output(resolver, tx, |output| {
        Ok(output_has_subject(subject, output))
    })
}

async fn find_txs<D>(
    domain: &Facade<D>,
    deps: &mut InputDeps,
    subject: &[u8],
    chain: &ChainSummary,
    pagination: &Pagination,
    block: BlockBody,
) -> Result<Vec<AssetTransactionsInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let subject = subject.to_vec();
    let matched = match_block_txs(domain, deps, pagination, block, move |resolver, tx| {
        tx_has_subject(resolver, &subject, tx)
    })
    .await?;

    let mut matches: Vec<_> = matched
        .into_iter()
        .map(|tx| AssetTransactionsInner {
            tx_hash: hex::encode(tx.hash.as_slice()),
            tx_index: tx.index as i32,
            block_height: tx.block_number as i32,
            block_time: chain.slot_time(tx.block_slot) as i32,
        })
        .collect();

    if matches!(pagination.order, Order::Desc) {
        matches = matches.into_iter().rev().collect();
    }

    Ok(matches)
}

pub async fn by_subject_transactions<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetTransactionsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let (subject, _) = resolve_asset_state(&domain, &subject)?;

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

/// Alias of `/assets/{subject}/transactions` that returns only the tx hashes.
/// Same scan, same order/count/page pagination, thinner payload.
pub async fn by_subject_txs<D>(
    path: Path<String>,
    Query(mut params): Query<PaginationParameters>,
    state: State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    // Blockfrost does not define `from`/`to` for this variant and ignores any
    // supplied values, so the alias drops them before delegating.
    params.from = None;
    params.to = None;

    let Json(transactions) = by_subject_transactions(path, Query(params), state).await?;

    let hashes = transactions.into_iter().map(|x| x.tx_hash).collect();

    Ok(Json(hashes))
}

/// `GET /assets/policy/{policy}`: the assets a policy minted, ordered by
/// first mint, from the first-mint period logs.
pub async fn by_policy<D>(
    Path(policy_hex): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetPolicyInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let policy = hex::decode(&policy_hex).map_err(|_| Error::InvalidPolicy)?;
    let policy: Hash<28> = <[u8; 28]>::try_from(policy.as_slice())
        .map_err(|_| Error::InvalidPolicy)?
        .into();

    let chain = domain.get_chain_summary()?;
    let Some(mary_epoch) = chain.first_mary_epoch() else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let order = match pagination.order {
        Order::Asc => SlotOrder::Asc,
        Order::Desc => SlotOrder::Desc,
    };
    let (skip, count) = (pagination.skip(), pagination.count);

    let subjects = domain
        .query()
        .run_blocking(move |domain| {
            first_mints::policy_page(
                &domain,
                &chain,
                mary_epoch..=tip_epoch,
                &policy,
                order,
                skip,
                count,
            )
            .map_err(DomainError::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Blockfrost returns 404 when the policy has no mint operations.
    let subjects = subjects.ok_or(StatusCode::NOT_FOUND)?;

    let mut items = Vec::with_capacity(subjects.len());

    for subject in subjects {
        let asset_key = dolos_cardano::AssetKey::of(&subject);
        let asset_state = domain
            .read_cardano_entity::<AssetState>(&asset_key)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        items.push(AssetPolicyInner {
            asset: hex::encode(&subject),
            quantity: asset_state.quantity().to_string(),
        });
    }

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mapping::assets::decode_asset_subject;
    use crate::test_support::{
        first_mint_chain, first_mint_policies, health_beside, TestApp, TestFault, SLOW_READ,
    };
    use blockfrost_openapi::models::asset::Asset;
    use dolos_cardano::model::PolicyMintChunk;
    use dolos_core::AsyncQueryFacade;
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use pallas::ledger::traverse::MultiEraBlock;

    /// The scan the period logs replaced: the policy's tagged blocks from
    /// genesis, each name kept at its first appearance in a mint.
    async fn scan_policy(domain: &ToyDomain, policy: &[u8]) -> Vec<Vec<u8>> {
        let end_slot = domain.state().read_cursor().unwrap().unwrap().slot();
        let stream = AsyncQueryFacade::new(domain.clone()).blocks_by_policy_stream(
            policy,
            0,
            end_slot,
            SlotOrder::Asc,
        );
        let mut stream = Box::pin(stream);

        let mut seen = std::collections::HashSet::new();
        let mut subjects = Vec::new();

        while let Some(res) = stream.next().await {
            let Some(block) = res.unwrap().1 else {
                continue;
            };
            let block = MultiEraBlock::decode(&block).unwrap();

            for tx in block.txs() {
                for policy_assets in tx.mints() {
                    if policy_assets.policy().as_slice() != policy {
                        continue;
                    }

                    for asset in policy_assets.assets() {
                        let subject = [policy, asset.name()].concat();

                        if seen.insert(subject.clone()) {
                            subjects.push(subject);
                        }
                    }
                }
            }
        }

        subjects
    }

    fn invalid_assets() -> Vec<String> {
        vec![
            // non-hex characters
            "not-hex-asset".to_string(),
            // valid hex, shorter than a policy id
            "abcd".to_string(),
            // valid hex, longer than a policy id plus the longest asset name
            "f".repeat(122),
            // valid length, non-hex characters
            "z".repeat(56),
        ]
    }

    fn missing_asset() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[tokio::test]
    async fn assets_by_subject_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Asset = serde_json::from_slice(&bytes).expect("failed to parse asset");
    }

    #[tokio::test]
    async fn assets_by_subject_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/addresses");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let addresses: Vec<AssetAddressesInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset addresses");
        assert!(!addresses.is_empty());
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/addresses");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/addresses", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/addresses");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

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

    #[tokio::test]
    async fn assets_by_subject_txs_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/txs");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let hashes: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert!(!hashes.is_empty());

        let known: std::collections::HashSet<&String> = app
            .vectors()
            .blocks
            .iter()
            .flat_map(|block| block.tx_hashes.iter())
            .collect();
        for hash in &hashes {
            assert!(known.contains(hash), "unknown tx hash {hash}");
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_matches_transactions() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        for query in [
            "",
            "?order=desc",
            "?page=2&count=2",
            "?order=desc&page=1&count=3",
        ] {
            let (status, bytes) = app
                .get_bytes(&format!("/assets/{asset}/transactions{query}"))
                .await;
            assert_eq!(status, StatusCode::OK);
            let expected: Vec<String> =
                serde_json::from_slice::<Vec<AssetTransactionsInner>>(&bytes)
                    .expect("failed to parse asset transactions")
                    .into_iter()
                    .map(|x| x.tx_hash)
                    .collect();

            assert!(!expected.is_empty(), "no transactions for query {query:?}");

            let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/txs{query}")).await;
            assert_eq!(status, StatusCode::OK);
            let actual: Vec<String> =
                serde_json::from_slice(&bytes).expect("failed to parse asset txs");

            assert_eq!(actual, expected, "mismatch for query {query:?}");
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_ignores_from_to() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");

        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/txs")).await;
        assert_eq!(status, StatusCode::OK);
        let all: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asset txs");

        // the range must actually constrain `/transactions`, otherwise the
        // comparison below proves nothing
        let (status, bytes) = app
            .get_bytes(&format!(
                "/assets/{asset}/transactions?from={}&to={}",
                block.block_number, block.block_number
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let constrained: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset transactions");
        assert!(constrained.len() < all.len());

        let (status, bytes) = app
            .get_bytes(&format!(
                "/assets/{asset}/txs?from={}&to={}",
                block.block_number, block.block_number
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ranged: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert_eq!(ranged, all);

        // a malformed range is ignored as well, never validated
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/txs?from=not-a-number"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ranged: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert_eq!(ranged, all);
    }

    #[tokio::test]
    async fn assets_by_subject_txs_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/txs");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/txs", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_txs_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/txs");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    fn unminted_policy() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    #[tokio::test]
    async fn assets_by_policy_happy_path() {
        let app = TestApp::new();
        let policy = app.vectors().policy_id.clone();
        let unit = app.vectors().asset_unit.clone();
        let path = format!("/assets/policy/{policy}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let assets: Vec<AssetPolicyInner> =
            serde_json::from_slice(&bytes).expect("failed to parse policy assets");
        assert!(assets.iter().any(|a| a.asset == unit));
    }

    #[tokio::test]
    async fn assets_by_policy_ascending_pagination_preserves_first_mint_order() {
        let asset_names = ["FIRST", "SECOND", "THIRD"];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: asset_names.len(),
            txs_per_block: 1,
            asset_names_by_block: asset_names.iter().map(|x| (*x).to_string()).collect(),
            ..Default::default()
        });
        let policy = app.vectors().policy_id.as_str();

        for (index, asset_name) in asset_names.into_iter().enumerate() {
            let page = index + 1;
            let path = format!("/assets/policy/{policy}?order=asc&page={page}&count=1");
            let (status, bytes) = app.get_bytes(&path).await;
            assert_eq!(
                status,
                StatusCode::OK,
                "unexpected status {status} with body: {}",
                String::from_utf8_lossy(&bytes)
            );

            let assets: Vec<AssetPolicyInner> =
                serde_json::from_slice(&bytes).expect("failed to parse policy assets");
            let expected = format!("{policy}{}", hex::encode(asset_name));
            assert_eq!(assets.len(), 1);
            assert_eq!(assets[0].asset, expected);
            assert_eq!(assets[0].quantity, "1");
        }
    }

    /// Every page in both orders of every policy, as the scan the logs
    /// replaced answers it, over a chain whose first epochs are sealed and
    /// whose last are staged.
    #[tokio::test]
    async fn assets_by_policy_matches_the_policy_scan() {
        use first_mint_policies::*;

        let mut domain = None;
        let app = TestApp::new_with_cfg_and_setup(first_mint_chain(), |inner, _| {
            domain = Some(inner.clone())
        });
        let domain = domain.expect("setup ran");

        let sealed = domain
            .archive()
            .iter_logs_typed::<PolicyMintChunk>(None)
            .unwrap()
            .count();
        assert!(sealed > 4, "{sealed} sealed rows");

        for policy in [ALPHA, BETA, GAMMA, SIBLING] {
            let all = scan_policy(&domain, &policy).await;
            assert!(!all.is_empty());

            for (order, name) in [(Order::Asc, "asc"), (Order::Desc, "desc")] {
                let ordered: Vec<String> = match order {
                    Order::Asc => all.iter().map(hex::encode).collect(),
                    Order::Desc => all.iter().rev().map(hex::encode).collect(),
                };

                for count in [1, 3, 100] {
                    for page in 1..=all.len().div_ceil(count) + 1 {
                        let skip = (page - 1) * count;
                        let expected: Vec<String> =
                            ordered.iter().skip(skip).take(count).cloned().collect();

                        let path = format!(
                            "/assets/policy/{}?order={name}&count={count}&page={page}",
                            hex::encode(policy)
                        );
                        let (status, bytes) = app.get_bytes(&path).await;
                        assert_eq!(status, StatusCode::OK, "{path}");

                        let got: Vec<AssetPolicyInner> = serde_json::from_slice(&bytes).unwrap();
                        let got: Vec<String> = got.into_iter().map(|x| x.asset).collect();

                        assert_eq!(got, expected, "{path}");
                    }
                }
            }
        }

        let path = format!("/assets/policy/{}", unminted_policy());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_policy_bad_request() {
        let app = TestApp::new();
        // This policy contains non-hexadecimal characters.
        assert_status(&app, "/assets/policy/not-hex", StatusCode::BAD_REQUEST).await;
        // This policy contains fewer than 28 bytes.
        assert_status(&app, "/assets/policy/abcd", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn assets_by_policy_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/policy/{}", unminted_policy());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_policy_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let policy = app.vectors().policy_id.clone();
        let path = format!("/assets/policy/{policy}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_asset_addresses_do_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowUtxoReads(SLOW_READ)));
        let path = format!("/assets/{}/addresses", app.vectors().asset_unit);

        assert_eq!(health_beside(&app, &path).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn chunked_asset_holdings_match_unchunked() {
        let app = TestApp::new();
        let facade = app.facade();
        let asset = decode_asset_subject(&app.vectors().asset_unit).expect("asset subject");

        let utxos = facade.state().utxos_by_asset(&asset).expect("asset refs");
        assert!(utxos.len() > 2, "too few utxos to split into chunks");

        let whole = asset_holdings(&facade, asset.clone(), usize::MAX)
            .await
            .expect("holdings");

        for chunk in [1, 2] {
            let chunked = asset_holdings(&facade, asset.clone(), chunk)
                .await
                .expect("holdings");
            assert_eq!(chunked, whole);
        }
    }
}
