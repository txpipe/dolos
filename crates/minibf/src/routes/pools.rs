use std::{collections::HashMap, time::Duration};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    drep_metadata_error::{Code as MetadataErrorCode, DrepMetadataError},
    pool_delegators_inner::PoolDelegatorsInner,
    pool_history_inner::PoolHistoryInner,
    pool_list_extended_inner::PoolListExtendedInner,
    PoolListExtendedInnerMetadata, PoolMetadata as PoolMetadataModel,
};
use dolos_cardano::{
    model::{AccountState, PoolState},
    PoolDelegation, StakeLog,
};
use dolos_core::{BlockSlot, Domain, EntityKey};
use futures::future::join_all;
use itertools::Itertools;
use pallas::{
    codec::minicbor,
    crypto::hash::Hasher,
    ledger::{addresses::Network, primitives::StakeCredential},
};
use serde::Serialize;

use crate::{
    error::Error,
    mapping::{bech32_pool, pool_offchain_metadata, rational_to_f64, IntoModel},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

fn decode_pool_id(pool_id: &str) -> Result<Vec<u8>, Error> {
    if pool_id.starts_with("pool1") {
        let (_, operator) = bech32::decode(pool_id).map_err(|_| StatusCode::BAD_REQUEST)?;
        return Ok(operator);
    } else if pool_id.len() == 56 {
        return hex::decode(pool_id).map_err(|_| Error::Code(StatusCode::BAD_REQUEST));
    }

    Err(Error::Code(StatusCode::BAD_REQUEST))
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum PoolMetadataResponse {
    Metadata(PoolMetadataModel),
    Empty(EmptyObject),
}

#[derive(Default, Serialize)]
pub struct EmptyObject {}

fn build_pool_extended_metadata(
    onchain: Option<&pallas::ledger::primitives::PoolMetadata>,
    offchain: Option<crate::mapping::PoolOffchainMetadata>,
) -> Option<Box<PoolListExtendedInnerMetadata>> {
    onchain.map(|onchain| {
        Box::new(match offchain {
            Some(offchain) => PoolListExtendedInnerMetadata {
                url: Some(onchain.url.clone()),
                hash: Some(hex::encode(&*onchain.hash)),
                error: None,
                ticker: Some(offchain.ticker),
                name: Some(offchain.name),
                description: Some(offchain.description),
                homepage: Some(offchain.homepage),
            },
            None => PoolListExtendedInnerMetadata {
                url: Some(onchain.url.clone()),
                hash: Some(hex::encode(&*onchain.hash)),
                ..Default::default()
            },
        })
    })
}

fn build_pool_metadata_response(
    operator: impl AsRef<[u8]>,
    onchain: Option<&pallas::ledger::primitives::PoolMetadata>,
    offchain: Option<crate::mapping::PoolOffchainMetadata>,
    error: Option<DrepMetadataError>,
) -> Result<PoolMetadataResponse, StatusCode> {
    let operator = operator.as_ref();

    let Some(onchain) = onchain else {
        return Ok(PoolMetadataResponse::Empty(EmptyObject::default()));
    };

    Ok(PoolMetadataResponse::Metadata(PoolMetadataModel {
        pool_id: bech32_pool(operator)?,
        hex: hex::encode(operator),
        url: Some(onchain.url.clone()),
        hash: Some(hex::encode(&*onchain.hash)),
        error: error.map(Box::new),
        ticker: offchain.as_ref().map(|x| x.ticker.clone()),
        name: offchain.as_ref().map(|x| x.name.clone()),
        description: offchain.as_ref().map(|x| x.description.clone()),
        homepage: offchain.as_ref().map(|x| x.homepage.clone()),
    }))
}

fn hash_mismatch_error(url: &str, expected_hash: &[u8], actual_hash: &[u8]) -> DrepMetadataError {
    DrepMetadataError::new(
        MetadataErrorCode::HashMismatch,
        format!(
            "Hash mismatch when fetching metadata from {url}. Expected \"{}\" but got \"{}\".",
            hex::encode(expected_hash),
            hex::encode(actual_hash),
        ),
    )
}

fn http_response_error(url: &str, status: StatusCode) -> DrepMetadataError {
    let reason = status.canonical_reason().unwrap_or("Unknown");

    DrepMetadataError::new(
        MetadataErrorCode::HttpResponseError,
        format!(
            "Error Offchain Pool: HTTP Response error from {url} resulted in HTTP status code : {} \"{reason}\"",
            status.as_u16(),
        ),
    )
}

async fn fetch_pool_offchain_metadata(
    pool: &PoolState,
) -> Option<crate::mapping::PoolOffchainMetadata> {
    let metadata = pool
        .snapshot
        .live()
        .and_then(|x| x.params.pool_metadata.as_ref());

    match metadata {
        Some(metadata) => {
            pool_offchain_metadata(&metadata.url, Some(metadata.hash.as_slice())).await
        }
        None => None,
    }
}

async fn fetch_pool_metadata_with_error(
    pool: &PoolState,
) -> (
    Option<crate::mapping::PoolOffchainMetadata>,
    Option<DrepMetadataError>,
) {
    let Some(metadata) = pool
        .snapshot
        .live()
        .and_then(|x| x.params.pool_metadata.as_ref())
    else {
        return (None, None);
    };

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .redirect(reqwest::redirect::Policy::limited(3))
        .user_agent("Dolos MiniBF")
        .build()
    {
        Ok(client) => client,
        Err(_) => return (None, None),
    };

    let response = match client.get(&metadata.url).send().await {
        Ok(response) => response,
        Err(_) => return (None, None),
    };

    if response.status() != StatusCode::OK {
        return (
            None,
            Some(http_response_error(&metadata.url, response.status())),
        );
    }

    let body = match response.bytes().await {
        Ok(body) => body,
        Err(_) => return (None, None),
    };

    let actual_hash = Hasher::<256>::hash(body.as_ref());

    if actual_hash.as_ref() != metadata.hash.as_slice() {
        return (
            None,
            Some(hash_mismatch_error(
                &metadata.url,
                metadata.hash.as_slice(),
                actual_hash.as_ref(),
            )),
        );
    }

    match serde_json::from_slice(body.as_ref()) {
        Ok(offchain) => (Some(offchain), None),
        Err(_) => (None, None),
    }
}

pub async fn all_extended<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListExtendedInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let mut live_stake_map = HashMap::new();
    let mut active_stake_map = HashMap::new();
    for x in domain
        .iter_cardano_entities::<AccountState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        let (_, state) = x.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if let Some(PoolDelegation::Pool(hash)) = state.pool.live() {
            let stake = state.stake.live().map(|x| x.total()).unwrap_or(0);
            live_stake_map
                .entry(*hash)
                .and_modify(|entry| *entry += stake)
                .or_insert(stake);
        };
        if let Some(PoolDelegation::Pool(hash)) = state.pool.set() {
            let stake = state.stake.set().map(|x| x.total()).unwrap_or(0);
            active_stake_map
                .entry(*hash)
                .and_modify(|entry| *entry += stake)
                .or_insert(stake);
        };
    }
    let circulating_supply = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .initial_pots
        .circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let pools = domain
        .iter_cardano_entities::<PoolState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .flat_map(|x| {
            let Ok((key, state)) = x else {
                return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
            };
            if state.snapshot.live().map(|x| x.is_retired).unwrap_or(false) {
                return None;
            }
            Some(Ok((state.register_slot, (key, state))))
        })
        .collect::<Result<Vec<(BlockSlot, (EntityKey, PoolState))>, StatusCode>>()?
        .into_iter()
        .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
        .map(|(_, x)| x)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect_vec();

    let metadata_futures: Vec<_> = pools
        .iter()
        .map(|(_, pool)| fetch_pool_offchain_metadata(pool))
        .collect();

    let metadata_results = join_all(metadata_futures).await;

    let mut out = vec![];

    for ((_, pool), fetched_metadata) in pools.into_iter().zip(metadata_results) {
        let poolhex = hex::encode(pool.operator);
        let pool_id = bech32_pool(pool.operator)?;
        let params = pool.snapshot.live().map(|x| x.params.clone());
        let metadata = params
            .as_ref()
            .and_then(|x| build_pool_extended_metadata(x.pool_metadata.as_ref(), fetched_metadata));

        let live = live_stake_map.get(&pool.operator).copied();
        let active = active_stake_map.get(&pool.operator).copied();

        out.push(PoolListExtendedInner {
            pool_id,
            hex: poolhex,
            live_stake: live.map(|x| x.to_string()).unwrap_or("0".to_string()),
            active_stake: active.map(|x| x.to_string()).unwrap_or("0".to_string()),
            live_saturation: live
                .map(|x| x as f64 * optimal as f64 / circulating_supply as f64)
                .unwrap_or_default(),
            blocks_minted: pool.blocks_minted_total as i32,
            declared_pledge: params
                .as_ref()
                .map(|x| x.pledge.to_string())
                .unwrap_or_default(),
            margin_cost: params
                .as_ref()
                .map(|x| rational_to_f64::<6>(&x.margin))
                .unwrap_or_default(),
            fixed_cost: params
                .as_ref()
                .map(|x| x.cost.to_string())
                .unwrap_or_default(),
            metadata,
        });
    }

    Ok(Json(out))
}

pub async fn by_id_metadata<D: Domain>(
    Path(id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<PoolMetadataResponse>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;
    let pool = domain
        .read_cardano_entity::<PoolState>(operator.as_slice())?
        .ok_or(StatusCode::NOT_FOUND)?;

    let onchain = pool
        .snapshot
        .live()
        .and_then(|x| x.params.pool_metadata.as_ref());
    let (offchain, error) = fetch_pool_metadata_with_error(&pool).await;

    Ok(Json(build_pool_metadata_response(
        operator, onchain, offchain, error,
    )?))
}

struct PoolDelegatorModelBuilder {
    delegator: StakeCredential,
    account: Option<dolos_cardano::model::AccountState>,
    network: Network,
}

impl IntoModel<PoolDelegatorsInner> for PoolDelegatorModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<PoolDelegatorsInner, StatusCode> {
        let address = crate::mapping::stake_cred_to_address(&self.delegator, self.network)
            .to_bech32()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let live_stake = self.account.map(|x| x.live_stake()).unwrap_or_default();

        Ok(PoolDelegatorsInner {
            address,
            live_stake: live_stake.to_string(),
        })
    }
}

pub async fn by_id_delegators<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolDelegatorsInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;
    if !domain.cardano_entity_exists::<PoolState>(operator.as_slice())? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let network = domain.get_network_id()?;

    let iter = domain
        .iter_cardano_entities::<AccountState>(None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let filtered = iter.filter_ok(|(_, account)| {
        account
            .delegated_pool_live()
            .or(account.retired_pool.as_ref())
            .is_some_and(|f| f.as_slice() == operator.as_slice())
    });

    let pagination = Pagination::try_from(params)?;

    let page: Vec<_> = filtered
        .skip(pagination.skip())
        .take(pagination.count)
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|(delegator, account)| {
            let delegator: StakeCredential = minicbor::decode(delegator.as_ref())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let builder = PoolDelegatorModelBuilder {
                delegator,
                account: Some(account),
                network,
            };

            builder.into_model()
        })
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(mapped))
}

// HACK: blockfrost dbsync version computes fees at the SQL query level using
// the formula: `FLOOR(fee + (rewards - fee) * margin)`.
//
// This is not strictly correct, as the operator share has much more involved
// formula. This method is a workaround to make the data compatible with the
// blockfrost dbsync version.
fn bf_compatible_fees(log: &StakeLog) -> u64 {
    let margin = log
        .margin_cost
        .as_ref()
        .map(
            |pallas::ledger::primitives::RationalNumber {
                 numerator,
                 denominator,
             }| num_rational::Rational64::new(*numerator as i64, *denominator as i64),
        )
        .unwrap_or(num_rational::Rational64::from_integer(0));

    let rewards = num_rational::Rational64::from_integer(log.total_rewards as i64);
    let fixed_cost = num_rational::Rational64::from_integer(log.fixed_cost as i64);

    let variable_fees = (rewards - fixed_cost) * margin;
    let fixed_fees = fixed_cost;

    let fees = variable_fees + fixed_fees;
    let fees = fees.to_integer() as u64;

    if fees > log.total_rewards {
        log.total_rewards
    } else {
        fees
    }
}

pub async fn by_id_history<D: Domain>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolHistoryInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
{
    let operator = decode_pool_id(&id)?;
    let pagination = Pagination::try_from(params)?;
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (epoch, _) = summary.slot_epoch(tip);

    let mut entries = domain
        .iter_cardano_logs_per_epoch::<StakeLog>(operator.into(), 0..epoch)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // Apply order before pagination
    if matches!(pagination.order, crate::pagination::Order::Desc) {
        entries.reverse()
    };

    let mapped: Vec<_> = entries
        .into_iter()
        .filter(|(_, log)| log.total_stake > 0)
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(epoch, log)| {
            Ok(PoolHistoryInner {
                epoch: epoch as i32,
                blocks: log.blocks_minted as i32,
                active_stake: log.total_stake.to_string(),
                active_size: log.relative_size,
                delegators_count: log.delegators_count as i32,
                rewards: log.total_rewards.to_string(),
                fees: bf_compatible_fees(&log).to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(mapped))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::{
        pool_delegators_inner::PoolDelegatorsInner, pool_list_extended_inner::PoolListExtendedInner,
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::ledger::primitives::PoolMetadata;

    fn invalid_pool_id() -> &'static str {
        "not-a-pool"
    }

    fn missing_pool_id() -> &'static str {
        "pool1qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qursw2w89e2"
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
    async fn pools_extended_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools/extended?page=999999").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool list extended");
    }

    #[tokio::test]
    async fn pools_extended_paginated() {
        let cfg = SyntheticBlockConfig {
            slot: 500_000,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg(cfg);
        let (status_1, bytes_1) = app.get_bytes("/pools/extended?page=1&count=1").await;
        let (status_2, bytes_2) = app.get_bytes("/pools/extended?page=2&count=1").await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse pool list extended page 1");
        let page_2: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse pool list extended page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 0);
    }
    #[tokio::test]
    async fn pools_extended_bad_request() {
        let app = TestApp::new();
        let path = "/pools/extended?count=invalid";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_extended_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools/extended", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[test]
    fn pool_metadata_response_empty_without_onchain_metadata() {
        let response =
            build_pool_metadata_response([1u8; 28], None, None, None).expect("pool metadata");

        assert!(matches!(response, PoolMetadataResponse::Empty(_)));
    }

    #[test]
    fn pool_metadata_response_includes_onchain_and_offchain_fields() {
        let operator = [1u8; 28];
        let onchain = PoolMetadata {
            url: "https://example.com/pool.json".to_string(),
            hash: vec![2u8; 32].into(),
        };
        let offchain = crate::mapping::PoolOffchainMetadata {
            ticker: "TICK".to_string(),
            name: "Pool Name".to_string(),
            description: "Pool Description".to_string(),
            homepage: "https://example.com".to_string(),
        };

        let response = build_pool_metadata_response(operator, Some(&onchain), Some(offchain), None)
            .expect("pool metadata");

        let PoolMetadataResponse::Metadata(response) = response else {
            panic!("expected metadata response");
        };

        assert_eq!(
            response.pool_id,
            bech32_pool(operator).expect("bech32 pool id")
        );
        let expected_hash = hex::encode([2u8; 32]);
        assert_eq!(response.hex, hex::encode(operator));
        assert_eq!(
            response.url.as_deref(),
            Some("https://example.com/pool.json")
        );
        assert_eq!(response.hash.as_deref(), Some(expected_hash.as_str()));
        assert_eq!(response.ticker.as_deref(), Some("TICK"));
        assert_eq!(response.name.as_deref(), Some("Pool Name"));
        assert_eq!(response.description.as_deref(), Some("Pool Description"));
        assert_eq!(response.homepage.as_deref(), Some("https://example.com"));
        assert!(response.error.is_none());
    }

    #[test]
    fn pool_metadata_response_includes_error_details() {
        let operator = [1u8; 28];
        let onchain = PoolMetadata {
            url: "https://tinyurl.com/39a7pnv5".to_string(),
            hash: vec![
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67,
                0x89, 0xab, 0xcd, 0xef,
            ]
            .into(),
        };
        let error = hash_mismatch_error(
            &onchain.url,
            onchain.hash.as_slice(),
            &[
                0xb1, 0x23, 0xea, 0x83, 0xd1, 0xf8, 0x7a, 0xfc, 0xb9, 0x5a, 0x76, 0x66, 0x1b, 0xe5,
                0x08, 0xb3, 0x7a, 0x09, 0x57, 0xc7, 0xfa, 0x95, 0xba, 0xa8, 0x83, 0xa8, 0x0d, 0x12,
                0x03, 0xff, 0xcd, 0x94,
            ],
        );

        let response = build_pool_metadata_response(operator, Some(&onchain), None, Some(error))
            .expect("pool metadata");

        let PoolMetadataResponse::Metadata(response) = response else {
            panic!("expected metadata response");
        };

        let error = response.error.expect("expected error");
        assert_eq!(error.code, MetadataErrorCode::HashMismatch);
        assert!(error
            .message
            .contains("Hash mismatch when fetching metadata"));
    }

    #[test]
    fn pool_metadata_http_response_error_matches_expected_format() {
        let error =
            http_response_error("https://blockfrost.io/fakemetadata", StatusCode::NOT_FOUND);

        assert_eq!(error.code, MetadataErrorCode::HttpResponseError);
        assert_eq!(
            error.message,
            "Error Offchain Pool: HTTP Response error from https://blockfrost.io/fakemetadata resulted in HTTP status code : 404 \"Not Found\""
        );
    }

    #[tokio::test]
    async fn pools_metadata_happy_path_returns_empty_object_when_unset() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/metadata");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let response: serde_json::Value =
            serde_json::from_slice(&bytes).expect("failed to parse pool metadata");
        assert_eq!(response, serde_json::json!({}));
    }

    #[tokio::test]
    async fn pools_metadata_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/metadata", invalid_pool_id());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_metadata_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/metadata", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pools_metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/metadata");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn pools_delegators_happy_path() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/delegators?page=999999");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<PoolDelegatorsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool delegators");
    }

    #[tokio::test]
    async fn pools_delegators_paginated() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.as_str();
        let path_page_1 = format!("/pools/{pool_id}/delegators?page=1&count=1");
        let path_page_2 = format!("/pools/{pool_id}/delegators?page=2&count=1");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<PoolDelegatorsInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse delegators page 1");
        let page_2: Vec<PoolDelegatorsInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse delegators page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 0);
    }
    #[tokio::test]
    async fn pools_delegators_bad_request() {
        let app = TestApp::new();
        let path = format!("/pools/{}/delegators", invalid_pool_id());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_delegators_not_found() {
        let app = TestApp::new();
        let path = format!("/pools/{}/delegators", missing_pool_id());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn pools_delegators_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.as_str();
        let path = format!("/pools/{pool_id}/delegators");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
