//! `/pools/extended`: the registered pools with their stake, pledge and
//! off-chain metadata.

use std::collections::HashMap;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::pool_list_extended_inner::PoolListExtendedInner;
use dolos_cardano::{
    model::{AccountState, PoolState},
    pool_history, PoolHash, PoolKey,
};
use dolos_core::Domain;
use futures::future::join_all;
use itertools::Itertools;

use crate::{
    error::Error,
    log_and_500,
    mapping::{bech32_pool, pools::build_pool_extended_metadata, rational_to_f64},
    pagination::{Pagination, PaginationParameters},
    scan::ENTITY_CHUNK,
    Facade,
};

use super::{
    fetch_pool_offchain_metadata, listing_order, load_listed_pools, load_live_stakes, select_pools,
};

pub async fn all_extended<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListExtendedInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
    Option<AccountState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let live_stake_map = load_live_stakes(&domain, ENTITY_CHUNK).await?;

    let circulating_supply = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .initial_pots
        .circulating();
    let optimal = domain
        .get_current_effective_pparams()?
        .ensure_k()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut listed: HashMap<PoolHash, (PoolKey, PoolState)> =
        load_listed_pools(&domain, ENTITY_CHUNK)
            .await?
            .into_iter()
            .map(|(key, pool)| (pool.operator, (key, pool)))
            .collect();

    let operators: Vec<PoolHash> = listed.keys().copied().collect();
    let order = listing_order(&domain, &operators).await?;

    let pools = select_pools(order, &pagination)
        .into_iter()
        .filter_map(|operator| listed.remove(&operator))
        .collect_vec();

    let chain = domain.get_chain_summary()?;
    let blocks_minted = pool_history::blocks_minted(
        &domain.inner,
        &chain,
        pools.iter().map(|(_, pool)| pool.clone()).collect(),
    )
    .await
    .map_err(log_and_500("failed to derive the pools' lifetime blocks"))?;

    let metadata_futures: Vec<_> = pools
        .iter()
        .map(|(_, pool)| fetch_pool_offchain_metadata(pool))
        .collect();

    let metadata_results = join_all(metadata_futures).await;

    let mut out = vec![];

    for (((_, pool), fetched_metadata), blocks_minted) in
        pools.into_iter().zip(metadata_results).zip(blocks_minted)
    {
        let poolhex = hex::encode(pool.operator);
        let pool_id = bech32_pool(pool.operator)?;
        let params = pool.snapshot.live().map(|x| x.params.clone());
        let metadata = params
            .as_ref()
            .and_then(|x| build_pool_extended_metadata(x.pool_metadata.as_ref(), fetched_metadata));

        let live = live_stake_map.get(&pool.operator).copied();
        let active = pool.snapshot.set().and_then(|x| x.stake);

        out.push(PoolListExtendedInner {
            pool_id,
            hex: poolhex,
            live_stake: live.map(|x| x.to_string()).unwrap_or("0".to_string()),
            active_stake: active.map(|x| x.to_string()).unwrap_or("0".to_string()),
            live_saturation: live
                .map(|x| x as f64 * optimal as f64 / circulating_supply as f64)
                .unwrap_or_default(),
            blocks_minted: blocks_minted as i32,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::assert_status,
        test_support::{health_beside, TestApp, TestFault, SLOW_READ},
    };
    use blockfrost_openapi::models::pool_list_extended_inner::PoolListExtendedInner;
    use dolos_testing::synthetic::SyntheticBlockConfig;

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

    #[tokio::test(flavor = "current_thread")]
    async fn slow_extended_listing_does_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowStateScans(SLOW_READ)));

        assert_eq!(health_beside(&app, "/pools/extended").await, StatusCode::OK);
    }
}
