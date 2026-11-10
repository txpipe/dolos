//! `/pools/extended`: the registered pools with their stake, pledge and
//! off-chain metadata.

use std::{collections::HashMap, time::Duration};

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
    offchain::{
        cached_pool_metadata, fetch_listed_pool_metadata, settle_within, PoolMetadataLoad,
        MAX_CONCURRENT_POOL_FETCHES,
    },
    pagination::{Pagination, PaginationParameters},
    scan::ENTITY_CHUNK,
    Facade,
};

use super::{listing_order, load_listed_pools, load_live_stakes, select_pools};

/// How long `/pools/extended` waits for the metadata of its misses, the wait
/// for a fill permit included.
const PAGE_METADATA_DEADLINE: Duration = Duration::from_secs(5);

/// The off-chain metadata of each row of a pool page, in page order, within
/// [`PAGE_METADATA_DEADLINE`].
pub(super) async fn page_pool_metadata<C: dolos_core::CacheStore>(
    cache: &C,
    onchain: &[Option<&pallas::ledger::primitives::PoolMetadata>],
) -> Vec<PoolMetadataLoad> {
    page_pool_metadata_with(
        cache,
        onchain,
        |metadata| fetch_listed_pool_metadata(cache, metadata),
        PAGE_METADATA_DEADLINE,
    )
    .await
}

/// [`page_pool_metadata`] over `load`. Starts at most
/// [`MAX_CONCURRENT_POOL_FETCHES`] misses at a time.
async fn page_pool_metadata_with<'a, C, L>(
    cache: &C,
    onchain: &[Option<&'a pallas::ledger::primitives::PoolMetadata>],
    load: impl Fn(&'a pallas::ledger::primitives::PoolMetadata) -> L,
    deadline: Duration,
) -> Vec<PoolMetadataLoad>
where
    C: dolos_core::CacheStore,
    L: std::future::Future<Output = PoolMetadataLoad>,
{
    let cached = join_all(onchain.iter().map(|metadata| async move {
        match metadata {
            Some(metadata) => cached_pool_metadata(cache, metadata).await,
            None => Some((None, None)),
        }
    }))
    .await;

    // only a row with on-chain metadata can miss
    let loads = onchain
        .iter()
        .zip(&cached)
        .filter(|(_, cached)| cached.is_none())
        .filter_map(|(metadata, _)| *metadata)
        .map(load)
        .collect();

    let mut settled = settle_within(loads, MAX_CONCURRENT_POOL_FETCHES, deadline)
        .await
        .into_iter();

    cached
        .into_iter()
        .map(|cached| {
            cached
                .or_else(|| settled.next().flatten())
                .unwrap_or((None, None))
        })
        .collect()
}

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

    let onchain: Vec<_> = pools
        .iter()
        .map(|(_, pool)| {
            pool.snapshot
                .live()
                .and_then(|x| x.params.pool_metadata.as_ref())
        })
        .collect();

    let metadata_results = page_pool_metadata(domain.inner.cache(), &onchain).await;

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
        mapping::pool_connection_error as connection_error,
        routes::pools::testing::{assert_status, retiring_pool},
        test_support::{health_beside, TestApp, TestFault, SLOW_READ},
    };
    use blockfrost_openapi::models::pool_list_extended_inner::PoolListExtendedInner;
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::{crypto::hash::Hasher, ledger::primitives::PoolMetadata};
    use std::sync::Arc;

    #[tokio::test]
    async fn a_pool_list_stops_waiting_while_its_cache_fill_continues() {
        use dolos_core::{CacheEntry, CacheStore as _};
        use std::convert::Infallible;
        use tokio::sync::oneshot;

        let body = br#"{"name":"Pool","description":"Pool","ticker":"POOL","homepage":"https://example.com"}"#;
        let metadata = PoolMetadata {
            url: "https://example.invalid/pool.json".to_string(),
            hash: Hasher::<256>::hash(body).to_vec().into(),
        };
        let mut pool = retiring_pool([1u8; 28], None);
        pool.snapshot.unwrap_live_mut().params.pool_metadata = Some(metadata.clone());
        let app = TestApp::new();
        let cache = app.facade().inner.cache().clone();
        let (started, ready) = oneshot::channel();
        let (release, released) = oneshot::channel();
        let fill = tokio::spawn({
            let (cache, metadata) = (cache.clone(), metadata.clone());
            async move {
                cache
                    .get_or_fill_async(
                        crate::offchain::POOL_OFFCHAIN.name,
                        metadata.hash.as_slice(),
                        async move {
                            let _ = started.send(());
                            released.await.unwrap();
                            Ok::<_, Infallible>(CacheEntry::new(body.to_vec(), 0))
                        },
                    )
                    .await
                    .unwrap();
            }
        });
        ready.await.unwrap();
        let onchain = [pool.snapshot.live().unwrap().params.pool_metadata.as_ref()];
        let (listed, _) =
            tokio::time::timeout(Duration::from_secs(6), page_pool_metadata(&cache, &onchain))
                .await
                .expect("the list waited beyond its metadata deadline")
                .remove(0);
        assert!(listed.is_none());
        release.send(()).unwrap();
        fill.await.unwrap();
        let (listed, _) = page_pool_metadata(&cache, &onchain).await.remove(0);
        assert_eq!(listed.unwrap().ticker, "POOL");
    }

    /// A cold page starts at most one fill per permit and queues none. Its
    /// started fills keep their bodies, and the next request fetches the
    /// rest.
    #[tokio::test]
    async fn a_cold_page_starts_no_more_fetches_than_its_slots() {
        use crate::offchain::{load_pool_metadata, OnOverload};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::sync::Semaphore;

        let rows = MAX_CONCURRENT_POOL_FETCHES + 4;
        let bodies: Vec<_> = (0..rows)
            .map(|i| {
                format!(r#"{{"name":"P{i}","description":"","ticker":"P{i}","homepage":""}}"#)
                    .into_bytes()
            })
            .collect();
        let metadata: Vec<_> = bodies
            .iter()
            .enumerate()
            .map(|(i, body)| PoolMetadata {
                url: format!("https://example.invalid/{i}.json"),
                hash: Hasher::<256>::hash(body).to_vec().into(),
            })
            .collect();
        let onchain: Vec<_> = metadata.iter().map(Some).collect();

        let app = TestApp::new();
        let cache = app.facade().inner.cache().clone();
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(AtomicUsize::new(0));
        let held = |index: usize| {
            let (body, gate, started) = (bodies[index].clone(), gate.clone(), started.clone());
            move |_| {
                let (body, gate, started) = (body.clone(), gate.clone(), started.clone());
                async move {
                    started.fetch_add(1, Ordering::SeqCst);
                    let _ = gate.acquire().await;
                    Ok(body)
                }
            }
        };
        let page = |deadline| {
            page_pool_metadata_with(
                &cache,
                &onchain,
                |metadata| {
                    let index = metadata.url.trim_end_matches(".json");
                    let index: usize = index.rsplit('/').next().unwrap().parse().unwrap();
                    load_pool_metadata(&cache, metadata, OnOverload::Unfetched, held(index))
                },
                deadline,
            )
        };

        let cold = page(Duration::from_millis(100)).await;
        assert!(cold.iter().all(|(listed, _)| listed.is_none()));
        assert_eq!(started.load(Ordering::SeqCst), MAX_CONCURRENT_POOL_FETCHES);

        // the started fetches outlive the page and keep their bodies
        gate.add_permits(Semaphore::MAX_PERMITS / 2);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let kept = page_pool_metadata(&cache, &onchain[..MAX_CONCURRENT_POOL_FETCHES]);
                if kept.await.iter().all(|(listed, _)| listed.is_some()) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a fetch the cold page started never kept its body");
        assert_eq!(started.load(Ordering::SeqCst), MAX_CONCURRENT_POOL_FETCHES);

        let warm = page(Duration::from_secs(5)).await;
        assert!(warm.iter().all(|(listed, _)| listed.is_some()));
        assert_eq!(started.load(Ordering::SeqCst), rows);
    }

    #[tokio::test]
    async fn an_extended_row_carries_the_error_of_a_failed_fetch() {
        let failed = PoolMetadata {
            url: "https://example.invalid/failed.json".to_string(),
            hash: vec![3u8; 32].into(),
        };
        let app = TestApp::new();
        let cache = app.facade().inner.cache().clone();
        let error = connection_error(&failed.url);
        crate::offchain::cache_pool_failure(&cache, &failed, error.clone()).await;

        let loaded = page_pool_metadata(&cache, &[Some(&failed), None]).await;
        assert!(loaded[0].0.is_none());
        assert_eq!(loaded[0].1, Some(error.clone()));
        assert!(loaded[1].0.is_none() && loaded[1].1.is_none());

        let row = build_pool_extended_metadata(Some(&failed), loaded[0].clone()).unwrap();
        assert_eq!(row.url.as_deref(), Some(failed.url.as_str()));
        assert_eq!(row.error.as_deref(), Some(&error));
        assert!(row.ticker.is_none());
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

    #[tokio::test(flavor = "current_thread")]
    async fn slow_extended_listing_does_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowStateScans(SLOW_READ)));

        assert_eq!(health_beside(&app, "/pools/extended").await, StatusCode::OK);
    }
}
