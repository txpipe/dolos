//! `/pools`: the registered pools, in db-sync's listing order.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::{model::PoolState, PoolHash};
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::bech32_pool,
    pagination::{Pagination, PaginationParameters},
    scan::ENTITY_CHUNK,
    Facade,
};

use super::{listing_order, load_listed_pools, select_pools};

pub async fn all<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let pools: Vec<PoolHash> = load_listed_pools(&domain, ENTITY_CHUNK)
        .await?
        .into_iter()
        .map(|(_, pool)| pool.operator)
        .collect();

    let pools = listing_order(&domain, &pools).await?;

    let out = select_pools(pools, &pagination)
        .into_iter()
        .map(bech32_pool)
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::assert_status,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::pool_list_extended_inner::PoolListExtendedInner;

    #[tokio::test]
    async fn pools_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");

        let pool_id = app.vectors().pool_id.as_str();
        assert!(
            pools.iter().any(|candidate| candidate == pool_id),
            "expected pool {pool_id} in list, got {pools:?}"
        );
    }

    #[tokio::test]
    async fn pools_matches_extended_ids() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools?page=1&count=100").await;
        let (extended_status, extended_bytes) =
            app.get_bytes("/pools/extended?page=1&count=100").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(extended_status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");
        let extended: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&extended_bytes).expect("failed to parse pool list extended");

        let extended_ids: Vec<String> = extended.into_iter().map(|pool| pool.pool_id).collect();

        assert_eq!(pools, extended_ids);
    }

    #[tokio::test]
    async fn pools_order_desc_reverses_asc() {
        let app = TestApp::new();
        let (asc_status, asc_bytes) = app.get_bytes("/pools?order=asc&count=100").await;
        let (desc_status, desc_bytes) = app.get_bytes("/pools?order=desc&count=100").await;

        assert_eq!(asc_status, StatusCode::OK);
        assert_eq!(desc_status, StatusCode::OK);

        let asc: Vec<String> =
            serde_json::from_slice(&asc_bytes).expect("failed to parse asc pool list");
        let mut desc: Vec<String> =
            serde_json::from_slice(&desc_bytes).expect("failed to parse desc pool list");

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn pools_matches_extended_ids_desc() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools?order=desc&count=100").await;
        let (extended_status, extended_bytes) =
            app.get_bytes("/pools/extended?order=desc&count=100").await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(extended_status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");
        let extended: Vec<PoolListExtendedInner> =
            serde_json::from_slice(&extended_bytes).expect("failed to parse pool list extended");

        let extended_ids: Vec<String> = extended.into_iter().map(|pool| pool.pool_id).collect();

        assert_eq!(pools, extended_ids);
    }

    /// A pool the Shelley genesis stakes registered before any certificate,
    /// so it lists before the pools the chain registers.
    #[tokio::test]
    async fn pools_list_a_genesis_pool_first() {
        const GENESIS: [u8; 28] = [0xff; 28];

        let app = TestApp::builder().with_genesis_pool(GENESIS).build();
        let (status, bytes) = app.get_bytes("/pools?count=100").await;
        assert_eq!(status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");

        assert!(pools.len() > 1, "the chain registers pools of its own");
        assert_eq!(pools[0], crate::mapping::bech32_pool(GENESIS).unwrap());
    }

    #[tokio::test]
    async fn pools_empty_out_of_range_page() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools?page=694269").await;

        assert_eq!(status, StatusCode::OK);

        let pools: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse pool list");
        assert!(pools.is_empty());
    }

    #[tokio::test]
    async fn pools_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/pools?count=invalid", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn pools_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
