//! `/pools/retired`: the pools whose retirement took effect.

use axum::{
    extract::{Query, State},
    Json,
};
use blockfrost_openapi::models::pool_list_retire_inner::PoolListRetireInner;
use dolos_cardano::{model::PoolState, PoolHash};
use dolos_core::Domain;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::load_retirement_page;

pub async fn all_retired<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListRetireInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let out = load_retirement_page(domain, pagination, true, retired_candidates).await?;

    Ok(Json(out))
}

fn retired_candidates(pools: impl IntoIterator<Item = PoolState>) -> Vec<(u64, PoolHash)> {
    pools
        .into_iter()
        .filter_map(|pool| {
            let is_retired = pool.snapshot.live().map(|x| x.is_retired).unwrap_or(false);
            let retiring_epoch = pool.retiring_epoch?;
            is_retired.then_some((retiring_epoch, pool.operator))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::{
            assert_retirement_order, assert_status, retired_pool, retirement_order_app,
        },
        test_support::{TestApp, TestFault},
    };
    use axum::http::StatusCode;
    use blockfrost_openapi::models::pool_list_retire_inner::PoolListRetireInner;
    use pallas::crypto::hash::Hash;

    #[tokio::test]
    async fn pools_retired_orders_an_epoch_by_retirement_position() {
        let app = retirement_order_app(true);
        assert_retirement_order(&app, "/pools/retired").await;
    }

    #[tokio::test]
    async fn pools_retired_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools/retired").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let retired: Vec<PoolListRetireInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool list retire");

        assert!(
            retired.is_empty(),
            "the synthetic ledger registers pools but never retires them, so the list is empty"
        );
    }

    #[test]
    fn retired_candidates_keep_retired_pools() {
        let pools = vec![
            // The pools are retired, so they stay.
            retired_pool([1u8; 28], Some(5), true),
            retired_pool([2u8; 28], Some(10), true),
            // The pool has a retirement epoch but is not retired yet, so it is
            // left out.
            retired_pool([3u8; 28], Some(12), false),
            // The pool does not retire, so it is left out.
            retired_pool([4u8; 28], None, false),
        ];

        let mut selected = retired_candidates(pools);
        selected.sort_unstable();

        assert_eq!(
            selected,
            vec![(5, Hash::from([1u8; 28])), (10, Hash::from([2u8; 28]))]
        );
    }

    #[tokio::test]
    async fn pools_retired_bad_request() {
        let app = TestApp::new();
        assert_status(
            &app,
            "/pools/retired?count=invalid",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn pools_retired_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools/retired", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
