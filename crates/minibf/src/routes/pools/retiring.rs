//! `/pools/retiring`: the pools with a pending retirement.

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

pub async fn all_retiring<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolListRetireInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (current_epoch, _) = summary.slot_epoch(tip);

    // a pool in the log retired epochs ago, so only state holds retiring ones
    let out = load_retirement_page(domain, pagination, false, move |pools| {
        retiring_candidates(pools, current_epoch)
    })
    .await?;

    Ok(Json(out))
}

fn retiring_candidates(
    pools: impl IntoIterator<Item = PoolState>,
    current_epoch: u64,
) -> Vec<(u64, PoolHash)> {
    pools
        .into_iter()
        .filter_map(|pool| {
            let retiring_epoch = pool.retiring_epoch?;
            (retiring_epoch > current_epoch).then_some((retiring_epoch, pool.operator))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::{
            assert_retirement_order, assert_status, retirement_order_app, retiring_pool,
        },
        test_support::{TestApp, TestFault},
    };
    use axum::http::StatusCode;
    use blockfrost_openapi::models::pool_list_retire_inner::PoolListRetireInner;
    use pallas::crypto::hash::Hash;

    #[tokio::test]
    async fn pools_retiring_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/pools/retiring").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let retiring: Vec<PoolListRetireInner> =
            serde_json::from_slice(&bytes).expect("failed to parse pool list retire");

        assert!(
            retiring.is_empty(),
            "synthetic ledger registers pools but never schedules a future retirement, so the list is expected to be empty"
        );
    }

    #[test]
    fn retiring_candidates_keep_future_retirements() {
        let pools = vec![
            // The retirement epoch is past or current, so the pool is left out.
            retiring_pool([1u8; 28], Some(5)),
            retiring_pool([2u8; 28], Some(10)),
            // The pool does not retire, so it is left out.
            retiring_pool([3u8; 28], None),
            // The retirement epoch is in the future, so the pool stays.
            retiring_pool([4u8; 28], Some(12)),
            retiring_pool([5u8; 28], Some(11)),
        ];

        let mut selected = retiring_candidates(pools, 10);
        selected.sort_unstable();

        assert_eq!(
            selected,
            vec![(11, Hash::from([5u8; 28])), (12, Hash::from([4u8; 28]))]
        );
    }

    #[tokio::test]
    async fn pools_retiring_orders_an_epoch_by_retirement_position() {
        let app = retirement_order_app(false);
        assert_retirement_order(&app, "/pools/retiring").await;
    }

    #[tokio::test]
    async fn pools_retiring_bad_request() {
        let app = TestApp::new();
        assert_status(
            &app,
            "/pools/retiring?count=invalid",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn pools_retiring_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/pools/retiring", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
