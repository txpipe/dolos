use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_stake_pool_content_inner::EpochStakePoolContentInner;
use dolos_cardano::model::PoolState;
use dolos_core::Domain;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    routes::pools::parse_pool_id_bounded,
    Facade,
};

use super::{current_epoch, ensure_epoch_in_range, stake_distribution_page};

pub async fn by_number_stakes_pool<D: Domain>(
    Path((epoch, pool_id)): Path<(u64, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<EpochStakePoolContentInner>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    ensure_epoch_in_range(epoch)?;

    let (chain, current) = current_epoch(&domain)?;

    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let Some(operator) = parse_pool_id_bounded(&pool_id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !domain.cardano_entity_exists::<PoolState>(operator)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let page = stake_distribution_page(&domain, &chain, epoch, Some(operator), &pagination).await?;

    let out = page
        .into_iter()
        .map(|(stake_address, log)| EpochStakePoolContentInner {
            stake_address,
            amount: log.active_stake.unwrap_or(0).to_string(),
        })
        .collect();

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn epochs_stakes_pool_happy_path() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        let pool_id = app.vectors().pool_id.clone();
        let (status, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes/{pool_id}"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let stakes: Vec<EpochStakePoolContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch pool stakes");

        // Both non-zero seeded delegators point at the vectors' pool; the
        // zero-stake one is excluded.
        assert_eq!(stakes.len(), 2);
        assert!(stakes.iter().all(|x| x.amount != "0"));
        assert!(stakes
            .iter()
            .any(|x| x.stake_address == app.vectors().stake_address));
    }

    #[tokio::test]
    async fn epochs_stakes_pool_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        let pool_id = app.vectors().pool_id.clone();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes/{pool_id}?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes/{pool_id}?count=1&page=2"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<EpochStakePoolContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse pool stakes page 1");
        let page_2: Vec<EpochStakePoolContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse pool stakes page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].stake_address, page_2[0].stake_address);
    }

    #[tokio::test]
    async fn epochs_stakes_pool_bad_request() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        assert_status(
            &app,
            &format!("/epochs/{epoch}/stakes/not-a-pool"),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_stakes_pool_not_found() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        // Well-formed pool id that is not registered.
        assert_status(
            &app,
            &format!(
                "/epochs/{epoch}/stakes/pool1qurswpc8qurswpc8qurswpc8qurswpc8qurswpc8qursw2w89e2"
            ),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_stakes_pool_future_epoch_not_found() {
        let app = TestApp::new();
        let pool_id = app.vectors().pool_id.clone();
        assert_status(
            &app,
            &format!("/epochs/999999/stakes/{pool_id}"),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_stakes_pool_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let pool_id = app.vectors().pool_id.clone();
        assert_status(
            &app,
            &format!("/epochs/0/stakes/{pool_id}"),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
