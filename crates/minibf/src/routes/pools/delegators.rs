//! `/pools/{pool_id}/delegators`: the accounts delegating to a pool.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::pool_delegators_inner::PoolDelegatorsInner;
use dolos_cardano::{
    model::{AccountState, PoolState},
    AccountKey, PoolHash,
};
use dolos_core::Domain;
use pallas::ledger::primitives::StakeCredential;

use crate::{
    error::Error,
    mapping::{pools::PoolDelegatorModelBuilder, IntoModel},
    pagination::{Pagination, PaginationParameters},
    scan::{EntityScan, ENTITY_CHUNK},
    Facade,
};

use super::{parse_pool_id_bounded, pool_exists};

pub async fn by_id_delegators<D>(
    Path(id): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<PoolDelegatorsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let Some(hash) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };
    if !pool_exists(&domain, hash).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let network = domain.get_network_id()?;

    let page = load_delegators_page(
        &domain,
        hash,
        pagination.skip(),
        pagination.count,
        ENTITY_CHUNK,
    )
    .await?;

    let mapped: Vec<_> = page
        .into_iter()
        .map(|(delegator, account)| {
            let delegator = StakeCredential::from(delegator);

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

/// One page of the accounts whose live stake goes to `pool`, in key order,
/// `chunk` accounts per blocking task. The scan stops once the page is full.
async fn load_delegators_page<D>(
    domain: &Facade<D>,
    pool: PoolHash,
    skip: usize,
    count: usize,
    chunk: usize,
) -> Result<Vec<(AccountKey, AccountState)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
{
    let mut accounts = EntityScan::<AccountState>::all(chunk);
    let mut seen = 0usize;
    let mut page = Vec::new();

    while page.len() < count {
        let Some(delegators) = accounts
            .next(domain, move |accounts| {
                accounts
                    .into_iter()
                    .filter(|(_, account)| {
                        account
                            .delegated_pool_live()
                            .or(account.retired_pool.as_ref())
                            .is_some_and(|f| *f == pool)
                    })
                    .collect::<Vec<_>>()
            })
            .await?
        else {
            break;
        };

        for delegator in delegators {
            if seen >= skip && page.len() < count {
                page.push(delegator);
            }
            seen += 1;
        }
    }

    Ok(page)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::pools::testing::{assert_status, invalid_pool_id, missing_pool_id, vector_pool},
        test_support::{health_beside, replicated_app, TestApp, TestFault, SLOW_READ},
    };
    use blockfrost_openapi::models::pool_delegators_inner::PoolDelegatorsInner;

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

    #[tokio::test(flavor = "current_thread")]
    async fn slow_delegators_listing_does_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowStateScans(SLOW_READ)));
        let path = format!("/pools/{}/delegators", app.vectors().pool_id);

        assert_eq!(health_beside(&app, &path).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn chunked_delegator_pages_match_unchunked() {
        let app = replicated_app();
        let facade = app.facade();
        let operator = vector_pool(&app);

        let page = |skip, count, chunk| {
            let facade = facade.clone();
            async move {
                load_delegators_page(&facade, operator, skip, count, chunk)
                    .await
                    .expect("delegators")
                    .into_iter()
                    .map(|(key, _)| key)
                    .collect::<Vec<_>>()
            }
        };

        assert!(
            page(0, 100, usize::MAX).await.len() > 2,
            "too few delegators to split into chunks"
        );

        for (skip, count) in [(0, 100), (1, 2), (2, 1), (4, 3), (10, 5)] {
            let whole = page(skip, count, usize::MAX).await;

            for chunk in [1, 2] {
                assert_eq!(
                    page(skip, count, chunk).await,
                    whole,
                    "skip {skip}, count {count}"
                );
            }
        }
    }
}
