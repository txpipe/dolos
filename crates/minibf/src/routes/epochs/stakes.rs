use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_stake_content_inner::EpochStakeContentInner;
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::bech32_pool,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{current_epoch, stake_distribution_page};

pub async fn by_number_stakes<D: Domain>(
    Path(epoch): Path<u64>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<EpochStakeContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let (chain, current) = current_epoch(&domain)?;

    // Blockfrost 404s future epochs; an epoch with no logged distribution is
    // an empty page.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let page = stake_distribution_page(&domain, &chain, epoch, None, &pagination).await?;

    let out = page
        .into_iter()
        .map(|(stake_address, log)| {
            let pool = log.pool_id.ok_or_else(|| {
                tracing::error!("account epoch log carries stake with no pool");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

            Ok(EpochStakeContentInner {
                stake_address,
                pool_id: bech32_pool(pool)?,
                amount: log.active_stake.unwrap_or(0).to_string(),
            })
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(out))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn epochs_stakes_happy_path() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;
        let path = format!("/epochs/{epoch}/stakes");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let stakes: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch stakes");

        // The seeder writes the vectors' account plus one synthetic script
        // credential (both delegated to the vectors' pool) and one
        // zero-stake credential, which must be excluded for Blockfrost
        // parity.
        assert_eq!(stakes.len(), 2);
        assert!(stakes.iter().all(|x| x.amount != "0"));

        let seeded = stakes
            .iter()
            .find(|x| x.stake_address == app.vectors().stake_address)
            .expect("seeded stake address missing from distribution");

        assert_eq!(seeded.pool_id, app.vectors().pool_id);
        assert_eq!(seeded.amount, "7000000");
    }

    #[tokio::test]
    async fn epochs_stakes_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch() - 1;

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/epochs/{epoch}/stakes?count=1&page=2"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse stakes page 1");
        let page_2: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse stakes page 2");

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1[0].stake_address, page_2[0].stake_address);
    }

    #[tokio::test]
    async fn epochs_stakes_empty_epoch() {
        let app = TestApp::new();
        // Epoch 0 is in range but nothing is seeded there.
        let (status, bytes) = app.get_bytes("/epochs/0/stakes").await;

        assert_eq!(status, StatusCode::OK);
        let stakes: Vec<EpochStakeContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse empty stakes");
        assert!(stakes.is_empty());
    }

    #[tokio::test]
    async fn epochs_stakes_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/not-a-number/stakes", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_stakes_not_found() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/999999/stakes", StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_stakes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/epochs/0/stakes", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_stakes_archive_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/epochs/0/stakes", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
