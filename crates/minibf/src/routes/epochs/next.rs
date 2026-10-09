use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_content::EpochContent;
use dolos_cardano::model::EpochState;
use dolos_core::Domain;
use pallas::ledger::primitives::Epoch;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{collect_epoch_contents, current_epoch, parse_epoch_digits};

pub async fn by_number_next<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(number): Path<String>,
    Query(params): Query<PaginationParameters>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let epoch = parse_epoch_digits(&number)?;
    let (chain, current) = current_epoch(&domain)?;

    // The reference epoch must exist for the listing to be valid.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    // Ascending, up to and including the current epoch.
    let epochs: Vec<Epoch> = ((epoch + 1)..=current)
        .skip(pagination.skip())
        .take(pagination.count)
        .collect();

    collect_epoch_contents(&domain, &chain, current, epochs).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::TestApp;

    #[tokio::test]
    async fn epochs_by_number_next_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/0/next";
        let content: Vec<EpochContent> = get_ok(&app, path).await;

        // The result is in strict ascending order. Every epoch is greater than the
        // requested epoch.
        let mut prev = None;
        for item in &content {
            assert!(item.epoch > 0);
            if let Some(prev) = prev {
                assert!(item.epoch > prev);
            }
            prev = Some(item.epoch);
        }
    }

    #[tokio::test]
    async fn epochs_by_number_next_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/0/next?count=0";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_by_number_next_not_found() {
        let app = TestApp::new();
        let path = "/epochs/999999/next";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_by_number_next_lists_up_to_the_current_epoch() {
        let app = TestApp::new();
        assert_eq!(get_epochs(&app, "/epochs/0/next").await, vec![1, 2]);
        assert!(get_epochs(&app, "/epochs/2/next").await.is_empty());
    }

    #[tokio::test]
    async fn epochs_by_number_next_paginated() {
        let app = TestApp::new();
        assert_eq!(
            get_epochs(&app, "/epochs/0/next?count=1&page=1").await,
            vec![1]
        );
        assert_eq!(
            get_epochs(&app, "/epochs/0/next?count=1&page=2").await,
            vec![2]
        );
        assert!(get_epochs(&app, "/epochs/0/next?count=1&page=3")
            .await
            .is_empty());
    }
}
