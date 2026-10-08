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

use super::{collect_epoch_contents, current_epoch, ensure_epoch_in_range};

pub async fn by_number_previous<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
    Query(params): Query<PaginationParameters>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    ensure_epoch_in_range(epoch)?;
    let (chain, current) = current_epoch(&domain)?;

    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    // Pages count back from `epoch - 1`, but each page is ascending, like
    // Blockfrost's.
    let count = pagination.count as u64;
    let skip = pagination.skip() as u64;

    // Inclusive bounds of the page.
    let high = epoch.saturating_sub(1 + skip);
    let low = high.saturating_sub(count.saturating_sub(1));

    let epochs: Vec<Epoch> = if epoch == 0 || epoch.saturating_sub(1) < skip {
        Vec::new()
    } else {
        (low..=high).collect()
    };

    collect_epoch_contents(&domain, &chain, current, epochs).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::TestApp;

    #[tokio::test]
    async fn epochs_by_number_previous_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/2/previous";
        let content: Vec<EpochContent> = get_ok(&app, path).await;

        // Every epoch in the result is before the requested epoch, in ascending
        // order.
        let mut prev = None;
        for item in &content {
            assert!(item.epoch < 2);
            if let Some(prev) = prev {
                assert!(item.epoch > prev);
            }
            prev = Some(item.epoch);
        }
    }

    #[tokio::test]
    async fn epochs_by_number_previous_of_zero_is_empty() {
        let app = TestApp::new();
        let path = "/epochs/0/previous";
        let content: Vec<EpochContent> = get_ok(&app, path).await;
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn epochs_by_number_previous_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/2/previous?page=0";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_by_number_previous_lists_the_epochs_before() {
        let app = TestApp::new();
        assert_eq!(get_epochs(&app, "/epochs/2/previous").await, vec![0, 1]);
    }

    #[tokio::test]
    async fn epochs_by_number_previous_pages_count_back() {
        let app = TestApp::new();
        assert_eq!(
            get_epochs(&app, "/epochs/2/previous?count=1&page=1").await,
            vec![1]
        );
        assert_eq!(
            get_epochs(&app, "/epochs/2/previous?count=1&page=2").await,
            vec![0]
        );
        assert!(get_epochs(&app, "/epochs/2/previous?count=1&page=3")
            .await
            .is_empty());
    }
}
