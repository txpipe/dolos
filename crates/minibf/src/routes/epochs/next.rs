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

use super::{collect_epoch_contents, ensure_epoch_in_range};

pub async fn by_number_next<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
    Query(params): Query<PaginationParameters>,
) -> Result<Json<Vec<EpochContent>>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    ensure_epoch_in_range(epoch)?;
    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let (current, _) = chain.slot_epoch(tip);

    // The reference epoch must exist for the listing to be valid.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    // Collect the epochs after `epoch`, up to and including the current epoch,
    // in ascending order. The pagination selects the window.
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
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: Vec<EpochContent> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content array");

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
}
