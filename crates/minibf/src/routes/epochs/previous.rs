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
    let tip = domain.get_tip_slot()?;
    let chain = domain.get_chain_summary()?;
    let (current, _) = chain.slot_epoch(tip);

    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    // Collect the epochs before `epoch`, from `epoch - 1` backward. The result
    // is always in ascending order, the same as the reference implementation.
    let count = pagination.count as u64;
    let skip = pagination.skip() as u64;

    // The highest and lowest epoch in the page. Both bounds are inclusive.
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
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: Vec<EpochContent> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content array");

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
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: Vec<EpochContent> =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content array");
        assert!(content.is_empty());
    }

    #[tokio::test]
    async fn epochs_by_number_previous_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/2/previous?page=0";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }
}
