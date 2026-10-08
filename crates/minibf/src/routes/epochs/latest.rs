use axum::{extract::State, Json};
use blockfrost_openapi::models::epoch_content::EpochContent;
use dolos_cardano::model::EpochState;
use dolos_core::Domain;

use crate::{error::Error, mapping::IntoModel as _, Facade};

use super::{build_epoch_content, current_epoch, derive_current_active_stake, load_epoch_state};

pub async fn latest<D: Domain>(State(domain): State<Facade<D>>) -> Result<Json<EpochContent>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let (chain, current) = current_epoch(&domain)?;

    // The current epoch always has a live `EpochState`, so this never returns a
    // 404 error.
    let state = load_epoch_state(&domain, &chain, current, current)?;
    let active_stake = derive_current_active_stake(&domain, &chain, current).await?;
    let model = build_epoch_content(&domain, &chain, current, state, Some(active_stake))?;

    Ok(model.into_response()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};
    use axum::http::StatusCode;

    #[tokio::test]
    async fn epochs_latest_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/latest";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let content: EpochContent =
            serde_json::from_slice(&bytes).expect("failed to parse epoch content");
        // The tip of the synthetic chain is in epoch 2, so `latest` resolves to
        // epoch 2.
        assert_eq!(content.epoch, 2);
        assert!(content.start_time < content.end_time);
        assert!(content.active_stake.is_some());
    }

    #[tokio::test]
    async fn epochs_latest_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/latest";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
