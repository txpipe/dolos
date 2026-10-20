use axum::{extract::State, Json};
use blockfrost_openapi::models::epoch_content::EpochContent;
use dolos_cardano::model::EpochState;
use dolos_core::Domain;

use crate::{error::Error, mapping::IntoModel as _, Facade};

use super::{build_epoch_content, current_epoch, load_epoch_state};

pub async fn latest<D: Domain>(State(domain): State<Facade<D>>) -> Result<Json<EpochContent>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let (chain, current) = current_epoch(&domain)?;

    // The current epoch always has a live `EpochState`, so no 404 here.
    let state = load_epoch_state(&domain, &chain, current, current)?;
    let model = build_epoch_content(&domain, &chain, current, state)?;

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
        let content: EpochContent = get_ok(&app, path).await;
        // The tip of the synthetic chain is in epoch 2, so `latest` resolves to
        // epoch 2.
        assert_eq!(content.epoch, 2);
        assert!(content.start_time < content.end_time);
    }

    /// The current epoch's active stake is the one ESTART stored when the
    /// epoch opened; no account is scanned for it.
    #[tokio::test]
    async fn epochs_latest_reads_the_stored_active_stake() {
        let app = app_with_active_stake(7_000, &[]);
        let content: EpochContent = get_ok(&app, "/epochs/latest").await;
        assert_eq!(content.active_stake.as_deref(), Some("7000"));
    }

    #[tokio::test]
    async fn epochs_latest_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/latest";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
