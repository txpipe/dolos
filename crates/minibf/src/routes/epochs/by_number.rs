use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_content::EpochContent;
use dolos_cardano::model::EpochState;
use dolos_core::Domain;

use crate::{error::Error, mapping::IntoModel as _, Facade};

use super::{
    build_epoch_content, current_epoch, derive_current_active_stake, load_epoch_state,
    parse_epoch_digits,
};

pub async fn by_number<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(number): Path<String>,
) -> Result<Json<EpochContent>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let epoch = parse_epoch_digits(&number)?;

    let (chain, current) = current_epoch(&domain)?;

    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let state = load_epoch_state(&domain, &chain, current, epoch)?;
    let active_stake = if epoch == current {
        Some(derive_current_active_stake(&domain, &chain, current).await?)
    } else {
        None
    };
    let model = build_epoch_content(&domain, &chain, epoch, state, active_stake)?;

    Ok(model.into_response()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn epochs_by_number_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/1";
        let content: EpochContent = get_ok(&app, path).await;
        assert_eq!(content.epoch, 1);
        // The synthetic chain puts all blocks in epoch 2, so epoch 1 has no
        // block. Its aggregates and rolling stats are zero.
        assert!(content.start_time < content.end_time);
    }

    #[tokio::test]
    async fn epochs_by_number_current_has_active_stake() {
        let app = TestApp::new();
        let path = "/epochs/2";
        let content: EpochContent = get_ok(&app, path).await;
        assert!(content.active_stake.is_some());
    }

    #[tokio::test]
    async fn epochs_by_number_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/not-a-number";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_by_number_not_found() {
        let app = TestApp::new();
        let path = "/epochs/999999";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_by_number_out_of_range() {
        // An epoch number greater than the `i32` range of the reference API gets a
        // bad-request error, not a 404 error for a missing epoch.
        let app = TestApp::new();
        assert_status(&app, "/epochs/696969696969", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/epochs/696969696969/next", StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            "/epochs/696969696969/previous",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_by_number_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/1";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
