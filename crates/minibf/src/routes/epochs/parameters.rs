use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;
use dolos_cardano::model::EpochState;
use dolos_core::Domain;
use pallas::ledger::primitives::Epoch;

use crate::{
    error::Error,
    mapping::{epochs::ParametersModelBuilder, IntoModel as _},
    Facade,
};

use super::{current_epoch, ensure_epoch_in_range, load_epoch_state};

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
) -> Result<Json<EpochParamContent>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    ensure_epoch_in_range(epoch)?;

    let (chain, current) = current_epoch(&domain)?;
    let state = load_epoch_state(&domain, &chain, current, epoch)?;

    // Like `build_epoch_content`: the live state of the current epoch can
    // carry another number than the one requested.
    let model = ParametersModelBuilder {
        epoch,
        params: state.pparams.live().cloned().unwrap_or_default(),
        genesis: &domain.genesis(),
        nonce: state.nonces.map(|x| x.active.to_string()),
    };

    Ok(model.into_response()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};
    use axum::http::StatusCode;

    #[tokio::test]
    async fn epochs_by_number_parameters_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/0/parameters";
        let _: EpochParamContent = get_ok(&app, path).await;
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_bad_request() {
        let app = TestApp::new();
        let path = "/epochs/not-a-number/parameters";
        assert_status(&app, path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_not_found() {
        let app = TestApp::new();
        let path = "/epochs/999999/parameters";
        assert_status(&app, path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/0/parameters";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_out_of_range() {
        // Past the `i32` range Blockfrost serves, the epoch is malformed, not
        // missing.
        let app = TestApp::new();
        assert_status(
            &app,
            "/epochs/2147483648/parameters",
            StatusCode::BAD_REQUEST,
        )
        .await;
        assert_status(
            &app,
            "/epochs/18446744073709551615/parameters",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn epochs_by_number_parameters_of_the_current_epoch() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let params: EpochParamContent = get_ok(&app, &format!("/epochs/{epoch}/parameters")).await;
        assert_eq!(params.epoch as u64, epoch);
    }
}
