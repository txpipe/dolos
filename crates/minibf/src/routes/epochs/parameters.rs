use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;
use dolos_core::Domain;
use pallas::ledger::primitives::Epoch;

use crate::{
    error::Error,
    mapping::{epochs::ParametersModelBuilder, IntoModel as _},
    Facade,
};

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<Epoch>,
) -> Result<Json<EpochParamContent>, Error> {
    let tip = domain.get_tip_slot()?;
    let summary = domain.get_chain_summary()?;
    let (curr, _) = summary.slot_epoch(tip);

    let epoch = if epoch == curr {
        dolos_cardano::load_epoch::<D>(domain.state())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        domain
            .get_epoch_log(epoch, &summary)?
            .ok_or(StatusCode::NOT_FOUND)?
    };

    let model = ParametersModelBuilder {
        epoch: epoch.number,
        params: epoch.pparams.live().cloned().unwrap_or_default(),
        genesis: &domain.genesis(),
        nonce: epoch.nonces.map(|x| x.active.to_string()),
    };

    Ok(model.into_response()?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn epochs_by_number_parameters_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/0/parameters";
        let (status, bytes) = app.get_bytes(path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: EpochParamContent =
            serde_json::from_slice(&bytes).expect("failed to parse epoch parameters");
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
}
