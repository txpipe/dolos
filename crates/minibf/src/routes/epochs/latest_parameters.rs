use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::{epochs::ParametersModelBuilder, IntoModel as _},
    Facade,
};

pub async fn latest_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<EpochParamContent>, Error> {
    let tip = domain.get_tip_slot()?;

    let summary = domain.get_chain_summary()?;

    let (epoch, _) = summary.slot_epoch(tip);

    let state = dolos_cardano::load_epoch::<D>(domain.state())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

    #[tokio::test]
    async fn epochs_latest_parameters_happy_path() {
        let app = TestApp::new();
        let path = "/epochs/latest/parameters";
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
    async fn epochs_latest_parameters_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let path = "/epochs/latest/parameters";
        assert_status(&app, path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
