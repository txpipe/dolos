use axum::{
    extract::{Path, State},
    Json,
};
use dolos_cardano::model::EpochState;
use dolos_core::Domain;

use crate::{
    error::Error,
    mapping::{epochs::ParametersModelBuilder, sorted_json, IntoModel as _},
    Facade,
};

use super::{current_epoch, load_epoch_state, parse_epoch};

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(number): Path<String>,
) -> Result<Json<serde_json::Value>, Error>
where
    Option<EpochState>: From<D::Entity>,
{
    let epoch = parse_epoch(&number)?;

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

    Ok(sorted_json(model.into_model()?)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};
    use axum::http::StatusCode;
    use blockfrost_openapi::models::epoch_param_content::EpochParamContent;

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

    #[tokio::test]
    async fn epochs_parameters_have_stable_bytes() {
        use dolos_cardano::{EpochValue, PParamValue};
        use dolos_core::{StateStore as _, StateWriter as _};
        use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};

        let app = TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
            let mut state = dolos_cardano::load_epoch::<ToyDomain>(domain.state()).unwrap();
            let params = state
                .pparams
                .live()
                .cloned()
                .unwrap_or_default()
                .with(PParamValue::CostModelsPlutusV1(vec![1, -2, 3]))
                .with(PParamValue::CostModelsPlutusV2(vec![4, 5, -6]))
                .with(PParamValue::CostModelsPlutusV3(vec![7, -8, 9]));
            state.pparams = EpochValue::with_live(state.number, params);
            let writer = domain.state().start_writer().unwrap();
            writer.write_entity_typed(&(), &state).unwrap();
            writer.commit().unwrap();
        });
        let epoch = app.tip_epoch();

        for path in [
            "/epochs/latest/parameters".to_string(),
            format!("/epochs/{epoch}/parameters"),
        ] {
            let (status, expected) = app.get_bytes(&path).await;
            assert_eq!(status, StatusCode::OK);

            let model: EpochParamContent = serde_json::from_slice(&expected).unwrap();
            assert_eq!(model.cost_models.unwrap().len(), 3);
            assert_eq!(model.cost_models_raw.flatten().unwrap().len(), 3);

            let body = std::str::from_utf8(&expected).unwrap();
            for field in ["cost_models", "cost_models_raw"] {
                let models = &body[body.find(&format!("\"{field}\":")).unwrap()..];
                assert!(
                    models.find("\"PlutusV1\"").unwrap() < models.find("\"PlutusV2\"").unwrap()
                );
                assert!(
                    models.find("\"PlutusV2\"").unwrap() < models.find("\"PlutusV3\"").unwrap()
                );
            }

            for _ in 0..16 {
                let (status, actual) = app.get_bytes(&path).await;
                assert_eq!(status, StatusCode::OK);
                assert_eq!(actual, expected, "response changed for {path}");
            }
        }
    }
}
