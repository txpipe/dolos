use axum::{Json, extract::State, http::StatusCode};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;

use dolos_core::{Domain, StateStore as _};

use crate::{Facade, mapping::IntoModel as _};

pub mod cost_models;
pub mod mapping;

pub async fn latest_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<EpochParamContent>, StatusCode> {
    let tip = domain
        .state()
        .cursor()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let summary = domain.get_chain_summary()?;

    let era = summary.era_for_slot(tip.as_ref().unwrap().slot());

    let model = mapping::ParametersModelBuilder {
        epoch: era.start.epoch,
        params: era.pparams.clone(),
        genesis: domain.genesis(),
    };

    model.into_response()
}
