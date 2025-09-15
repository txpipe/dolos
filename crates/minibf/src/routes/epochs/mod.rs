use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;

use dolos_core::Domain;

use crate::{mapping::IntoModel as _, Facade};

pub mod cost_models;
pub mod mapping;

pub async fn latest_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<EpochParamContent>, StatusCode> {
    let tip = domain.get_tip_slot()?;

    let summary = domain.get_chain_summary()?;

    let (epoch, _) = summary.slot_epoch(tip);

    let params = domain.get_live_pparams()?;

    let model = mapping::ParametersModelBuilder {
        epoch: epoch as u64,
        params,
        genesis: domain.genesis(),
    };

    model.into_response()
}

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<u64>,
) -> Result<Json<EpochParamContent>, StatusCode> {
    let params = domain.get_live_pparams()?;

    let model = mapping::ParametersModelBuilder {
        epoch,
        params,
        genesis: domain.genesis(),
    };

    model.into_response()
}
