use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
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

    let tip_slot = tip.as_ref().unwrap().slot();

    let summary = domain.get_chain_summary()?;

    let era = summary.era_for_slot(tip_slot);

    let (epoch, _) = dolos_cardano::slot_epoch(tip_slot, &summary);

    let model = mapping::ParametersModelBuilder {
        epoch: epoch as u64,
        params: era.pparams.clone(),
        genesis: domain.genesis(),
    };

    model.into_response()
}

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<u64>,
) -> Result<Json<EpochParamContent>, StatusCode> {
    dbg!(&epoch);

    let summary = domain.get_chain_summary()?;

    let era = summary.era_for_epoch(epoch);

    let model = mapping::ParametersModelBuilder {
        epoch,
        params: era.pparams.clone(),
        genesis: domain.genesis(),
    };

    model.into_response()
}
