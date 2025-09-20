use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;

use dolos_cardano::{EpochState, FixedNamespace, EPOCH_KEY_SET};
use dolos_core::{Domain, StateStore};

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
    let nonce = domain
        .state()
        .read_entity_typed::<EpochState>(EpochState::NS, &EPOCH_KEY_SET.into())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?
        .nonces
        .map(|x| x.active.to_string());

    let model = mapping::ParametersModelBuilder {
        epoch: epoch as u64,
        params,
        genesis: domain.genesis(),
        nonce,
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
        nonce: None,
    };

    model.into_response()
}
