use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::epoch_param_content::EpochParamContent;

use dolos_cardano::{EpochState, FixedNamespace, EPOCH_KEY_SET};
use dolos_core::{ArchiveStore, Domain, LogKey, StateStore, TemporalKey};

use crate::{error::Error, mapping::IntoModel as _, Facade};

pub mod cost_models;
pub mod mapping;

pub async fn latest_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<EpochParamContent>, Error> {
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

    Ok(model.into_response()?)
}

pub async fn by_number_parameters<D: Domain>(
    State(domain): State<Facade<D>>,
    Path(epoch): Path<u64>,
) -> Result<Json<EpochParamContent>, Error> {
    let epoch_start = domain.get_chain_summary()?.epoch_start(epoch);
    let state = domain
        .archive()
        .read_log_typed::<EpochState>(
            EpochState::NS,
            &LogKey::from(TemporalKey::from(epoch_start)),
        )
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let params = state.pparams;
    let nonce = state.nonces.map(|x| x.active.to_string());

    let model = mapping::ParametersModelBuilder {
        epoch,
        params,
        genesis: domain.genesis(),
        nonce,
    };

    Ok(model.into_response()?)
}
