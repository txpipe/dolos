use std::ops::Sub;

use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::{
    network_eras_inner::NetworkErasInner, network_eras_inner_end::NetworkErasInnerEnd,
    network_eras_inner_parameters::NetworkErasInnerParameters,
    network_eras_inner_start::NetworkErasInnerStart,
};
use chrono::{DateTime, FixedOffset};
use dolos_cardano::pparams::{ChainSummary, EraSummary};
use dolos_core::{Domain, Genesis};

use crate::{mapping::IntoModel, Facade};

struct EraModelBuilder<'a> {
    system_start: DateTime<FixedOffset>,
    era: &'a EraSummary,
    genesis: &'a Genesis,
}

impl<'a> IntoModel<NetworkErasInner> for EraModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<NetworkErasInner, StatusCode> {
        let start_time = dolos_cardano::slot_time_within_era(self.era.start.slot, self.era);
        let start_delta = start_time - self.system_start.timestamp() as u64;

        let end = self
            .era
            .end
            .as_ref()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let end_time = dolos_cardano::slot_time_within_era(end.slot, self.era);
        let end_delta = end_time - self.system_start.timestamp() as u64;

        let out = NetworkErasInner {
            start: Box::new(NetworkErasInnerStart {
                time: start_delta as f64,
                slot: self.era.start.slot as i32,
                epoch: self.era.start.epoch as i32,
            }),
            end: Box::new(NetworkErasInnerEnd {
                time: end_delta as f64,
                slot: end.slot as i32,
                epoch: end.epoch as i32,
            }),
            parameters: Box::new(NetworkErasInnerParameters {
                epoch_length: self.era.pparams.epoch_length() as i32,
                slot_length: self.era.pparams.slot_length() as f64,
                safe_zone: dolos_cardano::mutable_slots(self.genesis) as i32,
            }),
        };

        Ok(out)
    }
}

struct ChainModelBuilder<'a> {
    chain: ChainSummary,
    genesis: &'a Genesis,
}

impl<'a> IntoModel<Vec<NetworkErasInner>> for ChainModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<NetworkErasInner>, StatusCode> {
        let system_start = self.chain.first().start.timestamp;

        let out: Vec<_> = self
            .chain
            .iter_past()
            .map(|era| EraModelBuilder {
                system_start,
                era,
                genesis: self.genesis,
            })
            .map(|era| era.into_model())
            .collect::<Result<_, _>>()?;

        Ok(out)
    }
}

pub async fn eras<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<NetworkErasInner>>, StatusCode> {
    let chain = domain.get_chain_summary()?;
    let genesis = domain.genesis();

    let builder = ChainModelBuilder { chain, genesis };

    builder.into_response()
}
