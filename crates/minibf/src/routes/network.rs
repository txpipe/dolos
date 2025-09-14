use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::{
    network::Network, network_eras_inner::NetworkErasInner,
    network_eras_inner_end::NetworkErasInnerEnd,
    network_eras_inner_parameters::NetworkErasInnerParameters,
    network_eras_inner_start::NetworkErasInnerStart, network_stake::NetworkStake,
    network_supply::NetworkSupply,
};
use chrono::{DateTime, FixedOffset};
use dolos_cardano::{
    model::{EpochState, EPOCH_KEY_MARK},
    ChainSummary, EraSummary,
};
use dolos_core::{Domain, Genesis};

use crate::{mapping::IntoModel, Facade};

struct EraModelBuilder<'a> {
    system_start: u64,
    era: &'a EraSummary,
    genesis: &'a Genesis,
}

impl<'a> IntoModel<NetworkErasInner> for EraModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<NetworkErasInner, StatusCode> {
        let start_time = self.era.slot_time(self.era.start.slot);
        let start_delta = start_time - self.system_start;

        let end = self
            .era
            .end
            .as_ref()
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let end_time = self.era.slot_time(end.slot);
        let end_delta = end_time - self.system_start;

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
                epoch_length: self.era.epoch_length as i32,
                slot_length: self.era.slot_length as f64,
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

struct NetworkModelBuilder<'a> {
    genesis: &'a Genesis,
    state: EpochState,
}

impl<'a> IntoModel<Network> for NetworkModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Network, StatusCode> {
        let max_supply = self.genesis.shelley.max_lovelace_supply.unwrap_or_default();
        let total_supply = max_supply.saturating_sub(self.state.reserves);
        let circulating = total_supply.saturating_sub(self.state.deposits);

        Ok(Network {
            supply: Box::new(NetworkSupply {
                max: max_supply.to_string(),
                total: total_supply.to_string(),
                circulating: circulating.to_string(),
                locked: self.state.deposits.to_string(),
                treasury: self.state.treasury.to_string(),
                reserves: self.state.reserves.to_string(),
            }),
            stake: Box::new(NetworkStake {
                live: self.state.active_stake.to_string(),
                active: self.state.active_stake.to_string(),
            }),
        })
    }
}

pub async fn naked<D: Domain>(State(domain): State<Facade<D>>) -> Result<Json<Network>, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    let genesis = domain.genesis();

    let state = domain
        .read_cardano_entity::<EpochState>(EPOCH_KEY_MARK)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let builder = NetworkModelBuilder { genesis, state };

    builder.into_response()
}
