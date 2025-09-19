use axum::{extract::State, http::StatusCode, Json};
use blockfrost_openapi::models::{
    network::Network, network_eras_inner::NetworkErasInner,
    network_eras_inner_end::NetworkErasInnerEnd,
    network_eras_inner_parameters::NetworkErasInnerParameters,
    network_eras_inner_start::NetworkErasInnerStart, network_stake::NetworkStake,
    network_supply::NetworkSupply,
};
use dolos_cardano::{model::EpochState, mutable_slots, EraSummary, FixedNamespace};
use dolos_core::{BlockSlot, Domain, Genesis, StateStore};

use crate::{mapping::IntoModel, routes::genesis::parse_datetime_into_timestamp, Facade};

struct EraModelBuilder<'a> {
    tip: BlockSlot,
    system_start: u64,
    era: &'a EraSummary,
    genesis: &'a Genesis,
}

impl<'a> IntoModel<NetworkErasInner> for EraModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<NetworkErasInner, StatusCode> {
        let start_time = self.era.slot_time(self.era.start.slot);
        let start_delta = start_time - self.system_start;

        let (end_slot, end_epoch) = self
            .era
            .end
            .as_ref()
            .map(|x| (x.slot, x.epoch))
            .unwrap_or((self.tip, self.era.slot_epoch(self.tip).0 as u64));

        let end_time = self.era.slot_time(end_slot);
        let end_delta = end_time - self.system_start;

        let out = NetworkErasInner {
            start: Box::new(NetworkErasInnerStart {
                time: start_delta as f64,
                slot: self.era.start.slot as i32,
                epoch: self.era.start.epoch as i32,
            }),
            end: Box::new(NetworkErasInnerEnd {
                time: end_delta as f64,
                slot: end_slot as i32,
                epoch: end_epoch as i32,
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
    tip: BlockSlot,
    eras: Vec<(u16, EraSummary)>,
    genesis: &'a Genesis,
}

impl<'a> IntoModel<Vec<NetworkErasInner>> for ChainModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Vec<NetworkErasInner>, StatusCode> {
        let system_start = self
            .genesis
            .shelley
            .system_start
            .as_ref()
            .map(|x| parse_datetime_into_timestamp(x))
            .transpose()?
            .unwrap_or_default() as u64;
        let mut out = vec![];

        // Special, hardcoded stuff.
        let known_hardforks = [2, 3, 4, 5, 7, 9];
        match self.genesis.shelley.network_magic {
            Some(764824073) => {
                let epoch_length = 4320;
                let slot_length = 20;
                let safe_zone = 864;
                let end_epoch = 0;

                out.push(NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0.0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: (end_epoch * epoch_length * slot_length) as f64,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length,
                        slot_length: slot_length as f64,
                        safe_zone,
                    }),
                });
            }
            Some(1) => {
                let epoch_length = 4320;
                let slot_length = 20;
                let safe_zone = 864;
                let end_epoch = 0;

                out.push(NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0.0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: (end_epoch * epoch_length * slot_length) as f64,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length,
                        slot_length: slot_length as f64,
                        safe_zone,
                    }),
                });
            }
            Some(2) => {
                let epoch_length = 4320;
                let slot_length = 20;
                let safe_zone = 864;
                let end_epoch = 0;

                out.push(NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0.0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: (end_epoch * epoch_length * slot_length) as f64,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length,
                        slot_length: slot_length as f64,
                        safe_zone,
                    }),
                });

                let other = NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0.0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: (end_epoch * epoch_length * slot_length) as f64,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length: self.genesis.shelley.epoch_length.unwrap() as i32,
                        slot_length: self.genesis.shelley.slot_length.unwrap() as f64,
                        safe_zone: mutable_slots(self.genesis) as i32,
                    }),
                };
                out.push(other.clone());
                out.push(other.clone());
                out.push(other);
            }
            _ => {}
        };

        out.extend(
            self.eras
                .iter()
                .flat_map(|(protocol, era)| {
                    if known_hardforks.contains(protocol) {
                        Some(EraModelBuilder {
                            tip: self.tip,
                            system_start,
                            era,
                            genesis: self.genesis,
                        })
                    } else {
                        None
                    }
                })
                .map(|era| era.into_model())
                .collect::<Result<Vec<_>, _>>()?,
        );

        Ok(out)
    }
}

pub async fn eras<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<NetworkErasInner>>, StatusCode> {
    let genesis = domain.genesis();
    let tip = domain
        .get_tip_slot()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let eras = domain
        .state()
        .iter_entities_typed::<EraSummary>(EraSummary::NS, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|x| {
            let (key, era) = x.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let key: &[u8; 2] = key.as_ref()[..2]
                .try_into()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let protocol = u16::from_be_bytes(*key);
            Ok((protocol, era))
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    let builder = ChainModelBuilder { eras, genesis, tip };

    builder.into_response()
}

struct NetworkModelBuilder<'a> {
    genesis: &'a Genesis,
    active: EpochState,
    live: EpochState,
}

impl<'a> IntoModel<Network> for NetworkModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Network, StatusCode> {
        let max_supply = self.genesis.shelley.max_lovelace_supply.unwrap_or_default();
        let total_supply = max_supply.saturating_sub(self.active.reserves);
        let circulating = total_supply.saturating_sub(self.active.deposits);

        Ok(Network {
            supply: Box::new(NetworkSupply {
                max: max_supply.to_string(),
                total: total_supply.to_string(),
                circulating: circulating.to_string(),
                locked: self.active.deposits.to_string(),
                treasury: self.active.treasury.to_string(),
                reserves: self.active.reserves.to_string(),
            }),
            stake: Box::new(NetworkStake {
                live: self.live.active_stake.to_string(),
                active: self.active.active_stake.to_string(),
            }),
        })
    }
}

pub async fn naked<D: Domain>(State(domain): State<Facade<D>>) -> Result<Json<Network>, StatusCode>
where
    Option<EpochState>: From<D::Entity>,
{
    let genesis = domain.genesis();

    let active = dolos_cardano::load_live_epoch(&domain.inner)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let live = dolos_cardano::load_live_epoch(&domain.inner)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let builder = NetworkModelBuilder {
        genesis,
        active,
        live,
    };

    builder.into_response()
}
