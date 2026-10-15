use axum::http::StatusCode;
use blockfrost_openapi::models::{
    network::Network, network_eras_inner::NetworkErasInner,
    network_eras_inner_end::NetworkErasInnerEnd,
    network_eras_inner_parameters::NetworkErasInnerParameters,
    network_eras_inner_start::NetworkErasInnerStart, network_stake::NetworkStake,
    network_supply::NetworkSupply,
};
use dolos_cardano::{model::EpochState, mutable_slots, EraState};
use dolos_core::{BlockSlot, Genesis};

use super::{parse_datetime_into_timestamp, IntoModel};

pub struct ChainModelBuilder<'a> {
    pub tip: BlockSlot,
    pub eras: Vec<(u16, EraState)>,
    pub genesis: &'a Genesis,
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

        // Byron comes hardcoded per network; the loop below adds the eras
        // from Shelley on, one entry per era under its first protocol version.
        let known_hardforks = [2, 3, 4, 5, 7, 9];
        let mut previous = match self.genesis.shelley.network_magic {
            Some(764824073) => {
                let epoch_length = 21600;
                let slot_length = 20;
                let safe_zone = 4320;
                let end_epoch = 208;

                NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: end_epoch * epoch_length * slot_length,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length,
                        slot_length,
                        safe_zone,
                    }),
                }
            }
            Some(1) => {
                let epoch_length = 21600;
                let slot_length = 20;
                let safe_zone = 4320;
                let end_epoch = 4;

                NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: end_epoch * epoch_length * slot_length,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length,
                        slot_length,
                        safe_zone,
                    }),
                }
            }
            Some(2) => {
                let epoch_length = 4320;
                let slot_length = 20;
                let safe_zone = 864;
                let end_epoch = 0;

                out.push(NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: end_epoch * epoch_length * slot_length,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length,
                        slot_length,
                        safe_zone,
                    }),
                });

                // preview opens in Alonzo, so Shelley, Allegra and Mary are empty
                let other = NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: 0,
                        slot: 0,
                        epoch: 0,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: end_epoch * epoch_length * slot_length,
                        slot: end_epoch * epoch_length,
                        epoch: end_epoch,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length: self.genesis.shelley.epoch_length.unwrap() as i32,
                        slot_length: self.genesis.shelley.slot_length.unwrap() as i32,
                        safe_zone: mutable_slots(self.genesis) as i32,
                    }),
                };
                out.push(other.clone());
                out.push(other.clone());
                out.push(other.clone());
                other
            }
            _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };

        let eras: Vec<_> = self
            .eras
            .iter()
            .filter(|(protocol, _)| known_hardforks.contains(protocol))
            .collect();

        for (_, era) in eras {
            let start_time = era.slot_time(era.start.slot);
            let start_delta = start_time - system_start;

            // the tip ends the last era; the next era's start overwrites the rest
            let (end_slot, end_epoch) = (self.tip, era.slot_epoch(self.tip).0);
            let end_time = era.slot_time(end_slot);
            let end_delta = end_time - system_start;

            previous.end = Box::new(NetworkErasInnerEnd {
                time: start_delta as i32,
                slot: era.start.slot as i32,
                epoch: era.start.epoch as i32,
            });
            let current = NetworkErasInner {
                start: Box::new(NetworkErasInnerStart {
                    time: start_delta as i32,
                    slot: era.start.slot as i32,
                    epoch: era.start.epoch as i32,
                }),
                end: Box::new(NetworkErasInnerEnd {
                    time: end_delta as i32,
                    slot: end_slot as i32,
                    epoch: end_epoch as i32,
                }),
                parameters: Box::new(NetworkErasInnerParameters {
                    epoch_length: era.epoch_length as i32,
                    slot_length: era.slot_length as i32,
                    safe_zone: dolos_cardano::mutable_slots(self.genesis) as i32,
                }),
            };

            out.push(previous.clone());
            previous = current;
        }
        out.push(previous);

        Ok(out)
    }
}

pub struct NetworkModelBuilder<'a> {
    pub genesis: &'a Genesis,
    pub active: EpochState,
    pub live_stake: u64,
    pub active_stake: u64,
}

impl<'a> IntoModel<Network> for NetworkModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<Network, StatusCode> {
        let max_supply = self.genesis.shelley.max_lovelace_supply.unwrap_or_default();

        // TODO: Blockfrost's `total` is what Dolos calls circulating, and its
        // `circulating` leaves the deposits out; check the discrepancy.
        let total_supply = self.active.initial_pots.circulating();
        let circulating = self.active.initial_pots.utxos + self.active.initial_pots.rewards;

        Ok(Network {
            supply: Box::new(NetworkSupply {
                max: max_supply.to_string(),
                total: total_supply.to_string(),
                circulating: circulating.to_string(),
                locked: self.active.initial_pots.utxos.to_string(),
                treasury: self.active.initial_pots.treasury.to_string(),
                reserves: self.active.initial_pots.reserves.to_string(),
            }),
            // TODO: compute the snapshots the way the sweep does
            stake: Box::new(NetworkStake {
                live: self.live_stake.to_string(),
                active: self.active_stake.to_string(),
            }),
        })
    }
}
