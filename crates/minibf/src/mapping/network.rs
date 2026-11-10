use axum::http::StatusCode;
use blockfrost_openapi::models::{
    network::Network, network_eras_inner::NetworkErasInner,
    network_eras_inner_end::NetworkErasInnerEnd,
    network_eras_inner_parameters::NetworkErasInnerParameters,
    network_eras_inner_start::NetworkErasInnerStart, network_stake::NetworkStake,
    network_supply::NetworkSupply,
};
use dolos_cardano::{model::EpochState, mutable_slots, EraBoundary, EraState};
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

        let table = dolos_cardano::era_table(&self.eras, self.genesis)
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        let byron_safe_zone = self.genesis.byron.protocol_consts.k as i32 * 2;
        let shelley_safe_zone = mutable_slots(self.genesis) as i32;

        Ok(table
            .iter()
            .map(|era| {
                // the tip ends the open era, and a rollback can leave it behind that era
                let end = era.end.clone().unwrap_or_else(|| {
                    let slot = self.tip.max(era.start.slot);

                    EraBoundary {
                        epoch: era.slot_epoch(slot).0,
                        slot,
                        timestamp: era.slot_time(slot),
                    }
                });

                NetworkErasInner {
                    start: Box::new(NetworkErasInnerStart {
                        time: era.start.timestamp.saturating_sub(system_start) as i32,
                        slot: era.start.slot as i32,
                        epoch: era.start.epoch as i32,
                    }),
                    end: Box::new(NetworkErasInnerEnd {
                        time: end.timestamp.saturating_sub(system_start) as i32,
                        slot: end.slot as i32,
                        epoch: end.epoch as i32,
                    }),
                    parameters: Box::new(NetworkErasInnerParameters {
                        epoch_length: era.epoch_length as i32,
                        slot_length: era.slot_length as i32,
                        safe_zone: if era.protocol < 2 {
                            byron_safe_zone
                        } else {
                            shelley_safe_zone
                        },
                    }),
                }
            })
            .collect())
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

#[cfg(test)]
mod tests {
    use dolos_testing::era_fixtures::PublicNetwork;
    use serde_json::{json, Value};

    use super::*;

    fn row(start: (i64, i64, i64), end: (i64, i64, i64), params: (i64, i64, i64)) -> Value {
        json!({
            "start": { "time": start.0, "slot": start.1, "epoch": start.2 },
            "end": { "time": end.0, "slot": end.1, "epoch": end.2 },
            "parameters": { "epoch_length": params.0, "slot_length": params.1, "safe_zone": params.2 },
        })
    }

    fn eras_json(network: &PublicNetwork, eras: Vec<(u16, EraState)>, tip: BlockSlot) -> Value {
        let model = ChainModelBuilder {
            tip,
            eras,
            genesis: &network.genesis,
        }
        .into_model()
        .unwrap();

        serde_json::to_value(model).unwrap()
    }

    fn table_json(network: &PublicNetwork) -> Value {
        eras_json(network, network.eras.clone(), network.tip)
    }

    #[test]
    fn mainnet_era_table_is_stable() {
        let shelley = (432000, 1, 129600);

        assert_eq!(
            table_json(&PublicNetwork::mainnet()),
            json!([
                row((0, 0, 0), (89856000, 4492800, 208), (21600, 20, 4320)),
                row(
                    (89856000, 4492800, 208),
                    (101952000, 16588800, 236),
                    shelley
                ),
                row(
                    (101952000, 16588800, 236),
                    (108432000, 23068800, 251),
                    shelley
                ),
                row(
                    (108432000, 23068800, 251),
                    (125280000, 39916800, 290),
                    shelley
                ),
                row(
                    (125280000, 39916800, 290),
                    (157680000, 72316800, 365),
                    shelley
                ),
                row(
                    (157680000, 72316800, 365),
                    (219024000, 133660800, 507),
                    shelley
                ),
                row(
                    (219024000, 133660800, 507),
                    (287855307, 202492107, 666),
                    shelley
                ),
            ])
        );
    }

    #[test]
    fn preprod_era_table_is_stable() {
        let shelley = (432000, 1, 129600);

        assert_eq!(
            table_json(&PublicNetwork::preprod()),
            json!([
                row((0, 0, 0), (1728000, 86400, 4), (21600, 20, 4320)),
                row((1728000, 86400, 4), (2160000, 518400, 5), shelley),
                row((2160000, 518400, 5), (2592000, 950400, 6), shelley),
                row((2592000, 950400, 6), (3024000, 1382400, 7), shelley),
                row((3024000, 1382400, 7), (5184000, 3542400, 12), shelley),
                row((5184000, 3542400, 12), (70416000, 68774400, 163), shelley),
                row(
                    (70416000, 68774400, 163),
                    (140016791, 138375191, 324),
                    shelley
                ),
            ])
        );
    }

    #[test]
    fn preview_era_table_is_stable() {
        let shelley = (86400, 1, 25920);

        assert_eq!(
            table_json(&PublicNetwork::preview()),
            json!([
                row((0, 0, 0), (0, 0, 0), (4320, 20, 864)),
                row((0, 0, 0), (0, 0, 0), shelley),
                row((0, 0, 0), (0, 0, 0), shelley),
                row((0, 0, 0), (0, 0, 0), shelley),
                row((0, 0, 0), (259200, 259200, 3), shelley),
                row((259200, 259200, 3), (55814400, 55814400, 646), shelley),
                row(
                    (55814400, 55814400, 646),
                    (127402390, 127402390, 1474),
                    shelley
                ),
            ])
        );
    }

    #[test]
    fn an_early_tip_keeps_the_padded_bounds() {
        // state holds nothing past the padding yet
        let mainnet = PublicNetwork::mainnet();
        assert_eq!(
            eras_json(&mainnet, vec![], 1000),
            json!([row((0, 0, 0), (89856000, 4492800, 208), (21600, 20, 4320))])
        );

        let preview = PublicNetwork::preview();
        let table = eras_json(&preview, preview.eras[..1].to_vec(), 1000);
        assert_eq!(
            table.as_array().unwrap().last().unwrap(),
            &row((0, 0, 0), (0, 0, 0), (86400, 1, 25920))
        );
    }

    #[test]
    fn a_tip_behind_the_open_era_ends_it_at_its_start() {
        let mainnet = PublicNetwork::mainnet();
        let table = eras_json(&mainnet, mainnet.eras.clone(), 0);

        assert_eq!(
            table.as_array().unwrap().last().unwrap(),
            &row(
                (219024000, 133660800, 507),
                (219024000, 133660800, 507),
                (432000, 1, 129600)
            )
        );
    }

    #[test]
    fn a_custom_network_is_an_internal_error() {
        let mut genesis = dolos_cardano::include::preview::load();
        genesis.shelley.network_magic = Some(42);

        let result = ChainModelBuilder {
            tip: 0,
            eras: vec![],
            genesis: &genesis,
        }
        .into_model();

        assert_eq!(result.unwrap_err(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
