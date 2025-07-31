use axum::{Json, extract::State, http::StatusCode};
use blockfrost_openapi::models::genesis_content::GenesisContent;
use dolos_core::{Domain, Genesis};

use crate::{
    Facade,
    mapping::{IntoModel, round_f64},
};

fn parse_datetime_into_timestamp(s: &str) -> Result<i32, axum::http::StatusCode> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map(|x| x.timestamp() as i32)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

pub struct GenesisModelBuilder<'a> {
    pub genesis: &'a Genesis,
}

impl<'a> IntoModel<GenesisContent> for GenesisModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<GenesisContent, axum::http::StatusCode> {
        let Self { genesis } = self;

        let out = GenesisContent {
            active_slots_coefficient: genesis
                .shelley
                .active_slots_coeff
                .map(|x| round_f64::<6>(x as f64))
                .unwrap_or_default(),
            max_lovelace_supply: genesis
                .shelley
                .max_lovelace_supply
                .unwrap_or_default()
                .to_string(),
            network_magic: genesis.shelley.network_magic.unwrap_or_default() as i32,
            epoch_length: genesis.shelley.epoch_length.unwrap_or_default() as i32,
            slot_length: genesis.shelley.slot_length.unwrap_or_default() as i32,
            slots_per_kes_period: genesis.shelley.slots_per_kes_period.unwrap_or_default() as i32,
            update_quorum: genesis.shelley.update_quorum.unwrap_or_default() as i32,
            max_kes_evolutions: genesis.shelley.max_kes_evolutions.unwrap_or_default() as i32,
            security_param: genesis.shelley.security_param.unwrap_or_default() as i32,
            system_start: genesis
                .shelley
                .system_start
                .as_ref()
                .map(|x| parse_datetime_into_timestamp(x))
                .transpose()?
                .unwrap_or_default(),
        };

        Ok(out)
    }
}

pub async fn naked<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<GenesisContent>, StatusCode> {
    let model = GenesisModelBuilder {
        genesis: domain.genesis(),
    };

    model.into_response()
}
