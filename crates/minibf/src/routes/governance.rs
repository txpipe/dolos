use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::{model::DRepState, pparams::ChainSummary, slot_epoch};
use dolos_core::{ArchiveStore, BlockSlot, Domain, State3Store as _};
use pallas::ledger::validate::utils::MultiEraProtocolParameters;

use crate::{mapping::IntoModel, Facade};

fn parse_drep_id(drep_id: &str) -> Result<Vec<u8>, StatusCode> {
    match drep_id {
        "drep_always_abstain" => Ok(vec![0]),
        "drep_always_no_confidence" => Ok(vec![1]),
        drep_id => {
            let (hrp, drep_id) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

            if hrp.as_str() != "drep" {
                return Err(StatusCode::BAD_REQUEST);
            }

            let header_byte = drep_id.first().ok_or(StatusCode::BAD_REQUEST)?;

            // first 4 bits need to be equal to 0010
            if header_byte & 0b11110000 != 0b00100000 {
                return Err(StatusCode::BAD_REQUEST);
            }

            Ok(drep_id)
        }
    }
}

pub struct DrepModelBuilder<'a> {
    drep_id: String,
    state: DRepState,
    chain: &'a ChainSummary,
    tip: BlockSlot,
}

impl<'a> DrepModelBuilder<'a> {
    fn is_special_case(&self) -> bool {
        ["drep_always_abstain", "drep_always_no_confidence"].contains(&self.drep_id.as_str())
    }
}

impl<'a> IntoModel<blockfrost_openapi::models::drep::Drep> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<blockfrost_openapi::models::drep::Drep, StatusCode> {
        let (epoch, _) = dolos_cardano::slot_epoch(self.tip, self.chain);
        let last_active_epoch = self
            .state
            .last_active_slot
            .map(|x| slot_epoch(x, self.chain).0 as i32);

        let drep_activity = match &self.chain.era_for_slot(self.tip).pparams {
            MultiEraProtocolParameters::Conway(params) => params.drep_inactivity_period,
            _ => return Err(StatusCode::INTERNAL_SERVER_ERROR),
        };

        let out = blockfrost_openapi::models::drep::Drep {
            drep_id: self.drep_id.clone(),
            hex: if self.is_special_case() {
                "".to_string()
            } else {
                hex::encode(&self.state.drep_id)
            },
            amount: self.state.voting_power.to_string(),
            active: self.state.initial_slot.is_some(),
            active_epoch: if self.is_special_case() {
                None
            } else {
                self.state
                    .initial_slot
                    .map(|x| slot_epoch(x, self.chain).0 as i32)
            },
            has_script: self.state.has_script(),
            retired: self.state.retired,
            expired: if self.is_special_case() {
                false
            } else {
                match last_active_epoch {
                    Some(last_active_epoch) => {
                        ((epoch as i32) - last_active_epoch) > drep_activity as i32
                    }
                    None => false,
                }
            },
            last_active_epoch: if self.is_special_case() {
                None
            } else {
                last_active_epoch
            },
        };

        Ok(out)
    }
}

pub async fn drep_by_id<D: Domain>(
    Path(drep_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode> {
    let key = parse_drep_id(&drep_id)?;

    let drep_state = domain
        .state3()
        .read_entity_typed::<dolos_cardano::model::DRepState>(&key)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = DrepModelBuilder {
        drep_id,
        state: drep_state,
        chain: &chain,
        tip,
    };

    model.into_response()
}
