use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::{model::DRepState, ChainSummary, PParamsSet};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};

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
    pparams: PParamsSet,
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
        let (epoch, _) = self.chain.slot_epoch(self.tip);

        let last_active_epoch = self
            .state
            .last_active_slot
            .map(|x| self.chain.slot_epoch(x).0 as i32);

        let drep_activity = self.pparams.drep_inactivity_period_or_default() as i32;

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
                    .map(|x| self.chain.slot_epoch(x).0 as i32)
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
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let drep_bytes = parse_drep_id(&drep).map_err(|_| StatusCode::BAD_REQUEST)?;

    let drep_state = domain
        .read_cardano_entity::<DRepState>(drep_bytes.clone())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let pparams = domain
        .get_live_pparams()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let model = DrepModelBuilder {
        drep_id: drep,
        state: drep_state,
        pparams,
        chain: &chain,
        tip,
    };

    model.into_response()
}
