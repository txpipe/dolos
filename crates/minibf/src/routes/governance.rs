use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::{model::DRepState, pallas_extras, ChainSummary, PParamsSet};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::ledger::primitives::Epoch;

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
    drep_id_encoded: Vec<u8>,
    state: DRepState,
    pparams: PParamsSet,
    chain: &'a ChainSummary,
    tip: BlockSlot,
}

impl<'a> DrepModelBuilder<'a> {
    fn is_special_case(&self) -> bool {
        ["drep_always_abstain", "drep_always_no_confidence"].contains(&self.drep_id.as_str())
    }

    fn first_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
            return None;
        }

        self.state.initial_slot.map(|x| self.chain.slot_epoch(x).0)
    }

    fn last_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
            return None;
        }

        self.state
            .last_active_slot
            .map(|x| self.chain.slot_epoch(x).0)
    }

    fn is_drep_expired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }

        let last_active_epoch = self.last_active_epoch();

        let inactivity_period = self.pparams.drep_inactivity_period().unwrap_or_default();

        let expiring_epoch = last_active_epoch.map(|x| x + inactivity_period);

        let (current_epoch, _) = self.chain.slot_epoch(self.tip);

        expiring_epoch
            .map(|expiration| expiration <= current_epoch)
            .unwrap_or(false)
    }

    fn is_drep_retired(&self) -> bool {
        if self.is_special_case() {
            return false;
        }
        
        let (current_epoch, _) = self.chain.slot_epoch(self.tip);
        match (self.state.initial_slot, self.state.unregistered_at) {
            (Some(registered), Some(unregistered)) => {
                registered > unregistered || self.chain.slot_epoch(unregistered).0 <= current_epoch
            },
            (Some(_), None) => false,
            _ => unreachable!()
        }
    }

    fn is_drep_active(&self) -> bool {
        if self.is_special_case() {
            return true;
        }

        let (current_epoch, _) = self.chain.slot_epoch(self.tip);
        self.state.unregistered_at.map(|x| self.chain.slot_epoch(x).0 <= current_epoch).unwrap_or(false)
    }
}

impl<'a> IntoModel<blockfrost_openapi::models::drep::Drep> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<blockfrost_openapi::models::drep::Drep, StatusCode> {
        let expired = self.is_drep_expired();

        let out = blockfrost_openapi::models::drep::Drep {
            drep_id: self.drep_id.clone(),
            hex: if self.is_special_case() {
                "".to_string()
            } else {
                hex::encode(&self.drep_id_encoded)
            },
            amount: self.state.voting_power.to_string(),
            active: self.is_drep_active(),
            active_epoch: self.first_active_epoch().map(|x| x as i32),
            has_script: pallas_extras::drep_id_is_script(&self.drep_id_encoded),
            retired: self.is_drep_retired(),
            expired,
            last_active_epoch: self.last_active_epoch().map(|x| x as i32),
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

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let (epoch, _) = chain.slot_epoch(tip);

    let pparams = domain.get_current_effective_pparams(epoch)?;

    let model = DrepModelBuilder {
        drep_id: drep,
        drep_id_encoded: drep_bytes,
        state: drep_state,
        pparams,
        chain: &chain,
        tip,
    };

    model.into_response()
}
