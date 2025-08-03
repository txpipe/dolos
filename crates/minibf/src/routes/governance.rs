use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::{model::DRepState, pparams::ChainSummary};
use dolos_core::{ArchiveStore, BlockSlot, Domain, State3Store as _};

use crate::{
    mapping::{self, IntoModel},
    Facade,
};

pub enum DrepIdType {
    Script,
    Vk,
}

fn parse_drep_id_type(id: &[u8]) -> Result<DrepIdType, StatusCode> {
    let header_byte = id.get(0).ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // second 3 bits can be either 0010 or 0011 to indicate a script or a
    // verification key
    match header_byte & 0b00001111 {
        0b00000010 => Ok(DrepIdType::Script),
        0b00000011 => Ok(DrepIdType::Vk),
        _ => {
            return Err(StatusCode::BAD_REQUEST);
        }
    }
}

fn parse_drep_id(drep_id: &str) -> Result<Vec<u8>, StatusCode> {
    let (hrp, drep_id) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    if hrp.as_str() != "drep" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let header_byte = drep_id.get(0).ok_or(StatusCode::BAD_REQUEST)?;

    // first 4 bits need to be equal to 0010
    if header_byte & 0b11110000 != 0b00100000 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let drep_id = drep_id.get(1..).ok_or(StatusCode::BAD_REQUEST)?.to_vec();

    Ok(drep_id)
}

pub struct DrepModelBuilder<'a> {
    drep_id: Vec<u8>,
    state: DRepState,
    chain: &'a ChainSummary,
    tip: BlockSlot,
}

impl<'a> IntoModel<blockfrost_openapi::models::drep::Drep> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<blockfrost_openapi::models::drep::Drep, StatusCode> {
        let drep_type =
            parse_drep_id_type(&self.drep_id).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let drep_bech32 = mapping::bech32_drep(&self.drep_id)?;

        let (epoch, _) = dolos_cardano::slot_epoch(self.tip, self.chain);

        let active = self.state.start_epoch.map(|x| x <= epoch).unwrap_or(false);

        let out = blockfrost_openapi::models::drep::Drep {
            drep_id: drep_bech32,
            hex: hex::encode(self.drep_id),
            amount: self.state.voting_power.to_string(),
            active,
            active_epoch: self.state.start_epoch.map(|x| x as i32),
            has_script: matches!(drep_type, DrepIdType::Script),
            retired: self.state.retired,
            expired: self.state.expired,
            last_active_epoch: self.state.last_active_epoch.map(|x| x as i32),
        };

        Ok(out)
    }
}

pub async fn drep_by_id<D: Domain>(
    Path(drep_id): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode> {
    let drep_id = parse_drep_id(&drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    let drep_state = domain
        .state3()
        .read_entity_typed::<dolos_cardano::model::DRepState>(&drep_id)
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
