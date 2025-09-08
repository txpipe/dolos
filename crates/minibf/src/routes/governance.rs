use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::drep;
use dolos_cardano::{model::DRepState, pparams::ChainSummary};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::{codec::minicbor, crypto::hash::Hash, ledger::primitives::conway::DRep};

use crate::{
    mapping::{self, IntoModel},
    Facade,
};

pub enum DrepIdType {
    Script,
    Vk,
}

fn parse_drep_id_type(id: &[u8]) -> Result<DrepIdType, StatusCode> {
    let header_byte = id.first().ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    // second 3 bits can be either 0010 or 0011 to indicate a script or a
    // verification key
    match header_byte & 0b00001111 {
        0b00000010 => Ok(DrepIdType::Script),
        0b00000011 => Ok(DrepIdType::Vk),
        _ => Err(StatusCode::BAD_REQUEST),
    }
}

fn parse_drep_id(drep_id: &str) -> Result<DRep, StatusCode> {
    let (hrp, drep_id) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

    if hrp.as_str() != "drep" {
        return Err(StatusCode::BAD_REQUEST);
    }

    let header_byte = drep_id.first().ok_or(StatusCode::BAD_REQUEST)?;

    // first 4 bits need to be equal to 0010
    if header_byte & 0b11110000 != 0b00100000 {
        return Err(StatusCode::BAD_REQUEST);
    }

    let cred_byte = header_byte & 0b00001111;

    let drep_id: Hash<28> = drep_id
        .get(1..)
        .ok_or(StatusCode::BAD_REQUEST)?
        .try_into()
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let drep = match cred_byte {
        0b00000010 => DRep::Script(drep_id),
        0b00000011 => DRep::Key(drep_id),
        _ => return Err(StatusCode::BAD_REQUEST),
    };

    Ok(drep)
}

pub struct DrepModelBuilder<'a> {
    drep: DRep,
    state: DRepState,
    chain: &'a ChainSummary,
    tip: BlockSlot,
}

impl<'a> IntoModel<blockfrost_openapi::models::drep::Drep> for DrepModelBuilder<'a> {
    type SortKey = ();

    fn into_model(self) -> Result<blockfrost_openapi::models::drep::Drep, StatusCode> {
        let has_script = matches!(self.drep, DRep::Script(_));

        let drep_bech32 = mapping::bech32_drep(&self.drep)?;
        let drep_cbor =
            minicbor::to_vec(&self.drep).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let (epoch, _) = dolos_cardano::slot_epoch(self.tip, self.chain);

        let active = self.state.start_epoch.map(|x| x <= epoch).unwrap_or(false);

        let out = blockfrost_openapi::models::drep::Drep {
            drep_id: drep_bech32,
            hex: hex::encode(drep_cbor),
            amount: self.state.voting_power.to_string(),
            active,
            active_epoch: self.state.start_epoch.map(|x| x as i32),
            has_script,
            retired: self.state.retired,
            expired: self.state.expired,
            last_active_epoch: self.state.last_active_epoch.map(|x| x as i32),
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
    let drep = parse_drep_id(&drep).map_err(|_| StatusCode::BAD_REQUEST)?;

    let drep_bytes = minicbor::to_vec(&drep).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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

    let model = DrepModelBuilder {
        drep,
        state: drep_state,
        chain: &chain,
        tip,
    };

    model.into_response()
}
