use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::{model::DRepState, pallas_extras, ChainSummary, PParamsSet};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::ledger::primitives::Epoch;

use crate::{
    mapping::{bech32, IntoModel},
    Facade,
};

fn parse_drep_id(drep_id: &str) -> Result<(String, Vec<u8>, bool, bool), StatusCode> {
    match drep_id {
        "drep_always_abstain" => Ok((drep_id.to_string(), vec![0], false, true)),
        "drep_always_no_confidence" => Ok((drep_id.to_string(), vec![1], false, true)),
        drep_id => {
            let (hrp, payload) = bech32::decode(drep_id).map_err(|_| StatusCode::BAD_REQUEST)?;

            match (hrp.as_str(), payload.len()) {
                ("drep", 29) => {
                    let header_byte = payload.first().ok_or(StatusCode::BAD_REQUEST)?;

                    // first 4 bits need to be equal to 0010
                    if header_byte & 0b11110000 != 0b00100000 {
                        return Err(StatusCode::BAD_REQUEST);
                    }

                    Ok((drep_id.to_string(), payload, false, false))
                }
                ("drep", 28) => Ok((
                    drep_id.to_string(),
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_vkh", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_KEY_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                ("drep_script", 28) => Ok((
                    bech32(bech32::Hrp::parse("drep").unwrap(), &payload)
                        .map_err(|_| StatusCode::BAD_REQUEST)?,
                    [vec![pallas_extras::DREP_SCRIPT_PREFIX], payload].concat(),
                    true,
                    false,
                )),
                _ => Err(StatusCode::BAD_REQUEST),
            }
        }
    }
}

pub struct DrepModelBuilder<'a> {
    drep_id: String,
    drep_id_encoded: Vec<u8>,
    is_legacy: bool,
    state: Option<DRepState>,
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

        self.state
            .as_ref()?
            .registered_at
            .map(|x| self.chain.slot_epoch(x.0).0)
    }

    fn last_active_epoch(&self) -> Option<Epoch> {
        if self.is_special_case() {
            return None;
        }

        self.state
            .as_ref()?
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

        let Some(state) = self.state.as_ref() else {
            return false;
        };

        match (state.registered_at, state.unregistered_at) {
            (Some(registered), Some(unregistered)) => unregistered > registered,
            (Some(_), None) => false,
            _ => false,
        }
    }

    fn is_drep_active(&self) -> bool {
        !self.is_drep_retired()
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
            } else if self.is_legacy {
                hex::encode(&self.drep_id_encoded[1..])
            } else {
                hex::encode(&self.drep_id_encoded)
            },
            amount: self
                .state
                .as_ref()
                .map(|x| x.voting_power.to_string())
                .unwrap_or_default(),
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
    let (drep, drep_bytes, is_legacy, is_special_case) =
        parse_drep_id(&drep).map_err(|_| StatusCode::BAD_REQUEST)?;

    let drep_state = if is_special_case {
        None
    } else {
        Some(
            domain
                .read_cardano_entity::<DRepState>(drep_bytes.clone())?
                .ok_or(StatusCode::NOT_FOUND)?,
        )
    };

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let pparams = domain.get_current_effective_pparams()?;

    let model = DrepModelBuilder {
        drep_id: drep,
        drep_id_encoded: drep_bytes,
        is_legacy,
        state: drep_state,
        pparams,
        chain: &chain,
        tip,
    };

    model.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use bech32::{Bech32, Hrp};

    fn invalid_drep() -> &'static str {
        "not-a-drep"
    }

    fn missing_drep() -> String {
        let mut payload = Vec::with_capacity(29);
        payload.push(0b00100010);
        payload.extend_from_slice(&[8u8; 28]);
        let hrp = Hrp::parse_unchecked("drep");
        bech32::encode::<Bech32>(hrp, &payload).expect("failed to encode missing drep")
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _body) = app.get_bytes(path).await;
        assert_eq!(status, expected);
    }

    #[tokio::test]
    async fn governance_drep_happy_path() {
        let app = TestApp::new();
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let _model: blockfrost_openapi::models::drep::Drep =
            serde_json::from_slice(&body).expect("failed to parse drep model");
    }

    #[tokio::test]
    async fn governance_drep_bad_request() {
        let app = TestApp::new();
        let path = format!("/governance/dreps/{}", invalid_drep());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_not_found() {
        let app = TestApp::new();
        let missing = missing_drep();
        let path = format!("/governance/dreps/{missing}");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
