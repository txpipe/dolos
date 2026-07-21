use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::DrepsInner;
use dolos_cardano::{model::DRepState, pallas_extras, ChainSummary, PParamsSet};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::ledger::primitives::{conway::DRep, Epoch};

use crate::{
    error::Error,
    mapping::{bech32, IntoModel, DREP_HRP},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

fn drep_list_item(identifier: &DRep) -> Result<DrepsInner, StatusCode> {
    let out = match identifier {
        DRep::Key(hash) | DRep::Script(hash) => DrepsInner {
            drep_id: bech32(DREP_HRP, hash)?,
            hex: hex::encode(hash),
        },
        DRep::Abstain => DrepsInner {
            drep_id: "drep_always_abstain".to_string(),
            hex: "".to_string(),
        },
        DRep::NoConfidence => DrepsInner {
            drep_id: "drep_always_no_confidence".to_string(),
            hex: "".to_string(),
        },
    };

    Ok(out)
}

pub async fn all_dreps<D: Domain>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<DrepsInner>>, Error>
where
    Option<DRepState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;

    let mut dreps = vec![];

    for item in domain.iter_cardano_entities::<DRepState>(None)? {
        let (key, state) = item.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // Blockfrost orders this list by first on-chain appearance.
        let appeared_at = state
            .registered_at
            .or_else(|| state.last_active_slot.map(|slot| (slot, 0)))
            .unwrap_or_default();

        dreps.push((appeared_at, key, state.identifier));
    }

    dreps.sort_by(|(a_order, a_key, _), (b_order, b_key, _)| {
        (a_order, a_key).cmp(&(b_order, b_key))
    });

    if matches!(pagination.order, Order::Desc) {
        dreps.reverse();
    }

    let page = dreps
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .map(|(_, _, identifier)| drep_list_item(&identifier))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(Json(page))
}

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

        if self
            .state
            .as_ref()
            .map(|x| x.is_unregistered())
            .unwrap_or(true)
        {
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

        if self.is_drep_retired() {
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
    use blockfrost_openapi::models::drep::Drep as DrepModel;
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::crypto::hash::Hash;

    fn invalid_drep() -> &'static str {
        "not-a-drep"
    }

    fn encode_id(hrp: &str, payload: &[u8]) -> String {
        let hrp = Hrp::parse_unchecked(hrp);
        bech32::encode::<Bech32>(hrp, payload).expect("failed to encode bech32 id")
    }

    fn missing_drep() -> String {
        let payload = [vec![pallas_extras::DREP_KEY_PREFIX], vec![8u8; 28]].concat();
        encode_id("drep", &payload)
    }

    fn vector_drep_hash(app: &TestApp) -> Vec<u8> {
        let (_, payload) = bech32::decode(&app.vectors().drep_id).expect("invalid vector drep id");

        payload[1..].to_vec()
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _body) = app.get_bytes(path).await;
        assert_eq!(status, expected);
    }

    async fn get_drep(app: &TestApp, drep_id: &str) -> DrepModel {
        let path = format!("/governance/dreps/{drep_id}");
        let (status, body) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&body)
        );

        serde_json::from_slice(&body).expect("failed to parse drep model")
    }

    #[test]
    fn parse_drep_id_special_cases() {
        assert_eq!(
            parse_drep_id("drep_always_abstain"),
            Ok(("drep_always_abstain".to_string(), vec![0], false, true))
        );

        assert_eq!(
            parse_drep_id("drep_always_no_confidence"),
            Ok((
                "drep_always_no_confidence".to_string(),
                vec![1],
                false,
                true
            ))
        );
    }

    #[test]
    fn parse_drep_id_cip105_key() {
        let hash = vec![7u8; 28];
        let drep_id = encode_id("drep", &hash);

        assert_eq!(
            parse_drep_id(&drep_id),
            Ok((
                drep_id.clone(),
                [vec![pallas_extras::DREP_KEY_PREFIX], hash].concat(),
                true,
                false,
            ))
        );
    }

    #[test]
    fn parse_drep_id_normalizes_vkh_and_script() {
        let hash = vec![7u8; 28];
        let cip105 = encode_id("drep", &hash);

        assert_eq!(
            parse_drep_id(&encode_id("drep_vkh", &hash)),
            Ok((
                cip105.clone(),
                [vec![pallas_extras::DREP_KEY_PREFIX], hash.clone()].concat(),
                true,
                false,
            ))
        );

        assert_eq!(
            parse_drep_id(&encode_id("drep_script", &hash)),
            Ok((
                cip105,
                [vec![pallas_extras::DREP_SCRIPT_PREFIX], hash].concat(),
                true,
                false,
            ))
        );
    }

    #[test]
    fn parse_drep_id_rejects_malformed_ids() {
        // not bech32
        assert!(parse_drep_id(invalid_drep()).is_err());
        // wrong hrp
        assert!(parse_drep_id(&encode_id("pool", &[7u8; 28])).is_err());
        // wrong payload
        assert!(parse_drep_id(&encode_id("drep", &[7u8; 27])).is_err());
        assert!(parse_drep_id(&encode_id("drep", &[7u8; 30])).is_err());
        assert!(parse_drep_id(&encode_id("drep_vkh", &[7u8; 29])).is_err());
        assert!(parse_drep_id(&encode_id("drep_script", &[7u8; 29])).is_err());
    }

    #[test]
    fn drep_list_item_variants() {
        let hash = Hash::<28>::from([7u8; 28]);

        assert_eq!(
            drep_list_item(&DRep::Key(hash)),
            Ok(DrepsInner {
                drep_id: encode_id("drep", hash.as_ref()),
                hex: hex::encode(hash.as_ref()),
            })
        );

        assert_eq!(
            drep_list_item(&DRep::Script(hash)),
            Ok(DrepsInner {
                drep_id: encode_id("drep", hash.as_ref()),
                hex: hex::encode(hash.as_ref()),
            })
        );

        assert_eq!(
            drep_list_item(&DRep::Abstain),
            Ok(DrepsInner {
                drep_id: "drep_always_abstain".to_string(),
                hex: "".to_string(),
            })
        );

        assert_eq!(
            drep_list_item(&DRep::NoConfidence),
            Ok(DrepsInner {
                drep_id: "drep_always_no_confidence".to_string(),
                hex: "".to_string(),
            })
        );
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

    #[tokio::test]
    async fn governance_drep_happy_path() {
        let app = TestApp::builder()
            .with_cfg(SyntheticBlockConfig {
                drep_deposit: 7777,
                ..Default::default()
            })
            .with_protocol(9)
            .build();

        let drep_id = app.vectors().drep_id.clone();
        let model = get_drep(&app, &drep_id).await;

        let (_, payload) = bech32::decode(&drep_id).expect("invalid vector drep id");

        let expected = DrepModel {
            drep_id,
            hex: hex::encode(&payload),
            amount: "7777".to_string(),
            active: true,
            active_epoch: Some(2),
            has_script: false,
            retired: false,
            expired: false,
            last_active_epoch: Some(2),
        };

        assert_eq!(model, expected);
    }

    #[tokio::test]
    async fn governance_drep_special_ids() {
        let app = TestApp::new();

        for id in ["drep_always_abstain", "drep_always_no_confidence"] {
            let model = get_drep(&app, id).await;

            let expected = DrepModel {
                drep_id: id.to_string(),
                hex: "".to_string(),
                amount: "".to_string(),
                active: true,
                active_epoch: None,
                has_script: false,
                retired: false,
                expired: false,
                last_active_epoch: None,
            };

            assert_eq!(model, expected);
        }
    }

    #[tokio::test]
    async fn governance_drep_by_id_accepts_legacy_encodings() {
        let app = TestApp::new();
        let hash = vector_drep_hash(&app);
        let cip105 = encode_id("drep", &hash);
        let cip129 = get_drep(&app, &app.vectors().drep_id.clone()).await;

        let expected = DrepModel {
            drep_id: cip105.clone(),
            hex: hex::encode(&hash),
            ..cip129
        };

        assert_eq!(get_drep(&app, &cip105).await, expected);
        assert_eq!(
            get_drep(&app, &encode_id("drep_vkh", &hash)).await,
            expected
        );
    }

    #[tokio::test]
    async fn governance_drep_by_id_script_variant_not_found() {
        let app = TestApp::new();
        let hash = vector_drep_hash(&app);

        let path = format!("/governance/dreps/{}", encode_id("drep_script", &hash));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        let cip129_script = [vec![pallas_extras::DREP_SCRIPT_PREFIX], hash].concat();
        let path = format!("/governance/dreps/{}", encode_id("drep", &cip129_script));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    async fn get_dreps_list(app: &TestApp, path: &str) -> Vec<DrepsInner> {
        let (status, body) = app.get_bytes(path).await;
        assert_eq!(status, StatusCode::OK);

        serde_json::from_slice(&body).expect("failed to parse dreps list")
    }

    #[tokio::test]
    async fn governance_dreps_list_happy_path() {
        let app = TestApp::new();
        let models = get_dreps_list(&app, "/governance/dreps").await;

        let expected_hash = vector_drep_hash(&app);

        assert_eq!(
            models,
            vec![DrepsInner {
                drep_id: encode_id("drep", &expected_hash),
                hex: hex::encode(&expected_hash),
            }]
        );
    }

    #[tokio::test]
    async fn governance_dreps_list_pagination() {
        let app = TestApp::new();

        let models = get_dreps_list(&app, "/governance/dreps?page=2").await;
        assert!(models.is_empty());

        let models = get_dreps_list(&app, "/governance/dreps?order=desc&count=1").await;
        assert_eq!(models.len(), 1);
    }

    #[tokio::test]
    async fn governance_dreps_list_bad_request() {
        let app = TestApp::new();

        assert_status(&app, "/governance/dreps?count=0", StatusCode::BAD_REQUEST).await;
        assert_status(
            &app,
            "/governance/dreps?order=sideways",
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn governance_dreps_list_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));

        assert_status(&app, "/governance/dreps", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
