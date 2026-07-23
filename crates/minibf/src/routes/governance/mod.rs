mod dreps;
mod metadata;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::DrepsInner;
use dolos_cardano::model::DRepState;
use dolos_core::{ArchiveStore as _, Domain};
use dreps::{drep_list_item, parse_drep_id, DrepModelBuilder};
use futures::future::join_all;
use metadata::fetch_drep_metadata;

use crate::{
    error::Error,
    mapping::IntoModel as _,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

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

        dreps.push((appeared_at, key, state));
    }

    dreps.sort_by(|(a_order, a_key, _), (b_order, b_key, _)| {
        (a_order, a_key).cmp(&(b_order, b_key))
    });

    if matches!(pagination.order, Order::Desc) {
        dreps.reverse();
    }

    let chain = domain.get_chain_summary()?;

    let (tip, _) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

    let pparams = domain.get_current_effective_pparams()?;

    let states: Vec<_> = dreps
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .map(|(_, _, state)| state)
        .collect();

    let metadata_futures: Vec<_> = states
        .iter()
        .map(|state| fetch_drep_metadata(state.anchor.clone()))
        .collect();

    let metadatas = join_all(metadata_futures).await;

    let page = states
        .into_iter()
        .zip(metadatas)
        .map(|(state, metadata)| {
            let mut model = drep_list_item(state, pparams.clone(), &chain, tip)?;
            model.metadata = metadata.map(Box::new);
            Ok(model)
        })
        .collect::<Result<Vec<_>, StatusCode>>()?;

    Ok(Json(page))
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
    use dolos_cardano::pallas_extras;
    use dolos_testing::synthetic::SyntheticBlockConfig;

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
        let app = TestApp::builder()
            .with_cfg(SyntheticBlockConfig {
                drep_deposit: 7777,
                ..Default::default()
            })
            .with_protocol(9)
            .build();

        let models = get_dreps_list(&app, "/governance/dreps").await;

        let expected_hash = vector_drep_hash(&app);

        assert_eq!(
            models,
            vec![DrepsInner {
                drep_id: encode_id("drep", &expected_hash),
                hex: hex::encode(&expected_hash),
                amount: "7777".to_string(),
                has_script: false,
                retired: false,
                expired: false,
                last_active_epoch: Some(2),
                metadata: None,
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
