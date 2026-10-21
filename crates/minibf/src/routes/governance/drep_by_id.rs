use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::model::DRepState;
use dolos_core::Domain;

use crate::{
    mapping::{
        governance::dreps::{parse_drep_id, DrepModelBuilder},
        IntoModel,
    },
    Facade,
};

use super::{chain_context, drep_key};

pub async fn drep_by_id<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let parsed = parse_drep_id(&drep)?;

    let drep_state = if parsed.is_special {
        domain.read_cardano_entity::<DRepState>(&drep_key(&parsed.encoded)?)?
    } else {
        Some(
            domain
                .read_cardano_entity::<DRepState>(&drep_key(&parsed.encoded)?)?
                .ok_or(StatusCode::NOT_FOUND)?,
        )
    };

    let (chain, tip, pparams) = chain_context(&domain)?;

    let model = DrepModelBuilder {
        drep_id: parsed.drep_id,
        drep_id_encoded: parsed.encoded,
        is_legacy: parsed.is_legacy,
        is_special: parsed.is_special,
        state: drep_state,
        pparams: &pparams,
        chain: &chain,
        tip,
    };

    model.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::drep::Drep as DrepModel;
    use dolos_cardano::pallas_extras;
    use dolos_testing::synthetic::SyntheticBlockConfig;

    fn vector_drep_hash(app: &TestApp) -> Vec<u8> {
        let (_, payload) = bech32::decode(&app.vectors().drep_id).expect("invalid vector drep id");

        payload[1..].to_vec()
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
            // the ledger's drep_distr counts the DRep's own deposit
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
                amount: "0".to_string(),
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

        // Blockfrost rejects the drep_vkh prefix
        let path = format!("/governance/dreps/{}", encode_id("drep_vkh", &hash));
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
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
}
