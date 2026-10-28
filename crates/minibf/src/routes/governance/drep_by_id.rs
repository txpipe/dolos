use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::model::DRepState;
use dolos_core::Domain;

use crate::{
    mapping::{
        governance::dreps::{drep_amount, parse_drep_id, DrepModelBuilder},
        IntoModel,
    },
    Facade,
};

use super::{drep_distribution, drep_key, read_drep_row};

pub async fn drep_by_id<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<blockfrost_openapi::models::drep::Drep>, StatusCode>
where
    Option<DRepState>: From<D::Entity>,
{
    let parsed = parse_drep_id(&drep)?;
    let key = drep_key(&parsed.encoded)?;

    let chain = domain.get_chain_summary()?;

    // the special DReps have no row
    let drep_state = read_drep_row(&domain.inner, &chain, &key)?;

    if drep_state.is_none() && !parsed.is_special {
        return Err(StatusCode::NOT_FOUND);
    }

    let tip = domain.get_tip_slot()?;
    let pparams = domain.get_current_effective_pparams()?;

    let amount = drep_amount(&key.0, drep_state.as_ref(), &drep_distribution(&domain)?);

    let model = DrepModelBuilder {
        drep_id: parsed.drep_id,
        drep_id_encoded: parsed.encoded,
        is_legacy: parsed.is_legacy,
        is_special: parsed.is_special,
        state: drep_state,
        amount,
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
            // registered after the last completed distribution, so in none
            // yet: Blockfrost serves 0, not the deposit
            amount: "0".to_string(),
            active: true,
            active_epoch: Some(2),
            has_script: false,
            retired: false,
            expired: false,
            last_active_epoch: Some(2),
        };

        assert_eq!(model, expected);
    }

    /// The special DReps have no row; they serve their entries in the last
    /// completed distribution, as Blockfrost does.
    #[tokio::test]
    async fn governance_drep_special_ids() {
        use pallas::ledger::primitives::conway::DRep;

        let app = TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
            seed_distribution(
                domain,
                [(DRep::Abstain, 40), (DRep::NoConfidence, 50)].into(),
            );
        });

        for (id, amount) in [
            ("drep_always_abstain", 40),
            ("drep_always_no_confidence", 50),
        ] {
            let model = get_drep(&app, id).await;

            let expected = DrepModel {
                drep_id: id.to_string(),
                hex: "".to_string(),
                amount: amount.to_string(),
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

    /// A regular DRep serves its entry in the last completed distribution,
    /// never the partial one the next boundary's shards are building.
    #[tokio::test]
    async fn governance_drep_amount_is_the_completed_distribution_entry() {
        let app = TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), |domain, _| {
            seed_distribution(domain, [(synthetic_drep(), 1_234)].into());
            seed_partial_distribution(domain, [(synthetic_drep(), 9_999)].into());
        });

        let model = get_drep(&app, &app.vectors().drep_id.clone()).await;

        assert_eq!(model.amount, "1234");
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
