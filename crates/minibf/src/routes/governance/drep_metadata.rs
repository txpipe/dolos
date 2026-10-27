use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::drep_metadata::DrepMetadata;
use dolos_cardano::model::DRepSightingState;
use dolos_core::Domain;

use crate::{
    mapping::governance::dreps::parse_drep_id,
    offchain::{fetch_drep_metadata, OnOverload},
    Facade,
};

use super::drep_key;

/// `GET /governance/dreps/{drep_id}/metadata`: the newest anchor a
/// registration or update of the DRep carried, and the off-chain metadata
/// for that anchor.
///
/// db-sync returns the latest registration or update that still has an
/// anchor: its query inner-joins `voting_anchor` and takes the newest row. So
/// a registration or update without one keeps the older anchor, though the
/// ledger clears it. The DRep's sighting holds that anchor, and the list
/// serves it too.
///
/// A DRep that never carried an anchor is a 404, not an empty body. The two
/// special DReps have no registration, so they are also a 404.
///
/// If the fetch fails, Dolos keeps the anchor and puts the cause in `error`,
/// like the proposal metadata endpoints. The hex and the DRep id use the
/// same CIP-129 or legacy form as `drep_by_id`.
pub async fn drep_metadata<D: Domain>(
    Path(drep): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<DrepMetadata>, StatusCode> {
    let parsed = parse_drep_id(&drep)?;

    // The special DReps have no registration, so they have no anchor.
    if parsed.is_special {
        return Err(StatusCode::NOT_FOUND);
    }

    let sighting = domain
        .read_cardano_entity::<DRepSightingState>(&drep_key(&parsed.encoded)?)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let anchor = sighting.anchor.as_ref().ok_or(StatusCode::NOT_FOUND)?;

    let hex = if parsed.is_legacy {
        hex::encode(&parsed.encoded[1..])
    } else {
        hex::encode(&parsed.encoded)
    };

    let gateways = domain.config.ipfs_gateways();

    // the cache the list fills, so one anchor reads alike on both endpoints
    // and a verified body is fetched once
    let metadata =
        fetch_drep_metadata(domain.inner.cache(), &gateways, anchor, OnOverload::Inline).await;

    Ok(Json(DrepMetadata {
        drep_id: parsed.drep_id,
        hex,
        url: metadata.url,
        hash: metadata.hash,
        json_metadata: metadata.json_metadata,
        bytes: metadata.bytes,
        error: metadata.error,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::governance::testing::*,
        test_support::{TestApp, TestFault},
    };
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use pallas::{crypto::hash::Hasher, ledger::primitives::conway::Anchor};

    #[tokio::test]
    async fn governance_drep_metadata_returns_anchor_error() {
        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let parsed = parse_drep_id(&vectors.drep_id).expect("failed to parse drep id");
            seed_drep_anchor(domain, parsed.encoded);
        });

        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}/metadata");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let model: DrepMetadata =
            serde_json::from_slice(&body).expect("failed to parse drep metadata");

        assert_eq!(&model.drep_id, drep);
        // the drep id in the vectors is the 29-byte CIP-129 form, so the hex keeps the
        // header
        assert_eq!(model.hex, hex::encode([vec![0x22], vec![7u8; 28]].concat()));
        assert_eq!(model.url, "https://example.invalid/drep");
        assert_eq!(model.hash, hex::encode([9u8; 32]));
        assert!(model.json_metadata.is_none());
        assert!(model.bytes.is_none());
        assert_eq!(
            model.error.expect("the fetch error is absent").code,
            blockfrost_openapi::models::dreps_inner_metadata_error::Code::ConnectionError
        );
    }

    /// `/governance/dreps/{drep_id}/metadata` answers from the cache the list
    /// fills: a body kept there is served without a fetch, which for this
    /// unresolvable url could only end in a connection error.
    #[tokio::test]
    async fn governance_drep_metadata_answers_from_the_metadata_cache() {
        let body = br#"{"name":"cached"}"#;
        let anchor = Anchor {
            url: "https://example.invalid/cached".to_string(),
            content_hash: Hasher::<256>::hash(body),
        };

        let app = TestApp::new_with_cfg_and_setup(SyntheticBlockConfig::default(), {
            let anchor = anchor.clone();

            move |domain, vectors| {
                let parsed = parse_drep_id(&vectors.drep_id).expect("failed to parse drep id");
                seed_drep_with_anchor(domain, parsed.encoded, anchor);
            }
        });

        crate::offchain::cache_verified_body(
            app.facade().inner.cache(),
            crate::offchain::OFFCHAIN.name,
            &anchor,
            body,
        )
        .await;

        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}/metadata");
        let (status, response) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let model: DrepMetadata =
            serde_json::from_slice(&response).expect("failed to parse drep metadata");

        assert_eq!(model.url, anchor.url);
        assert_eq!(
            model.json_metadata,
            Some(serde_json::json!({"name": "cached"}))
        );
        assert_eq!(model.bytes, Some(format!("\\x{}", hex::encode(body))));
        assert!(model.error.is_none());
    }

    /// An update without an anchor leaves the one the registration carried,
    /// as db-sync keeps it, though the ledger clears it.
    #[tokio::test]
    async fn governance_drep_metadata_keeps_the_anchor_an_update_omits() {
        use pallas::ledger::primitives::{
            conway::{Certificate, DRep},
            StakeCredential,
        };

        let hash = [0xe2; 28];
        let credential = StakeCredential::AddrKeyhash(hash.into());

        let registered = Anchor {
            url: "https://example.invalid/drep/registered".to_string(),
            content_hash: [0x51; 32].into(),
        };

        // block 0 registers the DRep with an anchor, block 1 updates it
        // without one
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 2,
            extra_certs_by_block: vec![
                vec![vec![Certificate::RegDRepCert(
                    credential.clone(),
                    1000,
                    Some(registered.clone()),
                )]],
                vec![vec![Certificate::UpdateDRepCert(credential, None)]],
            ],
            ..Default::default()
        });

        let drep = crate::mapping::bech32_drep(&DRep::Key(hash.into())).unwrap();

        let path = format!("/governance/dreps/{drep}/metadata");
        let (status, body) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let model: DrepMetadata =
            serde_json::from_slice(&body).expect("failed to parse drep metadata");

        assert_eq!(model.url, registered.url);
        assert_eq!(model.hash, hex::encode(registered.content_hash));
    }

    #[tokio::test]
    async fn governance_drep_metadata_without_anchor_returns_404() {
        // the default synthetic drep registers with no anchor
        let app = TestApp::new();
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}/metadata");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_metadata_bad_request() {
        let app = TestApp::new();
        let path = format!("/governance/dreps/{}/metadata", invalid_drep());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn governance_drep_metadata_not_found() {
        let app = TestApp::new();
        let missing = missing_drep();
        let path = format!("/governance/dreps/{missing}/metadata");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn governance_drep_metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let drep = &app.vectors().drep_id;
        let path = format!("/governance/dreps/{drep}/metadata");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
