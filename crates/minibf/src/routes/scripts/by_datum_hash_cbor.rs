//! `/scripts/datum/{datum_hash}/cbor`: a datum's CBOR.

use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::script_datum_cbor::ScriptDatumCbor;
use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::Domain;
use pallas::codec::minicbor;
use reqwest::StatusCode;

use crate::{error::Error, Facade};

use super::parse_datum_hash;

pub async fn by_datum_hash_cbor<D>(
    Path(datum_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptDatumCbor>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let datum = domain
        .query()
        .plutus_data(&parse_datum_hash(&datum_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ScriptDatumCbor {
        cbor: hex::encode(minicbor::to_vec(&datum).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::scripts::testing::{
            assert_status, fixture_app, invalid_datum_hash, missing_datum_hash,
        },
        test_support::{TestApp, TestFault},
    };

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_happy_path() {
        let app = fixture_app();
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptDatumCbor =
            serde_json::from_slice(&bytes).expect("failed to parse script datum cbor");
        assert_eq!(item.cbor, app.vectors().datum_cbor_hex);
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}/cbor", invalid_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/datum/{}/cbor", missing_datum_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_datum_hash_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let datum_hash = app.vectors().datum_hash.as_str();
        let path = format!("/scripts/datum/{datum_hash}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
