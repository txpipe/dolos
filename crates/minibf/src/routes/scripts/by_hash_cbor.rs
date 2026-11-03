//! `/scripts/{script_hash}/cbor`: a Plutus script's CBOR.

use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::script_cbor::ScriptCbor;
use dolos_cardano::indexes::{AsyncCardanoQueryExt, ScriptLanguage};
use dolos_core::Domain;
use reqwest::StatusCode;

use crate::{error::Error, Facade};

use super::parse_script_hash;

pub async fn by_hash_cbor<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptCbor>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let cbor = match script.language {
        ScriptLanguage::Native => None,
        _ => Some(hex::encode(script.script)),
    };

    Ok(Json(ScriptCbor { cbor }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::scripts::testing::{
            assert_status, fixture_app, invalid_script_hash, missing_script_hash,
        },
        test_support::{TestApp, TestFault},
    };

    #[tokio::test]
    async fn scripts_by_hash_cbor_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptCbor = serde_json::from_slice(&bytes).expect("failed to parse script cbor");
        assert_eq!(item.cbor, None);
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/cbor", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/cbor", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
