//! `/scripts/{script_hash}/json`: a native script as JSON.

use axum::{
    extract::{Path, State},
    http::header,
    response::{IntoResponse, Response},
};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, ScriptLanguage};
use dolos_core::Domain;
use pallas::{
    codec::minicbor,
    ledger::primitives::{alonzo::NativeScript, ToCanonicalJson},
};
use reqwest::StatusCode;

use crate::{error::Error, Facade};

use super::parse_script_hash;

pub async fn by_hash_json<D>(
    Path(script_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Response, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let script = domain
        .query()
        .script_by_hash(&parse_script_hash(&script_hash)?)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    // Rendered straight to text instead of through `ScriptJson`: a native
    // script can nest as deep as a transaction has bytes, and a
    // `serde_json::Value` of that depth overflows the worker stack when
    // serialized or dropped (see pallas #806).
    let json = match script.language {
        ScriptLanguage::Native => {
            let native: NativeScript =
                minicbor::decode(&script.script).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            native.to_json_string()
        }
        _ => "null".to_string(),
    };

    let body = format!(r#"{{"json":{json}}}"#);
    Ok(([(header::CONTENT_TYPE, "application/json")], body).into_response())
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
    use blockfrost_openapi::models::script_json::ScriptJson;

    #[tokio::test]
    async fn scripts_by_hash_json_happy_path() {
        let app = fixture_app();
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/json");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);

        let item: ScriptJson = serde_json::from_slice(&bytes).expect("failed to parse script json");
        assert!(item.json.is_some());
    }

    #[tokio::test]
    async fn scripts_by_hash_json_null_for_plutus_script() {
        let app = fixture_app();
        let script_hash = app.vectors().plutus_script_hash.as_str();
        let path = format!("/scripts/{script_hash}/json");
        let (status, headers, bytes) = app.get_with_headers(&path).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers.get(header::CONTENT_TYPE).map(|x| x.as_bytes()),
            Some(b"application/json".as_slice())
        );

        let item: ScriptJson = serde_json::from_slice(&bytes).expect("failed to parse script json");
        assert!(item.json.is_none());
    }

    #[tokio::test]
    async fn scripts_by_hash_json_not_found_for_invalid_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/json", invalid_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_json_not_found_for_missing_hash() {
        let app = fixture_app();
        let path = format!("/scripts/{}/json", missing_script_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn scripts_by_hash_json_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let script_hash = app.vectors().script_hash.as_str();
        let path = format!("/scripts/{script_hash}/json");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
