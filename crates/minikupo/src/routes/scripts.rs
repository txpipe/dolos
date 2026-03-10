use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_core::Domain;
use pallas::crypto::hash::Hash;

use crate::{bad_request, Facade};

pub async fn by_hash<D: Domain + Clone + Send + Sync + 'static>(
    State(facade): State<Facade<D>>,
    Path(script_hash): Path<String>,
) -> Response {
    let hash = match parse_script_hash(&script_hash) {
        Ok(hash) => hash,
        Err(_) => return bad_request(script_hash_hint()).into_response(),
    };

    let script = match facade.resolve_script(&hash).await {
        Ok(value) => value,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    (StatusCode::OK, Json(script)).into_response()
}

fn parse_script_hash(value: &str) -> Result<Hash<28>, StatusCode> {
    if value.len() != 56 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let bytes = hex::decode(value).map_err(|_| StatusCode::BAD_REQUEST)?;
    let bytes: [u8; 28] = bytes.try_into().map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Hash::new(bytes))
}

fn script_hash_hint() -> String {
    "Invalid script hash. Hash must be 56 lowercase hex characters.".to_string()
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::{
        test_support::{TestApp, TestFault},
        types::{Script, ScriptLanguage},
    };

    fn missing_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[tokio::test]
    async fn scripts_happy_path() {
        let app = TestApp::new();
        let path = format!("/scripts/{}", app.vectors().script_hash);
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let script: Script =
            serde_json::from_slice(&bytes).expect("failed to parse script response");
        assert_eq!(script.language, ScriptLanguage::Native);
        assert_eq!(script.script, app.vectors().script_cbor_hex);
    }

    #[tokio::test]
    async fn scripts_missing_returns_null() {
        let app = TestApp::new();
        let path = format!("/scripts/{}", missing_hash());
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);
        let script: Option<Script> =
            serde_json::from_slice(&bytes).expect("failed to parse null script response");
        assert_eq!(script, None);
    }

    #[tokio::test]
    async fn scripts_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/scripts/not-a-hash", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn scripts_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/scripts/{}", app.vectors().script_hash);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
