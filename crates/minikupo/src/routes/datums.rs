use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_core::Domain;
use pallas::crypto::hash::Hash;

use crate::{bad_request, Facade};

pub async fn by_hash<D: Domain>(
    State(facade): State<Facade<D>>,
    Path(datum_hash): Path<String>,
) -> Response {
    let bytes = match parse_datum_hash(&datum_hash) {
        Ok(bytes) => bytes,
        Err(_) => return bad_request(datum_hash_hint()).into_response(),
    };

    match facade.resolve_datum(&bytes).await {
        Ok(Some(datum)) => (StatusCode::OK, Json(datum)).into_response(),
        Ok(None) => (StatusCode::OK, Json(None::<crate::types::Datum>)).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

fn parse_datum_hash(value: &str) -> Result<Hash<32>, StatusCode> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let bytes = hex::decode(value).map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(bytes.as_slice().into())
}

fn datum_hash_hint() -> String {
    "Invalid datum hash. Hash must be 64 lowercase hex characters.".to_string()
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::{
        test_support::{TestApp, TestFault},
        types::Datum,
    };

    fn missing_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
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
    async fn datums_happy_path() {
        let app = TestApp::new();
        let path = format!("/datums/{}", app.vectors().datum_hash);
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let datum: Datum = serde_json::from_slice(&bytes).expect("failed to parse datum response");
        assert_eq!(datum.datum, app.vectors().datum_cbor_hex);
    }

    #[tokio::test]
    async fn datums_missing_returns_null() {
        let app = TestApp::new();
        let path = format!("/datums/{}", missing_hash());
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(status, StatusCode::OK);
        let datum: Option<Datum> =
            serde_json::from_slice(&bytes).expect("failed to parse null datum response");
        assert_eq!(datum, None);
    }

    #[tokio::test]
    async fn datums_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/datums/not-a-hash", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn datums_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/datums/{}", app.vectors().datum_hash);
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
