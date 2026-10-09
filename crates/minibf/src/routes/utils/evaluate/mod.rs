//! `POST /utils/txs/evaluate` and `POST /utils/txs/evaluate/utxos`.
//!
//! Like Blockfrost, Dolos sends these requests to an Ogmios v6 endpoint. It
//! answers in the Ogmios v5 format, or passes the v6 response through.

mod ogmios;
mod utxo_set;

use axum::{
    body::Bytes,
    extract::{Query, State},
    http::{header, HeaderMap},
    Json,
};
use base64::Engine as _;
use dolos_core::Domain;
use serde::Deserialize;
use serde_json::Value as JsonValue;

use crate::{error::Error, log_and_500, Facade};

#[derive(Debug, Deserialize)]
pub struct EvaluateParams {
    version: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EvaluateUtxosRequest {
    cbor: String,
    additional_utxo_set: Option<Vec<JsonValue>>,
}

/// The Ogmios version whose format the response uses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Version {
    V5,
    V6,
}

pub async fn txs_evaluate<D>(
    Query(params): Query<EvaluateParams>,
    State(domain): State<Facade<D>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<JsonValue>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    check_content_type(&headers, "application/cbor")?;
    let version = parse_version(params.version.as_deref())?;

    // Binary CBOR is never valid UTF-8, because a transaction starts with an
    // array header byte that cannot start a UTF-8 character.
    let cbor = match std::str::from_utf8(&body) {
        Ok(text) => base16_payload(text)?,
        Err(_) => hex::encode(&body),
    };

    respond(&domain, version, &cbor, vec![]).await
}

pub async fn txs_evaluate_utxos<D>(
    Query(params): Query<EvaluateParams>,
    State(domain): State<Facade<D>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<JsonValue>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    check_content_type(&headers, "application/json")?;
    let version = parse_version(params.version.as_deref())?;

    let request: EvaluateUtxosRequest = serde_json::from_slice(&body).map_err(|error| {
        Error::InvalidEvaluationRequest(format!("Invalid request body: {error}"))
    })?;

    let additional_utxo =
        utxo_set::to_v6(request.additional_utxo_set.as_deref().unwrap_or_default())
            .map_err(Error::InvalidEvaluationRequest)?;

    let cbor = base16_payload(&request.cbor)?;

    respond(&domain, version, &cbor, additional_utxo).await
}

async fn respond<D>(
    domain: &Facade<D>,
    version: Version,
    cbor: &str,
    additional_utxo: Vec<JsonValue>,
) -> Result<Json<JsonValue>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let url = domain
        .config
        .ogmios_url()
        .ok_or(Error::EvaluationNotConfigured)?;

    let response = ogmios::evaluate(url, cbor, additional_utxo)
        .await
        .map_err(log_and_500(
            "failed to evaluate the transaction with Ogmios",
        ))?;

    Ok(Json(match version {
        Version::V6 => response,
        Version::V5 => ogmios::to_v5(&response),
    }))
}

/// Returns the transaction as base16 text. Text with only hexadecimal digits
/// stays as it is, also with an odd length: Ogmios rejects it then. Other
/// text must be base64.
fn base16_payload(text: &str) -> Result<String, Error> {
    let text = text.trim();

    if !text.is_empty() && text.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Ok(text.to_string());
    }

    base64::engine::general_purpose::STANDARD
        .decode(text)
        .map(hex::encode)
        .map_err(|_| Error::InvalidTxPayload)
}

/// Blockfrost answers in the Ogmios v6 format for `version=6` and in the v5
/// format for any other 32-bit integer.
fn parse_version(version: Option<&str>) -> Result<Version, Error> {
    match version.map(str::parse::<i32>) {
        None => Ok(Version::V5),
        Some(Ok(6)) => Ok(Version::V6),
        Some(Ok(_)) => Ok(Version::V5),
        Some(Err(_)) => Err(Error::InvalidOgmiosVersion),
    }
}

/// Rejects a request without a Content-Type header, or with one that names
/// another media type.
fn check_content_type(headers: &HeaderMap, expected: &'static str) -> Result<(), Error> {
    let Some(value) = headers.get(header::CONTENT_TYPE) else {
        return Err(Error::MissingContentType);
    };

    let essence = value
        .to_str()
        .ok()
        .and_then(|value| value.split(';').next())
        .map(str::trim);

    match essence {
        Some(essence) if essence.eq_ignore_ascii_case(expected) => Ok(()),
        _ => Err(Error::InvalidContentType(expected)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use axum::{http::StatusCode, routing, Router};
    use serde_json::json;

    use super::*;
    use crate::test_support::TestApp;

    /// Spends `ffff…ff#0` with the redeemer "Hello, World!", from the
    /// Blockfrost test fixtures.
    const HELLO_TX: &str = "84A30081825820FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF00018182581D70C40F9129C2684046EB02325B96CA2899A6FA6478C1DDE9B5C53206A51A00D59F800200A10581840000D8799F4D48656C6C6F2C20576F726C6421FF820000F5F6";

    /// A mock Ogmios that answers every request with `response` and records
    /// the requests.
    struct MockOgmios {
        url: String,
        requests: Arc<Mutex<Vec<JsonValue>>>,
    }

    impl MockOgmios {
        async fn start(response: JsonValue) -> Self {
            let requests = Arc::new(Mutex::new(vec![]));
            let recorded = requests.clone();

            let app = Router::new().route(
                "/",
                routing::post(move |Json(request): Json<JsonValue>| {
                    recorded.lock().unwrap().push(request);
                    let response = response.clone();
                    async move { Json(response) }
                }),
            );

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

            Self { url, requests }
        }

        fn last_params(&self) -> JsonValue {
            let requests = self.requests.lock().unwrap();
            let request = requests.last().expect("one request");

            assert_eq!(request["jsonrpc"], "2.0");
            assert_eq!(request["method"], "evaluateTransaction");
            assert!(request["id"].is_string());

            request["params"].clone()
        }
    }

    fn success() -> JsonValue {
        json!({
            "jsonrpc": "2.0",
            "method": "evaluateTransaction",
            "result": [
                { "validator": { "index": 0, "purpose": "spend" }, "budget": { "memory": 15694, "cpu": 3776164 } }
            ],
            "id": "ogmios-id",
        })
    }

    async fn post(
        app: &TestApp,
        path: &str,
        content_type: &str,
        body: Vec<u8>,
    ) -> (StatusCode, JsonValue) {
        let (status, bytes) = app.post_bytes(path, content_type, body).await;
        (status, serde_json::from_slice(&bytes).expect("JSON body"))
    }

    #[tokio::test]
    async fn txs_evaluate_happy_path() {
        let ogmios = MockOgmios::start(success()).await;
        let app = TestApp::new_with_ogmios_url(&ogmios.url);

        let (status, body) = post(
            &app,
            "/utils/txs/evaluate",
            "application/cbor",
            HELLO_TX.into(),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            body,
            json!({
                "type": "jsonwsp/response",
                "version": "1.0",
                "servicename": "ogmios",
                "methodname": "EvaluateTx",
                "result": { "EvaluationResult": { "spend:0": { "memory": 15694, "steps": 3776164 } } },
                "reflection": { "id": "ogmios-id" },
            })
        );
        assert_eq!(
            ogmios.last_params(),
            json!({ "transaction": { "cbor": HELLO_TX } })
        );
    }

    #[tokio::test]
    async fn txs_evaluate_version_6_passes_the_response_through() {
        let ogmios = MockOgmios::start(success()).await;
        let app = TestApp::new_with_ogmios_url(&ogmios.url);

        for path in [
            "/utils/txs/evaluate?version=6",
            "/utils/txs/evaluate/utxos?version=6",
        ] {
            let body = if path.contains("utxos") {
                json!({ "cbor": HELLO_TX }).to_string().into_bytes()
            } else {
                HELLO_TX.into()
            };
            let content_type = if path.contains("utxos") {
                "application/json"
            } else {
                "application/cbor"
            };

            let (status, response) = post(&app, path, content_type, body).await;

            assert_eq!(status, StatusCode::OK, "{path}");
            assert_eq!(response, success(), "{path}");
        }
    }

    #[tokio::test]
    async fn txs_evaluate_forwards_each_payload_encoding_as_base16() {
        let ogmios = MockOgmios::start(success()).await;
        let app = TestApp::new_with_ogmios_url(&ogmios.url);
        let bytes = hex::decode(HELLO_TX).unwrap();
        let lowercase = HELLO_TX.to_lowercase();

        let cases = [
            (HELLO_TX.as_bytes().to_vec(), HELLO_TX.to_string()),
            (
                base64::engine::general_purpose::STANDARD
                    .encode(&bytes)
                    .into_bytes(),
                lowercase.clone(),
            ),
            (bytes.clone(), lowercase),
            // Ogmios rejects text with an odd length, so it goes through.
            (b"84a".to_vec(), "84a".to_string()),
        ];

        for (body, forwarded) in cases {
            let (status, _) = post(&app, "/utils/txs/evaluate", "application/cbor", body).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(ogmios.last_params()["transaction"]["cbor"], forwarded);
        }
    }

    #[tokio::test]
    async fn txs_evaluate_utxos_forwards_the_additional_utxos() {
        let ogmios = MockOgmios::start(success()).await;
        let app = TestApp::new_with_ogmios_url(&ogmios.url);

        let request = json!({
            "cbor": HELLO_TX,
            "additionalUtxoSet": [[
                { "txId": "ff".repeat(32), "index": 0 },
                {
                    "address": "addr_test1wrzqlyffcf5yq3htqge9h9k29zv6d7ny0rqam6d4c5eqdfgg0h7yw",
                    "value": { "coins": 14_000_000, "assets": { format!("{}.4d494e", "e1".repeat(28)): 1 } },
                    "datum": "d87980",
                    "script": { "plutus:v2": "4d01000033222220051200120011" },
                }
            ]],
        });

        let (status, body) = post(
            &app,
            "/utils/txs/evaluate/utxos",
            "application/json",
            request.to_string().into_bytes(),
        )
        .await;

        assert_eq!(status, StatusCode::OK);
        assert!(body["result"]["EvaluationResult"]["spend:0"].is_object());
        assert_eq!(
            ogmios.last_params(),
            json!({
                "transaction": { "cbor": HELLO_TX },
                "additionalUtxo": [{
                    "transaction": { "id": "ff".repeat(32) },
                    "index": 0,
                    "address": "addr_test1wrzqlyffcf5yq3htqge9h9k29zv6d7ny0rqam6d4c5eqdfgg0h7yw",
                    "value": { "ada": { "lovelace": 14_000_000 }, "e1".repeat(28): { "4d494e": 1 } },
                    "datum": "d87980",
                    "script": { "language": "plutus:v2", "cbor": "4d01000033222220051200120011" },
                }],
            })
        );
    }

    #[tokio::test]
    async fn txs_evaluate_not_configured() {
        let app = TestApp::new();

        for (path, content_type, body) in [
            (
                "/utils/txs/evaluate",
                "application/cbor",
                HELLO_TX.as_bytes().to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                json!({ "cbor": HELLO_TX }).to_string().into_bytes(),
            ),
        ] {
            let (status, body) = post(&app, path, content_type, body).await;

            assert_eq!(status, StatusCode::NOT_IMPLEMENTED, "{path}");
            assert_eq!(body["status_code"], 501);
            assert_eq!(body["error"], "Not Implemented");
        }
    }

    #[tokio::test]
    async fn txs_evaluate_ogmios_unreachable() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let app = TestApp::new_with_ogmios_url(&url);

        let (status, _) = app
            .post_bytes("/utils/txs/evaluate", "application/cbor", HELLO_TX.into())
            .await;

        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
    }

    #[tokio::test]
    async fn txs_evaluate_bad_request() {
        let ogmios = MockOgmios::start(success()).await;
        let app = TestApp::new_with_ogmios_url(&ogmios.url);

        let cases = [
            (
                "/utils/txs/evaluate",
                "application/cbor",
                b"invalid CBOR".to_vec(),
            ),
            (
                "/utils/txs/evaluate",
                "application/json",
                HELLO_TX.as_bytes().to_vec(),
            ),
            (
                "/utils/txs/evaluate?version=six",
                "application/cbor",
                HELLO_TX.as_bytes().to_vec(),
            ),
            (
                "/utils/txs/evaluate?version=2147483648",
                "application/cbor",
                HELLO_TX.as_bytes().to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/cbor",
                b"{}".to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                b"{".to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                br#"{"cbor": 1}"#.to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                br#"{"cbor": "invalid CBOR"}"#.to_vec(),
            ),
            (
                "/utils/txs/evaluate/utxos",
                "application/json",
                json!({ "cbor": HELLO_TX, "additionalUtxoSet": [[]] })
                    .to_string()
                    .into_bytes(),
            ),
        ];

        for (path, content_type, body) in cases {
            let (status, error) = post(&app, path, content_type, body).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{path} {content_type} {error}"
            );
            assert_eq!(error["status_code"], 400);
        }

        assert!(ogmios.requests.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn txs_evaluate_missing_content_type() {
        let app = TestApp::new_with_ogmios_url("http://127.0.0.1:1");

        for path in ["/utils/txs/evaluate", "/utils/txs/evaluate/utxos"] {
            let (status, body) = post(&app, path, "", HELLO_TX.as_bytes().to_vec()).await;

            assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE, "{path}");
            assert_eq!(
                body,
                json!({
                    "error": "Unsupported Media Type",
                    "message": "Unsupported Media Type: undefined",
                    "status_code": 415,
                })
            );
        }
    }

    #[tokio::test]
    async fn txs_evaluate_get_is_invalid_path() {
        let app = TestApp::new_with_ogmios_url("http://127.0.0.1:1");

        for path in ["/utils/txs/evaluate", "/utils/txs/evaluate/utxos"] {
            let (status, bytes) = app.get_bytes(path).await;
            let error: JsonValue = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(status, StatusCode::BAD_REQUEST, "{path}");
            assert_eq!(error["message"], "Invalid path.", "{path}");
        }
    }

    #[test]
    fn version_selects_the_format() {
        assert_eq!(parse_version(None).unwrap(), Version::V5);
        assert_eq!(parse_version(Some("5")).unwrap(), Version::V5);
        assert_eq!(parse_version(Some("7")).unwrap(), Version::V5);
        assert_eq!(parse_version(Some("6")).unwrap(), Version::V6);
        assert!(parse_version(Some("6.0")).is_err());
        assert!(parse_version(Some("2147483648")).is_err());
    }
}
