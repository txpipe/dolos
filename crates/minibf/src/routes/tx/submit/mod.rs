use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    Json,
};
use dolos_core::{ChainError, Domain, DomainError, MempoolError, SubmitExt};

use crate::{error::Error, Facade};

/// Whether `content_type` declares CBOR. Blockfrost compares the media type
/// alone, in any case, so `application/cbor; charset=utf-8` is CBOR too.
fn is_cbor(content_type: &str) -> bool {
    let media_type = content_type.split(';').next().unwrap_or_default();

    media_type.trim().eq_ignore_ascii_case("application/cbor")
}

/// Refuses a body not declared as CBOR, as Blockfrost does: `415`, naming the
/// content type the request gave, if any.
fn check_content_type(headers: &HeaderMap) -> Result<(), Error> {
    let named = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty());

    match named {
        Some(content_type) if is_cbor(content_type) => Ok(()),
        named => Err(Error::UnsupportedMediaType(named.map(str::to_string))),
    }
}

/// The reason the node gives for `error`: its message, then each cause's that
/// the message does not already hold.
fn reason(error: &DomainError) -> String {
    let mut reason = error.to_string();
    let mut cause = std::error::Error::source(error);

    while let Some(source) = cause {
        let text = source.to_string();

        if !reason.contains(&text) {
            reason = format!("{reason}: {text}");
        }

        cause = source.source();
    }

    reason
}

/// The answer to a transaction the node did not take: `400` with the reason
/// when the transaction is at fault, `409` when the mempool holds it already,
/// and `500` when the node is.
fn not_taken(error: DomainError) -> Error {
    let rejected = match &error {
        DomainError::ChainError(x) => matches!(
            x,
            ChainError::BrokenInvariant(_)
                | ChainError::DecodingError(_)
                | ChainError::CborDecodingError(_)
                | ChainError::AddressDecoding(_)
                | ChainError::Phase1ValidationRejected(_)
                | ChainError::Phase2ValidationRejected(_)
        ),
        DomainError::MempoolError(MempoolError::DuplicateTx) => return Error::TxInMempool,
        DomainError::MempoolError(x) => matches!(
            x,
            MempoolError::TraverseError(_)
                | MempoolError::InvalidTx(_)
                | MempoolError::DecodeError(_)
                | MempoolError::PlutusNotSupported
        ),
        _ => false,
    };

    if rejected {
        Error::TxRejected(reason(&error))
    } else {
        StatusCode::INTERNAL_SERVER_ERROR.into()
    }
}

/// Submits a transaction and answers its id as a JSON string, as Blockfrost
/// does: its clients parse the body as JSON.
pub async fn route<D>(
    State(domain): State<Facade<D>>,
    headers: HeaderMap,
    cbor: Bytes,
) -> Result<Json<String>, Error>
where
    D: Domain + SubmitExt + Clone + Send + Sync + 'static,
{
    check_content_type(&headers)?;

    // Validation reads the ledger and waits for the submit lock, so it runs
    // as blocking query work, where the request deadline can still answer.
    let result = domain
        .query()
        .run_blocking(move |domain| {
            let chain = domain.read_chain();
            domain.receive_tx("minibf", &chain, &cbor)
        })
        .await;

    let hash = result.map_err(not_taken)?;

    Ok(Json(hex::encode(hash)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault, SUBMIT_TESTS};
    use axum::{body::Body, http::Request};
    use dolos_core::QueryLimiter;
    use std::time::{Duration, Instant};

    async fn assert_status(app: &TestApp, content_type: &str, body: Vec<u8>, expected: StatusCode) {
        let (status, _body) = app.post_bytes("/tx/submit", content_type, body).await;
        assert_eq!(status, expected);
    }

    /// The answer to a submit of `body` under `content_type`, if any.
    async fn submit(
        app: &TestApp,
        content_type: Option<&str>,
        body: Vec<u8>,
    ) -> (StatusCode, HeaderMap, serde_json::Value) {
        let mut request = Request::post("/tx/submit");

        if let Some(content_type) = content_type {
            request = request.header(header::CONTENT_TYPE, content_type);
        }

        let request = request.body(Body::from(body)).unwrap();
        let (status, headers, body) = app.respond(request).await;
        let body = serde_json::from_slice(&body).expect("a JSON body");

        (status, headers, body)
    }

    /// The id of a submitted transaction is a JSON string, as Blockfrost
    /// answers it: its clients parse the body as JSON.
    #[tokio::test]
    async fn tx_submit_happy_path() {
        let app = TestApp::new();
        let tx = app.vectors().tx_cbor.clone();
        let (status, headers, body) = submit(&app, Some("application/cbor"), tx).await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CONTENT_TYPE], "application/json");

        let hash = body.as_str().expect("a JSON string");
        assert_eq!(hash.len(), 64);
        assert!(hex::decode(hash).is_ok());
    }

    /// A body not declared as CBOR is refused as Blockfrost refuses it: 415,
    /// naming the content type the request gave.
    #[tokio::test]
    async fn a_body_not_declared_as_cbor_gets_415() {
        let app = TestApp::new();

        for (content_type, message) in [
            (
                Some("application/json"),
                "Unsupported Media Type: application/json",
            ),
            (Some("text/plain"), "Unsupported Media Type: text/plain"),
            (Some(" "), "Unsupported Media Type"),
            (None, "Unsupported Media Type"),
        ] {
            let tx = app.vectors().tx_cbor.clone();
            let (status, _, body) = submit(&app, content_type, tx).await;

            assert_eq!(
                status,
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "{content_type:?}"
            );
            assert_eq!(
                body,
                serde_json::json!({
                    "status_code": 415,
                    "error": "Unsupported Media Type",
                    "message": message,
                }),
                "{content_type:?}",
            );
        }
    }

    /// Blockfrost reads the media type alone, in any case: the body under
    /// these content types is read as CBOR, and refused as a transaction.
    #[tokio::test]
    async fn the_media_type_is_read_alone_in_any_case() {
        let app = TestApp::new();

        for content_type in [
            "application/cbor; charset=utf-8",
            "Application/CBOR",
            " application/cbor ",
        ] {
            let (status, _, body) = submit(&app, Some(content_type), b"x".to_vec()).await;

            assert_eq!(status, StatusCode::BAD_REQUEST, "{content_type}");
            assert_eq!(body["error"], "Bad Request", "{content_type}");
        }
    }

    /// A transaction the node refuses gets Blockfrost's error body, with the
    /// node's reason as its message.
    #[tokio::test]
    async fn tx_submit_bad_request_invalid_cbor() {
        let app = TestApp::new();
        let body = vec![0xde, 0xad, 0xbe, 0xef];
        let (status, _, body) = submit(&app, Some("application/cbor"), body).await;

        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body["status_code"], 400);
        assert_eq!(body["error"], "Bad Request");

        let message = "chain error: decoding error: Unknown CBOR structure: deadbeef";
        assert_eq!(body["message"], message);
    }

    /// A transaction submitted again is refused with the node's reason: the
    /// copy it submitted first spends its inputs.
    #[tokio::test]
    async fn a_transaction_submitted_twice_is_refused_with_a_reason() {
        let app = TestApp::new();
        let tx = app.vectors().tx_cbor.clone();

        let (status, _, _) = submit(&app, Some("application/cbor"), tx.clone()).await;
        assert_eq!(status, StatusCode::OK);

        let (status, _, body) = submit(&app, Some("application/cbor"), tx).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(
            body["message"],
            "chain error: phase-1 script rejected the transaction: some transaction input is not \
             present in the UTxO set"
        );
    }

    #[tokio::test]
    #[ignore]
    async fn tx_submit_not_found() {
        let app = TestApp::new();
        assert_status(
            &app,
            "application/cbor",
            app.vectors().tx_cbor.clone(),
            StatusCode::NOT_FOUND,
        )
        .await;
    }

    #[tokio::test]
    async fn a_submit_held_past_the_deadline_gets_504() {
        let _serial = SUBMIT_TESTS.lock().await;
        let slow = Duration::from_secs(3);
        let app = TestApp::new_admitting(
            Some(TestFault::SlowUtxoReads(slow)),
            QueryLimiter::default(),
            1,
        );

        let started = Instant::now();
        let (status, _body) = app
            .post_bytes(
                "/tx/submit",
                "application/cbor",
                app.vectors().tx_cbor.clone(),
            )
            .await;

        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
        assert!(started.elapsed() < slow, "{:?}", started.elapsed());

        // The cut submit still holds the process-wide submit lock. Keep other
        // submit tests out until it ends: a probe submit queues behind it.
        let probe = TestApp::new();
        let _ = probe
            .post_bytes(
                "/tx/submit",
                "application/cbor",
                probe.vectors().tx_cbor.clone(),
            )
            .await;
    }

    /// Submission waits for a query permit like any other store read, so the
    /// deadline answers a submit queued behind a busy limiter.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_submit_waiting_for_a_query_permit_gets_504() {
        let _serial = SUBMIT_TESTS.lock().await;
        let app = TestApp::new_admitting(None, QueryLimiter::new(1), 1);
        let limiter = dolos_core::Domain::query_limiter(&app.facade().inner).clone();

        let (open, gate) = std::sync::mpsc::channel::<()>();
        let (started, running) = tokio::sync::oneshot::channel();
        let holder = tokio::spawn({
            let limiter = limiter.clone();
            async move {
                limiter
                    .run_blocking(move || {
                        let _ = started.send(());
                        let _ = gate.recv();
                    })
                    .await
            }
        });
        running.await.unwrap();
        assert_eq!(limiter.in_use(), 1);

        let (status, _body) = app
            .post_bytes(
                "/tx/submit",
                "application/cbor",
                app.vectors().tx_cbor.clone(),
            )
            .await;

        open.send(()).unwrap();
        holder.await.unwrap().unwrap();
        assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    }

    #[tokio::test]
    async fn tx_submit_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(
            &app,
            "application/cbor",
            app.vectors().tx_cbor.clone(),
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
