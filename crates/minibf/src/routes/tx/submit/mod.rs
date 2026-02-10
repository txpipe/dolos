use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
};
use dolos_core::{ChainError, Domain, DomainError, MempoolError, SubmitExt};

use crate::Facade;

fn is_valid_cbor_content_type(headers: &HeaderMap) -> bool {
    let Some(content_type) = headers.get(header::CONTENT_TYPE) else {
        return false;
    };

    let Ok(content_type) = content_type.to_str() else {
        return false;
    };

    content_type == "application/cbor"
}

pub async fn route<D: Domain>(
    State(domain): State<Facade<D>>,
    headers: HeaderMap,
    cbor: Bytes,
) -> Result<String, StatusCode> {
    if !is_valid_cbor_content_type(&headers) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let chain = domain.read_chain();
    let result = domain.inner.receive_tx(&chain, &cbor);

    let hash = result.map_err(|e| match e {
        DomainError::ChainError(x) => match x {
            ChainError::BrokenInvariant(_) => StatusCode::BAD_REQUEST,
            ChainError::DecodingError(_) => StatusCode::BAD_REQUEST,
            ChainError::CborDecodingError(_) => StatusCode::BAD_REQUEST,
            ChainError::AddressDecoding(_) => StatusCode::BAD_REQUEST,
            ChainError::Phase1ValidationRejected(_) => StatusCode::BAD_REQUEST,
            ChainError::Phase2ValidationRejected(_) => StatusCode::BAD_REQUEST,
            _ => StatusCode::INTERNAL_SERVER_ERROR,
        },
        DomainError::MempoolError(x) => match x {
            MempoolError::TraverseError(_) => StatusCode::BAD_REQUEST,
            MempoolError::InvalidTx(_) => StatusCode::BAD_REQUEST,
            MempoolError::DecodeError(_) => StatusCode::BAD_REQUEST,
            MempoolError::PlutusNotSupported => StatusCode::BAD_REQUEST,
            MempoolError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
            MempoolError::StateError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            MempoolError::IndexError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            MempoolError::PParamsNotAvailable => StatusCode::INTERNAL_SERVER_ERROR,
        },
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    Ok(hex::encode(hash))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};

    async fn assert_status(app: &TestApp, content_type: &str, body: Vec<u8>, expected: StatusCode) {
        let (status, _body) = app.post_bytes("/tx/submit", content_type, body).await;
        assert_eq!(status, expected);
    }

    #[tokio::test]
    async fn tx_submit_happy_path() {
        let app = TestApp::new();
        let (status, body) = app
            .post_bytes(
                "/tx/submit",
                "application/cbor",
                app.vectors().tx_cbor.clone(),
            )
            .await;
        assert_eq!(status, StatusCode::OK);
        let hash = String::from_utf8(body).expect("hash must be utf-8");
        assert_eq!(hash.len(), 64);
        assert!(hex::decode(hash).is_ok());
    }

    #[tokio::test]
    async fn tx_submit_bad_request_content_type() {
        let app = TestApp::new();
        assert_status(
            &app,
            "application/json",
            app.vectors().tx_cbor.clone(),
            StatusCode::BAD_REQUEST,
        )
        .await;
    }

    #[tokio::test]
    async fn tx_submit_bad_request_invalid_cbor() {
        let app = TestApp::new();
        assert_status(
            &app,
            "application/cbor",
            vec![0xde, 0xad, 0xbe, 0xef],
            StatusCode::BAD_REQUEST,
        )
        .await;
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
