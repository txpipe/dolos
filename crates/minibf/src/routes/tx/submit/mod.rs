use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
};
use dolos_core::{Domain, MempoolError, MempoolStore as _};

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

    let hash = domain
        .mempool()
        .receive_raw(&domain.inner, &cbor)
        .map_err(|e| match e {
            MempoolError::Phase1Error(_) => StatusCode::BAD_REQUEST,
            MempoolError::Phase2Error(_) => StatusCode::BAD_REQUEST,
            MempoolError::Phase2ExplicitError(_) => StatusCode::BAD_REQUEST,
            MempoolError::InvalidTx(_) => StatusCode::BAD_REQUEST,
            MempoolError::TraverseError(_) => StatusCode::BAD_REQUEST,
            MempoolError::StateError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            MempoolError::ChainError(_) => StatusCode::INTERNAL_SERVER_ERROR,
            MempoolError::DecodeError(_) => StatusCode::BAD_REQUEST,
            MempoolError::PlutusNotSupported => StatusCode::BAD_REQUEST,
            MempoolError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        })?;

    Ok(hex::encode(hash))
}
