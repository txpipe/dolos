use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
};
use dolos_core::{facade::receive_tx, ChainError, Domain, DomainError, MempoolError};

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

    let chain = domain.read_chain().await;
    let result = receive_tx(&domain.inner, &chain, &cbor);

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
            MempoolError::PParamsNotAvailable => StatusCode::INTERNAL_SERVER_ERROR,
        },
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    })?;

    Ok(hex::encode(hash))
}
