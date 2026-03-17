use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_cardano::CardanoError;
use dolos_core::Domain;
use pallas::crypto::hash::Hash;

use crate::{bad_request, Facade};

pub async fn by_hash<D: Domain<ChainSpecificError = CardanoError> + Clone + Send + Sync + 'static>(
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
