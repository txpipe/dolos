use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_cardano::{model::DatumState, FixedNamespace as _};
use dolos_core::{Domain, EntityKey, StateStore as _};

use crate::{bad_request, types::Datum, Facade};

pub async fn by_hash<D: Domain>(
    State(facade): State<Facade<D>>,
    Path(datum_hash): Path<String>,
) -> Response {
    let bytes = match parse_datum_hash(&datum_hash) {
        Ok(bytes) => bytes,
        Err(_) => return bad_request(datum_hash_hint()).into_response(),
    };

    let key = EntityKey::from(bytes.as_slice());

    let state = match facade
        .state()
        .read_entity_typed::<DatumState>(DatumState::NS, &key)
    {
        Ok(value) => value,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let body = state.map(|datum| Datum {
        datum: hex::encode(datum.bytes),
    });

    (StatusCode::OK, Json(body)).into_response()
}

fn parse_datum_hash(value: &str) -> Result<Vec<u8>, StatusCode> {
    if value.len() != 64 || !value.chars().all(|c| c.is_ascii_hexdigit()) {
        return Err(StatusCode::BAD_REQUEST);
    }

    let bytes = hex::decode(value).map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(bytes)
}

fn datum_hash_hint() -> String {
    "Invalid datum hash. Hash must be 64 lowercase hex characters.".to_string()
}
