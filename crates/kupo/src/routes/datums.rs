use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::Domain;
use pallas::{codec::minicbor, crypto::hash::Hash};

use crate::{bad_request, types::Datum, Facade};

pub async fn by_hash<D: Domain>(
    State(facade): State<Facade<D>>,
    Path(datum_hash): Path<String>,
) -> Response {
    let bytes = match parse_datum_hash(&datum_hash) {
        Ok(bytes) => bytes,
        Err(_) => return bad_request(datum_hash_hint()).into_response(),
    };

    match facade.query().plutus_data(&bytes).await {
        Ok(Some(datum)) => match minicbor::to_vec(datum) {
            Ok(bytes) => (
                StatusCode::OK,
                Json(Datum {
                    datum: hex::encode(bytes.as_slice()),
                }),
            )
                .into_response(),
            Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
        },
        Ok(None) => (StatusCode::OK, Json(None::<Datum>)).into_response(),
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
