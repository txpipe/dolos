use axum::{
    extract::{Path, State},
    Json,
};
use blockfrost_openapi::models::script_datum::ScriptDatum;
use dolos_core::{ArchiveStore, Domain};
use pallas::crypto::hash::Hash;
use reqwest::StatusCode;
use serde::{Deserialize, Serialize};

use crate::{
    error::Error,
    mapping::{IntoModel, PlutusDataWrapper},
    Facade,
};

#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    pub is_healthy: bool,
}

pub async fn by_datum_hash<D: Domain>(
    Path(datum_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<ScriptDatum>, Error> {
    if datum_hash.len() != 64 {
        // Oficial blockfrost returns this instead of bad request.
        return Err(StatusCode::NOT_FOUND.into());
    }
    let datum_hash = Hash::<32>::from(
        hex::decode(&datum_hash)
            .map_err(|_| StatusCode::NOT_FOUND)?
            .as_slice(),
    );

    let datum = domain
        .archive()
        .get_plutus_data(&datum_hash)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    Ok(Json(ScriptDatum {
        json_value: PlutusDataWrapper(datum).into_model()?,
    }))
}
