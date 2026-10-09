pub mod accounts;
pub mod addresses;
pub mod assets;
pub mod blocks;
pub mod epochs;
pub mod genesis;
pub mod governance;
pub mod health;
pub mod metadata;
pub mod metrics;
pub mod network;
pub mod pools;
pub mod scripts;
pub mod tx;
pub mod txs;
pub mod utils;
pub mod utxos;

use std::env;

use axum::{extract::State, http::StatusCode, Json};
use dolos_core::Domain;
use serde::{Deserialize, Serialize};

use crate::{error::Error, Facade, MinibfConfig};

#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    url: String,
    version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    revision: Option<String>,
}

impl From<&MinibfConfig> for RootResponse {
    fn from(value: &MinibfConfig) -> Self {
        Self {
            url: value
                .url
                .clone()
                .unwrap_or(value.listen_address.to_string()),
            version: env!("CARGO_PKG_VERSION").to_string(),
            revision: option_env!("GIT_REVISION").map(|x| x.to_string()),
        }
    }
}

pub async fn root<D: Domain>(
    State(domain): State<Facade<D>>,
) -> Result<Json<RootResponse>, StatusCode> {
    Ok(Json(RootResponse::from(&domain.config)))
}

/// Answers every path that matches no route the way Blockfrost does: a
/// global `400` with an "Invalid path" body, rather than an empty `404`.
pub async fn invalid_path() -> Error {
    Error::InvalidPath
}

/// Parses a path number that Blockfrost types as `integer`: an optional sign
/// and ASCII digits, from 0 through `i32::MAX`.
pub fn parse_path_number(raw: &str, not_integer: Error, out_of_range: Error) -> Result<u64, Error> {
    let digits = raw.strip_prefix(['+', '-']).unwrap_or(raw);

    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(not_integer);
    }

    match raw.parse::<i32>() {
        Ok(value) if value >= 0 => Ok(value as u64),
        _ => Err(out_of_range),
    }
}
