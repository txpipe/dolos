pub mod accounts;
pub mod addresses;
pub mod assets;
pub mod blocks;
pub mod epochs;
pub mod genesis;
pub mod governance;
pub mod health;
pub mod metadata;
pub mod network;
pub mod pools;
pub mod scripts;
pub mod tx;
pub mod txs;
pub mod utils;

use std::env;

use axum::{extract::State, Json};
use dolos_core::Domain;
use serde::{Deserialize, Serialize};

use crate::{error::Error, Facade, MinibfConfig};

/// The `/` body: the operator-configured `public_url`, when set, and the
/// package version. The bind address is never reported in its place.
#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    #[serde(skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    version: String,
}

impl From<&MinibfConfig> for RootResponse {
    fn from(value: &MinibfConfig) -> Self {
        Self {
            url: value.public_url.clone(),
            version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

pub async fn root<D: Domain>(State(domain): State<Facade<D>>) -> Result<Json<RootResponse>, Error> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn root_body(public_url: Option<&str>) -> serde_json::Value {
        let mut config = MinibfConfig::new("[::]:3000".parse().unwrap());
        config.public_url = public_url.map(str::to_string);

        serde_json::to_value(RootResponse::from(&config)).unwrap()
    }

    #[test]
    fn root_reports_the_configured_public_url() {
        let body = root_body(Some("https://api.example.com/"));

        assert_eq!(
            body,
            serde_json::json!({
                "url": "https://api.example.com/",
                "version": env!("CARGO_PKG_VERSION"),
            })
        );
    }

    #[test]
    fn root_omits_url_without_a_public_url() {
        let body = root_body(None);

        assert_eq!(
            body,
            serde_json::json!({ "version": env!("CARGO_PKG_VERSION") })
        );
    }
}
