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

/// A path segment that Blockfrost types as `integer`.
///
/// Blockfrost checks the type first. It checks the range `0..=i32::MAX` later.
#[derive(Debug, PartialEq, Eq)]
pub enum PathInteger {
    InRange(u64),
    OutOfRange,
}

impl PathInteger {
    /// Parses an optional sign and ASCII digits. Returns `None` for other text.
    pub fn parse(raw: &str) -> Option<Self> {
        let digits = raw.strip_prefix(['+', '-']).unwrap_or(raw);

        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }

        match raw.parse::<i32>() {
            Ok(value) if value >= 0 => Some(Self::InRange(value as u64)),
            _ => Some(Self::OutOfRange),
        }
    }

    /// Gives the value if it is from 0 through `i32::MAX`.
    pub fn in_range(self) -> Option<u64> {
        match self {
            Self::InRange(value) => Some(value),
            Self::OutOfRange => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PathInteger;

    #[test]
    fn path_integer_parse() {
        for (raw, expected) in [
            ("0", Some(PathInteger::InRange(0))),
            ("00001", Some(PathInteger::InRange(1))),
            ("+1", Some(PathInteger::InRange(1))),
            ("-0", Some(PathInteger::InRange(0))),
            ("2147483647", Some(PathInteger::InRange(2147483647))),
            ("2147483648", Some(PathInteger::OutOfRange)),
            ("99999999999999999999", Some(PathInteger::OutOfRange)),
            ("-1", Some(PathInteger::OutOfRange)),
            ("", None),
            ("+", None),
            ("-", None),
            ("--1", None),
            ("abc", None),
            ("1.5", None),
            ("1e3", None),
            (" 1", None),
            ("0x10", None),
        ] {
            assert_eq!(PathInteger::parse(raw), expected, "{raw:?}");
        }
    }
}
