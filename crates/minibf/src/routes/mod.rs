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
pub mod utxos;

use std::env;

use axum::{extract::State, http::StatusCode, Json};
use dolos_core::Domain;
use serde::{Deserialize, Serialize};

use crate::{Facade, MinibfConfig};

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
