use axum::{extract::State, http::StatusCode, Json};
use dolos_core::Domain;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::Facade;

#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    pub is_healthy: bool,
}

pub async fn naked<D: Domain>(
    State(facade): State<Facade<D>>,
) -> Result<Json<RootResponse>, StatusCode> {
    dbg!(facade.health());
    Ok(Json(RootResponse {
        is_healthy: facade.health().synced(),
    }))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ClockResponse {
    server_time: u128,
}

impl Default for ClockResponse {
    fn default() -> Self {
        let now = SystemTime::now();
        let duration_since_epoch = now.duration_since(UNIX_EPOCH).expect("Time went backwards");

        let server_time = duration_since_epoch.as_millis();
        Self { server_time }
    }
}

pub async fn clock() -> Result<Json<ClockResponse>, StatusCode> {
    Ok(Json(ClockResponse::default()))
}
