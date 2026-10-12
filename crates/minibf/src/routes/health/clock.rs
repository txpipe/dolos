use std::time::{SystemTime, UNIX_EPOCH};

use axum::{http::StatusCode, Json};
use serde::{Deserialize, Serialize};

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
