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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::TestApp;

    #[tokio::test]
    async fn health_clock_happy_path() {
        let app = TestApp::new();
        let now = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        };

        let before = now();
        let (status, bytes) = app.get_bytes("/health/clock").await;
        let after = now();
        assert_eq!(status, StatusCode::OK);

        // Unix time in milliseconds, taken while the request ran
        let clock: ClockResponse = serde_json::from_slice(&bytes).expect("failed to parse clock");
        assert!((before..=after).contains(&clock.server_time));
    }
}
