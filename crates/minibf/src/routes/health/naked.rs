use axum::{http::StatusCode, Json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    pub is_healthy: bool,
}

pub async fn naked() -> Result<Json<RootResponse>, StatusCode> {
    // TODO: tie this to the sync status; behind the tip means unhealthy.
    Ok(Json(RootResponse { is_healthy: true }))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::test_support::TestApp;

    #[tokio::test]
    async fn health_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/health").await;
        assert_eq!(status, StatusCode::OK);

        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("failed to parse health");
        assert_eq!(body, serde_json::json!({ "is_healthy": true }));
    }
}
