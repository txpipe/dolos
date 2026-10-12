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
