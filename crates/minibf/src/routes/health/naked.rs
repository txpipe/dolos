use axum::{http::StatusCode, Json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    pub is_healthy: bool,
}

pub async fn naked() -> Result<Json<RootResponse>, StatusCode> {
    // TODO: Relate this value to sync status. If not in tip, then unhealthy.
    Ok(Json(RootResponse { is_healthy: true }))
}
