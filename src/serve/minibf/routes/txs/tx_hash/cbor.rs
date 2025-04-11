use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::serve::minibf::SharedState;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TxCbor {
    pub cbor: String,
}

pub async fn route(
    Path(tx_hash): Path<String>,
    State(state): State<SharedState>,
) -> Result<Json<TxCbor>, StatusCode> {
    match state
        .chain
        .get_tx(&hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(tx) => Ok(Json(TxCbor {
            cbor: hex::encode(tx),
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}
