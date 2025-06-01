use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
};
use dolos_core::{ArchiveStore as _, Domain};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TxCbor {
    pub cbor: String,
}

pub async fn route<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<D>,
) -> Result<Json<TxCbor>, StatusCode> {
    match domain
        .archive()
        .get_tx(&hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(tx) => Ok(Json(TxCbor {
            cbor: hex::encode(tx),
        })),
        None => Err(StatusCode::NOT_FOUND),
    }
}
