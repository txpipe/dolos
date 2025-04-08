use rocket::{get, http::Status, State};
use serde::{Deserialize, Serialize};

use crate::chain::ChainStore;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TxCbor {
    pub cbor: String,
}

#[get("/txs/<tx_hash>/cbor", rank = 2)]
pub fn route(
    tx_hash: String,
    chain: &State<ChainStore>,
) -> Result<rocket::serde::json::Json<TxCbor>, Status> {
    match chain
        .get_tx(&hex::decode(tx_hash).map_err(|_| Status::BadRequest)?)
        .map_err(|_| Status::InternalServerError)?
    {
        Some(tx) => Ok(rocket::serde::json::Json(TxCbor {
            cbor: hex::encode(tx),
        })),
        None => Err(Status::NotFound),
    }
}
