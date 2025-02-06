pub mod txs;

use pallas::ledger::traverse::wellknown::GenesisValues;
use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    ledger::pparams::Genesis,
    serve::minibf::routes::blocks::Block,
    wal::{redb::WalStore, WalReader},
};

#[get("/blocks/latest", rank = 1)]
pub fn route(
    genesis: &State<Arc<Genesis>>,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let Some(magic) = genesis.shelley.network_magic else {
        return Err(Status::ServiceUnavailable);
    };

    let Some(values) = GenesisValues::from_magic(magic as u64) else {
        return Err(Status::ServiceUnavailable);
    };

    let tip = wal.find_tip().map_err(|_| Status::ServiceUnavailable)?;
    match tip {
        None => Err(Status::ServiceUnavailable),
        Some((_, point)) => {
            let raw = wal
                .read_block(&point)
                .map_err(|_| Status::ServiceUnavailable)?;

            match Block::find_in_wal(wal, &raw.hash.to_string(), &values) {
                Ok(Some(block)) => Ok(rocket::serde::json::Json(block)),
                Ok(None) => Err(Status::NotFound),
                Err(_) => Err(Status::ServiceUnavailable),
            }
        }
    }
}
