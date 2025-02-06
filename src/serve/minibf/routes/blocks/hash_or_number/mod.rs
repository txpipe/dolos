use pallas::ledger::traverse::wellknown::GenesisValues;
use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{ledger::pparams::Genesis, wal::redb::WalStore};

use super::Block;

pub mod addresses;
pub mod next;
pub mod previous;
pub mod txs;

#[get("/blocks/<hash_or_number>", rank = 2)]
pub fn route(
    hash_or_number: String,
    genesis: &State<Arc<Genesis>>,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let Some(magic) = genesis.shelley.network_magic else {
        return Err(Status::ServiceUnavailable);
    };

    let Some(values) = GenesisValues::from_magic(magic as u64) else {
        return Err(Status::ServiceUnavailable);
    };

    let block = Block::find_in_wal(wal, &hash_or_number, &values)?;
    match block {
        Some(block) => Ok(rocket::serde::json::Json(block)),
        None => Err(Status::NotFound),
    }
}
