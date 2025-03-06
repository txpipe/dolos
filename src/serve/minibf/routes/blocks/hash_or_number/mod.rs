use std::sync::Arc;

use rocket::{get, http::Status, State};

use crate::{ledger::pparams::Genesis, state::LedgerStore, wal::redb::WalStore};

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
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let block = Block::find_in_wal(wal, ledger, &hash_or_number, genesis)?;
    match block {
        Some(block) => Ok(rocket::serde::json::Json(block)),
        None => Err(Status::NotFound),
    }
}
