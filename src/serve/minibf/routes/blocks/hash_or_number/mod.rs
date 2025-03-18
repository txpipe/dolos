use std::sync::Arc;

use rocket::{get, http::Status, State};

use crate::{chain::ChainStore, ledger::pparams::Genesis, state::LedgerStore};

use super::Block;

pub mod addresses;
pub mod next;
pub mod previous;
pub mod txs;

#[get("/blocks/<hash_or_number>", rank = 2)]
pub fn route(
    hash_or_number: String,
    genesis: &State<Arc<Genesis>>,
    chain: &State<ChainStore>,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let block = Block::find_in_chain(chain, ledger, &hash_or_number, genesis)?;
    match block {
        Some(block) => Ok(rocket::serde::json::Json(block)),
        None => Err(Status::NotFound),
    }
}
