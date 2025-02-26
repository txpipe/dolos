pub mod txs;

use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    ledger::pparams::Genesis,
    serve::minibf::routes::blocks::Block,
    state::LedgerStore,
    wal::{redb::WalStore, WalReader},
};

#[get("/blocks/latest", rank = 1)]
pub fn route(
    genesis: &State<Arc<Genesis>>,
    wal: &State<WalStore>,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let tip = wal.find_tip().map_err(|_| Status::ServiceUnavailable)?;
    match tip {
        None => Err(Status::ServiceUnavailable),
        Some((_, point)) => {
            let raw = wal
                .read_block(&point)
                .map_err(|_| Status::ServiceUnavailable)?;

            match Block::find_in_wal(wal, ledger, &raw.hash.to_string(), genesis) {
                Ok(Some(block)) => Ok(rocket::serde::json::Json(block)),
                Ok(None) => Err(Status::NotFound),
                Err(_) => Err(Status::ServiceUnavailable),
            }
        }
    }
}
