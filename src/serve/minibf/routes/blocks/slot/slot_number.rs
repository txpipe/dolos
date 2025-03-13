use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    chain::ChainStore, ledger::pparams::Genesis, serve::minibf::routes::blocks::Block,
    state::LedgerStore,
};

#[get("/blocks/slot/<slot_number>")]
pub fn route(
    slot_number: u64,
    genesis: &State<Arc<Genesis>>,
    chain: &State<ChainStore>,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let body = chain
        .get_block_by_slot(&slot_number)
        .map_err(|_| Status::InternalServerError)?;

    match body {
        Some(body) => match Block::from_body(&body, chain, ledger, genesis) {
            Ok(Some(block)) => Ok(rocket::serde::json::Json(block)),
            Ok(None) => Err(Status::NotFound),
            Err(_) => Err(Status::ServiceUnavailable),
        },
        _ => Err(Status::NotFound),
    }
}
