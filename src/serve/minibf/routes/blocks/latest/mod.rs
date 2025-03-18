pub mod txs;

use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    chain::ChainStore, ledger::pparams::Genesis, serve::minibf::routes::blocks::Block,
    state::LedgerStore,
};

#[get("/blocks/latest", rank = 1)]
pub fn route(
    genesis: &State<Arc<Genesis>>,
    chain: &State<ChainStore>,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let tip = chain.get_tip().map_err(|_| Status::ServiceUnavailable)?;
    match tip {
        None => Err(Status::ServiceUnavailable),
        Some((_, body)) => match Block::from_body(&body, chain, ledger, genesis) {
            Ok(Some(block)) => Ok(rocket::serde::json::Json(block)),
            Ok(None) => Err(Status::NotFound),
            Err(_) => Err(Status::ServiceUnavailable),
        },
    }
}
