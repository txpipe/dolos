use pallas::ledger::traverse::MultiEraBlock;
use rocket::{get, http::Status, State};

use crate::{chain::ChainStore, serve::minibf::routes::blocks::hash_or_number_to_body};

#[get("/blocks/<hash_or_number>/txs", rank = 2)]
pub fn route(
    hash_or_number: String,
    chain: &State<ChainStore>,
) -> Result<rocket::serde::json::Json<Vec<String>>, Status> {
    let body =
        hash_or_number_to_body(&hash_or_number, chain).map_err(|_| Status::ServiceUnavailable)?;

    let block = MultiEraBlock::decode(&body).map_err(|_| Status::ServiceUnavailable)?;
    Ok(rocket::serde::json::Json(
        block.txs().iter().map(|tx| tx.hash().to_string()).collect(),
    ))
}
