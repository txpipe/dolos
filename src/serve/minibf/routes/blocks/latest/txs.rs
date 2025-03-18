use pallas::ledger::traverse::MultiEraBlock;
use rocket::{get, http::Status, State};

use crate::chain::ChainStore;

#[get("/blocks/latest/txs")]
pub fn route(chain: &State<ChainStore>) -> Result<rocket::serde::json::Json<Vec<String>>, Status> {
    let tip = chain.get_tip().map_err(|_| Status::ServiceUnavailable)?;
    match tip {
        None => Err(Status::ServiceUnavailable),
        Some((_, body)) => {
            let block = MultiEraBlock::decode(&body).map_err(|_| Status::ServiceUnavailable)?;
            let txs = block.txs().iter().map(|tx| tx.hash().to_string()).collect();
            Ok(rocket::serde::json::Json(txs))
        }
    }
}
