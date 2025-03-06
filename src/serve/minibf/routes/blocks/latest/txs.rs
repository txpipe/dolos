use pallas::ledger::traverse::MultiEraBlock;
use rocket::{get, http::Status, State};

use crate::wal::{redb::WalStore, WalReader};

#[get("/blocks/latest/txs")]
pub fn route(wal: &State<WalStore>) -> Result<rocket::serde::json::Json<Vec<String>>, Status> {
    let tip = wal.find_tip().map_err(|_| Status::ServiceUnavailable)?;
    match tip {
        None => Err(Status::ServiceUnavailable),
        Some((_, point)) => {
            let raw_block = wal
                .read_block(&point)
                .map_err(|_| Status::ServiceUnavailable)?;
            let block =
                MultiEraBlock::decode(&raw_block.body).map_err(|_| Status::ServiceUnavailable)?;
            let txs = block.txs().iter().map(|tx| tx.hash().to_string()).collect();
            Ok(rocket::serde::json::Json(txs))
        }
    }
}
