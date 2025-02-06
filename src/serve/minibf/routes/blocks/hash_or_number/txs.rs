use pallas::ledger::traverse::MultiEraBlock;
use rocket::{get, http::Status, State};

use crate::wal::{redb::WalStore, ReadUtils, WalReader};

#[get("/blocks/<hash_or_number>/txs", rank = 2)]
pub fn route(
    hash_or_number: String,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Vec<String>>, Status> {
    let maybe_raw = wal
        .crawl_from(None)
        .map_err(|_| Status::ServiceUnavailable)?
        .into_blocks()
        .find(|maybe_raw| match maybe_raw {
            Some(raw) => match MultiEraBlock::decode(&raw.body) {
                Ok(block) => {
                    block.hash().to_string() == hash_or_number
                        || block.number().to_string() == hash_or_number
                }
                Err(_) => false,
            },
            None => false,
        });

    match maybe_raw {
        Some(Some(raw)) => {
            let block = MultiEraBlock::decode(&raw.body).map_err(|_| Status::ServiceUnavailable)?;
            Ok(rocket::serde::json::Json(
                block.txs().iter().map(|tx| tx.hash().to_string()).collect(),
            ))
        }
        _ => Err(Status::NotFound),
    }
}
