use pallas::ledger::traverse::wellknown::GenesisValues;
use rocket::{get, http::Status, State};

use crate::{
    serve::minibf::routes::blocks::Block,
    wal::{redb::WalStore, ReadUtils, WalReader},
};

#[get("/blocks/slot/<slot_number>")]
pub fn route(
    slot_number: u64,
    genesis: &State<GenesisValues>,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let point = wal
        .crawl_from(None)
        .map_err(|_| Status::ServiceUnavailable)?
        .rev()
        .filter_forward()
        .into_blocks()
        .find(|maybe_block| match maybe_block {
            Some(block) => block.slot == slot_number,
            None => false,
        });

    match point {
        Some(Some(raw)) => match Block::find_in_wal(wal, &raw.hash.to_string(), genesis) {
            Ok(Some(block)) => Ok(rocket::serde::json::Json(block)),
            Ok(None) => Err(Status::NotFound),
            Err(_) => Err(Status::ServiceUnavailable),
        },
        _ => Err(Status::NotFound),
    }
}
