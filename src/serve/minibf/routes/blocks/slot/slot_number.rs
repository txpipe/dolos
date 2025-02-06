use pallas::ledger::traverse::wellknown::GenesisValues;
use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    ledger::pparams::Genesis,
    serve::minibf::routes::blocks::Block,
    wal::{redb::WalStore, ReadUtils, WalReader},
};

#[get("/blocks/slot/<slot_number>")]
pub fn route(
    slot_number: u64,
    genesis: &State<Arc<Genesis>>,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let Some(magic) = genesis.shelley.network_magic else {
        return Err(Status::ServiceUnavailable);
    };

    let Some(values) = GenesisValues::from_magic(magic as u64) else {
        return Err(Status::ServiceUnavailable);
    };

    let point = wal
        .crawl_from(None)
        .map_err(|_| Status::ServiceUnavailable)?
        .filter_forward()
        .into_blocks()
        .find(|maybe_block| match maybe_block {
            Some(block) => block.slot == slot_number,
            None => false,
        });

    match point {
        Some(Some(raw)) => match Block::find_in_wal(wal, &raw.hash.to_string(), &values) {
            Ok(Some(block)) => Ok(rocket::serde::json::Json(block)),
            Ok(None) => Err(Status::NotFound),
            Err(_) => Err(Status::ServiceUnavailable),
        },
        _ => Err(Status::NotFound),
    }
}
