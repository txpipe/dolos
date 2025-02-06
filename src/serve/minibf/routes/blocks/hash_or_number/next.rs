use pallas::ledger::traverse::{wellknown::GenesisValues, MultiEraBlock};
use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    ledger::pparams::Genesis,
    wal::{redb::WalStore, ReadUtils, WalReader},
};

use super::Block;

#[get("/blocks/<hash_or_number>/next", rank = 2)]
pub fn route(
    hash_or_number: String,
    genesis: &State<Arc<Genesis>>,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let Some(magic) = genesis.shelley.network_magic else {
        return Err(Status::ServiceUnavailable);
    };

    let Some(values) = GenesisValues::from_magic(magic as u64) else {
        return Err(Status::ServiceUnavailable);
    };

    let iterator = wal
        .crawl_from(None)
        .map_err(|_| Status::ServiceUnavailable)?
        .into_blocks();

    let mut prev = None;
    for raw in iterator.flatten() {
        let block = MultiEraBlock::decode(&raw.body).map_err(|_| Status::ServiceUnavailable)?;
        if block.hash().to_string() == hash_or_number
            || block.number().to_string() == hash_or_number
        {
            break;
        } else {
            prev = Some(raw.hash.to_string());
        }
    }
    match prev {
        Some(block) => {
            match Block::find_in_wal(wal, &block, &values)
                .map_err(|_| Status::ServiceUnavailable)?
            {
                Some(block) => Ok(rocket::serde::json::Json(block)),
                None => Err(Status::NotFound),
            }
        }
        None => Err(Status::NotFound),
    }
}
