pub mod txs;

use pallas::ledger::traverse::{wellknown::GenesisValues, MultiEraBlock};
use rocket::{get, http::Status, State};
use std::sync::Arc;

use crate::{
    ledger::pparams::Genesis,
    serve::minibf::routes::blocks::Block,
    wal::{redb::WalStore, WalReader},
};

#[get("/blocks/latest")]
pub fn route(
    genesis: &State<Arc<Genesis>>,
    wal: &State<WalStore>,
) -> Result<rocket::serde::json::Json<Block>, Status> {
    let tip = wal.find_tip().map_err(|_| Status::ServiceUnavailable)?;
    match tip {
        None => Err(Status::ServiceUnavailable),
        Some((_, point)) => {
            let raw_block = wal
                .read_block(&point)
                .map_err(|_| Status::ServiceUnavailable)?;
            let block =
                MultiEraBlock::decode(&raw_block.body).map_err(|_| Status::ServiceUnavailable)?;

            let Some(magic) = genesis.shelley.network_magic else {
                return Err(Status::ServiceUnavailable);
            };
            let Some(values) = GenesisValues::from_magic(magic as u64) else {
                return Err(Status::ServiceUnavailable);
            };

            let (epoch, epoch_slot) = block.epoch(&values);
            Ok(rocket::serde::json::Json(Block {
                slot: Some(block.slot()),
                hash: block.hash().to_string(),
                tx_count: block.tx_count() as u64,
                size: block.size() as u64,
                epoch: Some(epoch),
                epoch_slot: Some(epoch_slot),
                height: Some(block.number()),
                ..Default::default()
            }))
        }
    }
}
