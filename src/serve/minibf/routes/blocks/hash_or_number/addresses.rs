use itertools::Itertools;
use pallas::ledger::traverse::MultiEraBlock;
use rocket::{get, http::Status, State};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{
    state::LedgerStore,
    wal::{redb::WalStore, ReadUtils, WalReader},
};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlockAddress {
    address: String,
    transactions: Vec<BlockAddressTx>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
pub struct BlockAddressTx {
    tx_hash: String,
}

#[get("/blocks/<hash_or_number>/addresses", rank = 2)]
pub fn route(
    hash_or_number: String,
    wal: &State<WalStore>,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Vec<BlockAddress>>, Status> {
    let maybe_raw = wal
        .crawl_from(None)
        .map_err(|_| Status::ServiceUnavailable)?
        .rev() // Start from latest and work yourself backwards.
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
            let mut addresses = HashMap::new();

            for tx in block.txs() {
                // Handle inputs
                let utxos = ledger
                    .get_utxos(tx.inputs().iter().map(Into::into).collect())
                    .map_err(|_| Status::ServiceUnavailable)?;

                for (_, eracbor) in utxos {
                    let parsed =
                        pallas::ledger::traverse::MultiEraOutput::decode(eracbor.0, &eracbor.1)
                            .map_err(|_| Status::InternalServerError)?;
                    let address = parsed
                        .address()
                        .map_err(|_| Status::ServiceUnavailable)?
                        .to_bech32()
                        .map_err(|_| Status::ServiceUnavailable)?;
                    let tx_hash = tx.hash().to_string();
                    addresses
                        .entry(address.to_string())
                        .or_insert_with(Vec::new)
                        .push(BlockAddressTx { tx_hash });
                }

                // Handle outputs
                for output in tx.outputs() {
                    let address = output
                        .address()
                        .map_err(|_| Status::ServiceUnavailable)?
                        .to_bech32()
                        .map_err(|_| Status::ServiceUnavailable)?;
                    let tx_hash = tx.hash().to_string();
                    addresses
                        .entry(address.to_string())
                        .or_insert_with(Vec::new)
                        .push(BlockAddressTx { tx_hash });
                }
            }

            Ok(rocket::serde::json::Json(
                addresses
                    .into_iter()
                    .sorted_by_key(|(address, _)| address.clone())
                    .map(|(address, transactions)| BlockAddress {
                        address,
                        transactions: transactions.into_iter().unique().collect(),
                    })
                    .collect(),
            ))
        }
        _ => Err(Status::NotFound),
    }
}
