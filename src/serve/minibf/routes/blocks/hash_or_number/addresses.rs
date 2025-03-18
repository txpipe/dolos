use itertools::Itertools;
use pallas::ledger::traverse::MultiEraBlock;
use rocket::{get, http::Status, State};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{chain::ChainStore, state::LedgerStore};

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
    chain: &State<ChainStore>,
    ledger: &State<LedgerStore>,
) -> Result<rocket::serde::json::Json<Vec<BlockAddress>>, Status> {
    let body = match hex::decode(&hash_or_number) {
        Ok(hash) => match chain
            .get_block_by_hash(&hash)
            .map_err(|_| Status::InternalServerError)?
        {
            Some(body) => body,
            None => return Err(Status::NotFound),
        },
        Err(_) => match &hash_or_number.parse() {
            Ok(number) => match chain
                .get_block_by_number(number)
                .map_err(|_| Status::InternalServerError)?
            {
                Some(body) => body,
                None => return Err(Status::NotFound),
            },
            Err(_) => return Err(Status::BadRequest),
        },
    };

    let block = MultiEraBlock::decode(&body).map_err(|_| Status::ServiceUnavailable)?;
    let mut addresses = HashMap::new();

    for tx in block.txs() {
        // Handle inputs
        let utxos = ledger
            .get_utxos(tx.inputs().iter().map(Into::into).collect())
            .map_err(|_| Status::ServiceUnavailable)?;

        for (_, eracbor) in utxos {
            let parsed = pallas::ledger::traverse::MultiEraOutput::decode(eracbor.0, &eracbor.1)
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
