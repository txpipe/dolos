use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraBlock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::serve::minibf::SharedState;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BlockAddress {
    address: String,
    transactions: Vec<BlockAddressTx>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq, Hash)]
pub struct BlockAddressTx {
    tx_hash: String,
}

pub async fn route(
    Path(hash_or_number): Path<String>,
    State(state): State<SharedState>,
) -> Result<Json<Vec<BlockAddress>>, StatusCode> {
    let body = match hex::decode(&hash_or_number) {
        Ok(hash) => match state
            .chain
            .get_block_by_hash(&hash)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            Some(body) => body,
            None => return Err(StatusCode::NOT_FOUND),
        },
        Err(_) => match &hash_or_number.parse() {
            Ok(number) => match state
                .chain
                .get_block_by_number(number)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            {
                Some(body) => body,
                None => return Err(StatusCode::NOT_FOUND),
            },
            Err(_) => return Err(StatusCode::BAD_REQUEST),
        },
    };

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
    let mut addresses = HashMap::new();

    for tx in block.txs() {
        // Handle inputs
        let utxos = state
            .ledger
            .get_utxos(tx.inputs().iter().map(Into::into).collect())
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

        for (_, eracbor) in utxos {
            let parsed = pallas::ledger::traverse::MultiEraOutput::decode(eracbor.0, &eracbor.1)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let address = parsed
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
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
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .to_bech32()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            let tx_hash = tx.hash().to_string();
            addresses
                .entry(address.to_string())
                .or_insert_with(Vec::new)
                .push(BlockAddressTx { tx_hash });
        }
    }

    Ok(Json(
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
