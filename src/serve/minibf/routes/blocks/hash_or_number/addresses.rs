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
use dolos_core::EraCbor;

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
        Ok(hash) => match state.chain.get_block_by_hash(&hash).map_err(|err| {
            tracing::error!(err =? err, "Failed to get block by hash from chain.");
            StatusCode::INTERNAL_SERVER_ERROR
        })? {
            Some(body) => body,
            None => return Err(StatusCode::NOT_FOUND),
        },
        Err(_) => match &hash_or_number.parse() {
            Ok(number) => match state.chain.get_block_by_number(number).map_err(|err| {
                tracing::error!(err =? err, "Failed to get block by number from chain.");
                StatusCode::INTERNAL_SERVER_ERROR
            })? {
                Some(body) => body,
                None => return Err(StatusCode::NOT_FOUND),
            },
            Err(_) => return Err(StatusCode::BAD_REQUEST),
        },
    };

    let block = MultiEraBlock::decode(&body).map_err(|err| {
        tracing::error!(err =? err, "Failed to decode block form chain.");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    let mut addresses = HashMap::new();

    for tx in block.txs() {
        // Handle inputs
        let utxos = state
            .ledger
            .get_utxos(tx.inputs().iter().map(Into::into).collect())
            .map_err(|err| {
                tracing::error!(err =? err, "Failed to get utxos from ledger.");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;

        for (_, EraCbor(era, cbor)) in utxos {
            let era = era.try_into().expect("era out of range");

            let parsed =
                pallas::ledger::traverse::MultiEraOutput::decode(era, &cbor).map_err(|err| {
                    tracing::error!(err =? err, "Failed to get decode utxos from ledger.");
                    StatusCode::INTERNAL_SERVER_ERROR
                })?;
            let address = parsed.address().map_err(|err| {
                tracing::error!(err =? err, output =? parsed, "Invalid address on utxo in ledger.");
                StatusCode::INTERNAL_SERVER_ERROR
            })?
                .to_string();
            let tx_hash = tx.hash().to_string();
            addresses
                .entry(address.to_string())
                .or_insert_with(Vec::new)
                .push(BlockAddressTx { tx_hash });
        }

        // Handle outputs
        for output in tx.outputs() {
            let address = output.address().map_err(|err| {
                tracing::error!(err =? err, output =? output, "Invalid address on utxo in ledger.");
                StatusCode::INTERNAL_SERVER_ERROR
            })?.to_string();
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
