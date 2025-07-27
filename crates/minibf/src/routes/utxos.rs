use axum::http::StatusCode;
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use itertools::Itertools;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};
use std::collections::{HashMap, HashSet};

use dolos_core::{Domain, StateStore, TxoRef};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

pub fn load_utxo_models<D: Domain>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
) -> Result<Vec<AddressUtxoContentInner>, StatusCode> {
    let utxos = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // decoded
    let utxos: HashMap<_, _> = utxos
        .iter()
        .map(|(k, v)| MultiEraOutput::try_from(v).map(|x| (k, x)))
        .try_collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let tx_deps: Vec<_> = utxos.keys().map(|txoref| txoref.0).unique().collect();

    let block_deps = domain.get_block_with_tx_batch(tx_deps)?;

    // decoded
    let blocks_deps: HashMap<_, _> = block_deps
        .iter()
        .map(|(k, (cbor, order))| MultiEraBlock::decode(cbor).map(|x| (k, (x, order))))
        .try_collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut models: Vec<_> = utxos
        .into_iter()
        .map(|(TxoRef(tx_hash, txo_idx), txo)| {
            let builder = UtxoOutputModelBuilder::from_output(*txo_idx, txo);
            let block_data = blocks_deps.get(&tx_hash).cloned();

            if let Some((block, tx_order)) = block_data {
                builder.with_block_data(block, *tx_order)
            } else {
                builder
            }
        })
        .map(|x| x.into_model_with_sort_key())
        .try_collect()?;

    match pagination.order {
        Order::Asc => {
            models.sort_by_key(|(sort_key, _)| *sort_key);
        }
        Order::Desc => {
            models.sort_by_key(|(sort_key, _)| *sort_key);
            models.reverse();
        }
    }

    let models = models
        .into_iter()
        .map(|(_, utxo)| utxo)
        .enumerate()
        .filter_map(|(i, utxo)| pagination.as_included_item(i, utxo))
        .collect();

    Ok(models)
}
