use std::collections::{HashMap, HashSet};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
use itertools::Itertools;

use dolos_core::{ArchiveStore as _, Domain, StateStore as _, TxoRef};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput};

use crate::{
    mapping::{IntoModel, UtxoBlockData, UtxoOutputModelBuilder},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

fn load_utxo_models<D: Domain>(
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
    let block_deps: HashMap<_, _> = tx_deps
        .iter()
        .flat_map(
            |tx| match domain.archive().get_block_with_tx(tx.as_slice()) {
                Ok(Some((cbor, txorder))) => {
                    let Ok(block) = MultiEraBlock::decode(&cbor) else {
                        return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
                    };
                    let block_data = match UtxoBlockData::try_from((block, txorder)) {
                        Ok(data) => data,
                        Err(err) => return Some(Err(err)),
                    };
                    Some(Ok((tx, block_data)))
                }
                Ok(None) => None,
                Err(_) => Some(Err(StatusCode::INTERNAL_SERVER_ERROR)),
            },
        )
        .try_collect()?;

    let mut models: Vec<_> = utxos
        .into_iter()
        .map(|(TxoRef(tx_hash, txo_idx), txo)| {
            let builder = UtxoOutputModelBuilder::from_output(*txo_idx, txo);
            let block_data = block_deps.get(&tx_hash).cloned();

            if let Some(data) = block_data {
                builder.with_block_data(data)
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

pub async fn utxos<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, StatusCode> {
    let pagination = Pagination::try_from(params)?;

    let address = pallas::ledger::addresses::Address::from_bech32(&address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let refs = domain
        .state()
        .get_utxo_by_address(&address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    dbg!(&refs);

    let utxos = load_utxo_models(&domain, refs, pagination)?;

    Ok(Json(utxos))
}

pub async fn utxos_with_asset<D: Domain>(
    Path((address, asset)): Path<(String, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, StatusCode> {
    let pagination = Pagination::try_from(params)?;

    let address = pallas::ledger::addresses::Address::from_bech32(&address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let asset = hex::decode(asset).map_err(|_| StatusCode::BAD_REQUEST)?;

    let address_refs = domain
        .state()
        .get_utxo_by_address(&address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let asset_refs = domain
        .state()
        .get_utxo_by_asset(&asset)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let refs = address_refs.intersection(&asset_refs).cloned().collect();

    let utxos = load_utxo_models(&domain, refs, pagination)?;

    Ok(Json(utxos))
}
