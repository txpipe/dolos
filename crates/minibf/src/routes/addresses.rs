use std::collections::{HashMap, HashSet};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    address_transactions_content_inner::AddressTransactionsContentInner,
    address_utxo_content_inner::AddressUtxoContentInner,
};
use dolos_cardano::pparams::ChainSummary;
use dolos_core::{ArchiveStore, Domain, StateStore, TxoRef};
use itertools::Itertools;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx};
use rayon::prelude::*;

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
        .chunks(100)
        .collect::<Vec<_>>() // Chunk into slices of 100
        .par_iter() // Parallelize over the chunks
        .map(|chunk| {
            chunk
                .iter()
                .flat_map(|tx| {
                    match domain.archive().get_block_with_tx(tx.as_slice()) {
                        Ok(Some((cbor, txorder))) => {
                            let Ok(block) = MultiEraBlock::decode(&cbor) else {
                                return Some(Err(StatusCode::INTERNAL_SERVER_ERROR));
                            };
                            let block_data = match UtxoBlockData::try_from((block, txorder)) {
                                Ok(data) => data,
                                Err(err) => return Some(Err(err)),
                            };
                            Some(Ok((*tx, block_data))) // Clone tx to move into the new scope if needed by HashMap
                        }
                        Ok(None) => None,
                        Err(_) => Some(Err(StatusCode::INTERNAL_SERVER_ERROR)),
                    }
                })
                .collect::<Result<Vec<(_, UtxoBlockData)>, _>>()
        })
        .collect::<Result<Vec<Vec<_>>, _>>()?
        .into_iter()
        .flatten()
        .collect();

    let mut models: Vec<_> = utxos
        .into_iter()
        .map(|(TxoRef(tx_hash, txo_idx), txo)| {
            let builder = UtxoOutputModelBuilder::from_output(*txo_idx, txo);
            let block_data = block_deps.get(tx_hash).cloned();

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

struct TransactionWithAddressIter<A: ArchiveStore> {
    address: Vec<u8>,
    blocks: A::SparseBlockIter,
    chain: ChainSummary,
}

impl<A: ArchiveStore> TransactionWithAddressIter<A> {
    fn new(address: Vec<u8>, blocks: A::SparseBlockIter, chain: ChainSummary) -> Self {
        Self {
            address,
            blocks,
            chain,
        }
    }

    fn has_address(&self, tx: &MultiEraTx) -> Result<bool, StatusCode> {
        for (_, output) in tx.produces() {
            let address = output
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .to_vec();

            if address == self.address {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn find_txs(&self, block: &[u8]) -> Result<Vec<AddressTransactionsContentInner>, StatusCode> {
        let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut matches = vec![];

        for (idx, tx) in block.txs().iter().enumerate() {
            if self.has_address(tx)? {
                let model = AddressTransactionsContentInner {
                    tx_hash: hex::encode(tx.hash().as_slice()),
                    tx_index: idx as i32,
                    block_height: block.number() as i32,
                    block_time: dolos_cardano::slot_time(block.slot(), &self.chain) as i32,
                };

                matches.push(model);
            }
        }

        Ok(matches)
    }
}

impl<A: ArchiveStore> Iterator for TransactionWithAddressIter<A> {
    type Item = Vec<Result<AddressTransactionsContentInner, StatusCode>>;

    fn next(&mut self) -> Option<Self::Item> {
        let block = self.blocks.next()?;

        if block.is_err() {
            return Some(vec![Err(StatusCode::INTERNAL_SERVER_ERROR)]);
        }

        let (_, block) = block.unwrap();

        let txs = if let Some(block) = block {
            self.find_txs(&block)
        } else {
            Ok(vec![])
        };

        if txs.is_err() {
            return Some(vec![Err(StatusCode::INTERNAL_SERVER_ERROR)]);
        }

        Some(txs.unwrap().into_iter().map(Ok).collect())
    }
}

pub async fn transactions<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressTransactionsContentInner>>, StatusCode> {
    let pagination = Pagination::try_from(params)?;

    let address = pallas::ledger::addresses::Address::from_bech32(&address)
        .map_err(|_| StatusCode::BAD_REQUEST)?;

    let blocks = domain
        .archive()
        .iter_blocks_with_address(&address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transactions =
        TransactionWithAddressIter::<D::Archive>::new(address.to_vec(), blocks, chain)
            .flatten()
            .skip(pagination.from())
            .take(pagination.count as usize)
            .try_collect()?;

    Ok(Json(transactions))
}
