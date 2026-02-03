use std::collections::HashSet;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    address_transactions_content_inner::AddressTransactionsContentInner,
    address_utxo_content_inner::AddressUtxoContentInner,
};
use itertools::Either;
use pallas::ledger::{
    addresses::Address,
    traverse::{MultiEraBlock, MultiEraTx},
};

use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, CardanoIndexExt},
    ChainSummary,
};
use dolos_core::{BlockSlot, Domain, EraCbor, TxoRef};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

type VKeyOrAddress = Either<Vec<u8>, Vec<u8>>;

fn refs_for_address<D: Domain>(
    domain: &Facade<D>,
    address: &str,
) -> Result<HashSet<TxoRef>, Error> {
    if address.starts_with("addr_vkh") || address.starts_with("script") {
        let (_, addr) = bech32::decode(address).expect("failed to parse");

        Ok(domain.indexes().utxos_by_payment(&addr).map_err(|err| {
            dbg!(err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?)
    } else {
        let address = pallas::ledger::addresses::Address::from_bech32(address).map_err(|err| {
            dbg!(err);
            Error::InvalidAddress
        })?;
        Ok(domain
            .indexes()
            .utxos_by_address(&address.to_vec())
            .map_err(|err| {
                dbg!(err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?)
    }
}

async fn blocks_for_address<D>(
    domain: &Facade<D>,
    address: &str,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
) -> Result<(Vec<(BlockSlot, Option<Vec<u8>>)>, VKeyOrAddress), Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    if address.starts_with("addr_vkh") || address.starts_with("script") {
        let (_, addr) = bech32::decode(address).expect("failed to parse");

        Ok((
            domain
                .query()
                .blocks_by_payment(&addr, start_slot, end_slot)
                .await
                .map_err(|err| {
                    dbg!(err);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?,
            Either::Left(addr),
        ))
    } else {
        let address = pallas::ledger::addresses::Address::from_bech32(address)
            .map_err(|err| {
                dbg!(err);
                Error::InvalidAddress
            })?
            .to_vec();
        Ok((
            domain
                .query()
                .blocks_by_address(&address, start_slot, end_slot)
                .await
                .map_err(|err| {
                    dbg!(err);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?,
            Either::Right(address),
        ))
    }
}

async fn is_address_in_chain<D>(domain: &Facade<D>, address: &str) -> Result<bool, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let end_slot = domain.get_tip_slot()?;
    let start_slot = 0;

    if address.starts_with("addr_vkh") || address.starts_with("script") {
        let (_, addr) = bech32::decode(address).expect("failed to parse");

        Ok(domain
            .query()
            .blocks_by_payment(&addr, start_slot, end_slot)
            .await
            .map_err(|err| {
                dbg!(err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .iter()
            .any(|(_, block)| block.is_some()))
    } else {
        let address = pallas::ledger::addresses::Address::from_bech32(address).map_err(|err| {
            dbg!(err);
            Error::InvalidAddress
        })?;
        Ok(domain
            .query()
            .blocks_by_address(&address.to_vec(), start_slot, end_slot)
            .await
            .map_err(|err| {
                dbg!(err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .iter()
            .any(|(_, block)| block.is_some()))
    }
}

async fn is_asset_in_chain<D>(domain: &Facade<D>, asset: &[u8]) -> Result<bool, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let end_slot = domain.get_tip_slot()?;
    let start_slot = 0;

    Ok(domain
        .query()
        .blocks_by_asset(asset, start_slot, end_slot)
        .await
        .map_err(|err| {
            dbg!(err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .iter()
        .any(|(_, block)| block.is_some()))
}

pub async fn utxos<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let refs = refs_for_address(&domain, &address)?;

    // If the address is not seen on the chain, send 404.
    if refs.is_empty() {
        if is_address_in_chain(&domain, &address).await? {
            return Ok(Json(vec![]));
        }
        return Err(Error::Code(StatusCode::NOT_FOUND));
    }

    let utxos = super::utxos::load_utxo_models(&domain, refs, pagination).await?;

    Ok(Json(utxos))
}

pub async fn utxos_with_asset<D>(
    Path((address, asset)): Path<(String, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let mut should_filter = false;
    let refs = if &asset == "lovelace" {
        should_filter = true;
        refs_for_address(&domain, &address)?
    } else {
        let refs = refs_for_address(&domain, &address)?;
        let asset = hex::decode(asset).map_err(|_| Error::InvalidAsset)?;
        let asset_refs = domain
            .indexes()
            .utxos_by_asset(&asset)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if asset_refs.is_empty() {
            if is_asset_in_chain(&domain, &asset).await? {
                return Ok(Json(vec![]));
            } else {
                return Err(Error::Code(StatusCode::NOT_FOUND));
            }
        }

        refs.intersection(&asset_refs).cloned().collect()
    };

    if refs.is_empty() {
        if is_address_in_chain(&domain, &address).await? {
            return Ok(Json(vec![]));
        }
        return Err(Error::Code(StatusCode::NOT_FOUND));
    }

    let mut utxos = super::utxos::load_utxo_models(&domain, refs, pagination).await?;

    if should_filter {
        utxos.retain(|x| x.amount.iter().all(|x| x.unit == "lovelace"));
    }

    Ok(Json(utxos))
}

fn address_matches(address: &VKeyOrAddress, candidate: &Address) -> bool {
    match address {
        Either::Left(payment) => {
            if let Address::Shelley(shelley) = candidate {
                &shelley.payment().to_vec() == payment
            } else {
                false
            }
        }
        Either::Right(full) => full == &candidate.to_vec(),
    }
}

async fn has_address<D>(
    domain: &Facade<D>,
    address: &VKeyOrAddress,
    tx: &MultiEraTx<'_>,
) -> Result<bool, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    for (_, output) in tx.produces() {
        let candidate = output
            .address()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if address_matches(address, &candidate) {
            return Ok(true);
        }
    }

    for input in tx.consumes() {
        if let Some(EraCbor(era, cbor)) = domain
            .query()
            .tx_cbor(input.hash().as_slice().to_vec())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        {
            let parsed = MultiEraTx::decode_for_era(
                era.try_into()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                &cbor,
            )
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            if let Some(output) = parsed.produces_at(input.index() as usize) {
                let candidate = output
                    .address()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                if address_matches(address, &candidate) {
                    return Ok(true);
                }
            }
        }
    }

    Ok(false)
}

async fn find_txs<D>(
    domain: &Facade<D>,
    address: &VKeyOrAddress,
    chain: &ChainSummary,
    pagination: &Pagination,
    block: &[u8],
) -> Result<Vec<AddressTransactionsContentInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = vec![];

    for (idx, tx) in block.txs().iter().enumerate() {
        if !pagination.should_skip(block.number(), idx) && has_address(domain, address, tx).await? {
            let model = AddressTransactionsContentInner {
                tx_hash: hex::encode(tx.hash().as_slice()),
                tx_index: idx as i32,
                block_height: block.number() as i32,
                block_time: chain.slot_time(block.slot()) as i32,
            };

            matches.push(model);
        }
    }

    if matches!(pagination.order, Order::Desc) {
        matches = matches.into_iter().rev().collect();
    }

    Ok(matches)
}

pub async fn transactions<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressTransactionsContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit()?;
    let end_slot = domain.get_tip_slot()?;

    let (blocks, address) = blocks_for_address(&domain, &address, 0, end_slot).await?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut blocks = blocks;
    if matches!(pagination.order, Order::Desc) {
        blocks.reverse();
    }

    let mut matches = Vec::new();
    for (_slot, block) in blocks {
        let Some(block) = block else {
            continue;
        };

        let mut txs = find_txs(&domain, &address, &chain, &pagination, &block)
            .await
            .map_err(Error::Code)?;
        matches.append(&mut txs);

        if matches.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let transactions = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(transactions))
}

pub async fn txs<D>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit()?;
    let end_slot = domain.get_tip_slot()?;

    let (blocks, address) = blocks_for_address(&domain, &address, 0, end_slot).await?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut blocks = blocks;
    if matches!(pagination.order, Order::Desc) {
        blocks.reverse();
    }

    let mut matches = Vec::new();
    for (_slot, block) in blocks {
        let Some(block) = block else {
            continue;
        };

        let mut txs = find_txs(&domain, &address, &chain, &pagination, &block)
            .await
            .map_err(Error::Code)?;
        matches.append(&mut txs);

        if matches.len() >= pagination.from() + pagination.count {
            break;
        }
    }

    let transactions = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .map(|x| x.tx_hash)
        .collect();

    Ok(Json(transactions))
}
