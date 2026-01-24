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
use itertools::{Either, Itertools};
use pallas::ledger::{
    addresses::Address,
    traverse::{MultiEraBlock, MultiEraTx},
};

use dolos_cardano::{
    indexes::{CardanoIndexExt, CardanoQueryExt},
    ChainSummary,
};
use dolos_core::{BlockSlot, Domain, EraCbor, QueryHelpers, SparseBlockIter, TxoRef};

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
    if address.starts_with("addr_vkh") {
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

fn blocks_for_address<D: Domain>(
    domain: &D,
    address: &str,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
) -> Result<(SparseBlockIter<D::Indexes, D::Archive>, VKeyOrAddress), Error> {
    if address.starts_with("addr_vkh") {
        let (_, addr) = bech32::decode(address).expect("failed to parse");

        Ok((
            domain
                .blocks_by_payment(&addr, start_slot, end_slot)
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
                .blocks_by_address(&address, start_slot, end_slot)
                .map_err(|err| {
                    dbg!(err);
                    StatusCode::INTERNAL_SERVER_ERROR
                })?,
            Either::Right(address),
        ))
    }
}

fn is_address_in_chain<D: Domain>(domain: &Facade<D>, address: &str) -> Result<bool, Error> {
    let end_slot = domain.get_tip_slot()?;
    let start_slot = 0;

    if address.starts_with("addr_vkh") {
        let (_, addr) = bech32::decode(address).expect("failed to parse");

        Ok(domain
            .inner
            .blocks_by_payment(&addr, start_slot, end_slot)
            .map_err(|err| {
                dbg!(err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .next()
            .is_some())
    } else {
        let address = pallas::ledger::addresses::Address::from_bech32(address).map_err(|err| {
            dbg!(err);
            Error::InvalidAddress
        })?;
        Ok(domain
            .inner
            .blocks_by_address(&address.to_vec(), start_slot, end_slot)
            .map_err(|err| {
                dbg!(err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?
            .next()
            .is_some())
    }
}

fn is_asset_in_chain<D: Domain>(domain: &Facade<D>, asset: &[u8]) -> Result<bool, Error> {
    let end_slot = domain.get_tip_slot()?;
    let start_slot = 0;

    Ok(domain
        .inner
        .blocks_by_asset(asset, start_slot, end_slot)
        .map_err(|err| {
            dbg!(err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?
        .next()
        .is_some())
}

pub async fn utxos<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;

    let refs = refs_for_address(&domain, &address)?;

    // If the address is not seen on the chain, send 404.
    if refs.is_empty() {
        if is_address_in_chain(&domain, &address)? {
            return Ok(Json(vec![]));
        }
        return Err(Error::Code(StatusCode::NOT_FOUND));
    }

    let utxos = super::utxos::load_utxo_models(&domain, refs, pagination)?;

    Ok(Json(utxos))
}

pub async fn utxos_with_asset<D: Domain>(
    Path((address, asset)): Path<(String, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressUtxoContentInner>>, Error> {
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
            if is_asset_in_chain(&domain, &asset)? {
                return Ok(Json(vec![]));
            } else {
                return Err(Error::Code(StatusCode::NOT_FOUND));
            }
        }

        refs.intersection(&asset_refs).cloned().collect()
    };

    if refs.is_empty() {
        if is_address_in_chain(&domain, &address)? {
            return Ok(Json(vec![]));
        }
        return Err(Error::Code(StatusCode::NOT_FOUND));
    }

    let mut utxos = super::utxos::load_utxo_models(&domain, refs, pagination)?;

    if should_filter {
        utxos.retain(|x| x.amount.iter().all(|x| x.unit == "lovelace"));
    }

    Ok(Json(utxos))
}

struct TransactionWithAddressIter<D: Domain> {
    address: VKeyOrAddress,
    blocks: SparseBlockIter<D::Indexes, D::Archive>,
    chain: ChainSummary,
    pagination: Pagination,
    domain: D,
}

impl<D: Domain> TransactionWithAddressIter<D> {
    fn new(
        address: VKeyOrAddress,
        blocks: SparseBlockIter<D::Indexes, D::Archive>,
        chain: ChainSummary,
        pagination: Pagination,
        domain: D,
    ) -> Self {
        Self {
            address,
            blocks,
            chain,
            domain,
            pagination,
        }
    }

    fn address_matches(&self, address: &Address) -> bool {
        match &self.address {
            Either::Left(payment) => {
                if let Address::Shelley(shelley) = address {
                    &shelley.payment().to_vec() == payment
                } else {
                    false
                }
            }
            Either::Right(full) => full == &address.to_vec(),
        }
    }

    fn has_address(&self, tx: &MultiEraTx) -> Result<bool, StatusCode> {
        for (_, output) in tx.produces() {
            let address = output
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            if self.address_matches(&address) {
                return Ok(true);
            }
        }

        for input in tx.consumes() {
            if let Some(EraCbor(era, cbor)) = self
                .domain
                .tx_cbor(input.hash().as_slice())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            {
                let parsed = MultiEraTx::decode_for_era(
                    era.try_into()
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?,
                    &cbor,
                )
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
                if let Some(output) = parsed.produces_at(input.index() as usize) {
                    let address = output
                        .address()
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    if self.address_matches(&address) {
                        return Ok(true);
                    }
                }
            }
        }

        Ok(false)
    }

    fn find_txs(&self, block: &[u8]) -> Result<Vec<AddressTransactionsContentInner>, StatusCode> {
        let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut matches = vec![];

        for (idx, tx) in block.txs().iter().enumerate() {
            if !self.pagination.should_skip(block.number(), idx) && self.has_address(tx)? {
                let model = AddressTransactionsContentInner {
                    tx_hash: hex::encode(tx.hash().as_slice()),
                    tx_index: idx as i32,
                    block_height: block.number() as i32,
                    block_time: self.chain.slot_time(block.slot()) as i32,
                };

                matches.push(model);
            }
        }

        if matches!(self.pagination.order, Order::Desc) {
            matches = matches.into_iter().rev().collect();
        }

        Ok(matches)
    }
}

impl<D: Domain> Iterator for TransactionWithAddressIter<D> {
    type Item = Vec<Result<AddressTransactionsContentInner, StatusCode>>;

    fn next(&mut self) -> Option<Self::Item> {
        let block = match self.pagination.order {
            Order::Asc => self.blocks.next()?,
            Order::Desc => self.blocks.next_back()?,
        };

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
) -> Result<Json<Vec<AddressTransactionsContentInner>>, Error> {
    let pagination = Pagination::try_from(params)?;
    let end_slot = domain.get_tip_slot()?;

    let (blocks, address) = blocks_for_address(&domain.inner, &address, 0, end_slot)?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transactions = TransactionWithAddressIter::<D>::new(
        address,
        blocks,
        chain,
        pagination.clone(),
        domain.inner.clone(),
    )
    .flatten()
    .skip(pagination.from())
    .take(pagination.count)
    .try_collect()?;

    Ok(Json(transactions))
}

pub async fn txs<D: Domain>(
    Path(address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error> {
    let pagination = Pagination::try_from(params)?;
    let end_slot = domain.get_tip_slot()?;

    let (blocks, address) = blocks_for_address(&domain.inner, &address, 0, end_slot)?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let transactions = TransactionWithAddressIter::<D>::new(
        address,
        blocks,
        chain,
        pagination.clone(),
        domain.inner.clone(),
    )
    .flatten()
    .skip(pagination.from())
    .take(pagination.count)
    .collect::<Result<Vec<_>, _>>()?
    .into_iter()
    .map(|x| x.tx_hash)
    .collect();

    Ok(Json(transactions))
}
