use std::collections::HashSet;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    address_content::{AddressContent, Type as AddressType},
    address_transactions_content_inner::AddressTransactionsContentInner,
    address_utxo_content_inner::AddressUtxoContentInner,
    tx_content_output_amount_inner::TxContentOutputAmountInner,
};
use futures_util::{Stream, StreamExt};
use itertools::Either;
use pallas::ledger::{
    addresses::{Address, ShelleyPaymentPart, StakePayload},
    traverse::{MultiEraBlock, MultiEraOutput, MultiEraTx},
};

use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, CardanoIndexExt, SlotOrder},
    pallas_extras, ChainSummary,
};
use dolos_core::{BlockBody, BlockSlot, Domain, EraCbor, StateStore as _, TxoRef};

use crate::{
    error::Error,
    mapping::{aggregate_assets, IntoModel},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

impl From<Order> for SlotOrder {
    fn from(order: Order) -> Self {
        match order {
            Order::Asc => SlotOrder::Asc,
            Order::Desc => SlotOrder::Desc,
        }
    }
}

/// Represents a parsed address parameter
type VKeyOrAddress = Either<Vec<u8>, Vec<u8>>;

/// Stream of blocks returned by address queries
type BlockStream = std::pin::Pin<
    Box<dyn Stream<Item = Result<(BlockSlot, Option<BlockBody>), dolos_core::DomainError>> + Send>,
>;

enum ParsedAddress {
    Payment {
        key: Vec<u8>,
        script: bool,
    },
    Shelley {
        key: Vec<u8>,
        stake_address: Option<String>,
        script: bool,
    },
    Byron {
        key: Vec<u8>,
    },
}

/// Parse an address string into bytes for querying.
/// Supports:
/// - Payment credentials (addr_vkh*, script*) via bech32
/// - Shelley/stake addresses via bech32
/// - Byron addresses via base58
fn parse_address(address: &str) -> Result<ParsedAddress, Error> {
    // Payment credentials
    if address.starts_with("addr_vkh") || address.starts_with("script") {
        let (hrp, addr) = bech32::decode(address).map_err(|_| Error::InvalidAddress)?;
        return Ok(ParsedAddress::Payment {
            key: addr,
            script: hrp.as_str() == "script",
        });
    }

    // Try Shelley/stake bech32
    if let Ok(addr) = pallas::ledger::addresses::Address::from_bech32(address) {
        let key = addr.to_vec();

        return match addr {
            Address::Shelley(shelley) => {
                let stake_address = pallas_extras::shelley_address_to_stake_address(&shelley)
                    .map(|x| {
                        x.to_bech32()
                            .map_err(|_| Error::Code(StatusCode::INTERNAL_SERVER_ERROR))
                    })
                    .transpose()?;

                Ok(ParsedAddress::Shelley {
                    key,
                    stake_address,
                    script: matches!(shelley.payment(), ShelleyPaymentPart::Script(_)),
                })
            }
            Address::Stake(stake) => Ok(ParsedAddress::Shelley {
                key,
                stake_address: Some(
                    stake
                        .to_bech32()
                        .map_err(|_| Error::Code(StatusCode::INTERNAL_SERVER_ERROR))?,
                ),
                script: matches!(stake.payload(), StakePayload::Script(_)),
            }),
            Address::Byron(_) => Ok(ParsedAddress::Byron { key }),
        };
    }

    // Try Byron base58
    if let Ok(decoded) = base58::FromBase58::from_base58(address) {
        if let Ok(addr) = pallas::ledger::addresses::Address::from_bytes(&decoded) {
            if matches!(addr, Address::Byron(_)) {
                return Ok(ParsedAddress::Byron { key: addr.to_vec() });
            }
        }
    }

    Err(Error::InvalidAddress)
}

fn refs_for_address<D: Domain>(
    domain: &Facade<D>,
    address: &str,
) -> Result<HashSet<TxoRef>, Error> {
    let parsed = parse_address(address)?;
    refs_for_parsed_address(domain, &parsed)
}

fn refs_for_parsed_address<D: Domain>(
    domain: &Facade<D>,
    parsed: &ParsedAddress,
) -> Result<HashSet<TxoRef>, Error> {
    match parsed {
        ParsedAddress::Payment { key, .. } => {
            Ok(domain.indexes().utxos_by_payment(key).map_err(|err| {
                tracing::error!(?err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?)
        }
        ParsedAddress::Shelley { key, .. } | ParsedAddress::Byron { key } => {
            Ok(domain.indexes().utxos_by_address(key).map_err(|err| {
                tracing::error!(?err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?)
        }
    }
}

fn blocks_for_address_stream<D>(
    domain: &Facade<D>,
    address: &str,
    start_slot: BlockSlot,
    end_slot: BlockSlot,
    order: SlotOrder,
) -> Result<(BlockStream, VKeyOrAddress), Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    match parse_address(address)? {
        ParsedAddress::Payment { key, .. } => Ok((
            Box::pin(
                domain
                    .query()
                    .blocks_by_payment_stream(&key, start_slot, end_slot, order),
            ),
            Either::Left(key),
        )),
        ParsedAddress::Shelley { key, .. } | ParsedAddress::Byron { key } => Ok((
            Box::pin(
                domain
                    .query()
                    .blocks_by_address_stream(&key, start_slot, end_slot, order),
            ),
            Either::Right(key),
        )),
    }
}

struct AddressModelBuilder {
    address: String,
    parsed: ParsedAddress,
    amount: Vec<TxContentOutputAmountInner>,
}

impl IntoModel<AddressContent> for AddressModelBuilder {
    type SortKey = ();

    fn into_model(self) -> Result<AddressContent, StatusCode> {
        let (stake_address, r#type, script) = match self.parsed {
            ParsedAddress::Payment { script, .. } => (None, AddressType::Shelley, script),
            ParsedAddress::Shelley {
                stake_address,
                script,
                ..
            } => (stake_address, AddressType::Shelley, script),
            ParsedAddress::Byron { .. } => (None, AddressType::Byron, false),
        };

        Ok(AddressContent {
            address: self.address,
            amount: self.amount,
            stake_address,
            r#type,
            script,
        })
    }
}

fn amount_for_refs<D: Domain>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
) -> Result<Vec<TxContentOutputAmountInner>, Error> {
    let utxos = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let outputs: Vec<MultiEraOutput<'_>> = utxos
        .values()
        .map(|x| MultiEraOutput::try_from(x.as_ref()))
        .collect::<Result<_, _>>()
        .map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    Ok(aggregate_assets(outputs.iter()))
}

pub async fn by_address<D>(
    Path(address): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<AddressContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let parsed = parse_address(&address)?;
    let refs = refs_for_parsed_address(&domain, &parsed)?;

    if refs.is_empty() && !is_address_in_chain(&domain, &address).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let amount = amount_for_refs(&domain, refs)?;

    let model = AddressModelBuilder {
        address,
        parsed,
        amount,
    }
    .into_model()
    .map_err(Error::Code)?;

    Ok(Json(model))
}

async fn is_address_in_chain<D>(domain: &Facade<D>, address: &str) -> Result<bool, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let end_slot = domain.get_tip_slot()?;
    let start_slot = 0;

    let (mut stream, _) =
        blocks_for_address_stream(domain, address, start_slot, end_slot, SlotOrder::Asc)?;

    while let Some(res) = stream.next().await {
        match res {
            Ok((_, Some(_))) => return Ok(true),
            Err(err) => {
                tracing::error!(?err);
                return Err(StatusCode::INTERNAL_SERVER_ERROR.into());
            }
            _ => continue,
        }
    }

    Ok(false)
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
            tracing::error!(?err);
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
    pagination.enforce_max_scan_limit(domain.config.max_scan_items)?;

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let address_str = address.clone();
    let (stream, address) = blocks_for_address_stream(
        &domain,
        &address,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    )?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = Vec::new();

    let mut stream = Box::pin(stream);
    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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

    let transactions: Vec<AddressTransactionsContentInner> = matches
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    if transactions.is_empty() {
        let exists = is_address_in_chain(&domain, &address_str).await?;

        if !exists {
            return Err(StatusCode::NOT_FOUND.into());
        }
    }

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
    pagination.enforce_max_scan_limit(domain.config.max_scan_items)?;

    let (start_slot, end_slot) = pagination.start_and_end_slots(&domain).await?;
    let (stream, address) = blocks_for_address_stream(
        &domain,
        &address,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    )?;
    let chain = domain
        .get_chain_summary()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut matches = Vec::new();

    let mut stream = Box::pin(stream);
    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|err| {
            tracing::error!(?err);
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::{
        address_content::{AddressContent, Type as AddressType},
        address_transactions_content_inner::AddressTransactionsContentInner,
        address_utxo_content_inner::AddressUtxoContentInner,
    };

    fn invalid_address() -> &'static str {
        "not-an-address"
    }

    fn missing_address() -> &'static str {
        "addr_test1qqrswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpcgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyq3w9hxq"
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[tokio::test]
    async fn addresses_by_address_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContent =
            serde_json::from_slice(&bytes).expect("failed to parse address content");

        assert_eq!(item.address, address);
        assert_eq!(item.r#type, AddressType::Shelley);
        assert!(item.stake_address.is_some());
        assert!(!item.amount.is_empty());
    }

    #[tokio::test]
    async fn addresses_by_address_payment_credential_happy_path() {
        let app = TestApp::new();
        let address = Address::from_bech32(app.vectors().address.as_str())
            .expect("invalid synthetic test address");

        let Address::Shelley(shelley) = address else {
            panic!("expected shelley test address")
        };

        let payment = shelley.payment().to_vec();

        let payment_cred = bech32::encode::<bech32::Bech32>(
            bech32::Hrp::parse("addr_vkh").expect("invalid hrp"),
            &payment,
        )
        .expect("failed to encode payment credential");

        let path = format!("/addresses/{payment_cred}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let item: AddressContent =
            serde_json::from_slice(&bytes).expect("failed to parse address content");

        assert_eq!(item.address, payment_cred);
        assert_eq!(item.r#type, AddressType::Shelley);
        assert_eq!(item.stake_address, None);
        assert!(!item.script);
        assert!(!item.amount.is_empty());
    }

    #[tokio::test]
    async fn addresses_by_address_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_by_address_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_by_address_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn addresses_transactions_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address transactions");
    }

    #[tokio::test]
    async fn addresses_transactions_slot_constrained() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!(
            "/addresses/{address}/transactions?from={}&to={}",
            block.block_number, block.block_number
        );
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let items: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address transactions");
        assert!(!items.is_empty());
        for item in items {
            assert!(block.tx_hashes.contains(&item.tx_hash));
        }
    }

    #[tokio::test]
    async fn addresses_transactions_paginated() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path_page_1 = format!("/addresses/{address}/transactions?page=1&count=2");
        let path_page_2 = format!("/addresses/{address}/transactions?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse transactions page 1");
        let page_2: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse transactions page 2");

        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 2);

        let page_1_hashes: std::collections::HashSet<_> =
            page_1.into_iter().map(|x| x.tx_hash).collect();
        let page_2_hashes: std::collections::HashSet<_> =
            page_2.into_iter().map(|x| x.tx_hash).collect();
        assert!(page_1_hashes.is_disjoint(&page_2_hashes));
    }

    #[tokio::test]
    async fn addresses_transactions_order_asc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn addresses_transactions_order_desc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse transactions desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc.iter().map(|x| (x.block_height, x.tx_index)).collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn addresses_transactions_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/transactions", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_transactions_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/transactions", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_transactions_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/transactions");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn addresses_utxos_happy_path() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos?page=1");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address utxos");
    }

    #[tokio::test]
    async fn addresses_utxos_paginated() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path_page_1 = format!("/addresses/{address}/utxos?page=1&count=2");
        let path_page_2 = format!("/addresses/{address}/utxos?page=2&count=2");

        let (status_1, bytes_1) = app.get_bytes(&path_page_1).await;
        let (status_2, bytes_2) = app.get_bytes(&path_page_2).await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse utxos page 1");
        let page_2: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse utxos page 2");

        assert_eq!(page_1.len(), 2);
        assert_eq!(page_2.len(), 2);

        let page_1_hashes: std::collections::HashSet<_> = page_1
            .into_iter()
            .map(|x| format!("{}#{}", x.tx_hash, x.output_index))
            .collect();
        let page_2_hashes: std::collections::HashSet<_> = page_2
            .into_iter()
            .map(|x| format!("{}#{}", x.tx_hash, x.output_index))
            .collect();
        assert!(page_1_hashes.is_disjoint(&page_2_hashes));
    }

    #[tokio::test]
    async fn addresses_utxos_order_asc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos?order=asc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let asc: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos asc");
        if asc.is_empty() {
            return;
        }
        let asc_pos: Vec<_> = asc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(asc_pos.windows(2).all(|w| w[0] <= w[1]));
    }

    #[tokio::test]
    async fn addresses_utxos_order_desc() {
        let app = TestApp::new();
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos?order=desc&count=5");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let desc: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse utxos desc");
        if desc.is_empty() {
            return;
        }
        let desc_pos: Vec<_> = desc
            .iter()
            .map(|x| {
                let (block_number, tx_index) = app.vectors().tx_position(&x.tx_hash);
                (block_number, tx_index, x.output_index)
            })
            .collect();
        assert!(desc_pos.windows(2).all(|w| w[0] >= w[1]));
    }

    #[tokio::test]
    async fn addresses_utxos_bad_request() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/utxos", invalid_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn addresses_utxos_not_found() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/utxos", missing_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn addresses_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let address = app.vectors().address.as_str();
        let path = format!("/addresses/{address}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[test]
    fn test_parse_address_payment() {
        let addr = "addr_vkh1h7wl3l3w6heru0us8mdc3v3jlahq79w49cpypsuvgjhdwp5apep";
        let parsed = parse_address(addr);
        assert!(matches!(
            parsed,
            Ok(ParsedAddress::Payment {
                key: _,
                script: false
            })
        ));
    }

    #[test]
    fn test_parse_address_shelley() {
        let addr = "addr1q9dhugez3ka82k2kgh7r2lg0j7aztr8uell46kydfwu3vk6n8w2cdu8mn2ha278q6q25a9rc6gmpfeekavuargcd32vsvxhl7e";
        let parsed = parse_address(addr);
        assert!(matches!(parsed, Ok(ParsedAddress::Shelley { .. })));
    }

    #[test]
    fn test_parse_address_byron() {
        let addr = "37btjrVyb4KDXBNC4haBVPCrro8AQPHwvCMp3RFhhSVWwfFmZ6wwzSK6JK1hY6wHNmtrpTf1kdbva8TCneM2YsiXT7mrzT21EacHnPpz5YyUdj64na";
        let parsed = parse_address(addr);
        assert!(matches!(parsed, Ok(ParsedAddress::Byron { .. })));
    }

    #[test]
    fn test_parse_address_invalid() {
        let addr = "invalid_address";
        let parsed = parse_address(addr);
        assert!(matches!(parsed, Err(Error::InvalidAddress)));
    }
}
