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
use futures_util::{Stream, StreamExt};
use itertools::Either;
use pallas::ledger::{
    addresses::Address,
    traverse::{MultiEraBlock, MultiEraTx},
};

use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, CardanoIndexExt, SlotOrder},
    ChainSummary,
};
use dolos_core::{BlockBody, BlockSlot, Domain, EraCbor, TxoRef};

use crate::{
    error::Error,
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
    Payment(Vec<u8>),
    Full(Vec<u8>),
}

/// Parse an address string into bytes for querying.
/// Supports:
/// - Payment credentials (addr_vkh*, script*) via bech32
/// - Shelley/stake addresses via bech32
/// - Byron addresses via base58
fn parse_address(address: &str) -> Result<ParsedAddress, Error> {
    // Payment credentials
    if address.starts_with("addr_vkh") || address.starts_with("script") {
        let (_, addr) = bech32::decode(address).map_err(|_| Error::InvalidAddress)?;
        return Ok(ParsedAddress::Payment(addr));
    }

    // Try Shelley/stake bech32
    if let Ok(addr) = pallas::ledger::addresses::Address::from_bech32(address) {
        return Ok(ParsedAddress::Full(addr.to_vec()));
    }

    // Try Byron base58
    if let Ok(decoded) = base58::FromBase58::from_base58(address) {
        if let Ok(addr) = pallas::ledger::addresses::Address::from_bytes(&decoded) {
            if matches!(addr, Address::Byron(_)) {
                return Ok(ParsedAddress::Full(addr.to_vec()));
            }
        }
    }

    Err(Error::InvalidAddress)
}

fn refs_for_address<D: Domain>(
    domain: &Facade<D>,
    address: &str,
) -> Result<HashSet<TxoRef>, Error> {
    match parse_address(address)? {
        ParsedAddress::Payment(addr) => {
            Ok(domain.indexes().utxos_by_payment(&addr).map_err(|err| {
                tracing::error!(?err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?)
        }
        ParsedAddress::Full(addr) => {
            Ok(domain.indexes().utxos_by_address(&addr).map_err(|err| {
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
        ParsedAddress::Payment(addr) => Ok((
            Box::pin(
                domain
                    .query()
                    .blocks_by_payment_stream(&addr, start_slot, end_slot, order),
            ),
            Either::Left(addr),
        )),
        ParsedAddress::Full(addr) => Ok((
            Box::pin(
                domain
                    .query()
                    .blocks_by_address_stream(&addr, start_slot, end_slot, order),
            ),
            Either::Right(addr),
        )),
    }
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
    pagination.enforce_max_scan_limit()?;

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
    use blockfrost_openapi::models::{
        address_transactions_content_inner::AddressTransactionsContentInner,
        address_utxo_content_inner::AddressUtxoContentInner,
    };
    use crate::test_support::{TestApp, TestFault, ADDRESS};

    fn invalid_address() -> &'static str {
        "not-an-address"
    }

    fn missing_address() -> &'static str {
        "addr_test1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqq"
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
    async fn addresses_transactions_happy_path() {
        let app = TestApp::new();
        let path = format!("/addresses/{ADDRESS}/transactions?page=999999");
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
        let path = format!("/addresses/{ADDRESS}/transactions");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn addresses_utxos_happy_path() {
        let app = TestApp::new();
        let path = format!("/addresses/{ADDRESS}/utxos?page=999999");
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
        let path = format!("/addresses/{ADDRESS}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[test]
    fn test_parse_address_payment() {
        let addr = "addr_vkh1h7wl3l3w6heru0us8mdc3v3jlahq79w49cpypsuvgjhdwp5apep";
        let parsed = parse_address(addr);
        assert!(matches!(parsed, Ok(ParsedAddress::Payment(_))));
    }

    #[test]
    fn test_parse_address_shelley() {
        let addr = "addr1q9dhugez3ka82k2kgh7r2lg0j7aztr8uell46kydfwu3vk6n8w2cdu8mn2ha278q6q25a9rc6gmpfeekavuargcd32vsvxhl7e";
        let parsed = parse_address(addr);
        assert!(matches!(parsed, Ok(ParsedAddress::Full(_))));
    }

    #[test]
    fn test_parse_address_byron() {
        let addr = "37btjrVyb4KDXBNC4haBVPCrro8AQPHwvCMp3RFhhSVWwfFmZ6wwzSK6JK1hY6wHNmtrpTf1kdbva8TCneM2YsiXT7mrzT21EacHnPpz5YyUdj64na";
        let parsed = parse_address(addr);
        assert!(matches!(parsed, Ok(ParsedAddress::Full(_))));
    }

    #[test]
    fn test_parse_address_invalid() {
        let addr = "invalid_address";
        let parsed = parse_address(addr);
        assert!(matches!(parsed, Err(Error::InvalidAddress)));
    }
}
