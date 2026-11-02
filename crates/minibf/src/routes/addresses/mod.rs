use std::collections::HashSet;

use axum::http::StatusCode;
use blockfrost_openapi::models::{
    address_transactions_content_inner::AddressTransactionsContentInner,
    tx_content_output_amount_inner::TxContentOutputAmountInner,
};
use dolos_cardano::{
    indexes::{utxo_dimensions, AsyncCardanoQueryExt, CardanoStateIndexExt, SlotOrder},
    pallas_extras, ChainSummary,
};
use dolos_core::{BlockBody, BlockSlot, Domain, StateStore as _, Tag, TxoRef};
use futures_util::{Stream, StreamExt};
use itertools::Either;
use pallas::ledger::{
    addresses::{Address, ShelleyPaymentPart, StakePayload},
    traverse::{MultiEraOutput, MultiEraTx},
};

use crate::{
    error::Error,
    inputs::{match_block_txs, tx_touches_output, InputDeps, InputResolver},
    mapping::{aggregate_assets, AddressKind},
    pagination::{Order, Pagination},
    Facade,
};

mod by_address;
mod extended;
mod total;
mod transactions;
mod txs;
mod utxos;
mod utxos_asset;

pub use by_address::by_address;
pub use extended::extended;
pub use total::total;
pub use transactions::transactions;
pub use txs::txs;
pub use utxos::utxos;
pub use utxos_asset::utxos_with_asset;

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

/// The live-UTxO tag `address` answers under: its payment credential, or the
/// whole address.
fn address_tag(parsed: &ParsedAddress) -> Tag {
    match parsed {
        ParsedAddress::Payment { key, .. } => Tag::new(utxo_dimensions::PAYMENT, key.clone()),
        ParsedAddress::Shelley { key, .. } | ParsedAddress::Byron { key } => {
            Tag::new(utxo_dimensions::ADDRESS, key.clone())
        }
    }
}

fn refs_for_parsed_address<D: Domain>(
    domain: &Facade<D>,
    parsed: &ParsedAddress,
) -> Result<HashSet<TxoRef>, StatusCode> {
    match parsed {
        ParsedAddress::Payment { key, .. } => {
            Ok(domain.state().utxos_by_payment(key).map_err(|err| {
                tracing::error!(?err);
                StatusCode::INTERNAL_SERVER_ERROR
            })?)
        }
        ParsedAddress::Shelley { key, .. } | ParsedAddress::Byron { key } => {
            Ok(domain.state().utxos_by_address(key).map_err(|err| {
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

impl ParsedAddress {
    fn into_model_kind(self) -> AddressKind {
        match self {
            ParsedAddress::Payment { script, .. } => AddressKind::Payment { script },
            ParsedAddress::Shelley {
                stake_address,
                script,
                ..
            } => AddressKind::Shelley {
                stake_address,
                script,
            },
            ParsedAddress::Byron { .. } => AddressKind::Byron,
        }
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

fn has_address(
    resolver: &mut InputResolver<'_>,
    address: &VKeyOrAddress,
    tx: &MultiEraTx<'_>,
) -> Result<bool, StatusCode> {
    tx_touches_output(resolver, tx, |output| {
        let candidate = output
            .address()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(address_matches(address, &candidate))
    })
}

async fn find_txs<D>(
    domain: &Facade<D>,
    deps: &mut InputDeps,
    address: &VKeyOrAddress,
    chain: &ChainSummary,
    pagination: &Pagination,
    block: BlockBody,
) -> Result<Vec<AddressTransactionsContentInner>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let address = address.clone();
    let matched = match_block_txs(domain, deps, pagination, block, move |resolver, tx| {
        has_address(resolver, &address, tx)
    })
    .await?;

    let mut matches: Vec<_> = matched
        .into_iter()
        .map(|tx| AddressTransactionsContentInner {
            tx_hash: hex::encode(tx.hash.as_slice()),
            tx_index: tx.index as i32,
            block_height: tx.block_number as i32,
            block_time: chain.slot_time(tx.block_slot) as i32,
        })
        .collect();

    if matches!(pagination.order, Order::Desc) {
        matches = matches.into_iter().rev().collect();
    }

    Ok(matches)
}

#[cfg(test)]
pub(super) mod testing {
    use super::*;
    use crate::test_support::TestApp;
    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;

    pub fn invalid_address() -> &'static str {
        "not-an-address"
    }

    pub fn missing_address() -> &'static str {
        "addr_test1qqrswpc8qurswpc8qurswpc8qurswpc8qurswpc8qurswpcgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyqszqgpqyq3w9hxq"
    }

    pub async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    pub async fn utxo_page(app: &TestApp, path: &str) -> Vec<String> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status for {path}: {}",
            String::from_utf8_lossy(&bytes)
        );

        let page: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address utxos");

        page.into_iter()
            .map(|x| format!("{}#{}", x.tx_hash, x.output_index))
            .collect()
    }

    /// Walks `path` at `count` rows per page. Every page holds `count` rows
    /// except the last, and the pages join into `full`.
    pub async fn assert_pages_join(
        app: &TestApp,
        path: &str,
        order: &str,
        count: usize,
        full: &[String],
    ) {
        let mut walked = Vec::new();

        for page in 1..=full.len().div_ceil(count) + 1 {
            let rows = utxo_page(
                app,
                &format!("{path}?order={order}&count={count}&page={page}"),
            )
            .await;
            let left = full.len().saturating_sub((page - 1) * count);

            assert_eq!(
                rows.len(),
                left.min(count),
                "{path} order={order} count={count} page={page}"
            );

            walked.extend(rows);
        }

        assert_eq!(walked, full, "{path} order={order} count={count}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
