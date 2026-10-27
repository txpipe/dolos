use std::{collections::HashMap, sync::Arc};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset_addresses_inner::AssetAddressesInner;
use dolos_cardano::indexes::CardanoStateIndexExt;
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, StateStore as _, TxoRef};
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraOutput;

use crate::{
    error::Error,
    mapping::assets::resolve_asset,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// UTxOs of an asset read, decoded and located per blocking task. Each one
/// also costs an archive index lookup, so one chunk holds its permit for a
/// few milliseconds.
const ASSET_ADDRESS_CHUNK: usize = 256;

/// Per address: the earliest chain position of a UTxO holding the asset, and
/// the quantity held.
type AssetHoldings = HashMap<String, ((BlockSlot, u32), u128)>;

pub async fn by_subject_addresses<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetAddressesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let (asset, _) = resolve_asset(&domain, &subject)?;

    let addresses = asset_holdings(&domain, asset, ASSET_ADDRESS_CHUNK).await?;

    let mut items = addresses
        .into_iter()
        .sorted_by_key(|(_, (sort, _))| *sort)
        .map(|(address, (_, amount))| AssetAddressesInner {
            address,
            quantity: amount.to_string(),
        })
        .collect_vec();

    if matches!(pagination.order, Order::Desc) {
        items.reverse();
    }

    let sorted = items
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .collect_vec();

    Ok(Json(sorted))
}

/// The holdings of every address with a UTxO that holds `asset`, `chunk`
/// UTxOs per blocking task.
async fn asset_holdings<D>(
    domain: &Facade<D>,
    asset: Vec<u8>,
    chunk: usize,
) -> Result<AssetHoldings, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let asset = Arc::new(asset);

    let utxoset = {
        let asset = asset.clone();
        domain
            .blocking(move |domain| {
                domain
                    .state()
                    .utxos_by_asset(&asset)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .await?
    };

    let mut refs = utxoset.into_iter().collect_vec();
    refs.sort();

    let mut addresses = AssetHoldings::new();

    for refs in refs.chunks(chunk.max(1)) {
        let refs = refs.to_vec();
        let asset = asset.clone();

        let holdings = domain
            .blocking(move |domain| chunk_holdings(&domain, refs, &asset))
            .await?;

        for (address, (sort, amount)) in holdings {
            addresses
                .entry(address)
                .and_modify(|entry| {
                    entry.0 = entry.0.min(sort);
                    entry.1 += amount;
                })
                .or_insert((sort, amount));
        }
    }

    Ok(addresses)
}

fn chunk_holdings<D>(
    domain: &Facade<D>,
    refs: Vec<TxoRef>,
    asset: &[u8],
) -> Result<AssetHoldings, StatusCode>
where
    D: Domain,
{
    let utxos = domain
        .state()
        .get_utxos(refs)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut addresses = AssetHoldings::new();
    for (txoref, eracbor) in utxos {
        let sort = (
            domain
                .archive()
                .slot_by_tx_hash(txoref.0.as_slice())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?,
            txoref.1,
        );

        let utxo = MultiEraOutput::decode(eracbor.era().try_into().unwrap(), eracbor.cbor())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let amount = utxo
            .value()
            .assets()
            .iter()
            .flat_map(|x| {
                let subject = x.policy().to_vec();
                x.assets()
                    .iter()
                    .find(|x| [subject.as_slice(), x.name()].concat() == asset)
                    .map(|x| x.any_coin() as u128)
            })
            .sum();

        addresses
            .entry(
                utxo.address()
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                    .to_string(),
            )
            .and_modify(|entry| {
                entry.0 = entry.0.min(sort);
                entry.1 += amount;
            })
            .or_insert((sort, amount));
    }

    Ok(addresses)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::assets::decode_asset_subject,
        routes::assets::testing::*,
        test_support::{health_beside, TestApp, TestFault, SLOW_READ},
    };

    #[tokio::test]
    async fn assets_by_subject_addresses_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/addresses");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let addresses: Vec<AssetAddressesInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset addresses");
        assert!(!addresses.is_empty());
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/addresses");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/addresses", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_addresses_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/addresses");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn slow_asset_addresses_do_not_stall_health() {
        let app = TestApp::new_with_fault(Some(TestFault::SlowUtxoReads(SLOW_READ)));
        let path = format!("/assets/{}/addresses", app.vectors().asset_unit);

        assert_eq!(health_beside(&app, &path).await, StatusCode::OK);
    }

    #[tokio::test]
    async fn chunked_asset_holdings_match_unchunked() {
        let app = TestApp::new();
        let facade = app.facade();
        let asset = decode_asset_subject(&app.vectors().asset_unit).expect("asset subject");

        let utxos = facade.state().utxos_by_asset(&asset).expect("asset refs");
        assert!(utxos.len() > 2, "too few utxos to split into chunks");

        let whole = asset_holdings(&facade, asset.clone(), usize::MAX)
            .await
            .expect("holdings");

        for chunk in [1, 2] {
            let chunked = asset_holdings(&facade, asset.clone(), chunk)
                .await
                .expect("holdings");
            assert_eq!(chunked, whole);
        }
    }
}
