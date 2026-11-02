use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::account_addresses_assets_inner::AccountAddressesAssetsInner;
use dolos_cardano::{
    indexes::CardanoStateIndexExt,
    model::{AccountState, ProposalState},
};
use dolos_core::{async_query::BlockMetaResolver, Domain, StateStore as _};

use crate::{
    error::Error,
    mapping,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{account_exists, parse_account_key_param};

pub async fn by_stake_addresses_assets<D>(
    Path(stake_address): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AccountAddressesAssetsInner>>, Error>
where
    Option<AccountState>: From<D::Entity>,
    Option<ProposalState>: From<D::Entity>,
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let network = domain.get_network_id()?;
    let account_key = parse_account_key_param(&stake_address, network)?;

    if !account_exists(&domain, &account_key.account)? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    let refs = domain
        .state()
        .utxos_by_stake(&account_key.address.to_vec())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    // chain position of each utxo's tx: Blockfrost orders assets by the
    // position of the oldest (asc) or newest (desc) utxo holding them
    let mut block_meta = BlockMetaResolver::new(domain.query());
    let block_deps = block_meta
        .resolve_batch(utxos.keys().map(|txo_ref| txo_ref.0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let by_unit = mapping::aggregate_account_assets(&utxos, &block_deps)?;

    let mut entries: Vec<(String, mapping::AssetAggregate)> = by_unit.into_iter().collect();

    // mirrors the Blockfrost ordering: asc by the oldest utxo holding the
    // asset, desc by the newest (deliberately not reverses of each other).
    // ties resolve unit-ascending: entries iterate out of the BTreeMap in
    // unit order and sort_by is stable, so equal positions keep it
    match pagination.order {
        Order::Asc => entries.sort_by(|x, y| x.1.oldest.cmp(&y.1.oldest)),
        Order::Desc => entries.sort_by(|x, y| y.1.newest.cmp(&x.1.newest)),
    }

    let assets = entries
        .into_iter()
        .skip(pagination.skip())
        .take(pagination.count)
        .map(|(unit, agg)| AccountAddressesAssetsInner {
            unit,
            quantity: agg.quantity.to_string(),
        })
        .collect();

    Ok(Json(assets))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        routes::accounts::testing::*,
        test_support::{TestApp, TestFault},
    };
    use blockfrost_openapi::models::account_addresses_assets_inner::AccountAddressesAssetsInner;
    use dolos_core::{EraCbor, StateWriter as _, UtxoSetDelta, UtxoValue};
    use dolos_testing::{synthetic::SyntheticBlockConfig, utxo_with_value, MIN_UTXO_AMOUNT};
    use pallas::{
        crypto::hash::Hash,
        ledger::{
            addresses::Address,
            primitives::conway::{PositiveCoin, Value},
        },
    };
    use std::{
        collections::{BTreeMap, HashMap},
        sync::Arc,
    };

    fn asset_utxo(address: &str, assets: &[([u8; 28], u64)]) -> EraCbor {
        let multi: BTreeMap<_, _> = assets
            .iter()
            .map(|(policy, quantity)| {
                (
                    Hash::from(*policy),
                    BTreeMap::from_iter([(
                        pallas::codec::utils::Bytes::from(b"tok".to_vec()),
                        PositiveCoin::try_from(*quantity).unwrap(),
                    )]),
                )
            })
            .collect();

        utxo_with_value(address, Value::Multiasset(MIN_UTXO_AMOUNT, multi))
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_happy_path() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.as_str();
        let asset_unit = app.vectors().asset_unit.clone();

        let path = format!("/accounts/{stake_address}/addresses/assets");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let assets: Vec<AccountAddressesAssetsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account assets");

        let entry = assets
            .iter()
            .find(|a| a.unit == asset_unit)
            .expect("fixture asset missing from account assets");

        // cross-check the aggregate against the per-utxo amounts served by
        // the utxos endpoint for the same account
        let (status, bytes) = app
            .get_bytes(&format!("/accounts/{stake_address}/utxos?count=100"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let utxos: Vec<
            blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner,
        > = serde_json::from_slice(&bytes).expect("failed to parse account utxos");

        let expected: u128 = utxos
            .iter()
            .flat_map(|u| u.amount.iter())
            .filter(|a| a.unit == asset_unit)
            .map(|a| a.quantity.parse::<u128>().expect("bad quantity"))
            .sum();

        assert!(expected > 0, "fixture must hold the asset in some utxo");
        assert_eq!(entry.quantity, expected.to_string());
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_bad_request() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses/assets", invalid_stake_address());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_not_found() {
        let app = TestApp::new();
        let path = format!("/accounts/{}/addresses/assets", missing_stake_address());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let stake_address = app.vectors().stake_address.as_str();
        let path = format!("/accounts/{stake_address}/addresses/assets");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn accounts_by_stake_addresses_assets_orders_by_oldest_and_newest_utxos() {
        const ASSET_A: [u8; 28] = [0xAA; 28];
        const ASSET_B: [u8; 28] = [0xBB; 28];
        const ASSET_C: [u8; 28] = [0xCC; 28];

        let fixture_config = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            ..Default::default()
        };
        let app = TestApp::new_with_cfg_and_setup(fixture_config, |domain, vectors| {
            let stake_address = Address::from_bech32(&vectors.stake_address)
                .expect("invalid fixture stake address")
                .to_vec();
            let mut ordered_refs: Vec<_> = domain
                .state()
                .utxos_by_stake(&stake_address)
                .expect("failed to load fixture utxos")
                .into_iter()
                .collect();
            ordered_refs.sort_by_key(|txo_ref| {
                let (block_number, tx_index) = vectors.tx_position(&txo_ref.0.to_string());
                (block_number, tx_index, txo_ref.1)
            });
            assert_eq!(ordered_refs.len(), 3, "fixture needs three ordered utxos");

            let oldest_ref = ordered_refs[0].clone();
            let middle_ref = ordered_refs[1].clone();
            let newest_ref = ordered_refs[2].clone();

            // Three distinct assets are distributed across three UTxOs:
            // oldest UTxO: 5 units of ASSET_A
            // middle UTxO: 3 units of ASSET_B
            // newest UTxO: 7 units of ASSET_A and 1 unit of ASSET_C
            //
            // ASSET_A sorts first ascending because its oldest occurrence is
            // first, and first descending because its newest occurrence is
            // last.
            let replaced = [
                (oldest_ref, asset_utxo(&vectors.address, &[(ASSET_A, 5)])),
                (middle_ref, asset_utxo(&vectors.address, &[(ASSET_B, 3)])),
                (
                    newest_ref,
                    asset_utxo(&vectors.address, &[(ASSET_A, 7), (ASSET_C, 1)]),
                ),
            ];

            // the replaced UTxOs keep the position their tags are ordered by
            let current = domain
                .state()
                .get_utxos(ordered_refs.clone())
                .expect("failed to load fixture utxos");
            let produced_utxo = replaced
                .into_iter()
                .map(|(txo_ref, output)| {
                    let position = current[&txo_ref].position();
                    (txo_ref, Arc::new(UtxoValue::new(output, position)))
                })
                .collect::<HashMap<_, _>>();

            let writer = domain
                .state()
                .start_writer()
                .expect("failed to write state");
            writer
                .apply_utxoset(&UtxoSetDelta {
                    produced_utxo,
                    ..Default::default()
                })
                .expect("failed to replace fixture utxos");
            writer.commit().expect("failed to commit fixture utxos");
        });

        let stake_address = app.vectors().stake_address.as_str();
        let extract_units = |items: Vec<AccountAddressesAssetsInner>| {
            items.into_iter().map(|item| item.unit).collect::<Vec<_>>()
        };
        let asset_a_unit = asset_unit(ASSET_A);
        let asset_b_unit = asset_unit(ASSET_B);
        let asset_c_unit = asset_unit(ASSET_C);

        let (status, bytes) = app
            .get_bytes(&format!(
                "/accounts/{stake_address}/addresses/assets?order=asc"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ascending_assets: Vec<AccountAddressesAssetsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asc assets");
        assert_eq!(ascending_assets[0].quantity, "12");
        let ascending_units = extract_units(ascending_assets);

        let (status, bytes) = app
            .get_bytes(&format!(
                "/accounts/{stake_address}/addresses/assets?order=desc"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let descending_units =
            extract_units(serde_json::from_slice(&bytes).expect("failed to parse desc assets"));

        // Ascending uses each asset's oldest UTxO.
        assert_eq!(
            ascending_units,
            vec![
                asset_a_unit.clone(),
                asset_b_unit.clone(),
                asset_c_unit.clone(),
            ]
        );
        // Descending uses each asset's newest UTxO. ASSET_A and ASSET_C share
        // that position, so unit order breaks the tie.
        assert_eq!(
            descending_units,
            vec![asset_a_unit, asset_c_unit, asset_b_unit.clone()]
        );

        let (status, bytes) = app
            .get_bytes(&format!(
                "/accounts/{stake_address}/addresses/assets?order=asc&count=1&page=2"
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let second_page: Vec<AccountAddressesAssetsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse second assets page");
        assert_eq!(extract_units(second_page), vec![asset_b_unit]);
    }
}
