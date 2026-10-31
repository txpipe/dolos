use std::collections::{BTreeSet, HashMap};

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content_addresses_inner::BlockContentAddressesInner;
use dolos_core::Domain;
use pallas::crypto::hash::Hash;

use crate::{
    error::Error,
    inputs::{for_each_touched_output, InputDeps},
    mapping::{
        blocks::{touched_addresses_model, BlockModelBuilder},
        IntoModel as _,
    },
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{genesis, load_block_by_hash_or_number, names_genesis, parse_hash_or_number};

pub async fn by_hash_or_number_addresses<D>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContentAddressesInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;

    if names_genesis(&domain, &hash_or_number)? {
        let touched = genesis::genesis_txs(&domain.genesis())?
            .into_iter()
            .map(|tx| (tx.hash.to_string(), BTreeSet::from([tx.address])));

        return Ok(Json(
            touched_addresses_model(touched)
                .into_iter()
                .skip(pagination.skip())
                .take(pagination.count)
                .collect(),
        ));
    }

    let block = load_block_by_hash_or_number(&domain, &hash_or_number).await?;

    let builder = BlockModelBuilder::new(&block)?;

    let mut deps = InputDeps::default();

    let mut resolver = {
        let txs = builder.txs();
        deps.prepare(&domain, txs.iter()).await?
    };

    // genesis outputs, by tx hash, the first time an input misses the archive
    let mut genesis_outputs: Option<HashMap<Hash<32>, String>> = None;

    // Addresses of each tx's outputs plus the outputs it spends. A spent
    // genesis output is not in the archive, so it is read off the genesis
    // config; any other input missing from the archive is skipped, like
    // /txs/{hash}/utxos. Phase-2-failed txs stay in, as on Blockfrost.
    let builder = builder.collect_touched_addresses_with(|tx| {
        let mut addresses = BTreeSet::new();

        for_each_touched_output(&mut resolver, tx, |output| {
            let address = output
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            addresses.insert(address.to_string());

            Ok(false)
        })?;

        for input in tx.consumes() {
            if resolver.resolve(&input)?.is_some() {
                continue;
            }

            if genesis_outputs.is_none() {
                let outputs = genesis::genesis_txs(&domain.genesis())?
                    .into_iter()
                    .map(|tx| (tx.hash, tx.address))
                    .collect();

                genesis_outputs = Some(outputs);
            }

            let spent = genesis_outputs.as_ref().and_then(|x| x.get(input.hash()));

            if let Some(address) = spent {
                addresses.insert(address.clone());
            }
        }

        Ok(addresses)
    })?;

    let addresses: Vec<BlockContentAddressesInner> = builder.into_model()?;

    // sorted by address; Blockfrost ignores `order` here
    Ok(Json(
        addresses
            .into_iter()
            .skip(pagination.skip())
            .take(pagination.count)
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::{assert_status, invalid_block, missing_block};
    use crate::test_support::{TestApp, TestFault};

    async fn get_addresses(app: &TestApp, path: &str) -> Vec<BlockContentAddressesInner> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        serde_json::from_slice(&bytes).expect("failed to parse block addresses")
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_happy_path() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let address = app.vectors().address.clone();

        let path = format!("/blocks/{}/addresses", block.block_hash);
        let addresses = get_addresses(&app, &path).await;

        // sorted by address
        let sorted: Vec<_> = addresses.iter().map(|a| a.address.clone()).collect();
        let mut expected = sorted.clone();
        expected.sort();
        assert_eq!(sorted, expected, "addresses must be sorted alphabetically");

        // the first tx pays the fixture address in several outputs; one entry
        let entry = addresses
            .iter()
            .find(|a| a.address == address)
            .expect("fixture address missing from block addresses");

        let tx_hashes: Vec<_> = entry
            .transactions
            .iter()
            .map(|t| t.tx_hash.clone())
            .collect();
        assert_eq!(tx_hashes, vec![block.tx_hashes[0].clone()]);

        // every tx contributes at least one address
        assert!(addresses.len() >= block.tx_hashes.len());
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_ignores_order_param() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");

        let asc = get_addresses(
            &app,
            &format!("/blocks/{}/addresses?order=asc", block.block_hash),
        )
        .await;
        let desc = get_addresses(
            &app,
            &format!("/blocks/{}/addresses?order=desc", block.block_hash),
        )
        .await;

        // `order` has no effect
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_paginated() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");

        let all = get_addresses(&app, &format!("/blocks/{}/addresses", block.block_hash)).await;
        assert!(all.len() > 1, "fixture must produce multiple addresses");

        let page = get_addresses(
            &app,
            &format!("/blocks/{}/addresses?count=1&page=2", block.block_hash),
        )
        .await;

        assert_eq!(page, vec![all[1].clone()]);
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_past_the_end_page() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");

        let path = format!("/blocks/{}/addresses?count=100&page=99", block.block_hash);
        let addresses = get_addresses(&app, &path).await;

        assert!(
            addresses.is_empty(),
            "past-the-end page must be an empty 200"
        );
    }

    /// An address touched only by the second tx lands under that tx.
    #[test]
    fn block_addresses_attribute_touched_addresses() {
        use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
        use pallas::ledger::traverse::MultiEraBlock;

        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());
        let raw = blocks.first().expect("missing synthetic block");

        let block = MultiEraBlock::decode(raw).expect("failed to decode block");
        let txs = block.txs();
        let spender = txs.get(1).expect("fixture needs a second tx");
        let spender_hash = spender.hash().to_string();

        // never produced by the block, only reachable via inputs
        let input_side_address = "addr_input_side_only";

        let builder = BlockModelBuilder::new(raw).expect("failed to build block model");
        let addresses: Vec<BlockContentAddressesInner> = builder
            .collect_touched_addresses_with(|tx| {
                let mut touched = BTreeSet::new();

                if tx.hash().to_string() == spender_hash {
                    touched.insert(input_side_address.to_string());
                }

                Ok(touched)
            })
            .expect("failed to collect touched addresses")
            .into_model()
            .expect("failed to map block addresses");

        let entry = addresses
            .iter()
            .find(|entry| entry.address == input_side_address)
            .expect("touched address missing from response");

        let tx_hashes: Vec<_> = entry
            .transactions
            .iter()
            .map(|tx| tx.tx_hash.as_str())
            .collect();

        assert_eq!(tx_hashes, vec![spender_hash]);
    }

    /// Mapping before collecting touched addresses is a 500, not an empty list.
    #[test]
    fn block_addresses_require_touched_addresses() {
        use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};

        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());
        let raw = blocks.first().expect("missing synthetic block");

        let builder = BlockModelBuilder::new(raw).expect("failed to build block model");

        let result: Result<Vec<BlockContentAddressesInner>, StatusCode> = builder.into_model();

        assert_eq!(result, Err(StatusCode::INTERNAL_SERVER_ERROR));
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_bad_request() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/addresses", invalid_block());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_not_found() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/addresses", missing_block());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(
            &app,
            "/blocks/1/addresses",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_of_genesis() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/addresses", crate::hacks::GENESIS_HASH_PREVIEW);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let addresses: Vec<BlockContentAddressesInner> =
            serde_json::from_slice(&bytes).expect("failed to parse addresses");

        assert_eq!(addresses.len(), 8);
        assert_eq!(
            addresses[0].address,
            "FHnt4NL7yPXjpZtYj1YUiX9QYYUZGXDT9gA2PJXQFkTSMx3EgawXK5BUrCHdhe2"
        );
        assert_eq!(
            addresses[0].transactions[0].tx_hash,
            "4ceb4298a5d404ad5400513bd57f93693350ee1f499bf5b116db67a49e7e33f9"
        );
    }

    /// Checked against live Blockfrost on 2026-10-08.
    #[tokio::test]
    async fn blocks_by_hash_or_number_addresses_include_spent_genesis_outputs() {
        let (_, cbor) = include_str!("../../../testdata/preview-genesis-spend.txt")
            .lines()
            .find(|line| !line.starts_with('#'))
            .and_then(|line| line.split_once(' '))
            .unwrap();
        let block = std::sync::Arc::new(hex::decode(cbor).unwrap());

        let app =
            TestApp::new_with_archived_blocks(dolos_cardano::include::preview::load(), &[block]);

        let path =
            "/blocks/e19690d1a8ab6cab8ba342970bac8bc530a21425e011df916b1b84235217558c/addresses";
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(status, StatusCode::OK);

        let addresses: Vec<BlockContentAddressesInner> =
            serde_json::from_slice(&bytes).expect("failed to parse addresses");
        let addresses: Vec<&str> = addresses.iter().map(|x| x.address.as_str()).collect();

        assert_eq!(
            addresses,
            [
                "addr_test1vp8cprhse9pnnv7f4l3n6pj0afq2hjm6f7r2205dz0583egagfjah",
                "FHnt4NL7yPXvDWHa8bVs73UEUdJd64VxWXSFNqetECtYfTd9TtJguJ14Lu3feth",
            ]
        );
    }
}
