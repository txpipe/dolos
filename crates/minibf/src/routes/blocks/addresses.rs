use std::collections::BTreeSet;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content_addresses_inner::BlockContentAddressesInner;
use dolos_core::Domain;

use crate::{
    error::Error,
    inputs::{for_each_touched_output, InputDeps},
    mapping::{blocks::BlockModelBuilder, IntoModel as _},
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{load_block_by_hash_or_number, parse_hash_or_number};

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
    let block = load_block_by_hash_or_number(&domain, &hash_or_number).await?;

    let builder = BlockModelBuilder::new(&block)?;

    let mut deps = InputDeps::default();

    let mut resolver = {
        let txs = builder.txs();
        deps.prepare(&domain, txs.iter()).await?
    };

    // Collect the addresses each tx touches: its produced outputs plus the
    // source outputs of its inputs. Inputs whose source tx is missing from
    // the archive (possible on nodes without full history) are skipped,
    // omitting their addresses from the response — the same graceful
    // degradation as /txs/{hash}/utxos.
    //
    // Phase-2-failed txs are deliberately NOT skipped, diverging from
    // Blockfrost: their collateral inputs and collateral-return outputs move
    // funds, so those addresses are affected in ledger terms - considered a
    // bug in BF, not behavior worth reproducing
    let builder = builder.collect_touched_addresses_with(|tx| {
        let mut addresses = BTreeSet::new();

        for_each_touched_output(&mut resolver, tx, |output| {
            let address = output
                .address()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            addresses.insert(address.to_string());

            Ok(false)
        })?;

        Ok(addresses)
    })?;

    let addresses: Vec<BlockContentAddressesInner> = builder.into_model()?;

    // Blockfrost sorts this endpoint alphabetically by address and ignores
    // the `order` param; only count/page apply.
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

        // entries are sorted alphabetically by address
        let sorted: Vec<_> = addresses.iter().map(|a| a.address.clone()).collect();
        let mut expected = sorted.clone();
        expected.sort();
        assert_eq!(sorted, expected, "addresses must be sorted alphabetically");

        // the first tx of each fixture block pays the fixture address; its
        // entry must reference that tx exactly once even though the address
        // can appear in multiple outputs of it (dedup per address per tx)
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

        // every tx must contribute at least its own output address
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

        // Blockfrost sorts alphabetically regardless of the order param
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

    /// Feeds the builder one set of touched addresses per tx and checks the
    /// grouping: a fake address touched only by the second tx must come back
    /// listed under exactly that tx.
    #[test]
    fn block_addresses_attribute_touched_addresses() {
        use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
        use pallas::ledger::traverse::MultiEraBlock;

        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());
        let raw = blocks.first().expect("missing synthetic block");

        let block = MultiEraBlock::decode(raw).expect("failed to decode block");
        let txs = block.txs();
        // the second tx, so attribution can't pass by "always the first tx"
        let spender = txs.get(1).expect("fixture needs a second tx");
        let spender_hash = spender.hash().to_string();

        // an address only reachable through input resolution, never produced
        // by any tx of the block
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

    /// Mapping to the addresses model without collecting touched addresses
    /// first must be a 500, not a silently empty response.
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
}
