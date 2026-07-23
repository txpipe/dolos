use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    block_content::BlockContent, block_content_addresses_inner::BlockContentAddressesInner,
};
use dolos_cardano::ChainSummary;
use dolos_core::{archive::Skippable as _, ArchiveStore as _, BlockBody, Domain};
use futures::future::try_join_all;
use itertools::Either;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    hacks,
    mapping::{BlockModelBuilder, IntoModel as _},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

type HashOrNumber = Either<Vec<u8>, u64>;

fn parse_hash_or_number(hash_or_number: &str) -> Result<HashOrNumber, Error> {
    if hash_or_number.is_empty() {
        return Err(Error::InvalidBlockHash);
    }

    if hash_or_number.chars().all(|c| c.is_numeric() || c == '-') {
        let number = hash_or_number
            .parse()
            .map_err(|_| Error::InvalidBlockNumber)?;

        Ok(Either::Right(number))
    } else {
        let hash = hex::decode(hash_or_number).map_err(|_| Error::InvalidBlockHash)?;

        Ok(Either::Left(hash))
    }
}

async fn load_block_by_hash_or_number<D>(
    domain: &Facade<D>,
    hash_or_number: &HashOrNumber,
) -> Result<BlockBody, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    match hash_or_number {
        Either::Left(hash) => Ok(domain
            .query()
            .block_by_hash(hash.clone())
            .await
            .map_err(|_| Error::InvalidBlockHash)?
            .ok_or(StatusCode::NOT_FOUND)?),
        Either::Right(number) => {
            let (tip, _) = domain
                .archive()
                .get_tip()
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

            if *number > tip {
                return Err(Error::InvalidBlockNumber);
            }

            Ok(domain
                .query()
                .block_by_number(*number)
                .await
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
                .ok_or(StatusCode::NOT_FOUND)?)
        }
    }
}

async fn build_block_model<D>(
    domain: &Facade<D>,
    block: &BlockBody,
    tip: &BlockBody,
    chain: &ChainSummary,
) -> Result<BlockContent, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut builder = BlockModelBuilder::new(block)?;

    let previous_hash = builder.previous_hash();

    let maybe_previous = if let Some(prev_hash) = previous_hash {
        domain
            .query()
            .block_by_hash(prev_hash.as_ref().to_vec())
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    } else {
        None
    };

    if let Some(previous) = maybe_previous.as_ref() {
        builder = builder.with_previous(previous)?;
    }

    let maybe_next = domain
        .query()
        .block_by_number(builder.next_number())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if let Some(next) = maybe_next.as_ref() {
        builder = builder.with_next(next)?;
    }

    builder = builder.with_tip(tip)?;

    builder = builder.with_chain(chain);

    builder.into_model()
}

pub async fn by_hash_or_number<D>(
    Path(hash_or_number): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;

    let block = match load_block_by_hash_or_number(&domain, &hash_or_number).await {
        Ok(block) => block,
        Err(Error::Code(StatusCode::NOT_FOUND)) => {
            if Either::Right(0) == hash_or_number {
                if let Some(block) =
                    hacks::genesis_block_for_domain(&domain).map_err(Error::Code)?
                {
                    return Ok(Json(block));
                }
            }

            if let Either::Left(hash) = &hash_or_number {
                if hacks::is_genesis_hash_for_domain(&domain, hash).map_err(Error::Code)? {
                    if let Some(block) =
                        hacks::genesis_block_for_domain(&domain).map_err(Error::Code)?
                    {
                        return Ok(Json(block));
                    }
                }
            }

            return Err(Error::Code(StatusCode::NOT_FOUND));
        }
        Err(e) => return Err(e),
    };

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let mut model = build_block_model(&domain, &block, &tip, &chain).await?;
    hacks::maybe_set_genesis_previous_block(&domain, &mut model);

    Ok(Json(model))
}

pub async fn by_hash_or_number_previous<D>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContent>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
    let curr = load_block_by_hash_or_number(&domain, &hash_or_number).await?;

    let curr = MultiEraBlock::decode(&curr).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let curr_slot = curr.slot();
    let curr_number = curr.number() as usize;

    let from = pagination.from();
    let actual_count = curr_number.saturating_sub(from).min(pagination.count);

    let bodies = if actual_count > 0 {
        let mut iter = domain
            .archive()
            .get_range(None, Some(curr_slot))
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

        // Skip past pages we don't need using key-only traversal (no block data read).
        iter.skip_backward(from);

        iter.rev()
            .take(actual_count)
            .map(|(_, body)| body)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    drop(curr);

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let futures = bodies
        .iter()
        .map(|body| build_block_model(&domain, body, &tip, &chain));
    let mut output = try_join_all(futures).await?;

    let mut block_0 = hacks::genesis_block_for_domain(&domain).map_err(Error::Code)?;

    if let Some(_genesis) = block_0.as_ref() {
        for block in output.iter_mut() {
            hacks::maybe_set_genesis_previous_block(&domain, block);
        }

        let to = from.saturating_add(pagination.count);
        let genesis_index = curr_number;
        let genesis_in_range = from <= genesis_index && genesis_index < to;

        if genesis_in_range {
            if let Some(genesis) = block_0.take() {
                output.push(genesis);
            }
        }
    }

    let output = match pagination.order {
        Order::Asc => output.into_iter().rev().collect(),
        Order::Desc => output,
    };

    Ok(Json(output))
}

pub async fn by_hash_or_number_next<D>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContent>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
    let is_genesis = match &hash_or_number {
        Either::Left(hash) => {
            hacks::is_genesis_hash_for_domain(&domain, hash).map_err(Error::Code)?
        }
        _ => false,
    };

    let bodies = if is_genesis {
        let mut iterator = domain
            .archive()
            .get_range(None, None)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

        // Skip past pages we don't need using key-only traversal (no block data read).
        iterator.skip_forward(pagination.from());

        iterator
            .take(pagination.count)
            .map(|(_, body)| body)
            .collect::<Vec<_>>()
    } else {
        let curr = load_block_by_hash_or_number(&domain, &hash_or_number).await?;

        let curr = MultiEraBlock::decode(&curr).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let bodies = {
            let mut iterator = domain
                .archive()
                .get_range(Some(curr.slot()), None)
                .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

            // Discard first block (the reference block itself).
            iterator.skip_forward(1);

            // Skip past pages we don't need using key-only traversal (no block data read).
            iterator.skip_forward(pagination.from());

            iterator
                .take(pagination.count)
                .map(|(_, body)| body)
                .collect::<Vec<_>>()
        };

        drop(curr);

        bodies
    };

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let futures = bodies
        .iter()
        .map(|body| build_block_model(&domain, body, &tip, &chain));
    let mut output = try_join_all(futures).await?;
    if is_genesis {
        for block in output.iter_mut() {
            hacks::maybe_set_genesis_previous_block(&domain, block);
        }
    }
    let output = match pagination.order {
        Order::Asc => output,
        Order::Desc => output.into_iter().rev().collect(),
    };

    Ok(Json(output))
}

pub async fn latest<D>(State(domain): State<Facade<D>>) -> Result<Json<BlockContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let mut model = build_block_model(&domain, &tip, &tip, &chain).await?;
    hacks::maybe_set_genesis_previous_block(&domain, &mut model);

    Ok(Json(model))
}

pub async fn by_slot<D>(
    Path(slot_number): Path<u64>,
    State(domain): State<Facade<D>>,
) -> Result<Json<BlockContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let block = domain
        .archive()
        .get_block_by_slot(&slot_number)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let chain = domain.get_chain_summary()?;

    let mut model = build_block_model(&domain, &block, &tip, &chain).await?;
    hacks::maybe_set_genesis_previous_block(&domain, &mut model);

    Ok(Json(model))
}

pub async fn by_hash_or_number_txs<D>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
    let block = load_block_by_hash_or_number(&domain, &hash_or_number).await?;

    let model = BlockModelBuilder::new(&block)?;

    let txs: Vec<String> = model.into_model()?;
    let txs = match pagination.order {
        Order::Asc => txs,
        Order::Desc => txs.into_iter().rev().collect(),
    };

    Ok(Json(
        txs.into_iter()
            .skip(pagination.skip())
            .take(pagination.count)
            .collect(),
    ))
}

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

    let mut builder = BlockModelBuilder::new(&block)?;

    let deps = builder.required_input_deps();
    let deps = domain.get_tx_batch(deps).await?;

    // deps missing from the archive (possible on nodes without full history)
    // are skipped, omitting their addresses from the response — the same
    // graceful degradation as /txs/{hash}/utxos
    for (key, cbor) in deps.iter() {
        if let Some(cbor) = cbor {
            builder.load_dep(*key, cbor)?;
        }
    }

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

pub async fn latest_txs<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let (_, tip) = domain
        .archive()
        .get_tip()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let model = BlockModelBuilder::new(&tip)?;

    let txs: Vec<String> = model.into_model()?;
    let txs = match pagination.order {
        Order::Asc => txs,
        Order::Desc => txs.into_iter().rev().collect(),
    };

    Ok(Json(
        txs.into_iter()
            .skip(pagination.skip())
            .take(pagination.count)
            .collect(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::block_content::BlockContent;

    fn invalid_block() -> &'static str {
        "not-a-hash"
    }

    fn missing_block() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
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
    async fn blocks_latest_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/blocks/latest").await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: BlockContent =
            serde_json::from_slice(&bytes).expect("failed to parse block content");
    }

    #[tokio::test]
    async fn blocks_latest_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/blocks/latest", StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_happy_path() {
        let app = TestApp::new();
        let block_hash = app.vectors().block_hash.as_str();
        let path = format!("/blocks/{block_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: BlockContent =
            serde_json::from_slice(&bytes).expect("failed to parse block content");
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_bad_request() {
        let app = TestApp::new();
        let path = format!("/blocks/{}", invalid_block());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_not_found() {
        let app = TestApp::new();
        let path = format!("/blocks/{}", missing_block());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = "/blocks/1".to_string();
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_order_asc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/blocks/{}/txs?order=asc", block.block_hash);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asc txs");

        assert_eq!(txs, block.tx_hashes);
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_order_desc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/blocks/{}/txs?order=desc", block.block_hash);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse desc txs");

        let mut reversed = block.tx_hashes.clone();
        reversed.reverse();
        assert_eq!(txs, reversed);
    }

    #[tokio::test]
    async fn blocks_latest_txs_order_asc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs?order=asc").await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asc txs");

        assert_eq!(txs, block.tx_hashes);
    }

    #[tokio::test]
    async fn blocks_latest_txs_order_desc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs?order=desc").await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse desc txs");

        let mut reversed = block.tx_hashes.clone();
        reversed.reverse();
        assert_eq!(txs, reversed);
    }

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

    /// Exercises the `produces_at` edge for input-side address resolution.
    /// Invalid script txs don't produce their regular outputs, but they can
    /// produce a collateral return at the next output index. When a later tx
    /// spends that collateral return, block-address mapping must resolve the
    /// collateral-return address from the dependency tx CBOR.
    #[test]
    fn block_addresses_resolves_spent_collateral_return_address() {
        use dolos_core::EraCbor;
        use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
        use pallas::ledger::traverse::{Era, MultiEraBlock, MultiEraTx};

        // Invalid Conway tx with no regular outputs and one collateral-return
        // output at index 0.
        let dep_tx_cbor = hex::decode(
            "84a600d9010281825820010101010101010101010101010101010101010101010101010101010101010100018002070dd901028182582002020202020202020202020202020202020202020202020202020202020202020010a200581d607393621349f3555f70c975392c84879ba400d08d14ab3d572d7ece80011a0016e360111a0007a120a0f4f6",
        )
        .expect("invalid dep tx fixture hex");
        let collateral_return_index = 0usize;

        let dep_tx =
            MultiEraTx::decode_for_era(Era::Conway, &dep_tx_cbor).expect("failed to decode dep tx");
        assert!(
            dep_tx.output_at(collateral_return_index).is_none(),
            "regular outputs do not include collateral return index"
        );
        let collateral_return_address = dep_tx
            .produces_at(collateral_return_index)
            .expect("collateral return must be produced at index 0")
            .address()
            .expect("collateral return must have an address")
            .to_string();

        let (blocks, _, _) = build_synthetic_blocks(SyntheticBlockConfig::default());
        let raw = blocks.first().expect("missing synthetic block");

        let block = MultiEraBlock::decode(raw).expect("failed to decode block");
        let txs = block.txs();
        let spender = txs.get(1).expect("fixture needs a second tx");
        let consumed = spender.consumes();
        let input = consumed.first().expect("spender must consume");
        assert_eq!(input.index() as usize, collateral_return_index);

        let mut builder = BlockModelBuilder::new(raw).expect("failed to build block model");
        let dep_cbor = EraCbor(Era::Conway.into(), dep_tx_cbor);
        builder
            .load_dep(*input.hash(), &dep_cbor)
            .expect("failed to load dep");
        let addresses: Vec<BlockContentAddressesInner> =
            builder.into_model().expect("failed to map block addresses");

        let entry = addresses
            .iter()
            .find(|entry| entry.address == collateral_return_address)
            .expect("collateral-return address missing from response");

        assert!(
            entry
                .transactions
                .iter()
                .any(|tx| tx.tx_hash == spender.hash().to_string()),
            "spender tx must be attributed to the consumed collateral-return address"
        );
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
