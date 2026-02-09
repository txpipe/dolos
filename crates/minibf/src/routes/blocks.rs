use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
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
}
