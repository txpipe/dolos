use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{archive::Skippable as _, ArchiveStore as _, Domain};
use futures::future::try_join_all;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{
    build_block_model, genesis, load_block_by_hash_or_number, parse_hash_or_number, tip_block,
};

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

        // key-only skip, no block data read
        iter.skip_backward(from);

        iter.rev()
            .take(actual_count)
            .map(|(_, body)| body)
            .collect::<Vec<_>>()
    } else {
        Vec::new()
    };

    drop(curr);

    let tip = tip_block(&domain)?;
    let chain = domain.get_chain_summary()?;

    let futures = bodies
        .iter()
        .map(|body| build_block_model(&domain, body, &tip, &chain));
    let mut output = try_join_all(futures).await?;

    let mut block_0 = genesis::genesis_block(&domain).map_err(Error::Code)?;

    if let Some(_genesis) = block_0.as_ref() {
        for block in output.iter_mut() {
            genesis::set_genesis_previous_block(&domain, block);
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn blocks_previous_happy_path() {
        let app = TestApp::new();
        let vectors = &app.vectors().blocks;
        let last = vectors.last().expect("missing block vectors");

        let blocks = get_blocks(&app, &format!("/blocks/{}/previous", last.block_hash)).await;

        // genesis first, then every block before the current one, ascending
        let mut expected = vec![GENESIS_HASH];
        expected.extend(
            vectors[..vectors.len() - 1]
                .iter()
                .map(|b| b.block_hash.as_str()),
        );
        assert_eq!(hashes(&blocks), expected);
    }

    #[tokio::test]
    async fn blocks_previous_order_desc() {
        let app = TestApp::new();
        let vectors = &app.vectors().blocks;
        let last = vectors.last().expect("missing block vectors");

        let asc = get_blocks(&app, &format!("/blocks/{}/previous", last.block_hash)).await;
        let desc = get_blocks(
            &app,
            &format!("/blocks/{}/previous?order=desc", last.block_hash),
        )
        .await;

        let mut reversed = asc;
        reversed.reverse();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn blocks_previous_paginated() {
        let app = TestApp::new();
        let vectors = &app.vectors().blocks;
        let last = vectors.last().expect("missing block vectors");

        // page 2 of size 1 is the block two before the current one
        let page = get_blocks(
            &app,
            &format!("/blocks/{}/previous?count=1&page=2", last.block_hash),
        )
        .await;
        assert_eq!(
            hashes(&page),
            vec![vectors[vectors.len() - 3].block_hash.as_str()]
        );
    }

    #[tokio::test]
    async fn blocks_previous_of_first_block_is_genesis() {
        let app = TestApp::new();
        let first = app.vectors().blocks.first().expect("missing block vectors");

        let blocks = get_blocks(&app, &format!("/blocks/{}/previous", first.block_hash)).await;
        assert_eq!(hashes(&blocks), vec![GENESIS_HASH]);
    }

    #[tokio::test]
    async fn blocks_previous_bad_request() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/previous", invalid_block());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn blocks_previous_not_found() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/previous", missing_block());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_previous_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(
            &app,
            "/blocks/1/previous",
            StatusCode::INTERNAL_SERVER_ERROR,
        )
        .await;
    }
}
