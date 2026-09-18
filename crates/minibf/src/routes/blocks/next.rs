use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{archive::Skippable as _, ArchiveStore as _, Domain};
use futures::future::try_join_all;
use itertools::Either;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

use super::{
    build_block_model, genesis, load_block_by_hash_or_number, parse_hash_or_number, tip_block,
};

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
        Either::Left(hash) => genesis::is_genesis_hash(&domain, hash).map_err(Error::Code)?,
        _ => false,
    };

    let bodies = if is_genesis {
        let mut iterator = domain
            .archive()
            .get_range(None, None)
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;

        // key-only skip, no block data read
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

            // drop the reference block itself, then key-only skip
            iterator.skip_forward(1);
            iterator.skip_forward(pagination.from());

            iterator
                .take(pagination.count)
                .map(|(_, body)| body)
                .collect::<Vec<_>>()
        };

        drop(curr);

        bodies
    };

    let tip = tip_block(&domain)?;
    let chain = domain.get_chain_summary()?;

    let futures = bodies
        .iter()
        .map(|body| build_block_model(&domain, body, &tip, &chain));
    let mut output = try_join_all(futures).await?;
    if is_genesis {
        for block in output.iter_mut() {
            genesis::set_genesis_previous_block(&domain, block);
        }
    }
    let output = match pagination.order {
        Order::Asc => output,
        Order::Desc => output.into_iter().rev().collect(),
    };

    Ok(Json(output))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::blocks::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn blocks_next_happy_path() {
        let app = TestApp::new();
        let vectors = &app.vectors().blocks;
        let first = vectors.first().expect("missing block vectors");

        let blocks = get_blocks(&app, &format!("/blocks/{}/next", first.block_hash)).await;

        let expected: Vec<&str> = vectors[1..].iter().map(|b| b.block_hash.as_str()).collect();
        assert_eq!(hashes(&blocks), expected);
    }

    #[tokio::test]
    async fn blocks_next_order_desc() {
        let app = TestApp::new();
        let first = app.vectors().blocks.first().expect("missing block vectors");

        let asc = get_blocks(&app, &format!("/blocks/{}/next", first.block_hash)).await;
        let desc = get_blocks(
            &app,
            &format!("/blocks/{}/next?order=desc", first.block_hash),
        )
        .await;

        let mut reversed = asc;
        reversed.reverse();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn blocks_next_paginated() {
        let app = TestApp::new();
        let vectors = &app.vectors().blocks;
        let first = vectors.first().expect("missing block vectors");

        let page = get_blocks(
            &app,
            &format!("/blocks/{}/next?count=1&page=2", first.block_hash),
        )
        .await;
        assert_eq!(hashes(&page), vec![vectors[2].block_hash.as_str()]);
    }

    #[tokio::test]
    async fn blocks_next_of_genesis_starts_at_first_block() {
        let app = TestApp::new();
        let vectors = &app.vectors().blocks;

        let blocks = get_blocks(&app, &format!("/blocks/{GENESIS_HASH}/next")).await;

        let expected: Vec<&str> = vectors.iter().map(|b| b.block_hash.as_str()).collect();
        assert_eq!(hashes(&blocks), expected);
    }

    #[tokio::test]
    async fn blocks_next_of_tip_is_empty() {
        let app = TestApp::new();
        let last = app.vectors().blocks.last().expect("missing block vectors");

        let blocks = get_blocks(&app, &format!("/blocks/{}/next", last.block_hash)).await;
        assert!(blocks.is_empty());
    }

    #[tokio::test]
    async fn blocks_next_bad_request() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/next", invalid_block());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn blocks_next_not_found() {
        let app = TestApp::new();
        let path = format!("/blocks/{}/next", missing_block());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn blocks_next_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/blocks/1/next", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
