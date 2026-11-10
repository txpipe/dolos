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

use crate::{error::Error, pagination::PaginationParameters, Facade};

use super::{
    blocks_after, blocks_after_bound, build_block_model, count_and_page, genesis,
    load_block_by_hash_or_number, parse_hash_or_number, tip_block,
};

pub async fn by_hash_or_number_next<D>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<BlockContent>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = count_and_page(params)?;

    let hash_or_number = parse_hash_or_number(&hash_or_number)?;
    let is_genesis = match &hash_or_number {
        Either::Left(hash) => genesis::is_genesis_hash(&domain, hash).map_err(Error::Code)?,
        _ => false,
    };

    let curr = match is_genesis {
        true => None,
        false => Some(load_block_by_hash_or_number(&domain, &hash_or_number).await?),
    };

    let chain = domain.get_chain_summary()?;

    let bodies = {
        let curr = curr
            .as_deref()
            .map(MultiEraBlock::decode)
            .transpose()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // the tip the bound is taken against; the models read theirs after
        // the page
        let tip = tip_block(&domain)?;
        let tip = MultiEraBlock::decode(&tip).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // an impossible page is empty without a walk
        if pagination.from() >= blocks_after_bound(curr.as_ref(), &tip, &chain) {
            Vec::new()
        } else {
            let mut iterator = match &curr {
                None => domain
                    .archive()
                    .get_range(None, None)
                    .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?,
                Some(curr) => blocks_after(&domain, curr)?,
            };

            // key-only skip, no block data read
            iterator.skip_forward(pagination.from());

            iterator
                .take(pagination.count)
                .map(|(_, body)| body)
                .collect::<Vec<_>>()
        }
    };

    // read after the page: a block that rolled in during the walk is in the
    // page, so the tip its confirmations count from must not be older than it
    let tip = tip_block(&domain)?;

    let futures = bodies
        .iter()
        .map(|body| build_block_model(&domain, body, &tip, &chain));
    let output = try_join_all(futures).await?;

    // Blockfrost takes no `order` here
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
    async fn blocks_next_ignores_order() {
        let app = TestApp::new();
        let first = app.vectors().blocks.first().expect("missing block vectors");

        let asc = get_blocks(&app, &format!("/blocks/{}/next", first.block_hash)).await;
        let desc = get_blocks(
            &app,
            &format!("/blocks/{}/next?order=desc", first.block_hash),
        )
        .await;

        assert_eq!(desc, asc);

        // as is any other value, like `from` and `to`: Blockfrost reads
        // `count` and `page` here and nothing else
        for query in ["order=a", "from=abc&to=x", "from=1e3"] {
            let ignored =
                get_blocks(&app, &format!("/blocks/{}/next?{query}", first.block_hash)).await;

            assert_eq!(ignored, asc, "{query}");
        }
    }

    #[tokio::test]
    async fn blocks_next_impossible_page_is_empty() {
        let app = TestApp::new();
        let first = app.vectors().blocks.first().expect("missing block vectors");

        for id in [first.block_hash.as_str(), GENESIS_HASH] {
            let path = format!("/blocks/{id}/next?count=100&page=21474836");

            assert!(get_blocks(&app, &path).await.is_empty(), "{id}");
        }
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

    #[tokio::test]
    async fn blocks_next_steps_over_boundary_siblings() {
        let chain = mainnet_byron_chain(true);
        let main = |slot: u64| chain.main[&slot].hash.as_str();
        let next = |id: &str| format!("/blocks/{id}/next?count=2");

        // the main block sharing slot 0 with boundary block 0 is not its own next
        let blocks = get_blocks(&chain.app, &next(main(0))).await;
        assert_eq!(hashes(&blocks), [main(1), main(21598)]);

        // the last block of epoch 0 is followed by boundary block 1
        let blocks = get_blocks(&chain.app, &next(main(21599))).await;
        assert_eq!(hashes(&blocks), [chain.ebb_1.hash.as_str(), main(21600)]);
        assert_eq!(blocks[0].next_block.as_deref(), Some(main(21600)));

        let block = get_block(&chain.app, &format!("/blocks/{}", main(21599))).await;
        assert_eq!(block.next_block.as_deref(), Some(chain.ebb_1.hash.as_str()));

        let genesis = crate::hacks::GENESIS_HASH_MAINNET;
        let blocks = get_blocks(&chain.app, &next(genesis)).await;
        assert_eq!(hashes(&blocks), [chain.ebb_0.hash.as_str(), main(0)]);
    }
}
