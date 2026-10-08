use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::block_content::BlockContent;
use dolos_core::{archive::Skippable as _, Domain};
use futures::future::try_join_all;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{
    blocks_before, build_block_model, genesis, load_block_by_hash_or_number, parse_hash_or_number,
    tip_block,
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

    // walking back, the page starts `from` blocks before the reference block
    // and the genesis block closes the chain
    let (bodies, reaches_genesis) = {
        let curr = MultiEraBlock::decode(&curr).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let mut iter = blocks_before(&domain, &curr)?;

        let from = pagination.from();

        // key-only skip; the block before the page tells whether it starts at all
        let starts = match from {
            0 => true,
            _ => {
                iter.skip_backward(from - 1);
                iter.next_back().is_some()
            }
        };

        match starts {
            true => {
                let bodies: Vec<_> = iter
                    .rev()
                    .take(pagination.count)
                    .map(|(_, body)| body)
                    .collect();
                let reaches_genesis = bodies.len() < pagination.count;

                (bodies, reaches_genesis)
            }
            false => (Vec::new(), false),
        }
    };

    let tip = tip_block(&domain)?;
    let chain = domain.get_chain_summary()?;

    let futures = bodies
        .iter()
        .map(|body| build_block_model(&domain, body, &tip, &chain));
    let mut output = try_join_all(futures).await?;

    for block in output.iter_mut() {
        genesis::set_genesis_previous_block(&domain, block);
    }

    if reaches_genesis {
        if let Some(genesis) = genesis::genesis_block(&domain).map_err(Error::Code)? {
            output.push(genesis);
        }
    }

    // walked back, served ascending; Blockfrost takes no `order` here
    output.reverse();

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
    async fn blocks_previous_ignores_order() {
        let app = TestApp::new();
        let vectors = &app.vectors().blocks;
        let last = vectors.last().expect("missing block vectors");

        let asc = get_blocks(&app, &format!("/blocks/{}/previous", last.block_hash)).await;
        let desc = get_blocks(
            &app,
            &format!("/blocks/{}/previous?order=desc", last.block_hash),
        )
        .await;

        assert_eq!(desc, asc);
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

    #[tokio::test]
    async fn blocks_previous_steps_over_boundary_siblings() {
        let chain = mainnet_byron_chain(true);
        let main = |slot: u64| chain.main[&slot].hash.as_str();
        let genesis = crate::hacks::GENESIS_HASH_MAINNET;

        // boundary block 0 sits on the same slot, before the main block
        let path = format!("/blocks/{}/previous", main(0));
        let blocks = get_blocks(&chain.app, &path).await;
        assert_eq!(hashes(&blocks), [genesis, chain.ebb_0.hash.as_str()]);

        let path = format!("/blocks/{}/previous?count=2", main(21600));
        let blocks = get_blocks(&chain.app, &path).await;
        assert_eq!(hashes(&blocks), [main(21599), chain.ebb_1.hash.as_str()]);

        // pages count back from the reference block, genesis closing the chain
        let page = |page: u32| format!("/blocks/{}/previous?count=2&page={page}", main(1));
        let blocks = get_blocks(&chain.app, &page(1)).await;
        assert_eq!(hashes(&blocks), [chain.ebb_0.hash.as_str(), main(0)]);
        let blocks = get_blocks(&chain.app, &page(2)).await;
        assert_eq!(hashes(&blocks), [genesis]);
        let blocks = get_blocks(&chain.app, &page(3)).await;
        assert!(blocks.is_empty());
    }
}
