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
    build_block_model, genesis, load_block_by_hash_or_number, parse_hash_or_number,
    single_block_content, tip_block,
};

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
                if let Some(block) = genesis::genesis_block(&domain).map_err(Error::Code)? {
                    return Ok(Json(block));
                }
            }

            if let Either::Left(hash) = &hash_or_number {
                if genesis::is_genesis_hash(&domain, hash).map_err(Error::Code)? {
                    if let Some(block) = genesis::genesis_block(&domain).map_err(Error::Code)? {
                        return Ok(Json(block));
                    }
                }
            }

            return Err(Error::Code(StatusCode::NOT_FOUND));
        }
        Err(e) => return Err(e),
    };

    let chain = domain.get_chain_summary()?;

    let model = single_block_content(&domain, &block, &chain).await?;

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
    use crate::routes::blocks::testing::{assert_status, invalid_block, missing_block};
    use crate::test_support::{TestApp, TestFault};

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
}
