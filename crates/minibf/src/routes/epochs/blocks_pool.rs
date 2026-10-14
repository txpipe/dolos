use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use std::{iter::Peekable, ops::Range};

use dolos_cardano::{
    model::{PoolKey, PoolState},
    PoolHash,
};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
use pallas::crypto::hash::Hasher;

use crate::{
    error::Error,
    log_and_500,
    mapping::blocks::decode_block_header,
    pagination::{Order, Pagination, PaginationParameters},
    routes::pools::parse_pool_id_bounded,
    Facade,
};

use super::{current_epoch, epoch_slot_range, parse_epoch};

/// Blocks read and header-decoded per blocking task. One chunk holds its
/// permit for a few milliseconds.
const EPOCH_BLOCKS_CHUNK: usize = 256;

/// The hashes of one page of the blocks in `slots` that `issuer` minted, and
/// whether it minted any of the blocks read. The blocks are read `chunk` at a
/// time per blocking task. A chunk ends on a slot boundary, so the next one
/// resumes past the last slot read; the scan stops once the page is full.
async fn issuer_page<D: Domain>(
    domain: &Facade<D>,
    issuer: PoolHash,
    slots: Range<BlockSlot>,
    order: Order,
    skip: usize,
    count: usize,
    chunk: usize,
) -> Result<(Vec<String>, bool), StatusCode> {
    let Range { mut start, mut end } = slots;
    let chunk = chunk.max(1);
    let mut page = Vec::new();
    let mut seen = 0usize;
    let mut minted_here = false;

    while page.len() < count {
        let (hashes, last, exhausted) = domain
            .blocking(move |domain| {
                let iter = domain
                    .archive()
                    .get_range(Some(start), Some(end))
                    .map_err(log_and_500("failed to range-scan epoch blocks"))?;

                let blocks: Box<dyn Iterator<Item = (BlockSlot, Vec<u8>)>> = match order {
                    Order::Asc => Box::new(iter),
                    Order::Desc => Box::new(iter.rev()),
                };

                issuer_chunk(blocks.peekable(), issuer, chunk)
            })
            .await?;

        minted_here |= !hashes.is_empty();

        for hash in hashes {
            if seen >= skip && page.len() < count {
                page.push(hash);
            }
            seen += 1;
        }

        let Some(last) = last else {
            break;
        };

        if exhausted {
            break;
        }

        match order {
            Order::Asc => start = last + 1,
            Order::Desc => end = last,
        }
    }

    Ok((page, minted_here))
}

/// The hashes of the blocks `issuer` minted among the next `chunk` blocks, read
/// on through the last block's slot. Returns the last slot read and whether the
/// blocks ran out.
fn issuer_chunk(
    mut blocks: Peekable<impl Iterator<Item = (BlockSlot, Vec<u8>)>>,
    issuer: PoolHash,
    chunk: usize,
) -> Result<(Vec<String>, Option<BlockSlot>, bool), StatusCode> {
    let mut hashes = Vec::new();
    let mut read = 0usize;
    let mut last = None;

    while let Some((slot, body)) = blocks.next_if(|(slot, _)| read < chunk || Some(*slot) == last) {
        read += 1;
        last = Some(slot);

        let Some(header) = decode_block_header(&body)? else {
            continue;
        };

        let Some(key) = header.issuer_vkey() else {
            continue;
        };

        if Hasher::<224>::hash(key) == issuer {
            hashes.push(header.hash().to_string());
        }
    }

    let exhausted = blocks.peek().is_none();

    Ok((hashes, last, exhausted))
}

pub async fn by_number_blocks_pool<D: Domain>(
    Path((number, pool_id)): Path<(String, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    let epoch = parse_epoch(&number)?;

    let (chain, current) = current_epoch(&domain)?;

    // Blockfrost 404s epochs that don't exist yet.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }
    let Some(hash) = parse_pool_id_bounded(&pool_id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let (start, end) = epoch_slot_range(&chain, epoch);

    let (page, minted_here) = issuer_page(
        &domain,
        hash,
        start..end,
        pagination.order,
        pagination.skip(),
        pagination.count,
        EPOCH_BLOCKS_CHUNK,
    )
    .await?;

    // Blockfrost 404s a pool db-sync never saw. A pool registers before it
    // mints, so a block found here proves it as well as a `PoolState` does.
    if !minted_here && !domain.cardano_entity_exists::<PoolState>(&PoolKey(hash))? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::epochs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn epochs_blocks_pool_happy_path() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let by_pool: Vec<String> = get_ok(&app, &format!("/epochs/{epoch}/blocks/{pool}")).await;

        // The issuer pool minted every block, so the filtered list equals the
        // unfiltered sibling endpoint.
        let all: Vec<String> = get_ok(&app, &format!("/epochs/{epoch}/blocks")).await;

        assert!(!by_pool.is_empty());
        assert_eq!(by_pool, all);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let page_1: Vec<String> = get_ok(
            &app,
            &format!("/epochs/{epoch}/blocks/{pool}?count=1&page=1"),
        )
        .await;
        let page_2: Vec<String> = get_ok(
            &app,
            &format!("/epochs/{epoch}/blocks/{pool}?count=1&page=2"),
        )
        .await;

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1, page_2);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_desc_is_reversed_asc() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let asc: Vec<String> =
            get_ok(&app, &format!("/epochs/{epoch}/blocks/{pool}?order=asc")).await;
        let mut desc: Vec<String> =
            get_ok(&app, &format!("/epochs/{epoch}/blocks/{pool}?order=desc")).await;

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_registered_pool_without_blocks_is_empty() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = app.vectors().pool_id.clone();

        let hashes: Vec<String> = get_ok(&app, &format!("/epochs/{epoch}/blocks/{pool}")).await;
        assert!(hashes.is_empty());
    }

    #[tokio::test]
    async fn epochs_blocks_pool_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/epochs/0/blocks/notapool", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn epochs_blocks_pool_unknown_pool_not_found() {
        let app = TestApp::new();
        let pool = hex::encode([7u8; 28]);
        let path = format!("/epochs/{}/blocks/{pool}", app.tip_epoch());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_blocks_pool_future_epoch_not_found() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();
        let path = format!("/epochs/{}/blocks/{pool}", app.tip_epoch() + 10);
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn epochs_blocks_pool_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/epochs/0/blocks/{}", toy_issuer_pool());
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn chunked_issuer_pages_match_unchunked() {
        let app = TestApp::new();
        let facade = app.facade();
        let issuer = Hasher::<224>::hash(&[0x10, 0x11]);
        let slots = 0..facade.get_tip_slot().expect("tip") + 1;

        let (all, minted) = issuer_page(
            &facade,
            issuer,
            slots.clone(),
            Order::Asc,
            0,
            100,
            usize::MAX,
        )
        .await
        .expect("issuer page");
        assert!(
            minted && all.len() > 2,
            "too few blocks to split into chunks"
        );

        for order in [Order::Asc, Order::Desc] {
            for (skip, count) in [(0, 100), (1, 2), (2, 1), (3, 5), (10, 5)] {
                let whole = issuer_page(
                    &facade,
                    issuer,
                    slots.clone(),
                    order,
                    skip,
                    count,
                    usize::MAX,
                )
                .await
                .expect("issuer page");

                for chunk in [1, 2] {
                    let chunked =
                        issuer_page(&facade, issuer, slots.clone(), order, skip, count, chunk)
                            .await
                            .expect("issuer page");
                    assert_eq!(chunked, whole, "{order:?}, skip {skip}, count {count}");
                }
            }
        }
    }
}
