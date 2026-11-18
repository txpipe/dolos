//! `/pools/{pool_id}/blocks`: the blocks a pool minted.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::{indexes::CardanoArchiveIndexExt, model::PoolState, PoolHash};
use dolos_core::{ArchiveStore as _, BlockSlot, Domain};

use crate::{
    error::Error,
    log_and_500,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{parse_pool_id_bounded, pool_exists};

/// Blocks minted by a pool, oldest first.
///
/// The `pool_blocks` archive dimension tags each block with its issuer pool,
/// so the page is a key-only slot scan plus one body read per listed block.
/// The cost does not grow with the page number, so deep pages stay cheap and
/// the scan budget does not apply.
pub async fn by_id_blocks<D>(
    Path(id): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    // Drop `from`/`to` before validation: Blockfrost never reads them here,
    // so a malformed or reversed window is ignored rather than rejected.
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;
    let Some(pool) = parse_pool_id_bounded(&id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let tip = domain.get_tip_slot()?;

    let (page_slots, minted_any) = pool_block_page_slots(
        &domain,
        pool,
        tip,
        pagination.order,
        pagination.skip(),
        pagination.count,
        POOL_BLOCKS_CHUNK,
    )
    .await?;

    let page = domain
        .blocking(move |domain| {
            let mut page = Vec::with_capacity(page_slots.len());

            for slot in page_slots {
                let body = domain
                    .archive()
                    .get_block_by_slot(&slot)
                    .map_err(log_and_500("failed to read a block of a pool"))?;

                // A tagged slot always holds a block, and a Byron block is
                // never tagged. Skip either, rather than fail the page.
                let Some(body) = body else {
                    tracing::warn!(slot, "pool blocks index points at a missing block");
                    continue;
                };

                let Some(header) = crate::mapping::blocks::decode_block_header(&body)? else {
                    tracing::warn!(slot, "pool blocks index points at a Byron block");
                    continue;
                };

                page.push(header.hash().to_string());
            }

            Ok(page)
        })
        .await?;

    // Blockfrost 404s a pool that db-sync never saw. A `PoolState` row, in
    // state or in the final-row log, covers every pool that registered on
    // chain, and a pool must register before it can mint. The index acts as a
    // second proof of existence, for any issuer that has no entity.
    if !minted_any && !pool_exists(&domain, pool).await? {
        return Err(StatusCode::NOT_FOUND.into());
    }

    Ok(Json(page))
}

/// Pool block index slots read per blocking task while a page is located.
const POOL_BLOCKS_CHUNK: usize = 4096;

/// The slots of one page of the blocks `pool` minted up to `tip`, and whether
/// the pool minted any. The index is read `chunk` slots per blocking task; a
/// pool tags at most one block per slot, so each chunk resumes past the last
/// slot it read.
async fn pool_block_page_slots<D>(
    domain: &Facade<D>,
    pool: PoolHash,
    tip: BlockSlot,
    order: crate::pagination::Order,
    skip: usize,
    count: usize,
    chunk: usize,
) -> Result<(Vec<BlockSlot>, bool), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let (mut start, mut end) = (0, tip);
    let mut seen = 0usize;
    let mut page = Vec::new();
    let mut minted_any = false;

    loop {
        // Never read past the page.
        let want = (skip + count).saturating_sub(seen).clamp(1, chunk.max(1));

        let slots = domain
            .blocking(move |domain| {
                let slots = domain
                    .archive()
                    .slots_by_pool_blocks(pool.as_slice(), start, end)
                    .map_err(log_and_500("failed to read the pool blocks index"))?;

                match order {
                    crate::pagination::Order::Asc => {
                        slots.take(want).collect::<Result<Vec<_>, _>>()
                    }
                    crate::pagination::Order::Desc => {
                        slots.rev().take(want).collect::<Result<Vec<_>, _>>()
                    }
                }
                .map_err(log_and_500("failed to page the pool blocks index"))
            })
            .await?;

        minted_any |= !slots.is_empty();

        for slot in &slots {
            if seen >= skip && page.len() < count {
                page.push(*slot);
            }
            seen += 1;
        }

        let Some(&last) = slots.last() else {
            break;
        };

        if slots.len() < want || page.len() >= count {
            break;
        }

        match order {
            crate::pagination::Order::Asc => start = last + 1,
            crate::pagination::Order::Desc if last == 0 => break,
            crate::pagination::Order::Desc => end = last - 1,
        }
    }

    Ok((page, minted_any))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        mapping::bech32_pool,
        pagination::Order,
        routes::pools::testing::{
            assert_error_message, assert_status, invalid_pool_id, wrong_length_pool_ids,
        },
        test_support::{TestApp, TestFault},
    };
    use pallas::crypto::hash::Hasher;

    /// Every synthetic block is minted by the same fixed issuer key, so the
    /// pool derived from it owns the whole toy chain.
    fn toy_issuer_pool() -> String {
        bech32_pool(Hasher::<224>::hash(&[0x10, 0x11])).expect("valid pool id")
    }

    /// The hex form of the same pool id. Blockfrost accepts either form.
    fn toy_issuer_pool_hex() -> String {
        Hasher::<224>::hash(&[0x10, 0x11]).to_string()
    }

    #[tokio::test]
    async fn pools_blocks_happy_path() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (status, bytes) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=100"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let hashes: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        assert!(!hashes.is_empty());

        for hash in &hashes {
            assert_eq!(hash.len(), 64);
            assert!(hash
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
        }

        // The toy chain sits in one epoch, so the sibling epoch endpoint
        // lists exactly the same blocks.
        let epoch = app.tip_epoch();
        let (_, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?count=100"))
            .await;
        let by_epoch: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        assert_eq!(hashes, by_epoch);
    }

    #[tokio::test]
    async fn pools_blocks_hex_id_matches_bech32() {
        let app = TestApp::new();

        let (_, bech32_bytes) = app
            .get_bytes(&format!("/pools/{}/blocks?count=100", toy_issuer_pool()))
            .await;
        let (status, hex_bytes) = app
            .get_bytes(&format!(
                "/pools/{}/blocks?count=100",
                toy_issuer_pool_hex()
            ))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&hex_bytes)
        );

        let from_bech32: Vec<String> = serde_json::from_slice(&bech32_bytes).unwrap();
        let from_hex: Vec<String> = serde_json::from_slice(&hex_bytes).unwrap();

        assert!(!from_hex.is_empty());
        assert_eq!(from_bech32, from_hex);
    }

    #[tokio::test]
    async fn pools_blocks_paginated() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=1&page=2"))
            .await;
        let (status_far, bytes_far) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=1&page=1000"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);
        assert_eq!(status_far, StatusCode::OK);

        let page_1: Vec<String> = serde_json::from_slice(&bytes_1).unwrap();
        let page_2: Vec<String> = serde_json::from_slice(&bytes_2).unwrap();
        let page_far: Vec<String> = serde_json::from_slice(&bytes_far).unwrap();

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1, page_2);
        assert!(page_far.is_empty());
    }

    #[tokio::test]
    async fn pools_blocks_desc_is_reversed_asc() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (_, asc_bytes) = app
            .get_bytes(&format!("/pools/{pool}/blocks?order=asc&count=100"))
            .await;
        let (_, desc_bytes) = app
            .get_bytes(&format!("/pools/{pool}/blocks?order=desc&count=100"))
            .await;

        let asc: Vec<String> = serde_json::from_slice(&asc_bytes).unwrap();
        let mut desc: Vec<String> = serde_json::from_slice(&desc_bytes).unwrap();

        assert!(!asc.is_empty());

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn pools_blocks_registered_pool_without_blocks_is_empty() {
        let app = TestApp::new();
        let pool = app.vectors().pool_id.clone();

        let (status, bytes) = app.get_bytes(&format!("/pools/{pool}/blocks")).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let hashes: Vec<String> = serde_json::from_slice(&bytes).unwrap();
        assert!(hashes.is_empty());
    }

    #[tokio::test]
    async fn pools_blocks_bad_request() {
        let app = TestApp::new();

        let path = format!("/pools/{}/blocks", invalid_pool_id());
        assert_error_message(&app, &path, "Invalid or malformed pool id format.").await;

        // The endpoint does the query string check before the pool ID check.
        let path = format!("/pools/{}/blocks?count=0", invalid_pool_id());
        assert_error_message(&app, &path, "querystring/count must be >= 1").await;
    }

    #[tokio::test]
    async fn pools_blocks_bad_pagination() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        assert_error_message(
            &app,
            &format!("/pools/{pool}/blocks?order=a"),
            "querystring/order must be equal to one of the allowed values",
        )
        .await;
        assert_error_message(
            &app,
            &format!("/pools/{pool}/blocks?page=0"),
            "querystring/page must be >= 1",
        )
        .await;
        assert_error_message(
            &app,
            &format!("/pools/{pool}/blocks?count=101"),
            "querystring/count must be <= 100",
        )
        .await;
    }

    /// Blockfrost does not declare `from`/`to` for this route, so a malformed
    /// or reversed window changes nothing.
    #[tokio::test]
    async fn pools_blocks_ignores_from_to() {
        let app = TestApp::new();
        let pool = toy_issuer_pool();

        let (status, plain) = app
            .get_bytes(&format!("/pools/{pool}/blocks?count=100"))
            .await;
        assert_eq!(status, StatusCode::OK);

        let expected: Vec<String> = serde_json::from_slice(&plain).unwrap();
        assert!(!expected.is_empty());

        for window in ["from=bad", "from=999999999&to=1", "from=abc&to=xyz"] {
            let (status, bytes) = app
                .get_bytes(&format!("/pools/{pool}/blocks?count=100&{window}"))
                .await;

            assert_eq!(
                status,
                StatusCode::OK,
                "unexpected status {status} for {window} with body: {}",
                String::from_utf8_lossy(&bytes)
            );

            let hashes: Vec<String> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(hashes, expected, "{window} changed the body");
        }
    }

    #[tokio::test]
    async fn pools_blocks_not_found() {
        let app = TestApp::new();
        let pool = bech32_pool([0xff; 28]).expect("valid pool id");
        let path = format!("/pools/{pool}/blocks");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;

        for pool_id in wrong_length_pool_ids(&app) {
            let path = format!("/pools/{pool_id}/blocks");
            assert_status(&app, &path, StatusCode::NOT_FOUND).await;
        }
    }

    #[tokio::test]
    async fn pools_blocks_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let path = format!("/pools/{}/blocks", toy_issuer_pool());
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn chunked_pool_block_pages_match_unchunked() {
        let app = TestApp::new();
        let facade = app.facade();
        let pool = Hasher::<224>::hash(&[0x10, 0x11]);
        let tip = facade.get_tip_slot().expect("tip");

        let whole_asc = pool_block_page_slots(&facade, pool, tip, Order::Asc, 0, 100, usize::MAX)
            .await
            .expect("slots");
        assert!(whole_asc.0.len() > 2, "too few blocks to split into chunks");

        for order in [Order::Asc, Order::Desc] {
            for (skip, count) in [(0, 100), (1, 2), (2, 1), (3, 5), (10, 5)] {
                let whole =
                    pool_block_page_slots(&facade, pool, tip, order, skip, count, usize::MAX)
                        .await
                        .expect("slots");

                for chunk in [1, 2] {
                    let chunked =
                        pool_block_page_slots(&facade, pool, tip, order, skip, count, chunk)
                            .await
                            .expect("slots");
                    assert_eq!(chunked, whole, "{order:?}, skip {skip}, count {count}");
                }
            }
        }
    }
}
