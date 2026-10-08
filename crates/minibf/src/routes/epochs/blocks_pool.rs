use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use dolos_cardano::model::PoolState;
use dolos_core::{ArchiveStore as _, Domain};
use pallas::crypto::hash::Hasher;

use crate::{
    error::Error,
    log_and_500,
    pagination::{Order, Pagination, PaginationParameters},
    routes::pools::parse_pool_id_bounded,
    Facade,
};

use super::{current_epoch, decode_block_header, ensure_epoch_in_range, epoch_slot_range};

pub async fn by_number_blocks_pool<D: Domain>(
    Path((epoch, pool_id)): Path<(u64, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    ensure_epoch_in_range(epoch)?;

    let (chain, current) = current_epoch(&domain)?;

    // Blockfrost 404s epochs that don't exist yet.
    if epoch > current {
        return Err(StatusCode::NOT_FOUND.into());
    }
    let Some(hash) = parse_pool_id_bounded(&pool_id)? else {
        return Err(StatusCode::NOT_FOUND.into());
    };

    let (start, end) = epoch_slot_range(&chain, epoch);

    let inner = domain.inner.clone();
    let issuer = hash;
    let skip = pagination.skip();
    let count = pagination.count;
    let order = pagination.order;

    let (page, minted_here) =
        tokio::task::spawn_blocking(move || -> Result<(Vec<String>, bool), StatusCode> {
            let iter = inner
                .archive()
                .get_range(Some(start), Some(end))
                .map_err(log_and_500("failed to range-scan epoch blocks"))?;

            let scan = |blocks: &mut dyn Iterator<Item = (u64, Vec<u8>)>| {
                let mut minted_here = false;
                let mut page = Vec::new();
                let mut seen = 0usize;

                for (_slot, body) in blocks {
                    let Some(header) = decode_block_header(&body)? else {
                        continue;
                    };

                    let Some(key) = header.issuer_vkey() else {
                        continue;
                    };

                    if Hasher::<224>::hash(key) != issuer {
                        continue;
                    }

                    minted_here = true;

                    if seen >= skip && page.len() < count {
                        page.push(header.hash().to_string());
                    }

                    seen += 1;

                    if page.len() >= count {
                        break;
                    }
                }

                Ok::<_, StatusCode>((page, minted_here))
            };

            match order {
                Order::Asc => {
                    let mut blocks = iter.into_iter();
                    scan(&mut blocks)
                }
                Order::Desc => {
                    let mut blocks = iter.rev();
                    scan(&mut blocks)
                }
            }
        })
        .await
        .map_err(log_and_500("epoch block scan task failed"))??;

    // Blockfrost 404s a pool db-sync never saw. A pool registers before it
    // mints, so a block found here proves it as well as a `PoolState` does.
    if !minted_here && !domain.cardano_entity_exists::<PoolState>(hash)? {
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

        let (status, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}"))
            .await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let by_pool: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        // The issuer pool minted every block, so the filtered list equals the
        // unfiltered sibling endpoint.
        let (_, bytes) = app.get_bytes(&format!("/epochs/{epoch}/blocks")).await;
        let all: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse hashes");

        assert!(!by_pool.is_empty());
        assert_eq!(by_pool, all);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_paginated() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?count=1&page=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?count=1&page=2"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<String> = serde_json::from_slice(&bytes_1).unwrap();
        let page_2: Vec<String> = serde_json::from_slice(&bytes_2).unwrap();

        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);
        assert_ne!(page_1, page_2);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_desc_is_reversed_asc() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = toy_issuer_pool();

        let (_, bytes_asc) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?order=asc"))
            .await;
        let (_, bytes_desc) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}?order=desc"))
            .await;

        let asc: Vec<String> = serde_json::from_slice(&bytes_asc).unwrap();
        let mut desc: Vec<String> = serde_json::from_slice(&bytes_desc).unwrap();

        desc.reverse();
        assert_eq!(asc, desc);
    }

    #[tokio::test]
    async fn epochs_blocks_pool_registered_pool_without_blocks_is_empty() {
        let app = TestApp::new();
        let epoch = app.tip_epoch();
        let pool = app.vectors().pool_id.clone();

        let (status, bytes) = app
            .get_bytes(&format!("/epochs/{epoch}/blocks/{pool}"))
            .await;

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
}
