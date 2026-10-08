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
    mapping::blocks::decode_block_header,
    pagination::{Order, Pagination, PaginationParameters},
    routes::pools::parse_pool_id_bounded,
    Facade,
};

use super::{current_epoch, parse_epoch_integer, epoch_slot_range};

pub async fn by_number_blocks_pool<D: Domain>(
    Path((number, pool_id)): Path<(String, String)>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    Option<PoolState>: From<D::Entity>,
{
    let number = parse_epoch_integer(&number)?;
    let pagination = Pagination::try_from(params)?;
    let epoch = number.in_range().ok_or(Error::InvalidEpochNumber)?;

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
}
