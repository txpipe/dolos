//! `/assets/{asset}/history`: every mint and burn of one asset.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset_history_inner::{Action, AssetHistoryInner};
use dolos_cardano::{
    indexes::{AsyncCardanoQueryExt, SlotOrder},
    model::AssetState,
};
use dolos_core::Domain;
use futures_util::StreamExt;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// The mint entry of `subject` in `tx`, as Blockfrost renders its
/// `ma_tx_mint` row. A phase-2-invalid tx mints nothing, and db-sync writes no
/// row for it.
fn mint_event(tx: &MultiEraTx<'_>, subject: &[u8]) -> Option<AssetHistoryInner> {
    if !tx.is_valid() {
        return None;
    }

    let (policy, name) = subject.split_at(28);

    let quantity = tx
        .mints()
        .iter()
        .filter(|x| x.policy().as_slice() == policy)
        .flat_map(|x| x.assets())
        .find(|x| x.name() == name)?
        .mint_coin()?;

    // the same split as ryo's SQL: anything below zero is a burn
    let action = if quantity < 0 {
        Action::Burned
    } else {
        Action::Minted
    };

    Some(AssetHistoryInner {
        tx_hash: hex::encode(tx.hash()),
        action,
        amount: quantity.to_string(),
    })
}

fn collect_mint_events(
    block: &[u8],
    subject: &[u8],
    order: Order,
    found: &mut Vec<AssetHistoryInner>,
) -> Result<(), StatusCode> {
    let block = MultiEraBlock::decode(block).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let txs = block.txs();

    let txs: Box<dyn Iterator<Item = &MultiEraTx<'_>>> = match order {
        Order::Asc => Box::new(txs.iter()),
        Order::Desc => Box::new(txs.iter().rev()),
    };

    found.extend(txs.filter_map(|tx| mint_event(tx, subject)));

    Ok(())
}

/// `GET /assets/{asset}/history`: the mints and burns of an asset in chain
/// order, the rows ryo reads from `ma_tx_mint`.
///
/// Every mint or burn moves the asset through an output or a spent input, so
/// the archive's asset tag finds the blocks. The asset state bounds the walk
/// on both ends: nothing happens before `initial_slot`, and `mint_tx_count`
/// says how many rows exist, so the scan stops once it has them all and a
/// page past the last one answers without a scan.
pub async fn by_subject_history<D>(
    Path(subject): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetHistoryInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    // Blockfrost does not define `from`/`to` here and ignores them
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;

    let (subject, state) = super::resolve_asset_state(&domain, &subject)?;

    let total = state.mint_tx_count as usize;

    // free to answer, so it is served even past the scan limit
    if pagination.from() >= total {
        return Ok(Json(vec![]));
    }

    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    let needed = (pagination.from() + pagination.count).min(total);

    let start_slot = state.initial_slot.unwrap_or_default();
    let end_slot = domain.get_tip_slot()?;

    let stream = domain.query().blocks_by_asset_stream(
        &subject,
        start_slot,
        end_slot,
        SlotOrder::from(pagination.order),
    );
    let mut stream = Box::pin(stream);

    let mut found = Vec::new();

    while let Some(res) = stream.next().await {
        let (_slot, block) = res.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        // a pruned block is gone from the archive along with its rows
        let Some(block) = block else {
            continue;
        };

        collect_mint_events(&block, &subject, pagination.order, &mut found)?;

        if found.len() >= needed {
            break;
        }
    }

    let page = found
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .collect();

    Ok(Json(page))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    async fn get_history(app: &TestApp, query: &str) -> Vec<AssetHistoryInner> {
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/history{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse asset history")
    }

    fn hashes(rows: &[AssetHistoryInner]) -> Vec<String> {
        rows.iter().map(|x| x.tx_hash.clone()).collect()
    }

    /// Every tx of the synthetic chain mints the asset, in chain order.
    fn chain_txs(app: &TestApp) -> Vec<String> {
        app.vectors()
            .blocks
            .iter()
            .flat_map(|block| block.tx_hashes.iter().cloned())
            .collect()
    }

    #[tokio::test]
    async fn assets_by_subject_history_happy_path() {
        let app = TestApp::new();

        let rows = get_history(&app, "").await;
        assert_eq!(hashes(&rows), chain_txs(&app));
        assert!(rows
            .iter()
            .all(|x| x.action == Action::Minted && x.amount == "1"));
    }

    #[tokio::test]
    async fn assets_by_subject_history_order_desc() {
        let app = TestApp::new();

        let rows = get_history(&app, "?order=desc").await;
        let expected = chain_txs(&app).into_iter().rev().collect_vec();
        assert_eq!(hashes(&rows), expected);
    }

    #[tokio::test]
    async fn assets_by_subject_history_paginated() {
        let app = TestApp::new();
        let txs = chain_txs(&app);

        // pages cut across block boundaries in both orders
        let rows = get_history(&app, "?page=2&count=4").await;
        assert_eq!(hashes(&rows), txs[4..8]);

        let rows = get_history(&app, "?order=desc&page=2&count=4").await;
        let desc = txs.iter().rev().cloned().collect_vec();
        assert_eq!(hashes(&rows), desc[4..8]);

        // the last page is short, the one after it is empty
        let rows = get_history(&app, "?page=4&count=4").await;
        assert_eq!(hashes(&rows), txs[12..]);

        let rows = get_history(&app, "?page=5&count=4").await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn assets_by_subject_history_burns() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            mint_amount: -3,
            ..Default::default()
        });

        let rows = get_history(&app, "").await;
        assert_eq!(hashes(&rows), chain_txs(&app));
        assert!(rows
            .iter()
            .all(|x| x.action == Action::Burned && x.amount == "-3"));
    }

    #[tokio::test]
    async fn assets_by_subject_history_skips_invalid_txs() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            invalid_txs_by_block: vec![vec![], vec![1]],
            ..Default::default()
        });

        // a phase-2 failure applies no mint, so it has no history row
        let invalid = app.vectors().blocks[1].tx_hashes[1].clone();
        let expected = chain_txs(&app)
            .into_iter()
            .filter(|x| *x != invalid)
            .collect_vec();

        let rows = get_history(&app, "").await;
        assert_eq!(hashes(&rows), expected);
    }

    #[tokio::test]
    async fn assets_by_subject_history_ignores_from_to() {
        let app = TestApp::new();
        let all = get_history(&app, "").await;

        let block = app.vectors().blocks.first().expect("missing block vectors");
        let query = format!("?from={0}&to={0}", block.block_number);
        assert_eq!(get_history(&app, &query).await, all);

        // a malformed range is ignored as well, never validated
        assert_eq!(get_history(&app, "?from=not-a-number").await, all);
    }

    #[tokio::test]
    async fn assets_by_subject_history_scan_limit() {
        let app = TestApp::new_with_scan_limit(SyntheticBlockConfig::default(), 2);
        let asset = app.vectors().asset_unit.as_str();

        assert_eq!(get_history(&app, "?count=2").await.len(), 2);

        let path = format!("/assets/{asset}/history?count=3");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;

        // a page past the last row needs no scan, so the limit does not apply
        assert!(get_history(&app, "?count=3&page=3").await.is_empty());
    }

    #[tokio::test]
    async fn assets_by_subject_history_bad_request() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        for subject in ["not-hex-asset", "abcd", &"f".repeat(122), &"z".repeat(56)] {
            let path = format!("/assets/{subject}/history");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }

        for query in ["?count=0", "?page=x", "?order=sideways"] {
            let path = format!("/assets/{asset}/history{query}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_history_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/history", "f".repeat(84));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_history_internal_error() {
        for fault in [TestFault::StateStoreError, TestFault::ArchiveStoreError] {
            let app = TestApp::new_with_fault(Some(fault));
            let asset = app.vectors().asset_unit.as_str();
            let path = format!("/assets/{asset}/history");
            assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
        }
    }
}
