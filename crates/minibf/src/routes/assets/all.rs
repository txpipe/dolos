//! `/assets`: every asset, in order of first mint.

use std::collections::BTreeSet;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::assets_inner::AssetsInner;
use dolos_cardano::model::{AssetState, FixedNamespace as _};
use dolos_core::{ArchiveStore as _, BlockBody, BlockSlot, Domain, EntityKey, StateStore as _};
use pallas::ledger::traverse::{MultiEraBlock, MultiEraTx};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// Subjects whose first mint is `tx`, in the order Blockfrost lists them.
///
/// Blockfrost sorts `/assets` by the first `ma_tx_mint` row of each asset, and
/// db-sync writes those rows per tx in `(policy, name)` order, so assets born
/// in the same tx come out sorted by subject bytes (the policy is a fixed 28
/// bytes, so a plain byte comparison of the subject is exactly that). The
/// ledger state decides what counts as a first mint: the tx must be the
/// asset's `initial_tx`, which also keeps fully burned assets on the list.
fn first_minted_subjects<D: Domain>(
    domain: &D,
    tx: &MultiEraTx<'_>,
) -> Result<Vec<Vec<u8>>, StatusCode> {
    let tx_hash = tx.hash();
    let mut subjects = BTreeSet::new();

    for policy_assets in tx.mints() {
        for asset in policy_assets.assets() {
            let subject = [policy_assets.policy().as_slice(), asset.name()].concat();
            let key = EntityKey::from(
                pallas::crypto::hash::Hasher::<256>::hash(subject.as_slice()).as_slice(),
            );

            let state = domain
                .state()
                .read_entity_typed::<AssetState>(AssetState::NS, &key)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            if state.and_then(|x| x.initial_tx) == Some(tx_hash) {
                subjects.insert(subject);
            }
        }
    }

    Ok(subjects.into_iter().collect())
}

/// Walk the archive in `order` collecting first-minted subjects until `needed`
/// of them are known. The scan covers every block from `start` to the archive
/// tip; the caller bounds the amount of work through the pagination scan
/// limit.
fn scan_first_mints<D: Domain>(
    domain: &D,
    start: BlockSlot,
    order: Order,
    needed: usize,
) -> Result<Vec<Vec<u8>>, StatusCode> {
    let iter = domain
        .archive()
        .get_range(Some(start), None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let blocks: Box<dyn Iterator<Item = (BlockSlot, BlockBody)>> = match order {
        Order::Asc => Box::new(iter),
        Order::Desc => Box::new(iter.rev()),
    };

    let mut found = Vec::new();

    for (_, body) in blocks {
        let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let txs = block.txs();

        let txs: Box<dyn Iterator<Item = &MultiEraTx<'_>>> = match order {
            Order::Asc => Box::new(txs.iter()),
            Order::Desc => Box::new(txs.iter().rev()),
        };

        for tx in txs {
            let mut minted = first_minted_subjects(domain, tx)?;

            if minted.is_empty() {
                continue;
            }

            if matches!(order, Order::Desc) {
                minted.reverse();
            }

            found.append(&mut minted);

            if found.len() >= needed {
                return Ok(found);
            }
        }
    }

    Ok(found)
}

/// `GET /assets`: every asset ever minted, ordered by its first mint tx.
///
/// The state cannot serve this directly: `AssetState` is keyed by a hash of
/// the subject and does not store the subject itself, and the archive indexes
/// keep hashed keys too, so there is no store to enumerate subjects from. The
/// listing therefore replays mints from the archive, starting where native
/// assets begin (Mary) or, for `desc`, from the tip backwards, and stops as
/// soon as the requested page is covered. Like the other scanning endpoints
/// the reachable depth is bounded by `max_scan_items`.
pub async fn all<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    // Native assets exist since Mary, so nothing before that era can appear.
    let chain = domain.get_chain_summary()?;
    let Some(mary_epoch) = chain.first_mary_epoch() else {
        return Ok(Json(vec![]));
    };
    let start = chain.epoch_start(mary_epoch);

    let order = pagination.order;
    let needed = pagination.from() + pagination.count;

    let subjects = domain
        .query()
        .run_blocking(move |domain| Ok(scan_first_mints(&domain, start, order, needed)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    let mut items = Vec::with_capacity(pagination.count);
    for subject in subjects
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
    {
        let entity_key = pallas::crypto::hash::Hasher::<256>::hash(subject.as_slice());
        let asset_state = domain
            .read_cardano_entity::<AssetState>(entity_key.as_slice())?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        items.push(AssetsInner {
            asset: hex::encode(&subject),
            quantity: asset_state.quantity().to_string(),
        });
    }

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::asset::Asset;
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    async fn get_assets(app: &TestApp, query: &str) -> Vec<AssetsInner> {
        let path = format!("/assets{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse assets")
    }

    #[tokio::test]
    async fn assets_all_happy_path() {
        // the default synthetic chain mints one asset in every tx
        let app = TestApp::new();
        let unit = app.vectors().asset_unit.clone();

        let assets = get_assets(&app, "").await;
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].asset, unit);

        // quantity is the net supply, the same number `/assets/{asset}` reports
        let (status, bytes) = app.get_bytes(&format!("/assets/{unit}")).await;
        assert_eq!(status, StatusCode::OK);
        let detail: Asset = serde_json::from_slice(&bytes).expect("failed to parse asset");
        assert_eq!(assets[0].quantity, detail.quantity);
    }

    /// Three assets, one per block, so the listing order is the mint order.
    fn three_asset_app() -> (TestApp, Vec<String>) {
        let asset_names = ["FIRST", "SECOND", "THIRD"];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: asset_names.len(),
            txs_per_block: 1,
            asset_names_by_block: asset_names.iter().map(|x| (*x).to_string()).collect(),
            ..Default::default()
        });
        let policy = app.vectors().policy_id.clone();
        let units = asset_names
            .iter()
            .map(|name| format!("{policy}{}", hex::encode(name)))
            .collect();
        (app, units)
    }

    #[tokio::test]
    async fn assets_all_orders_by_first_mint() {
        let (app, units) = three_asset_app();

        let asc = get_assets(&app, "?order=asc").await;
        assert_eq!(asc.iter().map(|x| x.asset.clone()).collect_vec(), units);
        assert!(asc.iter().all(|x| x.quantity == "1"));

        let desc = get_assets(&app, "?order=desc").await;
        let expected = units.iter().rev().cloned().collect_vec();
        assert_eq!(desc.iter().map(|x| x.asset.clone()).collect_vec(), expected);
    }

    #[tokio::test]
    async fn assets_all_paginates_in_both_orders() {
        let (app, units) = three_asset_app();

        // asc: page 2 of size 1 is the second asset ever minted
        let page = get_assets(&app, "?order=asc&page=2&count=1").await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].asset, units[1]);

        // desc: page 1 of size 1 is the newest asset
        let page = get_assets(&app, "?order=desc&page=1&count=1").await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].asset, units[2]);

        // desc: page 3 of size 1 is the oldest asset
        let page = get_assets(&app, "?order=desc&page=3&count=1").await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].asset, units[0]);

        // a page past the end is empty, not an error
        let page = get_assets(&app, "?page=4&count=1").await;
        assert!(page.is_empty());
    }

    #[tokio::test]
    async fn assets_all_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/assets?count=0", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/assets?page=x", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/assets?order=sideways", StatusCode::BAD_REQUEST).await;
        // page * count beyond the default scan limit (3000)
        assert_status(&app, "/assets?page=31&count=100", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn assets_all_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/assets", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
