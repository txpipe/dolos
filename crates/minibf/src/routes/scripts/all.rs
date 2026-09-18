//! `/scripts`: every script, in order of first appearance.

use std::collections::{BTreeSet, HashSet};

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::scripts_inner::ScriptsInner;
use dolos_cardano::{indexes::archive_dimensions, pallas_extras};
use dolos_core::{ArchiveStore as _, BlockBody, BlockSlot, Domain};
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{ComputeHash, MultiEraBlock, MultiEraTx, OriginalHash},
};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// Hashes of the scripts `tx` carries, in the order Blockfrost lists them.
///
/// Blockfrost sorts `/scripts` by the db-sync `script` row, and db-sync writes
/// those rows per tx: the reference script of each output first, then the
/// witness scripts out of a map keyed by script hash, so sorted by hash. These
/// are the same scripts the archive tags, which keeps the listing to what
/// `/scripts/{script_hash}` can resolve.
fn tx_script_hashes(tx: &MultiEraTx<'_>) -> Vec<Hash<28>> {
    let mut hashes: Vec<Hash<28>> = tx
        .produces()
        .iter()
        .filter_map(|(_, output)| output.script_ref())
        .map(|script_ref| pallas_extras::script_ref_hash(&script_ref))
        .collect();

    let mut witnesses = BTreeSet::new();
    witnesses.extend(tx.native_scripts().iter().map(|x| x.original_hash()));
    witnesses.extend(tx.plutus_v1_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v2_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v3_scripts().iter().map(|x| x.compute_hash()));

    hashes.extend(witnesses);

    hashes
}

/// Whether the block at `slot` is the first one tagged with `script`.
fn is_first_seen_at<D: Domain>(
    domain: &D,
    script: &Hash<28>,
    slot: BlockSlot,
) -> Result<bool, Error> {
    let first = domain
        .archive()
        .slots_by_tag(archive_dimensions::SCRIPT, script.as_slice(), 0, slot)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .next()
        .transpose()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(first == Some(slot))
}

/// Scripts that show up for the first time in `block`, oldest first.
///
/// A script repeats a lot (every spend of a validator carries or references
/// it), so each hash is looked up once per block.
fn first_seen_scripts<D: Domain>(
    domain: &D,
    slot: BlockSlot,
    block: &MultiEraBlock<'_>,
) -> Result<Vec<Hash<28>>, Error> {
    let mut checked = HashSet::new();
    let mut scripts = Vec::new();

    for tx in block.txs() {
        for hash in tx_script_hashes(&tx) {
            if checked.insert(hash) && is_first_seen_at(domain, &hash, slot)? {
                scripts.push(hash);
            }
        }
    }

    Ok(scripts)
}

/// Walk the archive in `order` collecting first-seen scripts until `needed` of
/// them are known.
///
/// `needed` bounds the results, not the work: any number of blocks can sit
/// between two new scripts, and a page that the archive cannot fill would
/// replay every block from `start` to the tip. The walk therefore also stops
/// after `budget` blocks and says so, instead of holding a blocking query slot
/// for the length of the chain.
fn scan_first_seen_scripts<D: Domain>(
    domain: &D,
    start: BlockSlot,
    order: Order,
    needed: usize,
    budget: usize,
) -> Result<Vec<Hash<28>>, Error> {
    let iter = domain
        .archive()
        .get_range(Some(start), None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let blocks: Box<dyn Iterator<Item = (BlockSlot, BlockBody)>> = match order {
        Order::Asc => Box::new(iter),
        Order::Desc => Box::new(iter.rev()),
    };

    let mut found = Vec::new();

    for (scanned, (slot, body)) in blocks.enumerate() {
        // the iterator handed us another block while the budget is spent, so
        // the answer is somewhere further in and out of reach for this request
        if scanned == budget {
            return Err(Error::ScanBudgetExceeded);
        }

        let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut scripts = first_seen_scripts(domain, slot, &block)?;

        if matches!(order, Order::Desc) {
            scripts.reverse();
        }

        found.append(&mut scripts);

        if found.len() >= needed {
            return Ok(found);
        }
    }

    Ok(found)
}

/// `GET /scripts`: every script seen on chain, ordered by first appearance.
///
/// No store enumerates scripts: they are not ledger state, and the archive
/// index keeps a hash of the script hash, so it answers "where is this
/// script" but not "which scripts are there". The listing therefore replays
/// the archive, starting where scripts begin (Shelley) or, for `desc`, from
/// the tip backwards, and stops as soon as the requested page is covered.
///
/// `max_scan_items` bounds the request twice: it caps the page depth like on
/// the other scanning endpoints, and it caps the blocks the replay may decode.
/// A page the replay cannot reach within that many blocks is refused, because
/// a short answer would read as the end of the list.
pub async fn all<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ScriptsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;

    // Byron has no scripts, so nothing before Shelley can appear.
    let chain = domain.get_chain_summary()?;
    let start = chain.epoch_start(chain.first_shelley_epoch());

    let order = pagination.order;
    let needed = pagination.from() + pagination.count;
    let budget = domain.config.max_scan_items() as usize;

    let scripts = domain
        .query()
        .run_blocking(move |domain| {
            Ok(scan_first_seen_scripts(
                &domain, start, order, needed, budget,
            ))
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    let items = scripts
        .into_iter()
        .skip(pagination.from())
        .take(pagination.count)
        .map(|script| ScriptsInner {
            script_hash: script.to_string(),
        })
        .collect();

    Ok(Json(items))
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
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    async fn get_scripts(app: &TestApp, query: &str) -> Vec<String> {
        let path = format!("/scripts{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<ScriptsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse scripts");

        items.into_iter().map(|x| x.script_hash).collect()
    }

    /// The first tx of every synthetic block carries the same two scripts: a
    /// native one as the reference script of its output and a plutus one as a
    /// witness. That is the listing order, and what every later block repeats.
    fn expected(app: &TestApp) -> Vec<String> {
        vec![
            app.vectors().script_hash.clone(),
            app.vectors().plutus_script_hash.clone(),
        ]
    }

    #[tokio::test]
    async fn scripts_all_happy_path() {
        let app = TestApp::new();

        // three blocks repeat the scripts, each one is listed once
        let scripts = get_scripts(&app, "").await;
        assert_eq!(scripts, expected(&app));

        // everything listed resolves
        for script in scripts {
            assert_status(&app, &format!("/scripts/{script}"), StatusCode::OK).await;
        }
    }

    #[tokio::test]
    async fn scripts_all_order_desc() {
        let app = TestApp::new();

        let desc = get_scripts(&app, "?order=desc").await;
        let reversed = expected(&app).into_iter().rev().collect_vec();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn scripts_all_paginates_in_both_orders() {
        let app = TestApp::new();
        let expected = expected(&app);

        // asc: page 2 of size 1 is the second script ever seen
        let page = get_scripts(&app, "?order=asc&page=2&count=1").await;
        assert_eq!(page, vec![expected[1].clone()]);

        // desc: page 1 of size 1 is the newest script
        let page = get_scripts(&app, "?order=desc&page=1&count=1").await;
        assert_eq!(page, vec![expected[1].clone()]);

        // desc: page 2 of size 1 is the oldest script
        let page = get_scripts(&app, "?order=desc&page=2&count=1").await;
        assert_eq!(page, vec![expected[0].clone()]);

        // a page past the end is empty, not an error
        let page = get_scripts(&app, "?page=3&count=1").await;
        assert!(page.is_empty());
    }

    #[tokio::test]
    async fn scripts_all_stops_at_scan_budget() {
        // four blocks carrying the same scripts: only the first block shows
        // new ones, so anything past two scripts costs blocks and yields
        // nothing
        let app = TestApp::new_with_scan_limit(
            SyntheticBlockConfig {
                block_count: 4,
                txs_per_block: 1,
                ..Default::default()
            },
            3,
        );

        // a page the scan covers before the budget runs out is served
        let page = get_scripts(&app, "?count=2").await;
        assert_eq!(page.len(), 2);

        // a page that would need a fourth block is refused, not truncated
        let (status, bytes) = app.get_bytes("/scripts?count=3").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        let body = String::from_utf8_lossy(&bytes);
        assert!(body.contains("archive blocks"), "unexpected body: {body}");
    }

    #[tokio::test]
    async fn scripts_all_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/scripts?count=0", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/scripts?page=x", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/scripts?order=sideways", StatusCode::BAD_REQUEST).await;
        // page * count beyond the default scan limit (3000)
        assert_status(&app, "/scripts?page=31&count=100", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn scripts_all_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/scripts", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
