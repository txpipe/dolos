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
