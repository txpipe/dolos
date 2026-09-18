//! `/scripts`: every script, in order of first appearance.

use std::collections::{BTreeSet, HashMap, HashSet};

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
/// witness scripts out of a map keyed by script hash, so sorted by hash. A tx
/// that failed phase-2 validation stops at its outputs (the collateral
/// return): db-sync never reads its witnesses.
fn tx_script_hashes(tx: &MultiEraTx<'_>) -> Vec<Hash<28>> {
    let mut hashes: Vec<Hash<28>> = tx
        .produces()
        .iter()
        .filter_map(|(_, output)| output.script_ref())
        .map(|script_ref| pallas_extras::script_ref_hash(&script_ref))
        .collect();

    if !tx.is_valid() {
        return hashes;
    }

    let mut witnesses = BTreeSet::new();
    witnesses.extend(tx.native_scripts().iter().map(|x| x.original_hash()));
    witnesses.extend(tx.plutus_v1_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v2_scripts().iter().map(|x| x.compute_hash()));
    witnesses.extend(tx.plutus_v3_scripts().iter().map(|x| x.compute_hash()));

    hashes.extend(witnesses);

    hashes
}

/// Hashes of the scripts a block carries, oldest first and once each.
fn block_script_hashes(body: &BlockBody) -> Result<Vec<Hash<28>>, StatusCode> {
    let block = MultiEraBlock::decode(body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut seen = HashSet::new();

    Ok(block
        .txs()
        .iter()
        .flat_map(tx_script_hashes)
        .filter(|hash| seen.insert(*hash))
        .collect())
}

/// Works out which block introduces a script.
///
/// The archive tags are candidates, not proof: they are keyed by a 64-bit
/// digest of the script hash, and they cover the witnesses of txs that failed
/// phase-2 validation, which the listing skips. A tagged block only counts
/// once it is read by the same rules the listing reads blocks by, the way
/// `find_first_by_tag` checks its candidates. Whatever gets decoded and
/// resolved is kept for the rest of the request, so a script costs one tag
/// query and, when it is a repeat, at most one extra block.
struct FirstSeen<'a, D: Domain> {
    domain: &'a D,
    carried: HashMap<BlockSlot, Vec<Hash<28>>>,
    first_slot: HashMap<Hash<28>, BlockSlot>,
}

impl<'a, D: Domain> FirstSeen<'a, D> {
    fn new(domain: &'a D) -> Self {
        Self {
            domain,
            carried: HashMap::new(),
            first_slot: HashMap::new(),
        }
    }

    /// Scripts carried by the block at `slot`, whose body the caller holds.
    fn scripts_of(
        &mut self,
        slot: BlockSlot,
        body: &BlockBody,
    ) -> Result<Vec<Hash<28>>, StatusCode> {
        if let Some(hashes) = self.carried.get(&slot) {
            return Ok(hashes.clone());
        }

        let hashes = block_script_hashes(body)?;
        self.carried.insert(slot, hashes.clone());

        Ok(hashes)
    }

    /// A tagged slot the archive holds no block for carries nothing.
    fn carries(&mut self, slot: BlockSlot, script: &Hash<28>) -> Result<bool, StatusCode> {
        if !self.carried.contains_key(&slot) {
            let body = self
                .domain
                .archive()
                .get_block_by_slot(&slot)
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let hashes = match body {
                Some(body) => block_script_hashes(&body)?,
                None => vec![],
            };

            self.carried.insert(slot, hashes);
        }

        Ok(self.carried[&slot].contains(script))
    }

    /// Whether `script`, carried by the block at `slot`, shows up there first.
    fn is_first_seen_at(&mut self, script: &Hash<28>, slot: BlockSlot) -> Result<bool, StatusCode> {
        if let Some(first) = self.first_slot.get(script) {
            return Ok(*first == slot);
        }

        let candidates = self
            .domain
            .archive()
            .slots_by_tag(archive_dimensions::SCRIPT, script.as_slice(), 0, slot)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mut first = slot;

        for candidate in candidates {
            let candidate = candidate.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            if candidate == slot || self.carries(candidate, script)? {
                first = candidate;
                break;
            }
        }

        self.first_slot.insert(*script, first);

        Ok(first == slot)
    }
}

/// Whether the archive still holds every block from `start` on.
///
/// Pruning (`sync.max_history`) drops the oldest blocks and their tags, and a
/// script's first retained repeat would then pass for its first appearance.
/// Nothing is missing when the oldest block predates `start`, or is the first
/// of its chain: block numbers count from 0, or from 1 past a boundary block.
fn holds_history_from<D: Domain>(domain: &D, start: BlockSlot) -> Result<bool, StatusCode> {
    let oldest = domain
        .archive()
        .get_range(None, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .next();

    let Some((slot, body)) = oldest else {
        return Ok(true);
    };

    if slot < start {
        return Ok(true);
    }

    let block = MultiEraBlock::decode(&body).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(block.number() <= 1)
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
    if !holds_history_from(domain, start)? {
        return Err(Error::HistoryPruned);
    }

    let iter = domain
        .archive()
        .get_range(Some(start), None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let blocks: Box<dyn Iterator<Item = (BlockSlot, BlockBody)>> = match order {
        Order::Asc => Box::new(iter),
        Order::Desc => Box::new(iter.rev()),
    };

    let mut first_seen = FirstSeen::new(domain);
    let mut found = Vec::new();

    for (scanned, (slot, body)) in blocks.enumerate() {
        // the iterator handed us another block while the budget is spent, so
        // the answer is somewhere further in and out of reach for this request
        if scanned == budget {
            return Err(Error::ScanBudgetExceeded);
        }

        let mut scripts = Vec::new();

        for script in first_seen.scripts_of(slot, &body)? {
            if first_seen.is_first_seen_at(&script, slot)? {
                scripts.push(script);
            }
        }

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
/// the tip backwards, and stops as soon as the requested page is covered. A
/// node that prunes its history cannot tell a first appearance from a repeat
/// and refuses.
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
    use crate::test_support::{TestApp, TestDomainBuilder, TestFault};
    use dolos_core::{ArchiveIndexDelta, ArchiveWriter as _, Tag};
    use dolos_testing::synthetic::SyntheticBlockConfig;
    use itertools::Itertools;
    use pallas::{
        codec::{
            minicbor,
            utils::{Bytes, CborWrap, KeepRaw, Nullable},
        },
        ledger::primitives::{
            alonzo::NativeScript,
            conway::{
                PostAlonzoTransactionOutput, ScriptRef, TransactionBody, TransactionInput,
                TransactionOutput, Tx, Value, WitnessSet,
            },
            NonEmptySet, PlutusScript, Set,
        },
    };

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

    /// A Conway tx with a native reference script on its collateral return
    /// and a plutus script among its witnesses.
    fn tx_cbor(success: bool) -> (Vec<u8>, Hash<28>, Hash<28>) {
        let native = NativeScript::InvalidHereafter(7);
        let native_hash = native.compute_hash();

        let plutus = PlutusScript::<2>(Bytes::from(vec![0x46, 0x01, 0x00, 0x00, 0x22, 0x26, 0x01]));
        let plutus_hash = plutus.compute_hash();

        let collateral_return = PostAlonzoTransactionOutput {
            address: Bytes::from(vec![0x60; 29]),
            value: Value::Coin(1_500_000),
            datum_option: None,
            script_ref: Some(CborWrap(ScriptRef::NativeScript(KeepRaw::from(native)))),
        };

        let input = TransactionInput {
            transaction_id: [1u8; 32].into(),
            index: 0,
        };

        let body = TransactionBody {
            inputs: Set::from(vec![input.clone()]),
            outputs: vec![],
            fee: 200_000,
            ttl: None,
            certificates: None,
            withdrawals: None,
            auxiliary_data_hash: None,
            validity_interval_start: None,
            mint: None,
            script_data_hash: None,
            collateral: Some(NonEmptySet::try_from(vec![input]).expect("non-empty collateral")),
            required_signers: None,
            network_id: None,
            collateral_return: Some(TransactionOutput::PostAlonzo(KeepRaw::from(
                collateral_return,
            ))),
            total_collateral: Some(500_000),
            reference_inputs: None,
            voting_procedures: None,
            proposal_procedures: None,
            treasury_value: None,
            donation: None,
        };

        let body = minicbor::to_vec(&body).expect("failed to encode tx body");
        let body = minicbor::decode::<KeepRaw<'_, TransactionBody<'_>>>(&body)
            .expect("failed to decode tx body")
            .to_owned();

        let witness_set = WitnessSet {
            vkeywitness: None,
            native_script: None,
            bootstrap_witness: None,
            plutus_v1_script: None,
            plutus_data: None,
            redeemer: None,
            plutus_v2_script: Some(
                NonEmptySet::try_from(vec![plutus]).expect("non-empty plutus script set"),
            ),
            plutus_v3_script: None,
        };

        let tx = Tx {
            transaction_body: body,
            transaction_witness_set: KeepRaw::from(witness_set),
            success,
            auxiliary_data: Nullable::Null,
        };

        let cbor = minicbor::to_vec(tx).expect("failed to encode tx");

        (cbor, native_hash, plutus_hash)
    }

    #[test]
    fn scripts_all_skips_witnesses_of_invalid_txs() {
        // db-sync reads the collateral return of a tx that failed phase-2
        // validation and nothing else
        let (cbor, native, _) = tx_cbor(false);
        let tx = MultiEraTx::decode(&cbor).expect("failed to decode tx");
        assert!(!tx.is_valid());
        assert_eq!(tx_script_hashes(&tx), vec![native]);

        // the same tx, valid: no collateral return is produced, the witnesses
        // count
        let (cbor, _, plutus) = tx_cbor(true);
        let tx = MultiEraTx::decode(&cbor).expect("failed to decode tx");
        assert!(tx.is_valid());
        assert_eq!(tx_script_hashes(&tx), vec![plutus]);
    }

    #[test]
    fn scripts_all_tags_prove_nothing_on_their_own() {
        let (domain, vectors) =
            TestDomainBuilder::new_with_synthetic(SyntheticBlockConfig::default()).finish();

        let scripts = [&vectors.script_hash, &vectors.plutus_script_hash];
        let first_block = vectors.blocks.first().expect("missing block vectors").slot;
        assert!(first_block > 0);

        // what a colliding key or the witnesses of an invalid tx leave behind:
        // a tag ahead of the first block that actually carries the script
        let tags = scripts
            .iter()
            .map(|script| {
                let script = hex::decode(script).expect("invalid script hash");
                Tag::new(archive_dimensions::SCRIPT, script)
            })
            .collect();

        let writer = domain.archive().start_writer().expect("no writer");
        writer
            .apply_index(&[ArchiveIndexDelta {
                slot: 0,
                block_hash: vec![0; 32],
                block_number: None,
                tx_hashes: vec![],
                tags,
            }])
            .expect("failed to write tags");
        writer.commit().expect("failed to commit tags");

        let found = scan_first_seen_scripts(&domain, 0, Order::Asc, usize::MAX, usize::MAX)
            .unwrap_or_else(|_| panic!("scan failed"));

        let found = found.iter().map(|x| x.to_string()).collect_vec();
        assert_eq!(found, scripts.iter().map(|x| x.to_string()).collect_vec());
    }

    #[tokio::test]
    async fn scripts_all_refuses_pruned_history() {
        // a chain whose oldest block is not its first: what pruning leaves
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            start_block: 10,
            slot: 100,
            ..Default::default()
        });

        let (status, bytes) = app.get_bytes("/scripts").await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        let body = String::from_utf8_lossy(&bytes);
        assert!(
            body.contains("prunes its chain history"),
            "unexpected body: {body}"
        );
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
