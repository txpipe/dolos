use axum::http::StatusCode;
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraOutput;
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use dolos_core::async_query::{BlockMetaResolver, BlockRefMeta, LOCATE_CHUNK};
use dolos_core::{
    ArchiveStore as _, BlockSlot, Domain, EraCbor, StateStore as _, TxHash, TxoIdx, TxoRef,
};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

/// The outputs that a UTxO listing returns.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UtxoFilter {
    /// Every output.
    All,
    /// Only outputs without native assets, the rows that Blockfrost lists for
    /// the `lovelace` asset.
    AdaOnly,
}

impl UtxoFilter {
    /// Returns `true` when the listing returns `output`.
    pub fn keeps(self, output: &MultiEraOutput<'_>) -> bool {
        match self {
            UtxoFilter::All => true,
            UtxoFilter::AdaOnly => output
                .value()
                .assets()
                .iter()
                .all(|policy| policy.assets().is_empty()),
        }
    }
}

/// UTxO refs read, decoded and filtered per blocking task. One chunk holds
/// its permit for a few milliseconds.
const UTXO_CHUNK: usize = 1024;

/// Loads one page of the UTxOs in `refs` that `filter` keeps. Only the last
/// page can have fewer than `count` rows.
pub async fn load_utxo_models<D, T>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: Pagination,
    filter: UtxoFilter,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    // A filter needs every output decoded, and the window loads only the
    // page's outputs. Keep the refs that pass before the window is cut.
    let refs = match filter {
        UtxoFilter::All => refs,
        _ => select_utxos(domain, refs, filter, UTXO_CHUNK)
            .await?
            .into_iter()
            .map(|(txo_ref, _)| txo_ref)
            .collect(),
    };

    let mut rows = page_rows(domain, refs, &pagination).await?;

    let mut window = select_window(&rows, &pagination);
    if window.rows.is_empty() {
        return Ok(Vec::new());
    }

    let mut utxos = select_utxos(
        domain,
        window.refs().into_iter().collect(),
        UtxoFilter::All,
        UTXO_CHUNK,
    )
    .await?;

    // Refresh once if a block spent listed refs after the tag read.
    if utxos.len() != window.rows.len() {
        let refs = rows.iter().map(|(_, txo_ref)| txo_ref.clone()).collect();
        utxos = select_utxos(domain, refs, UtxoFilter::All, UTXO_CHUNK).await?;
        let live: HashSet<_> = utxos.iter().map(|(txo_ref, _)| txo_ref.clone()).collect();
        rows.retain(|(_, txo_ref)| live.contains(txo_ref));
        window = select_window(&rows, &pagination);
        let refs: HashSet<_> = window.refs().into_iter().collect();
        utxos.retain(|(txo_ref, _)| refs.contains(txo_ref));
    }

    let PageWindow { rows, skip } = window;
    let window_slots: HashMap<_, _> = rows
        .into_iter()
        .map(|(slot, txo_ref)| (txo_ref, slot))
        .collect();

    let mut block_meta = BlockMetaResolver::new(domain.query());
    let block_deps = block_meta
        .resolve_batch(utxos.iter().map(|(txo_ref, _)| txo_ref.0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut rows: Vec<_> = utxos
        .into_iter()
        // The window cut the page by its slot, so sort by that slot first. A
        // block that `sync.max_history` pruned keeps its index slot until the
        // sweep but has no block data, and `page_sort_key` alone would move its
        // rows out of their slot and cut the page in a different place.
        .map(|(txo_ref, cbor)| {
            let block_data = block_deps.get(&txo_ref.0).cloned();
            let slot = window_slots.get(&txo_ref).copied().flatten();
            (
                (slot, page_sort_key(&txo_ref, block_data.as_ref())),
                txo_ref,
                cbor,
                block_data,
            )
        })
        .collect();

    match pagination.order {
        Order::Asc => {
            rows.sort_by_key(|(sort_key, ..)| *sort_key);
        }
        Order::Desc => {
            rows.sort_by_key(|(sort_key, ..)| *sort_key);
            rows.reverse();
        }
    }

    let mut out = Vec::new();
    for (_, txo_ref, cbor, block_data) in rows.into_iter().skip(skip).take(pagination.count) {
        let output = MultiEraOutput::try_from(cbor.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let TxoRef(tx_hash, txo_idx) = txo_ref;
        let builder = UtxoOutputModelBuilder::from_output(tx_hash, txo_idx, output);

        let builder = if let Some(x) = block_data {
            builder.with_block_data(x)
        } else {
            builder
        };

        out.push(<UtxoOutputModelBuilder<'_> as IntoModel<T>>::into_model(
            builder,
        )?);
    }

    Ok(out)
}

/// The rows that can land on the requested page, in page order, and the
/// offset of the page's first row among them.
struct PageWindow {
    rows: Vec<(Option<BlockSlot>, TxoRef)>,
    skip: usize,
}

impl PageWindow {
    fn refs(&self) -> Vec<TxoRef> {
        self.rows
            .iter()
            .map(|(_, txo_ref)| txo_ref.clone())
            .collect()
    }
}

/// Every row of the listing as `(slot, ref)`, in page order.
///
/// The page order (`page_sort_key`) needs each row's transaction index, and
/// only decoding the row's block yields it. The slot does not: the archive's
/// exact index answers it with one point read per transaction. Ordering by
/// slot first already puts every block's rows next to each other and in their
/// final place relative to other blocks, so the page comes out exact once the
/// blocks cut by its two edges are taken whole and re-ordered by the full key.
async fn page_rows<D>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    pagination: &Pagination,
) -> Result<Vec<(Option<BlockSlot>, TxoRef)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tx_hashes: Vec<TxHash> = refs.iter().map(|txo_ref| txo_ref.0).unique().collect();

    let mut slots = HashMap::with_capacity(tx_hashes.len());
    for hashes in tx_hashes.chunks(LOCATE_CHUNK) {
        let hashes = hashes.to_vec();
        let located = domain
            .query()
            .run_blocking(move |domain| {
                let mut slots = HashMap::with_capacity(hashes.len());
                for hash in hashes {
                    if let Some(slot) = domain.archive().slot_by_tx_hash(hash.as_slice())? {
                        slots.insert(hash, slot);
                    }
                }
                Ok(slots)
            })
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        slots.extend(located);
    }

    let mut rows: Vec<_> = refs
        .into_iter()
        .map(|txo_ref| (slots.get(&txo_ref.0).copied(), txo_ref))
        .collect();

    sort_rows(&mut rows, pagination.order);

    Ok(rows)
}

/// Sort `(slot, ref)` rows into page order: slot, then `TxoRef`, rows without
/// a slot first, and the whole order reversed for `order=desc`.
fn sort_rows(rows: &mut [(Option<BlockSlot>, TxoRef)], order: Order) {
    rows.sort();
    if let Order::Desc = order {
        rows.reverse();
    }
}

/// Pick the page out of `(slot, ref)` rows in page order, widened at both
/// edges to whole slots.
///
/// Rows without a slot sort first, by `TxoRef`, which is already their final
/// order, so the window never widens into them.
fn select_window(rows: &[(Option<BlockSlot>, TxoRef)], pagination: &Pagination) -> PageWindow {
    let from = pagination.from();
    let to = pagination.to().min(rows.len());
    if from >= to {
        return PageWindow {
            rows: Vec::new(),
            skip: 0,
        };
    }

    let same_slot = |i: usize, slot: Option<BlockSlot>| slot.is_some() && rows[i].0 == slot;

    let mut start = from;
    while start > 0 && same_slot(start - 1, rows[from].0) {
        start -= 1;
    }

    let mut end = to;
    while end < rows.len() && same_slot(end, rows[to - 1].0) {
        end += 1;
    }

    PageWindow {
        rows: rows[start..end].to_vec(),
        skip: from - start,
    }
}

/// Read and decode the outputs behind `refs`, `chunk` refs per blocking
/// task, and keep those that `filter` keeps. Every output must decode.
async fn select_utxos<D>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    filter: UtxoFilter,
    chunk: usize,
) -> Result<Vec<(TxoRef, Arc<EraCbor>)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let mut refs: Vec<_> = refs.into_iter().collect();
    refs.sort();

    let mut selected = Vec::new();

    for refs in refs.chunks(chunk.max(1)) {
        let refs = refs.to_vec();

        let kept = domain
            .blocking(move |domain| {
                let utxos = domain
                    .state()
                    .get_utxos(refs)
                    .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                let mut kept = Vec::with_capacity(utxos.len());
                for (txo_ref, cbor) in utxos {
                    let output = MultiEraOutput::try_from(cbor.as_ref())
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    if filter.keeps(&output) {
                        kept.push((txo_ref, cbor));
                    }
                }

                Ok(kept)
            })
            .await?;

        selected.extend(kept);
    }

    Ok(selected)
}

/// The page order for UTxO models in the same window slot: chain position
/// first, `TxoRef` second.
///
/// Chain position is `None` for an output whose creation block was pruned by
/// `sync.max_history` — the block that carries its slot no longer exists, so
/// true chain order is unrecoverable for it. The `TxoRef` tie-breaker keeps
/// those rows in a deterministic order across requests; without it their
/// order came from `HashMap` iteration, and two page requests could slice two
/// different shufflings, duplicating or dropping rows.
///
/// `None` sorts before every known position, which approximates chain order:
/// a pruned creation block is older than every retained one.
fn page_sort_key(
    txo_ref: &TxoRef,
    block_data: Option<&BlockRefMeta>,
) -> (Option<(u64, usize, u32)>, TxHash, TxoIdx) {
    let TxoRef(tx_hash, txo_idx) = *txo_ref;

    (
        block_data.map(|data| (data.slot, data.tx_index, txo_idx)),
        tx_hash,
        txo_idx,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestDomainBuilder};
    use axum::body::Body;
    use axum::http::Request;
    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
    use dolos_cardano::indexes::CardanoStateIndexExt as _;
    use dolos_core::config::MinibfConfig;
    use dolos_core::import::ImportExt as _;
    use dolos_core::{ArchiveWriter as _, ChainPoint, StateWriter as _, UtxoSetDelta};
    use dolos_testing::measured::MeasuredStores;
    use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
    use dolos_testing::toy_domain::{MemoryStores, ToyDomain, ToyStores as _};
    use http_body_util::BodyExt as _;
    use pallas::crypto::hash::Hash;
    use pallas::ledger::addresses::Address;
    use tower::util::ServiceExt as _;

    /// Pins the pruned-row ordering contract: no chain position means the
    /// `TxoRef` decides, deterministically, and the whole unknowable group
    /// sorts before any row with a known position.
    #[test]
    fn page_sort_key_orders_pruned_rows_by_txo_ref() {
        let low = TxoRef(Hash::from([0xaa; 32]), 1);
        let high = TxoRef(Hash::from([0xbb; 32]), 0);
        let low_later = TxoRef(Hash::from([0xaa; 32]), 2);
        let key = |txo_ref: &TxoRef| page_sort_key(txo_ref, None);

        // no block data: the TxoRef alone decides, tx hash before output index
        assert!(key(&low) < key(&high));
        assert!(key(&low) < key(&low_later));
        assert!(key(&low_later) < key(&high));

        // a known chain position sorts after the whole unknowable group,
        // regardless of its TxoRef
        let block = BlockRefMeta {
            slot: 1,
            hash: Hash::from([0x11; 32]),
            height: 1,
            tx_hash: Hash::from([0x00; 32]),
            tx_index: 0,
        };
        let positioned = page_sort_key(&TxoRef(Hash::from([0x00; 32]), 0), Some(&block));

        assert!(key(&high) < positioned);
    }

    async fn selected(app: &TestApp, filter: UtxoFilter, chunk: usize) -> Vec<TxoRef> {
        let facade = app.facade();
        let address = pallas::ledger::addresses::Address::from_bech32(&app.vectors().address)
            .expect("vector address");
        let refs = facade
            .state()
            .utxos_by_address(&address.to_vec())
            .expect("address refs");

        let mut refs: Vec<_> = select_utxos(&facade, refs, filter, chunk)
            .await
            .expect("selected utxos")
            .into_iter()
            .map(|(txo_ref, _)| txo_ref)
            .collect();
        refs.sort();
        refs
    }

    #[tokio::test]
    async fn chunked_utxo_selection_matches_unchunked() {
        let app = TestApp::new();

        for filter in [UtxoFilter::All, UtxoFilter::AdaOnly] {
            let whole = selected(&app, filter, usize::MAX).await;
            let chunked = selected(&app, filter, 2).await;

            assert_eq!(chunked, whole);
        }

        assert!(
            selected(&app, UtxoFilter::All, usize::MAX).await.len() > 2,
            "too few utxos to split into chunks"
        );
    }

    fn txo(byte: u8, idx: TxoIdx) -> TxoRef {
        TxoRef(Hash::from([byte; 32]), idx)
    }

    fn pagination(order: Order, count: usize, page: u64) -> Pagination {
        Pagination {
            count,
            page,
            order,
            ..Default::default()
        }
    }

    fn select(rows: &[(Option<BlockSlot>, TxoRef)], pagination: Pagination) -> PageWindow {
        let mut rows = rows.to_vec();
        sort_rows(&mut rows, pagination.order);
        select_window(&rows, &pagination)
    }

    /// A page edge inside a slot widens the window to the whole slot, so the
    /// block decode can still order that slot's rows by transaction index; an
    /// edge inside the slot-less group does not widen.
    #[test]
    fn select_window_widens_page_edges_to_whole_slots() {
        let rows = vec![
            (None, txo(0x02, 0)),
            (None, txo(0x01, 0)),
            (Some(10), txo(0x13, 0)),
            (Some(10), txo(0x11, 0)),
            (Some(10), txo(0x12, 0)),
            (Some(20), txo(0x21, 0)),
            (Some(20), txo(0x21, 1)),
        ];

        // page 2 of count 2 covers sorted positions 2..4, cutting slot 10
        let window = select(&rows, pagination(Order::Asc, 2, 2));
        assert_eq!(
            window.refs(),
            vec![txo(0x11, 0), txo(0x12, 0), txo(0x13, 0)]
        );
        assert_eq!(window.skip, 0);

        // page 3 of count 2 covers 4..6: the tail of slot 10 and half of 20
        let window = select(&rows, pagination(Order::Asc, 2, 3));
        assert_eq!(
            window.refs(),
            vec![
                txo(0x11, 0),
                txo(0x12, 0),
                txo(0x13, 0),
                txo(0x21, 0),
                txo(0x21, 1)
            ]
        );
        assert_eq!(window.skip, 2);

        // the slot-less group is final order already and never widens
        let window = select(&rows, pagination(Order::Asc, 1, 1));
        assert_eq!(window.refs(), vec![txo(0x01, 0)]);
        assert_eq!(window.skip, 0);

        // descending mirrors the whole order, slot-less rows last
        let window = select(&rows, pagination(Order::Desc, 1, 2));
        assert_eq!(window.refs(), vec![txo(0x21, 1), txo(0x21, 0)]);
        assert_eq!(window.skip, 1);

        let window = select(&rows, pagination(Order::Desc, 2, 4));
        assert_eq!(window.refs(), vec![txo(0x01, 0)]);
        assert_eq!(window.skip, 0);

        // a page past the end is empty
        let window = select(&rows, pagination(Order::Asc, 100, 2));
        assert!(window.rows.is_empty());
    }

    async fn utxo_page(app: &TestApp, stake_address: &str, query: &str) -> Vec<String> {
        let path = format!("/accounts/{stake_address}/utxos?{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status for {path}: {}",
            String::from_utf8_lossy(&bytes)
        );

        let page: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse account utxos");

        page.into_iter()
            .map(|x| format!("{}#{}", x.tx_hash, x.output_index))
            .collect()
    }

    /// Every page size, in both orders, must slice the same sequence the
    /// single full page returns. The default fixture puts three of the
    /// account's outputs in each block, so small pages cut through blocks.
    #[tokio::test]
    async fn pages_concatenate_to_the_full_listing() {
        let app = TestApp::new();
        let stake_address = app.vectors().stake_address.clone();

        for order in ["asc", "desc"] {
            let full = utxo_page(&app, &stake_address, &format!("order={order}&count=100")).await;
            assert!(full.len() > 3, "fixture too small to cut through a block");

            for count in [1, 2, 4] {
                let mut walked = Vec::new();
                for page in 1.. {
                    let rows = utxo_page(
                        &app,
                        &stake_address,
                        &format!("order={order}&count={count}&page={page}"),
                    )
                    .await;
                    if rows.is_empty() {
                        break;
                    }
                    walked.extend(rows);
                }

                assert_eq!(walked, full, "order={order} count={count}");
            }
        }
    }

    /// On a `sync.max_history` node the archive index keeps a pruned block's
    /// slot until the sweep, but the block's rows have no block data. Pages
    /// must still slice the same sequence as the single full page.
    #[tokio::test]
    async fn pages_concatenate_while_the_index_outlives_pruned_blocks() {
        let cfg = SyntheticBlockConfig {
            block_count: 8,
            txs_per_block: 3,
            ..Default::default()
        };
        // undo drops the bodies and keeps the index, as between two sweeps
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let writer = domain.archive().start_writer().unwrap();
            for block in vectors.blocks.iter().take(3) {
                writer.undo(&ChainPoint::Slot(block.slot)).unwrap();
            }
            writer.commit().unwrap();
        });
        let stake_address = app.vectors().stake_address.clone();

        for order in ["asc", "desc"] {
            let full = utxo_page(&app, &stake_address, &format!("order={order}&count=100")).await;
            assert_eq!(full.len(), 24, "the pruned blocks' rows must stay listed");

            for count in [1, 2, 4, 5] {
                let mut walked = Vec::new();
                for page in 1.. {
                    let rows = utxo_page(
                        &app,
                        &stake_address,
                        &format!("order={order}&count={count}&page={page}"),
                    )
                    .await;
                    if rows.is_empty() {
                        break;
                    }
                    walked.extend(rows);
                }

                assert_eq!(walked, full, "order={order} count={count}");
            }
        }
    }

    /// A block can spend a listed UTxO between the caller's tag read and the
    /// load. No page may come back shorter than the rows left from its offset,
    /// since clients stop paging at the first short page.
    #[tokio::test]
    async fn a_ref_missing_from_the_set_does_not_shorten_a_page() {
        let (domain, vectors) = TestDomainBuilder::new_with_synthetic(SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 1,
            ..Default::default()
        })
        .finish();
        let address = Address::from_bech32(&vectors.address).unwrap().to_vec();
        let live = domain.state().utxos_by_address(&address).unwrap();
        let facade = Facade {
            inner: domain,
            config: MinibfConfig::new("[::]:0".parse().unwrap()),
            cache: Default::default(),
        };

        let count = 2;
        for spent in live.iter().map(|txo_ref| TxoRef(txo_ref.0, 7)) {
            let mut refs = live.clone();
            refs.insert(spent);

            for page in 1..=live.len().div_ceil(count) as u64 {
                let pagination = pagination(Order::Asc, count, page);
                let left = live.len() - pagination.from();
                let rows: Vec<AddressUtxoContentInner> =
                    load_utxo_models(&facade, refs.clone(), pagination, UtxoFilter::All)
                        .await
                        .unwrap();

                assert!(
                    rows.len() >= count.min(left),
                    "page {page} has {} rows",
                    rows.len()
                );
            }
        }
    }

    #[tokio::test]
    async fn many_spent_refs_need_only_one_refresh() {
        let block_count = UTXO_CHUNK + 100;
        let (blocks, vectors, chain_config) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count,
            txs_per_block: 1,
            slot: 1,
            ..Default::default()
        });
        let domain = ToyDomain::with_stores(
            Arc::new(dolos_cardano::include::preview::load()),
            chain_config,
            None,
            None,
            MeasuredStores::new(MemoryStores::open()),
        );
        domain.import_blocks(blocks).unwrap();
        let address = Address::from_bech32(&vectors.address).unwrap().to_vec();
        let refs = domain.state().utxos_by_address(&address).unwrap();
        assert_eq!(refs.len(), block_count);
        let facade = Facade {
            inner: domain,
            config: MinibfConfig::new("[::]:0".parse().unwrap()),
            cache: Default::default(),
        };
        let rows = page_rows(&facade, refs.clone(), &pagination(Order::Asc, 100, 1))
            .await
            .unwrap();
        let spent: Vec<_> = rows[99..block_count - 1]
            .iter()
            .map(|(_, txo_ref)| txo_ref.clone())
            .collect();
        let writer = facade.state().start_writer().unwrap();
        writer
            .apply_utxoset(&UtxoSetDelta {
                consumed_utxo: facade.state().get_utxos(spent.clone()).unwrap(),
                ..Default::default()
            })
            .unwrap();
        writer.commit().unwrap();
        let mut live = refs.clone();
        for txo_ref in spent {
            live.remove(&txo_ref);
        }

        for order in [Order::Asc, Order::Desc] {
            for (count, page) in [(100, 1), (99, 2)] {
                let pagination = pagination(order, count, page);
                let expected: Vec<AddressUtxoContentInner> =
                    load_utxo_models(&facade, live.clone(), pagination.clone(), UtxoFilter::All)
                        .await
                        .unwrap();
                facade.state().counters.reset();
                let actual: Vec<AddressUtxoContentInner> =
                    load_utxo_models(&facade, refs.clone(), pagination, UtxoFilter::All)
                        .await
                        .unwrap();
                assert_eq!(actual, expected);
                assert_eq!(actual.len(), if page == 1 { 100 } else { 1 });
                let work = facade.state().counters.snapshot();
                assert_eq!(work.utxo_reads, (refs.len() + count) as u64);
                assert_eq!(work.block_reads, actual.len() as u64);
            }
        }
    }

    /// Block reads follow the rows a page returns, not the rows the address
    /// holds: a one-row page reads a twentieth of what the twenty-row page
    /// reads.
    #[tokio::test]
    async fn a_page_fetches_only_its_own_blocks() {
        let block_count = 20;
        let (blocks, vectors, chain_config) = build_synthetic_blocks(SyntheticBlockConfig {
            block_count,
            txs_per_block: 1,
            slot: 1,
            ..Default::default()
        });
        let domain = ToyDomain::with_stores(
            Arc::new(dolos_cardano::include::preview::load()),
            chain_config,
            None,
            None,
            MeasuredStores::new(MemoryStores::open()),
        );
        domain
            .import_blocks(blocks)
            .expect("import synthetic blocks");

        let router = crate::build_router(
            MinibfConfig::new("[::]:0".parse().expect("listen address")),
            domain.clone(),
        );

        let mut block_reads = Vec::new();
        for count in [1, 100] {
            domain.archive().counters.reset();
            let path = format!("/addresses/{}/utxos?count={count}", vectors.address);
            let res = router
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            let bytes = res.into_body().collect().await.unwrap().to_bytes();
            let rows: Vec<AddressUtxoContentInner> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(rows.len(), count.min(block_count));

            block_reads.push(domain.archive().counters.snapshot().block_reads);
        }

        assert!(block_reads[0] > 0);
        assert_eq!(block_reads[1], block_reads[0] * block_count as u64);
    }
}
