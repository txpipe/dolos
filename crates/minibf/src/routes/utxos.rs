use axum::http::StatusCode;
use itertools::Itertools;
use pallas::ledger::traverse::MultiEraOutput;
use std::collections::{HashMap, HashSet};

use dolos_cardano::indexes::AsyncCardanoQueryExt;
use dolos_core::async_query::BlockMetaResolver;
use dolos_core::{ArchiveStore as _, BlockSlot, Domain, StateStore as _, TxHash, TxoIdx, TxoRef};

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
        _ => kept_refs(domain, refs, filter)?,
    };

    let mut rows = page_rows(domain, refs, &pagination).await?;

    // A block can spend a listed UTxO after the caller read the tags. Drop the
    // missing ref and select again, so that only the last page can be short.
    let (window, utxos) = loop {
        let window = select_window(&rows, &pagination);

        if window.refs.is_empty() {
            return Ok(Vec::new());
        }

        let utxos = domain
            .state()
            .get_utxos(window.refs.clone())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        if utxos.len() == window.refs.len() {
            break (window, utxos);
        }

        let missing: HashSet<_> = window
            .refs
            .iter()
            .filter(|txo_ref| !utxos.contains_key(*txo_ref))
            .collect();

        rows.retain(|(_, txo_ref)| !missing.contains(txo_ref));
    };

    // decoded
    let utxos: HashMap<_, _> = utxos
        .iter()
        .map(|(k, v)| MultiEraOutput::try_from(v.as_ref()).map(|x| (k, x)))
        .try_collect()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut block_meta = BlockMetaResolver::new(domain.query());
    let block_deps = block_meta
        .resolve_batch(utxos.keys().map(|txo_ref| txo_ref.0))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut models: Vec<_> = utxos
        .into_iter()
        .map(|(TxoRef(tx_hash, txo_idx), txo)| {
            let builder = UtxoOutputModelBuilder::from_output(*tx_hash, *txo_idx, txo);
            let block_data = block_deps.get(tx_hash).cloned();

            if let Some(x) = block_data {
                builder.with_block_data(x)
            } else {
                builder
            }
        })
        .map(|x| (page_sort_key::<T>(&x), x))
        .collect();

    match pagination.order {
        Order::Asc => {
            models.sort_by_key(|(sort_key, _)| *sort_key);
        }
        Order::Desc => {
            models.sort_by_key(|(sort_key, _)| *sort_key);
            models.reverse();
        }
    }

    let mut out = Vec::new();
    for builder in models
        .into_iter()
        .map(|(_, builder)| builder)
        .skip(window.skip)
        .take(pagination.count)
    {
        let key: Vec<u8> = builder.txo_ref().into();
        let consumed_by = domain
            .query()
            .tx_by_spent_txo(&key)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let builder = if let Some(consumed_by) = consumed_by {
            builder.with_consumed_by(consumed_by)
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
    refs: Vec<TxoRef>,
    skip: usize,
}

/// The refs in `refs` whose output `filter` keeps.
fn kept_refs<D: Domain>(
    domain: &Facade<D>,
    refs: HashSet<TxoRef>,
    filter: UtxoFilter,
) -> Result<HashSet<TxoRef>, StatusCode> {
    let utxos = domain
        .state()
        .get_utxos(refs.into_iter().collect())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    utxos
        .iter()
        .filter_map(
            |(txo_ref, cbor)| match MultiEraOutput::try_from(cbor.as_ref()) {
                Ok(output) => filter.keeps(&output).then(|| Ok(txo_ref.clone())),
                Err(_) => Some(Err(StatusCode::INTERNAL_SERVER_ERROR)),
            },
        )
        .collect()
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

    let slots: HashMap<TxHash, BlockSlot> = domain
        .query()
        .run_blocking(move |domain| {
            let mut slots = HashMap::with_capacity(tx_hashes.len());
            for hash in tx_hashes {
                if let Some(slot) = domain.archive().slot_by_tx_hash(hash.as_slice())? {
                    slots.insert(hash, slot);
                }
            }
            Ok(slots)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

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
            refs: Vec::new(),
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
        refs: rows[start..end]
            .iter()
            .map(|(_, txo_ref)| txo_ref.clone())
            .collect(),
        skip: from - start,
    }
}

/// The page order for a UTxO model: chain position first, `TxoRef` second.
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
fn page_sort_key<T>(
    builder: &UtxoOutputModelBuilder<'_>,
) -> (Option<(u64, usize, u32)>, TxHash, TxoIdx)
where
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let TxoRef(tx_hash, txo_idx) = builder.txo_ref();

    (
        <UtxoOutputModelBuilder<'_> as IntoModel<T>>::sort_key(builder),
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
    use dolos_core::async_query::BlockRefMeta;
    use dolos_core::config::MinibfConfig;
    use dolos_core::import::ImportExt as _;
    use dolos_testing::measured::MeasuredStores;
    use dolos_testing::synthetic::{build_synthetic_blocks, SyntheticBlockConfig};
    use dolos_testing::toy_domain::{MemoryStores, ToyDomain, ToyStores as _};
    use http_body_util::BodyExt as _;
    use pallas::codec::minicbor;
    use pallas::crypto::hash::Hash;
    use pallas::ledger::addresses::Address;
    use pallas::ledger::primitives::conway::{PostAlonzoTransactionOutput, Value};
    use pallas::ledger::traverse::Era;
    use std::sync::Arc;
    use tower::util::ServiceExt as _;

    fn output_bytes() -> Vec<u8> {
        let output = PostAlonzoTransactionOutput {
            address: vec![0x60; 29].into(),
            value: Value::Coin(1_000_000),
            datum_option: None,
            script_ref: None,
        };

        minicbor::to_vec(&output).unwrap()
    }

    /// Pins the pruned-row ordering contract: no chain position means the
    /// `TxoRef` decides, deterministically, and the whole unknowable group
    /// sorts before any row with a known position.
    #[test]
    fn page_sort_key_orders_pruned_rows_by_txo_ref() {
        let bytes = output_bytes();
        fn output(b: &[u8]) -> MultiEraOutput<'_> {
            MultiEraOutput::decode(Era::Conway, b).unwrap()
        }
        let key = page_sort_key::<AddressUtxoContentInner>;

        let low = UtxoOutputModelBuilder::from_output(Hash::from([0xaa; 32]), 1, output(&bytes));
        let high = UtxoOutputModelBuilder::from_output(Hash::from([0xbb; 32]), 0, output(&bytes));
        let low_later =
            UtxoOutputModelBuilder::from_output(Hash::from([0xaa; 32]), 2, output(&bytes));

        // no block data: the TxoRef alone decides, tx hash before output index
        assert!(key(&low) < key(&high));
        assert!(key(&low) < key(&low_later));
        assert!(key(&low_later) < key(&high));

        // a known chain position sorts after the whole unknowable group,
        // regardless of its TxoRef
        let positioned =
            UtxoOutputModelBuilder::from_output(Hash::from([0x00; 32]), 0, output(&bytes))
                .with_block_data(BlockRefMeta {
                    slot: 1,
                    hash: Hash::from([0x11; 32]),
                    height: 1,
                    tx_hash: Hash::from([0x00; 32]),
                    tx_index: 0,
                });

        assert!(key(&high) < key(&positioned));
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
        assert_eq!(window.refs, vec![txo(0x11, 0), txo(0x12, 0), txo(0x13, 0)]);
        assert_eq!(window.skip, 0);

        // page 3 of count 2 covers 4..6: the tail of slot 10 and half of 20
        let window = select(&rows, pagination(Order::Asc, 2, 3));
        assert_eq!(
            window.refs,
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
        assert_eq!(window.refs, vec![txo(0x01, 0)]);
        assert_eq!(window.skip, 0);

        // descending mirrors the whole order, slot-less rows last
        let window = select(&rows, pagination(Order::Desc, 1, 2));
        assert_eq!(window.refs, vec![txo(0x21, 1), txo(0x21, 0)]);
        assert_eq!(window.skip, 1);

        let window = select(&rows, pagination(Order::Desc, 2, 4));
        assert_eq!(window.refs, vec![txo(0x01, 0)]);
        assert_eq!(window.skip, 0);

        // a page past the end is empty
        let window = select(&rows, pagination(Order::Asc, 100, 2));
        assert!(window.refs.is_empty());
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
