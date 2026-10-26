use axum::http::StatusCode;
use pallas::ledger::traverse::MultiEraOutput;
use std::{collections::HashSet, sync::Arc};

use dolos_cardano::indexes::AsyncCardanoQueryExt as _;
use dolos_core::{Domain, StateStore as _, Tag, TagEntry, TagOrder, TagStart, TxoRef, UtxoValue};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

/// The first window a filtered walk reads. Each next one doubles, up to
/// [`MAX_WINDOW`]. Tests take two, so a fixture of a few rows crosses windows.
const FIRST_WINDOW: usize = if cfg!(test) { 2 } else { 256 };

/// The most tag entries, probes or UTxO values one blocking task returns, so a
/// walk holds a query permit for a few milliseconds at a time, as the scans
/// do. A tag read can still step over more keys than it returns, in its task:
///
/// - a plain listing's first read also steps over the page's offset, a tag key
///   per skipped row;
/// - a read under a lookup key that prefixes longer ones steps over theirs, as
///   [`StateStore::utxos_by_tag_page`](dolos_core::StateStore::utxos_by_tag_page)
///   says: an asset with an empty name over its policy's named assets, or
///   `SpaceBud1` over `SpaceBud10`.
///
/// Tests take four, so a fixture walk reaches it.
const MAX_WINDOW: usize = if cfg!(test) { 4 } else { 1024 };

/// The tag order a page order walks.
fn tag_order(order: &Order) -> TagOrder {
    match order {
        Order::Asc => TagOrder::Asc,
        Order::Desc => TagOrder::Desc,
    }
}

/// Reads up to `count` entries of `tag`, in creation order, from `start`.
async fn tag_page<D>(
    domain: &Facade<D>,
    tag: &Tag,
    order: TagOrder,
    start: TagStart,
    count: usize,
) -> Result<Vec<TagEntry>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    debug_assert!(count <= MAX_WINDOW);

    let tag = tag.clone();

    domain
        .blocking(move |domain| {
            domain
                .state()
                .utxos_by_tag_page(tag.dimension, &tag.key, order, start, count)
                .map_err(|err| {
                    tracing::error!(?err);
                    StatusCode::INTERNAL_SERVER_ERROR
                })
        })
        .await
}

/// Returns `true` when no live UTxO carries `tag`.
pub async fn tag_is_empty<D>(domain: &Facade<D>, tag: &Tag) -> Result<bool, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    Ok(tag_page(domain, tag, TagOrder::Asc, TagStart::Skip(0), 1)
        .await?
        .is_empty())
}

/// The entries of a listing, in creation order, read a window at a time.
///
/// Each read of a tag resumes just past the last entry the walk read, not at
/// a count of entries, so a UTxO spent or rolled back between two reads shifts
/// none of the entries still ahead.
///
/// Reads are not one snapshot, so a fork during the walk can hide a live row
/// from one response. When a rollback and a fork block re-include a
/// transaction at a position the walk has already passed, its UTxOs' entries
/// land behind the walk and no later read reaches them. The next request lists
/// them. A response never lists a UTxO that was spent when its value was read.
struct Walk {
    source: Source,
    order: TagOrder,
}

enum Source {
    Tag {
        tag: Tag,
        start: TagStart,
        done: bool,
    },
    Entries(std::vec::IntoIter<TagEntry>),
}

impl Walk {
    /// A walk of `tag` that starts at `start`.
    fn tag(tag: Tag, order: TagOrder, start: TagStart) -> Self {
        let source = Source::Tag {
            tag,
            start,
            done: false,
        };

        Self { source, order }
    }

    /// A walk of entries already read in the listing's order.
    fn entries(entries: Vec<TagEntry>, order: TagOrder) -> Self {
        let source = Source::Entries(entries.into_iter());

        Self { source, order }
    }

    /// The next `count` entries, fewer when the walk ends: none once it has.
    async fn next<D>(
        &mut self,
        domain: &Facade<D>,
        count: usize,
    ) -> Result<Vec<TagEntry>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        match &mut self.source {
            Source::Tag { done: true, .. } => Ok(Vec::new()),
            Source::Tag { tag, start, done } => {
                let entries = tag_page(domain, tag, self.order, start.clone(), count).await?;

                *done = entries.len() < count;

                if let Some(last) = entries.last() {
                    *start = TagStart::After(last.clone());
                }

                Ok(entries)
            }
            Source::Entries(entries) => Ok(entries.by_ref().take(count).collect()),
        }
    }
}

/// A page's rows, gathered in walk order.
///
/// A ref counts once: a fork that re-creates a UTxO at a later position
/// between two reads puts its entry ahead of the walk a second time. It counts
/// once its value loads, so an entry whose UTxO a rollback removed before the
/// value read leaves the ref to the entry the fork block writes.
struct Rows {
    rows: Vec<(TxoRef, Arc<UtxoValue>)>,
    counted: HashSet<TxoRef>,
    count: usize,
}

impl Rows {
    fn new(count: usize) -> Self {
        Self {
            rows: Vec::with_capacity(count),
            counted: HashSet::new(),
            count,
        }
    }

    fn full(&self) -> bool {
        self.rows.len() >= self.count
    }

    fn missing(&self) -> usize {
        self.count.saturating_sub(self.rows.len())
    }

    /// The entries whose ref the walk has not counted, each ref once.
    fn uncounted(&self, entries: Vec<TagEntry>) -> Vec<TagEntry> {
        let mut refs = HashSet::new();

        entries
            .into_iter()
            .filter(|entry| !self.counted.contains(&entry.txo) && refs.insert(entry.txo.clone()))
            .collect()
    }

    /// Counts the refs of `entries`.
    fn count_entries(&mut self, entries: &[TagEntry]) {
        self.counted
            .extend(entries.iter().map(|entry| entry.txo.clone()));
    }

    /// Loads the UTxOs behind the uncounted entries of `entries`, as
    /// [`load_values`] does, and counts the ones that load.
    async fn load<D>(
        &mut self,
        domain: &Facade<D>,
        entries: Vec<TagEntry>,
        ada_only: bool,
    ) -> Result<Vec<(TxoRef, Arc<UtxoValue>)>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        let loaded = load_values(domain, self.uncounted(entries), ada_only).await?;

        self.counted
            .extend(loaded.iter().map(|(txo, _)| txo.clone()));

        Ok(loaded)
    }
}

/// Loads one page of the UTxOs that carry `tag`, in creation order: slot, tx
/// index, output index, ascending or descending.
///
/// The tag is read from the page's offset, and only the page's UTxOs and their
/// blocks after that. The first read steps over the offset itself, a tag key
/// per skipped row. A UTxO spent between the tag read and its value read
/// leaves a gap that the next entries fill, so only the last page is short.
pub async fn load_tag_page<D, T>(
    domain: &Facade<D>,
    tag: &Tag,
    pagination: &Pagination,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let order = tag_order(&pagination.order);
    let mut walk = Walk::tag(tag.clone(), order, TagStart::Skip(pagination.skip()));
    let mut rows = Rows::new(pagination.count);

    while !rows.full() {
        let entries = walk.next(domain, rows.missing().min(MAX_WINDOW)).await?;

        if entries.is_empty() {
            break;
        }

        let loaded = rows.load(domain, entries, false).await?;
        rows.rows.extend(loaded);
    }

    into_models(domain, rows.rows).await
}

/// What a filtered listing walks, in creation order.
pub enum UtxoWalk {
    /// The UTxOs of a tag whose outputs hold no native asset, the rows that
    /// Blockfrost lists for the `lovelace` asset. Each row's value decides, so
    /// every walked row is read.
    AdaOnly(Tag),
    /// The UTxOs of `entries`, one tag read whole, that the tag `other` also
    /// holds. Each entry is probed in `other` by its full key, so a row that
    /// does not match costs a key read, not a value read.
    ///
    /// The probe looks for the position `entries` was read at. When a rollback
    /// and a fork block re-include a row's transaction at another position in
    /// between, the probe misses it and one response leaves the live row out,
    /// as a walk does for a row the fork moves behind it.
    Intersection { entries: Vec<TagEntry>, other: Tag },
}

/// Loads one page of the UTxOs `walk` keeps, in creation order, and whether
/// any of the walk's UTxOs passed its filter at all.
///
/// Rows are walked in windows until the page is past its offset and full, or
/// the walk ends, so the cost follows the rows walked, not the size of the
/// tag. Only the page's blocks are read.
pub async fn load_filtered_page<D, T>(
    domain: &Facade<D>,
    walk: UtxoWalk,
    pagination: &Pagination,
) -> Result<(Vec<T>, bool), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let order = tag_order(&pagination.order);

    let (mut source, other) = match walk {
        UtxoWalk::AdaOnly(tag) => (Walk::tag(tag, order, TagStart::Skip(0)), None),
        UtxoWalk::Intersection { entries, other } => (Walk::entries(entries, order), Some(other)),
    };

    let mut rows = Rows::new(pagination.count);
    let mut skip = pagination.skip();
    let mut matched = false;
    let mut window = FIRST_WINDOW;

    while !rows.full() {
        let entries = source.next(domain, window).await?;

        if entries.is_empty() {
            break;
        }

        window = window.saturating_mul(2).min(MAX_WINDOW);

        let Some(other) = &other else {
            for row in rows.load(domain, entries, true).await? {
                matched = true;

                if skip > 0 {
                    skip -= 1;
                } else {
                    rows.rows.push(row);

                    if rows.full() {
                        break;
                    }
                }
            }

            continue;
        };

        let held = probe(domain, other, entries).await?;
        matched |= !held.is_empty();

        // a row before the page counts on the probe's word, its value unread
        let mut held = rows.uncounted(held);
        let skipped = skip.min(held.len());
        skip -= skipped;

        let page = held.split_off(skipped);
        rows.count_entries(&held);

        // the page's values only, a UTxO spent since the probe giving its
        // place to the next entry
        let mut page = page.into_iter();

        while !rows.full() {
            let window: Vec<_> = page.by_ref().take(rows.missing()).collect();

            if window.is_empty() {
                break;
            }

            let loaded = rows.load(domain, window, false).await?;
            rows.rows.extend(loaded);
        }
    }

    Ok((into_models(domain, rows.rows).await?, matched))
}

/// The entries of `entries` that `tag` also holds, in order.
async fn probe<D>(
    domain: &Facade<D>,
    tag: &Tag,
    entries: Vec<TagEntry>,
) -> Result<Vec<TagEntry>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    debug_assert!(entries.len() <= MAX_WINDOW);

    let tag = tag.clone();

    domain
        .blocking(move |domain| {
            domain
                .state()
                .utxos_with_tag(tag.dimension, &tag.key, entries)
                .map_err(|err| {
                    tracing::error!(?err);
                    StatusCode::INTERNAL_SERVER_ERROR
                })
        })
        .await
}

/// Reads two tags in `order`, a window at a time, until one of them ends
/// before its window does. Returns that one's entries, read whole, then that
/// tag and the other.
///
/// Neither tag is read much past the smaller one's end, and every read takes
/// at most [`MAX_WINDOW`] entries. The entries are read before any probe, so a
/// fork in between can hide a row, as [`UtxoWalk::Intersection`] says.
pub async fn smaller_tag<D>(
    domain: &Facade<D>,
    first: Tag,
    second: Tag,
    order: &Order,
) -> Result<(Vec<TagEntry>, Tag, Tag), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let order = tag_order(order);
    let mut walks = [
        (
            Walk::tag(first.clone(), order, TagStart::Skip(0)),
            Vec::new(),
        ),
        (
            Walk::tag(second.clone(), order, TagStart::Skip(0)),
            Vec::new(),
        ),
    ];
    let mut window = FIRST_WINDOW;

    loop {
        for (i, (walk, entries)) in walks.iter_mut().enumerate() {
            let read = walk.next(domain, window).await?;
            let ended = read.len() < window;
            entries.extend(read);

            if ended {
                let entries = std::mem::take(entries);

                return Ok(match i {
                    0 => (entries, first, second),
                    _ => (entries, second, first),
                });
            }
        }

        window = window.saturating_mul(2).min(MAX_WINDOW);
    }
}

/// The output holds no native asset.
fn holds_no_asset(output: &MultiEraOutput<'_>) -> bool {
    output
        .value()
        .assets()
        .iter()
        .all(|policy| policy.assets().is_empty())
}

/// Reads the UTxOs behind `entries` on the blocking pool, in the order of
/// `entries`, keeping only those whose output holds no native asset when
/// `ada_only` is set. A UTxO spent since its entry was read is skipped; every
/// output the filter reads must decode.
async fn load_values<D>(
    domain: &Facade<D>,
    entries: Vec<TagEntry>,
    ada_only: bool,
) -> Result<Vec<(TxoRef, Arc<UtxoValue>)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    debug_assert!(entries.len() <= MAX_WINDOW);

    domain
        .blocking(move |domain| {
            let refs: Vec<TxoRef> = entries.into_iter().map(|entry| entry.txo).collect();

            let mut utxos = domain
                .state()
                .get_utxos(refs.clone())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let mut kept = Vec::with_capacity(refs.len());
            for txo_ref in refs {
                let Some(value) = utxos.remove(&txo_ref) else {
                    continue;
                };

                if ada_only {
                    let output = MultiEraOutput::try_from(value.as_ref())
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    if !holds_no_asset(&output) {
                        continue;
                    }
                }

                kept.push((txo_ref, value));
            }

            Ok(kept)
        })
        .await
}

/// The models of a page's rows, in the rows' order. The block hash comes from
/// the block at each row's creation position; a row without one (genesis,
/// configured) or whose block was pruned gets none.
///
/// The lookup and the value read are not one snapshot. When a rollback removes
/// a row's block after its value was read, the row lists without a block hash
/// for one response, unless a fork block at that slot already carries its
/// transaction at the same index. A request after the fork reads the row's new
/// position and its block.
async fn into_models<D, T>(
    domain: &Facade<D>,
    rows: Vec<(TxoRef, Arc<UtxoValue>)>,
) -> Result<Vec<T>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let located = rows
        .iter()
        .filter_map(|(txo_ref, value)| Some((txo_ref.0, value.position()?)))
        .collect::<Vec<_>>();

    let block_deps = domain
        .query()
        .block_meta_by_positions(located)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut out = Vec::with_capacity(rows.len());
    for (txo_ref, value) in rows {
        let output = MultiEraOutput::try_from(value.as_ref())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        let TxoRef(tx_hash, txo_idx) = txo_ref;
        let builder = UtxoOutputModelBuilder::from_output(tx_hash, txo_idx, output);

        let builder = if let Some(x) = block_deps.get(&tx_hash).cloned() {
            builder.with_block_data(x)
        } else {
            builder
        };

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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
    use dolos_cardano::indexes::{
        utxo_dimensions, utxo_index_delta_from_utxo_delta, utxo_tags, CardanoStateIndexExt as _,
    };
    use dolos_core::{
        async_query::BlockMetaResolver, EraCbor, MempoolStore as _, MempoolTx, StateStore,
        StateWriter as _, UtxoIndexDelta, UtxoPosition, UtxoSetDelta,
    };
    use dolos_testing::{synthetic::SyntheticBlockConfig, TestAddress, MIN_UTXO_AMOUNT};
    use pallas::{
        crypto::hash::Hash,
        ledger::{
            addresses::Address,
            primitives::conway::{PositiveCoin, Value},
            traverse::{Era, MultiEraTx},
        },
    };

    use crate::test_support::TestApp;

    use super::*;

    /// A listed row: the ref, the block that created it, the units it holds.
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct Row {
        txo: String,
        block: String,
        units: Vec<String>,
    }

    async fn page_rows(app: &TestApp, path: &str) -> Vec<Row> {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "{path}: {}",
            String::from_utf8_lossy(&bytes)
        );

        let page: Vec<AddressUtxoContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse address utxos");

        page.into_iter()
            .map(|x| Row {
                txo: format!("{}#{}", x.tx_hash, x.output_index),
                block: x.block,
                units: x.amount.into_iter().map(|amount| amount.unit).collect(),
            })
            .collect()
    }

    /// Every row of `path` in `order`, `count` at a time: the walk ends at the
    /// first short page.
    async fn walk(app: &TestApp, path: &str, order: &str, count: usize) -> Vec<Row> {
        let mut rows = Vec::new();

        for page in 1.. {
            let found = page_rows(
                app,
                &format!("{path}?order={order}&count={count}&page={page}"),
            )
            .await;
            let short = found.len() < count;
            rows.extend(found);

            if short {
                return rows;
            }
        }

        unreachable!()
    }

    /// The rows the listing served before UTxOs stored their position: the
    /// whole tag, every tx's block found through the exact index, sorted by
    /// slot, tx index and output index, with the rows no block holds first by
    /// ref.
    async fn whole_set_rows(app: &TestApp, address: &str, order: &str) -> Vec<Row> {
        let facade = app.facade();
        let address = Address::from_bech32(address).expect("bech32 address");
        let refs = facade
            .state()
            .utxos_by_address(&address.to_vec())
            .expect("address refs");

        let blocks = BlockMetaResolver::new(facade.query())
            .resolve_batch(refs.iter().map(|txo| txo.0))
            .await
            .expect("block metadata");

        let mut rows: Vec<_> = refs
            .into_iter()
            .map(|TxoRef(hash, idx)| {
                let meta = blocks.get(&hash);
                let key = (meta.map(|meta| (meta.slot, meta.tx_index, idx)), hash, idx);
                let block = meta.map(|meta| meta.hash.to_string()).unwrap_or_default();

                (key, format!("{hash}#{idx}"), block)
            })
            .collect();

        rows.sort_by(|a, b| a.0.cmp(&b.0));
        if order == "desc" {
            rows.reverse();
        }

        rows.into_iter()
            .map(|(_, txo, block)| (txo, block))
            .map(|(txo, block)| Row {
                txo,
                block,
                units: Vec::new(),
            })
            .collect()
    }

    fn without_units(rows: Vec<Row>) -> Vec<Row> {
        rows.into_iter()
            .map(|row| Row {
                units: Vec::new(),
                ..row
            })
            .collect()
    }

    /// Writes `utxos` with their tags, as a block that produces them does.
    fn produce(state: &impl StateStore, utxos: Vec<(TxoRef, UtxoValue)>) {
        let delta = UtxoSetDelta {
            produced_utxo: utxos
                .into_iter()
                .map(|(txo, value)| (txo, Arc::new(value)))
                .collect(),
            ..Default::default()
        };

        let writer = state.start_writer().expect("state writer");
        writer.apply_utxoset(&delta).expect("apply utxos");
        writer
            .apply_utxo_tags(&utxo_index_delta_from_utxo_delta(&delta))
            .expect("apply tags");
        writer.commit().expect("commit");
    }

    /// Writes `outputs` as UTxOs no block produced, with their tags.
    fn configure(domain: &dolos_testing::toy_domain::ToyDomain, outputs: Vec<(TxoRef, EraCbor)>) {
        let utxos = outputs
            .into_iter()
            .map(|(txo, output)| (txo, UtxoValue::new(output, None)))
            .collect();

        produce(domain.state(), utxos);
    }

    fn asset_output(address: TestAddress, policy: &str, name: &str) -> EraCbor {
        let policy: [u8; 28] = hex::decode(policy)
            .expect("hex policy")
            .try_into()
            .expect("28-byte policy");

        let assets = BTreeMap::from([(
            Hash::from(policy),
            BTreeMap::from([(
                pallas::codec::utils::Bytes::from(name.as_bytes().to_vec()),
                PositiveCoin::try_from(5).expect("positive"),
            )]),
        )]);

        dolos_testing::utxo_with_value(address, Value::Multiasset(MIN_UTXO_AMOUNT, assets))
    }

    /// Every tx of every block also pays the listed address twice, so rows
    /// share a block at several tx indexes and a tx at several output
    /// indexes. Two configured UTxOs on the address, one hashing below and one
    /// above every tx, give the no-position rows a ref order to keep.
    fn chain_with_configured_rows() -> TestApp {
        chain_with_configured_rows_over(4)
    }

    /// [`chain_with_configured_rows`] over `block_count` blocks. Each block
    /// adds one row that holds the vectors' asset.
    fn chain_with_configured_rows_over(block_count: usize) -> TestApp {
        let base = SyntheticBlockConfig::default();
        let address = Address::from_bech32(&base.address)
            .expect("bech32 address")
            .to_vec();
        let cfg = SyntheticBlockConfig {
            block_count,
            txs_per_block: 3,
            extra_outputs_by_block: vec![vec![vec![address.clone(), address]; 3]; block_count],
            ..base
        };

        TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let address = TestAddress::Custom(vectors.address.clone());

            configure(
                domain,
                vec![
                    (
                        TxoRef(Hash::from([0xff; 32]), 0),
                        dolos_testing::utxo_with_value(address.clone(), Value::Coin(3_000_000)),
                    ),
                    (
                        TxoRef(Hash::from([0x00; 32]), 1),
                        dolos_testing::utxo_with_value(address, Value::Coin(4_000_000)),
                    ),
                ],
            );
        })
    }

    /// Paged from the ordered tag read, a listing serves the rows and the
    /// block hashes the whole-set listing served, in the same order, for
    /// every page size and both orders.
    #[tokio::test]
    async fn pages_follow_the_whole_set_order() {
        let app = chain_with_configured_rows();
        let address = app.vectors().address.clone();
        let path = format!("/addresses/{address}/utxos");

        for order in ["asc", "desc"] {
            let expected = whole_set_rows(&app, &address, order).await;

            assert!(expected.len() > 24, "the fixture lists too few rows");
            assert!(expected.iter().any(|row| row.block.is_empty()));
            assert!(expected.iter().any(|row| !row.block.is_empty()));

            for count in [1, 2, 3, 5, 100] {
                let walked = without_units(walk(&app, &path, order, count).await);
                assert_eq!(walked, expected, "order={order} count={count}");
            }
        }
    }

    /// An asset listing is the address listing narrowed to the rows that hold
    /// the asset, whichever of the two tags is the smaller one to walk.
    #[tokio::test]
    async fn asset_listings_narrow_the_address_listing() {
        let cfg = SyntheticBlockConfig {
            block_count: 6,
            txs_per_block: 1,
            asset_names_by_block: ["A", "B", "A", "B", "A", "B"].map(String::from).to_vec(),
            ..Default::default()
        };

        // Alice holds one UTxO of asset A, which three of the chain's outputs
        // hold too: her tag is the smaller one there, the asset's is on the
        // chain's address
        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            configure(
                domain,
                vec![
                    (
                        TxoRef(Hash::from([0xa1; 32]), 0),
                        asset_output(TestAddress::Alice, &vectors.policy_id, "A"),
                    ),
                    (
                        TxoRef(Hash::from([0xa1; 32]), 1),
                        dolos_testing::utxo_with_value(TestAddress::Alice, Value::Coin(2_000_000)),
                    ),
                ],
            );
        });

        let unit = format!("{}{}", app.vectors().policy_id, hex::encode("A"));
        let alice = Address::from_bytes(&TestAddress::Alice.to_bytes())
            .expect("alice address")
            .to_string();

        for address in [app.vectors().address.clone(), alice] {
            for order in ["asc", "desc"] {
                let listed: Vec<Row> =
                    walk(&app, &format!("/addresses/{address}/utxos"), order, 100)
                        .await
                        .into_iter()
                        .filter(|row| row.units.contains(&unit))
                        .collect();

                assert!(!listed.is_empty(), "{address} holds none of the asset");

                for count in [1, 2, 100] {
                    let path = format!("/addresses/{address}/utxos/{unit}");
                    assert_eq!(
                        walk(&app, &path, order, count).await,
                        listed,
                        "{address} order={order} count={count}"
                    );
                }
            }
        }
    }

    fn address_tag(app: &TestApp) -> Tag {
        let address = Address::from_bech32(&app.vectors().address).expect("bech32 address");
        Tag::new(utxo_dimensions::ADDRESS, address.to_vec())
    }

    /// Spends `txo` the way a block does: out of the set and out of its tags.
    fn spend(state: &impl StateStore, txo: &TxoRef) {
        let consumed_utxo = state.get_utxos(vec![txo.clone()]).expect("utxo read");
        assert_eq!(consumed_utxo.len(), 1, "{txo} is live");

        let delta = UtxoSetDelta {
            consumed_utxo,
            ..Default::default()
        };

        let writer = state.start_writer().expect("state writer");
        writer.apply_utxoset(&delta).expect("apply utxos");
        writer
            .apply_utxo_tags(&utxo_index_delta_from_utxo_delta(&delta))
            .expect("apply tags");
        writer.commit().expect("commit");
    }

    /// Tags `output` under `txo` at `position` and leaves the set alone: what
    /// a listing finds when the UTxO is spent between its tag read and its
    /// value read.
    fn tag_only<D: Domain>(
        domain: &D,
        txo: TxoRef,
        position: Option<UtxoPosition>,
        output: &UtxoValue,
    ) {
        let output = MultiEraOutput::try_from(output).expect("output");
        let delta = UtxoIndexDelta {
            produced: vec![(txo, position, utxo_tags(&output))],
            consumed: vec![],
        };

        let writer = domain.state().start_writer().expect("state writer");
        writer.apply_utxo_tags(&delta).expect("apply tags");
        writer.commit().expect("commit");
    }

    /// The value of the listed row `txo`.
    fn value_of(app: &TestApp, txo: &str) -> UtxoValue {
        let txo: TxoRef = txo.parse().expect("txo ref");
        let utxos = app
            .facade()
            .state()
            .get_utxos(vec![txo.clone()])
            .expect("utxo read");

        (*utxos[&txo]).clone()
    }

    /// A walk resumes just past the last entry it read. A row spent behind it
    /// moves nothing ahead: a read at a count of entries would pass over the
    /// row the spend shifted into its place.
    #[tokio::test]
    async fn walks_resume_past_the_last_entry_read() {
        for order in [TagOrder::Asc, TagOrder::Desc] {
            let app = chain_with_configured_rows();
            let facade = app.facade();
            let tag = address_tag(&app);

            let all = facade
                .state()
                .utxos_by_tag_page(
                    tag.dimension,
                    &tag.key,
                    order,
                    TagStart::Skip(0),
                    usize::MAX,
                )
                .expect("whole tag");
            assert!(all.len() > 6, "the fixture lists too few rows");

            let mut walk = Walk::tag(tag, order, TagStart::Skip(0));

            let first = walk.next(&facade, 3).await.expect("first read");
            assert_eq!(first, all[..3], "{order:?}");

            spend(facade.inner.state(), &first[0].txo);

            let next = walk.next(&facade, 3).await.expect("second read");
            assert_eq!(next, all[3..6], "{order:?}: the walk skipped a row");
        }
    }

    /// A row whose UTxO is gone by the time its value is read gives its place
    /// to the next one, at either end of the tag, so a page that is not the
    /// last is full, on the plain listing and on an asset's.
    #[tokio::test]
    async fn pages_fill_past_rows_spent_since_the_tag_read() {
        let app = chain_with_configured_rows();
        let address = app.vectors().address.clone();
        let unit = app.vectors().asset_unit.clone();

        let asc = whole_set_rows(&app, &address, "asc").await;
        let desc = whole_set_rows(&app, &address, "desc").await;
        let with_asset = walk(&app, &format!("/addresses/{address}/utxos"), "asc", 100)
            .await
            .into_iter()
            .filter(|row| row.units.contains(&unit))
            .collect::<Vec<_>>();
        assert!(with_asset.len() > 3, "the fixture holds too few asset rows");

        // gone rows at the head of the tag in each order, holding the asset
        let output = value_of(&app, &with_asset[0].txo);
        let domain = app.facade().inner;
        tag_only(&domain, TxoRef(Hash::from([0x01; 32]), 9), None, &output);
        tag_only(
            &domain,
            TxoRef(Hash::from([0x02; 32]), 0),
            Some(UtxoPosition::new(u64::MAX, 0)),
            &output,
        );

        for count in [1, 3, 4] {
            let path = format!("/addresses/{address}/utxos?count={count}");

            let rows = without_units(page_rows(&app, &format!("{path}&order=asc")).await);
            assert_eq!(rows, asc[..count], "asc count={count}");

            let rows = without_units(page_rows(&app, &format!("{path}&order=desc")).await);
            assert_eq!(rows, desc[..count], "desc count={count}");

            let path = format!("/addresses/{address}/utxos/{unit}?count={count}&order=asc");
            assert_eq!(page_rows(&app, &path).await, with_asset[..count]);
        }
    }

    /// A ref the tag holds at two positions, as a fork that re-creates a UTxO
    /// later in the chain leaves it between two reads, is listed once.
    #[tokio::test]
    async fn a_ref_reached_twice_is_one_row() {
        let app = chain_with_configured_rows();
        let address = app.vectors().address.clone();

        let asc = whole_set_rows(&app, &address, "asc").await;
        let lovelace_path = format!("/addresses/{address}/utxos/lovelace?order=asc&count=100");
        let lovelace = page_rows(&app, &lovelace_path).await;
        assert!(
            lovelace.len() > 2,
            "the fixture lists too few lovelace rows"
        );

        let listed = &lovelace[1].txo;
        let domain = app.facade().inner;
        tag_only(
            &domain,
            listed.parse().expect("txo ref"),
            Some(UtxoPosition::new(u64::MAX, 0)),
            &value_of(&app, listed),
        );

        let path = format!("/addresses/{address}/utxos?order=asc&count=100");
        assert_eq!(without_units(page_rows(&app, &path).await), asc);
        assert_eq!(page_rows(&app, &lovelace_path).await, lovelace);
    }

    /// A ref counts once its value loads. A rollback that removes a UTxO
    /// between its tag read and its value read leaves the ref uncounted, so
    /// the entry the fork block writes for it further on is a row.
    #[tokio::test]
    async fn a_ref_counts_once_its_value_loads() {
        let app = chain_with_configured_rows();
        let facade = app.facade();

        let mut walk = Walk::tag(address_tag(&app), TagOrder::Asc, TagStart::Skip(0));
        let mut rows = Rows::new(100);

        let first = walk.next(&facade, MAX_WINDOW).await.expect("first read");
        let gone = first
            .iter()
            .find(|entry| entry.order != UtxoPosition::tag_order(None))
            .expect("a block row in the first read")
            .txo
            .clone();
        let value = value_of(&app, &gone.to_string());

        spend(facade.inner.state(), &gone);

        let loaded = rows
            .load(&facade, first, false)
            .await
            .expect("first values");
        assert!(loaded.iter().all(|(txo, _)| *txo != gone));

        let position = Some(UtxoPosition::new(u64::MAX, 0));
        produce(
            facade.inner.state(),
            vec![(gone.clone(), UtxoValue::new(value.output, position))],
        );

        let mut listed = false;

        loop {
            let next = walk.next(&facade, MAX_WINDOW).await.expect("next read");

            if next.is_empty() {
                break;
            }

            let loaded = rows.load(&facade, next, false).await.expect("next values");
            listed |= loaded.iter().any(|(txo, _)| *txo == gone);
        }

        assert!(listed, "the re-created UTxO is no row");
    }

    /// Through the routes, a rollback between the tag read and the value read
    /// of the window that holds a row, then a fork block that re-creates the
    /// row past the walk, moves the row to the end of the plain listing and of
    /// the `lovelace` walk: each counts a ref only once its value loads.
    #[tokio::test]
    async fn listings_count_a_ref_once_its_value_loads() {
        for listing in ["", "/lovelace"] {
            let app = chain_with_configured_rows();
            let address = app.vectors().address.clone();
            let path = format!("/addresses/{address}/utxos{listing}?order=asc&count=100");

            let before: Vec<String> = page_rows(&app, &path)
                .await
                .into_iter()
                .map(|row| row.txo)
                .collect();
            let moved = before
                .iter()
                .find(|txo| value_of(&app, txo).position().is_some())
                .expect("a block row")
                .clone();

            let txo: TxoRef = moved.parse().expect("txo ref");
            let position = Some(UtxoPosition::new(u64::MAX, 0));
            let mut fork = Some(UtxoValue::new(value_of(&app, &moved).output, position));
            let mut rolled_back = false;

            app.facade().inner.after_tag_page(move |entries, state| {
                if !rolled_back {
                    if entries.iter().any(|entry| entry.txo == txo) {
                        spend(state, &txo);
                        rolled_back = true;
                    }
                } else if let Some(value) = fork.take() {
                    produce(state, vec![(txo.clone(), value)]);
                }
            });

            let after: Vec<String> = page_rows(&app, &path)
                .await
                .into_iter()
                .map(|row| row.txo)
                .collect();

            let mut expected: Vec<String> =
                before.into_iter().filter(|txo| *txo != moved).collect();
            expected.push(moved);

            assert_eq!(after, expected, "listing {listing:?}");
        }
    }

    /// Both of an asset listing's tags are read a window at a time, each read
    /// within [`MAX_WINDOW`], which `tag_page` asserts: these tags outlast the
    /// windows a schedule that doubled past it would read.
    #[tokio::test]
    async fn asset_listings_read_both_tags_within_the_window() {
        let app = chain_with_configured_rows_over(2 * (FIRST_WINDOW + MAX_WINDOW));
        let address = app.vectors().address.clone();
        let unit = app.vectors().asset_unit.clone();

        let rows = walk(&app, &format!("/addresses/{address}/utxos"), "asc", 100).await;
        let with_asset: Vec<Row> = rows
            .iter()
            .filter(|row| row.units.contains(&unit))
            .cloned()
            .collect();

        let past_the_cap = FIRST_WINDOW + MAX_WINDOW;
        assert!(
            with_asset.len() > past_the_cap,
            "the asset tag is too small"
        );
        assert!(rows.len() > past_the_cap, "the address tag is too small");

        for count in [1, 100] {
            let path = format!("/addresses/{address}/utxos/{unit}");
            assert_eq!(walk(&app, &path, "asc", count).await, with_asset);
        }
    }

    /// Listings read the confirmed set, as Blockfrost's do: a pending tx that
    /// spends a listed UTxO and pays the same address changes no row.
    #[tokio::test]
    async fn pending_txs_leave_the_listing_alone() {
        let app = TestApp::new();
        let cbor = app.vectors().tx_cbor.clone();
        let tx = MultiEraTx::decode_for_era(Era::Conway, &cbor).expect("submit tx");

        let spent = TxoRef::from(&tx.consumes()[0]);
        let pays = tx.produces()[0].1.address().expect("output address");
        let path = format!("/addresses/{pays}/utxos");

        let before = walk(&app, &path, "asc", 100).await;
        assert!(
            before.iter().any(|row| row.txo == spent.to_string()),
            "the pending tx spends a listed UTxO"
        );

        app.facade()
            .mempool()
            .receive(MempoolTx::new(
                tx.hash(),
                EraCbor(Era::Conway.into(), cbor.clone()),
                vec![],
            ))
            .expect("mempool accepts the tx");
        assert!(app.facade().mempool().has_pending());

        for order in ["asc", "desc"] {
            let mut expected = before.clone();
            if order == "desc" {
                expected.reverse();
            }

            assert_eq!(walk(&app, &path, order, 1).await, expected, "order={order}");
        }
    }
}
