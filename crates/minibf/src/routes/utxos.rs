use axum::http::StatusCode;
use pallas::ledger::traverse::MultiEraOutput;
use std::{collections::HashSet, sync::Arc};

use dolos_cardano::indexes::{utxo_tags, AsyncCardanoQueryExt as _};
use dolos_core::{Domain, StateStore as _, Tag, TagOrder, TxoRef, UtxoValue};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::{Order, Pagination},
    Facade,
};

/// The outputs that a filtered UTxO listing returns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UtxoFilter {
    /// Only outputs without native assets, the rows that Blockfrost lists for
    /// the `lovelace` asset.
    AdaOnly,
    /// Only outputs that carry this live-UTxO tag: the other half of an
    /// intersection of two tags.
    Tagged(Tag),
}

impl UtxoFilter {
    /// Returns `true` when the listing returns `output`.
    ///
    /// `Tagged` derives the output's tags the way every tag write does, so it
    /// answers what reading the tag would.
    pub fn keeps(&self, output: &MultiEraOutput<'_>) -> bool {
        match self {
            UtxoFilter::AdaOnly => output
                .value()
                .assets()
                .iter()
                .all(|policy| policy.assets().is_empty()),
            UtxoFilter::Tagged(tag) => utxo_tags(output).contains(tag),
        }
    }
}

/// The first chunk of refs a filtered listing reads and decodes. Each further
/// chunk doubles, so the offset a tag read restarts from stays within the rows
/// already walked. Tests take two, so a fixture of a few rows crosses chunks.
const WALK_CHUNK: usize = if cfg!(test) { 2 } else { 256 };

/// The tag order a page order walks.
fn tag_order(order: &Order) -> TagOrder {
    match order {
        Order::Asc => TagOrder::Asc,
        Order::Desc => TagOrder::Desc,
    }
}

/// Reads one page of the refs that carry `tag`, in creation order.
async fn tag_page<D>(
    domain: &Facade<D>,
    tag: &Tag,
    order: TagOrder,
    offset: usize,
    count: usize,
) -> Result<Vec<TxoRef>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let tag = tag.clone();

    domain
        .blocking(move |domain| {
            domain
                .state()
                .utxos_by_tag_page(tag.dimension, &tag.key, order, offset, count)
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
    Ok(tag_page(domain, tag, TagOrder::Asc, 0, 1).await?.is_empty())
}

/// Loads one page of the UTxOs that carry `tag`, in creation order: slot, tx
/// index, output index, ascending or descending.
///
/// Only the page's refs are read from the tag, and only the page's UTxOs and
/// their blocks after that.
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
    let refs = tag_page(domain, tag, order, pagination.skip(), pagination.count).await?;
    let rows = load_values(domain, refs, None).await?;

    into_models(domain, rows).await
}

/// Where a filtered listing reads its refs from, in creation order.
pub enum UtxoWalk {
    /// A tag, read a chunk at a time.
    Tag(Tag),
    /// Refs already read in the page's order.
    Refs(Vec<TxoRef>),
}

/// Loads one page of the UTxOs of `walk` that `filter` keeps, in creation
/// order, and whether any of the walk's UTxOs passed the filter at all.
///
/// Rows are read and decoded in order until the page is past its offset and
/// full, or the walk ends, so the cost follows the rows walked, not the size
/// of the tag. Only the page's blocks are read.
pub async fn load_filtered_page<D, T>(
    domain: &Facade<D>,
    walk: UtxoWalk,
    filter: UtxoFilter,
    pagination: &Pagination,
) -> Result<(Vec<T>, bool), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    T: serde::Serialize,
    for<'a> UtxoOutputModelBuilder<'a>: IntoModel<T, SortKey = (u64, usize, u32)>,
{
    let order = tag_order(&pagination.order);
    let skip = pagination.skip();
    let count = pagination.count;

    let mut chunks = Chunks::new(walk);
    let mut rows = Vec::with_capacity(count);
    let mut kept = 0;
    // a block that lands between two chunk reads can shift a ref into the
    // next chunk, so a ref is counted once
    let mut seen = HashSet::new();

    while rows.len() < count {
        let Some(refs) = chunks.next(domain, order).await? else {
            break;
        };

        for row in load_values(domain, refs, Some(filter.clone())).await? {
            if !seen.insert(row.0.clone()) {
                continue;
            }

            if kept >= skip {
                rows.push(row);
            }

            kept += 1;

            if rows.len() == count {
                break;
            }
        }
    }

    Ok((into_models(domain, rows).await?, kept > 0))
}

/// The refs of a walk, in chunks that start at [`WALK_CHUNK`] and double.
struct Chunks {
    source: UtxoWalk,
    read: usize,
    size: usize,
    done: bool,
}

impl Chunks {
    fn new(source: UtxoWalk) -> Self {
        Self {
            source,
            read: 0,
            size: WALK_CHUNK,
            done: false,
        }
    }

    async fn next<D>(
        &mut self,
        domain: &Facade<D>,
        order: TagOrder,
    ) -> Result<Option<Vec<TxoRef>>, StatusCode>
    where
        D: Domain + Clone + Send + Sync + 'static,
    {
        if self.done {
            return Ok(None);
        }

        let refs = match &self.source {
            UtxoWalk::Tag(tag) => tag_page(domain, tag, order, self.read, self.size).await?,
            UtxoWalk::Refs(refs) => {
                let start = self.read.min(refs.len());
                let end = self.read.saturating_add(self.size).min(refs.len());
                refs[start..end].to_vec()
            }
        };

        self.done = refs.len() < self.size;
        self.read += refs.len();
        self.size = self.size.saturating_mul(2);

        Ok((!refs.is_empty()).then_some(refs))
    }
}

/// Reads two tags in `order`, from the start in windows that double, until
/// one of them ends before its window does. Returns that one, read whole,
/// first, and the other tag second.
///
/// Every read stops at its window, and the last window is under twice the
/// smaller tag's size, so neither tag is read much past the smaller one's end.
pub async fn smaller_tag<D>(
    domain: &Facade<D>,
    first: Tag,
    second: Tag,
    order: &Order,
) -> Result<(Vec<TxoRef>, Tag, Tag), StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let order = tag_order(order);
    let mut window = WALK_CHUNK;

    loop {
        let refs = tag_page(domain, &first, order, 0, window).await?;
        if refs.len() < window {
            return Ok((refs, first, second));
        }

        let refs = tag_page(domain, &second, order, 0, window).await?;
        if refs.len() < window {
            return Ok((refs, second, first));
        }

        window = window.saturating_mul(2);
    }
}

/// Reads the UTxOs behind `refs` on the blocking pool, in the order of `refs`,
/// keeping those that `filter` keeps when there is one. A ref whose UTxO was
/// spent since its tag was read is skipped; every output a filter reads must
/// decode.
async fn load_values<D>(
    domain: &Facade<D>,
    refs: Vec<TxoRef>,
    filter: Option<UtxoFilter>,
) -> Result<Vec<(TxoRef, Arc<UtxoValue>)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    domain
        .blocking(move |domain| {
            let mut utxos = domain
                .state()
                .get_utxos(refs.clone())
                .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

            let mut kept = Vec::with_capacity(refs.len());
            for txo_ref in refs {
                let Some(value) = utxos.remove(&txo_ref) else {
                    continue;
                };

                if let Some(filter) = &filter {
                    let output = MultiEraOutput::try_from(value.as_ref())
                        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

                    if !filter.keeps(&output) {
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
    use dolos_cardano::indexes::{utxo_index_delta_from_utxo_delta, CardanoStateIndexExt as _};
    use dolos_core::{
        async_query::BlockMetaResolver, EraCbor, MempoolStore as _, MempoolTx, StateWriter as _,
        UtxoSetDelta,
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

    /// Writes `outputs` as UTxOs no block produced, with their tags.
    fn configure(domain: &dolos_testing::toy_domain::ToyDomain, outputs: Vec<(TxoRef, EraCbor)>) {
        let delta = UtxoSetDelta {
            produced_utxo: outputs
                .into_iter()
                .map(|(txo, output)| (txo, Arc::new(UtxoValue::new(output, None))))
                .collect(),
            ..Default::default()
        };

        let writer = domain.state().start_writer().expect("state writer");
        writer.apply_utxoset(&delta).expect("apply utxos");
        writer
            .apply_utxo_tags(&utxo_index_delta_from_utxo_delta(&delta))
            .expect("apply tags");
        writer.commit().expect("commit");
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

    /// Two configured UTxOs on the listed address, one hashing below and one
    /// above every tx, so the no-position rows have a ref order to keep.
    fn chain_with_configured_rows() -> TestApp {
        let cfg = SyntheticBlockConfig {
            block_count: 4,
            txs_per_block: 3,
            ..Default::default()
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

            assert!(expected.len() > 12, "the fixture lists too few rows");
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
