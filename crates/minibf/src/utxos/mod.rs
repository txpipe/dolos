use dolos_core::TagOrder;

use crate::pagination::Order;

mod filtered;
mod models;
mod page;
mod rows;
mod slots;
mod walk;

pub use filtered::{load_filtered_page, smaller_tag, UtxoWalk};
pub use page::load_tag_page;
pub use slots::load_tag_page_in_slots;
pub use walk::tag_is_empty;

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

#[cfg(test)]
mod testing {
    use crate::test_support::TestApp;
    use axum::http::StatusCode;
    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
    use dolos_cardano::indexes::{
        utxo_dimensions, utxo_index_delta_from_utxo_delta, utxo_tags, CardanoStateIndexExt as _,
    };
    use dolos_core::{
        async_query::BlockMetaResolver, Domain, EraCbor, StateStore, StateWriter as _, Tag, TxoRef,
        UtxoIndexDelta, UtxoPosition, UtxoSetDelta, UtxoValue,
    };
    use dolos_testing::{synthetic::SyntheticBlockConfig, TestAddress, MIN_UTXO_AMOUNT};
    use pallas::{
        crypto::hash::Hash,
        ledger::{
            addresses::Address,
            primitives::conway::{PositiveCoin, Value},
            traverse::MultiEraOutput,
        },
    };
    use std::{collections::BTreeMap, sync::Arc};

    /// A listed row: the ref, the block that created it, the units it holds.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(super) struct Row {
        pub(super) txo: String,
        pub(super) block: String,
        pub(super) units: Vec<String>,
    }

    pub(super) async fn page_rows(app: &TestApp, path: &str) -> Vec<Row> {
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
    pub(super) async fn walk(app: &TestApp, path: &str, order: &str, count: usize) -> Vec<Row> {
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
    pub(super) async fn whole_set_rows(app: &TestApp, address: &str, order: &str) -> Vec<Row> {
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

    pub(super) fn without_units(rows: Vec<Row>) -> Vec<Row> {
        rows.into_iter()
            .map(|row| Row {
                units: Vec::new(),
                ..row
            })
            .collect()
    }

    /// Writes `utxos` with their tags, as a block that produces them does.
    pub(super) fn produce(state: &impl StateStore, utxos: Vec<(TxoRef, UtxoValue)>) {
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
    pub(super) fn configure(
        domain: &dolos_testing::toy_domain::ToyDomain,
        outputs: Vec<(TxoRef, EraCbor)>,
    ) {
        let utxos = outputs
            .into_iter()
            .map(|(txo, output)| (txo, UtxoValue::new(output, None)))
            .collect();

        produce(domain.state(), utxos);
    }

    pub(super) fn asset_output(address: TestAddress, policy: &str, name: &str) -> EraCbor {
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
    pub(super) fn chain_with_configured_rows() -> TestApp {
        chain_with_configured_rows_over(4)
    }

    /// [`chain_with_configured_rows`] over `block_count` blocks. Each block
    /// adds one row that holds the vectors' asset.
    pub(super) fn chain_with_configured_rows_over(block_count: usize) -> TestApp {
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

    pub(super) fn address_tag(app: &TestApp) -> Tag {
        let address = Address::from_bech32(&app.vectors().address).expect("bech32 address");
        Tag::new(utxo_dimensions::ADDRESS, address.to_vec())
    }

    /// Spends `txo` the way a block does: out of the set and out of its tags.
    pub(super) fn spend(state: &impl StateStore, txo: &TxoRef) {
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
    pub(super) fn tag_only<D: Domain>(
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
    pub(super) fn value_of(app: &TestApp, txo: &str) -> UtxoValue {
        let txo: TxoRef = txo.parse().expect("txo ref");
        let utxos = app
            .facade()
            .state()
            .get_utxos(vec![txo.clone()])
            .expect("utxo read");

        (*utxos[&txo]).clone()
    }
}
