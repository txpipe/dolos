use axum::http::StatusCode;
use std::sync::Arc;

use dolos_core::{Domain, Tag, TagStart, TxoRef, UtxoValue};

use crate::{
    mapping::{IntoModel, UtxoOutputModelBuilder},
    pagination::Pagination,
    Facade,
};

use super::{models::into_models, rows::Rows, tag_order, walk::Walk, MAX_WINDOW};

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
    let rows = tag_page_rows(domain, tag, pagination).await?;

    into_models(domain, rows, None).await
}

/// The rows of one page of `tag`, as [`load_tag_page`] lists them.
pub(super) async fn tag_page_rows<D>(
    domain: &Facade<D>,
    tag: &Tag,
    pagination: &Pagination,
) -> Result<Vec<(TxoRef, Arc<UtxoValue>)>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
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

    Ok(rows.rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        pagination::{Order, Pagination},
        test_support::TestApp,
        utxos::{testing::*, MAX_WINDOW},
        Facade,
    };
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use blockfrost_openapi::models::address_utxo_content_inner::AddressUtxoContentInner;
    use dolos_cardano::indexes::utxo_dimensions;
    use dolos_core::{
        config::MinibfConfig, import::ImportExt as _, ArchiveStore as _, ArchiveWriter as _,
        ChainPoint, Domain, EraCbor, MempoolStore as _, MempoolTx, StateStore, StateWriter as _,
        Tag, TagOrder, TagStart, TxoRef, UtxoSetDelta,
    };
    use dolos_testing::{
        measured::{MeasuredStores, WorkSnapshot},
        synthetic::{build_synthetic_blocks, SyntheticBlockConfig},
        toy_domain::{MemoryStores, ToyDomain, ToyStores as _},
    };
    use http_body_util::BodyExt as _;
    use pallas::ledger::{
        addresses::Address,
        traverse::{Era, MultiEraTx},
    };
    use std::sync::Arc;
    use tower::util::ServiceExt as _;

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

    fn pagination(order: Order, count: usize, page: u64) -> Pagination {
        Pagination {
            count,
            page,
            order,
            ..Default::default()
        }
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

    /// On a rolling-history node the archive index keeps a pruned block's
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

    #[tokio::test]
    async fn many_spent_refs_are_walked_once() {
        let block_count = 4 * MAX_WINDOW + 100;
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
        let tag = Tag::new(utxo_dimensions::ADDRESS, address);
        let facade = Facade {
            inner: domain,
            config: MinibfConfig::new("[::]:0".parse().unwrap()),
        };
        let entries = facade
            .state()
            .utxos_by_tag_page(
                tag.dimension,
                &tag.key,
                TagOrder::Asc,
                TagStart::Skip(0),
                usize::MAX,
            )
            .unwrap();
        assert_eq!(entries.len(), block_count);

        let spent: Vec<_> = entries[99..block_count - 1]
            .iter()
            .map(|entry| entry.txo.clone())
            .collect();
        let writer = facade.state().start_writer().unwrap();
        writer
            .apply_utxoset(&UtxoSetDelta {
                consumed_utxo: facade.state().get_utxos(spent).unwrap(),
                ..Default::default()
            })
            .unwrap();
        writer.commit().unwrap();

        for order in [Order::Asc, Order::Desc] {
            let mut live: Vec<_> = entries[..99]
                .iter()
                .chain(&entries[block_count - 1..])
                .map(|entry| entry.txo.to_string())
                .collect();
            if let Order::Desc = order {
                live.reverse();
            }

            for count in [99, 100] {
                let pagination = pagination(order, count, 1);
                let expected: Vec<_> = live
                    .iter()
                    .skip(pagination.skip())
                    .take(count)
                    .cloned()
                    .collect();
                facade.state().counters.reset();
                let actual: Vec<AddressUtxoContentInner> =
                    load_tag_page(&facade, &tag, &pagination).await.unwrap();
                let actual: Vec<_> = actual
                    .into_iter()
                    .map(|row| format!("{}#{}", row.tx_hash, row.output_index))
                    .collect();
                assert_eq!(actual, expected);
                assert_eq!(actual.len(), count);
                let work = facade.state().counters.snapshot();
                assert_eq!(
                    work.utxo_reads,
                    (count
                        + if count == 100 || matches!(order, Order::Desc) {
                            block_count - 100
                        } else {
                            0
                        }) as u64
                );
                assert_eq!(work.utxo_refs, work.utxo_reads);
                assert_eq!(work.block_reads, actual.len() as u64);
                assert_eq!(work.exact_lookups, 0);
                assert_eq!(work.tip_reads, 0);
            }
        }
    }

    /// The work one page does, whatever the address holds. A page of five
    /// rows over 20 UTxOs and over 200 reads the same rows and blocks, in
    /// both orders and past the first page.
    #[tokio::test]
    async fn a_page_costs_the_same_at_any_address_size() {
        async fn page_work(block_count: usize, query: &str) -> WorkSnapshot {
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

            domain.archive().counters.reset();
            let path = format!("/addresses/{}/utxos?{query}", vectors.address);
            let res = router
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(res.status(), StatusCode::OK);

            let bytes = res.into_body().collect().await.unwrap().to_bytes();
            let rows: Vec<AddressUtxoContentInner> = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(rows.len(), 5, "{query}");

            // block sizes grow with the slot, so the decoded bytes do too
            WorkSnapshot {
                decoded_bytes: 0,
                ..domain.archive().counters.snapshot()
            }
        }

        for query in [
            "count=5",
            "count=5&page=2",
            "count=5&order=desc",
            "count=5&page=2&order=desc",
        ] {
            let small = page_work(20, query).await;
            let large = page_work(200, query).await;

            assert_eq!(small.utxo_reads, 5, "{query}");
            assert_eq!(small.block_reads, 5, "{query}");
            assert_eq!(small, large, "{query}");
        }
    }
}
