//! `/assets/{asset}/utxos`: the live UTxOs holding an asset.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset_utxo_content_inner::AssetUtxoContentInner;
use dolos_cardano::indexes::CardanoStateIndexExt as _;
use dolos_cardano::model::AssetState;
use dolos_core::Domain;

use super::resolve_asset_state;
use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

/// `GET /assets/{subject}/utxos`
///
/// Live UTxOs that carry the asset, in chain order. `from` / `to` bound the
/// creation block height, inclusive; an `:index` on them is validated but,
/// as on Blockfrost, cuts nothing. A valid but unknown asset is a 404; a known
/// asset that no live UTxO holds any more (fully burned) is an empty page.
/// UTxOs whose creation block was pruned by `sync.max_history` are not listed:
/// the row requires block data the node no longer has.
///
/// `max_scan_items` caps the page depth, as on `/assets` and
/// `/assets/{asset}/transactions`. It is a depth guard only: every UTxO of
/// the asset still costs one index lookup, whatever the page.
pub async fn by_subject_utxos<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items())?;
    let (asset, _) = resolve_asset_state(&domain, &subject)?;

    let refs = domain
        .state()
        .utxos_by_asset(&asset)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let utxos =
        crate::routes::utxos::load_utxo_models_in_height_range(&domain, refs, pagination).await?;

    Ok(Json(utxos))
}

#[cfg(test)]
mod tests {
    use super::super::testing::*;
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use dolos_testing::synthetic::SyntheticBlockConfig;

    fn parse_utxos(bytes: &[u8]) -> Vec<AssetUtxoContentInner> {
        serde_json::from_slice(bytes).expect("failed to parse asset utxos")
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/utxos");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let items = parse_utxos(&bytes);
        assert!(!items.is_empty());

        for item in items {
            // every UTxO must carry the asset and sit where the chain put it
            assert!(item.amount.iter().any(|x| x.unit == asset));
            assert!(item.amount.iter().any(|x| x.unit == "lovelace"));
            let (block_number, _) = app.vectors().tx_position(&item.tx_hash);
            assert_eq!(item.block_height as u64, block_number);
            assert!(!item.block.is_empty());
            assert!(item.block_time > 0);
            assert_eq!(item.inline_datum, None);
            assert_eq!(item.inline_datum_json, None);
        }
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_paginated() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        let (status_1, bytes_1) = app
            .get_bytes(&format!("/assets/{asset}/utxos?page=1&count=1"))
            .await;
        let (status_2, bytes_2) = app
            .get_bytes(&format!("/assets/{asset}/utxos?page=2&count=1"))
            .await;

        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1 = parse_utxos(&bytes_1);
        let page_2 = parse_utxos(&bytes_2);
        assert_eq!(page_1.len(), 1);
        assert_eq!(page_2.len(), 1);

        let key = |x: &AssetUtxoContentInner| format!("{}#{}", x.tx_hash, x.output_index);
        assert_ne!(key(&page_1[0]), key(&page_2[0]));
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_order() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?order=asc"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let asc = parse_utxos(&bytes);

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?order=desc"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let desc = parse_utxos(&bytes);

        assert!(asc.len() > 1);
        let position = |x: &AssetUtxoContentInner| {
            let (_, tx_index) = app.vectors().tx_position(&x.tx_hash);
            (x.block_height, tx_index, x.output_index)
        };
        let asc_pos: Vec<_> = asc.iter().map(position).collect();
        assert!(asc_pos.windows(2).all(|w| w[0] < w[1]));

        let mut reversed = asc;
        reversed.reverse();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_height_constrained() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/utxos")).await;
        assert_eq!(status, StatusCode::OK);
        let all = parse_utxos(&bytes);
        let heights: std::collections::BTreeSet<_> = all.iter().map(|x| x.block_height).collect();
        assert!(heights.len() > 1, "need UTxOs in more than one block");
        let pivot = *heights.iter().nth(1).expect("second height");

        // inclusive on both ends
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?from={pivot}&to={pivot}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let within = parse_utxos(&bytes);
        assert!(!within.is_empty());
        assert!(within.iter().all(|x| x.block_height == pivot));

        // one-sided bounds
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?from={pivot}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let from_pivot = parse_utxos(&bytes);
        assert!(from_pivot.iter().all(|x| x.block_height >= pivot));
        assert!(from_pivot.len() < all.len());

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?to={pivot}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let to_pivot = parse_utxos(&bytes);
        assert!(to_pivot.iter().all(|x| x.block_height <= pivot));
        assert_eq!(from_pivot.len() + to_pivot.len(), all.len() + within.len());

        // a range past the tip is an empty page, not an error
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?from=99999999"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(parse_utxos(&bytes).is_empty());
    }

    /// Blockfrost reads `from` / `to` here as plain heights: an `:index` is
    /// validated, a reversed pair is still a 400, but it cuts nothing inside
    /// the edge block.
    #[tokio::test]
    async fn assets_by_subject_utxos_index_cuts_nothing() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let get = |query: String| {
            let app = &app;
            async move {
                let (status, bytes) = app
                    .get_bytes(&format!("/assets/{asset}/utxos?{query}"))
                    .await;
                assert_eq!(status, StatusCode::OK, "{query}");
                parse_utxos(&bytes)
            }
        };

        let all = get(String::new()).await;
        let height = all[all.len() / 2].block_height;
        let in_block: Vec<_> = all
            .iter()
            .filter(|x| x.block_height == height)
            .cloned()
            .collect();
        assert!(in_block.len() > 1, "need a block with several rows");

        for query in [
            format!("from={height}:2&to={height}"),
            format!("from={height}&to={height}:0"),
            format!("from={height}:1&to={height}:1"),
        ] {
            assert_eq!(get(query).await, in_block);
        }

        let path = format!("/assets/{asset}/utxos?from={height}:2&to={height}:1");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    /// A bound Blockfrost does not read as one (more than two `:` parts) is
    /// ignored, not rejected; empty parts default.
    #[tokio::test]
    async fn assets_by_subject_utxos_bounds_parse_like_blockfrost() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let get = |query: &str| {
            let app = &app;
            let path = format!("/assets/{asset}/utxos?{query}");
            async move {
                let (status, bytes) = app.get_bytes(&path).await;
                assert_eq!(status, StatusCode::OK, "{path}");
                parse_utxos(&bytes)
            }
        };

        let all = get("").await;
        let height = all.last().expect("rows").block_height;

        assert_eq!(get("from=10:2:garbage").await, all);
        assert_eq!(get("to=1:2:3&order=asc").await, all);
        assert_eq!(
            get(&format!("from={height}:")).await,
            get(&format!("from={height}")).await
        );
        assert_eq!(get("from=:2").await, all);
    }

    /// A Byron epoch-boundary block takes the height of the main block
    /// before it and the slot of the main block after it, and the index keeps
    /// the boundary block's slot for that height. Simulated here by pointing
    /// height `h` at the slot of `h + 1`: both bounds still cut at the exact
    /// heights, including the heights on either side of `h`.
    #[tokio::test]
    async fn assets_by_subject_utxos_height_bounds_across_a_byron_boundary() {
        use dolos_core::{indexes::ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _};

        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        let reference = TestApp::new_with_cfg_and_setup(cfg.clone(), |_, _| {});
        let asset = reference.vectors().asset_unit.clone();
        let (status, bytes) = reference.get_bytes(&format!("/assets/{asset}/utxos")).await;
        assert_eq!(status, StatusCode::OK);
        let all = parse_utxos(&bytes);

        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let (ebb, next) = (&vectors.blocks[1], &vectors.blocks[2]);
            let writer = domain.archive().start_writer().unwrap();
            writer
                .apply_index(&[ArchiveIndexDelta {
                    slot: next.slot,
                    block_number: Some(ebb.block_number),
                    ..Default::default()
                }])
                .unwrap();
            writer.commit().unwrap();

            // the index now answers the boundary block's slot for `h`
            let slot = domain
                .archive()
                .slot_by_block_number(ebb.block_number)
                .unwrap();
            assert_eq!(slot, Some(next.slot));
        });

        let h = app.vectors().blocks[1].block_number as i32;
        assert!(all.iter().any(|x| x.block_height == h - 1));
        assert!(all.iter().any(|x| x.block_height == h));
        assert!(all.iter().any(|x| x.block_height == h + 1));

        // each query and the heights it has to select
        let cases = [
            (format!("from={h}"), h..=i32::MAX),
            (format!("to={h}"), i32::MIN..=h),
            (format!("from={h}&to={h}"), h..=h),
            (format!("from={}", h + 1), h + 1..=i32::MAX),
            (format!("to={}", h + 1), i32::MIN..=h + 1),
            (format!("to={}", h - 1), i32::MIN..=h - 1),
            (format!("from={}&to={h}", h - 1), h - 1..=h),
            (format!("from={h}:2&to={h}:2"), h..=h),
        ];

        for (query, heights) in cases {
            let (status, bytes) = app
                .get_bytes(&format!("/assets/{asset}/utxos?{query}"))
                .await;
            assert_eq!(status, StatusCode::OK, "{query}");

            let expected: Vec<_> = all
                .iter()
                .filter(|x| heights.contains(&x.block_height))
                .cloned()
                .collect();
            assert!(!expected.is_empty(), "{query} selects nothing");
            assert_eq!(parse_utxos(&bytes), expected, "{query}");
        }
    }

    /// A block pruned between the slot lookup and the block read leaves its
    /// rows without block data. Simulated by pointing one tx at the slot of a
    /// later block that does not hold it: the index still answers, the block
    /// read does not. Such a row keeps its place in the offset and drops out
    /// of the page, which is topped up, so walking the pages one row at a time
    /// still reaches every other row and never the vanished one.
    #[tokio::test]
    async fn assets_by_subject_utxos_block_vanished_mid_request() {
        use dolos_core::{indexes::ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _};

        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        let reference = TestApp::new_with_cfg_and_setup(cfg.clone(), |_, _| {});
        let asset = reference.vectors().asset_unit.clone();
        // a height range, so a row without block data is left out
        let first = reference.vectors().blocks[0].block_number;
        let query = format!("/assets/{asset}/utxos?from={first}");
        let (status, bytes) = reference.get_bytes(&query).await;
        assert_eq!(status, StatusCode::OK);
        let all = parse_utxos(&bytes);

        let gone = reference.vectors().blocks[1].tx_hashes[0].clone();
        assert!(
            all.iter().any(|x| x.tx_hash == gone),
            "fixture needs its rows"
        );
        let expected: Vec<_> = all.iter().filter(|x| x.tx_hash != gone).cloned().collect();

        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let writer = domain.archive().start_writer().unwrap();
            writer
                .apply_index(&[ArchiveIndexDelta {
                    slot: vectors.blocks[3].slot,
                    tx_hashes: vec![hex::decode(&gone).unwrap()],
                    ..Default::default()
                }])
                .unwrap();
            writer.commit().unwrap();
        });

        let (status, bytes) = app.get_bytes(&query).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), expected);

        // The offset counts the vanished row where the scan lists it: in block
        // 3's slot with its tx index unknown, so tied with block 3's tx 0 and
        // ordered by hash. Its page serves the next row; every other page the
        // row listed there, so a client that read past it before it vanished
        // loses nothing. Starting at block 3 puts it in the `from` edge zone.
        let block_3 = &app.vectors().blocks[3];
        for from in [first, block_3.block_number] {
            let in_range: Vec<_> = expected
                .iter()
                .filter(|x| x.block_height as u64 >= from)
                .cloned()
                .collect();
            let at = in_range
                .iter()
                .position(|x| x.block_height as u64 == block_3.block_number)
                .expect("rows in block 3")
                + usize::from(gone > block_3.tx_hashes[0]);
            let mut listed = in_range.clone();
            listed.insert(at, in_range[at].clone());

            let mut served = Vec::new();
            for page in 1..=listed.len() {
                let (status, bytes) = app
                    .get_bytes(&format!(
                        "/assets/{asset}/utxos?from={from}&count=1&page={page}"
                    ))
                    .await;
                assert_eq!(status, StatusCode::OK, "page {page}");
                served.extend(parse_utxos(&bytes));
            }
            assert_eq!(served, listed, "from={from}");
        }

        // The scan still lists the vanished row, so the walk has `all.len()`
        // positions. Every page up to there must come back full: a short page
        // would read as the end of the list. Desc puts the vanished row last in
        // its slot group, so its page has to be topped up from the next window.
        for order in ["asc", "desc"] {
            for count in [1, 2] {
                let pages = all.len().div_ceil(count);
                let mut walked = Vec::new();

                for page in 1..=pages + 1 {
                    let (status, bytes) = app
                        .get_bytes(&format!("{query}&order={order}&count={count}&page={page}"))
                        .await;
                    assert_eq!(status, StatusCode::OK, "{order} page {page}");
                    let rows = parse_utxos(&bytes);

                    if page < pages {
                        assert_eq!(rows.len(), count, "{order}/{count}: page {page} is short");
                    }
                    if page > pages {
                        assert!(rows.is_empty(), "{order}/{count}: rows past the end");
                    }
                    walked.extend(rows);
                }

                assert!(walked.iter().all(|x| x.tx_hash != gone));
                for row in &expected {
                    assert!(
                        walked.contains(row),
                        "{order}/{count}: {}#{} never served",
                        row.tx_hash,
                        row.output_index
                    );
                }
            }
        }
    }

    /// Paging in desc order walks the asc listing backwards, across slot
    /// groups and windows, with and without a height range.
    #[tokio::test]
    async fn assets_by_subject_utxos_desc_walk() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let first = app.vectors().blocks[1].block_number;
        let last = app.vectors().blocks[3].block_number;

        for range in [String::new(), format!("&from={first}&to={last}")] {
            let (status, bytes) = app
                .get_bytes(&format!("/assets/{asset}/utxos?order=asc{range}"))
                .await;
            assert_eq!(status, StatusCode::OK);
            let mut expected = parse_utxos(&bytes);
            expected.reverse();
            assert!(expected.len() > 3, "need several slot groups");

            for count in [1, 2] {
                let mut walked = Vec::new();
                for page in 1..=expected.len().div_ceil(count) {
                    let (status, bytes) = app
                        .get_bytes(&format!(
                            "/assets/{asset}/utxos?order=desc&count={count}&page={page}{range}"
                        ))
                        .await;
                    assert_eq!(status, StatusCode::OK);
                    walked.extend(parse_utxos(&bytes));
                }
                assert_eq!(walked, expected, "count={count}{range}");
            }
        }
    }

    /// A bound height the index does not know is told apart by the oldest
    /// block held, not the tip. A rollback drops the index entries before the
    /// bodies, so the tip can still show a height the index no longer has:
    /// such a `from` is past the held chain (an empty page) and such a `to`
    /// bounds nothing that is held, rather than the reverse.
    #[tokio::test]
    async fn assets_by_subject_utxos_bound_rolled_back_from_the_index() {
        use dolos_core::{indexes::ArchiveIndexDelta, ArchiveStore as _, ArchiveWriter as _};

        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        let app = TestApp::new_with_cfg_and_setup(cfg, |domain, vectors| {
            let tip = vectors.blocks.last().unwrap();
            let writer = domain.archive().start_writer().unwrap();
            writer
                .undo_index(&[ArchiveIndexDelta {
                    slot: tip.slot,
                    block_number: Some(tip.block_number),
                    ..Default::default()
                }])
                .unwrap();
            writer.commit().unwrap();

            // the body is still the tip, the index lost its height
            let slot = domain.archive().slot_by_block_number(tip.block_number);
            assert_eq!(slot.unwrap(), None);
            assert_eq!(
                domain.archive().get_tip().unwrap().map(|x| x.0),
                Some(tip.slot)
            );
        });

        let asset = app.vectors().asset_unit.as_str();
        let tip = app.vectors().blocks.last().unwrap().block_number;

        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/utxos")).await;
        assert_eq!(status, StatusCode::OK);
        let all = parse_utxos(&bytes);
        assert!(all.len() > 1);

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?from={tip}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(parse_utxos(&bytes).is_empty());

        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/utxos?to={tip}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), all);
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/utxos");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }

        let asset = app.vectors().asset_unit.as_str();
        for query in ["from=abc", "from=999999999&to=1", "order=a", "count=0"] {
            let path = format!("/assets/{asset}/utxos?{query}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_pruned_history() {
        let full = TestApp::new();
        let asset = full.vectors().asset_unit.clone();

        let (status, bytes) = full.get_bytes(&format!("/assets/{asset}/utxos")).await;
        assert_eq!(status, StatusCode::OK);
        let all = parse_utxos(&bytes);

        let pruned = TestApp::new_pruned();
        let first_retained = pruned.vectors().blocks[2].block_number as i32;
        let pruned_height = pruned.vectors().blocks[0].block_number;

        // rows of pruned blocks are gone, the rest keeps its order
        let (status, bytes) = pruned.get_bytes(&format!("/assets/{asset}/utxos")).await;
        assert_eq!(status, StatusCode::OK);
        let retained = parse_utxos(&bytes);
        let expected: Vec<_> = all
            .iter()
            .filter(|x| x.block_height >= first_retained)
            .cloned()
            .collect();
        assert!(!expected.is_empty(), "fixture needs retained rows");
        assert_eq!(retained, expected);

        // a pruned lower bound is no bound: everything held is newer
        let (status, bytes) = pruned
            .get_bytes(&format!("/assets/{asset}/utxos?from={pruned_height}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), expected);

        // a pruned upper bound excludes everything held
        let (status, bytes) = pruned
            .get_bytes(&format!("/assets/{asset}/utxos?to={pruned_height}"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert!(parse_utxos(&bytes).is_empty());

        // an upper bound past the tip is no bound
        let (status, bytes) = pruned
            .get_bytes(&format!("/assets/{asset}/utxos?to=99999999"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), expected);

        // pages stay aligned: page 1 of size 1 is the first retained row
        let (status, bytes) = pruned
            .get_bytes(&format!("/assets/{asset}/utxos?count=1&page=1"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), vec![expected[0].clone()]);
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/utxos", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn assets_by_subject_utxos_scan_limit() {
        let app = TestApp::new_with_scan_limit(
            SyntheticBlockConfig {
                block_count: 5,
                txs_per_block: 3,
                ..Default::default()
            },
            3,
        );
        let asset = app.vectors().asset_unit.as_str();

        // a page within the budget is served
        let path = format!("/assets/{asset}/utxos?count=3&page=1");
        assert_status(&app, &path, StatusCode::OK).await;

        // one reaching past it is refused, not cut short
        let path = format!("/assets/{asset}/utxos?count=2&page=2");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }
}
