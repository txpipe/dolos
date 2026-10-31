//! `/assets/{asset}/utxos`: the live UTxOs holding an asset.

use axum::{
    extract::{Path, Query, State},
    Json,
};
use blockfrost_openapi::models::asset_utxo_content_inner::AssetUtxoContentInner;
use dolos_cardano::indexes::utxo_dimensions;
use dolos_core::{Domain, Tag};

use crate::{
    error::Error,
    mapping::assets::resolve_asset,
    pagination::{Pagination, PaginationParameters},
    routes::utxos::{load_tag_page_in_heights, HeightRange},
    Facade,
};

/// `GET /assets/{subject}/utxos`
///
/// The live UTxOs that carry the asset, in creation order: block, tx index,
/// output index, the order db-sync's `tx_out.id` gives Blockfrost. `from` and
/// `to` bound the creating block's height, inclusive; an `:index` on them is
/// validated but, as on Blockfrost, cuts nothing. A valid but unknown asset
/// is a 404; a known asset no live UTxO holds any more (fully burned) is an
/// empty page. A UTxO whose creating block the archive no longer holds is
/// not listed: the row reports that block.
///
/// The page comes off the live-UTxO tags in creation order: the offset costs
/// tag entries, the page its rows and one block read per slot it touches,
/// however many live UTxOs the asset has.
pub async fn by_subject_utxos<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let (asset, _) = resolve_asset(&domain, &subject).await?;

    let tag = Tag::new(utxo_dimensions::ASSET, asset);
    let range = HeightRange::of(&pagination);

    let utxos = load_tag_page_in_heights(&domain, &tag, &pagination, &range).await?;

    Ok(Json(utxos))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use super::*;
    use crate::{
        routes::assets::testing::{assert_status, invalid_assets, missing_asset},
        test_support::{TestApp, TestFault},
    };
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

    /// Bounds Blockfrost rejects (empty parts, a third part, a number past
    /// `i32::MAX`) are a 400, not ignored.
    #[tokio::test]
    async fn assets_by_subject_utxos_bounds_parse_like_blockfrost() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        for query in [
            "from=10:2:garbage",
            "to=1:2:3",
            "from=10:",
            "from=:2",
            "from=",
            "from=99999999999",
        ] {
            let path = format!("/assets/{asset}/utxos?{query}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
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

    /// A fork between the tag read and the value read can move a row's
    /// transaction to a block outside the range, while its tag entry inside
    /// the range is the one the walk read. The row is held against the height
    /// of the block it is in: it is no row of the page.
    #[tokio::test]
    async fn assets_by_subject_utxos_hold_a_moved_row_against_the_range() {
        use dolos_cardano::indexes::utxo_tags;
        use dolos_core::{
            indexes::UtxoIndexDelta, StateStore as _, StateWriter as _, TxoRef, UtxoPosition,
        };
        use pallas::ledger::traverse::MultiEraOutput;

        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 2,
            ..Default::default()
        });
        let asset = app.vectors().asset_unit.clone();
        let blocks = app.vectors().blocks.clone();
        let (from, to) = (blocks[1].block_number, blocks[3].block_number);
        let path = format!("/assets/{asset}/utxos?from={from}&to={to}");

        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        let within = parse_utxos(&bytes);
        assert!(within
            .iter()
            .all(|x| (from..=to).contains(&(x.block_height as u64))));

        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/utxos")).await;
        assert_eq!(status, StatusCode::OK);
        let moved = parse_utxos(&bytes)
            .into_iter()
            .find(|x| x.block_height as u64 == blocks[4].block_number)
            .expect("a row past the range");

        // the tag entry of a row of the last block sits inside the range,
        // off its edges, as a fork that moved the row there would leave it
        let domain = app.facade().inner;
        let txo = TxoRef(
            moved.tx_hash.parse().expect("tx hash"),
            moved.output_index as u32,
        );
        let value = domain
            .state()
            .get_utxos(vec![txo.clone()])
            .expect("utxo read")
            .remove(&txo)
            .expect("the moved row's value");
        let output = MultiEraOutput::try_from(value.as_ref()).expect("output");
        let position = Some(UtxoPosition::new(blocks[2].slot, 0));
        let delta = UtxoIndexDelta {
            produced: vec![(txo, position, utxo_tags(&output))],
            consumed: vec![],
        };
        let writer = domain.state().start_writer().expect("state writer");
        writer.apply_utxo_tags(&delta).expect("apply tags");
        writer.commit().expect("commit");

        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), within);
    }

    /// A block pruned between the index read and the block read leaves its
    /// rows without block data. Simulated by moving one tx's rows to the slot
    /// of a later block that does not hold it: the index still lists them,
    /// the block read does not find the tx. Such a row keeps its place in
    /// the offset and drops out of the page, which is topped up, so walking
    /// the pages one row at a time still reaches every other row and never
    /// the vanished one.
    #[tokio::test]
    async fn assets_by_subject_utxos_block_vanished_mid_request() {
        use dolos_cardano::indexes::utxo_index_delta_from_utxo_delta;
        use std::sync::Arc;

        use dolos_core::{
            EraCbor, StateStore as _, StateWriter as _, UtxoIndexDelta, UtxoPosition, UtxoSetDelta,
            UtxoValue,
        };

        let cfg = SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        };

        let reference = TestApp::new_with_cfg_and_setup(cfg.clone(), |_, _| {});
        let asset = reference.vectors().asset_unit.clone();
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
            let gone_hash: dolos_core::TxHash = gone.parse().unwrap();
            let subject = hex::decode(&asset).unwrap();

            // the tx's live rows, with the tags their bodies carry
            let refs: Vec<_> = domain
                .state()
                .utxos_by_tag(utxo_dimensions::ASSET, &subject)
                .unwrap()
                .into_iter()
                .filter(|txo| txo.0 == gone_hash)
                .collect();
            assert!(!refs.is_empty(), "fixture needs the tx's rows");
            let bodies = domain.state().get_utxos(refs.clone()).unwrap();
            let tagged = utxo_index_delta_from_utxo_delta(&UtxoSetDelta {
                produced_utxo: bodies.clone(),
                ..Default::default()
            })
            .produced;

            // out of their own position, back in under block 3's first tx:
            // the tags that order them and the values that locate them
            let position = UtxoPosition::new(vectors.blocks[3].slot, 0);
            let moved: Vec<_> = tagged
                .iter()
                .cloned()
                .map(|(txo, _, tags)| (txo, Some(position), tags))
                .collect();
            let produced_utxo = bodies
                .iter()
                .map(|(txo, value)| {
                    let output = EraCbor(value.era(), value.cbor().to_vec());
                    (
                        txo.clone(),
                        Arc::new(UtxoValue::new(output, Some(position))),
                    )
                })
                .collect();

            let writer = domain.state().start_writer().unwrap();
            writer
                .apply_utxo_tags(&UtxoIndexDelta {
                    consumed: tagged,
                    ..Default::default()
                })
                .unwrap();
            writer.commit().unwrap();

            let writer = domain.state().start_writer().unwrap();
            writer
                .apply_utxoset(&UtxoSetDelta {
                    produced_utxo,
                    ..Default::default()
                })
                .unwrap();
            writer
                .apply_utxo_tags(&UtxoIndexDelta {
                    produced: moved,
                    ..Default::default()
                })
                .unwrap();
            writer.commit().unwrap();
        });

        let (status, bytes) = app.get_bytes(&query).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), expected);

        // The offset counts the vanished row where the tag lists it: at block
        // 3's first tx, tied with that tx's own rows and ordered by ref. Its
        // page serves the next row; every other page the row listed there, so
        // a client that read past it before it vanished loses nothing.
        // Starting at block 3 puts it in the `from` edge zone.
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

        // pages stay aligned: page 1 of size 1 is the first retained row, and
        // the pruned rows cost nothing on the pages after it either — a walk
        // one row at a time lists every retained row once and nothing else
        let (status, bytes) = pruned
            .get_bytes(&format!("/assets/{asset}/utxos?count=1&page=1"))
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), vec![expected[0].clone()]);

        for from in [String::new(), format!("&from={pruned_height}")] {
            let mut walked = Vec::new();
            for page in 1..=expected.len() + 1 {
                let (status, bytes) = pruned
                    .get_bytes(&format!("/assets/{asset}/utxos?count=1&page={page}{from}"))
                    .await;
                assert_eq!(status, StatusCode::OK, "page {page}");
                walked.extend(parse_utxos(&bytes));
            }
            assert_eq!(walked, expected, "from={from:?}");
        }
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
}
