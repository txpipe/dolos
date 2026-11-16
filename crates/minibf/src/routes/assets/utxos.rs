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
    pagination::{Pagination, PaginationError, PaginationNumberAndIndex, PaginationParameters},
    utxos::load_tag_page_in_slots,
    Facade,
};

/// `GET /assets/{subject}/utxos`
///
/// The live UTxOs that carry the asset, in creation order: block, tx index,
/// output index, the order db-sync's `tx_out.id` gives Blockfrost. `from` and
/// `to` bound the creating block's height, inclusive, and resolve to slots as
/// on every other `from` / `to` listing ([`Pagination::start_and_end_slots`]);
/// an `:index` on them cuts nothing, as on Blockfrost. A valid but unknown
/// asset is a 404; a known asset no live UTxO holds any more (fully burned) is
/// an empty page. A UTxO whose creating block the archive no longer holds is
/// listed with its block fields empty, as on the address listing.
///
/// The page comes off the live-UTxO tags in creation order: the offset costs
/// tag entries, the page its rows and their blocks, however many live UTxOs
/// the asset has.
pub async fn by_subject_utxos<D>(
    Path(subject): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetUtxoContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(heights_only(params)?)?;
    let (asset, _) = resolve_asset(&domain, &subject).await?;

    let tag = Tag::new(utxo_dimensions::ASSET, asset);

    let slots = match (&pagination.from, &pagination.to) {
        (None, None) => None,
        _ => {
            let (start, end) = pagination.start_and_end_slots(&domain).await?;
            Some(start..=end)
        }
    };

    let utxos = load_tag_page_in_slots(&domain, &tag, &pagination, slots).await?;

    Ok(Json(utxos))
}

/// `params` with each `from` / `to` bound cut to its height, once the bound
/// has parsed whole. Blockfrost reads the bounds here as heights alone, so an
/// `:index` must be well formed but neither cuts rows nor takes part in the
/// order check between the two bounds.
fn heights_only(params: PaginationParameters) -> Result<PaginationParameters, PaginationError> {
    let height = |bound: Option<String>| {
        bound
            .map(|bound| PaginationNumberAndIndex::try_from(bound).map(|x| x.number.to_string()))
            .transpose()
    };

    Ok(PaginationParameters {
        from: height(params.from)?,
        to: height(params.to)?,
        ..params
    })
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

        // a height the chain does not hold is a 400, as on the other
        // `from` / `to` listings
        for query in ["from=99999999", "to=99999999"] {
            let path = format!("/assets/{asset}/utxos?{query}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    /// Blockfrost reads `from` / `to` here as plain heights: an `:index` cuts
    /// nothing inside the edge block.
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
            // a reversed index included: the heights alone are compared
            format!("from={height}:2&to={height}:1"),
        ] {
            assert_eq!(get(query).await, in_block);
        }
    }

    fn five_blocks() -> SyntheticBlockConfig {
        SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 2,
            ..Default::default()
        }
    }

    /// The five-block chain with one more tag entry in block `block`: beside
    /// the asset row of the block's first transaction, for a ref of that
    /// transaction that is not live. It is what a walk holds of a UTxO spent
    /// between its tag read and its value read.
    fn chain_with_a_spent_entry_in(block: usize) -> TestApp {
        use dolos_cardano::indexes::utxo_tags;
        use dolos_core::{
            indexes::UtxoIndexDelta, StateStore as _, StateWriter as _, TxoRef, UtxoPosition,
        };
        use pallas::ledger::traverse::MultiEraOutput;

        TestApp::new_with_cfg_and_setup(five_blocks(), move |domain, vectors| {
            let at = &vectors.blocks[block];
            let tx_hash = at.tx_hashes[0].parse().expect("tx hash");
            let row = TxoRef(tx_hash, 0);

            let value = domain
                .state()
                .get_utxos(vec![row.clone()])
                .expect("utxo read")
                .remove(&row)
                .expect("the asset row of the block's first transaction");
            let output = MultiEraOutput::try_from(value.as_ref()).expect("output");

            let spent = TxoRef(tx_hash, u32::MAX - 1);
            let position = Some(UtxoPosition::new(at.slot, 0));
            let delta = UtxoIndexDelta {
                produced: vec![(spent, position, utxo_tags(&output))],
                consumed: vec![],
            };

            let writer = domain.state().start_writer().expect("state writer");
            writer.apply_utxo_tags(&delta).expect("apply tags");
            writer.commit().expect("commit");
        })
    }

    /// The one-row page of `query` past `skip` rows.
    async fn row_past(app: &TestApp, query: &str, skip: usize) -> Vec<AssetUtxoContentInner> {
        let asset = app.vectors().asset_unit.as_str();
        let page = skip + 1;
        let path = format!("/assets/{asset}/utxos?{query}&count=1&page={page}");

        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK, "{path}");

        parse_utxos(&bytes)
    }

    /// An offset means the same whichever bound the walk starts at. A UTxO
    /// spent since its tag read holds its place in the offset when its block
    /// is in the range, its value unread, and holds none when its block is
    /// outside the range.
    #[tokio::test]
    async fn assets_by_subject_utxos_count_a_spent_row_in_the_offset() {
        let plain = TestApp::new_with_cfg(five_blocks());
        let asset = plain.vectors().asset_unit.clone();
        let blocks = plain.vectors().blocks.clone();
        let (below, from) = (blocks[1].block_number, blocks[2].block_number);

        // from the block below, the same rows come after that block's own
        let on_edge = format!("from={from}");
        let off_edge = format!("from={below}");

        let listing = |query: &str| format!("/assets/{asset}/utxos?{query}&count=100");
        let (_, bytes) = plain.get_bytes(&listing(&on_edge)).await;
        let rows = parse_utxos(&bytes);
        let (_, bytes) = plain.get_bytes(&listing(&off_edge)).await;
        let lead = parse_utxos(&bytes).len() - rows.len();
        assert!(rows.len() > 2 && lead > 0, "the fixture lists too few rows");

        // spent in the first block of the range: it holds a place in the
        // offset wherever the range puts its block
        let app = chain_with_a_spent_entry_in(2);
        let mut shifted = false;

        for skip in 0..=rows.len() {
            let row = row_past(&app, &on_edge, skip).await;
            assert_eq!(row, row_past(&app, &off_edge, skip + lead).await, "{skip}");
            shifted |= row.first() != rows.get(skip);
        }

        assert!(shifted, "the spent entry held no place in the offset");

        // spent in the block below the range: not a row of it, so no place
        let app = chain_with_a_spent_entry_in(1);

        for skip in 0..=rows.len() {
            let expected: Vec<_> = rows.get(skip).cloned().into_iter().collect();
            assert_eq!(row_past(&app, &on_edge, skip).await, expected, "{skip}");
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

    #[tokio::test]
    async fn assets_by_subject_utxos_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/utxos");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }

        let asset = app.vectors().asset_unit.as_str();
        for query in [
            "from=abc",
            "from=10:x",
            "to=1:",
            "from=999999999&to=1",
            "order=a",
            "count=0",
        ] {
            let path = format!("/assets/{asset}/utxos?{query}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    /// A pruned archive still lists every live UTxO of the asset, in the same
    /// order: a row whose creating block is gone keeps its place with its
    /// block fields empty, as on the address listing.
    #[tokio::test]
    async fn assets_by_subject_utxos_pruned_history() {
        let full = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 5,
            txs_per_block: 3,
            ..Default::default()
        });
        let asset = full.vectors().asset_unit.clone();
        let listing = |query: &str| format!("/assets/{asset}/utxos?{query}");

        let (status, bytes) = full.get_bytes(&listing("")).await;
        assert_eq!(status, StatusCode::OK);
        let all = parse_utxos(&bytes);

        let pruned = TestApp::new_pruned();
        let first_retained = pruned.vectors().blocks[2].block_number as i32;

        let (status, bytes) = pruned.get_bytes(&listing("")).await;
        assert_eq!(status, StatusCode::OK);
        let listed = parse_utxos(&bytes);
        assert_eq!(listed.len(), all.len());

        let (mut held, mut gone) = (0, 0);

        for (row, expected) in listed.iter().zip(&all) {
            assert_eq!(
                (&row.tx_hash, row.output_index),
                (&expected.tx_hash, expected.output_index)
            );

            if expected.block_height >= first_retained {
                assert_eq!(row, expected);
                held += 1;
            } else {
                assert_eq!(row.block, "");
                assert_eq!(row.block_height, 0);
                assert_eq!(row.block_time, 0);
                assert_eq!(row.amount, expected.amount);
                gone += 1;
            }
        }

        assert!(held > 0 && gone > 0, "fixture needs held and pruned rows");

        // a bound within the held history selects what it does on the full
        // archive
        let query = format!("from={first_retained}");
        let (status, bytes) = full.get_bytes(&listing(&query)).await;
        assert_eq!(status, StatusCode::OK);
        let expected = parse_utxos(&bytes);
        assert!(!expected.is_empty());

        let (status, bytes) = pruned.get_bytes(&listing(&query)).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(parse_utxos(&bytes), expected);
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
