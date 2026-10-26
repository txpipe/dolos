//! `/assets/{asset}/history`: every mint and burn of one asset.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::asset_history_inner::{Action, AssetHistoryInner};
use dolos_cardano::{indexes::SlotOrder, model::AssetState, periods::asset_mints};
use dolos_core::{Domain, DomainError};
use pallas::crypto::hash::Hash;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

/// A mint as Blockfrost renders its `ma_tx_mint` row: the signed quantity,
/// and `burned` for anything below zero, the same split as ryo's SQL.
fn history_row((tx_hash, amount): (Hash<32>, i64)) -> AssetHistoryInner {
    let action = if amount < 0 {
        Action::Burned
    } else {
        Action::Minted
    };

    AssetHistoryInner {
        tx_hash: hex::encode(tx_hash),
        action,
        amount: amount.to_string(),
    }
}

/// `GET /assets/{asset}/history`: the mints and burns of an asset in chain
/// order, the rows ryo reads from `ma_tx_mint`.
///
/// The page comes from the asset-mint period logs, from the epoch of the
/// asset's first mint on: one chunk header per sealed epoch and the chunks the
/// page covers, so no page decodes a block and none is refused.
pub async fn by_subject_history<D>(
    Path(subject): Path<String>,
    Query(mut params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetHistoryInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AssetState>: From<D::Entity>,
{
    // Blockfrost does not define `from`/`to` here and ignores them
    params.from = None;
    params.to = None;

    let pagination = Pagination::try_from(params)?;

    let (subject, state) = crate::mapping::assets::resolve_asset_state(&domain, &subject)?;

    let Some(initial_slot) = state.initial_slot else {
        return Ok(Json(vec![]));
    };

    let chain = domain.get_chain_summary()?;
    let (first_epoch, _) = chain.slot_epoch(initial_slot);
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let asset = dolos_cardano::AssetKey::of(&subject);
    let order = SlotOrder::from(pagination.order);
    let (skip, count) = (pagination.skip(), pagination.count);

    let mints = domain
        .query()
        .run_blocking(move |domain| {
            asset_mints::page(
                &domain,
                &chain,
                first_epoch..=tip_epoch,
                &asset,
                order,
                skip,
                count,
            )
            .map_err(DomainError::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    Ok(Json(mints.into_iter().map(history_row).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::{
        asset::Asset, asset_policy_inner::AssetPolicyInner, assets_inner::AssetsInner,
    };
    use dolos_cardano::model::{AssetMintChunk, AssetMintSet};
    use dolos_core::{import::ImportExt as _, ArchiveStore as _, RangeOf, StateStore as _};
    use dolos_testing::synthetic::{SyntheticBlockConfig, SyntheticVectors, TxMints};
    use itertools::Itertools;
    use pallas::ledger::primitives::{alonzo, TransactionInput};

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    async fn history_of(app: &TestApp, unit: &str, query: &str) -> Vec<AssetHistoryInner> {
        let path = format!("/assets/{unit}/history{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse asset history")
    }

    async fn get_history(app: &TestApp, query: &str) -> Vec<AssetHistoryInner> {
        history_of(app, &app.vectors().asset_unit.clone(), query).await
    }

    fn hashes(rows: &[AssetHistoryInner]) -> Vec<String> {
        rows.iter().map(|x| x.tx_hash.clone()).collect()
    }

    /// Every tx of the synthetic chain mints the asset, in chain order.
    fn chain_txs(app: &TestApp) -> Vec<String> {
        app.vectors()
            .blocks
            .iter()
            .flat_map(|block| block.tx_hashes.iter().cloned())
            .collect()
    }

    #[tokio::test]
    async fn assets_by_subject_history_happy_path() {
        let app = TestApp::new();

        let rows = get_history(&app, "").await;
        assert_eq!(hashes(&rows), chain_txs(&app));
        assert!(rows
            .iter()
            .all(|x| x.action == Action::Minted && x.amount == "1"));
    }

    #[tokio::test]
    async fn assets_by_subject_history_order_desc() {
        let app = TestApp::new();

        let rows = get_history(&app, "?order=desc").await;
        let expected = chain_txs(&app).into_iter().rev().collect_vec();
        assert_eq!(hashes(&rows), expected);
    }

    #[tokio::test]
    async fn assets_by_subject_history_paginated() {
        let app = TestApp::new();
        let txs = chain_txs(&app);

        // pages cut across block boundaries in both orders
        let rows = get_history(&app, "?page=2&count=4").await;
        assert_eq!(hashes(&rows), txs[4..8]);

        let rows = get_history(&app, "?order=desc&page=2&count=4").await;
        let desc = txs.iter().rev().cloned().collect_vec();
        assert_eq!(hashes(&rows), desc[4..8]);

        // the last page is short, the one after it is empty
        let rows = get_history(&app, "?page=4&count=4").await;
        assert_eq!(hashes(&rows), txs[12..]);

        let rows = get_history(&app, "?page=5&count=4").await;
        assert!(rows.is_empty());
    }

    #[tokio::test]
    async fn assets_by_subject_history_burns() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            mint_amount: -3,
            ..Default::default()
        });

        let rows = get_history(&app, "").await;
        assert_eq!(hashes(&rows), chain_txs(&app));
        assert!(rows
            .iter()
            .all(|x| x.action == Action::Burned && x.amount == "-3"));
    }

    #[tokio::test]
    async fn assets_by_subject_history_ignores_from_to() {
        let app = TestApp::new();
        let all = get_history(&app, "").await;

        let block = app.vectors().blocks.first().expect("missing block vectors");
        let query = format!("?from={0}&to={0}", block.block_number);
        assert_eq!(get_history(&app, &query).await, all);

        // a malformed range is ignored as well, never validated
        assert_eq!(get_history(&app, "?from=not-a-number").await, all);
    }

    #[tokio::test]
    async fn assets_by_subject_history_skips_transfers() {
        // block 1 mints `ONCE`, block 2 only spends it
        let cfg = SyntheticBlockConfig {
            block_count: 3,
            txs_per_block: 1,
            asset_names_by_block: ["ONCE", "OTHER", "OTHER"]
                .iter()
                .map(|x| (*x).to_string())
                .collect(),
            spend_previous_outputs: true,
            ..Default::default()
        };

        let app = TestApp::new_with_cfg(cfg);
        let unit = format!("{}{}", app.vectors().policy_id, hex::encode("ONCE"));
        let mint = app.vectors().blocks[0].tx_hashes[0].clone();

        for order in ["asc", "desc"] {
            let rows = history_of(&app, &unit, &format!("?order={order}")).await;
            assert_eq!(hashes(&rows), std::slice::from_ref(&mint), "order={order}");
        }
    }

    /// Blockfrost refuses no page, so neither does this one: the chain mints
    /// the asset in 3,120 txs, and the pages past item 3,000 answer in both
    /// orders.
    #[tokio::test]
    async fn assets_by_subject_history_pages_past_item_3000() {
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: 13,
            txs_per_block: 240,
            ..Default::default()
        });
        let txs = chain_txs(&app);
        assert_eq!(txs.len(), 3_120);

        let rows = get_history(&app, "?page=31&count=100").await;
        assert_eq!(hashes(&rows), txs[3_000..3_100]);

        let rows = get_history(&app, "?page=32&count=100").await;
        assert_eq!(hashes(&rows), txs[3_100..]);

        let rows = get_history(&app, "?order=desc&page=31&count=100").await;
        let desc = txs.iter().rev().cloned().collect_vec();
        assert_eq!(hashes(&rows), desc[3_000..3_100]);

        assert!(get_history(&app, "?page=33&count=100").await.is_empty());
    }

    const POLICY: [u8; 28] = [0x44; 28];
    const OTHER: [u8; 28] = [0x55; 28];

    fn unit(policy: [u8; 28], name: &str) -> String {
        format!("{}{}", hex::encode(policy), hex::encode(name))
    }

    /// `H`'s mints and burns over preview epochs 2 to 7, two blocks per epoch,
    /// beside other assets in the same txs and an asset of the same name under
    /// another policy. The first epochs seal and the last stay staged. Block
    /// 3's first tx, a burn of `H`, is phase-2-invalid.
    fn history_mints() -> Vec<Vec<TxMints>> {
        let h = |amount| (POLICY, "H".to_string(), amount);
        let o = |amount| (POLICY, "O".to_string(), amount);
        let q = |amount| (OTHER, "H".to_string(), amount);

        vec![
            vec![vec![h(5), o(1)], vec![h(-2)]],
            vec![vec![q(1)], vec![h(3)]],
            vec![vec![o(1), h(1), q(-1)], vec![]],
            vec![vec![h(-4)], vec![o(1)]],
            vec![vec![], vec![h(7)]],
            vec![vec![o(-1)], vec![]],
            vec![vec![h(2)], vec![h(-1)]],
            vec![vec![], vec![]],
            vec![vec![h(10)], vec![]],
            vec![vec![q(2)], vec![h(-3)]],
            vec![vec![h(1)], vec![o(1)]],
            vec![vec![], vec![h(-1)]],
        ]
    }

    const INVALID: (usize, u32) = (3, 0);

    fn history_chain() -> SyntheticBlockConfig {
        let mints_by_block = history_mints();
        let mut invalid_txs_by_block = vec![vec![]; mints_by_block.len()];
        invalid_txs_by_block[INVALID.0] = vec![INVALID.1];

        SyntheticBlockConfig {
            block_count: mints_by_block.len(),
            txs_per_block: 2,
            slot_step: 43_200,
            mints_by_block,
            invalid_txs_by_block,
            plain_txs: true,
            seed_amount: dolos_testing::MIN_UTXO_AMOUNT + 7,
            ..Default::default()
        }
    }

    /// `H`'s rows as Blockfrost lists them: valid txs only, in chain order.
    fn expected_history(vectors: &SyntheticVectors) -> Vec<(String, i64)> {
        let mut out = Vec::new();

        for (b, (txs, block)) in history_mints().iter().zip(&vectors.blocks).enumerate() {
            for (t, mints) in txs.iter().enumerate() {
                if (b, t as u32) == INVALID {
                    continue;
                }

                for (policy, name, amount) in mints {
                    if *policy == POLICY && name == "H" {
                        out.push((block.tx_hashes[t].clone(), *amount));
                    }
                }
            }
        }

        out
    }

    /// One Alonzo block after the chain, whose tx mints zero `H` and zero
    /// `ZERO`: entries db-sync never stores.
    fn zero_mint_block(vectors: &SyntheticVectors) -> dolos_core::RawBlock {
        let last = vectors.blocks.last().expect("chain has no blocks");

        let input = TransactionInput {
            transaction_id: last.tx_hashes[0].parse().expect("tx hash"),
            index: 0,
        };

        let mut assets = std::collections::BTreeMap::new();
        assets.insert(b"H".to_vec().into(), 0);
        assets.insert(b"ZERO".to_vec().into(), 0);

        let mut mint: alonzo::Mint = std::collections::BTreeMap::new();
        mint.insert(POLICY.into(), assets);

        let (raw, _, _) = dolos_testing::synthetic::sample_alonzo_block(
            last.block_number + 1,
            last.slot + 1,
            Some(last.block_hash.parse().expect("block hash")),
            input,
            vec![],
            Some(mint),
        );

        raw
    }

    fn history_app() -> TestApp {
        TestApp::new_with_cfg_and_setup(history_chain(), |domain, vectors| {
            domain
                .import_blocks(vec![zero_mint_block(vectors)])
                .expect("failed to import the zero mint");
        })
    }

    #[tokio::test]
    async fn assets_by_subject_history_lists_mints_and_burns_across_sealed_and_staged_epochs() {
        let app = history_app();
        let h = unit(POLICY, "H");

        // H has rows sealed into chunks and rows still staged
        let asset = dolos_cardano::AssetKey::of(&hex::decode(&h).unwrap());
        let facade = app.facade();
        let sealed = facade
            .archive()
            .iter_logs_typed::<AssetMintChunk>(None)
            .unwrap()
            .filter(|row| row.as_ref().unwrap().1 .0 == asset)
            .count();
        let staged = facade
            .state()
            .iter_entities_typed::<AssetMintSet>(Some(RangeOf::prefix(&asset)))
            .unwrap()
            .count();

        // epochs 2 and 3 sealed, a chunk each; 4 to 7 staged
        assert_eq!((sealed, staged), (2, 7));

        let expected = expected_history(app.vectors());
        let n = expected.len();
        assert_eq!(n, 11);

        let rows = history_of(&app, &h, "").await;
        let got: Vec<(String, i64)> = rows
            .iter()
            .map(|x| (x.tx_hash.clone(), x.amount.parse().unwrap()))
            .collect();
        assert_eq!(got, expected);

        for row in &rows {
            let burn = row.amount.starts_with('-');
            assert_eq!(row.action == Action::Burned, burn, "{row:?}");
        }

        // every page, in both orders, cuts the same list
        let desc = expected.iter().rev().cloned().collect_vec();

        for count in [1, 3, 5] {
            for page in 1..=(n / count + 1) {
                let from = ((page - 1) * count).min(n);
                let to = (page * count).min(n);

                let asc = history_of(&app, &h, &format!("?count={count}&page={page}")).await;
                let want = expected[from..to].iter().map(|x| x.0.clone()).collect_vec();
                assert_eq!(hashes(&asc), want, "asc count={count} page={page}");

                let query = format!("?order=desc&count={count}&page={page}");
                let rows = history_of(&app, &h, &query).await;
                let want = desc[from..to].iter().map(|x| x.0.clone()).collect_vec();
                assert_eq!(hashes(&rows), want, "desc count={count} page={page}");
            }
        }
    }

    /// Neither the zero-quantity entry nor the phase-2-invalid burn is a row.
    #[tokio::test]
    async fn assets_by_subject_history_skips_zero_quantities_and_invalid_txs() {
        let app = history_app();
        let vectors = app.vectors();

        let rows = history_of(&app, &unit(POLICY, "H"), "").await;

        let invalid = &vectors.blocks[INVALID.0].tx_hashes[INVALID.1 as usize];
        assert!(rows.iter().all(|x| x.tx_hash != *invalid));
        assert!(rows.iter().all(|x| x.amount != "0"));

        // the zero mint is the chain's last tx, and the only mint of `ZERO`
        let path = format!("/assets/{}/history", unit(POLICY, "ZERO"));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    async fn get_json<T: serde::de::DeserializeOwned>(app: &TestApp, path: &str) -> T {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse response")
    }

    /// A zero-quantity entry creates no asset, and leaves a minted one as the
    /// chain without it left it.
    #[tokio::test]
    async fn zero_mint_adds_no_asset_state() {
        let app = history_app();
        let zero = unit(POLICY, "ZERO");

        assert_status(&app, &format!("/assets/{zero}"), StatusCode::NOT_FOUND).await;

        let baseline = TestApp::new_with_cfg(history_chain());
        let path = format!("/assets/{}", unit(POLICY, "H"));
        let want: Asset = get_json(&baseline, &path).await;
        let got: Asset = get_json(&app, &path).await;
        assert_eq!(got.mint_or_burn_count, want.mint_or_burn_count);
        assert_eq!(got.initial_mint_tx_hash, want.initial_mint_tx_hash);

        let all: Vec<AssetsInner> = get_json(&app, "/assets").await;
        assert!(all.iter().any(|x| x.asset == unit(POLICY, "H")));
        assert!(all.iter().all(|x| x.asset != zero));

        let path = format!("/assets/policy/{}", hex::encode(POLICY));
        let by_policy: Vec<AssetPolicyInner> = get_json(&app, &path).await;
        assert!(by_policy.iter().any(|x| x.asset == unit(POLICY, "H")));
        assert!(by_policy.iter().all(|x| x.asset != zero));
    }

    #[tokio::test]
    async fn assets_by_subject_history_bad_request() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        for subject in ["not-hex-asset", "abcd", &"f".repeat(122), &"z".repeat(56)] {
            let path = format!("/assets/{subject}/history");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }

        for query in ["?count=0", "?page=x", "?order=sideways"] {
            let path = format!("/assets/{asset}/history{query}");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_history_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/history", "f".repeat(84));
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_history_internal_error() {
        for fault in [TestFault::StateStoreError, TestFault::ArchiveStoreError] {
            let app = TestApp::new_with_fault(Some(fault));
            let asset = app.vectors().asset_unit.as_str();
            let path = format!("/assets/{asset}/history");
            assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
        }
    }
}
