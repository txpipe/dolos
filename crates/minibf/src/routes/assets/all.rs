//! `/assets`: every asset, in order of first mint.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::assets_inner::AssetsInner;
use dolos_cardano::{indexes::SlotOrder, periods::first_mints};
use dolos_core::{Domain, DomainError};

use crate::{
    error::Error,
    mapping::assets::asset_summaries,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// `GET /assets`: every asset ever minted, ordered by its first mint.
///
/// The order is Blockfrost's: by first mint tx, and by subject bytes within a
/// tx. The page comes from the first-mint period logs, which need no block
/// decoding at any depth.
pub async fn all<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AssetsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    // Native assets exist since Mary, so nothing before that era can appear.
    let chain = domain.get_chain_summary()?;
    let Some(mary_epoch) = chain.first_mary_epoch() else {
        return Ok(Json(vec![]));
    };
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let order = match pagination.order {
        Order::Asc => SlotOrder::Asc,
        Order::Desc => SlotOrder::Desc,
    };
    let (skip, count) = (pagination.skip(), pagination.count);

    let subjects = domain
        .query()
        .run_blocking(move |domain| {
            first_mints::all_page(&domain, &chain, mary_epoch..=tip_epoch, order, skip, count)
                .map_err(DomainError::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items = asset_summaries(&domain, subjects)
        .await?
        .into_iter()
        .map(|(subject, summary)| AssetsInner {
            asset: hex::encode(&subject),
            quantity: summary.fold.quantity().to_string(),
        })
        .collect();

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{first_mint_chain, TestApp, TestFault};
    use blockfrost_openapi::models::asset::Asset;
    use dolos_cardano::model::{FirstMintChunk, FirstMintSet};
    use dolos_core::{ArchiveStore as _, StateStore as _};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use itertools::Itertools;

    use axum::http::StatusCode;

    use dolos_core::Domain;

    use crate::pagination::Order;

    /// The scan the period logs replaced: every block from Mary on, a tx's
    /// subjects in byte order, kept where the tx is the asset's first mint.
    mod scan {
        use std::collections::{BTreeSet, HashMap};

        use dolos_core::{ArchiveStore as _, BlockSlot, Domain};
        use pallas::{
            crypto::hash::Hash,
            ledger::traverse::{MultiEraBlock, MultiEraTx},
        };

        use crate::pagination::Order;

        /// Each subject's first mint: the first valid tx minting it with a
        /// non-zero quantity, the roll's rule.
        pub fn first_mint_txs<D: Domain>(domain: &D) -> HashMap<Vec<u8>, Hash<32>> {
            let mut out = HashMap::new();

            for (_, body) in domain.archive().get_range(None, None).unwrap() {
                let block = MultiEraBlock::decode(&body).unwrap();

                for tx in block.txs().iter().filter(|tx| tx.is_valid()) {
                    for policy_assets in tx.mints() {
                        for asset in policy_assets.assets() {
                            if asset.mint_coin().unwrap_or_default() != 0 {
                                let subject =
                                    [policy_assets.policy().as_slice(), asset.name()].concat();
                                out.entry(subject).or_insert(tx.hash());
                            }
                        }
                    }
                }
            }

            out
        }

        fn first_minted_subjects(
            firsts: &HashMap<Vec<u8>, Hash<32>>,
            tx: &MultiEraTx<'_>,
        ) -> Vec<Vec<u8>> {
            let mut subjects = BTreeSet::new();

            for policy_assets in tx.mints() {
                for asset in policy_assets.assets() {
                    subjects.insert([policy_assets.policy().as_slice(), asset.name()].concat());
                }
            }

            let tx_hash = tx.hash();

            subjects
                .into_iter()
                .filter(|subject| firsts.get(subject) == Some(&tx_hash))
                .collect()
        }

        pub fn first_mints<D: Domain>(
            domain: &D,
            firsts: &HashMap<Vec<u8>, Hash<32>>,
            start: BlockSlot,
            order: Order,
            needed: usize,
        ) -> Vec<Vec<u8>> {
            let iter = domain.archive().get_range(Some(start), None).unwrap();

            let blocks: Box<dyn Iterator<Item = _>> = match order {
                Order::Asc => Box::new(iter),
                Order::Desc => Box::new(iter.rev()),
            };

            let mut found = Vec::new();

            for (_, body) in blocks {
                let block = MultiEraBlock::decode(&body).unwrap();
                let txs = block.txs();

                let txs: Box<dyn Iterator<Item = &MultiEraTx<'_>>> = match order {
                    Order::Asc => Box::new(txs.iter()),
                    Order::Desc => Box::new(txs.iter().rev()),
                };

                for tx in txs {
                    let mut minted = first_minted_subjects(firsts, tx);

                    if matches!(order, Order::Desc) {
                        minted.reverse();
                    }

                    found.append(&mut minted);

                    if found.len() >= needed {
                        return found;
                    }
                }
            }

            found
        }
    }

    /// The rich mint chain, and the domain behind it for the scan to read.
    fn first_mint_app() -> (TestApp, ToyDomain) {
        let mut domain = None;
        let app = TestApp::new_with_cfg_and_setup(first_mint_chain(), |inner, _| {
            domain = Some(inner.clone())
        });
        (app, domain.expect("setup ran"))
    }

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    async fn get_assets(app: &TestApp, query: &str) -> Vec<AssetsInner> {
        let path = format!("/assets{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        serde_json::from_slice(&bytes).expect("failed to parse assets")
    }

    #[tokio::test]
    async fn assets_all_happy_path() {
        // the default synthetic chain mints one asset in every tx
        let app = TestApp::new();
        let unit = app.vectors().asset_unit.clone();

        let assets = get_assets(&app, "").await;
        assert_eq!(assets.len(), 1);
        assert_eq!(assets[0].asset, unit);

        // quantity is the net supply, the same number `/assets/{asset}` reports
        let (status, bytes) = app.get_bytes(&format!("/assets/{unit}")).await;
        assert_eq!(status, StatusCode::OK);
        let detail: Asset = serde_json::from_slice(&bytes).expect("failed to parse asset");
        assert_eq!(assets[0].quantity, detail.quantity);
    }

    /// Three assets, one per block, so the listing order is the mint order.
    fn three_asset_app() -> (TestApp, Vec<String>) {
        let asset_names = ["FIRST", "SECOND", "THIRD"];
        let app = TestApp::new_with_cfg(SyntheticBlockConfig {
            block_count: asset_names.len(),
            txs_per_block: 1,
            asset_names_by_block: asset_names.iter().map(|x| (*x).to_string()).collect(),
            ..Default::default()
        });
        let policy = app.vectors().policy_id.clone();
        let units = asset_names
            .iter()
            .map(|name| format!("{policy}{}", hex::encode(name)))
            .collect();
        (app, units)
    }

    #[tokio::test]
    async fn assets_all_orders_by_first_mint() {
        let (app, units) = three_asset_app();

        let asc = get_assets(&app, "?order=asc").await;
        assert_eq!(asc.iter().map(|x| x.asset.clone()).collect_vec(), units);
        assert!(asc.iter().all(|x| x.quantity == "1"));

        let desc = get_assets(&app, "?order=desc").await;
        let expected = units.iter().rev().cloned().collect_vec();
        assert_eq!(desc.iter().map(|x| x.asset.clone()).collect_vec(), expected);
    }

    #[tokio::test]
    async fn assets_all_paginates_in_both_orders() {
        let (app, units) = three_asset_app();

        // asc: page 2 of size 1 is the second asset ever minted
        let page = get_assets(&app, "?order=asc&page=2&count=1").await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].asset, units[1]);

        // desc: page 1 of size 1 is the newest asset
        let page = get_assets(&app, "?order=desc&page=1&count=1").await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].asset, units[2]);

        // desc: page 3 of size 1 is the oldest asset
        let page = get_assets(&app, "?order=desc&page=3&count=1").await;
        assert_eq!(page.len(), 1);
        assert_eq!(page[0].asset, units[0]);

        // a page past the end is empty, not an error
        let page = get_assets(&app, "?page=4&count=1").await;
        assert!(page.is_empty());
    }

    /// Every page in both orders, as the scan the logs replaced answers it,
    /// over a chain whose first epochs are sealed and whose last are staged.
    #[tokio::test]
    async fn assets_all_matches_the_first_mint_scan() {
        let (app, domain) = first_mint_app();

        let sealed = domain
            .archive()
            .iter_logs_typed::<FirstMintChunk>(None)
            .unwrap()
            .count();
        let staged = domain
            .state()
            .iter_entities_typed::<FirstMintSet>(None)
            .unwrap()
            .count();
        assert!(sealed > 2, "{sealed} sealed rows");
        assert!(staged > 0, "nothing staged");

        let chain = dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
        let start = chain.epoch_start(chain.first_mary_epoch().unwrap());
        let firsts = scan::first_mint_txs(&domain);
        let total = scan::first_mints(&domain, &firsts, start, Order::Asc, usize::MAX).len();
        assert!(total > 300, "{total} first mints");

        for (order, name) in [(Order::Asc, "asc"), (Order::Desc, "desc")] {
            for count in [1, 7, 100] {
                for page in 1..=total.div_ceil(count) + 1 {
                    let skip = (page - 1) * count;
                    let expected = scan::first_mints(&domain, &firsts, start, order, skip + count)
                        .into_iter()
                        .skip(skip)
                        .take(count)
                        .map(hex::encode)
                        .collect_vec();

                    let query = format!("?order={name}&count={count}&page={page}");
                    let got = get_assets(&app, &query)
                        .await
                        .into_iter()
                        .map(|x| x.asset)
                        .collect_vec();

                    assert_eq!(got, expected, "{query}");
                }
            }
        }
    }

    /// Pages as deep as the chain goes are served: there is no scan to bound.
    #[tokio::test]
    async fn assets_all_serves_pages_past_the_scan_limit() {
        let (app, _) = first_mint_app();

        let page = get_assets(&app, "?count=100&page=4").await;
        assert!(!page.is_empty());

        let page = get_assets(&app, "?count=100&page=31").await;
        assert!(page.is_empty());
    }

    #[tokio::test]
    async fn assets_all_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/assets?count=0", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/assets?page=x", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/assets?order=sideways", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn assets_all_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/assets", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
