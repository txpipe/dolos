//! `/scripts`: every script, in order of first appearance.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::scripts_inner::ScriptsInner;
use dolos_cardano::periods::first_scripts;
use dolos_core::SlotOrder;
use dolos_core::{ArchiveStore as _, Domain, DomainError};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// `GET /scripts`: every script seen on chain, ordered by first appearance.
///
/// The order is Blockfrost's, the order db-sync first sees scripts in. The
/// page comes from the first-script period logs, which need no block decoding
/// at any depth.
pub async fn all<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ScriptsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    // Scripts exist since Shelley, so nothing before that era can appear.
    let chain = domain.get_chain_summary()?;
    let shelley_epoch = chain.first_shelley_epoch();
    let (tip_epoch, _) = chain.slot_epoch(domain.get_tip_slot()?);

    let order = match pagination.order {
        Order::Asc => SlotOrder::Asc,
        Order::Desc => SlotOrder::Desc,
    };
    let (skip, count) = (pagination.skip(), pagination.count);

    let scripts = domain
        .query()
        .run_blocking(move |domain| {
            let Some((archived, _)) = domain.archive().get_tip()? else {
                return Ok(vec![]);
            };

            first_scripts::all_page(
                &domain,
                &chain,
                shelley_epoch..=tip_epoch,
                archived,
                order,
                skip,
                count,
            )
            .map_err(DomainError::from)
        })
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let items = scripts
        .into_iter()
        .map(|hash| ScriptsInner {
            script_hash: hash.to_string(),
        })
        .collect();

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use dolos_testing::{synthetic::SyntheticBlockConfig, toy_domain::ToyDomain};
    use itertools::Itertools;

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, bytes) = app.get_bytes(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    async fn get_scripts(app: &TestApp, query: &str) -> Vec<String> {
        let path = format!("/scripts{query}");
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} for {path} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let items: Vec<ScriptsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse scripts");

        items.into_iter().map(|x| x.script_hash).collect()
    }

    /// The first tx of every synthetic block carries the same three scripts:
    /// a native one as the reference script of its output, a plutus one as a
    /// witness and a native one in its auxiliary data. That is the listing
    /// order, and what every later block repeats.
    fn expected(app: &TestApp) -> Vec<String> {
        vec![
            app.vectors().script_hash.clone(),
            app.vectors().plutus_script_hash.clone(),
            dolos_testing::synthetic::aux_script_hash(),
        ]
    }

    #[tokio::test]
    async fn scripts_all_happy_path() {
        let app = TestApp::new();

        // five blocks repeat the scripts, each one is listed once
        let scripts = get_scripts(&app, "").await;
        assert_eq!(scripts, expected(&app));

        // everything listed resolves
        for script in scripts {
            assert_status(&app, &format!("/scripts/{script}"), StatusCode::OK).await;
        }
    }

    /// The roll commits state before the archive gets the batch's blocks, so
    /// a script the archive cannot serve yet is left off the listing, from
    /// either end.
    #[tokio::test]
    async fn scripts_all_lists_only_archived_scripts() {
        use dolos_cardano::model::{FirstScriptSet, ScriptKey, ScriptState};
        use dolos_cardano::periods::{GroupKey, Ungrouped};
        use dolos_core::{StateStore as _, StateWriter as _};
        use pallas::crypto::hash::Hash;

        let phantom = Hash::<28>::new([0xee; 28]);

        let app = TestApp::new_with_cfg_and_setup(
            SyntheticBlockConfig {
                block_count: 5,
                ..Default::default()
            },
            |domain, _| {
                // a fourth script, first seen by a batch whose blocks the
                // archive does not hold yet
                let chain =
                    dolos_cardano::eras::load_era_summary::<ToyDomain>(domain.state()).unwrap();
                let cursor = domain.state().read_cursor().unwrap().unwrap().slot();
                let (epoch, _) = chain.slot_epoch(cursor);

                let first = ScriptState {
                    slot: cursor + 1,
                    tx_index: 0,
                    order: 0,
                };

                let writer = domain.state().start_writer().unwrap();
                writer
                    .write_entity_typed(&ScriptKey(phantom), &first)
                    .unwrap();
                writer
                    .write_entity_typed(
                        &GroupKey {
                            group: Ungrouped,
                            epoch,
                            member: ScriptKey(phantom),
                        },
                        &FirstScriptSet::from(first),
                    )
                    .unwrap();
                writer.commit().unwrap();
            },
        );

        let expected = expected(&app);
        assert_eq!(get_scripts(&app, "").await, expected);

        let page = get_scripts(&app, "?order=desc&page=1&count=1").await;
        assert_eq!(page, vec![expected[2].clone()]);

        assert!(get_scripts(&app, "?page=4&count=1").await.is_empty());
    }

    #[tokio::test]
    async fn scripts_all_order_desc() {
        let app = TestApp::new();

        let desc = get_scripts(&app, "?order=desc").await;
        let reversed = expected(&app).into_iter().rev().collect_vec();
        assert_eq!(desc, reversed);
    }

    #[tokio::test]
    async fn scripts_all_paginates_in_both_orders() {
        let app = TestApp::new();
        let expected = expected(&app);

        // asc: page 2 of size 1 is the second script ever seen
        let page = get_scripts(&app, "?order=asc&page=2&count=1").await;
        assert_eq!(page, vec![expected[1].clone()]);

        // desc: page 1 of size 1 is the newest script
        let page = get_scripts(&app, "?order=desc&page=1&count=1").await;
        assert_eq!(page, vec![expected[2].clone()]);

        // desc: page 3 of size 1 is the oldest script
        let page = get_scripts(&app, "?order=desc&page=3&count=1").await;
        assert_eq!(page, vec![expected[0].clone()]);

        // a page in the middle, from either side
        let page = get_scripts(&app, "?order=asc&page=2&count=2").await;
        assert_eq!(page, vec![expected[2].clone()]);
        let page = get_scripts(&app, "?order=desc&page=2&count=2").await;
        assert_eq!(page, vec![expected[0].clone()]);

        // a page that straddles the end is cut short, from either side
        let page = get_scripts(&app, "?order=asc&page=1&count=5").await;
        assert_eq!(page, expected);
        let page = get_scripts(&app, "?order=desc&page=1&count=5").await;
        assert_eq!(page, expected.iter().rev().cloned().collect_vec());
    }

    #[tokio::test]
    async fn scripts_all_past_the_end_is_empty() {
        let app = TestApp::new();

        for query in [
            "?page=4&count=1",
            "?order=desc&page=4&count=1",
            // as deep as a page goes: nothing is scanned to get there
            "?page=21474836",
            "?order=desc&page=21474836",
        ] {
            let page = get_scripts(&app, query).await;
            assert!(page.is_empty(), "{query} is not empty: {page:?}");
        }
    }

    #[tokio::test]
    async fn scripts_all_does_not_depend_on_scan_budget() {
        // a block budget of one: the listing reads the period logs, not blocks
        let app = TestApp::new_with_scan_limit(
            SyntheticBlockConfig {
                block_count: 4,
                txs_per_block: 1,
                ..Default::default()
            },
            1,
        );

        assert_eq!(get_scripts(&app, "").await, expected(&app));
        assert!(get_scripts(&app, "?page=2").await.is_empty());
    }

    #[tokio::test]
    async fn scripts_all_bad_request() {
        let app = TestApp::new();
        assert_status(&app, "/scripts?count=0", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/scripts?count=101", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/scripts?page=x", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/scripts?page=21474837", StatusCode::BAD_REQUEST).await;
        assert_status(&app, "/scripts?order=sideways", StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn scripts_all_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::StateStoreError));
        assert_status(&app, "/scripts", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
