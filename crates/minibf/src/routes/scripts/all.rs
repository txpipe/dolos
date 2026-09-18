//! `/scripts`: every script, in order of first appearance.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::scripts_inner::ScriptsInner;
use dolos_cardano::model::{FixedNamespace as _, ScriptSeqState};
use dolos_core::{Domain, StateStore as _};

use crate::{
    error::Error,
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

/// One page of the script registry.
///
/// The roll numbers scripts in the order Blockfrost lists them (the db-sync
/// `script` row), densely, so a page is a key range: counted from the start
/// for `asc` and from the total for `desc`. Nothing is scanned and no block is
/// decoded, however deep the page is.
fn read_page<D: Domain>(domain: &D, pagination: &Pagination) -> Result<Vec<String>, StatusCode> {
    let state = domain.state();

    let total = ScriptSeqState::count(state).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let from = pagination.from() as u64;
    let count = pagination.count as u64;

    let (start, end) = match pagination.order {
        Order::Asc => (from.min(total), from.saturating_add(count).min(total)),
        Order::Desc => {
            let end = total.saturating_sub(from);
            (end.saturating_sub(count), end)
        }
    };

    let range = ScriptSeqState::key(start)..ScriptSeqState::key(end);

    let mut scripts: Vec<String> = state
        .iter_entities_typed::<ScriptSeqState>(ScriptSeqState::NS, Some(range))
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(|row| row.map(|(_, row)| row.script_hash.to_string()))
        .collect::<Result<_, _>>()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    if matches!(pagination.order, Order::Desc) {
        scripts.reverse();
    }

    Ok(scripts)
}

/// `GET /scripts`: every script seen on chain, ordered by first appearance.
pub async fn all<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<ScriptsInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;

    let scripts = domain
        .query()
        .run_blocking(move |domain| Ok(read_page(&domain, &pagination)))
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)??;

    let items = scripts
        .into_iter()
        .map(|script_hash| ScriptsInner { script_hash })
        .collect();

    Ok(Json(items))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{TestApp, TestFault};
    use dolos_testing::synthetic::SyntheticBlockConfig;
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

    /// The first tx of every synthetic block carries the same two scripts: a
    /// native one as the reference script of its output and a plutus one as a
    /// witness. That is the listing order, and what every later block repeats.
    fn expected(app: &TestApp) -> Vec<String> {
        vec![
            app.vectors().script_hash.clone(),
            app.vectors().plutus_script_hash.clone(),
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
        assert_eq!(page, vec![expected[1].clone()]);

        // desc: page 2 of size 1 is the oldest script
        let page = get_scripts(&app, "?order=desc&page=2&count=1").await;
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
            "?page=3&count=1",
            "?order=desc&page=3&count=1",
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
        // a block budget of one: the listing reads the registry, not blocks
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
