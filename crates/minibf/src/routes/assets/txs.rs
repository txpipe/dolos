use axum::{
    extract::{Path, Query, State},
    Json,
};
use dolos_core::Domain;

use crate::{error::Error, pagination::PaginationParameters, Facade};

use super::by_subject_transactions;

/// Alias of `/assets/{subject}/transactions` that returns only the tx hashes.
/// Same scan, same order/count/page pagination, thinner payload.
pub async fn by_subject_txs<D>(
    path: Path<String>,
    Query(mut params): Query<PaginationParameters>,
    state: State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    // Blockfrost does not define `from`/`to` for this variant and ignores any
    // supplied values, so the alias drops them before delegating.
    params.from = None;
    params.to = None;

    let Json(transactions) = by_subject_transactions(path, Query(params), state).await?;

    let hashes = transactions.into_iter().map(|x| x.tx_hash).collect();

    Ok(Json(hashes))
}

#[cfg(test)]
mod tests {
    use crate::{
        routes::assets::testing::*,
        test_support::{TestApp, TestFault},
    };
    use axum::http::StatusCode;
    use blockfrost_openapi::models::asset_transactions_inner::AssetTransactionsInner;

    #[tokio::test]
    async fn assets_by_subject_txs_happy_path() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/txs");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let hashes: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert!(!hashes.is_empty());

        let known: std::collections::HashSet<&String> = app
            .vectors()
            .blocks
            .iter()
            .flat_map(|block| block.tx_hashes.iter())
            .collect();
        for hash in &hashes {
            assert!(known.contains(hash), "unknown tx hash {hash}");
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_matches_transactions() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();

        for query in [
            "",
            "?order=desc",
            "?page=2&count=2",
            "?order=desc&page=1&count=3",
        ] {
            let (status, bytes) = app
                .get_bytes(&format!("/assets/{asset}/transactions{query}"))
                .await;
            assert_eq!(status, StatusCode::OK);
            let expected: Vec<String> =
                serde_json::from_slice::<Vec<AssetTransactionsInner>>(&bytes)
                    .expect("failed to parse asset transactions")
                    .into_iter()
                    .map(|x| x.tx_hash)
                    .collect();

            assert!(!expected.is_empty(), "no transactions for query {query:?}");

            let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/txs{query}")).await;
            assert_eq!(status, StatusCode::OK);
            let actual: Vec<String> =
                serde_json::from_slice(&bytes).expect("failed to parse asset txs");

            assert_eq!(actual, expected, "mismatch for query {query:?}");
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_ignores_from_to() {
        let app = TestApp::new();
        let asset = app.vectors().asset_unit.as_str();
        let block = app.vectors().blocks.first().expect("missing block vectors");

        let (status, bytes) = app.get_bytes(&format!("/assets/{asset}/txs")).await;
        assert_eq!(status, StatusCode::OK);
        let all: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asset txs");

        // the range must actually constrain `/transactions`, otherwise the
        // comparison below proves nothing
        let (status, bytes) = app
            .get_bytes(&format!(
                "/assets/{asset}/transactions?from={}&to={}",
                block.block_number, block.block_number
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let constrained: Vec<AssetTransactionsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse asset transactions");
        assert!(constrained.len() < all.len());

        let (status, bytes) = app
            .get_bytes(&format!(
                "/assets/{asset}/txs?from={}&to={}",
                block.block_number, block.block_number
            ))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ranged: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert_eq!(ranged, all);

        // a malformed range is ignored as well, never validated
        let (status, bytes) = app
            .get_bytes(&format!("/assets/{asset}/txs?from=not-a-number"))
            .await;
        assert_eq!(status, StatusCode::OK);
        let ranged: Vec<String> =
            serde_json::from_slice(&bytes).expect("failed to parse asset txs");
        assert_eq!(ranged, all);
    }

    #[tokio::test]
    async fn assets_by_subject_txs_bad_request() {
        let app = TestApp::new();
        for asset in invalid_assets() {
            let path = format!("/assets/{asset}/txs");
            assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
        }
    }

    #[tokio::test]
    async fn assets_by_subject_txs_not_found() {
        let app = TestApp::new();
        let path = format!("/assets/{}/txs", missing_asset());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn assets_by_subject_txs_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let asset = app.vectors().asset_unit.as_str();
        let path = format!("/assets/{asset}/txs");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
