use axum::{
    extract::{Query, State},
    Json,
};
use dolos_core::Domain;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{paged_block_txs, tip_block};

pub async fn latest_txs<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let tip = tip_block(&domain)?;

    Ok(Json(paged_block_txs(&tip, &pagination)?))
}

#[cfg(test)]
mod tests {
    use axum::http::StatusCode;

    use crate::test_support::TestApp;

    #[tokio::test]
    async fn blocks_latest_txs_order_asc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs?order=asc").await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asc txs");

        assert_eq!(txs, block.tx_hashes);
    }

    #[tokio::test]
    async fn blocks_latest_txs_order_desc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, bytes) = app.get_bytes("/blocks/latest/txs?order=desc").await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse desc txs");

        let mut reversed = block.tx_hashes.clone();
        reversed.reverse();
        assert_eq!(txs, reversed);
    }
}
