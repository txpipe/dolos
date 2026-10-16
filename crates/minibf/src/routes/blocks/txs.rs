use axum::{
    extract::{Path, Query, State},
    Json,
};
use dolos_core::Domain;

use crate::{
    error::Error,
    pagination::{Pagination, PaginationParameters},
    Facade,
};

use super::{
    genesis, load_block_by_hash_or_number, names_genesis, paged, paged_block_txs,
    parse_hash_or_number,
};

pub async fn by_hash_or_number_txs<D>(
    Path(hash_or_number): Path<String>,
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<String>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    let hash_or_number = parse_hash_or_number(&hash_or_number)?;

    if names_genesis(&domain, &hash_or_number)? {
        let txs = genesis::genesis_txs(&domain.genesis())?
            .into_iter()
            .map(|tx| tx.hash.to_string())
            .collect();

        return Ok(Json(paged(txs, &pagination)));
    }

    let block = load_block_by_hash_or_number(&domain, &hash_or_number).await?;

    Ok(Json(paged_block_txs(&block, &pagination)?))
}

#[cfg(test)]
mod tests {

    use crate::test_support::TestApp;
    use axum::http::StatusCode;

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_order_asc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/blocks/{}/txs?order=asc", block.block_hash);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse asc txs");

        assert_eq!(txs, block.tx_hashes);
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_order_desc() {
        let app = TestApp::new();
        let block = app.vectors().blocks.first().expect("missing block vectors");
        let path = format!("/blocks/{}/txs?order=desc", block.block_hash);
        let (status, bytes) = app.get_bytes(&path).await;
        assert_eq!(status, StatusCode::OK);

        let txs: Vec<String> = serde_json::from_slice(&bytes).expect("failed to parse desc txs");

        let mut reversed = block.tx_hashes.clone();
        reversed.reverse();
        assert_eq!(txs, reversed);
    }

    #[tokio::test]
    async fn blocks_by_hash_or_number_txs_of_genesis() {
        use crate::routes::blocks::testing::GENESIS_HASH;

        let app = TestApp::new();
        let get = |query: &'static str| {
            let app = &app;
            async move {
                let path = format!("/blocks/{GENESIS_HASH}/txs{query}");
                let (status, bytes) = app.get_bytes(&path).await;
                assert_eq!(status, StatusCode::OK);
                serde_json::from_slice::<Vec<String>>(&bytes).expect("failed to parse txs")
            }
        };

        // preview's eight genesis outputs, in db-sync's order
        let asc = get("").await;
        assert_eq!(asc.len(), 8);
        assert_eq!(
            asc[0],
            "4ceb4298a5d404ad5400513bd57f93693350ee1f499bf5b116db67a49e7e33f9"
        );

        // reversed, unlike Blockfrost's genesis listing, which pages from the
        // back but keeps each page ascending
        let mut desc = get("?order=desc").await;
        desc.reverse();
        assert_eq!(desc, asc);

        assert_eq!(get("?count=3&page=2").await, asc[3..6]);
    }
}
