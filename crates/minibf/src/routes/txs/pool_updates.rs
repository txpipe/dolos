use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::tx_content_pool_certs_inner::TxContentPoolCertsInner;
use dolos_cardano::PoolState;
use dolos_core::Domain;

use crate::{
    mapping::{txs::TxModelBuilder, IntoModel as _},
    Facade,
};

use super::parse_tx_hash;

pub async fn by_hash_pool_updates<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolCertsInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let hash = parse_tx_hash(&tx_hash)?;

    let network = domain.get_network_id()?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let chain = domain.get_chain_summary()?;

    let mut tx = TxModelBuilder::new(&raw, order)?
        .with_network(network)
        .with_chain(chain);
    tx.fetch_pool_metadata().await?;
    tx.set_affected_pools(&domain).await?;

    tx.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::routes::txs::testing::*;
    use crate::test_support::{TestApp, TestFault};

    #[tokio::test]
    async fn txs_by_hash_pool_updates_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_updates");
        let _: Vec<TxContentPoolCertsInner> = get_ok(&app, &path).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{INVALID_HASH}/pool_updates");
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{MISSING_HASH}/pool_updates");
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_updates");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
