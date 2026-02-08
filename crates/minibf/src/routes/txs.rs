use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    tx_content::TxContent, tx_content_cbor::TxContentCbor,
    tx_content_delegations_inner::TxContentDelegationsInner,
    tx_content_metadata_cbor_inner::TxContentMetadataCborInner,
    tx_content_metadata_inner::TxContentMetadataInner, tx_content_mirs_inner::TxContentMirsInner,
    tx_content_pool_certs_inner::TxContentPoolCertsInner,
    tx_content_pool_retires_inner::TxContentPoolRetiresInner,
    tx_content_redeemers_inner::TxContentRedeemersInner,
    tx_content_stake_addr_inner::TxContentStakeAddrInner, tx_content_utxo::TxContentUtxo,
    tx_content_withdrawals_inner::TxContentWithdrawalsInner,
};

use dolos_cardano::{indexes::AsyncCardanoQueryExt, AccountState, DRepState, PoolState};
use dolos_core::Domain;

use crate::{
    log_and_500,
    mapping::{IntoModel as _, TxModelBuilder},
    Facade,
};

pub async fn by_hash<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContent>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
    Option<DRepState>: From<D::Entity>,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let chain = domain.get_chain_summary()?;

    let mut builder = TxModelBuilder::new(&raw, order)?
        .with_chain(chain)
        .with_historical_pparams::<D>(&domain)?;

    builder.compute_deposit(&domain)?;

    builder.into_response()
}

pub async fn by_hash_cbor<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContentCbor>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_utxos<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContentUtxo>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let mut builder = TxModelBuilder::new(&raw, order)?;

    let mut consumed_deps = std::collections::HashMap::new();
    for x in builder.required_consumed_deps()? {
        let bytes: Vec<u8> = x.clone().into();
        let maybe = domain
            .query()
            .tx_by_spent_txo(&bytes)
            .await
            .map_err(log_and_500("failed to query tx by spent txo"))?;
        if let Some(tx) = maybe {
            consumed_deps.insert(x, tx);
        }
    }
    builder = builder.with_consumed_deps(consumed_deps);

    let deps = builder.required_deps()?;
    let deps = domain.get_tx_batch(deps).await?;

    for (key, cbor) in deps.iter() {
        if let Some(cbor) = cbor {
            builder.load_dep(*key, cbor)?;
        }
    }

    builder.into_response()
}

pub async fn by_hash_metadata<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentMetadataInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_metadata_cbor<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentMetadataCborInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let builder = TxModelBuilder::new(&raw, order)?;

    builder.into_response()
}

pub async fn by_hash_redeemers<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentRedeemersInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let chain = domain.get_chain_summary()?;

    let mut builder = TxModelBuilder::new(&raw, order)?
        .with_chain(chain)
        .with_historical_pparams::<D>(&domain)?;

    let deps = builder.required_deps()?;
    let deps = domain.get_tx_batch(deps).await?;

    for (key, cbor) in deps.iter() {
        if let Some(cbor) = cbor {
            builder.load_dep(*key, cbor)?;
        }
    }

    builder.into_response()
}

pub async fn by_hash_withdrawals<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentWithdrawalsInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_delegations<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentDelegationsInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let network = domain.get_network_id()?;
    let chain = domain.get_chain_summary()?;

    let tx = TxModelBuilder::new(&raw, order)?
        .with_network(network)
        .with_chain(chain);

    tx.into_response()
}

pub async fn by_hash_mirs<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentMirsInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let network = domain.get_network_id()?;

    let tx = TxModelBuilder::new(&raw, order)?.with_network(network);

    tx.into_response()
}

pub async fn by_hash_pool_retires<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolRetiresInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_pool_updates<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolCertsInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
    Option<PoolState>: From<D::Entity>,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

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

pub async fn by_hash_stakes<D>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentStakeAddrInner>>, StatusCode>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let network = domain.get_network_id()?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let tx = TxModelBuilder::new(&raw, order)?.with_network(network);

    tx.into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockfrost_openapi::models::{
        tx_content::TxContent, tx_content_cbor::TxContentCbor,
        tx_content_delegations_inner::TxContentDelegationsInner,
        tx_content_metadata_cbor_inner::TxContentMetadataCborInner,
        tx_content_metadata_inner::TxContentMetadataInner, tx_content_mirs_inner::TxContentMirsInner,
        tx_content_pool_certs_inner::TxContentPoolCertsInner,
        tx_content_pool_retires_inner::TxContentPoolRetiresInner,
        tx_content_redeemers_inner::TxContentRedeemersInner,
        tx_content_stake_addr_inner::TxContentStakeAddrInner, tx_content_utxo::TxContentUtxo,
        tx_content_withdrawals_inner::TxContentWithdrawalsInner,
    };
    use crate::test_support::{TestApp, TestFault};

    fn missing_hash() -> &'static str {
        "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    }

    fn invalid_hash() -> &'static str {
        "not-a-hash"
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

    #[tokio::test]
    async fn txs_by_hash_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let parsed: TxContent =
            serde_json::from_slice(&bytes).expect("failed to parse tx content");
        assert_eq!(parsed.hash, tx_hash);
        assert!(!parsed.block.is_empty());
        assert!(parsed.block_height > 0);
    }

    #[tokio::test]
    async fn txs_by_hash_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_cbor_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: TxContentCbor =
            serde_json::from_slice(&bytes).expect("failed to parse tx content cbor");
    }

    #[tokio::test]
    async fn txs_by_hash_cbor_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/cbor", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_cbor_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/cbor", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/utxos");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: TxContentUtxo =
            serde_json::from_slice(&bytes).expect("failed to parse tx content utxo");
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/utxos", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/utxos", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_utxos_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/utxos");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentMetadataInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx metadata");
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/metadata", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/metadata", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata/cbor");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentMetadataCborInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx metadata cbor");
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/metadata/cbor", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/metadata/cbor", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_metadata_cbor_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/metadata/cbor");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_redeemers_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/redeemers");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentRedeemersInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx redeemers");
    }

    #[tokio::test]
    async fn txs_by_hash_redeemers_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/redeemers", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_redeemers_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/redeemers", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_redeemers_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/redeemers");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }


    #[tokio::test]
    async fn txs_by_hash_delegations_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/delegations");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentDelegationsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx delegations");
    }

    #[tokio::test]
    async fn txs_by_hash_delegations_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/delegations", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_delegations_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/delegations", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_delegations_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/delegations");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_mirs_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/mirs");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentMirsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx mirs");
    }

    #[tokio::test]
    async fn txs_by_hash_mirs_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/mirs", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_mirs_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/mirs", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_mirs_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/mirs");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_retires_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_retires");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentPoolRetiresInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx pool retires");
    }

    #[tokio::test]
    async fn txs_by_hash_pool_retires_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/pool_retires", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_retires_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/pool_retires", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_retires_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_retires");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_updates");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentPoolCertsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx pool updates");
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/pool_updates", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/pool_updates", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_pool_updates_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/pool_updates");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_stakes_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/stakes");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentStakeAddrInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx stakes");
    }

    #[tokio::test]
    async fn txs_by_hash_stakes_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/stakes", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_stakes_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/stakes", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_stakes_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/stakes");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }

    #[tokio::test]
    async fn txs_by_hash_withdrawals_happy_path() {
        let app = TestApp::new();
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/withdrawals");
        let (status, bytes) = app.get_bytes(&path).await;

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let _: Vec<TxContentWithdrawalsInner> =
            serde_json::from_slice(&bytes).expect("failed to parse tx withdrawals");
    }

    #[tokio::test]
    async fn txs_by_hash_withdrawals_bad_request() {
        let app = TestApp::new();
        let path = format!("/txs/{}/withdrawals", invalid_hash());
        assert_status(&app, &path, StatusCode::BAD_REQUEST).await;
    }

    #[tokio::test]
    async fn txs_by_hash_withdrawals_not_found() {
        let app = TestApp::new();
        let path = format!("/txs/{}/withdrawals", missing_hash());
        assert_status(&app, &path, StatusCode::NOT_FOUND).await;
    }

    #[tokio::test]
    async fn txs_by_hash_withdrawals_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::IndexStoreError));
        let tx_hash = app.vectors().tx_hash.as_str();
        let path = format!("/txs/{tx_hash}/withdrawals");
        assert_status(&app, &path, StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
