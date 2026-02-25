use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use blockfrost_openapi::models::{
    address_transactions_content_inner::AddressTransactionsContentInner,
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
use dolos_core::{ArchiveStore as _, Domain};
use itertools::Either;
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    error::Error,
    hacks, log_and_500,
    mapping::{IntoModel as _, TxModelBuilder},
    pagination::{Order, Pagination, PaginationParameters},
    Facade,
};

pub async fn all_txs<D>(
    Query(params): Query<PaginationParameters>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<AddressTransactionsContentInner>>, Error>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let pagination = Pagination::try_from(params)?;
    pagination.enforce_max_scan_limit(domain.config.max_scan_items)?;

    let chain = domain.get_chain_summary()?;

    let blocks = domain
        .archive()
        .get_range(None, None)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let blocks = match pagination.order {
        Order::Asc => Either::Left(blocks),
        Order::Desc => Either::Right(blocks.rev()),
    };

    let mut results = Vec::new();
    let mut tx_count: usize = 0;

    for (_slot, block_cbor) in blocks {
        let block = MultiEraBlock::decode(&block_cbor)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let block_time = chain.slot_time(block.slot()) as i32;
        let block_height = block.number() as i32;

        for (tx_idx, tx) in block.txs().iter().enumerate() {
            if pagination.includes(tx_count) {
                results.push(AddressTransactionsContentInner {
                    tx_hash: hex::encode(tx.hash().as_slice()),
                    tx_index: tx_idx as i32,
                    block_height,
                    block_time,
                });
            }
            tx_count += 1;
            if tx_count >= pagination.to() {
                return Ok(Json(results));
            }
        }
    }

    Ok(Json(results))
}

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

    let (raw, order) = match domain.get_block_by_tx_hash(&hash).await {
        Ok(block) => block,
        Err(StatusCode::NOT_FOUND) => {
            return Ok(Json(hacks::genesis_tx_content_for_hash(&domain, &hash)?));
        }
        Err(err) => return Err(err),
    };

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

    let (raw, order) = match domain.get_block_by_tx_hash(&hash).await {
        Ok(block) => block,
        Err(StatusCode::NOT_FOUND) => {
            return Ok(Json(
                hacks::genesis_tx_utxos_for_hash(&domain, &hash).await?,
            ));
        }
        Err(err) => return Err(err),
    };

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
    use crate::test_support::{TestApp, TestFault};
    use blockfrost_openapi::models::{
        address_transactions_content_inner::AddressTransactionsContentInner,
        tx_content::TxContent, tx_content_cbor::TxContentCbor,
        tx_content_delegations_inner::TxContentDelegationsInner,
        tx_content_metadata_cbor_inner::TxContentMetadataCborInner,
        tx_content_metadata_inner::TxContentMetadataInner,
        tx_content_mirs_inner::TxContentMirsInner,
        tx_content_pool_certs_inner::TxContentPoolCertsInner,
        tx_content_pool_retires_inner::TxContentPoolRetiresInner,
        tx_content_redeemers_inner::TxContentRedeemersInner,
        tx_content_stake_addr_inner::TxContentStakeAddrInner, tx_content_utxo::TxContentUtxo,
        tx_content_withdrawals_inner::TxContentWithdrawalsInner,
    };

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
        let parsed: TxContent = serde_json::from_slice(&bytes).expect("failed to parse tx content");
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

    #[tokio::test]
    async fn all_txs_happy_path() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/txs").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
        let parsed: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse all txs");
        assert!(!parsed.is_empty());
    }

    #[tokio::test]
    async fn all_txs_order_asc() {
        let app = TestApp::new();
        let (status, bytes) = app.get_bytes("/txs?order=asc&count=100").await;
        assert_eq!(status, StatusCode::OK);
        let asc: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes).expect("failed to parse all txs asc");

        let (_, bytes_desc) = app.get_bytes("/txs?order=desc&count=100").await;
        let desc: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes_desc).expect("failed to parse all txs desc");

        // asc: block heights are non-decreasing
        let asc_heights: Vec<_> = asc.iter().map(|x| x.block_height).collect();
        assert!(asc_heights.windows(2).all(|w| w[0] <= w[1]));

        // desc: block heights are non-increasing
        let desc_heights: Vec<_> = desc.iter().map(|x| x.block_height).collect();
        assert!(desc_heights.windows(2).all(|w| w[0] >= w[1]));

        // both orderings cover the same set of transactions
        let mut asc_hashes: Vec<_> = asc.iter().map(|x| x.tx_hash.clone()).collect();
        let mut desc_hashes: Vec<_> = desc.iter().map(|x| x.tx_hash.clone()).collect();
        asc_hashes.sort();
        desc_hashes.sort();
        assert_eq!(asc_hashes, desc_hashes);
    }

    #[tokio::test]
    async fn all_txs_paginated() {
        let app = TestApp::new();
        let (status_1, bytes_1) = app.get_bytes("/txs?page=1&count=2").await;
        let (status_2, bytes_2) = app.get_bytes("/txs?page=2&count=2").await;
        assert_eq!(status_1, StatusCode::OK);
        assert_eq!(status_2, StatusCode::OK);

        let page_1: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes_1).expect("failed to parse page 1");
        let page_2: Vec<AddressTransactionsContentInner> =
            serde_json::from_slice(&bytes_2).expect("failed to parse page 2");

        assert_eq!(page_1.len(), 2);
        // pages should not overlap
        for tx in &page_1 {
            assert!(!page_2.iter().any(|t| t.tx_hash == tx.tx_hash));
        }
    }

    #[tokio::test]
    async fn all_txs_scan_limit_exceeded() {
        let app = TestApp::new();
        // page=31, count=100 => 3100 > DEFAULT_MAX_SCAN_ITEMS(3000)
        assert_status(&app, "/txs?page=31&count=100", StatusCode::BAD_REQUEST).await;
    }
}
