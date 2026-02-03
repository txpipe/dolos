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

use dolos_cardano::indexes::AsyncCardanoQueryExt;
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
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let chain = domain.get_chain_summary()?;

    let builder = TxModelBuilder::new(&raw, order)?
        .with_chain(chain)
        .with_historical_pparams::<D>(&domain)?;

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
{
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let network = domain.get_network_id()?;

    let (raw, order) = domain.get_block_by_tx_hash(&hash).await?;

    let chain = domain.get_chain_summary()?;
    let tx = TxModelBuilder::new(&raw, order)?
        .with_network(network)
        .with_chain(chain);

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
