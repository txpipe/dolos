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
use dolos_core::{ArchiveStore as _, Domain};

use crate::{
    mapping::{IntoModel as _, TxModelBuilder},
    Facade,
};

pub async fn by_hash<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContent>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(&hash)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let chain = domain.get_chain_summary()?;

    let pparams = domain.get_live_pparams()?;

    let tx = TxModelBuilder::new(&raw, order)?
        .with_chain(chain)
        .with_pparams(pparams);

    tx.into_response()
}

pub async fn by_hash_cbor<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContentCbor>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_utxos<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<TxContentUtxo>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let mut builder = TxModelBuilder::new(&raw, order)?;

    let deps = builder.required_deps()?;
    let deps = domain.get_tx_batch(deps)?;

    for (key, cbor) in deps.iter() {
        if let Some(cbor) = cbor {
            builder.load_dep(*key, cbor)?;
        }
    }

    builder.into_response()
}

pub async fn by_hash_metadata<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentMetadataInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_metadata_cbor<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentMetadataCborInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let builder = TxModelBuilder::new(&raw, order)?;

    builder.into_response()
}

pub async fn by_hash_redeemers<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentRedeemersInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let chain = domain.get_chain_summary()?;

    let mut builder = TxModelBuilder::new(&raw, order)?.with_chain(chain);

    let deps = builder.required_deps()?;
    let deps = domain.get_tx_batch(deps)?;

    for (key, cbor) in deps.iter() {
        if let Some(cbor) = cbor {
            builder.load_dep(*key, cbor)?;
        }
    }

    builder.into_response()
}

pub async fn by_hash_withdrawals<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentWithdrawalsInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_delegations<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentDelegationsInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let network = domain.get_network_id()?;
    let chain = domain.get_chain_summary()?;

    let tx = TxModelBuilder::new(&raw, order)?
        .with_network(network)
        .with_chain(chain);

    tx.into_response()
}

pub async fn by_hash_mirs<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentMirsInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let network = domain.get_network_id()?;

    let tx = TxModelBuilder::new(&raw, order)?.with_network(network);

    tx.into_response()
}

pub async fn by_hash_pool_retires<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolRetiresInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tx = TxModelBuilder::new(&raw, order)?;

    tx.into_response()
}

pub async fn by_hash_pool_updates<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentPoolCertsInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let network = domain.get_network_id()?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let chain = domain.get_chain_summary()?;
    let tx = TxModelBuilder::new(&raw, order)?
        .with_network(network)
        .with_chain(chain);

    tx.into_response()
}

pub async fn by_hash_stakes<D: Domain>(
    Path(tx_hash): Path<String>,
    State(domain): State<Facade<D>>,
) -> Result<Json<Vec<TxContentStakeAddrInner>>, StatusCode> {
    let hash = hex::decode(tx_hash).map_err(|_| StatusCode::BAD_REQUEST)?;

    let network = domain.get_network_id()?;

    let (raw, order) = domain
        .archive()
        .get_block_with_tx(hash.as_slice())
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;

    let tx = TxModelBuilder::new(&raw, order)?.with_network(network);

    tx.into_response()
}
