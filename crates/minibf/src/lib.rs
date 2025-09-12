use axum::{
    extract::Request,
    http::StatusCode,
    routing::{get, post},
    Router, ServiceExt,
};
use dolos_cardano::{
    model::{AccountState, AssetState, DRepState, EpochState, FixedNamespace, PoolState},
    ChainSummary, PParamsSet,
};
use itertools::Itertools;
use pallas::{crypto::hash::Hash, ledger::addresses::Network};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    net::SocketAddr,
    ops::{Deref, Range},
};
use tower::Layer;
use tower_http::{cors::CorsLayer, normalize_path::NormalizePathLayer, trace};
use tracing::Level;

use dolos_core::{
    ArchiveStore as _, BlockSlot, CancelToken, Domain, Entity, EntityKey, EraCbor, ServeError,
    State3Error, State3Store as _, TxOrder,
};

mod error;
pub(crate) mod mapping;
mod pagination;
mod routes;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub permissive_cors: Option<bool>,
    pub metadata_max_scan_depth: Option<usize>,
    pub token_registry_url: Option<String>,
    pub url: Option<String>,
}

#[derive(Clone)]
pub struct Facade<D: Domain> {
    pub inner: D,
    pub config: Config,
}

impl<D: Domain> Deref for Facade<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub type TxMap = HashMap<Hash<32>, Option<EraCbor>>;
pub type BlockWithTx = (Vec<u8>, TxOrder);
pub type BlockWithTxMap = HashMap<Hash<32>, BlockWithTx>;

impl<D: Domain> Facade<D> {
    pub fn get_tip_slot(&self) -> Result<BlockSlot, StatusCode> {
        let tip = self
            .state3()
            .read_cursor()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .ok_or(StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(tip)
    }

    pub fn get_network_id(&self) -> Result<Network, StatusCode> {
        match self.genesis().shelley.network_id.as_ref() {
            Some(x) if x == "Mainnet" => Ok(Network::Mainnet),
            Some(x) if x == "Testnet" => Ok(Network::Testnet),
            _ => Err(StatusCode::INTERNAL_SERVER_ERROR),
        }
    }

    pub fn get_chain_summary(&self) -> Result<ChainSummary, StatusCode> {
        let summary = dolos_cardano::eras::load_era_summary(&self.inner)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(summary)
    }

    pub fn get_live_pparams(&self) -> Result<PParamsSet, StatusCode> {
        let pparams = dolos_cardano::load_live_pparams(&self.inner)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(pparams)
    }

    pub fn get_tx(&self, hash: Hash<32>) -> Result<Option<EraCbor>, StatusCode> {
        let tx = self
            .archive()
            .get_tx(hash.as_slice())
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(tx)
    }

    pub fn get_tx_batch(
        &self,
        hashes: impl IntoIterator<Item = Hash<32>>,
    ) -> Result<TxMap, StatusCode> {
        let txs = hashes
            .into_iter()
            .map(|h| self.get_tx(h).map(|tx| (h, tx)))
            .try_collect()?;

        Ok(txs)
    }

    pub fn get_block_with_tx_batch(
        &self,
        hashes: impl IntoIterator<Item = Hash<32>>,
    ) -> Result<BlockWithTxMap, StatusCode> {
        let blocks = hashes
            .into_iter()
            .map(|h| {
                self.archive()
                    .get_block_with_tx(h.as_slice())
                    .map(|x| (h, x))
            })
            .filter_map_ok(|(k, v)| v.map(|x| (k, x)))
            .try_collect()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(blocks)
    }

    pub fn iter_cardano_entities<T>(
        &self,
        range: Option<Range<EntityKey>>,
    ) -> Result<impl Iterator<Item = Result<(EntityKey, T), State3Error>>, StatusCode>
    where
        T: FixedNamespace + Entity,
        Option<T>: From<D::Entity>,
    {
        let generic = self
            .state3()
            .iter_entities_typed(T::NS, range)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let mapped = generic.map(|x| x.map(|(k, v)| (k, T::from(v))));

        Ok(mapped)
    }

    pub fn read_cardano_entity<T>(&self, key: impl Into<EntityKey>) -> Result<Option<T>, StatusCode>
    where
        T: FixedNamespace,
        Option<T>: From<D::Entity>,
    {
        let key = key.into();

        let entity = self
            .state3()
            .read_entity_typed(T::NS, &key)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let downcast = entity.and_then(|e| Option::<T>::from(e));

        Ok(downcast)
    }
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver
where
    Option<AccountState>: From<D::Entity>,
    Option<PoolState>: From<D::Entity>,
    Option<AssetState>: From<D::Entity>,
    Option<EpochState>: From<D::Entity>,
    Option<DRepState>: From<D::Entity>,
{
    type Config = Config;

    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        let app = Router::new()
            .route("/", get(routes::root::<D>))
            .route("/health", get(routes::health::naked))
            .route("/health/clock", get(routes::health::clock))
            .route("/genesis", get(routes::genesis::naked::<D>))
            .route("/network", get(routes::network::naked::<D>))
            .route("/network/eras", get(routes::network::eras::<D>))
            .route(
                "/accounts/{stake_address}",
                get(routes::accounts::by_stake::<D>),
            )
            .route(
                "/accounts/{stake_address}/registrations",
                get(routes::accounts::by_stake_registrations::<D>),
            )
            .route(
                "/accounts/{stake_address}/delegations",
                get(routes::accounts::by_stake_delegations::<D>),
            )
            .route(
                "/accounts/{stake_address}/addresses",
                get(routes::accounts::by_stake_addresses::<D>),
            )
            .route(
                "/accounts/{stake_address}/utxos",
                get(routes::accounts::by_stake_utxos::<D>),
            )
            .route(
                "/accounts/{stake_address}/rewards",
                get(routes::accounts::by_stake_rewards::<D>),
            )
            .route(
                "/addresses/{address}/utxos",
                get(routes::addresses::utxos::<D>),
            )
            .route(
                "/addresses/{address}/utxos/{asset}",
                get(routes::addresses::utxos_with_asset::<D>),
            )
            .route(
                "/addresses/{address}/transactions",
                get(routes::addresses::transactions::<D>),
            )
            .route("/addresses/{address}/txs", get(routes::addresses::txs::<D>))
            .route("/blocks/latest", get(routes::blocks::latest::<D>))
            .route("/blocks/latest/txs", get(routes::blocks::latest_txs::<D>))
            .route(
                "/blocks/{hash_or_number}",
                get(routes::blocks::by_hash_or_number::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/next",
                get(routes::blocks::by_hash_or_number_next::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/previous",
                get(routes::blocks::by_hash_or_number_previous::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/txs",
                get(routes::blocks::by_hash_or_number_txs::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/addresses",
                get(routes::blocks::by_hash_or_number_addresses::<D>),
            )
            .route(
                "/blocks/slot/{slot_number}",
                get(routes::blocks::by_slot::<D>),
            )
            .route(
                "/epochs/{epoch}/parameters",
                get(routes::epochs::by_number_parameters::<D>),
            )
            .route(
                "/epochs/latest/parameters",
                get(routes::epochs::latest_parameters::<D>),
            )
            .route("/tx/submit", post(routes::tx::submit::route::<D>))
            .route("/txs/{tx_hash}", get(routes::txs::by_hash::<D>))
            .route("/txs/{tx_hash}/cbor", get(routes::txs::by_hash_cbor::<D>))
            .route("/txs/{tx_hash}/utxos", get(routes::txs::by_hash_utxos::<D>))
            .route(
                "/txs/{tx_hash}/metadata",
                get(routes::txs::by_hash_metadata::<D>),
            )
            .route(
                "/txs/{tx_hash}/metadata/cbor",
                get(routes::txs::by_hash_metadata_cbor::<D>),
            )
            .route(
                "/txs/{tx_hash}/redeemers",
                get(routes::txs::by_hash_redeemers::<D>),
            )
            .route(
                "/txs/{tx_hash}/withdrawals",
                get(routes::txs::by_hash_withdrawals::<D>),
            )
            .route(
                "/txs/{tx_hash}/delegations",
                get(routes::txs::by_hash_delegations::<D>),
            )
            .route("/txs/{tx_hash}/mirs", get(routes::txs::by_hash_mirs::<D>))
            .route(
                "/txs/{tx_hash}/pool_updates",
                get(routes::txs::by_hash_pool_updates::<D>),
            )
            .route(
                "/txs/{tx_hash}/pool_retires",
                get(routes::txs::by_hash_pool_retires::<D>),
            )
            .route(
                "/txs/{tx_hash}/stakes",
                get(routes::txs::by_hash_stakes::<D>),
            )
            .route("/assets/{subject}", get(routes::assets::by_subject::<D>))
            .route(
                "/metadata/txs/labels/{label}",
                get(routes::metadata::by_label_json::<D>),
            )
            .route(
                "/metadata/txs/labels/{label}/cbor",
                get(routes::metadata::by_label_cbor::<D>),
            )
            .route(
                "/pools/{id}/delegators",
                get(routes::pools::by_id_delegators::<D>),
            )
            .route("/pools/extended", get(routes::pools::all_extended::<D>))
            .route(
                "/governance/dreps/{drep_id}",
                get(routes::governance::drep_by_id::<D>),
            )
            .with_state(Facade::<D> {
                inner: domain,
                config: cfg.clone(),
            })
            .layer(
                trace::TraceLayer::new_for_http()
                    .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                    .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
            )
            .layer(if cfg.permissive_cors.unwrap_or_default() {
                CorsLayer::permissive()
            } else {
                CorsLayer::new()
            });
        let app = NormalizePathLayer::trim_trailing_slash().layer(app);

        let listener = tokio::net::TcpListener::bind(cfg.listen_address)
            .await
            .map_err(ServeError::BindError)?;

        axum::serve(listener, ServiceExt::<Request>::into_make_service(app))
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await
            .map_err(ServeError::ShutdownError)?;

        Ok(())
    }
}
