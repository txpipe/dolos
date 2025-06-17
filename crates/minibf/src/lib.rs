use axum::{
    extract::Request,
    http::StatusCode,
    routing::{get, post},
    Router, ServiceExt,
};
use dolos_cardano::pparams::ChainSummary;
use itertools::Itertools;
use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraUpdate};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, net::SocketAddr, ops::Deref};
use tower::Layer;
use tower_http::{cors::CorsLayer, normalize_path::NormalizePathLayer, trace};
use tracing::Level;

use dolos_core::{ArchiveStore as _, CancelToken, Domain, EraCbor, ServeError, StateStore as _};

pub(crate) mod mapping;
mod pagination;
mod routes;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub permissive_cors: Option<bool>,
}

#[derive(Clone)]
pub struct Facade<D: Domain> {
    pub inner: D,
}

impl<D: Domain> Deref for Facade<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

pub type TxMap = HashMap<Hash<32>, Option<EraCbor>>;

impl<D: Domain> Facade<D> {
    pub fn get_chain_summary(&self) -> Result<ChainSummary, StatusCode> {
        let tip = self
            .state()
            .cursor()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let slot = tip.map(|t| t.slot()).unwrap_or_default();

        let updates = self
            .state()
            .get_pparams(slot)
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .into_iter()
            .map(|eracbor| {
                MultiEraUpdate::try_from(eracbor).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
            })
            .collect::<Result<Vec<MultiEraUpdate>, StatusCode>>()?;

        let summary = dolos_cardano::pparams::fold_with_hacks(self.genesis(), &updates, slot);

        Ok(summary)
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
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = Config;

    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        let app = Router::new()
            .route(
                "/accounts/{stake_address}/utxos",
                get(routes::accounts::stake_address::utxos::route::<D>),
            )
            .route(
                "/addresses/{address}/utxos",
                get(routes::addresses::address::utxos::route::<D>),
            )
            .route(
                "/addresses/{address}/utxos/{asset}",
                get(routes::addresses::address::utxos::asset::route::<D>),
            )
            .route("/blocks/latest", get(routes::blocks::latest::route::<D>))
            .route(
                "/blocks/latest/txs",
                get(routes::blocks::latest::txs::route::<D>),
            )
            .route(
                "/blocks/{hash_or_number}",
                get(routes::blocks::hash_or_number::route::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/addresses",
                get(routes::blocks::hash_or_number::addresses::route::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/next",
                get(routes::blocks::hash_or_number::next::route::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/previous",
                get(routes::blocks::hash_or_number::previous::route::<D>),
            )
            .route(
                "/blocks/{hash_or_number}/txs",
                get(routes::blocks::hash_or_number::txs::route::<D>),
            )
            .route(
                "/blocks/slot/{slot_number}",
                get(routes::blocks::slot::slot_number::route::<D>),
            )
            .route(
                "/epochs/latest/parameters",
                get(routes::epochs::latest::parameters::route::<D>),
            )
            .route("/tx/submit", post(routes::tx::submit::route::<D>))
            .route("/txs/{tx_hash}", get(routes::txs::by_hash::<D>))
            .route("/txs/{tx_hash}/cbor", get(routes::txs::by_hash_cbor::<D>))
            .route("/txs/{tx_hash}/utxos", get(routes::txs::by_hash_utxos::<D>))
            .with_state(Facade::<D> { inner: domain })
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
