use axum::{
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use tower_http::{cors::CorsLayer, trace};
use tracing::Level;

use crate::chain::ChainStore;
use crate::prelude::Error;
use crate::{ledger::pparams::Genesis, mempool::Mempool, state::LedgerStore};

mod common;
mod routes;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub permissive_cors: Option<bool>,
}

pub struct State {
    pub genesis: Arc<Genesis>,
    pub ledger: LedgerStore,
    pub chain: ChainStore,
    pub mempool: Mempool,
}

pub type SharedState = Arc<State>;

pub async fn serve(
    cfg: Config,
    genesis: Arc<Genesis>,
    ledger: LedgerStore,
    chain: ChainStore,
    mempool: Mempool,
    exit: CancellationToken,
) -> Result<(), Error> {
    let app = Router::new()
        .route(
            "/accounts/{stake_address}/utxos",
            get(routes::accounts::stake_address::utxos::route),
        )
        .route(
            "/addresses/{address}/utxos",
            get(routes::addresses::address::utxos::route),
        )
        .route(
            "/addresses/{address}/utxos/{asset}",
            get(routes::addresses::address::utxos::asset::route),
        )
        .route("/blocks/latest", get(routes::blocks::latest::route))
        .route(
            "/blocks/latest/txs",
            get(routes::blocks::latest::txs::route),
        )
        .route(
            "/blocks/{hash_or_number}",
            get(routes::blocks::hash_or_number::route),
        )
        .route(
            "/blocks/{hash_or_number}/addresses",
            get(routes::blocks::hash_or_number::addresses::route),
        )
        .route(
            "/blocks/{hash_or_number}/next",
            get(routes::blocks::hash_or_number::next::route),
        )
        .route(
            "/blocks/{hash_or_number}/previous",
            get(routes::blocks::hash_or_number::previous::route),
        )
        .route(
            "/blocks/{hash_or_number}/txs",
            get(routes::blocks::hash_or_number::txs::route),
        )
        .route(
            "/blocks/slot/{slot_number}",
            get(routes::blocks::slot::slot_number::route),
        )
        .route(
            "/epochs/latest/parameters",
            get(routes::epochs::latest::parameters::route),
        )
        .route("/tx/submit", post(routes::tx::submit::route))
        .route(
            "/txs/{tx_hash}/cbor",
            get(routes::txs::tx_hash::cbor::route),
        )
        .with_state(SharedState::new(State {
            genesis,
            ledger,
            mempool,
            chain,
        }))
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

    let listener = tokio::net::TcpListener::bind(cfg.listen_address)
        .await
        .map_err(|_| Error::ServerError("Failed to bind TCP listener for MiniBF".to_string()))?;

    axum::serve(listener, app)
        .with_graceful_shutdown(async move { exit.cancelled().await })
        .await
        .map_err(Error::server)?;

    Ok(())
}