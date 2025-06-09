use axum::{
    Router,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use tower_http::{cors::CorsLayer, trace};
use tracing::Level;

use dolos_core::{CancelToken, Domain, ServeError};

mod common;
mod routes;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub permissive_cors: Option<bool>,
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
            .route(
                "/txs/{tx_hash}/cbor",
                get(routes::txs::tx_hash::cbor::route::<D>),
            )
            .with_state(domain.clone())
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
            .map_err(ServeError::BindError)?;

        axum::serve(listener, app)
            .with_graceful_shutdown(async move { cancel.cancelled().await })
            .await
            .map_err(ServeError::ShutdownError)?;

        Ok(())
    }
}
