use futures_util::future::try_join;
use jsonrpsee::server::{RpcModule, Server};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;

use dolos_core::{Domain, Genesis};

use crate::prelude::Error;

mod adapter;
mod methods;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
    pub max_optimize_rounds: u8,
    pub permissive_cors: Option<bool>,
}

#[derive(Clone)]
pub struct Context<D: Domain> {
    pub domain: D,
    pub config: Arc<Config>,
}

pub async fn serve<D: Domain>(
    cfg: Config,
    domain: D,
    exit: CancellationToken,
) -> Result<(), Error> {
    let cors_layer = if cfg.permissive_cors.unwrap_or_default() {
        CorsLayer::permissive()
    } else {
        CorsLayer::new()
    };

    let middleware = ServiceBuilder::new().layer(cors_layer);
    let server = Server::builder()
        .set_http_middleware(middleware)
        .build(cfg.listen_address)
        .await
        .map_err(Error::server)?;

    let mut module = RpcModule::new(Context {
        domain,
        config: Arc::new(cfg),
    });

    module
        .register_async_method("trp.resolve", |params, context, _| async {
            methods::trp_resolve(params, context).await
        })
        .map_err(Error::server)?;

    module
        .register_method("health", |_, context, _| methods::health(context))
        .map_err(Error::server)?;

    let handle = server.start(module);

    let server = async {
        handle.clone().stopped().await;
        Ok::<(), Error>(())
    };

    let cancellation = async {
        exit.cancelled().await;
        info!("Gracefully shuting down trp.");
        let _ = handle.stop(); // Empty result with AlreadyStoppedError, can be ignored.
        Ok::<(), Error>(())
    };

    try_join(server, cancellation).await?;
    Ok(())
}
