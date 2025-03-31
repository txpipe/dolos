use futures_util::future::try_join;
use jsonrpsee::server::{RpcModule, Server};
use serde::{Deserialize, Serialize};
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;

use crate::prelude::Error;
use crate::{ledger::pparams::Genesis, state::LedgerStore};

pub mod methods;

#[derive(Deserialize, Serialize, Clone)]
pub struct Config {
    pub listen_address: SocketAddr,
}

#[derive(Clone)]
pub struct Context {
    pub genesis: Arc<Genesis>,
    pub ledger: LedgerStore,
}

pub async fn serve(
    cfg: Config,
    genesis: Arc<Genesis>,
    ledger: LedgerStore,
    exit: CancellationToken,
) -> Result<(), Error> {
    let middleware = ServiceBuilder::new().layer(CorsLayer::permissive());
    let server = Server::builder()
        .set_http_middleware(middleware)
        .build(cfg.listen_address)
        .await
        .map_err(Error::server)?;

    let mut module = RpcModule::new(Context { genesis, ledger });
    module
        .register_async_method("trp.resolve", |params, context, _| async {
            methods::trp_resolve(params, context).await
        })
        .map_err(Error::server)?;

    let handle = server.start(module);

    let server = async {
        handle.clone().stopped().await;
        Ok::<(), Error>(())
    };

    let cancellation = async {
        exit.cancelled().await;
        info!("Gracefully shuting down minibf.");
        let _ = handle.stop(); // Empty result with AlreadyStoppedError, can be ignored.
        Ok::<(), Error>(())
    };

    try_join(server, cancellation).await?;
    Ok(())
}
