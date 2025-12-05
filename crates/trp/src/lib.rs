use jsonrpsee::server::{RpcModule, Server};
use std::sync::Arc;
use tokio::select;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;

use dolos_core::{config::TrpConfig, CancelToken, Domain, ServeError};

mod compiler;
mod error;
mod mapping;
mod methods;
mod metrics;
mod utxos;

pub use error::Error;
pub use utxos::UtxoLock;

#[derive(Clone)]
pub struct Context<D: Domain> {
    pub domain: D,
    pub config: Arc<TrpConfig>,
    pub metrics: metrics::Metrics,
    pub locks: Arc<UtxoLock>,
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = TrpConfig;

    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
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
            .map_err(ServeError::BindError)?;

        let mut module = RpcModule::new(Context {
            domain,
            config: Arc::new(cfg.clone()),
            metrics: metrics::Metrics::new(),
            locks: Arc::new(UtxoLock::new()),
        });

        module
            .register_async_method("trp.resolve", |params, context, _| async move {
                let response = methods::trp_resolve(params, context.clone()).await;

                context.metrics.track_request(
                    "trp-resolve",
                    match response.as_ref() {
                        Ok(_) => 200,
                        Err(err) => err.code(),
                    },
                );

                response
            })
            .map_err(|_| ServeError::Internal("failed to register trp.resolve".into()))?;

        module
            .register_async_method("trp.submit", |params, context, _| async move {
                let response = methods::trp_submit(params, context.clone()).await;

                context.metrics.track_request(
                    "trp-submit",
                    match response.as_ref() {
                        Ok(_) => 200,
                        Err(err) => err.code(),
                    },
                );

                response
            })
            .map_err(|_| ServeError::Internal("failed to register trp.submit".into()))?;

        module
            .register_method("health", |_, context, _| methods::health(context))
            .map_err(|_| ServeError::Internal("failed to register health".into()))?;

        let handle = server.start(module);

        select! {
            _ = handle.clone().stopped() => {
                Ok(())
            }
            _ = cancel.cancelled() => {
                info!("exit requested, shutting down trp");
                let _ = handle.stop(); // Empty result with AlreadyStoppedError, can be ignored.
                Ok(())
            }
        }
    }
}
