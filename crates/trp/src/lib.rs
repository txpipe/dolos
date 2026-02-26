use jsonrpsee::server::{RpcModule, Server};
use std::sync::Arc;
use tokio::select;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;

use dolos_core::{config::TrpConfig, CancelToken, Domain, ServeError, SubmitExt};

mod compiler;
mod error;
mod mapping;
mod methods;
mod metrics;
mod utxos;

pub use error::Error;

#[derive(Clone)]
pub struct Context<D: Domain> {
    pub domain: D,
    pub config: Arc<TrpConfig>,
    pub metrics: metrics::Metrics,
}

pub struct Driver;

impl<D: Domain + SubmitExt, C: CancelToken> dolos_core::Driver<D, C> for Driver {
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
            .register_async_method("trp.checkStatus", |params, context, _| async move {
                let response = methods::trp_check_status(params, context.clone()).await;

                context.metrics.track_request(
                    "trp-check-status",
                    match response.as_ref() {
                        Ok(_) => 200,
                        Err(err) => err.code(),
                    },
                );

                response
            })
            .map_err(|_| ServeError::Internal("failed to register trp.checkStatus".into()))?;

        module
            .register_async_method("trp.dumpLogs", |params, context, _| async move {
                let response = methods::trp_dump_logs(params, context.clone()).await;

                context.metrics.track_request(
                    "trp-dump-logs",
                    match response.as_ref() {
                        Ok(_) => 200,
                        Err(err) => err.code(),
                    },
                );

                response
            })
            .map_err(|_| ServeError::Internal("failed to register trp.dumpLogs".into()))?;

        module
            .register_async_method("trp.peekPending", |params, context, _| async move {
                let response = methods::trp_peek_pending(params, context.clone()).await;

                context.metrics.track_request(
                    "trp-peek-pending",
                    match response.as_ref() {
                        Ok(_) => 200,
                        Err(err) => err.code(),
                    },
                );

                response
            })
            .map_err(|_| ServeError::Internal("failed to register trp.peekPending".into()))?;

        module
            .register_async_method("trp.peekInflight", |params, context, _| async move {
                let response = methods::trp_peek_inflight(params, context.clone()).await;

                context.metrics.track_request(
                    "trp-peek-inflight",
                    match response.as_ref() {
                        Ok(_) => 200,
                        Err(err) => err.code(),
                    },
                );

                response
            })
            .map_err(|_| ServeError::Internal("failed to register trp.peekInflight".into()))?;

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
