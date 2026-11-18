use jsonrpsee::{
    server::{RpcModule, Server},
    types::Params,
    IntoResponse,
};
use std::{future::Future, sync::Arc};
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

/// Registers `handler` as `method`, recording each call's metrics.
fn register<D, T, F, Fut>(
    module: &mut RpcModule<Context<D>>,
    method: &'static str,
    handler: F,
) -> Result<(), ServeError>
where
    D: Domain,
    F: Fn(Params<'static>, Arc<Context<D>>) -> Fut + Clone + Send + Sync + 'static,
    Fut: Future<Output = Result<T, Error>> + Send,
    Result<T, Error>: IntoResponse + 'static,
{
    module
        .register_async_method(method, move |params, context, _| {
            context
                .metrics
                .track(method, handler(params, context.clone()))
        })
        .map_err(|_| ServeError::Internal(format!("failed to register {method}").into()))?;

    Ok(())
}

/// The TRP methods, served over `context`.
fn rpc_module<D: Domain + SubmitExt>(
    context: Context<D>,
) -> Result<RpcModule<Context<D>>, ServeError> {
    let mut module = RpcModule::new(context);

    register(&mut module, "trp.resolve", methods::trp_resolve)?;
    register(&mut module, "trp.submit", methods::trp_submit)?;
    register(&mut module, "trp.checkStatus", methods::trp_check_status)?;
    register(&mut module, "trp.dumpLogs", methods::trp_dump_logs)?;
    register(&mut module, "trp.peekPending", methods::trp_peek_pending)?;
    register(&mut module, "trp.peekInflight", methods::trp_peek_inflight)?;

    module
        .register_method("health", |_, context, _| methods::health(context))
        .map_err(|_| ServeError::Internal("failed to register health".into()))?;

    Ok(module)
}

pub struct Driver;

impl<D: Domain + SubmitExt, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = TrpConfig;

    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        let cors_layer = if cfg.permissive_cors() {
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

        let module = rpc_module(Context {
            domain,
            config: Arc::new(cfg.clone()),
            metrics: metrics::Metrics::new(),
        })?;

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
