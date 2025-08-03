use jsonrpsee::{
    server::{RpcModule, Server},
    types::ErrorCode,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};
use std::{net::SocketAddr, sync::Arc};
use tokio::select;
use tower::ServiceBuilder;
use tower_http::cors::CorsLayer;
use tracing::info;
use tx3_resolver::inputs::{CanonicalQuery, SearchSpace};

use dolos_core::{CancelToken, Domain, ServeError};

mod compiler;
mod mapping;
mod methods;
mod metrics;
mod utxos;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    StateError(#[from] dolos_core::StateError),

    #[error(transparent)]
    TraverseError(#[from] pallas::ledger::traverse::Error),

    #[error(transparent)]
    AddressError(#[from] pallas::ledger::addresses::Error),

    #[error(transparent)]
    DecodeError(#[from] pallas::codec::minicbor::decode::Error),

    #[error(transparent)]
    MempoolError(dolos_core::MempoolError),

    #[error(transparent)]
    ArgsError(#[from] tx3_sdk::trp::args::Error),

    #[error(transparent)]
    ResolveError(tx3_resolver::Error),

    #[error(transparent)]
    JsonRpcError(#[from] jsonrpsee::types::ErrorObjectOwned),

    #[error("TIR version {provided} is not supported, expected {expected}")]
    UnsupportedTir { expected: String, provided: String },

    #[error("invalid TIR envelope")]
    InvalidTirEnvelope,

    #[error("failed to decode IR bytes")]
    InvalidTirBytes,

    #[error("only txs from Conway era are supported")]
    UnsupportedTxEra,

    #[error("node can't resolve txs while running at era {era}")]
    UnsupportedEra { era: String },

    #[error("missing argument `{key}` of type {ty:?}")]
    MissingTxArg { key: String, ty: tx3_lang::ir::Type },

    #[error("input `{0}` not resolved")]
    InputNotResolved(String, CanonicalQuery, SearchSpace),

    #[error("tx script returned failure")]
    TxScriptFailure(Vec<String>),
}

impl From<tx3_resolver::Error> for Error {
    fn from(error: tx3_resolver::Error) -> Self {
        let tx3_resolver::Error::InputsError(error) = error else {
            return Error::ResolveError(error);
        };

        let tx3_resolver::inputs::Error::InputNotResolved(name, q, ss) = error else {
            return Error::ResolveError(error.into());
        };

        return Error::InputNotResolved(name, q, ss);
    }
}

impl From<dolos_core::MempoolError> for Error {
    fn from(error: dolos_core::MempoolError) -> Self {
        match error {
            dolos_core::MempoolError::Phase2ExplicitError(x) => Error::TxScriptFailure(x),
            _ => Error::MempoolError(error),
        }
    }
}

impl Error {
    pub fn code(&self) -> i32 {
        match self {
            Error::JsonRpcError(err) => err.code(),
            Error::InvalidTirEnvelope => ErrorCode::InvalidParams.code(),
            Error::InvalidTirBytes => ErrorCode::InvalidParams.code(),
            Error::ArgsError(_) => ErrorCode::InvalidParams.code(),
            Error::UnsupportedEra { .. } => ErrorCode::InternalError.code(),
            Error::StateError(_) => ErrorCode::InternalError.code(),
            Error::TraverseError(_) => ErrorCode::InternalError.code(),
            Error::AddressError(_) => ErrorCode::InternalError.code(),
            Error::DecodeError(_) => ErrorCode::InternalError.code(),
            Error::MempoolError(_) => ErrorCode::InternalError.code(),
            Error::ResolveError(_) => ErrorCode::InternalError.code(),
            // custom errors
            Error::UnsupportedTir { .. } => -32000,
            Error::UnsupportedTxEra => -32001,
            Error::MissingTxArg { .. } => -32002,
            Error::InputNotResolved(_, _, _) => -32003,
            Error::TxScriptFailure(_) => -32004,
        }
    }

    pub fn data(&self) -> Option<Value> {
        match self {
            Error::JsonRpcError(err) => err.data().and_then(|v| serde_json::to_value(v).ok()),
            Error::UnsupportedTir { provided, expected } => Some(json!({
                "provided": provided,
                "expected": expected,
            })),
            Error::InputNotResolved(name, q, ss) => Some(json!({
                "name": name,
                "query": {
                    "address": q.address.as_ref().map(|a| hex::encode(a)),
                    "min_amount": q.min_amount,
                    "refs": q.refs.iter().map(|r| format!("{}#{}", hex::encode(&r.txid), r.index)).collect::<Vec<_>>(),
                    "support_many": q.support_many,
                    "collateral": q.collateral,
                },
                "search_space": {
                    "matched": ss.matched.iter().map(|r| format!("{}#{}", hex::encode(&r.txid), r.index)).collect::<Vec<_>>(),
                    "by_address_count": ss.by_address_count,
                    "by_asset_class_count": ss.by_asset_class_count,
                    "by_ref_count": ss.by_ref_count,
                },
            })),
            Error::TxScriptFailure(x) => Some(json!({
                "logs": x,
            })),
            Error::MissingTxArg { key, ty } => Some(json!({
                "key": key,
                "type": ty,
            })),
            _ => None,
        }
    }
}

impl From<Error> for tx3_lang::backend::Error {
    fn from(error: Error) -> Self {
        tx3_lang::backend::Error::StoreError(error.to_string())
    }
}

impl From<Error> for jsonrpsee::types::ErrorObject<'_> {
    fn from(error: Error) -> Self {
        let message = error.to_string();

        jsonrpsee::types::ErrorObject::owned(error.code(), message, error.data())
    }
}

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
    pub metrics: metrics::Metrics,
}

pub struct Driver;

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver {
    type Config = Config;

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
