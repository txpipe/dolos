use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router, ServiceExt,
};
use dolos_core::{config::KupoConfig, AsyncQueryFacade, CancelToken, Domain, ServeError};
use std::ops::Deref;
use tower_http::{cors::CorsLayer, normalize_path::NormalizePathLayer, trace};
use tracing::Level;

use crate::types::BadRequest;

pub mod patterns;
mod routes;
mod types;

#[derive(Clone)]
pub struct Facade<D: Domain> {
    pub inner: D,
    pub config: KupoConfig,
}

impl<D: Domain> Deref for Facade<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<D: Domain> Facade<D> {
    pub fn query(&self) -> AsyncQueryFacade<D>
    where
        D: Clone + Send + Sync + 'static,
    {
        AsyncQueryFacade::new(self.inner.clone())
    }
}

pub struct Driver;

pub fn build_router<D>(cfg: KupoConfig, domain: D) -> Router
where
    D: Domain + Clone + Send + Sync + 'static,
{
    build_router_with_facade(Facade {
        inner: domain,
        config: cfg,
    })
}

pub(crate) fn build_router_with_facade<D>(facade: Facade<D>) -> Router
where
    D: Domain + Clone + Send + Sync + 'static,
{
    let permissive_cors = facade.config.permissive_cors.unwrap_or_default();
    let app = Router::new()
        .route("/matches/{*pattern}", get(routes::matches::by_pattern::<D>))
        .route("/datums/{datum-hash}", get(routes::datums::by_hash::<D>))
        .route("/scripts/{script-hash}", get(routes::scripts::by_hash::<D>))
        .route("/metadata/{slot-no}", get(routes::metadata::by_slot::<D>))
        .route("/health", get(routes::health::health::<D>))
        .with_state(facade)
        .layer(
            trace::TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        )
        .layer(if permissive_cors {
            CorsLayer::permissive()
        } else {
            CorsLayer::new()
        });

    app.layer(NormalizePathLayer::trim_trailing_slash())
}

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver
where
    D: Clone + Send + Sync + 'static,
{
    type Config = KupoConfig;

    async fn run(cfg: Self::Config, domain: D, cancel: C) -> Result<(), ServeError> {
        let app = build_router(cfg.clone(), domain);

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

pub(crate) fn bad_request(hint: impl Into<String>) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(BadRequest {
            hint: Some(hint.into()),
        }),
    )
        .into_response()
}
