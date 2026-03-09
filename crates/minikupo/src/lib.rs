use axum::{
    extract::Request,
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
    Json, Router, ServiceExt,
};
use dolos_cardano::indexes::{AsyncCardanoQueryExt, ScriptLanguage as CardanoLanguage};
use dolos_core::{config::MinikupoConfig, AsyncQueryFacade, CancelToken, Domain, ServeError};
use pallas::{codec::minicbor, crypto::hash::Hash};
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
    pub config: MinikupoConfig,
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

    pub async fn resolve_script(
        &self,
        script_hash: &Hash<28>,
    ) -> Result<Option<types::Script>, StatusCode>
    where
        D: Clone + Send + Sync + 'static,
    {
        let script = self
            .query()
            .script_by_hash(script_hash)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        Ok(script.map(|data| {
            let language = match data.language {
                CardanoLanguage::Native => types::ScriptLanguage::Native,
                CardanoLanguage::PlutusV1 => types::ScriptLanguage::PlutusV1,
                CardanoLanguage::PlutusV2 => types::ScriptLanguage::PlutusV2,
                CardanoLanguage::PlutusV3 => types::ScriptLanguage::PlutusV3,
            };

            types::Script {
                language,
                script: hex::encode(data.script),
            }
        }))
    }

    pub async fn resolve_datum(
        &self,
        datum_hash: &Hash<32>,
    ) -> Result<Option<types::Datum>, StatusCode>
    where
        D: Clone + Send + Sync + 'static,
    {
        let datum = self
            .query()
            .plutus_data(datum_hash)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

        let datum = datum
            .map(minicbor::to_vec)
            .transpose()
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .map(|bytes| types::Datum {
                datum: hex::encode(bytes.as_slice()),
            });

        Ok(datum)
    }
}

pub struct Driver;

pub fn build_router<D>(cfg: MinikupoConfig, domain: D) -> Router
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
        .merge(api_router::<D>())
        .nest("/v1", api_router::<D>())
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

fn api_router<D>() -> Router<Facade<D>>
where
    D: Domain + Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/matches/{*pattern}", get(routes::matches::by_pattern::<D>))
        .route("/datums/{datum-hash}", get(routes::datums::by_hash::<D>))
        .route("/scripts/{script-hash}", get(routes::scripts::by_hash::<D>))
        .route("/metadata/{slot-no}", get(routes::metadata::by_slot::<D>))
        .route("/health", get(routes::health::health::<D>))
}

impl<D: Domain, C: CancelToken> dolos_core::Driver<D, C> for Driver
where
    D: Clone + Send + Sync + 'static,
{
    type Config = MinikupoConfig;

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

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::{to_bytes, Body},
        http::{Method, Request, StatusCode},
    };
    use dolos_core::config::MinikupoConfig;
    use dolos_testing::toy_domain::ToyDomain;
    use tower::util::ServiceExt;

    fn test_router() -> Router {
        let facade = Facade {
            inner: ToyDomain::new(None, None),
            config: MinikupoConfig {
                listen_address: "[::]:0".parse().expect("invalid listen address"),
                permissive_cors: None,
            },
        };

        build_router_with_facade(facade)
    }

    async fn get_response(path: &str) -> (StatusCode, Vec<u8>) {
        let request = Request::builder()
            .method(Method::GET)
            .uri(path)
            .body(Body::empty())
            .expect("failed to build request");

        let response = test_router()
            .oneshot(request)
            .await
            .expect("request failed");

        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("failed to read response body");

        (status, bytes.to_vec())
    }

    async fn assert_same_response(path: &str) {
        let (root_status, root_body) = get_response(path).await;
        let (v1_status, v1_body) = get_response(&format!("/v1{path}")).await;

        assert_eq!(root_status, v1_status);
        assert_eq!(root_body, v1_body);
    }

    #[tokio::test]
    async fn exposes_health_under_v1() {
        assert_same_response("/health").await;
    }

    #[tokio::test]
    async fn exposes_matches_under_v1() {
        assert_same_response("/matches/not-a-valid-pattern").await;
    }

    #[tokio::test]
    async fn exposes_datums_under_v1() {
        assert_same_response("/datums/not-a-valid-hash").await;
    }

    #[tokio::test]
    async fn exposes_scripts_under_v1() {
        assert_same_response("/scripts/not-a-valid-hash").await;
    }

    #[tokio::test]
    async fn exposes_metadata_under_v1() {
        assert_same_response("/metadata/99999999").await;
    }
}
