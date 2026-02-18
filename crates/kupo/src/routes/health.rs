use axum::{
    extract::State,
    http::{header, HeaderMap, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use dolos_core::{ArchiveStore as _, Domain};
use pallas::ledger::traverse::MultiEraBlock;

use crate::{
    types::{ConnectionStatus, Health, HealthConfiguration},
    Facade,
};

pub async fn health<D: Domain>(State(facade): State<Facade<D>>, headers: HeaderMap) -> Response {
    let tip = match facade
        .query()
        .run_blocking(|domain| Ok(domain.archive().get_tip()?))
        .await
    {
        Ok(tip) => tip,
        Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    let mut response_headers = HeaderMap::new();

    let most_recent_checkpoint = match tip {
        Some((slot, block)) => {
            let block = match MultiEraBlock::decode(&block) {
                Ok(value) => value,
                Err(_) => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
            };

            let header_hash = block.header().hash().to_string();

            let header_name = header::HeaderName::from_static("x-most-recent-checkpoint");
            if let Ok(value) = HeaderValue::from_str(&slot.to_string()) {
                response_headers.insert(header_name, value);
            }

            if let Ok(value) = HeaderValue::from_str(&header_hash) {
                response_headers.insert(header::ETAG, value);
            }

            Some(slot)
        }
        None => None,
    };

    let health = Health {
        connection_status: ConnectionStatus::Connected,
        most_recent_node_tip: None,
        most_recent_checkpoint,
        seconds_since_last_block: None,
        network_synchronization: None,
        configuration: HealthConfiguration::default(),
        version: format!("{}-2.11.0", env!("CARGO_PKG_VERSION")),
    };

    let wants_prometheus = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.contains("text/plain"))
        .unwrap_or(false);

    if wants_prometheus {
        response_headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain;charset=utf-8"),
        );

        return (StatusCode::OK, response_headers, health.to_prometheus()).into_response();
    }

    (StatusCode::OK, response_headers, Json(health)).into_response()
}
