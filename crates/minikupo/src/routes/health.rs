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

#[cfg(test)]
mod tests {
    use axum::http::{header, StatusCode};

    use crate::{
        test_support::{TestApp, TestFault},
        types::{ConnectionStatus, Health},
    };

    async fn assert_status(app: &TestApp, path: &str, expected: StatusCode) {
        let (status, _, bytes) = app.get_response(path).await;
        assert_eq!(
            status,
            expected,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );
    }

    #[tokio::test]
    async fn health_json_happy_path() {
        let app = TestApp::new();
        let block = app.vectors().blocks.last().expect("missing block vectors");
        let (status, headers, bytes) = app.get_response("/health").await;
        let expected_checkpoint = block.slot.to_string();

        assert_eq!(
            status,
            StatusCode::OK,
            "unexpected status {status} with body: {}",
            String::from_utf8_lossy(&bytes)
        );

        let health: Health = serde_json::from_slice(&bytes).expect("failed to parse health");
        assert_eq!(health.connection_status, ConnectionStatus::Connected);
        assert_eq!(health.most_recent_checkpoint, Some(block.slot));
        assert_eq!(
            headers
                .get("x-most-recent-checkpoint")
                .and_then(|x| x.to_str().ok()),
            Some(expected_checkpoint.as_str())
        );
        assert_eq!(
            headers.get(header::ETAG).and_then(|x| x.to_str().ok()),
            Some(block.block_hash.as_str())
        );
    }

    #[tokio::test]
    async fn health_prometheus_happy_path() {
        let app = TestApp::new();
        let (status, headers, bytes) = app
            .get_response_with_accept("/health", Some("text/plain"))
            .await;

        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers
                .get(header::CONTENT_TYPE)
                .and_then(|x| x.to_str().ok()),
            Some("text/plain;charset=utf-8")
        );

        let body = String::from_utf8(bytes).expect("health body should be utf8");
        assert!(body.contains("kupo_most_recent_checkpoint"));
        assert!(body.contains("kupo_connection_status"));
    }

    #[tokio::test]
    async fn health_no_tip_happy_path() {
        let app = TestApp::new_empty();
        let (status, headers, bytes) = app.get_response("/health").await;

        assert_eq!(status, StatusCode::OK);

        let health: Health = serde_json::from_slice(&bytes).expect("failed to parse health");
        assert_eq!(health.most_recent_checkpoint, None);
        assert!(headers.get("x-most-recent-checkpoint").is_none());
        assert!(headers.get(header::ETAG).is_none());
    }

    #[tokio::test]
    async fn health_internal_error() {
        let app = TestApp::new_with_fault(Some(TestFault::ArchiveStoreError));
        assert_status(&app, "/health", StatusCode::INTERNAL_SERVER_ERROR).await;
    }
}
