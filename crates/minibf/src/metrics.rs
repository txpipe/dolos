//! Request metrics of the MiniBF server.
//!
//! [`record`] times every request with the shared HTTP request instruments,
//! labelled by route template, method and status. It runs outside the
//! deadline, so a request the deadline cut is a 504 and a dropped request is
//! one its client abandoned.

use axum::{
    extract::{MatchedPath, Request, State},
    http::Method,
    middleware::Next,
    response::Response,
};
use dolos_core::telemetry::requests::{Family, RequestMetrics};
use opentelemetry::KeyValue;

/// The meter of the MiniBF server's instruments.
pub const METER: &str = "dolos-minibf";

/// The `surface` label of the MiniBF server's samples.
pub const SURFACE: &str = "minibf";

const HTTP_ROUTE: &str = "http.route";
const HTTP_REQUEST_METHOD: &str = "http.request.method";
const HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";

/// The route label of a request no route matched.
const UNMATCHED: &str = "unmatched";

/// The method label of a request whose method is not a standard one.
const OTHER_METHOD: &str = "_OTHER";

const STANDARD_METHODS: [&str; 9] = [
    "GET", "HEAD", "POST", "PUT", "DELETE", "CONNECT", "OPTIONS", "TRACE", "PATCH",
];

/// The instruments on the global [`METER`].
pub fn request_metrics() -> RequestMetrics {
    RequestMetrics::new(Family::Http, METER, SURFACE)
}

fn method_label(method: &Method) -> &'static str {
    STANDARD_METHODS
        .into_iter()
        .find(|standard| *standard == method.as_str())
        .unwrap_or(OTHER_METHOD)
}

/// Records one request. Add it with `Router::layer`, so it sees the
/// request's [`MatchedPath`].
pub async fn record(
    State(metrics): State<RequestMetrics>,
    request: Request,
    next: Next,
) -> Response {
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map_or(UNMATCHED, MatchedPath::as_str)
        .to_owned();

    let guard = metrics.start([
        KeyValue::new(HTTP_ROUTE, route),
        KeyValue::new(HTTP_REQUEST_METHOD, method_label(request.method())),
    ]);

    let response = next.run(request).await;

    guard.finish([KeyValue::new(
        HTTP_RESPONSE_STATUS_CODE,
        i64::from(response.status().as_u16()),
    )]);

    response
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
    };
    use dolos_core::QueryLimiter;

    use crate::test_support::{TestApp, TestFault};

    const DURATION: &str = "http.server.request.duration";
    const ACTIVE: &str = "http.server.active_requests";
    const DROPPED: &str = "dolos.http.server.request.dropped";

    fn duration(method: &str, route: &str, status: u16) -> String {
        format!(
            "{DURATION}{{http.request.method={method},http.response.status_code={status},http.route={route},surface=minibf}} count=1"
        )
    }

    fn active(method: &str, route: &str, value: i64) -> String {
        format!(
            "{ACTIVE}{{http.request.method={method},http.route={route},surface=minibf}} {value}"
        )
    }

    #[tokio::test]
    async fn a_templated_route_is_labelled_by_its_template() {
        let app = TestApp::new();
        let path = format!("/addresses/{}/utxos", app.vectors().address);

        assert_eq!(app.get_bytes(&path).await.0, StatusCode::OK);

        assert_eq!(
            app.metric_points(DURATION),
            [duration("GET", "/addresses/{address}/utxos", 200)]
        );
        assert_eq!(
            app.metric_points(ACTIVE),
            [active("GET", "/addresses/{address}/utxos", 0)]
        );
        assert!(app.metric_points(DROPPED).is_empty());
    }

    #[tokio::test]
    async fn a_route_under_the_prefix_is_labelled_with_the_prefix() {
        let app = TestApp::new_with_route_prefix("/api/v0");
        let block = &app.vectors().block_hash;

        let (status, _) = app.get_bytes(&format!("/api/v0/blocks/{block}")).await;
        assert_eq!(status, StatusCode::OK);

        assert_eq!(
            app.metric_points(DURATION),
            [duration("GET", "/api/v0/blocks/{hash_or_number}", 200)]
        );
    }

    #[tokio::test]
    async fn an_unknown_path_is_labelled_unmatched() {
        let app = TestApp::new_with_route_prefix("/api/v0");

        assert_eq!(
            app.get_bytes("/api/v0/nonexistent").await.0,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(app.get_bytes("/network").await.0, StatusCode::BAD_REQUEST);

        let unmatched = duration("GET", "unmatched", 400).replace("count=1", "count=2");
        assert_eq!(app.metric_points(DURATION), [unmatched]);
    }

    #[tokio::test]
    async fn a_nonstandard_method_is_labelled_other() {
        let app = TestApp::new();

        for method in [Method::from_bytes(b"BREW").unwrap(), Method::POST] {
            let request = Request::builder()
                .method(method)
                .uri("/network")
                .body(Body::empty())
                .unwrap();

            assert_eq!(app.send(request).await, StatusCode::METHOD_NOT_ALLOWED);
        }

        assert_eq!(
            app.metric_points(DURATION),
            [
                duration("POST", "/network", 405),
                duration("_OTHER", "/network", 405),
            ]
        );
    }

    /// The deadline answers inside the middleware, so the request is a 504,
    /// not a drop.
    #[tokio::test]
    async fn a_deadline_is_recorded_as_a_504() {
        let app = TestApp::new_admitting(
            None,
            QueryLimiter::with_heavy_scans(32, 1, Duration::from_secs(10)),
            1,
        );
        let _held = dolos_core::Domain::query_limiter(&app.facade().inner)
            .admit_heavy()
            .await
            .expect("a free slot");
        let path = format!("/assets/{}/addresses", app.vectors().asset_unit);

        assert_eq!(app.get_bytes(&path).await.0, StatusCode::GATEWAY_TIMEOUT);

        assert_eq!(
            app.metric_points(DURATION),
            [duration("GET", "/assets/{subject}/addresses", 504)]
        );
        assert!(app.metric_points(DROPPED).is_empty());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_its_caller_drops_counts_as_dropped() {
        let app = TestApp::new_admitting(
            Some(TestFault::SlowUtxoReads(Duration::from_secs(1))),
            QueryLimiter::with_heavy_scans(32, 1, Duration::ZERO),
            120,
        );
        let path = format!("/addresses/{}/utxos", app.vectors().address);

        let abandoned = tokio::time::timeout(Duration::from_millis(200), app.get_bytes(&path));
        assert!(abandoned.await.is_err(), "the slow read answered early");

        let route = "/addresses/{address}/utxos";
        assert_eq!(
            app.metric_points(DROPPED),
            [format!(
                "{DROPPED}{{http.request.method=GET,http.route={route},surface=minibf}} 1"
            )]
        );
        assert_eq!(app.metric_points(ACTIVE), [active("GET", route, 0)]);
        assert!(app.metric_points(DURATION).is_empty());
    }
}
