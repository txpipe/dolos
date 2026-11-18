//! Request metrics of the MiniKupo server.
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

/// The meter of the MiniKupo server's instruments.
pub const METER: &str = "dolos-minikupo";

/// The `surface` label of the MiniKupo server's samples.
pub const SURFACE: &str = "minikupo";

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

    fn duration(method: &str, route: &str, status: u16, count: u64) -> String {
        format!(
            "{DURATION}{{http.request.method={method},http.response.status_code={status},http.route={route},surface=minikupo}} count={count}"
        )
    }

    fn active(method: &str, route: &str, value: i64) -> String {
        format!(
            "{ACTIVE}{{http.request.method={method},http.route={route},surface=minikupo}} {value}"
        )
    }

    #[tokio::test]
    async fn a_templated_route_is_labelled_by_its_template_at_the_root_and_under_v1() {
        let app = TestApp::new();
        let slot = app.vectors().blocks.last().expect("a block").slot;
        let pattern = app.vectors().address.clone();

        for path in [
            format!("/metadata/{slot}"),
            format!("/v1/metadata/{slot}"),
            format!("/matches/{pattern}"),
        ] {
            assert_eq!(app.get_bytes(&path).await.0, StatusCode::OK, "{path}");
        }

        assert_eq!(
            app.metric_points(DURATION),
            [
                duration("GET", "/matches/{*pattern}", 200, 1),
                duration("GET", "/metadata/{slot-no}", 200, 1),
                duration("GET", "/v1/metadata/{slot-no}", 200, 1),
            ]
        );
        assert_eq!(
            app.metric_points(ACTIVE),
            [
                active("GET", "/matches/{*pattern}", 0),
                active("GET", "/metadata/{slot-no}", 0),
                active("GET", "/v1/metadata/{slot-no}", 0),
            ]
        );
        assert!(app.metric_points(DROPPED).is_empty());
    }

    #[tokio::test]
    async fn health_is_an_unknown_route() {
        let app = TestApp::new();

        for path in ["/health", "/v1/health", "/nonexistent"] {
            assert_eq!(app.get_bytes(path).await.0, StatusCode::NOT_FOUND, "{path}");
        }

        assert_eq!(
            app.metric_points(DURATION),
            [duration("GET", "unmatched", 404, 3)]
        );
    }

    #[tokio::test]
    async fn a_nonstandard_method_is_labelled_other() {
        let app = TestApp::new();
        let path = format!("/metadata/{}", app.vectors().blocks[0].slot);

        for method in [Method::from_bytes(b"BREW").unwrap(), Method::POST] {
            let request = Request::builder()
                .method(method)
                .uri(&path)
                .body(Body::empty())
                .unwrap();

            assert_eq!(app.send(request).await, StatusCode::METHOD_NOT_ALLOWED);
        }

        assert_eq!(
            app.metric_points(DURATION),
            [
                duration("POST", "/metadata/{slot-no}", 405, 1),
                duration("_OTHER", "/metadata/{slot-no}", 405, 1),
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
        let _held = dolos_core::Domain::query_limiter(app.domain())
            .admit_heavy()
            .await
            .expect("a free slot");
        let path = format!("/matches/{}.*", app.vectors().policy_id);

        assert_eq!(app.get_bytes(&path).await.0, StatusCode::GATEWAY_TIMEOUT);

        assert_eq!(
            app.metric_points(DURATION),
            [duration("GET", "/matches/{*pattern}", 504, 1)]
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
        let path = format!("/matches/{}.*", app.vectors().policy_id);

        let abandoned = tokio::time::timeout(Duration::from_millis(200), app.get_bytes(&path));
        assert!(abandoned.await.is_err(), "the slow read answered early");

        let route = "/matches/{*pattern}";
        assert_eq!(
            app.metric_points(DROPPED),
            [format!(
                "{DROPPED}{{http.request.method=GET,http.route={route},surface=minikupo}} 1"
            )]
        );
        assert_eq!(app.metric_points(ACTIVE), [active("GET", route, 0)]);
        assert!(app.metric_points(DURATION).is_empty());
    }
}
