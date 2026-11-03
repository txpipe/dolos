//! The heavy-scan limit and the request deadline, through the router.

use std::time::{Duration, Instant};

use axum::http::{header, StatusCode};
use dolos_core::QueryLimiter;
use serde_json::{json, Value};

use crate::test_support::{TestApp, TestFault};

const QUEUE: Duration = Duration::from_millis(300);
const SLOW_READ: Duration = Duration::from_secs(3);

fn limiter(app: &TestApp) -> QueryLimiter {
    dolos_core::Domain::query_limiter(app.domain()).clone()
}

fn heavy_patterns(app: &TestApp) -> Vec<String> {
    let v = app.vectors();

    vec![
        v.address.clone(),
        v.stake_address.clone(),
        format!("{}/*", v.policy_id),
        format!("*/{}", v.policy_id),
        format!("{}.*", v.policy_id),
        format!("{}.{}", v.policy_id, v.asset_name_hex),
    ]
}

fn output_ref_patterns(app: &TestApp) -> Vec<String> {
    let tx = &app.vectors().tx_hash;
    vec![format!("0@{tx}"), format!("*@{tx}")]
}

async fn wait_until_free(limiter: &QueryLimiter) {
    let started = Instant::now();

    while limiter.heavy_admitted() > 0 {
        assert!(
            started.elapsed() < SLOW_READ * 3,
            "a heavy slot stayed taken"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn heavy_matches_wait_out_the_queue_then_get_503() {
    let app = TestApp::new_admitting(None, QueryLimiter::with_heavy_scans(32, 1, QUEUE), 120);
    let held = limiter(&app).admit_heavy().await.expect("a free slot");

    for root in ["/matches", "/v1/matches"] {
        for pattern in heavy_patterns(&app) {
            let path = format!("{root}/{pattern}");
            let started = Instant::now();
            let (status, headers, body) = app.get_response(&path).await;

            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
            assert!(started.elapsed() >= QUEUE, "{path} skipped the queue");
            assert_eq!(headers[header::RETRY_AFTER], "1", "{path}");

            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(
                body,
                json!({ "hint": "The node is serving its limit of heavy requests. Retry later." })
            );
        }

        for pattern in output_ref_patterns(&app) {
            let path = format!("{root}/{pattern}");
            assert_eq!(app.get_bytes(&path).await.0, StatusCode::OK, "{path}");
        }
    }

    for path in ["/health", "/metadata/0"] {
        assert_ne!(
            app.get_bytes(path).await.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{path}"
        );
    }

    drop(held);

    for pattern in heavy_patterns(&app) {
        let path = format!("/matches/{pattern}");
        assert_eq!(app.get_bytes(&path).await.0, StatusCode::OK, "{path}");
    }
}

#[tokio::test]
async fn a_queued_heavy_match_proceeds_when_a_slot_frees() {
    let queue = Duration::from_secs(5);
    let app = TestApp::new_admitting(None, QueryLimiter::with_heavy_scans(32, 1, queue), 120);
    let held = limiter(&app).admit_heavy().await.expect("a free slot");
    let path = format!("/matches/{}.*", app.vectors().policy_id);

    let started = Instant::now();
    let request = app.get_bytes(&path);
    let release = async {
        tokio::time::sleep(QUEUE).await;
        drop(held);
    };
    let ((status, _), ()) = tokio::join!(request, release);

    assert_eq!(status, StatusCode::OK);
    assert!(started.elapsed() < queue, "{:?}", started.elapsed());
}

/// The deadline wraps admission: a match waiting for a slot is answered 504
/// at the deadline, not 503 at the end of a longer queue.
#[tokio::test]
async fn the_deadline_counts_the_wait_for_a_slot() {
    let queue = Duration::from_secs(10);
    let app = TestApp::new_admitting(None, QueryLimiter::with_heavy_scans(32, 1, queue), 1);
    let _held = limiter(&app).admit_heavy().await.expect("a free slot");
    let path = format!("/matches/{}.*", app.vectors().policy_id);

    let started = Instant::now();
    let (status, body) = app.get_bytes(&path).await;

    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < Duration::from_secs(3));

    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        body,
        json!({ "hint": "The request took longer than this node's deadline." })
    );
}

/// A match cut by its deadline leaves its store read running; the slot stays
/// taken until that read ends, so no replacement enters beside it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_match_past_its_deadline_keeps_its_slot_until_its_read_ends() {
    let app = TestApp::new_admitting(
        Some(TestFault::SlowUtxoReads(SLOW_READ)),
        QueryLimiter::with_heavy_scans(32, 1, Duration::ZERO),
        1,
    );
    let limiter = limiter(&app);
    let policy = format!("/matches/{}.*", app.vectors().policy_id);
    let address = format!("/matches/{}", app.vectors().address);

    let started = Instant::now();
    assert_eq!(app.get_bytes(&policy).await.0, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < SLOW_READ);

    assert_eq!(limiter.heavy_admitted(), 1);
    assert_eq!(limiter.heavy_in_use(), 1);
    assert_eq!(
        app.get_bytes(&address).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(app.get_bytes("/health").await.0, StatusCode::OK);

    wait_until_free(&limiter).await;
    assert!(started.elapsed() >= SLOW_READ);
    assert_eq!(limiter.heavy_in_use(), 0);
}
