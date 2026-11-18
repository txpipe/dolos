//! The heavy-scan limit and the request deadline, through the router.

use std::time::{Duration, Instant};

use axum::http::{header, StatusCode};
use dolos_core::{telemetry::queries::LimiterGauges, QueryLimiter};
use dolos_testing::metrics::{HistogramPoint, MetricsRecorder, Snapshot};
use serde_json::{json, Value};

use crate::test_support::{TestApp, TestFault};

const QUEUE: Duration = Duration::from_millis(300);
/// Scheduling allowance on a measured queue wait, well below the gap
/// between [`QUEUE`] and the default queue.
const QUEUE_SLACK: Duration = Duration::from_secs(1);
const SLOW_READ: Duration = Duration::from_secs(3);

fn limiter(app: &TestApp) -> QueryLimiter {
    dolos_core::Domain::query_limiter(app.domain()).clone()
}

fn admissions(snapshot: &Snapshot, outcome: &str) -> HistogramPoint {
    snapshot
        .histogram(
            "dolos.query.heavy.admission.duration",
            &[("outcome", outcome)],
        )
        .unwrap_or_else(|| panic!("no {outcome} admission"))
}

fn value(snapshot: &Snapshot, name: &str) -> f64 {
    snapshot.value(name, &[]).unwrap_or(0.0)
}

/// A route that reads no UTxOs: the metadata of the tip block.
fn light_path(app: &TestApp) -> String {
    let block = app.vectors().blocks.last().expect("missing block vectors");
    format!("/metadata/{}", block.slot)
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
    let capture = MetricsRecorder::default();
    let app = TestApp::new_admitting(
        None,
        QueryLimiter::with_heavy_scans(32, 1, QUEUE).with_meter(&capture.meter("test")),
        120,
    );
    let held = limiter(&app).admit_heavy().await.expect("a free slot");

    for root in ["/matches", "/v1/matches"] {
        for pattern in heavy_patterns(&app) {
            let path = format!("{root}/{pattern}");
            let started = Instant::now();
            let (status, headers, body) = app.get_response(&path).await;

            assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
            let waited = started.elapsed();
            assert!(waited >= QUEUE, "{path} skipped the queue");
            assert!(waited < QUEUE + QUEUE_SLACK, "{path} waited {waited:?}");
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

    // Each 503 is one rejected admission, as long as the queue.
    let rejected = admissions(&capture.collect(), "rejected");
    assert_eq!(rejected.count, 2 * heavy_patterns(&app).len() as u64);
    assert!(rejected.min.unwrap() >= QUEUE.as_secs_f64(), "{rejected:?}");

    assert_ne!(
        app.get_bytes("/metadata/0").await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    drop(held);

    for pattern in heavy_patterns(&app) {
        let path = format!("/matches/{pattern}");
        assert_eq!(app.get_bytes(&path).await.0, StatusCode::OK, "{path}");
    }
}

#[tokio::test]
async fn a_queued_heavy_match_proceeds_when_a_slot_frees() {
    let queue = Duration::from_secs(5);
    let capture = MetricsRecorder::default();
    let app = TestApp::new_admitting(
        None,
        QueryLimiter::with_heavy_scans(32, 1, queue).with_meter(&capture.meter("test")),
        120,
    );
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

    // The match asked for its slot before the release began its sleep, so
    // its admission took at least that sleep. The held slot is the other.
    let admitted = admissions(&capture.collect(), "admitted");
    assert_eq!(admitted.count, 2);
    assert!(admitted.max.unwrap() >= QUEUE.as_secs_f64(), "{admitted:?}");
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
    let capture = MetricsRecorder::default();
    let app = TestApp::new_admitting(
        Some(TestFault::SlowUtxoReads(SLOW_READ)),
        QueryLimiter::with_heavy_scans(32, 1, Duration::ZERO).with_meter(&capture.meter("test")),
        1,
    );
    let limiter = limiter(&app);
    let _gauges = LimiterGauges::with_meter(&limiter, &capture.meter("test"));
    let policy = format!("/matches/{}.*", app.vectors().policy_id);
    let address = format!("/matches/{}", app.vectors().address);

    let started = Instant::now();
    assert_eq!(app.get_bytes(&policy).await.0, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < SLOW_READ);

    assert_eq!(limiter.heavy_admitted(), 1);
    assert_eq!(limiter.heavy_in_use(), 1);

    // The gauges read the slot and permits the surviving read holds.
    let held = capture.collect();
    assert_eq!(value(&held, "dolos.query.limiter.capacity"), 32.0);
    assert_eq!(value(&held, "dolos.query.heavy.capacity"), 1.0);
    assert_eq!(value(&held, "dolos.query.heavy.share.capacity"), 16.0);
    assert_eq!(value(&held, "dolos.query.heavy.admitted"), 1.0);
    assert_eq!(value(&held, "dolos.query.heavy.share.in_use"), 1.0);
    assert_eq!(value(&held, "dolos.query.limiter.in_use"), 1.0);
    assert_eq!(value(&held, "dolos.query.blocking.in_flight"), 1.0);
    assert_eq!(value(&held, "dolos.query.blocking.outlived"), 1.0);

    assert_eq!(
        app.get_bytes(&address).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(app.get_bytes(&light_path(&app)).await.0, StatusCode::OK);

    let still = capture.collect();
    assert!(started.elapsed() < SLOW_READ);
    assert_eq!(value(&still, "dolos.query.blocking.outlived"), 1.0);

    wait_until_free(&limiter).await;
    assert!(started.elapsed() >= SLOW_READ);
    assert_eq!(limiter.heavy_in_use(), 0);

    // The closure lowers both counters as it ends, just after its slot frees,
    // one after the other.
    let ended = Instant::now();
    while {
        let snapshot = capture.collect();
        value(&snapshot, "dolos.query.blocking.in_flight") > 0.0
            || value(&snapshot, "dolos.query.blocking.outlived") > 0.0
    } {
        assert!(
            ended.elapsed() < Duration::from_secs(1),
            "the read never ended"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let free = capture.collect();
    assert_eq!(value(&free, "dolos.query.blocking.outlived"), 0.0);
    assert_eq!(value(&free, "dolos.query.heavy.admitted"), 0.0);
    assert_eq!(value(&free, "dolos.query.limiter.in_use"), 0.0);
}
