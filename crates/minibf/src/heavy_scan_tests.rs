//! The heavy-scan limit and the request deadline, through the router.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use axum::http::{header, StatusCode};
use dolos_core::QueryLimiter;
use serde_json::{json, Value};

use crate::test_support::{TestApp, TestFault};

const QUEUE: Duration = Duration::from_millis(300);
/// Scheduling allowance on a measured queue wait, well below the gap
/// between [`QUEUE`] and the default queue.
const QUEUE_SLACK: Duration = Duration::from_secs(1);
const SLOW_READ: Duration = Duration::from_secs(3);

fn limiter(app: &TestApp) -> QueryLimiter {
    dolos_core::Domain::query_limiter(&app.facade().inner).clone()
}

/// Every heavy-scan route, the two account scans of the stakes-only ruling
/// included.
fn heavy_paths(app: &TestApp) -> Vec<String> {
    let v = app.vectors();
    let epoch = app.tip_epoch();

    vec![
        format!("/addresses/{}/utxos", v.address),
        format!("/addresses/{}/utxos/{}", v.address, v.asset_unit),
        format!("/accounts/{}/utxos", v.stake_address),
        format!("/scripts/{}/utxos", v.script_hash),
        format!("/assets/{}/addresses", v.asset_unit),
        "/pools/extended".to_string(),
        format!("/pools/{}", v.pool_id),
        format!("/pools/{}/delegators", v.pool_id),
        format!("/epochs/{epoch}/stakes/{}", v.pool_id),
        format!("/governance/dreps/{}/delegators", v.drep_id),
    ]
}

/// Routes outside the limit, the budgeted transaction scans and the epoch's
/// pool blocks among them.
fn light_paths(app: &TestApp) -> Vec<String> {
    let v = app.vectors();
    let epoch = app.tip_epoch();

    vec![
        "/health".to_string(),
        "/blocks/latest".to_string(),
        format!("/blocks/{}", v.block_hash),
        format!("/txs/{}", v.tx_hash),
        format!("/scripts/datum/{}/cbor", v.datum_hash),
        format!("/accounts/{}", v.stake_address),
        format!("/addresses/{}/transactions", v.address),
        format!("/assets/{}/transactions", v.asset_unit),
        format!("/epochs/{epoch}/blocks/{}", v.pool_id),
        format!("/epochs/{epoch}/stakes"),
        "/pools".to_string(),
    ]
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

async fn wait_until_busy(limiter: &QueryLimiter) {
    let started = Instant::now();

    while limiter.heavy_in_use() == 0 {
        assert!(started.elapsed() < SLOW_READ * 3, "no heavy scan started");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[derive(Default)]
struct Peaks {
    stop: AtomicBool,
    admitted: AtomicUsize,
    heavy_in_use: AtomicUsize,
    in_use: AtomicUsize,
}

/// Samples the limiter until stopped, keeping each counter's peak.
fn sample(limiter: QueryLimiter) -> (Arc<Peaks>, tokio::task::JoinHandle<()>) {
    let peaks = Arc::new(Peaks::default());

    let task = tokio::spawn({
        let peaks = peaks.clone();
        async move {
            while !peaks.stop.load(Ordering::SeqCst) {
                peaks
                    .admitted
                    .fetch_max(limiter.heavy_admitted(), Ordering::SeqCst);
                peaks
                    .heavy_in_use
                    .fetch_max(limiter.heavy_in_use(), Ordering::SeqCst);
                peaks.in_use.fetch_max(limiter.in_use(), Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    });

    (peaks, task)
}

#[tokio::test]
async fn heavy_routes_wait_out_the_queue_then_get_503() {
    let app = TestApp::new_admitting(None, QueryLimiter::with_heavy_scans(32, 1, QUEUE), 120);
    let held = limiter(&app).admit_heavy().await.expect("a free slot");

    for path in heavy_paths(&app) {
        let started = Instant::now();
        let (status, headers, body) = app.get_with_headers(&path).await;

        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
        let waited = started.elapsed();
        assert!(waited >= QUEUE, "{path} skipped the queue");
        assert!(waited < QUEUE + QUEUE_SLACK, "{path} waited {waited:?}");
        assert_eq!(headers[header::RETRY_AFTER], "1", "{path}");

        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            body,
            json!({
                "status_code": 503,
                "error": "Service Unavailable",
                "message": "The node is serving its limit of heavy requests. Retry later.",
            }),
            "{path}"
        );
    }

    for path in light_paths(&app) {
        let (status, _) = app.get_bytes(&path).await;
        assert_ne!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
    }

    drop(held);

    for path in heavy_paths(&app) {
        let (status, _) = app.get_bytes(&path).await;
        assert_ne!(status, StatusCode::SERVICE_UNAVAILABLE, "{path}");
    }
}

#[tokio::test]
async fn the_limit_admits_its_configured_count() {
    for heavy in [1, 3] {
        let app = TestApp::new_admitting(
            None,
            QueryLimiter::with_heavy_scans(32, heavy, Duration::ZERO),
            120,
        );
        let limiter = limiter(&app);
        let path = format!("/addresses/{}/utxos", app.vectors().address);

        let mut held = Vec::new();
        for _ in 1..heavy {
            held.push(limiter.admit_heavy().await.expect("a free slot"));
        }

        assert_eq!(app.get_bytes(&path).await.0, StatusCode::OK, "{heavy}");

        held.push(limiter.admit_heavy().await.expect("a free slot"));
        assert_eq!(
            app.get_bytes(&path).await.0,
            StatusCode::SERVICE_UNAVAILABLE,
            "{heavy}"
        );
    }
}

#[tokio::test]
async fn a_queued_heavy_request_proceeds_when_a_slot_frees() {
    let queue = Duration::from_secs(5);
    let app = TestApp::new_admitting(None, QueryLimiter::with_heavy_scans(32, 1, queue), 120);
    let held = limiter(&app).admit_heavy().await.expect("a free slot");
    let path = format!("/accounts/{}/utxos", app.vectors().stake_address);

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

#[tokio::test]
async fn the_route_prefix_keeps_the_limit() {
    let app = TestApp::new_with_route_prefix("/api/v0");
    let limiter = limiter(&app);
    let mut held = Vec::new();
    while let Some(slot) = tokio::time::timeout(Duration::from_millis(10), limiter.admit_heavy())
        .await
        .ok()
        .flatten()
    {
        held.push(slot);
    }
    assert_eq!(held.len(), QueryLimiter::DEFAULT_HEAVY_LIMIT);

    let path = format!("/api/v0/addresses/{}/utxos", app.vectors().address);
    let started = Instant::now();
    assert_eq!(
        app.get_bytes(&path).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert!(started.elapsed() >= QueryLimiter::DEFAULT_HEAVY_QUEUE);
}

/// The deadline wraps admission: a request waiting for a slot is answered
/// 504 at the deadline, not 503 at the end of a longer queue.
#[tokio::test]
async fn the_deadline_counts_the_wait_for_a_slot() {
    let queue = Duration::from_secs(10);
    let app = TestApp::new_admitting(None, QueryLimiter::with_heavy_scans(32, 1, queue), 1);
    let _held = limiter(&app).admit_heavy().await.expect("a free slot");
    let path = format!("/assets/{}/addresses", app.vectors().asset_unit);

    let started = Instant::now();
    let (status, body) = app.get_bytes(&path).await;

    assert_eq!(status, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < Duration::from_secs(3));

    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        body,
        json!({
            "status_code": 504,
            "error": "Gateway Timeout",
            "message": "The request took longer than this node's deadline.",
        })
    );
}

/// A request cut by its deadline leaves its store read running; the slot
/// stays taken until that read ends, so no replacement enters beside it,
/// while light routes keep answering.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_past_its_deadline_keeps_its_slot_until_its_read_ends() {
    let app = TestApp::new_admitting(
        Some(TestFault::SlowUtxoReads(SLOW_READ)),
        QueryLimiter::with_heavy_scans(32, 1, Duration::ZERO),
        1,
    );
    let limiter = limiter(&app);
    let (peaks, sampler) = sample(limiter.clone());
    let path = format!("/addresses/{}/utxos", app.vectors().address);
    let replacement = format!("/scripts/{}/utxos", app.vectors().script_hash);

    let started = Instant::now();
    assert_eq!(app.get_bytes(&path).await.0, StatusCode::GATEWAY_TIMEOUT);
    assert!(started.elapsed() < SLOW_READ);

    assert_eq!(limiter.heavy_admitted(), 1);
    assert_eq!(limiter.heavy_in_use(), 1);
    assert_eq!(
        app.get_bytes(&replacement).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );
    assert_eq!(app.get_bytes("/blocks/latest").await.0, StatusCode::OK);

    wait_until_free(&limiter).await;
    assert!(started.elapsed() >= SLOW_READ);

    peaks.stop.store(true, Ordering::SeqCst);
    sampler.await.unwrap();
    assert_eq!(peaks.admitted.load(Ordering::SeqCst), 1);
    assert_eq!(limiter.heavy_in_use(), 0);
}

/// A client that goes away mid-scan frees nothing until the read it started
/// ends; then a replacement enters and completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_client_keeps_its_slot_until_its_read_ends() {
    let app = Arc::new(TestApp::new_admitting(
        Some(TestFault::SlowUtxoReads(SLOW_READ)),
        QueryLimiter::with_heavy_scans(32, 1, Duration::ZERO),
        120,
    ));
    let limiter = limiter(&app);
    let path = format!("/addresses/{}/utxos", app.vectors().address);

    let started = Instant::now();
    let client = tokio::spawn({
        let (app, path) = (app.clone(), path.clone());
        async move { app.get_bytes(&path).await }
    });

    wait_until_busy(&limiter).await;
    client.abort();
    assert!(client.await.unwrap_err().is_cancelled());

    assert_eq!(limiter.heavy_admitted(), 1);
    assert_eq!(
        app.get_bytes(&path).await.0,
        StatusCode::SERVICE_UNAVAILABLE
    );

    wait_until_free(&limiter).await;
    assert!(started.elapsed() >= SLOW_READ);
    assert_eq!(app.get_bytes(&path).await.0, StatusCode::OK);
}

/// Heavy requests, more than the limit, hold at most the heavy share of
/// query permits while light requests complete beside them, at the default
/// and at other query and heavy limits.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heavy_requests_leave_query_permits_to_light_routes() {
    for (cap, heavy) in [(32, 8), (4, 4), (8, 2), (64, 16)] {
        let app = Arc::new(TestApp::new_admitting(
            Some(TestFault::SlowUtxoReads(Duration::from_millis(300))),
            QueryLimiter::with_heavy_scans(cap, heavy, Duration::from_secs(30)),
            120,
        ));
        let limiter = limiter(&app);
        let (peaks, sampler) = sample(limiter.clone());

        let paths = [
            format!("/addresses/{}/utxos", app.vectors().address),
            format!("/accounts/{}/utxos", app.vectors().stake_address),
            format!("/assets/{}/addresses", app.vectors().asset_unit),
        ];

        let requests: Vec<_> = (0..heavy * 2)
            .map(|i| {
                let app = app.clone();
                let path = paths[i % paths.len()].clone();
                tokio::spawn(async move { app.get_bytes(&path).await.0 })
            })
            .collect();

        wait_until_busy(&limiter).await;

        for _ in 0..3 {
            let started = Instant::now();
            assert_eq!(app.get_bytes("/blocks/latest").await.0, StatusCode::OK);
            assert!(
                started.elapsed() < Duration::from_millis(250),
                "cap {cap}, heavy {heavy}: a light request took {:?}",
                started.elapsed()
            );
        }

        for request in requests {
            assert_eq!(request.await.unwrap(), StatusCode::OK, "cap {cap}");
        }

        peaks.stop.store(true, Ordering::SeqCst);
        sampler.await.unwrap();

        let share = limiter.heavy_share();
        let heavy_peak = peaks.heavy_in_use.load(Ordering::SeqCst);
        assert!(
            (1..=share).contains(&heavy_peak),
            "cap {cap}, heavy {heavy}: {heavy_peak} heavy permits, share {share}"
        );
        assert!(peaks.admitted.load(Ordering::SeqCst) <= heavy);
        assert!(peaks.in_use.load(Ordering::SeqCst) <= cap);
        assert_eq!(limiter.heavy_admitted(), 0);
    }
}
