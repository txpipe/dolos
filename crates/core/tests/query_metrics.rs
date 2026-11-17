//! The query limiter's waits, admissions and blocking closures, as exported.

use std::{
    sync::{mpsc, Arc, Mutex},
    time::{Duration, Instant},
};

use dolos_core::{telemetry::DURATION_BOUNDARIES, QueryLimiter};
use dolos_testing::metrics::{MetricsCapture, Snapshot};

const WAIT: &str = "dolos.query.limiter.wait.duration";
const ADMISSION: &str = "dolos.query.heavy.admission.duration";
const IN_FLIGHT: &str = "dolos.query.blocking.in_flight";
const OUTLIVED: &str = "dolos.query.blocking.outlived";
const DURATION: &str = "dolos.query.blocking.duration";

const HOLD: Duration = Duration::from_millis(300);

fn count(snapshot: &Snapshot, name: &str) -> f64 {
    snapshot.value(name, &[]).unwrap_or(0.0)
}

async fn wait_for(what: &str, mut done: impl FnMut() -> bool) {
    let started = Instant::now();

    while !done() {
        assert!(started.elapsed() < Duration::from_secs(10), "{what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closure_waiting_for_a_permit_records_at_least_its_wait() {
    let capture = MetricsCapture::new();
    let limiter = QueryLimiter::new(1).with_meter(&capture.meter());
    let first_ended = Arc::new(Mutex::new(None));

    let first = tokio::spawn({
        let (limiter, first_ended) = (limiter.clone(), first_ended.clone());
        async move {
            limiter
                .run_blocking(move || {
                    std::thread::sleep(HOLD);
                    *first_ended.lock().unwrap() = Some(Instant::now());
                })
                .await
        }
    });

    wait_for("the first closure took the permit", || {
        limiter.in_use() == 1
    })
    .await;

    let called = Instant::now();
    limiter.run_blocking(|| ()).await.unwrap();
    first.await.unwrap().unwrap();

    // The permit frees after the first closure stamps its end.
    let waited = first_ended.lock().unwrap().unwrap() - called;

    let snapshot = capture.collect();
    let wait = snapshot.histogram(WAIT, &[]).unwrap();
    assert_eq!(wait.count, 2);
    assert!(
        wait.max.unwrap() >= waited.as_secs_f64(),
        "{wait:?}, {waited:?}"
    );
    assert_eq!(wait.bounds, DURATION_BOUNDARIES);

    let duration = snapshot.histogram(DURATION, &[]).unwrap();
    assert_eq!(duration.count, 2);
    assert!(duration.max.unwrap() >= HOLD.as_secs_f64(), "{duration:?}");

    assert_eq!(count(&snapshot, IN_FLIGHT), 0.0);
    assert_eq!(count(&snapshot, OUTLIVED), 0.0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_closure_whose_caller_is_dropped_outlives_it_until_it_ends() {
    let capture = MetricsCapture::new();
    let limiter = QueryLimiter::new(4).with_meter(&capture.meter());
    let (release, released) = mpsc::channel::<()>();

    let caller = tokio::spawn({
        let limiter = limiter.clone();
        async move { limiter.run_blocking(move || released.recv()).await }
    });

    wait_for("the closure started", || limiter.in_use() == 1).await;

    let snapshot = capture.collect();
    assert_eq!(count(&snapshot, IN_FLIGHT), 1.0);
    assert_eq!(count(&snapshot, OUTLIVED), 0.0);

    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());

    let snapshot = capture.collect();
    assert_eq!(count(&snapshot, IN_FLIGHT), 1.0);
    assert_eq!(count(&snapshot, OUTLIVED), 1.0);
    assert!(snapshot.histogram(DURATION, &[]).is_none());

    release.send(()).unwrap();

    wait_for("the closure ended", || {
        count(&capture.collect(), IN_FLIGHT) == 0.0
    })
    .await;

    let snapshot = capture.collect();
    assert_eq!(count(&snapshot, OUTLIVED), 0.0);
    assert_eq!(snapshot.histogram(DURATION, &[]).unwrap().count, 1);
    assert_eq!(limiter.in_use(), 0);
}

#[tokio::test]
async fn a_panicking_closure_still_ends() {
    let capture = MetricsCapture::new();
    let limiter = QueryLimiter::new(1).with_meter(&capture.meter());

    let outcome = limiter.run_blocking(|| panic!("a failed read")).await;
    assert!(outcome.is_err());

    let snapshot = capture.collect();
    assert_eq!(count(&snapshot, IN_FLIGHT), 0.0);
    assert_eq!(count(&snapshot, OUTLIVED), 0.0);
    assert_eq!(snapshot.histogram(DURATION, &[]).unwrap().count, 1);
    assert_eq!(limiter.in_use(), 0);
}

#[tokio::test]
async fn admission_records_each_outcome() {
    let capture = MetricsCapture::new();
    let limiter = QueryLimiter::with_heavy_scans(4, 1, Duration::from_millis(100))
        .with_meter(&capture.meter());

    let held = limiter.admit_heavy().await.expect("a free slot");
    assert!(limiter.admit_heavy().await.is_none());
    drop(held);

    let snapshot = capture.collect();
    let admitted = snapshot
        .histogram(ADMISSION, &[("outcome", "admitted")])
        .unwrap();
    let rejected = snapshot
        .histogram(ADMISSION, &[("outcome", "rejected")])
        .unwrap();

    assert_eq!(admitted.count, 1);
    assert_eq!(rejected.count, 1);
    assert!(rejected.min.unwrap() >= 0.1, "{rejected:?}");
}
