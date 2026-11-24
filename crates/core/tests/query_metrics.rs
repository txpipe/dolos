//! The query limiter's waits, admissions and blocking closures, as exported.

use std::{
    sync::{mpsc, Arc, Mutex},
    time::{Duration, Instant},
};

use dolos_core::{
    telemetry::{queries::LimiterGauges, DURATION_BOUNDARIES},
    QueryLimiter,
};
use dolos_testing::metrics::{MetricsRecorder, Snapshot};

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
    let capture = MetricsRecorder::default();
    let limiter = QueryLimiter::new(1).with_meter(&capture.meter("test"));
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
    let capture = MetricsRecorder::default();
    let limiter = QueryLimiter::new(4).with_meter(&capture.meter("test"));
    let (release, released) = mpsc::channel::<()>();

    let caller = tokio::spawn({
        let limiter = limiter.clone();
        async move { limiter.run_blocking(move || released.recv()).await }
    });

    // The permit is taken a moment before the closure is counted.
    wait_for("the closure started", || {
        limiter.in_use() == 1 && count(&capture.collect(), IN_FLIGHT) == 1.0
    })
    .await;

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

    // The closure's end lowers the two counters one after the other.
    wait_for("the closure ended", || {
        let snapshot = capture.collect();
        count(&snapshot, IN_FLIGHT) == 0.0 && count(&snapshot, OUTLIVED) == 0.0
    })
    .await;

    let snapshot = capture.collect();
    assert_eq!(count(&snapshot, OUTLIVED), 0.0);
    assert_eq!(snapshot.histogram(DURATION, &[]).unwrap().count, 1);
    assert_eq!(limiter.in_use(), 0);
}

#[tokio::test]
async fn a_panicking_closure_still_ends() {
    let capture = MetricsRecorder::default();
    let limiter = QueryLimiter::new(1).with_meter(&capture.meter("test"));

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
    let capture = MetricsRecorder::default();
    let limiter = QueryLimiter::with_heavy_scans(4, 1, Duration::from_millis(100))
        .with_meter(&capture.meter("test"));

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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_occupancy_gauge_reads_its_own_count() {
    let capture = MetricsRecorder::default();
    let limiter =
        QueryLimiter::with_heavy_scans(8, 2, Duration::ZERO).with_meter(&capture.meter("test"));
    let _gauges = LimiterGauges::with_meter(&limiter, &capture.meter("test"));

    // Two slots taken, one heavy closure and two light ones running: the
    // in-use, admitted and share readings all differ.
    let _idle = limiter.admit_heavy().await.expect("a free slot");
    let busy = limiter.admit_heavy().await.expect("a second slot");

    let mut releases = Vec::new();
    let mut closures = Vec::new();

    let (release, released) = mpsc::channel::<()>();
    releases.push(release);
    closures.push(tokio::spawn({
        let limiter = limiter.clone();
        busy.scope(async move { limiter.run_blocking(move || released.recv()).await })
    }));

    for _ in 0..2 {
        let (release, released) = mpsc::channel::<()>();
        releases.push(release);
        closures.push(tokio::spawn({
            let limiter = limiter.clone();
            async move { limiter.run_blocking(move || released.recv()).await }
        }));
    }

    wait_for("three closures hold permits", || {
        limiter.in_use() == 3 && limiter.heavy_in_use() == 1
    })
    .await;

    let held = capture.collect();
    assert_eq!(count(&held, "dolos.query.limiter.capacity"), 8.0);
    assert_eq!(count(&held, "dolos.query.heavy.capacity"), 2.0);
    assert_eq!(count(&held, "dolos.query.heavy.share.capacity"), 4.0);
    assert_eq!(count(&held, "dolos.query.limiter.in_use"), 3.0);
    assert_eq!(count(&held, "dolos.query.heavy.admitted"), 2.0);
    assert_eq!(count(&held, "dolos.query.heavy.share.in_use"), 1.0);

    for release in releases {
        release.send(()).unwrap();
    }
    for closure in closures {
        closure.await.unwrap().unwrap().unwrap();
    }

    let free = capture.collect();
    assert_eq!(count(&free, "dolos.query.heavy.capacity"), 2.0);
    assert_eq!(count(&free, "dolos.query.limiter.in_use"), 0.0);
    assert_eq!(count(&free, "dolos.query.heavy.admitted"), 1.0);
    assert_eq!(count(&free, "dolos.query.heavy.share.in_use"), 0.0);
}
