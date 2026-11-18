//! Query limiter instruments: how full the limiter and heavy-scan admission
//! are, how long callers wait for them, and the blocking closures they run.
//!
//! Every limiter records the histograms and the closure counters, so those
//! series cover the whole process. The occupancy gauges read one limiter and
//! are registered once, for the domain's, with [`LimiterGauges::register`].

use std::{
    fmt,
    sync::{Arc, Mutex, PoisonError, Weak},
    time::Instant,
};

use opentelemetry::{
    global,
    metrics::{Histogram, Meter, ObservableGauge, UpDownCounter},
    KeyValue,
};

use super::{DURATION_BOUNDARIES, METER, OUTCOME};
use crate::QueryLimiter;

/// The instruments a [`QueryLimiter`] records as it runs.
pub(crate) struct QueryMetrics {
    limiter_wait: Histogram<f64>,
    heavy_admission: Histogram<f64>,
    blocking_in_flight: UpDownCounter<i64>,
    blocking_outlived: UpDownCounter<i64>,
    blocking_duration: Histogram<f64>,
    admitted: [KeyValue; 1],
    rejected: [KeyValue; 1],
}

impl fmt::Debug for QueryMetrics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryMetrics").finish_non_exhaustive()
    }
}

impl QueryMetrics {
    /// The instruments on this crate's global meter.
    pub(crate) fn global() -> Self {
        Self::with_meter(&global::meter(METER))
    }

    pub(crate) fn with_meter(meter: &Meter) -> Self {
        let duration = |name: &'static str| {
            meter
                .f64_histogram(name)
                .with_unit("s")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build()
        };
        let closures = |name: &'static str| {
            meter
                .i64_up_down_counter(name)
                .with_unit("{closure}")
                .build()
        };

        Self {
            limiter_wait: duration("dolos.query.limiter.wait.duration"),
            heavy_admission: duration("dolos.query.heavy.admission.duration"),
            blocking_in_flight: closures("dolos.query.blocking.in_flight"),
            blocking_outlived: closures("dolos.query.blocking.outlived"),
            blocking_duration: duration("dolos.query.blocking.duration"),
            admitted: [KeyValue::new(OUTCOME, "admitted")],
            rejected: [KeyValue::new(OUTCOME, "rejected")],
        }
    }

    /// A caller waited since `called` for the permits its closure holds.
    pub(crate) fn waited(&self, called: Instant) {
        self.limiter_wait
            .record(called.elapsed().as_secs_f64(), &[]);
    }

    /// A heavy scan asked for a slot at `called` and was `admitted` or not.
    pub(crate) fn admission(&self, called: Instant, admitted: bool) {
        let outcome = if admitted {
            &self.admitted
        } else {
            &self.rejected
        };

        self.heavy_admission
            .record(called.elapsed().as_secs_f64(), outcome);
    }

    /// A closure is about to be spawned: raises the in-flight count and
    /// returns the closure's half and the caller's half of its tracking.
    pub(crate) fn spawn(self: &Arc<Self>) -> (ClosureGuard, CallerGuard) {
        self.blocking_in_flight.add(1, &[]);

        let state = Arc::new(Mutex::new(ClosureState::default()));

        let closure = ClosureGuard {
            metrics: self.clone(),
            state: state.clone(),
            started: None,
        };
        let caller = CallerGuard {
            metrics: self.clone(),
            state,
        };

        (closure, caller)
    }
}

#[derive(Default)]
struct ClosureState {
    ended: bool,
    outlived: bool,
}

fn lock(state: &Mutex<ClosureState>) -> std::sync::MutexGuard<'_, ClosureState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Moves into a blocking closure; its drop is the closure's end, panics
/// included, and lowers what the closure raised.
pub(crate) struct ClosureGuard {
    metrics: Arc<QueryMetrics>,
    state: Arc<Mutex<ClosureState>>,
    started: Option<Instant>,
}

impl ClosureGuard {
    /// The closure starts running; its duration is recorded from here.
    pub(crate) fn start(mut self) -> Self {
        self.started = Some(Instant::now());
        self
    }
}

impl Drop for ClosureGuard {
    fn drop(&mut self) {
        if let Some(started) = self.started {
            let took = started.elapsed().as_secs_f64();
            self.metrics.blocking_duration.record(took, &[]);
        }

        // Under the lock, so the caller's raise always precedes this lower.
        let mut state = lock(&self.state);
        state.ended = true;

        self.metrics.blocking_in_flight.add(-1, &[]);

        if state.outlived {
            self.metrics.blocking_outlived.add(-1, &[]);
        }
    }
}

/// Held by the caller while it awaits the closure; dropped before the
/// closure ends, it counts the closure as outliving its caller.
pub(crate) struct CallerGuard {
    metrics: Arc<QueryMetrics>,
    state: Arc<Mutex<ClosureState>>,
}

impl Drop for CallerGuard {
    fn drop(&mut self) {
        let mut state = lock(&self.state);

        if !state.ended {
            state.outlived = true;
            self.metrics.blocking_outlived.add(1, &[]);
        }
    }
}

/// The occupancy gauges of one limiter: `dolos.query.limiter.capacity` and
/// `.in_use`, `dolos.query.heavy.capacity` and `.admitted`, and
/// `dolos.query.heavy.share.capacity` and `.in_use`.
///
/// The gauges hold the limiter weakly, so they report nothing once this value
/// is dropped: keep it for as long as the limiter serves.
pub struct LimiterGauges {
    _limiter: Arc<QueryLimiter>,
    _gauges: Vec<ObservableGauge<u64>>,
}

impl LimiterGauges {
    /// Registers the gauges of `limiter` on this crate's global meter.
    pub fn register(limiter: &QueryLimiter) -> Self {
        Self::with_meter(limiter, &global::meter(METER))
    }

    /// The same gauges on `meter`.
    pub fn with_meter(limiter: &QueryLimiter, meter: &Meter) -> Self {
        let limiter = Arc::new(limiter.clone());

        let gauge = |name: &'static str, unit: &'static str, read: fn(&QueryLimiter) -> usize| {
            let limiter: Weak<QueryLimiter> = Arc::downgrade(&limiter);

            meter
                .u64_observable_gauge(name)
                .with_unit(unit)
                .with_callback(move |observer| {
                    if let Some(limiter) = limiter.upgrade() {
                        observer.observe(read(&limiter) as u64, &[]);
                    }
                })
                .build()
        };

        let gauges = vec![
            gauge(
                "dolos.query.limiter.capacity",
                "{permit}",
                QueryLimiter::cap,
            ),
            gauge(
                "dolos.query.limiter.in_use",
                "{permit}",
                QueryLimiter::in_use,
            ),
            gauge(
                "dolos.query.heavy.capacity",
                "{slot}",
                QueryLimiter::heavy_limit,
            ),
            gauge(
                "dolos.query.heavy.admitted",
                "{slot}",
                QueryLimiter::heavy_admitted,
            ),
            gauge(
                "dolos.query.heavy.share.capacity",
                "{permit}",
                QueryLimiter::heavy_share,
            ),
            gauge(
                "dolos.query.heavy.share.in_use",
                "{permit}",
                QueryLimiter::heavy_in_use,
            ),
        ];

        Self {
            _limiter: limiter,
            _gauges: gauges,
        }
    }
}
