//! Mempool instruments: how many transactions wait, what became of each one,
//! how long it took to reach a block and how long its submission took.
//!
//! The size gauge reads the store when the meter collects it. The lifecycle
//! counter and the confirmation histogram come from one [`Lifecycle`]
//! subscriber over the store's event stream, which keeps its timings in
//! memory only: a transaction received before a restart is not timed.

use std::{
    collections::HashMap,
    sync::{Arc, Weak},
    time::{Duration, Instant},
};

use futures_util::StreamExt as _;
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, Meter, ObservableGauge},
    KeyValue,
};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tracing::warn;

use super::{DURATION_BOUNDARIES, METER, OUTCOME, STATE};
use crate::{
    ChainError, DomainError, MempoolCount, MempoolError, MempoolEvent, MempoolStore,
    MempoolTxStage, TxHash,
};

/// Bucket boundaries, in seconds, of `dolos.mempool.confirmation.duration`.
pub const CONFIRMATION_BOUNDARIES: [f64; 10] = [
    5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];

/// The `state` value of a lifecycle stage.
pub fn stage_name(stage: &MempoolTxStage) -> &'static str {
    match stage {
        MempoolTxStage::Pending => "pending",
        MempoolTxStage::Propagated => "propagated",
        MempoolTxStage::Acknowledged => "acknowledged",
        MempoolTxStage::Confirmed => "confirmed",
        MempoolTxStage::Finalized => "finalized",
        MempoolTxStage::Dropped => "dropped",
        MempoolTxStage::RolledBack => "rolled_back",
        MempoolTxStage::Unknown => "unknown",
    }
}

type ReadCount = dyn Fn() -> MempoolCount + Send + Sync;

/// The `dolos.mempool.txs` gauge, by `state`: `pending` or `inflight`.
///
/// The gauge holds the store weakly, so it reports nothing once this value
/// is dropped: keep it as long as the mempool is open.
pub struct MempoolGauge {
    _read: Arc<ReadCount>,
    _txs: ObservableGauge<u64>,
}

impl MempoolGauge {
    /// Registers the gauge on this crate's global meter over `store`.
    pub fn register<M: MempoolStore>(store: M) -> Self {
        Self::with_meter(store, &global::meter(METER))
    }

    /// The same gauge on `meter`.
    pub fn with_meter<M: MempoolStore>(store: M, meter: &Meter) -> Self {
        let read: Arc<ReadCount> = Arc::new(move || store.count());
        let weak: Weak<ReadCount> = Arc::downgrade(&read);

        let txs = meter
            .u64_observable_gauge("dolos.mempool.txs")
            .with_unit("{tx}")
            .with_callback(move |observer| {
                if let Some(count) = weak.upgrade().map(|read| read()) {
                    observer.observe(count.pending as u64, &[KeyValue::new(STATE, "pending")]);
                    observer.observe(count.inflight as u64, &[KeyValue::new(STATE, "inflight")]);
                }
            })
            .build();

        Self {
            _read: read,
            _txs: txs,
        }
    }
}

/// When the subscriber first saw a transaction pending.
struct Timing {
    since: Instant,
    timed: bool,
}

/// The subscriber behind `dolos.mempool.transitions` and
/// `dolos.mempool.confirmation.duration`.
///
/// A `confirmed` event with more than one confirmation only deepens a
/// confirmation already counted, so it is not a transition.
pub struct Lifecycle {
    transitions: Counter<u64>,
    confirmation: Histogram<f64>,
    pending: HashMap<TxHash, Timing>,
}

impl Lifecycle {
    /// The subscriber on this crate's global meter.
    pub fn new() -> Self {
        Self::with_meter(&global::meter(METER))
    }

    /// The subscriber on `meter`.
    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            transitions: meter
                .u64_counter("dolos.mempool.transitions")
                .with_unit("{tx}")
                .build(),
            confirmation: meter
                .f64_histogram("dolos.mempool.confirmation.duration")
                .with_unit("s")
                .with_boundaries(CONFIRMATION_BOUNDARIES.to_vec())
                .build(),
            pending: HashMap::new(),
        }
    }

    /// Counts the transition `event` reports and times a first confirmation.
    pub fn observe(&mut self, event: &MempoolEvent) {
        let tx = &event.tx;

        if tx.stage == MempoolTxStage::Confirmed && tx.confirmations > 1 {
            return;
        }

        self.transitions
            .add(1, &[KeyValue::new(STATE, stage_name(&tx.stage))]);

        match tx.stage {
            MempoolTxStage::Pending => {
                self.pending.entry(tx.hash).or_insert(Timing {
                    since: Instant::now(),
                    timed: false,
                });
            }
            MempoolTxStage::Confirmed => {
                if let Some(timing) = self.pending.get_mut(&tx.hash) {
                    if !timing.timed {
                        timing.timed = true;
                        let took = timing.since.elapsed().as_secs_f64();
                        self.confirmation.record(took, &[]);
                    }
                }
            }
            MempoolTxStage::Finalized | MempoolTxStage::Dropped => {
                self.pending.remove(&tx.hash);
            }
            _ => (),
        }
    }

    /// Counts `missed` events the stream skipped as `unknown`.
    pub fn missed(&mut self, missed: u64) {
        self.transitions.add(
            missed,
            &[KeyValue::new(STATE, stage_name(&MempoolTxStage::Unknown))],
        );
    }

    /// How many transactions the subscriber remembers.
    pub fn remembered(&self) -> usize {
        self.pending.len()
    }

    /// Consumes `stream` until it ends.
    pub async fn run<S>(&mut self, mut stream: S)
    where
        S: futures_core::Stream<Item = Result<MempoolEvent, MempoolError>> + Unpin,
    {
        while let Some(item) = stream.next().await {
            match item {
                Ok(event) => self.observe(&event),
                Err(error) => match missed_events(&error) {
                    Some(missed) => self.missed(missed),
                    None => warn!(%error, "mempool event stream failed"),
                },
            }
        }
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

/// The number of events a lagging broadcast stream reports it skipped.
fn missed_events(error: &MempoolError) -> Option<u64> {
    let MempoolError::Internal(inner) = error else {
        return None;
    };

    match inner.downcast_ref::<BroadcastStreamRecvError>()? {
        BroadcastStreamRecvError::Lagged(missed) => Some(*missed),
    }
}

/// The `outcome` of a submission: `accepted`, `invalid` (phase 1),
/// `script_failed` (phase 2), `duplicate` or `error`.
pub fn submit_outcome<T>(result: &Result<T, DomainError>) -> &'static str {
    match result {
        Ok(_) => "accepted",
        Err(DomainError::ChainError(ChainError::Phase1ValidationRejected(_))) => "invalid",
        Err(DomainError::ChainError(ChainError::Phase2ValidationRejected(_))) => "script_failed",
        Err(DomainError::MempoolError(MempoolError::DuplicateTx)) => "duplicate",
        Err(_) => "error",
    }
}

/// The `dolos.submit.duration` histogram.
pub struct SubmitMetrics {
    duration: Histogram<f64>,
}

impl SubmitMetrics {
    /// The histogram on this crate's global meter. Submissions are rare
    /// enough to look it up for each one, which keeps it bound to the
    /// provider installed when it records.
    pub fn new() -> Self {
        Self::with_meter(&global::meter(METER))
    }

    /// The histogram on `meter`.
    pub fn with_meter(meter: &Meter) -> Self {
        Self {
            duration: meter
                .f64_histogram("dolos.submit.duration")
                .with_unit("s")
                .with_boundaries(DURATION_BOUNDARIES.to_vec())
                .build(),
        }
    }

    /// Records a submission that took `took` and ended in `result`.
    pub fn record<T>(&self, took: Duration, result: &Result<T, DomainError>) {
        self.duration.record(
            took.as_secs_f64(),
            &[KeyValue::new(OUTCOME, submit_outcome(result))],
        );
    }
}

impl Default for SubmitMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use opentelemetry::metrics::MeterProvider as _;
    use opentelemetry_sdk::metrics::{
        data::{AggregatedMetrics, MetricData},
        InMemoryMetricExporter, SdkMeterProvider,
    };
    use pallas::ledger::validate::utils::ValidationError;

    use super::*;

    fn provider() -> (SdkMeterProvider, InMemoryMetricExporter) {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        (provider, exporter)
    }

    /// `(outcome, count)` of every `dolos.submit.duration` point, sorted.
    fn submissions(
        provider: &SdkMeterProvider,
        exporter: &InMemoryMetricExporter,
    ) -> Vec<(String, u64)> {
        exporter.reset();
        provider.force_flush().unwrap();

        let mut points = Vec::new();

        for resource in exporter.get_finished_metrics().unwrap() {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    assert_eq!(metric.name(), "dolos.submit.duration");
                    assert_eq!(metric.unit(), "s");

                    let AggregatedMetrics::F64(MetricData::Histogram(histogram)) = metric.data()
                    else {
                        panic!("{} is not a histogram", metric.name());
                    };

                    for point in histogram.data_points() {
                        assert_eq!(point.bounds().collect::<Vec<_>>(), DURATION_BOUNDARIES);
                        let outcome = point
                            .attributes()
                            .find(|kv| kv.key.as_str() == OUTCOME)
                            .map(|kv| kv.value.to_string())
                            .unwrap();
                        points.push((outcome, point.count()));
                    }
                }
            }
        }

        points.sort();
        points
    }

    fn phase1() -> DomainError {
        ChainError::Phase1ValidationRejected(ValidationError::TxAndProtParamsDiffer).into()
    }

    fn phase2() -> DomainError {
        ChainError::Phase2ValidationRejected(vec!["no".into()]).into()
    }

    #[test]
    fn each_submission_result_maps_to_its_outcome() {
        let cases: [(Result<(), DomainError>, &str); 6] = [
            (Ok(()), "accepted"),
            (Err(phase1()), "invalid"),
            (Err(phase2()), "script_failed"),
            (Err(MempoolError::DuplicateTx.into()), "duplicate"),
            (
                Err(ChainError::Phase2EvaluationError("cost model".into()).into()),
                "error",
            ),
            (Err(MempoolError::PParamsNotAvailable.into()), "error"),
        ];

        for (result, outcome) in cases {
            assert_eq!(submit_outcome(&result), outcome, "{result:?}");
        }
    }

    #[test]
    fn a_submission_records_its_duration_under_its_outcome() {
        let (provider, exporter) = provider();
        let metrics = SubmitMetrics::with_meter(&provider.meter("test"));

        metrics.record(Duration::from_millis(3), &Ok::<_, DomainError>(()));
        metrics.record(Duration::from_millis(3), &Ok::<_, DomainError>(()));
        metrics.record::<()>(Duration::from_millis(3), &Err(phase1()));
        metrics.record::<()>(Duration::from_millis(3), &Err(phase2()));
        metrics.record::<()>(
            Duration::from_millis(3),
            &Err(MempoolError::DuplicateTx.into()),
        );

        assert_eq!(
            submissions(&provider, &exporter),
            [
                ("accepted".to_string(), 2),
                ("duplicate".to_string(), 1),
                ("invalid".to_string(), 1),
                ("script_failed".to_string(), 1),
            ]
        );
    }

    #[test]
    fn a_lagged_stream_reports_how_many_events_it_missed() {
        let lagged = MempoolError::Internal(Box::new(BroadcastStreamRecvError::Lagged(7)));
        let other = MempoolError::Internal("broken".into());

        assert_eq!(missed_events(&lagged), Some(7));
        assert_eq!(missed_events(&other), None);
        assert_eq!(missed_events(&MempoolError::DuplicateTx), None);
    }

    #[test]
    fn every_stage_has_a_lower_snake_case_name() {
        let stages = [
            MempoolTxStage::Pending,
            MempoolTxStage::Propagated,
            MempoolTxStage::Acknowledged,
            MempoolTxStage::Confirmed,
            MempoolTxStage::Finalized,
            MempoolTxStage::Dropped,
            MempoolTxStage::RolledBack,
            MempoolTxStage::Unknown,
        ];

        assert_eq!(
            stages.iter().map(stage_name).collect::<Vec<_>>(),
            [
                "pending",
                "propagated",
                "acknowledged",
                "confirmed",
                "finalized",
                "dropped",
                "rolled_back",
                "unknown"
            ]
        );
    }
}
