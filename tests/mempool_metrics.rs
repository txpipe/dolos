//! The mempool's size gauge, lifecycle counter and confirmation histogram,
//! collected through an in-memory exporter against both stores.

use dolos_core::builtin::EphemeralMempool;
use dolos_core::telemetry::mempool::{stage_name, Lifecycle, MempoolGauge};
use dolos_core::{ChainPoint, MempoolStore, MempoolTxStage, TxHash};
use dolos_redb3::mempool::RedbMempool;
use dolos_testing::mempool::make_test_mempool_tx;
use dolos_testing::{slot_to_chainpoint, tx_sequence_to_hash};
use futures_util::{FutureExt as _, StreamExt as _};
use opentelemetry::metrics::MeterProvider as _;
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporter, SdkMeterProvider,
};

const FINALIZE: u32 = 2;
const DROP: u32 = 2;

fn hash(n: u64) -> TxHash {
    tx_sequence_to_hash(n)
}

fn point(slot: u64) -> ChainPoint {
    slot_to_chainpoint(slot)
}

struct Meters {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl Meters {
    fn new() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        Self { provider, exporter }
    }

    /// Every exported point as `name[unit]{attributes} value`, sorted; a
    /// histogram's value is its sample count.
    fn collect(&self) -> Vec<String> {
        self.exporter.reset();
        self.provider.force_flush().unwrap();

        let mut points = Vec::new();

        for resource in self.exporter.get_finished_metrics().unwrap() {
            for scope in resource.scope_metrics() {
                for metric in scope.metrics() {
                    let mut push = |attributes: Vec<String>, value: String| {
                        let mut attributes = attributes;
                        attributes.sort();
                        points.push(format!(
                            "{}[{}]{{{}}} {value}",
                            metric.name(),
                            metric.unit(),
                            attributes.join(",")
                        ));
                    };
                    let labels = |kvs: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
                        kvs.map(|kv| format!("{}={}", kv.key, kv.value)).collect()
                    };

                    match metric.data() {
                        AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                            for point in histogram.data_points() {
                                push(labels(&mut point.attributes()), point.count().to_string());
                            }
                        }
                        AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                            for point in sum.data_points() {
                                push(labels(&mut point.attributes()), point.value().to_string());
                            }
                        }
                        AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                            for point in gauge.data_points() {
                                push(labels(&mut point.attributes()), point.value().to_string());
                            }
                        }
                        _ => panic!("unexpected instrument {}", metric.name()),
                    }
                }
            }
        }

        points.sort();
        points
    }

    /// The points of `name` only.
    fn of(&self, name: &str) -> Vec<String> {
        self.collect()
            .into_iter()
            .filter(|point| point.starts_with(&format!("{name}[")))
            .collect()
    }
}

/// A subscriber on `meters` over `store`'s events from now on.
struct Subscriber<S: MempoolStore> {
    lifecycle: Lifecycle,
    events: S::Stream,
    stages: Vec<&'static str>,
}

impl<S: MempoolStore> Subscriber<S> {
    fn new(store: &S, meters: &Meters) -> Self {
        Self {
            lifecycle: Lifecycle::with_meter(&meters.provider.meter("test")),
            events: store.subscribe(),
            stages: Vec::new(),
        }
    }

    /// Observes every event emitted so far, keeping their stages in order.
    fn drain(&mut self) {
        while let Some(Some(event)) = self.events.next().now_or_never() {
            let event = event.unwrap();
            self.stages.push(stage_name(&event.tx.stage));
            self.lifecycle.observe(&event);
        }
    }
}

fn transitions(meters: &Meters) -> Vec<String> {
    meters.of("dolos.mempool.transitions")
}

fn confirmations(meters: &Meters) -> Vec<String> {
    meters.of("dolos.mempool.confirmation.duration")
}

fn acknowledged<S: MempoolStore>(store: &S, n: u64) -> TxHash {
    store.receive(make_test_mempool_tx(hash(n))).unwrap();
    store.mark_inflight(&[hash(n)]).unwrap();
    store.mark_acknowledged(&[hash(n)]).unwrap();
    hash(n)
}

// ---------------------------------------------------------------------------
// Trait-generic assertions
// ---------------------------------------------------------------------------

fn assert_the_gauge_follows_the_store<S: MempoolStore>(store: S) {
    let meters = Meters::new();
    let _gauge = MempoolGauge::with_meter(store.clone(), &meters.provider.meter("test"));

    let txs = |pending: usize, inflight: usize| {
        vec![
            format!("dolos.mempool.txs[{{tx}}]{{state=inflight}} {inflight}"),
            format!("dolos.mempool.txs[{{tx}}]{{state=pending}} {pending}"),
        ]
    };

    assert_eq!(meters.of("dolos.mempool.txs"), txs(0, 0));

    store.receive(make_test_mempool_tx(hash(1))).unwrap();
    store.receive(make_test_mempool_tx(hash(2))).unwrap();
    assert_eq!(meters.of("dolos.mempool.txs"), txs(2, 0));

    store.mark_inflight(&[hash(1)]).unwrap();
    assert_eq!(meters.of("dolos.mempool.txs"), txs(1, 1));

    store.mark_acknowledged(&[hash(1)]).unwrap();
    store
        .confirm(&point(1), &[hash(1)], &[], FINALIZE, DROP)
        .unwrap();
    assert_eq!(meters.of("dolos.mempool.txs"), txs(1, 1));

    store
        .confirm(&point(2), &[hash(1)], &[], FINALIZE, DROP)
        .unwrap();
    assert_eq!(meters.of("dolos.mempool.txs"), txs(1, 0));
}

fn assert_a_finalized_tx_counts_each_stage_once<S: MempoolStore>(store: S) {
    let meters = Meters::new();
    let mut subscriber = Subscriber::new(&store, &meters);

    // Three, so the second confirmation only deepens the first.
    let finalize = 3;
    let tx = acknowledged(&store, 1);
    for slot in 1..=finalize as u64 {
        store
            .confirm(&point(slot), &[tx], &[], finalize, DROP)
            .unwrap();
    }
    subscriber.drain();

    assert_eq!(
        subscriber.stages,
        [
            "pending",
            "propagated",
            "acknowledged",
            "confirmed",
            "confirmed",
            "finalized"
        ]
    );
    assert_eq!(
        transitions(&meters),
        [
            "dolos.mempool.transitions[{tx}]{state=acknowledged} 1",
            "dolos.mempool.transitions[{tx}]{state=confirmed} 1",
            "dolos.mempool.transitions[{tx}]{state=finalized} 1",
            "dolos.mempool.transitions[{tx}]{state=pending} 1",
            "dolos.mempool.transitions[{tx}]{state=propagated} 1",
        ]
    );
    assert_eq!(
        confirmations(&meters),
        ["dolos.mempool.confirmation.duration[s]{} 1"]
    );
    assert_eq!(subscriber.lifecycle.remembered(), 0);
}

/// `rollback` is the stage the store reports a rolled-back tx at.
fn assert_a_reconfirmation_is_not_timed_again<S: MempoolStore>(store: S, rollback: &str) {
    let meters = Meters::new();
    let mut subscriber = Subscriber::new(&store, &meters);

    let tx = acknowledged(&store, 1);
    store.confirm(&point(1), &[tx], &[], 6, DROP).unwrap();
    store.confirm(&point(2), &[], &[tx], 6, DROP).unwrap();
    store.confirm(&point(2), &[tx], &[], 6, DROP).unwrap();
    subscriber.drain();

    let mut expected = vec![
        "dolos.mempool.transitions[{tx}]{state=acknowledged} 1".to_string(),
        "dolos.mempool.transitions[{tx}]{state=confirmed} 2".to_string(),
        "dolos.mempool.transitions[{tx}]{state=propagated} 1".to_string(),
    ];
    match rollback {
        "pending" => expected.push("dolos.mempool.transitions[{tx}]{state=pending} 2".into()),
        _ => expected.extend([
            "dolos.mempool.transitions[{tx}]{state=pending} 1".to_string(),
            format!("dolos.mempool.transitions[{{tx}}]{{state={rollback}}} 1"),
        ]),
    }
    expected.sort();

    assert_eq!(transitions(&meters), expected);
    assert_eq!(
        confirmations(&meters),
        ["dolos.mempool.confirmation.duration[s]{} 1"]
    );
    assert_eq!(subscriber.lifecycle.remembered(), 1);
}

fn assert_a_dropped_tx_is_forgotten<S: MempoolStore>(store: S) {
    let meters = Meters::new();
    let mut subscriber = Subscriber::new(&store, &meters);

    store.receive(make_test_mempool_tx(hash(1))).unwrap();
    store.mark_inflight(&[hash(1)]).unwrap();
    for slot in 1..=DROP as u64 {
        store
            .confirm(&point(slot), &[], &[], FINALIZE, DROP)
            .unwrap();
    }
    subscriber.drain();

    assert_eq!(
        transitions(&meters),
        [
            "dolos.mempool.transitions[{tx}]{state=dropped} 1",
            "dolos.mempool.transitions[{tx}]{state=pending} 1",
            "dolos.mempool.transitions[{tx}]{state=propagated} 1",
        ]
    );
    assert!(confirmations(&meters).is_empty());
    assert_eq!(subscriber.lifecycle.remembered(), 0);
}

/// More events than the store's channel holds, read only once the store is
/// gone: the subscriber counts what the stream skipped as `unknown`.
async fn assert_missed_events_count_as_unknown<S: MempoolStore>(store: S) {
    let meters = Meters::new();
    let mut lifecycle = Lifecycle::with_meter(&meters.provider.meter("test"));
    let events = store.subscribe();

    for n in 0..20 {
        store.receive(make_test_mempool_tx(hash(n))).unwrap();
    }
    drop(store);

    lifecycle.run(events).await;

    let counted = transitions(&meters);
    assert_eq!(counted.len(), 2, "{counted:?}");

    let value = |point: &str| point.rsplit(' ').next().unwrap().parse::<u64>().unwrap();
    assert!(counted[0].contains("state=pending"));
    assert!(counted[1].contains("state=unknown"));
    assert!(value(&counted[1]) > 0);
    assert_eq!(value(&counted[0]) + value(&counted[1]), 20);
}

// ---------------------------------------------------------------------------
// Ephemeral
// ---------------------------------------------------------------------------

#[test]
fn ephemeral_gauge_follows_the_store() {
    assert_the_gauge_follows_the_store(EphemeralMempool::new());
}

#[test]
fn ephemeral_finalized_tx_counts_each_stage_once() {
    assert_a_finalized_tx_counts_each_stage_once(EphemeralMempool::new());
}

#[test]
fn ephemeral_reconfirmation_is_not_timed_again() {
    assert_a_reconfirmation_is_not_timed_again(
        EphemeralMempool::new(),
        stage_name(&MempoolTxStage::RolledBack),
    );
}

#[test]
fn ephemeral_dropped_tx_is_forgotten() {
    assert_a_dropped_tx_is_forgotten(EphemeralMempool::new());
}

#[tokio::test]
async fn ephemeral_missed_events_count_as_unknown() {
    assert_missed_events_count_as_unknown(EphemeralMempool::new()).await;
}

// ---------------------------------------------------------------------------
// Redb
// ---------------------------------------------------------------------------

fn redb() -> RedbMempool {
    RedbMempool::in_memory().unwrap()
}

#[test]
fn redb_gauge_follows_the_store() {
    assert_the_gauge_follows_the_store(redb());
}

#[test]
fn redb_finalized_tx_counts_each_stage_once() {
    assert_a_finalized_tx_counts_each_stage_once(redb());
}

#[test]
fn redb_reconfirmation_is_not_timed_again() {
    assert_a_reconfirmation_is_not_timed_again(redb(), stage_name(&MempoolTxStage::Pending));
}

#[test]
fn redb_dropped_tx_is_forgotten() {
    assert_a_dropped_tx_is_forgotten(redb());
}

#[tokio::test]
async fn redb_missed_events_count_as_unknown() {
    assert_missed_events_count_as_unknown(redb()).await;
}
