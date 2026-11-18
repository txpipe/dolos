//! The storage series of the `dolos` and `dolos-core` meters, collected
//! through an in-memory exporter.
//!
//! The instruments bind to the global meter provider when they are first
//! used, so this binary holds one test: the provider is installed before any
//! domain.

mod node;

use std::collections::BTreeMap;

use dolos_cardano::CardanoDelta;
use dolos_core::{
    telemetry, ArchiveStore as _, ChainPoint, Domain, LogEntry, LogValue, SyncExt as _,
    WalStore as _,
};
use dolos_testing::{
    faults::{FaultyToyDomain, TestFault},
    toy_domain::ToyDomain,
};
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporter, SdkMeterProvider,
};

/// Every point of the two meters as `name{attributes}`: a gauge's value, or
/// a histogram's sample count.
fn collect(
    provider: &SdkMeterProvider,
    exporter: &InMemoryMetricExporter,
) -> BTreeMap<String, u64> {
    exporter.reset();
    provider.force_flush().unwrap();

    let mut points = BTreeMap::new();

    for resource in exporter.get_finished_metrics().unwrap() {
        for scope in resource.scope_metrics() {
            if ![dolos::adapters::metrics::METER, telemetry::METER].contains(&scope.scope().name())
            {
                continue;
            }

            for metric in scope.metrics() {
                let series = |attributes: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
                    let mut attributes: Vec<_> = attributes
                        .map(|kv| format!("{}={}", kv.key, kv.value))
                        .collect();
                    attributes.sort();
                    format!("{}{{{}}}", metric.name(), attributes.join(","))
                };

                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            points.insert(series(&mut point.attributes()), point.value());
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            points.insert(series(&mut point.attributes()), point.count());
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    println!("{points:#?}");
    points
}

fn commits(points: &BTreeMap<String, u64>, store: &str) -> u64 {
    let series = format!("dolos.storage.commit.duration{{store={store}}}");
    points.get(&series).copied().unwrap_or_default()
}

fn housekeeping(points: &BTreeMap<String, u64>, outcome: &str) -> u64 {
    let series = format!("dolos.storage.housekeeping.duration{{outcome={outcome}}}");
    points.get(&series).copied().unwrap_or_default()
}

/// A WAL entry at `slot` with a nonzero hash.
fn wal_entry(slot: u64) -> LogEntry<CardanoDelta> {
    let mut hash = [1u8; 32];
    hash[0..8].copy_from_slice(&slot.to_be_bytes());
    (ChainPoint::Specific(slot, hash.into()), LogValue::origin())
}

#[test]
fn the_domain_reports_its_disk_range_commit_and_housekeeping_series() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    global::set_meter_provider(provider.clone());

    let (node, genesis, blocks) = node::fixture("");
    let domain = node::start(&node, &genesis);

    // An empty archive reports no range.
    let points = collect(&provider, &exporter);
    assert!(!points.contains_key("dolos.storage.archive.start.slot{}"));
    assert!(!points.contains_key("dolos.storage.archive.tip.slot{}"));

    // The first block also brings the genesis work.
    domain.roll_forward(blocks[0].clone()).unwrap();
    let before = collect(&provider, &exporter);

    // One block is one work unit, committing once to each store.
    domain.roll_forward(blocks[1].clone()).unwrap();
    let after = collect(&provider, &exporter);

    for store in ["wal", "state", "archive"] {
        assert_eq!(
            commits(&after, store),
            commits(&before, store) + 1,
            "{store} commits"
        );
    }

    domain.roll_forward_batch(blocks[2..].to_vec()).unwrap();
    let points = collect(&provider, &exporter);

    for store in ["blocks", "wal"] {
        let usage = points[&format!("dolos.storage.disk.usage{{store={store}}}")];
        assert!(usage > 0, "{store} disk usage");
    }

    let wal_start = domain.wal().find_start().unwrap().unwrap().0.slot();
    let wal_tip = domain.wal().find_tip().unwrap().unwrap().0.slot();
    let archived: Vec<_> = domain
        .archive()
        .get_range(None, None)
        .unwrap()
        .map(|(slot, _)| slot)
        .collect();

    assert_eq!(points["dolos.storage.wal.start.slot{}"], wal_start);
    assert_eq!(points["dolos.storage.wal.tip.slot{}"], wal_tip);
    assert_eq!(
        points["dolos.storage.archive.start.slot{}"],
        *archived.first().unwrap()
    );
    assert_eq!(
        points["dolos.storage.archive.tip.slot{}"],
        *archived.last().unwrap()
    );

    domain.shutdown().unwrap();
    drop(domain);

    // A WAL backlog beyond one bounded round: `pending`, then `done`.
    let toy = ToyDomain::new(None, None);
    let tip = 14_999 + dolos_cardano::mutable_slots(&toy.genesis());
    let entries: Vec<_> = (1..15_000).chain([tip]).map(wal_entry).collect();
    toy.wal().append_entries(&entries).unwrap();

    let before = collect(&provider, &exporter);
    assert!(!toy.housekeeping().unwrap());
    let after = collect(&provider, &exporter);
    assert_eq!(
        housekeeping(&after, "pending"),
        housekeeping(&before, "pending") + 1
    );
    assert_eq!(housekeeping(&after, "done"), housekeeping(&before, "done"));

    let mut rounds = 1;
    while !toy.housekeeping().unwrap() {
        rounds += 1;
        assert!(rounds < 100, "housekeeping did not converge");
    }
    let done = collect(&provider, &exporter);
    assert_eq!(
        housekeeping(&done, "done"),
        housekeeping(&after, "done") + 1
    );
    assert_eq!(
        housekeeping(&done, "pending"),
        housekeeping(&after, "pending") + rounds - 1
    );

    // A store error is `error`.
    let faulty = FaultyToyDomain::new(ToyDomain::new(None, None), TestFault::WalStoreError);
    assert!(faulty.housekeeping().is_err());
    let failed = collect(&provider, &exporter);
    assert_eq!(
        housekeeping(&failed, "error"),
        housekeeping(&done, "error") + 1
    );
}
