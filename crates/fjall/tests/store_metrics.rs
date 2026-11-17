//! The `dolos-fjall` meter's disk, compaction and flush series, collected
//! through an in-memory exporter.
//!
//! The instruments register on the global meter provider when a store opens,
//! so this binary holds one test: the provider is installed before any store.

use dolos_fjall::{metrics, Preset, StateStore};
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporter, SdkMeterProvider,
};

/// Every point of the meter's series but the L0 gauge, as
/// `name{attributes}`, with its value.
fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Vec<(String, f64)> {
    exporter.reset();
    provider.force_flush().unwrap();

    let mut points = Vec::new();

    for resource in exporter.get_finished_metrics().unwrap() {
        for scope in resource.scope_metrics() {
            if scope.scope().name() != metrics::METER {
                continue;
            }

            for metric in scope.metrics() {
                if metric.name() == metrics::L0_RUNS_MAX {
                    continue;
                }

                let mut push = |attributes: Vec<String>, value: f64| {
                    let mut attributes = attributes;
                    attributes.sort();
                    let series = format!("{}{{{}}}", metric.name(), attributes.join(","));
                    points.push((series, value));
                };

                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            let labels = point
                                .attributes()
                                .map(|kv| format!("{}={}", kv.key, kv.value))
                                .collect();
                            push(labels, point.value() as f64);
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        assert!(sum.is_monotonic(), "{} is not a counter", metric.name());

                        for point in sum.data_points() {
                            let labels = point
                                .attributes()
                                .map(|kv| format!("{}={}", kv.key, kv.value))
                                .collect();
                            push(labels, point.value() as f64);
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Sum(sum)) => {
                        assert!(sum.is_monotonic(), "{} is not a counter", metric.name());

                        for point in sum.data_points() {
                            let labels = point
                                .attributes()
                                .map(|kv| format!("{}={}", kv.key, kv.value))
                                .collect();
                            push(labels, point.value());
                        }
                    }
                    _ => panic!("unexpected instrument {}", metric.name()),
                }
            }
        }
    }

    points.sort_by(|a, b| a.0.cmp(&b.0));
    println!("{points:#?}");
    points
}

fn series(points: &[(String, f64)]) -> Vec<&str> {
    points.iter().map(|(series, _)| series.as_str()).collect()
}

fn value(points: &[(String, f64)], series: &str) -> f64 {
    points
        .iter()
        .find(|(name, _)| name == series)
        .unwrap_or_else(|| panic!("{series} was not exported"))
        .1
}

#[test]
fn a_writable_store_reports_its_disk_and_compaction_series_and_a_view_none() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    global::set_meter_provider(provider.clone());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("state");
    let tuning = Preset::Standard.state();

    let state = StateStore::open(&path, Preset::Standard).unwrap();

    // One table in `state-tags`, so its disk usage is above zero.
    let tags = state
        .database()
        .keyspace("state-tags", || tuning.tags.options())
        .unwrap();
    tags.insert("a", "value").unwrap();
    tags.rotate_memtable_and_wait().unwrap();
    drop(tags);

    let points = collect(&provider, &exporter);

    assert_eq!(
        series(&points),
        [
            "dolos.storage.compaction.time{store=state}",
            "dolos.storage.compactions.active{store=state}",
            "dolos.storage.compactions.completed{store=state}",
            "dolos.storage.disk.usage{keyspace=state-cursor,store=state}",
            "dolos.storage.disk.usage{keyspace=state-entities,store=state}",
            "dolos.storage.disk.usage{keyspace=state-tags,store=state}",
            "dolos.storage.disk.usage{keyspace=state-utxos,store=state}",
            "dolos.storage.flushes.outstanding{store=state}",
            "dolos.storage.write_buffer.size{store=state}",
        ]
    );

    let tags_bytes = value(
        &points,
        "dolos.storage.disk.usage{keyspace=state-tags,store=state}",
    );
    let expected = state
        .disk_usage()
        .into_iter()
        .find(|(name, _, _)| *name == "state-tags")
        .unwrap()
        .1;
    assert!(tags_bytes > 0.0);
    assert_eq!(tags_bytes, expected as f64);

    // A closed store reports nothing, and neither does a view over it.
    state.shutdown().unwrap();
    drop(state);
    let view = StateStore::open_view(&path, Preset::Standard).unwrap();

    assert_eq!(collect(&provider, &exporter), Vec::new());

    drop(view);
}
