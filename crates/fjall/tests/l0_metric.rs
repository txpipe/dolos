//! The `dolos-fjall` meter's L0 gauge, collected through an in-memory exporter.
//!
//! The gauge registers on the global meter provider when a store opens, so
//! this binary holds one test: the provider is installed before any store.

use dolos_core::StateSchema;
use dolos_fjall::{archive::ArchiveStore, metrics, Preset, StateStore};
use fjall::AbstractTree;
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporter, SdkMeterProvider,
};

/// Below the INDEX threshold of 8, so the open's compaction leaves them.
const RUNS: u32 = 5;

/// Fewer than `RUNS`, and below the default threshold of 4, so `state-utxos`
/// holds runs too but is not the largest.
const UTXO_RUNS: u32 = 3;

/// Every `(store, keyspace, runs)` point the gauge reports when collected now.
fn collect(provider: &SdkMeterProvider, exporter: &InMemoryMetricExporter) -> Vec<String> {
    exporter.reset();
    provider.force_flush().unwrap();

    let mut points = Vec::new();

    for resource in exporter.get_finished_metrics().unwrap() {
        for scope in resource.scope_metrics() {
            if scope.scope().name() != metrics::METER {
                continue;
            }

            for metric in scope.metrics() {
                assert_eq!(metric.name(), metrics::L0_RUNS_MAX);

                let AggregatedMetrics::U64(MetricData::Gauge(gauge)) = metric.data() else {
                    panic!("{} is not a u64 gauge", metric.name());
                };

                for point in gauge.data_points() {
                    let mut attributes: Vec<_> = point
                        .attributes()
                        .map(|kv| format!("{}={}", kv.key, kv.value))
                        .collect();
                    attributes.sort();
                    points.push(format!(
                        "{}{{{}}} {}",
                        metric.name(),
                        attributes.join(","),
                        point.value()
                    ));
                }
            }
        }
    }

    points.sort();
    println!("{points:#?}");
    points
}

#[test]
fn the_gauge_reports_each_open_stores_most_l0_runs_with_its_keyspace() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    global::set_meter_provider(provider.clone());

    let dir = tempfile::tempdir().unwrap();
    let state_path = dir.path().join("state");
    let tuning = Preset::Standard.state();

    // Keys at both ends make every table overlap, so each flush is its own L0
    // run; a manual flush queues no compaction.
    let add_run = |keyspace: &fjall::Keyspace, run: u32| {
        keyspace.insert("a", run.to_be_bytes()).unwrap();
        keyspace.insert("z", run.to_be_bytes()).unwrap();
        keyspace.tree.flush_active_memtable(0).unwrap();
    };

    {
        let db = tuning.database.builder(&state_path).open().unwrap();
        let utxos = db
            .keyspace("state-utxos", || tuning.utxos.options())
            .unwrap();
        let tags = db.keyspace("state-tags", || tuning.tags.options()).unwrap();

        for run in 0..UTXO_RUNS {
            add_run(&utxos, run);
        }

        for run in 0..RUNS {
            add_run(&tags, run);
        }
    }

    let state = StateStore::open(&state_path, Preset::Standard).unwrap();
    let archive = ArchiveStore::open(
        StateSchema::default(),
        dir.path().join("archive"),
        None,
        Preset::Standard,
    )
    .unwrap();

    assert_eq!(
        collect(&provider, &exporter),
        [
            "fjall_l0_runs_max{keyspace=archive-blocks,store=archive} 0".to_string(),
            format!("fjall_l0_runs_max{{keyspace=state-tags,store=state}} {RUNS}"),
        ]
    );

    // The gauge reads the open store at each collect.
    let tags = state
        .database()
        .keyspace("state-tags", || tuning.tags.options())
        .unwrap();
    add_run(&tags, RUNS);
    drop(tags);

    assert_eq!(
        collect(&provider, &exporter),
        [
            "fjall_l0_runs_max{keyspace=archive-blocks,store=archive} 0".to_string(),
            format!(
                "fjall_l0_runs_max{{keyspace=state-tags,store=state}} {}",
                RUNS + 1
            ),
        ]
    );

    // A closed store reports nothing, and neither does a view over it.
    state.shutdown().unwrap();
    drop(state);
    let view = StateStore::open_view(&state_path, Preset::Standard).unwrap();

    assert_eq!(
        collect(&provider, &exporter),
        ["fjall_l0_runs_max{keyspace=archive-blocks,store=archive} 0"]
    );

    drop(view);
    archive.shutdown().unwrap();
    drop(archive);

    let archive_view = ArchiveStore::open_view(
        StateSchema::default(),
        dir.path().join("archive"),
        None,
        Preset::Standard,
    )
    .unwrap();

    assert_eq!(collect(&provider, &exporter), Vec::<String>::new());

    drop(archive_view);
}
