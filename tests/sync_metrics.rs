//! The sync series of the `dolos` and `dolos-core` meters, collected through
//! an in-memory exporter.
//!
//! The `dolos-core` instruments bind to the global meter provider when they
//! are first used, so every test installs it first, and the tests run one at
//! a time and compare what they changed. A sync daemon's signal hooks stay
//! with this binary.

use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use dolos::adapters::DomainAdapter;
use dolos::engine::DomainBuilder;
use dolos::sync::{
    apply,
    metrics::{ExportedDaemon, MetricsFlush},
    SyncLimit, SyncProgress,
};
use dolos_core::config::{
    ArchiveStoreConfig, ChainConfig, RetryConfig, RootConfig, StateStoreConfig,
};
use dolos_core::{
    telemetry, ArchiveStore as _, ChainPoint, Domain as _, PullEvent, RawBlock, SyncExt as _,
};
use dolos_testing::blocks::make_conway_block_with_prev;
use dolos_testing::peer::{PeerFixture, PeerOptions};
use gasket::metrics::Reading;
use gasket::runtime::{Tether, TetherState};
use opentelemetry::global;
use opentelemetry_sdk::metrics::{
    data::{AggregatedMetrics, MetricData},
    InMemoryMetricExporter, SdkMeterProvider,
};
use serial_test::serial;

const PREVIEW_MAGIC: u64 = 2;

/// The provider every test exports through, installed as the global one
/// before any instrument exists.
fn provider() -> &'static (SdkMeterProvider, InMemoryMetricExporter) {
    static PROVIDER: OnceLock<(SdkMeterProvider, InMemoryMetricExporter)> = OnceLock::new();

    PROVIDER.get_or_init(|| {
        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();
        global::set_meter_provider(provider.clone());

        (provider, exporter)
    })
}

/// Every point of the two meters as `name{attributes}`, collected now.
fn collect() -> BTreeMap<String, f64> {
    let (provider, exporter) = provider();

    exporter.reset();
    provider.force_flush().unwrap();

    exported()
}

/// Every point of the two meters the exporter holds, as `name{attributes}`:
/// a counter's or a gauge's value, or for a histogram `name.count{…}` and
/// `name.sum{…}`. A later export of a series replaces an earlier one.
fn exported() -> BTreeMap<String, f64> {
    let (_, exporter) = provider();

    let mut points = BTreeMap::new();

    for resource in exporter.get_finished_metrics().unwrap() {
        for scope in resource.scope_metrics() {
            if ![dolos::sync::metrics::METER, telemetry::METER].contains(&scope.scope().name()) {
                continue;
            }

            for metric in scope.metrics() {
                let series = |suffix: &str, attributes: &mut dyn Iterator<Item = &opentelemetry::KeyValue>| {
                    let mut attributes: Vec<_> = attributes
                        .map(|kv| format!("{}={}", kv.key, kv.value))
                        .collect();
                    attributes.sort();
                    format!("{}{suffix}{{{}}}", metric.name(), attributes.join(","))
                };

                match metric.data() {
                    AggregatedMetrics::U64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            points
                                .insert(series("", &mut point.attributes()), point.value() as f64);
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Sum(sum)) => {
                        for point in sum.data_points() {
                            points.insert(series("", &mut point.attributes()), point.value());
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            points
                                .insert(series("", &mut point.attributes()), point.value() as f64);
                        }
                    }
                    AggregatedMetrics::I64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            points
                                .insert(series("", &mut point.attributes()), point.value() as f64);
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Gauge(gauge)) => {
                        for point in gauge.data_points() {
                            points.insert(series("", &mut point.attributes()), point.value());
                        }
                    }
                    AggregatedMetrics::F64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            points.insert(
                                series(".count", &mut point.attributes()),
                                point.count() as f64,
                            );
                            points.insert(series(".sum", &mut point.attributes()), point.sum());
                        }
                    }
                    AggregatedMetrics::U64(MetricData::Histogram(histogram)) => {
                        for point in histogram.data_points() {
                            points.insert(
                                series(".count", &mut point.attributes()),
                                point.count() as f64,
                            );
                            points.insert(
                                series(".sum", &mut point.attributes()),
                                point.sum() as f64,
                            );
                        }
                    }
                    _ => {}
                }
            }
        }
    }

    points
}

fn value(points: &BTreeMap<String, f64>, series: &str) -> f64 {
    points.get(series).copied().unwrap_or_default()
}

fn domain_over(root: &std::path::Path) -> (RootConfig, DomainAdapter) {
    let mut config = RootConfig::new(ChainConfig::known("preview"));
    config.storage.path = root.to_path_buf();
    config.storage.state = StateStoreConfig::ephemeral();
    config.storage.archive = ArchiveStoreConfig::ephemeral();
    config.sync.retry = RetryConfig {
        max_retries: 1,
        backoff_unit_seconds: 0,
        ..Default::default()
    };

    let genesis = dolos_cardano::networks::PREVIEW.genesis();
    let domain = DomainBuilder::new(&config, Arc::new(genesis))
        .build()
        .unwrap();

    (config, domain)
}

/// Chained blocks at slots `1..=n`.
fn chain(n: u64) -> Vec<(ChainPoint, RawBlock)> {
    let mut blocks = Vec::new();
    let mut parent = None;

    for slot in 1..=n {
        let (point, block) = make_conway_block_with_prev(slot, parent, slot);
        parent = point.hash();
        blocks.push((point, block));
    }

    blocks
}

/// Wait until `done` holds, or panic naming `what` after `limit`.
fn wait_until(what: &str, limit: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;

    while !done() {
        assert!(Instant::now() < deadline, "timed out waiting until {what}");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// A count a stage reports itself.
fn stage_count(tether: &Tether, key: &str) -> u64 {
    tether
        .read_metrics()
        .unwrap()
        .into_iter()
        .find_map(|(name, reading)| match (name, reading) {
            (name, Reading::Count(count)) if name == key => Some(count),
            _ => None,
        })
        .unwrap_or(0)
}

#[test]
#[serial]
fn a_sync_exports_each_stage_s_blocks_and_framework_series() {
    provider();

    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path());
    let peer = PeerFixture::start(
        "127.0.0.1:0",
        PREVIEW_MAGIC,
        PeerOptions {
            initial_blocks: 20,
            ..Default::default()
        },
    )
    .unwrap();

    let before = collect();

    let pipeline = dolos::sync::sync(
        &config.sync,
        PREVIEW_MAGIC,
        peer.address().to_string(),
        SyncLimit::Follow,
        domain.clone(),
        MetricsFlush::default(),
    )
    .unwrap();

    let progress = pipeline.progress;
    let _gauges = pipeline.gauges;
    let daemon = ExportedDaemon::new(gasket::daemon::Daemon::new(pipeline.tethers));

    wait_until("the target is committed", Duration::from_secs(60), || {
        progress.caught_up()
    });

    let archived = domain.archive().get_range(None, None).unwrap().count() as f64;
    assert!(archived > 0.0);

    let points = collect();

    for stage in ["pull", "apply"] {
        assert_eq!(
            value(
                &points,
                &format!("dolos.sync.stage.blocks{{stage={stage}}}")
            ),
            archived,
            "{stage} blocks"
        );

        for name in ["ticks", "units", "retries", "phase", "busy_time"] {
            let series = format!("dolos.sync.stage.{name}{{stage={stage}}}");
            assert!(points.contains_key(&series), "{series} was not exported");
        }
    }

    assert!(points.contains_key("dolos.sync.stage.upstream_tip_slot{stage=pull}"));
    assert!(points.contains_key("dolos.sync.stage.housekeeping_runs{stage=apply}"));
    assert_eq!(value(&points, "dolos.sync.caught_up{}"), 1.0);
    assert!(points.contains_key("dolos.sync.tip.lag{}"));
    assert!(points.contains_key("dolos.sync.upstream.last_delivery.age{}"));
    let connected = "dolos.sync.upstream.sessions{outcome=connected}";
    assert_eq!(value(&points, connected) - value(&before, connected), 1.0);

    // The session opened with a rollback to its intersection, which undid
    // nothing and is no rollback of the chain.
    assert_eq!(
        value(&points, "dolos.sync.stage.rollbacks{stage=apply}"),
        0.0
    );
    assert_eq!(
        value(&points, "dolos.sync.rollback.depth.count{}"),
        value(&before, "dolos.sync.rollback.depth.count{}"),
    );

    daemon.teardown();

    // Released, the bridge reports no stage.
    let points = collect();
    assert!(!points.contains_key("dolos.sync.stage.blocks{stage=pull}"));
}

/// The bridge reads a stage only while it runs, so a catch-up's apply stage,
/// which ends once it commits the target, exports its counts before it ends.
#[test]
#[serial]
fn a_catch_up_exports_the_apply_stage_s_counts_before_it_ends() {
    let (provider, exporter) = provider();

    let temp = tempfile::tempdir().unwrap();
    let (config, domain) = domain_over(temp.path());
    let peer = PeerFixture::start(
        "127.0.0.1:0",
        PREVIEW_MAGIC,
        PeerOptions {
            initial_blocks: 20,
            ..Default::default()
        },
    )
    .unwrap();

    exporter.reset();
    let pipeline = dolos::sync::sync(
        &config.sync,
        PREVIEW_MAGIC,
        peer.address().to_string(),
        SyncLimit::CatchUp,
        domain.clone(),
        MetricsFlush::new(provider.clone()),
    )
    .unwrap();

    let progress = pipeline.progress;
    let _gauges = pipeline.gauges;
    let daemon = ExportedDaemon::new(gasket::daemon::Daemon::new(pipeline.tethers));

    let apply = || daemon.tethers().find(|t| t.name() == "apply").unwrap();
    wait_until(
        "the apply stage's thread ends",
        Duration::from_secs(60),
        || matches!(apply().check_state(), TetherState::Finished(_)),
    );
    assert!(progress.caught_up());
    assert_eq!(apply().end_cause(), Some(gasket::runtime::EndCause::Done));

    let archived = domain.archive().get_range(None, None).unwrap().count() as f64;
    assert!(archived > 0.0);

    let points = exported();
    assert_eq!(
        value(&points, "dolos.sync.stage.blocks{stage=apply}"),
        archived
    );
    assert!(points.contains_key("dolos.sync.stage.units{stage=apply}"));
    assert_eq!(value(&points, "dolos.sync.caught_up{}"), 1.0);

    // Ended, the stage is no longer read.
    let points = collect();
    assert!(!points.contains_key("dolos.sync.stage.blocks{stage=apply}"));

    daemon.teardown();
}

#[test]
#[serial]
fn a_rollback_of_three_blocks_records_one_depth_of_three() {
    provider();

    let temp = tempfile::tempdir().unwrap();
    let (_, domain) = domain_over(temp.path());

    let mut apply = apply::Stage::new(
        domain.clone(),
        SyncLimit::Follow,
        Arc::new(SyncProgress::default()),
        Duration::from_secs(3600),
    );

    let mut feed = gasket::messaging::OutputPort::<PullEvent>::default();
    let (to_apply, from_feed) = gasket::messaging::tokio::mpsc_channel(50);
    feed.connect(to_apply);
    apply.upstream.connect(from_feed);

    let tether = gasket::runtime::spawn_stage(apply, gasket::runtime::Policy::default());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let blocks = chain(5);
    runtime.block_on(async {
        for (_, block) in &blocks {
            feed.send(PullEvent::RollForward(block.clone()).into())
                .await
                .unwrap();
        }
    });

    wait_until("five blocks are applied", Duration::from_secs(30), || {
        stage_count(&tether, "blocks") == 5
    });
    assert_eq!(stage_count(&tether, "rollbacks"), 0);

    let before = collect();

    runtime.block_on(async {
        feed.send(PullEvent::Rollback(blocks[1].0.clone()).into())
            .await
            .unwrap();
    });

    wait_until("the rollback is applied", Duration::from_secs(30), || {
        stage_count(&tether, "rollbacks") == 1
    });

    let after = collect();
    let delta = |series: &str| value(&after, series) - value(&before, series);

    assert_eq!(delta("dolos.sync.rollback.depth.count{}"), 1.0);
    assert_eq!(delta("dolos.sync.rollback.depth.sum{}"), 3.0);
    assert_eq!(stage_count(&tether, "blocks"), 5);

    tether.dismiss_stage().unwrap();
    tether.join_stage();
}

#[test]
#[serial]
fn a_roll_forward_records_a_roll_duration() {
    provider();

    let temp = tempfile::tempdir().unwrap();
    let (_, domain) = domain_over(temp.path());
    let blocks = chain(2);

    // The first block also brings the genesis work.
    domain.roll_forward(blocks[0].1.clone()).unwrap();
    let before = collect();

    domain.roll_forward(blocks[1].1.clone()).unwrap();
    let after = collect();

    let series = "dolos.sync.work_unit.duration.count{work_unit=roll}";
    assert_eq!(value(&after, series) - value(&before, series), 1.0);
    assert!(before.contains_key("dolos.sync.work_unit.duration.count{work_unit=genesis}"));
}
