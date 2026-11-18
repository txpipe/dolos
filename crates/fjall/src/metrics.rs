//! Store metrics, read when the meter collects them and never on a write.

use std::cmp::Reverse;
use std::sync::{Arc, Weak};

use dolos_core::telemetry;
use fjall::{AbstractTree as _, Database, Keyspace};
use opentelemetry::{
    global,
    metrics::{Meter, ObservableCounter, ObservableGauge},
    KeyValue,
};

/// The meter every fjall store reports under.
pub const METER: &str = "dolos-fjall";

/// The gauge of a store's largest L0 run count.
pub const L0_RUNS_MAX: &str = "dolos.storage.l0_runs_max";

/// The gauge of the bytes a store's keyspaces take on disk.
pub const DISK_USAGE: &str = "dolos.storage.disk.usage";

/// The gauge of the compactions a store runs now.
pub const COMPACTIONS_ACTIVE: &str = "dolos.storage.compactions.active";

/// The counter of the compactions a store finished since it opened.
pub const COMPACTIONS_COMPLETED: &str = "dolos.storage.compactions.completed";

/// The counter of the time a store's compactions took since it opened.
pub const COMPACTION_TIME: &str = "dolos.storage.compaction.time";

/// The gauge of a store's queued memtable flushes.
pub const FLUSHES_OUTSTANDING: &str = "dolos.storage.flushes.outstanding";

/// The gauge of the bytes a store's memtables hold.
pub const WRITE_BUFFER_SIZE: &str = "dolos.storage.write_buffer.size";

/// What the instruments read: an open store's database and keyspaces.
struct Source {
    db: Database,
    keyspaces: Vec<(&'static str, Keyspace)>,
}

/// A writable store's instruments and what they read. The instruments hold
/// the source weakly, so they read nothing once the store has closed.
pub(crate) struct StoreMetrics {
    _source: Arc<Source>,
    _gauges: Vec<ObservableGauge<u64>>,
    _completed: ObservableCounter<u64>,
    _compaction_time: ObservableCounter<f64>,
}

impl StoreMetrics {
    /// Report `db` and its `keyspaces` as `store`.
    pub(crate) fn report(
        store: &'static str,
        db: Database,
        keyspaces: Vec<(&'static str, Keyspace)>,
    ) -> Self {
        let source = Arc::new(Source { db, keyspaces });
        let meter = global::meter(METER);

        let per_keyspace = |name, unit, description, read: fn(&Keyspace) -> u64| {
            let weak = Arc::downgrade(&source);

            meter
                .u64_observable_gauge(name)
                .with_unit(unit)
                .with_description(description)
                .with_callback(move |observer| {
                    let Some(source) = weak.upgrade() else {
                        return;
                    };

                    for (keyspace, handle) in &source.keyspaces {
                        observer.observe(read(handle), &keyspace_attributes(store, keyspace));
                    }
                })
                .build()
        };

        let l0_runs = {
            let weak = Arc::downgrade(&source);

            meter
                .u64_observable_gauge(L0_RUNS_MAX)
                .with_unit("{run}")
                .with_description(
                    "The most L0 runs in one of the store's keyspaces; fjall halts its writes at 30",
                )
                .with_callback(move |observer| {
                    if let Some((keyspace, runs)) = weak.upgrade().and_then(|s| largest(&s)) {
                        observer.observe(runs as u64, &keyspace_attributes(store, keyspace));
                    }
                })
                .build()
        };

        let gauges = vec![
            l0_runs,
            per_keyspace(
                DISK_USAGE,
                "By",
                "The bytes the store's files take on disk",
                Keyspace::disk_space,
            ),
            database_gauge(
                &meter,
                &source,
                store,
                COMPACTIONS_ACTIVE,
                "{compaction}",
                "The compactions the store runs now",
                |db| db.active_compactions() as u64,
            ),
            database_gauge(
                &meter,
                &source,
                store,
                FLUSHES_OUTSTANDING,
                "{flush}",
                "The memtable flushes the store has queued",
                |db| db.outstanding_flushes() as u64,
            ),
            database_gauge(
                &meter,
                &source,
                store,
                WRITE_BUFFER_SIZE,
                "By",
                "The bytes the store's memtables hold",
                Database::write_buffer_size,
            ),
        ];

        let completed = {
            let weak = Arc::downgrade(&source);

            meter
                .u64_observable_counter(COMPACTIONS_COMPLETED)
                .with_unit("{compaction}")
                .with_description("The compactions the store finished since it opened")
                .with_callback(move |observer| {
                    if let Some(source) = weak.upgrade() {
                        let completed = source.db.compactions_completed() as u64;
                        observer.observe(completed, &store_attributes(store));
                    }
                })
                .build()
        };

        let compaction_time = {
            let weak = Arc::downgrade(&source);

            meter
                .f64_observable_counter(COMPACTION_TIME)
                .with_unit("s")
                .with_description("The time the store's compactions took since it opened")
                .with_callback(move |observer| {
                    if let Some(source) = weak.upgrade() {
                        let took = source.db.time_compacting().as_secs_f64();
                        observer.observe(took, &store_attributes(store));
                    }
                })
                .build()
        };

        Self {
            _source: source,
            _gauges: gauges,
            _completed: completed,
            _compaction_time: compaction_time,
        }
    }
}

fn store_attributes(store: &'static str) -> [KeyValue; 1] {
    [KeyValue::new(telemetry::STORE, store)]
}

fn keyspace_attributes(store: &'static str, keyspace: &'static str) -> [KeyValue; 2] {
    [
        KeyValue::new(telemetry::STORE, store),
        KeyValue::new(telemetry::KEYSPACE, keyspace),
    ]
}

/// A gauge, labelled with `store`, of `read` over the source's database.
fn database_gauge(
    meter: &Meter,
    source: &Arc<Source>,
    store: &'static str,
    name: &'static str,
    unit: &'static str,
    description: &'static str,
    read: fn(&Database) -> u64,
) -> ObservableGauge<u64> {
    let weak: Weak<Source> = Arc::downgrade(source);

    meter
        .u64_observable_gauge(name)
        .with_unit(unit)
        .with_description(description)
        .with_callback(move |observer| {
            if let Some(source) = weak.upgrade() {
                observer.observe(read(&source.db), &store_attributes(store));
            }
        })
        .build()
}

/// The keyspace with the most L0 runs, the first one on a tie.
fn largest(source: &Source) -> Option<(&'static str, usize)> {
    source
        .keyspaces
        .iter()
        .map(|(name, keyspace)| (*name, keyspace.tree.l0_run_count()))
        .min_by_key(|(_, runs)| Reverse(*runs))
}
