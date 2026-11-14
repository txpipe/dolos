//! Store metrics, read when the meter collects them and never on a write.

use std::cmp::Reverse;
use std::sync::{Arc, Weak};

use fjall::{AbstractTree as _, Keyspace};
use opentelemetry::{global, metrics::ObservableGauge, KeyValue};

/// The meter every fjall store reports under.
pub const METER: &str = "dolos-fjall";

/// The gauge of a store's largest L0 run count.
pub const L0_RUNS_MAX: &str = "fjall_l0_runs_max";

/// A writable store's keyspaces and the gauge that reads them. The gauge
/// holds them weakly, so it reads nothing once the store has closed.
pub(crate) struct L0Runs {
    _keyspaces: Arc<[(&'static str, Keyspace)]>,
    _gauge: ObservableGauge<u64>,
}

impl L0Runs {
    /// Report, as `store`, the most L0 runs any of `keyspaces` holds, with
    /// that keyspace's name. fjall halts a keyspace's writes at 30 runs.
    pub(crate) fn report(store: &'static str, keyspaces: Vec<(&'static str, Keyspace)>) -> Self {
        let keyspaces: Arc<[_]> = keyspaces.into();
        let weak = Arc::downgrade(&keyspaces);

        let gauge = global::meter(METER)
            .u64_observable_gauge(L0_RUNS_MAX)
            .with_unit("{run}")
            .with_description(
                "The most L0 runs in one of the store's keyspaces; fjall halts its writes at 30",
            )
            .with_callback(move |observer| {
                let Some((keyspace, runs)) = largest(&weak) else {
                    return;
                };

                let attributes = [
                    KeyValue::new("store", store),
                    KeyValue::new("keyspace", keyspace),
                ];
                observer.observe(runs as u64, &attributes);
            })
            .build();

        Self {
            _keyspaces: keyspaces,
            _gauge: gauge,
        }
    }
}

/// The keyspace with the most L0 runs, the first one on a tie.
fn largest(keyspaces: &Weak<[(&'static str, Keyspace)]>) -> Option<(&'static str, usize)> {
    let keyspaces = keyspaces.upgrade()?;

    keyspaces
        .iter()
        .map(|(name, keyspace)| (*name, keyspace.tree.l0_run_count()))
        .min_by_key(|(_, runs)| Reverse(*runs))
}
