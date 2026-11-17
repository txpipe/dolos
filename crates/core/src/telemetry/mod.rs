//! What every Dolos metric shares: the meter of this crate, the histogram
//! boundaries, the attribute keys and the request instruments.
//!
//! A name the OpenTelemetry semantic conventions define is used as defined;
//! every other name sits under `dolos.`, by area and then by subject.
//! Instruments bind to the meter provider that is global when they are
//! created, so they are created only once the provider is installed.

use std::sync::{Arc, Weak};

use opentelemetry::{global, metrics::ObservableGauge};

pub mod requests;

/// The meter of the instruments this crate records.
pub const METER: &str = "dolos-core";

/// Bucket boundaries, in seconds, of every duration histogram.
pub const DURATION_BOUNDARIES: [f64; 12] = [
    0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
];

/// The surface that served a request: `minibf`, `minikupo`, `grpc`, `trp`.
pub const SURFACE: &str = "surface";

/// A pipeline stage.
pub const STAGE: &str = "stage";

/// A store: `state`, `archive`, `wal`, `cache`.
pub const STORE: &str = "store";

/// A keyspace within a store.
pub const KEYSPACE: &str = "keyspace";

/// A cache namespace.
pub const NAMESPACE: &str = "namespace";

/// A cache tier: `memory` or `disk`.
pub const TIER: &str = "tier";

/// The result of an operation.
pub const OUTCOME: &str = "outcome";

/// The crate version of the running build.
pub const VERSION: &str = "version";

/// The commit of the running build, or `unknown` when it was built without
/// one.
pub const GIT_SHA: &str = "git_sha";

/// The slot and block number of the chain tip, when there is one.
type ReadTip = dyn Fn() -> Option<(u64, u64)> + Send + Sync;

/// The `dolos.sync.tip.slot` and `dolos.sync.tip.block_number` gauges.
///
/// The gauges hold the reader weakly, so they report nothing once this value
/// is dropped: keep it as long as the stores the reader reads are open.
pub struct TipGauges {
    _read: Arc<ReadTip>,
    _slot: ObservableGauge<u64>,
    _block_number: ObservableGauge<u64>,
}

impl TipGauges {
    /// Registers both gauges on this crate's meter, reading the tip with
    /// `read` each time the meter collects them.
    pub fn register(read: impl Fn() -> Option<(u64, u64)> + Send + Sync + 'static) -> Self {
        let read: Arc<ReadTip> = Arc::new(read);
        let meter = global::meter(METER);

        let gauge = |name: &'static str, unit: &'static str, pick: fn((u64, u64)) -> u64| {
            let read: Weak<ReadTip> = Arc::downgrade(&read);

            meter
                .u64_observable_gauge(name)
                .with_unit(unit)
                .with_callback(move |observer| {
                    if let Some(tip) = read.upgrade().and_then(|read| read()) {
                        observer.observe(pick(tip), &[]);
                    }
                })
                .build()
        };

        let slot = gauge("dolos.sync.tip.slot", "{slot}", |(slot, _)| slot);
        let block_number = gauge("dolos.sync.tip.block_number", "{block}", |(_, number)| {
            number
        });

        Self {
            _read: read,
            _slot: slot,
            _block_number: block_number,
        }
    }
}
