//! The sync's series on the `dolos` meter: the stages' metrics through the
//! gasket bridge, the upstream sessions, and the gauges over a running sync's
//! progress and the chain tip.

use std::ops::Deref;
use std::sync::{Arc, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

use dolos_core::telemetry::OUTCOME;
use gasket::daemon::Daemon;
use opentelemetry::{
    global,
    metrics::{Counter, Meter, ObservableGauge},
    KeyValue,
};
use pallas::interop::utxorpc::LedgerContext as _;

use super::SyncProgress;
use crate::adapters::DomainAdapter;

/// The meter of the instruments this crate records.
pub const METER: &str = "dolos";

/// The prefix of the stage series the bridge exports, each with the `stage`
/// attribute.
pub const STAGE_NAMESPACE: &str = "dolos.sync.stage";

/// A sync daemon whose stages' metrics are exported while it runs.
pub struct ExportedDaemon {
    daemon: Arc<Daemon>,
    bridge: gasket_otel::Instruments,
}

impl ExportedDaemon {
    /// Exports `daemon`'s stages on the `dolos` meter.
    pub fn new(daemon: Daemon) -> Self {
        Self::on(&global::meter(METER), daemon)
    }

    /// Exports `daemon`'s stages on `meter`.
    pub fn on(meter: &Meter, daemon: Daemon) -> Self {
        let daemon = Arc::new(daemon);
        let bridge = gasket_otel::register(meter, STAGE_NAMESPACE, Arc::clone(&daemon));

        Self { daemon, bridge }
    }

    /// Stops the export, then dismisses the stages and waits for them to end.
    pub fn teardown(self) {
        let Self { daemon, bridge } = self;
        drop(daemon);

        Arc::try_unwrap(bridge.release())
            .expect("the bridge holds the only other handle on the daemon")
            .teardown();
    }
}

impl Deref for ExportedDaemon {
    type Target = Daemon;

    fn deref(&self) -> &Daemon {
        &self.daemon
    }
}

/// `dolos.sync.upstream.sessions`: the pull stage's sessions with the peer,
/// by `outcome`.
pub(crate) struct UpstreamSessions(Counter<u64>);

impl UpstreamSessions {
    pub(crate) fn on(meter: &Meter) -> Self {
        Self(
            meter
                .u64_counter("dolos.sync.upstream.sessions")
                .with_unit("{session}")
                .with_description("Sessions with the upstream peer, by outcome")
                .build(),
        )
    }

    fn count(&self, outcome: &'static str) {
        self.0.add(1, &[KeyValue::new(OUTCOME, outcome)]);
    }

    /// A session connected and found its intersection.
    pub(crate) fn connected(&self) {
        self.count("connected");
    }

    /// An attempt that did not connect or found no intersection.
    pub(crate) fn failed(&self) {
        self.count("failed");
    }

    /// A connected session that ended.
    pub(crate) fn lost(&self) {
        self.count("lost");
    }
}

impl Default for UpstreamSessions {
    fn default() -> Self {
        Self::on(&global::meter(METER))
    }
}

/// The wall-clock time, in Unix seconds, of the chain tip's slot.
type TipTime = dyn Fn() -> Option<u64> + Send + Sync;

type Clock = dyn Fn() -> SystemTime + Send + Sync;

struct Sources {
    progress: Arc<SyncProgress>,
    tip_time: Box<TipTime>,
    clock: Box<Clock>,
}

/// The `dolos.sync.tip.lag`, `dolos.sync.caught_up` and
/// `dolos.sync.upstream.last_delivery.age` gauges.
///
/// The gauges hold their sources weakly, so they report nothing once this
/// value is dropped: keep it for the life of the sync.
pub struct SyncGauges {
    _sources: Arc<Sources>,
    _tip_lag: ObservableGauge<f64>,
    _caught_up: ObservableGauge<u64>,
    _last_delivery_age: ObservableGauge<f64>,
}

impl SyncGauges {
    /// The gauges of a sync over `domain` on the `dolos` meter, against the
    /// system clock.
    pub fn over(progress: &Arc<SyncProgress>, domain: &DomainAdapter) -> Self {
        Self::over_on(&global::meter(METER), progress, domain, SystemTime::now)
    }

    /// [`Self::over`] on `meter`, against `clock`.
    pub(crate) fn over_on(
        meter: &Meter,
        progress: &Arc<SyncProgress>,
        domain: &DomainAdapter,
        clock: impl Fn() -> SystemTime + Send + Sync + 'static,
    ) -> Self {
        let domain = domain.clone();

        Self::register(
            meter,
            progress.clone(),
            move || {
                let (slot, _) = crate::adapters::archive_tip(&domain.archive)?;
                domain.get_slot_timestamp(slot)
            },
            clock,
        )
    }

    /// Registers the gauges on `meter`: the tip lag is `clock` minus
    /// `tip_time`, and reports nothing while `tip_time` has none.
    pub fn register(
        meter: &Meter,
        progress: Arc<SyncProgress>,
        tip_time: impl Fn() -> Option<u64> + Send + Sync + 'static,
        clock: impl Fn() -> SystemTime + Send + Sync + 'static,
    ) -> Self {
        let sources = Arc::new(Sources {
            progress,
            tip_time: Box::new(tip_time),
            clock: Box::new(clock),
        });

        let weak = Arc::downgrade(&sources);
        let tip_lag = meter
            .f64_observable_gauge("dolos.sync.tip.lag")
            .with_unit("s")
            .with_description("Wall clock minus the time of the chain tip's slot")
            .with_callback(move |observer| {
                if let Some(lag) = weak.upgrade().and_then(|sources| sources.tip_lag()) {
                    observer.observe(lag, &[]);
                }
            })
            .build();

        let weak: Weak<Sources> = Arc::downgrade(&sources);
        let caught_up = meter
            .u64_observable_gauge("dolos.sync.caught_up")
            .with_description("1 once the stores committed the upstream tip, else 0")
            .with_callback(move |observer| {
                if let Some(sources) = weak.upgrade() {
                    observer.observe(u64::from(sources.progress.caught_up()), &[]);
                }
            })
            .build();

        let weak: Weak<Sources> = Arc::downgrade(&sources);
        let last_delivery_age = meter
            .f64_observable_gauge("dolos.sync.upstream.last_delivery.age")
            .with_unit("s")
            .with_description("Time since the upstream peer last delivered")
            .with_callback(move |observer| {
                let last = weak
                    .upgrade()
                    .and_then(|sources| sources.progress.last_delivery());

                if let Some(last) = last {
                    observer.observe(last.elapsed().as_secs_f64(), &[]);
                }
            })
            .build();

        Self {
            _sources: sources,
            _tip_lag: tip_lag,
            _caught_up: caught_up,
            _last_delivery_age: last_delivery_age,
        }
    }
}

impl Sources {
    fn tip_lag(&self) -> Option<f64> {
        let tip = (self.tip_time)()?;
        let now = (self.clock)().duration_since(UNIX_EPOCH).ok()?;

        Some(now.as_secs_f64() - tip as f64)
    }
}
