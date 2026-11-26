use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::config::{
    EmulatorConfig, PeerConfig, RetryConfig, RootConfig, SyncConfig, UpstreamConfig,
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::sync::watch;

use crate::adapters::DomainAdapter;
use crate::prelude::*;

pub mod apply;
pub mod emulator;
pub mod metrics;
pub mod pull;
mod retry;
pub mod submit;

#[cfg(test)]
mod tests;

const HOUSEKEEPING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(60);

/// When a sync ends: an invocation input, never configuration. A stop epoch,
/// set on the domain, ends either one if the ledger reaches it first.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncLimit {
    /// Follow the upstream tip until stopped.
    #[default]
    Follow,
    /// End once the WAL and the state have committed the tip the peer
    /// reported when chain-sync first reached it, pulling nothing past it.
    /// An emulator upstream has no such tip, so only a stop epoch ends it.
    CatchUp,
}

/// What a running sync is doing, as the node's status file reports it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SyncState {
    /// No session with the upstream has opened yet.
    Starting,
    /// A session is open, and the stores have not yet committed the tip the
    /// upstream reported when chain-sync first reached it.
    CatchingUp,
    /// The stores committed that tip. An emulator upstream, which has no tip
    /// to catch up to, is followed from its first block.
    Following,
    /// The pull stage is retrying the upstream: a session ended, or an
    /// attempt to open one failed.
    UpstreamLost,
}

/// Where the pull stage stands with the upstream. [`SyncProgress`] keeps it
/// as its `u8`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Session {
    Unopened,
    Open,
    Retrying,
}

impl Session {
    fn from_code(code: u8) -> Self {
        match code {
            code if code == Self::Open as u8 => Self::Open,
            code if code == Self::Retrying as u8 => Self::Retrying,
            _ => Self::Unopened,
        }
    }
}

/// What a running sync has reached, shared by its stages and read by
/// whoever runs it.
#[derive(Debug)]
pub struct SyncProgress {
    target: watch::Sender<Option<ChainPoint>>,
    caught_up: AtomicBool,
    stop_epoch_reached: AtomicBool,
    upstream_failure: OnceLock<UpstreamFailure>,
    /// How many times the pull stage's sessions delivered something.
    deliveries: AtomicU64,
    /// When they last did.
    last_delivery: Mutex<Option<Instant>>,
    /// Whether the upstream is the emulator, which produces the chain itself.
    emulated: bool,
    /// The pull stage's [`Session`].
    session: AtomicU8,
    /// The slot of the tip the upstream reported last, or [`NEVER`].
    upstream_tip_slot: AtomicU64,
    /// When the upstream last sent anything, in Unix milliseconds, or
    /// [`NEVER`].
    heard_at: AtomicU64,
    /// When the pipeline last advanced, in Unix milliseconds, or [`NEVER`]:
    /// a block applied, a rollback that undid blocks, or the upstream
    /// answering at a tip the stores hold.
    advanced_at: AtomicU64,
    /// The slot the stores stand on, as the sync found them and the apply
    /// stage since left them, or [`NEVER`] when that is unknown.
    standing_slot: AtomicU64,
}

/// What an unset reading of [`SyncProgress`] holds.
const NEVER: u64 = u64::MAX;

fn now_unix_ms() -> u64 {
    let since = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    u64::try_from(since.as_millis()).unwrap_or(NEVER - 1)
}

fn reading(value: &AtomicU64) -> Option<u64> {
    Some(value.load(Ordering::SeqCst)).filter(|value| *value != NEVER)
}

fn unix_ms(value: &AtomicU64) -> Option<SystemTime> {
    reading(value).map(|ms| UNIX_EPOCH + Duration::from_millis(ms))
}

/// Why a stage gave up on the upstream peer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpstreamFailure {
    /// The stage, the peer, the attempts and the last one's cause.
    pub message: String,
    /// Whether the peer refused the last attempt's handshake: it serves
    /// another network, or shares no protocol version with dolos.
    pub refused: bool,
    /// Whether the WAL holds one point alone, as a reset or a bootstrap
    /// leaves it, which the peer, past it, does not hold: a fork dropped that
    /// block, and the store can't sync on.
    pub stranded: bool,
}

impl fmt::Display for UpstreamFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl Default for SyncProgress {
    fn default() -> Self {
        Self {
            target: watch::Sender::new(None),
            caught_up: AtomicBool::new(false),
            stop_epoch_reached: AtomicBool::new(false),
            upstream_failure: OnceLock::new(),
            deliveries: AtomicU64::new(0),
            last_delivery: Mutex::new(None),
            emulated: false,
            session: AtomicU8::new(Session::Unopened as u8),
            upstream_tip_slot: AtomicU64::new(NEVER),
            heard_at: AtomicU64::new(NEVER),
            advanced_at: AtomicU64::new(NEVER),
            standing_slot: AtomicU64::new(NEVER),
        }
    }
}

impl SyncProgress {
    /// The progress of a sync over `domain`, whose stores stand where the
    /// last run left them.
    fn over(domain: &DomainAdapter) -> Result<Self, Error> {
        let progress = Self::default();
        let cursor = domain.state().read_cursor()?;
        progress.note_standing(cursor.as_ref().map_or(0, ChainPoint::slot));

        Ok(progress)
    }

    /// [`Self::over`], for a sync from the emulator.
    fn emulated(domain: &DomainAdapter) -> Result<Self, Error> {
        Ok(Self {
            emulated: true,
            ..Self::over(domain)?
        })
    }

    /// What the sync is doing: the pull stage's session first, then whether
    /// the stores committed the target.
    pub fn state(&self) -> SyncState {
        match Session::from_code(self.session.load(Ordering::SeqCst)) {
            Session::Unopened => SyncState::Starting,
            Session::Retrying => SyncState::UpstreamLost,
            Session::Open if self.emulated || self.caught_up() => SyncState::Following,
            Session::Open => SyncState::CatchingUp,
        }
    }

    /// The slot of the tip the upstream reported last; none before the
    /// first.
    pub fn upstream_tip_slot(&self) -> Option<u64> {
        reading(&self.upstream_tip_slot)
    }

    /// When the upstream last sent anything; none before the first message.
    pub fn heard_at(&self) -> Option<SystemTime> {
        unix_ms(&self.heard_at)
    }

    /// When the pipeline last advanced: a block applied, a rollback that
    /// undid blocks, or the upstream answering at a tip the stores hold. None
    /// before the first.
    pub fn advanced_at(&self) -> Option<SystemTime> {
        unix_ms(&self.advanced_at)
    }

    /// A session with the upstream opened.
    fn note_session_open(&self) {
        self.session.store(Session::Open as u8, Ordering::SeqCst);
    }

    /// The pull stage is retrying the upstream: a session ended, or an
    /// attempt to open one failed.
    fn note_session_retrying(&self) {
        self.session
            .store(Session::Retrying as u8, Ordering::SeqCst);
    }

    /// The upstream sent a message.
    fn note_heard(&self) {
        self.heard_at.store(now_unix_ms(), Ordering::SeqCst);
    }

    /// The upstream sent a message reporting its tip at `slot`.
    fn note_upstream_tip(&self, slot: u64) {
        self.upstream_tip_slot.store(slot, Ordering::SeqCst);
        self.note_heard();
    }

    /// The pipeline advanced.
    fn note_advance(&self) {
        self.advanced_at.store(now_unix_ms(), Ordering::SeqCst);
    }

    /// The stores stand on `slot`.
    fn note_standing(&self, slot: u64) {
        self.standing_slot.store(slot, Ordering::SeqCst);
    }

    /// The apply stage moved the stores onto `slot`: the pipeline advanced.
    fn note_applied(&self, slot: u64) {
        self.note_standing(slot);
        self.note_advance();
    }

    /// Whether the stores stand on `tip` or past its slot.
    fn stands_on(&self, tip: &ChainPoint) -> bool {
        reading(&self.standing_slot).is_some_and(|slot| slot >= tip.slot())
    }

    /// The tip the peer reported when chain-sync first reached it; none until
    /// then.
    pub fn target(&self) -> Option<ChainPoint> {
        self.target.borrow().clone()
    }

    /// Whether the WAL and the state have committed the target.
    pub fn caught_up(&self) -> bool {
        self.caught_up.load(Ordering::SeqCst)
    }

    /// Whether the ledger reached the stop epoch, which ends the sync.
    pub fn stop_epoch_reached(&self) -> bool {
        self.stop_epoch_reached.load(Ordering::SeqCst)
    }

    /// Whether the sync ended as `limit` asks: a stop epoch ends either
    /// limit, a committed target only a catch-up.
    pub fn succeeded(&self, limit: SyncLimit) -> bool {
        self.stop_epoch_reached() || (limit == SyncLimit::CatchUp && self.caught_up())
    }

    /// Why a stage gave up on the upstream peer, if one did: the first to.
    pub fn upstream_failure(&self) -> Option<&UpstreamFailure> {
        self.upstream_failure.get()
    }

    /// Fix the target at `tip`; only the first call counts, so a tip that
    /// moves later leaves it where it is.
    fn fix_target(&self, tip: ChainPoint) -> bool {
        self.target.send_if_modified(|target| {
            if target.is_some() {
                return false;
            }

            *target = Some(tip);
            true
        })
    }

    fn watch_target(&self) -> watch::Receiver<Option<ChainPoint>> {
        self.target.subscribe()
    }

    fn mark_caught_up(&self) {
        self.caught_up.store(true, Ordering::SeqCst);
    }

    fn mark_stop_epoch_reached(&self) {
        self.stop_epoch_reached.store(true, Ordering::SeqCst);
    }

    fn record_upstream_failure(&self, failure: UpstreamFailure) {
        let _ = self.upstream_failure.set(failure);
    }

    /// A pull session delivered a block or reached the tip: the peer serves
    /// the sync.
    fn note_delivery(&self) {
        self.deliveries.fetch_add(1, Ordering::SeqCst);
        *self.last_delivery.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        self.note_heard();
    }

    /// When a pull session last delivered something; none before the first.
    pub fn last_delivery(&self) -> Option<Instant> {
        *self.last_delivery.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn deliveries(&self) -> u64 {
        self.deliveries.load(Ordering::SeqCst)
    }
}

/// Whether the state cursor stands on `target`. Origin is reached while the
/// state holds no block.
fn state_on(target: &ChainPoint, state: Option<&ChainPoint>) -> bool {
    match target {
        ChainPoint::Origin => matches!(state, None | Some(ChainPoint::Origin)),
        target => state == Some(target),
    }
}

/// Whether the stores have committed `target` and nothing past it: the state
/// cursor stands on it, and the WAL holds it with no entry at a later slot.
/// The WAL's last entry need not be the target: a Byron epoch boundary block
/// shares its slot with the block after it, and can sort after it. A rollback
/// below the target moves the stores off it, and the wait goes on from there.
fn committed(
    target: &ChainPoint,
    state: Option<&ChainPoint>,
    wal_tip: Option<&ChainPoint>,
    wal_holds_target: bool,
) -> bool {
    if !state_on(target, state) {
        return false;
    }

    match target {
        ChainPoint::Origin => matches!(wal_tip, None | Some(ChainPoint::Origin)),
        target => wal_holds_target && wal_tip.is_some_and(|tip| tip.slot() == target.slot()),
    }
}

/// A sync's stages, the progress they share, and the gauges over it, which
/// report while they are kept.
pub struct Pipeline {
    pub tethers: Vec<gasket::runtime::Tether>,
    pub progress: Arc<SyncProgress>,
    pub gauges: metrics::SyncGauges,
}

fn define_gasket_policy(config: &RetryConfig) -> gasket::runtime::Policy {
    let retries = gasket::retries::Policy {
        max_retries: config.max_retries,
        backoff_unit: Duration::from_secs(config.backoff_unit_seconds),
        backoff_factor: config.backoff_factor,
        max_backoff: Duration::from_secs(config.max_backoff_seconds),
        dismissible: config.dismissible,
    };

    gasket::runtime::Policy {
        // TODO: we skip checking timeouts to avoid stalling the pipeline on slow work units. The
        // long-term solution is to scope work units to fit within a particular quota. (#1041)
        tick_timeout: None,
        bootstrap_retry: retries.clone(),
        work_retry: retries.clone(),
        teardown_retry: retries.clone(),
    }
}

/// The configured peer, or the network's default one.
pub fn peer_address(config: &RootConfig, network: &ResolvedNetwork) -> Result<String, Error> {
    let configured = match &config.upstream {
        Some(UpstreamConfig::Peer(PeerConfig { peer_address })) => peer_address.clone(),
        _ => None,
    };

    configured
        .or_else(|| network.default_peer().map(str::to_string))
        .ok_or_else(|| Error::ConfigError("`upstream.peer_address` is not set".into()))
}

/// The sync `config` sets up, which flushes `export` before it ends on its
/// own.
pub fn pipeline(
    config: &RootConfig,
    limit: SyncLimit,
    domain: DomainAdapter,
    export: metrics::MetricsFlush,
) -> Result<Pipeline, Error> {
    let network = ResolvedNetwork::resolve(&config.chain)
        .map_err(|error| Error::ConfigError(error.to_string()))?;

    match &config.upstream {
        Some(UpstreamConfig::Emulator(cfg)) => devnet(cfg, domain, &config.sync.retry, export),
        _ => sync(
            &config.sync,
            network.magic(),
            peer_address(config, &network)?,
            limit,
            domain,
            export,
        ),
    }
}

pub fn sync(
    config: &SyncConfig,
    network_magic: u64,
    peer_address: String,
    limit: SyncLimit,
    domain: DomainAdapter,
    export: metrics::MetricsFlush,
) -> Result<Pipeline, Error> {
    let progress = Arc::new(SyncProgress::over(&domain)?);

    let mut pull = pull::Stage::new(
        config,
        peer_address.clone(),
        network_magic,
        limit,
        progress.clone(),
        domain.wal().clone(),
    );

    let mut apply = apply::Stage::new(
        domain.clone(),
        limit,
        progress.clone(),
        HOUSEKEEPING_INTERVAL,
    )
    .with_final_export(export);

    let (to_apply, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_apply);
    apply.upstream.connect(from_pull);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(&config.retry);

    let mut tethers = vec![
        gasket::runtime::spawn_stage(pull, policy.clone()),
        gasket::runtime::spawn_stage(apply, policy.clone()),
    ];

    // A catch-up submits no transactions: the submit stage keeps its own
    // peer session, whose end would end the catch-up while the apply stage
    // still drains toward the target.
    if limit == SyncLimit::Follow {
        let submit = submit::Stage::new(
            peer_address,
            network_magic,
            domain.mempool().clone(),
            &config.retry,
            progress.clone(),
        );
        tethers.push(gasket::runtime::spawn_stage(submit, policy.clone()));
    }

    let gauges = metrics::SyncGauges::over(&progress, &domain);

    Ok(Pipeline {
        tethers,
        progress,
        gauges,
    })
}

/// The emulator produces the chain itself, so there is no upstream tip to
/// catch up to: the pipeline follows it, and only a stop epoch ends it.
pub fn devnet(
    emulator_cfg: &EmulatorConfig,
    domain: DomainAdapter,
    retries: &RetryConfig,
    export: metrics::MetricsFlush,
) -> Result<Pipeline, Error> {
    let progress = Arc::new(SyncProgress::emulated(&domain)?);

    let mut emulator = emulator::Stage::new(
        domain.wal().clone(),
        domain.mempool().clone(),
        emulator_cfg.block_production_interval,
        progress.clone(),
    );

    let mut apply = apply::Stage::new(
        domain.clone(),
        SyncLimit::Follow,
        progress.clone(),
        HOUSEKEEPING_INTERVAL,
    )
    .with_final_export(export);

    let (to_apply, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    emulator.downstream.connect(to_apply);
    apply.upstream.connect(from_pull);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(retries);

    let emulator = gasket::runtime::spawn_stage(emulator, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());
    let gauges = metrics::SyncGauges::over(&progress, &domain);

    Ok(Pipeline {
        tethers: vec![emulator, apply],
        progress,
        gauges,
    })
}
