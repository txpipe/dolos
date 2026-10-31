use dolos_cardano::networks::ResolvedNetwork;
use dolos_core::config::{
    EmulatorConfig, PeerConfig, RetryConfig, RootConfig, SyncConfig, UpstreamConfig,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;

use crate::adapters::DomainAdapter;
use crate::prelude::*;

pub mod apply;
pub mod emulator;
pub mod pull;
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
    CatchUp,
}

/// What a running sync has reached, shared by its stages and read by
/// whoever runs it.
#[derive(Debug)]
pub struct SyncProgress {
    target: watch::Sender<Option<ChainPoint>>,
    caught_up: AtomicBool,
    stop_epoch_reached: AtomicBool,
}

impl Default for SyncProgress {
    fn default() -> Self {
        Self {
            target: watch::Sender::new(None),
            caught_up: AtomicBool::new(false),
            stop_epoch_reached: AtomicBool::new(false),
        }
    }
}

impl SyncProgress {
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
}

/// Whether the WAL tip and the state cursor both stand on `target`. A
/// rollback below the target moves them off it, and the wait goes on from
/// there. Origin is reached while neither store holds a block.
fn committed(
    target: &ChainPoint,
    wal_tip: Option<&ChainPoint>,
    state: Option<&ChainPoint>,
) -> bool {
    let at = |point: Option<&ChainPoint>| match target {
        ChainPoint::Origin => matches!(point, None | Some(ChainPoint::Origin)),
        target => point == Some(target),
    };

    at(wal_tip) && at(state)
}

/// A sync's stages, and the progress they share.
pub struct Pipeline {
    pub tethers: Vec<gasket::runtime::Tether>,
    pub progress: Arc<SyncProgress>,
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

pub fn pipeline(
    config: &RootConfig,
    limit: SyncLimit,
    domain: DomainAdapter,
) -> Result<Pipeline, Error> {
    let network = ResolvedNetwork::resolve(&config.chain)
        .map_err(|error| Error::ConfigError(error.to_string()))?;

    match &config.upstream {
        Some(UpstreamConfig::Emulator(_)) if limit == SyncLimit::CatchUp => Err(
            Error::ConfigError("an emulator upstream has no tip to catch up to".into()),
        ),
        Some(UpstreamConfig::Emulator(cfg)) => devnet(cfg, domain, &config.sync.retry),
        _ => sync(
            &config.sync,
            network.magic(),
            peer_address(config, &network)?,
            limit,
            domain,
        ),
    }
}

pub fn sync(
    config: &SyncConfig,
    network_magic: u64,
    peer_address: String,
    limit: SyncLimit,
    domain: DomainAdapter,
) -> Result<Pipeline, Error> {
    let progress = Arc::new(SyncProgress::default());

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
    );

    let submit = submit::Stage::new(peer_address, network_magic, domain.mempool().clone());

    let (to_apply, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    pull.downstream.connect(to_apply);
    apply.upstream.connect(from_pull);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(&config.retry);

    let pull = gasket::runtime::spawn_stage(pull, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());
    let submit = gasket::runtime::spawn_stage(submit, policy.clone());

    Ok(Pipeline {
        tethers: vec![pull, apply, submit],
        progress,
    })
}

/// The emulator produces the chain itself, so there is no upstream tip to
/// catch up to: the pipeline follows it.
pub fn devnet(
    emulator_cfg: &EmulatorConfig,
    domain: DomainAdapter,
    retries: &RetryConfig,
) -> Result<Pipeline, Error> {
    let progress = Arc::new(SyncProgress::default());

    let mut emulator = emulator::Stage::new(
        domain.wal().clone(),
        domain.mempool().clone(),
        emulator_cfg.block_production_interval,
    );

    let mut apply = apply::Stage::new(
        domain.clone(),
        SyncLimit::Follow,
        progress.clone(),
        HOUSEKEEPING_INTERVAL,
    );

    let (to_apply, from_pull) = gasket::messaging::tokio::mpsc_channel(50);
    emulator.downstream.connect(to_apply);
    apply.upstream.connect(from_pull);

    // output to outside of out pipeline
    // apply.downstream.connect(output);

    let policy = define_gasket_policy(retries);

    let emulator = gasket::runtime::spawn_stage(emulator, policy.clone());
    let apply = gasket::runtime::spawn_stage(apply, policy.clone());

    Ok(Pipeline {
        tethers: vec![emulator, apply],
        progress,
    })
}
