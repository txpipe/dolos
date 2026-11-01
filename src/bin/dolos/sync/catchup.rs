//! `sync catchup`: pull the chain from the upstream peer until the WAL and the
//! state commit the tip it reported on reaching it, or on with `--follow`.

use std::time::Duration;

use dolos::adapters::DomainAdapter;
use dolos::prelude::*;
use dolos::sync::{SyncLimit, SyncProgress};
use dolos_core::config::{RootConfig, UpstreamConfig};
use gasket::daemon::Daemon;
use miette::{Context, IntoDiagnostic};

use crate::output::Usage;

#[derive(Debug, clap::Args)]
pub struct Args {
    /// Keep following the chain once the upstream tip is committed
    #[arg(long)]
    follow: bool,

    /// Stop once the ledger reaches this epoch, on its first block: the only
    /// state a Stelae snapshot of that epoch is cut from
    #[arg(long, value_name = "N")]
    stop_epoch: Option<u64>,
}

impl Args {
    fn sync_limit(&self) -> SyncLimit {
        if self.follow {
            SyncLimit::Follow
        } else {
            SyncLimit::CatchUp
        }
    }

    /// What the sync ends on, said of a sync that ended without it.
    fn unmet(&self) -> &'static str {
        match (self.sync_limit(), self.stop_epoch) {
            (SyncLimit::CatchUp, None) => "before the stores committed the catch-up target",
            (SyncLimit::CatchUp, Some(_)) => {
                "before the stores committed the catch-up target or the ledger reached the stop epoch"
            }
            (SyncLimit::Follow, None) => "while following the chain",
            (SyncLimit::Follow, Some(_)) => "before the ledger reached the stop epoch",
        }
    }
}

/// How often the pipeline is checked on.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let limit = args.sync_limit();

    if limit == SyncLimit::CatchUp
        && args.stop_epoch.is_none()
        && matches!(config.upstream, Some(UpstreamConfig::Emulator(_)))
    {
        return Err(Usage::new(
            "an emulator upstream has no tip to catch up to",
            "pass `--follow` or `--stop-epoch` to sync from it",
        )
        .into());
    }

    crate::common::setup_tracing(&config.logging, config.telemetry.as_ref())?;

    let domain = crate::common::setup_domain_with_stop_epoch(config, args.stop_epoch)?;

    let sync = dolos::sync::pipeline(config, limit, domain.clone())
        .into_diagnostic()
        .context("bootstrapping sync pipeline")?;

    let progress = sync.progress;
    let daemon = Daemon::new(sync.tethers);

    let interrupted = wait(&daemon, &progress);
    daemon.teardown();

    eprintln!("committed: {}", committed(&domain));

    if progress.succeeded(limit) {
        return Ok(());
    }

    if interrupted {
        return Err(miette::miette!("interrupted {}", args.unmet()));
    }

    match progress.upstream_failure() {
        Some(failure) => Err(miette::miette!(
            help = "`[sync.retry]` sets how many failed attempts in a row are retried, \
                    and the backoff between them",
            "the sync ended {}: {failure}",
            args.unmet()
        )),
        None => Err(miette::miette!(
            help = "the log above names the stage that failed",
            "the sync ended {}",
            args.unmet()
        )),
    }
}

/// Wait for the pipeline to end, saying when the target is fixed and when it
/// is committed. Returns whether a signal ended it.
fn wait(daemon: &Daemon, progress: &SyncProgress) -> bool {
    let mut said = Said::default();

    loop {
        said.update(progress);

        if daemon.is_terminated() {
            return true;
        }

        if daemon.has_ended() {
            said.update(progress);
            return false;
        }

        std::thread::sleep(POLL_INTERVAL);
    }
}

/// What has been said about the target so far.
#[derive(Default)]
struct Said {
    target: bool,
    caught_up: bool,
}

impl Said {
    fn update(&mut self, progress: &SyncProgress) {
        if !self.target {
            if let Some(target) = progress.target() {
                eprintln!("target: {}", describe(&target));
                self.target = true;
            }
        }

        if self.target && !self.caught_up && progress.caught_up() {
            eprintln!("target committed");
            self.caught_up = true;
        }
    }
}

/// Where the state stands, and the WAL tip beside it when that differs.
fn committed(domain: &DomainAdapter) -> String {
    let state = domain.state().read_cursor();
    let wal = domain
        .wal()
        .find_tip()
        .map(|tip| tip.map(|(point, _)| point));

    match (state, wal) {
        (Ok(state), Ok(wal)) if state == wal => describe_cursor(state.as_ref()),
        (Ok(state), Ok(wal)) => format!(
            "{}; the WAL at {}",
            describe_cursor(state.as_ref()),
            describe_cursor(wal.as_ref())
        ),
        (Err(error), _) => format!("unknown, reading the state cursor failed: {error}"),
        (_, Err(error)) => format!("unknown, reading the WAL tip failed: {error}"),
    }
}

fn describe_cursor(point: Option<&ChainPoint>) -> String {
    point.map_or_else(|| "nothing".into(), describe)
}

fn describe(point: &ChainPoint) -> String {
    match point {
        ChainPoint::Origin => "Origin".into(),
        ChainPoint::Slot(slot) => format!("slot {slot}"),
        ChainPoint::Specific(slot, hash) => format!("slot {slot}, block {hash}"),
    }
}
