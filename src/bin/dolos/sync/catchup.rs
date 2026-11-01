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

    /// What the sync ends on, said of a sync that ended without it. An
    /// emulator upstream has no catch-up target, only a stop epoch.
    fn unmet(&self, emulator: bool) -> &'static str {
        let target = self.sync_limit() == SyncLimit::CatchUp && !emulator;

        match (target, self.stop_epoch) {
            (true, None) => "before the stores committed the catch-up target",
            (true, Some(_)) => {
                "before the stores committed the catch-up target or the ledger reached the stop epoch"
            }
            (false, Some(_)) => "before the ledger reached the stop epoch",
            (false, None) => "while following the chain",
        }
    }
}

/// How often the pipeline is checked on.
const POLL_INTERVAL: Duration = Duration::from_millis(250);

#[tokio::main]
pub async fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    let limit = args.sync_limit();
    let emulator = matches!(config.upstream, Some(UpstreamConfig::Emulator(_)));

    if limit == SyncLimit::CatchUp && args.stop_epoch.is_none() && emulator {
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

    let end = wait(&daemon, &progress);
    daemon.teardown();

    eprintln!("committed: {}", committed(&domain));

    if progress.succeeded(limit) {
        return Ok(());
    }

    let unmet = args.unmet(emulator);

    match end {
        End::Interrupted => Err(miette::miette!("interrupted {unmet}")),
        End::Stopped(stages) => Err(crate::common::sync_failure(
            &format!("the sync ended {unmet}"),
            &progress,
            &stages,
        )),
    }
}

/// How the pipeline ended.
enum End {
    /// A signal ended it.
    Interrupted,
    /// These stages ended on their own.
    Stopped(Vec<String>),
}

/// Wait for the pipeline to end, saying when the target is fixed and when it
/// is committed.
fn wait(daemon: &Daemon, progress: &SyncProgress) -> End {
    let mut said = Said::default();

    loop {
        said.update(progress);

        if daemon.is_terminated() {
            return End::Interrupted;
        }

        if daemon.has_ended() {
            said.update(progress);
            return End::Stopped(crate::common::ended_stages(daemon));
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
