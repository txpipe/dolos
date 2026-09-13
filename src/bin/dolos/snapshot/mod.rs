//! Inspecting and reproducing this node's Stelae snapshot profile.
//!
//! Dolos's own word is "snapshot"; the protocol's is "stele". The translation
//! happens here and nowhere else — see `crates/snapshot/PROFILE.md` and
//! `adrs/004_stelae_snapshots.md`.
//!
//! `digest` says what one *would* be; `verify` checks a published one and can
//! reproduce it; `inspect` reads one's table of contents without pulling a
//! layer. Publisher commands live in the Stelae publisher application, and
//! restore remains under `dolos bootstrap stelae`.
//!
//! ## One epoch selection, however many commands take one
//!
//! [`EpochRange`] is the profile crate's rather than either command's, because
//! digest and `verify --reproduce` must select the same records. One parser,
//! one restriction, one reading of the plan they produce:
//! [`dolos_snapshot::planning`].

use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;
use dolos_snapshot::{
    export::Plan,
    facade::{Selection, SnapshotSource as _, StoreSnapshot},
    planning::{self, PlanReport},
};
use miette::{Context as _, IntoDiagnostic as _};

mod digest;
mod inspect;
mod verify;

pub use dolos_snapshot::planning::EpochRange;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// computes a stele's inscription and identity without writing one
    Digest(digest::Args),

    /// checks a published stele's digests — manifest against inscription,
    /// every blob against both of its digests, the history chain's shape —
    /// and, with --reproduce, rebuilds every layer from this node's stores
    Verify(verify::Args),

    /// prints what a published stele contains, without pulling a layer
    Inspect(inspect::Args),
}

#[derive(Debug, Parser)]
pub struct Args {
    #[command(subcommand)]
    command: Command,
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    match &args.command {
        Command::Digest(x) => digest::run(config, x),
        Command::Verify(x) => verify::run(config, x),
        Command::Inspect(x) => inspect::run(config, x),
    }
}

/// This node's plan, narrowed by the operator's selection.
///
/// One sequence for `digest` and `verify --reproduce`, because a node that gave
/// them different plans would be verifying a different document. `what` is the
/// word the failing command uses for the plan it was building.
pub fn planned(
    config: &RootConfig,
    stores: &crate::common::Stores,
    selection: &Selection,
    what: &'static str,
) -> miette::Result<Plan> {
    let genesis = crate::common::open_genesis_files(&config.genesis)?;

    let retained = planning::retained_epochs(config)
        .into_diagnostic()
        .context("reading snapshot.state_epochs")?;

    StoreSnapshot::new(&stores.archive, &stores.state)
        .selected_plan(u64::from(genesis.network_magic()), retained, *selection)
        .into_diagnostic()
        .context(what)
}

/// The report every command opens with: where the node stands and what the
/// selection covers.
///
/// The numbers are [`PlanReport`]'s, so a verifier comparing a `digest` run
/// against a published stele is comparing the same arithmetic. What is here is
/// the four lines it is said in. Written to `stderr`, because `digest` puts a
/// document on `stdout` and a report interleaved with it would not be one.
pub fn report_plan(plan: &Plan) -> miette::Result<()> {
    let report = PlanReport::read(plan).into_diagnostic()?;

    eprintln!("network:  {} ({})", report.network, report.magic);
    eprintln!("cursor:   {}", report.cursor);
    eprintln!("sequence: {} (tag {})", report.sequence, report.tag);

    eprintln!(
        "band:     {} epochs per index traversal ({} MiB budgeted)",
        report.band_epochs, report.band_budget_mib,
    );

    match report.epochs {
        Some(span) => eprintln!(
            "epochs:   {}..={} ({} of them, slots {}..={})",
            span.first, span.last, span.count, span.start_slot, span.end_slot,
        ),
        // The state tip alone is a legitimate plan; say so rather than
        // printing an empty range and looking like a mistake.
        None => eprintln!("epochs:   none selected; the state tip only"),
    }

    // Printed always and not only when it is set, because an empty list is a
    // choice with consequences — it is what can make this reproduction's
    // parameters differ from a published stele's.
    eprintln!(
        "dumps:    {:?} retained ({} due at this sequence)",
        report.retained, report.dumps_due,
    );

    Ok(())
}
