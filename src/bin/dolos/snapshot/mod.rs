//! Publishing this node's data as a Stelae snapshot.
//!
//! Dolos's own word is "snapshot"; the protocol's is "stele". The translation
//! happens here and nowhere else — see `crates/snapshot/PROFILE.md` and
//! `adrs/004_stelae_snapshots.md`.
//!
//! `publish` writes one; `digest` says what one *would* be; `verify` checks a
//! published one, digests only; `inspect` reads one's table of contents
//! without pulling a layer. `sign` is the rest of the
//! publisher-productization slice, and restore is its own.
//!
//! Publishing into a registry is the OCI transport, behind `dolos-snapshot`'s
//! `oci` feature — which this binary's dependency enables unconditionally (root
//! `Cargo.toml`), so every shipped `dolos` has `--repo`.
//!
//! ## One epoch selection, however many commands take one
//!
//! [`EpochRange`] is the profile crate's rather than either command's, because
//! a publisher that names "epochs 500 through 519" to one and gets a different
//! window from the other is a publisher verifying a different stele than the
//! one they published — and being told it matches. One parser, one restriction,
//! one reading of the plan they produce: [`dolos_snapshot::planning`].

use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;
use dolos_snapshot::{
    export::{self, Plan},
    planning::{self, PlanReport},
};
use miette::{Context as _, IntoDiagnostic as _};

use crate::feedback::Feedback;

#[cfg(feature = "mithril")]
mod backfill;
mod digest;
mod inspect;
mod publish;
mod verify;

pub use dolos_snapshot::planning::EpochRange;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// writes a stele to a local directory or an OCI repository
    Publish(publish::Args),

    /// replays mithril history one epoch at a time, publishing a stele at
    /// each boundary into an OCI repository
    #[cfg(feature = "mithril")]
    Backfill(backfill::Args),

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

pub fn run(config: &RootConfig, args: &Args, feedback: &Feedback) -> miette::Result<()> {
    match &args.command {
        // `feedback` reaches `publish` alone: it is the only one of the four
        // that waits on a store walk and a network, and the other three are
        // over in the time it takes to print what they found.
        Command::Publish(x) => publish::run(config, x, feedback),
        #[cfg(feature = "mithril")]
        Command::Backfill(x) => backfill::run(config, x, feedback),
        Command::Digest(x) => digest::run(config, x),
        Command::Verify(x) => verify::run(config, x),
        Command::Inspect(x) => inspect::run(config, x),
    }
}

/// The three knobs every command that walks these stores takes.
///
/// Spelled per command rather than flattened into one clap group, because the
/// help text is not the same everywhere: `verify` takes all three only under
/// `--reproduce`, and says so. What is shared is what they mean, which is
/// [`dolos_snapshot::planning`]'s.
pub struct Selection {
    pub epochs: Option<EpochRange>,
    pub index_band: Option<std::num::NonZeroUsize>,
    pub producers: Option<std::num::NonZeroUsize>,
}

/// This node's plan, narrowed by the operator's selection.
///
/// One sequence for `publish`, `digest` and `verify --reproduce`, because a
/// node that gave them different plans would be verifying a different document
/// than the one it published — and being told it does not match. `what` is the
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

    let plan = export::plan(&stores.state, u64::from(genesis.network_magic()), retained)
        .into_diagnostic()
        .context(what)?;

    let plan = planning::restrict(plan, selection.epochs);
    let plan = planning::banded(plan, selection.index_band);

    Ok(planning::produced(plan, selection.producers))
}

/// The report every command opens with: where the node stands and what the
/// selection covers.
///
/// The numbers are [`PlanReport`]'s, so a publisher comparing a `digest` run
/// against the `publish` that produced a stele is comparing the same
/// arithmetic. What is here is the four lines it is said in. Written to
/// `stderr`, because `digest` puts a document on `stdout` and a report
/// interleaved with it would not be one.
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
        // The state tip alone is a legitimate publish; say so rather than
        // printing an empty range and looking like a mistake.
        None => eprintln!("epochs:   none selected; the state tip only"),
    }

    // Printed always and not only when it is set, because an empty list is a
    // choice with consequences — it is what makes this publisher's parameters
    // differ from a co-signer's that retains dumps, and the line is where an
    // operator sees the two do not match.
    eprintln!(
        "dumps:    {:?} retained ({} due at this sequence)",
        report.retained, report.dumps_due,
    );

    Ok(())
}
