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

use std::path::Path;

use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;
use dolos_snapshot::{
    boundary::Boundary,
    export::Plan,
    facade::{Selection, SnapshotSource as _, StoreSnapshot},
    planning::{self, PlanReport},
    RetainedEpochs,
};
use miette::{Context as _, IntoDiagnostic as _};

mod digest;
mod inspect;
mod verify;

pub use dolos_snapshot::planning::EpochRange;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// computes a stele's inscription and identity without writing one, from
    /// stores standing on the first block of an epoch, as a `stop_epoch` sync
    /// leaves them
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

/// The publisher's retained state-dump epochs, from `--state-epochs` or
/// `--state-epochs-file`, read and validated.
///
/// Nothing is defaulted: naming neither is the empty list, a publisher that
/// retains the tip alone, which [`report_plan`] prints so that a mismatch
/// against a published stele is explained.
pub fn retained_epochs(listed: &[u64], file: Option<&Path>) -> miette::Result<RetainedEpochs> {
    let epochs = match file {
        Some(path) => read_state_epochs(path)?,
        None => listed.to_vec(),
    };

    planning::retained_epochs(&epochs)
        .into_diagnostic()
        .context("reading the state epochs")
}

fn read_state_epochs(path: &Path) -> miette::Result<Vec<u64>> {
    let text = std::fs::read_to_string(path)
        .into_diagnostic()
        .with_context(|| format!("reading the state epochs in {}", path.display()))?;

    parse_state_epochs(&text)
        .with_context(|| format!("reading the state epochs in {}", path.display()))
}

/// Epoch numbers separated by commas or whitespace, newlines included.
fn parse_state_epochs(text: &str) -> miette::Result<Vec<u64>> {
    text.split(|c: char| c == ',' || c.is_whitespace())
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            entry
                .parse()
                .map_err(|_| miette::miette!("{entry:?} is not an epoch number"))
        })
        .collect()
}

/// This node's plan, narrowed by the operator's selection, and the boundary
/// evidence that makes it publishable.
///
/// One sequence for `digest` and `verify --reproduce`, because a node that gave
/// them different plans would be verifying a different document. The boundary
/// is checked here, before anything slow, though the walk checks it again.
/// `what` is the word the failing command uses for the plan it was building.
pub fn planned(
    config: &RootConfig,
    stores: &crate::common::Stores,
    selection: &Selection,
    retained: RetainedEpochs,
    what: &'static str,
) -> miette::Result<(Plan, Boundary)> {
    let genesis = crate::common::open_genesis_files(&config.genesis)?;
    let snapshot = StoreSnapshot::new(&stores.archive, &stores.state);

    let plan = snapshot
        .selected_plan(u64::from(genesis.network_magic()), retained, *selection)
        .into_diagnostic()
        .context(what)?;

    let boundary = snapshot.boundary(&plan).into_diagnostic().context(what)?;

    Ok((plan, boundary))
}

/// The report every command opens with: where the node stands and what the
/// selection covers.
///
/// The numbers are [`PlanReport`]'s, so a verifier comparing a `digest` run
/// against a published stele is comparing the same arithmetic. What is here is
/// the four lines it is said in. Written to `stderr`, because `digest` puts a
/// document on `stdout` and a report interleaved with it would not be one.
pub fn report_plan(plan: &Plan, boundary: &Boundary) -> miette::Result<()> {
    let report = PlanReport::read(plan).into_diagnostic()?;

    eprintln!("network:  {} ({})", report.network, report.magic);
    eprintln!("cursor:   {}", report.cursor);
    eprintln!("sequence: {} (tag {})", report.sequence, report.tag);
    eprintln!(
        "boundary: first block of epoch {}, after {}",
        boundary.epoch, boundary.parent,
    );

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_state_epochs_file_lists_epochs_by_comma_or_line() {
        assert_eq!(
            parse_state_epochs("208, 236\n290\n\n365\n").unwrap(),
            vec![208, 236, 290, 365]
        );
        assert_eq!(parse_state_epochs("").unwrap(), Vec::<u64>::new());
        assert!(parse_state_epochs("208, epoch-236").is_err());
    }

    #[test]
    fn a_state_epochs_file_is_read_in_place_of_the_list() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs.txt");
        std::fs::write(&path, "208,236\n290\n").unwrap();

        assert_eq!(
            retained_epochs(&[], Some(&path)).unwrap().as_slice(),
            [208, 236, 290]
        );

        let missing = dir.path().join("missing.txt");
        let message = format!("{:?}", retained_epochs(&[], Some(&missing)).unwrap_err());
        assert!(message.contains("missing.txt"), "{message}");
    }

    #[test]
    fn the_listed_epochs_stand_without_a_file() {
        assert_eq!(
            retained_epochs(&[208, 236], None).unwrap().as_slice(),
            [208, 236]
        );
        assert!(retained_epochs(&[], None).unwrap().as_slice().is_empty());
    }

    /// A list the plan would refuse is refused as it is read, from either
    /// source.
    #[test]
    fn an_invalid_list_is_refused_as_it_is_read() {
        assert!(retained_epochs(&[236, 208], None).is_err());

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("epochs.txt");
        std::fs::write(&path, "208\n208\n").unwrap();

        assert!(retained_epochs(&[], Some(&path)).is_err());
    }
}
