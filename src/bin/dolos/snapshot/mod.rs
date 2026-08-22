//! Publishing this node's data as a Stelae snapshot.
//!
//! Dolos's own word is "snapshot"; the protocol's is "stele". The translation
//! happens here and nowhere else — see `solution/stelae` and
//! `adrs/004_stelae_snapshots.md`.
//!
//! `publish` writes one; `digest` says what one *would* be; `verify` checks a
//! published one, digests only; `inspect` reads one's table of contents
//! without pulling a layer. `sign` is the rest of the
//! publisher-productization slice, and restore is its own.
//!
//! Publishing into a registry is behind the `registry` feature, which is
//! default-off; `--repo` in a build without it is a refusal naming the feature.
//!
//! ## One epoch selection, however many commands take one
//!
//! [`EpochRange`] lives here rather than in either command, because a publisher
//! that names "epochs 500 through 519" to one and gets a different window from
//! the other is a publisher verifying a different stele than the one they
//! published — and being told it matches. One parser, one restriction, applied
//! by [`restrict`].

use clap::{Parser, Subcommand};
use dolos_core::config::RootConfig;
use dolos_snapshot::{
    export::{IndexBand, Plan},
    RetainedEpochs,
};
use miette::{Context as _, IntoDiagnostic as _};

use crate::feedback::Feedback;

mod digest;
mod inspect;
mod publish;
mod verify;

#[derive(Debug, Subcommand)]
pub enum Command {
    /// writes a stele to a local directory or an OCI repository
    Publish(publish::Args),

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
        Command::Digest(x) => digest::run(config, x),
        Command::Verify(x) => verify::run(config, x),
        Command::Inspect(x) => inspect::run(config, x),
    }
}

/// An epoch selection, in Rust's own range spellings.
///
/// Spelled the way a reader already knows how to read: `..` excludes its end,
/// `..=` includes it. Both are accepted because a publisher naming "epochs 500
/// through 519" and one naming "up to and including 519" are both natural, and
/// silently picking one of the two meanings is how an operator publishes an
/// epoch short.
#[derive(Debug, Clone, Copy)]
pub struct EpochRange {
    first: Option<u64>,
    last: Option<u64>,
}

impl std::str::FromStr for EpochRange {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bad = |why: &str| format!("{raw:?} is not an epoch range: {why}");

        let parse = |part: &str| -> Result<Option<u64>, String> {
            match part.trim() {
                "" => Ok(None),
                value => value
                    .parse::<u64>()
                    .map(Some)
                    .map_err(|e| bad(&format!("{value:?}: {e}"))),
            }
        };

        // `..=` first: `..` is a prefix of it, so testing in the other order
        // would read `500..=520` as a range ending at `=520`.
        let (raw, inclusive) = match raw.split_once("..=") {
            Some(_) => (raw.replacen("..=", "..", 1), true),
            None => (raw.to_owned(), false),
        };

        let Some((start, end)) = raw.split_once("..") else {
            // A bare number: exactly that epoch.
            let only = parse(&raw)?.ok_or_else(|| bad("it is empty"))?;

            return Ok(Self {
                first: Some(only),
                last: Some(only),
            });
        };

        let first = parse(start)?;
        let end = parse(end)?;

        let last = match (end, inclusive) {
            (Some(end), false) => Some(
                end.checked_sub(1)
                    .ok_or_else(|| bad("an exclusive end of 0 selects nothing"))?,
            ),
            (None, true) => return Err(bad("`..=` needs an end")),
            (end, _) => end,
        };

        if let (Some(first), Some(last)) = (first, last) {
            if first > last {
                return Err(bad("it starts after it ends"));
            }
        }

        Ok(Self { first, last })
    }
}

/// The retained state-dump epochs this node publishes under.
///
/// Read here rather than in each command for the same reason [`restrict`] is:
/// `publish`, `digest` and `verify --reproduce` all put this list in
/// `parameters`, and a node that gave them different lists would be verifying
/// a different document than the one it published — and being told it does not
/// match. An absent `[snapshot]` section means an empty list, which is a
/// publisher that retains the tip alone.
pub fn retained_epochs(config: &RootConfig) -> miette::Result<RetainedEpochs> {
    let configured = config
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.state_epochs.clone())
        .unwrap_or_default();

    RetainedEpochs::new(configured)
        .into_diagnostic()
        .context("reading snapshot.state_epochs")
}

/// Apply an operator's selection to a plan, or leave it whole.
///
/// The one place either command narrows a plan. `restrict_epochs` takes two
/// options and a caller could always inline the call; the point is that neither
/// command gets to spell the mapping from a range to those two options its own
/// way.
pub fn restrict(plan: Plan, range: Option<EpochRange>) -> Plan {
    match range {
        Some(range) => plan.restrict_epochs(range.first, range.last),
        None => plan,
    }
}

/// Apply an operator's index band to a plan, or leave the measured default.
///
/// The one place any command spells the mapping, for the reason [`restrict`] is
/// shared: `publish`, `digest` and `verify --reproduce` all pay the same index
/// traversals, and a knob one of them spelled its own way would be a knob an
/// operator has to learn three times.
///
/// Unlike [`restrict`], this changes nothing about the document: banding
/// reorders when index records are read, never which layer they land in. Two
/// runs at different bands produce the same digest, which is why a
/// reproduction is free to band differently than the publish it checks.
pub fn banded(plan: Plan, band: Option<std::num::NonZeroUsize>) -> Plan {
    match band {
        Some(band) => plan.with_band(IndexBand::new(band)),
        None => plan,
    }
}

/// The report every command opens with: where the node stands and what the
/// selection covers.
///
/// Shared so that a publisher comparing a `digest` run against the `publish`
/// that produced a stele is comparing the same four lines. Written to `stderr`,
/// because `digest` puts a document on `stdout` and a report interleaved with
/// it would not be one.
pub fn report_plan(plan: &Plan) -> miette::Result<()> {
    let tag = plan.tag().into_diagnostic()?;

    eprintln!(
        "network:  {} ({})",
        plan.network.name(),
        plan.network.magic()
    );
    eprintln!("cursor:   {}", plan.cursor);
    eprintln!("sequence: {} (tag {tag})", plan.sequence);

    // Clamped to the epochs actually selected, because the band chunks them:
    // a `--epochs 500..502` publish opens three sinks whatever the band says,
    // and the unclamped budget would overstate it by orders of magnitude.
    let band = plan.band.epochs().min(plan.epochs.len());

    eprintln!(
        "band:     {band} epochs per index traversal ({} MiB budgeted)",
        band.saturating_mul(IndexBand::SINK_BYTES) / (1024 * 1024),
    );

    match (plan.epochs.first(), plan.epochs.last()) {
        (Some(first), Some(last)) => eprintln!(
            "epochs:   {}..={} ({} of them, slots {}..={})",
            first.epoch,
            last.epoch,
            plan.epochs.len(),
            first.start_slot,
            last.end_slot,
        ),
        // The state tip alone is a legitimate publish; say so rather than
        // printing an empty range and looking like a mistake.
        _ => eprintln!("epochs:   none selected; the state tip only"),
    }

    // Printed always and not only when it is set, because an empty list is a
    // choice with consequences — it is what makes this publisher's parameters
    // differ from a co-signer's that retains dumps, and the line is where an
    // operator sees the two do not match.
    let due = plan.retained.due(plan.sequence).count();

    eprintln!(
        "dumps:    {:?} retained ({due} due at this sequence)",
        plan.retained.as_slice(),
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(raw: &str) -> (Option<u64>, Option<u64>) {
        let range: EpochRange = raw.parse().unwrap();
        (range.first, range.last)
    }

    #[test]
    fn epoch_ranges_read_the_way_rust_ranges_do() {
        assert_eq!(parse("500..520"), (Some(500), Some(519)));
        assert_eq!(parse("500..=520"), (Some(500), Some(520)));
        assert_eq!(parse("500.."), (Some(500), None));
        assert_eq!(parse("..520"), (None, Some(519)));
        assert_eq!(parse("..=520"), (None, Some(520)));
        assert_eq!(parse(".."), (None, None));
        assert_eq!(parse("500"), (Some(500), Some(500)));
    }

    #[test]
    fn a_nonsensical_range_is_refused() {
        for raw in ["520..500", "..0", "abc", "", "500..abc", "500..=", "-1"] {
            assert!(raw.parse::<EpochRange>().is_err(), "{raw:?}");
        }
    }
}
