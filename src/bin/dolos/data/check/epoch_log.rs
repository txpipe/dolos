//! Epoch log contiguity — the archive's per-epoch `EpochState` snapshots.
//!
//! EWRAP writes one snapshot per epoch as it closes it, keyed by the slot the
//! epoch started at. Those snapshots are what every historical epoch query
//! reads, so three things have to hold: there is one per completed epoch and
//! no gaps, each one was actually closed (`end` is populated by `wrapup.flush`
//! before the commit), and value is conserved across each hand-off.
//!
//! The conservation rule is the ledger's own. ESTART computes the next
//! epoch's pots from the ending epoch's and asserts
//! `pots.is_consistent(epoch.initial_pots.max_supply())` — the total supply
//! cannot change at a boundary, only move between pots. This check asks the
//! same question of what is on disk, through the same [`Pots::is_consistent`],
//! for every logged hand-off.

use dolos_cardano::model::{EpochState, SingletonEntity as _};
use dolos_cardano::FixedNamespace as _;
use dolos_core::{ArchiveStore, LogKey, StateStore};
use miette::{Context as _, IntoDiagnostic as _};
use pallas::ledger::primitives::Epoch;

use super::{CheckKind, Issue};

const CHECK: CheckKind = CheckKind::EpochLog;

/// Verify the archive's epoch log against the live epoch.
///
/// `live` is the epoch the state store is currently in. The log covers
/// *completed* epochs, so the last logged snapshot is the one before it.
pub fn check_epoch_log<E: std::fmt::Display>(
    snapshots: impl Iterator<Item = Result<(LogKey, EpochState), E>>,
    live: Epoch,
) -> Vec<Issue> {
    let mut issues = Vec::new();
    let mut previous: Option<EpochState> = None;

    for record in snapshots {
        let snapshot = match record {
            Ok((_, snapshot)) => snapshot,
            Err(err) => {
                issues.push(Issue::new(
                    CHECK,
                    format!("an entry of the `epochs` log does not decode: {err}"),
                ));
                continue;
            }
        };

        // Only a *completed* epoch owes end stats. The live epoch's own
        // snapshot may be present and open — the harness seeds one so
        // historical queries have something to read, and a node interrupted
        // between EWRAP's archive commit and ESTART's state commit leaves one
        // too.
        if snapshot.number < live && snapshot.end.is_none() {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "the logged snapshot for epoch {} carries no end stats; the epoch was \
                     archived before EWRAP closed it",
                    snapshot.number
                ),
            ));
        }

        if snapshot.number > live {
            issues.push(Issue::new(
                CHECK,
                format!(
                    "the `epochs` log holds a snapshot for epoch {} but the state store is in \
                     epoch {live}; no boundary can have written it",
                    snapshot.number
                ),
            ));
        }

        if let Some(previous) = &previous {
            if snapshot.number != previous.number + 1 {
                issues.push(Issue::new(
                    CHECK,
                    format!(
                        "the `epochs` log jumps from epoch {} to epoch {}; {} snapshot(s) are \
                         missing",
                        previous.number,
                        snapshot.number,
                        snapshot.number.saturating_sub(previous.number + 1),
                    ),
                ));
            }

            // Total supply is invariant across a boundary — ESTART only moves
            // value between pots. A hand-off that does not conserve it means
            // one of the two snapshots is not what the ledger produced.
            let expected = previous.initial_pots.max_supply();

            if !snapshot.initial_pots.is_consistent(expected) {
                issues.push(Issue::new(
                    CHECK,
                    format!(
                        "the hand-off from epoch {} to epoch {} does not conserve supply: {} \
                         lovelace before, {} after",
                        previous.number,
                        snapshot.number,
                        expected,
                        snapshot.initial_pots.max_supply(),
                    ),
                ));
            }
        }

        previous = Some(snapshot);
    }

    match previous {
        // An empty log is only coherent before the first boundary.
        None if live > 0 => issues.push(Issue::new(
            CHECK,
            format!("the state store is in epoch {live} but the `epochs` log is empty"),
        )),
        // The log ends at the last *completed* epoch, or at the live one when
        // its snapshot is already on disk (see the `end` rule above). Any
        // shorter and completed epochs went unrecorded.
        Some(last) if last.number + 1 < live => issues.push(Issue::new(
            CHECK,
            format!(
                "the `epochs` log ends at epoch {} but the state store is in epoch {live}; the \
                 log should hold one snapshot per completed epoch",
                last.number
            ),
        )),
        _ => (),
    }

    issues
}

pub fn run(stores: &crate::common::Stores) -> miette::Result<Vec<Issue>> {
    let epoch = stores
        .state
        .read_entity_typed::<EpochState>(EpochState::NS, &EpochState::singleton_key())
        .into_diagnostic()
        .context("reading the live epoch state")?;

    let Some(epoch) = epoch else {
        // Nothing to compare the log against; `cursors` reports a state store
        // that was never bootstrapped.
        return Ok(Vec::new());
    };

    let snapshots = stores
        .archive
        .iter_logs_typed::<EpochState>(EpochState::NS, None)
        .into_diagnostic()
        .context("iterating the `epochs` log")?;

    Ok(check_epoch_log(snapshots, epoch.number))
}

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_cardano::model::EndStats;
    use dolos_cardano::pots::Pots;
    use dolos_core::TemporalKey;

    const MAX_SUPPLY: u64 = 45_000_000_000_000_000;

    fn snapshot(number: Epoch, utxos: u64) -> (LogKey, EpochState) {
        let state = EpochState {
            number,
            end: Some(EndStats::default()),
            initial_pots: Pots {
                utxos,
                reserves: MAX_SUPPLY - utxos,
                ..Pots::default()
            },
            ..EpochState::default()
        };

        (LogKey::from(TemporalKey::from(number)), state)
    }

    fn log(
        entries: Vec<(LogKey, EpochState)>,
    ) -> impl Iterator<Item = Result<(LogKey, EpochState), String>> {
        entries.into_iter().map(Ok)
    }

    #[test]
    fn a_contiguous_closed_log_passes() {
        let entries = vec![snapshot(0, 100), snapshot(1, 200), snapshot(2, 300)];

        let issues = check_epoch_log(log(entries), 3);

        assert!(issues.is_empty(), "{issues:?}");
    }

    #[test]
    fn an_empty_log_at_genesis_passes() {
        let issues = check_epoch_log(log(vec![]), 0);
        assert!(issues.is_empty(), "{issues:?}");
    }

    /// The corruption fixture: a snapshot missing from the middle of the log.
    #[test]
    fn a_gap_is_reported() {
        let entries = vec![snapshot(0, 100), snapshot(2, 300)];

        let issues = check_epoch_log(log(entries), 3);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert_eq!(issues[0].check, CheckKind::EpochLog);
        assert!(issues[0].detail.contains("jumps from epoch 0 to epoch 2"));
        assert!(issues[0].detail.contains("1 snapshot(s) are missing"));
    }

    #[test]
    fn an_unclosed_snapshot_is_reported() {
        let mut entries = vec![snapshot(0, 100), snapshot(1, 200)];
        entries[1].1.end = None;

        let issues = check_epoch_log(log(entries), 2);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("carries no end stats"));
    }

    /// The live epoch's own snapshot is allowed to be open: the boundary that
    /// closes it has not run. The harness seeds exactly this shape, and so
    /// does a node interrupted between EWRAP's archive commit and ESTART's
    /// state commit.
    #[test]
    fn the_live_epochs_open_snapshot_is_tolerated() {
        let mut entries = vec![snapshot(0, 100), snapshot(1, 200)];
        entries[1].1.end = None;

        let issues = check_epoch_log(log(entries), 1);

        assert!(issues.is_empty(), "{issues:?}");
    }

    /// A snapshot the live state store cannot have produced.
    #[test]
    fn a_snapshot_above_the_live_epoch_is_reported() {
        let entries = vec![snapshot(0, 100), snapshot(1, 200)];

        let issues = check_epoch_log(log(entries), 0);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("snapshot for epoch 1"));
    }

    /// A hand-off that invents or destroys lovelace: the UTxO pot grows
    /// without anything else shrinking.
    #[test]
    fn a_non_conserving_handoff_is_reported() {
        let mut entries = vec![snapshot(0, 100), snapshot(1, 200)];
        entries[1].1.initial_pots.utxos += 7;

        let issues = check_epoch_log(log(entries), 2);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("does not conserve supply"));
    }

    #[test]
    fn a_log_that_lags_the_live_epoch_is_reported() {
        let entries = vec![snapshot(0, 100), snapshot(1, 200)];

        let issues = check_epoch_log(log(entries), 9);

        assert_eq!(issues.len(), 1, "{issues:?}");
        assert!(issues[0].detail.contains("ends at epoch 1"));
        assert!(issues[0].detail.contains("epoch 9"));
    }
}
