//! One epoch selection, and one reading of the plan it produces.
//!
//! `publish`, `digest` and `verify --reproduce` all build a plan from the same
//! stores and all narrow it by the same knobs. A publisher that named "epochs
//! 500 through 519" to one and got a different window from another would be
//! verifying a different document than the one it published — and being told it
//! matches. So the parse, the modifiers and the arithmetic a command reports
//! live here, once, and a command spells none of them again.

use dolos_core::{config::RootConfig, BlockSlot};

use crate::{
    export::{IndexBand, Plan, Producers},
    Error, RetainedEpochs,
};

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

impl EpochRange {
    /// The first epoch selected, or `None` for "from the beginning".
    pub fn first(&self) -> Option<u64> {
        self.first
    }

    /// The last epoch selected, inclusive, or `None` for "to the cursor".
    pub fn last(&self) -> Option<u64> {
        self.last
    }
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

/// The retained state-dump epochs a node publishes under.
///
/// Read here rather than in each command for the same reason [`restrict`] is:
/// `publish`, `digest` and `verify --reproduce` all put this list in
/// `parameters`, and a node that gave them different lists would be verifying
/// a different document than the one it published — and being told it does not
/// match. An absent `[snapshot]` section means an empty list, which is a
/// publisher that retains the tip alone.
pub fn retained_epochs(config: &RootConfig) -> Result<RetainedEpochs, Error> {
    let configured = config
        .snapshot
        .as_ref()
        .map(|snapshot| snapshot.state_epochs.clone())
        .unwrap_or_default();

    RetainedEpochs::new(configured)
}

/// Apply an operator's selection to a plan, or leave it whole.
///
/// The one place any command narrows a plan. `restrict_epochs` takes two
/// options and a caller could always inline the call; the point is that no
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

/// Apply an operator's producer pool to a plan, or leave the sized default.
///
/// Shared for the reason [`banded`] is: the same three commands pay the same
/// store walks, so the knob that pools them is spelled once. Like the band,
/// this changes nothing about the document — layers are reassembled by their
/// position in it, never by completion order — so a reproduction is free to
/// pool differently than the publish it checks.
pub fn produced(plan: Plan, producers: Option<std::num::NonZeroUsize>) -> Plan {
    match producers {
        Some(producers) => plan.with_producers(Producers::new(producers)),
        None => plan,
    }
}

/// The epochs a plan covers, once there is at least one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EpochSpan {
    pub first: u64,
    pub last: u64,
    /// How many epochs are selected, which is not `last - first + 1`: a
    /// restriction can leave gaps the store never held.
    pub count: usize,
    pub start_slot: BlockSlot,
    pub end_slot: BlockSlot,
}

/// Where the node stands and what the selection covers, as numbers.
///
/// Derived here rather than in each command so that a publisher comparing a
/// `digest` run against the `publish` that produced a stele is comparing the
/// same arithmetic — the band clamp and the dump count above all, both of which
/// are easy to spell two ways and impossible to notice when they disagree.
/// Composing the sentences is the command's.
#[derive(Debug, Clone)]
pub struct PlanReport {
    pub network: String,
    pub magic: u64,
    pub cursor: String,
    pub sequence: u64,
    /// The tag the sequence renders as, which is the protocol's and can refuse.
    pub tag: String,
    /// Epochs per index traversal, **clamped to the epochs actually selected**:
    /// the band chunks them, so a `--epochs 500..502` publish opens three sinks
    /// whatever the band says, and the unclamped budget would overstate it by
    /// orders of magnitude.
    pub band_epochs: usize,
    /// What those sinks are budgeted, in MiB.
    pub band_budget_mib: usize,
    /// `None` when the state tip alone is selected, which is a legitimate
    /// publish rather than a mistake.
    pub epochs: Option<EpochSpan>,
    /// The dump epochs this publisher retains. An empty list is a choice with
    /// consequences — it is what makes this publisher's parameters differ from
    /// a co-signer's that retains dumps — so it is reported either way.
    pub retained: Vec<u64>,
    /// How many of them fall due at this sequence.
    pub dumps_due: usize,
}

impl PlanReport {
    pub fn read(plan: &Plan) -> Result<Self, Error> {
        let band = plan.band.epochs().min(plan.epochs.len());

        Ok(Self {
            network: plan.network.name().to_owned(),
            magic: plan.network.magic(),
            cursor: plan.cursor.to_string(),
            sequence: plan.sequence,
            tag: plan.tag()?,
            band_epochs: band,
            band_budget_mib: band.saturating_mul(IndexBand::SINK_BYTES) / (1024 * 1024),
            epochs: match (plan.epochs.first(), plan.epochs.last()) {
                (Some(first), Some(last)) => Some(EpochSpan {
                    first: first.epoch,
                    last: last.epoch,
                    count: plan.epochs.len(),
                    start_slot: first.start_slot,
                    end_slot: last.end_slot,
                }),
                _ => None,
            },
            retained: plan.retained.as_slice().to_vec(),
            dumps_due: plan.retained.due(plan.sequence).count(),
        })
    }
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
