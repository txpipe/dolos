//! One producer pool, and one reading of the plan a command walks.
//!
//! `publish`, `digest` and `verify --reproduce` all build a plan from the same
//! stores and report it in the same numbers. A publisher comparing a `digest`
//! run against the `publish` that produced a stele has to be reading the same
//! arithmetic, so the knob and the report live here, once, and a command
//! spells neither of them again.

use dolos_core::BlockSlot;

use crate::{
    export::{Plan, Producers},
    Error,
};

/// Apply an operator's producer pool to a plan, or leave the sized default.
///
/// The same three commands pay the same store walks, so the knob that pools
/// them is spelled once. It changes nothing about the document — layers are
/// reassembled by their position in it, never by completion order — so a
/// reproduction is free to pool differently than the publish it checks.
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
    /// How many epochs the plan covers.
    pub count: usize,
    pub start_slot: BlockSlot,
    pub end_slot: BlockSlot,
}

/// Where the node stands and what the plan covers, as numbers.
///
/// Derived here rather than in each command so that a publisher comparing a
/// `digest` run against the `publish` that produced a stele is comparing the
/// same arithmetic — the dump count above all, which is easy to spell two ways
/// and impossible to notice when two commands disagree.
/// Composing the sentences is the command's.
#[derive(Debug, Clone)]
pub struct PlanReport {
    pub network: String,
    pub magic: u64,
    pub cursor: String,
    pub sequence: u64,
    /// The tag the sequence renders as, which is the protocol's and can refuse.
    pub tag: String,
    /// `None` when the plan covers no epoch.
    pub epochs: Option<EpochSpan>,
}

impl PlanReport {
    pub fn read(plan: &Plan) -> Result<Self, Error> {
        Ok(Self {
            network: plan.network.name().to_owned(),
            magic: plan.network.magic(),
            cursor: plan.cursor.to_string(),
            sequence: plan.sequence,
            tag: plan.tag()?,
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
        })
    }
}
