use std::fmt;

use dolos::storage::Access;
use dolos_core::{
    bootstrap::{
        catch_up_state_only, catch_up_stores, recovery_plan, RecoveredStore, RecoveryPlan,
    },
    config::RootConfig,
    seed_wal_from_state, BlockSlot,
};
use miette::{Context, IntoDiagnostic};
use serde::Serialize;

use dolos::prelude::*;

use super::{applied, relative_to_wal, Positions};
use crate::output::{emit, FormatArgs, Usage};

#[derive(Debug, clap::Args)]
pub struct Args {
    /// When the WAL cannot bring the stores to its tip, reset it to the state
    /// cursor instead, losing its rollback history
    #[arg(long)]
    discard_wal: bool,

    /// Print what recovery would do, and change nothing
    #[arg(long)]
    dry_run: bool,

    #[command(flatten)]
    format: FormatArgs,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Action {
    /// Nothing in the WAL to replay.
    None,
    /// Replay the WAL into the stores behind its tip.
    Replay,
    /// Reset the WAL to the state cursor.
    DiscardWal,
    /// Seed a WAL that holds no block at the state cursor, losing nothing.
    SeedWal,
    /// Refused until `--discard-wal` allows resetting the WAL.
    NeedsDiscard,
    /// Refused, `--discard-wal` or not.
    Refuse,
}

/// What `sync recover` does to the stores as they stand, and why.
#[derive(Debug, Serialize)]
struct Recovery {
    #[serde(flatten)]
    plan: RecoveryPlan,
    action: Action,
    /// What the WAL lacks to bring the stores to its tip.
    gap: Option<String>,
    /// Why recovery refuses even with `--discard-wal`.
    refusal: Option<String>,
    /// What still disagrees once the action is done.
    remains: Vec<String>,
}

impl Recovery {
    /// Plan the recovery of the stores at `positions`, as `plan` replays
    /// them; `discard_wal` allows resetting the WAL.
    fn decide(mut plan: RecoveryPlan, positions: &Positions, discard_wal: bool) -> Self {
        // An archive the dataset does not keep reads empty, so the plan always
        // shows an archive step: there is nothing to replay into.
        if !positions.archive_kept {
            plan.steps
                .retain(|step| step.store != RecoveredStore::Archive);
        }

        // An emptied rolling archive is whole again with whatever blocks reach
        // it next, so replaying the WAL's blocks refills it; with none to
        // replay, it waits for a sync.
        if positions.archive_refills() {
            plan.steps
                .retain(|step| step.store != RecoveredStore::Archive || step.entries > 0);

            for step in &mut plan.steps {
                step.covered |= step.store == RecoveredStore::Archive;
            }
        }

        plan.covers_lag = plan.steps.iter().all(|step| step.covered);

        // A WAL that holds no block, empty or Origin alone, has no rollback
        // history to lose.
        let seed = positions.wal().is_none() && positions.state().is_some();

        let gap = if seed { None } else { gap(&plan, positions) };

        let (action, refusal) = match &gap {
            None if seed => match discard_refusal(positions) {
                Some(refusal) => (Action::Refuse, Some(refusal)),
                None => (Action::SeedWal, None),
            },
            None if plan.steps.is_empty() => (Action::None, None),
            None => (Action::Replay, None),
            Some(_) => match discard_refusal(positions) {
                Some(refusal) => (Action::Refuse, Some(refusal)),
                None if discard_wal => (Action::DiscardWal, None),
                None => (Action::NeedsDiscard, None),
            },
        };

        // Replaying or reseeding brings the state to the WAL tip, and the
        // archive too when the plan replays into it; any other archive stays
        // where it is.
        let remains = match action {
            Action::None | Action::Replay | Action::DiscardWal | Action::SeedWal => {
                let target = match action {
                    Action::DiscardWal | Action::SeedWal => positions.state(),
                    _ => positions.wal(),
                };

                let replayed = action == Action::Replay
                    && plan
                        .steps
                        .iter()
                        .any(|step| step.store == RecoveredStore::Archive);

                let archive = if replayed { target } else { positions.archive };

                if positions.archive_kept && archive != target {
                    vec![relative_to_wal("archive", archive, target)]
                } else {
                    vec![]
                }
            }
            Action::NeedsDiscard | Action::Refuse => positions.disagreements(),
        };

        Self {
            plan,
            action,
            gap,
            refusal,
            remains,
        }
    }
}

/// What the WAL lacks to bring the stores to its tip: the genesis a state
/// with no cursor never applied, the blocks between a store and a WAL that
/// starts past it, or every block a state ahead of the WAL applied.
fn gap(plan: &RecoveryPlan, positions: &Positions) -> Option<String> {
    // Genesis's effects never pass through the WAL.
    if positions.state.is_none() && positions.wal().is_some() {
        return Some(
            "the state has no cursor, so it lacks the genesis the WAL's blocks build on".into(),
        );
    }

    if positions.state() > positions.wal() {
        let ahead = relative_to_wal("state", positions.state(), positions.wal());

        // A cursor without a hash marks a slot the state reached, such as an
        // epoch boundary, not a block it applied.
        let hashless = positions
            .state
            .as_ref()
            .is_some_and(|cursor| !cursor.is_fully_defined());

        return Some(if hashless {
            format!("{ahead}, and its cursor has no block hash")
        } else {
            format!("{ahead}, so it lacks the blocks the state applied")
        });
    }

    let missing: Vec<_> = plan
        .steps
        .iter()
        .filter(|step| !step.covered)
        .map(|step| {
            let store = match step.store {
                RecoveredStore::State => "state",
                RecoveredStore::Archive => "archive",
            };

            let at = match step.from {
                Some(slot) => format!("at slot {slot}"),
                None => "empty".to_string(),
            };

            match positions.wal_history {
                Some(start) => format!(
                    "the WAL's history starts at slot {start}, past the {store} {at}, so it lacks the blocks in between"
                ),
                None => format!("the WAL lacks blocks past the {store} {at}"),
            }
        })
        .collect();

    (!missing.is_empty()).then(|| missing.join("; "))
}

/// Why resetting the WAL to the state cursor would not leave a dataset a node
/// can sync on from; none when it would.
fn discard_refusal(positions: &Positions) -> Option<String> {
    let Some(cursor) = &positions.state else {
        return Some("the state has no cursor to reset the WAL to".into());
    };

    if !cursor.is_fully_defined() {
        return Some(format!(
            "the state cursor at slot {} has no block hash to reset the WAL to",
            cursor.slot()
        ));
    }

    if positions.archive_kept
        && positions.archive < positions.state()
        && !positions.archive_refills()
    {
        let archive = match positions.archive {
            Some(slot) => format!("at slot {slot}"),
            None => "empty".to_string(),
        };

        return Some(format!(
            "the archive, {archive}, is behind the state at slot {}, and the WAL cannot fill it",
            cursor.slot()
        ));
    }

    None
}

const REFUSED_HELP: &str = "rebuild the state from the archive with `dolos data rebuild`, or replace the dataset with `dolos bootstrap --force`";

const DISCARD_HELP: &str =
    "pass `--discard-wal` to reset the WAL to the state cursor, losing its rollback history";

const SYNC_HELP: &str = "a sync with `dolos sync catchup` brings the stores together";

fn slot(slot: Option<BlockSlot>) -> String {
    slot.map_or("empty".to_string(), |slot| format!("slot {slot}"))
}

impl fmt::Display for Recovery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "wal        {}", slot(applied(self.plan.wal.as_ref())))?;
        writeln!(f, "state      {}", slot(applied(self.plan.state.as_ref())))?;
        writeln!(f, "archive    {}", slot(self.plan.archive))?;

        for step in &self.plan.steps {
            let store = match step.store {
                RecoveredStore::State => "state",
                RecoveredStore::Archive => "archive",
            };

            writeln!(
                f,
                "replay     {} block(s) into the {store}, from {} to slot {}{}",
                step.entries,
                slot(step.from),
                step.to.slot(),
                if step.covered { "" } else { ", with a gap" },
            )?;
        }

        if let Some(gap) = &self.gap {
            writeln!(f, "gap        {gap}")?;
        }

        for remaining in &self.remains {
            writeln!(f, "remains    {remaining}")?;
        }

        match self.action {
            Action::None => write!(f, "action     none: nothing in the WAL to replay"),
            Action::Replay => write!(
                f,
                "action     replay the WAL into the stores behind its tip"
            ),
            Action::DiscardWal => write!(
                f,
                "action     reset the WAL to the state cursor at {}, losing its rollback history",
                slot(applied(self.plan.state.as_ref()))
            ),
            Action::SeedWal => write!(
                f,
                "action     seed the WAL, which holds no block, at the state cursor at {}",
                slot(applied(self.plan.state.as_ref()))
            ),
            Action::NeedsDiscard => write!(f, "action     refused: {DISCARD_HELP}"),
            Action::Refuse => write!(
                f,
                "action     refused: {}",
                self.refusal.as_deref().unwrap_or_default()
            ),
        }
    }
}

pub fn run(config: &RootConfig, args: &Args) -> miette::Result<()> {
    crate::common::setup_tracing_warnings()?;

    let access = if args.dry_run {
        Access::Observe
    } else {
        Access::Mutate
    };

    let stores = crate::common::open_existing_stores(config, access)?;
    let positions = Positions::read(config, &stores)?;

    let plan = recovery_plan(&stores.wal, &stores.state, &stores.archive)
        .into_diagnostic()
        .context("planning the recovery")?;

    let recovery = Recovery::decide(plan, &positions, args.discard_wal);

    emit(&recovery, &args.format)?;

    if args.dry_run {
        return Ok(());
    }

    match recovery.action {
        Action::None => {}
        Action::Replay => {
            let domain = crate::common::assemble_domain(config, &stores)?;

            // An archive the dataset does not keep has nothing to replay into.
            let replayed = if positions.archive_kept {
                catch_up_stores(&domain)
            } else {
                catch_up_state_only(&domain)
            };

            replayed.into_diagnostic().context("replaying the WAL")?;

            domain
                .shutdown()
                .into_diagnostic()
                .context("closing the stores")?;
        }
        Action::DiscardWal | Action::SeedWal => {
            let seeded = seed_wal_from_state(&stores.state, &stores.wal).into_diagnostic()?;

            stores.wal.shutdown().into_diagnostic()?;

            if let (Action::DiscardWal, WalSeed::Seeded(point)) = (recovery.action, seeded) {
                eprintln!(
                    "warning: the WAL now starts at the state cursor, {point}: its rollback history is lost"
                );
            }
        }
        Action::NeedsDiscard => {
            let gap = recovery.gap.unwrap_or_default();

            return Err(Usage::new(
                format!("the WAL cannot bring the stores to its tip: {gap}"),
                DISCARD_HELP,
            )
            .into());
        }
        Action::Refuse => {
            let refusal = recovery.refusal.unwrap_or_default();

            return Err(miette::miette!(help = REFUSED_HELP, "{refusal}"));
        }
    }

    for remaining in &recovery.remains {
        eprintln!("warning: {remaining}, which recovery does not change: {SYNC_HELP}");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use dolos_core::{bootstrap::RecoveryStep, ChainPoint};

    use super::super::RollbackFloor;
    use super::*;

    fn point(slot: BlockSlot) -> ChainPoint {
        ChainPoint::Specific(slot, [slot as u8 + 1; 32].into())
    }

    /// Stores with the WAL from `start` to `tip`, the state at `state` and
    /// the archive, kept, at `archive`.
    fn positions(
        start: BlockSlot,
        tip: BlockSlot,
        state: BlockSlot,
        archive: Option<BlockSlot>,
    ) -> Positions {
        Positions {
            wal_start: Some(point(start)),
            wal_tip: Some(point(tip)),
            wal_history: Some(start),
            rollback_floor: Some(RollbackFloor {
                point: point(start),
                is_block: true,
            }),
            state: Some(point(state)),
            archive,
            archive_kept: true,
            archive_rolling: false,
        }
    }

    /// The plan `recovery_plan` makes for `positions`.
    fn plan(positions: &Positions) -> RecoveryPlan {
        let tip = positions.wal_tip.clone().unwrap();
        let start = positions.wal_start.as_ref().unwrap().slot();

        let stores = [
            (RecoveredStore::State, positions.state()),
            (RecoveredStore::Archive, positions.archive),
        ];

        let steps: Vec<_> = stores
            .into_iter()
            .filter(|(_, from)| *from < Some(tip.slot()))
            .map(|(store, from)| RecoveryStep {
                store,
                from,
                to: tip.clone(),
                entries: tip.slot() - from.unwrap_or(start),
                covered: start <= from.unwrap_or(0),
            })
            .collect();

        RecoveryPlan {
            wal: Some(tip),
            state: positions.state.clone(),
            archive: positions.archive,
            covers_lag: steps.iter().all(|step| step.covered),
            steps,
        }
    }

    fn decide(positions: &Positions, discard_wal: bool) -> Recovery {
        Recovery::decide(plan(positions), positions, discard_wal)
    }

    #[test]
    fn stores_at_the_wal_tip_need_nothing() {
        let at_tip = positions(100, 200, 200, Some(200));

        let recovery = decide(&at_tip, false);
        assert_eq!(recovery.action, Action::None);
        assert!(recovery.remains.is_empty());
    }

    #[test]
    fn a_lag_the_wal_covers_is_replayed() {
        let lagging = positions(100, 200, 150, Some(120));

        let recovery = decide(&lagging, false);
        assert_eq!(recovery.action, Action::Replay);
        assert_eq!(recovery.gap, None);
        assert_eq!(recovery.plan.steps.len(), 2);
    }

    #[test]
    fn a_wal_that_starts_past_the_state_needs_discarding() {
        let short = positions(180, 200, 150, Some(150));

        let recovery = decide(&short, false);
        assert_eq!(recovery.action, Action::NeedsDiscard);
        let gap = recovery.gap.unwrap();
        assert!(gap.contains("history starts at slot 180"), "{gap}");
        assert!(gap.contains("state at slot 150"), "{gap}");

        assert_eq!(decide(&short, true).action, Action::DiscardWal);
    }

    #[test]
    fn a_state_ahead_of_the_wal_needs_discarding() {
        let ahead = positions(100, 200, 250, Some(250));

        let recovery = decide(&ahead, false);
        assert_eq!(recovery.action, Action::NeedsDiscard);
        let gap = recovery.gap.unwrap();
        assert!(gap.contains("ahead of the WAL tip"), "{gap}");
        assert!(gap.contains("lacks the blocks the state applied"), "{gap}");
    }

    /// A WAL with no block, beside the state cursor at `state`: what a WAL
    /// version wipe leaves.
    fn without_wal(state: ChainPoint, archive: Option<BlockSlot>) -> (Positions, RecoveryPlan) {
        let positions = Positions {
            wal_start: None,
            wal_tip: None,
            wal_history: None,
            rollback_floor: None,
            state: Some(state),
            archive,
            archive_kept: true,
            archive_rolling: false,
        };

        let plan = RecoveryPlan {
            wal: None,
            state: positions.state.clone(),
            archive,
            covers_lag: true,
            steps: vec![],
        };

        (positions, plan)
    }

    #[test]
    fn a_wal_without_blocks_is_seeded_without_the_flag() {
        for discard_wal in [false, true] {
            let (empty, plan) = without_wal(point(250), Some(250));

            let recovery = Recovery::decide(plan, &empty, discard_wal);
            assert_eq!(recovery.action, Action::SeedWal);
            assert_eq!(recovery.gap, None);
            assert!(recovery.remains.is_empty());
        }

        let (behind, plan) = without_wal(point(250), Some(200));
        let recovery = Recovery::decide(plan, &behind, true);
        assert_eq!(recovery.action, Action::Refuse);
        assert!(recovery.refusal.unwrap().contains("archive, at slot 200"));

        let (hashless, plan) = without_wal(ChainPoint::Slot(250), Some(250));
        let recovery = Recovery::decide(plan, &hashless, true);
        assert_eq!(recovery.action, Action::Refuse);
        assert!(recovery.refusal.unwrap().contains("no block hash"));
    }

    /// A cursor without a hash names no block, so the gap claims none the
    /// state applied.
    #[test]
    fn a_hashless_state_ahead_of_the_wal_claims_no_blocks() {
        let mut ahead = positions(100, 200, 250, Some(250));
        ahead.state = Some(ChainPoint::Slot(250));

        let recovery = decide(&ahead, true);
        assert_eq!(recovery.action, Action::Refuse);

        let gap = recovery.gap.unwrap();
        assert!(
            gap.contains("state, at slot 250, is ahead of the WAL tip at slot 200"),
            "{gap}"
        );
        assert!(gap.contains("no block hash"), "{gap}");
        assert!(!gap.contains("lacks"), "{gap}");
    }

    #[test]
    fn an_archive_behind_the_state_is_refused_with_the_flag() {
        let short = positions(180, 200, 200, Some(150));

        for discard_wal in [false, true] {
            let recovery = decide(&short, discard_wal);
            assert_eq!(recovery.action, Action::Refuse);
            assert!(recovery.refusal.unwrap().contains("archive, at slot 150"));
        }
    }

    #[test]
    fn an_archive_ahead_of_the_state_allows_discarding() {
        let short = positions(180, 200, 150, Some(190));

        let recovery = decide(&short, true);
        assert_eq!(recovery.action, Action::DiscardWal);
        assert_eq!(recovery.remains.len(), 1, "{:?}", recovery.remains);
    }

    #[test]
    fn a_no_op_archive_is_left_out() {
        let mut lagging = positions(100, 200, 150, None);
        lagging.archive_kept = false;

        let mut plan = plan(&lagging);
        plan.steps.push(RecoveryStep {
            store: RecoveredStore::Archive,
            from: None,
            to: point(200),
            entries: 100,
            covered: false,
        });

        let recovery = Recovery::decide(plan, &lagging, false);
        assert_eq!(recovery.action, Action::Replay);
        assert!(recovery.plan.covers_lag);
        assert_eq!(recovery.plan.steps.len(), 1);
    }

    /// A state store emptied beside a WAL from Origin, which the plan alone
    /// would replay into it.
    #[test]
    fn a_state_without_a_cursor_is_refused() {
        let mut emptied = positions(0, 200, 200, Some(200));
        emptied.state = None;

        for discard_wal in [false, true] {
            let recovery = decide(&emptied, discard_wal);
            assert_eq!(recovery.action, Action::Refuse);
            assert!(recovery.gap.unwrap().contains("genesis"));
            assert!(recovery.refusal.unwrap().contains("no cursor"));
        }
    }

    #[test]
    fn an_emptied_rolling_archive_is_refilled_from_the_wal() {
        let mut emptied = positions(100, 200, 200, None);
        emptied.archive_rolling = true;

        let recovery = decide(&emptied, false);
        assert_eq!(recovery.action, Action::Replay);
        assert!(recovery.plan.covers_lag);
        assert!(recovery.remains.is_empty(), "{:?}", recovery.remains);

        // A full archive lacks the blocks before the WAL's.
        emptied.archive_rolling = false;
        assert_eq!(decide(&emptied, true).action, Action::Refuse);

        // A rolling archive that holds blocks would keep a hole up to them.
        let mut behind = positions(180, 200, 200, Some(150));
        behind.archive_rolling = true;
        assert_eq!(decide(&behind, true).action, Action::Refuse);
    }

    /// With no block in the WAL to replay, an emptied rolling archive waits
    /// for a sync, and lets the WAL be reset or seeded meanwhile.
    #[test]
    fn an_emptied_rolling_archive_without_blocks_waits_for_a_sync() {
        let mut reset = positions(200, 200, 200, None);
        reset.archive_rolling = true;

        let recovery = decide(&reset, false);
        assert_eq!(recovery.action, Action::None);
        assert!(recovery.plan.steps.is_empty());
        assert_eq!(recovery.remains.len(), 1, "{:?}", recovery.remains);

        let (mut empty, plan) = without_wal(point(250), None);
        empty.archive_rolling = true;
        let recovery = Recovery::decide(plan, &empty, false);
        assert_eq!(recovery.action, Action::SeedWal);
        assert_eq!(recovery.remains.len(), 1, "{:?}", recovery.remains);

        let mut short = positions(180, 200, 150, None);
        short.archive_rolling = true;
        let recovery = decide(&short, true);
        assert_eq!(recovery.action, Action::DiscardWal);
        assert_eq!(recovery.remains.len(), 1, "{:?}", recovery.remains);
    }

    #[test]
    fn a_state_cursor_without_a_hash_cannot_be_reset_to() {
        let mut short = positions(180, 200, 150, Some(150));
        short.state = Some(ChainPoint::Slot(150));

        let recovery = decide(&short, true);
        assert_eq!(recovery.action, Action::Refuse);
        assert!(recovery.refusal.unwrap().contains("no block hash"));
    }

    /// A state lag beside an emptied rolling archive: the WAL brings both
    /// stores to its tip, so nothing is discarded, flag or not.
    #[test]
    fn a_state_lag_beside_an_emptied_rolling_archive_is_replayed() {
        let mut lagging = positions(100, 200, 150, None);
        lagging.archive_rolling = true;

        for discard_wal in [false, true] {
            let recovery = decide(&lagging, discard_wal);
            assert_eq!(recovery.action, Action::Replay);
            assert!(recovery.plan.covers_lag);
            assert_eq!(recovery.plan.steps.len(), 2);
            assert!(recovery.remains.is_empty(), "{:?}", recovery.remains);
        }
    }

    /// A state behind a WAL reset past it, beside an emptied rolling archive:
    /// the WAL holds no block for either store, and the state still needs the
    /// flag.
    #[test]
    fn a_state_behind_a_reset_point_beside_an_emptied_rolling_archive_needs_discarding() {
        let mut reset = positions(200, 200, 150, None);
        reset.archive_rolling = true;

        let step = |store, from| RecoveryStep {
            store,
            from,
            to: point(200),
            entries: 0,
            covered: false,
        };

        let plan = RecoveryPlan {
            wal: reset.wal_tip.clone(),
            state: reset.state.clone(),
            archive: None,
            covers_lag: false,
            steps: vec![
                step(RecoveredStore::State, Some(150)),
                step(RecoveredStore::Archive, None),
            ],
        };

        let recovery = Recovery::decide(plan.clone(), &reset, false);
        assert_eq!(recovery.action, Action::NeedsDiscard);
        assert_eq!(recovery.plan.steps.len(), 1);

        let recovery = Recovery::decide(plan, &reset, true);
        assert_eq!(recovery.action, Action::DiscardWal);
    }

    /// A WAL holding Origin alone beside a state with no cursor: genesis was
    /// interrupted between its two commits, and a boot runs it again.
    #[test]
    fn an_interrupted_genesis_needs_nothing() {
        let interrupted = Positions {
            wal_start: Some(ChainPoint::Origin),
            wal_tip: Some(ChainPoint::Origin),
            wal_history: None,
            rollback_floor: None,
            state: None,
            archive: None,
            archive_kept: true,
            archive_rolling: false,
        };

        let plan = RecoveryPlan {
            wal: Some(ChainPoint::Origin),
            state: None,
            archive: None,
            covers_lag: true,
            steps: vec![],
        };

        for discard_wal in [false, true] {
            let recovery = Recovery::decide(plan.clone(), &interrupted, discard_wal);
            assert_eq!(recovery.action, Action::None);
            assert_eq!(recovery.gap, None);
            assert!(recovery.remains.is_empty(), "{:?}", recovery.remains);
        }
    }
}
