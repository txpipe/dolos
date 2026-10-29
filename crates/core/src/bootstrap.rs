//! Domain initialization and integrity checks.
//!
//! This module provides functionality for bootstrapping the domain at startup.
//! It ensures storage consistency and drains any pending initialization work
//! using the full sync lifecycle (WAL + tip notifications).

use std::sync::Arc;

use tracing::{error, info, warn};

use crate::{
    sync::drain_pending_work, ArchiveStore, ArchiveWriter as _, BlockSlot, ChainLogic, ChainPoint,
    Domain, DomainError, EntityMap, StateStore, StateWriter as _, WalStore,
};

/// Extension trait for domain bootstrapping operations.
///
/// This trait extends any `Domain` implementation with methods for
/// initializing and verifying domain integrity at startup.
pub trait BootstrapExt: Domain {
    /// Check domain integrity.
    ///
    /// Ensures WAL and archive are in sync with the state store.
    /// This should be called at startup before processing any new blocks.
    fn check_integrity(&self) -> Result<(), DomainError>;

    /// Bootstrap the domain.
    ///
    /// Performs integrity checks and drains any pending initialization work.
    /// Uses the full sync lifecycle (WAL + tip notifications) since after
    /// bootstrap the node is considered "live".
    fn bootstrap(&self) -> Result<(), DomainError>;
}

impl<D: Domain> BootstrapExt for D {
    fn check_integrity(&self) -> Result<(), DomainError> {
        check_wal_in_sync_with_state(self)?;
        check_archive_in_sync_with_state(self)?;

        Ok(())
    }

    fn bootstrap(&self) -> Result<(), DomainError> {
        self.check_integrity()?;

        catch_up_stores(self)?;

        // Drain any work that might have been defined by the initialization
        // using the sync lifecycle (full WAL + tip notifications)
        let mut chain = self.write_chain();
        drain_pending_work(&mut *chain, self)?;

        Ok(())
    }
}

/// Check that WAL is consistent with the state store.
///
/// WAL at or ahead of state is normal — `catch_up_stores` replays WAL entries
/// to bring every other store (state included) up to the WAL tip. State ahead
/// of WAL is an error that requires explicit repair via `dolos doctor
/// reset-wal`.
fn check_wal_in_sync_with_state<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let wal = domain.wal().find_tip()?.map(|(point, _)| point);
    let state = domain.state().read_cursor()?;

    match (wal, state) {
        (Some(ref wal_tip), Some(ref state)) if wal_tip.slot() >= state.slot() => {
            // WAL is at or ahead of state. Normal case (including post-crash
            // where WAL committed but state didn't). catch_up_stores handles it.
            info!(%wal_tip, %state, "WAL is in sync with state");
        }
        (Some(ref wal_tip), Some(ref state)) => {
            // State is ahead of WAL — something wrote state without WAL
            // (e.g. import mode, ESTART after OOM).
            error!(%wal_tip, %state, "state is ahead of WAL");
            return Err(DomainError::InconsistentState {
                wal: Some(wal_tip.clone()),
                state: Some(state.clone()),
            });
        }
        (None, Some(ref state)) if state.is_fully_defined() => {
            // WAL is empty but state has a fully-defined cursor — likely a
            // post-bootstrap or post-version-wipe situation. Auto-reseed the
            // WAL from the state cursor so the node can resume syncing
            // without manual intervention.
            warn!(%state, "WAL is empty but state cursor is fully defined; reseeding WAL from state");
            domain.wal().reset_to(state)?;
        }
        (None, Some(ref state)) => {
            // State cursor is partially defined (e.g. only a slot, no hash):
            // we can't safely reseed without a chain hash. Surface as an
            // error so the user can recover deliberately.
            error!(%state, "WAL is empty but state cursor is not fully defined");
            return Err(DomainError::InconsistentState {
                wal: None,
                state: Some(state.clone()),
            });
        }
        (Some(ref wal_tip), None) if wal_tip == &ChainPoint::Origin => {
            // WAL reset to Origin but no state cursor — a crash mid-genesis
            // (genesis `commit_wal` resets the WAL to Origin before
            // `commit_state` writes the cursor). A completed genesis leaves
            // the cursor at Origin, so it takes the `(Some, Some)` arm above;
            // this arm only fires when genesis was interrupted. Genesis
            // re-runs cleanly on the next block.
            info!(%wal_tip, "WAL at origin, no state cursor (interrupted genesis)");
        }
        (Some(ref wal_tip), None) => {
            error!(%wal_tip, "WAL exists but no state found");
            return Err(DomainError::InconsistentState {
                wal: Some(wal_tip.clone()),
                state: None,
            });
        }
        (None, None) => {
            // Fresh node, nothing to check.
        }
    }

    Ok(())
}

/// Check if archive is in sync with state store.
///
/// Logs warnings/errors if there's a mismatch but doesn't attempt to fix it.
fn check_archive_in_sync_with_state<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let archive = domain.archive().get_tip()?.map(|(slot, _)| slot);
    let state = domain.state().read_cursor()?.map(|x| x.slot());

    match (archive, state) {
        (Some(archive), Some(state)) => {
            if archive != state {
                error!(%archive, %state, "archive is out of sync");
            }
        }
        (None, Some(_)) => {
            warn!("archive is missing");
        }
        (Some(_), None) => {
            error!("found archive but no state");
        }
        (None, None) => (),
    }

    Ok(())
}

/// How to react when a store fails to reach the catch-up target.
enum CatchUpSeverity {
    /// Consensus-critical (state): a residual gap is unrecoverable, so abort
    /// the boot with `InconsistentState`.
    Fatal,
    /// Not consensus-critical (the archive): a residual gap is logged
    /// but the node still boots, matching `check_archive_in_sync_with_state`.
    Lenient,
}

/// Verify a store reached the catch-up `target` after replay.
///
/// `reached` is the store's position after catch-up. Comparison is by slot: a
/// `Slot`-only cursor at the target's slot (e.g. post-ESTART) counts as
/// reached even though the points differ. A store can fall short when the WAL
/// no longer holds the entries needed to replay (e.g. after `reset_to`);
/// `severity` decides whether that aborts the boot or is only flagged.
fn verify_caught_up(
    store: &str,
    reached: Option<ChainPoint>,
    target: &ChainPoint,
    severity: CatchUpSeverity,
) -> Result<(), DomainError> {
    if reached
        .as_ref()
        .is_some_and(|point| point.slot() >= target.slot())
    {
        return Ok(());
    }

    match severity {
        CatchUpSeverity::Fatal => {
            error!(?reached, %target, store, "catch-up could not reach the WAL target");
            Err(DomainError::InconsistentState {
                wal: Some(target.clone()),
                state: reached,
            })
        }
        CatchUpSeverity::Lenient => {
            error!(?reached, %target, store, "store still behind WAL target after catch-up");
            Ok(())
        }
    }
}

/// Catch up the state and archive stores by replaying WAL entries.
///
/// The WAL commits first in the work-unit lifecycle, so after a crash it is
/// the most advanced store. Every other store reconciles forward to the WAL
/// tip: state first (covering a crash between `commit_wal` and
/// `commit_state`), then the archive with its index entries (covering a crash
/// between the state commit and the archive commit). [`recovery_plan`] says
/// what this would do, without doing it.
pub fn catch_up_stores<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let target = match domain.wal().find_tip()? {
        // nothing to catch up
        None => return Ok(()),
        // Origin means no blocks have been processed yet — state and archive
        // are correctly empty, so there is nothing to replay.
        Some((ChainPoint::Origin, _)) => return Ok(()),
        Some((point, _)) => point,
    };

    catch_up_state(domain, &target)?;
    catch_up_archive(domain, &target)?;

    Ok(())
}

/// What [`catch_up_stores`] would do to the stores as they stand.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RecoveryPlan {
    /// The WAL tip, which every store catches up to.
    pub wal: Option<ChainPoint>,
    pub state: Option<ChainPoint>,
    /// The archive tip's slot.
    pub archive: Option<BlockSlot>,
    /// Whether every step is [`RecoveryStep::covered`].
    pub covers_lag: bool,
    /// The replays, in the order they run; none when every store is at the
    /// WAL tip.
    pub steps: Vec<RecoveryStep>,
}

impl RecoveryPlan {
    /// Whether the stores are already at the WAL tip.
    pub fn is_current(&self) -> bool {
        self.steps.is_empty()
    }
}

/// A store's replay of the WAL entries after its own position.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RecoveryStep {
    pub store: RecoveredStore,
    /// The slot the store stands at; `None` for a store with nothing applied.
    pub from: Option<BlockSlot>,
    pub to: ChainPoint,
    /// The WAL entries replayed, synthetic ones aside.
    pub entries: u64,
    /// Whether the WAL holds every block after `from`: it reaches back to
    /// `from`, and no `reset_to` past `from` discarded blocks. Catch-up does
    /// not check this: with real blocks after the gap it replays them and
    /// reaches the WAL tip, the gap's blocks never applied. When the tip
    /// itself is a synthetic entry it cannot reach, the state's catch-up
    /// fails, while the archive's logs it and leaves the archive behind. A
    /// no-op archive
    /// (`history.mode = "none"`) has no tip, so it always replays from
    /// nothing.
    pub covered: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveredStore {
    State,
    Archive,
}

/// Compute, reading only, what [`catch_up_stores`] would replay into the
/// state and archive stores, and whether the WAL holds what it needs to.
pub fn recovery_plan<W, S, A>(wal: &W, state: &S, archive: &A) -> Result<RecoveryPlan, DomainError>
where
    W: WalStore,
    S: StateStore,
    A: ArchiveStore,
{
    let state_cursor = state.read_cursor()?;
    let archive_tip = archive.get_tip()?.map(|(slot, _)| slot);

    let mut plan = RecoveryPlan {
        wal: None,
        state: state_cursor.clone(),
        archive: archive_tip,
        covers_lag: true,
        steps: Vec::new(),
    };

    let target = match wal.find_tip()? {
        None => return Ok(plan),
        Some((point, _)) => point,
    };

    plan.wal = Some(target.clone());

    if target == ChainPoint::Origin {
        return Ok(plan);
    }

    let wal_start = wal.find_start()?.map(|(point, _)| point.slot());

    // As `catch_up_state` reads it: Origin, or no cursor, is nothing applied.
    let state_slot = match &state_cursor {
        None | Some(ChainPoint::Origin) => None,
        Some(point) => Some(point.slot()),
    };

    let stores = [
        (RecoveredStore::State, state_slot),
        (RecoveredStore::Archive, archive_tip),
    ];

    for (store, from) in stores {
        if from.is_some_and(|slot| slot >= target.slot()) {
            continue;
        }

        // The WAL has to reach back to the store, and hold real blocks all
        // the way from it: a synthetic entry past the store is a `reset_to`
        // that discarded the blocks before it.
        let mut covered = wal_start.is_some_and(|start| start <= from.unwrap_or(0));
        let mut entries = 0;

        let start = match from {
            Some(slot) => wal.locate_point(slot)?,
            None => None,
        };

        for (point, block) in wal.iter_blocks(start, Some(target.clone()))? {
            if Some(point.slot()) <= from {
                continue;
            }

            if block.is_empty() {
                covered &= point == ChainPoint::Origin;
            } else {
                entries += 1;
            }
        }

        plan.covers_lag &= covered;

        plan.steps.push(RecoveryStep {
            store,
            from,
            to: target.clone(),
            entries,
            covered,
        });
    }

    Ok(plan)
}

/// Catch up the state store by replaying WAL entries.
///
/// A crash between `commit_wal` and `commit_state` leaves the WAL holding
/// blocks whose effects never reached the state store. Only roll work units
/// write WAL entries, and each entry fully captures its state mutation
/// (entity deltas + block + resolved inputs), so forward-replaying them here
/// is lossless. The live-UTxO tags are re-derived from the same entries and
/// land in the same commit as the UTxO set they project. Boundary work units
/// never write the WAL, so they can't leave the WAL ahead of state; recovering
/// a crash *during* a boundary is a separate concern (#1018).
fn catch_up_state<D: Domain>(domain: &D, target: &ChainPoint) -> Result<(), DomainError> {
    let state_cursor = domain.state().read_cursor()?;

    if state_cursor.as_ref() == Some(target) {
        return Ok(());
    }

    // Origin (or no cursor) means nothing has been applied yet — replay the
    // whole WAL.
    let state_slot = match &state_cursor {
        None | Some(ChainPoint::Origin) => None,
        Some(point) => Some(point.slot()),
    };

    // Find the WAL start point from the state cursor
    let start = match state_slot {
        Some(slot) => domain.wal().locate_point(slot)?,
        None => None,
    };

    let logs = domain.wal().iter_logs(start, Some(target.clone()))?;

    let mut count = 0u64;

    for (point, mut log) in logs {
        // Skip entries at or before the current state cursor
        if Some(point.slot()) <= state_slot {
            continue;
        }

        // Skip synthetic entries (from reset_to) — they carry no effects
        if log.block.is_empty() {
            continue;
        }

        let writer = domain.state().start_writer()?;

        // Forward mirror of the rollback loop in `sync.rs`: load each entity
        // at its pre-block value, apply the deltas in block order, persist.
        let mut entities = EntityMap::default();
        crate::state::apply_delta_chunk::<D>(&mut entities, domain.state(), &mut log.delta)?;
        crate::state::save_entities::<D>(&writer, &entities)?;

        let catchup = D::Chain::compute_catchup(&log.block, &log.inputs, point.clone())?;

        writer.apply_utxoset(&catchup.utxo_delta)?;
        writer.apply_utxo_tags(&catchup.utxo_index_delta)?;

        writer.set_cursor(point.clone())?;

        // Commit per entry so a later block touching the same entity reloads
        // the value this block just wrote.
        writer.commit()?;

        count += 1;
    }

    if count > 0 {
        info!(count, "state caught up from WAL");
    }

    // A WAL that claims a tip the state can't reach (e.g. entries wiped by
    // `reset_to`) is unrecoverable — fail loudly instead of leaving a silent
    // gap the upstream intersection would then skip past.
    verify_caught_up(
        "state",
        domain.state().read_cursor()?,
        target,
        CatchUpSeverity::Fatal,
    )
}

/// Catch up the archive store by replaying WAL entries.
///
/// A crash between `commit_state` and `commit_archive` leaves the WAL (and
/// the state) holding blocks the archive never saw. Each entry carries the
/// block and its resolved inputs, so the archive's index entries — the tags
/// and the exact lookups the block projects — are re-derived here and land in
/// the same commit as the block, as they do on the sync path.
fn catch_up_archive<D: Domain>(domain: &D, target: &ChainPoint) -> Result<(), DomainError> {
    let archive_tip = domain.archive().get_tip()?.map(|(slot, _)| slot);
    let target_slot = target.slot();

    if archive_tip == Some(target_slot) {
        return Ok(());
    }

    // Find the WAL start point: if archive has data, start from a point
    // corresponding to the archive tip; otherwise start from the beginning.
    let start = match archive_tip {
        Some(slot) => domain.wal().locate_point(slot)?,
        None => None,
    };

    let logs = domain.wal().iter_logs(start, Some(target.clone()))?;

    let writer = domain.archive().start_writer()?;
    let mut count = 0u64;

    for (point, log) in logs {
        // Skip the start point itself (already in archive) and anything at or before it
        if Some(point.slot()) <= archive_tip {
            continue;
        }

        // Skip synthetic entries (from reset_to) — they carry no block
        if log.block.is_empty() {
            continue;
        }

        let catchup = D::Chain::compute_catchup(&log.block, &log.inputs, point.clone())?;

        writer.apply(&point, &Arc::new(log.block))?;
        writer.apply_index(&catchup.archive_index_deltas)?;
        count += 1;
    }

    if count > 0 {
        writer.commit()?;
        info!(count, "archive caught up from WAL");
    }

    let archive_tip = domain
        .archive()
        .get_tip()?
        .map(|(slot, _)| ChainPoint::Slot(slot));

    verify_caught_up("archive", archive_tip, target, CatchUpSeverity::Lenient)
}

#[cfg(test)]
mod tests {
    // Tests for bootstrap catch-up live in `tests/bootstrap.rs`
    // (workspace-level integration test) because they need `ToyDomain` from
    // `dolos-testing`, which re-exports `dolos-core` and would create a
    // duplicate-crate conflict inside lib-level `#[cfg(test)]`.
}
