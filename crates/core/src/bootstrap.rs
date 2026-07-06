//! Domain initialization and integrity checks.
//!
//! This module provides functionality for bootstrapping the domain at startup.
//! It ensures storage consistency and drains any pending initialization work
//! using the full sync lifecycle (WAL + tip notifications).

use tracing::{error, info, warn};

use crate::{
    sync::drain_pending_work, ArchiveStore, ArchiveWriter as _, ChainLogic, ChainPoint, Domain,
    DomainError, EntityMap, IndexStore, IndexWriter as _, StateStore, StateWriter as _, WalStore,
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
            // WAL at Origin with no state cursor — fresh node after genesis.
            info!(%wal_tip, "WAL at origin, no state cursor (post-genesis)");
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

/// Catch up state, archive and index stores by replaying WAL entries.
///
/// The WAL commits first in the work-unit lifecycle, so after a crash it is
/// the most advanced store. Every other store reconciles forward to the WAL
/// tip: state first (covering a crash between `commit_wal` and
/// `commit_state`), then archive and indexes (covering a crash between the
/// state commit and the archive/index commits).
fn catch_up_stores<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let target = match domain.wal().find_tip()? {
        // nothing to catch up
        None => return Ok(()),
        // Origin means no blocks have been processed yet — state, archive and
        // indexes are correctly empty, so there is nothing to replay.
        Some((ChainPoint::Origin, _)) => return Ok(()),
        Some((point, _)) => point,
    };

    catch_up_state(domain, &target)?;
    catch_up_archive(domain, &target)?;
    catch_up_indexes(domain, &target)?;

    Ok(())
}

/// Catch up the state store by replaying WAL entries.
///
/// A crash between `commit_wal` and `commit_state` leaves the WAL holding
/// blocks whose effects never reached the state store. Only roll work units
/// write WAL entries, and each entry fully captures its state mutation
/// (entity deltas + block + resolved inputs), so forward-replaying them here
/// is lossless. Boundary work units never write the WAL, so they can't leave
/// the WAL ahead of state; recovering a crash *during* a boundary is a
/// separate concern (#1018).
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

        writer.set_cursor(point.clone())?;

        // Commit per entry so a later block touching the same entity reloads
        // the value this block just wrote.
        writer.commit()?;

        count += 1;
    }

    if count > 0 {
        info!(count, "state caught up from WAL");
    }

    // Post-condition: the replay must actually reach the target. A WAL that
    // claims a tip the state can't reach (e.g. entries wiped by `reset_to`)
    // is unrecoverable here — fail loudly instead of leaving a silent gap.
    // Compared by slot: a Slot-only cursor at the target's slot (post-ESTART)
    // is already at the target even though the points differ.
    let cursor = domain.state().read_cursor()?;

    let reached = cursor
        .as_ref()
        .is_some_and(|cursor| cursor.slot() >= target.slot());

    if !reached {
        error!(?cursor, %target, "state catch-up could not reach the WAL target");
        return Err(DomainError::InconsistentState {
            wal: Some(target.clone()),
            state: cursor,
        });
    }

    Ok(())
}

/// Catch up archive store by replaying WAL blocks.
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

    let blocks = domain.wal().iter_blocks(start, Some(target.clone()))?;

    let writer = domain.archive().start_writer()?;
    let mut count = 0u64;

    for (point, block) in blocks {
        // Skip the start point itself (already in archive) and anything at or before it
        if Some(point.slot()) <= archive_tip {
            continue;
        }

        // Skip synthetic entries (from reset_to) — they carry no block
        if block.is_empty() {
            continue;
        }

        writer.apply(&point, &block)?;
        count += 1;
    }

    if count > 0 {
        writer.commit()?;
        info!(count, "archive caught up from WAL");
    }

    // Archive can legitimately remain behind when the WAL no longer holds the
    // missing blocks (e.g. after `reset_to`). Flag it instead of failing —
    // archive completeness is not consensus-critical, matching the lenient
    // handling in `check_archive_in_sync_with_state`.
    let archive_tip = domain.archive().get_tip()?.map(|(slot, _)| slot);

    if archive_tip < Some(target_slot) {
        error!(
            ?archive_tip,
            target_slot, "archive still behind WAL target after catch-up"
        );
    }

    Ok(())
}

/// Catch up index store by replaying WAL log entries.
fn catch_up_indexes<D: Domain>(domain: &D, target: &ChainPoint) -> Result<(), DomainError> {
    let index_cursor = domain.indexes().cursor()?;

    if index_cursor.as_ref() == Some(target) {
        return Ok(());
    }

    let index_slot = index_cursor.as_ref().map(|p| p.slot());

    // Find the WAL start point from the index cursor
    let start = match index_slot {
        Some(slot) => domain.wal().locate_point(slot)?,
        None => None,
    };

    let logs = domain.wal().iter_logs(start, Some(target.clone()))?;

    let writer = domain.indexes().start_writer()?;
    let mut count = 0u64;

    for (point, log) in logs {
        // Skip entries at or before the current index cursor
        if Some(point.slot()) <= index_slot {
            continue;
        }

        // Skip synthetic entries (from reset_to) — they carry no effects
        if log.block.is_empty() {
            continue;
        }

        let catchup = D::Chain::compute_catchup(&log.block, &log.inputs, point)?;

        writer.apply(&catchup.index_delta)?;
        count += 1;
    }

    if count > 0 {
        writer.commit()?;
        info!(count, "indexes caught up from WAL");
    }

    // Same lenient handling as archive: flag a residual lag instead of
    // failing the boot.
    let index_cursor = domain.indexes().cursor()?;
    let index_slot = index_cursor.as_ref().map(|p| p.slot());

    if index_slot < Some(target.slot()) {
        error!(?index_cursor, %target, "indexes still behind WAL target after catch-up");
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests for bootstrap catch-up live in `tests/bootstrap.rs` (workspace-level
    // integration test) because they need `ToyDomain` from `dolos-testing`,
    // which re-exports `dolos-core` and would create a duplicate-crate conflict
    // inside lib-level `#[cfg(test)]`.
}
