//! Domain initialization and integrity checks.
//!
//! This module provides functionality for bootstrapping the domain at startup.
//! It ensures storage consistency and drains any pending initialization work
//! using the full sync lifecycle (WAL + tip notifications).

use tracing::{error, info, warn};

use crate::{
    sync::drain_pending_work, ArchiveStore, ArchiveWriter as _, ChainLogic, ChainPoint, Domain,
    DomainError, IndexStore, IndexWriter as _, StateStore, WalStore,
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
/// WAL at or ahead of state is normal — the existing `catch_up_stores` handles
/// replaying WAL entries to bring other stores up. State ahead of WAL is an
/// error that requires explicit repair via `dolos doctor reset-wal`.
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

/// Catch up archive and index stores by replaying WAL entries.
///
/// After `check_wal_in_sync_with_state`, the WAL is at or ahead of the state.
/// If archive or index stores are behind (e.g., crash between state commit
/// and archive/index commit), this function replays the missing WAL entries
/// to bring them back in sync.
fn catch_up_stores<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let state_cursor = match domain.state().read_cursor()? {
        // nothing to catch up
        None => return Ok(()),
        // Origin means no blocks have been processed yet — archive and indexes
        // are correctly empty, so there is nothing to replay.
        Some(ChainPoint::Origin) => return Ok(()),
        Some(cursor) => cursor,
    };

    catch_up_archive(domain, &state_cursor)?;
    catch_up_indexes(domain, &state_cursor)?;

    Ok(())
}

/// Catch up archive store by replaying WAL blocks.
fn catch_up_archive<D: Domain>(domain: &D, state_cursor: &ChainPoint) -> Result<(), DomainError> {
    let archive_tip = domain.archive().get_tip()?.map(|(slot, _)| slot);
    let state_slot = state_cursor.slot();

    if archive_tip == Some(state_slot) {
        return Ok(());
    }

    // Find the WAL start point: if archive has data, start from a point
    // corresponding to the archive tip; otherwise start from the beginning.
    let start = match archive_tip {
        Some(slot) => domain.wal().locate_point(slot)?,
        None => None,
    };

    let blocks = domain
        .wal()
        .iter_blocks(start, Some(state_cursor.clone()))?;

    let writer = domain.archive().start_writer()?;
    let mut count = 0u64;

    for (point, block) in blocks {
        // Skip the start point itself (already in archive) and anything at or before it
        if Some(point.slot()) <= archive_tip {
            continue;
        }

        writer.apply(&point, &block)?;
        count += 1;
    }

    if count > 0 {
        writer.commit()?;
        info!(count, "archive caught up from WAL");
    }

    Ok(())
}

/// Catch up index store by replaying WAL log entries.
fn catch_up_indexes<D: Domain>(domain: &D, state_cursor: &ChainPoint) -> Result<(), DomainError> {
    let index_cursor = domain.indexes().cursor()?;

    if index_cursor.as_ref() == Some(state_cursor) {
        return Ok(());
    }

    let index_slot = index_cursor.as_ref().map(|p| p.slot());

    // Find the WAL start point from the index cursor
    let start = match index_slot {
        Some(slot) => domain.wal().locate_point(slot)?,
        None => None,
    };

    let logs = domain.wal().iter_logs(start, Some(state_cursor.clone()))?;

    let writer = domain.indexes().start_writer()?;
    let mut count = 0u64;

    for (point, log) in logs {
        // Skip entries at or before the current index cursor
        if Some(point.slot()) <= index_slot {
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

    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests for bootstrap catch-up live in `tests/bootstrap.rs` (workspace-level
    // integration test) because they need `ToyDomain` from `dolos-testing`,
    // which re-exports `dolos-core` and would create a duplicate-crate conflict
    // inside lib-level `#[cfg(test)]`.
}
