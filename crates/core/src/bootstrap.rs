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
        ensure_wal_in_sync_with_state(self)?;
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

/// Ensure WAL is in sync with state store.
///
/// If the WAL tip doesn't match the state cursor, reset WAL to match state.
fn ensure_wal_in_sync_with_state<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let wal = domain.wal().find_tip()?.map(|(point, _)| point);
    let state = domain.state().read_cursor()?;

    match (wal, state) {
        (Some(wal), Some(state)) => {
            if wal != state {
                warn!(%wal, %state, "wal is out of sync");
                info!("resetting wal to match state");
                domain.wal().reset_to(&state)?;
            }
        }
        (None, Some(state)) => {
            warn!(%state, "missing wal, resetting to match state");
            info!("resetting wal to match state");
            domain.wal().reset_to(&state)?;
        }
        (Some(_), None) => {
            // Weird case, strictly speaking, this should clear the wal rather than reset to origin
            warn!("missing wal, resetting to origin");
            info!("resetting wal to origin");
            domain.wal().reset_to(&ChainPoint::Origin)?;
        }
        (None, None) => (),
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
/// After `ensure_wal_in_sync_with_state`, the WAL matches the state cursor.
/// If archive or index stores are behind (e.g., crash between state commit
/// and archive/index commit), this function replays the missing WAL entries
/// to bring them back in sync.
fn catch_up_stores<D: Domain>(domain: &D) -> Result<(), DomainError> {
    let state_cursor = match domain.state().read_cursor()? {
        Some(cursor) => cursor,
        None => return Ok(()), // nothing to catch up
    };

    catch_up_archive(domain, &state_cursor)?;
    catch_up_indexes(domain, &state_cursor)?;

    Ok(())
}

/// Catch up archive store by replaying WAL blocks.
fn catch_up_archive<D: Domain>(
    domain: &D,
    state_cursor: &ChainPoint,
) -> Result<(), DomainError> {
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
fn catch_up_indexes<D: Domain>(
    domain: &D,
    state_cursor: &ChainPoint,
) -> Result<(), DomainError> {
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

    let logs = domain
        .wal()
        .iter_logs(start, Some(state_cursor.clone()))?;

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
    // Tests will be added once we have the full integration in place
}
