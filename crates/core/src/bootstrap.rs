//! Domain initialization and integrity checks.
//!
//! This module provides functionality for bootstrapping the domain at startup.
//! It ensures storage consistency and drains any pending initialization work
//! using the full sync lifecycle (WAL + tip notifications).

use tracing::{error, info, warn};

use crate::{
    sync::drain_pending_work, ArchiveStore, ChainPoint, Domain, DomainError, StateStore, WalStore,
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
    async fn bootstrap(&self) -> Result<(), DomainError>;
}

impl<D: Domain> BootstrapExt for D {
    fn check_integrity(&self) -> Result<(), DomainError> {
        ensure_wal_in_sync_with_state(self)?;
        check_archive_in_sync_with_state(self)?;

        Ok(())
    }

    async fn bootstrap(&self) -> Result<(), DomainError> {
        self.check_integrity()?;

        // TODO: we should probably catch up stores here
        // catch_up_stores(self)?;

        // Drain any work that might have been defined by the initialization
        // using the sync lifecycle (full WAL + tip notifications)
        let mut chain = self.write_chain();
        drain_pending_work(&mut *chain, self).await?;

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

#[cfg(test)]
mod tests {
    // Tests will be added once we have the full integration in place
}
