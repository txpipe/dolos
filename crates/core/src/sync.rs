//! Live block synchronization with full lifecycle.
//!
//! This module provides functionality for processing blocks received from
//! the network during live synchronization. Unlike bulk import, sync uses
//! the full work unit lifecycle:
//!
//! - WAL commits for crash recovery and rollback support
//! - Tip notifications for live subscribers
//!
//! Use this for processing blocks from network peers during live operation.

use tracing::{debug, info, instrument};

use crate::{BlockSlot, ChainLogic, Domain, DomainError, RawBlock, WorkUnit};

/// Extension trait for live block synchronization.
///
/// This trait extends any `Domain` implementation with methods for
/// processing blocks received from the network during live sync.
pub trait SyncExt: Domain {
    /// Process a single block during live synchronization.
    ///
    /// This function handles a single block received during live sync,
    /// executing any resulting work units with the full lifecycle including
    /// WAL commits and tip notifications.
    ///
    /// # Arguments
    ///
    /// * `block` - The raw block to process
    ///
    /// # Returns
    ///
    /// The slot of the processed block.
    fn roll_forward(&self, block: RawBlock) -> Result<BlockSlot, DomainError>;
}

impl<D: Domain> SyncExt for D {
    #[instrument(skip_all)]
    fn roll_forward(&self, block: RawBlock) -> Result<BlockSlot, DomainError> {
        let mut chain = self.write_chain();

        // Drain first in case there's previous work that needs to be applied (eg: initialization)
        drain_pending_work::<D>(&mut *chain, self)?;

        let last = chain.receive_block(block)?;

        drain_pending_work::<D>(&mut *chain, self)?;

        Ok(last)
    }
}

/// Drain all pending work from the chain logic using sync lifecycle.
pub(crate) fn drain_pending_work<D: Domain>(
    chain: &mut D::Chain,
    domain: &D,
) -> Result<(), DomainError> {
    while let Some(mut work) = <D::Chain as ChainLogic>::pop_work::<D>(chain, domain) {
        execute_work_unit(domain, &mut work)?;
    }

    Ok(())
}

/// Execute a work unit through the full sync lifecycle.
///
/// Sync lifecycle includes all phases:
/// 1. `load()` - Load required data from storage
/// 2. `compute()` - Execute computation over loaded data
/// 3. `commit_wal()` - Persist to write-ahead log
/// 4. `commit_state()` - Apply changes to state store
/// 5. `commit_archive()` - Apply changes to archive store
/// 6. `commit_indexes()` - Apply changes to index stores
/// 7. `notify_tip()` - Notify tip subscribers
///
/// This function is public primarily for testing scenarios where direct
/// work unit execution is needed (e.g., manual genesis initialization).
#[instrument(skip_all, fields(work_unit = %work.name()))]
pub fn execute_work_unit<D: Domain>(domain: &D, work: &mut D::WorkUnit) -> Result<(), DomainError> {
    info!("executing work unit");

    work.load(domain)?;
    debug!("load phase complete");

    work.compute()?;
    debug!("compute phase complete");

    work.commit_wal(domain)?;
    debug!("wal commit complete");

    work.commit_state(domain)?;
    debug!("state commit complete");

    work.commit_archive(domain)?;
    debug!("archive commit complete");

    work.commit_indexes(domain)?;
    debug!("index commit complete");

    // Notify tip events to subscribers
    for event in work.tip_events() {
        domain.notify_tip(event);
    }

    info!("work unit completed");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests will be added once we have the full integration in place
}
