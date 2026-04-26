//! Bulk block import for immutable chain data.
//!
//! This module provides functionality for importing blocks from trusted,
//! immutable sources such as Mithril snapshots or local archives. The import
//! process is optimized for throughput by:
//!
//! - Skipping WAL commits (crash recovery is handled by re-import)
//! - Skipping tip notifications (no live subscribers during bulk import)
//!
//! Use this for initial sync or catch-up from trusted data sources.

use tracing::{debug, instrument};

use crate::{
    work_unit::run_phase, BlockSlot, ChainLogic, Domain, DomainError, RawBlock, WorkUnit,
};

/// Extension trait for bulk block import operations.
///
/// This trait extends any `Domain` implementation with methods for
/// efficiently importing blocks from trusted, immutable sources.
pub trait ImportExt: Domain {
    /// Import a batch of blocks during bulk import operations.
    ///
    /// This function processes multiple blocks efficiently for initial sync
    /// or catch-up scenarios. The import process skips WAL commits and tip
    /// notifications for performance.
    ///
    /// # Arguments
    ///
    /// * `raw` - Vector of raw blocks to import
    ///
    /// # Returns
    ///
    /// The slot of the last imported block.
    fn import_blocks(&self, raw: Vec<RawBlock>) -> Result<BlockSlot, DomainError>;
}

impl<D: Domain> ImportExt for D {
    fn import_blocks(&self, mut raw: Vec<RawBlock>) -> Result<BlockSlot, DomainError> {
        let mut last = 0;
        let mut chain = self.write_chain();

        for block in raw.drain(..) {
            if !chain.can_receive_block() {
                drain_pending_work::<D>(&mut *chain, self)?;
            }

            last = chain.receive_block(block)?;
        }

        // One last drain to ensure we're up to date
        drain_pending_work::<D>(&mut *chain, self)?;

        Ok(last)
    }
}

/// Drain all pending work from the chain logic using import lifecycle.
fn drain_pending_work<D: Domain>(chain: &mut D::Chain, domain: &D) -> Result<(), DomainError> {
    while let Some(mut work) = <D::Chain as ChainLogic>::pop_work::<D>(chain, domain) {
        execute_work_unit(domain, &mut work)?;
    }

    Ok(())
}

/// Execute a work unit through the import lifecycle.
///
/// Import lifecycle skips WAL commits and tip notifications for performance:
/// 1. `load()` - Load required data from storage
/// 2. `compute()` - Execute computation over loaded data
/// 3. `commit_state()` - Apply changes to state store
/// 4. `commit_archive()` - Apply changes to archive store
/// 5. `commit_indexes()` - Apply changes to index stores
///
/// Skipped phases:
/// - `commit_wal()` - Not needed for immutable data import
/// - `notify_tip()` - No subscribers during bulk import
#[instrument(skip_all, name = "work_unit", fields(name = %work.name()))]
fn execute_work_unit<D: Domain>(domain: &D, work: &mut D::WorkUnit) -> Result<(), DomainError> {
    debug!("executing work unit (import)");

    run_phase("load", || work.load(domain))?;
    run_phase("compute", || work.compute())?;

    // Skip WAL commit for import - data comes from trusted immutable source
    debug!("skipping wal commit (import mode)");

    run_phase("commit_state", || work.commit_state(domain))?;
    run_phase("commit_archive", || work.commit_archive(domain))?;
    run_phase("commit_indexes", || work.commit_indexes(domain))?;

    // Skip tip notifications for import - no live subscribers
    debug!("skipping tip notifications (import mode)");

    debug!("work unit completed (import)");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests will be added once we have the full integration in place
}
