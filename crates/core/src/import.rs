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

use tracing::{debug, info, instrument};

use crate::{BlockSlot, ChainLogic, Domain, DomainError, RawBlock, WorkUnit};

/// Extension trait for bulk block import operations.
///
/// This trait extends any `Domain` implementation with methods for
/// efficiently importing blocks from trusted, immutable sources.
#[trait_variant::make(Send)]
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
    async fn import_blocks(&self, raw: Vec<RawBlock>) -> Result<BlockSlot, DomainError>;
}

impl<D: Domain> ImportExt for D {
    async fn import_blocks(&self, mut raw: Vec<RawBlock>) -> Result<BlockSlot, DomainError> {
        let mut last = 0;
        let mut chain = self.write_chain().await;

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
#[instrument(skip_all, fields(work_unit = %work.name()))]
fn execute_work_unit<D: Domain>(domain: &D, work: &mut D::WorkUnit) -> Result<(), DomainError> {
    info!("executing work unit (import)");

    work.load(domain)?;
    debug!("load phase complete");

    work.compute()?;
    debug!("compute phase complete");

    // Skip WAL commit for import - data comes from trusted immutable source
    debug!("skipping wal commit (import mode)");

    work.commit_state(domain)?;
    debug!("state commit complete");

    work.commit_archive(domain)?;
    debug!("archive commit complete");

    work.commit_indexes(domain)?;
    debug!("index commit complete");

    // Skip tip notifications for import - no live subscribers
    debug!("skipping tip notifications (import mode)");

    info!("work unit completed (import)");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests will be added once we have the full integration in place
}
