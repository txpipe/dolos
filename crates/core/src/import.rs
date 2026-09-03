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
    sync::run_lifecycle, BlockSlot, ChainLogic, ChainPoint, Domain, DomainError, RawBlock,
    StateError, StateStore, WalError, WalStore, WorkUnit,
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
/// 1. `initialize()` - Shard-agnostic setup
/// 2. For each shard `0..total_shards()`: a. `load()` - Load required data from
///    storage b. `compute()` - Execute computation over loaded data c.
///    `commit_state()` - Apply changes to state store d. `commit_archive()` -
///    Apply changes to archive store e. `commit_indexes()` - Apply changes to
///    index stores
/// 3. `finalize()` - Shard-agnostic teardown
///
/// Skipped phases:
/// - `commit_wal()` - Not needed for immutable data import
/// - `notify_tip()` - No subscribers during bulk import
#[instrument(skip_all, name = "work_unit", fields(name = %work.name()))]
fn execute_work_unit<D: Domain>(domain: &D, work: &mut D::WorkUnit) -> Result<(), DomainError> {
    debug!("executing work unit (import)");

    run_lifecycle(domain, work, false)?;

    // Skip tip notifications for import - no live subscribers
    debug!("skipping tip notifications (import mode)");

    debug!("work unit completed (import)");
    Ok(())
}

#[cfg(test)]
mod tests {
    // Tests will be added once we have the full integration in place
}

/// What seeding the WAL from the state cursor found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalSeed {
    /// No cursor: there is nothing to seed from, which is not a failure.
    NoCursor,
    /// The WAL now stands at this point.
    Seeded(ChainPoint),
}

/// Why the WAL could not be seeded from the state cursor.
///
/// One variant per step, each carrying the message the bootstrap command has
/// always printed, so the move costs an operator nothing.
#[derive(Debug, thiserror::Error)]
pub enum WalSeedError {
    #[error("reading state cursor")]
    ReadCursor(#[source] StateError),

    #[error("state cursor at slot {slot} has no block hash, cannot seed WAL")]
    Undefined { slot: BlockSlot },

    #[error("seeding WAL from state cursor")]
    Reset(#[source] WalError),
}

/// Seed the WAL from the state cursor so that `find_intersect` works after a
/// bulk import.
///
/// [`ImportExt`] skips WAL commits for throughput, and so do several bootstrap
/// mechanisms, which leaves the WAL empty or stale beside a state store that
/// has moved. This puts the WAL tip back on the state cursor whatever filled
/// the stores — so it sits beside the import path that creates the gap rather
/// than in any one command that has to close it.
///
/// Generic over the two store traits rather than over a [`Domain`], because a
/// bootstrap has the two stores open and no domain assembled; a caller holding
/// a domain passes `domain.state()` and `domain.wal()`.
pub fn seed_wal_from_state<S, W>(state: &S, wal: &W) -> Result<WalSeed, WalSeedError>
where
    S: StateStore,
    W: WalStore,
{
    let cursor = state.read_cursor().map_err(WalSeedError::ReadCursor)?;

    let Some(cursor) = cursor else {
        return Ok(WalSeed::NoCursor);
    };

    if !cursor.is_fully_defined() {
        return Err(WalSeedError::Undefined {
            slot: cursor.slot(),
        });
    }

    wal.reset_to(&cursor).map_err(WalSeedError::Reset)?;

    Ok(WalSeed::Seeded(cursor))
}
