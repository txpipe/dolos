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

use std::sync::Arc;
use tracing::{debug, info, instrument, warn};

use crate::{
    ArchiveStore as _, BlockSlot, ChainLogic, ChainPoint, Domain, DomainError, EntityDelta as _,
    IndexStore as _, IndexWriter as _, MempoolStore, RawBlock, StateStore, StateWriter as _,
    TipEvent, WalStore, WorkUnit,
};

const MEMPOOL_FINALIZE_THRESHOLD: u32 = 6;
const MEMPOOL_DROP_THRESHOLD: u32 = 2;

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
    async fn roll_forward(&self, block: RawBlock) -> Result<BlockSlot, DomainError>;

    /// Roll back the chain to a previous point.
    ///
    /// Iterates WAL entries after the target point in reverse order,
    /// undoing each block's effects on state, UTxOs, and indexes.
    async fn rollback(&self, to: &ChainPoint) -> Result<(), DomainError>;
}

impl<D: Domain> SyncExt for D {
    #[instrument(skip_all)]
    async fn roll_forward(&self, block: RawBlock) -> Result<BlockSlot, DomainError> {
        // Drain first in case there's previous work that needs to be applied (eg: initialization)
        let pending_work = {
            let mut chain = self.write_chain();
            collect_pending_work::<D>(&mut *chain, self)
        };

        for mut work in pending_work {
            execute_work_unit(self, &mut work).await?;
        }

        let (last, pending_work) = {
            let mut chain = self.write_chain();
            let last = chain.receive_block(block)?;
            let pending_work = collect_pending_work::<D>(&mut *chain, self);

            (last, pending_work)
        };

        for mut work in pending_work {
            execute_work_unit(self, &mut work).await?;
        }

        Ok(last)
    }

    #[instrument(skip_all, fields(rollback_to = %to))]
    async fn rollback(&self, to: &ChainPoint) -> Result<(), DomainError> {
        let undo_blocks = self.wal().iter_logs(Some(to.clone()), None)?;

        let writer = self.state().start_writer()?;
        let index_writer = self.indexes().start_writer()?;

        for (point, mut log) in undo_blocks.rev() {
            if point == *to {
                // Final cursor update - build an empty delta with just the cursor
                let empty_delta = crate::IndexDelta {
                    cursor: point.clone(),
                    ..Default::default()
                };
                index_writer.apply(&empty_delta)?;
                writer.set_cursor(point.clone())?;
                break;
            }

            let entities = log
                .delta
                .iter()
                .map(|delta| delta.key())
                .collect::<Vec<_>>();

            let mut entities =
                crate::state::load_entity_chunk::<Self>(entities.as_slice(), self.state())?;

            for (key, entity) in entities.iter_mut() {
                for delta in log.delta.iter_mut() {
                    if delta.key() == *key {
                        delta.undo(entity);
                    }
                }
            }

            let block = Arc::new(log.block);

            let undo_data = D::Chain::compute_undo(&block, &log.inputs, point.clone())?;

            // Apply UTxO undo to state
            writer.apply_utxoset(&undo_data.utxo_delta)?;

            // Apply index delta for the undo
            index_writer.undo(&undo_data.index_delta)?;

            // TODO: we should differ notifications until we commit the writers
            self.notify_tip(TipEvent::Undo(point.clone(), block));

            // Move rolled-back transactions back to pending in mempool
            if let Err(e) = self
                .mempool()
                .confirm(
                    &point,
                    &[],
                    &undo_data.tx_hashes,
                    MEMPOOL_FINALIZE_THRESHOLD,
                    MEMPOOL_DROP_THRESHOLD,
                )
                .await
            {
                warn!(?e, %point, "mempool rollback confirm failed");
            }

            info!(%point, "block undone");
        }

        writer.commit()?;
        index_writer.commit()?;

        self.archive().truncate_front(to)?;

        self.wal().truncate_front(to)?;

        Ok(())
    }
}

pub(crate) fn collect_pending_work<D: Domain>(
    chain: &mut D::Chain,
    domain: &D,
) -> Vec<<D::Chain as ChainLogic>::WorkUnit<D>> {
    let mut pending_work = Vec::new();

    while let Some(work) = <D::Chain as ChainLogic>::pop_work::<D>(chain, domain) {
        pending_work.push(work);
    }

    pending_work
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
pub async fn execute_work_unit<D: Domain>(
    domain: &D,
    work: &mut D::WorkUnit,
) -> Result<(), DomainError> {
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

    update_mempool(domain, work).await;

    // Notify tip events to subscribers
    for event in work.tip_events() {
        domain.notify_tip(event);
    }

    info!("work unit completed");
    Ok(())
}

async fn update_mempool<D: Domain>(domain: &D, work: &D::WorkUnit) {
    for update in work.mempool_updates() {
        if let Err(e) = domain
            .mempool()
            .confirm(
                &update.point,
                &update.seen_txs,
                &[],
                MEMPOOL_FINALIZE_THRESHOLD,
                MEMPOOL_DROP_THRESHOLD,
            )
            .await
        {
            warn!(?e, point = %update.point, "mempool confirm failed");
        }
    }
}

#[cfg(test)]
mod tests {
    // Tests will be added once we have the full integration in place
}
