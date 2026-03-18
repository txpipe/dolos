//! Roll work unit implementation.
//!
//! The roll work unit processes batches of blocks, updating the ledger state
//! with new transactions, UTxOs, and entity changes.

use std::sync::Arc;

use dolos_core::{Domain, DomainError, MempoolUpdate, RawBlock, TipEvent, WorkUnit};
use tracing::{debug, info};

use crate::roll::batch::WorkBatch;
use crate::{roll, Cache, CardanoDelta, CardanoEntity, CardanoError, CardanoGenesis, CardanoLogic};

/// Work unit for processing a batch of blocks ("rolling" the chain forward).
pub struct RollWorkUnit {
    /// The batch of blocks to process
    batch: WorkBatch,

    /// Genesis configuration
    genesis: Arc<CardanoGenesis>,

    /// Whether this is live mode (emit tip notifications)
    live_mode: bool,

    /// Cached era info needed for delta computation
    cache: Cache,
}

impl RollWorkUnit {
    /// Create a new roll work unit.
    pub(crate) fn new(
        batch: WorkBatch,
        genesis: Arc<CardanoGenesis>,
        live_mode: bool,
        cache: Cache,
    ) -> Self {
        Self {
            batch,
            genesis,
            live_mode,
            cache,
        }
    }
}

impl<D> WorkUnit<D> for RollWorkUnit
where
    D: Domain<
        Chain = CardanoLogic,
        Entity = CardanoEntity,
        EntityDelta = CardanoDelta,
        ChainSpecificError = CardanoError,
    >,
{
    fn name(&self) -> &'static str {
        "roll"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        debug!(blocks = self.batch.blocks.len(), "loading roll batch UTxOs");

        self.batch.load_utxos(domain)?;
        self.batch.decode_utxos()?;

        roll::compute_delta::<D>(
            self.genesis.clone(),
            &self.cache,
            domain.state(),
            &mut self.batch,
        )?;

        debug!("roll batch loaded and deltas computed");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError<D::ChainSpecificError>> {
        // Deltas are computed during load() since they require state access.
        Ok(())
    }

    fn commit_wal(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        debug!("committing roll batch to WAL");

        // Ensure blocks are sorted before WAL commit
        self.batch.sort_by_slot();

        self.batch.commit_wal(domain)?;

        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        debug!("loading entities for roll batch");

        // Load entities that will be modified
        self.batch.load_entities(domain)?;

        // Apply deltas to entities
        self.batch.apply_entities()?;

        debug!("committing roll batch to state");

        // Commit state changes
        self.batch.commit_state(domain)?;

        info!(
            point = %self.batch.first_point(),
            "rolling forward"
        );

        Ok(())
    }

    fn commit_archive(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        debug!("committing roll batch to archive");

        self.batch.commit_archive(domain)?;

        Ok(())
    }

    fn commit_indexes(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        debug!("committing roll batch to indexes");

        self.batch.commit_indexes(domain)?;

        Ok(())
    }

    fn tip_events(&self) -> Vec<TipEvent> {
        if !self.live_mode {
            return Vec::new();
        }

        self.batch
            .blocks
            .iter()
            .map(|block| {
                let point = block.point();
                let raw: RawBlock = block.raw();
                debug!(%point, "roll forward");
                TipEvent::Apply(point, raw)
            })
            .collect()
    }

    fn mempool_updates(&self) -> Vec<MempoolUpdate> {
        if !self.live_mode {
            return Vec::new();
        }

        self.batch
            .blocks
            .iter()
            .map(|block| MempoolUpdate {
                point: block.point(),
                seen_txs: block
                    .block
                    .view()
                    .txs()
                    .iter()
                    .map(|tx| crate::pallas_hash_to_core(tx.hash()))
                    .collect(),
            })
            .collect()
    }
}
