//! Roll work unit implementation.
//!
//! The roll work unit processes batches of blocks, updating the ledger state
//! with new transactions, UTxOs, and entity changes.

use std::sync::Arc;

use dolos_core::{Domain, DomainError, Genesis, RawBlock, TipEvent, WorkUnit};
use tracing::{debug, info};

use crate::{roll::batch::WorkBatch, CardanoDelta, CardanoEntity, CardanoLogic};

/// Work unit for processing a batch of blocks ("rolling" the chain forward).
pub struct RollWorkUnit {
    /// The batch of blocks to process
    batch: WorkBatch,

    /// Genesis configuration
    #[allow(dead_code)]
    genesis: Arc<Genesis>,

    /// Whether this is live mode (emit tip notifications)
    live_mode: bool,
}

impl RollWorkUnit {
    /// Create a new roll work unit.
    pub fn new(batch: WorkBatch, genesis: Arc<Genesis>, live_mode: bool) -> Self {
        Self {
            batch,
            genesis,
            live_mode,
        }
    }

    /// Decode UTxOs using the provided chain logic.
    /// This must be called after load_utxos and before compute.
    pub fn decode_utxos(&mut self, chain: &CardanoLogic) -> Result<(), DomainError> {
        self.batch.decode_utxos(chain)?;
        Ok(())
    }
}

impl<D> WorkUnit<D> for RollWorkUnit
where
    D: Domain<Chain = CardanoLogic, Entity = CardanoEntity, EntityDelta = CardanoDelta>,
{
    fn name(&self) -> &'static str {
        "roll"
    }

    fn load(&mut self, _domain: &D) -> Result<(), DomainError> {
        // UTxO loading and decoding is done in CardanoLogic::pop_work
        // because it requires access to both domain and chain logic
        debug!(
            blocks = self.batch.blocks.len(),
            "roll batch already loaded"
        );
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        info!(
            first_slot = self.batch.first_slot(),
            last_slot = self.batch.last_slot(),
            blocks = self.batch.blocks.len(),
            "computing roll deltas"
        );

        // Note: The actual delta computation happens in pop_work in CardanoLogic
        // because it needs access to the chain logic via the domain.
        // This is a slight deviation from the pure compute model, but
        // necessary due to how the visitor pattern works.

        Ok(())
    }

    fn commit_wal(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!("committing roll batch to WAL");

        // Ensure blocks are sorted before WAL commit
        self.batch.sort_by_slot();

        self.batch.commit_wal(domain)?;

        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!("loading entities for roll batch");

        // Load entities that will be modified
        self.batch.load_entities(domain)?;

        // Apply deltas to entities
        self.batch.apply_entities()?;

        debug!("committing roll batch to state");

        // Commit state changes
        self.batch.commit_state(domain)?;

        Ok(())
    }

    fn commit_archive(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!("committing roll batch to archive");

        self.batch.commit_archive(domain)?;

        Ok(())
    }

    fn commit_indexes(&mut self, domain: &D) -> Result<(), DomainError> {
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
                info!(%point, "roll forward");
                TipEvent::Apply(point, raw)
            })
            .collect()
    }
}
