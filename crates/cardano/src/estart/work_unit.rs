//! Estart (Epoch Start) finalize work unit.
//!
//! Closes the epoch-start pipeline after the per-account `EStartShard`
//! units have run. Handles the global (non-account-keyed) work:
//! - Pool / DRep / proposal snapshot transitions
//! - Nonce transitions
//! - Epoch number increment + pot recalculation (`EpochTransition`)
//! - Era transitions (if protocol version changes)
//! - Cursor advance (only this unit moves the cursor; shards must not)

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::CardanoLogic;

use super::WorkContext;

/// Work unit for epoch start processing.
pub struct EstartWorkUnit {
    slot: BlockSlot,
    #[allow(dead_code)]
    config: CardanoConfig,
    genesis: Arc<Genesis>,

    // Loaded
    context: Option<WorkContext>,
}

impl EstartWorkUnit {
    /// Create a new estart work unit.
    pub fn new(slot: BlockSlot, config: CardanoConfig, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            config,
            genesis,
            context: None,
        }
    }

    /// Access the ended (completed) epoch state, available after load.
    pub fn ended_state(&self) -> Option<&crate::EpochState> {
        self.context.as_ref().map(|ctx| ctx.ended_state())
    }
}

impl<D> WorkUnit<D> for EstartWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "estart"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading estart finalize work context");

        let context = WorkContext::load_finalize::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = context.starting_epoch_no(), "starting epoch");

        self.context = Some(context);

        debug!("estart finalize context loaded");

        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        // Computation is done during load via the visitor pattern
        debug!("estart compute phase (deltas already computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "committing estart finalize state changes");

        let context = self
            .context
            .as_mut()
            .ok_or_else(|| DomainError::Internal("estart context not loaded".into()))?;

        context.commit_finalize::<D>(domain.state(), domain.archive(), self.slot)?;

        debug!("estart finalize state committed");
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        // Archive writes are done in commit_state via context.commit()
        Ok(())
    }
}
