//! Estart (Epoch Start) work unit implementation.
//!
//! The estart work unit handles the start of a new epoch including:
//! - Nonce transitions
//! - Account/pool snapshot transitions
//! - Epoch number increment
//! - Pot recalculation
//! - Era transitions (if protocol version changes)

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, WorkUnit};
use tracing::{debug, info};

use crate::{CardanoError, CardanoGenesis, CardanoLogic};

use super::WorkContext;

/// Work unit for epoch start processing.
pub struct EstartWorkUnit {
    slot: BlockSlot,
    #[allow(dead_code)]
    config: CardanoConfig,
    genesis: Arc<CardanoGenesis>,

    // Loaded
    context: Option<WorkContext>,
}

impl EstartWorkUnit {
    /// Create a new estart work unit.
    pub fn new(slot: BlockSlot, config: CardanoConfig, genesis: Arc<CardanoGenesis>) -> Self {
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
    D: Domain<Chain = CardanoLogic, ChainSpecificError = CardanoError>,
{
    fn name(&self) -> &'static str {
        "estart"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        debug!(slot = self.slot, "loading estart work context");

        let context = WorkContext::load::<D>(domain.state(), self.genesis.clone())?;

        info!(epoch = context.starting_epoch_no(), "starting epoch");

        self.context = Some(context);

        debug!("estart context loaded");

        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError<D::ChainSpecificError>> {
        // Computation is done during load via the visitor pattern
        debug!("estart compute phase (deltas already computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        debug!(slot = self.slot, "committing estart state changes");

        let context = self.context.as_mut().ok_or_else(|| {
            DomainError::Internal("estart context not loaded".into())
        })?;

        context.commit::<D>(domain.state(), domain.archive(), self.slot)?;

        debug!("estart state committed");
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError<D::ChainSpecificError>> {
        // Archive writes are done in commit_state via context.commit()
        Ok(())
    }
}
