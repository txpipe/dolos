//! Ewrap (Epoch Wrap) work unit implementation.
//!
//! The ewrap work unit handles epoch boundary processing including:
//! - Applying rewards to accounts
//! - Processing pool retirements
//! - Handling governance proposal enactment
//! - DRep expiration

use std::sync::Arc;

use dolos_core::{config::CardanoConfig, BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::{rewards::RewardMap, rupd::RupdWork, CardanoLogic};

use super::BoundaryWork;

/// Work unit for epoch boundary wrap-up processing.
pub struct EwrapWorkUnit {
    slot: BlockSlot,
    #[allow(dead_code)]
    config: CardanoConfig,
    genesis: Arc<Genesis>,
    rewards: RewardMap<RupdWork>,

    // Loaded
    boundary: Option<BoundaryWork>,
}

impl EwrapWorkUnit {
    /// Create a new ewrap work unit.
    pub fn new(
        slot: BlockSlot,
        config: CardanoConfig,
        genesis: Arc<Genesis>,
        rewards: RewardMap<RupdWork>,
    ) -> Self {
        Self {
            slot,
            config,
            genesis,
            rewards,
            boundary: None,
        }
    }
}

impl<D> WorkUnit<D> for EwrapWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "ewrap"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        info!(slot = self.slot, "loading ewrap boundary context");

        let rewards = std::mem::take(&mut self.rewards);

        self.boundary = Some(BoundaryWork::load::<D>(
            domain.state(),
            self.genesis.clone(),
            rewards,
        )?);

        debug!("ewrap boundary context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        // Computation is done during load via compute_deltas
        // This is because the visitor pattern needs access to state
        debug!("ewrap compute phase (deltas already computed during load)");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        info!(slot = self.slot, "committing ewrap state changes");

        let boundary = self.boundary.as_mut().ok_or_else(|| {
            DomainError::InconsistentState("ewrap boundary not loaded".to_string())
        })?;

        boundary.commit::<D>(domain.state(), domain.archive())?;

        debug!("ewrap state committed");
        Ok(())
    }

    fn commit_archive(&mut self, _domain: &D) -> Result<(), DomainError> {
        // Archive writes are done in commit_state via boundary.commit()
        // because they're interleaved with state commits
        Ok(())
    }
}
