//! Rupd (Reward Update) work unit implementation.
//!
//! The rupd work unit computes rewards at the stability window boundary
//! (4k slots before epoch end).

use std::sync::Arc;

use dolos_core::{BlockSlot, Domain, DomainError, Genesis, WorkUnit};
use tracing::{debug, info};

use crate::{rewards::RewardMap, CardanoLogic};

use super::RupdWork;

/// Work unit for computing rewards at the stability window.
pub struct RupdWorkUnit {
    slot: BlockSlot,
    genesis: Arc<Genesis>,

    // Loaded
    work: Option<RupdWork>,

    // Computed
    rewards: Option<RewardMap<RupdWork>>,
}

impl RupdWorkUnit {
    /// Create a new rupd work unit.
    pub fn new(slot: BlockSlot, genesis: Arc<Genesis>) -> Self {
        Self {
            slot,
            genesis,
            work: None,
            rewards: None,
        }
    }

    /// Get the computed rewards (to be stored in cache for ewrap).
    pub fn take_rewards(&mut self) -> Option<RewardMap<RupdWork>> {
        self.rewards.take()
    }
}

impl<D> WorkUnit<D> for RupdWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "rupd"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        info!(slot = self.slot, "loading rupd work context");

        self.work = Some(RupdWork::load::<D>(domain.state(), &self.genesis)?);

        debug!("rupd context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        info!(slot = self.slot, "computing rewards");

        let work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::InconsistentState("rupd work not loaded".to_string()))?;

        let rewards = crate::rewards::define_rewards(work)?;

        self.rewards = Some(rewards);

        debug!("rewards computed");
        Ok(())
    }

    fn commit_state(&mut self, _domain: &D) -> Result<(), DomainError> {
        // Rupd stores rewards in the chain logic cache for ewrap to consume
        // This is handled by the pop_work logic in CardanoLogic
        //
        // TODO: Consider using state store for inter-work-unit data flow
        // for a cleaner architecture
        Ok(())
    }

    fn commit_archive(&mut self, domain: &D) -> Result<(), DomainError> {
        let work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::InconsistentState("rupd work not loaded".to_string()))?;

        let rewards = self
            .rewards
            .as_ref()
            .ok_or_else(|| DomainError::InconsistentState("rewards not computed".to_string()))?;

        // Log stake snapshot data to archive
        super::log_work::<D>(work, rewards, domain.archive())?;

        debug!("rupd archive logs committed");
        Ok(())
    }
}
