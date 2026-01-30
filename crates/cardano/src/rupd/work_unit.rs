//! Rupd (Reward Update) work unit implementation.
//!
//! The rupd work unit computes rewards at the stability window boundary
//! (4k slots before epoch end). Computed rewards are persisted to state store
//! as PendingRewardState entities, to be consumed by EWRAP.

use std::sync::Arc;

use dolos_core::{BlockSlot, Domain, DomainError, Genesis, StateStore, StateWriter, WorkUnit};
use tracing::debug;

use crate::{
    rewards::{Reward, RewardMap},
    CardanoLogic, FixedNamespace, PendingRewardState,
};

use super::{credential_to_key, RupdWork};

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
}

impl<D> WorkUnit<D> for RupdWorkUnit
where
    D: Domain<Chain = CardanoLogic>,
{
    fn name(&self) -> &'static str {
        "rupd"
    }

    fn load(&mut self, domain: &D) -> Result<(), DomainError> {
        debug!(slot = self.slot, "loading rupd work context");

        self.work = Some(RupdWork::load::<D>(domain.state(), &self.genesis)?);

        debug!("rupd context loaded");
        Ok(())
    }

    fn compute(&mut self) -> Result<(), DomainError> {
        debug!(slot = self.slot, "computing rewards");

        let work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::InconsistentState("rupd work not loaded".to_string()))?;

        let rewards = crate::rewards::define_rewards(work)?;

        debug!(pending_count = rewards.len(), "rewards computed");

        self.rewards = Some(rewards);

        debug!("rewards computed");
        Ok(())
    }

    fn commit_state(&mut self, domain: &D) -> Result<(), DomainError> {
        let _work = self
            .work
            .as_ref()
            .ok_or_else(|| DomainError::InconsistentState("rupd work not loaded".to_string()))?;

        let rewards = self
            .rewards
            .as_ref()
            .ok_or_else(|| DomainError::InconsistentState("rewards not computed".to_string()))?;

        debug!(
            pending_count = rewards.len(),
            "persisting pending rewards to state"
        );

        let writer = domain.state().start_writer()?;

        // Persist each pending reward as a PendingRewardState entity
        for (credential, reward) in rewards.iter_pending() {
            let key = credential_to_key(credential);

            // Convert Reward to PendingRewardState
            let (as_leader, as_delegator) = match reward {
                Reward::MultiPool(r) => (
                    r.leader_rewards().collect(),
                    r.delegator_rewards().collect(),
                ),
                Reward::PreAllegra(r) => {
                    let (pool, value) = r.pool_and_value();
                    if r.is_leader() {
                        (vec![(pool, value)], vec![])
                    } else {
                        (vec![], vec![(pool, value)])
                    }
                }
            };

            let state = PendingRewardState {
                credential: credential.clone(),
                is_spendable: reward.is_spendable(),
                as_leader,
                as_delegator,
            };

            writer.write_entity_typed(&key, &state)?;
        }

        // Also update the epoch state with incentives
        let incentives = rewards.incentives().clone();

        // Load current epoch state
        let epoch_key = dolos_core::EntityKey::from(crate::model::CURRENT_EPOCH_KEY);
        if let Some(mut epoch_state) = domain
            .state()
            .read_entity_typed::<crate::EpochState>(crate::EpochState::NS, &epoch_key)?
        {
            // Update incentives
            epoch_state.incentives = Some(incentives);

            // Write back
            writer.write_entity_typed(&epoch_key, &epoch_state)?;
        }

        writer.commit()?;

        debug!("rupd state committed");
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

        #[cfg(feature = "rupd-snapshot-dump")]
        {
            let out_dir = domain.storage_config().path.join("rupd-snapshot");
            super::dump_snapshot_csv(work, self.genesis.as_ref(), &out_dir);
        }

        debug!("rupd archive logs committed");
        Ok(())
    }
}
