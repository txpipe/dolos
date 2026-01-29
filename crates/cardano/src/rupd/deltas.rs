//! Delta types for RUPD (Reward Update) work unit.
//!
//! These deltas handle persisting computed rewards to state store.

use dolos_core::{EntityKey, NsKey};
use pallas::{codec::minicbor, ledger::primitives::StakeCredential};
use serde::{Deserialize, Serialize};

use crate::{
    model::CURRENT_EPOCH_KEY, pots::EpochIncentives, EpochState, FixedNamespace,
    PendingRewardState, PoolHash,
};

/// Helper to derive EntityKey from StakeCredential.
pub fn credential_to_key(cred: &StakeCredential) -> EntityKey {
    let enc = minicbor::to_vec(cred).unwrap();
    EntityKey::from(enc)
}

/// Delta to enqueue a pending reward for an account.
/// Applied by RUPD after computing rewards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueReward {
    pub credential: StakeCredential,
    pub is_spendable: bool,
    pub as_leader: Vec<(PoolHash, u64)>,
    pub as_delegator: Vec<(PoolHash, u64)>,
}

impl EnqueueReward {
    pub fn new(
        credential: StakeCredential,
        is_spendable: bool,
        as_leader: Vec<(PoolHash, u64)>,
        as_delegator: Vec<(PoolHash, u64)>,
    ) -> Self {
        Self {
            credential,
            is_spendable,
            as_leader,
            as_delegator,
        }
    }
}

impl dolos_core::EntityDelta for EnqueueReward {
    type Entity = PendingRewardState;

    fn key(&self) -> NsKey {
        NsKey::from((PendingRewardState::NS, credential_to_key(&self.credential)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        *entity = Some(PendingRewardState {
            credential: self.credential.clone(),
            is_spendable: self.is_spendable,
            as_leader: self.as_leader.clone(),
            as_delegator: self.as_delegator.clone(),
        });
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        *entity = None;
    }
}

/// Delta to set epoch incentives on the current epoch state.
/// Applied by RUPD after computing rewards to store incentives metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SetEpochIncentives {
    pub incentives: EpochIncentives,
    prev_incentives: Option<EpochIncentives>,
}

impl SetEpochIncentives {
    pub fn new(incentives: EpochIncentives) -> Self {
        Self {
            incentives,
            prev_incentives: None,
        }
    }
}

impl dolos_core::EntityDelta for SetEpochIncentives {
    type Entity = EpochState;

    fn key(&self) -> NsKey {
        NsKey::from((EpochState::NS, EntityKey::from(CURRENT_EPOCH_KEY)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("EpochState must exist");
        self.prev_incentives = entity.incentives.take();
        entity.incentives = Some(self.incentives.clone());
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("EpochState must exist");
        entity.incentives = self.prev_incentives.clone();
    }
}
