//! Delta types for RUPD (Reward Update) work unit.
//!
//! These deltas handle persisting computed rewards to state store.

use dolos_core::{EntityKey, NsKey};
use pallas::{codec::minicbor, ledger::primitives::StakeCredential};
use serde::{Deserialize, Serialize};

use crate::{
    model::CURRENT_EPOCH_KEY, pots::EpochIncentives, EpochState, FixedNamespace, PendingMirState,
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

/// Delta to enqueue a pending MIR (Move Instantaneous Reward) for an account.
/// Created during block roll when MIR certificates are processed.
///
/// Behavior varies by protocol version:
/// - Pre-Alonzo (protocol < 5): MIRs OVERWRITE previous values for the same credential.
/// - Alonzo+ (protocol >= 5): MIRs ACCUMULATE for the same credential.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueMir {
    pub credential: StakeCredential,
    pub from_reserves: u64,
    pub from_treasury: u64,
    /// If true, overwrite previous MIR values (pre-Alonzo behavior).
    /// If false, accumulate MIR values (Alonzo+ behavior).
    pub overwrite: bool,
}

impl EnqueueMir {
    pub fn new(
        credential: StakeCredential,
        from_reserves: u64,
        from_treasury: u64,
        overwrite: bool,
    ) -> Self {
        Self {
            credential,
            from_reserves,
            from_treasury,
            overwrite,
        }
    }

    pub fn from_reserves(credential: StakeCredential, amount: u64, overwrite: bool) -> Self {
        Self::new(credential, amount, 0, overwrite)
    }

    pub fn from_treasury(credential: StakeCredential, amount: u64, overwrite: bool) -> Self {
        Self::new(credential, 0, amount, overwrite)
    }
}

impl dolos_core::EntityDelta for EnqueueMir {
    type Entity = PendingMirState;

    fn key(&self) -> NsKey {
        NsKey::from((PendingMirState::NS, credential_to_key(&self.credential)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        // Behavior depends on overwrite flag (determined by protocol version):
        // - Pre-Alonzo (overwrite=true): Later MIRs overwrite earlier ones (Map.union semantics)
        // - Alonzo+ (overwrite=false): MIRs accumulate (Map.unionWith (<>) semantics)
        if self.overwrite {
            // Pre-Alonzo: overwrite with new values
            *entity = Some(PendingMirState {
                credential: self.credential.clone(),
                from_reserves: self.from_reserves,
                from_treasury: self.from_treasury,
            });
        } else if let Some(existing) = entity.as_mut() {
            // Alonzo+: accumulate
            existing.from_reserves += self.from_reserves;
            existing.from_treasury += self.from_treasury;
        } else {
            *entity = Some(PendingMirState {
                credential: self.credential.clone(),
                from_reserves: self.from_reserves,
                from_treasury: self.from_treasury,
            });
        }
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        if let Some(existing) = entity.as_mut() {
            existing.from_reserves = existing.from_reserves.saturating_sub(self.from_reserves);
            existing.from_treasury = existing.from_treasury.saturating_sub(self.from_treasury);
            if existing.from_reserves == 0 && existing.from_treasury == 0 {
                *entity = None;
            }
        }
    }
}

/// Delta to dequeue (consume) a pending MIR after applying it.
/// Applied by EWRAP after MIRs are assigned to accounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DequeueMir {
    pub credential: StakeCredential,
    /// Previous state stored for rollback
    prev: Option<PendingMirState>,
}

impl DequeueMir {
    pub fn new(credential: StakeCredential) -> Self {
        Self {
            credential,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for DequeueMir {
    type Entity = PendingMirState;

    fn key(&self) -> NsKey {
        NsKey::from((PendingMirState::NS, credential_to_key(&self.credential)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        // Store previous state for undo, then remove the entity
        self.prev = entity.take();
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        // Restore the previous state
        *entity = self.prev.clone();
    }
}
