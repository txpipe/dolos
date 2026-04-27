use dolos_core::{EntityKey, NsKey};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::StakeCredential,
};
use serde::{Deserialize, Serialize};

use super::{pools::PoolHash, FixedNamespace as _};

/// Helper to derive EntityKey from StakeCredential.
pub fn credential_to_key(cred: &StakeCredential) -> EntityKey {
    let enc = minicbor::to_vec(cred).unwrap();
    EntityKey::from(enc)
}

/// Pending reward for a single account, waiting to be applied at the epoch
/// boundary. Created by RUPD, consumed by `Ewrap` (the per-account
/// leg of the boundary pipeline).
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingRewardState {
    #[n(0)]
    pub credential: StakeCredential,

    #[n(1)]
    pub is_spendable: bool,

    /// Rewards earned as pool operator (pool_hash, amount)
    #[n(2)]
    pub as_leader: Vec<(PoolHash, u64)>,

    /// Rewards earned as delegator (pool_hash, amount)
    #[n(3)]
    pub as_delegator: Vec<(PoolHash, u64)>,
}

impl PendingRewardState {
    pub fn total_value(&self) -> u64 {
        self.as_leader.iter().map(|(_, v)| v).sum::<u64>()
            + self.as_delegator.iter().map(|(_, v)| v).sum::<u64>()
    }

    /// Convert to a list of (pool_hash, amount, as_leader) tuples for logging.
    pub fn into_log_entries(&self) -> Vec<(PoolHash, u64, bool)> {
        let leader = self.as_leader.iter().map(|(p, v)| (*p, *v, true));
        let delegator = self.as_delegator.iter().map(|(p, v)| (*p, *v, false));
        leader.chain(delegator).collect()
    }
}

entity_boilerplate!(PendingRewardState, "pending_rewards");

/// Pending MIR (Move Instantaneous Reward) for a single account, waiting to
/// be applied at the epoch boundary. Created during block roll when MIR
/// certificates are processed, consumed by `Ewrap` (MIR processing is part
/// of the global Ewrap phase, not the per-account `Ewrap` phase).
///
/// Unlike regular rewards, MIRs come from either reserves or treasury.
/// During Ewrap, MIRs are only applied to registered accounts — MIRs to
/// unregistered accounts stay in their source pot (no transfer occurs).
#[derive(Debug, Clone, Encode, Decode, Serialize, Deserialize, PartialEq, Eq)]
pub struct PendingMirState {
    #[n(0)]
    pub credential: StakeCredential,

    /// Amount from reserves
    #[n(1)]
    pub from_reserves: u64,

    /// Amount from treasury
    #[n(2)]
    pub from_treasury: u64,
}

impl PendingMirState {
    pub fn total_value(&self) -> u64 {
        self.from_reserves + self.from_treasury
    }
}

entity_boilerplate!(PendingMirState, "pending_mirs");

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_pending_reward_state()(
            credential in root::any_stake_credential(),
            is_spendable in any::<bool>(),
            as_leader in prop::collection::vec((root::any_pool_hash(), root::any_lovelace()), 0..3),
            as_delegator in prop::collection::vec((root::any_pool_hash(), root::any_lovelace()), 0..3),
        ) -> PendingRewardState {
            PendingRewardState { credential, is_spendable, as_leader, as_delegator }
        }
    }

    prop_compose! {
        pub fn any_pending_mir_state()(
            credential in root::any_stake_credential(),
            from_reserves in root::any_lovelace(),
            from_treasury in root::any_lovelace(),
        ) -> PendingMirState {
            PendingMirState { credential, from_reserves, from_treasury }
        }
    }
}

// --- Deltas ---

/// Delta to enqueue a pending reward for an account.
/// Applied by RUPD after computing rewards.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnqueueReward {
    pub credential: StakeCredential,
    pub is_spendable: bool,
    pub as_leader: Vec<(PoolHash, u64)>,
    pub as_delegator: Vec<(PoolHash, u64)>,

    pub(crate) prev: Option<PendingRewardState>,
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
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for EnqueueReward {
    type Entity = PendingRewardState;

    fn key(&self) -> NsKey {
        NsKey::from((PendingRewardState::NS, credential_to_key(&self.credential)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        self.prev = entity.clone();
        *entity = Some(PendingRewardState {
            credential: self.credential.clone(),
            is_spendable: self.is_spendable,
            as_leader: self.as_leader.clone(),
            as_delegator: self.as_delegator.clone(),
        });
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        *entity = self.prev.clone();
    }
}

/// Delta to dequeue (consume) a pending reward after applying it.
/// Applied by `Ewrap` after rewards are assigned to accounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DequeueReward {
    pub credential: StakeCredential,
    /// Previous state stored for rollback
    pub(crate) prev: Option<PendingRewardState>,
}

impl DequeueReward {
    pub fn new(credential: StakeCredential) -> Self {
        Self {
            credential,
            prev: None,
        }
    }
}

impl dolos_core::EntityDelta for DequeueReward {
    type Entity = PendingRewardState;

    fn key(&self) -> NsKey {
        NsKey::from((PendingRewardState::NS, credential_to_key(&self.credential)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        // Store previous state for undo, then remove the entity
        self.prev = entity.take();
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        *entity = self.prev.clone();
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

    pub(crate) prev: Option<PendingMirState>,
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
            prev: None,
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
        self.prev = entity.clone();

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
        *entity = self.prev.clone();
    }
}

/// Delta to dequeue (consume) a pending MIR after applying it.
/// Applied by `Ewrap` after MIRs are assigned to accounts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DequeueMir {
    pub credential: StakeCredential,
    /// Previous state stored for rollback
    pub(crate) prev: Option<PendingMirState>,
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
        *entity = self.prev.clone();
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use super::testing::{any_pending_mir_state, any_pending_reward_state};
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use proptest::prelude::*;

    prop_compose! {
        fn any_enqueue_reward()(
            credential in root::any_stake_credential(),
            is_spendable in any::<bool>(),
            as_leader in prop::collection::vec((root::any_pool_hash(), root::any_lovelace()), 0..3),
            as_delegator in prop::collection::vec((root::any_pool_hash(), root::any_lovelace()), 0..3),
        ) -> EnqueueReward {
            EnqueueReward::new(credential, is_spendable, as_leader, as_delegator)
        }
    }

    prop_compose! {
        fn any_dequeue_reward()(
            credential in root::any_stake_credential(),
        ) -> DequeueReward {
            DequeueReward::new(credential)
        }
    }

    prop_compose! {
        fn any_enqueue_mir()(
            credential in root::any_stake_credential(),
            from_reserves in root::any_lovelace(),
            from_treasury in root::any_lovelace(),
            overwrite in any::<bool>(),
        ) -> EnqueueMir {
            EnqueueMir::new(credential, from_reserves, from_treasury, overwrite)
        }
    }

    prop_compose! {
        fn any_dequeue_mir()(
            credential in root::any_stake_credential(),
        ) -> DequeueMir {
            DequeueMir::new(credential)
        }
    }

    proptest! {
        #[test]
        fn enqueue_reward_roundtrip(
            entity in prop::option::of(any_pending_reward_state()),
            delta in any_enqueue_reward(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn dequeue_reward_roundtrip(
            entity in prop::option::of(any_pending_reward_state()),
            delta in any_dequeue_reward(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn enqueue_mir_roundtrip(
            entity in prop::option::of(any_pending_mir_state()),
            delta in any_enqueue_mir(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn dequeue_mir_roundtrip(
            entity in prop::option::of(any_pending_mir_state()),
            delta in any_dequeue_mir(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }
    }
}
