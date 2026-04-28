use dolos_core::{BlockSlot, EntityKey, NsKey};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::{conway::RationalNumber, Epoch, PoolMetadata, Relay},
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{
    epoch_value::{EpochValue, TransitionDefault},
    FixedNamespace as _,
};
use crate::pallas_extras::MultiEraPoolRegistration;

pub type PoolHash = Hash<28>;

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolParams {
    #[n(0)]
    pub vrf_keyhash: Hash<32>,

    #[n(1)]
    pub pledge: u64,

    #[n(2)]
    pub cost: u64,

    #[n(3)]
    pub margin: RationalNumber,

    #[n(4)]
    pub reward_account: Vec<u8>,

    #[n(5)]
    pub pool_owners: Vec<Hash<28>>,

    #[n(6)]
    pub relays: Vec<Relay>,

    #[n(7)]
    pub pool_metadata: Option<PoolMetadata>,
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolState {
    #[n(1)]
    pub operator: PoolHash,

    #[n(2)]
    pub snapshot: EpochValue<PoolSnapshot>,

    #[n(11)]
    pub blocks_minted_total: u32,

    #[n(12)]
    pub register_slot: u64,

    #[n(13)]
    pub retiring_epoch: Option<u64>,

    #[n(16)]
    pub deposit: u64,
}

/// Pool state that is epoch-specific
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolSnapshot {
    #[n(1)]
    pub is_retired: bool,

    #[n(2)]
    pub blocks_minted: u32,

    #[n(3)]
    pub params: PoolParams,

    #[n(4)]
    pub is_new: bool,
}

impl TransitionDefault for PoolSnapshot {
    fn next_value(current: Option<&Self>) -> Option<Self> {
        let current = current.expect("no prior pool snapshot");

        Some(PoolSnapshot {
            is_retired: current.is_retired,
            params: current.params.clone(),
            blocks_minted: 0,
            is_new: false,
        })
    }
}

entity_boilerplate!(PoolState, "pools");

impl PoolState {
    pub fn live_saturation(&self) -> RationalNumber {
        // TODO: implement
        RationalNumber {
            numerator: 0,
            denominator: 1,
        }
    }
}

impl From<MultiEraPoolRegistration> for PoolParams {
    fn from(cert: MultiEraPoolRegistration) -> Self {
        PoolParams {
            vrf_keyhash: cert.vrf_keyhash,
            pledge: cert.pledge,
            cost: cert.cost,
            margin: cert.margin,
            reward_account: cert.reward_account,
            pool_owners: cert.pool_owners,
            relays: cert.relays,
            pool_metadata: cert.pool_metadata,
        }
    }
}

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::epoch_value::testing::any_epoch_value;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_pool_params()(
            vrf in root::any_hash_32(),
            pledge in root::any_lovelace(),
            cost in root::any_lovelace(),
            margin in root::any_rational(),
            reward_account in prop::collection::vec(any::<u8>(), 29..30),
            n_owners in 0usize..3usize,
        )(
            vrf in Just(vrf),
            pledge in Just(pledge),
            cost in Just(cost),
            margin in Just(margin),
            reward_account in Just(reward_account),
            pool_owners in prop::collection::vec(root::any_hash_28(), n_owners..=n_owners),
        ) -> PoolParams {
            PoolParams {
                vrf_keyhash: vrf,
                pledge,
                cost,
                margin,
                reward_account,
                pool_owners,
                relays: vec![],
                pool_metadata: None,
            }
        }
    }

    prop_compose! {
        pub fn any_pool_snapshot()(
            is_retired in any::<bool>(),
            blocks_minted in 0u32..1000u32,
            params in any_pool_params(),
            is_new in any::<bool>(),
        ) -> PoolSnapshot {
            PoolSnapshot { is_retired, blocks_minted, params, is_new }
        }
    }

    prop_compose! {
        pub fn any_pool_state()(
            operator in root::any_pool_hash(),
            snapshot in any_epoch_value(any_pool_snapshot().boxed()),
            blocks_minted_total in 0u32..10_000u32,
            register_slot in root::any_slot(),
            retiring_epoch in prop::option::of(root::any_epoch()),
            deposit in root::any_lovelace(),
        ) -> PoolState {
            PoolState {
                operator,
                snapshot,
                blocks_minted_total,
                register_slot,
                retiring_epoch,
                deposit,
            }
        }
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolRegistration {
    pub(crate) cert: MultiEraPoolRegistration,
    pub(crate) slot: BlockSlot,
    pub(crate) epoch: Epoch,

    // params
    pub(crate) pool_deposit: u64,

    // undo: `was_new` flags the creation branch (undo removes the entity); otherwise we
    // restore the touched EpochValue snapshot and `retiring_epoch`.
    pub(crate) was_new: bool,
    pub(crate) prev_snapshot: Option<EpochValue<PoolSnapshot>>,
    pub(crate) prev_retiring_epoch: Option<u64>,
}

impl PoolRegistration {
    pub fn new(
        cert: MultiEraPoolRegistration,
        slot: BlockSlot,
        epoch: Epoch,
        pool_deposit: u64,
    ) -> Self {
        Self {
            cert,
            slot,
            epoch,
            pool_deposit,
            was_new: false,
            prev_snapshot: None,
            prev_retiring_epoch: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolRegistration {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        let key = self.cert.operator.as_slice();
        NsKey::from((PoolState::NS, key))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            debug!(
                slot = self.slot,
                operator = hex::encode(self.cert.operator),
                "updating pool registration",
            );

            // save undo info
            self.prev_snapshot = Some(entity.snapshot.clone());
            self.prev_retiring_epoch = entity.retiring_epoch;

            let is_currently_retired = entity.snapshot.unwrap_live().is_retired;

            if is_currently_retired {
                // if the pool is currently retired, we need to assume this overrides the record as a new registration.
                // Preserve blocks_minted accrued in the current epoch so we don't lose leader rewards.
                let preserved_blocks = entity.snapshot.unwrap_live().blocks_minted;
                entity.snapshot.replace(
                    PoolSnapshot {
                        is_retired: false,
                        is_new: true,
                        blocks_minted: preserved_blocks,
                        params: self.cert.clone().into(),
                    },
                    self.epoch,
                );
            } else {
                entity.snapshot.schedule(
                    self.epoch,
                    Some(PoolSnapshot {
                        is_retired: false,
                        is_new: false,
                        blocks_minted: 0,
                        params: self.cert.clone().into(),
                    }),
                );
            }

            entity.retiring_epoch = None;
        } else {
            debug!(
                slot = self.slot,
                operator = hex::encode(self.cert.operator),
                "applying pool registration",
            );

            self.was_new = true;

            let snapshot = PoolSnapshot {
                is_retired: false,
                blocks_minted: 0,
                params: self.cert.clone().into(),
                is_new: true,
            };

            let state = PoolState {
                register_slot: self.slot,
                operator: self.cert.operator,
                // please note that new pools will udpate its live snapshot directly. This differs
                // from the behavior of existing pools where the change is scheduled for the next
                // epoch.
                snapshot: EpochValue::with_live(self.epoch, snapshot),
                blocks_minted_total: 0,
                retiring_epoch: None,
                deposit: self.pool_deposit,
            };

            *entity = Some(state);
        }
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        if self.was_new {
            *entity = None;
            return;
        }
        let entity = entity.as_mut().expect("existing pool");
        entity.snapshot = self.prev_snapshot.clone().expect("apply captured snapshot");
        entity.retiring_epoch = self.prev_retiring_epoch;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintedBlocksInc {
    pub(crate) operator: Hash<28>,
    pub(crate) count: u32,
}

impl dolos_core::EntityDelta for MintedBlocksInc {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.operator.as_slice()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            entity.blocks_minted_total += self.count;
            let live = entity.snapshot.unwrap_live_mut();
            live.blocks_minted += self.count;
        }
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            entity.blocks_minted_total -= self.count;
            let live = entity.snapshot.unwrap_live_mut();
            live.blocks_minted -= self.count;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDeRegistration {
    pub(crate) operator: Hash<28>,
    pub(crate) epoch: u64,

    // undo
    pub(crate) prev_retiring_epoch: Option<u64>,
    pub(crate) prev_deposit: Option<u64>,
}

impl PoolDeRegistration {
    pub fn new(operator: Hash<28>, epoch: u64) -> Self {
        Self {
            operator,
            epoch,
            prev_retiring_epoch: None,
            prev_deposit: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolDeRegistration {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.operator.as_slice()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            // save undo info
            self.prev_retiring_epoch = entity.retiring_epoch;
            self.prev_deposit = Some(entity.deposit);

            debug!(
                operator = hex::encode(self.operator),
                epoch = self.epoch,
                "retiring pool"
            );

            // apply changes
            entity.retiring_epoch = Some(self.epoch);
            entity.deposit = 0;
        }
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            entity.retiring_epoch = self.prev_retiring_epoch;
            entity.deposit = self.prev_deposit.unwrap_or(0);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolWrapUp {
    pub(crate) pool_hash: PoolHash,

    // undo: whole EpochValue snapshot captured pre-apply.
    pub(crate) prev_snapshot: Option<EpochValue<PoolSnapshot>>,
}

impl PoolWrapUp {
    pub fn new(pool_hash: PoolHash) -> Self {
        Self {
            pool_hash,
            prev_snapshot: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolWrapUp {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool_hash.as_slice()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");

        self.prev_snapshot = Some(entity.snapshot.clone());

        let snapshot = entity.snapshot.scheduled_or_default();

        snapshot.is_retired = true;
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");
        entity.snapshot = self.prev_snapshot.clone().expect("apply captured snapshot");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolTransition {
    pub(crate) pool: EntityKey,
    pub(crate) next_epoch: Epoch,

    // undo: save the whole EpochValue so rotation can be reversed in one shot.
    pub(crate) prev_snapshot: Option<EpochValue<PoolSnapshot>>,
}

impl PoolTransition {
    pub fn new(pool: EntityKey, next_epoch: Epoch) -> Self {
        Self {
            pool,
            next_epoch,
            prev_snapshot: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolTransition {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool.clone()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");

        self.prev_snapshot = Some(entity.snapshot.clone());

        entity.snapshot.default_transition(self.next_epoch);
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");
        entity.snapshot = self.prev_snapshot.clone().expect("apply captured snapshot");
    }
}

#[cfg(test)]
mod prop_tests {
    use super::*;
    use super::testing::any_pool_state;
    use crate::model::testing::{self as root, assert_delta_roundtrip};
    use crate::pallas_extras::testing::any_multi_era_pool_registration;
    use proptest::prelude::*;

    prop_compose! {
        fn any_pool_registration()(
            cert in any_multi_era_pool_registration(),
            slot in root::any_slot(),
            epoch in root::any_epoch(),
            pool_deposit in root::any_lovelace(),
        ) -> PoolRegistration {
            PoolRegistration::new(cert, slot, epoch, pool_deposit)
        }
    }

    prop_compose! {
        fn any_minted_blocks_inc()(
            operator in root::any_hash_28(),
            count in 1u32..100u32,
        ) -> MintedBlocksInc {
            MintedBlocksInc { operator, count }
        }
    }

    prop_compose! {
        fn any_pool_deregistration()(
            operator in root::any_hash_28(),
            epoch in root::any_epoch(),
        ) -> PoolDeRegistration {
            PoolDeRegistration::new(operator, epoch)
        }
    }

    prop_compose! {
        fn any_pool_wrap_up()(
            pool_hash in root::any_pool_hash(),
        ) -> PoolWrapUp {
            PoolWrapUp::new(pool_hash)
        }
    }

    // `PoolTransition::apply` expects `entity.snapshot.live` populated so the
    // `TransitionDefault` impl on `PoolSnapshot` can clone it forward.
    // Our `any_pool_state` always fills `live`, so this holds.
    prop_compose! {
        fn any_pool_transition()(
            pool in root::any_hash_28(),
            next_epoch in root::any_epoch(),
        ) -> PoolTransition {
            PoolTransition::new(dolos_core::EntityKey::from(pool.as_slice()), next_epoch)
        }
    }

    proptest! {
        #[test]
        fn pool_registration_roundtrip(
            entity in prop::option::of(any_pool_state()),
            delta in any_pool_registration(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn minted_blocks_inc_roundtrip(
            entity in prop::option::of(any_pool_state()),
            delta in any_minted_blocks_inc(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn pool_deregistration_roundtrip(
            entity in prop::option::of(any_pool_state()),
            delta in any_pool_deregistration(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn pool_wrap_up_roundtrip(
            entity in any_pool_state(),
            delta in any_pool_wrap_up(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn pool_transition_roundtrip(
            entity in any_pool_state(),
            delta in any_pool_transition(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }
    }
}
