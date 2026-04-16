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

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
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

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolRegistration {
    pub(crate) cert: MultiEraPoolRegistration,
    pub(crate) slot: BlockSlot,
    pub(crate) epoch: Epoch,

    // params
    pub(crate) pool_deposit: u64,

    // undo
    pub(crate) is_new: Option<bool>,
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
            is_new: None,
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

            // save undo info
            self.is_new = Some(true);

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

    fn undo(&self, _entity: &mut Option<PoolState>) {
        // Placeholder undo logic. Ensure this does not panic.
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

    fn undo(&self, _entity: &mut Option<PoolState>) {
        // no-op: undo not yet comprehensively implemented
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

    fn undo(&self, _entity: &mut Option<PoolState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolWrapUp {
    pub(crate) pool_hash: PoolHash,
}

impl PoolWrapUp {
    pub fn new(pool_hash: PoolHash) -> Self {
        Self { pool_hash }
    }
}

impl dolos_core::EntityDelta for PoolWrapUp {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool_hash.as_slice()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");

        let snapshot = entity.snapshot.scheduled_or_default();

        snapshot.is_retired = true;
    }

    fn undo(&self, _entity: &mut Option<PoolState>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolTransition {
    pub(crate) pool: EntityKey,
    pub(crate) next_epoch: Epoch,
}

impl PoolTransition {
    pub fn new(pool: EntityKey, next_epoch: Epoch) -> Self {
        Self { pool, next_epoch }
    }
}

impl dolos_core::EntityDelta for PoolTransition {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.pool.clone()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        let entity = entity.as_mut().expect("existing pool");

        // apply changes
        entity.snapshot.default_transition(self.next_epoch);
    }

    fn undo(&self, _entity: &mut Option<PoolState>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}
