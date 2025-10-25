use std::ops::Deref;

use dolos_core::batch::WorkDeltas;
use dolos_core::{BlockSlot, ChainError, NsKey};
use pallas::crypto::hash::{Hash, Hasher};
use pallas::ledger::primitives::Epoch;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx};
use serde::{Deserialize, Serialize};
use tracing::debug;

use crate::model::FixedNamespace as _;
use crate::pallas_extras::MultiEraPoolRegistration;
use crate::{model::PoolState, pallas_extras, roll::BlockVisitor};
use crate::{CardanoLogic, EpochValue, PParamsSet, PoolParams, PoolSnapshot};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolRegistration {
    cert: MultiEraPoolRegistration,
    slot: BlockSlot,
    epoch: Epoch,

    // params
    pool_deposit: u64,

    // undo
    is_new: Option<bool>,
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
                entity.snapshot.replace(
                    PoolSnapshot {
                        is_retired: false,
                        is_new: true,
                        blocks_minted: 0,
                        params: self.cert.clone().into(),
                    },
                    self.epoch,
                );
            } else {
                entity.snapshot.schedule(
                    self.epoch,
                    Some(PoolSnapshot {
                        is_retired: false,
                        blocks_minted: 0,
                        params: self.cert.clone().into(),
                        is_new: false,
                    }),
                );
            }

            // please note that for updates to existing pools we are scheduling the change
            // for the next epoch. This differs from the behavior of new pools where the
            // change applies to the live epoch.
            entity.snapshot.schedule(
                self.epoch,
                Some(PoolSnapshot {
                    is_retired: false,
                    blocks_minted: 0,
                    params: self.cert.clone().into(),
                    is_new: false,
                }),
            );
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
        todo!()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MintedBlocksInc {
    operator: Hash<28>,
    count: u32,
}

impl dolos_core::EntityDelta for MintedBlocksInc {
    type Entity = PoolState;

    fn key(&self) -> NsKey {
        NsKey::from((PoolState::NS, self.operator.as_slice()))
    }

    fn apply(&mut self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            entity.blocks_minted_total += self.count;
            entity.snapshot.unwrap_live_mut().blocks_minted += self.count;
        }
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            entity.blocks_minted_total -= self.count;
            entity.snapshot.unwrap_live_mut().blocks_minted -= self.count;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDeRegistration {
    operator: Hash<28>,
    epoch: u64,

    // undo
    prev_retiring_epoch: Option<u64>,
    prev_deposit: Option<u64>,
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

#[derive(Default, Clone)]
pub struct PoolStateVisitor {
    epoch: Option<Epoch>,
    deposit: Option<u64>,
}

impl BlockVisitor for PoolStateVisitor {
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        pparams: &PParamsSet,
        epoch: Epoch,
    ) -> Result<(), ChainError> {
        self.epoch = Some(epoch);
        self.deposit = pparams.ensure_pool_deposit().ok();

        if let Some(key) = block.header().issuer_vkey() {
            let operator: Hash<28> = Hasher::<224>::hash(key);
            deltas.add_for_entity(MintedBlocksInc { operator, count: 1 });
        }

        Ok(())
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        if let Some(cert) = pallas_extras::cert_as_pool_registration(cert) {
            let epoch = self.epoch.expect("value set in root");
            let deposit = self.deposit.expect("value set in root");

            deltas.add_for_entity(PoolRegistration::new(
                cert.clone(),
                block.slot(),
                epoch,
                deposit,
            ));
        }

        match cert {
            MultiEraCert::AlonzoCompatible(cow) => {
                if let pallas::ledger::primitives::alonzo::Certificate::PoolRetirement(
                    operator,
                    epoch,
                ) = cow.deref().deref()
                {
                    deltas.add_for_entity(PoolDeRegistration::new(*operator, *epoch));
                }
            }
            MultiEraCert::Conway(cow) => {
                if let pallas::ledger::primitives::conway::Certificate::PoolRetirement(
                    operator,
                    epoch,
                ) = cow.deref().deref()
                {
                    deltas.add_for_entity(PoolDeRegistration::new(*operator, *epoch));
                }
            }
            _ => {}
        };

        Ok(())
    }
}
