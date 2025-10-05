use std::ops::Deref;

use dolos_core::batch::WorkDeltas;
use dolos_core::{BlockSlot, ChainError, NsKey};
use pallas::crypto::hash::{Hash, Hasher};
use pallas::ledger::primitives::Epoch;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx};
use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::model::FixedNamespace as _;
use crate::pallas_extras::MultiEraPoolRegistration;
use crate::{model::PoolState, pallas_extras, roll::BlockVisitor};
use crate::{CardanoLogic, EpochValue, PParamsSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolRegistration {
    cert: MultiEraPoolRegistration,
    slot: BlockSlot,
    epoch: Epoch,

    // params
    pool_deposit: u64,

    // undo
    prev_entity: Option<PoolState>,
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
            prev_entity: None,
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
        self.prev_entity = entity.clone();

        debug!(
            operator = hex::encode(self.cert.operator),
            "applying pool registration",
        );

        if let Some(entity) = entity {
            entity.vrf_keyhash = self.cert.vrf_keyhash;
            entity.reward_account = self.cert.reward_account.to_vec();
            entity.pool_owners = self.cert.pool_owners.clone();
            entity.relays = self.cert.relays.clone();
            entity.declared_pledge = self.cert.pledge;
            entity.margin_cost = self.cert.margin.clone();
            entity.fixed_cost = self.cert.cost;
            entity.metadata = self.cert.pool_metadata.clone();
        } else {
            let state = PoolState {
                register_slot: self.slot,
                vrf_keyhash: self.cert.vrf_keyhash,
                reward_account: self.cert.reward_account.to_vec(),
                pool_owners: self.cert.pool_owners.clone(),
                relays: self.cert.relays.clone(),
                declared_pledge: self.cert.pledge,
                margin_cost: self.cert.margin.clone(),
                fixed_cost: self.cert.cost,
                metadata: self.cert.pool_metadata.clone(),
                total_stake: EpochValue::new(0, self.epoch),
                blocks_minted_total: 0,
                blocks_minted_epoch: 0,
                retiring_epoch: None,
                is_retired: false,
                deposit: self.pool_deposit,
            };

            *entity = Some(state);
        }
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        *entity = self.prev_entity.clone();
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
            entity.blocks_minted_epoch += self.count;
        }
    }

    fn undo(&self, entity: &mut Option<PoolState>) {
        if let Some(entity) = entity {
            entity.blocks_minted_total = entity.blocks_minted_total.saturating_sub(self.count);
            entity.blocks_minted_epoch = entity.blocks_minted_epoch.saturating_sub(self.count);
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

            // TODO: should be debug
            warn!(operator = hex::encode(self.operator), "retiring pool");

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
