use std::ops::Deref;

use dolos_core::batch::WorkDeltas;
use dolos_core::{BlockSlot, ChainError, EntityKey, NsKey};
use pallas::codec::minicbor;
use pallas::crypto::hash::{Hash, Hasher};
use pallas::ledger::primitives::StakeCredential;
use pallas::ledger::traverse::{MultiEraBlock, MultiEraCert, MultiEraTx};
use serde::{Deserialize, Serialize};
use tracing::{debug, trace};

use crate::model::FixedNamespace as _;
use crate::pallas_extras::MultiEraPoolRegistration;
use crate::{model::PoolState, pallas_extras, roll::BlockVisitor};
use crate::{AccountState, CardanoLogic, PParamsSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolRegistration {
    slot: BlockSlot,
    cert: MultiEraPoolRegistration,

    // params
    pool_deposit: u64,

    // undo
    prev_entity: Option<PoolState>,
}

impl PoolRegistration {
    pub fn new(slot: BlockSlot, cert: MultiEraPoolRegistration, pool_deposit: u64) -> Self {
        Self {
            slot,
            cert,
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

        let entity = entity.get_or_insert_with(|| PoolState::new(self.slot, self.cert.vrf_keyhash));

        entity.vrf_keyhash = self.cert.vrf_keyhash;
        entity.reward_account = self.cert.reward_account.to_vec();
        entity.pool_owners = self.cert.pool_owners.clone();
        entity.relays = self.cert.relays.clone();
        entity.declared_pledge = self.cert.pledge;
        entity.margin_cost = self.cert.margin.clone();
        entity.fixed_cost = self.cert.cost;
        entity.metadata = self.cert.pool_metadata.clone();
        entity.deposit = self.pool_deposit;
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
pub struct PoolAccountDetected {
    operator: Hash<28>,
    reward_account: StakeCredential,

    // undo
    is_new: bool,
}

impl PoolAccountDetected {
    pub fn new(operator: Hash<28>, reward_account: StakeCredential) -> Self {
        Self {
            operator,
            reward_account,
            is_new: false,
        }
    }
}

impl dolos_core::EntityDelta for PoolAccountDetected {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let key = minicbor::to_vec(&self.reward_account).unwrap();
        NsKey::from((AccountState::NS, key))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        if entity.is_none() {
            self.is_new = true;

            if tracing::enabled!(tracing::Level::DEBUG) {
                let account_key = minicbor::to_vec(&self.reward_account).unwrap();
                let account_key = EntityKey::from(account_key);
                debug!(operator=%self.operator, account_key=%account_key, "initializing pool account");
            }

            *entity = Some(AccountState::default());
        } else {
            self.is_new = false;
            trace!(operator=%self.operator, "pool account already exists");
        }
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        if self.is_new {
            *entity = None;
        }
    }
}

#[derive(Default, Clone)]
pub struct PoolStateVisitor {
    pool_deposit: Option<u64>,
}

impl BlockVisitor for PoolStateVisitor {
    fn visit_root(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        pparams: &PParamsSet,
    ) -> Result<(), ChainError> {
        self.pool_deposit = Some(pparams.ensure_pool_deposit()?);

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
            let pool_deposit = self.pool_deposit.expect("value set in root");

            deltas.add_for_entity(PoolRegistration::new(
                block.slot(),
                cert.clone(),
                pool_deposit,
            ));

            // Reward accounts for pool don't need to go through the standard stake
            // registration process. This is why we need to track the account directly on
            // pool registration.

            let cred = pallas_extras::pool_reward_account(&cert.reward_account).unwrap();
            deltas.add_for_entity(PoolAccountDetected::new(cert.operator, cred));
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
