use dolos_core::batch::WorkDeltas;
use dolos_core::{ChainError, NsKey};
use pallas::codec::minicbor;
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::conway::DRep;
use pallas::ledger::{
    addresses::Address,
    primitives::StakeCredential,
    traverse::{MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraTx},
};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::model::FixedNamespace as _;
use crate::{model::AccountState, pallas_extras, roll::BlockVisitor};
use crate::{CardanoLogic, PParamsSet};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TrackSeenAddresses {
    cred: StakeCredential,
    full_address: Vec<u8>,
    full_address_new: Option<bool>,
}

impl TrackSeenAddresses {
    pub fn new(cred: StakeCredential, full_address: Address) -> Self {
        Self {
            cred,
            full_address: full_address.to_vec(),
            full_address_new: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledAmountInc {
    cred: StakeCredential,
    amount: u64,
}

impl dolos_core::EntityDelta for ControlledAmountInc {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.controlled_amount += self.amount;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.controlled_amount -= self.amount;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledAmountDec {
    cred: StakeCredential,
    amount: u64,
}

impl dolos_core::EntityDelta for ControlledAmountDec {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        // TODO: saturating sub shouldn't be necesary
        //entity.controlled_amount -= self.amount;
        entity.controlled_amount = entity.controlled_amount.saturating_sub(self.amount);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.controlled_amount += self.amount;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeRegistration {
    cred: StakeCredential,
    slot: u64,
    deposit: u64,

    // undo
    prev_registered_at: Option<u64>,
    prev_deposit: Option<u64>,
}

impl StakeRegistration {
    pub fn new(cred: StakeCredential, slot: u64, deposit: u64) -> Self {
        Self {
            cred,
            slot,
            deposit,
            prev_registered_at: None,
            prev_deposit: None,
        }
    }
}

impl dolos_core::EntityDelta for StakeRegistration {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_deposit = Some(entity.deposit);

        entity.registered_at = Some(self.slot);
        entity.deposit = self.deposit;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.registered_at = self.prev_registered_at;
        entity.deposit = self.prev_deposit.unwrap_or(0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDelegation {
    cred: StakeCredential,
    pool: Hash<28>,

    // undo
    prev_pool_id: Option<Vec<u8>>,
}

impl StakeDelegation {
    pub fn new(cred: StakeCredential, pool: Hash<28>) -> Self {
        Self {
            cred,
            pool,
            prev_pool_id: None,
        }
    }
}

impl dolos_core::EntityDelta for StakeDelegation {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_pool_id = entity.latest_pool.clone();

        entity.latest_pool = Some(self.pool.to_vec());
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.latest_pool = self.prev_pool_id.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteDelegation {
    cred: StakeCredential,
    drep: DRep,

    // undo
    prev_drep_id: Option<DRep>,
}

impl VoteDelegation {
    pub fn new(cred: StakeCredential, drep: DRep) -> Self {
        Self {
            cred,
            drep,
            prev_drep_id: None,
        }
    }
}

impl dolos_core::EntityDelta for VoteDelegation {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_drep_id = entity.latest_drep.clone();

        entity.latest_drep = Some(self.drep.clone());
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.latest_drep = self.prev_drep_id.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDeregistration {
    cred: StakeCredential,
    slot: u64,

    // undo
    prev_registered_at: Option<u64>,
    prev_pool_id: Option<Vec<u8>>,
    prev_drep: Option<DRep>,
    prev_deposit: Option<u64>,
}

impl StakeDeregistration {
    pub fn new(cred: StakeCredential, slot: u64) -> Self {
        Self {
            cred,
            slot,
            prev_registered_at: None,
            prev_pool_id: None,
            prev_drep: None,
            prev_deposit: None,
        }
    }
}

impl dolos_core::EntityDelta for StakeDeregistration {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_pool_id = entity.latest_pool.clone();
        self.prev_drep = entity.latest_drep.clone();
        self.prev_deposit = Some(entity.deposit);

        entity.registered_at = None;
        entity.latest_pool = None;
        entity.latest_drep = None;
        entity.deposit = 0;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.registered_at = self.prev_registered_at;
        entity.latest_pool = self.prev_pool_id.clone();
        entity.latest_drep = self.prev_drep.clone();
        entity.deposit = self.prev_deposit.unwrap_or(0);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalInc {
    cred: StakeCredential,
    amount: u64,
}

impl dolos_core::EntityDelta for WithdrawalInc {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.get_or_insert_default();
        entity.withdrawals_sum += self.amount;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.get_or_insert_default();
        entity.withdrawals_sum = entity.withdrawals_sum.saturating_sub(self.amount);
    }
}

#[derive(Default, Clone)]
pub struct AccountVisitor {
    deposit: Option<u64>,
}

impl BlockVisitor for AccountVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        pparams: &PParamsSet,
    ) -> Result<(), ChainError> {
        let deposit = pparams.ensure_key_deposit()?;
        self.deposit = Some(deposit);
        Ok(())
    }

    fn visit_input(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        let address = resolved.address().unwrap();

        let Some(cred) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

        deltas.add_for_entity(ControlledAmountDec {
            cred,
            amount: resolved.value().coin(),
        });

        Ok(())
    }

    fn visit_output(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &MultiEraOutput,
    ) -> Result<(), ChainError> {
        let address = output.address().unwrap();

        let Some(cred) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

        deltas.add_for_entity(ControlledAmountInc {
            cred: cred.clone(),
            amount: output.value().coin(),
        });

        Ok(())
    }

    fn visit_cert(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            let deposit = self.deposit.expect("value set in root");
            deltas.add_for_entity(StakeRegistration::new(cred, block.slot(), deposit));
        }

        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            deltas.add_for_entity(StakeDelegation::new(cert.delegator, cert.pool));
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            deltas.add_for_entity(StakeDeregistration::new(cred, block.slot()));
        }

        if let Some(cert) = pallas_extras::cert_as_vote_delegation(cert) {
            deltas.add_for_entity(VoteDelegation::new(cert.delegator, cert.drep));
        }

        Ok(())
    }

    fn visit_withdrawal(
        &mut self,
        deltas: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        account: &[u8],
        amount: u64,
    ) -> Result<(), ChainError> {
        let address = Address::from_bytes(account)?;

        let Some(cred) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

        deltas.add_for_entity(WithdrawalInc { cred, amount });

        Ok(())
    }
}
