use std::borrow::Cow;

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
use tracing::debug;

use crate::model::FixedNamespace as _;
use crate::CardanoLogic;
use crate::{model::AccountState, pallas_extras, roll::BlockVisitor};

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

impl dolos_core::EntityDelta for TrackSeenAddresses {
    type Entity = AccountState;

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        let was_new = entity.seen_addresses.insert(self.full_address.clone());

        self.full_address_new = Some(was_new);
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        if self.full_address_new.unwrap_or(false) {
            entity.seen_addresses.remove(&self.full_address);
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

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.live_stake += self.amount;
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.live_stake -= self.amount;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledAmountDec {
    cred: StakeCredential,
    amount: u64,
}

impl dolos_core::EntityDelta for ControlledAmountDec {
    type Entity = AccountState;

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        // TODO: saturating sub shouldn't be necesary
        //entity.controlled_amount -= self.amount;
        entity.live_stake = entity.live_stake.saturating_sub(self.amount);
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.live_stake += self.amount;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeRegistration {
    cred: StakeCredential,
    slot: u64,

    // undo
    prev_registered_at: Option<u64>,
}

impl StakeRegistration {
    pub fn new(cred: StakeCredential, slot: u64) -> Self {
        Self {
            cred,
            slot,
            prev_registered_at: None,
        }
    }
}

impl dolos_core::EntityDelta for StakeRegistration {
    type Entity = AccountState;

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        self.prev_registered_at = entity.registered_at;
        entity.registered_at = Some(self.slot);
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.registered_at = self.prev_registered_at;
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

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_pool_id = entity.pool_id.clone();

        entity.pool_id = Some(self.pool.to_vec());
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.pool_id = self.prev_pool_id.clone();
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

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_drep_id = entity.drep.clone();

        entity.drep = Some(self.drep.clone());
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.drep = self.prev_drep_id.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDeregistration {
    cred: StakeCredential,
    slot: u64,

    // undo
    prev_registered_at: Option<u64>,
    prev_pool_id: Option<Vec<u8>>,
}

impl StakeDeregistration {
    pub fn new(cred: StakeCredential, slot: u64) -> Self {
        Self {
            cred,
            slot,
            prev_registered_at: None,
            prev_pool_id: None,
        }
    }
}

impl dolos_core::EntityDelta for StakeDeregistration {
    type Entity = AccountState;

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_pool_id = entity.pool_id.clone();

        entity.registered_at = None;
        entity.pool_id = None;
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.registered_at = self.prev_registered_at;
        entity.pool_id = self.prev_pool_id.clone();
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalInc {
    cred: StakeCredential,
    amount: u64,
}

impl dolos_core::EntityDelta for WithdrawalInc {
    type Entity = AccountState;

    fn key(&self) -> Cow<'_, NsKey> {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        Cow::Owned(NsKey::from((AccountState::NS, enc)))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.get_or_insert_default();
        entity.withdrawals_sum += self.amount;
    }

    fn undo(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.get_or_insert_default();
        entity.withdrawals_sum = entity.withdrawals_sum.saturating_sub(self.amount);
    }
}

pub struct AccountVisitor;

impl BlockVisitor for AccountVisitor {
    fn visit_input(
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

        deltas.add_for_entity(TrackSeenAddresses::new(cred, address));

        Ok(())
    }

    fn visit_cert(
        deltas: &mut WorkDeltas<CardanoLogic>,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), ChainError> {
        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            debug!("detected stake registration");

            deltas.add_for_entity(StakeRegistration::new(cred, block.slot()));
        }

        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            debug!(%cert.pool, "detected stake delegation");

            deltas.add_for_entity(StakeDelegation::new(cert.delegator, cert.pool));
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            debug!("detected stake deregistration");

            deltas.add_for_entity(StakeDeregistration::new(cred, block.slot()));
        }

        if let Some(cert) = pallas_extras::cert_as_vote_delegation(cert) {
            debug!("detected vote delegation");

            deltas.add_for_entity(VoteDelegation::new(cert.delegator, cert.drep));
        }

        Ok(())
    }

    fn visit_withdrawal(
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
