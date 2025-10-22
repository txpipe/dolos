use dolos_core::batch::WorkDeltas;
use dolos_core::{BlockSlot, ChainError, NsKey};
use pallas::codec::minicbor;
use pallas::crypto::hash::Hash;
use pallas::ledger::primitives::conway::DRep;
use pallas::ledger::primitives::Epoch;
use pallas::ledger::{
    addresses::Address,
    primitives::StakeCredential,
    traverse::{MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraTx},
};
use serde::{Deserialize, Serialize};

use crate::model::FixedNamespace as _;
use crate::{model::AccountState, pallas_extras, roll::BlockVisitor};
use crate::{CardanoLogic, DRepDelegation, PParamsSet, PoolDelegation};

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
    epoch: Epoch,
}

impl dolos_core::EntityDelta for ControlledAmountInc {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_with(|| AccountState::new(self.epoch, self.cred.clone()));

        entity.stake.unwrap_live_mut().utxo_sum += self.amount;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        entity.stake.unwrap_live_mut().utxo_sum -= self.amount;
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
        let entity = entity.as_mut().expect("existing account");

        entity.stake.unwrap_live_mut().utxo_sum -= self.amount;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        entity.stake.unwrap_live_mut().utxo_sum += self.amount;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeRegistration {
    cred: StakeCredential,
    slot: u64,
    epoch: Epoch,
    deposit: u64,

    // undo
    prev_registered_at: Option<u64>,
    prev_deregistered_at: Option<u64>,
    prev_deposit: Option<u64>,
}

impl StakeRegistration {
    pub fn new(cred: StakeCredential, slot: u64, epoch: Epoch, deposit: u64) -> Self {
        Self {
            cred,
            slot,
            epoch,
            deposit,
            prev_registered_at: None,
            prev_deregistered_at: None,
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
        let entity = entity.get_or_insert_with(|| AccountState::new(self.epoch, self.cred.clone()));

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_deregistered_at = entity.deregistered_at;

        tracing::debug!(
            slot = self.slot,
            account = hex::encode(minicbor::to_vec(&self.cred).unwrap()),
            "applying registration"
        );

        entity.registered_at = Some(self.slot);
        entity.deregistered_at = None;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        entity.registered_at = self.prev_registered_at;
        entity.deregistered_at = self.prev_deregistered_at;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDelegation {
    cred: StakeCredential,
    pool: Hash<28>,
    epoch: Epoch,

    // undo
    prev_pool: Option<PoolDelegation>,
}

impl StakeDelegation {
    pub fn new(cred: StakeCredential, pool: Hash<28>, epoch: Epoch) -> Self {
        Self {
            cred,
            pool,
            epoch,
            prev_pool: None,
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
        let entity = entity.as_mut().expect("existing account");

        tracing::debug!(pool = hex::encode(self.pool), "applying delegation");

        // save undo
        self.prev_pool = entity.pool.live().cloned();

        // apply changes
        entity
            .pool
            .replace(PoolDelegation::Pool(self.pool.clone()), self.epoch);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        entity.pool.reset(self.prev_pool.clone());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteDelegation {
    cred: StakeCredential,
    drep: DRep,
    vote_delegated_at: BlockSlot,
    epoch: Epoch,

    // undo
    prev_drep: Option<DRepDelegation>,
    prev_vote_delegated_at: Option<BlockSlot>,
}

impl VoteDelegation {
    pub fn new(
        cred: StakeCredential,
        drep: DRep,
        vote_delegated_at: BlockSlot,
        epoch: Epoch,
    ) -> Self {
        Self {
            cred,
            drep,
            vote_delegated_at,
            epoch,
            prev_drep: None,
            prev_vote_delegated_at: None,
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
        let entity = entity.as_mut().expect("existing account");

        // save undo
        self.prev_drep = entity.drep.live().cloned();
        self.prev_vote_delegated_at = entity.vote_delegated_at;

        // apply changes
        entity.vote_delegated_at = Some(self.vote_delegated_at);
        entity.drep.replace(Some(self.drep.clone()), self.epoch);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        entity.vote_delegated_at = self.prev_vote_delegated_at;
        entity.drep.reset(self.prev_drep.clone());
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDeregistration {
    cred: StakeCredential,
    slot: u64,
    epoch: Epoch,

    // undo
    prev_registered_at: Option<u64>,
    prev_deregistered_at: Option<u64>,
    prev_pool: Option<PoolDelegation>,
    prev_drep: Option<DRepDelegation>,
    prev_deposit: Option<u64>,
}

impl StakeDeregistration {
    pub fn new(cred: StakeCredential, slot: u64, epoch: Epoch) -> Self {
        Self {
            cred,
            slot,
            epoch,
            prev_registered_at: None,
            prev_deregistered_at: None,
            prev_pool: None,
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
        let entity = entity.as_mut().expect("existing account");

        tracing::debug!(slot = self.slot, "applying deregistration");

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_deregistered_at = entity.deregistered_at;
        self.prev_pool = entity.pool.live().cloned();
        self.prev_drep = entity.drep.live().cloned();

        // TODO: understand if we should keep the registered_at value even if the
        // account is deregistered
        entity.registered_at = None;

        entity.deregistered_at = Some(self.slot);

        entity.pool.clear(self.epoch);
        entity.drep.clear(self.epoch);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        todo!()
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
        let entity = entity.as_mut().expect("existing account");

        entity.stake.unwrap_live_mut().withdrawals_sum += self.amount;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        entity.stake.unwrap_live_mut().withdrawals_sum -= self.amount;
    }
}

#[derive(Default, Clone)]
pub struct AccountVisitor {
    deposit: Option<u64>,
    epoch: Option<Epoch>,
}

impl BlockVisitor for AccountVisitor {
    fn visit_root(
        &mut self,
        _: &mut WorkDeltas<CardanoLogic>,
        _: &MultiEraBlock,
        pparams: &PParamsSet,
        epoch: Epoch,
    ) -> Result<(), ChainError> {
        self.deposit = pparams.ensure_key_deposit().ok();
        self.epoch = Some(epoch);
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
        let address = output.address().expect("valid address");
        let epoch = self.epoch.expect("value set in root");

        let Some(cred) = pallas_extras::address_as_stake_cred(&address) else {
            return Ok(());
        };

        deltas.add_for_entity(ControlledAmountInc {
            cred: cred.clone(),
            amount: output.value().coin(),
            epoch,
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
        let epoch = self.epoch.expect("value set in root");

        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            let deposit = self.deposit.expect("value set in root");
            let epoch = self.epoch.expect("value set in root");
            deltas.add_for_entity(StakeRegistration::new(cred, block.slot(), epoch, deposit));
        }

        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            deltas.add_for_entity(StakeDelegation::new(cert.delegator, cert.pool, epoch));
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            deltas.add_for_entity(StakeDeregistration::new(cred, block.slot(), epoch));
        }

        if let Some(cert) = pallas_extras::cert_as_vote_delegation(cert) {
            deltas.add_for_entity(VoteDelegation::new(
                cert.delegator,
                cert.drep,
                block.slot(),
                epoch,
            ));
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
