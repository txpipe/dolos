use std::borrow::Cow;

use dolos_core::{NsKey, State3Error, StateDelta};
use pallas::codec::minicbor;
use pallas::crypto::hash::Hash;
use pallas::ledger::addresses::{ShelleyDelegationPart, StakePayload};
use pallas::ledger::{
    addresses::Address,
    primitives::StakeCredential,
    traverse::{MultiEraBlock, MultiEraCert, MultiEraInput, MultiEraOutput, MultiEraTx},
};
use tracing::debug;

use crate::model::FixedNamespace as _;
use crate::roll::CardanoDelta;
use crate::{model::AccountState, pallas_extras, roll::BlockVisitor};

#[derive(Debug, Clone)]
pub struct TrackSeenAddresses {
    cred: StakeCredential,
    full_address: Address,
    full_address_new: Option<bool>,
}

impl TrackSeenAddresses {
    pub fn new(cred: StakeCredential, full_address: Address) -> Self {
        Self {
            cred,
            full_address,
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

        let was_new = entity.seen_addresses.insert(self.full_address.to_vec());

        self.full_address_new = Some(was_new);
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();

        if self.full_address_new.unwrap_or(false) {
            entity.seen_addresses.remove(&self.full_address.to_vec());
        }
    }
}

#[derive(Debug, Clone)]
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
        entity.controlled_amount += self.amount;
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.controlled_amount -= self.amount;
    }
}

#[derive(Debug, Clone)]
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
        let _ = entity.controlled_amount.saturating_sub(self.amount);
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.controlled_amount += self.amount;
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
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
    }

    fn undo(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_default();
        entity.registered_at = self.prev_registered_at;
        entity.pool_id = self.prev_pool_id.clone();
    }
}

pub struct AccountVisitor<'a> {
    delta: &'a mut StateDelta<CardanoDelta>,
}

impl<'a> From<&'a mut StateDelta<CardanoDelta>> for AccountVisitor<'a> {
    fn from(delta: &'a mut StateDelta<CardanoDelta>) -> Self {
        Self { delta }
    }
}

impl AccountVisitor<'_> {
    fn extract_stake_cred(output: &MultiEraOutput) -> Option<(StakeCredential, Address)> {
        let full = output.address().ok()?;

        let stake = match &full {
            Address::Shelley(x) => match x.delegation() {
                ShelleyDelegationPart::Key(x) => Some(StakeCredential::AddrKeyhash(*x)),
                ShelleyDelegationPart::Script(x) => Some(StakeCredential::ScriptHash(*x)),
                _ => None,
            },
            Address::Stake(x) => match x.payload() {
                StakePayload::Stake(x) => Some(StakeCredential::AddrKeyhash(*x)),
                StakePayload::Script(x) => Some(StakeCredential::ScriptHash(*x)),
            },
            _ => None,
        }?;

        Some((stake, full))
    }
}

impl<'a> BlockVisitor for AccountVisitor<'a> {
    fn visit_input(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: &MultiEraInput,
        resolved: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((cred, _)) = Self::extract_stake_cred(resolved) else {
            return Ok(());
        };

        self.delta.add_delta(ControlledAmountDec {
            cred,
            amount: resolved.value().coin(),
        });

        Ok(())
    }

    fn visit_output(
        &mut self,
        _: &MultiEraBlock,
        _: &MultiEraTx,
        _: u32,
        output: &MultiEraOutput,
    ) -> Result<(), State3Error> {
        let Some((cred, full_address)) = Self::extract_stake_cred(output) else {
            return Ok(());
        };

        self.delta.add_delta(ControlledAmountInc {
            cred: cred.clone(),
            amount: output.value().coin(),
        });

        self.delta
            .add_delta(TrackSeenAddresses::new(cred, full_address));

        Ok(())
    }

    fn visit_cert(
        &mut self,
        block: &MultiEraBlock,
        _: &MultiEraTx,
        cert: &MultiEraCert,
    ) -> Result<(), State3Error> {
        if let Some(cred) = pallas_extras::cert_as_stake_registration(cert) {
            debug!("detected stake registration");

            self.delta
                .add_delta(StakeRegistration::new(cred, block.slot()));
        }

        if let Some(cert) = pallas_extras::cert_as_stake_delegation(cert) {
            debug!(%cert.pool, "detected stake delegation");

            self.delta
                .add_delta(StakeDelegation::new(cert.delegator, cert.pool));
        }

        if let Some(cred) = pallas_extras::cert_as_stake_deregistration(cert) {
            debug!("detected stake deregistration");

            self.delta
                .add_delta(StakeDeregistration::new(cred, block.slot()));
        }

        Ok(())
    }
}
