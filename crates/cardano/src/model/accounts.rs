use dolos_core::{BlockSlot, EntityKey, NsKey, TxOrder};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::{conway::DRep, Epoch, StakeCredential},
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{
    epoch_value::{EpochValue, TransitionDefault},
    eras::EraProtocol,
    pools::PoolHash,
    FixedNamespace as _,
};
use crate::{add, sub};

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct Stake {
    #[n(0)]
    pub utxo_sum: u64,

    #[n(1)]
    pub rewards_sum: u64,

    #[n(2)]
    pub withdrawals_sum: u64,

    #[n(3)]
    pub utxo_sum_at_pointer_addresses: u64,
}

impl Stake {
    pub fn total(&self) -> u64 {
        let mut out = self.utxo_sum;
        out = add!(out, self.rewards_sum);
        out = sub!(out, self.withdrawals_sum);

        out
    }

    pub fn total_pre_conway(&self) -> u64 {
        let mut out = self.utxo_sum;
        out = add!(out, self.utxo_sum_at_pointer_addresses);
        out = add!(out, self.rewards_sum);
        out = sub!(out, self.withdrawals_sum);

        out
    }

    pub fn total_for_era(&self, era: EraProtocol) -> u64 {
        match era {
            x if x < 9 => self.total_pre_conway(),
            _ => self.total(),
        }
    }

    pub fn withdrawable(&self) -> u64 {
        sub!(self.rewards_sum, self.withdrawals_sum)
    }
}

impl TransitionDefault for Stake {
    fn next_value(current: Option<&Self>) -> Option<Self> {
        current.cloned()
    }
}

// HACK: seems that encoding `Some(None)` to CBOR is a lossy operation, the
// decoding return None. To avoid dealing with this issue at the `minicbor`
// crate, we're creating this pseud-option enum to identify the existence or
// lack of delegation.
#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Serialize, Deserialize)]
pub enum PoolDelegation {
    #[n(0)]
    Pool(#[n(0)] PoolHash),

    #[n(1)]
    NotDelegated,
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Serialize, Deserialize)]
pub enum DRepDelegation {
    #[n(0)]
    Delegated(#[n(0)] DRep),

    #[n(1)]
    NotDelegated,
}

impl TransitionDefault for PoolDelegation {
    fn next_value(current: Option<&Self>) -> Option<Self> {
        current.cloned()
    }
}

impl TransitionDefault for DRepDelegation {
    fn next_value(current: Option<&Self>) -> Option<Self> {
        current.cloned()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode)]
pub struct AccountState {
    #[n(0)]
    pub registered_at: Option<u64>,

    #[n(1)]
    pub stake: EpochValue<Stake>,

    #[n(2)]
    pub pool: EpochValue<PoolDelegation>,

    #[n(3)]
    pub drep: EpochValue<DRepDelegation>,

    #[n(4)]
    pub vote_delegated_at: Option<(BlockSlot, TxOrder)>,

    #[n(5)]
    pub deregistered_at: Option<u64>,

    #[n(6)]
    pub credential: StakeCredential,

    #[n(7)]
    #[cbor(default)]
    pub retired_pool: Option<PoolHash>,
}

entity_boilerplate!(AccountState, "accounts");

impl AccountState {
    pub fn new(epoch: Epoch, credential: StakeCredential) -> Self {
        Self {
            credential,
            registered_at: None,
            stake: EpochValue::with_live(epoch, Default::default()),
            pool: EpochValue::new(epoch),
            drep: EpochValue::new(epoch),
            vote_delegated_at: None,
            deregistered_at: None,
            retired_pool: None,
        }
    }

    pub fn live_stake(&self) -> u64 {
        self.stake.live().map(|x| x.total()).unwrap_or_default()
    }

    pub fn active_stake(&self) -> u64 {
        self.stake.go().map(|x| x.total()).unwrap_or_default()
    }

    pub fn is_registered(&self) -> bool {
        match (self.registered_at, self.deregistered_at) {
            (Some(_), None) => true,
            (Some(start), Some(end)) => start >= end,
            (None, _) => false,
        }
    }

    /// Check if the account was registered at a specific slot.
    /// Used for RUPD-time filtering where we need registration status at the RUPD slot,
    /// not the current chain tip.
    ///
    /// Note: When an account deregisters, `registered_at` is cleared to `None`.
    /// So we handle the case where `deregistered_at` is set but `registered_at` is not:
    /// if the deregistration slot is after the target slot, the account was registered.
    pub fn is_registered_at(&self, slot: u64) -> bool {
        match (self.registered_at, self.deregistered_at) {
            // Never registered and never deregistered
            (None, None) => false,
            // Deregistered but registered_at was cleared - account was registered
            // until the deregistration slot. Check if target slot is before deregistration.
            (None, Some(dereg)) => dereg > slot,
            // Currently registered, never deregistered - check if registered by target slot
            (Some(reg), None) => reg <= slot,
            // Both set (re-registration case) - check if registered and not yet deregistered
            (Some(reg), Some(dereg)) => reg <= slot && dereg > slot,
        }
    }

    pub fn delegated_pool_at(&self, epoch: Epoch) -> Option<&PoolHash> {
        self.pool.snapshot_at(epoch).and_then(|x| match x {
            PoolDelegation::Pool(pool) => Some(pool),
            _ => None,
        })
    }

    pub fn delegated_pool_live(&self) -> Option<&PoolHash> {
        match self.pool.live() {
            Some(PoolDelegation::Pool(pool)) => Some(pool),
            _ => None,
        }
    }

    pub fn delegated_drep_at(&self, epoch: Epoch) -> Option<&DRep> {
        self.drep.snapshot_at(epoch).and_then(|x| match x {
            DRepDelegation::Delegated(drep) => Some(drep),
            _ => None,
        })
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledAmountInc {
    pub(crate) cred: StakeCredential,
    pub(crate) is_pointer: bool,
    pub(crate) amount: u64,
    pub(crate) epoch: Epoch,
}

impl dolos_core::EntityDelta for ControlledAmountInc {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.get_or_insert_with(|| AccountState::new(self.epoch, self.cred.clone()));

        let stake = entity.stake.unwrap_live_mut();

        if self.is_pointer {
            debug!(amount=%self.amount, "adding to pointer utxo sum");
            stake.utxo_sum_at_pointer_addresses =
                add!(stake.utxo_sum_at_pointer_addresses, self.amount);
        } else {
            stake.utxo_sum = add!(stake.utxo_sum, self.amount);
        }
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // TODO: implement undo
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledAmountDec {
    pub(crate) cred: StakeCredential,
    pub(crate) is_pointer: bool,
    pub(crate) amount: u64,
}

impl dolos_core::EntityDelta for ControlledAmountDec {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        let stake = entity.stake.unwrap_live_mut();

        if self.is_pointer {
            stake.utxo_sum_at_pointer_addresses =
                sub!(stake.utxo_sum_at_pointer_addresses, self.amount);
        } else {
            stake.utxo_sum = sub!(stake.utxo_sum, self.amount);
        }
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeRegistration {
    pub(crate) cred: StakeCredential,
    pub(crate) slot: u64,
    pub(crate) epoch: Epoch,
    pub(crate) deposit: u64,

    // undo
    pub(crate) prev_registered_at: Option<u64>,
    pub(crate) prev_deregistered_at: Option<u64>,
    pub(crate) prev_deposit: Option<u64>,
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

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDelegation {
    pub(crate) cred: StakeCredential,
    pub(crate) pool: Hash<28>,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_pool: Option<PoolDelegation>,
    pub(crate) prev_retired_pool: Option<PoolHash>,
}

impl StakeDelegation {
    pub fn new(cred: StakeCredential, pool: Hash<28>, epoch: Epoch) -> Self {
        Self {
            cred,
            pool,
            epoch,
            prev_pool: None,
            prev_retired_pool: None,
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
        self.prev_retired_pool = entity.retired_pool;

        // apply changes
        entity
            .pool
            .replace(PoolDelegation::Pool(self.pool), self.epoch);
        entity.retired_pool = None;
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteDelegation {
    pub(crate) cred: StakeCredential,
    pub(crate) drep: DRep,
    pub(crate) vote_delegated_at: (BlockSlot, TxOrder),
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_drep: Option<DRepDelegation>,
    pub(crate) prev_vote_delegated_at: Option<(BlockSlot, TxOrder)>,
}

impl VoteDelegation {
    pub fn new(
        cred: StakeCredential,
        drep: DRep,
        slot: BlockSlot,
        order: TxOrder,
        epoch: Epoch,
    ) -> Self {
        Self {
            cred,
            drep,
            vote_delegated_at: (slot, order),
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
        entity
            .drep
            .replace(DRepDelegation::Delegated(self.drep.clone()), self.epoch);
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDeregistration {
    pub(crate) cred: StakeCredential,
    pub(crate) slot: u64,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_registered_at: Option<u64>,
    pub(crate) prev_deregistered_at: Option<u64>,
    pub(crate) prev_pool: Option<PoolDelegation>,
    pub(crate) prev_drep: Option<DRepDelegation>,
    pub(crate) prev_deposit: Option<u64>,
    pub(crate) prev_retired_pool: Option<PoolHash>,
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
            prev_retired_pool: None,
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
        self.prev_retired_pool = entity.retired_pool;

        // TODO: understand if we should keep the registered_at value even if the
        // account is deregistered
        entity.registered_at = None;
        entity.deregistered_at = Some(self.slot);
        entity
            .pool
            .replace(PoolDelegation::NotDelegated, self.epoch);
        entity.retired_pool = None;

        entity
            .drep
            .replace(DRepDelegation::NotDelegated, self.epoch);
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalInc {
    pub(crate) cred: StakeCredential,
    pub(crate) amount: u64,
}

impl dolos_core::EntityDelta for WithdrawalInc {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.cred).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        let stake = entity.stake.unwrap_live_mut();
        stake.withdrawals_sum = add!(stake.withdrawals_sum, self.amount);
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDelegatorRetire {
    pub(crate) delegator: EntityKey,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_pool: Option<PoolDelegation>,
}

impl PoolDelegatorRetire {
    pub fn new(delegator: EntityKey, epoch: Epoch) -> Self {
        Self {
            delegator,
            epoch,
            prev_pool: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolDelegatorRetire {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.delegator.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("account should exist");

        debug!(delegator=%self.delegator, "retiring pool delegator");

        // save undo info
        self.prev_pool = entity.pool.live().cloned();

        // apply changes
        entity
            .pool
            .schedule(self.epoch, Some(PoolDelegation::NotDelegated));

        let Some(PoolDelegation::Pool(pool)) = self.prev_pool else {
            unreachable!("account delegated to pool")
        };
        entity.retired_pool = Some(pool);
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepDelegatorDrop {
    pub(crate) delegator: EntityKey,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_drep: Option<DRep>,
}

impl DRepDelegatorDrop {
    pub fn new(delegator: EntityKey, epoch: Epoch) -> Self {
        Self {
            delegator,
            epoch,
            prev_drep: None,
        }
    }
}

impl dolos_core::EntityDelta for DRepDelegatorDrop {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.delegator.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(delegator=%self.delegator, "dropping drep delegator");

        // apply changes
        entity
            .drep
            .schedule(self.epoch, Some(DRepDelegation::NotDelegated));
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreasuryWithdrawal {
    pub(crate) account: StakeCredential,
    pub(crate) amount: u64,
}

impl TreasuryWithdrawal {
    pub fn new(account: StakeCredential, amount: u64) -> Self {
        Self { account, amount }
    }
}

impl dolos_core::EntityDelta for TreasuryWithdrawal {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.account).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(account=?self.account, amount=%self.amount, "applying treasury withdrawal");

        let stake = entity.stake.unwrap_live_mut();
        stake.rewards_sum = add!(stake.rewards_sum, self.amount);
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDepositRefund {
    pub(crate) pool_deposit: u64,
    pub(crate) account: StakeCredential,
}

impl PoolDepositRefund {
    pub fn new(pool_deposit: u64, account: StakeCredential) -> Self {
        Self {
            pool_deposit,
            account,
        }
    }
}

impl dolos_core::EntityDelta for PoolDepositRefund {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.account).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        let stake = entity.stake.scheduled_or_default();

        stake.rewards_sum += self.pool_deposit;
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalDepositRefund {
    pub(crate) proposal_deposit: u64,
    pub(crate) account: StakeCredential,
}

impl ProposalDepositRefund {
    pub fn new(proposal_deposit: u64, account: StakeCredential) -> Self {
        Self {
            proposal_deposit,
            account,
        }
    }
}

impl dolos_core::EntityDelta for ProposalDepositRefund {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        let enc = minicbor::to_vec(&self.account).unwrap();
        NsKey::from((AccountState::NS, enc))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        tracing::debug!(cred=?self.account, deposit=%self.proposal_deposit, "applying proposal deposit refund");

        let stake = entity.stake.scheduled_or_default();

        stake.rewards_sum += self.proposal_deposit;
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignRewards {
    pub(crate) account: EntityKey,
    pub(crate) reward: u64,
}

impl AssignRewards {
    pub fn new(account: EntityKey, reward: u64) -> Self {
        Self { account, reward }
    }
}

impl dolos_core::EntityDelta for AssignRewards {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(account=%self.account, "assigning rewards");

        let stake = entity.stake.unwrap_live_mut();
        stake.rewards_sum = add!(stake.rewards_sum, self.reward);
    }

    fn undo(&self, _entity: &mut Option<Self::Entity>) {
        // no-op: undo not yet comprehensively implemented
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTransition {
    pub(crate) account: EntityKey,
    pub(crate) next_epoch: Epoch,
}

impl AccountTransition {
    pub fn new(account: EntityKey, next_epoch: Epoch) -> Self {
        Self {
            account,
            next_epoch,
        }
    }
}

impl dolos_core::EntityDelta for AccountTransition {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::from((AccountState::NS, self.account.clone()))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        // apply changes
        entity.stake.default_transition(self.next_epoch);
        entity.pool.default_transition(self.next_epoch);
        entity.drep.default_transition(self.next_epoch);
    }

    fn undo(&self, _entity: &mut Option<AccountState>) {
        // Placeholder undo logic. Ensure this does not panic.
    }
}
