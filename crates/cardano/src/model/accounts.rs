use dolos_core::{
    keys::exact, BlockSlot, FixedKey, KeyBytes, KeyCodec, KeyError, KeyPrefix, NsKey, RangeOf,
    TxOrder,
};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::{conway::DRep, Epoch, StakeCredential},
};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{
    epoch_value::{EpochValue, TransitionDefault},
    eras::EraKey,
    pools::PoolHash,
};
use crate::{
    add,
    shard::{validate_total_shards, PREFIX_SPACE},
    sub,
};

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Serialize, Deserialize, Default)]
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

    pub fn total_for_era(&self, era: EraKey) -> u64 {
        match era {
            x if x < 9 => self.total_pre_conway(),
            _ => self.total(),
        }
    }

    pub fn withdrawable(&self) -> u64 {
        sub!(self.rewards_sum, self.withdrawals_sum)
    }

    /// No balance: no UTxO value, at a pointer address or otherwise, and no
    /// reward balance.
    pub fn is_empty(&self) -> bool {
        self.utxo_sum == 0 && self.utxo_sum_at_pointer_addresses == 0 && self.withdrawable() == 0
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

/// The variant of a stake credential, keyed by the CBOR header its
/// [`AccountKey`]s start with: `0x82 ‖ variant ‖ 0x58 0x1c`.
///
/// The header carries no entropy, so it is the prefix the account shards
/// split under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CredentialKind {
    KeyHash,
    Script,
}

impl CredentialKind {
    const ARRAY: u8 = 0x82;
    const BYTES_28: [u8; 2] = [0x58, 0x1c];

    fn variant(self) -> u8 {
        match self {
            Self::KeyHash => 0,
            Self::Script => 1,
        }
    }

    pub fn of(credential: &StakeCredential) -> Self {
        match credential {
            StakeCredential::AddrKeyhash(_) => Self::KeyHash,
            StakeCredential::ScriptHash(_) => Self::Script,
        }
    }

    fn credential(self, hash: Hash<28>) -> StakeCredential {
        match self {
            Self::KeyHash => StakeCredential::AddrKeyhash(hash),
            Self::Script => StakeCredential::ScriptHash(hash),
        }
    }
}

impl KeyCodec for CredentialKind {
    fn encode_key(&self, out: &mut KeyBytes) {
        out.extend_from_slice(&[
            Self::ARRAY,
            self.variant(),
            Self::BYTES_28[0],
            Self::BYTES_28[1],
        ]);
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        match exact::<4>(bytes)? {
            [Self::ARRAY, 0, 0x58, 0x1c] => Ok(Self::KeyHash),
            [Self::ARRAY, 1, 0x58, 0x1c] => Ok(Self::Script),
            header => Err(KeyError::Malformed(format!(
                "stake credential header {}",
                hex::encode(header)
            ))),
        }
    }
}

impl FixedKey for CredentialKind {
    const LEN: usize = 4;
}

impl KeyPrefix<AccountKey> for CredentialKind {}

/// A stake credential, keyed by its CBOR encoding: the
/// [`CredentialKind`] header and the 28-byte hash.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct AccountKey(pub StakeCredential);

impl AccountKey {
    pub fn credential(&self) -> &StakeCredential {
        &self.0
    }

    /// The two ranges, one per credential variant, of shard `index` of
    /// `total`. Shards split on the first byte of the hash, the first byte
    /// after the header, so `total` must divide 256.
    ///
    /// Panics on an invalid split in every build profile: `total` can come
    /// from persisted progress, and a broken partition would skip or repeat
    /// accounts silently.
    pub fn shard_ranges(index: u32, total: u32) -> Vec<RangeOf<AccountKey>> {
        if let Err(e) = validate_total_shards(total) {
            panic!("shard_ranges: {e}");
        }

        assert!(
            index < total,
            "shard_ranges: shard index ({index}) must be < total shards ({total})",
        );

        let step = PREFIX_SPACE / total;
        let bucket = |kind: CredentialKind, first: u32| {
            let mut hash = [0u8; 28];
            hash[0] = first as u8;
            AccountKey(kind.credential(hash.into()))
        };

        [CredentialKind::KeyHash, CredentialKind::Script]
            .into_iter()
            .map(|kind| {
                let range = RangeOf::prefix(&kind).starting_at(&bucket(kind, index * step));

                if index + 1 == total {
                    range
                } else {
                    range.ending_before(&bucket(kind, (index + 1) * step))
                }
            })
            .collect()
    }
}

impl From<StakeCredential> for AccountKey {
    fn from(value: StakeCredential) -> Self {
        Self(value)
    }
}

impl From<&StakeCredential> for AccountKey {
    fn from(value: &StakeCredential) -> Self {
        Self(value.clone())
    }
}

impl From<AccountKey> for StakeCredential {
    fn from(value: AccountKey) -> Self {
        value.0
    }
}

impl KeyCodec for AccountKey {
    fn encode_key(&self, out: &mut KeyBytes) {
        CredentialKind::of(&self.0).encode_key(out);

        match &self.0 {
            StakeCredential::AddrKeyhash(hash) | StakeCredential::ScriptHash(hash) => {
                out.extend_from_slice(hash.as_slice())
            }
        }
    }

    fn decode_key(bytes: &[u8]) -> Result<Self, KeyError> {
        let (kind, hash) = <(CredentialKind, [u8; 28])>::decode_key(bytes)?;
        Ok(Self(kind.credential(hash.into())))
    }
}

impl FixedKey for AccountKey {
    const LEN: usize = 32;
}

display_as_stored!(AccountKey);

entity_boilerplate!(AccountState, "accounts", state = AccountKey);

#[cfg(test)]
pub(crate) mod testing {
    use super::*;
    use crate::model::epoch_value::testing::any_epoch_value;
    use crate::model::testing as root;
    use proptest::prelude::*;

    prop_compose! {
        pub fn any_stake()(
            utxo_sum in root::any_lovelace(),
            rewards_sum in root::any_lovelace(),
            withdrawals_sum in 0u64..1u64,
            utxo_sum_at_pointer_addresses in root::any_lovelace(),
        ) -> Stake {
            // withdrawals_sum constrained to 0 so `rewards_sum - withdrawals_sum` in
            // `Stake::total()` never underflows.
            Stake {
                utxo_sum,
                rewards_sum,
                withdrawals_sum,
                utxo_sum_at_pointer_addresses,
            }
        }
    }

    pub fn any_pool_delegation() -> impl Strategy<Value = PoolDelegation> {
        prop_oneof![
            root::any_pool_hash().prop_map(PoolDelegation::Pool),
            Just(PoolDelegation::NotDelegated),
        ]
    }

    pub fn any_drep_delegation() -> impl Strategy<Value = DRepDelegation> {
        prop_oneof![
            root::any_drep().prop_map(DRepDelegation::Delegated),
            Just(DRepDelegation::NotDelegated),
        ]
    }

    prop_compose! {
        pub fn any_account_state()(
            credential in root::any_stake_credential(),
            registered_at in prop::option::of(root::any_slot()),
            deregistered_at in prop::option::of(root::any_slot()),
            epoch in root::any_epoch(),
            stake in any_epoch_value(any_stake().boxed()),
            pool in any_epoch_value(any_pool_delegation().boxed()),
            drep in any_epoch_value(any_drep_delegation().boxed()),
            vote_delegated_at in prop::option::of((root::any_slot(), root::any_tx_order())),
            retired_pool in prop::option::of(root::any_pool_hash()),
        ) -> AccountState {
            // ESTART rotates stake/pool/drep in lockstep, so a healthy account
            // has all three at the same epoch. The `strict` feature asserts
            // this; rebase the independently drawn values onto one epoch.
            AccountState {
                credential,
                registered_at,
                deregistered_at,
                stake: crate::model::epoch_value::testing::rebase(stake, epoch),
                pool: crate::model::epoch_value::testing::rebase(pool, epoch),
                drep: crate::model::epoch_value::testing::rebase(drep, epoch),
                vote_delegated_at,
                retired_pool,
            }
        }
    }
}

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

    /// The snapshot rotation ESTART applies as `next_epoch` opens: stake,
    /// pool and drep delegation each move one epoch forward.
    pub fn rotate(&mut self, next_epoch: Epoch) {
        self.stake.default_transition(next_epoch);
        self.pool.default_transition(next_epoch);
        self.drep.default_transition(next_epoch);
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
    /// Used for RUPD-time filtering where we need registration status at the
    /// RUPD slot, not the current chain tip.
    ///
    /// Note: When an account deregisters, `registered_at` is cleared to `None`.
    /// So we handle the case where `deregistered_at` is set but `registered_at`
    /// is not: if the deregistration slot is after the target slot, the
    /// account was registered.
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

    pub fn delegated_drep_live(&self) -> Option<&DRep> {
        match self.drep.live() {
            Some(DRepDelegation::Delegated(drep)) => Some(drep),
            _ => None,
        }
    }

    /// Whether ESTART deletes the row: the account is unregistered, holds no
    /// balance, every stake snapshot is empty and every delegation snapshot
    /// undelegated, and no retired pool is left to serve. Registration
    /// history does not keep a row alive: [`Self::remainder`] keeps what the
    /// API serves of it.
    pub fn is_dead(&self) -> bool {
        !self.is_registered()
            && self.retired_pool.is_none()
            && every_snapshot(&self.stake, |x| x.is_none_or(Stake::is_empty))
            && every_snapshot(&self.pool, |x| {
                matches!(x, None | Some(PoolDelegation::NotDelegated))
            })
            && every_snapshot(&self.drep, |x| {
                matches!(x, None | Some(DRepDelegation::NotDelegated))
            })
    }

    /// What the row leaves in `pruned-accounts` when ESTART deletes it, or
    /// `None` when it carries no history: no lifetime rewards and no
    /// deregistration.
    pub fn remainder(&self) -> Option<PrunedAccountState> {
        let remainder = PrunedAccountState {
            lifetime_rewards: self.stake.live().map(|x| x.rewards_sum).unwrap_or_default(),
            deregistered_at: self.deregistered_at,
        };

        (remainder != PrunedAccountState::default()).then_some(remainder)
    }
}

fn every_snapshot<T: Clone + std::fmt::Debug>(
    value: &EpochValue<T>,
    holds: impl Fn(Option<&T>) -> bool,
) -> bool {
    [
        value.next(),
        value.live(),
        value.mark(),
        value.set(),
        value.go(),
    ]
    .into_iter()
    .all(holds)
}

/// What the dead rows ESTART pruned for a credential leave for the API, kept
/// under the credential's [`AccountKey`] in its own namespace. Nothing during
/// sync reads it.
///
/// One row per credential, summed over every pruned life. A dead row holds
/// no reward balance, so its lifetime withdrawals equal its lifetime
/// rewards, and one sum serves both. The newest deregistration sets
/// `active_epoch` when the credential has no row, or a row with neither
/// date.
#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct PrunedAccountState {
    #[n(0)]
    pub lifetime_rewards: u64,

    #[n(1)]
    pub deregistered_at: Option<u64>,
}

entity_boilerplate!(PrunedAccountState, "pruned-accounts", state = AccountKey);

impl PrunedAccountState {
    /// Add a later pruned life of the same credential.
    pub fn absorb(&mut self, later: &PrunedAccountState) {
        self.lifetime_rewards = add!(self.lifetime_rewards, later.lifetime_rewards);
        self.deregistered_at = self.deregistered_at.max(later.deregistered_at);
    }

    /// Add the pruned history back to `account`, the credential's live row or
    /// a fresh one, so it serves what the row would have served had ESTART
    /// never pruned it: the lifetime sums, and the deregistration when the
    /// row has neither date.
    pub fn restore(&self, account: &mut AccountState) {
        let mut live = account.stake.live().cloned().unwrap_or_default();
        live.rewards_sum = add!(live.rewards_sum, self.lifetime_rewards);
        live.withdrawals_sum = add!(live.withdrawals_sum, self.lifetime_rewards);
        account.stake.reset(Some(live));

        if account.registered_at.is_none() && account.deregistered_at.is_none() {
            account.deregistered_at = self.deregistered_at;
        }
    }

    /// Take back `rewards`, the lifetime rewards of a pruned life a rollback
    /// restored, which the restored row counts again. `None` when nothing is
    /// left.
    pub fn without(mut self, rewards: u64) -> Option<Self> {
        self.lifetime_rewards = sub!(self.lifetime_rewards, rewards);

        (self != Self::default()).then_some(self)
    }
}

// --- Deltas ---

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledAmountInc {
    pub(crate) cred: StakeCredential,
    pub(crate) is_pointer: bool,
    pub(crate) amount: u64,
    pub(crate) epoch: Epoch,

    // undo: was the account created by apply? If not, the pre-apply live Stake.
    pub(crate) was_new: bool,
    pub(crate) prev_stake_live: Option<Stake>,
}

impl ControlledAmountInc {
    pub fn new(cred: StakeCredential, is_pointer: bool, amount: u64, epoch: Epoch) -> Self {
        Self {
            cred,
            is_pointer,
            amount,
            epoch,
            was_new: false,
            prev_stake_live: None,
        }
    }
}

impl dolos_core::EntityDelta for ControlledAmountInc {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.cred))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_with(|| AccountState::new(self.epoch, self.cred.clone()));

        let stake = entity.stake.unwrap_live_mut();
        self.prev_stake_live = Some(stake.clone());

        if self.is_pointer {
            debug!(amount=%self.amount, "adding to pointer utxo sum");
            stake.utxo_sum_at_pointer_addresses =
                add!(stake.utxo_sum_at_pointer_addresses, self.amount);
        } else {
            stake.utxo_sum = add!(stake.utxo_sum, self.amount);
        }
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        if self.was_new {
            *entity = None;
            return;
        }
        let entity = entity.as_mut().expect("existing account");
        let stake = entity.stake.unwrap_live_mut();
        *stake = self.prev_stake_live.clone().expect("apply captured stake");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlledAmountDec {
    pub(crate) cred: StakeCredential,
    pub(crate) is_pointer: bool,
    pub(crate) amount: u64,

    // undo
    pub(crate) prev_stake_live: Option<Stake>,
}

impl ControlledAmountDec {
    pub fn new(cred: StakeCredential, is_pointer: bool, amount: u64) -> Self {
        Self {
            cred,
            is_pointer,
            amount,
            prev_stake_live: None,
        }
    }
}

impl dolos_core::EntityDelta for ControlledAmountDec {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.cred))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        let stake = entity.stake.unwrap_live_mut();
        self.prev_stake_live = Some(stake.clone());

        if self.is_pointer {
            stake.utxo_sum_at_pointer_addresses =
                sub!(stake.utxo_sum_at_pointer_addresses, self.amount);
        } else {
            stake.utxo_sum = sub!(stake.utxo_sum, self.amount);
        }
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");
        let stake = entity.stake.unwrap_live_mut();
        *stake = self.prev_stake_live.clone().expect("apply captured stake");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeRegistration {
    pub(crate) cred: StakeCredential,
    pub(crate) slot: u64,
    pub(crate) epoch: Epoch,
    pub(crate) deposit: u64,

    // undo
    pub(crate) was_new: bool,
    pub(crate) prev_registered_at: Option<u64>,
    pub(crate) prev_deregistered_at: Option<u64>,
}

impl StakeRegistration {
    pub fn new(cred: StakeCredential, slot: u64, epoch: Epoch, deposit: u64) -> Self {
        Self {
            cred,
            slot,
            epoch,
            deposit,
            was_new: false,
            prev_registered_at: None,
            prev_deregistered_at: None,
        }
    }
}

impl dolos_core::EntityDelta for StakeRegistration {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.cred))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        self.was_new = entity.is_none();

        let entity = entity.get_or_insert_with(|| AccountState::new(self.epoch, self.cred.clone()));

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_deregistered_at = entity.deregistered_at;

        tracing::debug!(
            slot = self.slot,
            account = %AccountKey::from(&self.cred),
            "applying registration"
        );

        entity.registered_at = Some(self.slot);
        entity.deregistered_at = None;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        if self.was_new {
            *entity = None;
            return;
        }
        let entity = entity.as_mut().expect("existing account");
        entity.registered_at = self.prev_registered_at;
        entity.deregistered_at = self.prev_deregistered_at;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StakeDelegation {
    pub(crate) cred: StakeCredential,
    pub(crate) pool: Hash<28>,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_pool: Option<EpochValue<PoolDelegation>>,
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
        NsKey::of::<AccountState>(&AccountKey::from(&self.cred))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        tracing::debug!(pool = hex::encode(self.pool), "applying delegation");

        // save undo
        self.prev_pool = Some(entity.pool.clone());
        self.prev_retired_pool = entity.retired_pool;

        // apply changes
        entity
            .pool
            .replace(PoolDelegation::Pool(self.pool), self.epoch);
        entity.retired_pool = None;
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");
        entity.pool = self.prev_pool.clone().expect("apply captured pool");
        entity.retired_pool = self.prev_retired_pool;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VoteDelegation {
    pub(crate) cred: StakeCredential,
    pub(crate) drep: DRep,
    pub(crate) vote_delegated_at: (BlockSlot, TxOrder),
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_drep: Option<EpochValue<DRepDelegation>>,
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
        NsKey::of::<AccountState>(&AccountKey::from(&self.cred))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        // save undo
        self.prev_drep = Some(entity.drep.clone());
        self.prev_vote_delegated_at = entity.vote_delegated_at;

        // apply changes
        entity.vote_delegated_at = Some(self.vote_delegated_at);
        entity
            .drep
            .replace(DRepDelegation::Delegated(self.drep.clone()), self.epoch);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");
        entity.vote_delegated_at = self.prev_vote_delegated_at;
        entity.drep = self.prev_drep.clone().expect("apply captured drep");
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
    pub(crate) prev_pool: Option<EpochValue<PoolDelegation>>,
    pub(crate) prev_drep: Option<EpochValue<DRepDelegation>>,
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
            prev_retired_pool: None,
        }
    }
}

impl dolos_core::EntityDelta for StakeDeregistration {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.cred))
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        tracing::debug!(slot = self.slot, "applying deregistration");

        // save undo info
        self.prev_registered_at = entity.registered_at;
        self.prev_deregistered_at = entity.deregistered_at;
        self.prev_pool = Some(entity.pool.clone());
        self.prev_drep = Some(entity.drep.clone());
        self.prev_retired_pool = entity.retired_pool;

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

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");
        entity.registered_at = self.prev_registered_at;
        entity.deregistered_at = self.prev_deregistered_at;
        entity.pool = self.prev_pool.clone().expect("apply captured pool");
        entity.drep = self.prev_drep.clone().expect("apply captured drep");
        entity.retired_pool = self.prev_retired_pool;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WithdrawalInc {
    pub(crate) cred: StakeCredential,
    pub(crate) amount: u64,

    // undo
    pub(crate) prev_stake_live: Option<Stake>,
}

impl WithdrawalInc {
    pub fn new(cred: StakeCredential, amount: u64) -> Self {
        Self {
            cred,
            amount,
            prev_stake_live: None,
        }
    }
}

impl dolos_core::EntityDelta for WithdrawalInc {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.cred))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        let stake = entity.stake.unwrap_live_mut();
        self.prev_stake_live = Some(stake.clone());
        stake.withdrawals_sum = add!(stake.withdrawals_sum, self.amount);
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");
        let stake = entity.stake.unwrap_live_mut();
        *stake = self.prev_stake_live.clone().expect("apply captured stake");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDelegatorRetire {
    pub(crate) delegator: AccountKey,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_pool: Option<EpochValue<PoolDelegation>>,
    pub(crate) prev_retired_pool: Option<PoolHash>,
}

impl PoolDelegatorRetire {
    pub fn new(delegator: AccountKey, epoch: Epoch) -> Self {
        Self {
            delegator,
            epoch,
            prev_pool: None,
            prev_retired_pool: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolDelegatorRetire {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&self.delegator)
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("account should exist");

        debug!(delegator=%self.delegator, "retiring pool delegator");

        // save undo info
        self.prev_pool = Some(entity.pool.clone());
        self.prev_retired_pool = entity.retired_pool;

        // TODO: read via `entity.pool.snapshot_at(self.epoch)` (the same
        // accessor the visitor uses in `ewrap::drops::BoundaryVisitor`)
        // instead of `live()`, and guard the `schedule` call below on
        // `entity.pool.epoch() == Some(self.epoch)` with skip-on-mismatch.
        // The visitor reads the snapshot at `current_epoch`; once the
        // entity has been rotated past that epoch, `live()` and
        // `snapshot_at(epoch)` disagree, and the `unreachable!` below
        // fires (see the EWRAP-replay crash recovery analysis). The
        // `ewrap_progress` short-circuit removes the known way to hit
        // this divergence, but the underlying invariant — visitor and
        // delta agree on which slot to read — is still fragile.
        let prev_pool_live = entity.pool.live().cloned();

        // apply changes
        entity
            .pool
            .schedule(self.epoch, Some(PoolDelegation::NotDelegated));

        let Some(PoolDelegation::Pool(pool)) = prev_pool_live else {
            unreachable!("account delegated to pool")
        };
        entity.retired_pool = Some(pool);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");
        entity.pool = self.prev_pool.clone().expect("apply captured pool");
        entity.retired_pool = self.prev_retired_pool;
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DRepDelegatorDrop {
    pub(crate) delegator: AccountKey,
    pub(crate) epoch: Epoch,

    // undo
    pub(crate) prev_drep: Option<EpochValue<DRepDelegation>>,
}

impl DRepDelegatorDrop {
    pub fn new(delegator: AccountKey, epoch: Epoch) -> Self {
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
        NsKey::of::<AccountState>(&self.delegator)
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(delegator=%self.delegator, "dropping drep delegator");

        // save undo info
        self.prev_drep = Some(entity.drep.clone());

        // TODO: guard the `schedule` call on `entity.drep.epoch() == Some(self.epoch)`
        // with skip-on-mismatch, mirroring the planned hardening in
        // `PoolDelegatorRetire::apply`. `schedule` writes to `next` regardless
        // of the entity's current epoch; if the entity has been rotated past
        // `self.epoch`, this scheduling lands on the wrong boundary.
        entity
            .drep
            .schedule(self.epoch, Some(DRepDelegation::NotDelegated));
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");
        entity.drep = self.prev_drep.clone().expect("apply captured drep");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TreasuryWithdrawal {
    pub(crate) account: StakeCredential,
    pub(crate) amount: u64,

    // undo
    pub(crate) prev_stake_live: Option<Stake>,
}

impl TreasuryWithdrawal {
    pub fn new(account: StakeCredential, amount: u64) -> Self {
        Self {
            account,
            amount,
            prev_stake_live: None,
        }
    }
}

impl dolos_core::EntityDelta for TreasuryWithdrawal {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.account))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(account=?self.account, amount=%self.amount, "applying treasury withdrawal");

        let stake = entity.stake.unwrap_live_mut();
        self.prev_stake_live = Some(stake.clone());
        stake.rewards_sum = add!(stake.rewards_sum, self.amount);
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");
        let stake = entity.stake.unwrap_live_mut();
        *stake = self.prev_stake_live.clone().expect("apply captured stake");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolDepositRefund {
    pub(crate) pool_deposit: u64,
    pub(crate) account: StakeCredential,

    // undo
    pub(crate) prev_stake: Option<EpochValue<Stake>>,
}

impl PoolDepositRefund {
    pub fn new(pool_deposit: u64, account: StakeCredential) -> Self {
        Self {
            pool_deposit,
            account,
            prev_stake: None,
        }
    }
}

impl dolos_core::EntityDelta for PoolDepositRefund {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.account))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        self.prev_stake = Some(entity.stake.clone());

        let stake = entity.stake.scheduled_or_default();

        stake.rewards_sum += self.pool_deposit;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");
        entity.stake = self.prev_stake.clone().expect("apply captured stake");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProposalDepositRefund {
    pub(crate) proposal_deposit: u64,
    pub(crate) account: StakeCredential,

    // undo
    pub(crate) prev_stake: Option<EpochValue<Stake>>,
}

impl ProposalDepositRefund {
    pub fn new(proposal_deposit: u64, account: StakeCredential) -> Self {
        Self {
            proposal_deposit,
            account,
            prev_stake: None,
        }
    }
}

impl dolos_core::EntityDelta for ProposalDepositRefund {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&AccountKey::from(&self.account))
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        tracing::debug!(cred=?self.account, deposit=%self.proposal_deposit, "applying proposal deposit refund");

        self.prev_stake = Some(entity.stake.clone());

        let stake = entity.stake.scheduled_or_default();

        stake.rewards_sum += self.proposal_deposit;
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");
        entity.stake = self.prev_stake.clone().expect("apply captured stake");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AssignRewards {
    pub(crate) account: AccountKey,
    pub(crate) reward: u64,

    // undo
    pub(crate) prev_stake_live: Option<Stake>,
}

impl AssignRewards {
    pub fn new(account: AccountKey, reward: u64) -> Self {
        Self {
            account,
            reward,
            prev_stake_live: None,
        }
    }
}

impl dolos_core::EntityDelta for AssignRewards {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&self.account)
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");

        debug!(account=%self.account, "assigning rewards");

        let stake = entity.stake.unwrap_live_mut();
        self.prev_stake_live = Some(stake.clone());
        stake.rewards_sum = add!(stake.rewards_sum, self.reward);
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        let entity = entity.as_mut().expect("existing account");
        let stake = entity.stake.unwrap_live_mut();
        *stake = self.prev_stake_live.clone().expect("apply captured stake");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccountTransition {
    pub(crate) account: AccountKey,
    pub(crate) next_epoch: Epoch,

    // undo: snapshot each EpochValue before the rotation.
    pub(crate) prev_stake: Option<EpochValue<Stake>>,
    pub(crate) prev_pool: Option<EpochValue<PoolDelegation>>,
    pub(crate) prev_drep: Option<EpochValue<DRepDelegation>>,
}

impl AccountTransition {
    pub fn new(account: AccountKey, next_epoch: Epoch) -> Self {
        Self {
            account,
            next_epoch,
            prev_stake: None,
            prev_pool: None,
            prev_drep: None,
        }
    }
}

impl dolos_core::EntityDelta for AccountTransition {
    type Entity = AccountState;

    fn key(&self) -> NsKey {
        NsKey::of::<AccountState>(&self.account)
    }

    fn apply(&mut self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");

        // save undo info
        self.prev_stake = Some(entity.stake.clone());
        self.prev_pool = Some(entity.pool.clone());
        self.prev_drep = Some(entity.drep.clone());

        entity.rotate(self.next_epoch);
    }

    fn undo(&self, entity: &mut Option<AccountState>) {
        let entity = entity.as_mut().expect("existing account");
        entity.stake = self.prev_stake.clone().expect("apply captured stake");
        entity.pool = self.prev_pool.clone().expect("apply captured pool");
        entity.drep = self.prev_drep.clone().expect("apply captured drep");
    }
}

#[cfg(test)]
mod prop_tests {
    use super::testing::any_account_state;
    use super::*;
    use crate::model::testing::{
        self as root, assert_delta_roundtrip, assert_delta_serde_roundtrip,
    };
    use proptest::prelude::*;

    /// Build an `AccountState` whose `pool.live` is guaranteed to be
    /// `PoolDelegation::Pool(..)`. Required by `PoolDelegatorRetire`, which
    /// panics via `unreachable!()` if `prev_pool` isn't a real pool
    /// delegation.
    fn any_account_with_active_pool() -> impl Strategy<Value = AccountState> {
        (any_account_state(), root::any_pool_hash()).prop_map(|(mut acct, pool)| {
            let live = acct.pool.live().cloned();
            let epoch = acct.pool.epoch().unwrap_or(3);
            if live.is_some() {
                acct.pool.replace(PoolDelegation::Pool(pool), epoch);
            } else {
                acct.pool = EpochValue::with_live(epoch, PoolDelegation::Pool(pool));
            }
            acct
        })
    }

    prop_compose! {
        fn any_controlled_amount_inc()(
            cred in root::any_stake_credential(),
            is_pointer in any::<bool>(),
            amount in root::any_lovelace(),
            epoch in root::any_epoch(),
        ) -> ControlledAmountInc {
            ControlledAmountInc::new(cred, is_pointer, amount, epoch)
        }
    }

    prop_compose! {
        fn any_controlled_amount_dec()(
            cred in root::any_stake_credential(),
            is_pointer in any::<bool>(),
            amount in 0u64..1_000u64,
        ) -> ControlledAmountDec {
            ControlledAmountDec::new(cred, is_pointer, amount)
        }
    }

    prop_compose! {
        fn any_stake_registration()(
            cred in root::any_stake_credential(),
            slot in root::any_slot(),
            epoch in root::any_epoch(),
            deposit in root::any_lovelace(),
        ) -> StakeRegistration {
            StakeRegistration::new(cred, slot, epoch, deposit)
        }
    }

    prop_compose! {
        fn any_stake_delegation()(
            cred in root::any_stake_credential(),
            pool in root::any_hash_28(),
            epoch in root::any_epoch(),
        ) -> StakeDelegation {
            StakeDelegation::new(cred, pool, epoch)
        }
    }

    prop_compose! {
        fn any_vote_delegation()(
            cred in root::any_stake_credential(),
            drep in root::any_drep(),
            slot in root::any_slot(),
            order in root::any_tx_order(),
            epoch in root::any_epoch(),
        ) -> VoteDelegation {
            VoteDelegation::new(cred, drep, slot, order, epoch)
        }
    }

    prop_compose! {
        fn any_stake_deregistration()(
            cred in root::any_stake_credential(),
            slot in root::any_slot(),
            epoch in root::any_epoch(),
        ) -> StakeDeregistration {
            StakeDeregistration::new(cred, slot, epoch)
        }
    }

    prop_compose! {
        fn any_withdrawal_inc()(
            cred in root::any_stake_credential(),
            amount in root::any_lovelace(),
        ) -> WithdrawalInc {
            WithdrawalInc::new(cred, amount)
        }
    }

    prop_compose! {
        fn any_pool_delegator_retire()(
            pool in root::any_hash_28(),
            epoch in root::any_epoch(),
        ) -> PoolDelegatorRetire {
            PoolDelegatorRetire::new(AccountKey(StakeCredential::AddrKeyhash(pool)), epoch)
        }
    }

    prop_compose! {
        fn any_drep_delegator_drop()(
            cred_hash in root::any_hash_28(),
            epoch in root::any_epoch(),
        ) -> DRepDelegatorDrop {
            DRepDelegatorDrop::new(AccountKey(StakeCredential::AddrKeyhash(cred_hash)), epoch)
        }
    }

    prop_compose! {
        fn any_treasury_withdrawal()(
            account in root::any_stake_credential(),
            amount in root::any_lovelace(),
        ) -> TreasuryWithdrawal {
            TreasuryWithdrawal::new(account, amount)
        }
    }

    prop_compose! {
        fn any_pool_deposit_refund()(
            pool_deposit in root::any_lovelace(),
            account in root::any_stake_credential(),
        ) -> PoolDepositRefund {
            PoolDepositRefund::new(pool_deposit, account)
        }
    }

    prop_compose! {
        fn any_proposal_deposit_refund()(
            proposal_deposit in root::any_lovelace(),
            account in root::any_stake_credential(),
        ) -> ProposalDepositRefund {
            ProposalDepositRefund::new(proposal_deposit, account)
        }
    }

    prop_compose! {
        fn any_assign_rewards()(
            account_hash in root::any_hash_28(),
            reward in root::any_lovelace(),
        ) -> AssignRewards {
            AssignRewards::new(AccountKey(StakeCredential::AddrKeyhash(account_hash)), reward)
        }
    }

    prop_compose! {
        fn any_account_transition()(
            account_hash in root::any_hash_28(),
            next_epoch in root::any_epoch(),
        ) -> AccountTransition {
            AccountTransition::new(AccountKey(StakeCredential::AddrKeyhash(account_hash)), next_epoch)
        }
    }

    proptest! {
        #[test]
        fn controlled_amount_inc_roundtrip(
            entity in prop::option::of(any_account_state()),
            delta in any_controlled_amount_inc(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn controlled_amount_dec_roundtrip(
            entity in any_account_state(),
            delta in any_controlled_amount_dec(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn stake_registration_roundtrip(
            entity in prop::option::of(any_account_state()),
            delta in any_stake_registration(),
        ) {
            assert_delta_roundtrip(entity, delta);
        }

        #[test]
        fn stake_delegation_roundtrip(
            entity in any_account_state(),
            mut delta in any_stake_delegation(),
        ) {
            // `apply` asserts alignment under `strict`, so the delta carries
            // the entity's epoch.
            delta.epoch = entity.pool.epoch().expect("generated at epoch position");
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn vote_delegation_roundtrip(
            entity in any_account_state(),
            mut delta in any_vote_delegation(),
        ) {
            delta.epoch = entity.drep.epoch().expect("generated at epoch position");
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn stake_deregistration_roundtrip(
            entity in any_account_state(),
            mut delta in any_stake_deregistration(),
        ) {
            delta.epoch = entity.pool.epoch().expect("generated at epoch position");
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn withdrawal_inc_roundtrip(
            entity in any_account_state(),
            delta in any_withdrawal_inc(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn pool_delegator_retire_roundtrip(
            entity in any_account_with_active_pool(),
            mut delta in any_pool_delegator_retire(),
        ) {
            delta.epoch = entity.pool.epoch().expect("generated at epoch position");
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn drep_delegator_drop_roundtrip(
            entity in any_account_state(),
            mut delta in any_drep_delegator_drop(),
        ) {
            delta.epoch = entity.drep.epoch().expect("generated at epoch position");
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn treasury_withdrawal_roundtrip(
            entity in any_account_state(),
            delta in any_treasury_withdrawal(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn pool_deposit_refund_roundtrip(
            entity in any_account_state(),
            delta in any_pool_deposit_refund(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn proposal_deposit_refund_roundtrip(
            entity in any_account_state(),
            delta in any_proposal_deposit_refund(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn assign_rewards_roundtrip(
            entity in any_account_state(),
            delta in any_assign_rewards(),
        ) {
            assert_delta_roundtrip(Some(entity), delta);
        }

        #[test]
        fn account_transition_roundtrip(
            entity in any_account_state(),
            mut delta in any_account_transition(),
        ) {
            // `transition` asserts `next_epoch` is exactly one past the
            // entity's epoch under `strict`.
            delta.next_epoch = entity.stake.epoch().expect("generated at epoch position") + 1;
            assert_delta_roundtrip(Some(entity), delta);
        }

        // --- WAL serialize → deserialize → undo round-trips ---
        //
        // These exercise the path the WAL takes: serialize the (post-apply)
        // delta with bincode (which is what `crates/redb3/src/wal/mod.rs`
        // uses), deserialize it, then assert undo still restores the original
        // entity. The plain `_roundtrip` variants above use the same in-memory
        // delta instance for apply and undo and so don't catch regressions
        // where a `prev_*` field isn't `serde`-serialized or where the WAL
        // row is written before apply runs.

        #[test]
        fn controlled_amount_inc_serde_roundtrip(
            entity in prop::option::of(any_account_state()),
            delta in any_controlled_amount_inc(),
        ) {
            assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn controlled_amount_dec_serde_roundtrip(
            entity in any_account_state(),
            delta in any_controlled_amount_dec(),
        ) {
            assert_delta_serde_roundtrip(Some(entity), delta);
        }

        #[test]
        fn stake_registration_serde_roundtrip(
            entity in prop::option::of(any_account_state()),
            delta in any_stake_registration(),
        ) {
            assert_delta_serde_roundtrip(entity, delta);
        }

        #[test]
        fn stake_delegation_serde_roundtrip(
            entity in any_account_state(),
            mut delta in any_stake_delegation(),
        ) {
            delta.epoch = entity.pool.epoch().expect("generated at epoch position");
            assert_delta_serde_roundtrip(Some(entity), delta);
        }

        #[test]
        fn stake_deregistration_serde_roundtrip(
            entity in any_account_state(),
            mut delta in any_stake_deregistration(),
        ) {
            delta.epoch = entity.pool.epoch().expect("generated at epoch position");
            assert_delta_serde_roundtrip(Some(entity), delta);
        }

        #[test]
        fn vote_delegation_serde_roundtrip(
            entity in any_account_state(),
            mut delta in any_vote_delegation(),
        ) {
            delta.epoch = entity.drep.epoch().expect("generated at epoch position");
            assert_delta_serde_roundtrip(Some(entity), delta);
        }

        #[test]
        fn withdrawal_inc_serde_roundtrip(
            entity in any_account_state(),
            delta in any_withdrawal_inc(),
        ) {
            assert_delta_serde_roundtrip(Some(entity), delta);
        }
    }
}

#[cfg(test)]
mod key_tests {
    use dolos_core::keys::encode;
    use proptest::prelude::*;

    use super::*;
    use crate::model::testing as root;

    #[test]
    fn layout_is_pinned() {
        let account = AccountKey(StakeCredential::AddrKeyhash(Hash::new([0xaa; 28])));
        assert_eq!(
            hex::encode(encode(&account)),
            format!("8200581c{}", "aa".repeat(28))
        );

        let script = AccountKey(StakeCredential::ScriptHash(Hash::new([0xbb; 28])));
        assert_eq!(
            hex::encode(encode(&script)),
            format!("8201581c{}", "bb".repeat(28))
        );
    }

    /// The account key is the credential's own CBOR, unchanged.
    #[test]
    fn keys_are_the_credential_cbor() {
        for credential in [
            StakeCredential::AddrKeyhash(Hash::new([0x42; 28])),
            StakeCredential::ScriptHash(Hash::new([0x43; 28])),
        ] {
            assert_eq!(
                encode(&AccountKey(credential.clone())).to_vec(),
                minicbor::to_vec(&credential).unwrap()
            );
        }
    }

    #[test]
    fn malformed_keys_are_refused() {
        let account = encode(&AccountKey(StakeCredential::AddrKeyhash(Hash::new(
            [1; 28],
        ))));
        assert!(AccountKey::decode_key(&account[..31]).is_err());
        assert!(AccountKey::decode_key(&[account.as_slice(), &[0]].concat()).is_err());

        let mut bad_header = account.clone();
        bad_header[1] = 2;
        assert!(AccountKey::decode_key(&bad_header).is_err());
    }

    proptest! {
        #[test]
        fn keys_round_trip(credential in root::any_stake_credential()) {
            let key = AccountKey(credential);
            prop_assert_eq!(AccountKey::decode_key(&encode(&key)), Ok(key));
        }

        /// Exactly one shard range holds any account key.
        #[test]
        fn shards_partition_the_accounts(credential in root::any_stake_credential(), total in prop::sample::select(vec![1u32, 2, 16, 32, 256])) {
            let key = AccountKey(credential);

            let hits = (0..total)
                .flat_map(|index| AccountKey::shard_ranges(index, total))
                .filter(|range| range.contains(&key))
                .count();

            prop_assert_eq!(hits, 1);
        }
    }

    #[test]
    fn shard_ranges_split_on_the_first_hash_byte() {
        let key = |kind: CredentialKind, first: u8| {
            let mut hash = [0xffu8; 28];
            hash[0] = first;
            AccountKey(kind.credential(hash.into()))
        };

        for kind in [CredentialKind::KeyHash, CredentialKind::Script] {
            let index = usize::from(kind == CredentialKind::Script);

            let third = &AccountKey::shard_ranges(3, 16)[index];
            assert!(third.contains(&key(kind, 0x30)));
            assert!(third.contains(&key(kind, 0x3f)));
            assert!(!third.contains(&key(kind, 0x40)));
            assert!(!third.contains(&key(kind, 0x2f)));

            let last = &AccountKey::shard_ranges(15, 16)[index];
            assert!(last.contains(&key(kind, 0xff)));
            assert!(!last.contains(&key(kind, 0xef)));
        }
    }

    #[test]
    #[should_panic(expected = "must divide")]
    fn shard_ranges_refuse_a_non_divisor() {
        AccountKey::shard_ranges(0, 3);
    }
}
