use std::{
    cmp::Ordering,
    collections::{BTreeMap, HashSet},
};

use dolos_core::{
    BlockSlot, ChainError, EntityKey, EntityValue, Namespace, NamespaceType, NsKey, StateSchema,
};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::{
        hash::Hash,
        nonce::{generate_epoch_nonce, generate_rolling_nonce},
    },
    ledger::primitives::{
        conway::{CostModels, DRep, DRepVotingThresholds, PoolVotingThresholds, ProposalProcedure},
        Coin, Epoch, ExUnitPrices, ExUnits, Nonce, PoolMetadata, ProtocolVersion, RationalNumber,
        Relay, StakeCredential, UnitInterval,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    estart::{
        nonces::NonceTransition,
        reset::{AccountTransition, EpochTransition, PoolTransition},
    },
    ewrap::{
        govactions::ProposalEnactment,
        retires::{
            DRepDelegatorDrop, DRepExpiration, PoolDelegatorDrop, PoolDepositRefund,
            ProposalExpiration,
        },
        rewards::AssignRewards,
        wrapup::{EpochWrapUp, PoolWrapUp},
    },
    pallas_extras::{
        self, default_cost_models, default_drep_voting_thresholds, default_ex_unit_prices,
        default_ex_units, default_nonce, default_pool_voting_thresholds, default_rational_number,
    },
    pots::{EpochIncentives, Pots},
    roll::{
        accounts::{
            ControlledAmountDec, ControlledAmountInc, StakeDelegation, StakeDeregistration,
            StakeRegistration, VoteDelegation, WithdrawalInc,
        },
        assets::MintStatsUpdate,
        dreps::{DRepActivity, DRepRegistration, DRepUnRegistration},
        epochs::{EpochStatsUpdate, NoncesUpdate, PParamsUpdate},
        pools::{MintedBlocksInc, PoolDeRegistration, PoolRegistration},
        proposals::NewProposal,
    },
};

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq, Copy)]
enum EpochPosition {
    #[n(0)]
    Genesis,

    #[n(1)]
    Epoch(#[n(0)] Epoch),
}

impl EpochPosition {
    pub fn mark(&self) -> Option<Epoch> {
        match self {
            EpochPosition::Epoch(epoch) if *epoch >= 1 => Some(*epoch - 1),
            _ => None,
        }
    }
    pub fn set(&self) -> Option<Epoch> {
        match self {
            EpochPosition::Epoch(epoch) if *epoch >= 2 => Some(*epoch - 2),
            _ => None,
        }
    }
    pub fn go(&self) -> Option<Epoch> {
        match self {
            EpochPosition::Epoch(epoch) if *epoch >= 3 => Some(*epoch - 3),
            _ => None,
        }
    }
}

impl PartialEq<Epoch> for EpochPosition {
    fn eq(&self, other: &Epoch) -> bool {
        match self {
            EpochPosition::Genesis => false,
            EpochPosition::Epoch(epoch) => *epoch == *other,
        }
    }
}

impl std::ops::Add<Epoch> for EpochPosition {
    type Output = EpochPosition;

    fn add(self, other: Epoch) -> Self::Output {
        match self {
            EpochPosition::Genesis if other == 0 => EpochPosition::Genesis,
            EpochPosition::Genesis => EpochPosition::Epoch(other - 1),
            EpochPosition::Epoch(current) => EpochPosition::Epoch(current + other),
        }
    }
}

impl std::ops::AddAssign<Epoch> for EpochPosition {
    fn add_assign(&mut self, other: Epoch) {
        *self = *self + other;
    }
}

/// Allows implementing types to define what the default value is when
/// transitioning to the next epoch. Some scenarios require a reset of its value
/// and some others just a copy of the live one.
pub trait TransitionDefault: Sized {
    fn next_value(current: Option<&Self>) -> Option<Self>;
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochValue<T> {
    /// The epoch representing the live version of the value
    #[n(0)]
    epoch: EpochPosition,

    /// The next version of the value already scheduled for the next epoch
    #[n(1)]
    next: Option<T>,

    /// The current, mutating version of the value
    #[n(2)]
    live: Option<T>,

    /// Epoch - 1 version of the value
    #[n(4)]
    mark: Option<T>,

    /// Epoch - 2 version of the value
    #[n(5)]
    set: Option<T>,

    /// Epoch - 3 version of the value
    #[n(6)]
    go: Option<T>,
}

impl<T> EpochValue<T>
where
    T: Clone + std::fmt::Debug,
{
    pub fn new(epoch: Epoch) -> Self {
        Self {
            epoch: EpochPosition::Epoch(epoch),
            go: None,
            set: None,
            mark: None,
            live: None,
            next: None,
        }
    }

    pub fn with_live(epoch: Epoch, live: T) -> Self {
        Self {
            epoch: EpochPosition::Epoch(epoch),
            go: None,
            set: None,
            mark: None,
            live: Some(live),
            next: None,
        }
    }

    pub fn with_scheduled(epoch: Epoch, next: T) -> Self {
        Self {
            epoch: EpochPosition::Epoch(epoch),
            go: None,
            set: None,
            mark: None,
            live: None,
            next: Some(next),
        }
    }

    pub fn with_genesis(live: T) -> Self {
        Self {
            epoch: EpochPosition::Epoch(0),
            go: None,
            set: None,
            mark: Some(live.clone()),
            live: Some(live),
            next: None,
        }
    }

    /// Returns the epoch of the live value
    pub fn epoch(&self) -> Option<Epoch> {
        match self.epoch {
            EpochPosition::Genesis => None,
            EpochPosition::Epoch(epoch) => Some(epoch),
        }
    }

    /// Returns a reference to the live value that matches the ongoing epoch.
    pub fn live(&self) -> Option<&T> {
        self.live.as_ref()
    }

    pub fn unwrap_live(&self) -> &T {
        self.live.as_ref().expect("live value not initialized")
    }

    pub fn unwrap_live_mut(&mut self) -> &mut T {
        self.live.as_mut().expect("live value not initialized")
    }

    pub fn go(&self) -> Option<&T> {
        self.go.as_ref()
    }

    pub fn set(&self) -> Option<&T> {
        self.set.as_ref()
    }

    pub fn mark(&self) -> Option<&T> {
        self.mark.as_ref()
    }

    pub fn next(&self) -> Option<&T> {
        self.next.as_ref()
    }

    pub fn next_mut(&mut self) -> Option<&mut T> {
        self.next.as_mut()
    }

    /// Schedules the next value to be applied on the next epoch transition
    pub fn schedule(&mut self, current_epoch: Epoch, next: Option<T>) {
        assert_eq!(self.epoch, current_epoch);

        self.schedule_unchecked(next)
    }

    /// Same as schedule, but without checking that that the epoch matches.
    pub fn schedule_unchecked(&mut self, next: Option<T>) {
        self.next = next;
    }

    /// Mutates the live value for the current epoch without rotating any of
    /// the previous values
    pub fn live_mut(&mut self, epoch: Epoch) -> &mut Option<T> {
        assert_eq!(self.epoch, epoch);
        self.live_mut_unchecked()
    }

    /// Same as mutate, but without checking that that the epoch matches.
    pub fn live_mut_unchecked(&mut self) -> &mut Option<T> {
        assert!(
            self.next.is_none(),
            "can't change live value when next value is already scheduled"
        );

        &mut self.live
    }

    /// Resets the live value for the current epoch.
    pub fn reset(&mut self, live: Option<T>) {
        self.reset_unchecked(live);
    }

    /// Same as reset, but without checking that that the epoch matches.
    pub fn reset_unchecked(&mut self, live: Option<T>) {
        self.live = live;
    }

    /// Replaces the live value for the current epoch without rotating any of
    /// the previous values
    pub fn replace(&mut self, live: T, epoch: Epoch) {
        assert_eq!(self.epoch, epoch);
        self.live = Some(live);
    }

    /// Same as replace, but without checking that that the epoch matches.
    pub fn replace_unchecked(&mut self, live: T) {
        self.live = Some(live);
    }

    /// Transitions into the next epoch by taking a snapshot of the live value
    /// and rotating the previous ones.
    pub fn transition(&mut self, next_epoch: Epoch) {
        assert_eq!(self.epoch + 1, next_epoch);
        self.transition_unchecked();
    }

    /// Same as transition, but without checking that that the epoch matches.
    pub fn transition_unchecked(&mut self) {
        self.go = self.set.clone();
        self.set = self.mark.clone();
        self.mark = self.live.clone();
        self.live = self.next.take();

        self.epoch += 1;
    }

    /// Returns the value for the snapshot taken at the end of the given epoch.
    pub fn snapshot_at(&self, ending_epoch: Epoch) -> Option<&T> {
        if self.epoch == ending_epoch {
            self.live.as_ref()
        } else if self.epoch.mark() == Some(ending_epoch) {
            self.mark.as_ref()
        } else if self.epoch.set() == Some(ending_epoch) {
            self.set.as_ref()
        } else if self.epoch.go() == Some(ending_epoch) {
            self.go.as_ref()
        } else {
            None
        }
    }

    pub fn try_snapshot_at(&self, epoch: Epoch) -> Result<&T, ChainError> {
        match self.snapshot_at(epoch) {
            Some(value) => Ok(value),
            None => {
                dbg!(self);
                Err(ChainError::EpochValueVersionNotFound(epoch))
            }
        }
    }
}

impl<T> EpochValue<T>
where
    T: TransitionDefault + std::fmt::Debug + Clone,
{
    pub fn scheduled_or_default(&mut self) -> &mut T {
        if self.next.is_none() {
            self.next = T::next_value(self.live.as_ref());
        }

        self.next.as_mut().unwrap()
    }

    /// Transitions into the next epoch using the scheduled value, falling back
    /// to the default value if the next is not scheduled.
    pub fn default_transition(&mut self, next_epoch: Epoch) {
        if self.next.is_none() {
            let next = T::next_value(self.live.as_ref());
            self.next = next;
        }

        self.transition(next_epoch);
    }
}

pub trait FixedNamespace {
    const NS: &'static str;
}

macro_rules! entity_boilerplate {
    ($type:ident, $ns:literal) => {
        impl FixedNamespace for $type {
            const NS: &str = $ns;
        }

        impl dolos_core::Entity for $type {
            fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, ChainError> {
                assert_eq!(ns, $type::NS);
                let value = pallas::codec::minicbor::decode(value)?;
                Ok(value)
            }

            fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
                let value = pallas::codec::minicbor::to_vec(value).unwrap();
                ($type::NS, value)
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct RewardLog {
    #[n(0)]
    pub amount: u64,

    #[n(1)]
    pub pool_id: Vec<u8>,

    #[n(2)]
    pub as_leader: bool,
}

entity_boilerplate!(RewardLog, "rewards");

#[derive(Debug, Clone, PartialEq, Decode, Encode, Default)]
pub struct StakeLog {
    /// Number of blocks created by pool
    #[n(0)]
    pub blocks_minted: u64,

    /// Total stake in Lovelaces
    #[n(1)]
    pub total_stake: u64,

    /// Pool size (percentage) of overall active stake at that epoch
    #[n(2)]
    pub relative_size: f64,

    /// Number of delegators for epoch
    #[n(3)]
    pub delegators_count: u64,

    /// Live pledge
    #[n(6)]
    pub live_pledge: u64,

    /// Declared pledge
    #[n(7)]
    pub declared_pledge: u64,

    /// Total rewards for epoch
    #[n(8)]
    pub total_rewards: u64,

    /// Total fees for epoch
    #[n(9)]
    pub operator_share: u64,

    /// Fixed cost
    #[n(10)]
    pub fixed_cost: u64,

    /// Margin cost
    #[n(11)]
    pub margin_cost: Option<RationalNumber>,
}

entity_boilerplate!(StakeLog, "stakes");

pub type PoolHash = Hash<28>;

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct Stake {
    #[n(0)]
    pub utxo_sum: Lovelace,

    #[n(1)]
    pub rewards_sum: Lovelace,

    #[n(2)]
    pub withdrawals_sum: Lovelace,

    #[n(3)]
    #[cbor(default)]
    pub utxo_sum_at_pointer_addresses: Lovelace,
}

impl Stake {
    pub fn total(&self) -> u64 {
        let mut out = self.utxo_sum;
        out += self.rewards_sum;
        out -= self.withdrawals_sum;

        out
    }

    pub fn total_pre_conway(&self) -> u64 {
        let mut out = self.utxo_sum;
        out += self.utxo_sum_at_pointer_addresses;
        out += self.rewards_sum;
        out -= self.withdrawals_sum;

        out
    }

    pub fn total_for_era(&self, era: EraProtocol) -> u64 {
        match era {
            x if x < 9 => self.total_pre_conway(),
            _ => self.total(),
        }
    }

    pub fn withdrawable(&self) -> u64 {
        let mut out = self.rewards_sum;
        out -= self.withdrawals_sum;

        out
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

pub type DRepDelegation = Option<DRep>;

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
    pub vote_delegated_at: Option<BlockSlot>,

    #[n(5)]
    pub deregistered_at: Option<u64>,

    #[n(6)]
    pub credential: StakeCredential,
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
        }
    }

    pub fn live_stake(&self) -> u64 {
        self.stake.live().map(|x| x.total()).unwrap_or_default()
    }

    pub fn is_registered(&self) -> bool {
        match (self.registered_at, self.deregistered_at) {
            (Some(_), None) => true,
            (Some(start), Some(end)) => start >= end,
            (None, _) => false,
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
        self.drep.snapshot_at(epoch).and_then(|x| x.as_ref())
    }
}

#[derive(Debug, Encode, Decode, Clone, Default)]
pub struct AssetState {
    #[n(0)]
    pub quantity_bytes: [u8; 16],

    #[n(1)]
    pub initial_tx: Option<Hash<32>>,

    #[n(2)]
    pub initial_slot: Option<u64>,

    #[n(3)]
    pub mint_tx_count: u64,
}

entity_boilerplate!(AssetState, "assets");

impl AssetState {
    pub fn add_quantity(&mut self, value: i128) {
        let old = i128::from_be_bytes(self.quantity_bytes);
        let new = old.saturating_add(value).to_be_bytes();
        self.quantity_bytes = new;
    }

    pub fn quantity(&self) -> i128 {
        i128::from_be_bytes(self.quantity_bytes)
    }
}

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

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
pub struct Proposal {
    #[n(0)]
    pub slot: BlockSlot,

    #[n(1)]
    pub transaction: Hash<32>,

    #[n(2)]
    pub idx: u32,

    #[n(3)]
    pub proposal: ProposalProcedure,

    #[n(4)]
    pub ratified_epoch: Option<Epoch>,

    #[n(5)]
    pub enacted_epoch: Option<Epoch>,

    #[n(6)]
    pub dropped_epoch: Option<Epoch>,

    #[n(7)]
    pub expired_epoch: Option<Epoch>,
}

entity_boilerplate!(Proposal, "proposals");

impl Proposal {
    pub fn new(
        slot: BlockSlot,
        transaction: Hash<32>,
        idx: u32,
        proposal: ProposalProcedure,
    ) -> Self {
        Self {
            slot,
            transaction,
            idx,
            proposal,
            ratified_epoch: None,
            enacted_epoch: None,
            dropped_epoch: None,
            expired_epoch: None,
        }
    }

    pub fn key(&self) -> EntityKey {
        Self::build_entity_key(self.transaction, self.idx)
    }

    /// Get ID of the proposal in its string form, as found on explorers.
    pub fn id_as_string(&self) -> String {
        format!("{}#{}", hex::encode(self.transaction), self.idx)
    }

    pub fn build_entity_key(transaction: Hash<32>, idx: u32) -> EntityKey {
        EntityKey::from([idx.to_be_bytes().as_slice(), transaction.as_slice()].concat())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub enum PParamKind {
    SystemStart = 0,
    EpochLength = 1,
    SlotLength = 2,
    MinFeeA = 3,
    MinFeeB = 4,
    MaxBlockBodySize = 5,
    MaxTransactionSize = 6,
    MaxBlockHeaderSize = 7,
    KeyDeposit = 8,
    PoolDeposit = 9,
    DesiredNumberOfStakePools = 10,
    ProtocolVersion = 11,
    MinUtxoValue = 12,
    MinPoolCost = 13,
    ExpansionRate = 14,
    TreasuryGrowthRate = 15,
    MaximumEpoch = 16,
    PoolPledgeInfluence = 17,
    DecentralizationConstant = 18,
    ExtraEntropy = 19,
    AdaPerUtxoByte = 20,
    ExecutionCosts = 21,
    MaxTxExUnits = 22,
    MaxBlockExUnits = 23,
    MaxValueSize = 24,
    CollateralPercentage = 25,
    MaxCollateralInputs = 26,
    PoolVotingThresholds = 27,
    DrepVotingThresholds = 28,
    MinCommitteeSize = 29,
    CommitteeTermLimit = 30,
    GovernanceActionValidityPeriod = 31,
    GovernanceActionDeposit = 32,
    DrepDeposit = 33,
    DrepInactivityPeriod = 34,
    MinFeeRefScriptCostPerByte = 35,
    CostModelsPlutusV1 = 36,
    CostModelsPlutusV2 = 37,
    CostModelsPlutusV3 = 38,
    CostModelsUnknown = 39,
}

impl PParamKind {
    pub fn default_value(self) -> PParamValue {
        match self {
            Self::SystemStart => PParamValue::SystemStart(0),
            Self::EpochLength => PParamValue::EpochLength(0),
            Self::SlotLength => PParamValue::SlotLength(0),
            Self::MinFeeA => PParamValue::MinFeeA(0),
            Self::MinFeeB => PParamValue::MinFeeB(0),
            Self::MaxBlockBodySize => PParamValue::MaxBlockBodySize(0),
            Self::MaxTransactionSize => PParamValue::MaxTransactionSize(0),
            Self::MaxBlockHeaderSize => PParamValue::MaxBlockHeaderSize(0),
            Self::KeyDeposit => PParamValue::KeyDeposit(0),
            Self::PoolDeposit => PParamValue::PoolDeposit(0),
            Self::DesiredNumberOfStakePools => PParamValue::DesiredNumberOfStakePools(0),
            Self::ProtocolVersion => PParamValue::ProtocolVersion(ProtocolVersion::default()),
            Self::MinUtxoValue => PParamValue::MinUtxoValue(0),
            Self::MinPoolCost => PParamValue::MinPoolCost(0),
            Self::ExpansionRate => PParamValue::ExpansionRate(default_rational_number()),
            Self::TreasuryGrowthRate => PParamValue::TreasuryGrowthRate(default_rational_number()),
            Self::MaximumEpoch => PParamValue::MaximumEpoch(0),
            Self::PoolPledgeInfluence => {
                PParamValue::PoolPledgeInfluence(default_rational_number())
            }
            Self::DecentralizationConstant => {
                PParamValue::DecentralizationConstant(default_rational_number())
            }
            Self::ExtraEntropy => PParamValue::ExtraEntropy(default_nonce()),
            Self::AdaPerUtxoByte => PParamValue::AdaPerUtxoByte(0),
            Self::ExecutionCosts => PParamValue::ExecutionCosts(default_ex_unit_prices()),
            Self::MaxTxExUnits => PParamValue::MaxTxExUnits(default_ex_units()),
            Self::MaxBlockExUnits => PParamValue::MaxBlockExUnits(default_ex_units()),
            Self::MaxValueSize => PParamValue::MaxValueSize(0),
            Self::CollateralPercentage => PParamValue::CollateralPercentage(0),
            Self::MaxCollateralInputs => PParamValue::MaxCollateralInputs(0),
            Self::PoolVotingThresholds => {
                PParamValue::PoolVotingThresholds(default_pool_voting_thresholds())
            }
            Self::DrepVotingThresholds => {
                PParamValue::DrepVotingThresholds(default_drep_voting_thresholds())
            }
            Self::MinCommitteeSize => PParamValue::MinCommitteeSize(0),
            Self::CommitteeTermLimit => PParamValue::CommitteeTermLimit(0),
            Self::GovernanceActionValidityPeriod => PParamValue::GovernanceActionValidityPeriod(0),
            Self::GovernanceActionDeposit => PParamValue::GovernanceActionDeposit(0),
            Self::DrepDeposit => PParamValue::DrepDeposit(0),
            Self::DrepInactivityPeriod => PParamValue::DrepInactivityPeriod(0),
            Self::MinFeeRefScriptCostPerByte => {
                PParamValue::MinFeeRefScriptCostPerByte(default_rational_number())
            }
            Self::CostModelsPlutusV1 => {
                PParamValue::CostModelsPlutusV1(default_cost_models().plutus_v1.unwrap_or_default())
            }
            Self::CostModelsPlutusV2 => {
                PParamValue::CostModelsPlutusV2(default_cost_models().plutus_v2.unwrap_or_default())
            }
            Self::CostModelsPlutusV3 => {
                PParamValue::CostModelsPlutusV3(default_cost_models().plutus_v3.unwrap_or_default())
            }
            Self::CostModelsUnknown => {
                PParamValue::CostModelsUnknown(default_cost_models().unknown)
            }
        }
    }
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[cbor(flat)]
pub enum PParamValue {
    #[n(0)]
    SystemStart(#[n(0)] u64),

    #[n(1)]
    EpochLength(#[n(0)] u64),

    #[n(2)]
    SlotLength(#[n(0)] u64),

    #[n(3)]
    MinFeeA(#[n(0)] u64),

    #[n(4)]
    MinFeeB(#[n(0)] u64),

    #[n(5)]
    MaxBlockBodySize(#[n(0)] u64),

    #[n(6)]
    MaxTransactionSize(#[n(0)] u64),

    #[n(7)]
    MaxBlockHeaderSize(#[n(0)] u64),

    #[n(8)]
    KeyDeposit(#[n(0)] Coin),

    #[n(9)]
    PoolDeposit(#[n(0)] Coin),

    #[n(10)]
    DesiredNumberOfStakePools(#[n(0)] u32),

    #[n(11)]
    ProtocolVersion(#[n(0)] ProtocolVersion),

    #[n(12)]
    MinUtxoValue(#[n(0)] Coin),

    #[n(13)]
    MinPoolCost(#[n(0)] Coin),

    #[n(14)]
    ExpansionRate(#[n(0)] UnitInterval),

    #[n(15)]
    TreasuryGrowthRate(#[n(0)] UnitInterval),

    #[n(16)]
    MaximumEpoch(#[n(0)] Epoch),

    #[n(17)]
    PoolPledgeInfluence(#[n(0)] RationalNumber),

    #[n(18)]
    DecentralizationConstant(#[n(0)] UnitInterval),

    #[n(19)]
    ExtraEntropy(#[n(0)] Nonce),

    #[n(20)]
    AdaPerUtxoByte(#[n(0)] Coin),

    #[n(21)]
    ExecutionCosts(#[n(0)] ExUnitPrices),

    #[n(22)]
    MaxTxExUnits(#[n(0)] ExUnits),

    #[n(23)]
    MaxBlockExUnits(#[n(0)] ExUnits),

    #[n(24)]
    MaxValueSize(#[n(0)] u32),

    #[n(25)]
    CollateralPercentage(#[n(0)] u32),

    #[n(26)]
    MaxCollateralInputs(#[n(0)] u32),

    #[n(27)]
    PoolVotingThresholds(#[n(0)] PoolVotingThresholds),

    #[n(28)]
    DrepVotingThresholds(#[n(0)] DRepVotingThresholds),

    #[n(29)]
    MinCommitteeSize(#[n(0)] u64),

    #[n(30)]
    CommitteeTermLimit(#[n(0)] Epoch),

    #[n(31)]
    GovernanceActionValidityPeriod(#[n(0)] Epoch),

    #[n(32)]
    GovernanceActionDeposit(#[n(0)] Coin),

    #[n(33)]
    DrepDeposit(#[n(0)] Coin),

    #[n(34)]
    DrepInactivityPeriod(#[n(0)] Epoch),

    #[n(35)]
    MinFeeRefScriptCostPerByte(#[n(0)] UnitInterval),

    #[n(36)]
    CostModelsPlutusV1(#[n(0)] Vec<i64>),

    #[n(37)]
    CostModelsPlutusV2(#[n(0)] Vec<i64>),

    #[n(38)]
    CostModelsPlutusV3(#[n(0)] Vec<i64>),

    #[n(39)]
    CostModelsUnknown(#[n(0)] BTreeMap<u64, Vec<i64>>),
}

impl PParamValue {
    pub fn kind(&self) -> PParamKind {
        match self {
            Self::SystemStart(_) => PParamKind::SystemStart,
            Self::EpochLength(_) => PParamKind::EpochLength,
            Self::SlotLength(_) => PParamKind::SlotLength,
            Self::MinFeeA(_) => PParamKind::MinFeeA,
            Self::MinFeeB(_) => PParamKind::MinFeeB,
            Self::MaxBlockBodySize(_) => PParamKind::MaxBlockBodySize,
            Self::MaxTransactionSize(_) => PParamKind::MaxTransactionSize,
            Self::MaxBlockHeaderSize(_) => PParamKind::MaxBlockHeaderSize,
            Self::KeyDeposit(_) => PParamKind::KeyDeposit,
            Self::PoolDeposit(_) => PParamKind::PoolDeposit,
            Self::DesiredNumberOfStakePools(_) => PParamKind::DesiredNumberOfStakePools,
            Self::ProtocolVersion(_) => PParamKind::ProtocolVersion,
            Self::MinUtxoValue(_) => PParamKind::MinUtxoValue,
            Self::MinPoolCost(_) => PParamKind::MinPoolCost,
            Self::ExpansionRate(_) => PParamKind::ExpansionRate,
            Self::TreasuryGrowthRate(_) => PParamKind::TreasuryGrowthRate,
            Self::MaximumEpoch(_) => PParamKind::MaximumEpoch,
            Self::PoolPledgeInfluence(_) => PParamKind::PoolPledgeInfluence,
            Self::DecentralizationConstant(_) => PParamKind::DecentralizationConstant,
            Self::ExtraEntropy(_) => PParamKind::ExtraEntropy,
            Self::AdaPerUtxoByte(_) => PParamKind::AdaPerUtxoByte,
            Self::ExecutionCosts(_) => PParamKind::ExecutionCosts,
            Self::MaxTxExUnits(_) => PParamKind::MaxTxExUnits,
            Self::MaxBlockExUnits(_) => PParamKind::MaxBlockExUnits,
            Self::MaxValueSize(_) => PParamKind::MaxValueSize,
            Self::CollateralPercentage(_) => PParamKind::CollateralPercentage,
            Self::MaxCollateralInputs(_) => PParamKind::MaxCollateralInputs,
            Self::PoolVotingThresholds(_) => PParamKind::PoolVotingThresholds,
            Self::DrepVotingThresholds(_) => PParamKind::DrepVotingThresholds,
            Self::MinCommitteeSize(_) => PParamKind::MinCommitteeSize,
            Self::CommitteeTermLimit(_) => PParamKind::CommitteeTermLimit,
            Self::GovernanceActionValidityPeriod(_) => PParamKind::GovernanceActionValidityPeriod,
            Self::GovernanceActionDeposit(_) => PParamKind::GovernanceActionDeposit,
            Self::DrepDeposit(_) => PParamKind::DrepDeposit,
            Self::DrepInactivityPeriod(_) => PParamKind::DrepInactivityPeriod,
            Self::MinFeeRefScriptCostPerByte(_) => PParamKind::MinFeeRefScriptCostPerByte,
            Self::CostModelsPlutusV1(_) => PParamKind::CostModelsPlutusV1,
            Self::CostModelsPlutusV2(_) => PParamKind::CostModelsPlutusV2,
            Self::CostModelsPlutusV3(_) => PParamKind::CostModelsPlutusV3,
            Self::CostModelsUnknown(_) => PParamKind::CostModelsUnknown,
        }
    }
}

#[derive(Debug, Encode, Decode, Clone, Default, Serialize, Deserialize)]
pub struct PParamsSet {
    #[n(0)]
    values: Vec<PParamValue>,
}

impl TransitionDefault for PParamsSet {
    fn next_value(current: Option<&Self>) -> Option<Self> {
        current.cloned()
    }
}

impl EpochValue<PParamsSet> {
    pub fn era_transition(&self) -> Option<EraTransition> {
        let original = self.unwrap_live().protocol_major_or_default();

        let update = self.next().and_then(|p| p.protocol_major())?;

        if original == update {
            return None;
        }

        Some(EraTransition {
            prev_version: EraProtocol::from(original),
            new_version: EraProtocol::from(update),
        })
    }
}

macro_rules! pgetter {
    ($kind:ident, $ty:ty) => {
        paste::paste! {
            pub fn [<$kind:snake>](&self) -> Option<$ty> {
                let value = self.get(PParamKind::$kind)?;

                let PParamValue::$kind(x) = value else {
                    panic!("pparam $kind doesn't match value");
                };

                Some(x.clone())
            }


            pub fn [<$kind:snake _or_default>](&self) -> $ty {
                let value = self.get_or_default(PParamKind::$kind);

                let PParamValue::$kind(x) = value else {
                    panic!("pparam $kind doesn't match value");
                };

                x
            }
        }
    };
}

macro_rules! ensure_pparam {
    ($kind:ident, $ty:ty) => {
        paste::paste! {
            pub fn [<ensure_ $kind:snake>](&self) -> Result<$ty, ChainError> {
                self.$kind().ok_or(ChainError::PParamsNotFound(stringify!($kind).to_string()))
            }
        }
    };
}

impl PParamsSet {
    pub fn is_byron(&self) -> bool {
        self.protocol_major_or_default() < 2
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn get(&self, kind: PParamKind) -> Option<&PParamValue> {
        self.values.iter().find(|value| value.kind() == kind)
    }

    pub fn get_mut(&mut self, kind: PParamKind) -> Option<&mut PParamValue> {
        self.values.iter_mut().find(|value| value.kind() == kind)
    }

    pub fn set(&mut self, value: PParamValue) {
        let existing = self.get_mut(value.kind());

        if let Some(existing) = existing {
            *existing = value;
        } else {
            self.values.push(value);
        }
    }

    pub fn clear(&mut self, kind: PParamKind) {
        self.values.retain(|x| x.kind() != kind);
    }

    pub fn with(mut self, value: PParamValue) -> Self {
        self.set(value);

        self
    }

    pub fn merge(&mut self, other: Self) {
        for param in other.values {
            self.set(param);
        }
    }

    pub fn get_or_default(&self, kind: PParamKind) -> PParamValue {
        self.get(kind)
            .cloned()
            .unwrap_or_else(|| PParamKind::default_value(kind))
    }

    pub fn protocol_major(&self) -> Option<u16> {
        self.protocol_version().map(|(major, _)| major as u16)
    }

    pub fn protocol_major_or_default(&self) -> u16 {
        self.protocol_major().unwrap_or(0)
    }

    pub fn k(&self) -> Option<u32> {
        self.desired_number_of_stake_pools()
    }

    pub fn a0(&self) -> Option<RationalNumber> {
        self.pool_pledge_influence()
    }

    pub fn tau(&self) -> Option<RationalNumber> {
        self.treasury_growth_rate()
    }

    pub fn rho(&self) -> Option<RationalNumber> {
        self.expansion_rate()
    }

    pub fn d(&self) -> Option<RationalNumber> {
        self.decentralization_constant()
    }

    pub fn cost_models_for_script_languages(&self) -> CostModels {
        CostModels {
            plutus_v1: self.cost_models_plutus_v1(),
            plutus_v2: self.cost_models_plutus_v2(),
            plutus_v3: self.cost_models_plutus_v3(),
            unknown: self.cost_models_unknown_or_default(),
        }
    }

    ensure_pparam!(system_start, u64);
    ensure_pparam!(slot_length, u64);
    ensure_pparam!(d, RationalNumber);
    ensure_pparam!(rho, RationalNumber);
    ensure_pparam!(tau, RationalNumber);
    ensure_pparam!(k, u32);
    ensure_pparam!(a0, RationalNumber);
    ensure_pparam!(epoch_length, u64);
    ensure_pparam!(drep_inactivity_period, u64);
    ensure_pparam!(key_deposit, u64);
    ensure_pparam!(pool_deposit, u64);
    ensure_pparam!(governance_action_validity_period, u64);
    ensure_pparam!(protocol_version, ProtocolVersion);

    pgetter!(SystemStart, u64);
    pgetter!(EpochLength, u64);
    pgetter!(SlotLength, u64);
    pgetter!(MinFeeA, u64);
    pgetter!(MinFeeB, u64);
    pgetter!(MaxBlockBodySize, u64);
    pgetter!(MaxTransactionSize, u64);
    pgetter!(MaxBlockHeaderSize, u64);
    pgetter!(KeyDeposit, u64);
    pgetter!(PoolDeposit, u64);
    pgetter!(DesiredNumberOfStakePools, u32);
    pgetter!(ProtocolVersion, ProtocolVersion);
    pgetter!(MinUtxoValue, u64);
    pgetter!(MinPoolCost, u64);
    pgetter!(ExpansionRate, RationalNumber);
    pgetter!(TreasuryGrowthRate, RationalNumber);
    pgetter!(MaximumEpoch, u64);
    pgetter!(PoolPledgeInfluence, RationalNumber);
    pgetter!(DecentralizationConstant, RationalNumber);
    pgetter!(ExtraEntropy, Nonce);
    pgetter!(AdaPerUtxoByte, u64);
    pgetter!(CostModelsPlutusV1, Vec<i64>);
    pgetter!(CostModelsPlutusV2, Vec<i64>);
    pgetter!(CostModelsPlutusV3, Vec<i64>);
    pgetter!(CostModelsUnknown, BTreeMap<u64, Vec<i64>>);
    pgetter!(ExecutionCosts, ExUnitPrices);
    pgetter!(MaxTxExUnits, ExUnits);
    pgetter!(MaxBlockExUnits, ExUnits);
    pgetter!(MaxValueSize, u32);
    pgetter!(CollateralPercentage, u32);
    pgetter!(MaxCollateralInputs, u32);
    pgetter!(PoolVotingThresholds, PoolVotingThresholds);
    pgetter!(DrepVotingThresholds, DRepVotingThresholds);
    pgetter!(MinCommitteeSize, u64);
    pgetter!(CommitteeTermLimit, u64);
    pgetter!(GovernanceActionValidityPeriod, u64);
    pgetter!(GovernanceActionDeposit, u64);
    pgetter!(DrepDeposit, u64);
    pgetter!(DrepInactivityPeriod, u64);
    pgetter!(MinFeeRefScriptCostPerByte, RationalNumber);
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
pub struct Nonces {
    #[n(0)]
    pub active: Hash<32>,

    #[n(1)]
    pub evolving: Hash<32>,

    #[n(2)]
    pub candidate: Hash<32>,

    #[n(3)]
    pub tail: Option<Hash<32>>,
}

impl Nonces {
    pub fn bootstrap(shelley_hash: Hash<32>) -> Self {
        Self {
            active: shelley_hash,
            evolving: shelley_hash,
            candidate: shelley_hash,
            tail: None,
        }
    }

    pub fn roll(
        &self,
        update_candidate: bool,
        nonce_vrf_output: &[u8],
        tail: Option<Hash<32>>,
    ) -> Nonces {
        let evolving = generate_rolling_nonce(self.evolving, nonce_vrf_output);

        Self {
            active: self.active,
            evolving,
            candidate: if update_candidate {
                evolving
            } else {
                self.candidate
            },
            tail,
        }
    }

    /// Compute active nonce for next epoch.
    pub fn sweep(&self, previous_tail: Option<Hash<32>>, extra_entropy: Option<&[u8]>) -> Self {
        Self {
            active: match previous_tail {
                Some(tail) => generate_epoch_nonce(self.candidate, tail, extra_entropy),
                None => self.candidate,
            },
            candidate: self.evolving,
            evolving: self.evolving,
            tail: self.tail,
        }
    }
}

pub type Lovelace = u64;

/// Epoch data that is gathered as part of the block rolling process
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, Default)]
pub struct RollingStats {
    #[n(2)]
    pub produced_utxos: Lovelace,

    #[n(3)]
    pub consumed_utxos: Lovelace,

    #[n(4)]
    pub gathered_fees: Lovelace,

    #[n(5)]
    pub new_accounts: u64,

    #[n(6)]
    pub removed_accounts: u64,

    #[n(7)]
    pub withdrawals: Lovelace,

    #[n(8)]
    pub registered_pools: HashSet<PoolHash>,

    #[n(13)]
    pub blocks_minted: u32,

    #[n(14)]
    pub drep_deposits: Lovelace,

    #[n(15)]
    pub proposal_deposits: Lovelace,

    #[n(16)]
    pub drep_refunds: Lovelace,

    #[n(17)]
    pub proposal_refunds: Lovelace,

    #[n(18)]
    #[cbor(default)]
    pub treasury_donations: Lovelace,
}

impl TransitionDefault for RollingStats {
    fn next_value(_: Option<&Self>) -> Option<Self> {
        Some(Self::default())
    }
}

/// Stats that are gathered at the end of the epoch
#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
pub struct EndStats {
    #[n(0)]
    pub pool_deposit_count: u64,

    #[n(1)]
    pub pool_refund_count: u64,

    #[n(2)]
    pub pool_invalid_refund_count: u64,

    #[n(3)]
    pub epoch_incentives: EpochIncentives,

    #[n(4)]
    pub effective_rewards: u64,

    #[n(5)]
    pub unspendable_rewards: u64,

    // TODO: deprecate
    #[n(6)]
    pub __proposal_deposits: Lovelace,

    #[n(7)]
    pub proposal_refunds: Lovelace,

    // TODO: deprecate
    #[n(8)]
    pub __drep_deposits: Lovelace,

    // TODO: deprecate
    #[n(9)]
    pub __drep_refunds: Lovelace,
}

#[derive(Debug, Encode, Decode, Clone)]
pub struct EpochState {
    #[n(0)]
    pub number: Epoch,

    #[n(1)]
    pub initial_pots: Pots,

    #[n(2)]
    pub rolling: EpochValue<RollingStats>,

    #[n(9)]
    pub pparams: EpochValue<PParamsSet>,

    #[n(10)]
    pub largest_stable_slot: BlockSlot,

    #[n(11)]
    pub previous_nonce_tail: Option<Hash<32>>,

    #[n(12)]
    pub nonces: Option<Nonces>,

    #[n(13)]
    pub end: Option<EndStats>,
}

#[derive(Debug)]
pub struct EraTransition {
    pub prev_version: EraProtocol,
    pub new_version: EraProtocol,
}

impl EraTransition {
    /// Check if this boundary is transitioning to shelley for the first time.
    pub fn entering_shelley(&self) -> bool {
        self.prev_version < 2 && self.new_version == 2
    }
}

entity_boilerplate!(EpochState, "epochs");

pub const CURRENT_EPOCH_KEY: &[u8] = b"0";

pub fn drep_to_entity_key(value: &DRep) -> EntityKey {
    let bytes = match value {
        DRep::Key(key) => [vec![pallas_extras::DREP_KEY_PREFIX], key.to_vec()].concat(),
        DRep::Script(key) => [vec![pallas_extras::DREP_SCRIPT_PREFIX], key.to_vec()].concat(),
        // Invented keys for convenience
        DRep::Abstain => vec![0],
        DRep::NoConfidence => vec![1],
    };

    EntityKey::from(bytes)
}

#[derive(Debug, Encode, Decode, Clone)]
pub struct DRepState {
    #[n(0)]
    pub initial_slot: Option<u64>,

    #[n(1)]
    pub voting_power: u64,

    #[n(2)]
    pub last_active_slot: Option<u64>,

    #[n(3)]
    pub unregistered_at: Option<BlockSlot>,

    #[n(4)]
    pub expired: bool,

    #[n(5)]
    pub deposit: u64,

    #[n(6)]
    pub identifier: DRep,
}

impl DRepState {
    pub fn new(identifier: DRep) -> Self {
        Self {
            initial_slot: None,
            voting_power: 0,
            last_active_slot: None,
            unregistered_at: None,
            expired: false,
            deposit: 0,
            identifier,
        }
    }
}

entity_boilerplate!(DRepState, "dreps");

#[derive(Debug, Clone, Copy)]
pub struct EraProtocol(u16);

impl std::fmt::Display for EraProtocol {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl PartialEq<u16> for EraProtocol {
    fn eq(&self, other: &u16) -> bool {
        self.0 == *other
    }
}

impl PartialOrd<u16> for EraProtocol {
    fn partial_cmp(&self, other: &u16) -> Option<Ordering> {
        self.0.partial_cmp(other)
    }
}

impl From<u16> for EraProtocol {
    fn from(value: u16) -> Self {
        Self(value)
    }
}

impl From<EraProtocol> for u16 {
    fn from(value: EraProtocol) -> Self {
        value.0
    }
}

impl From<EraProtocol> for EntityKey {
    fn from(value: EraProtocol) -> Self {
        EntityKey::from(&value.0.to_be_bytes())
    }
}

impl From<EntityKey> for EraProtocol {
    fn from(value: EntityKey) -> Self {
        let bytes: [u8; 2] = value.as_ref()[..2].try_into().unwrap();
        Self(u16::from_be_bytes(bytes))
    }
}

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize)]
pub struct EraBoundary {
    #[n(0)]
    pub epoch: u64,

    #[n(1)]
    pub slot: u64,

    #[n(2)]
    pub timestamp: u64,
}

#[derive(Debug, Encode, Decode, Clone)]
pub struct EraSummary {
    #[n(0)]
    pub start: EraBoundary,

    #[n(1)]
    pub end: Option<EraBoundary>,

    #[n(2)]
    pub epoch_length: u64,

    #[n(3)]
    pub slot_length: u64,

    #[n(4)]
    #[cbor(default)]
    pub protocol: u16,
}

entity_boilerplate!(EraSummary, "eras");

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum CardanoEntity {
    EraSummary(EraSummary),
    AccountState(AccountState),
    AssetState(AssetState),
    PoolState(PoolState),
    EpochState(EpochState),
    DRepState(DRepState),
    Proposal(Proposal),
    RewardLog(RewardLog),
    StakeLog(StakeLog),
}

macro_rules! variant_boilerplate {
    ($variant:ident) => {
        impl From<CardanoEntity> for Option<$variant> {
            fn from(value: CardanoEntity) -> Self {
                match value {
                    CardanoEntity::$variant(x) => Some(x),
                    _ => None,
                }
            }
        }

        impl From<$variant> for CardanoEntity {
            fn from(value: $variant) -> Self {
                CardanoEntity::$variant(value)
            }
        }
    };
}

variant_boilerplate!(EraSummary);
variant_boilerplate!(AccountState);
variant_boilerplate!(AssetState);
variant_boilerplate!(PoolState);
variant_boilerplate!(EpochState);
variant_boilerplate!(DRepState);
variant_boilerplate!(Proposal);
variant_boilerplate!(RewardLog);
variant_boilerplate!(StakeLog);

impl dolos_core::Entity for CardanoEntity {
    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, ChainError> {
        match ns {
            EraSummary::NS => EraSummary::decode_entity(ns, value).map(Into::into),
            AccountState::NS => AccountState::decode_entity(ns, value).map(Into::into),
            AssetState::NS => AssetState::decode_entity(ns, value).map(Into::into),
            PoolState::NS => PoolState::decode_entity(ns, value).map(Into::into),
            EpochState::NS => EpochState::decode_entity(ns, value).map(Into::into),
            DRepState::NS => DRepState::decode_entity(ns, value).map(Into::into),
            Proposal::NS => Proposal::decode_entity(ns, value).map(Into::into),
            RewardLog::NS => RewardLog::decode_entity(ns, value).map(Into::into),
            StakeLog::NS => StakeLog::decode_entity(ns, value).map(Into::into),
            _ => Err(ChainError::InvalidNamespace(ns)),
        }
    }

    fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
        match value {
            Self::EraSummary(x) => {
                let (ns, enc) = EraSummary::encode_entity(x);
                (ns, enc)
            }
            Self::AccountState(x) => {
                let (ns, enc) = AccountState::encode_entity(x);
                (ns, enc)
            }
            Self::AssetState(x) => {
                let (ns, enc) = AssetState::encode_entity(x);
                (ns, enc)
            }
            Self::PoolState(x) => {
                let (ns, enc) = PoolState::encode_entity(x);
                (ns, enc)
            }
            Self::EpochState(x) => {
                let (ns, enc) = EpochState::encode_entity(x);
                (ns, enc)
            }
            Self::DRepState(x) => {
                let (ns, enc) = DRepState::encode_entity(x);
                (ns, enc)
            }
            Self::Proposal(x) => {
                let (ns, enc) = Proposal::encode_entity(x);
                (ns, enc)
            }
            Self::RewardLog(x) => {
                let (ns, enc) = RewardLog::encode_entity(x);
                (ns, enc)
            }
            Self::StakeLog(x) => {
                let (ns, enc) = StakeLog::encode_entity(x);
                (ns, enc)
            }
        }
    }
}

pub fn build_schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert(EraSummary::NS, NamespaceType::KeyValue);
    schema.insert(AccountState::NS, NamespaceType::KeyValue);
    schema.insert(AssetState::NS, NamespaceType::KeyValue);
    schema.insert(PoolState::NS, NamespaceType::KeyValue);
    schema.insert(EpochState::NS, NamespaceType::KeyValue);
    schema.insert(DRepState::NS, NamespaceType::KeyValue);
    schema.insert(Proposal::NS, NamespaceType::KeyValue);
    schema.insert(RewardLog::NS, NamespaceType::KeyValue);
    schema.insert(StakeLog::NS, NamespaceType::KeyValue);
    schema
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CardanoDelta {
    ControlledAmountInc(ControlledAmountInc),
    ControlledAmountDec(ControlledAmountDec),
    StakeRegistration(StakeRegistration),
    StakeDelegation(StakeDelegation),
    StakeDeregistration(StakeDeregistration),
    PoolRegistration(PoolRegistration),
    PoolDeRegistration(PoolDeRegistration),
    MintedBlocksInc(MintedBlocksInc),
    MintStatsUpdate(MintStatsUpdate),
    EpochStatsUpdate(EpochStatsUpdate),
    DRepRegistration(DRepRegistration),
    DRepUnRegistration(DRepUnRegistration),
    DRepActivity(DRepActivity),
    DRepExpiration(DRepExpiration),
    WithdrawalInc(WithdrawalInc),
    VoteDelegation(VoteDelegation),
    PParamsUpdate(PParamsUpdate),
    NoncesUpdate(NoncesUpdate),
    NewProposal(NewProposal),
    ProposalEnactment(ProposalEnactment),
    PoolDelegatorDrop(PoolDelegatorDrop),
    AssignRewards(AssignRewards),
    NonceTransition(NonceTransition),
    PoolTransition(PoolTransition),
    AccountTransition(AccountTransition),
    ProposalExpiration(ProposalExpiration),
    PoolDepositRefund(PoolDepositRefund),
    EpochTransition(EpochTransition),
    EpochWrapUp(EpochWrapUp),
    DRepDelegatorDrop(DRepDelegatorDrop),
    PoolWrapUp(PoolWrapUp),
}

impl CardanoDelta {
    pub fn downcast_apply<T, D>(delta: &mut D, entity: &mut Option<CardanoEntity>)
    where
        Option<T>: From<CardanoEntity>,
        D: dolos_core::EntityDelta<Entity = T>,
        T: Into<CardanoEntity>,
    {
        let mut sub_entity = entity.take().and_then(|x| x.into());
        delta.apply(&mut sub_entity);
        *entity = sub_entity.map(|x| x.into());
    }

    pub fn downcast_undo<T, D>(delta: &D, entity: &mut Option<CardanoEntity>)
    where
        Option<T>: From<CardanoEntity>,
        D: dolos_core::EntityDelta<Entity = T>,
        T: Into<CardanoEntity>,
    {
        let mut sub_entity = entity.take().and_then(|x| x.into());
        delta.undo(&mut sub_entity);
        *entity = sub_entity.map(|x| x.into());
    }
}

macro_rules! delta_from {
    ($type:ident) => {
        impl From<$type> for CardanoDelta {
            fn from(value: $type) -> Self {
                Self::$type(value)
            }
        }
    };
}

delta_from!(ControlledAmountInc);
delta_from!(ControlledAmountDec);
delta_from!(StakeRegistration);
delta_from!(StakeDelegation);
delta_from!(StakeDeregistration);
delta_from!(PoolRegistration);
delta_from!(PoolDeRegistration);
delta_from!(MintedBlocksInc);
delta_from!(MintStatsUpdate);
delta_from!(EpochStatsUpdate);
delta_from!(DRepRegistration);
delta_from!(DRepUnRegistration);
delta_from!(DRepActivity);
delta_from!(DRepExpiration);
delta_from!(WithdrawalInc);
delta_from!(VoteDelegation);
delta_from!(PParamsUpdate);
delta_from!(NoncesUpdate);
delta_from!(NewProposal);
delta_from!(ProposalEnactment);
delta_from!(PoolDelegatorDrop);
delta_from!(AssignRewards);
delta_from!(NonceTransition);
delta_from!(PoolTransition);
delta_from!(AccountTransition);
delta_from!(ProposalExpiration);
delta_from!(PoolDepositRefund);
delta_from!(EpochTransition);
delta_from!(EpochWrapUp);
delta_from!(DRepDelegatorDrop);
delta_from!(PoolWrapUp);

impl dolos_core::EntityDelta for CardanoDelta {
    type Entity = super::model::CardanoEntity;

    fn key(&self) -> NsKey {
        match self {
            Self::ControlledAmountInc(x) => x.key(),
            Self::ControlledAmountDec(x) => x.key(),
            Self::StakeRegistration(x) => x.key(),
            Self::StakeDelegation(x) => x.key(),
            Self::StakeDeregistration(x) => x.key(),
            Self::PoolRegistration(x) => x.key(),
            Self::PoolDeRegistration(x) => x.key(),
            Self::MintedBlocksInc(x) => x.key(),
            Self::MintStatsUpdate(x) => x.key(),
            Self::EpochStatsUpdate(x) => x.key(),
            Self::DRepRegistration(x) => x.key(),
            Self::DRepActivity(x) => x.key(),
            Self::DRepUnRegistration(x) => x.key(),
            Self::DRepExpiration(x) => x.key(),
            Self::WithdrawalInc(x) => x.key(),
            Self::VoteDelegation(x) => x.key(),
            Self::PParamsUpdate(x) => x.key(),
            Self::NoncesUpdate(x) => x.key(),
            Self::NewProposal(x) => x.key(),
            Self::PoolDelegatorDrop(x) => x.key(),
            Self::AssignRewards(x) => x.key(),
            Self::NonceTransition(x) => x.key(),
            Self::PoolTransition(x) => x.key(),
            Self::AccountTransition(x) => x.key(),
            Self::ProposalExpiration(x) => x.key(),
            Self::ProposalEnactment(x) => x.key(),
            Self::PoolDepositRefund(x) => x.key(),
            Self::EpochTransition(x) => x.key(),
            Self::EpochWrapUp(x) => x.key(),
            Self::DRepDelegatorDrop(x) => x.key(),
            Self::PoolWrapUp(x) => x.key(),
        }
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        match self {
            Self::ControlledAmountInc(x) => Self::downcast_apply(x, entity),
            Self::ControlledAmountDec(x) => Self::downcast_apply(x, entity),
            Self::StakeRegistration(x) => Self::downcast_apply(x, entity),
            Self::StakeDelegation(x) => Self::downcast_apply(x, entity),
            Self::StakeDeregistration(x) => Self::downcast_apply(x, entity),
            Self::PoolRegistration(x) => Self::downcast_apply(x, entity),
            Self::PoolDeRegistration(x) => Self::downcast_apply(x, entity),
            Self::MintedBlocksInc(x) => Self::downcast_apply(x, entity),
            Self::MintStatsUpdate(x) => Self::downcast_apply(x, entity),
            Self::EpochStatsUpdate(x) => Self::downcast_apply(x, entity),
            Self::DRepRegistration(x) => Self::downcast_apply(x, entity),
            Self::DRepUnRegistration(x) => Self::downcast_apply(x, entity),
            Self::DRepActivity(x) => Self::downcast_apply(x, entity),
            Self::DRepExpiration(x) => Self::downcast_apply(x, entity),
            Self::WithdrawalInc(x) => Self::downcast_apply(x, entity),
            Self::VoteDelegation(x) => Self::downcast_apply(x, entity),
            Self::PParamsUpdate(x) => Self::downcast_apply(x, entity),
            Self::NoncesUpdate(x) => Self::downcast_apply(x, entity),
            Self::NewProposal(x) => Self::downcast_apply(x, entity),
            Self::PoolDelegatorDrop(x) => Self::downcast_apply(x, entity),
            Self::AssignRewards(x) => Self::downcast_apply(x, entity),
            Self::NonceTransition(x) => Self::downcast_apply(x, entity),
            Self::PoolTransition(x) => Self::downcast_apply(x, entity),
            Self::AccountTransition(x) => Self::downcast_apply(x, entity),
            Self::ProposalExpiration(x) => Self::downcast_apply(x, entity),
            Self::ProposalEnactment(x) => Self::downcast_apply(x, entity),
            Self::PoolDepositRefund(x) => Self::downcast_apply(x, entity),
            Self::EpochTransition(x) => Self::downcast_apply(x, entity),
            Self::EpochWrapUp(x) => Self::downcast_apply(x, entity),
            Self::DRepDelegatorDrop(x) => Self::downcast_apply(x, entity),
            Self::PoolWrapUp(x) => Self::downcast_apply(x, entity),
        }
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        match self {
            Self::ControlledAmountInc(x) => Self::downcast_undo(x, entity),
            Self::ControlledAmountDec(x) => Self::downcast_undo(x, entity),
            Self::StakeRegistration(x) => Self::downcast_undo(x, entity),
            Self::StakeDelegation(x) => Self::downcast_undo(x, entity),
            Self::StakeDeregistration(x) => Self::downcast_undo(x, entity),
            Self::PoolRegistration(x) => Self::downcast_undo(x, entity),
            Self::PoolDeRegistration(x) => Self::downcast_undo(x, entity),
            Self::MintedBlocksInc(x) => Self::downcast_undo(x, entity),
            Self::MintStatsUpdate(x) => Self::downcast_undo(x, entity),
            Self::EpochStatsUpdate(x) => Self::downcast_undo(x, entity),
            Self::DRepRegistration(x) => Self::downcast_undo(x, entity),
            Self::DRepUnRegistration(x) => Self::downcast_undo(x, entity),
            Self::DRepActivity(x) => Self::downcast_undo(x, entity),
            Self::DRepExpiration(x) => Self::downcast_undo(x, entity),
            Self::WithdrawalInc(x) => Self::downcast_undo(x, entity),
            Self::VoteDelegation(x) => Self::downcast_undo(x, entity),
            Self::PParamsUpdate(x) => Self::downcast_undo(x, entity),
            Self::NoncesUpdate(x) => Self::downcast_undo(x, entity),
            Self::NewProposal(x) => Self::downcast_undo(x, entity),
            Self::PoolDelegatorDrop(x) => Self::downcast_undo(x, entity),
            Self::AssignRewards(x) => Self::downcast_undo(x, entity),
            Self::NonceTransition(x) => Self::downcast_undo(x, entity),
            Self::PoolTransition(x) => Self::downcast_undo(x, entity),
            Self::AccountTransition(x) => Self::downcast_undo(x, entity),
            Self::ProposalExpiration(x) => Self::downcast_undo(x, entity),
            Self::ProposalEnactment(x) => Self::downcast_undo(x, entity),
            Self::PoolDepositRefund(x) => Self::downcast_undo(x, entity),
            Self::EpochTransition(x) => Self::downcast_undo(x, entity),
            Self::EpochWrapUp(x) => Self::downcast_undo(x, entity),
            Self::DRepDelegatorDrop(x) => Self::downcast_undo(x, entity),
            Self::PoolWrapUp(x) => Self::downcast_undo(x, entity),
        }
    }
}
