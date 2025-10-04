use std::{cmp::Ordering, collections::BTreeMap};

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
        Relay, UnitInterval,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    pallas_extras::{
        self, default_cost_models, default_drep_voting_thresholds, default_ex_unit_prices,
        default_ex_units, default_nonce, default_pool_voting_thresholds, default_rational_number,
    },
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
    sweep::{
        retires::{
            DRepDelegatorDrop, DRepExpiration, PoolDelegatorDrop, PoolRetirement,
            ProposalExpiration,
        },
        rewards::{AssignDelegatorRewards, AssignPoolRewards},
        transition::{AccountTransition, PoolTransition, ProposalEnactment},
    },
};

#[derive(Debug, Encode, Decode, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct EpochValue<T> {
    /// Current epoch version of the value
    #[n(0)]
    pub latest: T,

    /// Epoch - 1 version of the value
    #[n(1)]
    pub previous: Option<T>,

    /// Epoch - 2 version of the value
    #[n(2)]
    pub stable: Option<T>,

    /// The epoch at which this value was updated for the last time
    #[n(3)]
    pub epoch: Epoch,
}

impl<T> EpochValue<T>
where
    T: Clone + std::fmt::Debug,
{
    pub fn new(latest: T, epoch: Epoch) -> Self {
        Self {
            latest,
            previous: None,
            stable: None,
            epoch,
        }
    }

    /// Updates the latest value for the current epoch without rotating any of
    /// the previous values
    pub fn update(&mut self, latest: T, epoch: Epoch) {
        assert_eq!(epoch, self.epoch);
        self.latest = latest;
    }

    /// Same as update, but without checking that that the epoch matches.
    pub fn update_unchecked(&mut self, latest: T) {
        self.latest = latest;
    }

    /// Transitions into the next epoch by rotating the previous values and
    /// cloning the latest one.
    pub fn transition(&mut self, next_epoch: Epoch) {
        assert_eq!(next_epoch, self.epoch + 1);
        self.transition_unchecked();
    }

    /// Same as transition, but without checking that that the epoch matches.
    pub fn transition_unchecked(&mut self) {
        self.stable = self.previous.clone();
        self.previous = Some(self.latest.clone());
        self.epoch += 1;
        // latest remains the same
    }

    pub fn version_for(&self, epoch: Epoch) -> Option<&T> {
        if epoch == self.epoch {
            Some(&self.latest)
        } else if epoch == self.epoch - 1 {
            self.previous.as_ref()
        } else if epoch == self.epoch - 2 {
            self.stable.as_ref()
        } else {
            None
        }
    }

    pub fn try_version_for(&self, epoch: Epoch) -> Result<&T, ChainError> {
        match self.version_for(epoch) {
            Some(value) => Ok(value),
            None => {
                dbg!(self);
                Err(ChainError::EpochValueVersionNotFound(epoch))
            }
        }
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
    pub blocks_minted: u32,

    /// Active (Snapshot of live stake 2 epochs ago) stake in Lovelaces
    #[n(1)]
    pub active_stake: u64,

    /// Pool size (percentage) of overall active stake at that epoch
    #[n(2)]
    pub active_size: f64,

    /// Number of delegators for epoch
    #[n(3)]
    pub delegators_count: u64,

    /// Total rewards received before distribution to delegators
    #[n(4)]
    pub rewards: u64,

    /// Pool operator rewards
    #[n(5)]
    pub fees: u64,

    /// Live pledge
    #[n(6)]
    pub live_pledge: u64,

    /// Declared pledge
    #[n(7)]
    pub declared_pledge: u64,
}

entity_boilerplate!(StakeLog, "stakes");

pub type PoolHash = Hash<28>;

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode)]
pub struct AccountState {
    #[n(0)]
    pub registered_at: Option<u64>,

    #[n(1)]
    pub controlled_amount: u64,

    #[n(2)]
    pub total_stake: EpochValue<u64>,

    #[n(3)]
    pub rewards_sum: u64,

    #[n(4)]
    pub withdrawals_sum: u64,

    #[n(5)]
    pub reserves_sum: u64,

    #[n(6)]
    pub treasury_sum: u64,

    #[n(7)]
    pub pool: EpochValue<Option<PoolHash>>,

    #[n(9)]
    pub drep: EpochValue<Option<DRep>>,

    #[n(11)]
    pub deposit: u64,

    #[n(12)]
    pub deregistered_at: Option<u64>,
}

entity_boilerplate!(AccountState, "accounts");

impl AccountState {
    pub fn new(epoch: Epoch) -> Self {
        Self {
            registered_at: None,
            controlled_amount: 0,
            total_stake: EpochValue::new(0, epoch),
            rewards_sum: 0,
            withdrawals_sum: 0,
            reserves_sum: 0,
            treasury_sum: 0,
            pool: EpochValue::new(None, epoch),
            drep: EpochValue::new(None, epoch),
            deposit: 0,
            deregistered_at: None,
        }
    }

    pub fn withdrawable_amount(&self) -> u64 {
        self.rewards_sum.saturating_add(self.withdrawals_sum)
    }

    pub fn is_registered(&self) -> bool {
        match (self.registered_at, self.deregistered_at) {
            (Some(_), None) => true,
            (Some(start), Some(end)) => start >= end,
            (None, _) => false,
        }
    }

    /// Computes the new stake from current values taking into account
    /// registration status.
    pub fn live_stake(&self) -> u64 {
        let mut out = self.controlled_amount;
        out += self.rewards_sum;
        out = out.saturating_sub(self.withdrawals_sum);

        out
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
pub struct PoolState {
    #[n(0)]
    pub vrf_keyhash: Hash<32>,

    #[n(1)]
    pub reward_account: Vec<u8>,

    #[n(2)]
    pub pool_owners: Vec<Hash<28>>,

    #[n(3)]
    pub relays: Vec<Relay>,

    #[n(4)]
    pub declared_pledge: u64,

    #[n(5)]
    pub margin_cost: RationalNumber,

    #[n(6)]
    pub fixed_cost: u64,

    #[n(7)]
    pub metadata: Option<PoolMetadata>,

    #[n(8)]
    pub total_stake: EpochValue<u64>,

    #[n(11)]
    pub blocks_minted_total: u32,

    #[n(12)]
    pub register_slot: u64,

    #[n(13)]
    pub retiring_epoch: Option<u64>,

    #[n(14)]
    pub is_retired: bool,

    #[n(15)]
    pub blocks_minted_epoch: u32,

    #[n(16)]
    pub deposit: u64,
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

#[derive(Debug, Encode, Decode, Clone, Default, Serialize)]
pub struct PParamsSet {
    #[n(0)]
    values: Vec<PParamValue>,

    #[n(1)]
    version: u16,
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
    pub fn new(version: u16) -> Self {
        Self {
            values: Vec::new(),
            version,
        }
    }

    /// Clone values while incrementing the original version number.
    ///
    /// This is used during forks to setup a starting set of values for the next
    /// version. It usually follows with several `with` calls to set the values
    /// for the new version.
    pub fn bump_clone(&self) -> Self {
        Self {
            values: self.values.clone(),
            version: self.version + 1,
        }
    }

    /// The original version of the pparams set
    ///
    /// Since the protocol version param might be updated throughout an epoch to
    /// flag a fork, we need this value to understand the version that defines
    /// the format used to construct the params originally.
    pub fn version(&self) -> u16 {
        self.version
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

    pub fn with(mut self, value: PParamValue) -> Self {
        self.set(value);

        self
    }

    pub fn get_or_default(&self, kind: PParamKind) -> PParamValue {
        self.get(kind)
            .cloned()
            .unwrap_or_else(|| PParamKind::default_value(kind))
    }

    pub fn protocol_major(&self) -> Option<u16> {
        self.protocol_version().map(|(major, _)| major as u16)
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

#[derive(Debug, Encode, Decode, Clone)]
pub struct EpochState {
    #[n(0)]
    pub number: Epoch,

    #[n(2)]
    pub deposits: u64,

    #[n(3)]
    pub reserves: u64,

    #[n(4)]
    pub treasury: u64,

    #[n(5)]
    pub utxos: u64,

    #[n(6)]
    pub gathered_fees: u64,

    #[n(7)]
    pub gathered_deposits: u64,

    #[n(8)]
    pub decayed_deposits: u64,

    #[n(9)]
    pub effective_rewards: Option<u64>,

    #[n(10)]
    pub unspendable_rewards: Option<u64>,

    #[n(11)]
    pub pparams: PParamsSet,

    #[n(12)]
    pub largest_stable_slot: BlockSlot,

    #[n(13)]
    pub nonces: Option<Nonces>,

    #[n(14)]
    pub blocks_minted: u32,

    #[n(15)]
    pub treasury_tax: Option<u64>,
}

impl EpochState {
    pub fn incentives(&self) -> Option<u64> {
        let total = self.treasury_tax.unwrap_or_default()
            + self.effective_rewards.unwrap_or_default()
            + self.unspendable_rewards.unwrap_or_default();

        Some(total)
    }
}

entity_boilerplate!(EpochState, "epochs");

pub const EPOCH_KEY_GO: &[u8] = b"2";
pub const EPOCH_KEY_SET: &[u8] = b"1";
pub const EPOCH_KEY_MARK: &[u8] = b"0";

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

#[derive(Debug, Encode, Decode, Clone, Default)]
pub struct DRepState {
    // TODO: field is deprecated, remove it in the next breaking change
    #[n(0)]
    pub __drep_id: Vec<u8>,

    #[n(1)]
    pub initial_slot: Option<u64>,

    #[n(2)]
    pub voting_power: u64,

    #[n(3)]
    pub last_active_slot: Option<u64>,

    #[n(4)]
    pub retired: bool,

    #[n(5)]
    pub expired: bool,

    #[n(6)]
    pub deposit: u64,
}

impl DRepState {
    pub fn new() -> Self {
        Self {
            __drep_id: vec![],
            initial_slot: None,
            voting_power: 0,
            last_active_slot: None,
            retired: false,
            expired: false,
            deposit: 0,
        }
    }
}

entity_boilerplate!(DRepState, "dreps");

#[derive(Debug, Clone, Copy)]
pub struct EraProtocol(u16);

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
}

entity_boilerplate!(EraSummary, "eras");

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
    DRepDelegatorDrop(DRepDelegatorDrop),
    PoolRetirement(PoolRetirement),
    AssignPoolRewards(AssignPoolRewards),
    AssignDelegatorRewards(AssignDelegatorRewards),
    //AssignEpochRewards(AssignEpochRewards),
    PoolTransition(PoolTransition),
    AccountTransition(AccountTransition),
    ProposalExpiration(ProposalExpiration),
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
delta_from!(DRepDelegatorDrop);
delta_from!(PoolRetirement);
delta_from!(AssignPoolRewards);
delta_from!(AssignDelegatorRewards);
//delta_from!(AssignEpochRewards);
delta_from!(PoolTransition);
delta_from!(AccountTransition);
delta_from!(ProposalExpiration);

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
            Self::PoolRetirement(x) => x.key(),
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
            Self::DRepDelegatorDrop(x) => x.key(),
            Self::AssignPoolRewards(x) => x.key(),
            Self::AssignDelegatorRewards(x) => x.key(),
            //Self::AssignEpochRewards(x) => x.key(),
            Self::PoolTransition(x) => x.key(),
            Self::AccountTransition(x) => x.key(),
            Self::ProposalExpiration(x) => x.key(),
            Self::ProposalEnactment(x) => x.key(),
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
            Self::PoolRetirement(x) => Self::downcast_apply(x, entity),
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
            Self::DRepDelegatorDrop(x) => Self::downcast_apply(x, entity),
            Self::AssignPoolRewards(x) => Self::downcast_apply(x, entity),
            Self::AssignDelegatorRewards(x) => Self::downcast_apply(x, entity),
            //Self::AssignEpochRewards(x) => Self::downcast_apply(x, entity),
            Self::PoolTransition(x) => Self::downcast_apply(x, entity),
            Self::AccountTransition(x) => Self::downcast_apply(x, entity),
            Self::ProposalExpiration(x) => Self::downcast_apply(x, entity),
            Self::ProposalEnactment(x) => Self::downcast_apply(x, entity),
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
            Self::PoolRetirement(x) => Self::downcast_undo(x, entity),
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
            Self::DRepDelegatorDrop(x) => Self::downcast_undo(x, entity),
            Self::AssignPoolRewards(x) => Self::downcast_undo(x, entity),
            Self::AssignDelegatorRewards(x) => Self::downcast_undo(x, entity),
            //Self::AssignEpochRewards(x) => Self::downcast_undo(x, entity),
            Self::PoolTransition(x) => Self::downcast_undo(x, entity),
            Self::AccountTransition(x) => Self::downcast_undo(x, entity),
            Self::ProposalExpiration(x) => Self::downcast_undo(x, entity),
            Self::ProposalEnactment(x) => Self::downcast_undo(x, entity),
        }
    }
}
