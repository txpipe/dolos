use std::{
    cmp::Ordering,
    ops::{Deref, DerefMut},
};

use dolos_core::{
    BlockSlot, ChainError, EntityKey, EntityValue, Namespace, NamespaceType, NsKey, StateError,
    StateSchema,
};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::{
        hash::Hash,
        nonce::{generate_epoch_nonce, generate_rolling_nonce},
    },
    ledger::primitives::{
        conway::{CostModels, DRep, DRepVotingThresholds, PoolVotingThresholds},
        Coin, Epoch, ExUnitPrices, ExUnits, Nonce, PoolMetadata, ProtocolVersion, RationalNumber,
        Relay, UnitInterval,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    pallas_extras::{
        default_cost_models, default_drep_voting_thresholds, default_ex_unit_prices,
        default_ex_units, default_nonce, default_pool_voting_thresholds, default_rational_number,
    },
    roll::{
        accounts::{
            ControlledAmountDec, ControlledAmountInc, StakeDelegation, StakeDeregistration,
            StakeRegistration, VoteDelegation, WithdrawalInc,
        },
        assets::MintStatsUpdate,
        dreps::{DRepRegistration, DRepUnRegistration},
        epochs::{EpochStatsUpdate, NoncesUpdate, PParamsUpdate},
        pools::{MintedBlocksInc, PoolRegistration},
    },
};

pub trait FixedNamespace {
    const NS: &'static str;
}

macro_rules! entity_boilerplate {
    ($type:ident, $ns:literal) => {
        impl FixedNamespace for $type {
            const NS: &str = $ns;
        }

        impl dolos_core::Entity for $type {
            fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, StateError> {
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
    pub epoch: u32,

    #[n(1)]
    pub amount: u64,

    #[n(2)]
    pub pool_id: Vec<u8>,

    #[n(3)]
    pub as_leader: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct AccountState {
    #[n(0)]
    pub registered_at: Option<u64>,

    #[n(1)]
    pub controlled_amount: u64,

    #[n(2)]
    pub wait_stake: u64,

    #[n(3)]
    pub active_stake: u64,

    #[n(4)]
    pub rewards_sum: u64,

    #[n(5)]
    pub withdrawals_sum: u64,

    #[n(6)]
    pub reserves_sum: u64,

    #[n(7)]
    pub treasury_sum: u64,

    #[n(8)]
    pub latest_pool: Option<Vec<u8>>,

    #[n(9)]
    pub active_pool: Option<Vec<u8>>,

    #[n(10)]
    pub drep: Option<DRep>,
}

entity_boilerplate!(AccountState, "accounts");

impl AccountState {
    pub fn withdrawable_amount(&self) -> u64 {
        self.rewards_sum.saturating_add(self.withdrawals_sum)
    }

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
    pub active_stake: u64,

    #[n(9)]
    pub wait_stake: u64,

    #[n(10)]
    pub __live_stake: u64,

    #[n(11)]
    pub blocks_minted: u32,

    #[n(12)]
    pub register_slot: u64,
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

impl PoolState {
    pub fn new(slot: BlockSlot, vrf_keyhash: Hash<32>) -> Self {
        Self {
            register_slot: slot,
            vrf_keyhash,
            reward_account: Default::default(),
            pool_owners: Default::default(),
            relays: Default::default(),
            declared_pledge: Default::default(),
            margin_cost: RationalNumber {
                numerator: 0,
                denominator: 1,
            },
            fixed_cost: Default::default(),
            metadata: Default::default(),
            active_stake: Default::default(),
            wait_stake: Default::default(),
            __live_stake: Default::default(),
            blocks_minted: Default::default(),
        }
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
    CostModelsForScriptLanguages = 21,
    ExecutionCosts = 22,
    MaxTxExUnits = 23,
    MaxBlockExUnits = 24,
    MaxValueSize = 25,
    CollateralPercentage = 26,
    MaxCollateralInputs = 27,
    PoolVotingThresholds = 28,
    DrepVotingThresholds = 29,
    MinCommitteeSize = 30,
    CommitteeTermLimit = 31,
    GovernanceActionValidityPeriod = 32,
    GovernanceActionDeposit = 33,
    DrepDeposit = 34,
    DrepInactivityPeriod = 35,
    MinFeeRefScriptCostPerByte = 36,
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
            Self::CostModelsForScriptLanguages => {
                PParamValue::CostModelsForScriptLanguages(default_cost_models())
            }
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
    CostModelsForScriptLanguages(#[n(0)] CostModels),

    #[n(22)]
    ExecutionCosts(#[n(0)] ExUnitPrices),

    #[n(23)]
    MaxTxExUnits(#[n(0)] ExUnits),

    #[n(24)]
    MaxBlockExUnits(#[n(0)] ExUnits),

    #[n(25)]
    MaxValueSize(#[n(0)] u32),

    #[n(26)]
    CollateralPercentage(#[n(0)] u32),

    #[n(27)]
    MaxCollateralInputs(#[n(0)] u32),

    #[n(28)]
    PoolVotingThresholds(#[n(0)] PoolVotingThresholds),

    #[n(29)]
    DrepVotingThresholds(#[n(0)] DRepVotingThresholds),

    #[n(30)]
    MinCommitteeSize(#[n(0)] u64),

    #[n(31)]
    CommitteeTermLimit(#[n(0)] Epoch),

    #[n(32)]
    GovernanceActionValidityPeriod(#[n(0)] Epoch),

    #[n(33)]
    GovernanceActionDeposit(#[n(0)] Coin),

    #[n(34)]
    DrepDeposit(#[n(0)] Coin),

    #[n(35)]
    DrepInactivityPeriod(#[n(0)] Epoch),

    #[n(36)]
    MinFeeRefScriptCostPerByte(#[n(0)] UnitInterval),
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
            Self::CostModelsForScriptLanguages(_) => PParamKind::CostModelsForScriptLanguages,
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
        }
    }
}

#[derive(Debug, Encode, Decode, Clone, Default)]
#[cbor(transparent)]
pub struct PParamsSet(#[n(0)] Vec<PParamValue>);

impl Deref for PParamsSet {
    type Target = Vec<PParamValue>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for PParamsSet {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
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
    pub fn new() -> Self {
        Self(Vec::new())
    }

    pub fn get(&self, kind: PParamKind) -> Option<&PParamValue> {
        self.0.iter().find(|value| value.kind() == kind)
    }

    pub fn get_mut(&mut self, kind: PParamKind) -> Option<&mut PParamValue> {
        self.0.iter_mut().find(|value| value.kind() == kind)
    }

    pub fn set(&mut self, value: PParamValue) {
        let existing = self.get_mut(value.kind());

        if let Some(existing) = existing {
            *existing = value;
        } else {
            self.0.push(value);
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

    ensure_pparam!(rho, RationalNumber);
    ensure_pparam!(tau, RationalNumber);
    ensure_pparam!(k, u32);
    ensure_pparam!(a0, RationalNumber);

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
    pgetter!(CostModelsForScriptLanguages, CostModels);
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

#[derive(Debug, Encode, Decode, Clone, Default)]
pub struct EpochState {
    #[n(0)]
    pub number: u32,

    /// The static value representing what should be considered the active stake
    /// for this epoch (computed from -2 epochs ago).
    #[n(1)]
    pub active_stake: u64,

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
    pub rewards_to_distribute: Option<u64>,

    #[n(10)]
    pub rewards_to_treasury: Option<u64>,

    #[n(11)]
    pub pparams: PParamsSet,

    #[n(12)]
    pub largest_stable_slot: BlockSlot,

    #[n(13)]
    pub nonces: Option<Nonces>,
}

impl EpochState {
    pub fn rewards(&self) -> Option<u64> {
        let to_distribute = self.rewards_to_distribute?;
        let to_treasury = self.rewards_to_treasury?;
        Some(to_distribute + to_treasury)
    }
}

entity_boilerplate!(EpochState, "epochs");

pub const EPOCH_KEY_GO: &[u8] = b"2";
pub const EPOCH_KEY_SET: &[u8] = b"1";
pub const EPOCH_KEY_MARK: &[u8] = b"0";

#[derive(Debug, Encode, Decode, Clone, Default)]
pub struct DRepState {
    #[n(0)]
    pub drep_id: Vec<u8>,

    #[n(1)]
    pub initial_slot: Option<u64>,

    #[n(2)]
    pub voting_power: u64,

    #[n(3)]
    pub last_active_slot: Option<u64>,

    #[n(4)]
    pub retired: bool,
}

impl DRepState {
    /// Check that the first byte of the drep id finishes with the 0011 bytes.
    pub fn has_script(&self) -> bool {
        let first = self.drep_id.first().unwrap();
        first & 0b00001111 == 0b00000011
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

impl dolos_core::Entity for CardanoEntity {
    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, StateError> {
        match ns {
            EraSummary::NS => EraSummary::decode_entity(ns, value).map(Into::into),
            AccountState::NS => AccountState::decode_entity(ns, value).map(Into::into),
            AssetState::NS => AssetState::decode_entity(ns, value).map(Into::into),
            PoolState::NS => PoolState::decode_entity(ns, value).map(Into::into),
            EpochState::NS => EpochState::decode_entity(ns, value).map(Into::into),
            DRepState::NS => DRepState::decode_entity(ns, value).map(Into::into),
            _ => Err(StateError::InvalidNamespace(ns)),
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
    MintedBlocksInc(MintedBlocksInc),
    MintStatsUpdate(MintStatsUpdate),
    EpochStatsUpdate(EpochStatsUpdate),
    DRepRegistration(DRepRegistration),
    DRepUnRegistration(DRepUnRegistration),
    WithdrawalInc(WithdrawalInc),
    VoteDelegation(VoteDelegation),
    PParamsUpdate(PParamsUpdate),
    NoncesUpdate(NoncesUpdate),
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
delta_from!(MintedBlocksInc);
delta_from!(MintStatsUpdate);
delta_from!(EpochStatsUpdate);
delta_from!(DRepRegistration);
delta_from!(DRepUnRegistration);
delta_from!(WithdrawalInc);
delta_from!(VoteDelegation);
delta_from!(PParamsUpdate);
delta_from!(NoncesUpdate);

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
            Self::MintedBlocksInc(x) => x.key(),
            Self::MintStatsUpdate(x) => x.key(),
            Self::EpochStatsUpdate(x) => x.key(),
            Self::DRepRegistration(x) => x.key(),
            Self::DRepUnRegistration(x) => x.key(),
            Self::WithdrawalInc(x) => x.key(),
            Self::VoteDelegation(x) => x.key(),
            Self::PParamsUpdate(x) => x.key(),
            Self::NoncesUpdate(x) => x.key(),
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
            Self::MintedBlocksInc(x) => Self::downcast_apply(x, entity),
            Self::MintStatsUpdate(x) => Self::downcast_apply(x, entity),
            Self::EpochStatsUpdate(x) => Self::downcast_apply(x, entity),
            Self::DRepRegistration(x) => Self::downcast_apply(x, entity),
            Self::DRepUnRegistration(x) => Self::downcast_apply(x, entity),
            Self::WithdrawalInc(x) => Self::downcast_apply(x, entity),
            Self::VoteDelegation(x) => Self::downcast_apply(x, entity),
            Self::PParamsUpdate(x) => Self::downcast_apply(x, entity),
            Self::NoncesUpdate(x) => Self::downcast_apply(x, entity),
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
            Self::MintedBlocksInc(x) => Self::downcast_undo(x, entity),
            Self::MintStatsUpdate(x) => Self::downcast_undo(x, entity),
            Self::EpochStatsUpdate(x) => Self::downcast_undo(x, entity),
            Self::DRepRegistration(x) => Self::downcast_undo(x, entity),
            Self::DRepUnRegistration(x) => Self::downcast_undo(x, entity),
            Self::WithdrawalInc(x) => Self::downcast_undo(x, entity),
            Self::VoteDelegation(x) => Self::downcast_undo(x, entity),
            Self::PParamsUpdate(x) => Self::downcast_undo(x, entity),
            Self::NoncesUpdate(x) => Self::downcast_undo(x, entity),
        }
    }
}
