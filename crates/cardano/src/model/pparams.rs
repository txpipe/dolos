use std::collections::BTreeMap;

use dolos_core::ChainError;
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    ledger::primitives::{
        conway::{CostModels, DRepVotingThresholds, PoolVotingThresholds},
        Coin, Epoch, ExUnitPrices, ExUnits, Nonce, ProtocolVersion, RationalNumber,
        UnitInterval,
    },
};
use serde::{Deserialize, Serialize};

use super::{
    epoch_value::{EpochValue, TransitionDefault},
    eras::{EraProtocol, EraTransition},
};
use crate::pallas_extras::{
    default_cost_models, default_drep_voting_thresholds, default_ex_unit_prices,
    default_ex_units, default_nonce, default_pool_voting_thresholds, default_rational_number,
};

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

pub const SHELLEY_PROTOCOL: u16 = 2;

impl PParamsSet {
    pub fn is_byron(&self) -> bool {
        self.protocol_major_or_default() < SHELLEY_PROTOCOL
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
    ensure_pparam!(protocol_major, u16);

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
