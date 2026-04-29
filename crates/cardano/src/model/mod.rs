use dolos_core::{ChainError, EntityValue, Namespace, NamespaceType, NsKey, StateSchema};

pub trait FixedNamespace {
    const NS: &'static str;
}

macro_rules! entity_boilerplate {
    ($type:ident, $ns:literal) => {
        impl super::FixedNamespace for $type {
            const NS: &str = $ns;
        }

        impl dolos_core::Entity for $type {
            fn decode_entity(
                ns: dolos_core::Namespace,
                value: &dolos_core::EntityValue,
            ) -> Result<Self, dolos_core::ChainError> {
                assert_eq!(ns, <$type as super::FixedNamespace>::NS);
                let value = pallas::codec::minicbor::decode(value)?;
                Ok(value)
            }

            fn encode_entity(value: &Self) -> (dolos_core::Namespace, dolos_core::EntityValue) {
                let value = pallas::codec::minicbor::to_vec(value).unwrap();
                (<$type as super::FixedNamespace>::NS, value)
            }
        }
    };
}

pub mod accounts;
pub mod assets;
pub mod datums;
pub mod dreps;
pub mod epoch_value;
pub mod epochs;
pub mod eras;
pub mod logs;
pub mod pending;
pub mod pools;
pub mod pparams;
pub mod proposals;

#[cfg(test)]
pub(crate) mod testing;

pub use accounts::*;
pub use assets::*;
pub use datums::*;
pub use dreps::*;
pub use epoch_value::*;
pub use epochs::*;
pub use eras::*;
pub use logs::*;
pub use pending::*;
pub use pools::*;
pub use pparams::*;
pub use proposals::*;

// --- CardanoEntity ---

#[derive(Debug, Clone)]
pub enum CardanoEntity {
    EraSummary(Box<EraSummary>),
    AccountState(Box<AccountState>),
    AssetState(Box<AssetState>),
    PoolState(Box<PoolState>),
    EpochState(Box<EpochState>),
    DRepState(Box<DRepState>),
    ProposalState(Box<ProposalState>),
    LeaderRewardLog(Box<LeaderRewardLog>),
    MemberRewardLog(Box<MemberRewardLog>),
    PoolDepositRefundLog(Box<PoolDepositRefundLog>),
    StakeLog(Box<StakeLog>),
    DatumState(Box<DatumState>),
    PendingRewardState(Box<PendingRewardState>),
    PendingMirState(Box<PendingMirState>),
}

macro_rules! variant_boilerplate {
    ($variant:ident) => {
        impl From<CardanoEntity> for Option<$variant> {
            fn from(value: CardanoEntity) -> Self {
                match value {
                    CardanoEntity::$variant(x) => Some(*x),
                    _ => None,
                }
            }
        }

        impl From<$variant> for CardanoEntity {
            fn from(value: $variant) -> Self {
                CardanoEntity::$variant(Box::new(value))
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
variant_boilerplate!(ProposalState);
variant_boilerplate!(LeaderRewardLog);
variant_boilerplate!(MemberRewardLog);
variant_boilerplate!(PoolDepositRefundLog);
variant_boilerplate!(StakeLog);
variant_boilerplate!(DatumState);
variant_boilerplate!(PendingRewardState);
variant_boilerplate!(PendingMirState);

impl dolos_core::Entity for CardanoEntity {
    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, ChainError> {
        match ns {
            EraSummary::NS => EraSummary::decode_entity(ns, value).map(Into::into),
            AccountState::NS => AccountState::decode_entity(ns, value).map(Into::into),
            AssetState::NS => AssetState::decode_entity(ns, value).map(Into::into),
            PoolState::NS => PoolState::decode_entity(ns, value).map(Into::into),
            EpochState::NS => EpochState::decode_entity(ns, value).map(Into::into),
            DRepState::NS => DRepState::decode_entity(ns, value).map(Into::into),
            ProposalState::NS => ProposalState::decode_entity(ns, value).map(Into::into),
            LeaderRewardLog::NS => LeaderRewardLog::decode_entity(ns, value).map(Into::into),
            MemberRewardLog::NS => MemberRewardLog::decode_entity(ns, value).map(Into::into),
            PoolDepositRefundLog::NS => {
                PoolDepositRefundLog::decode_entity(ns, value).map(Into::into)
            }
            StakeLog::NS => StakeLog::decode_entity(ns, value).map(Into::into),
            DatumState::NS => DatumState::decode_entity(ns, value).map(Into::into),
            PendingRewardState::NS => PendingRewardState::decode_entity(ns, value).map(Into::into),
            PendingMirState::NS => PendingMirState::decode_entity(ns, value).map(Into::into),
            _ => Err(ChainError::InvalidNamespace(ns)),
        }
    }

    fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
        match value {
            Self::EraSummary(x) => EraSummary::encode_entity(x),
            Self::AccountState(x) => AccountState::encode_entity(x),
            Self::AssetState(x) => AssetState::encode_entity(x),
            Self::PoolState(x) => PoolState::encode_entity(x),
            Self::EpochState(x) => EpochState::encode_entity(x),
            Self::DRepState(x) => DRepState::encode_entity(x),
            Self::ProposalState(x) => ProposalState::encode_entity(x),
            Self::LeaderRewardLog(x) => LeaderRewardLog::encode_entity(x),
            Self::MemberRewardLog(x) => MemberRewardLog::encode_entity(x),
            Self::PoolDepositRefundLog(x) => PoolDepositRefundLog::encode_entity(x),
            Self::StakeLog(x) => StakeLog::encode_entity(x),
            Self::DatumState(x) => DatumState::encode_entity(x),
            Self::PendingRewardState(x) => PendingRewardState::encode_entity(x),
            Self::PendingMirState(x) => PendingMirState::encode_entity(x),
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
    schema.insert(ProposalState::NS, NamespaceType::KeyValue);
    schema.insert(LeaderRewardLog::NS, NamespaceType::KeyValue);
    schema.insert(MemberRewardLog::NS, NamespaceType::KeyValue);
    schema.insert(PoolDepositRefundLog::NS, NamespaceType::KeyValue);
    schema.insert(StakeLog::NS, NamespaceType::KeyValue);
    schema.insert(DatumState::NS, NamespaceType::KeyValue);
    schema.insert(PendingRewardState::NS, NamespaceType::KeyValue);
    schema.insert(PendingMirState::NS, NamespaceType::KeyValue);
    schema
}

// --- CardanoDelta ---

// Variant order is part of the on-disk WAL format: bincode encodes enum
// variants by positional index (no name tags). Indices 0..=38 are frozen
// to match pre-PR `main` so existing WAL rows decode correctly. New
// variants must be appended to the end. The `EpochTransition` and
// `EpochWrapUp` variants point at the *legacy* (deprecated) struct
// shapes for the same reason; `EpochTransitionV2` / `EpochWrapUpV2` are
// the live shapes used by all new commit paths.
#[allow(deprecated)]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum CardanoDelta {
    ControlledAmountInc(Box<ControlledAmountInc>),
    ControlledAmountDec(Box<ControlledAmountDec>),
    StakeRegistration(Box<StakeRegistration>),
    StakeDelegation(Box<StakeDelegation>),
    StakeDeregistration(Box<StakeDeregistration>),
    PoolRegistration(Box<PoolRegistration>),
    PoolDeRegistration(Box<PoolDeRegistration>),
    MintedBlocksInc(Box<MintedBlocksInc>),
    MintStatsUpdate(Box<MintStatsUpdate>),
    MetadataTxUpdate(Box<MetadataTxUpdate>),
    EpochStatsUpdate(Box<EpochStatsUpdate>),
    DRepRegistration(Box<DRepRegistration>),
    DRepUnRegistration(Box<DRepUnRegistration>),
    DRepActivity(Box<DRepActivity>),
    DRepExpiration(Box<DRepExpiration>),
    WithdrawalInc(Box<WithdrawalInc>),
    VoteDelegation(Box<VoteDelegation>),
    PParamsUpdate(Box<PParamsUpdate>),
    NoncesUpdate(Box<NoncesUpdate>),
    NewProposal(Box<NewProposal>),
    AssignRewards(Box<AssignRewards>),
    NonceTransition(Box<NonceTransition>),
    PoolTransition(Box<PoolTransition>),
    AccountTransition(Box<AccountTransition>),
    PoolDepositRefund(Box<PoolDepositRefund>),
    EpochTransition(Box<EpochTransition>),
    EpochWrapUp(Box<EpochWrapUp>),
    DRepDelegatorDrop(Box<DRepDelegatorDrop>),
    PoolDelegatorRetire(Box<PoolDelegatorRetire>),
    PoolWrapUp(Box<PoolWrapUp>),
    ProposalDepositRefund(Box<ProposalDepositRefund>),
    TreasuryWithdrawal(Box<TreasuryWithdrawal>),
    EnqueueMir(Box<EnqueueMir>),
    DequeueMir(Box<DequeueMir>),
    DatumRefIncrement(Box<DatumRefIncrement>),
    DatumRefDecrement(Box<DatumRefDecrement>),
    EnqueueReward(Box<EnqueueReward>),
    SetEpochIncentives(Box<SetEpochIncentives>),
    DequeueReward(Box<DequeueReward>),
    EWrapProgress(Box<EWrapProgress>),
    EStartProgress(Box<EStartProgress>),
    RupdProgress(Box<RupdProgress>),
    EpochWrapUpV2(Box<EpochWrapUpV2>),
    EpochTransitionV2(Box<EpochTransitionV2>),
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
                Self::$type(Box::new(value))
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
delta_from!(MetadataTxUpdate);
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
delta_from!(AssignRewards);
delta_from!(NonceTransition);
delta_from!(PoolTransition);
delta_from!(AccountTransition);
delta_from!(PoolDepositRefund);
#[allow(deprecated)]
mod legacy_delta_from {
    use super::*;
    delta_from!(EpochTransition);
    delta_from!(EpochWrapUp);
}
delta_from!(DRepDelegatorDrop);
delta_from!(PoolDelegatorRetire);
delta_from!(PoolWrapUp);
delta_from!(ProposalDepositRefund);
delta_from!(TreasuryWithdrawal);
delta_from!(EnqueueMir);
delta_from!(DequeueMir);
delta_from!(DatumRefIncrement);
delta_from!(DatumRefDecrement);
delta_from!(EnqueueReward);
delta_from!(SetEpochIncentives);
delta_from!(DequeueReward);
delta_from!(EWrapProgress);
delta_from!(EStartProgress);
delta_from!(RupdProgress);
delta_from!(EpochWrapUpV2);
delta_from!(EpochTransitionV2);

#[allow(deprecated)]
impl dolos_core::EntityDelta for CardanoDelta {
    type Entity = CardanoEntity;

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
            Self::MetadataTxUpdate(x) => x.key(),
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
            Self::AssignRewards(x) => x.key(),
            Self::NonceTransition(x) => x.key(),
            Self::PoolTransition(x) => x.key(),
            Self::AccountTransition(x) => x.key(),
            Self::PoolDepositRefund(x) => x.key(),
            Self::EpochTransition(x) => x.key(),
            Self::EpochWrapUp(x) => x.key(),
            Self::EWrapProgress(x) => x.key(),
            Self::EStartProgress(x) => x.key(),
            Self::RupdProgress(x) => x.key(),
            Self::EpochWrapUpV2(x) => x.key(),
            Self::EpochTransitionV2(x) => x.key(),
            Self::PoolDelegatorRetire(x) => x.key(),
            Self::DRepDelegatorDrop(x) => x.key(),
            Self::PoolWrapUp(x) => x.key(),
            Self::ProposalDepositRefund(x) => x.key(),
            Self::TreasuryWithdrawal(x) => x.key(),
            Self::EnqueueMir(x) => x.key(),
            Self::DequeueMir(x) => x.key(),
            Self::DatumRefIncrement(x) => x.key(),
            Self::DatumRefDecrement(x) => x.key(),
            Self::EnqueueReward(x) => x.key(),
            Self::SetEpochIncentives(x) => x.key(),
            Self::DequeueReward(x) => x.key(),
        }
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        match self {
            Self::ControlledAmountInc(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::ControlledAmountDec(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StakeRegistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StakeDelegation(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StakeDeregistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PoolRegistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PoolDeRegistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::MintedBlocksInc(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::MintStatsUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::MetadataTxUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EpochStatsUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepRegistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepUnRegistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepActivity(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepExpiration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::WithdrawalInc(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::VoteDelegation(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PParamsUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::NoncesUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::NewProposal(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::AssignRewards(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::NonceTransition(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PoolTransition(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::AccountTransition(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PoolDepositRefund(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EpochTransition(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EpochWrapUp(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EWrapProgress(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EStartProgress(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::RupdProgress(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EpochWrapUpV2(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EpochTransitionV2(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepDelegatorDrop(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PoolDelegatorRetire(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PoolWrapUp(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::ProposalDepositRefund(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::TreasuryWithdrawal(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EnqueueMir(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DequeueMir(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DatumRefIncrement(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DatumRefDecrement(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::EnqueueReward(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::SetEpochIncentives(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DequeueReward(x) => Self::downcast_apply(x.as_mut(), entity),
        }
    }

    fn undo(&self, entity: &mut Option<Self::Entity>) {
        match self {
            Self::ControlledAmountInc(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::ControlledAmountDec(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StakeRegistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StakeDelegation(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StakeDeregistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PoolRegistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PoolDeRegistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::MintedBlocksInc(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::MintStatsUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::MetadataTxUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EpochStatsUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepRegistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepUnRegistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepActivity(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepExpiration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::WithdrawalInc(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::VoteDelegation(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PParamsUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::NoncesUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::NewProposal(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::AssignRewards(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::NonceTransition(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PoolTransition(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::AccountTransition(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PoolDepositRefund(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EpochTransition(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EpochWrapUp(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EWrapProgress(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EStartProgress(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::RupdProgress(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EpochWrapUpV2(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EpochTransitionV2(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepDelegatorDrop(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PoolDelegatorRetire(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PoolWrapUp(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::ProposalDepositRefund(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::TreasuryWithdrawal(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EnqueueMir(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DequeueMir(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DatumRefIncrement(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DatumRefDecrement(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::EnqueueReward(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::SetEpochIncentives(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DequeueReward(x) => Self::downcast_undo(x.as_ref(), entity),
        }
    }
}
