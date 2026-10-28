use dolos_core::{ChainError, EntityValue, Namespace, NamespaceType, NsKey, StateSchema};

pub use dolos_core::{FixedNamespace, LogEntity, StateEntity};

/// A namespace that holds exactly one row, at the empty key.
///
/// Singletons don't fit the population-shaped machinery (namespace
/// streams, key ranges): readers address the row directly
/// (`crate::read_singleton`) and boundary commits apply deltas through
/// `WorkDeltas::apply_singleton`, which — unlike the streams — handles
/// the row not existing yet.
pub trait SingletonEntity: StateEntity<Key = ()> {
    fn ns_key() -> NsKey {
        NsKey::of::<Self>(&())
    }
}

/// `FixedNamespace` and `Entity` for an entity type, and the key kinds it is
/// stored under: `state = K` for a state entity, `log = K` for an archive
/// log (the part after the slot).
macro_rules! entity_boilerplate {
    ($type:ident, $ns:literal $(, state = $state:ty)? $(, log = $log:ty)?) => {
        impl dolos_core::FixedNamespace for $type {
            const NS: &str = $ns;
        }

        impl dolos_core::Entity for $type {
            fn decode_entity(
                ns: dolos_core::Namespace,
                value: &dolos_core::EntityValue,
            ) -> Result<Self, dolos_core::ChainError> {
                assert_eq!(ns, <$type as dolos_core::FixedNamespace>::NS);
                let value = pallas::codec::minicbor::decode(value)?;
                Ok(value)
            }

            fn encode_entity(value: &Self) -> (dolos_core::Namespace, dolos_core::EntityValue) {
                let value = pallas::codec::minicbor::to_vec(value).unwrap();
                (<$type as dolos_core::FixedNamespace>::NS, value)
            }
        }

        $(
            impl dolos_core::StateEntity for $type {
                type Key = $state;
            }
        )?

        $(
            impl dolos_core::LogEntity for $type {
                type Key = $log;
            }
        )?
    };
}

/// A key that is a hash, stored as the hash's bytes.
macro_rules! hash_key {
    ($(#[$doc:meta])* $name:ident, $hash:ty, $len:literal) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
        pub struct $name(pub $hash);

        impl From<$hash> for $name {
            fn from(value: $hash) -> Self {
                Self(value)
            }
        }

        impl From<$name> for $hash {
            fn from(value: $name) -> Self {
                value.0
            }
        }

        impl dolos_core::KeyCodec for $name {
            fn encode_key(&self, out: &mut dolos_core::KeyBytes) {
                out.extend_from_slice(self.0.as_slice());
            }

            fn decode_key(bytes: &[u8]) -> Result<Self, dolos_core::KeyError> {
                dolos_core::keys::exact::<$len>(bytes).map(|hash| Self(hash.into()))
            }
        }

        impl dolos_core::FixedKey for $name {
            const LEN: usize = $len;
        }
    };
}

/// Keys display as the hex of their stored bytes.
macro_rules! display_as_stored {
    ($($name:ty),*) => {
        $(
            impl std::fmt::Display for $name {
                fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    f.write_str(&hex::encode(dolos_core::keys::encode(self)))
                }
            }
        )*
    };
}

pub mod account_addresses;
pub mod accounts;
pub mod asset_mints;
pub mod assets;
pub mod datums;
pub mod drep_sightings;
pub mod dreps;
pub mod epoch_value;
pub mod epochs;
pub mod eras;
pub mod first_mints;
pub mod gov;
pub mod logs;
pub mod metadata_labels;
pub mod pending;
pub mod pools;
pub mod pparams;
pub mod proposals;
pub mod scripts;
pub mod stats;

#[cfg(test)]
pub(crate) mod testing;

pub use account_addresses::*;
pub use accounts::*;
pub use asset_mints::*;
pub use assets::*;
pub use datums::*;
pub use drep_sightings::*;
pub use dreps::*;
pub use epoch_value::*;
pub use epochs::*;
pub use eras::*;
pub use first_mints::*;
pub use gov::*;
pub use logs::*;
pub use metadata_labels::*;
pub use pending::*;
pub use pools::*;
pub use pparams::*;
pub use proposals::*;
pub use scripts::*;
pub use stats::*;

// --- CardanoEntity ---

#[derive(Debug, Clone)]
pub enum CardanoEntity {
    EraState(Box<EraState>),
    AccountState(Box<AccountState>),
    PoolState(Box<PoolState>),
    EpochState(Box<EpochState>),
    DRepState(Box<DRepState>),
    DRepSightingState(Box<DRepSightingState>),
    ProposalState(Box<ProposalState>),
    PoolEpochLog(Box<PoolEpochLog>),
    DatumState(Box<DatumState>),
    PendingRewardState(Box<PendingRewardState>),
    PendingMirState(Box<PendingMirState>),
    GovState(Box<GovState>),
    AccountEpochLog(Box<AccountEpochLog>),
    DRepEpochLog(Box<DRepEpochLog>),
    FirstMintSet(Box<FirstMintSet>),
    FirstMintChunk(Box<FirstMintChunk>),
    PolicyMintChunk(Box<PolicyMintChunk>),
    AccountAddressSet(Box<AccountAddressSet>),
    AccountAddressChunk(Box<AccountAddressChunk>),
    AddressStats(Box<AddressStats>),
    PaymentAddressSet(Box<PaymentAddressSet>),
    PaymentStats(Box<PaymentStats>),
    PaymentAddressChunk(Box<PaymentAddressChunk>),
    AccountStats(Box<AccountStats>),
    FirstScriptSet(Box<FirstScriptSet>),
    FirstScriptChunk(Box<FirstScriptChunk>),
    MetadataLabelState(Box<MetadataLabelState>),
    AssetMintSet(Box<AssetMintSet>),
    AssetMintChunk(Box<AssetMintChunk>),
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

variant_boilerplate!(EraState);
variant_boilerplate!(AccountState);
variant_boilerplate!(PoolState);
variant_boilerplate!(EpochState);
variant_boilerplate!(DRepState);
variant_boilerplate!(DRepSightingState);
variant_boilerplate!(ProposalState);
variant_boilerplate!(PoolEpochLog);
variant_boilerplate!(DatumState);
variant_boilerplate!(PendingRewardState);
variant_boilerplate!(PendingMirState);
variant_boilerplate!(GovState);
variant_boilerplate!(AccountEpochLog);
variant_boilerplate!(DRepEpochLog);
variant_boilerplate!(FirstMintSet);
variant_boilerplate!(FirstMintChunk);
variant_boilerplate!(PolicyMintChunk);
variant_boilerplate!(AccountAddressSet);
variant_boilerplate!(AccountAddressChunk);
variant_boilerplate!(AddressStats);
variant_boilerplate!(PaymentAddressSet);
variant_boilerplate!(PaymentStats);
variant_boilerplate!(PaymentAddressChunk);
variant_boilerplate!(AccountStats);
variant_boilerplate!(FirstScriptSet);
variant_boilerplate!(FirstScriptChunk);
variant_boilerplate!(MetadataLabelState);
variant_boilerplate!(AssetMintSet);
variant_boilerplate!(AssetMintChunk);

impl dolos_core::Entity for CardanoEntity {
    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, ChainError> {
        match ns {
            EraState::NS => EraState::decode_entity(ns, value).map(Into::into),
            AccountState::NS => AccountState::decode_entity(ns, value).map(Into::into),
            PoolState::NS => PoolState::decode_entity(ns, value).map(Into::into),
            EpochState::NS => EpochState::decode_entity(ns, value).map(Into::into),
            DRepState::NS => DRepState::decode_entity(ns, value).map(Into::into),
            DRepSightingState::NS => DRepSightingState::decode_entity(ns, value).map(Into::into),
            ProposalState::NS => ProposalState::decode_entity(ns, value).map(Into::into),
            PoolEpochLog::NS => PoolEpochLog::decode_entity(ns, value).map(Into::into),
            DatumState::NS => DatumState::decode_entity(ns, value).map(Into::into),
            PendingRewardState::NS => PendingRewardState::decode_entity(ns, value).map(Into::into),
            PendingMirState::NS => PendingMirState::decode_entity(ns, value).map(Into::into),
            GovState::NS => GovState::decode_entity(ns, value).map(Into::into),
            AccountEpochLog::NS => AccountEpochLog::decode_entity(ns, value).map(Into::into),
            DRepEpochLog::NS => DRepEpochLog::decode_entity(ns, value).map(Into::into),
            FirstMintSet::NS => FirstMintSet::decode_entity(ns, value).map(Into::into),
            FirstMintChunk::NS => FirstMintChunk::decode_entity(ns, value).map(Into::into),
            PolicyMintChunk::NS => PolicyMintChunk::decode_entity(ns, value).map(Into::into),
            AccountAddressSet::NS => AccountAddressSet::decode_entity(ns, value).map(Into::into),
            AccountAddressChunk::NS => {
                AccountAddressChunk::decode_entity(ns, value).map(Into::into)
            }
            AddressStats::NS => AddressStats::decode_entity(ns, value).map(Into::into),
            PaymentAddressSet::NS => PaymentAddressSet::decode_entity(ns, value).map(Into::into),
            PaymentStats::NS => PaymentStats::decode_entity(ns, value).map(Into::into),
            PaymentAddressChunk::NS => {
                PaymentAddressChunk::decode_entity(ns, value).map(Into::into)
            }
            AccountStats::NS => AccountStats::decode_entity(ns, value).map(Into::into),
            FirstScriptSet::NS => FirstScriptSet::decode_entity(ns, value).map(Into::into),
            FirstScriptChunk::NS => FirstScriptChunk::decode_entity(ns, value).map(Into::into),
            MetadataLabelState::NS => MetadataLabelState::decode_entity(ns, value).map(Into::into),
            AssetMintSet::NS => AssetMintSet::decode_entity(ns, value).map(Into::into),
            AssetMintChunk::NS => AssetMintChunk::decode_entity(ns, value).map(Into::into),
            _ => Err(ChainError::InvalidNamespace(ns)),
        }
    }

    fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
        match value {
            Self::EraState(x) => EraState::encode_entity(x),
            Self::AccountState(x) => AccountState::encode_entity(x),
            Self::PoolState(x) => PoolState::encode_entity(x),
            Self::EpochState(x) => EpochState::encode_entity(x),
            Self::DRepState(x) => DRepState::encode_entity(x),
            Self::DRepSightingState(x) => DRepSightingState::encode_entity(x),
            Self::ProposalState(x) => ProposalState::encode_entity(x),
            Self::PoolEpochLog(x) => PoolEpochLog::encode_entity(x),
            Self::DatumState(x) => DatumState::encode_entity(x),
            Self::PendingRewardState(x) => PendingRewardState::encode_entity(x),
            Self::PendingMirState(x) => PendingMirState::encode_entity(x),
            Self::GovState(x) => GovState::encode_entity(x),
            Self::AccountEpochLog(x) => AccountEpochLog::encode_entity(x),
            Self::DRepEpochLog(x) => DRepEpochLog::encode_entity(x),
            Self::FirstMintSet(x) => FirstMintSet::encode_entity(x),
            Self::FirstMintChunk(x) => FirstMintChunk::encode_entity(x),
            Self::PolicyMintChunk(x) => PolicyMintChunk::encode_entity(x),
            Self::AccountAddressSet(x) => AccountAddressSet::encode_entity(x),
            Self::AccountAddressChunk(x) => AccountAddressChunk::encode_entity(x),
            Self::AddressStats(x) => AddressStats::encode_entity(x),
            Self::PaymentAddressSet(x) => PaymentAddressSet::encode_entity(x),
            Self::PaymentStats(x) => PaymentStats::encode_entity(x),
            Self::PaymentAddressChunk(x) => PaymentAddressChunk::encode_entity(x),
            Self::AccountStats(x) => AccountStats::encode_entity(x),
            Self::FirstScriptSet(x) => FirstScriptSet::encode_entity(x),
            Self::FirstScriptChunk(x) => FirstScriptChunk::encode_entity(x),
            Self::MetadataLabelState(x) => MetadataLabelState::encode_entity(x),
            Self::AssetMintSet(x) => AssetMintSet::encode_entity(x),
            Self::AssetMintChunk(x) => AssetMintChunk::encode_entity(x),
        }
    }
}

fn register_state<E: StateEntity>(schema: &mut StateSchema) {
    schema.insert(E::NS, NamespaceType::KeyValue);
}

fn register_log<E: LogEntity>(schema: &mut StateSchema) {
    schema.insert(E::NS, NamespaceType::KeyValue);
}

/// The state store's namespaces: every entity declared with a state key.
///
/// An archive-only log has no state key, so it has no namespace here and a
/// stele carries no state layer for it.
pub fn build_schema() -> StateSchema {
    let mut schema = StateSchema::default();
    register_state::<EraState>(&mut schema);
    register_state::<AccountState>(&mut schema);
    register_state::<PoolState>(&mut schema);
    register_state::<EpochState>(&mut schema);
    register_state::<DRepState>(&mut schema);
    register_state::<DRepSightingState>(&mut schema);
    register_state::<ProposalState>(&mut schema);
    register_state::<DatumState>(&mut schema);
    register_state::<PendingRewardState>(&mut schema);
    register_state::<PendingMirState>(&mut schema);
    register_state::<GovState>(&mut schema);
    register_state::<FirstMintSet>(&mut schema);
    register_state::<AccountAddressSet>(&mut schema);
    register_state::<AddressStats>(&mut schema);
    register_state::<PaymentAddressSet>(&mut schema);
    register_state::<PaymentStats>(&mut schema);
    register_state::<AccountStats>(&mut schema);
    register_state::<FirstScriptSet>(&mut schema);
    register_state::<MetadataLabelState>(&mut schema);
    register_state::<AssetMintSet>(&mut schema);
    schema
}

/// The archive store's log namespaces: every entity declared with a log key.
pub fn build_log_schema() -> StateSchema {
    let mut schema = StateSchema::default();
    register_log::<EpochState>(&mut schema);
    register_log::<PoolEpochLog>(&mut schema);
    register_log::<AccountEpochLog>(&mut schema);
    register_log::<DRepEpochLog>(&mut schema);
    register_log::<FirstMintChunk>(&mut schema);
    register_log::<PolicyMintChunk>(&mut schema);
    register_log::<FirstScriptChunk>(&mut schema);
    register_log::<AccountAddressChunk>(&mut schema);
    register_log::<AddressStats>(&mut schema);
    register_log::<PaymentAddressChunk>(&mut schema);
    register_log::<AccountStats>(&mut schema);
    register_log::<AssetMintChunk>(&mut schema);
    register_log::<ProposalState>(&mut schema);
    register_log::<PoolState>(&mut schema);
    schema
}

#[cfg(test)]
mod schema_tests {
    use super::*;

    #[test]
    fn archive_only_logs_register_in_the_archive_alone() {
        let states = build_schema();
        let logs = build_log_schema();

        for ns in [
            AccountEpochLog::NS,
            PoolEpochLog::NS,
            DRepEpochLog::NS,
            AccountAddressChunk::NS,
            PaymentAddressChunk::NS,
            PolicyMintChunk::NS,
            FirstMintChunk::NS,
            AssetMintChunk::NS,
        ] {
            assert!(!states.contains_key(ns), "{ns} is a state namespace");
            assert!(logs.contains_key(ns), "{ns} is not a log namespace");
        }
    }

    /// A final-row kind keeps its live rows in state and its final rows in
    /// the log of the same namespace.
    #[test]
    fn final_row_kinds_register_in_both_stores() {
        let states = build_schema();
        let logs = build_log_schema();

        for ns in [ProposalState::NS, PoolState::NS] {
            assert!(states.contains_key(ns), "{ns} is not a state namespace");
            assert!(logs.contains_key(ns), "{ns} is not a log namespace");
        }
    }
}

// --- CardanoDelta ---

// Variant order is part of the on-disk WAL format: bincode encodes enum
// variants by positional index (no name tags). The index of every variant
// a released binary has written is frozen (0..=63 as of v1.7.0-alpha.0)
// so existing WAL rows decode correctly. New variants must be appended to
// the end. The `EpochTransition` and
// `EpochWrapUp` variants point at the *legacy* (deprecated) struct
// shapes for the same reason; `EpochTransitionV2` / `EpochWrapUpV2` are
// the live shapes used by all new commit paths. A removed delta keeps its
// index as a `Retired…` variant over [`RetiredDelta`].
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
    /// `MintStatsUpdate`, which kept `AssetState`'s supply, mint count and
    /// first mint; the asset-mint period logs derive them.
    RetiredMintStatsUpdate(RetiredDelta),
    /// `MetadataTxUpdate`, which kept `AssetState`'s metadata tx.
    RetiredMetadataTxUpdate(RetiredDelta),
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
    EpochWrapUpV3(Box<EpochWrapUpV3>),
    RetiredDRepAnchorUpdate(RetiredDelta),
    NewProposalV2(Box<NewProposalV2>),
    VoteCast(Box<VoteCast>),
    DRepExpiryUpdate(Box<DRepExpiryUpdate>),
    DRepDormancyRelease(Box<DRepDormancyRelease>),
    GovGenesisInit(Box<GovGenesisInit>),
    CommitteeAuth(Box<CommitteeAuth>),
    CommitteeResign(Box<CommitteeResign>),
    GovDormancyReset(Box<GovDormancyReset>),
    CommitteeUpdate(Box<CommitteeUpdate>),
    ConstitutionUpdate(Box<ConstitutionUpdate>),
    GovRootsUpdate(Box<GovRootsUpdate>),
    GovDistrAccumulate(Box<GovDistrAccumulate>),
    RetiredDRepPowerUpdate(RetiredDelta),
    GovDormancyTick(Box<GovDormancyTick>),
    CommitteeGc(Box<CommitteeGc>),
    GovDistrRotate(Box<GovDistrRotate>),
    ProposalResolved(Box<ProposalResolved>),
    GovDistrBoundaryCredit(Box<GovDistrBoundaryCredit>),
    GovDRepSnapshot(Box<GovDRepSnapshot>),
    StageFirstMint(Box<StageFirstMint>),
    StageAccountAddress(Box<StageAccountAddress>),
    StageAddressStats(Box<StageAddressStats>),
    StagePaymentAddress(Box<StagePaymentAddress>),
    StagePaymentStats(Box<StagePaymentStats>),
    StageAccountStats(Box<StageAccountStats>),
    MetadataLabelTxInc(Box<MetadataLabelTxInc>),
    /// `ScriptFirstSeen`, which wrote the `scripts` witness the entity index
    /// replaced.
    RetiredScriptFirstSeen(RetiredDelta),
    StageFirstScript(Box<StageFirstScript>),
    StageAssetMint(Box<StageAssetMint>),
    DRepSighting(Box<DRepSighting>),
    CommitteeDissolve(Box<CommitteeDissolve>),
    CommitteeReseat(Box<CommitteeReseat>),
    PoolCalidusUpdate(Box<PoolCalidusUpdate>),
}

/// The payload of a retired delta variant, which holds the variant's index so
/// no later variant reuses it. Nothing builds one, and the WAL version bump
/// that retired it wipes every WAL written before, so none decodes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RetiredDelta;

impl RetiredDelta {
    fn unreachable(&self) -> ! {
        unreachable!("a retired delta variant is never built")
    }
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
delta_from!(EpochStatsUpdate);
delta_from!(DRepRegistration);
delta_from!(DRepUnRegistration);
delta_from!(DRepActivity);
delta_from!(DRepExpiration);
delta_from!(DRepSighting);
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
    delta_from!(EpochWrapUpV2);
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
delta_from!(EpochTransitionV2);
delta_from!(EpochWrapUpV3);
delta_from!(NewProposalV2);
delta_from!(VoteCast);
delta_from!(DRepExpiryUpdate);
delta_from!(DRepDormancyRelease);
delta_from!(GovGenesisInit);
delta_from!(CommitteeAuth);
delta_from!(CommitteeResign);
delta_from!(GovDormancyReset);
delta_from!(CommitteeUpdate);
delta_from!(ConstitutionUpdate);
delta_from!(GovRootsUpdate);
delta_from!(GovDistrAccumulate);
delta_from!(GovDormancyTick);
delta_from!(CommitteeGc);
delta_from!(GovDistrRotate);
delta_from!(GovDRepSnapshot);
delta_from!(GovDistrBoundaryCredit);
delta_from!(ProposalResolved);
delta_from!(StageFirstMint);
delta_from!(StageAccountAddress);
delta_from!(StageAddressStats);
delta_from!(StagePaymentAddress);
delta_from!(StagePaymentStats);
delta_from!(StageAccountStats);
delta_from!(StageFirstScript);
delta_from!(MetadataLabelTxInc);
delta_from!(StageAssetMint);
delta_from!(CommitteeDissolve);
delta_from!(CommitteeReseat);
delta_from!(PoolCalidusUpdate);

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
            Self::EpochStatsUpdate(x) => x.key(),
            Self::DRepRegistration(x) => x.key(),
            Self::DRepActivity(x) => x.key(),
            Self::DRepUnRegistration(x) => x.key(),
            Self::DRepExpiration(x) => x.key(),
            Self::DRepSighting(x) => x.key(),
            Self::WithdrawalInc(x) => x.key(),
            Self::VoteDelegation(x) => x.key(),
            Self::PParamsUpdate(x) => x.key(),
            Self::NoncesUpdate(x) => x.key(),
            Self::NewProposal(x) => x.key(),
            Self::NewProposalV2(x) => x.key(),
            Self::VoteCast(x) => x.key(),
            Self::DRepExpiryUpdate(x) => x.key(),
            Self::DRepDormancyRelease(x) => x.key(),
            Self::GovGenesisInit(x) => x.key(),
            Self::CommitteeAuth(x) => x.key(),
            Self::CommitteeResign(x) => x.key(),
            Self::GovDormancyReset(x) => x.key(),
            Self::CommitteeUpdate(x) => x.key(),
            Self::ConstitutionUpdate(x) => x.key(),
            Self::GovRootsUpdate(x) => x.key(),
            Self::GovDistrAccumulate(x) => x.key(),
            Self::GovDormancyTick(x) => x.key(),
            Self::CommitteeGc(x) => x.key(),
            Self::GovDistrRotate(x) => x.key(),
            Self::GovDRepSnapshot(x) => x.key(),
            Self::GovDistrBoundaryCredit(x) => x.key(),
            Self::ProposalResolved(x) => x.key(),
            Self::StageFirstScript(x) => x.key(),
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
            Self::EpochWrapUpV3(x) => x.key(),
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
            Self::StageFirstMint(x) => x.key(),
            Self::StageAccountAddress(x) => x.key(),
            Self::StageAddressStats(x) => x.key(),
            Self::StagePaymentAddress(x) => x.key(),
            Self::StagePaymentStats(x) => x.key(),
            Self::StageAccountStats(x) => x.key(),
            Self::MetadataLabelTxInc(x) => x.key(),
            Self::StageAssetMint(x) => x.key(),
            Self::CommitteeDissolve(x) => x.key(),
            Self::CommitteeReseat(x) => x.key(),
            Self::PoolCalidusUpdate(x) => x.key(),
            Self::RetiredMintStatsUpdate(x)
            | Self::RetiredMetadataTxUpdate(x)
            | Self::RetiredDRepAnchorUpdate(x)
            | Self::RetiredDRepPowerUpdate(x)
            | Self::RetiredScriptFirstSeen(x) => x.unreachable(),
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
            Self::EpochStatsUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepRegistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepUnRegistration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepActivity(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepExpiration(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepSighting(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::WithdrawalInc(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::VoteDelegation(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PParamsUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::NoncesUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::NewProposal(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::NewProposalV2(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::VoteCast(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepExpiryUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::DRepDormancyRelease(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovGenesisInit(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::CommitteeAuth(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::CommitteeResign(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovDormancyReset(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::CommitteeUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::ConstitutionUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovRootsUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovDistrAccumulate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovDormancyTick(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::CommitteeGc(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovDistrRotate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovDRepSnapshot(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::GovDistrBoundaryCredit(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::ProposalResolved(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StageFirstScript(x) => Self::downcast_apply(x.as_mut(), entity),
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
            Self::EpochWrapUpV3(x) => Self::downcast_apply(x.as_mut(), entity),
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
            Self::StageFirstMint(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StageAccountAddress(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StageAddressStats(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StagePaymentAddress(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StagePaymentStats(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StageAccountStats(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::MetadataLabelTxInc(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::StageAssetMint(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::CommitteeDissolve(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::CommitteeReseat(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::PoolCalidusUpdate(x) => Self::downcast_apply(x.as_mut(), entity),
            Self::RetiredMintStatsUpdate(x)
            | Self::RetiredMetadataTxUpdate(x)
            | Self::RetiredDRepAnchorUpdate(x)
            | Self::RetiredDRepPowerUpdate(x)
            | Self::RetiredScriptFirstSeen(x) => x.unreachable(),
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
            Self::EpochStatsUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepRegistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepUnRegistration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepActivity(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepExpiration(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepSighting(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::WithdrawalInc(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::VoteDelegation(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PParamsUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::NoncesUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::NewProposal(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::NewProposalV2(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::VoteCast(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepExpiryUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::DRepDormancyRelease(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovGenesisInit(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::CommitteeAuth(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::CommitteeResign(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovDormancyReset(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::CommitteeUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::ConstitutionUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovRootsUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovDistrAccumulate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovDormancyTick(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::CommitteeGc(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovDistrRotate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovDRepSnapshot(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::GovDistrBoundaryCredit(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::ProposalResolved(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StageFirstScript(x) => Self::downcast_undo(x.as_ref(), entity),
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
            Self::EpochWrapUpV3(x) => Self::downcast_undo(x.as_ref(), entity),
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
            Self::StageFirstMint(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StageAccountAddress(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StageAddressStats(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StagePaymentAddress(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StagePaymentStats(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StageAccountStats(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::MetadataLabelTxInc(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::StageAssetMint(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::CommitteeDissolve(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::CommitteeReseat(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::PoolCalidusUpdate(x) => Self::downcast_undo(x.as_ref(), entity),
            Self::RetiredMintStatsUpdate(x)
            | Self::RetiredMetadataTxUpdate(x)
            | Self::RetiredDRepAnchorUpdate(x)
            | Self::RetiredDRepPowerUpdate(x)
            | Self::RetiredScriptFirstSeen(x) => x.unreachable(),
        }
    }
}

#[cfg(test)]
mod wal_tests {
    use super::*;
    use crate::periods::GroupKey;
    use pallas::ledger::primitives::StakeCredential;

    #[test]
    fn appended_delta_variant_indices_are_pinned() {
        let account = AccountKey(StakeCredential::AddrKeyhash([0x11; 28].into()));
        let deltas: Vec<(u32, CardanoDelta)> = vec![
            // retired, never reused
            (8, CardanoDelta::RetiredMintStatsUpdate(RetiredDelta)),
            (9, CardanoDelta::RetiredMetadataTxUpdate(RetiredDelta)),
            (45, CardanoDelta::RetiredDRepAnchorUpdate(RetiredDelta)),
            (58, CardanoDelta::RetiredDRepPowerUpdate(RetiredDelta)),
            (
                64,
                GovDRepSnapshot::new(DRepSnapshot {
                    closing_epoch: 7,
                    dreps: Default::default(),
                })
                .into(),
            ),
            (
                65,
                StageFirstMint::new(FirstMintSet {
                    policy: [0x22; 28].into(),
                    name: b"A".to_vec(),
                    epoch: 7,
                    slot: 100,
                    tx_index: 0,
                })
                .into(),
            ),
            (
                66,
                StageAccountAddress::new(AccountAddressSet {
                    account: account.0.clone(),
                    address: vec![0x33; 29],
                    epoch: 7,
                    slot: 100,
                    tx_index: 0,
                    output_index: 0,
                })
                .into(),
            ),
            (
                67,
                StageAddressStats::new(
                    (7, AddressKey::Unstaked(vec![0x33; 29])),
                    AddressStats::default(),
                )
                .into(),
            ),
            (
                68,
                StagePaymentAddress::new(PaymentAddressSet {
                    payment: [0x44; 28].into(),
                    epoch: 7,
                    tail: vec![0x60],
                })
                .into(),
            ),
            (
                69,
                StagePaymentStats::new(
                    GroupKey {
                        group: PaymentKey([0x44; 28].into()),
                        epoch: 7,
                        member: (),
                    },
                    PaymentStats { tx_count: 1 },
                )
                .into(),
            ),
            (
                70,
                StageAccountStats::new((7, account), AccountStats { tx_count: 1 }).into(),
            ),
            (71, MetadataLabelTxInc::new(674, 1).into()),
            (72, CardanoDelta::RetiredScriptFirstSeen(RetiredDelta)),
            (
                73,
                StageFirstScript::new(
                    7,
                    [0x55; 28].into(),
                    FirstScriptSet {
                        slot: 100,
                        tx_index: 0,
                        order: 0,
                    },
                )
                .into(),
            ),
            (
                74,
                StageAssetMint::mint(
                    AssetKey([0x66; 32].into()),
                    7,
                    (100, 0),
                    [0x77; 32].into(),
                    -5,
                    false,
                )
                .into(),
            ),
            (
                76,
                CommitteeDissolve::new(
                    pallas::ledger::primitives::conway::GovActionId {
                        transaction_id: [0x88; 32].into(),
                        action_index: 0,
                    },
                    7,
                )
                .into(),
            ),
            (77, CommitteeReseat::new().into()),
            (
                78,
                PoolCalidusUpdate::new(
                    [0x99; 28].into(),
                    CalidusRegistration {
                        pub_key: Some([0x57; 32]),
                        nonce: 12_345,
                        tx_hash: [0xaa; 32].into(),
                        slot: 100,
                    },
                )
                .into(),
            ),
        ];

        for (index, delta) in deltas {
            let encoded = bincode::serialize(&delta).unwrap();
            assert_eq!(&encoded[..4], &index.to_le_bytes());
            let decoded: CardanoDelta = bincode::deserialize(&encoded).unwrap();
            assert_eq!(bincode::serialize(&decoded).unwrap(), encoded);
        }
    }

    /// `ScriptFirstSeen` as WAL v5 wrote it decodes to the variant that
    /// retired it, not to a live one: its index is held.
    #[test]
    fn retired_variants_hold_their_index() {
        // the variant index, the script hash as a length-prefixed hex
        // string, the `ScriptState` slot and `was_new`
        let hash = [0x44u8; 28];
        let mut encoded = 72u32.to_le_bytes().to_vec();
        encoded.extend_from_slice(&56u64.to_le_bytes());
        encoded.extend_from_slice(hex::encode(hash).as_bytes());
        encoded.extend_from_slice(&100u64.to_le_bytes());
        encoded.push(1);

        assert!(matches!(
            bincode::deserialize::<CardanoDelta>(&encoded),
            Ok(CardanoDelta::RetiredScriptFirstSeen(RetiredDelta))
        ));
    }
}
