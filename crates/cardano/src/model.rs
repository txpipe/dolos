use std::{borrow::Cow, collections::HashSet};

use dolos_core::{EntityValue, Namespace, NamespaceType, NsKey, State3Error, StateSchema};

use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::{
        conway::{CostModels, DRep, DRepVotingThresholds, PoolVotingThresholds},
        Coin, Epoch, ExUnitPrices, ExUnits, Nonce, PoolMetadata, ProtocolVersion, RationalNumber,
        Relay, UnitInterval,
    },
};
use serde::{Deserialize, Serialize};

pub trait FixedNamespace {
    const NS: &'static str;
}

macro_rules! entity_boilerplate {
    ($type:ident, $ns:literal) => {
        impl FixedNamespace for $type {
            const NS: &str = $ns;
        }

        impl dolos_core::Entity for $type {
            fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, State3Error> {
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
    pub rewards_sum: u64,

    #[n(3)]
    pub withdrawals_sum: u64,

    #[n(4)]
    pub reserves_sum: u64,

    #[n(5)]
    pub treasury_sum: u64,

    #[n(6)]
    pub withdrawable_amount: u64,

    #[n(7)]
    pub pool_id: Option<Vec<u8>>,

    #[n(8)]
    pub drep: Option<DRep>,

    // capped size, LRU type cache
    #[n(9)]
    pub seen_addresses: HashSet<Vec<u8>>,

    #[n(10)]
    pub active_slots: HashSet<u64>,

    #[n(11)]
    pub rewards: Vec<RewardLog>,
}

entity_boilerplate!(AccountState, "accounts");

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

    pub fn quantity(&self) -> u128 {
        u128::from_be_bytes(self.quantity_bytes)
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
    pub live_stake: u64,

    #[n(10)]
    pub blocks_minted: u32,

    #[n(11)]
    pub live_saturation: f64,
}

entity_boilerplate!(PoolState, "pools");

impl PoolState {
    pub fn new(vrf_keyhash: Hash<32>) -> Self {
        Self {
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
            live_stake: Default::default(),
            blocks_minted: Default::default(),
            live_saturation: Default::default(),
        }
    }
}

#[derive(Debug, Encode, Decode, Clone, Default)]
pub struct EpochState {
    #[n(0)]
    pub supply_circulating: u64,

    #[n(1)]
    pub supply_locked: u64,

    #[n(2)]
    pub treasury: u64,

    #[n(3)]
    pub stake_live: u64,

    #[n(4)]
    pub stake_active: u64,

    #[n(5)]
    pub gathered_fees: u64,

    #[n(6)]
    pub decayed_deposits: u64,

    #[n(7)]
    pub rewards: u64,

    #[n(8)]
    pub number: u32,

    #[n(9)]
    pub reserves: u64,

    #[n(10)]
    pub end_reserves: Option<u64>,

    #[n(11)]
    pub to_treasury: Option<u64>,

    #[n(12)]
    pub to_distribute: Option<u64>,
}

entity_boilerplate!(EpochState, "epochs");

pub const EPOCH_KEY_GO: &[u8] = b"go";
pub const EPOCH_KEY_SET: &[u8] = b"set";
pub const EPOCH_KEY_MARK: &[u8] = b"mark";

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

/// we're purposely not including these Byron params because Dolos doesn't need
/// them for any logic:
///
/// script_version: u16
/// max_proposal_size: u64
/// mpc_thd: u64
/// heavy_del_thd: u64
/// update_vote_thd: u64
/// update_proposal_thd: u64
/// update_implicit: u64
/// soft_fork_rule: (u64, u64, u64)
/// unlock_stake_epoch: u64
#[derive(Debug, Encode, Decode, Clone)]
pub struct PParamsState {
    // shelley

    // byron alias: start_time
    // should be exposed as type chrono::DateTime<chrono::FixedOffset>
    #[n(0)]
    pub system_start: u64,

    #[n(1)]
    pub epoch_length: u64,

    // byron alias: slot_duration
    #[n(2)]
    pub slot_length: u64,

    // byron alias: multiplier
    #[n(3)]
    pub minfee_a: u32,

    // byron alias: summand
    #[n(4)]
    pub minfee_b: u32,

    // byron alias: max_block_size
    #[n(5)]
    pub max_block_body_size: u32,

    // byron alias: max_tx_size
    #[n(6)]
    pub max_transaction_size: u32,

    // byron alias: max_header_size
    #[n(7)]
    pub max_block_header_size: u32,

    #[n(8)]
    pub key_deposit: Coin,

    #[n(9)]
    pub pool_deposit: Coin,

    #[n(10)]
    pub desired_number_of_stake_pools: u32,

    // byron alias: block_version
    #[n(11)]
    pub protocol_version: ProtocolVersion,

    #[n(12)]
    pub min_utxo_value: Coin,

    #[n(13)]
    pub min_pool_cost: Coin,

    #[n(14)]
    pub expansion_rate: UnitInterval,

    #[n(15)]
    pub treasury_growth_rate: UnitInterval,

    #[n(16)]
    pub maximum_epoch: Epoch,

    #[n(17)]
    pub pool_pledge_influence: RationalNumber,

    #[n(18)]
    pub decentralization_constant: UnitInterval,

    #[n(19)]
    pub extra_entropy: Nonce,

    // alonzo
    #[n(20)]
    pub ada_per_utxo_byte: Coin,

    #[n(21)]
    pub cost_models_for_script_languages: CostModels,

    #[n(22)]
    pub execution_costs: ExUnitPrices,

    #[n(23)]
    pub max_tx_ex_units: ExUnits,

    #[n(24)]
    pub max_block_ex_units: ExUnits,

    #[n(25)]
    pub max_value_size: u32,

    #[n(26)]
    pub collateral_percentage: u32,

    #[n(27)]
    pub max_collateral_inputs: u32,

    // conway
    #[n(28)]
    pub pool_voting_thresholds: PoolVotingThresholds,

    #[n(29)]
    pub drep_voting_thresholds: DRepVotingThresholds,

    #[n(30)]
    pub min_committee_size: u64,

    #[n(31)]
    pub committee_term_limit: Epoch,

    #[n(32)]
    pub governance_action_validity_period: Epoch,

    #[n(33)]
    pub governance_action_deposit: Coin,

    #[n(34)]
    pub drep_deposit: Coin,

    #[n(35)]
    pub drep_inactivity_period: Epoch,

    #[n(36)]
    pub minfee_refscript_cost_per_byte: UnitInterval,
}

impl PParamsState {
    pub fn k(&self) -> u32 {
        self.desired_number_of_stake_pools
    }

    pub fn a0(&self) -> &RationalNumber {
        &self.pool_pledge_influence
    }

    pub fn tau(&self) -> &RationalNumber {
        &self.treasury_growth_rate
    }

    pub fn rho(&self) -> &RationalNumber {
        &self.expansion_rate
    }
}

entity_boilerplate!(PParamsState, "pparams");

#[derive(Debug, Clone)]
pub enum CardanoEntity {
    PParamsState(PParamsState),
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

variant_boilerplate!(PParamsState);
variant_boilerplate!(AccountState);
variant_boilerplate!(AssetState);
variant_boilerplate!(PoolState);
variant_boilerplate!(EpochState);
variant_boilerplate!(DRepState);

impl dolos_core::Entity for CardanoEntity {
    fn decode_entity(ns: Namespace, value: &EntityValue) -> Result<Self, State3Error> {
        match ns {
            PParamsState::NS => PParamsState::decode_entity(ns, value).map(Into::into),
            AccountState::NS => AccountState::decode_entity(ns, value).map(Into::into),
            AssetState::NS => AssetState::decode_entity(ns, value).map(Into::into),
            PoolState::NS => PoolState::decode_entity(ns, value).map(Into::into),
            EpochState::NS => EpochState::decode_entity(ns, value).map(Into::into),
            DRepState::NS => DRepState::decode_entity(ns, value).map(Into::into),
            _ => Err(State3Error::InvalidNamespace(ns)),
        }
    }

    fn encode_entity(value: &Self) -> (Namespace, EntityValue) {
        match value {
            Self::PParamsState(x) => {
                let (ns, enc) = PParamsState::encode_entity(x);
                (ns, enc.into())
            }
            Self::AccountState(x) => {
                let (ns, enc) = AccountState::encode_entity(x);
                (ns, enc.into())
            }
            Self::AssetState(x) => {
                let (ns, enc) = AssetState::encode_entity(x);
                (ns, enc.into())
            }
            Self::PoolState(x) => {
                let (ns, enc) = PoolState::encode_entity(x);
                (ns, enc.into())
            }
            Self::EpochState(x) => {
                let (ns, enc) = EpochState::encode_entity(x);
                (ns, enc.into())
            }
            Self::DRepState(x) => {
                let (ns, enc) = DRepState::encode_entity(x);
                (ns, enc.into())
            }
        }
    }
}

pub fn build_schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert(AccountState::NS, NamespaceType::KeyValue);
    schema.insert(AssetState::NS, NamespaceType::KeyValue);
    schema.insert(PoolState::NS, NamespaceType::KeyValue);
    schema.insert(EpochState::NS, NamespaceType::KeyValue);
    schema.insert(DRepState::NS, NamespaceType::KeyValue);
    schema
}

use crate::roll::accounts::{
    ControlledAmountDec, ControlledAmountInc, StakeDelegation, StakeDeregistration,
    StakeRegistration, TrackSeenAddresses, VoteDelegation, WithdrawalInc,
};
use crate::roll::assets::MintStatsUpdate;
use crate::roll::dreps::{DRepRegistration, DRepUnRegistration};
use crate::roll::epochs::EpochStatsUpdate;
use crate::roll::pools::{MintedBlocksInc, PoolRegistration};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CardanoDelta {
    ControlledAmountInc(ControlledAmountInc),
    ControlledAmountDec(ControlledAmountDec),
    TrackSeenAddresses(TrackSeenAddresses),
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

    pub fn downcast_undo<T, D>(delta: &mut D, entity: &mut Option<CardanoEntity>)
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
delta_from!(TrackSeenAddresses);
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

impl dolos_core::EntityDelta for CardanoDelta {
    type Entity = super::model::CardanoEntity;

    fn key(&self) -> Cow<'_, NsKey> {
        match self {
            Self::ControlledAmountInc(x) => x.key(),
            Self::ControlledAmountDec(x) => x.key(),
            Self::TrackSeenAddresses(x) => x.key(),
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
        }
    }

    fn apply(&mut self, entity: &mut Option<Self::Entity>) {
        match self {
            Self::ControlledAmountInc(x) => Self::downcast_apply(x, entity),
            Self::ControlledAmountDec(x) => Self::downcast_apply(x, entity),
            Self::TrackSeenAddresses(x) => Self::downcast_apply(x, entity),
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
        }
    }

    fn undo(&mut self, entity: &mut Option<Self::Entity>) {
        match self {
            Self::ControlledAmountInc(x) => Self::downcast_undo(x, entity),
            Self::ControlledAmountDec(x) => Self::downcast_undo(x, entity),
            Self::TrackSeenAddresses(x) => Self::downcast_undo(x, entity),
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
        }
    }
}
