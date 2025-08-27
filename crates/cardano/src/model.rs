use std::collections::HashSet;

use dolos_core::{
    BlockSlot, Entity, EntityValue, Namespace, NamespaceType, State3Error, StateSchema,
};
use pallas::{
    codec::minicbor::{self, Decode, Encode},
    crypto::hash::Hash,
    ledger::primitives::{PoolMetadata, RationalNumber, Relay, StakeCredential},
};

/// Macro to generate Entity implementation for a type
///
/// Usage: `impl_entity!(TypeName, "namespace", NamespaceType::KeyValue);`
macro_rules! impl_entity {
    ($type:ty, $ns:expr, $ns_type:expr) => {
        impl Entity for $type {
            const NS: Namespace = $ns;
            const NS_TYPE: NamespaceType = $ns_type;

            fn decode_value(value: EntityValue) -> Result<Self, State3Error> {
                Ok(minicbor::Decoder::new(value.as_slice()).decode()?)
            }

            fn encode_value(self) -> EntityValue {
                pallas::codec::minicbor::to_vec(&self).unwrap()
            }
        }
    };
}

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct AccountState {
    #[n(0)]
    pub active_epoch: Option<u32>,

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
    pub drep_id: Option<Vec<u8>>,

    // capped size, LRU type cache
    #[n(9)]
    pub seen_addresses: HashSet<Vec<u8>>,
}

impl_entity!(AccountState, "accounts", NamespaceType::KeyValue);

#[derive(Debug, Encode, Decode, Clone)]
pub struct AssetState {
    #[n(0)]
    pub quantity_bytes: [u8; 16],

    #[n(1)]
    pub initial_tx: Hash<32>,

    #[n(2)]
    pub mint_tx_count: u64,
}

impl_entity!(AssetState, "assets", NamespaceType::KeyValue);

impl AssetState {
    pub fn add_quantity(&mut self, value: i128) -> Result<(), State3Error> {
        let old = i128::from_be_bytes(self.quantity_bytes);
        let new = old.saturating_add(value).to_be_bytes();
        self.quantity_bytes = new;
        Ok(())
    }

    pub fn quantity(&self) -> u128 {
        u128::from_be_bytes(self.quantity_bytes)
    }
}

#[derive(Debug, Encode, Decode, Clone)]
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

impl_entity!(PoolState, "pools", NamespaceType::KeyValue);

#[derive(Debug, Encode, Decode, Clone)]
pub struct PoolDelegator(#[n(0)] pub StakeCredential);

impl_entity!(
    PoolDelegator,
    "pool_delegators",
    NamespaceType::KeyMultiValue
);

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
    pub gathered_fees: Option<u64>,

    #[n(6)]
    pub decayed_deposits: Option<u64>,
}

pub const CURRENT_EPOCH_KEY: &[u8] = b"current";

impl_entity!(EpochState, "epoch", NamespaceType::KeyValue);

#[derive(Debug, Encode, Decode, Clone)]
pub struct AccountActivity(#[n(0)] pub BlockSlot);

impl_entity!(
    AccountActivity,
    "account_activity",
    NamespaceType::KeyMultiValue
);

#[derive(Debug, Encode, Decode, Clone)]
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

impl_entity!(DRepState, "drep", NamespaceType::KeyValue);

#[derive(Debug, Encode, Decode, Clone)]
pub struct RewardLog {
    // we make sure epoch is the first item because we'll rely on it for sorting the items in a
    // multi-value store
    #[n(0)]
    pub epoch: u32,

    #[n(1)]
    pub amount: u64,

    #[n(2)]
    pub pool_id: Hash<28>,

    #[n(3)]
    pub as_leader: bool,
}

impl_entity!(RewardLog, "reward_log", NamespaceType::KeyMultiValue);

pub fn build_schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert(AccountState::NS, AccountState::NS_TYPE);
    schema.insert(AssetState::NS, AssetState::NS_TYPE);
    schema.insert(PoolState::NS, PoolState::NS_TYPE);
    schema.insert(PoolDelegator::NS, PoolDelegator::NS_TYPE);
    schema.insert(EpochState::NS, EpochState::NS_TYPE);
    schema.insert(DRepState::NS, DRepState::NS_TYPE);
    schema.insert(AccountActivity::NS, AccountActivity::NS_TYPE);
    schema.insert(RewardLog::NS, RewardLog::NS_TYPE);
    schema
}
