use std::collections::HashSet;

use dolos_core::{Entity, EntityValue, Namespace, NamespaceType, State3Error, StateSchema};
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
    #[n(2)]
    pub quantity: u64,

    #[n(3)]
    pub initial_tx: Hash<32>,

    #[n(4)]
    pub mint_tx_count: u64,
}

impl_entity!(AssetState, "assets", NamespaceType::KeyValue);

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

pub fn build_schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert(AccountState::NS, AccountState::NS_TYPE);
    schema.insert(AssetState::NS, AssetState::NS_TYPE);
    schema.insert(PoolState::NS, PoolState::NS_TYPE);
    schema.insert(PoolDelegator::NS, PoolDelegator::NS_TYPE);
    schema.insert(EpochState::NS, EpochState::NS_TYPE);
    schema
}
