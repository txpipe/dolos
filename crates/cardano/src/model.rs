use std::collections::HashSet;

use dolos_core::{Entity, EntityValue, Namespace, NamespaceType, StateSchema};
use pallas::codec::minicbor::{self, Decode, Encode};

#[derive(Debug, Clone, PartialEq, Eq, Decode, Encode, Default)]
pub struct AccountState {
    #[n(0)]
    pub active: bool,

    #[n(1)]
    pub active_epoch: u32,

    #[n(2)]
    pub controlled_amount: String,

    #[n(3)]
    pub rewards_sum: String,

    #[n(4)]
    pub withdrawals_sum: String,

    #[n(5)]
    pub reserves_sum: String,

    #[n(6)]
    pub treasury_sum: String,

    #[n(7)]
    pub withdrawable_amount: String,

    #[n(8)]
    pub pool_id: String,

    #[n(9)]
    pub drep_id: String,

    // capped size, LRU type cache
    #[n(10)]
    pub seen_addresses: HashSet<Vec<u8>>,
}

impl From<EntityValue> for AccountState {
    fn from(value: EntityValue) -> Self {
        let mut decoder = minicbor::Decoder::new(value.as_slice());
        decoder.decode().unwrap()
    }
}

impl Into<EntityValue> for AccountState {
    fn into(self) -> EntityValue {
        pallas::codec::minicbor::to_vec(&self).unwrap()
    }
}

impl Entity for AccountState {
    const NS: Namespace = "account_state";
}

pub fn build_schema() -> StateSchema {
    let mut schema = StateSchema::default();
    schema.insert("account_state", NamespaceType::KeyValue);
    schema
}
