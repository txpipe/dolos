use crate::querydb::prelude::StoreError;
use pallas::crypto::hash::Hash;
use pallas::ledger::traverse::MultiEraOutput;
use redb::Database;
use std::path::Path;

pub struct Store {
    _inner_store: Database,
}

impl Store {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Ok(Store {
            _inner_store: Database::create(path).map_err(StoreError::DatabaseInitilization)?,
        })
    }
    pub fn apply_block(&self, _block_cbor: &[u8]) {}

    pub fn get_chain_tip(&self) -> &[u8] {
        unimplemented!()
    }

    pub fn get_chain_parameters(&self) {
        unimplemented!()
    }

    pub fn get_utxos_from_address<'a, T>(&self) -> Box<T>
    where
        T: Iterator<Item = MultiEraOutput<'a>>,
    {
        unimplemented!()
    }

    pub fn get_utxo_from_reference(&self) -> Option<&[u8]> {
        unimplemented!()
    }

    pub fn get_tx_from_hash(&self, _tx_hash: Hash<32>) -> Option<&[u8]> {
        unimplemented!()
    }

    pub fn get_block_from_hash(&self, _block_hash: Hash<32>) -> Option<&[u8]> {
        unimplemented!()
    }

    pub fn get_utxos_from_beacon<'a, T>(&self, _beacon_policy_id: Hash<28>) -> std::boxed::Box<T>
    where
        T: Iterator<Item = MultiEraOutput<'a>>,
    {
        unimplemented!()
    }
}
