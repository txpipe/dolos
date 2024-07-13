use pallas;
use pallas::{
    codec::minicbor::decode::Error as DecodingError,
    ledger::{
        addresses::Address,
        traverse::{Era, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
    },
};
use redb::{
    Database, MultimapTableDefinition, ReadOnlyTable, ReadTransaction, ReadableTable,
    TableDefinition, WriteTransaction,
};
use std::{ops::Deref, path::Path};
use thiserror::Error;

// Given an address, table "utxo_by_addr" maps it to a list of pairs of a tx
// hash and an (output) index (each one representing a UTxO sitting at that
// address)
pub type UTxOByAddrKeyType<'a> = &'a [u8];
pub type UTxOByAddrValueType<'a> = (&'a [u8], u8);
pub type UTxOByAddrResultType = (Vec<u8>, u8);
pub const UTXO_BY_ADDR_INDEX: MultimapTableDefinition<UTxOByAddrKeyType, UTxOByAddrValueType> =
    MultimapTableDefinition::new("utxo_by_addr");

// Given a minting policy, table "utxo_by_beacon" maps it to a list of pairs of
// a tx hash and an (output) index (each one representing a UTxO containing a
// token of that policy)
pub type UTxOByBeaconKeyType<'a> = &'a [u8; 28];
pub type UTxOByBeaconValueType<'a> = (&'a [u8], u8);
pub type UTxOByBeaconResultType = (Vec<u8>, u8);
pub const UTXO_BY_POLICY_INDEX: MultimapTableDefinition<
    UTxOByBeaconKeyType,
    UTxOByBeaconValueType,
> = MultimapTableDefinition::new("utxo_by_beacon");

#[derive(Error, Debug)]
pub enum Error {
    #[error("error decoding address")]
    AddressDecoding(#[from] pallas::ledger::addresses::Error),

    #[error("error decoding block")]
    BlockDecoding(#[from] pallas::ledger::traverse::Error),

    #[error("key not found")]
    KeyNotFound,

    #[error("error decoding output")]
    OutputDecoding(#[from] pallas::codec::minicbor::decode::Error),

    #[error("utxo table invariant broken")]
    UTxOTableInvariantBroken,

    #[error("Redb error")]
    ReDBError(#[from] redb::Error),

    #[error("IO error")]
    IOError(#[from] std::io::Error),
}

impl Error {
    pub fn redb(inner: impl Into<redb::Error>) -> Self {
        Self::ReDBError(inner.into())
    }
}

pub struct Store {
    inner_store: Database,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Store {
            inner_store: Database::create(path).map_err(Error::redb)?,
        })
    }

    pub fn destroy(path: impl AsRef<Path>) -> Result<(), Error> {
        std::fs::remove_file(path).map_err(Error::IOError)
    }

    fn update_tx_outputs(&self, write_tx: &WriteTransaction, tx: &MultiEraTx) -> Result<(), Error> {
        let mut utxo_table = write_tx.open_table(UTXO_TABLE).map_err(Error::redb)?;

        let mut utxo_by_addr_table = write_tx
            .open_multimap_table(UTXO_BY_ADDR_INDEX)
            .map_err(Error::redb)?;

        let mut utxo_by_policy_table = write_tx
            .open_multimap_table(UTXO_BY_POLICY_INDEX)
            .map_err(Error::redb)?;

        for (index, tx_out) in tx.produces() {
            let tx_hash = tx.hash();

            utxo_by_addr_table
                .insert(
                    output_address(&tx_out)?.to_vec().as_slice(),
                    (tx_hash.as_slice(), index as u8),
                )
                .map_err(Error::redb)?;

            for policy in output_policy_ids(&tx_out) {
                utxo_by_policy_table
                    .insert(&policy, (tx_hash.as_slice(), index as u8))
                    .map_err(Error::redb)?;
            }
        }

        for multi_era_input in tx.consumes() {
            let tx_in: UTxOKeyType = (
                multi_era_input.hash().as_slice(),
                multi_era_input.index() as u8,
            );

            let encoded_tx_out = utxo_table
                .get(tx_in)
                .map_err(Error::redb)?
                .ok_or(Error::UTxOTableInvariantBroken)?
                .value()
                .to_vec();

            let tx_out: MultiEraOutput =
                decode_output(&encoded_tx_out).map_err(Error::OutputDecoding)?;

            utxo_by_addr_table
                .remove(output_address(&tx_out)?.to_vec().as_slice(), tx_in)
                .map_err(Error::redb)?;

            for policy in output_policy_ids(&tx_out) {
                utxo_by_policy_table
                    .remove(&policy, tx_in)
                    .map_err(Error::redb)?;
            }

            utxo_table.remove(tx_in).map_err(Error::redb)?;
        }

        Ok(())
    }

    pub fn get_utxos_for_address(
        &self,
        addr: &UTxOByAddrKeyType,
    ) -> Result<Vec<UTxOByAddrResultType>, Error> {
        let read_tx = self.inner_store.begin_read().map_err(Error::redb)?;

        let values = read_tx
            .open_multimap_table(UTXO_BY_ADDR_INDEX)
            .map_err(Error::redb)?
            .get(addr)
            .map_err(Error::redb)?
            .flatten()
            .map(|x| {
                let (a, b) = x.value();
                (Vec::from(a), b)
            })
            .collect();

        Ok(values)
    }

    pub fn get_utxos_from_beacon(
        &self,
        beacon_policy_id: &UTxOByBeaconKeyType,
    ) -> Result<Box<impl Iterator<Item = UTxOByBeaconResultType>>, Error> {
        let read_tx = self.inner_store.begin_read().map_err(Error::redb)?;

        let utxo_by_beacon_table = read_tx
            .open_multimap_table(UTXO_BY_POLICY_INDEX)
            .map_err(Error::redb)?;

        match utxo_by_beacon_table.get(beacon_policy_id) {
            Ok(database_results) => {
                let mut res = vec![];
                for val in database_results.flatten() {
                    let (tx_hash, tx_index) = val.value();
                    res.push((Vec::from(tx_hash), tx_index))
                }
                Ok(Box::new(res.into_iter()))
            }
            Err(err) => Err(Error::redb(err)),
        }
    }
}

fn output_policy_ids(output: &MultiEraOutput) -> Vec<[u8; 28]> {
    output
        .non_ada_assets()
        .iter()
        .map(MultiEraPolicyAssets::policy)
        .map(Deref::deref)
        .map(Clone::clone)
        .collect()
}

fn output_address(output: &MultiEraOutput) -> Result<Address, Error> {
    output.address().map_err(Error::AddressDecoding)
}

fn decode_output(encoded_output: &[u8]) -> Result<MultiEraOutput, DecodingError> {
    MultiEraOutput::decode(Era::Byron, encoded_output)
        .or_else(|_| MultiEraOutput::decode(Era::Alonzo, encoded_output))
        .or_else(|_| MultiEraOutput::decode(Era::Babbage, encoded_output))
}
