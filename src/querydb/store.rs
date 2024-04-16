use pallas::{
    codec::minicbor::decode::Error as DecodingError,
    ledger::{
        addresses::Address,
        traverse::{Era, MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
    },
};
use redb::{
    Database, ReadOnlyTable, ReadTransaction, ReadableMultimapTable, ReadableTable,
    WriteTransaction,
};
use std::{ops::Deref, path::Path};

use pallas;
use redb::{MultimapTableDefinition, TableDefinition};
use thiserror::Error;

// use std::error::Error;

// Given a block, table "block" maps its slot to its CBOR representation
pub type BlockKeyType<'a> = u64;
pub type BlockValueType<'a> = &'a [u8];
pub type BlockResultType = Vec<u8>;
pub const BLOCK_TABLE: TableDefinition<BlockKeyType, BlockValueType> =
    TableDefinition::new("block");

// Given a block, table "block_by_hash" maps its hash to its slot.
pub type BlockByHashKeyType<'a> = &'a [u8; 32];
pub type BlockByHashValueType<'a> = u64;
pub const BLOCK_BY_HASH_INDEX: TableDefinition<BlockByHashKeyType, BlockByHashValueType> =
    TableDefinition::new("block_by_hash");

// Given a transaction, table "tx" maps its hash to an encoding representation
// of it
// NOTE: transactions don't have a precise CBOR representation, so we use
// a library encoded representation instead
pub type TxKeyType<'a> = &'a [u8; 32];
pub type TxValueType<'a> = &'a [u8];
pub type TxResultType = Vec<u8>;
pub const TX_TABLE: TableDefinition<TxKeyType, TxValueType> = TableDefinition::new("tx");

// Given a UTxO, table "utxo" maps its output reference (a pair composed of the
// hash of the transaction that produced the UTxO and the index in the list of
// transaction outputs corresponding to it) to the result of encoding said UTxO
// NOTE: Just like transactions, UTxO's don't have a precise CBOR
// representation.
pub type UTxOKeyType<'a> = (&'a [u8], u8);
pub type UTxOValueType<'a> = &'a [u8];
pub type UTxOResultType = Vec<u8>;
pub const UTXO_TABLE: TableDefinition<UTxOKeyType, UTxOValueType> = TableDefinition::new("utxo");

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

// Table "prot_params" stores only the latest protocol parameters.
pub type ProtParamsKeyType = ();
pub type ProtParamsValueType<'a> = &'a [u8];
pub type ProtParamsResultType = Vec<u8>;
pub const PROT_PARAMS_TABLE: TableDefinition<ProtParamsKeyType, ProtParamsValueType> =
    TableDefinition::new("prot_params");

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

    pub fn apply_block(&self, block_cbor: BlockValueType) -> Result<(), Error> {
        let write_tx: WriteTransaction = self.inner_store.begin_write().map_err(Error::redb)?;
        let block: MultiEraBlock = self.store_block(&write_tx, block_cbor)?;
        self.store_txs(&write_tx, &block)?;
        write_tx.commit().map_err(Error::redb)?;

        Ok(())
    }

    fn store_block<'a>(
        &self,
        write_tx: &'a WriteTransaction,
        block_cbor: &'a [u8],
    ) -> Result<MultiEraBlock<'a>, Error> {
        let block: MultiEraBlock =
            MultiEraBlock::decode(block_cbor).map_err(Error::BlockDecoding)?;

        write_tx
            .open_table(BLOCK_TABLE)
            .map_err(Error::redb)?
            .insert(block.slot(), block_cbor)
            .map_err(Error::redb)?;

        write_tx
            .open_table(BLOCK_BY_HASH_INDEX)
            .map_err(Error::redb)?
            .insert(block.hash().deref(), block.slot())
            .map_err(Error::redb)?;

        Ok(block)
    }

    fn store_txs(&self, write_tx: &WriteTransaction, block: &MultiEraBlock) -> Result<(), Error> {
        let mut tx_table = write_tx.open_table(TX_TABLE).map_err(Error::redb)?;

        for tx in block.txs() {
            tx_table
                .insert(tx.hash().deref(), tx.encode().as_slice())
                .map_err(Error::redb)?;

            self.update_tx_outputs(write_tx, &tx)?;
        }

        Ok(())
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

    pub fn update_protocol_parameters(
        &self,
        prot_params: ProtParamsValueType,
    ) -> Result<(), Error> {
        let write_tx: WriteTransaction = self.inner_store.begin_write().map_err(Error::redb)?;

        let mut prot_params_table = write_tx
            .open_table(PROT_PARAMS_TABLE)
            .map_err(Error::redb)?;

        let _ = prot_params_table.insert((), prot_params);

        drop(prot_params_table);

        write_tx.commit().map_err(Error::redb)?;

        Ok(())
    }

    pub fn get_blockchain_tip(&self) -> Result<BlockResultType, Error> {
        self.inner_store
            .begin_read()
            .map_err(Error::redb)?
            .open_table(BLOCK_TABLE)
            .map_err(Error::redb)?
            .last()
            .map_err(Error::redb)?
            .ok_or(Error::KeyNotFound)
            .map(|entry| Vec::from(entry.1.value()))
    }

    pub fn get_protocol_parameters(&self) -> Result<ProtParamsResultType, Error> {
        self.inner_store
            .begin_read()
            .map_err(Error::redb)?
            .open_table(PROT_PARAMS_TABLE)
            .map_err(Error::redb)?
            .get(())
            .map_err(Error::redb)?
            .ok_or(Error::KeyNotFound)
            .map(|entry| Vec::from(entry.value()))
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

    pub fn get_utxo_from_reference(&self, utxo_ref: &UTxOKeyType) -> Option<UTxOResultType> {
        let read_tx: ReadTransaction = self.inner_store.begin_read().ok()?;
        let utxo_table: ReadOnlyTable<UTxOKeyType, UTxOValueType> =
            read_tx.open_table(UTXO_TABLE).ok()?;
        let res = utxo_table
            .get(utxo_ref)
            .ok()?
            .map(|val| Vec::from(val.value()));
        res
    }

    pub fn get_tx_from_hash(&self, tx_hash: &TxKeyType) -> Option<TxResultType> {
        let read_tx: ReadTransaction = self.inner_store.begin_read().ok()?;
        let tx_table: ReadOnlyTable<TxKeyType, TxValueType> = read_tx.open_table(TX_TABLE).ok()?;
        let res = tx_table
            .get(tx_hash)
            .ok()?
            .map(|val| Vec::from(val.value()));
        res
    }

    pub fn get_block_from_hash(&self, block_hash: &BlockKeyType) -> Option<BlockResultType> {
        self.inner_store
            .begin_read()
            .ok()?
            .open_table(BLOCK_TABLE)
            .ok()?
            .get(block_hash)
            .ok()?
            .map(|val| Vec::from(val.value()))
    }

    pub fn get_utxos_from_beacon(
        &self,
        beacon_policy_id: &UTxOByBeaconKeyType,
    ) -> Result<Box<impl Iterator<Item = UTxOByBeaconResultType>>, Error> {
        let read_tx = self.inner_store.begin_read().map_err(Error::redb)?;

        let utxo_by_beacon_table = read_tx
            .open_multimap_table(UTXO_BY_POLICY_INDEX)
            .map_err(Error::redb)?;

        let res = match utxo_by_beacon_table.get(beacon_policy_id) {
            Ok(database_results) => {
                let mut res = vec![];
                for val in database_results.flatten() {
                    let (tx_hash, tx_index) = val.value();
                    res.push((Vec::from(tx_hash), tx_index))
                }
                Ok(Box::new(res.into_iter()))
            }
            Err(err) => Err(Error::redb(err)),
        };
        res
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
