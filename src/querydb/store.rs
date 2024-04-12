use crate::querydb::prelude::{Error::*, *};
use pallas::{
    codec::minicbor::decode::Error as DecodingError,
    ledger::{
        addresses::Address,
        traverse::{Era, MultiEraBlock, MultiEraOutput, MultiEraPolicyAssets, MultiEraTx},
    },
};
use redb::{
    Database, MultimapTable, ReadOnlyMultimapTable, ReadOnlyTable, ReadTransaction,
    ReadableMultimapTable, ReadableTable, Table, WriteTransaction,
};
use std::{ops::Deref, path::Path};

pub struct Store {
    inner_store: Database,
}

impl Store {
    pub fn new(path: impl AsRef<Path>) -> Result<Self, Error> {
        Ok(Store {
            inner_store: Database::create(path).map_err(|e| ReDBError(Box::new(e)))?,
        })
    }
    pub fn apply_block(&self, block_cbor: BlockValueType) -> Result<(), Error> {
        let write_tx: WriteTransaction = self
            .inner_store
            .begin_write()
            .map_err(|e| ReDBError(Box::new(e)))?;
        let block: MultiEraBlock = self.store_block(&write_tx, block_cbor)?;
        self.store_txs(&write_tx, &block)?;
        write_tx.commit().map_err(|e| ReDBError(Box::new(e)))?;
        Ok(())
    }

    fn store_block<'a>(
        &self,
        write_tx: &'a WriteTransaction,
        block_cbor: &'a [u8],
    ) -> Result<MultiEraBlock<'a>, Error> {
        let block: MultiEraBlock = MultiEraBlock::decode(block_cbor).map_err(BlockDecoding)?;
        let mut block_table: Table<BlockKeyType, BlockValueType> = write_tx
            .open_table(BLOCK_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        let _ = block_table.insert(block.slot(), block_cbor);
        let mut block_by_hash_table: Table<BlockByHashKeyType, BlockByHashValueType> = write_tx
            .open_table(BLOCK_BY_HASH_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        let _ = block_by_hash_table.insert(block.hash().deref(), block.slot());
        Ok(block)
    }

    fn store_txs(&self, write_tx: &WriteTransaction, block: &MultiEraBlock) -> Result<(), Error> {
        let mut tx_table: Table<TxKeyType, TxValueType> = write_tx
            .open_table(TX_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        for tx in block.txs() {
            tx_table
                .insert(tx.hash().deref(), tx.encode().as_slice())
                .map_err(|e| ReDBError(Box::new(e)))?;
            self.update_tx_outputs(write_tx, &tx)?;
        }
        Ok(())
    }

    fn update_tx_outputs(&self, write_tx: &WriteTransaction, tx: &MultiEraTx) -> Result<(), Error> {
        let mut utxo_table: Table<UTxOKeyType, UTxOValueType> = write_tx
            .open_table(UTXO_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        let mut utxo_by_addr_table: MultimapTable<UTxOByAddrKeyType, UTxOByAddrValueType> =
            write_tx
                .open_multimap_table(UTXO_BY_ADDR_TABLE)
                .map_err(|e| ReDBError(Box::new(e)))?;
        let mut utxo_by_beacon_table: MultimapTable<UTxOByBeaconKeyType, UTxOByBeaconValueType> =
            write_tx
                .open_multimap_table(UTXO_BY_BEACON_TABLE)
                .map_err(|e| ReDBError(Box::new(e)))?;
        for (index, tx_out) in tx.produces() {
            let tx_hash = tx.hash();
            utxo_table
                .insert(
                    (tx_hash.as_slice(), index as u8),
                    tx_out.encode().as_slice(),
                )
                .map_err(|e| ReDBError(Box::new(e)))?;
            utxo_by_addr_table
                .insert(
                    output_address(&tx_out)?.to_vec().as_slice(),
                    (tx_hash.as_slice(), index as u8),
                )
                .map_err(|e| ReDBError(Box::new(e)))?;
            for policy in output_policy_ids(&tx_out) {
                utxo_by_beacon_table
                    .insert(&policy, (tx_hash.as_slice(), index as u8))
                    .map_err(|e| ReDBError(Box::new(e)))?;
            }
        }
        for multi_era_input in tx.consumes() {
            let tx_in: UTxOKeyType = (
                multi_era_input.hash().as_slice(),
                multi_era_input.index() as u8,
            );
            match utxo_table.get(tx_in).map_err(|e| ReDBError(Box::new(e)))? {
                Some(encoded_tx_out) => {
                    let tx_out: MultiEraOutput =
                        decode_output(encoded_tx_out.value()).map_err(OutputDecoding)?;
                    utxo_by_addr_table
                        .remove(output_address(&tx_out)?.to_vec().as_slice(), tx_in)
                        .map_err(|e| ReDBError(Box::new(e)))?;
                    for policy in output_policy_ids(&tx_out) {
                        utxo_by_beacon_table
                            .remove(&policy, tx_in)
                            .map_err(|e| ReDBError(Box::new(e)))?;
                    }
                }
                None => return Err(UTxOTableInvariantBroken), /* This means the input is not
                                                               * available for spending! */
            }
            utxo_table.remove(tx_in).map_err(|_| unreachable!())?;
        }
        Ok(())
    }

    pub fn update_protocol_parameters(
        &self,
        prot_params: ProtParamsValueType,
    ) -> Result<(), Error> {
        let write_tx: WriteTransaction = self
            .inner_store
            .begin_write()
            .map_err(|e| ReDBError(Box::new(e)))?;
        let mut prot_params_table: Table<ProtParamsKeyType, ProtParamsValueType> = write_tx
            .open_table(PROT_PARAMS_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        let _ = prot_params_table.insert((), prot_params);
        drop(prot_params_table);
        write_tx.commit().map_err(|e| ReDBError(Box::new(e)))?;
        Ok(())
    }

    pub fn get_blockchain_tip(&self) -> Result<BlockResultType, Error> {
        let read_tx: ReadTransaction = self
            .inner_store
            .begin_read()
            .map_err(|e| ReDBError(Box::new(e)))?;
        let blockchain_table: ReadOnlyTable<BlockKeyType, BlockValueType> = read_tx
            .open_table(BLOCK_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        let res = blockchain_table
            .last()
            .map_err(|e| ReDBError(Box::new(e)))?
            .ok_or(KeyNotFound)
            .map(|entry| Vec::from(entry.1.value()));
        res
    }

    pub fn get_protocol_parameters(&self) -> Result<ProtParamsResultType, Error> {
        let read_tx: ReadTransaction = self
            .inner_store
            .begin_read()
            .map_err(|e| ReDBError(Box::new(e)))?;
        let prot_params_table: ReadOnlyTable<ProtParamsKeyType, ProtParamsValueType> = read_tx
            .open_table(PROT_PARAMS_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        let res = prot_params_table
            .get(())
            .map_err(|e| ReDBError(Box::new(e)))?
            .ok_or(KeyNotFound)
            .map(|entry| Vec::from(entry.value()));
        res
    }

    pub fn get_utxos_from_address<T>(
        &self,
        addr: &UTxOByAddrKeyType,
    ) -> Result<Box<impl Iterator<Item = UTxOByAddrResultType>>, Error> {
        let read_tx: ReadTransaction = self
            .inner_store
            .begin_read()
            .map_err(|e| ReDBError(Box::new(e)))?;
        let utxo_by_addr_table: ReadOnlyMultimapTable<UTxOByAddrKeyType, UTxOByAddrValueType> =
            read_tx
                .open_multimap_table(UTXO_BY_ADDR_TABLE)
                .map_err(|e| ReDBError(Box::new(e)))?;
        let res = match utxo_by_addr_table.get(addr) {
            Ok(database_results) => {
                let mut res = vec![];
                for val in database_results.flatten() {
                    let (tx_hash, tx_index) = val.value();
                    res.push((Vec::from(tx_hash), tx_index))
                }
                Ok(Box::new(res.into_iter()))
            }
            Err(err) => Err(ReDBError(Box::new(err))),
        };
        res
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
        let read_tx: ReadTransaction = self.inner_store.begin_read().ok()?;
        let tx_table: ReadOnlyTable<BlockKeyType, BlockValueType> =
            read_tx.open_table(BLOCK_TABLE).ok()?;
        let res = tx_table
            .get(block_hash)
            .ok()?
            .map(|val| Vec::from(val.value()));
        res
    }

    pub fn get_utxos_from_beacon(
        &self,
        beacon_policy_id: &UTxOByBeaconKeyType,
    ) -> Result<Box<impl Iterator<Item = UTxOByBeaconResultType>>, Error> {
        let read_tx: ReadTransaction = self
            .inner_store
            .begin_read()
            .map_err(|e| ReDBError(Box::new(e)))?;
        let utxo_by_beacon_table: ReadOnlyMultimapTable<
            UTxOByBeaconKeyType,
            UTxOByBeaconValueType,
        > = read_tx
            .open_multimap_table(UTXO_BY_BEACON_TABLE)
            .map_err(|e| ReDBError(Box::new(e)))?;
        let res = match utxo_by_beacon_table.get(beacon_policy_id) {
            Ok(database_results) => {
                let mut res = vec![];
                for val in database_results.flatten() {
                    let (tx_hash, tx_index) = val.value();
                    res.push((Vec::from(tx_hash), tx_index))
                }
                Ok(Box::new(res.into_iter()))
            }
            Err(err) => Err(ReDBError(Box::new(err))),
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
    output.address().map_err(AddressDecoding)
}

fn decode_output(encoded_output: &[u8]) -> Result<MultiEraOutput, DecodingError> {
    MultiEraOutput::decode(Era::Byron, encoded_output)
        .or_else(|_| MultiEraOutput::decode(Era::Alonzo, encoded_output))
        .or_else(|_| MultiEraOutput::decode(Era::Babbage, encoded_output))
}
