use crate::querydb::prelude::*;
use pallas::{
    crypto::hash::Hash,
    ledger::traverse::{MultiEraBlock, MultiEraPolicyAssets, MultiEraTx},
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
    pub fn new(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        Ok(Store {
            inner_store: Database::create(path).map_err(|e| StoreError::ReDBError(Box::new(e)))?,
        })
    }
    pub fn apply_block(&self, block_cbor: &[u8]) -> Result<(), StoreError> {
        let write_tx: WriteTransaction = self
            .inner_store
            .begin_write()
            .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        let block: MultiEraBlock = Self::store_block(&write_tx, block_cbor)?;
        Self::store_txs(&write_tx, &block)?;
        write_tx
            .commit()
            .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        Ok(())
    }

    fn store_block<'a>(
        write_tx: &'a WriteTransaction,
        block_cbor: &'a [u8],
    ) -> Result<MultiEraBlock<'a>, StoreError> {
        let block: MultiEraBlock =
            MultiEraBlock::decode(block_cbor).map_err(StoreError::BlockDecoding)?;
        let mut block_table: Table<BlockKeyType, BlockValueType> = write_tx
            .open_table(BLOCK_TABLE)
            .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        let _ = block_table.insert(block.hash().deref(), block_cbor);
        let mut chain_tip_table: Table<ChainTipKeyType, ChainTipValueType> = write_tx
            .open_table(CHAIN_TIP_TABLE)
            .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        let _ = chain_tip_table.insert(
            chain_tip_table
                .len()
                .map_err(|e| StoreError::ReDBError(Box::new(e)))?
                + 1,
            block.hash().deref(),
        );
        Ok(block)
    }

    fn store_txs(write_tx: &WriteTransaction, block: &MultiEraBlock) -> Result<(), StoreError> {
        let mut tx_table: Table<TxKeyType, TxValueType> = write_tx
            .open_table(TX_TABLE)
            .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        for tx in block.txs() {
            tx_table
                .insert(tx.hash().deref(), tx.encode().as_slice())
                .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
            Self::store_utxos(write_tx, &tx)?;
        }
        Ok(())
    }

    fn store_utxos(write_tx: &WriteTransaction, tx: &MultiEraTx) -> Result<(), StoreError> {
        let mut utxo_table: Table<UTxOKeyType, UTxOValueType> = write_tx
            .open_table(UTXO_TABLE)
            .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        let mut utxo_by_addr_table: MultimapTable<UTxOByAddrKeyType, UTxOByAddrValueType> =
            write_tx
                .open_multimap_table(UTXO_BY_ADDR_TABLE)
                .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        let mut utxo_by_beacon_table: MultimapTable<UTxOByBeaconKeyType, UTxOByBeaconValueType> =
            write_tx
                .open_multimap_table(UTXO_BY_BEACON_TABLE)
                .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
        for (index, output) in tx.produces() {
            utxo_table
                .insert(
                    (tx.hash().deref().as_slice(), index as u8),
                    output.encode().as_slice(),
                )
                .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
            let addr: Vec<u8> = output
                .address()
                .map_err(StoreError::AddressDecoding)?
                .to_vec();
            utxo_by_addr_table
                .insert(addr.as_slice(), (tx.hash().deref().as_slice(), index as u8))
                .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
            for policy in output
                .non_ada_assets()
                .iter()
                .map(MultiEraPolicyAssets::policy)
            {
                utxo_by_beacon_table
                    .insert(policy.deref(), (tx.hash().deref().as_slice(), index as u8))
                    .map_err(|e| StoreError::ReDBError(Box::new(e)))?;
            }
        }
        Ok(())
    }

    pub fn get_chain_tip(&self) -> Result<ChainTipResultType, ReadError> {
        let read_tx: ReadTransaction = self
            .inner_store
            .begin_read()
            .map_err(ReadError::TransactionError)?;
        let chain_tip_table: ReadOnlyTable<ChainTipKeyType, ChainTipValueType> = read_tx
            .open_table(CHAIN_TIP_TABLE)
            .map_err(ReadError::TableError)?;
        let res = chain_tip_table
            .last()
            .map_err(ReadError::StorageError)?
            .ok_or(ReadError::ChainTipNotFound)
            .map(|entry| Vec::from(entry.1.value()));
        res
    }

    pub fn get_chain_parameters(&self) {
        unimplemented!()
    }

    pub fn get_utxos_from_address<T>(
        &self,
        addr: UTxOByAddrKeyType,
    ) -> Result<Box<impl Iterator<Item = UTxOByAddrResultType>>, ReadError> {
        let read_tx: ReadTransaction = self
            .inner_store
            .begin_read()
            .map_err(ReadError::TransactionError)?;
        let utxo_by_addr_table: ReadOnlyMultimapTable<UTxOByAddrKeyType, UTxOByAddrValueType> =
            read_tx
                .open_multimap_table(UTXO_BY_ADDR_TABLE)
                .map_err(ReadError::TableError)?;
        let res = match utxo_by_addr_table.get(addr) {
            Ok(database_results) => {
                let mut res = vec![];
                for val in database_results.flatten() {
                    let (tx_hash, tx_index) = val.value();
                    res.push((Vec::from(tx_hash), tx_index))
                }
                Ok(Box::new(res.into_iter()))
            }
            Err(err) => Err(ReadError::StorageError(err)),
        };
        res
    }

    pub fn get_utxo_from_reference(
        &self,
        tx_hash: &Hash<32>,
        tx_index: u8,
    ) -> Option<UTxOResultType> {
        let read_tx: ReadTransaction = self.inner_store.begin_read().ok()?;
        let utxo_table: ReadOnlyTable<UTxOKeyType, UTxOValueType> =
            read_tx.open_table(UTXO_TABLE).ok()?;
        let res = utxo_table
            .get((tx_hash.as_ref(), tx_index))
            .ok()?
            .map(|val| Vec::from(val.value()));
        res
    }

    pub fn get_tx_from_hash(&self, tx_hash: &Hash<32>) -> Option<TxResultType> {
        let read_tx: ReadTransaction = self.inner_store.begin_read().ok()?;
        let tx_table: ReadOnlyTable<TxKeyType, TxValueType> = read_tx.open_table(TX_TABLE).ok()?;
        let res = tx_table
            .get(tx_hash.deref())
            .ok()?
            .map(|val| Vec::from(val.value()));
        res
    }

    pub fn get_block_from_hash(&self, block_hash: &Hash<32>) -> Option<BlockResultType> {
        let read_tx: ReadTransaction = self.inner_store.begin_read().ok()?;
        let tx_table: ReadOnlyTable<BlockKeyType, BlockValueType> =
            read_tx.open_table(BLOCK_TABLE).ok()?;
        let res = tx_table
            .get(block_hash.deref())
            .ok()?
            .map(|val| Vec::from(val.value()));
        res
    }

    pub fn get_utxos_from_beacon(
        &self,
        beacon_policy_id: &Hash<28>,
    ) -> Result<Box<impl Iterator<Item = UTxOByBeaconResultType>>, ReadError> {
        let read_tx: ReadTransaction = self
            .inner_store
            .begin_read()
            .map_err(ReadError::TransactionError)?;
        let utxo_by_beacon_table: ReadOnlyMultimapTable<
            UTxOByBeaconKeyType,
            UTxOByBeaconValueType,
        > = read_tx
            .open_multimap_table(UTXO_BY_BEACON_TABLE)
            .map_err(ReadError::TableError)?;
        let res = match utxo_by_beacon_table.get(beacon_policy_id.deref()) {
            Ok(database_results) => {
                let mut res = vec![];
                for val in database_results.flatten() {
                    let (tx_hash, tx_index) = val.value();
                    res.push((Vec::from(tx_hash), tx_index))
                }
                Ok(Box::new(res.into_iter()))
            }
            Err(err) => Err(ReadError::StorageError(err)),
        };
        res
    }
}
