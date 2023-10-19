pub mod genesis;

use pallas::{crypto::hash::Hash, ledger::traverse::MultiEraBlock};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
};
use thiserror::Error;
use tracing::{error, info};

use rocksdb::{Options, WriteBatch, DB};

use crate::prelude::BlockHash;

use super::kvtable::*;

type TxHash = Hash<32>;
type OutputIndex = u64;
type UtxoBody = Vec<u8>;
type BlockSlot = u64;

#[derive(Error, Debug)]
pub enum Error {
    #[error("data error")]
    Data(super::kvtable::Error),

    #[error("missing utxo {0}#{1}")]
    MissingUtxo(TxHash, OutputIndex),

    #[error("missing stxi {0}#{1}")]
    MissingStxi(TxHash, OutputIndex),

    #[error("cbor decoding")]
    Cbor,
}

impl From<super::kvtable::Error> for Error {
    fn from(value: super::kvtable::Error) -> Self {
        Error::Data(value)
    }
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Eq, Hash)]
pub struct UtxoRef(pub TxHash, pub OutputIndex);

pub struct UtxoKV;

impl KVTable<DBSerde<UtxoRef>, DBBytes> for UtxoKV {
    const CF_NAME: &'static str = "UtxoKV";
}

// Spent transaction inputs
pub struct StxiKV;

impl KVTable<DBSerde<UtxoRef>, DBBytes> for StxiKV {
    const CF_NAME: &'static str = "StxiKV";
}

pub struct SlotKV;

#[derive(Serialize, Deserialize)]
pub struct SlotData {
    hash: BlockHash,
    #[deprecated]
    tombstones: Vec<UtxoRef>,
}

impl KVTable<DBInt, DBSerde<SlotData>> for SlotKV {
    const CF_NAME: &'static str = "SlotKV";
}

pub struct ApplyBatch<'a> {
    db: &'a rocksdb::DB,
    block_slot: BlockSlot,
    block_hash: BlockHash,
    utxo_inserts: HashMap<UtxoRef, UtxoBody>,
    stxi_inserts: HashMap<UtxoRef, UtxoBody>,
    utxo_deletes: Vec<UtxoRef>,
}

impl<'a> ApplyBatch<'a> {
    pub fn new(db: &'a rocksdb::DB, block_slot: BlockSlot, block_hash: BlockHash) -> Self {
        Self {
            db,
            block_slot,
            block_hash,
            utxo_inserts: HashMap::new(),
            stxi_inserts: HashMap::new(),
            utxo_deletes: Vec::new(),
        }
    }

    pub fn contains_utxo(&self, tx: TxHash, output: OutputIndex) -> bool {
        self.utxo_inserts.contains_key(&UtxoRef(tx, output))
    }

    pub fn insert_utxo(&mut self, tx: TxHash, output: OutputIndex, body: UtxoBody) {
        self.utxo_inserts.insert(UtxoRef(tx, output), body);
    }

    pub fn spend_utxo(&mut self, tx: TxHash, idx: OutputIndex, body: UtxoBody) {
        info!(%tx, idx, "spending utxo");

        let k = UtxoRef(tx, idx);

        self.stxi_inserts.insert(k.clone(), body);
        self.utxo_deletes.push(k);
    }

    pub fn spend_utxo_same_block(&mut self, tx: TxHash, idx: OutputIndex) {
        info!(%tx, idx, "spending utxo same block");

        let k = UtxoRef(tx, idx);

        let body = self.utxo_inserts.remove(&k).unwrap();

        self.stxi_inserts.insert(k.clone(), body);
        self.utxo_deletes.push(k)
    }
}

impl<'a> From<ApplyBatch<'a>> for WriteBatch {
    fn from(from: ApplyBatch<'a>) -> Self {
        let mut batch = WriteBatch::default();

        for (key, value) in from.utxo_inserts {
            UtxoKV::stage_upsert(from.db, DBSerde(key), DBBytes(value), &mut batch);
        }

        for key in from.utxo_deletes {
            UtxoKV::stage_delete(from.db, DBSerde(key), &mut batch);
        }

        for (key, value) in from.stxi_inserts {
            StxiKV::stage_upsert(from.db, DBSerde(key), DBBytes(value), &mut batch);
        }

        let k = DBInt(from.block_slot);

        #[allow(deprecated)]
        let v = DBSerde(SlotData {
            hash: from.block_hash,
            tombstones: vec![],
        });

        SlotKV::stage_upsert(from.db, k, v, &mut batch);

        batch
    }
}

pub struct UndoBatch<'a> {
    db: &'a rocksdb::DB,
    block_slot: BlockSlot,
    utxo_recovery: HashMap<UtxoRef, UtxoBody>,
    stxi_deletes: Vec<UtxoRef>,
    utxo_deletes: HashSet<UtxoRef>,
}

impl<'a> UndoBatch<'a> {
    pub fn new(db: &'a rocksdb::DB, block_slot: BlockSlot) -> Self {
        Self {
            db,
            block_slot,
            utxo_recovery: HashMap::new(),
            stxi_deletes: Vec::new(),
            utxo_deletes: HashSet::new(),
        }
    }

    pub fn would_delete_utxo(&self, tx: TxHash, output: OutputIndex) -> bool {
        self.utxo_deletes.contains(&UtxoRef(tx, output))
    }

    pub fn unspend_stxi(
        &mut self,
        tx: TxHash,
        output: OutputIndex,
        body: UtxoBody,
    ) -> Result<(), Error> {
        let k = UtxoRef(tx, output);

        self.utxo_recovery.insert(k.clone(), body);
        self.stxi_deletes.push(k);

        Ok(())
    }

    pub fn unspend_stxi_same_block(
        &mut self,
        tx: TxHash,
        output: OutputIndex,
    ) -> Result<(), Error> {
        let k = UtxoRef(tx, output);

        self.utxo_deletes.remove(&k);
        self.stxi_deletes.push(k);

        Ok(())
    }

    pub fn delete_utxo(&mut self, tx: TxHash, output: OutputIndex) {
        let k = UtxoRef(tx, output);
        self.utxo_deletes.insert(k);
    }
}

impl<'a> From<UndoBatch<'a>> for WriteBatch {
    fn from(from: UndoBatch<'a>) -> Self {
        let mut batch = WriteBatch::default();

        for (key, value) in from.utxo_recovery {
            UtxoKV::stage_upsert(from.db, DBSerde(key), DBBytes(value), &mut batch);
        }

        for key in from.utxo_deletes {
            UtxoKV::stage_delete(from.db, DBSerde(key), &mut batch);
        }

        for key in from.stxi_deletes {
            StxiKV::stage_delete(from.db, DBSerde(key), &mut batch);
        }

        let k = DBInt(from.block_slot);
        SlotKV::stage_delete(from.db, k, &mut batch);

        batch
    }
}

#[derive(Clone)]
pub struct ApplyDB {
    db: Arc<DB>,
}

impl ApplyDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open_cf(
            &opts,
            path,
            [UtxoKV::CF_NAME, StxiKV::CF_NAME, SlotKV::CF_NAME],
        )
        .map_err(|_| super::kvtable::Error::IO)?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn is_empty(&self) -> bool {
        SlotKV::is_empty(&self.db)
    }

    pub fn cursor(&self) -> Result<Option<(BlockSlot, BlockHash)>, Error> {
        let v = SlotKV::last_entry(&self.db)?;
        let out = v.map(|(s, d)| (s.0, d.0.hash));

        Ok(out)
    }

    pub fn get_utxo(&self, tx: TxHash, output: OutputIndex) -> Result<Option<UtxoBody>, Error> {
        let dbval = UtxoKV::get_by_key(&self.db, DBSerde(UtxoRef(tx, output)))?;
        Ok(dbval.map(|x| x.0))
    }

    pub fn get_stxi(&self, tx: TxHash, output: OutputIndex) -> Result<Option<UtxoBody>, Error> {
        let dbval = StxiKV::get_by_key(&self.db, DBSerde(UtxoRef(tx, output)))?;
        Ok(dbval.map(|x| x.0))
    }

    pub fn apply_block(&mut self, cbor: &[u8]) -> Result<(), Error> {
        let block = MultiEraBlock::decode(cbor).map_err(|_| Error::Cbor)?;
        let slot = block.slot();
        let hash = block.hash();

        let mut batch = ApplyBatch::new(&self.db, slot, hash);

        let txs = block.txs();

        for tx in txs.iter() {
            for (idx, produced) in tx.produces() {
                let body = produced.encode();
                batch.insert_utxo(tx.hash(), idx as u64, body);
            }
        }

        for tx in txs.iter() {
            for consumed in tx.consumes() {
                let hash = *consumed.hash();
                let idx = consumed.index();

                if batch.contains_utxo(hash, idx) {
                    batch.spend_utxo_same_block(hash, idx);
                } else {
                    let utxo = self
                        .get_utxo(hash, idx)?
                        .ok_or(Error::MissingUtxo(hash, idx))?;

                    batch.spend_utxo(hash, idx, utxo);
                };
            }
        }

        let batch = WriteBatch::from(batch);

        self.db
            .write(batch)
            .map_err(|_| super::kvtable::Error::IO)?;

        Ok(())
    }

    pub fn undo_block(&mut self, cbor: &[u8]) -> Result<(), Error> {
        let block = MultiEraBlock::decode(cbor).map_err(|_| Error::Cbor)?;
        let slot = block.slot();

        let mut batch = UndoBatch::new(&self.db, slot);

        for tx in block.txs() {
            for (idx, _) in tx.produces() {
                batch.delete_utxo(tx.hash(), idx as u64);
            }
        }

        for tx in block.txs() {
            for consumed in tx.consumes() {
                let hash = consumed.hash();
                let idx = consumed.index();

                if batch.would_delete_utxo(*hash, idx) {
                    batch.unspend_stxi_same_block(*hash, idx)?;
                } else {
                    let body = self
                        .get_stxi(*hash, idx)?
                        .ok_or(Error::MissingStxi(*hash, idx))?;

                    batch.unspend_stxi(*hash, idx, body)?;
                }
            }
        }

        let batch = WriteBatch::from(batch);

        self.db
            .write(batch)
            .map_err(|_| super::kvtable::Error::IO)?;

        info!(slot, "deleted block");

        Ok(())
    }

    pub fn compact(&self, _max_slot: u64) -> Result<(), Error> {
        // TODO: iterate by slot from start until max slot and delete utxos + tombstone
        todo!()
    }

    pub fn destroy(path: impl AsRef<Path>) -> Result<(), Error> {
        DB::destroy(&Options::default(), path).map_err(|_| super::kvtable::Error::IO)?;

        Ok(())
    }

    #[cfg(test)]
    pub fn insert_dummy_utxo(&mut self, hash: TxHash, index: OutputIndex) {
        let mut batch = WriteBatch::default();

        UtxoKV::stage_upsert(
            &self.db,
            DBSerde(UtxoRef(hash, index)),
            DBBytes(vec![]),
            &mut batch,
        );

        self.db.write(batch).unwrap();
    }

    #[cfg(test)]
    pub fn insert_dummy_stxi(&mut self, hash: TxHash, index: OutputIndex) {
        let mut batch = WriteBatch::default();

        StxiKV::stage_upsert(
            &self.db,
            DBSerde(UtxoRef(hash, index)),
            DBBytes(vec![]),
            &mut batch,
        );

        self.db.write(batch).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn with_tmp_db(op: fn(db: ApplyDB) -> ()) {
        let path = tempfile::tempdir().unwrap().into_path();
        let db = ApplyDB::open(path.clone()).unwrap();

        op(db);

        ApplyDB::destroy(path).unwrap();
    }

    fn load_test_block(name: &str) -> Vec<u8> {
        let path = std::path::PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").unwrap())
            .join("test_data")
            .join(name);

        let content = std::fs::read_to_string(&path).unwrap();
        hex::decode(content).unwrap()
    }

    #[test]
    fn test_apply_block() {
        with_tmp_db(|mut db| {
            // nice block with several txs, it includes chaining edge case
            let cbor = load_test_block("alonzo27.block");

            let block = MultiEraBlock::decode(&cbor).unwrap();

            let block_txs: Vec<_> = block.txs().iter().map(|tx| tx.hash()).collect();

            for tx in block.txs() {
                for input in tx.consumes() {
                    // skip inserting dummy utxo if it's part of the current block
                    if block_txs.contains(input.hash()) {
                        continue;
                    }

                    db.insert_dummy_utxo(*input.hash(), input.index());
                }
            }

            db.apply_block(&cbor).unwrap();

            for tx in block.txs() {
                for input in tx.consumes() {
                    // assert that consumed utxos are no longer in the unspent set
                    let utxo = db.get_utxo(*input.hash(), input.index()).unwrap();
                    assert!(utxo.is_none());

                    // assert that consumed utxos moved to the spent set
                    let stxi = db.get_stxi(*input.hash(), input.index()).unwrap();
                    assert!(stxi.is_some());
                }

                for (idx, _) in tx.produces() {
                    let utxo = db.get_utxo(tx.hash(), idx as u64).unwrap();
                    let stxi = db.get_stxi(tx.hash(), idx as u64).unwrap();

                    // assert that produced utxos were added to either unspent or spent set
                    assert_ne!(utxo.is_some(), stxi.is_some());
                }
            }
        });
    }

    #[test]
    fn test_undo_block() {
        with_tmp_db(|mut db| {
            // nice block with several txs, it includes chaining edge case
            let cbor = load_test_block("alonzo27.block");

            let block = MultiEraBlock::decode(&cbor).unwrap();

            let block_txs: Vec<_> = block.txs().iter().map(|tx| tx.hash()).collect();

            for tx in block.txs() {
                for input in tx.consumes() {
                    // skip inserting dummy stxi if it's part of the current block
                    if block_txs.contains(input.hash()) {
                        continue;
                    }

                    db.insert_dummy_stxi(*input.hash(), input.index());
                }
            }

            db.undo_block(&cbor).unwrap();

            for tx in block.txs() {
                for input in tx.consumes() {
                    // assert that consumed utxos go back to the unspent set, unless they are from
                    // the same block
                    let utxo = db.get_utxo(*input.hash(), input.index()).unwrap();

                    if block_txs.contains(input.hash()) {
                        assert!(utxo.is_none());
                    } else {
                        assert!(utxo.is_some());
                    }

                    // assert that consumed utxos are no longer in the spent set
                    let stxi = db.get_stxi(*input.hash(), input.index()).unwrap();
                    assert!(stxi.is_none());
                }

                for (idx, _) in tx.produces() {
                    // assert that produced utxos are no longer in the unspent set
                    let utxo = db.get_utxo(tx.hash(), idx as u64).unwrap();
                    assert!(utxo.is_none());
                }
            }
        });
    }
}
