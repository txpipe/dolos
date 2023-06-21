use pallas::crypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

use rocksdb::{Options, WriteBatch, DB};

use crate::prelude::BlockHash;

use super::kvtable::*;

type TxHash = Hash<32>;
type OutputIndex = u64;
type UtxoBody = Vec<u8>;
type BlockSlot = u64;

#[derive(Serialize, Deserialize, Clone)]
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
    tombstones: Vec<UtxoRef>,
}

impl KVTable<DBInt, DBSerde<SlotData>> for SlotKV {
    const CF_NAME: &'static str = "SlotKV";
}

pub struct BlockWriteBatch<'a>(&'a rocksdb::DB, BlockSlot, WriteBatch);

impl<'a> BlockWriteBatch<'a> {
    pub fn insert_utxo(&mut self, tx: TxHash, output: OutputIndex, body: UtxoBody) {
        UtxoKV::stage_upsert(
            self.0,
            DBSerde(UtxoRef(tx, output)),
            DBBytes(body),
            &mut self.2,
        )
    }

    pub fn spend_utxo(&mut self, tx: TxHash, output: OutputIndex) -> Result<(), Error> {
        let k = DBSerde(UtxoRef(tx, output));
        let v = UtxoKV::get_by_key(self.0, k.clone())?.ok_or(Error::NotFound)?;
        StxiKV::stage_upsert(self.0, k.clone(), v, &mut self.2);
        UtxoKV::stage_delete(self.0, k, &mut self.2);

        Ok(())
    }

    pub fn unspend_stxi(&mut self, tx: TxHash, output: OutputIndex) -> Result<(), Error> {
        let k = DBSerde(UtxoRef(tx, output));
        let v = StxiKV::get_by_key(self.0, k.clone())?.ok_or(Error::NotFound)?;
        UtxoKV::stage_upsert(self.0, k.clone(), v, &mut self.2);
        StxiKV::stage_delete(self.0, k, &mut self.2);

        Ok(())
    }

    pub fn delete_utxo(&mut self, tx: TxHash, output: OutputIndex) {
        let k = DBSerde(UtxoRef(tx, output));
        UtxoKV::stage_delete(self.0, k, &mut self.2);
    }

    pub fn insert_slot(&mut self, hash: BlockHash, tombstones: Vec<UtxoRef>) {
        let k = DBInt(self.1);
        let v = DBSerde(SlotData { hash, tombstones });
        SlotKV::stage_upsert(self.0, k, v, &mut self.2);
    }

    pub fn delete_slot(&mut self) {
        let k = DBInt(self.1);
        SlotKV::stage_delete(self.0, k, &mut self.2);
    }

    // TODO: change_params
}

impl<'a> From<BlockWriteBatch<'a>> for WriteBatch {
    fn from(value: BlockWriteBatch<'a>) -> Self {
        value.2
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
        .map_err(|_| Error::IO)?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn get_slot_data(&self, slot: BlockSlot) -> Result<Option<SlotData>, Error> {
        let v = SlotKV::get_by_key(&self.db, DBInt(slot))?;
        let out = v.map(|d| d.0);

        Ok(out)
    }

    pub fn cursor(&self) -> Result<Option<(BlockSlot, BlockHash)>, Error> {
        let entry = self.get_last_slot()?;
        let out = entry.map(|(s, d)| (s, d.hash));

        Ok(out)
    }

    pub fn get_last_slot(&self) -> Result<Option<(BlockSlot, SlotData)>, Error> {
        let v = SlotKV::last_entry(&self.db)?;
        let out = v.map(|(s, d)| (s.0, d.0));

        Ok(out)
    }

    pub fn get_utxo(&self, tx: TxHash, output: OutputIndex) -> Result<Option<UtxoBody>, Error> {
        let dbval = UtxoKV::get_by_key(&self.db, DBSerde(UtxoRef(tx, output)))?;
        Ok(dbval.map(|x| x.0))
    }

    pub fn start_block(&self, slot: BlockSlot) -> BlockWriteBatch {
        BlockWriteBatch(&self.db, slot, WriteBatch::default())
    }

    pub fn commit_block(&self, batch: BlockWriteBatch) -> Result<(), Error> {
        let batch = WriteBatch::from(batch);
        self.db.write(batch).map_err(|_| Error::IO)?;

        Ok(())
    }

    pub fn compact(&self, _max_slot: u64) -> Result<(), Error> {
        // TODO: iterate by slot from start until max slot and delete utxos + tombstone
        todo!()
    }

    pub fn destroy(path: impl AsRef<Path>) -> Result<(), Error> {
        DB::destroy(&Options::default(), path).map_err(|_| Error::IO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_tmp_db(op: fn(db: ApplyDB) -> ()) {
        let path = tempfile::tempdir().unwrap().into_path();
        let db = ApplyDB::open(path.clone()).unwrap();

        op(db);

        ApplyDB::destroy(path).unwrap();
    }

    fn dummy_utxo(tx: u64, idx: OutputIndex) -> (TxHash, OutputIndex, UtxoBody) {
        let hash = pallas::crypto::hash::Hasher::<256>::hash(tx.to_be_bytes().as_slice());
        (hash, idx, (tx + idx as u64).to_be_bytes().to_vec())
    }

    #[test]
    fn test_insert_slot_consistency() {
        with_tmp_db(|db| {
            let (tx1, idx1, _) = dummy_utxo(0, 0);
            let (tx2, idx2, _) = dummy_utxo(0, 1);

            let slot = 22;
            let hash = pallas::crypto::hash::Hasher::<256>::hash(44u32.to_be_bytes().as_slice());
            let tombstones = vec![UtxoRef(tx1, idx1), UtxoRef(tx2, idx2)];

            let mut apply = db.start_block(slot);

            apply.insert_slot(hash, tombstones.clone());

            db.commit_block(apply).unwrap();

            let (out_slot, out_data) = db.get_last_slot().unwrap().unwrap();
            assert_eq!(out_slot, slot);
            assert_eq!(out_data.hash, hash);

            // assert tombstone are there
            for (should, expect) in out_data.tombstones.iter().zip(tombstones) {
                assert_eq!(should.0, expect.0);
                assert_eq!(should.1, expect.1);
            }
        });
    }

    #[test]
    fn test_insert_utxos_consitency() {
        with_tmp_db(|db| {
            let mut batch = db.start_block(22);

            let (tx1, idx1, body1) = dummy_utxo(0, 0);
            let (tx2, idx2, body2) = dummy_utxo(0, 1);
            let (tx3, idx3, body3) = dummy_utxo(1, 0);

            batch.insert_utxo(tx1, idx1, body1.clone());
            batch.insert_utxo(tx2, idx2, body2.clone());
            batch.insert_utxo(tx3, idx3, body3.clone());

            db.commit_block(batch).unwrap();

            let persisted = db.get_utxo(tx1, idx1).unwrap().unwrap();
            assert_eq!(persisted, body1);

            let persisted = db.get_utxo(tx2, idx2).unwrap().unwrap();
            assert_eq!(persisted, body2);

            let persisted = db.get_utxo(tx3, idx3).unwrap().unwrap();
            assert_eq!(persisted, body3);
        });
    }

    #[test]
    fn test_spend_unspend_utxos() {
        with_tmp_db(|db| {
            let (tx1, idx1, body1) = dummy_utxo(0, 0);
            let (tx2, idx2, body2) = dummy_utxo(0, 1);
            let (tx3, idx3, body3) = dummy_utxo(1, 0);

            // producer blocker
            let slot = 22;
            let hash = pallas::crypto::hash::Hasher::<256>::hash(44u32.to_be_bytes().as_slice());
            let mut batch = db.start_block(slot);

            batch.insert_utxo(tx1, idx1, body1.clone());
            batch.insert_utxo(tx2, idx2, body2.clone());
            batch.insert_utxo(tx3, idx3, body3.clone());

            batch.insert_slot(hash, Default::default());

            db.commit_block(batch).unwrap();

            // spender block
            let slot = 23;
            let hash = pallas::crypto::hash::Hasher::<256>::hash(45u32.to_be_bytes().as_slice());
            let mut batch = db.start_block(slot);

            batch.spend_utxo(tx1, idx1).unwrap();
            batch.spend_utxo(tx3, idx3).unwrap();

            batch.insert_slot(hash, vec![UtxoRef(tx1, idx1), UtxoRef(tx3, idx3)]);

            db.commit_block(batch).unwrap();

            // assert some utxo are missing
            assert!(db.get_utxo(tx1, idx1).unwrap().is_none());
            assert!(db.get_utxo(tx2, idx2).unwrap().is_some());
            assert!(db.get_utxo(tx3, idx3).unwrap().is_none());

            // undo spender block
            let slot = 23;
            let mut batch = db.start_block(slot);

            batch.unspend_stxi(tx1, idx1).unwrap();
            batch.unspend_stxi(tx3, idx3).unwrap();

            batch.delete_slot();

            db.commit_block(batch).unwrap();

            // assert all utxo are back
            assert!(db.get_utxo(tx1, idx1).unwrap().is_some());
            assert!(db.get_utxo(tx2, idx2).unwrap().is_some());
            assert!(db.get_utxo(tx3, idx3).unwrap().is_some());

            // assert slot is missing
            assert!(db.get_slot_data(23).unwrap().is_none());
        });
    }
}
