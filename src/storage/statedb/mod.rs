use pallas::crypto::hash::Hash;
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use thiserror::Error;

use rocksdb::{Options, WriteBatch, DB};

use super::kvtable::*;

type TxHash = Hash<32>;
type OutputIndex = u32;
type UtxoBody = Vec<u8>;

#[derive(Serialize, Deserialize)]
struct UtxoRef(TxHash, OutputIndex);

pub struct UtxoKV;

impl KVTable<DBSerde<UtxoRef>, DBBytes> for UtxoKV {
    const CF_NAME: &'static str = "UtxoKV";
}

pub struct ApplyBlockWriteBatch(StateDB, WriteBatch);

impl ApplyBlockWriteBatch {
    pub fn insert_utxo(&mut self, tx: TxHash, output: OutputIndex, body: UtxoBody) {
        UtxoKV::stage_upsert(
            &self.0.db,
            DBSerde(UtxoRef(tx, output)),
            DBBytes(body),
            &mut self.1,
        )
    }

    pub fn consume_utxo(&mut self, tx: TxHash, output: OutputIndex) {
        UtxoKV::stage_delete(&self.0.db, DBSerde(UtxoRef(tx, output)), &mut self.1)
    }

    // TODO: change_params
    // TODO: change_tip

    pub fn commit(self) -> Result<StateDB, Error> {
        self.0.db.write(self.1).map_err(|_| Error::IO)?;

        Ok(self.0)
    }

    pub fn abort(self) -> StateDB {
        self.0
    }
}

#[derive(Clone)]
pub struct StateDB {
    db: Arc<DB>,
}

impl StateDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open_cf(&opts, path, [UtxoKV::CF_NAME]).map_err(|_| Error::IO)?;

        Ok(Self { db: Arc::new(db) })
    }

    pub fn get_utxo(&self, tx: TxHash, output: OutputIndex) -> Result<Option<UtxoBody>, Error> {
        let dbval = UtxoKV::get_by_key(&self.db, DBSerde(UtxoRef(tx, output)))?;
        Ok(dbval.map(|x| x.0))
    }

    pub fn block_apply(self) -> ApplyBlockWriteBatch {
        ApplyBlockWriteBatch(self, WriteBatch::default())
    }

    pub fn destroy(path: impl AsRef<Path>) -> Result<(), Error> {
        DB::destroy(&Options::default(), path).map_err(|_| Error::IO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn with_tmp_db(op: fn(db: StateDB) -> ()) {
        let path = tempfile::tempdir().unwrap().into_path();
        let db = StateDB::open(path.clone()).unwrap();

        op(db);

        StateDB::destroy(path).unwrap();
    }

    fn dummy_utxo(tx: u64, idx: OutputIndex) -> (TxHash, OutputIndex, UtxoBody) {
        let hash = pallas::crypto::hash::Hasher::<256>::hash(tx.to_be_bytes().as_slice());
        (hash, idx, (tx + idx as u64).to_be_bytes().to_vec())
    }

    #[test]
    fn test_insert_utxos() {
        with_tmp_db(|db| {
            let mut apply = db.block_apply();

            let (tx1, idx, body1) = dummy_utxo(0, 0);
            apply.insert_utxo(tx1, idx, body1.clone());

            let (tx2, idx, body2) = dummy_utxo(0, 1);
            apply.insert_utxo(tx2, idx, body2.clone());

            let (tx3, idx, body3) = dummy_utxo(1, 0);
            apply.insert_utxo(tx3, idx, body3.clone());

            let db = apply.commit().unwrap();

            let persisted = db.get_utxo(tx1, 0).unwrap().unwrap();
            assert_eq!(persisted, body1);

            let persisted = db.get_utxo(tx2, 1).unwrap().unwrap();
            assert_eq!(persisted, body2);

            let persisted = db.get_utxo(tx3, 0).unwrap().unwrap();
            assert_eq!(persisted, body3);
        });
    }

    #[test]
    fn test_consume_utxos() {
        with_tmp_db(|db| {
            let mut apply = db.block_apply();

            let (tx1, idx, body1) = dummy_utxo(0, 0);
            apply.insert_utxo(tx1, idx, body1.clone());

            let (tx2, idx, body2) = dummy_utxo(0, 1);
            apply.insert_utxo(tx2, idx, body2.clone());

            let (tx3, idx, body3) = dummy_utxo(1, 0);
            apply.insert_utxo(tx3, idx, body3.clone());

            let db = apply.commit().unwrap();

            let mut apply = db.block_apply();

            apply.consume_utxo(tx2, 1);

            let db = apply.commit().unwrap();

            let persisted = db.get_utxo(tx1, 0).unwrap();
            assert!(persisted.is_some());

            let persisted = db.get_utxo(tx2, 1).unwrap();
            assert!(persisted.is_none());

            let persisted = db.get_utxo(tx3, 0).unwrap();
            assert!(persisted.is_some())
        });
    }
}
