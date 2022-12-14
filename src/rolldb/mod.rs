mod blocks;
mod wal;
use pallas::crypto::hash::Hash;
use std::path::Path;
use thiserror::Error;

use rocksdb::{Options, WriteBatch, DB};

#[derive(Error, Debug)]
pub enum Error {
    #[error("IO error")]
    IO,

    #[error("serde error")]
    Serde,
}

const CHAIN_CF: &str = "chain";

type BlockSlot = u64;
type BlockHash = Hash<32>;
type BlockBody = Vec<u8>;

type RawKV = (Box<[u8]>, Box<[u8]>);

pub struct RollDB {
    db: DB,
    wal_seq: u64,
}

impl RollDB {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open_cf(&opts, path, [blocks::CF_NAME, CHAIN_CF, wal::CF_NAME])
            .map_err(|_| Error::IO)?;

        let wal_seq = wal::find_latest_seq(&db)?.into();

        Ok(Self { db, wal_seq })
    }

    pub fn get_block(&mut self, hash: Hash<32>) -> Result<Option<BlockBody>, Error> {
        blocks::get_body(&self.db, hash)
    }

    pub fn roll_forward(
        &mut self,
        slot: BlockSlot,
        hash: BlockHash,
        body: BlockBody,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();

        // keep track of the new block body
        blocks::stage_upsert(&self.db, hash, body, &mut batch)?;

        // advance the WAL to the new point
        let new_seq = wal::stage_roll_forward(&self.db, self.wal_seq, slot, hash, &mut batch)?;

        self.db.write(batch).map_err(|_| Error::IO)?;
        self.wal_seq = new_seq;

        Ok(())
    }

    pub fn roll_back(&mut self, until: BlockSlot) -> Result<(), Error> {
        let mut batch = WriteBatch::default();

        let new_seq = wal::stage_roll_back(&self.db, self.wal_seq, until, &mut batch)?;

        self.db.write(batch).map_err(|_| Error::IO)?;
        self.wal_seq = new_seq;

        Ok(())
    }

    pub fn find_tip(&self) -> Result<Option<(BlockSlot, BlockHash)>, Error> {
        // TODO: tip might be either on chain or WAL, we need to query both
        wal::find_tip(&self.db)
    }

    pub fn crawl_wal(&self) -> wal::CrawlIterator {
        wal::crawl_forward(&self.db)
    }

    pub fn destroy(path: impl AsRef<Path>) -> Result<(), Error> {
        DB::destroy(&Options::default(), path).map_err(|_| Error::IO)
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockBody, BlockHash, BlockSlot, RollDB};

    fn with_tmp_db(op: fn(db: RollDB) -> ()) {
        let path = tempfile::tempdir().unwrap().into_path();
        let db = RollDB::open(path.clone()).unwrap();

        op(db);

        RollDB::destroy(path).unwrap();
    }

    fn dummy_block(slot: u64) -> (BlockSlot, BlockHash, BlockBody) {
        let hash = pallas::crypto::hash::Hasher::<256>::hash(slot.to_be_bytes().as_slice());
        (slot, hash, slot.to_be_bytes().to_vec())
    }

    #[test]
    fn test_roll_forward_blackbox() {
        with_tmp_db(|mut db| {
            let (slot, hash, body) = dummy_block(11);
            db.roll_forward(slot, hash, body.clone()).unwrap();

            let persisted = db.get_block(hash).unwrap().unwrap();
            assert_eq!(persisted, body);

            let (tip_slot, tip_hash) = db.find_tip().unwrap().unwrap();
            assert_eq!(tip_slot, slot);
            assert_eq!(tip_hash, hash);
        });
    }

    #[test]
    fn test_roll_back_blackbox() {
        with_tmp_db(|mut db| {
            for i in 0..5 {
                let (slot, hash, body) = dummy_block(i * 10);
                db.roll_forward(slot, hash, body).unwrap();
            }

            db.roll_back(20).unwrap();

            let (tip_slot, _) = db.find_tip().unwrap().unwrap();
            assert_eq!(tip_slot, 20);
        });
    }

    //TODO: test rollback beyond K
    //TODO: test rollback with unknown slot
}
