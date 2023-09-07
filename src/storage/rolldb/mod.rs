use pallas::crypto::hash::Hash;
use std::{path::Path, sync::Arc};

use rocksdb::{Options, WriteBatch, DB};

use self::wal::WalKV;

use super::kvtable::*;

pub mod stream;
pub mod wal;

type BlockSlot = u64;
type BlockHash = Hash<32>;
type BlockBody = Vec<u8>;

#[derive(Clone)]
pub struct RollDB {
    db: Arc<DB>,
    pub tip_change: Arc<tokio::sync::Notify>,
    wal_seq: u64,
    k_param: u64,
}

pub struct BlockKV;

impl KVTable<DBHash, DBBytes> for BlockKV {
    const CF_NAME: &'static str = "BlockKV";
}

// slot => block hash
pub struct ChainKV;

impl KVTable<DBInt, DBHash> for ChainKV {
    const CF_NAME: &'static str = "ChainKV";
}

impl RollDB {
    pub fn open(path: impl AsRef<Path>, k_param: u64) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db = DB::open_cf(
            &opts,
            path,
            [BlockKV::CF_NAME, ChainKV::CF_NAME, wal::WalKV::CF_NAME],
        )
        .map_err(|_| Error::IO)?;

        let wal_seq = wal::WalKV::last_key(&db)?.map(|x| x.0).unwrap_or_default();

        Ok(Self {
            db: Arc::new(db),
            tip_change: Arc::new(tokio::sync::Notify::new()),
            wal_seq,
            k_param,
        })
    }

    pub fn get_block(&self, hash: Hash<32>) -> Result<Option<BlockBody>, Error> {
        let dbval = BlockKV::get_by_key(&self.db, DBHash(hash))?;
        Ok(dbval.map(|x| x.0))
    }

    pub fn roll_forward(
        &mut self,
        slot: BlockSlot,
        hash: BlockHash,
        body: BlockBody,
    ) -> Result<(), Error> {
        let mut batch = WriteBatch::default();

        // keep track of the new block body
        BlockKV::stage_upsert(&self.db, DBHash(hash), DBBytes(body), &mut batch);

        // add new block to ChainKV
        ChainKV::stage_upsert(&self.db, DBInt(slot), DBHash(hash), &mut batch);

        // advance the WAL to the new point
        let new_seq =
            wal::WalKV::stage_roll_forward(&self.db, self.wal_seq, slot, hash, &mut batch)?;

        self.db.write(batch).map_err(|_| Error::IO)?;
        self.wal_seq = new_seq;
        self.tip_change.notify_waiters();

        Ok(())
    }

    pub fn roll_back(&mut self, until: BlockSlot) -> Result<(), Error> {
        let mut batch = WriteBatch::default();

        let (new_seq, removed_blocks) =
            wal::WalKV::stage_roll_back(&self.db, self.wal_seq, until, &mut batch)?;

        // remove rollbacked blocks from ChainKV
        for slot in removed_blocks {
            ChainKV::stage_delete(&self.db, DBInt(slot), &mut batch);
        }

        self.db.write(batch).map_err(|_| Error::IO)?;
        self.wal_seq = new_seq;
        self.tip_change.notify_waiters();

        Ok(())
    }

    pub fn find_tip(&self) -> Result<Option<(BlockSlot, BlockHash)>, Error> {
        // TODO: tip might be either on chain or WAL, we need to query both
        wal::WalKV::find_tip(&self.db)
    }

    pub fn intersect_options(
        &self,
        max_items: usize,
    ) -> Result<Vec<(BlockSlot, BlockHash)>, Error> {
        let mut iter = wal::WalKV::iter_values(&self.db, rocksdb::IteratorMode::End)
            .filter_map(|res| res.ok())
            .filter(|v| !v.is_undo());

        let mut out = Vec::with_capacity(max_items);

        // crawl the wal exponentially
        while let Some(val) = iter.next() {
            out.push((val.slot(), *val.hash()));

            if out.len() >= max_items {
                break;
            }

            // skip exponentially
            let skip = 2usize.pow(out.len() as u32) - 1;
            for _ in 0..skip {
                iter.next();
            }
        }

        // add one extra item from the immutable chain just in case
        let tip = WalKV::find_tip(&self.db)?;

        if let Some((tip_slot, _)) = tip {
            // -1 as we don't want to include the block on the boundary if it exists
            let immutable_before = Box::<[u8]>::from(DBInt(tip_slot - self.k_param - 1));
            let before =
                rocksdb::IteratorMode::From(&immutable_before, rocksdb::Direction::Reverse);
            if let Some((DBInt(slot), DBHash(hash))) =
                ChainKV::iter_entries(&self.db, before).next().transpose()?
            {
                out.push((slot, hash))
            }
        }

        Ok(out)
    }

    pub fn crawl_wal(
        &self,
        start_seq: Option<u64>,
    ) -> impl Iterator<Item = Result<(wal::Seq, wal::Value), Error>> + '_ {
        let iter = match start_seq {
            Some(start_seq) => {
                let start_seq = Box::<[u8]>::from(DBInt(start_seq));
                let from = rocksdb::IteratorMode::From(&start_seq, rocksdb::Direction::Forward);
                wal::WalKV::iter_entries(&self.db, from)
            }
            None => {
                let from = rocksdb::IteratorMode::Start;
                wal::WalKV::iter_entries(&self.db, from)
            }
        };

        iter.map(|v| v.map(|(seq, val)| (seq.0, val.0)))
    }

    pub fn crawl_wal_from_cursor(
        &self,
        start_after: Option<(BlockSlot, BlockHash)>,
    ) -> Result<impl Iterator<Item = Result<(wal::Seq, wal::Value), Error>> + '_, Error> {
        if let Some((slot, hash)) = start_after {
            // TODO: Not sure this is 100% accurate:
            // i.e Apply(X), Apply(cursor), Undo(cursor), Mark(x)
            // We want to start at Apply(cursor) or Mark(cursor), but even then,
            // what if we have more than one Apply(cursor), how do we know
            // which is correct?
            let found = WalKV::scan_until(&self.db, rocksdb::IteratorMode::End, |v| {
                v.slot() == slot && v.hash().eq(&hash)
            })?;

            match found {
                Some(DBInt(seq)) => Ok(self.crawl_wal(Some(seq))),
                None => Err(Error::NotFound),
            }
        } else {
            Ok(self.crawl_wal(None))
        }
    }

    pub fn crawl_chain(&self) -> impl Iterator<Item = Result<(BlockSlot, BlockHash), Error>> + '_ {
        ChainKV::iter_entries(&self.db, rocksdb::IteratorMode::Start)
            .map(|res| res.map(|(x, y)| (x.0, y.0)))
    }

    pub fn read_chain_page(
        &self,
        from: BlockSlot,
        len: usize,
    ) -> impl Iterator<Item = Result<(BlockSlot, BlockHash), Error>> + '_ {
        ChainKV::iter_entries_from(&self.db, DBInt(from))
            .map(|res| res.map(|(x, y)| (x.0, y.0)))
            .take(len)
    }

    /// Prune the WAL of entries with slot values over `k_param` from the tip
    pub fn prune(&self) -> Result<(), Error> {
        let tip = wal::WalKV::find_tip(&self.db)?
            .map(|(slot, _)| slot)
            .unwrap_or_default();

        let mut iter = wal::WalKV::iter_entries(&self.db, rocksdb::IteratorMode::Start);

        let mut batch = WriteBatch::default();

        while let Some(Ok((wal_key, value))) = iter.next() {
            let slot_delta = tip - value.slot();

            if slot_delta <= self.k_param {
                break;
            } else {
                wal::WalKV::stage_delete(&self.db, wal_key, &mut batch);
            }
        }

        if !batch.is_empty() {
            self.db.write(batch).map_err(|_| Error::IO)?;
        }

        Ok(())
    }

    pub fn destroy(path: impl AsRef<Path>) -> Result<(), Error> {
        DB::destroy(&Options::default(), path).map_err(|_| Error::IO)
    }
}

#[cfg(test)]
mod tests {
    use super::{BlockBody, BlockHash, BlockSlot, RollDB};

    fn with_tmp_db<T>(k_param: u64, op: fn(db: RollDB) -> T) {
        let path = tempfile::tempdir().unwrap().into_path();
        let db = RollDB::open(path.clone(), k_param).unwrap();

        op(db);

        RollDB::destroy(path).unwrap();
    }

    fn dummy_block(slot: u64) -> (BlockSlot, BlockHash, BlockBody) {
        let hash = pallas::crypto::hash::Hasher::<256>::hash(slot.to_be_bytes().as_slice());
        (slot, hash, slot.to_be_bytes().to_vec())
    }

    #[test]
    fn test_roll_forward_blackbox() {
        with_tmp_db(30, |mut db| {
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
        with_tmp_db(30, |mut db| {
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

    #[test]
    fn test_prune_linear() {
        with_tmp_db(30, |mut db| {
            for i in 0..100 {
                let (slot, hash, body) = dummy_block(i * 10);
                db.roll_forward(slot, hash, body).unwrap();
            }

            db.prune().unwrap();

            let mut chain = db.crawl_chain();

            for i in 0..100 {
                let (slot, _) = chain.next().unwrap().unwrap();
                assert_eq!(i * 10, slot)
            }

            assert!(chain.next().is_none());

            let mut wal = db.crawl_wal(None);

            for i in 96..100 {
                let (_, val) = wal.next().unwrap().unwrap();
                assert_eq!(val.slot(), i * 10);
            }

            assert!(wal.next().is_none());
        });
    }

    #[test]
    fn test_prune_with_rollback() {
        with_tmp_db(30, |mut db| {
            for i in 0..100 {
                let (slot, hash, body) = dummy_block(i * 10);
                db.roll_forward(slot, hash, body).unwrap();
            }

            db.roll_back(800).unwrap();

            // tip is 800 (Mark)

            db.prune().unwrap();

            let mut chain = db.crawl_chain();

            for i in 0..=80 {
                let (slot, _) = chain.next().unwrap().unwrap();
                assert_eq!(i * 10, slot)
            }

            assert!(chain.next().is_none());

            let mut wal = db.crawl_wal(None);

            for i in 77..100 {
                let (_, val) = wal.next().unwrap().unwrap();
                assert!(val.is_apply());
                assert_eq!(val.slot(), i * 10);
            }

            for i in (81..100).rev() {
                let (_, val) = wal.next().unwrap().unwrap();
                assert!(val.is_undo());
                assert_eq!(val.slot(), i * 10);
            }

            let (_, val) = wal.next().unwrap().unwrap();
            assert!(val.is_mark());
            assert_eq!(val.slot(), 800);

            assert!(wal.next().is_none());
        });
    }

    #[test]
    fn test_chain_page() {
        with_tmp_db(30, |mut db| {
            for i in 0..100 {
                let (slot, hash, body) = dummy_block(i * 10);
                db.roll_forward(slot, hash, body).unwrap();
            }

            db.prune().unwrap();

            let mut chain = db.read_chain_page(200, 15);

            for i in 0..15 {
                let (slot, _) = chain.next().unwrap().unwrap();
                assert_eq!(200 + (i * 10), slot)
            }

            assert!(chain.next().is_none());
        });
    }

    #[test]
    fn test_intersect_options() {
        with_tmp_db(1000, |mut db| {
            for i in 0..200 {
                let (slot, hash, body) = dummy_block(i * 10);
                db.roll_forward(slot, hash, body).unwrap();
            }

            db.prune().unwrap();

            let intersect = db.intersect_options(10).unwrap();

            let expected = vec![1990, 1970, 1930, 1850, 1690, 1370, 980];

            for (out, exp) in intersect.iter().zip(expected) {
                assert_eq!(out.0, exp);
            }
        });
    }
}
