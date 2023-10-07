use pallas::crypto::hash::Hash;
use std::{path::Path, sync::Arc};
use tracing::warn;

use self::wal::WalKV;
use rocksdb::{Options, WriteBatch, DB};

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

        let wal_seq = wal::WalKV::initialize(&db)?;

        let out = Self {
            db: Arc::new(db),
            tip_change: Arc::new(tokio::sync::Notify::new()),
            wal_seq,
            k_param,
        };

        Ok(out)
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

        let new_seq = wal::WalKV::stage_roll_back(&self.db, self.wal_seq, until, &mut batch)?;

        // remove rollback-ed blocks from ChainKV
        let to_remove = ChainKV::iter_keys_from(&self.db, DBInt(until)).skip(1);

        for key in to_remove {
            ChainKV::stage_delete(&self.db, key?, &mut batch);
        }

        self.db.write(batch).map_err(|_| Error::IO)?;
        self.wal_seq = new_seq;
        self.tip_change.notify_waiters();

        Ok(())
    }

    pub fn roll_back_origin(&mut self) -> Result<(), Error> {
        let mut batch = WriteBatch::default();

        let new_seq = wal::WalKV::stage_roll_back_origin(&self.db, self.wal_seq, &mut batch)?;

        ChainKV::reset(&self.db)?;

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
            if tip_slot > self.k_param {
                // fetch first entry in ChainKV with slot lower than (tip slot - k param)
                let immutable_before = Box::<[u8]>::from(DBInt(tip_slot - self.k_param - 1));

                let before =
                    rocksdb::IteratorMode::From(&immutable_before, rocksdb::Direction::Reverse);

                if let Some((DBInt(slot), DBHash(hash))) =
                    ChainKV::iter_entries(&self.db, before).next().transpose()?
                {
                    out.push((slot, hash))
                }
            }
        }

        Ok(out)
    }

    pub fn crawl_wal_after(&self, seq: Option<u64>) -> wal::WalIterator {
        if let Some(seq) = seq {
            let seq = Box::<[u8]>::from(DBInt(seq));
            let from = rocksdb::IteratorMode::From(&seq, rocksdb::Direction::Forward);
            let mut iter = wal::WalKV::iter_entries(&self.db, from);

            // skip current
            iter.next();

            wal::WalIterator(iter)
        } else {
            let from = rocksdb::IteratorMode::Start;
            let iter = wal::WalKV::iter_entries(&self.db, from);
            wal::WalIterator(iter)
        }
    }

    pub fn find_wal_seq(&self, block: Option<(BlockSlot, BlockHash)>) -> Result<wal::Seq, Error> {
        if block.is_none() {
            return Ok(0);
        }

        let (slot, hash) = block.unwrap();

        // TODO: Not sure this is 100% accurate:
        // i.e Apply(X), Apply(cursor), Undo(cursor), Mark(x)
        // We want to start at Apply(cursor) or Mark(cursor), but even then,
        // what if we have more than one Apply(cursor), how do we know
        // which is correct?
        let found = WalKV::scan_until(&self.db, rocksdb::IteratorMode::End, |v| {
            v.slot() == slot && v.hash().eq(&hash) && v.is_apply()
        })?;

        match found {
            Some(DBInt(seq)) => Ok(seq),
            None => Err(Error::NotFound),
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

    /// Iterator over chain between two points (inclusive)
    ///
    /// To use Origin as start point set `from` to None.
    ///
    /// Returns None if either point in range don't exist or `to` point is
    /// earlier in chain than `from`.
    pub fn read_chain_range(
        &self,
        from: Option<(BlockSlot, BlockHash)>,
        to: (BlockSlot, BlockHash),
    ) -> Result<Option<impl Iterator<Item = Result<(BlockSlot, BlockHash), Error>> + '_>, Error>
    {
        // TODO: We want to use a snapshot here to avoid race condition where
        // point is checked to be in the ChainKV but it is rolled-back before we
        // create the iterator. Problem is `ChainKV` etc must take `DB`, not
        // `Snapshot<DB>`, so maybe we need a new way of creating something like
        // a "KVTableSnapshot" in addition to the current "KVTable" type, which
        // has methods on snapshots, but here I was having issues as there is
        // no `cf` method on Snapshot but it is used is KVTable.

        // let snapshot = self.db.snapshot();

        // check p2 not before p1
        let p1_slot = if let Some((slot, _)) = from {
            if to.0 < slot {
                warn!("chain range end slot before start slot");
                return Ok(None);
            } else {
                slot
            }
        } else {
            0 // Use 0 as slot for Origin
        };

        // check p1 exists in ChainKV if provided
        if let Some((slot, hash)) = from {
            match ChainKV::get_by_key(&self.db, DBInt(slot))? {
                Some(DBHash(found_hash)) => {
                    if hash != found_hash {
                        warn!("chain range start hash mismatch");
                        return Ok(None);
                    }
                }
                None => {
                    warn!("chain range start slot not found");
                    return Ok(None);
                }
            }
        }

        // check p2 exists in ChainKV
        match ChainKV::get_by_key(&self.db, DBInt(to.0))? {
            Some(DBHash(found_hash)) => {
                if to.1 != found_hash {
                    warn!("chain range end hash mismatch");
                    return Ok(None);
                }
            }
            None => {
                warn!("chain range end slot not found");
                return Ok(None);
            }
        };

        // return iterator between p1 and p2 inclusive
        Ok(Some(
            ChainKV::iter_entries_from(&self.db, DBInt(p1_slot))
                .map(|res| res.map(|(x, y)| (x.0, y.0)))
                .take_while(move |x| {
                    if let Ok((slot, _)) = x {
                        // iter returns None once point is after `to` slot
                        *slot <= to.0
                    } else {
                        false
                    }
                }),
        ))
    }

    /// Prune the WAL of entries with slot values over `k_param` from the tip
    pub fn prune_wal(&self) -> Result<(), Error> {
        let tip = wal::WalKV::find_tip(&self.db)?
            .map(|(slot, _)| slot)
            .unwrap_or_default();

        // iterate through all values in Wal from start
        let mut iter = wal::WalKV::iter_entries(&self.db, rocksdb::IteratorMode::Start);

        let mut batch = WriteBatch::default();

        while let Some(Ok((wal_key, value))) = iter.next() {
            // get the number of slots that have passed since the wal point
            let slot_delta = tip - value.slot();

            if slot_delta <= self.k_param {
                break;
            } else {
                wal::WalKV::stage_delete(&self.db, wal_key, &mut batch);
            }
        }

        self.db.write(batch).map_err(|_| Error::IO)?;

        Ok(())
    }

    /// Check if a point (pair of slot and block hash) exists in the ChainKV
    pub fn chain_contains(&self, slot: BlockSlot, hash: &BlockHash) -> Result<bool, Error> {
        if let Some(DBHash(found)) = ChainKV::get_by_key(&self.db, DBInt(slot))? {
            if found == *hash {
                return Ok(true);
            }
        }

        Ok(false)
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
    fn test_origin_event() {
        with_tmp_db(30, |db| {
            let mut iter = db.crawl_wal_after(None);

            let origin = iter.next();
            assert!(origin.is_some());

            let origin = origin.unwrap();
            assert!(origin.is_ok());

            let (seq, value) = origin.unwrap();
            assert_eq!(seq, 0);
            assert!(value.is_origin());
        });
    }

    #[test]
    fn test_roll_forward_blackbox() {
        with_tmp_db(30, |mut db| {
            let (slot, hash, body) = dummy_block(11);
            db.roll_forward(slot, hash, body.clone()).unwrap();

            // ensure block body is persisted
            let persisted = db.get_block(hash).unwrap().unwrap();
            assert_eq!(persisted, body);

            // ensure tip matches
            let (tip_slot, tip_hash) = db.find_tip().unwrap().unwrap();
            assert_eq!(tip_slot, slot);
            assert_eq!(tip_hash, hash);

            // ensure chain has item
            let (chain_slot, chain_hash) = db.crawl_chain().next().unwrap().unwrap();
            assert_eq!(chain_slot, slot);
            assert_eq!(chain_hash, hash);
        });
    }

    #[test]
    fn test_roll_back_blackbox() {
        with_tmp_db(30, |mut db| {
            for i in 0..=5 {
                let (slot, hash, body) = dummy_block(i * 10);
                db.roll_forward(slot, hash, body).unwrap();
            }

            db.roll_back(20).unwrap();

            // ensure tip show rollback point
            let (tip_slot, _) = db.find_tip().unwrap().unwrap();
            assert_eq!(tip_slot, 20);

            // ensure chain has items not rolled back
            let mut chain = db.crawl_chain();

            for i in 0..=2 {
                let (slot, _) = chain.next().unwrap().unwrap();
                assert_eq!(slot, i * 10);
            }

            // ensure chain stops here
            assert!(chain.next().is_none());
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

            db.prune_wal().unwrap();

            let mut chain = db.crawl_chain();

            for i in 0..100 {
                let (slot, _) = chain.next().unwrap().unwrap();
                assert_eq!(i * 10, slot)
            }

            assert!(chain.next().is_none());

            let mut wal = db.crawl_wal_after(None);

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

            db.prune_wal().unwrap();

            let mut chain = db.crawl_chain();

            for i in 0..=80 {
                let (slot, _) = chain.next().unwrap().unwrap();
                assert_eq!(i * 10, slot)
            }

            assert!(chain.next().is_none());

            let mut wal = db.crawl_wal_after(None);

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

            db.prune_wal().unwrap();

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

            db.prune_wal().unwrap();

            let intersect = db.intersect_options(10).unwrap();

            let expected = vec![1990, 1970, 1930, 1850, 1690, 1370, 980];

            for (out, exp) in intersect.iter().zip(expected) {
                assert_eq!(out.0, exp);
            }
        });
    }
}
