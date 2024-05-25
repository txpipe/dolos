use super::reader::ReadUtils;
use super::*;

pub trait WalWriter: WalReader {
    fn append_entries(&mut self, logs: impl Iterator<Item = LogValue>) -> Result<(), WalError>;

    fn roll_forward(&mut self, blocks: impl Iterator<Item = RawBlock>) -> Result<(), WalError> {
        self.append_entries(blocks.map(LogValue::Apply))
    }

    fn roll_back(&mut self, until: &ChainPoint) -> Result<(), WalError> {
        let seq = self.assert_point(until)?;

        // find all of the "apply" event in the wall and gather the contained block
        // data.
        let applies: Vec<_> = self
            .crawl_from(Some(seq))?
            .rev()
            .filter_apply()
            .into_blocks()
            .flatten()
            .collect();

        // take all of the applies, except the last one, and turn them into undo.
        let undos: Vec<_> = applies
            .into_iter()
            .filter(|x| !ChainPoint::from(x).eq(until))
            .map(LogValue::Undo)
            .collect();

        // the last one (which is the point the chain is at) is turned into a mark.
        let mark = std::iter::once(LogValue::Mark(until.clone()));

        self.append_entries(undos.into_iter().chain(mark))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn setup_ephemeral_db() -> redb::WalStore {
        super::redb::WalStore::memory().unwrap()
    }

    const DUMMY_BLOCK_BYTES: &str = "820183851a2d964a09582089d9b5a5b8ddc8d7e5a6795e9774d97faf1efea59b2caf7eaf9f8c5b32059df484830058200e5751c026e543b2e8ab2eb06099daa1d1e5df47778f7787faab45cdf12fe3a85820afc0da64183bf2664f3d4eec7238d524ba607faeeab24fc100eb861dba69971b8300582025777aca9e4a73d48fc73b4f961d345b06d4a6f349cb7916570d35537d53479f5820d36a2619a672494604e11bb447cbcf5231e9f2ba25c2169177edc941bd50ad6c5820afc0da64183bf2664f3d4eec7238d524ba607faeeab24fc100eb861dba69971b58204e66280cd94d591072349bec0a3090a53aa945562efb6d08d56e53654b0e40988482000058401bc97a2fe02c297880ce8ecfd997fe4c1ec09ee10feeee9f686760166b05281d6283468ffd93becb0c956ccddd642df9b1244c915911185fa49355f6f22bfab98101820282840058401bc97a2fe02c297880ce8ecfd997fe4c1ec09ee10feeee9f686760166b05281d6283468ffd93becb0c956ccddd642df9b1244c915911185fa49355f6f22bfab9584061261a95b7613ee6bf2067dad77b70349729b0c50d57bc1cf30de0db4a1e73a885d0054af7c23fc6c37919dba41c602a57e2d0f9329a7954b867338d6fb2c9455840e03e62f083df5576360e60a32e22bbb07b3c8df4fcab8079f1d6f61af3954d242ba8a06516c395939f24096f3df14e103a7d9c2b80a68a9363cf1f27c7a4e307584044f18ef23db7d2813415cb1b62e8f3ead497f238edf46bb7a97fd8e9105ed9775e8421d18d47e05a2f602b700d932c181e8007bbfb231d6f1a050da4ebeeba048483000000826a63617264616e6f2d736c00a058204ba92aa320c60acc9ad7b9a64f2eda55c4d2ec28e604faf186708b4f0c4e8edf849fff8300d9010280d90102809fff82809fff81a0";

    fn slot_to_hash(slot: u64) -> BlockHash {
        let mut hasher = pallas::crypto::hash::Hasher::<256>::new();
        hasher.input(&(slot as i32).to_le_bytes());
        hasher.finalize()
    }

    fn dummy_block_from_slot(slot: u64) -> RawBlock {
        let bytes = &hex::decode(DUMMY_BLOCK_BYTES).unwrap();
        let block = pallas::ledger::traverse::MultiEraBlock::decode(&bytes).unwrap();

        RawBlock {
            slot,
            hash: slot_to_hash(slot),
            era: block.era(),
            body: hex::decode(DUMMY_BLOCK_BYTES).unwrap(),
        }
    }

    #[test]
    fn test_origin_event() {
        let db = setup_ephemeral_db();

        let mut iter = db.crawl_from(None).unwrap();

        let origin = iter.next();
        assert!(origin.is_some());

        let (seq, value) = origin.unwrap();
        assert_eq!(seq, 0);
        assert!(matches!(value, LogValue::Mark(ChainPoint::Origin)));

        // ensure nothing else
        let origin = iter.next();
        assert!(origin.is_none());
    }

    #[test]
    fn test_basic_append() {
        let mut db = setup_ephemeral_db();

        let expected_block = dummy_block_from_slot(11);
        let expected_point = ChainPoint::Specific(11, expected_block.hash);

        db.roll_forward(std::iter::once(expected_block.clone()))
            .unwrap();

        // ensure tip matches
        let (seq, point) = db.find_tip().unwrap().unwrap();
        assert_eq!(seq, 1);
        assert_eq!(point, expected_point);

        // ensure point can be located
        let seq = db.locate_point(&expected_point).unwrap().unwrap();
        assert_eq!(seq, 1);

        // ensure chain has item
        let mut iter = db.crawl_from(None).unwrap();

        iter.next(); // origin

        let (seq, log) = iter.next().unwrap();
        assert_eq!(seq, 1);
        assert_eq!(log, LogValue::Apply(expected_block));

        // ensure nothing else
        let origin = iter.next();
        assert!(origin.is_none());
    }

    #[test]
    fn test_rollback_undos() {
        let mut db = setup_ephemeral_db();

        let forward = (0..=5).map(|x| dummy_block_from_slot(x * 10));
        db.roll_forward(forward).unwrap();

        let rollback_to = ChainPoint::Specific(20, slot_to_hash(20));
        db.roll_back(&rollback_to).unwrap();

        // ensure tip show rollback point
        let (_, tip_point) = db.find_tip().unwrap().unwrap();
        assert_eq!(tip_point, rollback_to);

        // after the previous actions, we should get the following sequence
        // Origin => Apply(0) => Apply(10) => Apply(20) => Apply(30) => Apply(40) =>
        // Apply(50) => Undo(50) => Undo(40) => Undo(30) => Mark(20)

        // ensure wal has correct sequence of events
        let mut wal = db.crawl_from(None).unwrap();

        let (seq, log) = wal.next().unwrap();
        assert_eq!(log, LogValue::Mark(ChainPoint::Origin));
        println!("{seq}");

        for i in 0..=5 {
            let (seq, log) = wal.next().unwrap();
            println!("{seq}");

            match log {
                LogValue::Apply(RawBlock { slot, .. }) => assert_eq!(slot, i * 10),
                _ => panic!("expected apply"),
            }
        }

        for i in (3..=5).rev() {
            let (seq, log) = wal.next().unwrap();
            println!("{seq}");

            match log {
                LogValue::Undo(RawBlock { slot, .. }) => assert_eq!(slot, i * 10),
                _ => panic!("expected undo"),
            }
        }

        let (seq, log) = wal.next().unwrap();
        assert_eq!(log, LogValue::Mark(rollback_to));
        println!("{seq}");

        // ensure chain stops here
        assert!(wal.next().is_none());
    }
}
