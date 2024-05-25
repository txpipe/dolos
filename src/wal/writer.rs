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

    #[test]
    fn test_origin_event() {
        let db = testing::empty_db();

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
        let mut db = testing::empty_db();

        let expected_block = testing::dummy_block_from_slot(11);
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
        let mut db = testing::empty_db();

        let forward = (0..=5).map(|x| testing::dummy_block_from_slot(x * 10));
        db.roll_forward(forward).unwrap();

        let rollback_to = ChainPoint::Specific(20, testing::slot_to_hash(20));
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
