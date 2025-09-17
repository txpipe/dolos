use itertools::Itertools as _;

use super::*;

#[trait_variant::make(Send)]
pub trait WalStore: Clone + Send + Sync + 'static {
    type Delta: EntityDelta;
    type LogIterator<'a>: DoubleEndedIterator<Item = LogEntry<Self::Delta>> + Sized + Sync + Send;
    type BlockIterator<'a>: DoubleEndedIterator<Item = (ChainPoint, RawBlock)> + Sized + Sync + Send;

    fn reset_to(&self, point: &ChainPoint) -> Result<(), WalError>;

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), WalError>;

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError>;

    fn locate_point(&self, around: BlockSlot) -> Result<Option<ChainPoint>, WalError>;

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<Self::Delta>>, WalError>;

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::LogIterator<'a>, WalError>;

    fn iter_blocks<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::BlockIterator<'a>, WalError>;

    fn read_sparse(
        &self,
        points: &[ChainPoint],
    ) -> Result<Vec<Option<LogValue<Self::Delta>>>, WalError> {
        points.iter().map(|p| self.read_entry(p)).try_collect()
    }

    fn append_entries(&self, logs: Vec<LogEntry<Self::Delta>>) -> Result<(), WalError>;

    fn remove_entries(&mut self, after: &ChainPoint) -> Result<(), WalError>;

    fn contains_point(&self, point: &ChainPoint) -> Result<bool, WalError> {
        let entry = self.read_entry(point)?;
        Ok(entry.is_some())
    }

    /// Asserts that a chain point exists in the WAL and returns the sequence
    ///
    /// Similar to `locate_point` but it expects a point to be found or
    /// otherwise return a NotFound error.
    fn assert_point(&self, point: &ChainPoint) -> Result<(), WalError> {
        let contains = self.contains_point(point)?;

        if !contains {
            return Err(WalError::PointNotFound(point.clone()));
        }

        Ok(())
    }

    fn iter_all<'a>(&'a self) -> Result<Self::LogIterator<'a>, WalError> {
        self.iter_logs(None, None)
    }

    fn find_start(&self) -> Result<Option<LogEntry<Self::Delta>>, WalError> {
        let start = self.iter_all()?.next();

        Ok(start)
    }

    fn find_tip(&self) -> Result<Option<LogEntry<Self::Delta>>, WalError> {
        let tip = self.iter_all()?.next_back();

        Ok(tip)
    }

    fn intersect_candidates(&self, max_items: usize) -> Result<Vec<ChainPoint>, WalError> {
        let mut iter = self.iter_all()?.rev();

        let mut out = Vec::with_capacity(max_items);

        // crawl the wal exponentially
        while let Some((point, _)) = iter.next() {
            out.push(point);

            if out.len() >= max_items {
                break;
            }

            // skip exponentially
            let skip = 2usize.pow(out.len() as u32) - 1;
            for _ in 0..skip {
                iter.next();
            }
        }

        Ok(out)
    }

    fn find_intersect(
        &self,
        intersect: &[ChainPoint],
    ) -> Result<Option<LogEntry<Self::Delta>>, WalError> {
        for candidate in intersect {
            if let Some(entry) = self.read_entry(candidate)? {
                return Ok(Some((candidate.clone(), entry)));
            }
        }

        Ok(None)
    }
}
