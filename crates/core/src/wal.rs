use itertools::Itertools as _;
use std::collections::BTreeSet;

use super::*;

pub trait ReadUtils<'a> {
    fn filter_apply(self) -> impl Iterator<Item = LogEntry>;
    fn filter_forward(self) -> impl Iterator<Item = LogEntry>;
    fn into_blocks(self) -> impl Iterator<Item = Option<RawBlock>>;
}

impl<T> ReadUtils<'_> for T
where
    T: Iterator<Item = LogEntry> + Sized,
{
    fn filter_apply(self) -> impl Iterator<Item = LogEntry> {
        self.filter(|(_, x)| matches!(x, LogValue::Apply(..)))
    }

    fn filter_forward(self) -> impl Iterator<Item = LogEntry> {
        self.filter(|(_, x)| matches!(x, LogValue::Apply(..) | LogValue::Mark(..)))
    }

    fn into_blocks(self) -> impl Iterator<Item = Option<RawBlock>> {
        self.map(|(_, x)| match x {
            LogValue::Apply(x) => Some(x.clone()),
            LogValue::Undo(x) => Some(x.clone()),
            _ => None,
        })
    }
}

#[trait_variant::make(Send)]
pub trait WalStore: Clone + Send + Sync + 'static {
    type LogIterator<'a>: DoubleEndedIterator<Item = LogEntry> + Sized + Sync + Send;

    async fn tip_change(&self);

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError>;

    fn crawl_range<'a>(
        &self,
        start: LogSeq,
        end: LogSeq,
    ) -> Result<Self::LogIterator<'a>, WalError>;

    fn crawl_from<'a>(&self, start: Option<LogSeq>) -> Result<Self::LogIterator<'a>, WalError>;

    /// Tries to find the WAL sequence for a chain point
    fn locate_point(&self, point: &ChainPoint) -> Result<Option<LogSeq>, WalError>;

    /// Asserts that a chain point exists in the WAL and returns the sequence
    ///
    /// Similar to `locate_point` but it expects a point to be found or
    /// otherwise return a NotFound error.
    fn assert_point(&self, point: &ChainPoint) -> Result<LogSeq, WalError> {
        self.locate_point(point)?
            .ok_or(WalError::PointNotFound(point.clone()))
    }

    fn find_start(&self) -> Result<Option<(LogSeq, ChainPoint)>, WalError> {
        let start = self
            .crawl_from(None)?
            .filter_forward()
            .map(|(seq, log)| (seq, (&log).into()))
            .next();

        Ok(start)
    }

    fn find_tip(&self) -> Result<Option<(LogSeq, ChainPoint)>, WalError> {
        let tip = self
            .crawl_from(None)?
            .rev()
            .filter_forward()
            .map(|(seq, log)| (seq, (&log).into()))
            .next();

        Ok(tip)
    }

    fn intersect_candidates(&self, max_items: usize) -> Result<Vec<ChainPoint>, WalError> {
        let mut iter = self.crawl_from(None)?.rev().filter_forward();

        let mut out = Vec::with_capacity(max_items);

        // crawl the wal exponentially
        while let Some((_, log)) = iter.next() {
            out.push((&log).into());

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
    ) -> Result<Option<(LogSeq, ChainPoint)>, WalError> {
        for canidate in intersect {
            if let Some(seq) = self.locate_point(canidate)? {
                return Ok(Some((seq, canidate.clone())));
            }
        }

        Ok(None)
    }

    fn read_block_range<'a>(
        &'a self,
        from: &ChainPoint,
        to: &ChainPoint,
    ) -> Result<impl Iterator<Item = RawBlock> + 'a, WalError> {
        let from = self.assert_point(from)?;
        let to = self.assert_point(to)?;

        let iter = self
            .crawl_range(from, to)?
            .filter_apply()
            .into_blocks()
            .flatten();

        Ok(iter)
    }

    fn read_block_page(
        &self,
        from: Option<&ChainPoint>,
        limit: usize,
    ) -> Result<impl Iterator<Item = RawBlock> + '_, WalError> {
        let from = from.map(|p| self.assert_point(p)).transpose()?;

        let iter = self
            .crawl_from(from)?
            .filter_apply()
            .into_blocks()
            .flatten()
            .take(limit);

        Ok(iter)
    }

    fn read_block(&self, point: &ChainPoint) -> Result<RawBlock, WalError> {
        let seq = self.assert_point(point)?;

        let block = self
            .crawl_from(Some(seq))?
            .filter_apply()
            .into_blocks()
            .flatten()
            .next()
            .ok_or_else(|| WalError::PointNotFound(point.clone()))?;

        Ok(block)
    }

    fn read_sparse_blocks(&self, points: &[ChainPoint]) -> Result<Vec<RawBlock>, WalError> {
        points.iter().map(|p| self.read_block(p)).try_collect()
    }

    fn append_entries(&self, logs: impl Iterator<Item = LogValue>) -> Result<(), WalError>;

    fn roll_forward(&self, blocks: impl Iterator<Item = RawBlock>) -> Result<(), WalError> {
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

/// Iterator for raw blocks present in WAL.
///
/// Skips undone blocks that are present in the next "lookahead" items in the
/// WAL sequence.
pub struct WalBlockReader<'a, T>
where
    T: WalStore,
{
    undone: BTreeSet<ChainPoint>,
    start: <T as WalStore>::LogIterator<'a>,
    window: <T as WalStore>::LogIterator<'a>,
}

impl<T> WalBlockReader<'_, T>
where
    T: WalStore,
{
    /// Create a new iterator of raw blocks skipping rollbacks.
    ///
    /// Setting a eager lookahead value may lead to unwanted results. This is
    /// intended to be the amount of slots by which a block is considered to
    /// be inmutable.
    pub fn try_new(wal: &T, start: Option<LogSeq>, lookahead: u64) -> Result<Self, WalError> {
        let mut undone = BTreeSet::new();
        let mut iter = wal.crawl_from(start)?;
        for (_, value) in iter.by_ref() {
            match &value {
                LogValue::Undo(raw) => {
                    let slot_delta = start.map(|start| raw.slot - start).unwrap_or(raw.slot);
                    if slot_delta > lookahead {
                        break;
                    }
                    undone.insert(raw.into());
                }
                LogValue::Apply(raw) => {
                    let slot_delta = start.map(|start| raw.slot - start).unwrap_or(raw.slot);
                    if slot_delta > lookahead {
                        break;
                    }
                }
                LogValue::Mark(_) => {}
            }
        }

        Ok(Self {
            undone,
            start: wal.crawl_from(start)?,
            window: iter,
        })
    }
}

impl<T> Iterator for WalBlockReader<'_, T>
where
    T: WalStore,
{
    type Item = RawBlock;
    fn next(&mut self) -> Option<<Self as Iterator>::Item> {
        if let Some((_, LogValue::Undo(raw))) = &self.window.next() {
            self.undone.insert(raw.into());
        }

        for next in self.start.by_ref() {
            if let (_, LogValue::Apply(raw)) = next {
                let point = (&raw).into();
                if self.undone.first() == Some(&point) {
                    self.undone.pop_first();
                } else {
                    return Some(raw);
                }
            }
        }
        None
    }
}
