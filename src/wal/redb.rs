use bincode;
use itertools::Itertools;
use redb::{Range, ReadableTable, TableDefinition};
use std::{path::Path, sync::Arc};
use tracing::{debug, info, warn};

use super::{
    BlockSlot, ChainPoint, LogEntry, LogSeq, LogValue, RawBlock, WalError, WalReader, WalWriter,
};

impl redb::Value for LogValue {
    type SelfType<'a> = Self;
    type AsBytes<'a>
        = Vec<u8>
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self
    where
        Self: 'a,
    {
        bincode::deserialize(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        bincode::serialize(value).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("logvalue")
    }
}

pub type AugmentedBlockSlot = i128;

const WAL: TableDefinition<LogSeq, LogValue> = TableDefinition::new("wal");
const POS: TableDefinition<AugmentedBlockSlot, LogSeq> = TableDefinition::new("pos");

fn point_to_augmented_slot(point: &ChainPoint) -> AugmentedBlockSlot {
    match point {
        ChainPoint::Origin => -1i128,
        ChainPoint::Specific(x, _) => *x as i128,
    }
}

pub struct WalIter<'a>(Range<'a, LogSeq, LogValue>);

impl<'a> Iterator for WalIter<'a> {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl<'a> DoubleEndedIterator for WalIter<'a> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
    }
}

impl<T> From<T> for WalError
where
    T: Into<redb::Error>,
{
    fn from(value: T) -> Self {
        WalError::IO(value.into().into())
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 50;

/// Concrete implementation of WalStore using Redb
#[derive(Clone)]
pub struct WalStore {
    db: Arc<redb::Database>,
    max_slots: Option<u64>,
    tip_change: Arc<tokio::sync::Notify>,
}

impl WalStore {
    pub fn is_empty(&self) -> Result<bool, WalError> {
        let wr = self.db.begin_read()?;

        if wr.list_tables()?.count() == 0 {
            return Ok(true);
        }

        let start = self.find_tip()?;

        if let Some((start, _)) = start {
            if start == 0 {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn initialize_from_origin(&mut self) -> Result<(), WalError> {
        if !self.is_empty()? {
            return Err(WalError::NotEmpty);
        }

        info!("initializing wal");
        self.append_entries(std::iter::once(LogValue::Mark(ChainPoint::Origin)))?;

        Ok(())
    }

    pub fn memory(max_slots: Option<u64>) -> Result<Self, WalError> {
        let db =
            redb::Database::builder().create_with_backend(redb::backends::InMemoryBackend::new())?;

        let out = Self {
            db: Arc::new(db),
            tip_change: Arc::new(tokio::sync::Notify::new()),
            max_slots,
        };

        Ok(out)
    }

    pub fn open(
        path: impl AsRef<Path>,
        cache_size: Option<usize>,
        max_slots: Option<u64>,
    ) -> Result<Self, WalError> {
        let inner = redb::Database::builder()
            .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "wal db is repairing"))
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        let out = Self {
            db: Arc::new(inner),
            tip_change: Arc::new(tokio::sync::Notify::new()),
            max_slots,
        };

        Ok(out)
    }

    pub fn db_mut(&mut self) -> Option<&mut redb::Database> {
        Arc::get_mut(&mut self.db)
    }

    // TODO: see how to expose this method through the official write interface
    // TODO: improve performance, this approach is immensely inefficient
    pub fn remove_range(
        &mut self,
        from: Option<LogSeq>,
        to: Option<LogSeq>,
    ) -> Result<(), WalError> {
        let wx = self.db.begin_write()?;
        {
            let mut wal = wx.open_table(WAL)?;

            wal.extract_if(|seq, _| match (from, to) {
                (None, None) => true,
                (Some(a), Some(b)) => seq >= a && seq <= b,
                (None, Some(x)) => seq <= x,
                (Some(x), None) => seq >= x,
            })?
            .collect_vec();
        }

        {
            let mut pos = wx.open_table(POS)?;

            pos.extract_if(|_, seq| match (from, to) {
                (None, None) => true,
                (Some(a), Some(b)) => seq >= a && seq <= b,
                (None, Some(x)) => seq <= x,
                (Some(x), None) => seq >= x,
            })?
            .collect_vec();
        }

        wx.commit()?;

        Ok(())
    }

    const MAX_PRUNE_SLOTS_PER_PASS: u64 = 10_000;

    /// Prunes the WAL history to maintain a maximum number of slots.
    ///
    /// This method attempts to remove older entries from the WAL to keep the
    /// total number of slots within the specified `max_slots` limit. It
    /// operates as follows:
    ///
    /// 1. Determines the start and last slots in the WAL.
    /// 2. Calculates the number of slots that exceed the `max_slots` limit.
    /// 3. If pruning is necessary, it removes entries older than a calculated
    ///    cutoff slot.
    /// 4. Pruning is limited to a maximum of `MAX_PRUNE_SLOTS_PER_PASS` slots
    ///    per invocation to avoid long-running operations.
    ///
    /// # Arguments
    ///
    /// * `max_slots` - The maximum number of slots to retain in the WAL.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation was successful, or a `WalError` if an
    /// error occurred.
    ///
    /// # Notes
    ///
    /// - If the WAL doesn't exceed the `max_slots` limit, no pruning occurs.
    /// - This method is typically called periodically as part of housekeeping
    ///   operations.
    /// - The actual number of slots pruned may be less than the calculated
    ///   excess to avoid long-running operations.
    pub fn prune_history(&mut self, max_slots: u64) -> Result<(), WalError> {
        let start_slot = match self.find_start()? {
            Some((_, ChainPoint::Origin)) => 0,
            Some((_, ChainPoint::Specific(slot, _))) => slot,
            _ => {
                debug!("no start point found, skipping housekeeping");
                return Ok(());
            }
        };

        let last_slot = match self.find_tip()? {
            Some((_, ChainPoint::Specific(slot, _))) => slot,
            _ => {
                debug!("no tip found, skipping housekeeping");
                return Ok(());
            }
        };

        let delta = last_slot - start_slot - max_slots;

        debug!(delta, last_slot, start_slot, "wal history delta computed");

        if delta <= max_slots {
            debug!(delta, max_slots, "no pruning necessary");
            return Ok(());
        }

        let max_prune = core::cmp::min(delta, Self::MAX_PRUNE_SLOTS_PER_PASS);

        let prune_before = start_slot + max_prune;

        info!(cutoff_slot = prune_before, "pruning wal for excess history");

        self.remove_before(prune_before)?;

        Ok(())
    }

    pub fn housekeeping(&mut self) -> Result<(), WalError> {
        if let Some(max_slots) = self.max_slots {
            info!(max_slots, "pruning wal for excess history");
            self.prune_history(max_slots)?;
        }

        Ok(())
    }

    /// Approximates the LogSeq for a given BlockSlot within a specified delta
    /// range.
    ///
    /// This function searches for the closest LogSeq entry to the target
    /// BlockSlot within the range [target - max_delta, target + max_delta].
    ///
    /// # Arguments
    ///
    /// * `target` - The target BlockSlot to approximate.
    /// * `max_delta` - The maximum allowed difference between the target and
    ///   the found BlockSlot.
    ///
    /// # Returns
    ///
    /// Returns a Result containing an Option<LogSeq>. If a suitable entry is
    /// found within the specified range, it returns Some(LogSeq), otherwise
    /// None. Returns an error if there's an issue accessing the database.
    ///
    /// # Errors
    ///
    /// This function will return an error if there's an issue with database
    /// operations.
    pub fn approximate_slot(
        &self,
        target: BlockSlot,
        search_range: impl std::ops::RangeBounds<BlockSlot>,
    ) -> Result<Option<LogSeq>, WalError> {
        let min_slot = match search_range.start_bound() {
            std::ops::Bound::Included(x) => *x as i128,
            std::ops::Bound::Excluded(x) => *x as i128 + 1,
            std::ops::Bound::Unbounded => i128::MIN,
        };

        let max_slot = match search_range.end_bound() {
            std::ops::Bound::Included(x) => *x as i128,
            std::ops::Bound::Excluded(x) => *x as i128 - 1,
            std::ops::Bound::Unbounded => i128::MAX,
        };

        let rx = self.db.begin_read()?;
        let table = rx.open_table(POS)?;

        let range = table.range(min_slot..max_slot)?;

        let deltas: Vec<_> = range
            .map_ok(|(k, v)| (target as i128 - k.value(), v.value()))
            .try_collect()?;

        let seq = deltas.into_iter().min_by_key(|(x, _)| *x).map(|(_, v)| v);

        Ok(seq)
    }

    /// Attempts to find an approximate LogSeq for a given BlockSlot with
    /// retries.
    ///
    /// This function repeatedly calls `approximate_slot` with an expanding
    /// search range until a suitable LogSeq is found or the maximum number
    /// of retries is reached.
    ///
    /// # Arguments
    ///
    /// * `target` - The target BlockSlot to approximate.
    /// * `search_range` - A closure that takes the current retry count and
    ///   returns a range of BlockSlots to search within. This allows for
    ///   dynamic expansion of the search range.
    ///
    /// # Returns
    ///
    /// Returns a Result containing an Option<LogSeq>. If a suitable entry is
    /// found within any of the attempted search ranges, it returns
    /// Some(LogSeq), otherwise None. Returns an error if there's an issue
    /// accessing the database.
    ///
    /// # Errors
    ///
    /// This function will return an error if there's an issue with database
    /// operations during any of the approximation attempts.
    ///
    /// # Examples
    ///
    /// ```
    /// let result = wal.approximate_slot_with_retry(
    ///     slot,
    ///     |retry| slot - 100 * retry..=slot + 100 * retry
    /// )?;
    /// ```
    pub fn approximate_slot_with_retry<F, R>(
        &self,
        target: BlockSlot,
        search_range: F,
    ) -> Result<Option<LogSeq>, WalError>
    where
        F: Fn(usize) -> R,
        R: std::ops::RangeBounds<BlockSlot>,
    {
        for i in 1..10 {
            let search_range = search_range(i);
            let seq = self.approximate_slot(target, search_range)?;

            if let Some(seq) = seq {
                return Ok(Some(seq));
            }
        }

        Ok(None)
    }

    /// Removes all entries from the WAL before the specified slot.
    ///
    /// This function is used to trim the WAL by removing all entries that are
    /// older than the given slot. It uses the `approximate_slot` function
    /// to find a suitable starting point for the deletion process.
    ///
    /// # Arguments
    ///
    /// * `slot` - The BlockSlot before which all entries should be removed.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation is successful, or a `WalError` if
    /// there's an issue.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - There's an issue with database operations.
    /// - The specified slot cannot be found or approximated in the WAL.
    ///
    /// # Note
    ///
    /// This operation is irreversible and should be used with caution. Make
    /// sure you have backups or are certain about trimming the WAL before
    /// calling this function.
    pub fn remove_before(&mut self, slot: BlockSlot) -> Result<(), WalError> {
        let wx = self.db.begin_write()?;

        {
            let last_seq = self
                .approximate_slot_with_retry(slot, |attempt| {
                    let start = slot - (20 * attempt as u64);
                    start..=slot
                })?
                .ok_or(WalError::SlotNotFound(slot))?;

            debug!(last_seq, "found max sequence to remove");

            let mut wal = wx.open_table(WAL)?;

            let mut to_remove = wal.extract_from_if(..last_seq, |_, _| true)?;

            while let Some(Ok((seq, _))) = to_remove.next() {
                debug!(seq = seq.value(), "removing log entry");
            }
        }

        {
            let mut pos = wx.open_table(POS)?;
            let mut to_remove = pos.extract_from_if(..(slot as i128), |_, _| true)?;

            while let Some(Ok((slot, _))) = to_remove.next() {
                debug!(slot = slot.value(), "removing log entry");
            }
        }

        wx.commit()?;

        Ok(())
    }
}

impl super::WalReader for WalStore {
    type LogIterator<'a> = WalIter<'a>;

    async fn tip_change(&self) -> Result<(), WalError> {
        self.tip_change.notified().await;

        Ok(())
    }

    fn crawl_range<'a>(
        &self,
        start: LogSeq,
        end: LogSeq,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = table.range(start..=end)?;

        Ok(WalIter(range))
    }

    fn crawl_from<'a>(&self, start: Option<LogSeq>) -> Result<Self::LogIterator<'a>, WalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = match start {
            Some(start) => table.range(start..)?,
            None => table.range(0..)?,
        };

        Ok(WalIter(range))
    }

    fn locate_point(&self, point: &super::ChainPoint) -> Result<Option<LogSeq>, WalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(POS)?;

        let pos_key = point_to_augmented_slot(point);
        let pos = table.get(pos_key)?.map(|x| x.value());

        Ok(pos)
    }
}

impl super::WalWriter for WalStore {
    fn append_entries(
        &mut self,
        logs: impl Iterator<Item = super::LogValue>,
    ) -> Result<(), super::WalError> {
        let wx = self.db.begin_write()?;

        {
            let mut wal = wx.open_table(WAL)?;
            let mut pos = wx.open_table(POS)?;

            let mut next_seq = wal.last()?.map(|(x, _)| x.value() + 1).unwrap_or_default();

            for log in logs {
                // Since we need to track Origin as part of the wal, we turn slots into signed
                // integers and treat -1 as the reference for Origin. This is not ideal from
                // disk space perspective, but good enough for this stage.
                let pos_key = match &log {
                    LogValue::Apply(RawBlock { slot, .. }) => *slot as i128,
                    LogValue::Undo(RawBlock { slot, .. }) => *slot as i128,
                    LogValue::Mark(x) => point_to_augmented_slot(x),
                };

                pos.insert(pos_key, next_seq)?;
                wal.insert(next_seq, log)?;

                next_seq += 1;
            }
        }

        wx.commit()?;

        self.tip_change.notify_waiters();

        Ok(())
    }
}
