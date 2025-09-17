use bincode;
use itertools::Itertools;
use redb::{Range, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, ops::RangeBounds, path::Path, sync::Arc};
use thiserror::Error;
use tracing::{debug, event_enabled, info, trace, warn, Level};

use dolos_core::{
    BlockSlot, ChainPoint, EntityDelta, LogEntry, LogValue, RawBlock, WalError, WalStore,
};

#[derive(Debug, Error)]
#[error(transparent)]
pub struct RedbWalError(#[from] WalError);

impl From<redb::Error> for RedbWalError {
    fn from(value: redb::Error) -> Self {
        Self(WalError::internal(value))
    }
}

impl From<RedbWalError> for WalError {
    fn from(value: RedbWalError) -> Self {
        value.0
    }
}

impl From<::redb::DatabaseError> for RedbWalError {
    fn from(value: ::redb::DatabaseError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::TableError> for RedbWalError {
    fn from(value: ::redb::TableError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::CommitError> for RedbWalError {
    fn from(value: ::redb::CommitError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::StorageError> for RedbWalError {
    fn from(value: ::redb::StorageError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::TransactionError> for RedbWalError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

pub type AugmentedBlockSlot = i128;
pub type DbLogValue = Vec<u8>;

#[derive(Debug)]
pub struct DbChainPoint([u8; 40]);

impl DbChainPoint {
    pub fn slot_range(range: impl RangeBounds<BlockSlot>) -> impl RangeBounds<DbChainPoint> {
        let min_slot = match range.start_bound() {
            std::ops::Bound::Included(x) => *x,
            std::ops::Bound::Excluded(x) => *x + 1,
            std::ops::Bound::Unbounded => BlockSlot::MIN,
        };

        let mut min_point = [0u8; 40];
        min_point[0..8].copy_from_slice(&min_slot.to_le_bytes());
        let min_point = DbChainPoint(min_point);

        let max_slot = match range.end_bound() {
            std::ops::Bound::Included(x) => *x,
            std::ops::Bound::Excluded(x) => *x - 1,
            std::ops::Bound::Unbounded => BlockSlot::MAX,
        };

        let mut max_point = [255u8; 40];
        max_point[0..8].copy_from_slice(&max_slot.to_le_bytes());
        let max_point = DbChainPoint(max_point);

        std::ops::RangeInclusive::new(min_point, max_point)
    }
}

impl redb::Value for DbChainPoint {
    type SelfType<'a>
        = Self
    where
        Self: 'a;

    type AsBytes<'a>
        = &'a [u8; 40]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(40)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let inner = <[u8; 40]>::try_from(data).unwrap();
        Self(inner)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        &value.0
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("chainpoint")
    }
}

impl redb::Key for DbChainPoint {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

impl From<ChainPoint> for DbChainPoint {
    fn from(value: ChainPoint) -> Self {
        DbChainPoint(value.into_bytes())
    }
}

impl From<DbChainPoint> for ChainPoint {
    fn from(value: DbChainPoint) -> Self {
        ChainPoint::from_bytes(value.0)
    }
}

pub struct DbChainPointRange {
    start: std::ops::Bound<DbChainPoint>,
    end: std::ops::Bound<DbChainPoint>,
}

impl std::ops::RangeBounds<DbChainPoint> for DbChainPointRange {
    fn start_bound(&self) -> std::ops::Bound<&DbChainPoint> {
        self.start.as_ref()
    }

    fn end_bound(&self) -> std::ops::Bound<&DbChainPoint> {
        self.end.as_ref()
    }
}

fn to_raw_log_value<T>(value: &LogValue<T>) -> DbLogValue
where
    T: EntityDelta,
    T: Serialize,
{
    bincode::serialize(value).unwrap()
}

fn from_raw_log_value<T>(value: &DbLogValue) -> LogValue<T>
where
    T: EntityDelta,
    T: DeserializeOwned,
{
    bincode::deserialize(value).unwrap()
}

const WAL: TableDefinition<DbChainPoint, DbLogValue> = TableDefinition::new("wal");

pub struct LogIter<'a, T>(Range<'a, DbChainPoint, DbLogValue>, PhantomData<T>);

impl<'a, T> From<Range<'a, DbChainPoint, DbLogValue>> for LogIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    fn from(value: Range<'a, DbChainPoint, DbLogValue>) -> Self {
        Self(value, PhantomData)
    }
}

impl<T> Iterator for LogIter<'_, T>
where
    T: EntityDelta + DeserializeOwned,
{
    type Item = LogEntry<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
            .map(|(k, v)| (k.into(), from_raw_log_value(&v)))
    }
}

impl<T> DoubleEndedIterator for LogIter<'_, T>
where
    T: EntityDelta,
    T: DeserializeOwned,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
            .map(|(k, v)| (k.into(), from_raw_log_value(&v)))
    }
}

pub struct BlockIter<'a, T>(LogIter<'a, T>);

impl<'a, T> Iterator for BlockIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    type Item = (ChainPoint, RawBlock);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(point, log)| (point, Arc::new(log.block)))
    }
}

impl<'a, T> DoubleEndedIterator for BlockIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|(point, log)| (point, Arc::new(log.block)))
    }
}

impl<'a, T> From<LogIter<'a, T>> for BlockIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    fn from(value: LogIter<'a, T>) -> Self {
        Self(value)
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 50;

/// Concrete implementation of WalStore using Redb
#[derive(Clone, Debug)]
pub struct RedbWalStore<T> {
    db: Arc<redb::Database>,
    _phantom: PhantomData<T>,
}

impl<T> RedbWalStore<T>
where
    T: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn is_empty(&self) -> Result<bool, RedbWalError> {
        let wr = self.db.begin_read()?;

        let tables = wr.list_tables()?;

        if tables.count() == 0 {
            return Ok(true);
        }

        let start = self.find_tip()?;

        if start.is_none() {
            return Ok(true);
        }

        Ok(false)
    }

    pub fn memory() -> Result<Self, WalError> {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(WalError::internal)?;

        let out = Self {
            db: Arc::new(db),
            _phantom: Default::default(),
        };

        out.ensure_initialized()?;

        Ok(out)
    }

    pub fn open(path: impl AsRef<Path>, cache_size: Option<usize>) -> Result<Self, WalError> {
        let inner = redb::Database::builder()
            .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "wal db is repairing"))
            .set_cache_size(1024 * 1024 * cache_size.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)
            .map_err(WalError::internal)?;

        let out = Self {
            db: Arc::new(inner),
            _phantom: Default::default(),
        };

        out.ensure_initialized()?;

        Ok(out)
    }

    pub fn db_mut(&mut self) -> Option<&mut redb::Database> {
        Arc::get_mut(&mut self.db)
    }

    pub fn ensure_initialized(&self) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);
        wx.open_table(WAL)?;
        wx.commit()?;
        Ok(())
    }

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
    /// 4. Optionally limits the number of slots pruned per invocation.
    ///
    /// # Arguments
    ///
    /// * `max_slots` - The maximum number of slots to retain in the WAL.
    /// * `max_prune` - Optional limit on the number of slots to prune in a
    ///   single operation.
    ///
    /// # Returns
    ///
    /// Returns `Ok` if the operation was successful, or a `WalError` if an
    /// error occurred. If the target slot is not found, it logs a warning and
    /// returns `Ok`.
    ///
    /// # Notes
    ///
    /// - If the WAL doesn't exceed the `max_slots` limit, no pruning occurs.
    /// - This method is typically called periodically as part of housekeeping
    ///   operations.
    /// - If `max_prune` is specified, it limits the number of slots pruned in a
    ///   single operation, which can help avoid long-running operations.
    /// - If `max_prune` is not specified, all excess slots will be pruned.
    pub fn prune_history(
        &self,
        max_slots: u64,
        max_prune: Option<u64>,
    ) -> Result<bool, RedbWalError> {
        let Some((start, _)) = self.find_start()? else {
            debug!("no start point found, skipping housekeeping");
            return Ok(true);
        };

        let Some((last, _)) = self.find_tip()? else {
            debug!("no tip found, skipping housekeeping");
            return Ok(true);
        };

        let start_slot = start.slot();
        let last_slot = last.slot();

        let delta = last_slot.saturating_sub(start_slot);
        let excess = delta.saturating_sub(max_slots);

        debug!(
            delta,
            excess, last_slot, start_slot, "wal history delta computed"
        );

        if excess == 0 {
            debug!(delta, max_slots, excess, "no pruning necessary");
            return Ok(true);
        }

        let (done, max_prune) = match max_prune {
            Some(max) => (excess <= max, core::cmp::min(excess, max)),
            None => (true, excess),
        };

        let prune_before = start_slot + max_prune;

        info!(
            cutoff_slot = prune_before,
            start_slot, excess, "pruning wal for excess history"
        );

        match self.remove_before(prune_before) {
            Err(RedbWalError(WalError::SlotNotFound(_))) => {
                warn!("pruning target slot not found, skipping");
                Ok(true)
            }
            Err(e) => Err(e),
            Ok(_) => Ok(done),
        }
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
    ) -> Result<Option<ChainPoint>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = DbChainPoint::slot_range(search_range);
        let range = table.range(range)?;

        let deltas: Vec<_> = range
            .map_ok(|(k, _)| k.value())
            .map_ok(ChainPoint::from)
            .map_ok(|point| (target - point.slot(), point))
            .try_collect()?;

        let point = deltas.into_iter().min_by_key(|(x, _)| *x).map(|(_, v)| v);

        Ok(point)
    }

    /// Attempts to find an approximate ChainPoint for a given BlockSlot with
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
    /// Returns a Result containing an Option<ChainPoint>. If a suitable entry
    /// is found within any of the attempted search ranges, it returns
    /// Some(ChainPoint), otherwise None. Returns an error if there's an issue
    /// accessing the database.
    ///
    /// # Errors
    ///
    /// This function will return an error if there's an issue with database
    /// operations during any of the approximation attempts.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let result = wal.approximate_slot_with_retry(
    ///     slot,
    ///     |retry| slot - 100 * retry..=slot + 100 * retry
    /// )?;
    /// ```
    pub fn approximate_slot_with_retry<F, R>(
        &self,
        target: BlockSlot,
        search_range: F,
    ) -> Result<Option<ChainPoint>, RedbWalError>
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
    pub fn remove_before(&self, slot: BlockSlot) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        let last_point = self
            .approximate_slot_with_retry(slot, |attempt| {
                let start = slot - (20 * attempt as u64);
                start..=slot
            })?
            .ok_or(RedbWalError(WalError::SlotNotFound(slot)))?;

        debug!(%last_point, "found max chain point to remove");

        {
            let mut wal = wx.open_table(WAL)?;

            let last_point = DbChainPoint::from(last_point);
            let mut to_remove = wal.extract_from_if(..last_point, |_, _| true)?;

            while let Some(Ok((point, _))) = to_remove.next() {
                if event_enabled!(Level::TRACE) {
                    let point = point.value();
                    let point = ChainPoint::from(point);
                    trace!(%point, "removing wal table entry");
                }
            }
        }

        wx.commit()?;

        Ok(())
    }

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<LogIter<'a, T>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let start = start.map(DbChainPoint::from);
        let end = end.map(DbChainPoint::from);

        let range = match (start, end) {
            (Some(start), Some(end)) => table.range(start..=end)?,
            (Some(start), None) => table.range(start..)?,
            (None, Some(end)) => table.range(..end)?,
            (None, None) => table.range::<DbChainPoint>(..)?,
        };

        Ok(LogIter::from(range))
    }

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<T>>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let key = DbChainPoint::from(key.clone());

        let value = table
            .get(&key)?
            .map(|v| v.value())
            .map(|v| from_raw_log_value(&v));

        Ok(value)
    }

    pub fn append_entries(&self, logs: &[LogEntry<T>]) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        {
            let mut wal = wx.open_table(WAL)?;

            for (point, log) in logs {
                let point = DbChainPoint::from(point.clone());
                let log = to_raw_log_value(log);
                wal.insert(point, log)?;
            }
        }

        wx.commit()?;

        Ok(())
    }

    fn remove_entries(&self, after: &ChainPoint) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        {
            let after = DbChainPoint::from(after.clone());
            let mut table = wx.open_table(WAL)?;

            let mut to_remove = table.extract_from_if(..after, |_, _| true)?;

            while let Some(Ok((point, _))) = to_remove.next() {
                if event_enabled!(Level::TRACE) {
                    let point = point.value();
                    let point = ChainPoint::from(point);
                    trace!(%point, "removing wal table entry");
                }
            }
        }

        wx.commit()?;

        Ok(())
    }

    fn reset_to(&self, point: &ChainPoint) -> Result<(), RedbWalError> {
        self.remove_entries(&ChainPoint::Origin)?;

        let entry = (point.clone(), LogValue::origin());
        self.append_entries(&[entry])?;

        Ok(())
    }
}

impl<T> WalStore for RedbWalStore<T>
where
    T: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Delta = T;
    type LogIterator<'a> = LogIter<'a, Self::Delta>;
    type BlockIterator<'a> = BlockIter<'a, Self::Delta>;

    fn reset_to(&self, point: &ChainPoint) -> Result<(), WalError> {
        RedbWalStore::reset_to(self, point).map_err(From::from)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError> {
        RedbWalStore::prune_history(self, max_slots, max_prune).map_err(From::from)
    }

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<Self::Delta>>, WalError> {
        RedbWalStore::read_entry(self, key).map_err(From::from)
    }

    fn locate_point(&self, around: BlockSlot) -> Result<Option<ChainPoint>, WalError> {
        let search = |retry| {
            let delta = 20 * retry as u64;
            let start = around - delta;
            let end = around + delta;
            start..=end
        };

        let x = self.approximate_slot_with_retry(around, search)?;

        Ok(x)
    }

    fn append_entries(&self, logs: Vec<LogEntry<Self::Delta>>) -> Result<(), WalError> {
        RedbWalStore::append_entries(self, &logs).map_err(WalError::from)?;

        Ok(())
    }

    fn remove_entries(&mut self, after: &ChainPoint) -> Result<(), WalError> {
        RedbWalStore::remove_entries(self, after).map_err(WalError::from)?;

        Ok(())
    }

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        RedbWalStore::iter_logs(self, start, end).map_err(From::from)
    }

    fn iter_blocks<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::BlockIterator<'a>, WalError> {
        let iter = RedbWalStore::iter_logs(self, start, end)?;
        let iter = BlockIter::from(iter);
        Ok(iter)
    }
}
