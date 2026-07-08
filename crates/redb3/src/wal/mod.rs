use bincode;
use itertools::Itertools;
use redb::{Range, ReadableDatabase, ReadableTableMetadata, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
use std::{marker::PhantomData, ops::RangeBounds, path::Path, sync::Arc};
use thiserror::Error;
use tracing::{debug, event_enabled, info, trace, warn, Level};

use dolos_core::{
    config::RedbWalConfig, BlockSlot, ChainPoint, EntityDelta, LogEntry, LogValue, RawBlock,
    WalError, WalStore,
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct DbChainPoint([u8; 40]);

impl DbChainPoint {
    /// Smallest key at `slot`: big-endian slot bytes + zero hash. Big-endian is
    /// required so redb's lexicographic key order matches slot order (see
    /// `ChainPoint::into_bytes`).
    fn min_bound(slot: BlockSlot) -> DbChainPoint {
        let mut point = [0u8; 40];
        point[0..8].copy_from_slice(&slot.to_be_bytes());
        DbChainPoint(point)
    }

    /// Largest key at `slot`: big-endian slot bytes + all-ones hash.
    fn max_bound(slot: BlockSlot) -> DbChainPoint {
        let mut point = [255u8; 40];
        point[0..8].copy_from_slice(&slot.to_be_bytes());
        DbChainPoint(point)
    }

    pub fn slot_range(range: impl RangeBounds<BlockSlot>) -> impl RangeBounds<DbChainPoint> {
        use std::ops::Bound;

        // Map bounds to key bounds directly; no `+1`/`-1` shift (would overflow
        // at BlockSlot::MAX / underflow at MIN). Excluded flips the hash extreme.
        let start = match range.start_bound() {
            Bound::Included(x) => Bound::Included(DbChainPoint::min_bound(*x)),
            Bound::Excluded(x) => Bound::Excluded(DbChainPoint::max_bound(*x)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(x) => Bound::Included(DbChainPoint::max_bound(*x)),
            Bound::Excluded(x) => Bound::Excluded(DbChainPoint::min_bound(*x)),
            Bound::Unbounded => Bound::Unbounded,
        };

        DbChainPointRange { start, end }
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

const WAL_METADATA: TableDefinition<&str, u32> = TableDefinition::new("wal_metadata");

const WAL_METADATA_VERSION_KEY: &str = "version";

/// Current WAL on-disk schema version.
///
/// Bump this whenever the serialized format of WAL entries changes in a way
/// that breaks reading older data. On open, an older or missing version
/// triggers an automatic wipe; a newer version triggers a refusal to start.
pub const CURRENT_WAL_VERSION: u32 = 1;

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
    /// Gracefully shutdown the WAL store.
    ///
    /// For Redb, this is a no-op since Redb handles cleanup automatically
    /// during drop without blocking issues.
    pub fn shutdown(&self) -> Result<(), RedbWalError> {
        Ok(())
    }

    pub fn is_empty(&self) -> Result<bool, RedbWalError> {
        // "Empty" here refers to absence of WAL data entries. The metadata
        // table is always present after `ensure_version()`, so an empty WAL
        // can still have a populated `wal_metadata` table.
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
        out.ensure_version()?;

        Ok(out)
    }

    pub fn open(path: impl AsRef<Path>, config: &RedbWalConfig) -> Result<Self, WalError> {
        let inner = redb::Database::builder()
            .set_repair_callback(|x| warn!(progress = x.progress() * 100f64, "wal db is repairing"))
            .set_cache_size(1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)
            .map_err(WalError::internal)?;

        let out = Self {
            db: Arc::new(inner),
            _phantom: Default::default(),
        };

        out.ensure_initialized()?;
        out.ensure_version()?;

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

    /// Read the current on-disk WAL schema version.
    ///
    /// Returns `Ok(None)` if the metadata table doesn't exist (legacy DB
    /// predating the versioning mechanism) or has no version key set.
    fn read_version(&self) -> Result<Option<u32>, RedbWalError> {
        let rx = self.db.begin_read()?;

        let table = match rx.open_table(WAL_METADATA) {
            Ok(t) => t,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(e) => return Err(RedbWalError::from(e)),
        };

        let value = table.get(WAL_METADATA_VERSION_KEY)?.map(|v| v.value());

        Ok(value)
    }

    /// Stamp the metadata table with the given version. Used when the WAL
    /// data table is empty (no destructive action needed).
    fn stamp_version(&self, version: u32) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);
        {
            let mut metadata = wx.open_table(WAL_METADATA)?;
            metadata.insert(WAL_METADATA_VERSION_KEY, version)?;
        }
        wx.commit()?;
        Ok(())
    }

    /// Atomically drain every entry from the WAL data table and stamp the
    /// metadata with the new version.
    ///
    /// This is a single redb transaction, so a crash between the drain and
    /// the version write cannot leave the DB in a half-upgraded state.
    fn wipe_and_stamp(&self, version: u32) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);
        {
            let mut wal = wx.open_table(WAL)?;
            wal.retain(|_, _| false)?;
        }
        {
            let mut metadata = wx.open_table(WAL_METADATA)?;
            metadata.insert(WAL_METADATA_VERSION_KEY, version)?;
        }
        wx.commit()?;
        Ok(())
    }

    /// Verify that the on-disk WAL is compatible with `CURRENT_WAL_VERSION`,
    /// performing a forced wipe + version bump if not.
    ///
    /// - Matching version → no-op.
    /// - Newer on-disk version → returns `WalError::IncompatibleVersion` to
    ///   prevent an old binary from destroying newer data.
    /// - Older or missing version with empty data → just stamps the version.
    /// - Older or missing version with non-empty data → wipes the data and
    ///   stamps the version (logs a warning).
    pub fn ensure_version(&self) -> Result<(), RedbWalError> {
        let found = self.read_version()?;

        match found {
            Some(v) if v == CURRENT_WAL_VERSION => Ok(()),
            Some(v) if v > CURRENT_WAL_VERSION => {
                Err(RedbWalError(WalError::IncompatibleVersion {
                    found: v,
                    expected: CURRENT_WAL_VERSION,
                }))
            }
            _ => {
                if self.data_table_is_empty()? {
                    info!(
                        version = CURRENT_WAL_VERSION,
                        "initializing WAL metadata version"
                    );
                    self.stamp_version(CURRENT_WAL_VERSION)?;
                } else {
                    warn!(
                        found = ?found,
                        expected = CURRENT_WAL_VERSION,
                        "WAL on-disk version is incompatible with running code; wiping WAL data"
                    );
                    self.wipe_and_stamp(CURRENT_WAL_VERSION)?;
                }
                Ok(())
            }
        }
    }

    /// Low-level check on whether the WAL data table contains any entries,
    /// using redb's `is_empty()` directly without deserializing values.
    /// Safe to call against incompatible on-disk data.
    fn data_table_is_empty(&self) -> Result<bool, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;
        Ok(table.is_empty()?)
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
    /// Returns `Ok(true)` when the WAL is within `max_slots` (or was pruned all
    /// the way down to it), and `Ok(false)` when `max_prune` capped this call
    /// before the target was reached and another round is needed. Pruning is
    /// deterministic: `remove_before` removes by cutoff bound, and any error it
    /// raises is propagated (the write transaction is not committed).
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

        self.remove_before(prune_before)?;

        Ok(done)
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
            .map_ok(|point| (target.abs_diff(point.slot()), point))
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

        {
            let mut wal = wx.open_table(WAL)?;

            // `..min_bound(slot)` removes every entry with slot < target and
            // keeps real entries at `slot` (nonzero hash). Deterministic even
            // where no entry exists near the cutoff.
            let bound = DbChainPoint::min_bound(slot);
            let to_remove = wal.extract_from_if(..bound, |_, _| true)?;

            // Drain fully; `?` propagates storage errors before commit, so an
            // error aborts the txn instead of persisting a partial prune.
            for entry in to_remove {
                let (point, _) = entry?;

                if event_enabled!(Level::TRACE) {
                    let point = ChainPoint::from(point.value());
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
            (None, Some(end)) => table.range(..=end)?,
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

            let bounds = after.clone()..;

            let range = table.extract_from_if(bounds, |current, _| current > after)?;

            for entry in range {
                let (point, _) = entry?;

                if event_enabled!(Level::TRACE) {
                    let point = ChainPoint::from(point.value());
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

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), RedbWalError> {
        self.remove_entries(after)?;

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

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), WalError> {
        RedbWalStore::truncate_front(self, after).map_err(From::from)
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
            let start = around.saturating_sub(delta);
            let end = around.saturating_add(delta);
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

#[cfg(test)]
mod tests {
    use super::*;
    use dolos_core::{ChainError, Entity, EntityValue, Namespace, NsKey};
    use serde::{Deserialize, Serialize};

    /// Minimal delta type used by tests to satisfy `RedbWalStore`'s generic
    /// bounds. Tests in this module never round-trip data through the WAL,
    /// so the apply/undo/encode/decode methods can be inert.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestDelta;

    #[derive(Clone, Debug)]
    struct TestEntity;

    impl Entity for TestEntity {
        fn decode_entity(_: Namespace, _: &EntityValue) -> Result<Self, ChainError> {
            unimplemented!("not exercised in WAL versioning tests")
        }

        fn encode_entity(_: &Self) -> (Namespace, EntityValue) {
            unimplemented!("not exercised in WAL versioning tests")
        }
    }

    impl EntityDelta for TestDelta {
        type Entity = TestEntity;

        fn key(&self) -> NsKey {
            unimplemented!("not exercised in WAL versioning tests")
        }

        fn apply(&mut self, _: &mut Option<Self::Entity>) {}

        fn undo(&self, _: &mut Option<Self::Entity>) {}
    }

    type TestStore = RedbWalStore<TestDelta>;

    fn open_test_store(path: &Path) -> Result<TestStore, WalError> {
        TestStore::open(path, &RedbWalConfig::default())
    }

    /// Open a redb DB directly (bypassing RedbWalStore::open) so tests can
    /// simulate legacy on-disk state that predates the metadata table.
    fn open_raw_db(path: &Path) -> redb::Database {
        redb::Database::builder().create(path).unwrap()
    }

    #[test]
    fn fresh_in_memory_stamps_current_version() {
        let store = TestStore::memory().unwrap();
        assert_eq!(store.read_version().unwrap(), Some(CURRENT_WAL_VERSION));
    }

    #[test]
    fn fresh_on_disk_stamps_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        let store = open_test_store(&path).unwrap();
        assert_eq!(store.read_version().unwrap(), Some(CURRENT_WAL_VERSION));
    }

    #[test]
    fn legacy_db_with_data_gets_wiped_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        // Create a legacy DB: WAL table populated, no metadata table.
        {
            let db = open_raw_db(&path);
            let wx = db.begin_write().unwrap();
            {
                let mut table = wx.open_table(WAL).unwrap();
                let key = DbChainPoint::from(ChainPoint::Specific(42, [1u8; 32].into()));
                table.insert(key, vec![0xDEu8, 0xAD, 0xBE, 0xEF]).unwrap();
            }
            wx.commit().unwrap();
        }

        // Sanity-check the legacy state.
        {
            let db = open_raw_db(&path);
            let rx = db.begin_read().unwrap();
            let table = rx.open_table(WAL).unwrap();
            assert!(!table.is_empty().unwrap());
            assert!(matches!(
                rx.open_table(WAL_METADATA),
                Err(redb::TableError::TableDoesNotExist(_))
            ));
        }

        let store = open_test_store(&path).unwrap();
        assert_eq!(store.read_version().unwrap(), Some(CURRENT_WAL_VERSION));
        assert!(store.data_table_is_empty().unwrap());
    }

    #[test]
    fn newer_version_on_disk_refuses_to_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        // Stamp metadata with a version newer than CURRENT_WAL_VERSION.
        {
            let db = open_raw_db(&path);
            let wx = db.begin_write().unwrap();
            {
                let mut metadata = wx.open_table(WAL_METADATA).unwrap();
                metadata
                    .insert(WAL_METADATA_VERSION_KEY, CURRENT_WAL_VERSION + 1)
                    .unwrap();
            }
            wx.commit().unwrap();
        }

        let err = open_test_store(&path).unwrap_err();
        match err {
            WalError::IncompatibleVersion { found, expected } => {
                assert_eq!(found, CURRENT_WAL_VERSION + 1);
                assert_eq!(expected, CURRENT_WAL_VERSION);
            }
            other => panic!("expected IncompatibleVersion, got {other:?}"),
        }
    }

    #[test]
    fn matching_version_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        // First open: stamps current version.
        open_test_store(&path).unwrap();

        // Second open: should succeed without rewriting (no panic, version intact).
        let store = open_test_store(&path).unwrap();
        assert_eq!(store.read_version().unwrap(), Some(CURRENT_WAL_VERSION));
    }

    /// WAL entry at `slot` with a deterministic nonzero hash (distinct from the
    /// zero-hash cutoff bound `remove_before` uses).
    fn entry(slot: BlockSlot) -> LogEntry<TestDelta> {
        let mut hash = [1u8; 32];
        hash[0..8].copy_from_slice(&slot.to_be_bytes());
        (ChainPoint::Specific(slot, hash.into()), LogValue::origin())
    }

    fn slots(store: &TestStore) -> Vec<BlockSlot> {
        store
            .iter_logs(None, None)
            .unwrap()
            .map(|(p, _)| p.slot())
            .collect()
    }

    /// Endianness regression: key order must equal slot order across the
    /// 2^8/2^16/2^32 boundaries, and scans must find realistic slots.
    #[test]
    fn slot_range_round_trip_across_byte_boundaries() {
        let store = TestStore::memory().unwrap();

        let targets: Vec<BlockSlot> = vec![
            255,
            256,
            65_535,
            65_536,
            4_294_967_295,
            4_294_967_296,
            127_000_000, // preprod-scale slot from the bug report
        ];

        // Insert in shuffled order to prove ordering comes from the keys.
        let mut insert_order = targets.clone();
        insert_order.rotate_left(3);
        let entries: Vec<_> = insert_order.iter().map(|s| entry(*s)).collect();
        store.append_entries(&entries).unwrap();

        // Lexicographic key order must equal slot order.
        let mut sorted = targets.clone();
        sorted.sort_unstable();
        assert_eq!(slots(&store), sorted, "key order must equal slot order");

        // Every slot is locatable and approximates to itself.
        for &s in &targets {
            let located = store.locate_point(s).unwrap();
            assert_eq!(
                located.map(|p| p.slot()),
                Some(s),
                "locate_point failed for slot {s}"
            );

            let approx = store.approximate_slot(s, s..=s).unwrap();
            assert_eq!(
                approx.map(|p| p.slot()),
                Some(s),
                "approximate_slot failed for slot {s}"
            );
        }

        // Scanning from a mid boundary returns exactly the at-or-after suffix.
        let from = entry(65_536).0;
        let suffix: Vec<BlockSlot> = store
            .iter_logs(Some(from), None)
            .unwrap()
            .map(|(p, _)| p.slot())
            .collect();
        assert_eq!(
            suffix,
            vec![65_536, 127_000_000, 4_294_967_295, 4_294_967_296]
        );
    }

    /// `slot_range` maps bounds to the enclosing key bounds without
    /// overflowing at `BlockSlot::MAX` / underflowing at `MIN` on `Excluded`.
    #[test]
    fn slot_range_handles_extreme_and_exclusive_bounds() {
        use std::ops::Bound;

        let r = DbChainPoint::slot_range((Bound::Excluded(BlockSlot::MAX), Bound::Unbounded));
        assert_eq!(
            r.start_bound(),
            Bound::Excluded(&DbChainPoint::max_bound(BlockSlot::MAX))
        );
        assert_eq!(r.end_bound(), Bound::Unbounded);

        let r = DbChainPoint::slot_range((Bound::Unbounded, Bound::Excluded(BlockSlot::MIN)));
        assert_eq!(r.start_bound(), Bound::Unbounded);
        assert_eq!(
            r.end_bound(),
            Bound::Excluded(&DbChainPoint::min_bound(BlockSlot::MIN))
        );

        let r = DbChainPoint::slot_range(10..=20);
        assert_eq!(
            r.start_bound(),
            Bound::Included(&DbChainPoint::min_bound(10))
        );
        assert_eq!(r.end_bound(), Bound::Included(&DbChainPoint::max_bound(20)));
    }

    /// Unbatched prune shrinks the WAL to exactly `max_slots`, tip preserved.
    #[test]
    fn prune_history_full_shrinks_to_max_slots() {
        let store = TestStore::memory().unwrap();

        let base: BlockSlot = 100_000_000;
        let count: u64 = 20_000;
        let entries: Vec<_> = (0..count).map(|i| entry(base + i)).collect();
        store.append_entries(&entries).unwrap();

        let last = base + count - 1;
        let max_slots = 5_000;

        let done = store.prune_history(max_slots, None).unwrap();
        assert!(done, "unbatched prune must finish in one call");

        let start = store.find_start().unwrap().unwrap().0.slot();
        let tip = store.find_tip().unwrap().unwrap().0.slot();

        assert_eq!(tip, last, "tip must be preserved");
        assert_eq!(start, last - max_slots, "retained window must be max_slots");
        assert_eq!(
            slots(&store).len() as u64,
            max_slots + 1,
            "exactly the protected window remains"
        );
    }

    /// Batched prune makes bounded progress, converges, and never prunes into
    /// the protected `max_slots` window.
    #[test]
    fn prune_history_batched_converges() {
        let store = TestStore::memory().unwrap();

        let base: BlockSlot = 100_000_000;
        let count: u64 = 20_000;
        let entries: Vec<_> = (0..count).map(|i| entry(base + i)).collect();
        store.append_entries(&entries).unwrap();

        let last = base + count - 1;
        let max_slots = 5_000;
        let max_prune = 3_000;

        // First round: large backlog is not cleared in one batch.
        let start_before = store.find_start().unwrap().unwrap().0.slot();
        let done = store.prune_history(max_slots, Some(max_prune)).unwrap();
        assert!(!done, "large backlog should not finish in one batch");

        let start_after = store.find_start().unwrap().unwrap().0.slot();
        assert!(start_after > start_before, "batch must advance the start");
        assert!(
            last - start_after >= max_slots,
            "must never prune into the protected window"
        );

        // Loop to completion, bounded to detect non-convergence.
        let mut done = false;
        let mut rounds = 0;
        while !done {
            done = store.prune_history(max_slots, Some(max_prune)).unwrap();
            rounds += 1;
            assert!(rounds < 100, "batched pruning did not converge");
        }

        let start = store.find_start().unwrap().unwrap().0.slot();
        let tip = store.find_tip().unwrap().unwrap().0.slot();
        assert_eq!(tip, last, "tip must be preserved");
        assert_eq!(start, last - max_slots, "converges to the max_slots window");
    }

    /// `remove_before` deletes older entries even with no entry near the cutoff
    /// (the old `approximate_slot(±180)` lookup silently skipped this case).
    #[test]
    fn remove_before_handles_sparse_wal() {
        let store = TestStore::memory().unwrap();

        let old = [1_000u64, 1_001, 1_002];
        let new = [1_000_000u64, 1_000_001, 1_000_002];
        let entries: Vec<_> = old.iter().chain(new.iter()).map(|s| entry(*s)).collect();
        store.append_entries(&entries).unwrap();

        // Cutoff sits in the empty gap, far from any entry.
        store.remove_before(500_000).unwrap();

        assert_eq!(slots(&store), new.to_vec(), "old cluster removed, new kept");
    }

    /// "remove before slot" keeps the cutoff slot itself.
    #[test]
    fn remove_before_keeps_entry_at_cutoff_slot() {
        let store = TestStore::memory().unwrap();
        let entries: Vec<_> = [10u64, 20, 30, 40].iter().map(|s| entry(*s)).collect();
        store.append_entries(&entries).unwrap();

        store.remove_before(30).unwrap();

        assert_eq!(slots(&store), vec![30, 40], "cutoff slot is retained");
    }
}
