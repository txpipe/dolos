use bincode;
use itertools::Itertools;
use redb::{Range, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};
use thiserror::Error;
use tracing::{debug, info, trace, warn};

use dolos_core::{BlockSlot, ChainPoint, LogEntry, LogSeq, RawBlock, WalError, WalStore};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogValue(dolos_core::LogValue);

impl From<dolos_core::LogValue> for LogValue {
    fn from(value: dolos_core::LogValue) -> Self {
        Self(value)
    }
}

impl From<LogValue> for dolos_core::LogValue {
    fn from(value: LogValue) -> Self {
        value.0
    }
}

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

const WAL: TableDefinition<LogSeq, LogValue> = TableDefinition::new("wal");
const POS: TableDefinition<AugmentedBlockSlot, LogSeq> = TableDefinition::new("pos");

fn point_to_augmented_slot(point: &ChainPoint) -> AugmentedBlockSlot {
    match point {
        ChainPoint::Origin => -1i128,
        ChainPoint::Specific(x, _) => *x as i128,
    }
}

pub struct WalIter<'a>(Range<'a, LogSeq, LogValue>);

impl Iterator for WalIter<'_> {
    type Item = LogEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value().into()))
    }
}

impl DoubleEndedIterator for WalIter<'_> {
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value().into()))
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 50;

/// Concrete implementation of WalStore using Redb
#[derive(Clone, Debug)]
pub struct RedbWalStore {
    db: Arc<redb::Database>,
    tip_change: Arc<tokio::sync::Notify>,
}

impl RedbWalStore {
    pub fn is_empty(&self) -> Result<bool, RedbWalError> {
        let wr = self.db.begin_read()?;

        let tables = wr.list_tables()?;

        if tables.count() == 0 {
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

    pub fn initialize_from_origin(&mut self) -> Result<(), RedbWalError> {
        if !self.is_empty()? {
            return Err(RedbWalError(WalError::NotEmpty));
        }

        info!("initializing wal");
        self.append_entries(std::iter::once(dolos_core::LogValue::Mark(
            ChainPoint::Origin,
        )))?;

        Ok(())
    }

    pub fn memory() -> Result<Self, WalError> {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(WalError::internal)?;

        let out = Self {
            db: Arc::new(db),
            tip_change: Arc::new(tokio::sync::Notify::new()),
        };

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
            tip_change: Arc::new(tokio::sync::Notify::new()),
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
    ) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

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
    ) -> Result<(), RedbWalError> {
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

        let delta = last_slot.saturating_sub(start_slot);
        let excess = delta.saturating_sub(max_slots);

        debug!(
            delta,
            excess, last_slot, start_slot, "wal history delta computed"
        );

        if excess == 0 {
            debug!(delta, max_slots, excess, "no pruning necessary");
            return Ok(());
        }

        let max_prune = match max_prune {
            Some(max) => core::cmp::min(excess, max),
            None => excess,
        };

        let prune_before = start_slot + max_prune;

        info!(
            cutoff_slot = prune_before,
            start_slot, excess, "pruning wal for excess history"
        );

        match self.remove_before(prune_before) {
            Err(RedbWalError(WalError::SlotNotFound(_))) => {
                warn!("pruning target slot not found, skipping");
                Ok(())
            }
            Err(e) => Err(e),
            Ok(_) => Ok(()),
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
    ) -> Result<Option<LogSeq>, RedbWalError> {
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
    ) -> Result<Option<LogSeq>, RedbWalError>
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
            let last_seq = self
                .approximate_slot_with_retry(slot, |attempt| {
                    let start = slot - (20 * attempt as u64);
                    start..=slot
                })?
                .ok_or(RedbWalError(WalError::SlotNotFound(slot)))?;

            debug!(last_seq, "found max sequence to remove");

            let mut wal = wx.open_table(WAL)?;

            let mut to_remove = wal.extract_from_if(..last_seq, |_, _| true)?;

            while let Some(Ok((seq, _))) = to_remove.next() {
                trace!(seq = seq.value(), "removing wal table entry");
            }
        }

        {
            let mut pos = wx.open_table(POS)?;
            let mut to_remove = pos.extract_from_if(..(slot as i128), |_, _| true)?;

            while let Some(Ok((slot, _))) = to_remove.next() {
                trace!(slot = slot.value(), "removing pos table entry");
            }
        }

        wx.commit()?;

        Ok(())
    }

    fn crawl_range<'a>(&self, start: LogSeq, end: LogSeq) -> Result<WalIter<'a>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = table.range(start..=end)?;

        Ok(WalIter(range))
    }

    fn crawl_from<'a>(&self, start: Option<LogSeq>) -> Result<WalIter<'a>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = match start {
            Some(start) => table.range(start..)?,
            None => table.range(0..)?,
        };

        Ok(WalIter(range))
    }

    fn locate_point(&self, point: &ChainPoint) -> Result<Option<LogSeq>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(POS)?;

        let pos_key = point_to_augmented_slot(point);
        let pos = table.get(pos_key)?.map(|x| x.value());

        Ok(pos)
    }

    fn append_entries(
        &mut self,
        logs: impl Iterator<Item = dolos_core::LogValue>,
    ) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        {
            let mut wal = wx.open_table(WAL)?;
            let mut pos = wx.open_table(POS)?;

            let mut next_seq = wal.last()?.map(|(x, _)| x.value() + 1).unwrap_or_default();

            for log in logs {
                // Since we need to track Origin as part of the wal, we turn slots into signed
                // integers and treat -1 as the reference for Origin. This is not ideal from
                // disk space perspective, but good enough for this stage.
                let pos_key = match &log {
                    dolos_core::LogValue::Apply(RawBlock { slot, .. }) => *slot as i128,
                    dolos_core::LogValue::Undo(RawBlock { slot, .. }) => *slot as i128,
                    dolos_core::LogValue::Mark(x) => point_to_augmented_slot(x),
                };

                pos.insert(pos_key, next_seq)?;
                wal.insert(next_seq, LogValue::from(log))?;

                next_seq += 1;
            }
        }

        wx.commit()?;

        self.tip_change.notify_waiters();

        Ok(())
    }
}

impl WalStore for RedbWalStore {
    type LogIterator<'a> = WalIter<'a>;

    async fn tip_change(&self) {
        self.tip_change.notified().await;
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<(), WalError> {
        RedbWalStore::prune_history(self, max_slots, max_prune).map_err(From::from)
    }

    fn crawl_range<'a>(
        &self,
        start: LogSeq,
        end: LogSeq,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        RedbWalStore::crawl_range(self, start, end).map_err(From::from)
    }

    fn crawl_from<'a>(&self, start: Option<LogSeq>) -> Result<Self::LogIterator<'a>, WalError> {
        RedbWalStore::crawl_from(self, start).map_err(From::from)
    }

    fn locate_point(&self, point: &ChainPoint) -> Result<Option<LogSeq>, WalError> {
        RedbWalStore::locate_point(self, point).map_err(From::from)
    }

    fn append_entries(
        &mut self,
        logs: impl Iterator<Item = dolos_core::LogValue>,
    ) -> Result<(), WalError> {
        RedbWalStore::append_entries(self, logs).map_err(From::from)
    }
}

#[cfg(test)]
mod tests {
    use dolos_core::WalBlockReader;

    use crate::testing::{dummy_block_from_slot, empty_wal_db, slot_to_hash};

    use super::*;

    fn dummy_block(slot: u64) -> RawBlock {
        let hash = pallas::crypto::hash::Hasher::<256>::hash(slot.to_be_bytes().as_slice());

        RawBlock {
            slot,
            hash,
            era: pallas::ledger::traverse::Era::Byron,
            body: slot.to_be_bytes().to_vec(),
        }
    }

    #[tokio::test]
    async fn test_wal_block_reader_happy_path() {
        let mut db = RedbWalStore::memory().unwrap();
        db.initialize_from_origin().unwrap();

        let blocks = (0..=5).map(dummy_block).collect_vec();
        db.roll_forward(blocks.clone().into_iter()).unwrap();

        let wal_block_reader = WalBlockReader::try_new(&db, None, 20).unwrap();
        let output_blocks = wal_block_reader.collect_vec();

        assert_eq!(blocks, output_blocks);
    }

    #[tokio::test]
    async fn test_wal_block_reader_undone_blocks_in_lookahead_window() {
        let mut db = RedbWalStore::memory().unwrap();
        db.initialize_from_origin().unwrap();

        let undone_chain_point = (&dummy_block(1)).into();
        db.roll_forward(
            vec![
                dummy_block(0),
                dummy_block(1),
                dummy_block(2),
                dummy_block(3),
            ]
            .into_iter(),
        )
        .unwrap();
        db.roll_back(&undone_chain_point).unwrap();
        db.roll_forward(vec![dummy_block(4), dummy_block(5), dummy_block(6)].into_iter())
            .unwrap();

        let wal_block_reader = WalBlockReader::try_new(&db, None, 20).unwrap();
        let output_blocks = wal_block_reader.collect_vec();

        assert_eq!(
            vec![
                dummy_block(0),
                dummy_block(1),
                dummy_block(4),
                dummy_block(5),
                dummy_block(6)
            ],
            output_blocks
        );
    }

    #[tokio::test]
    async fn test_wal_block_reader_undone_blocks_not_in_lookahead_window() {
        let mut db = RedbWalStore::memory().unwrap();
        db.initialize_from_origin().unwrap();

        let undone_chain_point = (&dummy_block(2)).into();
        db.roll_forward(
            vec![
                dummy_block(0),
                dummy_block(1),
                dummy_block(2),
                dummy_block(3),
                dummy_block(4),
                dummy_block(5),
                dummy_block(6),
            ]
            .into_iter(),
        )
        .unwrap();
        db.roll_back(&undone_chain_point).unwrap();

        let wal_block_reader = WalBlockReader::try_new(&db, None, 2).unwrap();
        let output_blocks = wal_block_reader.collect_vec();

        // With a correctly sized lookback window, only 0 1 and 2 should be returned
        assert_eq!(
            vec![
                dummy_block(0),
                dummy_block(1),
                dummy_block(2),
                dummy_block(3),
                dummy_block(4),
                dummy_block(5),
                dummy_block(6),
            ],
            output_blocks
        );

        let wal_block_reader = WalBlockReader::try_new(&db, None, 3).unwrap();
        let output_blocks = wal_block_reader.collect_vec();

        // With a correctly sized lookback window, only 0 1 and 2 should be returned
        assert_eq!(
            vec![
                dummy_block(0),
                dummy_block(1),
                dummy_block(2),
                dummy_block(3),
            ],
            output_blocks
        );
    }

    #[test]
    fn test_origin_event() {
        let db = empty_wal_db();

        let mut iter = db.crawl_from(None).unwrap();

        let origin = iter.next();
        assert!(origin.is_some());

        let (seq, value) = origin.unwrap();
        assert_eq!(seq, 0);
        assert!(matches!(
            value,
            dolos_core::LogValue::Mark(ChainPoint::Origin)
        ));

        // ensure nothing else
        let origin = iter.next();
        assert!(origin.is_none());
    }

    #[test]
    fn test_basic_append() {
        let mut db = empty_wal_db();

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
        assert_eq!(log, dolos_core::LogValue::Apply(expected_block));

        // ensure nothing else
        let origin = iter.next();
        assert!(origin.is_none());
    }

    #[test]
    fn test_rollback_undos() {
        let mut db = empty_wal_db();

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
        assert_eq!(log, dolos_core::LogValue::Mark(ChainPoint::Origin));
        println!("{seq}");

        for i in 0..=5 {
            let (seq, log) = wal.next().unwrap();
            println!("{seq}");

            match log {
                dolos_core::LogValue::Apply(RawBlock { slot, .. }) => assert_eq!(slot, i * 10),
                _ => panic!("expected apply"),
            }
        }

        for i in (3..=5).rev() {
            let (seq, log) = wal.next().unwrap();
            println!("{seq}");

            match log {
                dolos_core::LogValue::Undo(RawBlock { slot, .. }) => assert_eq!(slot, i * 10),
                _ => panic!("expected undo"),
            }
        }

        let (seq, log) = wal.next().unwrap();
        assert_eq!(log, dolos_core::LogValue::Mark(rollback_to));
        println!("{seq}");

        // ensure chain stops here
        assert!(wal.next().is_none());
    }
}
