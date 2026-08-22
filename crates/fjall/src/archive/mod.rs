//! Fjall-based archive store implementation for Dolos.
//!
//! Benchmark vehicle for the archive-index backend evaluation: the redb
//! archive keeps block bodies in flat segment files and holds two kinds of
//! index rows — a blocks location table and the derived-log namespaces — in
//! a copy-on-write B-tree. This module keeps the segment files (reusing the
//! redb crate's [`flatfiles`] store as-is) and moves only the index rows
//! into an LSM tree.
//!
//! ## Two Keyspace Design
//!
//! 1. **`archive-blocks`**: slot → packed 16-byte [`BlockLocation`]s, newest
//!    first. Key is the 8-byte big-endian slot; the value encoding is
//!    byte-identical to the redb blocks table, including the multi-location
//!    case (a Byron epoch-boundary block sharing its slot with the first main
//!    block of the epoch it opens).
//!
//! 2. **`archive-logs`**: all log namespaces with namespace hash prefix. Key:
//!    `[ns_hash:8][log_key:40]` (48 bytes), mirroring the state store's unified
//!    entities keyspace — one keyspace, not one per namespace, because
//!    per-namespace LSM trees blow the file-descriptor limit during heavy
//!    compaction.
//!
//! Unlike the redb writer, log batches are not reordered before insertion:
//! shuffling exists to work around redb's half-split of ascending B-tree
//! leaves, and an LSM memtable sorts its batch regardless of arrival order.

use std::collections::{HashMap, VecDeque};
use std::ops::{Bound, Range};
use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    config::FjallArchiveConfig, ArchiveError, ArchiveStore as CoreArchiveStore,
    ArchiveWriter as CoreArchiveWriter, BlockBody, BlockSlot, ChainPoint, EntityValue, LogKey,
    Namespace, RawBlock, StateSchema,
};
use fjall::{
    compaction::Leveled, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode,
    Readable, Snapshot,
};
use pallas::ledger::traverse::MultiEraBlock;

use dolos_redb3::archive::flatfiles::{
    decode_locations, encode_locations, BlockLocation, FlatFileStore,
};

use crate::Error;

pub mod log_keys;

use log_keys::{
    build_log_key, build_temporal_bound, decode_log_key, namespace_end, namespace_start,
    PREFIXED_LOG_KEY_SIZE,
};

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Keyspace names for the archive store
mod keyspace_names {
    /// Blocks location table (slot → packed BlockLocations)
    pub const BLOCKS: &str = "archive-blocks";
    /// Unified log namespaces keyspace
    pub const LOGS: &str = "archive-logs";
}

fn io_err(e: std::io::Error) -> ArchiveError {
    ArchiveError::InternalError(e.to_string())
}

fn fjall_err(e: fjall::Error) -> ArchiveError {
    ArchiveError::from(Error::Fjall(e))
}

/// Fjall-based archive store.
///
/// Block bodies live in flat segment files (shared, byte-identical layout
/// with the redb backend); only the location table and the log namespaces
/// live in the LSM tree.
#[derive(Clone)]
pub struct ArchiveStore {
    db: Database,
    blocks: Keyspace,
    logs: Keyspace,
    flatfiles: Arc<FlatFileStore>,
    schema: Arc<StateSchema>,
    flush_on_commit: bool,
    _tempdir: Option<Arc<tempfile::TempDir>>,
}

impl ArchiveStore {
    /// Open or create an archive store.
    ///
    /// `path` is the archive **directory** (e.g. `<storage.path>/archive/`).
    /// The fjall database is stored at `<path>/index`. Segment files are
    /// stored in `config.blocks_path` if set, otherwise in `<path>/`.
    pub fn open(
        schema: StateSchema,
        path: impl AsRef<Path>,
        config: &FjallArchiveConfig,
    ) -> Result<Self, Error> {
        let path = path.as_ref();
        std::fs::create_dir_all(path).map_err(|e| Error::Io(e.to_string()))?;

        let cache_size = config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB);
        let cache_bytes = (cache_size * 1024 * 1024) as u64;

        let mut builder = Database::builder(path.join("index")).cache_size(cache_bytes);

        if let Some(journal_mb) = config.max_journal_size {
            builder = builder.max_journaling_size((journal_mb as u64) * 1024 * 1024);
        }

        if let Some(threads) = config.worker_threads {
            builder = builder.worker_threads(threads);
        }

        let db = builder.open()?;

        let segments_dir = config
            .blocks_path
            .clone()
            .unwrap_or_else(|| path.to_path_buf());

        let flatfiles = FlatFileStore::new(segments_dir).map_err(|e| Error::Io(e.to_string()))?;

        Self::from_database(
            db,
            schema,
            flatfiles,
            config.flush_on_commit.unwrap_or(false),
            config.l0_threshold,
            config.memtable_size_mb,
            None,
        )
    }

    /// Create an archive store over temporary directories, for tests.
    ///
    /// Fjall has no in-memory backend; the tempdir guard is held by the
    /// store and cleaned up when the last clone drops.
    pub fn for_tempdir(schema: StateSchema) -> Result<Self, Error> {
        let dir = tempfile::TempDir::new().map_err(|e| Error::Io(e.to_string()))?;

        let db = Database::builder(dir.path().join("index")).open()?;

        let flatfiles =
            FlatFileStore::new(dir.path().to_path_buf()).map_err(|e| Error::Io(e.to_string()))?;

        let mut store = Self::from_database(db, schema, flatfiles, false, None, None, None)?;
        store._tempdir = Some(Arc::new(dir));

        Ok(store)
    }

    fn from_database(
        db: Database,
        schema: StateSchema,
        flatfiles: FlatFileStore,
        flush_on_commit: bool,
        l0_threshold: Option<u8>,
        memtable_size_mb: Option<usize>,
        tempdir: Option<Arc<tempfile::TempDir>>,
    ) -> Result<Self, Error> {
        let build_opts = || {
            let mut opts = KeyspaceCreateOptions::default();

            if let Some(threshold) = l0_threshold {
                opts = opts
                    .compaction_strategy(Arc::new(Leveled::default().with_l0_threshold(threshold)));
            }

            if let Some(size_mb) = memtable_size_mb {
                opts = opts.max_memtable_size((size_mb as u64) * 1024 * 1024);
            }

            opts
        };

        let blocks = db.keyspace(keyspace_names::BLOCKS, build_opts)?;
        let logs = db.keyspace(keyspace_names::LOGS, build_opts)?;

        Ok(Self {
            db,
            blocks,
            logs,
            flatfiles: Arc::new(flatfiles),
            schema: Arc::new(schema),
            flush_on_commit,
            _tempdir: tempdir,
        })
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Per-keyspace disk footprint: `(name, bytes, path)`.
    pub fn disk_usage(&self) -> Vec<(&'static str, u64, std::path::PathBuf)> {
        vec![
            (
                keyspace_names::BLOCKS,
                self.blocks.disk_space(),
                self.blocks.path().to_path_buf(),
            ),
            (
                keyspace_names::LOGS,
                self.logs.disk_space(),
                self.logs.path().to_path_buf(),
            ),
        ]
    }

    /// Gracefully shutdown the archive store.
    ///
    /// Persists all data and waits for outstanding flushes so fjall's drop
    /// implementation cannot hang on a full worker channel.
    pub fn shutdown(&self) -> Result<(), Error> {
        use std::time::Duration;

        tracing::info!("archive store: starting graceful shutdown");
        tracing::info!(
            flatfile_append_secs =
                dolos_redb3::archive::flatfiles::cumulative_append_time().as_secs_f64(),
            "archive store: cumulative flat-file append time"
        );

        self.db.persist(PersistMode::SyncAll)?;

        let mut wait_count = 0;
        while self.db.outstanding_flushes() > 0 {
            std::thread::sleep(Duration::from_millis(10));
            wait_count += 1;
            if wait_count % 100 == 0 {
                tracing::debug!(
                    "archive store: waiting for {} outstanding flushes",
                    self.db.outstanding_flushes()
                );
            }
            if wait_count > 6000 {
                tracing::warn!("archive store: timeout waiting for flushes, proceeding");
                break;
            }
        }

        tracing::info!("archive store: graceful shutdown complete");
        Ok(())
    }

    fn check_namespace(&self, ns: Namespace) -> Result<(), ArchiveError> {
        if !self.schema.contains_key(ns) {
            return Err(ArchiveError::NamespaceNotFound(ns));
        }
        Ok(())
    }

    /// Every location recorded at `slot`, in stored order (newest first).
    fn stored_locations(
        &self,
        snapshot: &Snapshot,
        slot: BlockSlot,
    ) -> Result<Vec<BlockLocation>, ArchiveError> {
        let value = snapshot
            .get(&self.blocks, slot.to_be_bytes())
            .map_err(fjall_err)?;

        match value {
            Some(bytes) => Ok(decode_locations(&bytes).collect()),
            None => Ok(vec![]),
        }
    }
}

/// Writer for batched archive operations.
///
/// Log writes go straight into the shared write batch: fjall applies a
/// batch's items to the memtable in order, and the memtable replaces on an
/// identical key, so the last write to a key within one batch is the one
/// that survives — the same end state redb's writer reaches by collapsing
/// each batch to its last writes. Blocks are buffered until `commit` so
/// their bodies can be appended to the segment files (and fsynced) before
/// any index entry that points at them is committed, the same crash window
/// the redb writer keeps.
///
/// `overlay` is the writer-local view of every blocks-table slot this
/// writer has touched. The write batch is invisible to reads until commit,
/// so a second block landing at the same slot within one batch (a Byron
/// boundary) and consecutive `undo`s at one slot resolve against the
/// overlay first and the committed state second — the reads redb gets for
/// free from its transaction seeing its own writes.
pub struct ArchiveWriter {
    store: ArchiveStore,
    batch: Mutex<OwnedWriteBatch>,
    pending_blocks: Mutex<Vec<(ChainPoint, RawBlock)>>,
    overlay: Mutex<HashMap<BlockSlot, Vec<BlockLocation>>>,
}

impl ArchiveWriter {
    fn resolve_locations(
        &self,
        overlay: &HashMap<BlockSlot, Vec<BlockLocation>>,
        slot: BlockSlot,
    ) -> Result<Vec<BlockLocation>, ArchiveError> {
        if let Some(locations) = overlay.get(&slot) {
            return Ok(locations.clone());
        }

        let snapshot = self.store.db.snapshot();
        self.store.stored_locations(&snapshot, slot)
    }
}

impl CoreArchiveWriter for ArchiveWriter {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError> {
        self.pending_blocks
            .lock()
            .unwrap()
            .push((point.clone(), block.clone()));
        Ok(())
    }

    fn write_log(
        &self,
        ns: Namespace,
        key: &LogKey,
        value: &EntityValue,
    ) -> Result<(), ArchiveError> {
        // The namespace is resolved here so an unknown one fails at the call
        // that names it rather than at commit.
        self.store.check_namespace(ns)?;

        let mut batch = self.batch.lock().unwrap();
        batch.insert(&self.store.logs, build_log_key(ns, key), value.as_slice());

        Ok(())
    }

    /// Undo the block at `point`.
    ///
    /// A rollback walks the chain backwards, so at a slot holding more than
    /// one block the one to remove is the newest — position 0 — and the slot
    /// survives until its last block is gone. The segment file is truncated
    /// at the removed block's offset immediately, mirroring the redb writer.
    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError> {
        let slot = point.slot();

        let mut overlay = self.overlay.lock().unwrap();
        let mut locations = self.resolve_locations(&overlay, slot)?;

        if locations.is_empty() {
            return Ok(());
        }

        let removed = locations.remove(0);

        let mut batch = self.batch.lock().unwrap();
        if locations.is_empty() {
            batch.remove(&self.store.blocks, slot.to_be_bytes());
        } else {
            batch.insert(
                &self.store.blocks,
                slot.to_be_bytes(),
                encode_locations(&locations),
            );
        }
        drop(batch);

        overlay.insert(slot, locations);

        self.store
            .flatfiles
            .truncate(removed.segment_id, removed.offset)
            .map_err(io_err)?;

        Ok(())
    }

    fn commit(self) -> Result<(), ArchiveError> {
        // 1. Batch-append all pending blocks to flat files (fsync inside).
        // 2. Insert all index entries into the write batch.
        // 3. Commit the batch (log rows are already in it).
        let pending = self.pending_blocks.into_inner().unwrap();
        let mut overlay = self.overlay.into_inner().unwrap();
        let mut batch = self.batch.into_inner().unwrap();

        if !pending.is_empty() {
            let items: Vec<(u32, &[u8])> = pending
                .iter()
                .map(|(point, block)| {
                    let segment_id = BlockLocation::segment_for_slot(point.slot());
                    (segment_id, block.as_slice())
                })
                .collect();

            let locations = self.store.flatfiles.append_batch(&items).map_err(io_err)?;

            let snapshot = self.store.db.snapshot();

            for (i, (point, body)) in pending.iter().enumerate() {
                let slot = point.slot();
                let incoming = locations[i];

                let existing = match overlay.get(&slot) {
                    Some(locations) => locations.clone(),
                    None => self.store.stored_locations(&snapshot, slot)?,
                };

                let merged = merge_location(existing, incoming, body, &self.store.flatfiles)?;

                batch.insert(
                    &self.store.blocks,
                    slot.to_be_bytes(),
                    encode_locations(&merged),
                );
                overlay.insert(slot, merged);
            }
        }

        let batch = batch.durability(Some(PersistMode::Buffer));
        batch.commit().map_err(fjall_err)?;

        if self.store.flush_on_commit {
            self.store
                .db
                .persist(PersistMode::Buffer)
                .map_err(fjall_err)?;
        }

        Ok(())
    }
}

/// Fold a newly written block into the locations already held at its slot.
///
/// An identical body means this block is being written again (a resumed
/// restore rewriting the layer it was in the middle of): the entry that
/// points at the original stays exactly where it is, and the copy just
/// appended is the one nothing points at — dead space, not corruption.
/// Repointing would move an index entry forward in the segment past a block
/// it precedes in the chain, and `undo` truncates at the offset it removes,
/// so that block's bytes would go with the cut.
///
/// Anything else is a second block at the same slot, and it takes position
/// 0: blocks arrive in chain order, so the newcomer is the one the slot
/// should resolve to.
fn merge_location(
    existing: Vec<BlockLocation>,
    incoming: BlockLocation,
    body: &RawBlock,
    flatfiles: &FlatFileStore,
) -> Result<Vec<BlockLocation>, ArchiveError> {
    if existing.is_empty() {
        return Ok(vec![incoming]);
    }

    for loc in existing.iter() {
        if loc.length as usize != body.len() {
            continue;
        }

        let stored = flatfiles.read(loc).map_err(io_err)?;

        if stored == **body {
            return Ok(existing);
        }
    }

    let mut merged = existing;
    merged.insert(0, incoming);

    Ok(merged)
}

/// Iterator over a range of log rows within one namespace.
///
/// Fuses after the first error: a range scan that failed mid-way must not
/// look like a shorter, complete result to a consumer that signs or
/// compares what it read.
pub struct LogIter {
    inner: fjall::Iter,
    done: bool,
}

impl Iterator for LogIter {
    type Item = Result<(LogKey, EntityValue), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }

        let guard = self.inner.next()?;

        match guard.into_inner() {
            Ok((key, value)) => {
                if key.len() < PREFIXED_LOG_KEY_SIZE {
                    self.done = true;
                    return Some(Err(ArchiveError::InternalError(format!(
                        "malformed archive log key of {} bytes",
                        key.len()
                    ))));
                }

                Some(Ok((decode_log_key(&key), value.to_vec())))
            }
            Err(e) => {
                self.done = true;
                Some(Err(fjall_err(e)))
            }
        }
    }
}

/// Empty iterator for the unused multimap read surface.
///
/// The trait declares the associated type but no trait method returns it,
/// and no log namespace is a multimap today.
pub struct EmptyEntityValueIter;

impl Iterator for EmptyEntityValueIter {
    type Item = Result<EntityValue, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

type IndexEntry = (BlockSlot, BlockLocation);

/// Iterator over a range of blocks, reading lazily from flat files.
///
/// A slot can hold more than one block, so an index entry expands to a run
/// of locations and whatever is left of the entry each end is working
/// through is buffered here. When one end's range runs dry it drains the
/// other end's buffer, which is what keeps a forward and a backward walk
/// from yielding a block twice or dropping one where they meet.
pub struct BlockIter {
    inner: fjall::Iter,
    front: VecDeque<IndexEntry>,
    back: VecDeque<IndexEntry>,
    flatfiles: Arc<FlatFileStore>,
}

impl BlockIter {
    fn decode_guard(guard: fjall::Guard) -> Option<(BlockSlot, Vec<BlockLocation>)> {
        let (key, value) = guard.into_inner().ok()?;

        let slot_bytes: [u8; 8] = key.as_ref().get(..8)?.try_into().ok()?;
        let slot = u64::from_be_bytes(slot_bytes);

        Some((slot, decode_locations(&value).collect()))
    }

    fn next_entry(&mut self) -> Option<IndexEntry> {
        loop {
            if let Some(entry) = self.front.pop_front() {
                return Some(entry);
            }

            let Some(guard) = self.inner.next() else {
                return self.back.pop_front();
            };

            let (slot, locations) = Self::decode_guard(guard)?;

            self.front
                .extend(locations.into_iter().rev().map(|loc| (slot, loc)));
        }
    }

    fn next_entry_back(&mut self) -> Option<IndexEntry> {
        loop {
            if let Some(entry) = self.back.pop_back() {
                return Some(entry);
            }

            let Some(guard) = self.inner.next_back() else {
                return self.front.pop_back();
            };

            let (slot, locations) = Self::decode_guard(guard)?;

            self.back
                .extend(locations.into_iter().rev().map(|loc| (slot, loc)));
        }
    }
}

impl Iterator for BlockIter {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let (slot, loc) = self.next_entry()?;
            match self.flatfiles.read(&loc) {
                Ok(data) => return Some((slot, data)),
                Err(_) => continue, // skip unreadable blocks
            }
        }
    }
}

impl DoubleEndedIterator for BlockIter {
    fn next_back(&mut self) -> Option<Self::Item> {
        loop {
            let (slot, loc) = self.next_entry_back()?;
            match self.flatfiles.read(&loc) {
                Ok(data) => return Some((slot, data)),
                Err(_) => continue,
            }
        }
    }
}

impl dolos_core::archive::Skippable for BlockIter {
    fn skip_forward(&mut self, n: usize) {
        for _ in 0..n {
            if self.next_entry().is_none() {
                break;
            }
        }
    }

    fn skip_backward(&mut self, n: usize) {
        for _ in 0..n {
            if self.next_entry_back().is_none() {
                break;
            }
        }
    }
}

impl CoreArchiveStore for ArchiveStore {
    type BlockIter<'a> = BlockIter;
    type Writer = ArchiveWriter;
    type LogIter = LogIter;
    type EntityValueIter = EmptyEntityValueIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        Ok(ArchiveWriter {
            batch: Mutex::new(self.db.batch()),
            store: self.clone(),
            pending_blocks: Mutex::new(Vec::new()),
            overlay: Mutex::new(HashMap::new()),
        })
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError> {
        self.check_namespace(ns)?;

        let snapshot = self.db.snapshot();
        let mut out = Vec::with_capacity(keys.len());

        for key in keys {
            let value = snapshot
                .get(&self.logs, build_log_key(ns, key))
                .map_err(fjall_err)?;
            out.push(value.map(|v| v.as_ref().to_vec()));
        }

        Ok(out)
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: Range<LogKey>,
    ) -> Result<Self::LogIter, ArchiveError> {
        self.check_namespace(ns)?;

        let start = build_log_key(ns, &range.start);
        let end = build_log_key(ns, &range.end);

        let snapshot = self.db.snapshot();
        let inner = snapshot.range(&self.logs, start.as_slice()..end.as_slice());

        Ok(LogIter { inner, done: false })
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        let snapshot = self.db.snapshot();
        let locations = self.stored_locations(&snapshot, *slot)?;

        match locations.first() {
            Some(loc) => Ok(Some(self.flatfiles.read(loc).map_err(io_err)?)),
            None => Ok(None),
        }
    }

    /// Every block the archive holds at `slot`, in chain order.
    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        let snapshot = self.db.snapshot();
        let locations = self.stored_locations(&snapshot, *slot)?;

        locations
            .into_iter()
            .rev()
            .map(|loc| self.flatfiles.read(&loc).map_err(io_err))
            .collect()
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        let start = match from {
            Some(slot) => Bound::Included(slot.to_be_bytes()),
            None => Bound::Unbounded,
        };
        let end = match to {
            Some(slot) => Bound::Excluded(slot.to_be_bytes()),
            None => Bound::Unbounded,
        };

        let snapshot = self.db.snapshot();
        let inner = snapshot.range(&self.blocks, (start, end));

        Ok(BlockIter {
            inner,
            front: VecDeque::new(),
            back: VecDeque::new(),
            flatfiles: self.flatfiles.clone(),
        })
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        for point in intersect {
            let ChainPoint::Specific(slot, hash) = point else {
                return Ok(Some(ChainPoint::Origin));
            };

            // A slot can hold more than one block, and an intersect names one
            // of them by hash, so every block recorded there is a candidate.
            for body in self.get_blocks_by_slot(slot)? {
                let decoded =
                    MultiEraBlock::decode(&body).map_err(ArchiveError::BlockDecodingError)?;

                if decoded.hash().eq(hash) {
                    return Ok(Some(ChainPoint::Specific(decoded.slot(), decoded.hash())));
                }
            }
        }

        Ok(None)
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        let snapshot = self.db.snapshot();

        let Some(guard) = snapshot.last_key_value(&self.blocks) else {
            return Ok(None);
        };

        let (key, value) = guard.into_inner().map_err(fjall_err)?;

        let slot_bytes: [u8; 8] = key
            .as_ref()
            .get(..8)
            .and_then(|b| b.try_into().ok())
            .ok_or_else(|| ArchiveError::InternalError("malformed blocks key".to_string()))?;
        let slot = u64::from_be_bytes(slot_bytes);

        let loc = BlockLocation::from_bytes(&value);
        let body = self.flatfiles.read(&loc).map_err(io_err)?;

        Ok(Some((slot, body)))
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        let snapshot = self.db.snapshot();

        let first = snapshot
            .first_key_value(&self.blocks)
            .map(|guard| guard.key())
            .transpose()
            .map_err(fjall_err)?;

        let Some(first) = first else {
            tracing::debug!("no start point found on chain, skipping housekeeping");
            return Ok(true);
        };

        let last = snapshot
            .last_key_value(&self.blocks)
            .map(|guard| guard.key())
            .transpose()
            .map_err(fjall_err)?;

        let Some(last) = last else {
            tracing::debug!("no tip found on chain, skipping housekeeping");
            return Ok(true);
        };

        let start = u64::from_be_bytes(first.as_ref()[..8].try_into().unwrap());
        let last = u64::from_be_bytes(last.as_ref()[..8].try_into().unwrap());

        let delta = last.saturating_sub(start);
        let excess = delta.saturating_sub(max_slots);

        if excess == 0 {
            tracing::debug!(delta, max_slots, "no pruning necessary on chain");
            return Ok(true);
        }

        let (done, max_prune) = match max_prune {
            Some(max) => (excess <= max, core::cmp::min(excess, max)),
            None => (true, excess),
        };

        let prune_before = start + max_prune;

        tracing::info!(
            cutoff_slot = prune_before,
            start,
            excess,
            "pruning archive for excess history"
        );

        let mut batch = self.db.batch();

        // Blocks strictly before the cutoff slot.
        let to_remove = snapshot.range(&self.blocks, ..prune_before.to_be_bytes().to_vec());
        for guard in to_remove {
            let key = guard.key().map_err(fjall_err)?;
            batch.remove(&self.blocks, key);
        }

        let threshold_segment = BlockLocation::segment_for_slot(prune_before);
        self.flatfiles
            .delete_segments_before(threshold_segment)
            .map_err(io_err)?;

        // Log rows with a temporal prefix strictly before the cutoff, per
        // namespace — the price of the shared keyspace's hash prefix.
        for (&ns, _) in self.schema.iter() {
            let range = snapshot.range(
                &self.logs,
                namespace_start(ns)..build_temporal_bound(ns, prune_before),
            );
            for guard in range {
                let key = guard.key().map_err(fjall_err)?;
                batch.remove(&self.logs, key);
            }
        }

        let batch = batch.durability(Some(PersistMode::Buffer));
        batch.commit().map_err(fjall_err)?;

        Ok(done)
    }

    /// Drop everything the archive holds after `after`.
    ///
    /// The cut is by slot: the block at `after`'s slot survives (including a
    /// second block sharing that slot), while log rows *at* the slot go with
    /// the cut — the exact boundary redb's `remove_after` draws by comparing
    /// full log keys against the bare 8-byte temporal prefix.
    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        let slot = after.slot();
        let snapshot = self.db.snapshot();

        let mut batch = self.db.batch();

        // Find the earliest location after `slot` to know where to truncate
        // the segment file: a slot holding more than one block contributes
        // all of them, so the cut lands before the earliest body it has to
        // drop and none survives.
        let mut earliest_after: Option<BlockLocation> = None;

        if let Some(from) = slot.checked_add(1) {
            let range = snapshot.range(&self.blocks, from.to_be_bytes().to_vec()..);

            for guard in range {
                let (key, value) = guard.into_inner().map_err(fjall_err)?;

                for loc in decode_locations(&value) {
                    match &earliest_after {
                        None => earliest_after = Some(loc),
                        Some(prev) => {
                            if loc.segment_id < prev.segment_id
                                || (loc.segment_id == prev.segment_id && loc.offset < prev.offset)
                            {
                                earliest_after = Some(loc);
                            }
                        }
                    }
                }

                batch.remove(&self.blocks, key);
            }
        }

        // Log rows with a temporal prefix at or after the cut slot.
        for (&ns, _) in self.schema.iter() {
            let range = snapshot.range(
                &self.logs,
                build_temporal_bound(ns, slot)..namespace_end(ns),
            );
            for guard in range {
                let key = guard.key().map_err(fjall_err)?;
                batch.remove(&self.logs, key);
            }
        }

        if let Some(loc) = earliest_after {
            self.flatfiles
                .truncate(loc.segment_id, loc.offset)
                .map_err(io_err)?;
        }

        let batch = batch.durability(Some(PersistMode::Buffer));
        batch.commit().map_err(fjall_err)?;

        Ok(())
    }
}
