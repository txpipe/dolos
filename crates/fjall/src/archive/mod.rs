//! Fjall-based archive store implementation for Dolos.
//!
//! The archive keeps block bodies in flat segment files ([`dolos_flatfiles`],
//! one zstd frame per body, named by the frame's physical location) and
//! holds the rows that point into the history — a blocks location table,
//! the derived-log namespaces, and the two index projections of the blocks
//! — in an LSM tree. Behavior is
//! pinned by the shared conformance suite (`tests/archive_conformance.rs`),
//! with the builtin memory archive as the oracle.
//!
//! ## Four Keyspace Design
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
//! 3. **`archive-tags`**: block tag queries (append-only). Key:
//!    `[dim_hash:8][key_hash:8][slot:8]` → empty. See [`tags`].
//!
//! 4. **`index-exact`**: exact-match lookups (block hash, block number, tx hash
//!    → slot). Key: `[dim_hash:8][key_data:var]` → `[slot:8]`. See [`exact`].
//!
//! The two index keyspaces are projections of the blocks and are written in
//! the same batch as the block locations, so the history and its lookups
//! commit together. They keep the compaction settings the standalone index
//! store gave them. A rollback removes its entries through
//! [`CoreArchiveWriter::undo_index`]; `truncate_front` does not touch them.
//!
//! ## Pruning the index keyspaces
//!
//! The slot is the *last* key component of a tag entry and the *value* of an
//! exact entry, so neither keyspace can be range-deleted by slot the way the
//! blocks and logs are. `prune_history` instead sweeps them: a full walk of
//! both keyspaces removing every entry below the cutoff. Under sliding
//! history the retained entry set is one window wide once the sweep has run,
//! so a walk costs the window, not the chain — and the sweep is amortized
//! across housekeeping rounds, running only when the cutoff has advanced by
//! a sixteenth of the window since the last one. A node restored from a
//! full-history stele pays one whole-keyspace scan on its first sweep.
//!
//! Unlike the redb writer, log batches are not reordered before insertion:
//! shuffling exists to work around redb's half-split of ascending B-tree
//! leaves, and an LSM memtable sorts its batch regardless of arrival order.

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::ops::{Bound, Range};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use dolos_core::{
    config::FjallArchiveConfig, key_hash, ArchiveError, ArchiveIndexDelta,
    ArchiveStore as CoreArchiveStore, ArchiveWriter as CoreArchiveWriter, BlockBody, BlockSlot,
    ChainPoint, EntityValue, ExactKind, IndexRecord, LogKey, Namespace, RawBlock, StateSchema,
    TagDimension, KEY_HASH_SIZE,
};
use fjall::{
    compaction::Leveled, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode,
    Readable, Snapshot,
};
use pallas::ledger::traverse::MultiEraBlock;

use dolos_flatfiles::{decode_locations, encode_locations, BlockLocation, FlatFileStore};

use crate::keys::{dim_prefix, hash_dimension, DIM_HASH_SIZE};
use crate::Error;

pub mod exact;
pub mod log_keys;
pub mod scan;
pub mod tags;

use log_keys::{
    build_log_key, build_temporal_bound, decode_log_key, namespace_end, namespace_start,
    PREFIXED_LOG_KEY_SIZE,
};

pub use exact::ExactRecordIterator as ExactIter;
pub use tags::{SlotIterator as SlotIter, TagRecordIterator as TagIter};

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Compaction settings of the two index keyspaces: what the standalone index
/// store applied to them before they moved here, kept so their behavior does
/// not change.
const INDEX_L0_THRESHOLD: u8 = 8;
const INDEX_MEMTABLE_SIZE_MB: usize = 128;

/// The index keyspaces are swept once the prune cutoff has advanced by this
/// fraction of the retained window since the last sweep.
const INDEX_SWEEP_WINDOW_DIVISOR: u64 = 16;

/// Most removals one sweep batch carries before it is committed and a new
/// one started, so the first sweep after a full-history restore — tens of
/// millions of entries — never builds one memory-resident batch.
const INDEX_SWEEP_BATCH_KEYS: usize = 100_000;

/// Keyspace names for the archive store
mod keyspace_names {
    /// Blocks location table (slot → packed BlockLocations)
    pub const BLOCKS: &str = "archive-blocks";
    /// Unified log namespaces keyspace
    pub const LOGS: &str = "archive-logs";
    /// Archive tags keyspace (append-only)
    pub const TAGS: &str = "archive-tags";
    /// Exact-match keyspace (block hash, tx hash, block number -> slot)
    pub const EXACT: &str = "index-exact";
}

fn io_err(e: std::io::Error) -> ArchiveError {
    ArchiveError::InternalError(e.to_string())
}

fn fjall_err(e: fjall::Error) -> ArchiveError {
    ArchiveError::from(Error::Fjall(e))
}

/// Fjall-based archive store.
///
/// Block bodies live in flat segment files; the location table, the log
/// namespaces and the two index keyspaces live in the LSM tree.
#[derive(Clone)]
pub struct ArchiveStore {
    db: Database,
    blocks: Keyspace,
    logs: Keyspace,
    tags: Keyspace,
    exact: Keyspace,
    flatfiles: Arc<FlatFileStore>,
    schema: Arc<StateSchema>,
    flush_on_commit: bool,
    /// The prune cutoff the index keyspaces were last swept up to; zero
    /// until the first sweep after open, which is why that one always runs.
    last_index_sweep: Arc<AtomicU64>,
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

        let index_opts = || {
            KeyspaceCreateOptions::default()
                .compaction_strategy(Arc::new(
                    Leveled::default().with_l0_threshold(INDEX_L0_THRESHOLD),
                ))
                .max_memtable_size((INDEX_MEMTABLE_SIZE_MB as u64) * 1024 * 1024)
        };
        let tags = db.keyspace(keyspace_names::TAGS, index_opts)?;
        let exact = db.keyspace(keyspace_names::EXACT, index_opts)?;

        Ok(Self {
            db,
            blocks,
            logs,
            tags,
            exact,
            flatfiles: Arc::new(flatfiles),
            schema: Arc::new(schema),
            flush_on_commit,
            last_index_sweep: Arc::new(AtomicU64::new(0)),
            _tempdir: tempdir,
        })
    }

    /// Which path each block batch took to the segment files, and the most
    /// an import batch held at once.
    pub fn append_stats(&self) -> dolos_flatfiles::AppendStats {
        self.flatfiles.append_stats()
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Per-keyspace disk footprint: `(name, bytes, path)`.
    pub fn disk_usage(&self) -> Vec<(&'static str, u64, std::path::PathBuf)> {
        [
            (keyspace_names::BLOCKS, &self.blocks),
            (keyspace_names::LOGS, &self.logs),
            (keyspace_names::TAGS, &self.tags),
            (keyspace_names::EXACT, &self.exact),
        ]
        .map(|(name, ks)| (name, ks.disk_space(), ks.path().to_path_buf()))
        .to_vec()
    }

    /// Fully compact every keyspace and sync the result to disk.
    ///
    /// The export path runs this as its sanitization step — the LSM analogue
    /// of redb's pre-export compaction — so an exported database directory
    /// carries settled segments rather than journal backlog and stale levels.
    pub fn compact(&self) -> Result<(), Error> {
        self.blocks.major_compact()?;
        self.logs.major_compact()?;
        self.tags.major_compact()?;
        self.exact.major_compact()?;
        self.db.persist(PersistMode::SyncAll)?;

        Ok(())
    }

    /// Gracefully shutdown the archive store.
    ///
    /// Persists all data and waits for outstanding flushes so fjall's drop
    /// implementation cannot hang on a full worker channel.
    pub fn shutdown(&self) -> Result<(), Error> {
        use std::time::Duration;

        tracing::info!("archive store: starting graceful shutdown");

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

    /// Remove every index entry below `prune_before` if the cutoff has moved
    /// far enough since the last sweep to be worth a walk of both keyspaces.
    ///
    /// The threshold is a sixteenth of the retained window (at least one
    /// slot), so a sliding node walks its window-sized index about sixteen
    /// times per window of chain instead of once per housekeeping round. The
    /// first prune after open always sweeps.
    fn sweep_indexes(
        &self,
        snapshot: &Snapshot,
        prune_before: BlockSlot,
        max_slots: u64,
    ) -> Result<(), ArchiveError> {
        let last = self.last_index_sweep.load(Ordering::Acquire);
        let threshold = (max_slots / INDEX_SWEEP_WINDOW_DIVISOR).max(1);

        if last != 0 && prune_before.saturating_sub(last) < threshold {
            tracing::debug!(
                cutoff_slot = prune_before,
                last_sweep = last,
                threshold,
                "index sweep deferred"
            );
            return Ok(());
        }

        let tags = self.sweep_below(snapshot, &self.tags, prune_before, |key, _| {
            tags::slot_of_entry(key)
        })?;
        let exact = self.sweep_below(snapshot, &self.exact, prune_before, |_, value| {
            exact::slot_of_entry(value)
        })?;

        self.last_index_sweep.store(prune_before, Ordering::Release);

        tracing::info!(
            cutoff_slot = prune_before,
            tags,
            exact,
            "swept archive index entries below cutoff"
        );

        Ok(())
    }

    /// Walk `keyspace` under `snapshot` and remove every entry whose slot,
    /// as `slot_of` reads it from the entry, is below `prune_before`.
    ///
    /// Removals are committed in batches of at most
    /// [`INDEX_SWEEP_BATCH_KEYS`], in sequence. Returns the number removed.
    fn sweep_below(
        &self,
        snapshot: &Snapshot,
        keyspace: &Keyspace,
        prune_before: BlockSlot,
        slot_of: impl Fn(&[u8], &[u8]) -> Result<BlockSlot, Error>,
    ) -> Result<u64, ArchiveError> {
        let mut removed = 0u64;
        let mut batch = self.db.batch();

        for guard in snapshot.iter(keyspace) {
            let (key, value) = guard.into_inner().map_err(fjall_err)?;

            if slot_of(&key, &value)? >= prune_before {
                continue;
            }

            batch.remove(keyspace, key);
            removed += 1;

            if batch.len() >= INDEX_SWEEP_BATCH_KEYS {
                let full = std::mem::replace(&mut batch, self.db.batch());
                full.durability(Some(PersistMode::Buffer))
                    .commit()
                    .map_err(fjall_err)?;
            }
        }

        if !batch.is_empty() {
            batch
                .durability(Some(PersistMode::Buffer))
                .commit()
                .map_err(fjall_err)?;
        }

        Ok(removed)
    }
}

/// Writer for batched archive operations.
///
/// Log writes and index entries go straight into the shared write batch:
/// fjall applies a batch's items to the memtable in order, and the memtable
/// replaces on an identical key, so the last write to a key within one batch
/// is the one that survives — the same end state redb's writer reaches by
/// collapsing each batch to its last writes. Blocks are buffered until
/// `commit` so their bodies can be appended to the segment files (and
/// fsynced) before any entry that points at them is committed, the same
/// crash window the redb writer keeps.
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
    append: Append,
    #[cfg(test)]
    fail_index_commit: bool,
}

/// How a writer's blocks reach the segment files at commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Append {
    /// One body after another on the committing thread: every ordinary
    /// writer, live sync and recovery included.
    Serial,
    /// Encoded in parallel windows on the shared Rayon pool — the writer an
    /// offline import asks for. Same frames at the same locations.
    Import,
}

impl ArchiveWriter {
    fn new(store: &ArchiveStore, append: Append) -> Self {
        Self {
            batch: Mutex::new(store.db.batch()),
            store: store.clone(),
            pending_blocks: Mutex::new(Vec::new()),
            overlay: Mutex::new(HashMap::new()),
            append,
            #[cfg(test)]
            fail_index_commit: false,
        }
    }

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
    /// survives until its last block is gone. The segment file is cut at the
    /// removed block's frame immediately.
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

    fn apply_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        let mut batch = self.batch.lock().unwrap();

        exact::apply(&mut batch, &self.store.exact, deltas)?;
        tags::apply(&mut batch, &self.store.tags, deltas)?;

        Ok(())
    }

    fn undo_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        let mut batch = self.batch.lock().unwrap();

        exact::undo(&mut batch, &self.store.exact, deltas)?;
        tags::undo(&mut batch, &self.store.tags, deltas)?;

        Ok(())
    }

    fn append_prehashed(
        &self,
        records: impl IntoIterator<Item = IndexRecord>,
    ) -> Result<(), ArchiveError> {
        let mut batch = self.batch.lock().unwrap();

        // Records arrive sorted, hence grouped by dimension/kind: hash each
        // group's dimension once instead of once per record. The cached
        // dimension is owned because a record no longer outlives its own
        // iteration step — the clone is one per group, not one per record.
        let mut tag_dim: Option<(Cow<'static, str>, [u8; DIM_HASH_SIZE])> = None;
        let mut exact_dim: Option<(ExactKind, [u8; DIM_HASH_SIZE])> = None;

        for record in records {
            match record {
                IndexRecord::Tag(tag) => {
                    let dim_hash = match &tag_dim {
                        Some((cached, hash)) if cached == &tag.dimension => *hash,
                        _ => {
                            let hash = hash_dimension(dim_prefix::BLOCK, tag.dimension());
                            tag_dim = Some((tag.dimension.clone(), hash));
                            hash
                        }
                    };

                    tags::insert_prehashed(&mut batch, &self.store.tags, &tag, dim_hash);
                }
                IndexRecord::Exact(exact) => {
                    let dim_hash = match exact_dim {
                        Some((cached, hash)) if cached == exact.kind => hash,
                        _ => {
                            let hash = hash_dimension(dim_prefix::EXACT, exact.kind.as_str());
                            exact_dim = Some((exact.kind, hash));
                            hash
                        }
                    };

                    exact::insert_prehashed(&mut batch, &self.store.exact, &exact, dim_hash);
                }
            }
        }

        Ok(())
    }

    fn commit(self) -> Result<(), ArchiveError> {
        // 1. Batch-append all pending blocks to flat files (fsync inside).
        // 2. Insert all location entries into the write batch.
        // 3. Commit the batch (log rows and index entries are already in it).
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

            let locations = match self.append {
                Append::Serial => self.store.flatfiles.append_batch(&items),
                Append::Import => self.store.flatfiles.import_batch(&items),
            }
            .map_err(io_err)?;

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

        #[cfg(test)]
        if self.fail_index_commit {
            return Err(io_err(std::io::Error::other(
                "injected index commit failure",
            )));
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
/// points at the original stays exactly where it is, and the frame just
/// appended is the one nothing points at — dead space, not corruption.
/// Repointing would move an index entry forward in the segment past a block
/// it precedes in the chain, and `undo` truncates at the offset it removes,
/// so that block's bytes would go with the cut. A location names a frame,
/// not a body length, so the comparison decodes each candidate.
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
    type SlotIter = SlotIter;
    type TagIter = TagIter;
    type ExactIter = ExactIter;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        Ok(ArchiveWriter::new(self, Append::Serial))
    }

    fn start_import_writer(&self) -> Result<Self::Writer, ArchiveError> {
        Ok(ArchiveWriter::new(self, Append::Import))
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

        // The index keyspaces cannot be range-deleted by slot; they are swept
        // under the same snapshot, after the rows that point at the blocks.
        self.sweep_indexes(&snapshot, prune_before, max_slots)?;

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

    fn slot_by_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        let snapshot = self.db.snapshot();
        exact::get_by_block_hash(&snapshot, &self.exact, block_hash).map_err(ArchiveError::from)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        let snapshot = self.db.snapshot();
        exact::get_by_block_number(&snapshot, &self.exact, number).map_err(ArchiveError::from)
    }

    fn slot_by_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        let snapshot = self.db.snapshot();
        exact::get_by_tx_hash(&snapshot, &self.exact, tx_hash).map_err(ArchiveError::from)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        // The stored key form is the write path's, not this method's: the same
        // `key_hash` an insert used, so a query cannot look under bytes an
        // insert would not have written. `None` is a key with no valid stored
        // form — today only a `metadata` label that is not eight bytes wide —
        // which is a malformed query rather than an empty result.
        let Some(hash) = key_hash(dimension, key) else {
            return Err(Error::Codec(format!(
                "{dimension} key must be {KEY_HASH_SIZE} bytes, got {}",
                key.len(),
            ))
            .into());
        };

        let snapshot = self.db.snapshot();

        SlotIter::new(&snapshot, &self.tags, dimension, hash, start, end)
            .map_err(ArchiveError::from)
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
        // One snapshot for the whole traversal: every dimension prefix is read
        // from the same MVCC view, so the record set is consistent even under
        // concurrent writes.
        let snapshot = self.db.snapshot();

        Ok(TagIter::new(snapshot, &self.tags, dimensions, slots))
    }

    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, ArchiveError> {
        let snapshot = self.db.snapshot();

        Ok(ExactIter::new(snapshot, &self.exact, slots))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn point(slot: u64) -> ChainPoint {
        ChainPoint::Specific(slot, pallas::crypto::hash::Hash::new([0u8; 32]))
    }

    fn body(slot: BlockSlot, tag: u8) -> RawBlock {
        Arc::new(
            format!("block at slot {slot} tag {tag} ")
                .repeat(12)
                .into_bytes(),
        )
    }

    fn write(store: &ArchiveStore, blocks: &[(BlockSlot, RawBlock)]) {
        let writer = store.start_writer().unwrap();
        for (slot, body) in blocks {
            writer.apply(&point(*slot), body).unwrap();
        }
        writer.commit().unwrap();
    }

    fn locations(store: &ArchiveStore, slot: BlockSlot) -> Vec<BlockLocation> {
        store.stored_locations(&store.db.snapshot(), slot).unwrap()
    }

    fn segment_bytes(store: &ArchiveStore, segment: u32) -> Vec<u8> {
        std::fs::read(store.flatfiles.segment_path(segment)).unwrap()
    }

    #[test]
    fn offline_index_failure_leaves_only_unindexed_frames_and_retry_keeps_original_locations() {
        let store = ArchiveStore::for_tempdir(StateSchema::default()).unwrap();
        write(&store, &[(1, body(1, 0))]);
        let original = locations(&store, 1);
        let mut writer = store.start_import_writer().unwrap();
        writer.apply(&point(2), &body(2, 0)).unwrap();
        writer.fail_index_commit = true;
        assert!(writer.commit().is_err());
        assert!(locations(&store, 2).is_empty());
        let dead_end = segment_bytes(&store, 0).len() as u64;
        let writer = store.start_import_writer().unwrap();
        writer.apply(&point(1), &body(1, 0)).unwrap();
        writer.apply(&point(2), &body(2, 0)).unwrap();
        writer.commit().unwrap();
        assert_eq!(locations(&store, 1), original);
        assert!(locations(&store, 2)[0].offset >= dead_end);
        assert_eq!(store.get_block_by_slot(&2).unwrap().unwrap(), *body(2, 0));
    }

    #[test]
    fn a_repeated_import_keeps_the_original_frame_and_leaves_the_copy_unnamed() {
        let store = ArchiveStore::for_tempdir(StateSchema::default()).unwrap();
        write(&store, &[(5, body(5, 0)), (9, body(9, 0))]);
        let original = locations(&store, 5);
        let len_before = segment_bytes(&store, 0).len();

        write(&store, &[(5, body(5, 0))]);

        assert_eq!(locations(&store, 5), original);
        assert!(
            segment_bytes(&store, 0).len() > len_before,
            "the copy is appended"
        );
        assert_eq!(store.get_block_by_slot(&5).unwrap().unwrap(), *body(5, 0));

        // A different body at the same slot is a second block, newest first.
        write(&store, &[(5, body(5, 1))]);
        let both = locations(&store, 5);
        assert_eq!(both.len(), 2);
        assert_eq!(both[1], original[0]);
        assert_eq!(
            store.get_blocks_by_slot(&5).unwrap(),
            vec![(*body(5, 0)).clone(), (*body(5, 1)).clone()]
        );
    }

    #[test]
    fn a_rollback_cuts_at_the_frame_and_retained_frames_are_not_rewritten() {
        let store = ArchiveStore::for_tempdir(StateSchema::default()).unwrap();
        write(&store, &[(1, body(1, 0)), (2, body(2, 0)), (3, body(3, 0))]);
        let before = segment_bytes(&store, 0);
        let cut = locations(&store, 3)[0].offset;
        let retained = locations(&store, 2);

        let writer = store.start_writer().unwrap();
        writer.undo(&point(3)).unwrap();
        writer.commit().unwrap();

        assert_eq!(segment_bytes(&store, 0), before[..cut as usize]);
        assert!(locations(&store, 3).is_empty());

        write(&store, &[(3, body(3, 1))]);
        let after = segment_bytes(&store, 0);
        assert_eq!(after[..cut as usize], before[..cut as usize]);
        assert_eq!(locations(&store, 3)[0].offset, cut);
        assert_eq!(locations(&store, 2), retained);
        assert_eq!(store.get_block_by_slot(&3).unwrap().unwrap(), *body(3, 1));
        assert_eq!(store.get_block_by_slot(&2).unwrap().unwrap(), *body(2, 0));
    }
}
