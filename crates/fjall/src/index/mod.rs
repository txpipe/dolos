//! Fjall-based index store implementation for Dolos (chain-agnostic).
//!
//! This module provides an implementation of the `IndexStore` trait using fjall,
//! an LSM-tree based embedded database. This is optimized for write-heavy workloads
//! with many keys, which is ideal for blockchain index data.
//!
//! ## Three Keyspace Design
//!
//! Indexes are organized into three keyspaces based on access patterns:
//!
//! 1. **`index-cursor`**: Chain position tracking (separate for different access pattern)
//!
//! 2. **`index-exact`**: Exact-match lookups (point queries)
//!    Key format: `[dim_hash:8][key_data:var]` -> `[slot:8]`
//!
//! 3. **`index-tags`**: Tag-based prefix scan queries
//!    - UTxO tags: `[dim_hash:8][lookup_key:var][txo_ref:36]` -> empty
//!    - Block tags: `[dim_hash:8][xxh3(tag_key):8][slot:8]` -> empty
//!
//! The `dim_hash` is computed as `xxh3(prefix + ":" + dimension)` where prefix is
//! "exact", "utxo", or "block". This makes the storage layer fully chain-agnostic.
//!
//! All multi-byte integers are big-endian encoded for correct lexicographic ordering.

use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    BlockSlot, ChainPoint, IndexDelta, IndexError, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter, TagDimension, UtxoSet,
};
use fjall::{
    compaction::Leveled, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode,
    Readable,
};

pub mod exact_keys;
pub mod history;
pub mod tag_keys;
pub mod utxos;

use crate::Error;

// Re-export the iterator type
pub use history::SlotIterator as SlotIter;

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Keyspace names for index store
mod keyspace_names {
    /// Cursor keyspace (separate for different access pattern)
    pub const CURSOR: &str = "index-cursor";
    /// Exact-match keyspace (block hash, tx hash, block number -> slot)
    pub const EXACT: &str = "index-exact";
    /// Tags keyspace (UTxO tags + block tags with dimension hash prefixes)
    pub const TAGS: &str = "index-tags";
}

/// Key for the cursor entry
const CURSOR_KEY: &[u8] = &[0u8];

/// Fjall-based index store implementation with three keyspaces.
///
/// Uses 3 keyspaces:
/// - `cursor`: Chain position tracking
/// - `exact`: Exact-match lookups (block/tx hash, block number)
/// - `tags`: UTxO tags and block tags with dimension hash prefixes
#[derive(Clone)]
pub struct IndexStore {
    db: Database,
    /// Cursor keyspace (separate due to different access pattern)
    cursor: Keyspace,
    /// Exact-match keyspace for point lookups
    exact: Keyspace,
    /// Tags keyspace for UTxO and block tags (prefix scans)
    tags: Keyspace,
    /// Configuration
    flush_on_commit: bool,
}

impl IndexStore {
    /// Open or create an index store at the given path
    ///
    /// # Parameters
    /// - `path`: Directory path for the database
    /// - `cache_size_mb`: Size of memory cache in MB (default: 500)
    /// - `max_journal_size_mb`: Maximum journal size in MB (default: Fjall's 512)
    /// - `flush_on_commit`: Whether to flush after each commit (default: Fjall's false)
    /// - `worker_threads`: Number of background compaction workers (default: Fjall's min(cores, 4))
    /// - `l0_threshold`: L0 compaction threshold (default: 4, lower = more aggressive)
    /// - `memtable_size_mb`: Memtable size in MB before flush (default: 64)
    pub fn open(
        path: impl AsRef<Path>,
        cache_size_mb: Option<usize>,
        max_journal_size_mb: Option<usize>,
        flush_on_commit: Option<bool>,
        worker_threads: Option<usize>,
        l0_threshold: Option<u8>,
        memtable_size_mb: Option<usize>,
    ) -> Result<Self, Error> {
        let cache_size = cache_size_mb.unwrap_or(DEFAULT_CACHE_SIZE_MB);
        let cache_bytes = (cache_size * 1024 * 1024) as u64;

        let mut builder = Database::builder(path.as_ref()).cache_size(cache_bytes);

        // Apply optional max journal size (otherwise use Fjall default of 512 MiB)
        if let Some(journal_mb) = max_journal_size_mb {
            builder = builder.max_journaling_size((journal_mb as u64) * 1024 * 1024);
        }

        // Apply optional worker threads (otherwise use Fjall default of min(cores, 4))
        if let Some(threads) = worker_threads {
            builder = builder.worker_threads(threads);
        }

        let db = builder.open()?;

        // Use Fjall default (false) if not specified
        let flush = flush_on_commit.unwrap_or(false);

        Self::from_database(db, flush, l0_threshold, memtable_size_mb)
    }

    /// Create an index store from an existing database
    fn from_database(
        db: Database,
        flush_on_commit: bool,
        l0_threshold: Option<u8>,
        memtable_size_mb: Option<usize>,
    ) -> Result<Self, Error> {
        // Build keyspace options with compaction settings
        let build_opts = || {
            let mut opts = KeyspaceCreateOptions::default();

            // Apply L0 threshold for more aggressive compaction if specified
            if let Some(threshold) = l0_threshold {
                opts = opts
                    .compaction_strategy(Arc::new(Leveled::default().with_l0_threshold(threshold)));
            }

            // Apply memtable size if specified
            if let Some(size_mb) = memtable_size_mb {
                opts = opts.max_memtable_size((size_mb as u64) * 1024 * 1024);
            }

            opts
        };

        // 3 keyspaces: cursor, exact, tags
        // db.keyspace expects a closure that returns KeyspaceCreateOptions
        let cursor = db.keyspace(keyspace_names::CURSOR, &build_opts)?;
        let exact = db.keyspace(keyspace_names::EXACT, &build_opts)?;
        let tags = db.keyspace(keyspace_names::TAGS, &build_opts)?;

        Ok(Self {
            db,
            cursor,
            exact,
            tags,
            flush_on_commit,
        })
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Get a reference to the exact-match keyspace
    pub fn exact_keyspace(&self) -> &Keyspace {
        &self.exact
    }

    /// Get a reference to the tags keyspace
    pub fn tags_keyspace(&self) -> &Keyspace {
        &self.tags
    }

    /// Gracefully shutdown the index store.
    ///
    /// This method ensures all pending work is completed before the database
    /// is dropped, preventing hangs in Fjall's drop implementation when the
    /// worker channel is full.
    ///
    /// Call this method before the IndexStore goes out of scope, especially
    /// after heavy write operations like bulk imports.
    pub fn shutdown(&self) -> Result<(), Error> {
        use std::time::Duration;

        tracing::info!("index store: starting graceful shutdown");

        // First, persist all data to ensure durability
        self.db.persist(PersistMode::SyncAll)?;
        tracing::debug!("index store: persist complete");

        // Wait for outstanding flushes to complete
        let mut wait_count = 0;
        while self.db.outstanding_flushes() > 0 {
            std::thread::sleep(Duration::from_millis(10));
            wait_count += 1;
            if wait_count % 100 == 0 {
                tracing::debug!(
                    "index store: waiting for {} outstanding flushes",
                    self.db.outstanding_flushes()
                );
            }
            // Safety timeout after 60 seconds
            if wait_count > 6000 {
                tracing::warn!(
                    "index store: timeout waiting for flushes, proceeding with shutdown"
                );
                break;
            }
        }

        tracing::info!("index store: graceful shutdown complete");
        Ok(())
    }
}

/// Writer for batched index operations.
///
/// Uses interior mutability via Mutex because the `IndexWriter` trait
/// requires `&self` for all methods, but fjall's `OwnedWriteBatch` needs
/// `&mut self` for insert/remove operations.
pub struct IndexStoreWriter {
    batch: Mutex<OwnedWriteBatch>,
    store: IndexStore,
}

impl CoreIndexWriter for IndexStoreWriter {
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        // Apply UTxO tag changes to tags keyspace
        utxos::apply(&mut batch, &self.store.tags, delta).map_err(IndexError::from)?;

        // Apply history index changes (exact to exact keyspace, tags to tags keyspace)
        history::apply(&mut batch, &self.store.exact, &self.store.tags, delta)
            .map_err(IndexError::from)?;

        // Set cursor
        let cursor_bytes =
            bincode::serialize(&delta.cursor).map_err(|e| Error::Codec(e.to_string()))?;
        batch.insert(&self.store.cursor, CURSOR_KEY, cursor_bytes);

        Ok(())
    }

    fn undo(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        // Undo UTxO tag changes
        utxos::undo(&mut batch, &self.store.tags, delta).map_err(IndexError::from)?;

        // Undo history index changes
        history::undo(&mut batch, &self.store.exact, &self.store.tags, delta)
            .map_err(IndexError::from)?;

        Ok(())
    }

    fn commit(self) -> Result<(), IndexError> {
        let batch = self
            .batch
            .into_inner()
            .map_err(|_| Error::LockPoisoned)?
            .durability(Some(PersistMode::Buffer));

        batch.commit().map_err(Error::Fjall)?;

        // Flush journal if configured to prevent accumulation
        if self.store.flush_on_commit {
            self.store
                .db
                .persist(PersistMode::Buffer)
                .map_err(Error::Fjall)?;
        }

        Ok(())
    }
}

impl CoreIndexStore for IndexStore {
    type Writer = IndexStoreWriter;
    type SlotIter = SlotIter;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        let batch = self.db.batch();
        Ok(IndexStoreWriter {
            batch: Mutex::new(batch),
            store: self.clone(),
        })
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        // Keyspaces are created on open, nothing to do here
        Ok(())
    }

    fn copy(&self, _target: &Self) -> Result<(), IndexError> {
        todo!("copy not implemented for fjall index store")
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        match snapshot
            .get(&self.cursor, CURSOR_KEY)
            .map_err(Error::from)?
        {
            Some(value) => {
                let point: ChainPoint =
                    bincode::deserialize(&value).map_err(|e| Error::Codec(e.to_string()))?;
                Ok(Some(point))
            }
            None => Ok(None),
        }
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        // Pass dimension string directly - chain-agnostic
        utxos::get_by_key(&snapshot, &self.tags, dimension, key).map_err(IndexError::from)
    }

    fn slot_by_block_hash(&self, block_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        history::get_by_block_hash(&snapshot, &self.exact, block_hash).map_err(IndexError::from)
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        history::get_by_block_number(&snapshot, &self.exact, number).map_err(IndexError::from)
    }

    fn slot_by_tx_hash(&self, tx_hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        history::get_by_tx_hash(&snapshot, &self.exact, tx_hash).map_err(IndexError::from)
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();

        // For metadata, key is already the u64 encoded as bytes
        if dimension == "metadata" {
            let metadata =
                u64::from_be_bytes(key.try_into().map_err(|_| {
                    IndexError::CodecError("metadata key must be 8 bytes".to_string())
                })?);
            return SlotIter::from_hash(&snapshot, &self.tags, dimension, metadata, start, end)
                .map_err(IndexError::from);
        }

        // Pass dimension string directly - chain-agnostic
        SlotIter::new(&snapshot, &self.tags, dimension, key, start, end).map_err(IndexError::from)
    }
}
