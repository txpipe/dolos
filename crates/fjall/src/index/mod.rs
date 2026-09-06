//! Fjall-based index store implementation for Dolos (chain-agnostic).
//!
//! What is left of the index store is its cursor: one keyspace, one key.
//!
//! The indexes it used to hold moved to the stores they project. The
//! live-UTxO tags (mutable, high-churn) live in the state store's `state-tags`
//! keyspace beside the UTxO set (see `crate::state::tags`); the archive tags
//! and the exact lookups live in the archive store's `archive-tags` and
//! `index-exact` keyspaces beside the block locations (see
//! `crate::archive::{tags, exact}`), written in the same batch as the blocks.
//!
//! ## Keyspace
//!
//! - **`index-cursor`**: chain position tracking. Key `[0x00]`, value a
//!   bincode-serialized `ChainPoint`.

use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    config::FjallIndexConfig, ChainPoint, IndexDelta, IndexError, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter,
};
use fjall::{
    compaction::Leveled, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode,
    Readable,
};

use crate::Error;

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;
/// Default max journal size in MB for index store
const DEFAULT_MAX_JOURNAL_SIZE_MB: usize = 1024;
/// Default background worker threads for index store
const DEFAULT_WORKER_THREADS: usize = 8;
/// Default L0 compaction threshold for index store
const DEFAULT_L0_THRESHOLD: u8 = 8;
/// Default memtable size in MB for index store
const DEFAULT_MEMTABLE_SIZE_MB: usize = 128;

/// Keyspace names for index store
mod keyspace_names {
    /// Cursor keyspace
    pub const CURSOR: &str = "index-cursor";
}

/// Key for the cursor entry
const CURSOR_KEY: &[u8] = &[0u8];

/// Fjall-based index store: the cursor keyspace.
#[derive(Clone)]
pub struct IndexStore {
    db: Database,
    cursor: Keyspace,
    flush_on_commit: bool,
}

impl IndexStore {
    /// Open or create an index store at the given path
    ///
    /// # Parameters
    /// - `path`: Directory path for the database
    /// - `config`: Fjall index configuration
    pub fn open(path: impl AsRef<Path>, config: &FjallIndexConfig) -> Result<Self, Error> {
        let cache_size = config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB);
        let cache_bytes = (cache_size * 1024 * 1024) as u64;

        let mut builder = Database::builder(path.as_ref()).cache_size(cache_bytes);

        let max_journal_size = config
            .max_journal_size
            .unwrap_or(DEFAULT_MAX_JOURNAL_SIZE_MB);
        builder = builder.max_journaling_size((max_journal_size as u64) * 1024 * 1024);

        let worker_threads = config.worker_threads.unwrap_or(DEFAULT_WORKER_THREADS);
        builder = builder.worker_threads(worker_threads);

        let db = builder.open()?;

        // Use false if not specified
        let flush = config.flush_on_commit.unwrap_or(false);
        let l0_threshold = config.l0_threshold.unwrap_or(DEFAULT_L0_THRESHOLD);
        let memtable_size_mb = config.memtable_size_mb.unwrap_or(DEFAULT_MEMTABLE_SIZE_MB);

        Self::from_database(db, flush, Some(l0_threshold), Some(memtable_size_mb))
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

        let cursor = db.keyspace(keyspace_names::CURSOR, build_opts)?;

        Ok(Self {
            db,
            cursor,
            flush_on_commit,
        })
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Per-keyspace disk footprint: `(name, bytes, path)`.
    pub fn disk_usage(&self) -> Vec<(&'static str, u64, std::path::PathBuf)> {
        vec![(
            keyspace_names::CURSOR,
            self.cursor.disk_space(),
            self.cursor.path().to_path_buf(),
        )]
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

        let cursor_bytes =
            bincode::serialize(&delta.cursor).map_err(|e| Error::Codec(e.to_string()))?;
        batch.insert(&self.store.cursor, CURSOR_KEY, cursor_bytes);

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

    /// Whole-store copy is not implemented here (#1036).
    ///
    /// Refusing rather than panicking is the convention the trait's newer
    /// methods already use: a caller reaching for a capability this backend
    /// does not carry gets an error it can handle.
    fn copy(&self, _target: &Self) -> Result<(), IndexError> {
        Err(IndexError::Unsupported("copy"))
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
}
