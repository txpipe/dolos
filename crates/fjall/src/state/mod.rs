//! Fjall-based state store implementation for Dolos.
//!
//! This module provides an implementation of the `StateStore` trait using fjall,
//! an LSM-tree based embedded database.
//!
//! ## Three Keyspace Design
//!
//! State is organized into three keyspaces based on access patterns:
//!
//! 1. **`state-cursor`**: Chain position tracking (single key-value)
//!
//! 2. **`state-utxos`**: UTxO set storage
//!    Key: `[tx_hash:32][index:4]` (36 bytes)
//!    Value: `[era:2][cbor:...]`
//!
//! 3. **`state-entities`**: All entity types with namespace hash prefix
//!    Key: `[ns_hash:8][entity_key:32]` (40 bytes)
//!    Value: entity CBOR bytes
//!
//! This design reduces the number of LSM-tree segment files compared to using
//! separate keyspaces per entity type, avoiding "too many open files" errors
//! during heavy compaction.

use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    config::FjallStateConfig, ChainPoint, EntityKey, EntityValue, Namespace, StateError,
    StateStore as CoreStateStore, StateWriter as CoreStateWriter, TxoRef, UtxoMap, UtxoSetDelta,
};
use fjall::{
    compaction::Leveled, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode,
    Readable,
};

pub mod entities;
pub mod entity_keys;
pub mod utxos;

use crate::Error;

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Keyspace names for state store
mod keyspace_names {
    /// Cursor keyspace (chain position)
    pub const CURSOR: &str = "state-cursor";
    /// UTxO set keyspace
    pub const UTXOS: &str = "state-utxos";
    /// Unified entities keyspace (all entity types with namespace prefix)
    pub const ENTITIES: &str = "state-entities";
}

/// Key for the cursor entry
const CURSOR_KEY: &[u8] = &[0u8];

/// Fjall-based state store implementation with unified entities keyspace.
///
/// Uses 3 keyspaces:
/// - `cursor`: Chain position tracking
/// - `utxos`: UTxO set storage
/// - `entities`: All entity types with namespace hash prefixes
#[derive(Clone)]
pub struct StateStore {
    db: Database,
    /// Cursor keyspace (chain position)
    cursor: Keyspace,
    /// UTxO set keyspace
    utxos: Keyspace,
    /// Unified entities keyspace (all entity types)
    entities: Keyspace,
    /// Configuration
    flush_on_commit: bool,
}

impl StateStore {
    /// Open or create a state store at the given path
    ///
    /// # Parameters
    /// - `path`: Directory path for the database
    /// - `config`: Fjall state configuration
    pub fn open(path: impl AsRef<Path>, config: &FjallStateConfig) -> Result<Self, Error> {
        let cache_size = config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB);
        let cache_bytes = (cache_size * 1024 * 1024) as u64;

        let mut builder = Database::builder(path.as_ref()).cache_size(cache_bytes);

        // Apply optional max journal size (otherwise use Fjall default of 512 MiB)
        if let Some(journal_mb) = config.max_journal_size {
            builder = builder.max_journaling_size((journal_mb as u64) * 1024 * 1024);
        }

        // Apply optional worker threads (otherwise use Fjall default of min(cores, 4))
        if let Some(threads) = config.worker_threads {
            builder = builder.worker_threads(threads);
        }

        let db = builder.open()?;

        // Use Fjall default (false) if not specified
        let flush = config.flush_on_commit.unwrap_or(false);

        Self::from_database(db, flush, config.l0_threshold, config.memtable_size_mb)
    }

    /// Create a state store from an existing database
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

        // 3 keyspaces: cursor, utxos, entities
        // db.keyspace expects a closure that returns KeyspaceCreateOptions
        let cursor = db.keyspace(keyspace_names::CURSOR, build_opts)?;
        let utxos = db.keyspace(keyspace_names::UTXOS, build_opts)?;
        let entities = db.keyspace(keyspace_names::ENTITIES, build_opts)?;

        Ok(Self {
            db,
            cursor,
            utxos,
            entities,
            flush_on_commit,
        })
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Get a reference to the entities keyspace
    pub fn entities_keyspace(&self) -> &Keyspace {
        &self.entities
    }

    /// Gracefully shutdown the state store.
    ///
    /// This method ensures all pending work is completed before the database
    /// is dropped, preventing hangs in Fjall's drop implementation when the
    /// worker channel is full.
    ///
    /// Call this method before the StateStore goes out of scope, especially
    /// after heavy write operations like bulk imports.
    pub fn shutdown(&self) -> Result<(), Error> {
        use std::time::Duration;

        tracing::info!("state store: starting graceful shutdown");

        // First, persist all data to ensure durability
        self.db.persist(PersistMode::SyncAll)?;
        tracing::debug!("state store: persist complete");

        // Wait for outstanding flushes to complete
        let mut wait_count = 0;
        while self.db.outstanding_flushes() > 0 {
            std::thread::sleep(Duration::from_millis(10));
            wait_count += 1;
            if wait_count % 100 == 0 {
                tracing::debug!(
                    "state store: waiting for {} outstanding flushes",
                    self.db.outstanding_flushes()
                );
            }
            // Safety timeout after 60 seconds
            if wait_count > 6000 {
                tracing::warn!(
                    "state store: timeout waiting for flushes, proceeding with shutdown"
                );
                break;
            }
        }

        tracing::info!("state store: graceful shutdown complete");
        Ok(())
    }
}

/// Writer for batched state operations.
pub struct StateWriter {
    batch: Mutex<OwnedWriteBatch>,
    store: StateStore,
}

impl CoreStateWriter for StateWriter {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        let cursor_bytes = bincode::serialize(&cursor).map_err(|e| Error::Codec(e.to_string()))?;
        batch.insert(&self.store.cursor, CURSOR_KEY, cursor_bytes);

        Ok(())
    }

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        entities::write_entity(&mut batch, &self.store.entities, ns, key, value);

        Ok(())
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        entities::delete_entity(&mut batch, &self.store.entities, ns, key);

        Ok(())
    }

    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), StateError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        // Apply UTxO changes
        utxos::apply_delta(&mut batch, &self.store.utxos, delta)?;

        Ok(())
    }

    fn commit(self) -> Result<(), StateError> {
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

impl CoreStateStore for StateStore {
    type EntityIter = entities::EntityIterator;
    type EntityValueIter = entities::EmptyEntityValueIterator;
    type Writer = StateWriter;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
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

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        entities::read_entities(&snapshot, &self.entities, ns, keys).map_err(StateError::from)
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        let batch = self.db.batch();
        Ok(StateWriter {
            batch: Mutex::new(batch),
            store: self.clone(),
        })
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        entities::EntityIterator::new(&snapshot, &self.entities, ns, range)
            .map_err(StateError::from)
    }

    fn iter_entity_values(
        &self,
        _ns: Namespace,
        _key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        // Multimap not supported - panic if called
        unimplemented!(
            "iter_entity_values is not supported in fjall state store (no multimap support)"
        )
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        utxos::get_utxos(&snapshot, &self.utxos, &refs).map_err(StateError::from)
    }
}
