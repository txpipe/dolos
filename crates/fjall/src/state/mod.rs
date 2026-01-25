//! Fjall-based state store implementation for Dolos.
//!
//! This module provides an implementation of the `StateStore` trait using fjall,
//! an LSM-tree based embedded database.
//!
//! ## Keyspaces
//!
//! - `state-cursor`: Chain position (single key-value)
//! - `state-utxos`: UTxO set storage
//! - `state-entity-{namespace}`: Dynamic entity tables (including datums)

use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    ChainPoint, EntityKey, EntityValue, Namespace, StateError, StateSchema,
    StateStore as CoreStateStore, StateWriter as CoreStateWriter, TxoRef, UtxoMap, UtxoSetDelta,
};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode};

pub mod entities;
pub mod utxos;

use crate::Error;

/// Default cache size in MB
const DEFAULT_CACHE_SIZE_MB: usize = 500;

/// Default max journal size in MB (2 GiB)
const DEFAULT_MAX_JOURNAL_SIZE_MB: usize = 2048;

/// Default flush on commit setting
const DEFAULT_FLUSH_ON_COMMIT: bool = true;

/// Keyspace names for state store
mod keyspace_names {
    pub const CURSOR: &str = "state-cursor";
    pub const UTXOS: &str = "state-utxos";

    /// Generate entity keyspace name from namespace
    pub fn entity_keyspace(ns: &str) -> String {
        format!("state-entity-{}", ns)
    }
}

/// Key for the cursor entry
const CURSOR_KEY: &[u8] = &[0u8];

/// Fjall-based state store implementation
#[derive(Clone)]
pub struct StateStore {
    db: Arc<Database>,
    cursor: Keyspace,
    utxos: Keyspace,
    entities: HashMap<Namespace, Keyspace>,
    flush_on_commit: bool,
}

impl StateStore {
    /// Open or create a state store at the given path
    pub fn open(
        schema: StateSchema,
        path: impl AsRef<Path>,
        cache_size_mb: Option<usize>,
        max_journal_size_mb: Option<usize>,
        flush_on_commit: Option<bool>,
    ) -> Result<Self, Error> {
        let cache_size = cache_size_mb.unwrap_or(DEFAULT_CACHE_SIZE_MB);
        let cache_bytes = (cache_size * 1024 * 1024) as u64;

        let max_journal = max_journal_size_mb.unwrap_or(DEFAULT_MAX_JOURNAL_SIZE_MB);
        let max_journal_bytes = (max_journal as u64) * 1024 * 1024;

        let flush = flush_on_commit.unwrap_or(DEFAULT_FLUSH_ON_COMMIT);

        let db = Database::builder(path.as_ref())
            .cache_size(cache_bytes)
            .max_journaling_size(max_journal_bytes)
            .open()?;

        Self::from_database(db, schema, flush)
    }

    /// Create a state store from an existing database
    fn from_database(
        db: Database,
        schema: StateSchema,
        flush_on_commit: bool,
    ) -> Result<Self, Error> {
        let opts = || KeyspaceCreateOptions::default();

        // Core keyspaces
        let cursor = db.keyspace(keyspace_names::CURSOR, opts)?;
        let utxos = db.keyspace(keyspace_names::UTXOS, opts)?;

        // Entity keyspaces from schema
        let mut entities = HashMap::new();
        for (ns, _ns_type) in schema.iter() {
            let ks_name = keyspace_names::entity_keyspace(ns);
            // We need to leak the string to get a 'static str for fjall
            let ks_name_static: &'static str = Box::leak(ks_name.into_boxed_str());
            let keyspace = db.keyspace(ks_name_static, opts)?;
            entities.insert(*ns, keyspace);
        }

        Ok(Self {
            db: Arc::new(db),
            cursor,
            utxos,
            entities,
            flush_on_commit,
        })
    }

    /// Get entity keyspace by namespace
    fn entity_keyspace(&self, ns: Namespace) -> Option<&Keyspace> {
        self.entities.get(ns)
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
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

        let keyspace = self
            .store
            .entity_keyspace(ns)
            .ok_or_else(|| Error::KeyspaceNotFound(ns.to_string()))?;

        entities::write_entity(&mut batch, keyspace, key, value);

        Ok(())
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        let keyspace = self
            .store
            .entity_keyspace(ns)
            .ok_or_else(|| Error::KeyspaceNotFound(ns.to_string()))?;

        entities::delete_entity(&mut batch, keyspace, key);

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
        match self.cursor.get(CURSOR_KEY).map_err(Error::from)? {
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
        let keyspace = self
            .entity_keyspace(ns)
            .ok_or_else(|| Error::KeyspaceNotFound(ns.to_string()))?;

        entities::read_entities(keyspace, keys).map_err(StateError::from)
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
        let keyspace = self
            .entity_keyspace(ns)
            .ok_or_else(|| Error::KeyspaceNotFound(ns.to_string()))?;

        entities::EntityIterator::new(keyspace, range).map_err(StateError::from)
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
        utxos::get_utxos(&self.utxos, &refs).map_err(StateError::from)
    }
}
