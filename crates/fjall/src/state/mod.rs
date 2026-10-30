//! Fjall-based state store implementation for Dolos.
//!
//! This module provides an implementation of the `StateStore` trait using
//! fjall, an LSM-tree based embedded database.
//!
//! ## Four Keyspace Design
//!
//! State is organized into four keyspaces based on access patterns:
//!
//! 1. **`state-cursor`**: Chain position tracking, and the compatibility
//!    identity the store recorded when it was created
//!
//! 2. **`state-utxos`**: UTxO set storage Key: `[tx_hash:32][index:4]` (36
//!    bytes) Value: CBOR `[era, body, slot, tx_index]` (`[era, body]` for a
//!    UTxO no block produced)
//!
//! 3. **`state-entities`**: All entity types with namespace hash prefix Key:
//!    `[ns_hash:8][entity_key:var]` Value: entity CBOR bytes
//!
//! 4. **`state-tags`**: Live-UTxO tags (mutable: insert on produce, delete on
//!    consume) Key: `[dim_hash:8][lookup_key:var][tag_order:10][txo_ref:36]` ->
//!    empty, so a lookup key's refs sort by creation position
//!
//! This design reduces the number of LSM-tree segment files compared to using
//! separate keyspaces per entity type, avoiding "too many open files" errors
//! during heavy compaction.
//!
//! The tags keyspace is a projection of the UTxO set and is written in the
//! same batch, so the two commit together; it keeps its own compaction
//! settings because its churn (a delete per spent output) is unlike the
//! entity and UTxO keyspaces'.

use std::path::Path;
use std::sync::{Arc, Mutex};

use dolos_core::{
    ChainPoint, EntityKey, EntityValue, KeyRange, Namespace, StateError,
    StateStore as CoreStateStore, StateWriter as CoreStateWriter, TagDimension, TagEntry, TagOrder,
    TagStart, TxoRef, UtxoIndexDelta, UtxoMap, UtxoSet, UtxoSetDelta,
};
use fjall::{Database, Keyspace, OwnedWriteBatch, PersistMode, Readable};

pub mod entities;
pub mod entity_keys;
pub mod tags;
pub mod utxos;

use crate::preset::{Preset, Tuning};
use crate::view::{View, Viewed};
use crate::Error;

/// Keyspace names for state store
mod keyspace_names {
    /// Cursor keyspace (chain position)
    pub const CURSOR: &str = "state-cursor";
    /// UTxO set keyspace
    pub const UTXOS: &str = "state-utxos";
    /// Unified entities keyspace (all entity types with namespace prefix)
    pub const ENTITIES: &str = "state-entities";
    /// Live-UTxO tags keyspace (mutable: insert on produce, delete on consume)
    pub const TAGS: &str = "state-tags";
}

/// Key for the cursor entry
const CURSOR_KEY: &[u8] = &[0u8];

/// Key for the compatibility identity, beside the cursor
const IDENTITY_KEY: &[u8] = b"identity";

/// Fjall-based state store implementation with unified entities keyspace.
///
/// Uses 4 keyspaces:
/// - `cursor`: Chain position tracking and the compatibility identity
/// - `utxos`: UTxO set storage
/// - `entities`: All entity types with namespace hash prefixes
/// - `tags`: Live-UTxO tags, a projection of `utxos`
#[derive(Clone)]
pub struct StateStore {
    db: Database,
    /// Cursor keyspace (chain position and compatibility identity)
    cursor: Keyspace,
    /// UTxO set keyspace
    utxos: Keyspace,
    /// Unified entities keyspace (all entity types)
    entities: Keyspace,
    /// Live-UTxO tags keyspace
    tags: Keyspace,
    preset: Preset,
    /// The mirror a read-only store opened over, dropped after the database
    /// and every iterator over it.
    view: Option<Arc<View>>,
}

impl StateStore {
    /// Open or create a state store at the given path, tuned by `preset`,
    /// removing the views beside it that killed observers left.
    pub fn open(path: impl AsRef<Path>, preset: Preset) -> Result<Self, Error> {
        let store = Self::open_at(path.as_ref(), preset, None)?;
        crate::view::remove_views(path.as_ref());

        Ok(store)
    }

    /// Open the state store at `path` without writing to it, over a
    /// [`View`] beside it. The store refuses writers.
    pub fn open_view(path: impl AsRef<Path>, preset: Preset) -> Result<Self, Error> {
        let path = path.as_ref();
        let view = Arc::new(View::of(path, path)?);

        Self::open_at(view.path(), preset, Some(Arc::clone(&view)))
    }

    fn open_at(path: &Path, preset: Preset, view: Option<Arc<View>>) -> Result<Self, Error> {
        let tuning = preset.state();
        let db = tuning.database.builder(path).open()?;

        let store = Self {
            cursor: db.keyspace(keyspace_names::CURSOR, || tuning.cursor.options())?,
            utxos: db.keyspace(keyspace_names::UTXOS, || tuning.utxos.options())?,
            entities: db.keyspace(keyspace_names::ENTITIES, || tuning.entities.options())?,
            tags: db.keyspace(keyspace_names::TAGS, || tuning.tags.options())?,
            db,
            preset,
            view,
        };

        tracing::info!(tuning = %store.tuning(), "state store opened");

        Ok(store)
    }

    /// The tuning the store opened with.
    pub fn tuning(&self) -> Tuning {
        let tuning = self.preset.state();

        Tuning::new(
            self.preset,
            &tuning.database,
            vec![
                (keyspace_names::CURSOR, tuning.cursor),
                (keyspace_names::UTXOS, tuning.utxos),
                (keyspace_names::ENTITIES, tuning.entities),
                (keyspace_names::TAGS, tuning.tags),
            ],
        )
    }

    /// Get a reference to the underlying database
    pub fn database(&self) -> &Database {
        &self.db
    }

    /// Get a reference to the entities keyspace
    pub fn entities_keyspace(&self) -> &Keyspace {
        &self.entities
    }

    /// The compatibility identity the store recorded when it was created.
    pub fn identity(&self) -> Result<Option<String>, Error> {
        let value = self.cursor.get(IDENTITY_KEY)?;

        Ok(value.map(|bytes| String::from_utf8_lossy(&bytes).into_owned()))
    }

    /// Record the compatibility identity, synced to disk before it returns.
    pub fn record_identity(&self, identity: &str) -> Result<(), Error> {
        self.writable()?;
        self.cursor.insert(IDENTITY_KEY, identity.as_bytes())?;
        self.db.persist(PersistMode::SyncAll)?;

        Ok(())
    }

    /// Refuse a write on a store opened read-only.
    fn writable(&self) -> Result<(), Error> {
        match self.view {
            Some(_) => Err(Error::ReadOnly),
            None => Ok(()),
        }
    }

    /// Whether the store holds a cursor or any row.
    pub fn holds_data(&self) -> Result<bool, Error> {
        if self.cursor.contains_key(CURSOR_KEY)? {
            return Ok(true);
        }

        for keyspace in [&self.utxos, &self.entities, &self.tags] {
            if !keyspace.is_empty()? {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Per-keyspace disk footprint: `(name, bytes, path)`, the path in the
    /// store's own folder.
    pub fn disk_usage(&self) -> Vec<(&'static str, u64, std::path::PathBuf)> {
        [
            (keyspace_names::CURSOR, &self.cursor),
            (keyspace_names::UTXOS, &self.utxos),
            (keyspace_names::ENTITIES, &self.entities),
            (keyspace_names::TAGS, &self.tags),
        ]
        .map(|(name, ks)| (name, ks.disk_space(), self.source_of(ks.path())))
        .to_vec()
    }

    /// Where `path` is in the store itself, when it is in a view's mirror.
    fn source_of(&self, path: &Path) -> std::path::PathBuf {
        match &self.view {
            Some(view) => view.source_of(path),
            None => path.to_path_buf(),
        }
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

    fn apply_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        tags::apply(&mut batch, &self.store.tags, delta)?;

        Ok(())
    }

    fn undo_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        let mut batch = self.batch.lock().map_err(|_| Error::LockPoisoned)?;

        tags::undo(&mut batch, &self.store.tags, delta)?;

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
        if self.store.preset.state().database.flush_on_commit {
            self.store
                .db
                .persist(PersistMode::Buffer)
                .map_err(Error::Fjall)?;
        }

        Ok(())
    }
}

impl CoreStateStore for StateStore {
    type EntityIter = Viewed<entities::EntityIterator>;
    type EntityValueIter = entities::EmptyEntityValueIterator;
    type UtxoIter = Viewed<utxos::UtxosIterator>;
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
        self.writable()?;

        let batch = self.db.batch();
        Ok(StateWriter {
            batch: Mutex::new(batch),
            store: self.clone(),
        })
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: KeyRange,
    ) -> Result<Self::EntityIter, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        let iter = entities::EntityIterator::new(&snapshot, &self.entities, ns, range)?;

        Ok(Viewed::new(iter, self.view.as_ref()))
    }

    /// Multi-value namespaces have no encoding in this store: its entity
    /// keyspace is one value per key, with no multimap to walk (#1036).
    ///
    /// Refusing is the convention the trait's newer methods already use, and
    /// what the builtin memory store answers. A panic here would take down a
    /// caller that has an error path anyway.
    fn iter_entity_values(
        &self,
        _ns: Namespace,
        _key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        Err(StateError::Unsupported("iter_entity_values"))
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        utxos::get_utxos(&snapshot, &self.utxos, &refs).map_err(StateError::from)
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        tags::get_by_key(&snapshot, &self.tags, dimension, key).map_err(StateError::from)
    }

    fn utxos_by_tag_page(
        &self,
        dimension: TagDimension,
        key: &[u8],
        order: TagOrder,
        start: TagStart,
        count: usize,
    ) -> Result<Vec<TagEntry>, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        tags::get_page_by_key(&snapshot, &self.tags, dimension, key, order, start, count)
            .map_err(StateError::from)
    }

    fn utxos_with_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        entries: Vec<TagEntry>,
    ) -> Result<Vec<TagEntry>, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        tags::filter_by_key(&snapshot, &self.tags, dimension, key, entries)
            .map_err(StateError::from)
    }

    fn iter_utxos(&self) -> Result<Self::UtxoIter, StateError> {
        // Use snapshot for MVCC reads to avoid deadlocks with concurrent writes
        let snapshot = self.db.snapshot();
        let iter = utxos::UtxosIterator::new(&snapshot, &self.utxos);

        Ok(Viewed::new(iter, self.view.as_ref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preset::{assert_fjall_holds, assert_memtables_hold};

    #[test]
    fn a_reopened_store_holds_its_preset() {
        let dir = tempfile::tempdir().unwrap();

        let store = StateStore::open(dir.path(), Preset::Standard).unwrap();
        store.shutdown().unwrap();
        drop(store);

        let store = StateStore::open(dir.path(), Preset::Standard).unwrap();
        assert_fjall_holds(store.database(), &store.tuning());
        assert_memtables_hold(store.database(), &store.tuning());
    }

    const NS: Namespace = "accounts";

    /// A store holding `rows` entities and `rows` UTxOs, each in a table,
    /// shut down by its writer.
    fn written(path: &Path, rows: u32) {
        let store = StateStore::open(path, Preset::Standard).unwrap();
        let writer = store.start_writer().unwrap();

        let mut delta = UtxoSetDelta::default();

        for row in 0..rows {
            let key = EntityKey::from_raw(&row.to_be_bytes());
            writer.write_entity(NS, &key, &vec![1; 8]).unwrap();

            let utxo = dolos_core::UtxoValue {
                output: dolos_core::EraCbor(1, vec![1; 8]),
                position: None,
            };
            delta.produced_utxo.insert(
                TxoRef(pallas::crypto::hash::Hash::new([0; 32]), row),
                Arc::new(utxo),
            );
        }

        writer.apply_utxoset(&delta).unwrap();
        writer.commit().unwrap();
        store.entities.rotate_memtable_and_wait().unwrap();
        store.utxos.rotate_memtable_and_wait().unwrap();
        store.shutdown().unwrap();
    }

    fn files(dir: &Path) -> Vec<(std::path::PathBuf, Vec<u8>)> {
        let mut found = Vec::new();

        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();

            if path.is_dir() {
                found.extend(files(&path));
            } else {
                found.push((path.clone(), std::fs::read(&path).unwrap()));
            }
        }

        found.sort();
        found
    }

    #[test]
    fn a_read_only_store_refuses_writers() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        written(&path, 1);

        let before = files(dir.path());

        {
            let store = StateStore::open_view(&path, Preset::Standard).unwrap();
            let error = store.start_writer().err().unwrap();
            assert!(error.to_string().contains("read-only"), "{error}");

            let error = store.record_identity("other").unwrap_err();
            assert!(matches!(error, Error::ReadOnly), "{error:?}");
        }

        assert!(
            files(dir.path()) == before,
            "a read-only store changed a file"
        );
    }

    /// An iterator drained after its read-only store is dropped reads every
    /// row, and the view goes with the iterator.
    #[test]
    fn an_iterator_outlives_its_read_only_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        written(&path, 20_000);

        let store = StateStore::open_view(&path, Preset::Standard).unwrap();
        let entities = store.iter_entities(NS, KeyRange::all()).unwrap();
        drop(store);
        assert_eq!(
            left(dir.path()).len(),
            2,
            "the view went before its iterator"
        );

        let rows = entities.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 20_000);
        assert_eq!(left(dir.path()), ["state"]);

        let store = StateStore::open_view(&path, Preset::Standard).unwrap();
        let utxos = store.iter_utxos().unwrap();
        drop(store);
        assert_eq!(
            left(dir.path()).len(),
            2,
            "the view went before its iterator"
        );

        let rows = utxos.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(rows.len(), 20_000);
        assert_eq!(left(dir.path()), ["state"]);
    }

    fn left(dir: &Path) -> Vec<std::ffi::OsString> {
        std::fs::read_dir(dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect()
    }

    /// A writable open removes the view a killed observer left, which would
    /// otherwise keep the tables the writer compacts away on disk.
    #[test]
    fn a_writable_open_removes_a_killed_observers_view() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        written(&path, 1);

        crate::view::left_by_a_kill(&path, &path);
        assert_eq!(left(dir.path()).len(), 2);

        let _store = StateStore::open(&path, Preset::Standard).unwrap();
        assert_eq!(left(dir.path()), ["state"]);
    }

    /// A view reports its footprint under the store's own folder, not the
    /// mirror it is dropped with.
    #[test]
    fn a_view_reports_the_paths_of_the_store() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("state");
        written(&path, 1);

        let store = std::path::absolute(&path).unwrap();
        let view = StateStore::open_view(&path, Preset::Standard).unwrap();

        for (name, _, keyspace) in view.disk_usage() {
            assert!(
                keyspace.starts_with(&store),
                "{name}: {}",
                keyspace.display()
            );
        }
    }
}
