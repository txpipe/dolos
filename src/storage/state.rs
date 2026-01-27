//! State store backend wrapper for runtime backend selection.
//!
//! This module provides an enum wrapper around the concrete state store implementations
//! (redb3 and fjall) that implements the `StateStore` trait.

use std::ops::Range;

use dolos_core::{
    config::{StateStoreConfig, StorageConfig},
    ChainPoint, EntityKey, EntityValue, Namespace, StateError, StateSchema,
    StateStore as CoreStateStore, StateWriter as CoreStateWriter, TxoRef, UtxoMap, UtxoSetDelta,
};

// ============================================================================
// StateStoreBackend - Main enum wrapper
// ============================================================================

/// Enum wrapper for state store backends.
///
/// This allows runtime selection of the state store backend via configuration.
#[derive(Clone)]
pub enum StateStoreBackend {
    Redb(dolos_redb3::state::StateStore),
    Fjall(dolos_fjall::StateStore),
}

impl StateStoreBackend {
    /// Open a state store based on the configuration.
    ///
    /// The path is resolved from the storage config using the `state_path()` helper.
    /// The schema is required for redb backends to initialize the entity tables.
    pub fn open(config: &StorageConfig, schema: StateSchema) -> Result<Self, StateError> {
        match &config.state {
            StateStoreConfig::Redb(cfg) => {
                let path = config.state_path().ok_or_else(|| {
                    StateError::InternalStoreError(
                        "cannot determine state path for ephemeral config".to_string(),
                    )
                })?;

                std::fs::create_dir_all(path.parent().unwrap_or(&path))
                    .map_err(|e| StateError::InternalStoreError(e.to_string()))?;

                let store = dolos_redb3::state::StateStore::open(schema, &path, cfg.cache)?;
                Ok(Self::Redb(store))
            }
            StateStoreConfig::InMemory => {
                let store = dolos_redb3::state::StateStore::in_memory(schema)?;
                Ok(Self::Redb(store))
            }
            StateStoreConfig::Fjall(cfg) => {
                let path = config.state_path().ok_or_else(|| {
                    StateError::InternalStoreError(
                        "cannot determine state path for ephemeral config".to_string(),
                    )
                })?;

                std::fs::create_dir_all(path.parent().unwrap_or(&path))
                    .map_err(|e| StateError::InternalStoreError(e.to_string()))?;

                // Fjall uses a unified entities keyspace with namespace hash prefixes,
                // so it doesn't need the schema to pre-create keyspaces
                let store = dolos_fjall::StateStore::open(
                    &path,
                    cfg.cache,
                    cfg.max_journal_size,
                    cfg.flush_on_commit,
                    cfg.worker_threads,
                    cfg.l0_threshold,
                    cfg.memtable_size_mb,
                )?;
                Ok(Self::Fjall(store))
            }
        }
    }

    /// Open an in-memory state store directly.
    pub fn in_memory(schema: StateSchema) -> Result<Self, StateError> {
        let store = dolos_redb3::state::StateStore::in_memory(schema)?;
        Ok(Self::Redb(store))
    }

    /// Gracefully shutdown the state store.
    ///
    /// This ensures all pending work is completed before the store is dropped.
    pub fn shutdown(&self) -> Result<(), StateError> {
        match self {
            Self::Redb(s) => s
                .shutdown()
                .map_err(|e| StateError::InternalStoreError(e.to_string())),
            Self::Fjall(s) => s
                .shutdown()
                .map_err(|e| StateError::InternalStoreError(e.to_string())),
        }
    }
}

// ============================================================================
// StateWriterBackend - Writer wrapper
// ============================================================================

/// Enum wrapper for state store writers.
pub enum StateWriterBackend {
    Redb(<dolos_redb3::state::StateStore as CoreStateStore>::Writer),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::Writer),
}

impl CoreStateWriter for StateWriterBackend {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.set_cursor(cursor),
            Self::Fjall(w) => w.set_cursor(cursor),
        }
    }

    fn write_entity(
        &self,
        ns: Namespace,
        key: &EntityKey,
        value: &EntityValue,
    ) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.write_entity(ns, key, value),
            Self::Fjall(w) => w.write_entity(ns, key, value),
        }
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.delete_entity(ns, key),
            Self::Fjall(w) => w.delete_entity(ns, key),
        }
    }

    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.apply_utxoset(delta),
            Self::Fjall(w) => w.apply_utxoset(delta),
        }
    }

    fn commit(self) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.commit(),
            Self::Fjall(w) => w.commit(),
        }
    }
}

// ============================================================================
// EntityIterBackend - Entity iterator wrapper
// ============================================================================

/// Enum wrapper for entity iterators.
pub enum EntityIterBackend {
    Redb(<dolos_redb3::state::StateStore as CoreStateStore>::EntityIter),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::EntityIter),
}

impl Iterator for EntityIterBackend {
    type Item = Result<(EntityKey, EntityValue), StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
        }
    }
}

// ============================================================================
// EntityValueIterBackend - Entity value iterator wrapper
// ============================================================================

/// Enum wrapper for entity value iterators.
pub enum EntityValueIterBackend {
    Redb(<dolos_redb3::state::StateStore as CoreStateStore>::EntityValueIter),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::EntityValueIter),
}

impl Iterator for EntityValueIterBackend {
    type Item = Result<EntityValue, StateError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
        }
    }
}

// ============================================================================
// StateStore trait implementation
// ============================================================================

impl CoreStateStore for StateStoreBackend {
    type EntityIter = EntityIterBackend;
    type EntityValueIter = EntityValueIterBackend;
    type Writer = StateWriterBackend;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        match self {
            Self::Redb(s) => s.read_cursor(),
            Self::Fjall(s) => s.read_cursor(),
        }
    }

    fn read_entities(
        &self,
        ns: Namespace,
        keys: &[&EntityKey],
    ) -> Result<Vec<Option<EntityValue>>, StateError> {
        match self {
            Self::Redb(s) => s.read_entities(ns, keys),
            Self::Fjall(s) => s.read_entities(ns, keys),
        }
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        match self {
            Self::Redb(s) => s.start_writer().map(StateWriterBackend::Redb),
            Self::Fjall(s) => s.start_writer().map(StateWriterBackend::Fjall),
        }
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        match self {
            Self::Redb(s) => s.iter_entities(ns, range).map(EntityIterBackend::Redb),
            Self::Fjall(s) => s.iter_entities(ns, range).map(EntityIterBackend::Fjall),
        }
    }

    fn iter_entity_values(
        &self,
        ns: Namespace,
        key: impl AsRef<[u8]>,
    ) -> Result<Self::EntityValueIter, StateError> {
        match self {
            Self::Redb(s) => s
                .iter_entity_values(ns, key)
                .map(EntityValueIterBackend::Redb),
            Self::Fjall(s) => s
                .iter_entity_values(ns, key)
                .map(EntityValueIterBackend::Fjall),
        }
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        match self {
            Self::Redb(s) => s.get_utxos(refs),
            Self::Fjall(s) => s.get_utxos(refs),
        }
    }
}
