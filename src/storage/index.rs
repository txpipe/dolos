//! Index store backend wrapper for runtime backend selection.
//!
//! This module provides an enum wrapper around the concrete index store implementations
//! (redb3, fjall, and noop) that implements the `IndexStore` trait.

use dolos_core::{
    builtin::{EmptySlotIter, NoOpIndexStore, NoOpIndexWriter},
    config::{IndexStoreConfig, StorageConfig},
    BlockSlot, ChainPoint, IndexDelta, IndexError, IndexStore as CoreIndexStore,
    IndexWriter as CoreIndexWriter, TagDimension, UtxoSet,
};

// ============================================================================
// IndexStoreBackend - Main enum wrapper
// ============================================================================

/// Enum wrapper for index store backends.
///
/// This allows runtime selection of the index store backend via configuration.
#[derive(Clone)]
pub enum IndexStoreBackend {
    Redb(dolos_redb3::indexes::IndexStore),
    Fjall(dolos_fjall::IndexStore),
    NoOp(NoOpIndexStore),
}

impl IndexStoreBackend {
    /// Open an index store based on the configuration.
    ///
    /// The path is resolved from the storage config using the `index_path()` helper.
    pub fn open(config: &StorageConfig) -> Result<Self, IndexError> {
        match &config.index {
            IndexStoreConfig::Redb(cfg) => {
                let path = config.index_path().ok_or_else(|| {
                    IndexError::DbError(
                        "cannot determine index path for ephemeral config".to_string(),
                    )
                })?;

                std::fs::create_dir_all(path.parent().unwrap_or(&path))
                    .map_err(|e| IndexError::DbError(e.to_string()))?;

                let store = dolos_redb3::indexes::IndexStore::open(&path, cfg.cache)?;
                Ok(Self::Redb(store))
            }
            IndexStoreConfig::InMemory => {
                let store = dolos_redb3::indexes::IndexStore::in_memory()?;
                Ok(Self::Redb(store))
            }
            IndexStoreConfig::Fjall(cfg) => {
                let path = config.index_path().ok_or_else(|| {
                    IndexError::DbError(
                        "cannot determine index path for ephemeral config".to_string(),
                    )
                })?;

                std::fs::create_dir_all(path.parent().unwrap_or(&path))
                    .map_err(|e| IndexError::DbError(e.to_string()))?;

                let store = dolos_fjall::IndexStore::open(
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
            IndexStoreConfig::NoOp => Ok(Self::NoOp(NoOpIndexStore::default())),
        }
    }

    /// Open an in-memory index store directly.
    pub fn in_memory() -> Result<Self, IndexError> {
        let store = dolos_redb3::indexes::IndexStore::in_memory()?;
        Ok(Self::Redb(store))
    }

    /// Gracefully shutdown the index store.
    ///
    /// This ensures all pending work is completed before the store is dropped.
    pub fn shutdown(&self) -> Result<(), IndexError> {
        match self {
            Self::Redb(s) => s.shutdown().map_err(|e| IndexError::DbError(e.to_string())),
            Self::Fjall(s) => s.shutdown().map_err(|e| IndexError::DbError(e.to_string())),
            Self::NoOp(s) => s.shutdown(),
        }
    }
}

// ============================================================================
// IndexWriterBackend - Writer wrapper
// ============================================================================

/// Enum wrapper for index store writers.
pub enum IndexWriterBackend {
    Redb(<dolos_redb3::indexes::IndexStore as CoreIndexStore>::Writer),
    Fjall(<dolos_fjall::IndexStore as CoreIndexStore>::Writer),
    NoOp(NoOpIndexWriter),
}

impl CoreIndexWriter for IndexWriterBackend {
    fn apply(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        match self {
            Self::Redb(w) => w.apply(delta),
            Self::Fjall(w) => w.apply(delta),
            Self::NoOp(w) => w.apply(delta),
        }
    }

    fn undo(&self, delta: &IndexDelta) -> Result<(), IndexError> {
        match self {
            Self::Redb(w) => w.undo(delta),
            Self::Fjall(w) => w.undo(delta),
            Self::NoOp(w) => w.undo(delta),
        }
    }

    fn commit(self) -> Result<(), IndexError> {
        match self {
            Self::Redb(w) => w.commit(),
            Self::Fjall(w) => w.commit(),
            Self::NoOp(w) => w.commit(),
        }
    }
}

// ============================================================================
// SlotIterBackend - Slot iterator wrapper
// ============================================================================

/// Enum wrapper for slot iterators.
pub enum SlotIterBackend {
    Redb(<dolos_redb3::indexes::IndexStore as CoreIndexStore>::SlotIter),
    Fjall(<dolos_fjall::IndexStore as CoreIndexStore>::SlotIter),
    NoOp(EmptySlotIter),
}

impl Iterator for SlotIterBackend {
    type Item = Result<BlockSlot, IndexError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl DoubleEndedIterator for SlotIterBackend {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next_back(),
            Self::Fjall(iter) => iter.next_back(),
            Self::NoOp(iter) => iter.next_back(),
        }
    }
}

// ============================================================================
// IndexStore trait implementation
// ============================================================================

impl CoreIndexStore for IndexStoreBackend {
    type Writer = IndexWriterBackend;
    type SlotIter = SlotIterBackend;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        match self {
            Self::Redb(s) => s.start_writer().map(IndexWriterBackend::Redb),
            Self::Fjall(s) => s.start_writer().map(IndexWriterBackend::Fjall),
            Self::NoOp(s) => s.start_writer().map(IndexWriterBackend::NoOp),
        }
    }

    fn initialize_schema(&self) -> Result<(), IndexError> {
        match self {
            Self::Redb(s) => s.initialize_schema(),
            Self::Fjall(s) => s.initialize_schema(),
            Self::NoOp(s) => s.initialize_schema(),
        }
    }

    fn copy(&self, target: &Self) -> Result<(), IndexError> {
        match (self, target) {
            (Self::Redb(s), Self::Redb(t)) => s.copy(t),
            (Self::Fjall(s), Self::Fjall(t)) => s.copy(t),
            (Self::NoOp(s), Self::NoOp(t)) => s.copy(t),
            _ => Err(IndexError::DbError(
                "Cannot copy between different backend types".to_string(),
            )),
        }
    }

    fn cursor(&self) -> Result<Option<ChainPoint>, IndexError> {
        match self {
            Self::Redb(s) => s.cursor(),
            Self::Fjall(s) => s.cursor(),
            Self::NoOp(s) => s.cursor(),
        }
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, IndexError> {
        match self {
            Self::Redb(s) => s.utxos_by_tag(dimension, key),
            Self::Fjall(s) => s.utxos_by_tag(dimension, key),
            Self::NoOp(s) => s.utxos_by_tag(dimension, key),
        }
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        match self {
            Self::Redb(s) => s.slot_by_block_hash(hash),
            Self::Fjall(s) => s.slot_by_block_hash(hash),
            Self::NoOp(s) => s.slot_by_block_hash(hash),
        }
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, IndexError> {
        match self {
            Self::Redb(s) => s.slot_by_block_number(number),
            Self::Fjall(s) => s.slot_by_block_number(number),
            Self::NoOp(s) => s.slot_by_block_number(number),
        }
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, IndexError> {
        match self {
            Self::Redb(s) => s.slot_by_tx_hash(hash),
            Self::Fjall(s) => s.slot_by_tx_hash(hash),
            Self::NoOp(s) => s.slot_by_tx_hash(hash),
        }
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, IndexError> {
        match self {
            Self::Redb(s) => s
                .slots_by_tag(dimension, key, start, end)
                .map(SlotIterBackend::Redb),
            Self::Fjall(s) => s
                .slots_by_tag(dimension, key, start, end)
                .map(SlotIterBackend::Fjall),
            Self::NoOp(s) => s
                .slots_by_tag(dimension, key, start, end)
                .map(SlotIterBackend::NoOp),
        }
    }
}
