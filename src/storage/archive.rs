//! Archive store backend wrapper for runtime backend selection.
//!
//! This module provides an enum wrapper around the concrete archive store implementations
//! (redb3 and noop) that implements the `ArchiveStore` trait.

use std::ops::Range;

use dolos_core::{
    archive::{
        ArchiveError, ArchiveStore as CoreArchiveStore, ArchiveWriter as CoreArchiveWriter, LogKey,
    },
    builtin::{EmptyBlockIter, EmptyLogIter, NoOpArchiveStore, NoOpArchiveWriter},
    config::{ArchiveStoreConfig, StorageConfig},
    BlockBody, BlockSlot, ChainPoint, EntityValue, Namespace, RawBlock, StateSchema,
};

// ============================================================================
// ArchiveStore - Main enum wrapper
// ============================================================================

/// Enum wrapper for archive store backends.
///
/// This allows runtime selection of the archive store backend via configuration.
#[derive(Clone)]
pub enum ArchiveStoreBackend {
    Redb(dolos_redb3::archive::ArchiveStore),
    NoOp(NoOpArchiveStore),
}

impl ArchiveStoreBackend {
    /// Open an archive store based on the configuration.
    ///
    /// The path is resolved from the storage config using the `archive_path()` helper.
    /// The schema is required for redb backends to initialize the entity tables.
    pub fn open(config: &StorageConfig, schema: StateSchema) -> Result<Self, ArchiveError> {
        match &config.archive {
            ArchiveStoreConfig::Redb(cfg) => {
                let path = config.archive_path().ok_or_else(|| {
                    ArchiveError::InternalError(
                        "cannot determine archive path for ephemeral config".to_string(),
                    )
                })?;

                std::fs::create_dir_all(path.parent().unwrap_or(&path))
                    .map_err(|e| ArchiveError::InternalError(e.to_string()))?;

                let archive = dolos_redb3::archive::ArchiveStore::open(schema, &path, cfg.cache)?;
                Ok(Self::Redb(archive))
            }
            ArchiveStoreConfig::InMemory => {
                let archive = dolos_redb3::archive::ArchiveStore::in_memory(schema)?;
                Ok(Self::Redb(archive))
            }
            ArchiveStoreConfig::NoOp => Ok(Self::NoOp(NoOpArchiveStore::default())),
        }
    }

    /// Open an in-memory archive store directly.
    pub fn in_memory(schema: StateSchema) -> Result<Self, ArchiveError> {
        let archive = dolos_redb3::archive::ArchiveStore::in_memory(schema)?;
        Ok(Self::Redb(archive))
    }

    /// Gracefully shutdown the archive store.
    ///
    /// This ensures all pending work is completed before the store is dropped.
    pub fn shutdown(&self) -> Result<(), ArchiveError> {
        match self {
            Self::Redb(s) => s
                .shutdown()
                .map_err(|e| ArchiveError::InternalError(e.to_string())),
            Self::NoOp(s) => s.shutdown(),
        }
    }
}

// ============================================================================
// ArchiveWriterBackend - Writer wrapper
// ============================================================================

/// Enum wrapper for archive store writers.
pub enum ArchiveWriterBackend {
    Redb(<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::Writer),
    NoOp(NoOpArchiveWriter),
}

impl CoreArchiveWriter for ArchiveWriterBackend {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError> {
        match self {
            Self::Redb(w) => w.apply(point, block),
            Self::NoOp(w) => w.apply(point, block),
        }
    }

    fn write_log(
        &self,
        ns: Namespace,
        key: &LogKey,
        value: &EntityValue,
    ) -> Result<(), ArchiveError> {
        match self {
            Self::Redb(w) => w.write_log(ns, key, value),
            Self::NoOp(w) => w.write_log(ns, key, value),
        }
    }

    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError> {
        match self {
            Self::Redb(w) => w.undo(point),
            Self::NoOp(w) => w.undo(point),
        }
    }

    fn commit(self) -> Result<(), ArchiveError> {
        match self {
            Self::Redb(w) => w.commit(),
            Self::NoOp(w) => w.commit(),
        }
    }
}

// ============================================================================
// BlockIterBackend - Block iterator wrapper
// ============================================================================

/// Enum wrapper for block iterators.
pub enum BlockIterBackend {
    Redb(<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::BlockIter<'static>),
    NoOp(EmptyBlockIter),
}

impl Iterator for BlockIterBackend {
    type Item = (BlockSlot, BlockBody);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl DoubleEndedIterator for BlockIterBackend {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next_back(),
            Self::NoOp(iter) => iter.next_back(),
        }
    }
}

// ============================================================================
// LogIterBackend - Log iterator wrapper
// ============================================================================

/// Enum wrapper for log iterators.
pub enum LogIterBackend {
    Redb(<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::LogIter),
    NoOp(EmptyLogIter),
}

impl Iterator for LogIterBackend {
    type Item = Result<(LogKey, EntityValue), ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

// ============================================================================
// EntityValueIterBackend - Entity value iterator wrapper
// ============================================================================

/// Enum wrapper for entity value iterators.
pub enum EntityValueIterBackend {
    Redb(<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::EntityValueIter),
    NoOp(dolos_core::builtin::EmptyEntityValueIter),
}

impl Iterator for EntityValueIterBackend {
    type Item = Result<EntityValue, ArchiveError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

// ============================================================================
// ArchiveStore trait implementation
// ============================================================================

impl CoreArchiveStore for ArchiveStoreBackend {
    type BlockIter<'a> = BlockIterBackend;
    type Writer = ArchiveWriterBackend;
    type LogIter = LogIterBackend;
    type EntityValueIter = EntityValueIterBackend;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::start_writer(s).map(ArchiveWriterBackend::Redb),
            Self::NoOp(s) => CoreArchiveStore::start_writer(s).map(ArchiveWriterBackend::NoOp),
        }
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::read_logs(s, ns, keys),
            Self::NoOp(s) => CoreArchiveStore::read_logs(s, ns, keys),
        }
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: Range<LogKey>,
    ) -> Result<Self::LogIter, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::iter_logs(s, ns, range).map(LogIterBackend::Redb),
            Self::NoOp(s) => CoreArchiveStore::iter_logs(s, ns, range).map(LogIterBackend::NoOp),
        }
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::get_block_by_slot(s, slot),
            Self::NoOp(s) => CoreArchiveStore::get_block_by_slot(s, slot),
        }
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::get_range(s, from, to).map(BlockIterBackend::Redb),
            Self::NoOp(s) => CoreArchiveStore::get_range(s, from, to).map(BlockIterBackend::NoOp),
        }
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::find_intersect(s, intersect),
            Self::NoOp(s) => CoreArchiveStore::find_intersect(s, intersect),
        }
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::get_tip(s),
            Self::NoOp(s) => CoreArchiveStore::get_tip(s),
        }
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::prune_history(s, max_slots, max_prune),
            Self::NoOp(s) => CoreArchiveStore::prune_history(s, max_slots, max_prune),
        }
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::truncate_front(s, after),
            Self::NoOp(s) => CoreArchiveStore::truncate_front(s, after),
        }
    }
}
