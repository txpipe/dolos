//! WAL store backend wrapper for runtime backend selection.
//!
//! This module provides an enum wrapper around the concrete WAL store implementations
//! that implements the `WalStore` trait.

use dolos_core::{
    config::{StorageConfig, WalStoreConfig},
    BlockSlot, ChainPoint, EntityDelta, LogEntry, LogValue, RawBlock, WalError, WalStore,
};
use serde::{de::DeserializeOwned, Serialize};

// ============================================================================
// WalStoreBackend - Main enum wrapper
// ============================================================================

/// Enum wrapper for WAL store backends.
///
/// This allows runtime selection of the WAL store backend via configuration.
/// The WAL is generic over the delta type to support different chain implementations.
#[derive(Clone, Debug)]
pub enum WalStoreBackend<D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Redb(dolos_redb3::wal::RedbWalStore<D>),
}

impl<D> WalStoreBackend<D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Open a WAL store based on the configuration.
    ///
    /// The path is resolved from the storage config using the `wal_path()` helper.
    pub fn open(config: &StorageConfig) -> Result<Self, WalError> {
        match &config.wal {
            WalStoreConfig::Redb(cfg) => {
                let path = config.wal_path().ok_or_else(|| {
                    WalError::internal("cannot determine WAL path for ephemeral config")
                })?;

                std::fs::create_dir_all(path.parent().unwrap_or(&path))
                    .map_err(WalError::internal)?;

                let wal = dolos_redb3::wal::RedbWalStore::open(&path, cfg.cache)?;
                Ok(Self::Redb(wal))
            }
            WalStoreConfig::InMemory => {
                let wal = dolos_redb3::wal::RedbWalStore::memory()?;
                Ok(Self::Redb(wal))
            }
        }
    }

    /// Open an in-memory WAL store directly.
    pub fn in_memory() -> Result<Self, WalError> {
        let wal = dolos_redb3::wal::RedbWalStore::memory()?;
        Ok(Self::Redb(wal))
    }

    /// Check if the WAL is empty.
    pub fn is_empty(&self) -> Result<bool, WalError> {
        match self {
            Self::Redb(s) => s.is_empty().map_err(|e| WalError::internal(e)),
        }
    }

    /// Get a mutable reference to the inner database for compaction operations.
    ///
    /// Returns `None` if there are other references to the database.
    pub fn db_mut(&mut self) -> Option<&mut dolos_redb3::redb::Database> {
        match self {
            Self::Redb(s) => s.db_mut(),
        }
    }

    /// Gracefully shutdown the WAL store.
    pub fn shutdown(&self) -> Result<(), WalError> {
        match self {
            Self::Redb(s) => s.shutdown().map_err(|e| WalError::internal(e)),
        }
    }
}

// ============================================================================
// WalStore trait implementation
// ============================================================================

/// Wrapper for WAL log iterators.
pub enum LogIterBackend<'a, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Redb(<dolos_redb3::wal::RedbWalStore<D> as WalStore>::LogIterator<'a>),
}

impl<D> Iterator for LogIterBackend<'_, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = LogEntry<D>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
        }
    }
}

impl<D> DoubleEndedIterator for LogIterBackend<'_, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next_back(),
        }
    }
}

/// Wrapper for WAL block iterators.
pub enum BlockIterBackend<'a, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Redb(<dolos_redb3::wal::RedbWalStore<D> as WalStore>::BlockIterator<'a>),
}

impl<D> Iterator for BlockIterBackend<'_, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Item = (ChainPoint, RawBlock);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
        }
    }
}

impl<D> DoubleEndedIterator for BlockIterBackend<'_, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next_back(),
        }
    }
}

impl<D> WalStore for WalStoreBackend<D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Delta = D;
    type LogIterator<'a> = LogIterBackend<'a, D>;
    type BlockIterator<'a> = BlockIterBackend<'a, D>;

    fn reset_to(&self, point: &ChainPoint) -> Result<(), WalError> {
        match self {
            Self::Redb(s) => WalStore::reset_to(s, point),
        }
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), WalError> {
        match self {
            Self::Redb(s) => WalStore::truncate_front(s, after),
        }
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError> {
        match self {
            Self::Redb(s) => WalStore::prune_history(s, max_slots, max_prune),
        }
    }

    fn locate_point(&self, around: BlockSlot) -> Result<Option<ChainPoint>, WalError> {
        match self {
            Self::Redb(s) => WalStore::locate_point(s, around),
        }
    }

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<Self::Delta>>, WalError> {
        match self {
            Self::Redb(s) => WalStore::read_entry(s, key),
        }
    }

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        match self {
            Self::Redb(s) => WalStore::iter_logs(s, start, end).map(LogIterBackend::Redb),
        }
    }

    fn iter_blocks<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::BlockIterator<'a>, WalError> {
        match self {
            Self::Redb(s) => WalStore::iter_blocks(s, start, end).map(BlockIterBackend::Redb),
        }
    }

    fn append_entries(&self, logs: Vec<LogEntry<Self::Delta>>) -> Result<(), WalError> {
        match self {
            Self::Redb(s) => WalStore::append_entries(s, logs),
        }
    }

    fn remove_entries(&mut self, after: &ChainPoint) -> Result<(), WalError> {
        match self {
            Self::Redb(s) => WalStore::remove_entries(s, after),
        }
    }
}
