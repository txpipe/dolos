//! Storage backend wrappers for runtime backend selection.
//!
//! This module provides enum wrappers around the concrete storage implementations
//! (redb3, fjall, and noop) that implement the core storage traits. This enables runtime
//! selection of storage backends via configuration.
//!
//! The `open` functions are simple matchers that delegate directly to concrete
//! implementations, passing through the backend-specific config struct. All path
//! resolution and directory creation is handled by the caller.

use std::{ops::Range, path::Path, path::PathBuf};

use dolos_core::{
    archive::{
        ArchiveError, ArchiveStore as CoreArchiveStore, ArchiveWriter as CoreArchiveWriter, LogKey,
    },
    builtin::{
        EmptyBlockIter, EmptyLogIter, EmptySlotIter, NoOpArchiveStore, NoOpArchiveWriter,
        NoOpIndexStore, NoOpIndexWriter,
    },
    config::{
        ArchiveStoreConfig, FjallIndexConfig, FjallStateConfig, IndexStoreConfig,
        MempoolStoreConfig, RedbArchiveConfig, RedbIndexConfig, RedbStateConfig, RedbWalConfig,
        RootConfig, StateStoreConfig, StorageVersion, WalStoreConfig,
    },
    BlockBody, BlockSlot, ChainPoint, EntityDelta, EntityKey, EntityValue, EraCbor, IndexDelta,
    IndexError, IndexStore as CoreIndexStore, IndexWriter as CoreIndexWriter, LogEntry, LogValue,
    MempoolError, MempoolEvent, MempoolStore, MempoolTx, MempoolTxStage, Namespace, RawBlock,
    StateError, StateSchema, StateStore as CoreStateStore, StateWriter as CoreStateWriter,
    TagDimension, TxHash, TxoRef, UtxoMap, UtxoSet, UtxoSetDelta, WalError, WalStore,
};
use serde::{de::DeserializeOwned, Serialize};

use crate::prelude::Error;

pub struct Stores<D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub wal: WalStoreBackend<D>,
    pub state: StateStoreBackend,
    pub archive: ArchiveStoreBackend,
    pub indexes: IndexStoreBackend,
    pub mempool: MempoolBackend,
}

/// Ensure the storage root directory exists.
pub fn ensure_storage_path(config: &RootConfig) -> Result<PathBuf, Error> {
    std::fs::create_dir_all(&config.storage.path)?;
    Ok(config.storage.path.clone())
}

fn check_storage_version(config: &RootConfig) -> Result<(), Error> {
    if config.storage.version != StorageVersion::V3 {
        return Err(Error::StorageError(format!(
            "unsupported storage version {:?}, only V3 is supported",
            config.storage.version
        )));
    }
    Ok(())
}

/// Ensure directory exists for a store path.
fn ensure_store_path(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

pub fn open_wal_store<D>(config: &RootConfig) -> Result<WalStoreBackend<D>, Error>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    let path = config.storage.wal_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(WalStoreBackend::open(&path, &config.storage.wal)?)
}

pub fn open_archive_store(config: &RootConfig) -> Result<ArchiveStoreBackend, Error> {
    let path = config.storage.archive_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(ArchiveStoreBackend::open(
        &path,
        dolos_cardano::model::build_schema(),
        &config.storage.archive,
    )?)
}

pub fn open_index_store(config: &RootConfig) -> Result<IndexStoreBackend, Error> {
    let path = config.storage.index_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(IndexStoreBackend::open(&path, &config.storage.index)?)
}

pub fn open_state_store(config: &RootConfig) -> Result<StateStoreBackend, Error> {
    let path = config.storage.state_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(StateStoreBackend::open(
        &path,
        dolos_cardano::model::build_schema(),
        &config.storage.state,
    )?)
}

pub fn open_mempool_store(config: &RootConfig) -> Result<MempoolBackend, Error> {
    match &config.storage.mempool {
        MempoolStoreConfig::InMemory => Ok(MempoolBackend::Ephemeral(
            dolos_core::builtin::EphemeralMempool::new(),
        )),
        MempoolStoreConfig::Redb(cfg) => {
            let path = config.storage.mempool_path().unwrap_or_default();
            ensure_store_path(&path)?;
            Ok(MempoolBackend::Redb(
                dolos_redb3::mempool::RedbMempool::open(path, cfg)?,
            ))
        }
    }
}

pub fn open_data_stores<D>(config: &RootConfig) -> Result<Stores<D>, Error>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    check_storage_version(config)?;

    Ok(Stores {
        wal: open_wal_store(config)?,
        state: open_state_store(config)?,
        archive: open_archive_store(config)?,
        indexes: open_index_store(config)?,
        mempool: open_mempool_store(config)?,
    })
}

// ============================================================================
// WAL Store Backend
// ============================================================================

/// Enum wrapper for WAL store backends.
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
    /// Open a WAL store with the Redb backend.
    pub fn open_redb(path: impl AsRef<Path>, config: &RedbWalConfig) -> Result<Self, WalError> {
        Ok(Self::Redb(dolos_redb3::wal::RedbWalStore::open(
            path, config,
        )?))
    }

    /// Create an in-memory WAL store.
    pub fn in_memory() -> Result<Self, WalError> {
        Ok(Self::Redb(dolos_redb3::wal::RedbWalStore::memory()?))
    }

    /// Open a WAL store based on the config variant.
    ///
    /// For `Redb`, the caller must provide the resolved path.
    /// For `InMemory`, the path is ignored and an in-memory store is created.
    pub fn open(path: impl AsRef<Path>, config: &WalStoreConfig) -> Result<Self, WalError> {
        match config {
            WalStoreConfig::Redb(cfg) => Self::open_redb(path, cfg),
            WalStoreConfig::InMemory => Self::in_memory(),
        }
    }

    pub fn is_empty(&self) -> Result<bool, WalError> {
        match self {
            Self::Redb(s) => s.is_empty().map_err(WalError::internal),
        }
    }

    pub fn db_mut(&mut self) -> Option<&mut dolos_redb3::redb::Database> {
        match self {
            Self::Redb(s) => s.db_mut(),
        }
    }

    pub fn shutdown(&self) -> Result<(), WalError> {
        match self {
            Self::Redb(s) => s.shutdown().map_err(WalError::internal),
        }
    }
}

pub enum WalLogIterBackend<'a, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Redb(<dolos_redb3::wal::RedbWalStore<D> as WalStore>::LogIterator<'a>),
}

impl<D> Iterator for WalLogIterBackend<'_, D>
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

impl<D> DoubleEndedIterator for WalLogIterBackend<'_, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next_back(),
        }
    }
}

pub enum WalBlockIterBackend<'a, D>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Redb(<dolos_redb3::wal::RedbWalStore<D> as WalStore>::BlockIterator<'a>),
}

impl<D> Iterator for WalBlockIterBackend<'_, D>
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

impl<D> DoubleEndedIterator for WalBlockIterBackend<'_, D>
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
    type LogIterator<'a> = WalLogIterBackend<'a, D>;
    type BlockIterator<'a> = WalBlockIterBackend<'a, D>;

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
            Self::Redb(s) => WalStore::iter_logs(s, start, end).map(WalLogIterBackend::Redb),
        }
    }

    fn iter_blocks<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::BlockIterator<'a>, WalError> {
        match self {
            Self::Redb(s) => WalStore::iter_blocks(s, start, end).map(WalBlockIterBackend::Redb),
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

// ============================================================================
// State Store Backend
// ============================================================================

/// Enum wrapper for state store backends.
#[derive(Clone)]
pub enum StateStoreBackend {
    Redb(dolos_redb3::state::StateStore),
    Fjall(dolos_fjall::StateStore),
}

impl StateStoreBackend {
    /// Open a state store with the Redb backend.
    pub fn open_redb(
        path: impl AsRef<Path>,
        schema: StateSchema,
        config: &RedbStateConfig,
    ) -> Result<Self, StateError> {
        Ok(Self::Redb(dolos_redb3::state::StateStore::open(
            schema, path, config,
        )?))
    }

    /// Open a state store with the Fjall backend.
    pub fn open_fjall(
        path: impl AsRef<Path>,
        config: &FjallStateConfig,
    ) -> Result<Self, StateError> {
        Ok(Self::Fjall(dolos_fjall::StateStore::open(path, config)?))
    }

    /// Create an in-memory state store.
    pub fn in_memory(schema: StateSchema) -> Result<Self, StateError> {
        Ok(Self::Redb(dolos_redb3::state::StateStore::in_memory(
            schema,
        )?))
    }

    /// Open a state store based on the config variant.
    ///
    /// For persistent backends, the caller must provide the resolved path.
    /// For `InMemory`, the path is ignored and an in-memory store is created.
    pub fn open(
        path: impl AsRef<Path>,
        schema: StateSchema,
        config: &StateStoreConfig,
    ) -> Result<Self, StateError> {
        match config {
            StateStoreConfig::Redb(cfg) => Self::open_redb(path, schema, cfg),
            StateStoreConfig::Fjall(cfg) => Self::open_fjall(path, cfg),
            StateStoreConfig::InMemory => Self::in_memory(schema),
        }
    }

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

pub enum StateWriterBackend {
    Redb(Box<<dolos_redb3::state::StateStore as CoreStateStore>::Writer>),
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
            Self::Redb(w) => (*w).commit(),
            Self::Fjall(w) => w.commit(),
        }
    }
}

pub enum StateEntityIterBackend {
    Redb(<dolos_redb3::state::StateStore as CoreStateStore>::EntityIter),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::EntityIter),
}

impl Iterator for StateEntityIterBackend {
    type Item = Result<(EntityKey, EntityValue), StateError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
        }
    }
}

pub enum StateEntityValueIterBackend {
    Redb(Box<<dolos_redb3::state::StateStore as CoreStateStore>::EntityValueIter>),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::EntityValueIter),
}

impl Iterator for StateEntityValueIterBackend {
    type Item = Result<EntityValue, StateError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
        }
    }
}

impl CoreStateStore for StateStoreBackend {
    type EntityIter = StateEntityIterBackend;
    type EntityValueIter = StateEntityValueIterBackend;
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
            Self::Redb(s) => s
                .start_writer()
                .map(|writer| StateWriterBackend::Redb(Box::new(writer))),
            Self::Fjall(s) => s.start_writer().map(StateWriterBackend::Fjall),
        }
    }

    fn iter_entities(
        &self,
        ns: Namespace,
        range: Range<EntityKey>,
    ) -> Result<Self::EntityIter, StateError> {
        match self {
            Self::Redb(s) => s.iter_entities(ns, range).map(StateEntityIterBackend::Redb),
            Self::Fjall(s) => s
                .iter_entities(ns, range)
                .map(StateEntityIterBackend::Fjall),
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
                .map(|iter| StateEntityValueIterBackend::Redb(Box::new(iter))),
            Self::Fjall(s) => s
                .iter_entity_values(ns, key)
                .map(StateEntityValueIterBackend::Fjall),
        }
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        match self {
            Self::Redb(s) => s.get_utxos(refs),
            Self::Fjall(s) => s.get_utxos(refs),
        }
    }
}

// ============================================================================
// Archive Store Backend
// ============================================================================

/// Enum wrapper for archive store backends.
#[derive(Clone)]
pub enum ArchiveStoreBackend {
    Redb(dolos_redb3::archive::ArchiveStore),
    NoOp(NoOpArchiveStore),
}

impl ArchiveStoreBackend {
    /// Open an archive store with the Redb backend.
    pub fn open_redb(
        path: impl AsRef<Path>,
        schema: StateSchema,
        config: &RedbArchiveConfig,
    ) -> Result<Self, ArchiveError> {
        Ok(Self::Redb(dolos_redb3::archive::ArchiveStore::open(
            schema, path, config,
        )?))
    }

    /// Create a no-op archive store that discards all writes.
    pub fn noop() -> Self {
        Self::NoOp(NoOpArchiveStore)
    }

    /// Create an in-memory archive store.
    pub fn in_memory(schema: StateSchema) -> Result<Self, ArchiveError> {
        Ok(Self::Redb(dolos_redb3::archive::ArchiveStore::in_memory(
            schema,
        )?))
    }

    /// Open an archive store based on the config variant.
    ///
    /// For persistent backends, the caller must provide the resolved path.
    /// For `InMemory`, the path is ignored and an in-memory store is created.
    /// For `NoOp`, the path and schema are ignored.
    pub fn open(
        path: impl AsRef<Path>,
        schema: StateSchema,
        config: &ArchiveStoreConfig,
    ) -> Result<Self, ArchiveError> {
        match config {
            ArchiveStoreConfig::Redb(cfg) => Self::open_redb(path, schema, cfg),
            ArchiveStoreConfig::InMemory => Self::in_memory(schema),
            ArchiveStoreConfig::NoOp => Ok(Self::noop()),
        }
    }

    pub fn shutdown(&self) -> Result<(), ArchiveError> {
        match self {
            Self::Redb(s) => s
                .shutdown()
                .map_err(|e| ArchiveError::InternalError(e.to_string())),
            Self::NoOp(s) => s.shutdown(),
        }
    }
}

pub enum ArchiveWriterBackend {
    Redb(Box<<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::Writer>),
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
            Self::Redb(w) => (*w).commit(),
            Self::NoOp(w) => w.commit(),
        }
    }
}

pub enum ArchiveBlockIterBackend {
    Redb(Box<<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::BlockIter<'static>>),
    NoOp(EmptyBlockIter),
}

impl Iterator for ArchiveBlockIterBackend {
    type Item = (BlockSlot, BlockBody);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl DoubleEndedIterator for ArchiveBlockIterBackend {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next_back(),
            Self::NoOp(iter) => iter.next_back(),
        }
    }
}

impl dolos_core::archive::Skippable for ArchiveBlockIterBackend {
    fn skip_forward(&mut self, n: usize) {
        match self {
            Self::Redb(iter) => iter.skip_forward(n),
            Self::NoOp(iter) => iter.skip_forward(n),
        }
    }

    fn skip_backward(&mut self, n: usize) {
        match self {
            Self::Redb(iter) => iter.skip_backward(n),
            Self::NoOp(iter) => iter.skip_backward(n),
        }
    }
}

pub enum ArchiveLogIterBackend {
    Redb(Box<<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::LogIter>),
    NoOp(EmptyLogIter),
}

impl Iterator for ArchiveLogIterBackend {
    type Item = Result<(LogKey, EntityValue), ArchiveError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

pub enum ArchiveEntityValueIterBackend {
    Redb(Box<<dolos_redb3::archive::ArchiveStore as CoreArchiveStore>::EntityValueIter>),
    NoOp(dolos_core::builtin::EmptyEntityValueIter),
}

impl Iterator for ArchiveEntityValueIterBackend {
    type Item = Result<EntityValue, ArchiveError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl CoreArchiveStore for ArchiveStoreBackend {
    type BlockIter<'a> = ArchiveBlockIterBackend;
    type Writer = ArchiveWriterBackend;
    type LogIter = ArchiveLogIterBackend;
    type EntityValueIter = ArchiveEntityValueIterBackend;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        match self {
            Self::Redb(s) => CoreArchiveStore::start_writer(s)
                .map(|writer| ArchiveWriterBackend::Redb(Box::new(writer))),
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
            Self::Redb(s) => CoreArchiveStore::iter_logs(s, ns, range)
                .map(|iter| ArchiveLogIterBackend::Redb(Box::new(iter))),
            Self::NoOp(s) => {
                CoreArchiveStore::iter_logs(s, ns, range).map(ArchiveLogIterBackend::NoOp)
            }
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
            Self::Redb(s) => CoreArchiveStore::get_range(s, from, to)
                .map(|iter| ArchiveBlockIterBackend::Redb(Box::new(iter))),
            Self::NoOp(s) => {
                CoreArchiveStore::get_range(s, from, to).map(ArchiveBlockIterBackend::NoOp)
            }
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

// ============================================================================
// Index Store Backend
// ============================================================================

/// Enum wrapper for index store backends.
#[derive(Clone)]
pub enum IndexStoreBackend {
    Redb(dolos_redb3::indexes::IndexStore),
    Fjall(dolos_fjall::IndexStore),
    NoOp(NoOpIndexStore),
}

impl IndexStoreBackend {
    /// Open an index store with the Redb backend.
    pub fn open_redb(path: impl AsRef<Path>, config: &RedbIndexConfig) -> Result<Self, IndexError> {
        Ok(Self::Redb(dolos_redb3::indexes::IndexStore::open(
            path, config,
        )?))
    }

    /// Open an index store with the Fjall backend.
    pub fn open_fjall(
        path: impl AsRef<Path>,
        config: &FjallIndexConfig,
    ) -> Result<Self, IndexError> {
        Ok(Self::Fjall(dolos_fjall::IndexStore::open(path, config)?))
    }

    /// Create a no-op index store that discards all writes.
    pub fn noop() -> Self {
        Self::NoOp(NoOpIndexStore)
    }

    /// Create an in-memory index store.
    pub fn in_memory() -> Result<Self, IndexError> {
        Ok(Self::Redb(dolos_redb3::indexes::IndexStore::in_memory()?))
    }

    /// Open an index store based on the config variant.
    ///
    /// For persistent backends, the caller must provide the resolved path.
    /// For `InMemory`, the path is ignored and an in-memory store is created.
    /// For `NoOp`, the path is ignored.
    pub fn open(path: impl AsRef<Path>, config: &IndexStoreConfig) -> Result<Self, IndexError> {
        match config {
            IndexStoreConfig::Redb(cfg) => Self::open_redb(path, cfg),
            IndexStoreConfig::Fjall(cfg) => Self::open_fjall(path, cfg),
            IndexStoreConfig::InMemory => Self::in_memory(),
            IndexStoreConfig::NoOp => Ok(Self::noop()),
        }
    }

    pub fn shutdown(&self) -> Result<(), IndexError> {
        match self {
            Self::Redb(s) => s.shutdown().map_err(|e| IndexError::DbError(e.to_string())),
            Self::Fjall(s) => s.shutdown().map_err(|e| IndexError::DbError(e.to_string())),
            Self::NoOp(s) => s.shutdown(),
        }
    }
}

pub enum IndexWriterBackend {
    Redb(Box<<dolos_redb3::indexes::IndexStore as CoreIndexStore>::Writer>),
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
            Self::Redb(w) => (*w).commit(),
            Self::Fjall(w) => w.commit(),
            Self::NoOp(w) => w.commit(),
        }
    }
}

pub enum IndexSlotIterBackend {
    Redb(Box<<dolos_redb3::indexes::IndexStore as CoreIndexStore>::SlotIter>),
    Fjall(<dolos_fjall::IndexStore as CoreIndexStore>::SlotIter),
    NoOp(EmptySlotIter),
}

impl Iterator for IndexSlotIterBackend {
    type Item = Result<BlockSlot, IndexError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl DoubleEndedIterator for IndexSlotIterBackend {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next_back(),
            Self::Fjall(iter) => iter.next_back(),
            Self::NoOp(iter) => iter.next_back(),
        }
    }
}

impl CoreIndexStore for IndexStoreBackend {
    type Writer = IndexWriterBackend;
    type SlotIter = IndexSlotIterBackend;

    fn start_writer(&self) -> Result<Self::Writer, IndexError> {
        match self {
            Self::Redb(s) => s
                .start_writer()
                .map(|writer| IndexWriterBackend::Redb(Box::new(writer))),
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
                "cannot copy between different backend types".into(),
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
                .map(|iter| IndexSlotIterBackend::Redb(Box::new(iter))),
            Self::Fjall(s) => s
                .slots_by_tag(dimension, key, start, end)
                .map(IndexSlotIterBackend::Fjall),
            Self::NoOp(s) => s
                .slots_by_tag(dimension, key, start, end)
                .map(IndexSlotIterBackend::NoOp),
        }
    }
}

// ============================================================================
// Mempool Store Backend
// ============================================================================

/// Enum wrapper for mempool store backends.
#[derive(Clone)]
pub enum MempoolBackend {
    Ephemeral(dolos_core::builtin::EphemeralMempool),
    Redb(dolos_redb3::mempool::RedbMempool),
}

pub enum MempoolStreamBackend {
    Ephemeral(dolos_core::builtin::EphemeralMempoolStream),
    Redb(dolos_redb3::mempool::RedbMempoolStream),
}

impl futures_core::Stream for MempoolStreamBackend {
    type Item = Result<MempoolEvent, MempoolError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.get_mut() {
            MempoolStreamBackend::Ephemeral(s) => std::pin::Pin::new(s).poll_next(cx),
            MempoolStreamBackend::Redb(s) => std::pin::Pin::new(s).poll_next(cx),
        }
    }
}

impl MempoolStore for MempoolBackend {
    type Stream = MempoolStreamBackend;

    fn receive(&self, tx: MempoolTx) -> Result<(), MempoolError> {
        match self {
            Self::Ephemeral(s) => s.receive(tx),
            Self::Redb(s) => s.receive(tx),
        }
    }

    fn has_pending(&self) -> bool {
        match self {
            Self::Ephemeral(s) => s.has_pending(),
            Self::Redb(s) => s.has_pending(),
        }
    }

    fn peek_pending(&self, limit: usize) -> Vec<MempoolTx> {
        match self {
            Self::Ephemeral(s) => s.peek_pending(limit),
            Self::Redb(s) => s.peek_pending(limit),
        }
    }

    fn pending(&self) -> Vec<(TxHash, EraCbor)> {
        match self {
            Self::Ephemeral(s) => s.pending(),
            Self::Redb(s) => s.pending(),
        }
    }

    fn mark_inflight(&self, hashes: &[TxHash]) {
        match self {
            Self::Ephemeral(s) => s.mark_inflight(hashes),
            Self::Redb(s) => s.mark_inflight(hashes),
        }
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) {
        match self {
            Self::Ephemeral(s) => s.mark_acknowledged(hashes),
            Self::Redb(s) => s.mark_acknowledged(hashes),
        }
    }

    fn get_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        match self {
            Self::Ephemeral(s) => s.get_inflight(tx_hash),
            Self::Redb(s) => s.get_inflight(tx_hash),
        }
    }

    fn apply(&self, seen_txs: &[TxHash], unseen_txs: &[TxHash]) {
        match self {
            Self::Ephemeral(s) => s.apply(seen_txs, unseen_txs),
            Self::Redb(s) => s.apply(seen_txs, unseen_txs),
        }
    }

    fn finalize(&self, threshold: u32) {
        match self {
            Self::Ephemeral(s) => s.finalize(threshold),
            Self::Redb(s) => s.finalize(threshold),
        }
    }

    fn check_stage(&self, tx_hash: &TxHash) -> MempoolTxStage {
        match self {
            Self::Ephemeral(s) => s.check_stage(tx_hash),
            Self::Redb(s) => s.check_stage(tx_hash),
        }
    }

    fn subscribe(&self) -> Self::Stream {
        match self {
            Self::Ephemeral(s) => MempoolStreamBackend::Ephemeral(s.subscribe()),
            Self::Redb(s) => MempoolStreamBackend::Redb(s.subscribe()),
        }
    }
}
