//! Storage backend wrappers for runtime backend selection.
//!
//! This module provides enum wrappers around the concrete storage
//! implementations (fjall, the builtin memory stores, and redb3's WAL and
//! mempool) that implement the core storage traits. This enables runtime
//! selection of storage backends via configuration.
//!
//! The `open` functions are simple matchers that delegate directly to concrete
//! implementations, passing through the backend-specific config struct. All
//! path resolution and directory creation is handled by the caller.

use std::{ops::Range, path::Path, path::PathBuf};

use dolos_core::{
    archive::{
        ArchiveError, ArchiveStore as CoreArchiveStore, ArchiveWriter as CoreArchiveWriter, LogKey,
    },
    builtin::{
        EmptyBlockIter, EmptyLogIter, EmptySlotIter, MemoryArchiveStore, MemoryStateStore,
        MemoryStateWriter, NoOpArchiveStore, NoOpArchiveWriter,
    },
    config::{
        ArchiveStoreConfig, FjallStateConfig, MempoolStoreConfig, RedbStateConfig, RedbWalConfig,
        RootConfig, StateStoreConfig, StorageVersion, WalStoreConfig,
    },
    ArchiveIndexDelta, BlockBody, BlockSlot, ChainPoint, EntityDelta, EntityKey, EntityValue,
    ExactRecord, IndexRecord, LogEntry, LogValue, MempoolError, MempoolEvent, MempoolStore,
    MempoolTx, Namespace, RawBlock, StateError, StateSchema, StateStore as CoreStateStore,
    StateWriter as CoreStateWriter, TagDimension, TagRecord, TxHash, TxStatus, TxoRef, UtxoEntry,
    UtxoIndexDelta, UtxoMap, UtxoSet, UtxoSetDelta, WalError, WalStore,
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
    pub mempool: MempoolBackend,
}

/// Ensure the storage root directory exists.
pub fn ensure_storage_path(config: &RootConfig) -> Result<PathBuf, Error> {
    std::fs::create_dir_all(&config.storage.path)?;
    Ok(config.storage.path.clone())
}

/// Empty the storage directory.
///
/// A `remove_dir_all` of the whole path and not a per-store wipe, which matters
/// for one thing that is not a store: a stele restore's progress file lives
/// inside `storage.path`, and a progress file that outlived the stores it
/// describes would tell the next `--continue` to skip layers whose data is
/// gone. Anything that clears storage has to clear that too, and taking the
/// directory is how this does it without having to remember.
pub fn clear_storage(storage_path: &Path) -> Result<(), Error> {
    std::fs::remove_dir_all(storage_path)
        .map_err(|e| Error::StorageError(format!("removing existing storage: {e}")))?;

    std::fs::create_dir_all(storage_path)
        .map_err(|e| Error::StorageError(format!("recreating storage directory: {e}")))?;

    Ok(())
}

/// What to do about data already in storage.
///
/// Deciding is separated from doing because only one of the outcomes is
/// destructive, and it must not happen until the run is fully known. An
/// interactive bootstrap asks which method to use — and, for a stele, where the
/// stele is — *after* the flags are parsed, so a `--force` that cleared first
/// would take a working node away on a typo, a cancel, or a machine with no
/// terminal, and hand back nothing in its place.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Existing {
    /// Data is there and `--skip-if-data` says to leave it alone.
    Skip,
    /// Go ahead, but clear storage first.
    Clear,
    /// Go ahead as things are.
    Proceed,
}

/// The three flags that say what to do about data already in storage.
#[derive(Debug, Clone, Copy, Default)]
pub struct ExistingDataPolicy {
    /// `--force`: clear storage and re-bootstrap.
    pub force: bool,
    /// `--skip-if-data`: leave it alone and exit zero.
    pub skip_if_data: bool,
    /// `--continue`: go ahead, trusting the run to resume.
    pub r#continue: bool,
}

/// Whether storage already holds a node: a state cursor is the whole test.
pub fn has_existing_data(config: &RootConfig) -> Result<bool, Error> {
    let state = open_state_store(config)?;

    Ok(state.read_cursor()?.is_some())
}

/// Read what is in storage and decide, without touching any of it.
///
/// The refusals happen here — a skip, and the refusal for existing data with no
/// flag saying what to do about it — so neither is a question asked of an
/// operator whose answer is then thrown away.
pub fn inspect_existing_data(
    config: &RootConfig,
    policy: ExistingDataPolicy,
) -> Result<Existing, Error> {
    check_storage_version(&config.storage.version)?;

    if policy.r#continue {
        return Ok(Existing::Proceed);
    }

    if !has_existing_data(config)? {
        return Ok(Existing::Proceed);
    }

    if policy.skip_if_data {
        return Ok(Existing::Skip);
    }

    if policy.force {
        return Ok(Existing::Clear);
    }

    Err(Error::message(
        "existing data detected in storage. Use --force to clear and re-bootstrap, \
         --skip-if-data to skip, or --continue to resume",
    ))
}

/// The storage version this binary reads. A store built by an older dolos is
/// not migrated in place: the supported path off it is a fresh `dolos init`
/// followed by a restore or a re-sync.
pub const CURRENT_STORAGE_VERSION: StorageVersion = StorageVersion::V4;

/// The migration guide the refusal points an operator at.
pub const MIGRATION_GUIDE_URL: &str = "https://docs.txpipe.io/dolos/migration/dolos-v2";

/// Refuse a configuration at any storage version but the current one.
///
/// The comparison is the whole compatibility policy: the version the config
/// declares against the one the binary carries, and nothing on disk. Every
/// store opener runs it before touching its path, so a refused configuration
/// has had no directory created and no store opened on its behalf — which is
/// what lets `dolos init` remain the one deliberate way past it.
fn check_storage_version(version: &StorageVersion) -> Result<(), Error> {
    if *version != CURRENT_STORAGE_VERSION {
        return Err(Error::StorageError(format!(
            "unsupported storage version `{version}`, this dolos only supports \
             `{CURRENT_STORAGE_VERSION}`; run `dolos init` to upgrade the configuration and \
             re-bootstrap the data — see the migration guide at {MIGRATION_GUIDE_URL}"
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
    check_storage_version(&config.storage.version)?;

    let path = config.storage.wal_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(WalStoreBackend::open(&path, &config.storage.wal)?)
}

pub fn open_archive_store(config: &RootConfig) -> Result<ArchiveStoreBackend, Error> {
    check_storage_version(&config.storage.version)?;

    let path = config.storage.archive_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(ArchiveStoreBackend::open(
        &path,
        dolos_cardano::model::build_schema(),
        &config.storage.archive,
    )?)
}

pub fn open_state_store(config: &RootConfig) -> Result<StateStoreBackend, Error> {
    check_storage_version(&config.storage.version)?;

    let path = config.storage.state_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(StateStoreBackend::open(
        &path,
        dolos_cardano::model::build_schema(),
        &config.storage.state,
    )?)
}

pub fn open_mempool_store(config: &RootConfig) -> Result<MempoolBackend, Error> {
    check_storage_version(&config.storage.version)?;

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
    check_storage_version(&config.storage.version)?;

    Ok(Stores {
        wal: open_wal_store(config)?,
        state: open_state_store(config)?,
        archive: open_archive_store(config)?,
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
    Memory(MemoryStateStore),
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
    ///
    /// Takes no schema: the builtin store discovers namespaces as they are
    /// written, so there is no table set to declare up front.
    pub fn in_memory() -> Result<Self, StateError> {
        Ok(Self::Memory(MemoryStateStore::new()))
    }

    /// Open a state store based on the config variant.
    ///
    /// For persistent backends, the caller must provide the resolved path.
    /// For `InMemory`, the path is ignored and an in-memory store is created.
    pub fn open(
        path: impl AsRef<Path>,
        _schema: StateSchema,
        config: &StateStoreConfig,
    ) -> Result<Self, StateError> {
        match config {
            StateStoreConfig::Fjall(cfg) => Self::open_fjall(path, cfg),
            StateStoreConfig::InMemory => Self::in_memory(),
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
            Self::Memory(s) => s.shutdown(),
        }
    }
}

pub enum StateWriterBackend {
    Redb(Box<<dolos_redb3::state::StateStore as CoreStateStore>::Writer>),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::Writer),
    Memory(MemoryStateWriter),
}

impl CoreStateWriter for StateWriterBackend {
    fn set_cursor(&self, cursor: ChainPoint) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.set_cursor(cursor),
            Self::Fjall(w) => w.set_cursor(cursor),
            Self::Memory(w) => w.set_cursor(cursor),
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
            Self::Memory(w) => w.write_entity(ns, key, value),
        }
    }

    fn delete_entity(&self, ns: Namespace, key: &EntityKey) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.delete_entity(ns, key),
            Self::Fjall(w) => w.delete_entity(ns, key),
            Self::Memory(w) => w.delete_entity(ns, key),
        }
    }

    fn apply_utxoset(&self, delta: &UtxoSetDelta) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.apply_utxoset(delta),
            Self::Fjall(w) => w.apply_utxoset(delta),
            Self::Memory(w) => w.apply_utxoset(delta),
        }
    }

    fn apply_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.apply_utxo_tags(delta),
            Self::Fjall(w) => w.apply_utxo_tags(delta),
            Self::Memory(w) => w.apply_utxo_tags(delta),
        }
    }

    fn undo_utxo_tags(&self, delta: &UtxoIndexDelta) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => w.undo_utxo_tags(delta),
            Self::Fjall(w) => w.undo_utxo_tags(delta),
            Self::Memory(w) => w.undo_utxo_tags(delta),
        }
    }

    fn commit(self) -> Result<(), StateError> {
        match self {
            Self::Redb(w) => (*w).commit(),
            Self::Fjall(w) => w.commit(),
            Self::Memory(w) => w.commit(),
        }
    }
}

pub enum StateEntityIterBackend {
    Redb(<dolos_redb3::state::StateStore as CoreStateStore>::EntityIter),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::EntityIter),
    Memory(<MemoryStateStore as CoreStateStore>::EntityIter),
}

impl Iterator for StateEntityIterBackend {
    type Item = Result<(EntityKey, EntityValue), StateError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::Memory(iter) => iter.next(),
        }
    }
}

pub enum StateEntityValueIterBackend {
    Redb(Box<<dolos_redb3::state::StateStore as CoreStateStore>::EntityValueIter>),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::EntityValueIter),
    Memory(<MemoryStateStore as CoreStateStore>::EntityValueIter),
}

impl Iterator for StateEntityValueIterBackend {
    type Item = Result<EntityValue, StateError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::Memory(iter) => iter.next(),
        }
    }
}

pub enum StateUtxoIterBackend {
    Redb(<dolos_redb3::state::StateStore as CoreStateStore>::UtxoIter),
    Fjall(<dolos_fjall::StateStore as CoreStateStore>::UtxoIter),
    Memory(<MemoryStateStore as CoreStateStore>::UtxoIter),
}

impl Iterator for StateUtxoIterBackend {
    type Item = Result<UtxoEntry, StateError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Redb(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::Memory(iter) => iter.next(),
        }
    }
}

impl CoreStateStore for StateStoreBackend {
    type EntityIter = StateEntityIterBackend;
    type EntityValueIter = StateEntityValueIterBackend;
    type UtxoIter = StateUtxoIterBackend;
    type Writer = StateWriterBackend;

    fn read_cursor(&self) -> Result<Option<ChainPoint>, StateError> {
        match self {
            Self::Redb(s) => s.read_cursor(),
            Self::Fjall(s) => s.read_cursor(),
            Self::Memory(s) => s.read_cursor(),
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
            Self::Memory(s) => s.read_entities(ns, keys),
        }
    }

    fn start_writer(&self) -> Result<Self::Writer, StateError> {
        match self {
            Self::Redb(s) => s
                .start_writer()
                .map(|writer| StateWriterBackend::Redb(Box::new(writer))),
            Self::Fjall(s) => s.start_writer().map(StateWriterBackend::Fjall),
            Self::Memory(s) => s.start_writer().map(StateWriterBackend::Memory),
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
            Self::Memory(s) => s
                .iter_entities(ns, range)
                .map(StateEntityIterBackend::Memory),
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
            Self::Memory(s) => s
                .iter_entity_values(ns, key)
                .map(StateEntityValueIterBackend::Memory),
        }
    }

    fn get_utxos(&self, refs: Vec<TxoRef>) -> Result<UtxoMap, StateError> {
        match self {
            Self::Redb(s) => s.get_utxos(refs),
            Self::Fjall(s) => s.get_utxos(refs),
            Self::Memory(s) => s.get_utxos(refs),
        }
    }

    fn utxos_by_tag(&self, dimension: TagDimension, key: &[u8]) -> Result<UtxoSet, StateError> {
        match self {
            Self::Redb(s) => s.utxos_by_tag(dimension, key),
            Self::Fjall(s) => s.utxos_by_tag(dimension, key),
            Self::Memory(s) => s.utxos_by_tag(dimension, key),
        }
    }

    fn iter_utxos(&self) -> Result<Self::UtxoIter, StateError> {
        match self {
            Self::Redb(s) => s.iter_utxos().map(StateUtxoIterBackend::Redb),
            Self::Fjall(s) => s.iter_utxos().map(StateUtxoIterBackend::Fjall),
            Self::Memory(s) => s.iter_utxos().map(StateUtxoIterBackend::Memory),
        }
    }
}

// ============================================================================
// Archive Store Backend
// ============================================================================

/// Enum wrapper for archive store backends.
#[derive(Clone)]
pub enum ArchiveStoreBackend {
    Memory(MemoryArchiveStore),
    /// Write-gated view over an already-open archive: reads and derived-log
    /// writes pass through, block appends and undos are discarded.
    ///
    /// This exists for replays over an archive that already holds the chain
    /// (`dolos doctor rebuild-state --rewrite-logs`): block appends are not
    /// idempotent — replaying them would double every shared segment file,
    /// whichever backend owns the index — while boundary log keys are
    /// slot-derived and identical under replay, so re-written log rows
    /// overwrite the originals in place.
    ///
    /// [`Self::logs_only`] is the sole constructor, and it never nests one
    /// gate inside another.
    LogsOnly(Box<ArchiveStoreBackend>),
    Fjall(dolos_fjall::archive::ArchiveStore),
    NoOp(NoOpArchiveStore),
}

impl ArchiveStoreBackend {
    /// Open an archive store with the Fjall backend.
    pub fn open_fjall(
        path: impl AsRef<Path>,
        schema: StateSchema,
        config: &dolos_core::config::FjallArchiveConfig,
    ) -> Result<Self, ArchiveError> {
        Ok(Self::Fjall(
            dolos_fjall::archive::ArchiveStore::open(schema, path, config)
                .map_err(ArchiveError::from)?,
        ))
    }

    /// Create a no-op archive store that discards all writes.
    pub fn noop() -> Self {
        Self::NoOp(NoOpArchiveStore)
    }

    /// Wrap this backend's already-open store in a [`Self::LogsOnly`] write
    /// gate.
    ///
    /// Clones the handle out of the open store rather than opening the path
    /// again, because a store that holds a file lock refuses to open the same
    /// path twice. Returns `None` when there is no store to wrap.
    ///
    /// The result therefore *aliases* the store it came from: both share the
    /// same underlying handle. Anything reaching for exclusive database
    /// access must refuse a `LogsOnly` value rather than unwrap it, because
    /// `Arc::get_mut` cannot succeed while the original handle is alive.
    pub fn logs_only(&self) -> Option<Self> {
        match self {
            Self::LogsOnly(_) => Some(self.clone()),
            Self::Memory(_) | Self::Fjall(_) => Some(Self::LogsOnly(Box::new(self.clone()))),
            Self::NoOp(_) => None,
        }
    }

    /// Create an in-memory archive store.
    pub fn in_memory(schema: StateSchema) -> Result<Self, ArchiveError> {
        Ok(Self::Memory(MemoryArchiveStore::new(schema)))
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
            ArchiveStoreConfig::Fjall(cfg) => Self::open_fjall(path, schema, cfg),
            ArchiveStoreConfig::InMemory => Self::in_memory(schema),
            ArchiveStoreConfig::NoOp => Ok(Self::noop()),
        }
    }

    pub fn shutdown(&self) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(s) => s.shutdown(),
            Self::LogsOnly(inner) => inner.shutdown(),
            Self::Fjall(s) => s.shutdown().map_err(ArchiveError::from),
            Self::NoOp(s) => s.shutdown(),
        }
    }
}

pub enum ArchiveWriterBackend {
    Memory(Box<<MemoryArchiveStore as CoreArchiveStore>::Writer>),
    /// Delegates `write_log` and `commit`; discards `apply`, `undo` and the
    /// index writes, which project the blocks the gate refuses.
    LogsOnly(Box<ArchiveWriterBackend>),
    Fjall(Box<<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::Writer>),
    NoOp(NoOpArchiveWriter),
}

impl CoreArchiveWriter for ArchiveWriterBackend {
    fn apply(&self, point: &ChainPoint, block: &RawBlock) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => w.apply(point, block),
            Self::LogsOnly(_) => Ok(()),
            Self::Fjall(w) => w.apply(point, block),
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
            Self::Memory(w) => w.write_log(ns, key, value),
            Self::LogsOnly(w) => w.write_log(ns, key, value),
            Self::Fjall(w) => w.write_log(ns, key, value),
            Self::NoOp(w) => w.write_log(ns, key, value),
        }
    }

    fn undo(&self, point: &ChainPoint) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => w.undo(point),
            Self::LogsOnly(_) => Ok(()),
            Self::Fjall(w) => w.undo(point),
            Self::NoOp(w) => w.undo(point),
        }
    }

    fn apply_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => w.apply_index(deltas),
            Self::LogsOnly(_) => Ok(()),
            Self::Fjall(w) => w.apply_index(deltas),
            Self::NoOp(w) => w.apply_index(deltas),
        }
    }

    fn undo_index(&self, deltas: &[ArchiveIndexDelta]) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => w.undo_index(deltas),
            Self::LogsOnly(_) => Ok(()),
            Self::Fjall(w) => w.undo_index(deltas),
            Self::NoOp(w) => w.undo_index(deltas),
        }
    }

    fn append_prehashed(
        &self,
        records: impl IntoIterator<Item = IndexRecord>,
    ) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => w.append_prehashed(records),
            Self::LogsOnly(_) => Ok(()),
            Self::Fjall(w) => w.append_prehashed(records),
            Self::NoOp(w) => w.append_prehashed(records),
        }
    }

    fn commit(self) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => (*w).commit(),
            Self::LogsOnly(w) => (*w).commit(),
            Self::Fjall(w) => (*w).commit(),
            Self::NoOp(w) => w.commit(),
        }
    }
}

pub enum ArchiveBlockIterBackend {
    Memory(Box<<MemoryArchiveStore as CoreArchiveStore>::BlockIter<'static>>),
    Fjall(Box<<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::BlockIter<'static>>),
    NoOp(EmptyBlockIter),
}

impl Iterator for ArchiveBlockIterBackend {
    type Item = (BlockSlot, BlockBody);
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl DoubleEndedIterator for ArchiveBlockIterBackend {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next_back(),
            Self::Fjall(iter) => iter.next_back(),
            Self::NoOp(iter) => iter.next_back(),
        }
    }
}

impl dolos_core::archive::Skippable for ArchiveBlockIterBackend {
    fn skip_forward(&mut self, n: usize) {
        match self {
            Self::Memory(iter) => iter.skip_forward(n),
            Self::Fjall(iter) => iter.skip_forward(n),
            Self::NoOp(iter) => iter.skip_forward(n),
        }
    }

    fn skip_backward(&mut self, n: usize) {
        match self {
            Self::Memory(iter) => iter.skip_backward(n),
            Self::Fjall(iter) => iter.skip_backward(n),
            Self::NoOp(iter) => iter.skip_backward(n),
        }
    }
}

pub enum ArchiveLogIterBackend {
    Memory(Box<<MemoryArchiveStore as CoreArchiveStore>::LogIter>),
    Fjall(Box<<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::LogIter>),
    NoOp(EmptyLogIter),
}

impl Iterator for ArchiveLogIterBackend {
    type Item = Result<(LogKey, EntityValue), ArchiveError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

pub enum ArchiveEntityValueIterBackend {
    Memory(Box<<MemoryArchiveStore as CoreArchiveStore>::EntityValueIter>),
    Fjall(Box<<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::EntityValueIter>),
    NoOp(dolos_core::builtin::EmptyEntityValueIter),
}

impl Iterator for ArchiveEntityValueIterBackend {
    type Item = Result<EntityValue, ArchiveError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

pub enum ArchiveSlotIterBackend {
    Memory(<MemoryArchiveStore as CoreArchiveStore>::SlotIter),
    Fjall(<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::SlotIter),
    NoOp(EmptySlotIter),
}

impl Iterator for ArchiveSlotIterBackend {
    type Item = Result<BlockSlot, ArchiveError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl DoubleEndedIterator for ArchiveSlotIterBackend {
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next_back(),
            Self::Fjall(iter) => iter.next_back(),
            Self::NoOp(iter) => iter.next_back(),
        }
    }
}

pub enum ArchiveTagIterBackend {
    Memory(<MemoryArchiveStore as CoreArchiveStore>::TagIter),
    Fjall(<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::TagIter),
    NoOp(<NoOpArchiveStore as CoreArchiveStore>::TagIter),
}

impl Iterator for ArchiveTagIterBackend {
    type Item = Result<TagRecord, ArchiveError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

pub enum ArchiveExactIterBackend {
    Memory(<MemoryArchiveStore as CoreArchiveStore>::ExactIter),
    Fjall(<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::ExactIter),
    NoOp(<NoOpArchiveStore as CoreArchiveStore>::ExactIter),
}

impl Iterator for ArchiveExactIterBackend {
    type Item = Result<ExactRecord, ArchiveError>;
    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Memory(iter) => iter.next(),
            Self::Fjall(iter) => iter.next(),
            Self::NoOp(iter) => iter.next(),
        }
    }
}

impl CoreArchiveStore for ArchiveStoreBackend {
    type BlockIter<'a> = ArchiveBlockIterBackend;
    type Writer = ArchiveWriterBackend;
    type LogIter = ArchiveLogIterBackend;
    type EntityValueIter = ArchiveEntityValueIterBackend;
    type SlotIter = ArchiveSlotIterBackend;
    type TagIter = ArchiveTagIterBackend;
    type ExactIter = ArchiveExactIterBackend;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::start_writer(s)
                .map(|writer| ArchiveWriterBackend::Memory(Box::new(writer))),
            Self::LogsOnly(inner) => CoreArchiveStore::start_writer(inner.as_ref())
                .map(|writer| ArchiveWriterBackend::LogsOnly(Box::new(writer))),
            Self::Fjall(s) => CoreArchiveStore::start_writer(s)
                .map(|writer| ArchiveWriterBackend::Fjall(Box::new(writer))),
            Self::NoOp(s) => CoreArchiveStore::start_writer(s).map(ArchiveWriterBackend::NoOp),
        }
    }

    fn read_logs(
        &self,
        ns: Namespace,
        keys: &[&LogKey],
    ) -> Result<Vec<Option<EntityValue>>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::read_logs(s, ns, keys),
            Self::LogsOnly(inner) => CoreArchiveStore::read_logs(inner.as_ref(), ns, keys),
            Self::Fjall(s) => CoreArchiveStore::read_logs(s, ns, keys),
            Self::NoOp(s) => CoreArchiveStore::read_logs(s, ns, keys),
        }
    }

    fn iter_logs(
        &self,
        ns: Namespace,
        range: Range<LogKey>,
    ) -> Result<Self::LogIter, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::iter_logs(s, ns, range)
                .map(|iter| ArchiveLogIterBackend::Memory(Box::new(iter))),
            Self::LogsOnly(inner) => CoreArchiveStore::iter_logs(inner.as_ref(), ns, range),
            Self::Fjall(s) => CoreArchiveStore::iter_logs(s, ns, range)
                .map(|iter| ArchiveLogIterBackend::Fjall(Box::new(iter))),
            Self::NoOp(s) => {
                CoreArchiveStore::iter_logs(s, ns, range).map(ArchiveLogIterBackend::NoOp)
            }
        }
    }

    fn get_block_by_slot(&self, slot: &BlockSlot) -> Result<Option<BlockBody>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::get_block_by_slot(s, slot),
            Self::LogsOnly(inner) => CoreArchiveStore::get_block_by_slot(inner.as_ref(), slot),
            Self::Fjall(s) => CoreArchiveStore::get_block_by_slot(s, slot),
            Self::NoOp(s) => CoreArchiveStore::get_block_by_slot(s, slot),
        }
    }

    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::get_blocks_by_slot(s, slot),
            Self::LogsOnly(inner) => CoreArchiveStore::get_blocks_by_slot(inner.as_ref(), slot),
            Self::Fjall(s) => CoreArchiveStore::get_blocks_by_slot(s, slot),
            Self::NoOp(s) => CoreArchiveStore::get_blocks_by_slot(s, slot),
        }
    }

    fn get_range<'a>(
        &self,
        from: Option<BlockSlot>,
        to: Option<BlockSlot>,
    ) -> Result<Self::BlockIter<'a>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::get_range(s, from, to)
                .map(|iter| ArchiveBlockIterBackend::Memory(Box::new(iter))),
            Self::LogsOnly(inner) => CoreArchiveStore::get_range(inner.as_ref(), from, to),
            Self::Fjall(s) => CoreArchiveStore::get_range(s, from, to)
                .map(|iter| ArchiveBlockIterBackend::Fjall(Box::new(iter))),
            Self::NoOp(s) => {
                CoreArchiveStore::get_range(s, from, to).map(ArchiveBlockIterBackend::NoOp)
            }
        }
    }

    fn find_intersect(&self, intersect: &[ChainPoint]) -> Result<Option<ChainPoint>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::find_intersect(s, intersect),
            Self::LogsOnly(inner) => CoreArchiveStore::find_intersect(inner.as_ref(), intersect),
            Self::Fjall(s) => CoreArchiveStore::find_intersect(s, intersect),
            Self::NoOp(s) => CoreArchiveStore::find_intersect(s, intersect),
        }
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::get_tip(s),
            Self::LogsOnly(inner) => CoreArchiveStore::get_tip(inner.as_ref()),
            Self::Fjall(s) => CoreArchiveStore::get_tip(s),
            Self::NoOp(s) => CoreArchiveStore::get_tip(s),
        }
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::prune_history(s, max_slots, max_prune),
            Self::LogsOnly(inner) => {
                CoreArchiveStore::prune_history(inner.as_ref(), max_slots, max_prune)
            }
            Self::Fjall(s) => CoreArchiveStore::prune_history(s, max_slots, max_prune),
            Self::NoOp(s) => CoreArchiveStore::prune_history(s, max_slots, max_prune),
        }
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::truncate_front(s, after),
            Self::LogsOnly(inner) => CoreArchiveStore::truncate_front(inner.as_ref(), after),
            Self::Fjall(s) => CoreArchiveStore::truncate_front(s, after),
            Self::NoOp(s) => CoreArchiveStore::truncate_front(s, after),
        }
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::slot_by_block_hash(s, hash),
            Self::LogsOnly(inner) => CoreArchiveStore::slot_by_block_hash(inner.as_ref(), hash),
            Self::Fjall(s) => CoreArchiveStore::slot_by_block_hash(s, hash),
            Self::NoOp(s) => CoreArchiveStore::slot_by_block_hash(s, hash),
        }
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::slot_by_block_number(s, number),
            Self::LogsOnly(inner) => CoreArchiveStore::slot_by_block_number(inner.as_ref(), number),
            Self::Fjall(s) => CoreArchiveStore::slot_by_block_number(s, number),
            Self::NoOp(s) => CoreArchiveStore::slot_by_block_number(s, number),
        }
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::slot_by_tx_hash(s, hash),
            Self::LogsOnly(inner) => CoreArchiveStore::slot_by_tx_hash(inner.as_ref(), hash),
            Self::Fjall(s) => CoreArchiveStore::slot_by_tx_hash(s, hash),
            Self::NoOp(s) => CoreArchiveStore::slot_by_tx_hash(s, hash),
        }
    }

    fn slots_by_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        start: BlockSlot,
        end: BlockSlot,
    ) -> Result<Self::SlotIter, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::slots_by_tag(s, dimension, key, start, end)
                .map(ArchiveSlotIterBackend::Memory),
            Self::LogsOnly(inner) => {
                CoreArchiveStore::slots_by_tag(inner.as_ref(), dimension, key, start, end)
            }
            Self::Fjall(s) => CoreArchiveStore::slots_by_tag(s, dimension, key, start, end)
                .map(ArchiveSlotIterBackend::Fjall),
            Self::NoOp(s) => CoreArchiveStore::slots_by_tag(s, dimension, key, start, end)
                .map(ArchiveSlotIterBackend::NoOp),
        }
    }

    fn iter_archive_tags(
        &self,
        dimensions: &[TagDimension],
        slots: Range<BlockSlot>,
    ) -> Result<Self::TagIter, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::iter_archive_tags(s, dimensions, slots)
                .map(ArchiveTagIterBackend::Memory),
            Self::LogsOnly(inner) => {
                CoreArchiveStore::iter_archive_tags(inner.as_ref(), dimensions, slots)
            }
            Self::Fjall(s) => CoreArchiveStore::iter_archive_tags(s, dimensions, slots)
                .map(ArchiveTagIterBackend::Fjall),
            Self::NoOp(s) => CoreArchiveStore::iter_archive_tags(s, dimensions, slots)
                .map(ArchiveTagIterBackend::NoOp),
        }
    }

    fn iter_exact_records(&self, slots: Range<BlockSlot>) -> Result<Self::ExactIter, ArchiveError> {
        match self {
            Self::Memory(s) => {
                CoreArchiveStore::iter_exact_records(s, slots).map(ArchiveExactIterBackend::Memory)
            }
            Self::LogsOnly(inner) => CoreArchiveStore::iter_exact_records(inner.as_ref(), slots),
            Self::Fjall(s) => {
                CoreArchiveStore::iter_exact_records(s, slots).map(ArchiveExactIterBackend::Fjall)
            }
            Self::NoOp(s) => {
                CoreArchiveStore::iter_exact_records(s, slots).map(ArchiveExactIterBackend::NoOp)
            }
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

    fn peek_pending(&self) -> Vec<MempoolTx> {
        match self {
            Self::Ephemeral(s) => s.peek_pending(),
            Self::Redb(s) => s.peek_pending(),
        }
    }

    fn mark_inflight(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
        match self {
            Self::Ephemeral(s) => s.mark_inflight(hashes),
            Self::Redb(s) => s.mark_inflight(hashes),
        }
    }

    fn mark_acknowledged(&self, hashes: &[TxHash]) -> Result<(), MempoolError> {
        match self {
            Self::Ephemeral(s) => s.mark_acknowledged(hashes),
            Self::Redb(s) => s.mark_acknowledged(hashes),
        }
    }

    fn find_inflight(&self, tx_hash: &TxHash) -> Option<MempoolTx> {
        match self {
            Self::Ephemeral(s) => s.find_inflight(tx_hash),
            Self::Redb(s) => s.find_inflight(tx_hash),
        }
    }

    fn peek_inflight(&self) -> Vec<MempoolTx> {
        match self {
            Self::Ephemeral(s) => s.peek_inflight(),
            Self::Redb(s) => s.peek_inflight(),
        }
    }

    fn confirm(
        &self,
        point: &ChainPoint,
        seen_txs: &[TxHash],
        unseen_txs: &[TxHash],
        finalize_threshold: u32,
        drop_threshold: u32,
    ) -> Result<(), MempoolError> {
        match self {
            Self::Ephemeral(s) => s.confirm(
                point,
                seen_txs,
                unseen_txs,
                finalize_threshold,
                drop_threshold,
            ),
            Self::Redb(s) => s.confirm(
                point,
                seen_txs,
                unseen_txs,
                finalize_threshold,
                drop_threshold,
            ),
        }
    }

    fn check_status(&self, tx_hash: &TxHash) -> TxStatus {
        match self {
            Self::Ephemeral(s) => s.check_status(tx_hash),
            Self::Redb(s) => s.check_status(tx_hash),
        }
    }

    fn dump_finalized(&self, cursor: u64, limit: usize) -> dolos_core::MempoolPage {
        match self {
            Self::Ephemeral(s) => s.dump_finalized(cursor, limit),
            Self::Redb(s) => s.dump_finalized(cursor, limit),
        }
    }

    fn subscribe(&self) -> Self::Stream {
        match self {
            Self::Ephemeral(s) => MempoolStreamBackend::Ephemeral(s.subscribe()),
            Self::Redb(s) => MempoolStreamBackend::Redb(s.subscribe()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn refusal<T>(result: Result<T, Error>) -> String {
        result.err().expect("expected a refusal").to_string()
    }

    /// A node configuration over `root`, with every store on its disk backend
    /// at its default path so that the root is all the data there is.
    fn config_over(root: &Path, version: &str) -> RootConfig {
        let toml = format!(
            r#"
            [upstream]
            peer_address = "unused.example:3001"

            [storage]
            version = "{version}"
            path = {path}

            [storage.mempool]
            backend = "redb"

            [genesis]
            byron_path = "byron.json"
            shelley_path = "shelley.json"
            alonzo_path = "alonzo.json"
            conway_path = "conway.json"

            [chain]
            type = "cardano"
            magic = 2
            is_testnet = true
            "#,
            path = toml::Value::String(root.display().to_string()),
        );

        toml::from_str(&toml).unwrap()
    }

    /// A v1.6-era configuration is refused, and the refusal names both the
    /// tool that performs the migration and the guide that describes it.
    #[test]
    fn older_storage_versions_are_refused_with_the_remedy() {
        for stale in [
            StorageVersion::V0,
            StorageVersion::V1,
            StorageVersion::V2,
            StorageVersion::V3,
        ] {
            let message = refusal(check_storage_version(&stale));

            assert!(
                message.contains(&stale.to_string()),
                "refusal must name the version found: {message}"
            );
            assert!(
                message.contains("v4"),
                "refusal must name the supported version: {message}"
            );
            assert!(
                message.contains("dolos init"),
                "refusal must name the remedy: {message}"
            );
            assert!(
                message.contains(MIGRATION_GUIDE_URL),
                "refusal must point at the migration guide: {message}"
            );
        }

        check_storage_version(&CURRENT_STORAGE_VERSION).unwrap();
    }

    /// Every path that opens a store on its own — the per-store openers the
    /// dump, doctor and bootstrap commands use, and bootstrap's look at
    /// existing data — refuses a stale configuration before it creates or
    /// opens anything, the same way the daemon's `open_data_stores` does.
    #[test]
    fn a_stale_config_is_refused_before_any_store_is_opened() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        let stale = config_over(&root, "v3");

        let refusals = [
            refusal(open_data_stores::<dolos_cardano::CardanoDelta>(&stale)),
            refusal(open_wal_store::<dolos_cardano::CardanoDelta>(&stale)),
            refusal(open_state_store(&stale)),
            refusal(open_archive_store(&stale)),
            refusal(open_mempool_store(&stale)),
            refusal(has_existing_data(&stale)),
            refusal(inspect_existing_data(
                &stale,
                ExistingDataPolicy {
                    force: true,
                    ..Default::default()
                },
            )),
            refusal(inspect_existing_data(
                &stale,
                ExistingDataPolicy {
                    r#continue: true,
                    ..Default::default()
                },
            )),
        ];

        for message in refusals {
            assert!(
                message.contains("dolos init"),
                "every entry point refuses with the same remedy: {message}"
            );
        }

        assert!(
            !root.exists(),
            "a refused configuration must not have had its storage created"
        );

        let current = config_over(&root, "v4");

        open_data_stores::<dolos_cardano::CardanoDelta>(&current)
            .expect("the current version opens");

        assert!(root.is_dir());
    }

    /// `in_memory` has to reach the builtin stores, not a memory-mode disk
    /// backend: that was the old wiring, and it was the reason the variant
    /// could not serve the snapshot export seam.
    #[test]
    fn in_memory_selects_the_builtin_stores() {
        let path = std::path::Path::new("/nonexistent");

        let state =
            StateStoreBackend::open(path, StateSchema::default(), &StateStoreConfig::InMemory)
                .expect("in_memory state store should open without touching the path");

        assert!(matches!(state, StateStoreBackend::Memory(_)));

        let archive =
            ArchiveStoreBackend::open(path, StateSchema::default(), &ArchiveStoreConfig::InMemory)
                .expect("in_memory archive store should open without touching the path");

        assert!(matches!(archive, ArchiveStoreBackend::Memory(_)));

        // And the seam the old wiring could not serve now answers.
        state.iter_utxos().expect("iter_utxos must be supported");
        archive
            .iter_archive_tags(&[], 0..1)
            .expect("iter_archive_tags must be supported");
        archive
            .start_writer()
            .expect("start_writer failed")
            .append_prehashed([])
            .expect("append_prehashed must be supported");
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use dolos_snapshot::restore::progress_path_in;

    /// A `--force` wipe takes the progress file with the data it describes.
    ///
    /// The hazard this rules out is specific and quiet. `--continue` reads the
    /// progress file and skips every layer it names; `--force` clears the
    /// stores. A progress file that survived a wipe would therefore tell the
    /// next run that layers it has no data for are already done, and the node
    /// that came out would be missing a slice of chain with nothing reporting
    /// it.
    ///
    /// Asserted against [`super::clear_storage`]'s actual behaviour rather than
    /// against the fact that it happens to call `remove_dir_all`: what has to
    /// stay true is the outcome, however the wipe is later spelled.
    #[test]
    fn clearing_storage_removes_a_restore_in_progress() {
        let temp = tempfile::tempdir().unwrap();
        let storage = temp.path().join("data");

        std::fs::create_dir_all(&storage).unwrap();

        let progress = progress_path_in(&storage);
        std::fs::write(&progress, b"{}").unwrap();

        // A stand-in for the stores the progress file describes, so the
        // assertion is about a directory that had a node in it.
        std::fs::write(storage.join("state"), b"a store").unwrap();

        assert!(progress.exists());

        super::clear_storage(&storage).unwrap();

        assert!(
            !progress.exists(),
            "a progress file outlived the stores it describes"
        );

        assert!(
            storage.is_dir(),
            "the storage directory itself has to come back, empty"
        );

        assert_eq!(
            std::fs::read_dir(&storage).unwrap().count(),
            0,
            "and come back empty"
        );
    }

    /// The progress file is *inside* the storage path.
    ///
    /// The other half of the test above, and the half that would fail first if
    /// the file were ever moved: a wipe of `storage.path` only takes it while
    /// it lives there.
    #[test]
    fn the_progress_file_lives_inside_the_storage_path() {
        let storage = std::path::Path::new("/var/lib/dolos/data");

        assert!(progress_path_in(storage).starts_with(storage));
    }
}
