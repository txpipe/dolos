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

use std::{
    ops::Range,
    path::{Component, Path, PathBuf},
};

use bytes::Bytes;
use dolos_core::{
    archive::{ArchiveError, ArchiveStore as CoreArchiveStore, ArchiveWriter as CoreArchiveWriter},
    builtin::{
        EmptyBlockIter, EmptyLogIter, EmptySlotIter, MemoryArchiveStore, MemoryStateStore,
        MemoryStateWriter, NoDiskTier, NoOpArchiveStore, NoOpArchiveWriter,
    },
    cache::Cost,
    compat::{reads_store_identity, IncompatibleStore},
    config::{
        Compatibility, FjallArchiveConfig, FjallStateConfig, RedbMempoolConfig, RedbStateConfig,
        RedbWalConfig, RootConfig, StateStoreConfig, StoreType,
    },
    ArchiveIndexDelta, BlockBody, BlockSlot, CacheDiskTier, CacheEntry, CacheError, ChainPoint,
    EntityDelta, EntityKey, EntityValue, ExactRecord, IndexRecord, KeyRange, LogEntry, LogKey,
    LogValue, MempoolError, MempoolEvent, MempoolStore, MempoolTx, Namespace, QueryLimiter,
    RawBlock, StateError, StateSchema, StateStore as CoreStateStore,
    StateWriter as CoreStateWriter, TagDimension, TagRecord, TieredCache, TxHash, TxStatus, TxoRef,
    UtxoEntry, UtxoIndexDelta, UtxoMap, UtxoSet, UtxoSetDelta, WalError, WalStore,
};
use dolos_redb3::cache::RedbCacheTier;
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

/// Those of `paths` that clearing the storage root does not reach. Paths are
/// compared as written, with `..` resolved lexically and links not followed.
pub fn outside_storage_root(
    config: &RootConfig,
    paths: impl IntoIterator<Item = Option<PathBuf>>,
) -> std::io::Result<Vec<PathBuf>> {
    let root = normalize(&config.storage.path)?;
    let mut outside = Vec::new();

    for path in paths.into_iter().flatten() {
        if !normalize(&path)?.starts_with(&root) {
            outside.push(path);
        }
    }

    Ok(outside)
}

/// `path` made absolute with `..` resolved lexically. Only a relative path
/// reads the working directory, so absolute ones survive a deleted one.
fn normalize(path: &Path) -> std::io::Result<PathBuf> {
    let mut normalized = PathBuf::new();

    for component in std::path::absolute(path)?.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                normalized.pop();
            }
            other => normalized.push(other.as_os_str()),
        }
    }

    Ok(normalized)
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
    /// `--skip-if-data`: leave a node alone and exit zero. A store this
    /// release refuses is not skipped.
    pub skip_if_data: bool,
    /// `--continue`: go ahead, trusting the run to resume.
    pub r#continue: bool,
}

/// What storage holds, as a bootstrap decides about it.
#[derive(Default)]
struct Found {
    /// The state holds a cursor.
    node: bool,
    /// The state failed to open for a reason other than its identity.
    unreadable: Option<Error>,
    /// The first identity refusal, and the directories of every refused store.
    refused: Option<(Error, Vec<Option<PathBuf>>)>,
}

impl Found {
    fn refuse(&mut self, refusal: Error, paths: impl IntoIterator<Item = Option<PathBuf>>) {
        let (_, dirs) = self.refused.get_or_insert_with(|| (refusal, Vec::new()));
        dirs.extend(paths);
    }
}

/// Open the stores already on disk, creating none: a state cursor is a node,
/// and failures to open are reported rather than raised. Both stores are
/// checked, so a refused state doesn't hide a refused archive.
fn found_in_storage(config: &RootConfig) -> Result<Found, Error> {
    let on_disk = |path: Option<PathBuf>| path.is_some_and(|path| path.exists());

    let mut found = Found::default();

    if on_disk(config.storage.state_path()) {
        match open_state_store(config) {
            Ok(state) => found.node = state.read_cursor()?.is_some(),
            Err(error @ Error::StateError(StateError::IncompatibleStore(_))) => {
                found.refuse(error, [config.storage.state_path()]);
            }
            Err(error) => found.unreadable = Some(error),
        }
    }

    if config.keeps_archive() && on_disk(config.storage.archive_path()) {
        if let Some(refusal) = archive_refusal(config) {
            let paths = [config.storage.archive_path(), config.storage.blocks_path()];
            found.refuse(refusal, paths);
        }
    }

    Ok(found)
}

/// The archive's identity refusal, if it has one. Only the index is read, so
/// the archive opens with its segments in its own directory, and a
/// `blocks_path` that is gone isn't created. Any other failure is left to the
/// command that uses the archive, as before stores recorded identities.
fn archive_refusal(config: &RootConfig) -> Option<Error> {
    let opened = ArchiveStoreBackend::open(
        config.storage.archive_path().unwrap_or_default(),
        None,
        dolos_cardano::model::build_log_schema(),
        config.storage.archive.kind,
    );

    match opened {
        Err(refusal @ ArchiveError::IncompatibleStore(_)) => Some(refusal.into()),
        _ => None,
    }
}

/// Whether storage already holds a node: a state cursor is the whole test. A
/// store this release refuses to open is refused here too.
pub fn has_existing_data(config: &RootConfig) -> Result<bool, Error> {
    let found = found_in_storage(config)?;

    if let Some((refusal, _)) = found.refused {
        return Err(refusal);
    }

    if let Some(error) = found.unreadable {
        return Err(error);
    }

    Ok(found.node)
}

/// Read what is in storage and decide, without touching any of it.
///
/// The refusals happen here — a skip, and the refusal for existing data with no
/// flag saying what to do about it — so neither is a question asked of an
/// operator whose answer is then thrown away.
///
/// A store this release refuses to open is data in the way that only `--force`
/// on its own clears. Every other flag, `--continue` included, gets its
/// refusal, and so does `--force` when a refused store keeps directories
/// outside the storage root it clears. A state that fails to open for another
/// reason is left to `--continue`, and stops every other flag, `--force`
/// included, since a running node holds its stores that way.
pub fn inspect_existing_data(
    config: &RootConfig,
    policy: ExistingDataPolicy,
) -> Result<Existing, Error> {
    let found = found_in_storage(config)?;

    if let Some((refusal, dirs)) = found.refused {
        return clear_refused(config, policy, refusal, dirs, found.unreadable);
    }

    if policy.r#continue {
        return Ok(Existing::Proceed);
    }

    if let Some(error) = found.unreadable {
        return Err(error);
    }

    if !found.node {
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

/// The decision on storage holding a store this release refuses: `--force`
/// on its own clears it when clearing the storage root reaches every refused
/// directory and the state isn't held by something else.
fn clear_refused(
    config: &RootConfig,
    policy: ExistingDataPolicy,
    refusal: Error,
    dirs: Vec<Option<PathBuf>>,
    unreadable: Option<Error>,
) -> Result<Existing, Error> {
    if !policy.force {
        return Err(refusal);
    }

    if policy.skip_if_data || policy.r#continue {
        return Err(Error::message(format!(
            "{refusal}\n`--force` clears a refused store only on its own: `--skip-if-data` \
             and `--continue` leave storage as it is"
        )));
    }

    if let Some(error) = unreadable {
        return Err(error);
    }

    let mut outside = outside_storage_root(config, dirs)?;
    outside.retain(|path| path.exists());

    if !outside.is_empty() {
        let listed = outside
            .iter()
            .map(|path| format!("  {}", path.display()))
            .collect::<Vec<_>>()
            .join("\n");

        return Err(Error::message(format!(
            "a store this release refuses keeps directories outside the storage root {}, \
             and `--force` clears only the root:\n{listed}\nRemove those directories by \
             hand, then run the bootstrap again",
            config.storage.path.display(),
        )));
    }

    Ok(Existing::Clear)
}

/// The data format this binary reads, as snapshot download URLs name it.
pub const CURRENT_STORAGE_VERSION: &str = "v4";

/// Ensure directory exists for a store path.
fn ensure_store_path(path: &Path) -> Result<(), Error> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    Ok(())
}

/// On disk at `sync.wal_path` exactly when the state is.
pub fn open_wal_store<D>(config: &RootConfig) -> Result<WalStoreBackend<D>, Error>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    match config.wal_path() {
        Some(path) => {
            ensure_store_path(&path)?;
            Ok(WalStoreBackend::open_redb(
                &path,
                &RedbWalConfig::default(),
            )?)
        }
        None => Ok(WalStoreBackend::in_memory()?),
    }
}

/// A no-op archive when `history.mode` is `none`.
pub fn open_archive_store(config: &RootConfig) -> Result<ArchiveStoreBackend, Error> {
    if !config.keeps_archive() {
        return Ok(ArchiveStoreBackend::noop());
    }

    let path = config.storage.archive_path().unwrap_or_default();
    ensure_store_path(&path)?;
    Ok(ArchiveStoreBackend::open(
        &path,
        config.storage.blocks_path(),
        dolos_cardano::model::build_log_schema(),
        config.storage.archive.kind,
    )?)
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
    match config.mempool_path() {
        None => Ok(MempoolBackend::Ephemeral(
            dolos_core::builtin::EphemeralMempool::new(),
        )),
        Some(path) => {
            ensure_store_path(&path)?;
            Ok(MempoolBackend::Redb(
                dolos_redb3::mempool::RedbMempool::open(path, &RedbMempoolConfig::default())?,
            ))
        }
    }
}

/// The cache store, filling under `limiter`, the domain's query limiter. An
/// enabled cache keeps its immutable entries in the redb disk tier too.
pub fn open_cache_store(config: &RootConfig, limiter: QueryLimiter) -> Result<CacheBackend, Error> {
    let disk = match config.storage.cache_path() {
        Some(path) => CacheDiskBackend::Redb(
            RedbCacheTier::open(path, config.storage.cache.disk_bytes())
                .map_err(|error| Error::StorageError(error.to_string()))?,
        ),
        None => CacheDiskBackend::Off(NoDiskTier),
    };

    Ok(TieredCache::new(&config.storage.cache, disk, limiter))
}

pub fn open_data_stores<D>(config: &RootConfig) -> Result<Stores<D>, Error>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    Ok(Stores {
        wal: open_wal_store(config)?,
        state: open_state_store(config)?,
        archive: open_archive_store(config)?,
        mempool: open_mempool_store(config)?,
    })
}

/// Open the state and archive stores through their identity check, then
/// close them, for a writer that filled storage without opening them.
pub fn check_store_identities(config: &RootConfig) -> Result<(), Error> {
    open_state_store(config)?.shutdown()?;
    open_archive_store(config)?.shutdown()?;

    Ok(())
}

/// Admit a persistent store only if it records an identity this release
/// reads, recording the current one on a new store before its first write.
///
/// A store without an identity is new while it holds nothing; holding data, a
/// release that predates store identities wrote it. The configuration plays
/// no part.
fn admit<E>(
    store: &'static str,
    path: &Path,
    identity: Option<String>,
    holds_data: impl FnOnce() -> Result<bool, dolos_fjall::Error>,
    record: impl FnOnce(&str) -> Result<(), dolos_fjall::Error>,
) -> Result<(), E>
where
    E: From<IncompatibleStore> + From<dolos_fjall::Error>,
{
    let found = match identity {
        Some(found) if reads_store_identity(&found) => return Ok(()),
        Some(found) => Some(found),
        None if holds_data()? => None,
        None => return Ok(record(Compatibility::current().as_str())?),
    };

    Err(IncompatibleStore {
        store,
        path: path.to_path_buf(),
        found,
    }
    .into())
}

pub type CacheBackend = TieredCache<CacheDiskBackend>;

// ============================================================================
// Cache Disk Tier Backend
// ============================================================================

/// The cache's disk tier: none while the cache is disabled.
pub enum CacheDiskBackend {
    Off(NoDiskTier),
    Redb(RedbCacheTier),
}

impl CacheDiskTier for CacheDiskBackend {
    fn open_namespace(&self, namespace: &str, version: u32) -> Result<(), CacheError> {
        match self {
            Self::Off(tier) => tier.open_namespace(namespace, version),
            Self::Redb(tier) => tier.open_namespace(namespace, version),
        }
    }

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<CacheEntry>, CacheError> {
        match self {
            Self::Off(tier) => tier.get(namespace, key),
            Self::Redb(tier) => tier.get(namespace, key),
        }
    }

    fn put(&self, namespace: &str, key: &[u8], value: &[u8], cost: Cost) -> Result<(), CacheError> {
        match self {
            Self::Off(tier) => tier.put(namespace, key, value, cost),
            Self::Redb(tier) => tier.put(namespace, key, value, cost),
        }
    }

    fn remove(&self, namespace: &str, key: &[u8]) -> Result<(), CacheError> {
        match self {
            Self::Off(tier) => tier.remove(namespace, key),
            Self::Redb(tier) => tier.remove(namespace, key),
        }
    }

    fn wipe(&self, namespace: &str) -> Result<(), CacheError> {
        match self {
            Self::Off(tier) => tier.wipe(namespace),
            Self::Redb(tier) => tier.wipe(namespace),
        }
    }

    fn previous_keys(&self, namespace: &str) -> Result<Vec<Bytes>, CacheError> {
        match self {
            Self::Off(tier) => tier.previous_keys(namespace),
            Self::Redb(tier) => tier.previous_keys(namespace),
        }
    }

    fn keys(&self, namespace: &str) -> Result<Vec<Bytes>, CacheError> {
        match self {
            Self::Off(tier) => tier.keys(namespace),
            Self::Redb(tier) => tier.keys(namespace),
        }
    }

    fn bytes(&self, namespace: &str) -> Result<u64, CacheError> {
        match self {
            Self::Off(tier) => tier.bytes(namespace),
            Self::Redb(tier) => tier.bytes(namespace),
        }
    }

    fn entries(&self, namespace: &str) -> Result<u64, CacheError> {
        match self {
            Self::Off(tier) => tier.entries(namespace),
            Self::Redb(tier) => tier.entries(namespace),
        }
    }

    fn flush(&self) -> Result<(), CacheError> {
        match self {
            Self::Off(tier) => tier.flush(),
            Self::Redb(tier) => tier.flush(),
        }
    }
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

    /// Open a state store with the Fjall backend, refusing one this release
    /// did not write (see [`admit`]).
    pub fn open_fjall(
        path: impl AsRef<Path>,
        config: &FjallStateConfig,
    ) -> Result<Self, StateError> {
        let path = path.as_ref();
        let store = dolos_fjall::StateStore::open(path, config)?;

        admit::<StateError>(
            "state",
            path,
            store.identity()?,
            || store.holds_data(),
            |identity| store.record_identity(identity),
        )?;

        Ok(Self::Fjall(store))
    }

    /// Create an in-memory state store.
    ///
    /// Takes no schema: the builtin store discovers namespaces as they are
    /// written, so there is no table set to declare up front.
    pub fn in_memory() -> Result<Self, StateError> {
        Ok(Self::Memory(MemoryStateStore::new()))
    }

    /// Open a state store of the configured type.
    ///
    /// For `fjall`, the caller must provide the resolved path. For
    /// `ephemeral`, the path is ignored and an in-memory store is created.
    pub fn open(
        path: impl AsRef<Path>,
        _schema: StateSchema,
        config: &StateStoreConfig,
    ) -> Result<Self, StateError> {
        match config.kind {
            StoreType::Fjall { .. } => Self::open_fjall(path, &FjallStateConfig::default()),
            StoreType::Ephemeral => Self::in_memory(),
        }
    }

    /// The compatibility identity the store recorded at creation; `None` for
    /// the stores that do not record one.
    pub fn identity(&self) -> Result<Option<String>, StateError> {
        match self {
            Self::Fjall(s) => Ok(s.identity()?),
            Self::Redb(_) | Self::Memory(_) => Ok(None),
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
        range: KeyRange,
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
    /// Write-gated view over an already-open archive: reads, derived-log
    /// writes and index applies pass through, block appends and undos are
    /// discarded.
    ///
    /// This exists for replays over an archive that already holds the chain
    /// (`dolos doctor rebuild-state --rewrite-logs`): block appends are not
    /// idempotent — replaying them would double every shared segment file,
    /// whichever backend owns the index — while boundary log keys are
    /// slot-derived and identical under replay, so re-written log rows
    /// overwrite the originals in place. Index entries are keyed by what they
    /// project and the slot, so a replay re-writes the ones the archive holds
    /// and adds the ones an older indexer never emitted (the auxiliary-data
    /// script tags `script_by_hash` falls back on).
    ///
    /// [`Self::logs_only`] is the sole constructor, and it never nests one
    /// gate inside another.
    LogsOnly(Box<ArchiveStoreBackend>),
    Fjall(dolos_fjall::archive::ArchiveStore),
    NoOp(NoOpArchiveStore),
}

impl ArchiveStoreBackend {
    /// Open an archive store with the Fjall backend, refusing one this release
    /// did not write (see [`admit`]).
    pub fn open_fjall(
        path: impl AsRef<Path>,
        schema: StateSchema,
        config: &dolos_core::config::FjallArchiveConfig,
    ) -> Result<Self, ArchiveError> {
        let path = path.as_ref();
        let store = dolos_fjall::archive::ArchiveStore::open(schema, path, config)?;

        admit::<ArchiveError>(
            "archive",
            path,
            store.identity()?,
            || store.holds_data(),
            |identity| store.record_identity(identity),
        )?;

        Ok(Self::Fjall(store))
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

    /// Open an archive store of the configured type.
    ///
    /// For `fjall`, the caller must provide the resolved paths: the archive
    /// directory and, when they live elsewhere, the block segments'. For
    /// `ephemeral`, both are ignored and an in-memory store is created.
    pub fn open(
        path: impl AsRef<Path>,
        blocks_path: Option<PathBuf>,
        schema: StateSchema,
        kind: StoreType,
    ) -> Result<Self, ArchiveError> {
        match kind {
            StoreType::Fjall { .. } => {
                let options = FjallArchiveConfig {
                    blocks_path,
                    ..Default::default()
                };
                Self::open_fjall(path, schema, &options)
            }
            StoreType::Ephemeral => Self::in_memory(schema),
        }
    }

    /// The compatibility identity the store recorded at creation; `None` for
    /// the stores that do not record one.
    pub fn identity(&self) -> Result<Option<String>, ArchiveError> {
        match self {
            Self::Fjall(s) => Ok(s.identity()?),
            Self::LogsOnly(inner) => inner.identity(),
            Self::Memory(_) | Self::NoOp(_) => Ok(None),
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
    /// Delegates `write_log`, `apply_index` and `commit`; discards `apply`,
    /// `undo`, `undo_index` and `append_prehashed`. See
    /// [`ArchiveStoreBackend::LogsOnly`].
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
            Self::LogsOnly(w) => w.apply_index(deltas),
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

    fn iter_logs(&self, ns: Namespace, range: KeyRange) -> Result<Self::LogIter, ArchiveError> {
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

    fn prune_history(
        &self,
        max_slots: u64,
        max_prune_slots: Option<u64>,
        max_prune_index_rows: Option<u64>,
    ) -> Result<bool, ArchiveError> {
        match self {
            Self::Memory(s) => {
                CoreArchiveStore::prune_history(s, max_slots, max_prune_slots, max_prune_index_rows)
            }
            Self::LogsOnly(inner) => CoreArchiveStore::prune_history(
                inner.as_ref(),
                max_slots,
                max_prune_slots,
                max_prune_index_rows,
            ),
            Self::Fjall(s) => {
                CoreArchiveStore::prune_history(s, max_slots, max_prune_slots, max_prune_index_rows)
            }
            Self::NoOp(s) => {
                CoreArchiveStore::prune_history(s, max_slots, max_prune_slots, max_prune_index_rows)
            }
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
    use dolos_core::{
        config::{ArchiveStoreConfig, ChainConfig, HistoryConfig, HistoryWindow},
        Tag,
    };

    use super::*;

    fn config_over(root: &Path) -> RootConfig {
        let mut config = RootConfig::new(ChainConfig::known("preview"));
        config.storage.path = root.to_path_buf();
        config
    }

    fn current() -> Option<String> {
        Some(Compatibility::current().as_str().to_string())
    }

    /// A state store with a cursor, written by a release that recorded
    /// `identity`, or by one that predates store identities.
    fn written_state(config: &RootConfig, identity: Option<&str>) {
        let path = config.storage.state_path().unwrap();
        let store = dolos_fjall::StateStore::open(path, &FjallStateConfig::default()).unwrap();

        if let Some(identity) = identity {
            store.record_identity(identity).unwrap();
        }

        let writer = store.start_writer().unwrap();
        writer.set_cursor(ChainPoint::Origin).unwrap();
        writer.commit().unwrap();
        store.shutdown().unwrap();
    }

    /// The archive counterpart of [`written_state`], holding an index entry.
    fn written_archive(config: &RootConfig, identity: Option<&str>) {
        let path = config.storage.archive_path().unwrap();
        let store = dolos_fjall::archive::ArchiveStore::open(
            StateSchema::default(),
            path,
            &FjallArchiveConfig::default(),
        )
        .unwrap();

        if let Some(identity) = identity {
            store.record_identity(identity).unwrap();
        }

        let writer = store.start_writer().unwrap();
        writer
            .apply_index(&[ArchiveIndexDelta {
                slot: 1,
                tags: vec![Tag::new(
                    dolos_cardano::indexes::archive_dimensions::SCRIPT,
                    vec![1u8; 28],
                )],
                ..Default::default()
            }])
            .unwrap();
        writer.commit().unwrap();
        store.shutdown().unwrap();
    }

    /// The refusal an open came to, or `None` when the store opened.
    fn refusal<T>(opened: Result<T, Error>) -> Option<IncompatibleStore> {
        match opened {
            Ok(_) => None,
            Err(Error::StateError(StateError::IncompatibleStore(refusal)))
            | Err(Error::ArchiveError(ArchiveError::IncompatibleStore(refusal))) => Some(refusal),
            Err(error) => panic!("not an identity refusal: {error}"),
        }
    }

    #[test]
    fn a_new_store_records_the_identity_and_reopens() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));

        let stores = open_data_stores::<dolos_cardano::CardanoDelta>(&config).unwrap();
        assert_eq!(stores.state.identity().unwrap(), current());
        assert_eq!(stores.archive.identity().unwrap(), current());

        let writer = stores.state.start_writer().unwrap();
        writer.set_cursor(ChainPoint::Origin).unwrap();
        writer.commit().unwrap();
        stores.state.shutdown().unwrap();
        stores.archive.shutdown().unwrap();
        drop(stores);

        let stores = open_data_stores::<dolos_cardano::CardanoDelta>(&config).unwrap();
        assert_eq!(
            stores.state.read_cursor().unwrap(),
            Some(ChainPoint::Origin)
        );
        assert_eq!(stores.state.identity().unwrap(), current());
        assert_eq!(stores.archive.identity().unwrap(), current());
    }

    /// A store left empty by a release that recorded nothing is a new store.
    #[test]
    fn an_empty_store_without_an_identity_is_initialised() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));

        let state = config.storage.state_path().unwrap();
        dolos_fjall::StateStore::open(&state, &FjallStateConfig::default())
            .unwrap()
            .shutdown()
            .unwrap();

        assert_eq!(
            open_state_store(&config).unwrap().identity().unwrap(),
            current()
        );
    }

    #[test]
    fn a_store_with_data_and_no_identity_is_refused_with_the_remedy() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        written_state(&config, None);
        written_archive(&config, None);

        let state = refusal(open_state_store(&config)).expect("the state store opened");
        assert_eq!(
            (state.store, &state.path, &state.found),
            ("state", &config.storage.state_path().unwrap(), &None)
        );

        let archive = refusal(open_archive_store(&config)).expect("the archive store opened");
        assert_eq!(
            (archive.store, &archive.path, &archive.found),
            ("archive", &config.storage.archive_path().unwrap(), &None)
        );

        let message = open_data_stores::<dolos_cardano::CardanoDelta>(&config)
            .err()
            .expect("the stores opened")
            .to_string();
        for remedy in [
            "dolos bootstrap relay --force",
            "dolos bootstrap stelae --force",
        ] {
            assert!(message.contains(remedy), "{message}");
        }

        // A refusal records nothing.
        assert!(refusal(open_state_store(&config)).is_some());
        assert!(refusal(open_archive_store(&config)).is_some());
    }

    #[test]
    fn an_unsupported_identity_is_refused() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        written_state(&config, Some("v1"));
        written_archive(&config, Some("v1"));

        let message = open_state_store(&config).err().unwrap().to_string();
        assert!(message.contains("`v1`"), "{message}");

        for refused in [
            refusal(open_state_store(&config)),
            refusal(open_archive_store(&config)),
        ] {
            assert_eq!(refused.unwrap().found.as_deref(), Some("v1"));
        }
    }

    /// The identity is the only input: every configuration a node could be
    /// given over the same stores opens what this release wrote and refuses
    /// the rest.
    #[test]
    fn editing_the_config_changes_no_outcome() {
        let edits: [fn(&mut RootConfig); 5] = [
            |_| {},
            |config| config.mempool.persistent = true,
            |config| config.history = HistoryConfig::rolling(HistoryWindow::slots(1_000)),
            |config| config.storage.archive.blocks_path = Some("segments".into()),
            |config| config.sync.wal_path = Some("journal".into()),
        ];

        for recorded in [None, Some("v1"), current().as_deref()] {
            let expected = match recorded {
                Some(identity) if reads_store_identity(identity) => None,
                _ => Some(recorded.map(str::to_string)),
            };

            for edit in edits {
                let temp = tempfile::tempdir().unwrap();
                let mut config = config_over(&temp.path().join("data"));
                edit(&mut config);
                written_state(&config, recorded);
                written_archive(&config, recorded);

                let outcomes = [
                    refusal(open_state_store(&config)).map(|refusal| refusal.found),
                    refusal(open_archive_store(&config)).map(|refusal| refusal.found),
                ];

                assert_eq!(
                    outcomes,
                    [expected.clone(), expected.clone()],
                    "{recorded:?}"
                );
            }
        }
    }

    /// Every combination of `--force`, `--skip-if-data` and `--continue`.
    fn every_policy() -> impl Iterator<Item = ExistingDataPolicy> {
        (0..8u8).map(|bits| ExistingDataPolicy {
            force: bits & 1 != 0,
            skip_if_data: bits & 2 != 0,
            r#continue: bits & 4 != 0,
        })
    }

    /// Over stores this release opens, `--continue` proceeds whatever storage
    /// holds, and the other flags decide only about a node: empty storage
    /// proceeds under every one.
    #[test]
    fn inspecting_storage_follows_the_flags_over_stores_this_release_opens() {
        for node in [false, true] {
            let temp = tempfile::tempdir().unwrap();
            let config = config_over(&temp.path().join("data"));

            if node {
                written_state(&config, current().as_deref());
            }

            for policy in every_policy() {
                let inspected = inspect_existing_data(&config, policy);

                if policy.r#continue || !node {
                    assert_eq!(inspected.unwrap(), Existing::Proceed, "{policy:?}");
                } else if policy.skip_if_data {
                    assert_eq!(inspected.unwrap(), Existing::Skip, "{policy:?}");
                } else if policy.force {
                    assert_eq!(inspected.unwrap(), Existing::Clear, "{policy:?}");
                } else {
                    assert!(inspected.is_err(), "{policy:?}");
                }
            }

            assert_eq!(has_existing_data(&config).unwrap(), node);
        }
    }

    /// Bootstrap's look at storage opens nothing absent, lets `--force` on
    /// its own clear a refused store, and refuses it under every other
    /// policy, resuming and skipping included. `--force` given with one of
    /// those is told it clears only on its own.
    #[test]
    fn inspecting_storage_reports_a_refused_store_as_data_in_the_way() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));

        let none = ExistingDataPolicy::default();
        assert_eq!(
            inspect_existing_data(&config, none).unwrap(),
            Existing::Proceed
        );
        assert!(!config.storage.path.exists());

        written_archive(&config, None);

        for policy in every_policy() {
            let inspected = inspect_existing_data(&config, policy);

            if !policy.force {
                assert!(refusal(inspected).is_some(), "{policy:?}");
            } else if policy.skip_if_data || policy.r#continue {
                let message = match inspected {
                    Err(Error::Message(message)) => message,
                    other => panic!("not refused as a conflict: {other:?}"),
                };

                assert!(message.contains("no compatibility identity"), "{message}");
                assert!(message.contains("only on its own"), "{message}");
            } else {
                assert_eq!(inspected.unwrap(), Existing::Clear);
            }
        }

        assert!(refusal(has_existing_data(&config)).is_some());
    }

    /// `--force` clears the storage root alone, so it refuses a refused store
    /// that keeps a directory outside the root, naming that directory, even
    /// behind a refused state, as in an upgrade where both stores are refused.
    /// Looking doesn't recreate a directory the operator removed, so once
    /// they are gone, `--force` clears the rest.
    #[test]
    fn force_names_what_it_cannot_clear_of_a_refused_store() {
        type Edit = fn(&mut RootConfig, &Path);
        type Outside = fn(&RootConfig) -> Option<PathBuf>;

        let cases: [(Edit, Outside); 3] = [
            (
                |config, elsewhere| config.storage.archive.path = Some(elsewhere.join("archive")),
                |config| config.storage.archive_path(),
            ),
            (
                |config, _| config.storage.archive.path = Some("../archive".into()),
                |config| config.storage.archive_path(),
            ),
            (
                |config, elsewhere| {
                    config.storage.archive.blocks_path = Some(elsewhere.join("segments"))
                },
                |config| config.storage.blocks_path(),
            ),
        ];

        let force = ExistingDataPolicy {
            force: true,
            ..Default::default()
        };

        for (edit, outside) in cases {
            for refused_state in [false, true] {
                let temp = tempfile::tempdir().unwrap();
                let mut config = config_over(&temp.path().join("data"));
                edit(&mut config, &temp.path().join("elsewhere"));
                std::fs::create_dir_all(&config.storage.path).unwrap();

                if refused_state {
                    written_state(&config, None);
                }

                written_archive(&config, None);

                let outside = outside(&config).unwrap();
                std::fs::create_dir_all(&outside).unwrap();

                let message = match inspect_existing_data(&config, force) {
                    Err(Error::Message(message)) => message,
                    other => panic!("not refused as outside the root: {other:?}"),
                };

                assert!(
                    message.contains(&outside.display().to_string()),
                    "{message}"
                );
                assert!(message.contains("by hand"), "{message}");

                std::fs::remove_dir_all(&outside).unwrap();
                let inspected = inspect_existing_data(&config, force).unwrap();
                assert!(!outside.exists(), "{}", outside.display());

                if refused_state {
                    assert_eq!(inspected, Existing::Clear);
                }
            }
        }
    }

    /// A state that fails to open for a reason other than its identity — an
    /// interrupted unpack, or a store a running node holds — is left to a
    /// resumed run, as before bootstrap read identities, and stops every
    /// other flag, `--force` included, a refused archive beside it or not.
    #[test]
    fn a_state_that_fails_to_open_is_left_to_a_resumed_run() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        std::fs::create_dir_all(&config.storage.path).unwrap();
        std::fs::write(config.storage.state_path().unwrap(), b"not a store").unwrap();

        let elsewhere = tempfile::tempdir().unwrap();
        let held = config_over(&elsewhere.path().join("data"));
        written_state(&held, current().as_deref());
        let _running = open_state_store(&held).unwrap();

        for config in [&config, &held] {
            assert!(open_state_store(config).is_err());

            for policy in every_policy() {
                let inspected = inspect_existing_data(config, policy);

                if policy.r#continue {
                    assert_eq!(inspected.unwrap(), Existing::Proceed, "{policy:?}");
                } else {
                    assert!(inspected.is_err(), "{policy:?}");
                }
            }
        }

        written_archive(&config, None);

        for policy in every_policy() {
            let inspected = inspect_existing_data(&config, policy);

            if policy.force && !policy.skip_if_data && !policy.r#continue {
                assert!(
                    matches!(inspected, Err(Error::StateError(_))),
                    "{inspected:?}"
                );
            } else if !policy.force {
                assert!(refusal(inspected).is_some(), "{policy:?}");
            } else {
                assert!(inspected.is_err(), "{policy:?}");
            }
        }
    }

    /// The archive is opened for its identity alone: one that fails to open
    /// for any other reason is the command's to report, as before bootstrap
    /// looked at it, and doesn't stand in the way of `--force` or a skip.
    #[test]
    fn an_archive_that_fails_to_open_is_left_to_the_command() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        written_state(&config, current().as_deref());
        std::fs::write(config.storage.archive_path().unwrap(), b"not a store").unwrap();
        assert!(open_archive_store(&config).is_err());

        let force = ExistingDataPolicy {
            force: true,
            ..Default::default()
        };
        let skip = ExistingDataPolicy {
            skip_if_data: true,
            ..Default::default()
        };
        assert_eq!(
            inspect_existing_data(&config, force).unwrap(),
            Existing::Clear
        );
        assert_eq!(
            inspect_existing_data(&config, skip).unwrap(),
            Existing::Skip
        );
    }

    #[test]
    fn fjall_stores_open_under_the_storage_root() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        let mut config = config_over(&root);
        config.mempool.persistent = true;

        let stores = open_data_stores::<dolos_cardano::CardanoDelta>(&config).unwrap();

        assert!(matches!(stores.state, StateStoreBackend::Fjall(_)));
        assert!(matches!(stores.archive, ArchiveStoreBackend::Fjall(_)));
        assert!(matches!(stores.mempool, MempoolBackend::Redb(_)));
        for store in ["wal", "state", "archive", "mempool"] {
            assert!(root.join(store).exists(), "{store}");
        }
    }

    #[test]
    fn ephemeral_state_keeps_the_wal_in_memory() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        let mut config = config_over(&root);
        config.storage.state = StateStoreConfig::ephemeral();
        config.storage.archive = ArchiveStoreConfig::ephemeral();

        let stores = open_data_stores::<dolos_cardano::CardanoDelta>(&config).unwrap();

        assert!(matches!(stores.state, StateStoreBackend::Memory(_)));
        assert!(matches!(stores.archive, ArchiveStoreBackend::Memory(_)));
        assert!(matches!(stores.mempool, MempoolBackend::Ephemeral(_)));
        assert!(!root.exists());
    }

    #[test]
    fn no_history_opens_no_archive() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config_over(&temp.path().join("data"));
        config.history = HistoryConfig::none();

        let archive = open_archive_store(&config).unwrap();

        assert!(matches!(archive, ArchiveStoreBackend::NoOp(_)));
        assert!(!temp.path().join("data").join("archive").exists());
    }

    /// `ephemeral` has to reach the builtin stores, not a memory-mode disk
    /// backend, which could not serve the snapshot export seam.
    #[test]
    fn ephemeral_selects_the_builtin_stores() {
        let path = std::path::Path::new("/nonexistent");

        let state =
            StateStoreBackend::open(path, StateSchema::default(), &StateStoreConfig::ephemeral())
                .expect("ephemeral state store should open without touching the path");

        assert!(matches!(state, StateStoreBackend::Memory(_)));

        let archive = ArchiveStoreBackend::open(
            path,
            None,
            StateSchema::default(),
            ArchiveStoreConfig::ephemeral().kind,
        )
        .expect("ephemeral archive store should open without touching the path");

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
