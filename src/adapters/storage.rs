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
        Compatibility, RedbMempoolConfig, RedbStateConfig, RedbWalConfig, RootConfig,
        StateStoreConfig, StoreType,
    },
    ArchiveIndexDelta, BlockBody, BlockSlot, CacheDiskTier, CacheEntry, CacheError, ChainPoint,
    EntityDelta, EntityKey, EntityKind, EntityRecord, EntityValue, ExactRecord, IndexRecord,
    KeyRange, LogEntry, LogKey, LogValue, MempoolError, MempoolEvent, MempoolStore, MempoolTx,
    Namespace, QueryLimiter, RawBlock, StateError, StateSchema, StateStore as CoreStateStore,
    StateWriter as CoreStateWriter, TagDimension, TagEntry, TagOrder, TagRecord, TagStart,
    TieredCache, TxHash, TxStatus, TxoRef, UtxoEntry, UtxoIndexDelta, UtxoMap, UtxoSet,
    UtxoSetDelta, WalError, WalStore,
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
/// describes would tell a resumed restore to skip layers whose data is gone.
/// Anything that clears storage has to clear that too, and taking the directory
/// is how this does it without having to remember.
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

/// What removing a directory takes, as [`std::fs::remove_dir_all`] removes
/// it: the directory, and every directory and link under it, links not
/// followed. Known by file identity, so that a second name for one of them,
/// a bind mount, a macOS firmlink or another case on a case-insensitive
/// volume, is the same directory.
///
/// Taken when the directory exists. Of one that doesn't, only its path is
/// known, and a path reaches into it as written.
pub struct Removal {
    taken: std::collections::HashSet<FileId>,
    pending: Option<PathBuf>,
}

/// How many links a path may pass through, as the OS limits them.
const MAX_LINKS: usize = 40;

impl Removal {
    /// What removing `dir` takes, every directory under it read: one that
    /// can't be listed is an error that names it, as it stops the removal.
    pub fn of(dir: &Path) -> std::io::Result<Self> {
        Self::walk(dir, false)
    }

    /// [`Removal::of`] as far as `dir` can be read: a directory under it that
    /// can't be listed is taken without what it holds, which the OS reaches
    /// only through it.
    pub fn of_readable(dir: &Path) -> std::io::Result<Self> {
        Self::walk(dir, true)
    }

    fn walk(dir: &Path, skip_unlisted: bool) -> std::io::Result<Self> {
        let mut taken = std::collections::HashSet::new();

        let meta = match std::fs::symlink_metadata(dir) {
            Ok(meta) => meta,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(Self {
                    taken,
                    pending: Some(settled(dir)?),
                });
            }
            Err(error) => return Err(at(dir, error)),
        };

        collect(dir, &meta, &mut taken, skip_unlisted)?;

        Ok(Self {
            taken,
            pending: None,
        })
    }

    /// Whether the removal takes `path`, or a directory or link the OS passes
    /// on its way to it: links and `..` are followed as the OS follows them,
    /// and the part of `path` that doesn't exist yet is made under the last
    /// directory that does.
    pub fn takes(&self, path: &Path) -> std::io::Result<bool> {
        if let Some(pending) = &self.pending {
            return Ok(settled(path)?.starts_with(pending));
        }

        self.takes_on_the_way(path, MAX_LINKS)
    }

    fn takes_on_the_way(&self, path: &Path, links: usize) -> std::io::Result<bool> {
        let mut reached = PathBuf::new();

        // The components past the last one that exists, which the OS makes
        // as new directories: none of them is taken, and a `..` among them
        // steps back out of one.
        let mut made = 0usize;

        for component in std::path::absolute(path)?.components() {
            if made > 0 {
                match component {
                    Component::ParentDir => made -= 1,
                    Component::CurDir => {}
                    _ => made += 1,
                }

                continue;
            }

            reached.push(component);

            if let Component::Prefix(_) = component {
                continue;
            }

            let meta = match std::fs::symlink_metadata(&reached) {
                Ok(meta) => meta,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    reached.pop();
                    made = 1;
                    continue;
                }
                Err(error) => return Err(at(&reached, error)),
            };

            if self
                .taken
                .contains(&file_id(&reached, &meta).map_err(|error| at(&reached, error))?)
            {
                return Ok(true);
            }

            if meta.is_symlink() {
                let links = links.checked_sub(1).ok_or_else(|| {
                    std::io::Error::other(format!(
                        "{} passes through too many links",
                        path.display()
                    ))
                })?;

                let target = std::fs::read_link(&reached).map_err(|error| at(&reached, error))?;
                let target = reached.parent().unwrap_or(&reached).join(target);

                if self.takes_on_the_way(&target, links)? {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }
}

/// Whether removing `dir` takes `path`; see [`Removal::takes`].
pub fn reaches_into(path: &Path, dir: &Path) -> std::io::Result<bool> {
    Removal::of(dir)?.takes(path)
}

/// Add `path`, whose metadata is `meta`, to `taken`, with what is under it
/// when it is a directory and not a link. What is gone by the time it is
/// read is not there to take, and, when `skip_unlisted`, nor is what is in a
/// directory that can't be listed.
fn collect(
    path: &Path,
    meta: &std::fs::Metadata,
    taken: &mut std::collections::HashSet<FileId>,
    skip_unlisted: bool,
) -> std::io::Result<()> {
    use std::io::ErrorKind;

    taken.insert(file_id(path, meta).map_err(|error| at(path, error))?);

    if !meta.is_dir() {
        return Ok(());
    }

    let entries = match std::fs::read_dir(path) {
        Ok(entries) => entries,
        Err(error) if error.kind() == ErrorKind::NotFound => return Ok(()),
        Err(error) if skip_unlisted && error.kind() == ErrorKind::PermissionDenied => return Ok(()),
        Err(error) => return Err(at(path, error)),
    };

    for entry in entries {
        let entry = entry.map_err(|error| at(path, error))?;
        let path = entry.path();
        let kind = entry.file_type().map_err(|error| at(&path, error))?;

        if kind.is_dir() || kind.is_symlink() {
            match std::fs::symlink_metadata(&path) {
                Ok(meta) => collect(&path, &meta, taken, skip_unlisted)?,
                Err(error) if error.kind() == ErrorKind::NotFound => {}
                Err(error) => return Err(at(&path, error)),
            }
        }
    }

    Ok(())
}

/// `error`, naming the path it happened at.
fn at(path: &Path, error: std::io::Error) -> std::io::Error {
    std::io::Error::new(error.kind(), format!("{}: {error}", path.display()))
}

#[cfg(unix)]
type FileId = (u64, u64);

#[cfg(not(unix))]
type FileId = PathBuf;

/// What a file is, whatever it is called: its device and inode.
#[cfg(unix)]
fn file_id(_: &Path, meta: &std::fs::Metadata) -> std::io::Result<FileId> {
    use std::os::unix::fs::MetadataExt;

    Ok((meta.dev(), meta.ino()))
}

/// What a file is, whatever it is called: its final path, which Windows
/// spells with the case the volume records. A link is its entry in its
/// directory's final path.
#[cfg(not(unix))]
fn file_id(path: &Path, meta: &std::fs::Metadata) -> std::io::Result<FileId> {
    if !meta.is_symlink() {
        return std::fs::canonicalize(path);
    }

    let parent = path.parent().unwrap_or(path);
    let name = path.file_name().unwrap_or_default();

    Ok(std::fs::canonicalize(parent)?.join(name))
}

/// `path` made absolute, with each part that exists resolved as the OS
/// resolves it and the rest as written.
fn settled(path: &Path) -> std::io::Result<PathBuf> {
    let mut reached = PathBuf::new();

    for component in std::path::absolute(path)?.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                reached.pop();
            }
            Component::Prefix(_) => reached.push(component),
            Component::RootDir | Component::Normal(_) => {
                reached.push(component);
                reached = std::fs::canonicalize(&reached).unwrap_or(reached);
            }
        }
    }

    Ok(reached)
}

/// `path` made absolute, with each `..` taken as the OS takes it: out of the
/// directory reached, through a link's target. Any other link is kept as
/// written, so the path still names what it named.
pub fn resolve_parent_dirs(path: &Path) -> std::io::Result<PathBuf> {
    let mut reached = PathBuf::new();

    for component in std::path::absolute(path)?.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => {
                reached = std::fs::canonicalize(&reached).unwrap_or(reached);
                reached.pop();
            }
            other => reached.push(other),
        }
    }

    Ok(reached)
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
    /// `--skip-if-data`: leave a node alone and exit zero. Data without a
    /// node, and a store this release refuses, are not skipped.
    pub skip_if_data: bool,
    /// `--continue`: go ahead, resuming a replay from its committed position.
    pub r#continue: bool,
}

/// What storage holds, as a bootstrap decides about it.
#[derive(Default)]
struct Found {
    /// The state holds a cursor.
    node: bool,
    /// The state failed to open for a reason other than its identity, or its
    /// cursor failed to read.
    unreadable: Option<Error>,
    /// The WAL, when another process holds it open.
    wal_in_use: Option<PathBuf>,
    /// The first identity refusal.
    refused: Option<Error>,
    /// The WAL, state, archive and segment directories holding data: a store
    /// with data or one this release refuses, a WAL or an archive that fails
    /// to open, or block segments.
    held: Vec<PathBuf>,
}

/// Open the stores whose directories exist, creating no store directory: a
/// state cursor is a node, and failures to open or to read the cursor are
/// reported rather than raised. The WAL is read without opening it as a
/// store, which would stamp or wipe one of another version.
fn found_in_storage(config: &RootConfig) -> Result<Found, Error> {
    let mut found = Found::default();

    if let Some(path) = config.storage.state_path().filter(|path| path.exists()) {
        match open_state_store(config) {
            Ok(state) => match state.read_cursor() {
                Ok(cursor) => {
                    found.node = cursor.is_some();

                    // Rows that fail to read count as data, as the archive's
                    // do, and are left to the command that uses the state.
                    if state.holds_data().unwrap_or(true) {
                        found.held.push(path);
                    }
                }
                // A cursor that fails to read can't tell a node from none.
                Err(error) => found.unreadable = Some(error.into()),
            },
            Err(error @ Error::StateError(StateError::IncompatibleStore(_))) => {
                found.refused = Some(error);
                found.held.push(path);
            }
            Err(error) => found.unreadable = Some(error),
        }
    }

    if let Some(path) = config.wal_path().filter(|path| path.exists()) {
        use dolos_redb3::wal::WalProbe;

        match dolos_redb3::wal::probe(&path) {
            Ok(WalProbe::Empty) => {}
            Ok(WalProbe::InUse) => found.wal_in_use = Some(path),
            // A WAL that fails to read counts as data, as the archive does.
            Ok(WalProbe::Entries) | Err(_) => found.held.push(path),
        }
    }

    if !config.keeps_archive() {
        return Ok(found);
    }

    if let Some(path) = config.storage.archive_path().filter(|path| path.exists()) {
        // Any failure but the identity's is left to the command that uses the
        // archive, as before stores recorded identities, and counts as data.
        let holds_data = match open_archive_index(config) {
            Ok(archive) => archive.holds_data().unwrap_or(true),
            Err(refusal @ ArchiveError::IncompatibleStore(_)) => {
                found.refused.get_or_insert(refusal.into());
                true
            }
            Err(_) => true,
        };

        if holds_data || holds_segments(&path) {
            found.held.push(path);
        }
    }

    if let Some(path) = config
        .storage
        .blocks_path()
        .filter(|path| holds_segments(path))
    {
        found.held.push(path);
    }

    Ok(found)
}

/// The archive opened to read its index alone, with its segments in its own
/// directory, so a `blocks_path` that is gone isn't created.
fn open_archive_index(config: &RootConfig) -> Result<ArchiveStoreBackend, ArchiveError> {
    ArchiveStoreBackend::open(
        config.storage.archive_path().unwrap_or_default(),
        None,
        dolos_cardano::model::build_log_schema(),
        config.storage.archive.kind,
    )
}

/// Whether `dir` holds a block segment file. Something there that can't be
/// listed counts, since what it holds can't be told.
fn holds_segments(dir: &Path) -> bool {
    match std::fs::read_dir(dir) {
        Ok(entries) => entries.flatten().any(|entry| {
            entry
                .file_name()
                .to_str()
                .and_then(dolos_fjall::flatfiles::parse_segment_filename)
                .is_some()
        }),
        Err(error) => error.kind() != std::io::ErrorKind::NotFound,
    }
}

/// Whether storage already holds a node: a state cursor is the whole test. A
/// store this release refuses to open is refused here too.
pub fn has_existing_data(config: &RootConfig) -> Result<bool, Error> {
    let found = found_in_storage(config)?;

    if let Some(refusal) = found.refused {
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
/// operator whose answer is then thrown away. Any managed store holding data
/// is existing data, a state without a cursor included: a bootstrap
/// interrupted before it commits a cursor leaves one, a Stelae restore,
/// which commits it last, or a replay stopped before its first commit.
/// `--skip-if-data` skips only a node, that of a replay interrupted after it
/// committed included, and refuses data without one, which a daemon would
/// sync from the origin on top of; the refusal with no flag offers it only
/// over a node.
///
/// A store this release refuses to open is data in the way that only `--force`
/// on its own clears; every other flag, `--continue` included, gets its
/// refusal. A state that fails to open for another reason, or whose cursor
/// fails to read, and a WAL another process holds, are left to `--continue`,
/// and stop every other flag, `--force` included: whether they hold a node
/// can't be told, and a running node holds its stores that way.
pub fn inspect_existing_data(
    config: &RootConfig,
    policy: ExistingDataPolicy,
) -> Result<Existing, Error> {
    let mut found = found_in_storage(config)?;

    if let Some(refusal) = found.refused.take() {
        return clear_refused(config, policy, refusal, found);
    }

    if policy.r#continue {
        return Ok(Existing::Proceed);
    }

    if let Some(error) = found.unreadable {
        return Err(error);
    }

    if let Some(wal) = found.wal_in_use {
        return Err(Error::message(format!(
            "the WAL at {} is in use by another process. Stop it, then run the bootstrap again",
            wal.display()
        )));
    }

    if found.held.is_empty() {
        return Ok(Existing::Proceed);
    }

    if policy.skip_if_data && found.node {
        return Ok(Existing::Skip);
    }

    let held: Vec<_> = found
        .held
        .iter()
        .map(|path| path.display().to_string())
        .collect();

    let detected = format!("existing data detected in storage ({})", held.join(", "));

    if policy.skip_if_data {
        return Err(Error::message(format!(
            "{detected}, but no node: there is no state cursor, as a bootstrap interrupted \
             before it commits one leaves none, and `--skip-if-data` leaves only a node. Use \
             --force to replace it"
        )));
    }

    if policy.force {
        return clear(config, &detected, found);
    }

    if !found.node {
        return Err(Error::message(format!(
            "{detected}. Use --force to replace it"
        )));
    }

    Err(Error::message(format!(
        "{detected}. Use --force to replace it, or --skip-if-data to leave it and exit 0"
    )))
}

/// The decision on storage holding a store this release refuses: `--force`
/// on its own clears it, as it clears any other data.
fn clear_refused(
    config: &RootConfig,
    policy: ExistingDataPolicy,
    refusal: Error,
    found: Found,
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

    clear(config, &refusal.to_string(), found)
}

/// `--force`'s decision: clear the storage root, unless storage also holds
/// what clearing the root doesn't reach — a state that fails to open for
/// another reason or whose cursor fails to read, which `--force` never
/// clears, a WAL another process holds, or a directory outside the root that
/// holds data. Those are named for removal by hand, after `detected`, each
/// with the reason `--force` leaves it.
fn clear(config: &RootConfig, detected: &str, found: Found) -> Result<Existing, Error> {
    let mut left = Vec::new();

    if let Some(error) = found.unreadable {
        let state = config.storage.state_path().unwrap_or_default();
        left.push(format!(
            "  {}: a state store that fails to open or to read its cursor, which `--force` \
             never clears: {error}",
            state.display()
        ));
    }

    if let Some(wal) = found.wal_in_use {
        left.push(format!(
            "  {}: a WAL another process holds open, which `--force` never clears",
            wal.display()
        ));
    }

    for path in outside_storage_root(config, found.held.into_iter().map(Some))? {
        left.push(format!(
            "  {}: data outside {}, the storage root `--force` clears",
            path.display(),
            config.storage.path.display(),
        ));
    }

    if left.is_empty() {
        return Ok(Existing::Clear);
    }

    Err(Error::message(format!(
        "{detected}\n`--force` would leave:\n{}\nStop anything using them, remove them by \
         hand, then run the bootstrap again",
        left.join("\n"),
    )))
}

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
            redb_opened(
                &path,
                WalStoreBackend::open_redb(&path, &RedbWalConfig::default()),
            )
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
    open_archive_at(config, &path, Access::Mutate)
}

pub fn open_state_store(config: &RootConfig) -> Result<StateStoreBackend, Error> {
    let path = config.storage.state_path().unwrap_or_default();
    ensure_store_path(&path)?;
    open_state_at(config, &path, Access::Mutate)
}

/// [`ArchiveStoreBackend::open`], read-only over a view under
/// [`Access::Observe`], with a lock another handle holds reported as
/// [`Error::InUse`].
fn open_archive_at(
    config: &RootConfig,
    path: &Path,
    access: Access,
) -> Result<ArchiveStoreBackend, Error> {
    let schema = dolos_cardano::model::build_log_schema();

    match config.storage.archive.kind {
        StoreType::Fjall { preset } => {
            let blocks = config.storage.blocks_path();
            let blocks = blocks.as_deref();

            let store = match access {
                Access::Observe => dolos_fjall::archive::ArchiveStore::open_view(
                    schema,
                    path,
                    blocks,
                    preset.into(),
                ),
                Access::Mutate => {
                    dolos_fjall::archive::ArchiveStore::open(schema, path, blocks, preset.into())
                }
            }
            .map_err(|error| fjall_failure(path, error, ArchiveError::from))?;

            Ok(ArchiveStoreBackend::admitted_fjall(store, path, access)?)
        }
        StoreType::Ephemeral => Ok(ArchiveStoreBackend::in_memory(schema)?),
    }
}

/// [`StateStoreBackend::open`], read-only over a view under
/// [`Access::Observe`], with a lock another handle holds reported as
/// [`Error::InUse`].
fn open_state_at(
    config: &RootConfig,
    path: &Path,
    access: Access,
) -> Result<StateStoreBackend, Error> {
    match config.storage.state.kind {
        StoreType::Fjall { preset } => {
            let store = match access {
                Access::Observe => dolos_fjall::StateStore::open_view(path, preset.into()),
                Access::Mutate => dolos_fjall::StateStore::open(path, preset.into()),
            }
            .map_err(|error| fjall_failure(path, error, StateError::from))?;

            Ok(StateStoreBackend::admitted_fjall(store, path, access)?)
        }
        StoreType::Ephemeral => Ok(StateStoreBackend::in_memory()?),
    }
}

fn fjall_failure<E: Into<Error>>(
    path: &Path,
    error: dolos_fjall::Error,
    wrap: impl FnOnce(dolos_fjall::Error) -> E,
) -> Error {
    match error {
        error if error.is_locked() => Error::InUse {
            path: path.to_path_buf(),
        },
        dolos_fjall::Error::View { path, reason } => Error::Unviewable { path, reason },
        error => wrap(error).into(),
    }
}

/// A redb open, with a file another handle holds reported as
/// [`Error::InUse`].
fn redb_opened<T, E>(path: &Path, opened: Result<T, E>) -> Result<T, Error>
where
    E: std::error::Error + Into<Error> + 'static,
{
    opened.map_err(|error| {
        if dolos_redb3::handle::is_already_open(&error) {
            Error::InUse {
                path: path.to_path_buf(),
            }
        } else {
            error.into()
        }
    })
}

pub fn open_mempool_store(config: &RootConfig) -> Result<MempoolBackend, Error> {
    match config.mempool_path() {
        None => Ok(MempoolBackend::Ephemeral(
            dolos_core::builtin::EphemeralMempool::new(),
        )),
        Some(path) => {
            ensure_store_path(&path)?;
            let mempool = redb_opened(
                &path,
                dolos_redb3::mempool::RedbMempool::open(&path, &RedbMempoolConfig::default()),
            )?;
            Ok(MempoolBackend::Redb(mempool))
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

/// What a command does with the stores it opens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// Reads only: the WAL and the mempool open read-only, the fjall stores
    /// over a view of their files beside them, refusing writers, and no
    /// identity is recorded on a store.
    Observe,
    /// Reads and writes, as the node does.
    Mutate,
}

/// Why [`open_existing_stores`] did not open a dataset.
#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    /// The store at `path`, a file or a database directory, is not there.
    #[error("no store at {}", .path.display())]
    Missing { path: PathBuf },

    #[error("{} is locked by another process", .path.display())]
    InUse { path: PathBuf },

    /// A redb file a crash left behind: only a writable open repairs it.
    #[error("{} was not shut down cleanly", .path.display())]
    Unclean { path: PathBuf },

    /// A fjall store an observing open could not mirror beside it.
    #[error("cannot mirror {} for a read-only open: {reason}", .path.display())]
    Unviewable { path: PathBuf, reason: String },

    #[error(transparent)]
    Storage(Error),
}

impl miette::Diagnostic for OpenError {
    fn help<'a>(&'a self) -> Option<Box<dyn std::fmt::Display + 'a>> {
        match self {
            Self::Missing { path } => Some(Box::new(format!(
                "no dataset at {} (run `dolos bootstrap`)",
                path.display()
            ))),
            Self::InUse { .. } => Some(Box::new(
                "dataset in use by another dolos process (stop it, or query its APIs)",
            )),
            Self::Unclean { .. } => Some(Box::new(
                "a reading command cannot open it until a writing one repairs it: start `dolos sync` or `dolos daemon` once",
            )),
            Self::Unviewable { .. } => Some(Box::new(
                "a reading command mirrors a store in the folder beside it, linking its tables: it needs write access there, on a filesystem with hard or symbolic links",
            )),
            Self::Storage(_) => None,
        }
    }
}

impl From<Error> for OpenError {
    fn from(error: Error) -> Self {
        match error {
            Error::InUse { path } => Self::InUse { path },
            Error::Unviewable { path, reason } => Self::Unviewable { path, reason },
            other => Self::Storage(other),
        }
    }
}

/// Open the stores of a dataset that is already there, as `access` says,
/// without changing what is on disk to do it.
///
/// Unlike [`open_data_stores`], a store that is not there is
/// [`OpenError::Missing`], never created; a missing mempool file is the one
/// exception, opened empty in memory, since it holds no chain data. Nothing
/// is caught up or bootstrapped: the stores are as the last writer left them.
/// A store another process holds is [`OpenError::InUse`]. Observing writes
/// no file of the dataset, and a redb file a crash left unrepaired, or empty,
/// is [`OpenError::Unclean`]; it mirrors each fjall store in a folder beside
/// it while the store lives, [`OpenError::Unviewable`] where it cannot. While
/// the observed stores live, a writer cannot open them.
pub fn open_existing_stores<D>(config: &RootConfig, access: Access) -> Result<Stores<D>, OpenError>
where
    D: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    existing(&config.storage.path)?;

    // The redb files first: an observing open refused for a crash-left one
    // has then opened no fjall store.
    let wal = match config.wal_path() {
        Some(path) => {
            existing(&path)?;

            let opened = match access {
                Access::Observe => {
                    dolos_redb3::wal::RedbWalStore::open_read_only(&path, &RedbWalConfig::default())
                }
                Access::Mutate => {
                    dolos_redb3::wal::RedbWalStore::open(&path, &RedbWalConfig::default())
                }
            };

            WalStoreBackend::Redb(redb_existing(&path, access, opened)?)
        }
        None => WalStoreBackend::in_memory().map_err(Error::from)?,
    };

    let mempool = match config.mempool_path().filter(|path| path.exists()) {
        Some(path) => {
            let opened = match access {
                Access::Observe => dolos_redb3::mempool::RedbMempool::open_read_only(
                    &path,
                    &RedbMempoolConfig::default(),
                ),
                Access::Mutate => {
                    dolos_redb3::mempool::RedbMempool::open(&path, &RedbMempoolConfig::default())
                }
            };

            MempoolBackend::Redb(redb_existing(&path, access, opened)?)
        }
        None => MempoolBackend::Ephemeral(dolos_core::builtin::EphemeralMempool::new()),
    };

    let state = existing_state(config, access)?;

    let archive = if !config.keeps_archive() {
        ArchiveStoreBackend::noop()
    } else {
        let path = config.storage.archive_path().unwrap_or_default();

        if matches!(config.storage.archive.kind, StoreType::Fjall { .. }) {
            fjall_existing(&path.join("index"))?;
            existing(&config.storage.blocks_path().unwrap_or_else(|| path.clone()))?;
        }

        open_archive_at(config, &path, access)?
    };

    Ok(Stores {
        wal,
        state,
        archive,
        mempool,
    })
}

/// The state store alone of a dataset that is already there, opened as
/// [`open_existing_stores`] opens it. A command that touches only the state
/// leaves the WAL, the archive and the mempool closed, so a writable open
/// cannot migrate or wipe them.
pub fn open_existing_state(
    config: &RootConfig,
    access: Access,
) -> Result<StateStoreBackend, OpenError> {
    existing(&config.storage.path)?;
    existing_state(config, access)
}

fn existing_state(config: &RootConfig, access: Access) -> Result<StateStoreBackend, OpenError> {
    let path = config.storage.state_path().unwrap_or_default();

    if matches!(config.storage.state.kind, StoreType::Fjall { .. }) {
        fjall_existing(&path)?;
    }

    Ok(open_state_at(config, &path, access)?)
}

/// [`redb_opened`], with an observing open refused for want of a repair,
/// or of an initialised file, reported as [`OpenError::Unclean`].
fn redb_existing<T, E>(path: &Path, access: Access, opened: Result<T, E>) -> Result<T, OpenError>
where
    E: std::error::Error + Into<Error> + 'static,
{
    use dolos_redb3::handle::{is_uninitialised, needs_repair};

    match opened {
        Err(error)
            if access == Access::Observe
                && (needs_repair(&error) || is_uninitialised(&error) && is_empty(path)) =>
        {
            Err(OpenError::Unclean {
                path: path.to_path_buf(),
            })
        }
        opened => Ok(redb_opened(path, opened)?),
    }
}

/// Whether the file at `path` is empty, as a crash during its creation
/// leaves it: a writable open initialises it, a read-only one cannot.
fn is_empty(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.len() == 0)
}

/// [`existing`] for a fjall database, which a directory without its marker
/// is not.
fn fjall_existing(path: &Path) -> Result<(), OpenError> {
    if dolos_fjall::database_exists(path) {
        Ok(())
    } else {
        Err(OpenError::Missing {
            path: path.to_path_buf(),
        })
    }
}

fn existing(path: &Path) -> Result<(), OpenError> {
    if path.exists() {
        Ok(())
    } else {
        Err(OpenError::Missing {
            path: path.to_path_buf(),
        })
    }
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
        preset: dolos_fjall::Preset,
    ) -> Result<Self, StateError> {
        let path = path.as_ref();
        let store = dolos_fjall::StateStore::open(path, preset)?;

        Self::admitted_fjall(store, path, Access::Mutate)
    }

    /// The fjall store opened at `path`, through [`admit`]. Under
    /// [`Access::Observe`] a new store is admitted without recording its
    /// identity.
    fn admitted_fjall(
        store: dolos_fjall::StateStore,
        path: &Path,
        access: Access,
    ) -> Result<Self, StateError> {
        admit::<StateError>(
            "state",
            path,
            store.identity()?,
            || store.holds_data(),
            |identity| match access {
                Access::Observe => Ok(()),
                Access::Mutate => store.record_identity(identity),
            },
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
            StoreType::Fjall { preset } => Self::open_fjall(path, preset.into()),
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

    /// Whether the store holds a cursor or any row, as its identity check
    /// reads it; `false` for the stores that do not record an identity.
    pub fn holds_data(&self) -> Result<bool, StateError> {
        match self {
            Self::Fjall(s) => Ok(s.holds_data()?),
            Self::Redb(_) | Self::Memory(_) => Ok(false),
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

    fn utxos_by_tag_page(
        &self,
        dimension: TagDimension,
        key: &[u8],
        order: TagOrder,
        start: TagStart,
        count: usize,
    ) -> Result<Vec<TagEntry>, StateError> {
        match self {
            Self::Redb(s) => s.utxos_by_tag_page(dimension, key, order, start, count),
            Self::Fjall(s) => s.utxos_by_tag_page(dimension, key, order, start, count),
            Self::Memory(s) => s.utxos_by_tag_page(dimension, key, order, start, count),
        }
    }

    fn utxos_with_tag(
        &self,
        dimension: TagDimension,
        key: &[u8],
        entries: Vec<TagEntry>,
    ) -> Result<Vec<TagEntry>, StateError> {
        match self {
            Self::Redb(s) => s.utxos_with_tag(dimension, key, entries),
            Self::Fjall(s) => s.utxos_with_tag(dimension, key, entries),
            Self::Memory(s) => s.utxos_with_tag(dimension, key, entries),
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
    /// writes, index applies and entity index writes pass through, block
    /// appends and undos are discarded.
    ///
    /// This exists for replays over an archive that already holds the chain
    /// (`dolos doctor rebuild-state --rewrite-logs`): block appends are not
    /// idempotent — replaying them would double every shared segment file,
    /// whichever backend owns the index — while boundary log keys are
    /// slot-derived and identical under replay, so re-written log rows
    /// overwrite the originals in place. Index entries are keyed by what they
    /// project and the slot, so a replay re-writes the ones the archive holds
    /// and adds the ones an older indexer never emitted. The replay re-runs
    /// every seal, so it writes the entity index again with the sealed logs;
    /// an entry is keyed by its id, so the rewrite lands in place.
    ///
    /// [`Self::logs_only`] is the sole constructor, and it never nests one
    /// gate inside another.
    LogsOnly(Box<ArchiveStoreBackend>),
    /// Read-gated view over an already-open archive: entity index reads pass
    /// through, every other read finds nothing and every write is discarded,
    /// as in [`Self::NoOp`].
    ///
    /// This exists for replays into a fresh state over an archive that holds
    /// the chain (`dolos doctor rebuild-state` without `--rewrite-logs`): the
    /// roll tells a script's first sighting from a repeat by the entity index
    /// below its batch, which only the source archive holds, while the seal
    /// must not read the logs the source sealed.
    ///
    /// [`Self::entity_reads`] is the sole constructor, and it never nests one
    /// gate inside another.
    EntityReads(Box<ArchiveStoreBackend>),
    Fjall(dolos_fjall::archive::ArchiveStore),
    NoOp(NoOpArchiveStore),
}

impl ArchiveStoreBackend {
    /// Open an archive store with the Fjall backend, refusing one this release
    /// did not write (see [`admit`]).
    pub fn open_fjall(
        path: impl AsRef<Path>,
        schema: StateSchema,
        blocks_path: Option<&Path>,
        preset: dolos_fjall::Preset,
    ) -> Result<Self, ArchiveError> {
        let path = path.as_ref();
        let store = dolos_fjall::archive::ArchiveStore::open(schema, path, blocks_path, preset)?;

        Self::admitted_fjall(store, path, Access::Mutate)
    }

    /// The fjall store opened at `path`, through [`admit`]. Under
    /// [`Access::Observe`] a new store is admitted without recording its
    /// identity.
    fn admitted_fjall(
        store: dolos_fjall::archive::ArchiveStore,
        path: &Path,
        access: Access,
    ) -> Result<Self, ArchiveError> {
        admit::<ArchiveError>(
            "archive",
            path,
            store.identity()?,
            || store.holds_data(),
            |identity| match access {
                Access::Observe => Ok(()),
                Access::Mutate => store.record_identity(identity),
            },
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
            Self::EntityReads(inner) => inner.logs_only(),
            Self::Memory(_) | Self::Fjall(_) => Some(Self::LogsOnly(Box::new(self.clone()))),
            Self::NoOp(_) => None,
        }
    }

    /// Wrap this backend's already-open store in a [`Self::EntityReads`] read
    /// gate, aliasing it as [`Self::logs_only`] does. Returns `None` when
    /// there is no store to wrap.
    pub fn entity_reads(&self) -> Option<Self> {
        match self {
            Self::EntityReads(_) => Some(self.clone()),
            Self::LogsOnly(inner) => inner.entity_reads(),
            Self::Memory(_) | Self::Fjall(_) => Some(Self::EntityReads(Box::new(self.clone()))),
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
            StoreType::Fjall { preset } => {
                Self::open_fjall(path, schema, blocks_path.as_deref(), preset.into())
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
            Self::Memory(_) | Self::EntityReads(_) | Self::NoOp(_) => Ok(None),
        }
    }

    /// Whether the index holds any block location, log row or index entry,
    /// as the identity check reads it; `false` for the stores that do not
    /// record an identity.
    pub fn holds_data(&self) -> Result<bool, ArchiveError> {
        match self {
            Self::Fjall(s) => Ok(s.holds_data()?),
            Self::LogsOnly(inner) => inner.holds_data(),
            Self::Memory(_) | Self::EntityReads(_) | Self::NoOp(_) => Ok(false),
        }
    }

    pub fn shutdown(&self) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(s) => s.shutdown(),
            Self::LogsOnly(inner) | Self::EntityReads(inner) => inner.shutdown(),
            Self::Fjall(s) => s.shutdown().map_err(ArchiveError::from),
            Self::NoOp(s) => s.shutdown(),
        }
    }
}

pub enum ArchiveWriterBackend {
    Memory(Box<<MemoryArchiveStore as CoreArchiveStore>::Writer>),
    /// Delegates `write_log`, `delete_log`, `apply_index`, `write_entity`
    /// and `commit`; discards `apply`,
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

    fn delete_log(&self, ns: Namespace, key: &LogKey) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => w.delete_log(ns, key),
            Self::LogsOnly(w) => w.delete_log(ns, key),
            Self::Fjall(w) => w.delete_log(ns, key),
            Self::NoOp(w) => w.delete_log(ns, key),
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

    fn write_entity(
        &self,
        kind: EntityKind,
        key: &[u8],
        slot: BlockSlot,
    ) -> Result<(), ArchiveError> {
        match self {
            Self::Memory(w) => w.write_entity(kind, key, slot),
            Self::LogsOnly(w) => w.write_entity(kind, key, slot),
            Self::Fjall(w) => w.write_entity(kind, key, slot),
            Self::NoOp(w) => w.write_entity(kind, key, slot),
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

pub enum ArchiveEntityRecordIterBackend {
    Memory(<MemoryArchiveStore as CoreArchiveStore>::EntityRecordIter),
    Fjall(<dolos_fjall::archive::ArchiveStore as CoreArchiveStore>::EntityRecordIter),
    NoOp(<NoOpArchiveStore as CoreArchiveStore>::EntityRecordIter),
}

impl Iterator for ArchiveEntityRecordIterBackend {
    type Item = Result<EntityRecord, ArchiveError>;
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
    type EntityRecordIter = ArchiveEntityRecordIterBackend;

    fn start_writer(&self) -> Result<Self::Writer, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::start_writer(s)
                .map(|writer| ArchiveWriterBackend::Memory(Box::new(writer))),
            Self::LogsOnly(inner) => CoreArchiveStore::start_writer(inner.as_ref())
                .map(|writer| ArchiveWriterBackend::LogsOnly(Box::new(writer))),
            Self::Fjall(s) => CoreArchiveStore::start_writer(s)
                .map(|writer| ArchiveWriterBackend::Fjall(Box::new(writer))),
            Self::EntityReads(_) => {
                CoreArchiveStore::start_writer(&NoOpArchiveStore).map(ArchiveWriterBackend::NoOp)
            }
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
            Self::EntityReads(_) => CoreArchiveStore::read_logs(&NoOpArchiveStore, ns, keys),
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
            Self::EntityReads(_) => CoreArchiveStore::iter_logs(&NoOpArchiveStore, ns, range)
                .map(ArchiveLogIterBackend::NoOp),
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
            Self::EntityReads(_) => CoreArchiveStore::get_block_by_slot(&NoOpArchiveStore, slot),
            Self::NoOp(s) => CoreArchiveStore::get_block_by_slot(s, slot),
        }
    }

    fn get_blocks_by_slot(&self, slot: &BlockSlot) -> Result<Vec<BlockBody>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::get_blocks_by_slot(s, slot),
            Self::LogsOnly(inner) => CoreArchiveStore::get_blocks_by_slot(inner.as_ref(), slot),
            Self::Fjall(s) => CoreArchiveStore::get_blocks_by_slot(s, slot),
            Self::EntityReads(_) => CoreArchiveStore::get_blocks_by_slot(&NoOpArchiveStore, slot),
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
            Self::EntityReads(_) => CoreArchiveStore::get_range(&NoOpArchiveStore, from, to)
                .map(ArchiveBlockIterBackend::NoOp),
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
            Self::EntityReads(_) => CoreArchiveStore::find_intersect(&NoOpArchiveStore, intersect),
            Self::NoOp(s) => CoreArchiveStore::find_intersect(s, intersect),
        }
    }

    fn get_tip(&self) -> Result<Option<(BlockSlot, BlockBody)>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::get_tip(s),
            Self::LogsOnly(inner) => CoreArchiveStore::get_tip(inner.as_ref()),
            Self::Fjall(s) => CoreArchiveStore::get_tip(s),
            Self::EntityReads(_) => CoreArchiveStore::get_tip(&NoOpArchiveStore),
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
            Self::EntityReads(_) => CoreArchiveStore::prune_history(
                &NoOpArchiveStore,
                max_slots,
                max_prune_slots,
                max_prune_index_rows,
            ),
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
            Self::EntityReads(_) => CoreArchiveStore::truncate_front(&NoOpArchiveStore, after),
            Self::NoOp(s) => CoreArchiveStore::truncate_front(s, after),
        }
    }

    fn slot_by_block_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::slot_by_block_hash(s, hash),
            Self::LogsOnly(inner) => CoreArchiveStore::slot_by_block_hash(inner.as_ref(), hash),
            Self::Fjall(s) => CoreArchiveStore::slot_by_block_hash(s, hash),
            Self::EntityReads(_) => CoreArchiveStore::slot_by_block_hash(&NoOpArchiveStore, hash),
            Self::NoOp(s) => CoreArchiveStore::slot_by_block_hash(s, hash),
        }
    }

    fn slot_by_block_number(&self, number: u64) -> Result<Option<BlockSlot>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::slot_by_block_number(s, number),
            Self::LogsOnly(inner) => CoreArchiveStore::slot_by_block_number(inner.as_ref(), number),
            Self::Fjall(s) => CoreArchiveStore::slot_by_block_number(s, number),
            Self::EntityReads(_) => {
                CoreArchiveStore::slot_by_block_number(&NoOpArchiveStore, number)
            }
            Self::NoOp(s) => CoreArchiveStore::slot_by_block_number(s, number),
        }
    }

    fn slot_by_tx_hash(&self, hash: &[u8]) -> Result<Option<BlockSlot>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::slot_by_tx_hash(s, hash),
            Self::LogsOnly(inner) => CoreArchiveStore::slot_by_tx_hash(inner.as_ref(), hash),
            Self::Fjall(s) => CoreArchiveStore::slot_by_tx_hash(s, hash),
            Self::EntityReads(_) => CoreArchiveStore::slot_by_tx_hash(&NoOpArchiveStore, hash),
            Self::NoOp(s) => CoreArchiveStore::slot_by_tx_hash(s, hash),
        }
    }

    fn entity_slots(
        &self,
        kind: EntityKind,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<BlockSlot>>, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::entity_slots(s, kind, keys),
            Self::LogsOnly(inner) | Self::EntityReads(inner) => {
                CoreArchiveStore::entity_slots(inner.as_ref(), kind, keys)
            }
            Self::Fjall(s) => CoreArchiveStore::entity_slots(s, kind, keys),
            Self::NoOp(s) => CoreArchiveStore::entity_slots(s, kind, keys),
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
            Self::EntityReads(_) => {
                CoreArchiveStore::slots_by_tag(&NoOpArchiveStore, dimension, key, start, end)
                    .map(ArchiveSlotIterBackend::NoOp)
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
            Self::EntityReads(_) => {
                CoreArchiveStore::iter_archive_tags(&NoOpArchiveStore, dimensions, slots)
                    .map(ArchiveTagIterBackend::NoOp)
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
            Self::EntityReads(_) => CoreArchiveStore::iter_exact_records(&NoOpArchiveStore, slots)
                .map(ArchiveExactIterBackend::NoOp),
            Self::NoOp(s) => {
                CoreArchiveStore::iter_exact_records(s, slots).map(ArchiveExactIterBackend::NoOp)
            }
        }
    }

    fn iter_entity_records(
        &self,
        kinds: &[EntityKind],
        slots: Range<BlockSlot>,
    ) -> Result<Self::EntityRecordIter, ArchiveError> {
        match self {
            Self::Memory(s) => CoreArchiveStore::iter_entity_records(s, kinds, slots)
                .map(ArchiveEntityRecordIterBackend::Memory),
            Self::LogsOnly(inner) | Self::EntityReads(inner) => {
                CoreArchiveStore::iter_entity_records(inner.as_ref(), kinds, slots)
            }
            Self::Fjall(s) => CoreArchiveStore::iter_entity_records(s, kinds, slots)
                .map(ArchiveEntityRecordIterBackend::Fjall),
            Self::NoOp(s) => CoreArchiveStore::iter_entity_records(s, kinds, slots)
                .map(ArchiveEntityRecordIterBackend::NoOp),
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
        EraCbor, Tag, UtxoValue,
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
        let store = dolos_fjall::StateStore::open(path, dolos_fjall::Preset::Standard).unwrap();

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
        opened_archive(config, identity).shutdown().unwrap();
    }

    /// [`written_archive`]'s store, left open.
    fn opened_archive(
        config: &RootConfig,
        identity: Option<&str>,
    ) -> dolos_fjall::archive::ArchiveStore {
        let path = config.storage.archive_path().unwrap();
        let store = dolos_fjall::archive::ArchiveStore::open(
            StateSchema::default(),
            path,
            None,
            dolos_fjall::Preset::Standard,
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
        store
    }

    /// Flip the first data byte of each table under `keyspace` but those in
    /// `kept`, so its store still opens and reading its rows fails the block
    /// checksum.
    fn corrupt_tables(keyspace: &Path, kept: &[PathBuf]) {
        // A table starts with its first data block, after a 33-byte header.
        for table in tables(keyspace) {
            if kept.contains(&table) {
                continue;
            }

            let mut bytes = std::fs::read(&table).unwrap();
            bytes[33] ^= 0xff;
            std::fs::write(&table, bytes).unwrap();
        }
    }

    fn tables(keyspace: &Path) -> Vec<PathBuf> {
        std::fs::read_dir(keyspace.join("tables"))
            .unwrap()
            .map(|table| table.unwrap().path())
            .collect()
    }

    /// A state this release wrote, holding a cursor that fails to read beside
    /// an identity that reads: each is flushed to a table of its own, and only
    /// the cursor's is corrupt. Its journal is emptied, as fjall leaves it once
    /// it has rotated: replaying the writes would put the cursor back in
    /// memory, above its table.
    fn written_state_with_unreadable_cursor(config: &RootConfig) {
        let path = config.storage.state_path().unwrap();
        let store = dolos_fjall::StateStore::open(&path, dolos_fjall::Preset::Standard).unwrap();
        let cursor = store
            .database()
            .keyspace("state-cursor", Default::default)
            .unwrap();

        store
            .record_identity(current().as_deref().unwrap())
            .unwrap();
        cursor.rotate_memtable_and_wait().unwrap();
        let keyspace = cursor.path().to_path_buf();
        let identity = tables(&keyspace);

        let writer = store.start_writer().unwrap();
        writer.set_cursor(ChainPoint::Origin).unwrap();
        writer.commit().unwrap();
        cursor.rotate_memtable_and_wait().unwrap();
        store.shutdown().unwrap();
        drop((cursor, store));

        for file in std::fs::read_dir(&path).unwrap() {
            let file = file.unwrap().path();

            if file.extension().is_some_and(|extension| extension == "jnl") {
                let journal = std::fs::OpenOptions::new().write(true).open(file).unwrap();
                let len = journal.metadata().unwrap().len();
                journal.set_len(0).unwrap();
                journal.set_len(len).unwrap();
            }
        }

        corrupt_tables(&keyspace, &identity);
    }

    /// A state this release wrote, holding a UTxO and no cursor, whose rows
    /// fail to read.
    fn written_unreadable_state(config: &RootConfig) {
        let store = opened_state_without_cursor(config);
        let utxos = store
            .database()
            .keyspace("state-utxos", Default::default)
            .unwrap();
        utxos.rotate_memtable_and_wait().unwrap();
        let keyspace = utxos.path().to_path_buf();
        store.shutdown().unwrap();
        drop((utxos, store));

        corrupt_tables(&keyspace, &[]);
    }

    /// An archive this release wrote, holding a tag, whose rows fail to read.
    fn written_unreadable_archive(config: &RootConfig) {
        let store = opened_archive(config, current().as_deref());
        let tags = store
            .database()
            .keyspace("archive-tags", Default::default)
            .unwrap();
        tags.rotate_memtable_and_wait().unwrap();
        let keyspace = tags.path().to_path_buf();
        store.shutdown().unwrap();
        drop((tags, store));

        corrupt_tables(&keyspace, &[]);
    }

    /// What `--force` on its own is told beside a refused store: the refusal
    /// a run without flags gets, then each of `left` on a line of its own.
    fn would_leave(config: &RootConfig, left: &[String]) -> String {
        let refusal = inspect_existing_data(config, ExistingDataPolicy::default()).unwrap_err();

        format!(
            "{refusal}\n`--force` would leave:\n{}\nStop anything using them, remove them by \
             hand, then run the bootstrap again",
            left.join("\n")
        )
    }

    /// The line naming a state that fails to open or to read its cursor, with
    /// the `error` it failed with.
    fn unreadable_state_line(config: &RootConfig, error: Error) -> String {
        format!(
            "  {}: a state store that fails to open or to read its cursor, which `--force` \
             never clears: {error}",
            config.storage.state_path().unwrap().display()
        )
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
        dolos_fjall::StateStore::open(&state, dolos_fjall::Preset::Standard)
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
            "dolos bootstrap stelae --force",
            "dolos bootstrap immutabledb --source <db> --force",
            "dolos data cleanup --force",
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

    /// A state this release opens holding a UTxO but no cursor, as a restore
    /// stopped before its last step leaves it.
    fn written_state_without_cursor(config: &RootConfig) {
        opened_state_without_cursor(config).shutdown().unwrap();
    }

    /// [`written_state_without_cursor`]'s store, left open.
    fn opened_state_without_cursor(config: &RootConfig) -> dolos_fjall::StateStore {
        let path = config.storage.state_path().unwrap();
        let state = dolos_fjall::StateStore::open(path, dolos_fjall::Preset::Standard).unwrap();
        state
            .record_identity(Compatibility::current().as_str())
            .unwrap();

        let utxo = (
            TxoRef(TxHash::from([1u8; 32]), 0),
            std::sync::Arc::new(UtxoValue::new(EraCbor(0, vec![0x80]), None)),
        );

        let writer = state.start_writer().unwrap();
        writer
            .apply_utxoset(&UtxoSetDelta {
                produced_utxo: [utxo].into_iter().collect(),
                ..Default::default()
            })
            .unwrap();
        writer.commit().unwrap();
        state
    }

    /// A WAL holding an entry and nothing else in storage.
    fn written_wal(config: &RootConfig) {
        let wal = open_wal_store::<dolos_cardano::CardanoDelta>(config).unwrap();
        wal.reset_to(&ChainPoint::Origin).unwrap();
        wal.shutdown().unwrap();
    }

    /// Over stores this release opens, `--continue` proceeds whatever storage
    /// holds, and the other flags decide about any store holding data: only
    /// empty storage, or an empty WAL file, proceeds under every one. A node
    /// is a state with a cursor, and only a node is skipped: data without
    /// one is refused under `--skip-if-data`, naming `--force`, and the
    /// refusal with no flag offers `--skip-if-data` only over a node.
    #[test]
    fn inspecting_storage_follows_the_flags_over_stores_this_release_opens() {
        /// What a case writes, whether storage then holds data, and whether
        /// it holds a node.
        type Case = (fn(&RootConfig), bool, bool);

        let writes: [Case; 6] = [
            (|_| {}, false, false),
            (
                |config| {
                    open_wal_store::<dolos_cardano::CardanoDelta>(config)
                        .unwrap()
                        .shutdown()
                        .unwrap()
                },
                false,
                false,
            ),
            (written_wal, true, false),
            (
                |config| written_archive(config, current().as_deref()),
                true,
                false,
            ),
            (written_state_without_cursor, true, false),
            (
                |config| written_state(config, current().as_deref()),
                true,
                true,
            ),
        ];

        for (write, held, node) in writes {
            let temp = tempfile::tempdir().unwrap();
            let config = config_over(&temp.path().join("data"));
            write(&config);

            for policy in every_policy() {
                let inspected = inspect_existing_data(&config, policy);

                if policy.r#continue || !held {
                    assert_eq!(inspected.unwrap(), Existing::Proceed, "{policy:?}");
                } else if policy.skip_if_data && node {
                    assert_eq!(inspected.unwrap(), Existing::Skip, "{policy:?}");
                } else if policy.skip_if_data {
                    let message = inspected.unwrap_err().to_string();
                    assert!(message.contains("existing data detected"), "{message}");
                    assert!(message.contains("no node"), "{message}");
                    assert!(message.contains("--force"), "{message}");
                } else if policy.force {
                    assert_eq!(inspected.unwrap(), Existing::Clear, "{policy:?}");
                } else {
                    let message = inspected.unwrap_err().to_string();
                    assert!(message.contains("existing data detected"), "{message}");
                    assert!(message.contains("--force"), "{message}");
                    assert_eq!(message.contains("--skip-if-data"), node, "{message}");
                }
            }

            assert_eq!(has_existing_data(&config).unwrap(), node);
        }
    }

    /// The WAL is read without being opened as a store, which would stamp
    /// its version: inspecting leaves its file as it was.
    #[test]
    fn inspecting_storage_reads_the_wal_without_writing_it() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        written_wal(&config);

        let path = config.wal_path().unwrap();
        let before = std::fs::read(&path).unwrap();

        assert!(inspect_existing_data(&config, ExistingDataPolicy::default()).is_err());
        assert_eq!(std::fs::read(&path).unwrap(), before);
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

    /// What a test leaves where a store belongs.
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Kept {
        /// A store with data and no identity.
        Refused,
        /// A store this release wrote.
        Current,
        /// Something no fjall store opens, as v1's redb archive.
        Foreign,
    }

    fn keep_state(config: &RootConfig, kept: Kept) {
        match kept {
            Kept::Refused => written_state(config, None),
            Kept::Current => written_state(config, current().as_deref()),
            Kept::Foreign => unreachable!("a state that fails to open stops `--force`"),
        }
    }

    fn keep_archive(config: &RootConfig, kept: Kept) {
        match kept {
            Kept::Refused => written_archive(config, None),
            Kept::Current => written_archive(config, current().as_deref()),
            Kept::Foreign => {
                std::fs::write(config.storage.archive_path().unwrap(), b"not a store").unwrap()
            }
        }
    }

    /// A block segment file in `dir`, as an archive of any release leaves one.
    fn written_segment(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join("000000.segment"), b"frames").unwrap();
    }

    type Edit = fn(&mut RootConfig, &Path);
    type Outside = fn(&RootConfig) -> Option<PathBuf>;

    /// Each way the configuration puts a store directory outside the root,
    /// and the directory it puts there.
    fn outside_layouts() -> [(Edit, Outside); 4] {
        [
            (
                |config, elsewhere| config.storage.state.path = Some(elsewhere.join("state")),
                |config| config.storage.state_path(),
            ),
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
        ]
    }

    fn force() -> ExistingDataPolicy {
        ExistingDataPolicy {
            force: true,
            ..Default::default()
        }
    }

    /// `--force` clears the storage root alone, so beside a refused store it
    /// refuses while a directory outside the root holds data, naming it: a
    /// state or archive with data, refused or current, an archive it can't
    /// open, or block segments. Looking doesn't recreate a directory the
    /// operator removed. Once it is gone, `--force` clears what the root
    /// still holds, refused or not.
    #[test]
    fn force_names_data_outside_the_root() {
        let stores = [
            (Kept::Refused, Kept::Refused),
            (Kept::Refused, Kept::Current),
            (Kept::Refused, Kept::Foreign),
            (Kept::Current, Kept::Refused),
        ];

        for (edit, outside) in outside_layouts() {
            for (state, archive) in stores {
                let temp = tempfile::tempdir().unwrap();
                let mut config = config_over(&temp.path().join("data"));
                edit(&mut config, &temp.path().join("elsewhere"));
                std::fs::create_dir_all(&config.storage.path).unwrap();
                std::fs::create_dir_all(temp.path().join("elsewhere")).unwrap();

                keep_state(&config, state);
                keep_archive(&config, archive);

                if let Some(blocks) = config.storage.blocks_path() {
                    written_segment(&blocks);
                }

                let outside = outside(&config).unwrap();

                let message = match inspect_existing_data(&config, force()) {
                    Err(Error::Message(message)) => message,
                    other => panic!("{state:?} {archive:?} not refused: {other:?}"),
                };

                assert!(
                    message.contains(&format!(
                        "{}: data outside {}, the storage root `--force` clears",
                        outside.display(),
                        config.storage.path.display()
                    )),
                    "{message}"
                );
                assert!(message.contains("by hand"), "{message}");
                assert!(message.contains("no compatibility identity"), "{message}");
                assert!(!message.contains("fails to open"), "{message}");

                if outside.is_dir() {
                    std::fs::remove_dir_all(&outside).unwrap();
                } else {
                    std::fs::remove_file(&outside).unwrap();
                }

                let inspected = inspect_existing_data(&config, force()).unwrap();
                assert!(!outside.exists(), "{}", outside.display());

                assert_eq!(inspected, Existing::Clear, "{state:?} {archive:?}");
            }
        }
    }

    /// Over stores this release opens, `--force` refuses the same way while
    /// data outside the root, a WAL included, would outlive the clear: the
    /// node it leaves wouldn't start. Once that data is gone, it clears.
    #[test]
    fn force_names_current_data_outside_the_root() {
        let wal: (Edit, Outside) = (
            |config, elsewhere| config.sync.wal_path = Some(elsewhere.join("wal")),
            |config| config.wal_path(),
        );

        for (edit, outside) in outside_layouts().into_iter().chain([wal]) {
            let temp = tempfile::tempdir().unwrap();
            let mut config = config_over(&temp.path().join("data"));
            edit(&mut config, &temp.path().join("elsewhere"));
            std::fs::create_dir_all(&config.storage.path).unwrap();
            std::fs::create_dir_all(temp.path().join("elsewhere")).unwrap();

            keep_state(&config, Kept::Current);
            keep_archive(&config, Kept::Current);
            written_wal(&config);

            if let Some(blocks) = config.storage.blocks_path() {
                written_segment(&blocks);
            }

            let outside = outside(&config).unwrap();

            let message = match inspect_existing_data(&config, force()) {
                Err(Error::Message(message)) => message,
                other => panic!("{} not refused: {other:?}", outside.display()),
            };

            assert!(message.contains("existing data detected"), "{message}");
            assert!(
                message.contains(&format!(
                    "{}: data outside {}, the storage root `--force` clears",
                    outside.display(),
                    config.storage.path.display()
                )),
                "{message}"
            );

            if outside.is_dir() {
                std::fs::remove_dir_all(&outside).unwrap();
            } else {
                std::fs::remove_file(&outside).unwrap();
            }

            assert_eq!(
                inspect_existing_data(&config, force()).unwrap(),
                Existing::Clear,
                "{}",
                outside.display()
            );
        }
    }

    /// A WAL another process holds can't be read, so it is in use rather
    /// than data: every flag but `--continue` refuses it, `--force` included,
    /// beside a refused store too. Once it is closed, it is data again.
    #[test]
    fn a_wal_another_process_holds_is_in_use() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        written_wal(&config);

        let held = open_wal_store::<dolos_cardano::CardanoDelta>(&config).unwrap();

        for policy in every_policy() {
            let inspected = inspect_existing_data(&config, policy);

            if policy.r#continue {
                assert_eq!(inspected.unwrap(), Existing::Proceed, "{policy:?}");
            } else {
                let message = inspected.unwrap_err().to_string();
                assert!(message.contains("in use by another process"), "{message}");
            }
        }

        written_archive(&config, None);

        let message = inspect_existing_data(&config, force())
            .unwrap_err()
            .to_string();
        assert!(message.contains("no compatibility identity"), "{message}");
        assert!(
            message.contains("a WAL another process holds open"),
            "{message}"
        );

        drop(held);

        assert_eq!(
            inspect_existing_data(&config, force()).unwrap(),
            Existing::Clear
        );
    }

    /// A WAL that fails to read, one left unclean by a process killed while
    /// it held it or one that is no store at all, counts as data: outside
    /// the root, `--force` names it rather than leave it behind.
    #[test]
    fn force_names_an_unreadable_wal_outside_the_root() {
        let temp = tempfile::tempdir().unwrap();
        let elsewhere = temp.path().join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();

        let with_wal = |wal: PathBuf| {
            let mut config = config_over(&temp.path().join("data"));
            config.sync.wal_path = Some(wal);
            config
        };

        std::fs::write(elsewhere.join("garbage"), b"not a store").unwrap();

        #[allow(unused_mut)]
        let mut names = vec!["garbage"];

        // Windows locks a WAL redb holds against the copy that leaves it
        // unclean.
        #[cfg(unix)]
        {
            let held = with_wal(elsewhere.join("held"));
            let wal = open_wal_store::<dolos_cardano::CardanoDelta>(&held).unwrap();
            wal.reset_to(&ChainPoint::Origin).unwrap();
            std::fs::copy(elsewhere.join("held"), elsewhere.join("unclean")).unwrap();
            wal.shutdown().unwrap();

            names.push("unclean");
        }

        for name in names {
            let path = elsewhere.join(name);
            assert!(dolos_redb3::wal::probe(&path).is_err(), "{name}");

            let config = with_wal(path.clone());

            let message = inspect_existing_data(&config, force())
                .unwrap_err()
                .to_string();
            assert!(
                message.contains(&format!("{}: data outside", path.display())),
                "{message}"
            );

            assert!(inspect_existing_data(&config, ExistingDataPolicy::default()).is_err());
        }
    }

    /// A path reaches into a directory however it is written: as it, under
    /// it, or through `..`; and, where the paths exist, through a link to it,
    /// a link inside it, or a link followed by `..`.
    #[test]
    fn a_path_reaches_into_a_directory_however_it_is_written() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");

        for (path, reaches) in [
            (root.clone(), true),
            (root.join("node-db"), true),
            (root.join("..").join("data").join("db"), true),
            (temp.path().to_path_buf(), false),
            (temp.path().join("elsewhere"), false),
            (temp.path().join("data-db"), false),
        ] {
            assert_eq!(
                reaches_into(&path, &root).unwrap(),
                reaches,
                "{}",
                path.display()
            );
        }

        #[cfg(unix)]
        {
            let elsewhere = temp.path().join("elsewhere");
            std::fs::create_dir_all(root.join("sub")).unwrap();
            std::fs::create_dir_all(&elsewhere).unwrap();

            let to_root = temp.path().join("to-root");
            std::os::unix::fs::symlink(&root, &to_root).unwrap();
            assert!(reaches_into(&to_root.join("db"), &root).unwrap());

            let inside = root.join("node-db");
            std::os::unix::fs::symlink(&elsewhere, &inside).unwrap();
            assert!(reaches_into(&inside, &root).unwrap());
            assert!(!reaches_into(&elsewhere, &root).unwrap());

            let to_sub = temp.path().join("to-sub");
            std::os::unix::fs::symlink(root.join("sub"), &to_sub).unwrap();
            assert!(reaches_into(&to_sub.join("..").join("node-db"), &root).unwrap());
            assert!(reaches_into(&to_sub.join("..").join("missing"), &root).unwrap());
        }

        #[cfg(target_os = "macos")]
        {
            let resolved = std::fs::canonicalize(&root).unwrap();
            let alias = Path::new("/System/Volumes/Data").join(resolved.strip_prefix("/").unwrap());

            if alias.is_dir() {
                assert!(reaches_into(&alias.join("node-db"), &root).unwrap());
            }
        }
    }

    /// A link's relative target is read from the link's own directory, as
    /// the OS reads it, not from the working directory.
    #[cfg(unix)]
    #[test]
    fn a_relative_link_is_followed_from_its_own_directory() {
        let temp = tempfile::tempdir().unwrap();
        let root = temp.path().join("data");
        std::fs::create_dir_all(root.join("stele")).unwrap();
        std::fs::create_dir_all(temp.path().join("srv")).unwrap();

        let link = temp.path().join("srv").join("stele");
        std::os::unix::fs::symlink(Path::new("..").join("data").join("stele"), &link).unwrap();

        assert!(reaches_into(&link, &root).unwrap());
        assert!(reaches_into(&link.join("blobs"), &root).unwrap());
    }

    /// A path through a directory not made yet and back out of it with `..`
    /// lands where the OS makes it, past the `..`, as `create_dir_all` takes
    /// it.
    #[test]
    fn a_path_through_a_directory_not_made_yet_lands_past_it() {
        let temp = tempfile::tempdir().unwrap();
        let db = temp.path().join("db");
        std::fs::create_dir_all(&db).unwrap();

        let missing = temp.path().join("missing");

        for (path, reaches) in [
            (missing.join("..").join("db").join("state"), true),
            (
                missing.join("deeper").join("..").join("..").join("db"),
                true,
            ),
            (missing.join("..").join("elsewhere").join("state"), false),
            (missing.join("db").join(".."), false),
        ] {
            assert_eq!(
                reaches_into(&path, &db).unwrap(),
                reaches,
                "{}",
                path.display()
            );
        }

        std::fs::create_dir_all(missing.join("..").join("db").join("state")).unwrap();
        assert!(db.join("state").is_dir());
    }

    /// A directory that can't be listed stops the whole removal of what
    /// holds it, which names it, and is passed over by a removal read as far
    /// as it can be.
    #[cfg(unix)]
    #[test]
    fn a_directory_that_cant_be_listed_is_named_or_passed_over() {
        use std::os::unix::fs::PermissionsExt;

        let temp = tempfile::tempdir().unwrap();
        let db = temp.path().join("db");
        let locked = db.join("lost+found");
        std::fs::create_dir_all(locked.join("inner")).unwrap();

        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let listed = std::fs::read_dir(&locked).is_ok();
        let whole = Removal::of(&db).err().map(|error| error.to_string());
        let readable = Removal::of_readable(&db).and_then(|removal| removal.takes(&locked));
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();

        // Whoever lists it anyway, as root does, has nothing to pass over.
        if listed {
            return;
        }

        let message = whole.expect("the directory was listed");
        assert!(message.contains(&locked.display().to_string()), "{message}");
        assert!(readable.unwrap());
    }

    /// Beside a refused store, a directory outside the root that holds no
    /// data doesn't stop `--force`: an empty one, a mount point's, or one
    /// holding an empty store this release opens.
    #[test]
    fn force_passes_over_outside_directories_without_data() {
        for (edit, outside) in outside_layouts() {
            for fill in ["empty", "mount point", "empty store"] {
                let temp = tempfile::tempdir().unwrap();
                let mut config = config_over(&temp.path().join("data"));
                edit(&mut config, &temp.path().join("elsewhere"));

                let outside = outside(&config).unwrap();
                let state_outside = config.storage.state_path() == Some(outside.clone());
                std::fs::create_dir_all(&outside).unwrap();

                match fill {
                    "mount point" => std::fs::create_dir(outside.join("lost+found")).unwrap(),
                    "empty store" if state_outside => {
                        open_state_store(&config).unwrap().shutdown().unwrap()
                    }
                    "empty store" => open_archive_store(&config).unwrap().shutdown().unwrap(),
                    _ => {}
                }

                if state_outside {
                    written_archive(&config, None);
                } else {
                    written_state(&config, None);
                }

                assert_eq!(
                    inspect_existing_data(&config, force()).unwrap(),
                    Existing::Clear,
                    "{fill} {}",
                    outside.display()
                );
            }
        }
    }

    /// Block segments outside the root are named on their own, without the
    /// index that located them: at a `blocks_path` whose archive directory is
    /// gone, or in an archive directory holding no index. So a new archive
    /// isn't appended to an old one's segments.
    #[test]
    fn force_names_segments_outside_the_root_without_their_index() {
        let layouts: [Edit; 2] = [
            |config, segments| config.storage.archive.blocks_path = Some(segments.into()),
            |config, segments| config.storage.archive.path = Some(segments.into()),
        ];

        for edit in layouts {
            let temp = tempfile::tempdir().unwrap();
            let mut config = config_over(&temp.path().join("data"));
            let segments = temp.path().join("segments");
            edit(&mut config, &segments);

            written_state(&config, None);
            written_segment(&segments);
            assert!(!config.storage.path.join("archive").exists());
            assert!(!segments.join("index").exists());

            let message = match inspect_existing_data(&config, force()) {
                Err(Error::Message(message)) => message,
                other => panic!("not refused naming the segments: {other:?}"),
            };

            assert!(
                message.contains(&format!("{}: data outside", segments.display())),
                "{message}"
            );

            std::fs::remove_file(segments.join("000000.segment")).unwrap();

            assert_eq!(
                inspect_existing_data(&config, force()).unwrap(),
                Existing::Clear
            );
        }
    }

    /// Every directory outside the root holding data is named in the one
    /// message, so removing the first named doesn't let `--force` clear over
    /// the rest.
    #[test]
    fn force_names_every_outside_directory_holding_data_at_once() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config_over(&temp.path().join("data"));
        let archive = temp.path().join("archive");
        let segments = temp.path().join("segments");
        config.storage.archive.path = Some(archive.clone());
        config.storage.archive.blocks_path = Some(segments.clone());

        written_state(&config, current().as_deref());
        written_archive(&config, None);
        written_segment(&segments);

        let message = match inspect_existing_data(&config, force()) {
            Err(Error::Message(message)) => message,
            other => panic!("not refused naming the outside directories: {other:?}"),
        };

        for outside in [&archive, &segments] {
            assert!(
                message.contains(&format!("{}: data outside", outside.display())),
                "{message}"
            );
        }
    }

    /// What can't be read counts as data, so beside a refused store `--force`
    /// names a directory outside the root it can't look into: an archive that
    /// fails to open, as one in use does, a store whose rows fail to read, or
    /// a `blocks_path` it can't list.
    #[test]
    fn force_names_outside_data_it_cannot_read() {
        let temp = tempfile::tempdir().unwrap();
        let elsewhere = temp.path();

        // An archive held open, as a running node holds it, beside a refused
        // state.
        let mut held = config_over(&elsewhere.join("held/data"));
        held.storage.archive.path = Some(elsewhere.join("held/archive"));
        written_state(&held, None);
        let _running = open_archive_store(&held).unwrap();

        let mut unreadable_archive = config_over(&elsewhere.join("archive/data"));
        unreadable_archive.storage.archive.path = Some(elsewhere.join("archive/archive"));
        written_state(&unreadable_archive, None);
        written_unreadable_archive(&unreadable_archive);
        assert!(open_archive_index(&unreadable_archive)
            .unwrap()
            .holds_data()
            .is_err());

        let mut unreadable_state = config_over(&elsewhere.join("state/data"));
        unreadable_state.storage.state.path = Some(elsewhere.join("state/state"));
        written_unreadable_state(&unreadable_state);
        written_archive(&unreadable_state, None);

        for (config, outside) in [
            (&held, held.storage.archive_path()),
            (
                &unreadable_archive,
                unreadable_archive.storage.archive_path(),
            ),
            (&unreadable_state, unreadable_state.storage.state_path()),
        ] {
            let outside = outside.unwrap();
            assert!(!holds_segments(&outside), "{}", outside.display());

            let message = match inspect_existing_data(config, force()) {
                Err(Error::Message(message)) => message,
                other => panic!("{} not named: {other:?}", outside.display()),
            };

            assert!(
                message.contains(&format!("{}: data outside", outside.display())),
                "{message}"
            );
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let mut config = config_over(&elsewhere.join("unlisted/data"));
            let segments = elsewhere.join("unlisted/segments");
            config.storage.archive.blocks_path = Some(segments.clone());
            written_state(&config, None);
            written_segment(&segments);

            let mode =
                |mode| std::fs::set_permissions(&segments, std::fs::Permissions::from_mode(mode));
            mode(0o000).unwrap();

            // Permissions don't bind a superuser, who lists it anyway.
            if std::fs::read_dir(&segments).is_err() {
                let inspected = inspect_existing_data(&config, force());
                mode(0o755).unwrap();

                let message = match inspected {
                    Err(Error::Message(message)) => message,
                    other => panic!("{} not named: {other:?}", segments.display()),
                };

                assert!(
                    message.contains(&format!("{}: data outside", segments.display())),
                    "{message}"
                );
            } else {
                mode(0o755).unwrap();
            }
        }
    }

    /// A state that opens but whose rows fail to read counts as data, so the
    /// flags decide about it as about any data without a node, and the
    /// failure is left to the command that uses the state.
    #[test]
    fn a_state_whose_rows_fail_to_read_is_left_to_the_command() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        written_unreadable_state(&config);
        assert!(open_state_store(&config).unwrap().holds_data().is_err());

        for policy in every_policy() {
            let inspected = inspect_existing_data(&config, policy);

            if policy.r#continue {
                assert_eq!(inspected.unwrap(), Existing::Proceed, "{policy:?}");
            } else if policy.skip_if_data {
                let message = inspected.unwrap_err().to_string();
                assert!(message.contains("no node"), "{message}");
            } else if policy.force {
                assert_eq!(inspected.unwrap(), Existing::Clear, "{policy:?}");
            } else {
                assert!(inspected.is_err(), "{policy:?}");
            }
        }

        assert!(!has_existing_data(&config).unwrap());
    }

    /// A state whose cursor fails to read can't be told from a node, so it is
    /// taken as one that fails to open: left to `--continue`, it stops every
    /// other flag, and beside a refused archive `--force` names it for removal
    /// by hand.
    #[test]
    fn a_state_whose_cursor_fails_to_read_is_left_to_a_resumed_run() {
        let temp = tempfile::tempdir().unwrap();
        let config = config_over(&temp.path().join("data"));
        written_state_with_unreadable_cursor(&config);
        let error: Error = open_state_store(&config)
            .unwrap()
            .read_cursor()
            .unwrap_err()
            .into();

        for policy in every_policy() {
            let inspected = inspect_existing_data(&config, policy);

            if policy.r#continue {
                assert_eq!(inspected.unwrap(), Existing::Proceed, "{policy:?}");
            } else {
                assert!(inspected.is_err(), "{policy:?}");
            }
        }

        assert!(has_existing_data(&config).is_err());

        written_archive(&config, None);

        let message = match inspect_existing_data(&config, force()) {
            Err(Error::Message(message)) => message,
            other => panic!("the state not named: {other:?}"),
        };

        assert!(message.contains("the archive store"), "{message}");
        assert_eq!(
            message,
            would_leave(&config, &[unreadable_state_line(&config, error)])
        );
    }

    /// With history off the archive plays no part: a leftover one, refused
    /// or outside the root, or segments at a `blocks_path` outside it, change
    /// no decision about a node or a refused state.
    #[test]
    fn inspecting_storage_without_history_passes_over_the_archive() {
        let temp = tempfile::tempdir().unwrap();
        let mut config = config_over(&temp.path().join("data"));
        config.history = HistoryConfig::none();
        written_state(&config, current().as_deref());
        written_archive(&config, None);

        for policy in every_policy() {
            let inspected = inspect_existing_data(&config, policy);

            if policy.r#continue {
                assert_eq!(inspected.unwrap(), Existing::Proceed, "{policy:?}");
            } else if policy.skip_if_data {
                assert_eq!(inspected.unwrap(), Existing::Skip, "{policy:?}");
            } else if policy.force {
                assert_eq!(inspected.unwrap(), Existing::Clear, "{policy:?}");
            } else {
                assert!(matches!(inspected, Err(Error::Message(_))), "{policy:?}");
            }
        }

        assert!(has_existing_data(&config).unwrap());

        let temp = tempfile::tempdir().unwrap();
        let mut config = config_over(&temp.path().join("data"));
        config.history = HistoryConfig::none();
        config.storage.archive.path = Some(temp.path().join("archive"));
        config.storage.archive.blocks_path = Some(temp.path().join("segments"));
        written_state(&config, None);
        written_archive(&config, None);
        written_segment(&temp.path().join("segments"));

        assert_eq!(
            inspect_existing_data(&config, force()).unwrap(),
            Existing::Clear
        );
    }

    /// A state that fails to open for a reason other than its identity — an
    /// interrupted unpack, or a store a running node holds — is left to a
    /// resumed run, as before bootstrap read identities, and stops every
    /// other flag, `--force` included. Beside a refused archive, `--force`
    /// with another flag is told it clears only on its own, and `--force` on
    /// its own names the state for removal by hand, with any data outside the
    /// root, then clears once they are gone.
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

        let mut config = config;
        let segments = elsewhere.path().join("segments");
        config.storage.archive.blocks_path = Some(segments.clone());
        written_archive(&config, None);
        written_segment(&segments);

        let state = config.storage.state_path().unwrap();
        let expected = would_leave(
            &config,
            &[
                unreadable_state_line(&config, open_state_store(&config).err().unwrap()),
                format!(
                    "  {}: data outside {}, the storage root `--force` clears",
                    segments.display(),
                    config.storage.path.display()
                ),
            ],
        );

        for policy in every_policy() {
            let message = match inspect_existing_data(&config, policy) {
                Err(Error::Message(message)) => message,
                inspected => {
                    assert!(!policy.force, "{policy:?}");
                    assert!(refusal(inspected).is_some(), "{policy:?}");
                    continue;
                }
            };

            if policy.skip_if_data || policy.r#continue {
                assert!(message.contains("only on its own"), "{message}");
                assert!(!message.contains("fails to open"), "{message}");
            } else {
                assert!(message.contains("the archive store"), "{message}");
                assert_eq!(message, expected);
            }
        }

        std::fs::remove_file(&state).unwrap();
        std::fs::remove_dir_all(&segments).unwrap();

        assert_eq!(
            inspect_existing_data(&config, force()).unwrap(),
            Existing::Clear
        );
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

    /// Telling whether a path lies outside the storage root reads the working
    /// directory only for a relative path, so absolute paths are placed from
    /// one that is gone. The check runs again in a child process started in a
    /// deleted directory, since the working directory is the whole process's.
    #[cfg(unix)]
    #[test]
    fn absolute_paths_are_placed_without_a_working_directory() {
        const CHILD: &str = "DOLOS_TEST_DELETED_WORKING_DIRECTORY";

        if std::env::var_os(CHILD).is_some() {
            let config = config_over(Path::new("/storage"));
            let paths = ["/storage/state", "/elsewhere/archive"].map(|path| Some(path.into()));

            let outside = outside_storage_root(&config, paths).unwrap();
            assert_eq!(outside, [PathBuf::from("/elsewhere/archive")]);
            assert!(outside_storage_root(&config, [Some("state".into())]).is_err());
            return;
        }

        let (_, module) = module_path!().split_once("::").unwrap();
        let name = format!("{module}::absolute_paths_are_placed_without_a_working_directory");

        let temp = tempfile::tempdir().unwrap();
        let gone = temp.path().join("gone");
        std::fs::create_dir(&gone).unwrap();

        let output = std::process::Command::new("sh")
            .args(["-c", r#"cd "$1" && rmdir "$1" && shift && exec "$@""#, "sh"])
            .arg(&gone)
            .arg(std::env::current_exe().unwrap())
            .args(["--exact", &name])
            .env(CHILD, "1")
            .output()
            .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(output.status.success(), "{stdout}");
        assert!(stdout.contains("1 passed"), "{stdout}");
        assert!(!gone.exists());
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

    /// Finding T2 of the first QA review of dolos#1511: a replay through the
    /// `LogsOnly` gate re-runs every seal, so the gate passes entity index
    /// writes and reads through rather than discarding them as it does
    /// block appends.
    #[test]
    fn the_logs_only_gate_passes_the_entity_index_through() {
        let store = ArchiveStoreBackend::in_memory(StateSchema::default()).unwrap();
        let gate = store.logs_only().unwrap();

        let kind = EntityKind::new("toy", 4);
        let id: &[u8] = &[7; 4];

        let writer = gate.start_writer().unwrap();
        writer.write_entity(kind, id, 10).unwrap();
        writer.commit().unwrap();

        assert_eq!(store.entity_slots(kind, &[id]).unwrap(), [Some(10)]);
        assert_eq!(gate.entity_slots(kind, &[id]).unwrap(), [Some(10)]);

        let records = gate
            .iter_entity_records(&[kind], 0..u64::MAX)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();

        assert_eq!(records, [EntityRecord::new(kind, id, 10).unwrap()]);
    }

    const DELETED_NS: &str = "deleted-logs";

    fn deleted_schema() -> StateSchema {
        let mut schema = StateSchema::default();
        schema.insert(DELETED_NS, dolos_core::NamespaceType::KeyValue);
        schema
    }

    /// A row written to `store` and deleted through `via`'s writer is gone.
    fn deleted_through(store: &ArchiveStoreBackend, via: &ArchiveStoreBackend, entity: u8) {
        let key = LogKey::of(10, &[entity; 32]);

        let writer = store.start_writer().unwrap();
        writer
            .write_log(DELETED_NS, &key, &b"row".to_vec())
            .unwrap();
        writer.commit().unwrap();

        assert_eq!(
            store.read_logs(DELETED_NS, &[&key]).unwrap(),
            [Some(b"row".to_vec())]
        );

        let writer = via.start_writer().unwrap();
        writer.delete_log(DELETED_NS, &key).unwrap();
        writer.commit().unwrap();

        assert_eq!(store.read_logs(DELETED_NS, &[&key]).unwrap(), [None]);
    }

    /// Finding TA8 of the third QA review of dolos#1524: `rebuild-state
    /// --rewrite-logs` seals through the `LogsOnly` gate, and its rewrites
    /// delete stale first-mint chunks, so the gate passes deletes through.
    #[test]
    fn the_logs_only_gate_passes_log_deletes_through() {
        let store = ArchiveStoreBackend::in_memory(deleted_schema()).unwrap();
        let gate = store.logs_only().unwrap();

        deleted_through(&store, &gate, 1);
    }

    /// The node's archive deletes a log row, directly and through the gate.
    #[test]
    fn the_fjall_archive_deletes_a_log_row() {
        let temp = tempfile::tempdir().unwrap();
        let store = ArchiveStoreBackend::open_fjall(
            temp.path(),
            deleted_schema(),
            None,
            dolos_fjall::Preset::Standard,
        )
        .unwrap();

        deleted_through(&store, &store, 1);

        let gate = store.logs_only().unwrap();
        deleted_through(&store, &gate, 2);
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
