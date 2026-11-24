use bincode;
use itertools::Itertools;
use redb::{Range, ReadableTableMetadata, TableDefinition};
use serde::{de::DeserializeOwned, Serialize};
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
    path::Path,
    sync::Arc,
};
use thiserror::Error;
use tracing::{debug, event_enabled, info, trace, warn, Level};

use dolos_core::{
    config::RedbWalConfig, BlockSlot, ChainPoint, EntityDelta, LogEntry, LogValue, RawBlock,
    WalError, WalStore,
};

use crate::{
    handle::Handle,
    layout::{check_layout, is_corrupt},
};

#[derive(Debug, Error)]
#[error(transparent)]
pub struct RedbWalError(#[from] WalError);

impl From<redb::Error> for RedbWalError {
    fn from(value: redb::Error) -> Self {
        Self(WalError::internal(value))
    }
}

impl From<RedbWalError> for WalError {
    fn from(value: RedbWalError) -> Self {
        value.0
    }
}

impl From<::redb::DatabaseError> for RedbWalError {
    fn from(value: ::redb::DatabaseError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::TableError> for RedbWalError {
    fn from(value: ::redb::TableError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::CommitError> for RedbWalError {
    fn from(value: ::redb::CommitError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::StorageError> for RedbWalError {
    fn from(value: ::redb::StorageError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

impl From<::redb::TransactionError> for RedbWalError {
    fn from(value: ::redb::TransactionError) -> Self {
        Self(WalError::internal(Box::new(::redb::Error::from(value))))
    }
}

pub type AugmentedBlockSlot = i128;
pub type DbLogValue = Vec<u8>;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct DbChainPoint([u8; 40]);

impl DbChainPoint {
    /// Smallest key at `slot`: big-endian slot bytes + zero hash. Big-endian is
    /// required so redb's lexicographic key order matches slot order (see
    /// `ChainPoint::into_bytes`).
    fn min_bound(slot: BlockSlot) -> DbChainPoint {
        let mut point = [0u8; 40];
        point[0..8].copy_from_slice(&slot.to_be_bytes());
        DbChainPoint(point)
    }

    /// Largest key at `slot`: big-endian slot bytes + all-ones hash.
    fn max_bound(slot: BlockSlot) -> DbChainPoint {
        let mut point = [255u8; 40];
        point[0..8].copy_from_slice(&slot.to_be_bytes());
        DbChainPoint(point)
    }

    pub fn slot_range(range: impl RangeBounds<BlockSlot>) -> impl RangeBounds<DbChainPoint> {
        use std::ops::Bound;

        // Map bounds to key bounds directly; no `+1`/`-1` shift (would overflow
        // at BlockSlot::MAX / underflow at MIN). Excluded flips the hash extreme.
        let start = match range.start_bound() {
            Bound::Included(x) => Bound::Included(DbChainPoint::min_bound(*x)),
            Bound::Excluded(x) => Bound::Excluded(DbChainPoint::max_bound(*x)),
            Bound::Unbounded => Bound::Unbounded,
        };

        let end = match range.end_bound() {
            Bound::Included(x) => Bound::Included(DbChainPoint::max_bound(*x)),
            Bound::Excluded(x) => Bound::Excluded(DbChainPoint::min_bound(*x)),
            Bound::Unbounded => Bound::Unbounded,
        };

        DbChainPointRange { start, end }
    }
}

impl redb::Value for DbChainPoint {
    type SelfType<'a>
        = Self
    where
        Self: 'a;

    type AsBytes<'a>
        = &'a [u8; 40]
    where
        Self: 'a;

    fn fixed_width() -> Option<usize> {
        Some(40)
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        let inner = <[u8; 40]>::try_from(data).unwrap();
        Self(inner)
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'b,
    {
        &value.0
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("chainpoint")
    }
}

impl redb::Key for DbChainPoint {
    fn compare(data1: &[u8], data2: &[u8]) -> std::cmp::Ordering {
        data1.cmp(data2)
    }
}

impl From<ChainPoint> for DbChainPoint {
    fn from(value: ChainPoint) -> Self {
        DbChainPoint(value.into_bytes())
    }
}

impl From<DbChainPoint> for ChainPoint {
    fn from(value: DbChainPoint) -> Self {
        ChainPoint::from_bytes(value.0)
    }
}

pub struct DbChainPointRange {
    start: std::ops::Bound<DbChainPoint>,
    end: std::ops::Bound<DbChainPoint>,
}

impl std::ops::RangeBounds<DbChainPoint> for DbChainPointRange {
    fn start_bound(&self) -> std::ops::Bound<&DbChainPoint> {
        self.start.as_ref()
    }

    fn end_bound(&self) -> std::ops::Bound<&DbChainPoint> {
        self.end.as_ref()
    }
}

fn to_raw_log_value<T>(value: &LogValue<T>) -> DbLogValue
where
    T: EntityDelta,
    T: Serialize,
{
    bincode::serialize(value).unwrap()
}

fn from_raw_log_value<T>(value: &DbLogValue) -> LogValue<T>
where
    T: EntityDelta,
    T: DeserializeOwned,
{
    bincode::deserialize(value).unwrap()
}

const WAL: TableDefinition<DbChainPoint, DbLogValue> = TableDefinition::new("wal");

const WAL_METADATA: TableDefinition<&str, u32> = TableDefinition::new("wal_metadata");

const WAL_METADATA_VERSION_KEY: &str = "version";

/// What [`probe`] found in a WAL file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalProbe {
    Empty,
    Entries,
    /// Another process holds the file open, so what it holds can't be read.
    InUse,
}

/// Whether the WAL file at `path` holds an entry, read without writing to it.
///
/// Opening the file as a [`RedbWalStore`] stamps its version, and resets one
/// this release can't read, so a caller deciding whether to touch the WAL at
/// all reads it here instead. A file that needs repair is an error.
pub fn probe(path: impl AsRef<Path>) -> Result<WalProbe, RedbWalError> {
    use redb::ReadableDatabase as _;

    let db = match redb::ReadOnlyDatabase::open(path) {
        Ok(db) => db,
        Err(redb::DatabaseError::DatabaseAlreadyOpen) => return Ok(WalProbe::InUse),
        Err(error) => return Err(error.into()),
    };

    let rx = db.begin_read()?;

    match rx.open_table(WAL) {
        Ok(table) if table.is_empty()? => Ok(WalProbe::Empty),
        Ok(_) => Ok(WalProbe::Entries),
        Err(redb::TableError::TableDoesNotExist(_)) => Ok(WalProbe::Empty),
        Err(error) => Err(error.into()),
    }
}

/// Current WAL on-disk schema version.
///
/// Bump this whenever the serialized format of WAL entries changes in a way
/// that breaks reading older data. The WAL is transient: a writable open
/// wipes the entries of a WAL recording any other version, older or newer,
/// or none, unless a store still needs one of them (see
/// [`RedbWalStore::open_replaying`]).
///
/// 2: deltas carry typed, variable-length entity keys (decision 0051).
/// 3: `EpochStatsUpdate` and `ScriptFirstSeen` drop the fields nothing reads.
/// 4: inputs carry each UTxO's creation position beside its output.
/// 5: staged asset mints carry whether their tx's CIP-25 metadata names the
///    asset.
/// 6: the DRep and pool deltas drop the fields minibf now derives, and
///    `DRepAnchorUpdate` and `DRepPowerUpdate` retire.
/// 7: `ScriptFirstSeen` retires.
/// 8: `MintStatsUpdate` and `MetadataTxUpdate` retire.
/// 9: the `Other` proposal action retires, and `proposed_in` is required.
/// 10: `DatumRefIncrement` records whether it filled a row's datum bytes.
pub const CURRENT_WAL_VERSION: u32 = 10;

/// What a WAL file holds, as [`RedbWalStore::inspect`] reads it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Found {
    /// Entries this release reads, or none, at [`CURRENT_WAL_VERSION`].
    Current,
    /// No entry, under another version or none.
    Empty,
    /// Entries under another version, or none.
    OtherVersion(Option<u32>),
}

/// The version `found` records, for a message.
fn recorded(found: Option<u32>) -> String {
    match found {
        Some(version) => format!("version {version}"),
        None => "no version".into(),
    }
}

/// Why this release can't read a WAL recording `found`, for a message.
fn other_version(found: Option<u32>) -> String {
    format!(
        "it records {}, and this release reads version {CURRENT_WAL_VERSION}",
        recorded(found)
    )
}

/// What a read-only view of a WAL file finds (see [`RedbWalStore::view`]).
enum Viewed<S> {
    /// Entries this release reads, at [`CURRENT_WAL_VERSION`].
    Readable(S),
    /// No entry.
    Empty,
    /// What this release can't read, and why; with the view, unless redb
    /// can't open the file.
    Unreadable(Option<S>, String),
}

/// Why [`RedbWalStore::admit`] did not admit the WAL as it is.
enum Unadmitted {
    /// What redb failed with; a corrupt WAL is reset.
    Failed(redb::Error),
    /// A store needs a block the WAL holds: [`WalError::Kept`].
    Kept(WalError),
}

impl From<redb::Error> for Unadmitted {
    fn from(error: redb::Error) -> Self {
        Self::Failed(error)
    }
}

impl From<Unadmitted> for WalError {
    fn from(unadmitted: Unadmitted) -> Self {
        match unadmitted {
            Unadmitted::Failed(error) => WalError::internal(error),
            Unadmitted::Kept(kept) => kept,
        }
    }
}

/// Whether the file at `path` is empty, as a crash during its creation leaves
/// it: a writable open initialises it, a read-only one fails on it.
fn is_empty_file(path: &Path) -> bool {
    std::fs::metadata(path).is_ok_and(|metadata| metadata.len() == 0)
}

/// How many of the newest entries an open decodes: those the pull stage's
/// intersect reads at every start, an exponential crawl that reads `2^n - 1`
/// entries for `n` candidates (five), with room for synthetic ones it skips.
const NEWEST_DECODED: usize = 64;

/// How much further from a slot each try of [`WalStore::locate_point`] looks
/// for an entry.
const LOCATE_STEP: u64 = 20;

/// The widest window [`WalStore::locate_point`] searches, on the last of
/// [`RedbWalStore::approximate_slot_with_retry`]'s nine tries.
const LOCATE_REACH: u64 = LOCATE_STEP * 9;

/// The entries a start replays into the other stores.
enum Replayed {
    Nothing,
    /// The entries from this key on.
    From(DbChainPoint),
    All,
}

/// What a start replays into stores at `positions`, locating each store's
/// start as `dolos_core::bootstrap` does: the entry nearest its slot, or
/// every entry for a store holding nothing or with no entry near its slot.
fn replayed(
    table: &redb::ReadOnlyTable<DbChainPoint, DbLogValue>,
    positions: &[Option<BlockSlot>],
) -> Result<Replayed, redb::Error> {
    let mut replayed = Replayed::Nothing;

    for &position in positions {
        let Some(slot) = position
            .map(|slot| nearest(table, slot))
            .transpose()?
            .flatten()
        else {
            return Ok(Replayed::All);
        };

        let start = DbChainPoint::min_bound(slot);

        replayed = match replayed {
            Replayed::From(current) => Replayed::From(current.min(start)),
            _ => Replayed::From(start),
        };
    }

    Ok(replayed)
}

/// Blocks a store still needs from the WAL, which a reset would lose. A
/// writable open keeps a WAL holding one instead of resetting it (see
/// [`RedbWalStore::open_replaying`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Needed {
    /// The slot of the first block needed.
    pub from: BlockSlot,
    /// The slot of the last block needed; none for every block from `from`.
    pub through: Option<BlockSlot>,
    /// Which store needs them, and why, as the refusal says it.
    pub by: String,
}

/// The slot of the entry nearest `slot`, as [`WalStore::locate_point`] finds
/// it: the first of the nearest within [`LOCATE_REACH`].
fn nearest(
    table: &redb::ReadOnlyTable<DbChainPoint, DbLogValue>,
    slot: BlockSlot,
) -> Result<Option<BlockSlot>, redb::Error> {
    let reach = slot.saturating_sub(LOCATE_REACH)..=slot.saturating_add(LOCATE_REACH);
    let mut nearest: Option<BlockSlot> = None;

    for entry in table.range(DbChainPoint::slot_range(reach))? {
        let found = ChainPoint::from(entry?.0.value()).slot();

        if nearest.is_none_or(|best| found.abs_diff(slot) < best.abs_diff(slot)) {
            nearest = Some(found);
        }
    }

    Ok(nearest)
}

pub struct LogIter<'a, T>(Range<'a, DbChainPoint, DbLogValue>, PhantomData<T>);

impl<'a, T> From<Range<'a, DbChainPoint, DbLogValue>> for LogIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    fn from(value: Range<'a, DbChainPoint, DbLogValue>) -> Self {
        Self(value, PhantomData)
    }
}

impl<T> Iterator for LogIter<'_, T>
where
    T: EntityDelta + DeserializeOwned,
{
    type Item = LogEntry<T>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
            .map(|(k, v)| (k.into(), from_raw_log_value(&v)))
    }
}

impl<T> DoubleEndedIterator for LogIter<'_, T>
where
    T: EntityDelta,
    T: DeserializeOwned,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|x| x.unwrap())
            .map(|(k, v)| (k.value(), v.value()))
            .map(|(k, v)| (k.into(), from_raw_log_value(&v)))
    }
}

pub struct BlockIter<'a, T>(LogIter<'a, T>);

impl<'a, T> Iterator for BlockIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    type Item = (ChainPoint, RawBlock);

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .next()
            .map(|(point, log)| (point, Arc::new(log.block)))
    }
}

impl<'a, T> DoubleEndedIterator for BlockIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        self.0
            .next_back()
            .map(|(point, log)| (point, Arc::new(log.block)))
    }
}

impl<'a, T> From<LogIter<'a, T>> for BlockIter<'a, T>
where
    T: EntityDelta + DeserializeOwned,
{
    fn from(value: LogIter<'a, T>) -> Self {
        Self(value)
    }
}

const DEFAULT_CACHE_SIZE_MB: usize = 50;

/// Concrete implementation of WalStore using Redb
#[derive(Clone, Debug)]
pub struct RedbWalStore<T> {
    db: Arc<Handle>,
    _phantom: PhantomData<T>,
}

impl<T> RedbWalStore<T>
where
    T: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Gracefully shutdown the WAL store.
    ///
    /// For Redb, this is a no-op since Redb handles cleanup automatically
    /// during drop without blocking issues.
    pub fn shutdown(&self) -> Result<(), RedbWalError> {
        Ok(())
    }

    pub fn is_empty(&self) -> Result<bool, RedbWalError> {
        // "Empty" here refers to absence of WAL data entries. The metadata
        // table is always present after `admit()` or `memory()`, so an empty WAL
        // can still have a populated `wal_metadata` table.
        let wr = self.db.begin_read()?;

        let tables = wr.list_tables()?;

        if tables.count() == 0 {
            return Ok(true);
        }

        let start = self.find_tip()?;

        if start.is_none() {
            return Ok(true);
        }

        Ok(false)
    }

    pub fn memory() -> Result<Self, WalError> {
        let db = redb::Database::builder()
            .create_with_backend(redb::backends::InMemoryBackend::new())
            .map_err(WalError::internal)?;

        let out = Self::over(Handle::Writable(db));

        out.ensure_initialized()
            .and_then(|()| out.stamp_version(CURRENT_WAL_VERSION))
            .map_err(WalError::internal)?;

        Ok(out)
    }

    /// Open the WAL at `path`, created if missing, at [`CURRENT_WAL_VERSION`].
    ///
    /// The WAL is transient, so one this release can't read is reset, never
    /// refused for its version, with one warning: the entries of a WAL
    /// recording another version, older or newer, or none, are wiped, and a
    /// file whose contents fail to decode is emptied. A lock, permission or
    /// I/O error fails, and leaves the file as it was. It decodes the first
    /// entry and the newest ones the intersect reads; [`Self::open_replaying`]
    /// adds those a start replays into the other stores, and keeps a WAL they
    /// still need.
    pub fn open(path: impl AsRef<Path>, config: &RedbWalConfig) -> Result<Self, WalError> {
        Self::open_replaying(path, config, &[], &[])
    }

    /// [`Self::open`] for a start that then catches up stores at `positions`
    /// (their slots, none for a store holding nothing) from the WAL: the
    /// entries such a replay reads are decoded too, so one that fails resets
    /// the WAL instead of panicking the replay.
    ///
    /// A WAL holding a block among those the stores still need, `needed`, is
    /// never reset: the open fails with [`WalError::Kept`] and leaves it as it
    /// is. So does a file redb can't open, which can't say what it holds,
    /// while any block is needed.
    pub fn open_replaying(
        path: impl AsRef<Path>,
        config: &RedbWalConfig,
        positions: &[Option<BlockSlot>],
        needed: &[Needed],
    ) -> Result<Self, WalError> {
        let path = path.as_ref();

        // Any writable open rewrites the file, so a WAL a read-only view can
        // read is kept before one; one needing a repair is decided after it.
        if !needed.is_empty() {
            if let Ok(Viewed::Unreadable(view, unreadable)) = Self::view(path, config, positions) {
                Self::keep_needed(view.as_ref(), path, needed, &unreadable)?;
            }
        }

        let (store, undecodable) = match Self::open_file(path, config) {
            Ok(out) => match out.admit(path, positions, needed) {
                Ok(()) => return Ok(out),
                Err(Unadmitted::Kept(kept)) => return Err(kept),
                Err(Unadmitted::Failed(error)) => (Some(out), error),
            },
            Err(error) => (None, error),
        };

        if !is_corrupt(&undecodable) {
            return Err(WalError::internal(undecodable));
        }

        Self::keep_needed(store.as_ref(), path, needed, &undecodable.to_string())?;

        // Dropped before the reset, so it no longer holds the file.
        drop(store);

        // Emptied in place, so a link to it and its permissions are kept.
        std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|error| {
                let reset = format!("resetting the WAL at {}: {error}", path.display());
                WalError::internal(std::io::Error::new(error.kind(), reset))
            })?;

        warn!(
            path = %path.display(),
            %undecodable,
            "the WAL fails to decode, and it is transient: it was reset empty"
        );

        let out = Self::reopen_emptied(path, config).map_err(WalError::internal)?;
        out.admit(path, positions, &[])?;

        Ok(out)
    }

    /// Open an existing WAL read-only (see [`Handle`]), writing nothing.
    ///
    /// A WAL this release can't read, as [`Self::open`] would reset it, is
    /// left as it is and read as an empty one in memory, with one warning. A
    /// file holding no database yet, as a crash during its creation leaves
    /// it, fails as redb reports it, as does a lock or I/O error.
    pub fn open_read_only(
        path: impl AsRef<Path>,
        config: &RedbWalConfig,
    ) -> Result<Self, WalError> {
        Self::open_read_only_replaying(path, config, &[], &[])
    }

    /// [`Self::open_read_only`] for a command that reads what a start would
    /// replay into stores at `positions`: it decodes the entries
    /// [`Self::open_replaying`] decodes, so it reads as empty a WAL that open
    /// would reset, or would keep for a block among `needed`, as its warning
    /// says.
    pub fn open_read_only_replaying(
        path: impl AsRef<Path>,
        config: &RedbWalConfig,
        positions: &[Option<BlockSlot>],
        needed: &[Needed],
    ) -> Result<Self, WalError> {
        let path = path.as_ref();

        let (view, unreadable) = match Self::view(path, config, positions)? {
            Viewed::Readable(out) => return Ok(out),
            Viewed::Empty => return Self::memory(),
            Viewed::Unreadable(view, unreadable) => (view, unreadable),
        };

        match Self::held_needed(view.as_ref(), needed) {
            None => warn!(
                path = %path.display(),
                reason = %unreadable,
                "this release can't read the WAL, so this command reads it as empty and leaves \
                 it as it is; the WAL is transient, and the next writable open resets it"
            ),
            Some(needed) => warn!(
                path = %path.display(),
                reason = %unreadable,
                %needed,
                "this release can't read the WAL, so this command reads it as empty and leaves \
                 it as it is; a store still needs what it holds, so a writable open keeps it and \
                 stops"
            ),
        }

        Self::memory()
    }

    /// What a read-only view of the WAL at `path` finds, decoding what
    /// [`Self::inspect`] decodes for stores at `positions`, writing nothing. A
    /// file holding no database yet, as a crash during its creation leaves
    /// it, fails as redb reports it, as does a lock or I/O error.
    fn view(
        path: &Path,
        config: &RedbWalConfig,
        positions: &[Option<BlockSlot>],
    ) -> Result<Viewed<Self>, WalError> {
        match Self::view_file(path, config) {
            Ok(out) => match out.inspect(positions) {
                Ok(Found::Current) => Ok(Viewed::Readable(out)),
                Ok(Found::Empty) => Ok(Viewed::Empty),
                Ok(Found::OtherVersion(found)) => {
                    Ok(Viewed::Unreadable(Some(out), other_version(found)))
                }
                Err(error) if is_corrupt(&error) => {
                    Ok(Viewed::Unreadable(Some(out), error.to_string()))
                }
                Err(error) => Err(WalError::internal(error)),
            },
            Err(error) if is_corrupt(&error) && !is_empty_file(path) => {
                Ok(Viewed::Unreadable(None, error.to_string()))
            }
            Err(error) => Err(WalError::internal(error)),
        }
    }

    /// Fail with [`WalError::Kept`] if the WAL at `path`, which this release
    /// can't read for `unreadable`, holds a block among `needed`; see
    /// [`Self::held_needed`].
    fn keep_needed(
        wal: Option<&Self>,
        path: &Path,
        needed: &[Needed],
        unreadable: &str,
    ) -> Result<(), WalError> {
        match Self::held_needed(wal, needed) {
            None => Ok(()),
            Some(needed) => Err(WalError::Kept {
                path: path.to_path_buf(),
                unreadable: unreadable.to_string(),
                needed,
            }),
        }
    }

    /// What `wal` holds of `needed`, as a refusal says it: the first block a
    /// store needs, or that it may hold one when its entries can't be read
    /// (`wal` is none for a file redb can't open). None when it holds none.
    fn held_needed(wal: Option<&Self>, needed: &[Needed]) -> Option<String> {
        needed.iter().find_map(|needed| {
            let held = match wal.map(|wal| wal.first_block_in(needed)) {
                Some(Ok(None)) => return None,
                Some(Ok(Some(slot))) => {
                    format!("the WAL holds the block at slot {slot}, which a reset would lose")
                }
                Some(Err(_)) | None => "the WAL may hold a block a reset would lose".to_string(),
            };

            Some(format!("{}: {held}", needed.by))
        })
    }

    /// The slot of the first block the WAL holds among `needed`'s slots. A
    /// reset point holds none; an entry of any other bytes counts as a block,
    /// whichever release wrote it.
    fn first_block_in(&self, needed: &Needed) -> Result<Option<BlockSlot>, redb::Error> {
        let rx = self.db.begin_read()?;

        let table = match rx.open_table(WAL) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };

        // The same bytes under every version: its delta list is empty.
        let reset_point = to_raw_log_value(&LogValue::<T>::origin());

        let through = needed.through.map_or(Bound::Unbounded, Bound::Included);
        let slots = (Bound::Included(needed.from), through);

        for entry in table.range(DbChainPoint::slot_range(slots))? {
            let (point, value) = entry?;

            if value.value() != reset_point {
                return Ok(Some(ChainPoint::from(point.value()).slot()));
            }
        }

        Ok(None)
    }

    fn over(db: Handle) -> Self {
        Self {
            db: Arc::new(db),
            _phantom: Default::default(),
        }
    }

    /// The database at `path`, created if missing. [`check_layout`] first
    /// fails a cut-short file as corrupt, which redb would panic on.
    fn open_file(path: &Path, config: &RedbWalConfig) -> Result<Self, redb::Error> {
        Self::create_file(path, config, |x| {
            warn!(progress = x.progress() * 100f64, "wal db is repairing")
        })
    }

    /// [`Self::open_file`] over the file a reset just emptied. redb repairs
    /// every file it initialises, which here recovers nothing, so it is not
    /// warned of.
    fn reopen_emptied(path: &Path, config: &RedbWalConfig) -> Result<Self, redb::Error> {
        Self::create_file(path, config, |x| {
            debug!(progress = x.progress() * 100f64, "wal db is initializing")
        })
    }

    fn create_file(
        path: &Path,
        config: &RedbWalConfig,
        repairing: impl Fn(&mut redb::RepairSession) + 'static,
    ) -> Result<Self, redb::Error> {
        check_layout(path)?;

        let db = redb::Database::builder()
            .set_repair_callback(repairing)
            .set_cache_size(1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB))
            .create(path)?;

        Ok(Self::over(Handle::Writable(db)))
    }

    /// [`Self::open_file`], read-only.
    fn view_file(path: &Path, config: &RedbWalConfig) -> Result<Self, redb::Error> {
        check_layout(path)?;

        let db = Handle::open_read_only(
            path,
            1024 * 1024 * config.cache.unwrap_or(DEFAULT_CACHE_SIZE_MB),
        )?;

        Ok(Self::over(db))
    }

    pub fn db_mut(&mut self) -> Option<&mut redb::Database> {
        Arc::get_mut(&mut self.db).and_then(Handle::writable_mut)
    }

    fn ensure_initialized(&self) -> Result<(), redb::Error> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);
        wx.open_table(WAL)?;
        wx.commit()?;
        Ok(())
    }

    /// Bring the WAL to [`CURRENT_WAL_VERSION`]: stamp one holding no entry,
    /// and wipe the entries of one recording another version, or none, unless
    /// it holds a block among `needed`. An error [`is_corrupt`] finds is on
    /// what the file holds, and [`Self::open_replaying`] resets the file for
    /// it.
    fn admit(
        &self,
        path: &Path,
        positions: &[Option<BlockSlot>],
        needed: &[Needed],
    ) -> Result<(), Unadmitted> {
        // Read before any write, so a WAL kept for `needed` is left as it was.
        match self.inspect(positions)? {
            Found::Current => Ok(()),
            Found::Empty => {
                info!(
                    version = CURRENT_WAL_VERSION,
                    "initializing WAL metadata version"
                );
                self.ensure_initialized()?;
                Ok(self.stamp_version(CURRENT_WAL_VERSION)?)
            }
            Found::OtherVersion(found) => {
                Self::keep_needed(Some(self), path, needed, &other_version(found))
                    .map_err(Unadmitted::Kept)?;

                self.wipe_and_stamp(CURRENT_WAL_VERSION)?;

                warn!(
                    path = %path.display(),
                    found = %recorded(found),
                    expected = CURRENT_WAL_VERSION,
                    "the WAL records another version, and it is transient: its entries were wiped"
                );

                Ok(())
            }
        }
    }

    /// What the WAL holds, read without writing to it.
    ///
    /// At the current version, the entries a start reads are decoded: the
    /// first, the newest [`NEWEST_DECODED`], and those it replays into stores
    /// at `positions` (see [`Self::open_replaying`]). One that fails is
    /// [`redb::Error::Corrupted`]. Other entries are decoded as they are read.
    fn inspect(&self, positions: &[Option<BlockSlot>]) -> Result<Found, redb::Error> {
        let rx = self.db.begin_read()?;

        let version = match rx.open_table(WAL_METADATA) {
            Ok(table) => table.get(WAL_METADATA_VERSION_KEY)?.map(|v| v.value()),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(error) => return Err(error.into()),
        };

        let table = match rx.open_table(WAL) {
            Ok(table) => table,
            Err(redb::TableError::TableDoesNotExist(_)) => return Ok(Found::Empty),
            Err(error) => return Err(error.into()),
        };

        if version != Some(CURRENT_WAL_VERSION) {
            return if table.is_empty()? {
                Ok(Found::Empty)
            } else {
                Ok(Found::OtherVersion(version))
            };
        }

        let first = table.range::<DbChainPoint>(..)?.take(1);
        let newest = table.range::<DbChainPoint>(..)?.rev().take(NEWEST_DECODED);

        let replayed = match replayed(&table, positions)? {
            Replayed::Nothing => None,
            Replayed::From(start) => Some(table.range(start..)?),
            Replayed::All => Some(table.range::<DbChainPoint>(..)?),
        };

        for entry in first.chain(newest).chain(replayed.into_iter().flatten()) {
            let (point, value) = entry?;

            if let Err(error) = bincode::deserialize::<LogValue<T>>(&value.value()) {
                let point = ChainPoint::from(point.value());

                return Err(redb::Error::Corrupted(format!(
                    "its entry at {point} fails to decode: {error}"
                )));
            }
        }

        Ok(Found::Current)
    }

    fn stamp_version(&self, version: u32) -> Result<(), redb::Error> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);
        {
            let mut metadata = wx.open_table(WAL_METADATA)?;
            metadata.insert(WAL_METADATA_VERSION_KEY, version)?;
        }
        wx.commit()?;
        Ok(())
    }

    /// Atomically drain every entry from the WAL data table and stamp the
    /// metadata with the new version.
    ///
    /// This is a single redb transaction, so a crash between the drain and
    /// the version write cannot leave the DB in a half-upgraded state.
    fn wipe_and_stamp(&self, version: u32) -> Result<(), redb::Error> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);
        {
            let mut wal = wx.open_table(WAL)?;
            wal.retain(|_, _| false)?;
        }
        {
            let mut metadata = wx.open_table(WAL_METADATA)?;
            metadata.insert(WAL_METADATA_VERSION_KEY, version)?;
        }
        wx.commit()?;
        Ok(())
    }

    /// Prunes the WAL history to maintain a maximum number of slots.
    ///
    /// This method attempts to remove older entries from the WAL to keep the
    /// total number of slots within the specified `max_slots` limit. It
    /// operates as follows:
    ///
    /// 1. Determines the start and last slots in the WAL.
    /// 2. Calculates the number of slots that exceed the `max_slots` limit.
    /// 3. If pruning is necessary, it removes entries older than a calculated
    ///    cutoff slot.
    /// 4. Optionally limits the number of slots pruned per invocation.
    ///
    /// # Arguments
    ///
    /// * `max_slots` - The maximum number of slots to retain in the WAL.
    /// * `max_prune` - Optional limit on the number of slots to prune in a
    ///   single operation.
    ///
    /// # Returns
    ///
    /// Returns `Ok(true)` when the WAL is within `max_slots` (or was pruned all
    /// the way down to it), and `Ok(false)` when `max_prune` capped this call
    /// before the target was reached and another round is needed. Pruning is
    /// deterministic: `remove_before` removes by cutoff bound, and any error it
    /// raises is propagated (the write transaction is not committed).
    ///
    /// # Notes
    ///
    /// - If the WAL doesn't exceed the `max_slots` limit, no pruning occurs.
    /// - This method is typically called periodically as part of housekeeping
    ///   operations.
    /// - If `max_prune` is specified, it limits the number of slots pruned in a
    ///   single operation, which can help avoid long-running operations.
    /// - If `max_prune` is not specified, all excess slots will be pruned.
    pub fn prune_history(
        &self,
        max_slots: u64,
        max_prune: Option<u64>,
    ) -> Result<bool, RedbWalError> {
        let Some((start, _)) = self.find_start()? else {
            debug!("no start point found, skipping housekeeping");
            return Ok(true);
        };

        let Some((last, _)) = self.find_tip()? else {
            debug!("no tip found, skipping housekeeping");
            return Ok(true);
        };

        let start_slot = start.slot();
        let last_slot = last.slot();

        let delta = last_slot.saturating_sub(start_slot);
        let excess = delta.saturating_sub(max_slots);

        debug!(
            delta,
            excess, last_slot, start_slot, "wal history delta computed"
        );

        if excess == 0 {
            debug!(delta, max_slots, excess, "no pruning necessary");
            return Ok(true);
        }

        let (done, max_prune) = match max_prune {
            Some(max) => (excess <= max, core::cmp::min(excess, max)),
            None => (true, excess),
        };

        let prune_before = start_slot + max_prune;

        info!(
            cutoff_slot = prune_before,
            start_slot, excess, "pruning wal for excess history"
        );

        self.remove_before(prune_before)?;

        Ok(done)
    }

    /// Approximates the LogSeq for a given BlockSlot within a specified delta
    /// range.
    ///
    /// This function searches for the closest LogSeq entry to the target
    /// BlockSlot within the range [target - max_delta, target + max_delta].
    ///
    /// # Arguments
    ///
    /// * `target` - The target BlockSlot to approximate.
    /// * `max_delta` - The maximum allowed difference between the target and
    ///   the found BlockSlot.
    ///
    /// # Returns
    ///
    /// Returns a Result containing an Option<LogSeq>. If a suitable entry is
    /// found within the specified range, it returns Some(LogSeq), otherwise
    /// None. Returns an error if there's an issue accessing the database.
    ///
    /// # Errors
    ///
    /// This function will return an error if there's an issue with database
    /// operations.
    pub fn approximate_slot(
        &self,
        target: BlockSlot,
        search_range: impl std::ops::RangeBounds<BlockSlot>,
    ) -> Result<Option<ChainPoint>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let range = DbChainPoint::slot_range(search_range);
        let range = table.range(range)?;

        let deltas: Vec<_> = range
            .map_ok(|(k, _)| k.value())
            .map_ok(ChainPoint::from)
            .map_ok(|point| (target.abs_diff(point.slot()), point))
            .try_collect()?;

        let point = deltas.into_iter().min_by_key(|(x, _)| *x).map(|(_, v)| v);

        Ok(point)
    }

    /// Attempts to find an approximate ChainPoint for a given BlockSlot with
    /// retries.
    ///
    /// This function repeatedly calls `approximate_slot` with an expanding
    /// search range until a suitable LogSeq is found or the maximum number
    /// of retries is reached.
    ///
    /// # Arguments
    ///
    /// * `target` - The target BlockSlot to approximate.
    /// * `search_range` - A closure that takes the current retry count and
    ///   returns a range of BlockSlots to search within. This allows for
    ///   dynamic expansion of the search range.
    ///
    /// # Returns
    ///
    /// Returns a Result containing an Option<ChainPoint>. If a suitable entry
    /// is found within any of the attempted search ranges, it returns
    /// Some(ChainPoint), otherwise None. Returns an error if there's an issue
    /// accessing the database.
    ///
    /// # Errors
    ///
    /// This function will return an error if there's an issue with database
    /// operations during any of the approximation attempts.
    ///
    /// # Examples
    ///
    /// ```ignore
    /// let result = wal.approximate_slot_with_retry(
    ///     slot,
    ///     |retry| slot - 100 * retry..=slot + 100 * retry
    /// )?;
    /// ```
    pub fn approximate_slot_with_retry<F, R>(
        &self,
        target: BlockSlot,
        search_range: F,
    ) -> Result<Option<ChainPoint>, RedbWalError>
    where
        F: Fn(usize) -> R,
        R: std::ops::RangeBounds<BlockSlot>,
    {
        for i in 1..10 {
            let search_range = search_range(i);
            let seq = self.approximate_slot(target, search_range)?;

            if let Some(seq) = seq {
                return Ok(Some(seq));
            }
        }

        Ok(None)
    }

    /// Removes all entries from the WAL before the specified slot.
    ///
    /// This function is used to trim the WAL by removing all entries that are
    /// older than the given slot. It uses the `approximate_slot` function
    /// to find a suitable starting point for the deletion process.
    ///
    /// # Arguments
    ///
    /// * `slot` - The BlockSlot before which all entries should be removed.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` if the operation is successful, or a `WalError` if
    /// there's an issue.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - There's an issue with database operations.
    /// - The specified slot cannot be found or approximated in the WAL.
    ///
    /// # Note
    ///
    /// This operation is irreversible and should be used with caution. Make
    /// sure you have backups or are certain about trimming the WAL before
    /// calling this function.
    pub fn remove_before(&self, slot: BlockSlot) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        {
            let mut wal = wx.open_table(WAL)?;

            // `..min_bound(slot)` removes every entry with slot < target and
            // keeps real entries at `slot` (nonzero hash). Deterministic even
            // where no entry exists near the cutoff.
            let bound = DbChainPoint::min_bound(slot);
            let to_remove = wal.extract_from_if(..bound, |_, _| true)?;

            // Drain fully; `?` propagates storage errors before commit, so an
            // error aborts the txn instead of persisting a partial prune.
            for entry in to_remove {
                let (point, _) = entry?;

                if event_enabled!(Level::TRACE) {
                    let point = ChainPoint::from(point.value());
                    trace!(%point, "removing wal table entry");
                }
            }
        }

        wx.commit()?;

        Ok(())
    }

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<LogIter<'a, T>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let start = start.map(DbChainPoint::from);
        let end = end.map(DbChainPoint::from);

        let range = match (start, end) {
            (Some(start), Some(end)) => table.range(start..=end)?,
            (Some(start), None) => table.range(start..)?,
            (None, Some(end)) => table.range(..=end)?,
            (None, None) => table.range::<DbChainPoint>(..)?,
        };

        Ok(LogIter::from(range))
    }

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<T>>, RedbWalError> {
        let rx = self.db.begin_read()?;
        let table = rx.open_table(WAL)?;

        let key = DbChainPoint::from(key.clone());

        let value = table
            .get(&key)?
            .map(|v| v.value())
            .map(|v| from_raw_log_value(&v));

        Ok(value)
    }

    pub fn append_entries(&self, logs: &[LogEntry<T>]) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        {
            let mut wal = wx.open_table(WAL)?;

            for (point, log) in logs {
                let point = DbChainPoint::from(point.clone());
                let log = to_raw_log_value(log);
                wal.insert(point, log)?;
            }
        }

        wx.commit()?;

        Ok(())
    }

    fn remove_entries(&self, after: &ChainPoint) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        {
            let after = DbChainPoint::from(after.clone());

            let mut table = wx.open_table(WAL)?;

            let bounds = after.clone()..;

            let range = table.extract_from_if(bounds, |current, _| current > after)?;

            for entry in range {
                let (point, _) = entry?;

                if event_enabled!(Level::TRACE) {
                    let point = ChainPoint::from(point.value());
                    trace!(%point, "removing wal table entry");
                }
            }
        }

        wx.commit()?;

        Ok(())
    }

    fn reset_to(&self, point: &ChainPoint) -> Result<(), RedbWalError> {
        self.remove_entries(&ChainPoint::Origin)?;

        let entry = (point.clone(), LogValue::origin());
        self.append_entries(&[entry])?;

        Ok(())
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), RedbWalError> {
        self.remove_entries(after)?;

        Ok(())
    }

    fn remove_points(&self, points: &[ChainPoint]) -> Result<(), RedbWalError> {
        let mut wx = self.db.begin_write()?;
        wx.set_quick_repair(true);

        {
            let mut table = wx.open_table(WAL)?;

            for point in points {
                table.remove(DbChainPoint::from(point.clone()))?;
            }
        }

        wx.commit()?;

        Ok(())
    }
}

impl<T> WalStore for RedbWalStore<T>
where
    T: EntityDelta + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Delta = T;
    type LogIterator<'a> = LogIter<'a, Self::Delta>;
    type BlockIterator<'a> = BlockIter<'a, Self::Delta>;

    fn reset_to(&self, point: &ChainPoint) -> Result<(), WalError> {
        RedbWalStore::reset_to(self, point).map_err(From::from)
    }

    fn truncate_front(&self, after: &ChainPoint) -> Result<(), WalError> {
        RedbWalStore::truncate_front(self, after).map_err(From::from)
    }

    fn remove_points(&self, points: &[ChainPoint]) -> Result<(), WalError> {
        RedbWalStore::remove_points(self, points).map_err(From::from)
    }

    fn prune_history(&self, max_slots: u64, max_prune: Option<u64>) -> Result<bool, WalError> {
        RedbWalStore::prune_history(self, max_slots, max_prune).map_err(From::from)
    }

    fn read_entry(&self, key: &ChainPoint) -> Result<Option<LogValue<Self::Delta>>, WalError> {
        RedbWalStore::read_entry(self, key).map_err(From::from)
    }

    fn locate_point(&self, around: BlockSlot) -> Result<Option<ChainPoint>, WalError> {
        let search = |retry| {
            let delta = LOCATE_STEP * retry as u64;
            let start = around.saturating_sub(delta);
            let end = around.saturating_add(delta);
            start..=end
        };

        let x = self.approximate_slot_with_retry(around, search)?;

        Ok(x)
    }

    fn append_entries(&self, logs: Vec<LogEntry<Self::Delta>>) -> Result<(), WalError> {
        RedbWalStore::append_entries(self, &logs).map_err(WalError::from)?;

        Ok(())
    }

    fn remove_entries(&mut self, after: &ChainPoint) -> Result<(), WalError> {
        RedbWalStore::remove_entries(self, after).map_err(WalError::from)?;

        Ok(())
    }

    fn iter_logs<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::LogIterator<'a>, WalError> {
        RedbWalStore::iter_logs(self, start, end).map_err(From::from)
    }

    fn iter_blocks<'a>(
        &self,
        start: Option<ChainPoint>,
        end: Option<ChainPoint>,
    ) -> Result<Self::BlockIterator<'a>, WalError> {
        let iter = RedbWalStore::iter_logs(self, start, end)?;
        let iter = BlockIter::from(iter);
        Ok(iter)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_logs::warned_while;
    use dolos_core::{ChainError, Entity, EntityValue, Namespace, NsKey};
    use redb::ReadableDatabase as _;
    use serde::{Deserialize, Serialize};

    /// Minimal delta type used by tests to satisfy `RedbWalStore`'s generic
    /// bounds. Tests in this module never round-trip data through the WAL,
    /// so the apply/undo/encode/decode methods can be inert.
    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestDelta;

    #[derive(Clone, Debug)]
    struct TestEntity;

    impl Entity for TestEntity {
        fn decode_entity(_: Namespace, _: &EntityValue) -> Result<Self, ChainError> {
            unimplemented!("not exercised in WAL versioning tests")
        }

        fn encode_entity(_: &Self) -> (Namespace, EntityValue) {
            unimplemented!("not exercised in WAL versioning tests")
        }
    }

    impl EntityDelta for TestDelta {
        type Entity = TestEntity;

        fn key(&self) -> NsKey {
            unimplemented!("not exercised in WAL versioning tests")
        }

        fn apply(&mut self, _: &mut Option<Self::Entity>) {}

        fn undo(&self, _: &mut Option<Self::Entity>) {}
    }

    type TestStore = RedbWalStore<TestDelta>;

    /// A named way to write a WAL file at a path.
    type Case = (&'static str, fn(&Path));

    fn open_test_store(path: &Path) -> Result<TestStore, WalError> {
        TestStore::open(path, &RedbWalConfig::default())
    }

    /// Open a redb DB directly (bypassing RedbWalStore::open) so tests can
    /// simulate legacy on-disk state that predates the metadata table.
    fn open_raw_db(path: &Path) -> redb::Database {
        redb::Database::builder().create(path).unwrap()
    }

    /// The version the store records, if any.
    fn stamped(store: &TestStore) -> Option<u32> {
        let rx = store.db.begin_read().unwrap();

        match rx.open_table(WAL_METADATA) {
            Ok(table) => table
                .get(WAL_METADATA_VERSION_KEY)
                .unwrap()
                .map(|v| v.value()),
            Err(redb::TableError::TableDoesNotExist(_)) => None,
            Err(error) => panic!("{error}"),
        }
    }

    /// Whether the store's WAL table holds no row, read without decoding.
    fn holds_no_entry(store: &TestStore) -> bool {
        let rx = store.db.begin_read().unwrap();
        rx.open_table(WAL).unwrap().is_empty().unwrap()
    }

    /// The bytes of an entry this release decodes.
    fn decodable() -> Vec<u8> {
        to_raw_log_value(&LogValue::<TestDelta>::origin())
    }

    /// The bytes of an entry no release decodes.
    fn undecodable() -> Vec<u8> {
        vec![0xDE, 0xAD, 0xBE, 0xEF]
    }

    /// Write a WAL file at `path` the way another release could have left
    /// it: stamped `version`, or unstamped, holding `values` at successive
    /// slots from 42.
    fn write_raw(path: &Path, version: Option<u32>, values: &[Vec<u8>]) {
        let entries: Vec<_> = (42..).zip(values.iter().cloned()).collect();
        write_raw_at(path, version, &entries);
    }

    /// [`write_raw`], holding each value at its slot.
    fn write_raw_at(path: &Path, version: Option<u32>, entries: &[(BlockSlot, Vec<u8>)]) {
        let db = open_raw_db(path);
        let wx = db.begin_write().unwrap();
        {
            if let Some(version) = version {
                let mut metadata = wx.open_table(WAL_METADATA).unwrap();
                metadata.insert(WAL_METADATA_VERSION_KEY, version).unwrap();
            }

            let mut table = wx.open_table(WAL).unwrap();

            for (slot, value) in entries {
                let key = DbChainPoint::from(ChainPoint::Specific(*slot, [1u8; 32].into()));
                table.insert(key, value.clone()).unwrap();
            }
        }
        wx.commit().unwrap();
    }

    /// Open the WAL at `path` writable and check it was reset: no entry, the
    /// current version, and usable.
    fn assert_reset_on_open(path: &Path) {
        let store = open_test_store(path).unwrap();
        assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION));
        assert!(holds_no_entry(&store));

        store.reset_to(&ChainPoint::Origin).unwrap();
        assert_eq!(store.find_tip().unwrap().unwrap().0, ChainPoint::Origin);
        drop(store);

        let store = open_test_store(path).unwrap();
        assert_eq!(store.find_tip().unwrap().unwrap().0, ChainPoint::Origin);
    }

    /// An entry is seen whatever version the file records, and reading it
    /// leaves the file as it was.
    #[test]
    fn probe_reads_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        open_test_store(&path).unwrap();
        assert_eq!(probe(&path).unwrap(), WalProbe::Empty);

        {
            let db = open_raw_db(&path);
            let wx = db.begin_write().unwrap();
            {
                let mut metadata = wx.open_table(WAL_METADATA).unwrap();
                metadata.insert(WAL_METADATA_VERSION_KEY, 1).unwrap();

                let mut table = wx.open_table(WAL).unwrap();
                let key = DbChainPoint::from(ChainPoint::Specific(42, [1u8; 32].into()));
                table.insert(key, vec![0xDEu8, 0xAD, 0xBE, 0xEF]).unwrap();
            }
            wx.commit().unwrap();
        }

        let before = std::fs::read(&path).unwrap();
        assert_eq!(probe(&path).unwrap(), WalProbe::Entries);
        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    /// A file with no WAL table, as a fresh redb file is, holds no entry.
    #[test]
    fn probe_without_a_table_is_empty() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        drop(open_raw_db(&path));

        assert_eq!(probe(&path).unwrap(), WalProbe::Empty);
    }

    /// A file a store holds open is in use, not data, and is still in use
    /// until the store closes.
    #[test]
    fn probe_under_an_open_store_is_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        let store = open_test_store(&path).unwrap();
        assert_eq!(probe(&path).unwrap(), WalProbe::InUse);

        drop(store);
        assert_eq!(probe(&path).unwrap(), WalProbe::Empty);
    }

    #[test]
    fn fresh_in_memory_stamps_current_version() {
        let store = TestStore::memory().unwrap();
        assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION));
    }

    /// A read-only open leaves the file as it found it, refuses writes, and
    /// is refused while a writer holds the file.
    #[test]
    fn read_only_open_never_writes() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        let writer = open_test_store(&path).unwrap();
        writer.reset_to(&ChainPoint::Origin).unwrap();

        let error = TestStore::open_read_only(&path, &RedbWalConfig::default()).unwrap_err();
        assert!(crate::handle::is_already_open(&error), "{error}");

        drop(writer);
        let before = std::fs::read(&path).unwrap();

        let store = TestStore::open_read_only(&path, &RedbWalConfig::default()).unwrap();
        assert_eq!(store.find_tip().unwrap().unwrap().0, ChainPoint::Origin);
        assert!(store.reset_to(&ChainPoint::Origin).is_err());
        drop(store);

        assert_eq!(std::fs::read(&path).unwrap(), before);
    }

    /// A WAL this release can't read is read as empty by a read-only open,
    /// and left byte-identical for the next writable open to reset.
    #[test]
    fn a_read_only_open_reads_a_wal_it_cannot_read_as_empty() {
        let cases: [Case; 8] = [
            ("older", |path| {
                write_raw(path, Some(CURRENT_WAL_VERSION - 1), &[decodable()])
            }),
            ("newer", |path| {
                write_raw(path, Some(CURRENT_WAL_VERSION + 1), &[decodable()])
            }),
            ("newer and empty", |path| {
                write_raw(path, Some(CURRENT_WAL_VERSION + 1), &[])
            }),
            ("unstamped", |path| write_raw(path, None, &[decodable()])),
            ("undecodable tip", |path| {
                write_raw(
                    path,
                    Some(CURRENT_WAL_VERSION),
                    &[decodable(), undecodable()],
                )
            }),
            ("undecodable start", |path| {
                write_raw(
                    path,
                    Some(CURRENT_WAL_VERSION),
                    &[undecodable(), decodable()],
                )
            }),
            ("not a database", |path| {
                std::fs::write(path, vec![0xa5; 64 * 1024]).unwrap()
            }),
            ("cut short past its header", |path| cut(path, 4096)),
        ];

        for (case, write) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            write(&path);

            let before = std::fs::read(&path).unwrap();

            let store = TestStore::open_read_only(&path, &RedbWalConfig::default())
                .unwrap_or_else(|error| panic!("{case}: {error}"));
            assert!(store.find_tip().unwrap().is_none(), "{case}");
            assert!(
                store.iter_logs(None, None).unwrap().next().is_none(),
                "{case}"
            );
            drop(store);

            assert!(
                std::fs::read(&path).unwrap() == before,
                "{case}: the file changed"
            );
        }
    }

    #[test]
    fn fresh_on_disk_stamps_current_version() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        let store = open_test_store(&path).unwrap();
        assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION));
    }

    #[test]
    fn legacy_db_with_data_gets_wiped_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        // Create a legacy DB: WAL table populated, no metadata table.
        {
            let db = open_raw_db(&path);
            let wx = db.begin_write().unwrap();
            {
                let mut table = wx.open_table(WAL).unwrap();
                let key = DbChainPoint::from(ChainPoint::Specific(42, [1u8; 32].into()));
                table.insert(key, vec![0xDEu8, 0xAD, 0xBE, 0xEF]).unwrap();
            }
            wx.commit().unwrap();
        }

        // Sanity-check the legacy state.
        {
            let db = open_raw_db(&path);
            let rx = db.begin_read().unwrap();
            let table = rx.open_table(WAL).unwrap();
            assert!(!table.is_empty().unwrap());
            assert!(matches!(
                rx.open_table(WAL_METADATA),
                Err(redb::TableError::TableDoesNotExist(_))
            ));
        }

        let store = open_test_store(&path).unwrap();
        assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION));
        assert!(holds_no_entry(&store));
    }

    /// Open a WAL stamped `version` that holds an entry, and check the open
    /// wiped it and stamped the current version.
    fn assert_wiped_on_open(version: u32) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        write_raw(&path, Some(version), &[undecodable()]);

        let store = open_test_store(&path).unwrap();
        assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION));
        assert!(holds_no_entry(&store));
    }

    /// A version-3 WAL holds inputs without their creation position, so
    /// opening it wipes the entries.
    #[test]
    fn a_version_3_wal_is_wiped_on_open() {
        assert_wiped_on_open(3);
    }

    /// A version-4 WAL holds staged asset mints without their CIP-25 flag, so
    /// opening it wipes the entries.
    #[test]
    fn a_version_4_wal_is_wiped_on_open() {
        assert_wiped_on_open(4);
    }

    /// A version-5 WAL holds DRep and pool deltas with the fields that left
    /// the records, so opening it wipes the entries.
    #[test]
    fn a_version_5_wal_is_wiped_on_open() {
        assert_wiped_on_open(5);
    }

    /// A WAL a newer release wrote is wiped as an older one is: this
    /// release can't read it, and the WAL is transient.
    #[test]
    fn a_newer_wal_is_wiped_on_open() {
        assert_wiped_on_open(CURRENT_WAL_VERSION + 1);
        assert_wiped_on_open(u32::MAX);
    }

    /// A newer WAL holding no entry is stamped with the current version.
    #[test]
    fn a_newer_empty_wal_is_stamped_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");
        write_raw(&path, Some(CURRENT_WAL_VERSION + 1), &[]);

        let store = open_test_store(&path).unwrap();
        assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION));
        assert!(holds_no_entry(&store));
    }

    /// At the current version, an entry at either end that fails to decode
    /// resets the WAL, as every open reads both ends, however long the WAL.
    #[test]
    fn an_entry_that_fails_to_decode_resets_the_wal() {
        // past the newest entries an open decodes
        let mut long = vec![decodable(); 2 * NEWEST_DECODED];
        long[0] = undecodable();

        let cases = [
            vec![undecodable()],
            vec![decodable(), undecodable()],
            vec![undecodable(), decodable()],
            vec![undecodable(), decodable(), undecodable()],
            long,
        ];

        for values in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            write_raw(&path, Some(CURRENT_WAL_VERSION), &values);

            assert_reset_on_open(&path);
        }
    }

    /// An entry between the ends among the newest, which the intersect reads
    /// at every start, resets the WAL too, so reading them can't panic: down
    /// to the deepest it reads, the 31st newest for five candidates.
    #[test]
    fn an_undecodable_entry_the_intersect_reads_resets_the_wal() {
        let mut deepest = vec![decodable(); 40];
        deepest[40 - 31] = undecodable();

        for values in [vec![decodable(), undecodable(), decodable()], deepest] {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            write_raw(&path, Some(CURRENT_WAL_VERSION), &values);

            let store = open_test_store(&path).unwrap();
            assert!(store.intersect_candidates(5).unwrap().is_empty());
            drop(store);

            assert_reset_on_open(&path);
        }
    }

    /// Past the first and the newest entries, an entry is decoded on open only
    /// when a start replays it into a store at one of the positions: from the
    /// entry nearest each slot, or every entry for a store holding nothing.
    #[test]
    fn an_open_decodes_the_entries_a_start_replays() {
        // slots 42 to 141, the one at 52 undecodable
        let mut values = vec![decodable(); 100];
        values[10] = undecodable();

        let cases: [(&[Option<BlockSlot>], bool); 11] = [
            (&[], false),
            (&[Some(60)], false),
            (&[Some(141), Some(60)], false),
            (&[Some(50)], true),
            (&[Some(141), Some(52)], true),
            (&[Some(52), Some(141)], true),
            (&[Some(141), None], true),
            // past the tip, with its nearest entry below it
            (&[Some(150)], false),
            // the tip, 180 slots below, is still within reach
            (&[Some(141 + 180)], false),
            // no entry within reach, so the replay reads them all
            (&[Some(141 + 181)], true),
            (&[Some(141 + 181), Some(141)], true),
        ];

        for (positions, reset) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            write_raw(&path, Some(CURRENT_WAL_VERSION), &values);

            assert_replayed(&path, positions, reset);
        }
    }

    /// Between two entries as near to a store's slot, a start replays from
    /// the lower one, as [`WalStore::locate_point`] finds it.
    #[test]
    fn an_open_replays_from_the_lower_of_two_nearest_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        // 50 undecodable and older than the newest decoded, 51 between it and 52
        let newest = (52..).take(NEWEST_DECODED).map(|slot| (slot, decodable()));
        let entries: Vec<_> = [(42, decodable()), (50, undecodable())]
            .into_iter()
            .chain(newest)
            .collect();
        write_raw_at(&path, Some(CURRENT_WAL_VERSION), &entries);

        assert_replayed(&path, &[Some(51)], true);
    }

    /// A start replays from the nearest entry above a store's slot too, as
    /// [`WalStore::locate_point`] looks both ways from it.
    #[test]
    fn an_open_replays_from_a_nearer_entry_above() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        // 50 undecodable and older than the newest decoded, 57 nearer 60 than it
        let newest = (60..).take(NEWEST_DECODED).map(|slot| (slot, decodable()));
        let entries: Vec<_> = [(42, decodable()), (50, undecodable())]
            .into_iter()
            .chain(newest)
            .collect();
        write_raw_at(&path, Some(CURRENT_WAL_VERSION), &entries);

        assert_replayed(&path, &[Some(57)], false);
    }

    /// Open the WAL at `path` for a start replaying into stores at
    /// `positions`, read-only and then writable, and check both find it
    /// unreadable exactly when `reset`: the read-only open reads it as empty
    /// and leaves the file as it was, and the writable one empties it.
    fn assert_replayed(path: &Path, positions: &[Option<BlockSlot>], reset: bool) {
        let before = std::fs::read(path).unwrap();

        let config = RedbWalConfig::default();

        let store = TestStore::open_read_only_replaying(path, &config, positions, &[]).unwrap();
        let read_empty = store.find_tip().unwrap().is_none();
        assert_eq!(read_empty, reset, "read-only {positions:?}");
        drop(store);

        assert!(
            std::fs::read(path).unwrap() == before,
            "read-only {positions:?}: the file changed"
        );

        let store = TestStore::open_replaying(path, &config, positions, &[]).unwrap();
        assert_eq!(holds_no_entry(&store), reset, "{positions:?}");
    }

    /// Decodable entries at the current version are kept as they are.
    #[test]
    fn decodable_entries_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");
        write_raw(
            &path,
            Some(CURRENT_WAL_VERSION),
            &[decodable(), decodable()],
        );

        let store = open_test_store(&path).unwrap();
        assert_eq!(slots(&store), vec![42, 43]);
    }

    /// A WAL whose tables another release laid out with other types, or as
    /// multimaps, fails to decode and is reset.
    #[test]
    fn a_wal_whose_tables_changed_type_is_reset() {
        let cases: [fn(&redb::WriteTransaction); 3] = [
            |wx| {
                let mut table = wx
                    .open_table(TableDefinition::<&str, u64>::new("wal"))
                    .unwrap();
                table.insert("tip", 1).unwrap();
            },
            |wx| {
                let mut table = wx
                    .open_table(TableDefinition::<&str, u64>::new("wal_metadata"))
                    .unwrap();
                table.insert(WAL_METADATA_VERSION_KEY, 1).unwrap();
            },
            |wx| {
                let mut table = wx
                    .open_multimap_table(redb::MultimapTableDefinition::<&str, u32>::new("wal"))
                    .unwrap();
                table.insert("tip", 1).unwrap();
            },
        ];

        for write in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");

            {
                let db = open_raw_db(&path);
                let wx = db.begin_write().unwrap();
                write(&wx);
                wx.commit().unwrap();
            }

            assert_reset_on_open(&path);
        }
    }

    /// A file redb can't read as a database, whether damaged, cut short or
    /// not one at all, is emptied and recreated.
    #[test]
    fn a_file_redb_cannot_read_is_reset() {
        let cases: [Case; 3] = [
            ("not a database", |path| {
                std::fs::write(path, vec![0xa5; 64 * 1024]).unwrap()
            }),
            ("cut short", |path| cut(path, 100)),
            ("cut short past its header", |path| cut(path, 4096)),
        ];

        for (case, write) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            write(&path);

            #[cfg(unix)]
            let identity = kept(&path);

            let store = open_test_store(&path).unwrap_or_else(|error| panic!("{case}: {error}"));
            assert!(holds_no_entry(&store), "{case}");
            drop(store);

            #[cfg(unix)]
            assert_eq!(kept(&path), identity, "{case}: the file was replaced");

            assert_reset_on_open(&path);
        }
    }

    /// The file's identity and mode, which a reset in place keeps.
    #[cfg(unix)]
    fn kept(path: &Path) -> (u64, u64, u32) {
        use std::os::unix::fs::MetadataExt as _;

        let metadata = std::fs::metadata(path).unwrap();
        (metadata.dev(), metadata.ino(), metadata.mode())
    }

    /// A WAL holding an entry, cut to `len` bytes.
    fn cut(path: &Path, len: u64) {
        let store = open_test_store(path).unwrap();
        store.append_entries(&[entry(10)]).unwrap();
        drop(store);

        let file = std::fs::OpenOptions::new().write(true).open(path).unwrap();
        file.set_len(len).unwrap();
    }

    /// A WAL another handle holds fails to open on the lock, writable or
    /// read-only, and is left as it was.
    #[test]
    fn a_held_wal_fails_on_the_lock_and_is_kept() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        let held = open_test_store(&path).unwrap();
        held.append_entries(&[entry(10)]).unwrap();

        // Windows enforces redb's lock on reads too
        #[cfg(unix)]
        let before = std::fs::read(&path).unwrap();

        let error = open_test_store(&path).unwrap_err();
        assert!(crate::handle::is_already_open(&error), "{error}");

        let error = TestStore::open_read_only(&path, &RedbWalConfig::default()).unwrap_err();
        assert!(crate::handle::is_already_open(&error), "{error}");

        #[cfg(unix)]
        assert!(std::fs::read(&path).unwrap() == before, "the file changed");
        drop(held);

        let store = open_test_store(&path).unwrap();
        assert_eq!(slots(&store), vec![10]);
    }

    /// A WAL it may not write, or may not read, fails on the permission,
    /// unreadable contents or not, with no warning, and is left as it was.
    #[cfg(unix)]
    #[test]
    fn a_wal_it_may_not_write_fails_and_is_kept() {
        use std::os::unix::fs::PermissionsExt as _;

        let writes: [fn(&Path); 3] = [
            |path| write_raw(path, Some(CURRENT_WAL_VERSION + 1), &[decodable()]),
            |path| std::fs::write(path, vec![0xa5; 64 * 1024]).unwrap(),
            // readable, it reaches the reset's truncate
            |path| cut(path, 4096),
        ];

        // read-only, and write-only: a reset could truncate the second
        for mode in [0o444, 0o200] {
            for write in writes {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("wal.redb");
                write(&path);

                let before = std::fs::read(&path).unwrap();
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();

                // root reads and writes it regardless
                let read = std::fs::File::open(&path).is_ok();
                let written = std::fs::OpenOptions::new().write(true).open(&path).is_ok();

                if read && written {
                    return;
                }

                let mut opened = None;
                let warned = warned_while(|| opened = Some(open_test_store(&path)));

                let error = opened.unwrap().unwrap_err();
                assert!(
                    format!("{error:?}").contains("PermissionDenied"),
                    "{mode:o}: {error:?}"
                );
                assert!(warned.is_empty(), "{mode:o}: {warned:?}");

                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
                assert!(
                    std::fs::read(&path).unwrap() == before,
                    "{mode:o}: the file changed"
                );
            }
        }
    }

    /// A reset, writable, and a reading as empty, read-only, each log one
    /// warning naming the WAL.
    #[test]
    fn each_reset_logs_one_warning_naming_the_wal() {
        // each case with what its writable open says
        let cases: [(Case, &str); 3] = [
            (
                ("newer", |path| {
                    write_raw(path, Some(CURRENT_WAL_VERSION + 1), &[decodable()])
                }),
                "entries were wiped",
            ),
            (
                ("undecodable", |path| {
                    write_raw(path, Some(CURRENT_WAL_VERSION), &[undecodable()])
                }),
                "reset empty",
            ),
            (("cut short", |path| cut(path, 4096)), "reset empty"),
        ];

        for ((case, write), said) in cases {
            for writable in [false, true] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("wal.redb");
                write(&path);

                let warned = warned_while(|| {
                    if writable {
                        open_test_store(&path).unwrap();
                    } else {
                        TestStore::open_read_only(&path, &RedbWalConfig::default()).unwrap();
                    }
                });

                let said = if writable { said } else { "reads it as empty" };

                assert_eq!(warned.len(), 1, "{case} {writable}: {warned:?}");
                assert!(warned[0].contains(said), "{case} {writable}: {warned:?}");
                assert!(
                    warned[0].contains(&format!("path={}", path.display())),
                    "{case} {writable}: {warned:?}"
                );
            }
        }
    }

    /// The bytes of an entry holding a block, as a roll appends it.
    fn block() -> Vec<u8> {
        to_raw_log_value(&LogValue::<TestDelta> {
            block: vec![0x82, 0x00, 0x80],
            delta: vec![],
            inputs: Default::default(),
        })
    }

    /// What a store behind the WAL's entry at slot 43 needs of it.
    fn needing(from: BlockSlot, through: Option<BlockSlot>) -> Needed {
        Needed {
            from,
            through,
            by: "the archive, at slot 42, is behind the state at slot 43".into(),
        }
    }

    /// WALs this release can't read, each over a reset point at slot 42 and
    /// a block at 43 where its entries can be read, with whether the open can
    /// tell the block is there.
    const KEPT_CASES: [(Case, bool); 6] = [
        (
            ("newer", |path| {
                write_raw(path, Some(CURRENT_WAL_VERSION + 1), &[decodable(), block()])
            }),
            true,
        ),
        (
            ("older", |path| {
                write_raw(path, Some(CURRENT_WAL_VERSION - 1), &[decodable(), block()])
            }),
            true,
        ),
        (
            ("unstamped", |path| {
                write_raw(path, None, &[decodable(), block()])
            }),
            true,
        ),
        (
            ("undecodable", |path| {
                write_raw(
                    path,
                    Some(CURRENT_WAL_VERSION),
                    &[decodable(), block(), undecodable()],
                )
            }),
            true,
        ),
        (
            ("tables changed type", |path| {
                let db = open_raw_db(path);
                let wx = db.begin_write().unwrap();
                wx.open_table(TableDefinition::<&str, u64>::new("wal"))
                    .unwrap()
                    .insert("tip", 1)
                    .unwrap();
                wx.commit().unwrap();
            }),
            false,
        ),
        (("cut short past its header", |path| cut(path, 4096)), false),
    ];

    /// A writable open keeps a WAL it can't read that holds a block a store
    /// still needs, or may hold one, and fails saying why and what to do,
    /// with no warning. Once nothing needs it, the next open resets it.
    #[test]
    fn a_wal_holding_a_block_a_store_needs_is_kept() {
        for ((case, write), seen) in KEPT_CASES {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            write(&path);

            let before = std::fs::read(&path).unwrap();

            let mut opened = None;
            let warned = warned_while(|| {
                opened = Some(TestStore::open_replaying(
                    &path,
                    &RedbWalConfig::default(),
                    &[],
                    &[needing(43, Some(43))],
                ))
            });

            let error = match opened.unwrap() {
                Err(error @ WalError::Kept { .. }) => error.to_string(),
                Err(error) => panic!("{case}: {error:?}"),
                Ok(_) => panic!("{case}: opened"),
            };

            assert!(warned.is_empty(), "{case}: {warned:?}");
            assert!(
                std::fs::read(&path).unwrap() == before,
                "{case}: the file changed"
            );

            let held = if seen {
                "the WAL holds the block at slot 43"
            } else {
                "the WAL may hold a block a reset would lose"
            };

            for said in [
                &format!("the WAL at {} is kept as it is", path.display()),
                "the archive, at slot 42, is behind the state at slot 43",
                held,
                "start that release once",
                "`dolos data rebuild`",
            ] {
                assert!(error.contains(said), "{case}: {error}");
            }

            assert_reset_on_open(&path);
        }
    }

    /// The blocks a store needs that a WAL doesn't hold keep nothing: the
    /// reset point before them holds none, and none lie past its tip.
    #[test]
    fn a_wal_holding_no_block_a_store_needs_is_reset() {
        let needed = [needing(42, Some(42)), needing(44, None)];

        let cases: [Case; 2] = [
            ("newer", |path| {
                write_raw(path, Some(CURRENT_WAL_VERSION + 1), &[decodable(), block()])
            }),
            ("undecodable", |path| {
                write_raw(
                    path,
                    Some(CURRENT_WAL_VERSION),
                    &[decodable(), undecodable()],
                )
            }),
        ];

        for (case, write) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            write(&path);

            let store = TestStore::open_replaying(&path, &RedbWalConfig::default(), &[], &needed)
                .unwrap_or_else(|error| panic!("{case}: {error}"));
            assert!(holds_no_entry(&store), "{case}");
            assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION), "{case}");
        }
    }

    /// A read-only open reads a WAL it can't read as empty whether or not a
    /// store needs it, leaving it byte-identical, and its one warning says
    /// which: a writable open keeps one that is needed, and resets the rest.
    #[test]
    fn a_read_only_open_says_whether_a_writable_one_keeps_the_wal() {
        for ((case, write), seen) in KEPT_CASES {
            for needed in [vec![needing(43, Some(43))], vec![]] {
                let dir = tempfile::tempdir().unwrap();
                let path = dir.path().join("wal.redb");
                write(&path);

                let before = std::fs::read(&path).unwrap();

                let warned = warned_while(|| {
                    let store = TestStore::open_read_only_replaying(
                        &path,
                        &RedbWalConfig::default(),
                        &[],
                        &needed,
                    )
                    .unwrap_or_else(|error| panic!("{case}: {error}"));

                    assert!(store.find_tip().unwrap().is_none(), "{case}");
                });

                assert!(
                    std::fs::read(&path).unwrap() == before,
                    "{case}: the file changed"
                );

                assert_eq!(warned.len(), 1, "{case}: {warned:?}");
                let warned = &warned[0];

                if needed.is_empty() {
                    assert!(warned.contains("next writable open resets it"), "{case}");
                    assert!(!warned.contains("needed="), "{case}: {warned}");
                } else {
                    let held = if seen { "holds the block" } else { "may hold" };

                    assert!(warned.contains("a writable open keeps it"), "{case}");
                    assert!(warned.contains("behind the state"), "{case}: {warned}");
                    assert!(warned.contains(held), "{case}: {warned}");
                }
            }
        }
    }

    /// [`write_raw`], as a crash leaves it: a copy of the file taken while a
    /// writer holds it, which a read-only open refuses until a writable one
    /// repairs it. The file as written is left beside it.
    #[cfg(unix)]
    fn write_crashed(path: &Path, version: Option<u32>, values: &[Vec<u8>]) -> std::path::PathBuf {
        let written = path.with_extension("written");
        write_raw(&written, version, values);

        let held = open_raw_db(&written);
        std::fs::copy(&written, path).unwrap();
        drop(held);

        written
    }

    /// A WAL file's version, and each entry's key and bytes.
    #[cfg(unix)]
    type Recorded = (Option<u32>, Vec<(Vec<u8>, Vec<u8>)>);

    /// What the WAL file at `path` records, read raw.
    #[cfg(unix)]
    fn recorded_in(path: &Path) -> Recorded {
        use redb::{ReadableDatabase as _, ReadableTable as _};

        let db = redb::ReadOnlyDatabase::open(path).unwrap();
        let rx = db.begin_read().unwrap();

        let version = rx
            .open_table(WAL_METADATA)
            .unwrap()
            .get(WAL_METADATA_VERSION_KEY)
            .unwrap()
            .map(|version| version.value());

        let entries = rx
            .open_table(WAL)
            .unwrap()
            .iter()
            .unwrap()
            .map(|entry| {
                let (key, value) = entry.unwrap();
                (key.value().0.to_vec(), value.value())
            })
            .collect();

        (version, entries)
    }

    /// A WAL a crash left, which only a writable open can read once it has
    /// repaired it, is kept after the repair for a block a store needs: its
    /// version and every entry as they were.
    #[cfg(unix)]
    #[test]
    fn a_wal_a_crash_left_is_kept_once_repaired() {
        let cases = [
            ("newer", CURRENT_WAL_VERSION + 1, vec![decodable(), block()]),
            (
                "undecodable",
                CURRENT_WAL_VERSION,
                vec![decodable(), block(), undecodable()],
            ),
        ];

        for (case, version, values) in cases {
            let dir = tempfile::tempdir().unwrap();
            let path = dir.path().join("wal.redb");
            let written = write_crashed(&path, Some(version), &values);

            let config = RedbWalConfig::default();

            let error = TestStore::open_read_only(&path, &config).unwrap_err();
            assert!(crate::handle::needs_repair(&error), "{case}: {error}");

            let opened = TestStore::open_replaying(&path, &config, &[], &[needing(43, Some(43))]);

            match opened {
                Err(error @ WalError::Kept { .. }) => {
                    let error = error.to_string();
                    assert!(error.contains("the block at slot 43"), "{case}: {error}");
                }
                Err(error) => panic!("{case}: {error:?}"),
                Ok(_) => panic!("{case}: opened"),
            }

            assert_eq!(recorded_in(&path), recorded_in(&written), "{case}");
        }
    }

    #[test]
    fn matching_version_is_a_noop() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("wal.redb");

        // First open: stamps current version.
        open_test_store(&path).unwrap();

        // Second open: should succeed without rewriting (no panic, version intact).
        let store = open_test_store(&path).unwrap();
        assert_eq!(stamped(&store), Some(CURRENT_WAL_VERSION));
    }

    /// WAL entry at `slot` with a deterministic nonzero hash (distinct from the
    /// zero-hash cutoff bound `remove_before` uses).
    fn entry(slot: BlockSlot) -> LogEntry<TestDelta> {
        let mut hash = [1u8; 32];
        hash[0..8].copy_from_slice(&slot.to_be_bytes());
        (ChainPoint::Specific(slot, hash.into()), LogValue::origin())
    }

    fn slots(store: &TestStore) -> Vec<BlockSlot> {
        store
            .iter_logs(None, None)
            .unwrap()
            .map(|(p, _)| p.slot())
            .collect()
    }

    /// Endianness regression: key order must equal slot order across the
    /// 2^8/2^16/2^32 boundaries, and scans must find realistic slots.
    #[test]
    fn slot_range_round_trip_across_byte_boundaries() {
        let store = TestStore::memory().unwrap();

        let targets: Vec<BlockSlot> = vec![
            255,
            256,
            65_535,
            65_536,
            4_294_967_295,
            4_294_967_296,
            127_000_000, // preprod-scale slot from the bug report
        ];

        // Insert in shuffled order to prove ordering comes from the keys.
        let mut insert_order = targets.clone();
        insert_order.rotate_left(3);
        let entries: Vec<_> = insert_order.iter().map(|s| entry(*s)).collect();
        store.append_entries(&entries).unwrap();

        // Lexicographic key order must equal slot order.
        let mut sorted = targets.clone();
        sorted.sort_unstable();
        assert_eq!(slots(&store), sorted, "key order must equal slot order");

        // Every slot is locatable and approximates to itself.
        for &s in &targets {
            let located = store.locate_point(s).unwrap();
            assert_eq!(
                located.map(|p| p.slot()),
                Some(s),
                "locate_point failed for slot {s}"
            );

            let approx = store.approximate_slot(s, s..=s).unwrap();
            assert_eq!(
                approx.map(|p| p.slot()),
                Some(s),
                "approximate_slot failed for slot {s}"
            );
        }

        // Scanning from a mid boundary returns exactly the at-or-after suffix.
        let from = entry(65_536).0;
        let suffix: Vec<BlockSlot> = store
            .iter_logs(Some(from), None)
            .unwrap()
            .map(|(p, _)| p.slot())
            .collect();
        assert_eq!(
            suffix,
            vec![65_536, 127_000_000, 4_294_967_295, 4_294_967_296]
        );
    }

    /// `slot_range` maps bounds to the enclosing key bounds without
    /// overflowing at `BlockSlot::MAX` / underflowing at `MIN` on `Excluded`.
    #[test]
    fn slot_range_handles_extreme_and_exclusive_bounds() {
        use std::ops::Bound;

        let r = DbChainPoint::slot_range((Bound::Excluded(BlockSlot::MAX), Bound::Unbounded));
        assert_eq!(
            r.start_bound(),
            Bound::Excluded(&DbChainPoint::max_bound(BlockSlot::MAX))
        );
        assert_eq!(r.end_bound(), Bound::Unbounded);

        let r = DbChainPoint::slot_range((Bound::Unbounded, Bound::Excluded(BlockSlot::MIN)));
        assert_eq!(r.start_bound(), Bound::Unbounded);
        assert_eq!(
            r.end_bound(),
            Bound::Excluded(&DbChainPoint::min_bound(BlockSlot::MIN))
        );

        let r = DbChainPoint::slot_range(10..=20);
        assert_eq!(
            r.start_bound(),
            Bound::Included(&DbChainPoint::min_bound(10))
        );
        assert_eq!(r.end_bound(), Bound::Included(&DbChainPoint::max_bound(20)));
    }

    /// Unbatched prune shrinks the WAL to exactly `max_slots`, tip preserved.
    #[test]
    fn prune_history_full_shrinks_to_max_slots() {
        let store = TestStore::memory().unwrap();

        let base: BlockSlot = 100_000_000;
        let count: u64 = 20_000;
        let entries: Vec<_> = (0..count).map(|i| entry(base + i)).collect();
        store.append_entries(&entries).unwrap();

        let last = base + count - 1;
        let max_slots = 5_000;

        let done = store.prune_history(max_slots, None).unwrap();
        assert!(done, "unbatched prune must finish in one call");

        let start = store.find_start().unwrap().unwrap().0.slot();
        let tip = store.find_tip().unwrap().unwrap().0.slot();

        assert_eq!(tip, last, "tip must be preserved");
        assert_eq!(start, last - max_slots, "retained window must be max_slots");
        assert_eq!(
            slots(&store).len() as u64,
            max_slots + 1,
            "exactly the protected window remains"
        );
    }

    /// Batched prune makes bounded progress, converges, and never prunes into
    /// the protected `max_slots` window.
    #[test]
    fn prune_history_batched_converges() {
        let store = TestStore::memory().unwrap();

        let base: BlockSlot = 100_000_000;
        let count: u64 = 20_000;
        let entries: Vec<_> = (0..count).map(|i| entry(base + i)).collect();
        store.append_entries(&entries).unwrap();

        let last = base + count - 1;
        let max_slots = 5_000;
        let max_prune = 3_000;

        // First round: large backlog is not cleared in one batch.
        let start_before = store.find_start().unwrap().unwrap().0.slot();
        let done = store.prune_history(max_slots, Some(max_prune)).unwrap();
        assert!(!done, "large backlog should not finish in one batch");

        let start_after = store.find_start().unwrap().unwrap().0.slot();
        assert!(start_after > start_before, "batch must advance the start");
        assert!(
            last - start_after >= max_slots,
            "must never prune into the protected window"
        );

        // Loop to completion, bounded to detect non-convergence.
        let mut done = false;
        let mut rounds = 0;
        while !done {
            done = store.prune_history(max_slots, Some(max_prune)).unwrap();
            rounds += 1;
            assert!(rounds < 100, "batched pruning did not converge");
        }

        let start = store.find_start().unwrap().unwrap().0.slot();
        let tip = store.find_tip().unwrap().unwrap().0.slot();
        assert_eq!(tip, last, "tip must be preserved");
        assert_eq!(start, last - max_slots, "converges to the max_slots window");
    }

    /// `remove_before` deletes older entries even with no entry near the cutoff
    /// (the old `approximate_slot(±180)` lookup silently skipped this case).
    #[test]
    fn remove_before_handles_sparse_wal() {
        let store = TestStore::memory().unwrap();

        let old = [1_000u64, 1_001, 1_002];
        let new = [1_000_000u64, 1_000_001, 1_000_002];
        let entries: Vec<_> = old.iter().chain(new.iter()).map(|s| entry(*s)).collect();
        store.append_entries(&entries).unwrap();

        // Cutoff sits in the empty gap, far from any entry.
        store.remove_before(500_000).unwrap();

        assert_eq!(slots(&store), new.to_vec(), "old cluster removed, new kept");
    }

    /// "remove before slot" keeps the cutoff slot itself.
    #[test]
    fn remove_before_keeps_entry_at_cutoff_slot() {
        let store = TestStore::memory().unwrap();
        let entries: Vec<_> = [10u64, 20, 30, 40].iter().map(|s| entry(*s)).collect();
        store.append_entries(&entries).unwrap();

        store.remove_before(30).unwrap();

        assert_eq!(slots(&store), vec![30, 40], "cutoff slot is retained");
    }
}
