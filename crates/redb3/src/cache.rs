//! The cache's disk tier (decision 0059): disposable entries in a redb file of
//! their own, within a byte budget, evicted lowest cost per byte first.
//!
//! Writes skip the fsync and [`flush`](CacheDiskTier::flush) makes them
//! durable, so a crash loses at most the entries written since the last flush.

use std::{
    path::Path,
    sync::{Arc, Mutex, PoisonError},
};

use bytes::Bytes;
use dolos_core::{cache::Cost, CacheDiskTier, CacheEntry, CacheError};
use redb::{
    backends::InMemoryBackend, Database, Durability, MultimapTableHandle as _, ReadOnlyTable,
    ReadTransaction, ReadableDatabase, ReadableTable, ReadableTableMetadata as _, TableDefinition,
    TableError, TableHandle as _, WriteTransaction,
};
use tracing::warn;

use crate::layout::{check_layout, check_unheld, is_corrupt};

/// The database's own page cache. It never shares one with fjall.
const PAGE_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// Hits whose access time waits for the next write. Past this many, further
/// hits keep their entries' old access times.
const MAX_TOUCHED: usize = 4096;

pub const FILE_NAME: &str = "cache.redb";

/// The layout of the file as a whole. Each namespace keeps its own version
/// besides.
pub const CACHE_FILE_VERSION: u32 = 1;

/// Namespace → format version, and the file's under [`FILE_VERSION_KEY`].
const VERSIONS: TableDefinition<&str, u32> = TableDefinition::new("cache-versions");

/// The empty name, which [`open_namespace`](CacheDiskTier::open_namespace)
/// refuses, so no namespace takes it.
const FILE_VERSION_KEY: &str = "";

/// Namespace → (entries, bytes).
const USAGE: TableDefinition<&str, (u64, u64)> = TableDefinition::new("cache-usage");

/// The access clock behind each entry's last access.
const CLOCK: TableDefinition<(), u64> = TableDefinition::new("cache-clock");

/// The eviction order across namespaces: rank → (namespace, key, size).
const RANKS: TableDefinition<&[u8], (&str, &[u8], u64)> = TableDefinition::new("cache-ranks");

type DataTable<'a> = TableDefinition<'a, &'static [u8], &'static [u8]>;

/// Key → (cost, size, last access).
type EntryTable<'a> = TableDefinition<'a, &'static [u8], (u64, u64, u64)>;

type KeyTable<'a> = TableDefinition<'a, &'static [u8], ()>;

/// The names of one namespace's tables.
struct Names {
    data: String,
    entries: String,
    previous: String,
}

impl Names {
    fn of(namespace: &str) -> Self {
        Self {
            data: format!("cache-data:{namespace}"),
            entries: format!("cache-entries:{namespace}"),
            previous: format!("cache-previous:{namespace}"),
        }
    }

    fn data(&self) -> DataTable<'_> {
        TableDefinition::new(&self.data)
    }

    fn entries(&self) -> EntryTable<'_> {
        TableDefinition::new(&self.entries)
    }

    fn previous(&self) -> KeyTable<'_> {
        TableDefinition::new(&self.previous)
    }
}

/// An entry's place in the eviction order, as the memory tier ranks it: lowest
/// cost per byte first, then least recently used. `used` is unique, so no two
/// ranks are equal.
fn rank(cost: Cost, size: u64, used: u64) -> [u8; 24] {
    let per_byte = (u128::from(cost) << 64) / u128::from(size.max(1));

    let mut rank = [0; 24];
    rank[..16].copy_from_slice(&per_byte.to_be_bytes());
    rank[16..].copy_from_slice(&used.to_be_bytes());
    rank
}

/// The cache's disk tier over one redb database. Clones share it.
#[derive(Clone)]
pub struct RedbCacheTier {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    budget: u64,
    touched: Mutex<Vec<(String, Bytes)>>,
}

impl RedbCacheTier {
    /// Opens `<dir>/cache.redb`, holding at most `budget` bytes of keys and
    /// values. A file redb finds corrupt or cut short, one laid out by another
    /// release, or one recording another [`CACHE_FILE_VERSION`] or none over
    /// entries is deleted and recreated empty, with one warning; one another
    /// process holds, or that fails to open for any other reason, such as its
    /// permissions, is an error that names it.
    pub fn open(dir: impl AsRef<Path>, budget: u64) -> Result<Self, CacheError> {
        let dir = dir.as_ref();
        let path = dir.join(FILE_NAME);

        std::fs::create_dir_all(dir)
            .map_err(|error| CacheError::Disk(format!("creating {}: {error}", dir.display())))?;

        let found = match Self::create(&path, budget) {
            Ok(Ok(tier)) => return Ok(tier),
            Ok(Err(found)) => found,
            Err(redb::Error::DatabaseAlreadyOpen) => {
                return Err(CacheError::InUse(path.display().to_string()));
            }
            Err(error) if !is_corrupt(&error) => {
                return Err(CacheError::Disk(format!(
                    "opening {}: {error}",
                    path.display()
                )));
            }
            Err(error) => FileVersion::Unreadable(error.to_string()),
        };

        warn!(
            path = %path.display(),
            %found,
            expected = CACHE_FILE_VERSION,
            "cache file incompatible, recreating it empty"
        );

        std::fs::remove_file(&path)
            .map_err(|error| CacheError::Disk(format!("deleting {}: {error}", path.display())))?;

        match Self::create(&path, budget) {
            Ok(Ok(tier)) => Ok(tier),
            Ok(Err(found)) => Err(CacheError::Disk(format!(
                "creating {}: a new file reads as {found}",
                path.display()
            ))),
            Err(error) => Err(CacheError::Disk(format!(
                "creating {}: {error}",
                path.display()
            ))),
        }
    }

    /// Fails as [`CacheError::InUse`] while another process holds
    /// `<dir>/cache.redb`. Unlike [`open`](Self::open) it never creates,
    /// resets or writes the file, so it can run before the state is admitted.
    pub fn check_unheld(dir: impl AsRef<Path>) -> Result<(), CacheError> {
        let path = dir.as_ref().join(FILE_NAME);

        match check_unheld(&path) {
            Ok(()) => Ok(()),
            Err(redb::Error::DatabaseAlreadyOpen) => {
                Err(CacheError::InUse(path.display().to_string()))
            }
            Err(error) => Err(CacheError::Disk(format!(
                "opening {}: {error}",
                path.display()
            ))),
        }
    }

    /// A tier that keeps its database in memory, for tests and ephemeral
    /// domains.
    pub fn memory(budget: u64) -> Result<Self, CacheError> {
        let db = Database::builder()
            .set_cache_size(PAGE_CACHE_BYTES)
            .create_with_backend(InMemoryBackend::new())
            .map_err(CacheError::disk)?;

        Self::initialized(db, budget)
            .map_err(CacheError::disk)?
            .map_err(|found| CacheError::Disk(format!("a new tier reads as {found}")))
    }

    /// The tier over the file at `path`, or what it records when that is not
    /// this release's layout.
    fn create(path: &Path, budget: u64) -> Result<Result<Self, FileVersion>, redb::Error> {
        check_layout(path)?;

        let db = Database::builder()
            .set_cache_size(PAGE_CACHE_BYTES)
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "cache db is repairing")
            })
            .create(path)?;

        Self::initialized(db, budget)
    }

    fn initialized(db: Database, budget: u64) -> Result<Result<Self, FileVersion>, redb::Error> {
        let wx = db.begin_write()?;
        let mut versions = wx.open_table(VERSIONS)?;
        let recorded = versions
            .get(FILE_VERSION_KEY)?
            .map(|version| version.value());
        let namespaces = versions.len()?;
        wx.open_table(USAGE)?;
        wx.open_table(CLOCK)?;
        wx.open_table(RANKS)?;

        // Namespace tables open lazily on use, so open them typed here to
        // find a layout of another release while it can still be recreated.
        let names: Vec<String> = wx
            .list_tables()?
            .map(|table| table.name().to_string())
            .chain(
                wx.list_multimap_tables()?
                    .map(|table| table.name().to_string()),
            )
            .collect();

        let mut holds_namespaces = namespaces > 0;

        for name in &names {
            match name.split_once(':') {
                Some(("cache-data", _)) => drop(wx.open_table(DataTable::new(name))?),
                Some(("cache-entries", _)) => drop(wx.open_table(EntryTable::new(name))?),
                Some(("cache-previous", _)) => drop(wx.open_table(KeyTable::new(name))?),
                _ => continue,
            }

            holds_namespaces = true;
        }

        match recorded {
            Some(CACHE_FILE_VERSION) => {}
            Some(other) => return Ok(Err(FileVersion::Other(other))),
            None if holds_namespaces => return Ok(Err(FileVersion::Unmarked)),
            None => {
                versions.insert(FILE_VERSION_KEY, CACHE_FILE_VERSION)?;
            }
        }

        drop(versions);
        wx.commit()?;

        Ok(Ok(Self {
            inner: Arc::new(Inner {
                db,
                budget,
                touched: Mutex::default(),
            }),
        }))
    }

    pub fn budget(&self) -> u64 {
        self.inner.budget
    }

    /// Bytes of keys and values held across every namespace.
    pub fn total_bytes(&self) -> Result<u64, CacheError> {
        let rx = self.read()?;
        let usage = rx.open_table(USAGE).map_err(CacheError::disk)?;
        total_bytes(&usage).map_err(CacheError::disk)
    }

    fn read(&self) -> Result<ReadTransaction, CacheError> {
        self.inner.db.begin_read().map_err(CacheError::disk)
    }

    /// Runs `apply` in one write transaction, after the access times of the
    /// hits since the last one.
    fn write(
        &self,
        durability: Durability,
        apply: impl FnOnce(&WriteTransaction) -> Result<(), redb::Error>,
    ) -> Result<(), CacheError> {
        self.write_wiping(durability, None, apply)
    }

    /// [`write`](Self::write) for an `apply` that wipes `wiped`, whose hits
    /// are dropped rather than written: redb 3.1 frees the pages of a table
    /// written and deleted in one transaction as if they were committed, and
    /// panics on it in debug builds.
    fn write_wiping(
        &self,
        durability: Durability,
        wiped: Option<&str>,
        apply: impl FnOnce(&WriteTransaction) -> Result<(), redb::Error>,
    ) -> Result<(), CacheError> {
        let mut touched = std::mem::take(&mut *lock(&self.inner.touched));

        if let Some(wiped) = wiped {
            touched.retain(|(namespace, _)| namespace != wiped);
        }

        let run = || -> Result<(), redb::Error> {
            let mut wx = self.inner.db.begin_write()?;
            wx.set_durability(durability)?;

            if matches!(durability, Durability::Immediate) {
                wx.set_quick_repair(true);
            }

            for (namespace, key) in touched {
                touch(&wx, &namespace, &key)?;
            }

            apply(&wx)?;
            wx.commit()?;
            Ok(())
        };

        run().map_err(CacheError::disk)
    }

    /// The keys of a namespace's table, none if no write ever created it.
    fn read_keys<V: redb::Value + 'static>(
        &self,
        def: TableDefinition<'_, &'static [u8], V>,
    ) -> Result<Vec<Bytes>, CacheError> {
        let rx = self.read()?;

        let Some(table) = open_read(&rx, def).map_err(CacheError::disk)? else {
            return Ok(Vec::new());
        };

        let keys = table.iter().map_err(CacheError::disk)?;

        keys.map(|item| {
            let (key, _) = item.map_err(CacheError::disk)?;
            Ok(Bytes::copy_from_slice(key.value()))
        })
        .collect()
    }

    fn usage(&self, namespace: &str) -> Result<(u64, u64), CacheError> {
        let rx = self.read()?;
        let usage = rx.open_table(USAGE).map_err(CacheError::disk)?;
        let found = usage.get(namespace).map_err(CacheError::disk)?;

        Ok(found.map(|usage| usage.value()).unwrap_or_default())
    }
}

impl CacheDiskTier for RedbCacheTier {
    fn open_namespace(&self, namespace: &str, version: u32) -> Result<(), CacheError> {
        if namespace == FILE_VERSION_KEY {
            return Err(CacheError::UnknownNamespace(namespace.to_string()));
        }

        let known = {
            let rx = self.read()?;
            let versions = rx.open_table(VERSIONS).map_err(CacheError::disk)?;
            let found = versions.get(namespace).map_err(CacheError::disk)?;
            found.map(|version| version.value())
        };

        if known == Some(version) {
            return Ok(());
        }

        self.write_wiping(Durability::None, Some(namespace), |wx| {
            if known.is_some() {
                wipe(wx, namespace)?;
            }

            wx.open_table(VERSIONS)?.insert(namespace, version)?;
            Ok(())
        })?;

        if let Some(found) = known {
            warn!(
                namespace,
                found,
                expected = version,
                "cache namespace kept under another format version, its entries were wiped"
            );
        }

        Ok(())
    }

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<CacheEntry>, CacheError> {
        let names = Names::of(namespace);
        let rx = self.read()?;

        let found = || -> Result<Option<CacheEntry>, redb::Error> {
            let (Some(data), Some(entries)) = (
                open_read(&rx, names.data())?,
                open_read(&rx, names.entries())?,
            ) else {
                return Ok(None);
            };

            let (Some(value), Some(entry)) = (data.get(key)?, entries.get(key)?) else {
                return Ok(None);
            };

            let (cost, _, _) = entry.value();
            Ok(Some(CacheEntry::new(value.value().to_vec(), cost)))
        };

        let found = found().map_err(CacheError::disk)?;

        if found.is_some() {
            let mut touched = lock(&self.inner.touched);

            if touched.len() < MAX_TOUCHED {
                touched.push((namespace.to_string(), Bytes::copy_from_slice(key)));
            }
        }

        Ok(found)
    }

    fn put(&self, namespace: &str, key: &[u8], value: &[u8], cost: Cost) -> Result<(), CacheError> {
        let budget = self.inner.budget;
        let size = (key.len() + value.len()) as u64;

        self.write(Durability::None, |wx| {
            remove(wx, namespace, key)?;

            if size > budget {
                return Ok(());
            }

            let used = tick(wx)?;
            let ranked = rank(cost, size, used);

            let victims = {
                let usage = wx.open_table(USAGE)?;
                let mut room = budget.saturating_sub(total_bytes(&usage)?);

                let ranks = wx.open_table(RANKS)?;
                let mut victims = Vec::new();

                for item in ranks.range(..ranked.as_slice())? {
                    if room >= size {
                        break;
                    }

                    let (_, victim) = item?;
                    let (namespace, key, size) = victim.value();
                    victims.push((namespace.to_string(), key.to_vec()));
                    room += size;
                }

                if room < size {
                    return Ok(());
                }

                victims
            };

            for (namespace, key) in victims {
                remove(wx, &namespace, &key)?;
            }

            let names = Names::of(namespace);
            wx.open_table(names.data())?.insert(key, value)?;
            wx.open_table(names.entries())?
                .insert(key, (cost, size, used))?;
            wx.open_table(RANKS)?
                .insert(ranked.as_slice(), (namespace, key, size))?;

            add_usage(wx, namespace, 1, size as i128)
        })
    }

    fn remove(&self, namespace: &str, key: &[u8]) -> Result<(), CacheError> {
        self.write(Durability::None, |wx| remove(wx, namespace, key).map(drop))
    }

    fn wipe(&self, namespace: &str) -> Result<(), CacheError> {
        self.write_wiping(Durability::None, Some(namespace), |wx| wipe(wx, namespace))
    }

    fn previous_keys(&self, namespace: &str) -> Result<Vec<Bytes>, CacheError> {
        self.read_keys(Names::of(namespace).previous())
    }

    fn keys(&self, namespace: &str) -> Result<Vec<Bytes>, CacheError> {
        self.read_keys(Names::of(namespace).entries())
    }

    fn bytes(&self, namespace: &str) -> Result<u64, CacheError> {
        Ok(self.usage(namespace)?.1)
    }

    fn entries(&self, namespace: &str) -> Result<u64, CacheError> {
        Ok(self.usage(namespace)?.0)
    }

    fn flush(&self) -> Result<(), CacheError> {
        self.write(Durability::Immediate, |_| Ok(()))
    }
}

/// What a cache file records in place of this release's
/// [`CACHE_FILE_VERSION`].
#[derive(Debug)]
enum FileVersion {
    Other(u32),
    /// No version, over namespaces a release before the file version kept.
    Unmarked,
    /// Contents redb cannot read: damage, a cut-short file, or a layout of
    /// another release.
    Unreadable(String),
}

impl std::fmt::Display for FileVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Other(version) => write!(f, "version {version}"),
            Self::Unmarked => f.write_str("no version over namespaces"),
            Self::Unreadable(error) => write!(f, "unreadable ({error})"),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Opens a table for reading, or none if no write ever created it.
fn open_read<K: redb::Key + 'static, V: redb::Value + 'static>(
    rx: &ReadTransaction,
    def: TableDefinition<'_, K, V>,
) -> Result<Option<ReadOnlyTable<K, V>>, redb::Error> {
    match rx.open_table(def) {
        Ok(table) => Ok(Some(table)),
        Err(TableError::TableDoesNotExist(_)) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn total_bytes(usage: &impl ReadableTable<&'static str, (u64, u64)>) -> Result<u64, redb::Error> {
    let mut total = 0;

    for item in usage.iter()? {
        let (_, usage) = item?;
        total += usage.value().1;
    }

    Ok(total)
}

fn add_usage(
    wx: &WriteTransaction,
    namespace: &str,
    entries: i64,
    bytes: i128,
) -> Result<(), redb::Error> {
    let mut usage = wx.open_table(USAGE)?;
    let (known_entries, known_bytes) = usage
        .get(namespace)?
        .map(|usage| usage.value())
        .unwrap_or_default();

    let entries = known_entries.saturating_add_signed(entries);
    let bytes = (i128::from(known_bytes) + bytes).max(0) as u64;

    if entries == 0 {
        usage.remove(namespace)?;
    } else {
        usage.insert(namespace, (entries, bytes))?;
    }

    Ok(())
}

fn tick(wx: &WriteTransaction) -> Result<u64, redb::Error> {
    let mut clock = wx.open_table(CLOCK)?;
    let used = clock.get(())?.map(|used| used.value()).unwrap_or(0) + 1;
    clock.insert((), used)?;
    Ok(used)
}

/// Makes a hit entry the most recently used, if it is still kept.
fn touch(wx: &WriteTransaction, namespace: &str, key: &[u8]) -> Result<(), redb::Error> {
    let names = Names::of(namespace);

    let Some((cost, size, used)) = wx
        .open_table(names.entries())?
        .get(key)?
        .map(|entry| entry.value())
    else {
        return Ok(());
    };

    let now = tick(wx)?;

    let mut ranks = wx.open_table(RANKS)?;
    ranks.remove(rank(cost, size, used).as_slice())?;
    ranks.insert(rank(cost, size, now).as_slice(), (namespace, key, size))?;

    wx.open_table(names.entries())?
        .insert(key, (cost, size, now))?;

    Ok(())
}

/// Drops one entry, reporting whether it was kept.
fn remove(wx: &WriteTransaction, namespace: &str, key: &[u8]) -> Result<bool, redb::Error> {
    let names = Names::of(namespace);

    let Some((cost, size, used)) = wx
        .open_table(names.entries())?
        .remove(key)?
        .map(|entry| entry.value())
    else {
        return Ok(false);
    };

    wx.open_table(names.data())?.remove(key)?;
    wx.open_table(RANKS)?
        .remove(rank(cost, size, used).as_slice())?;
    add_usage(wx, namespace, -1, -i128::from(size))?;

    Ok(true)
}

/// Drops every entry of `namespace`, recording its keys as its previous ones.
fn wipe(wx: &WriteTransaction, namespace: &str) -> Result<(), redb::Error> {
    let names = Names::of(namespace);

    let kept = {
        let entries = wx.open_table(names.entries())?;
        let mut kept = Vec::new();

        for item in entries.iter()? {
            let (key, entry) = item?;
            kept.push((key.value().to_vec(), entry.value()));
        }

        kept
    };

    wx.delete_table(names.previous())?;

    {
        let mut previous = wx.open_table(names.previous())?;
        let mut ranks = wx.open_table(RANKS)?;

        for (key, (cost, size, used)) in &kept {
            previous.insert(key.as_slice(), ())?;
            ranks.remove(rank(*cost, *size, *used).as_slice())?;
        }
    }

    wx.delete_table(names.data())?;
    wx.delete_table(names.entries())?;
    wx.open_table(USAGE)?.remove(namespace)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_logs::warned_while;

    const MIB: u64 = 1024 * 1024;

    fn put(tier: &RedbCacheTier, key: &str, size: usize, cost: Cost) {
        let value = vec![0; size - key.len()];
        tier.put("ns", key.as_bytes(), &value, cost).unwrap();
    }

    fn holds(tier: &RedbCacheTier, key: &str) -> bool {
        tier.get("ns", key.as_bytes()).unwrap().is_some()
    }

    fn sorted(mut keys: Vec<Bytes>) -> Vec<Bytes> {
        keys.sort();
        keys
    }

    #[test]
    fn an_immutable_entry_reads_back_after_a_reopen() {
        let dir = tempfile::tempdir().unwrap();

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        tier.put("ns", b"key", b"value", 7).unwrap();
        tier.flush().unwrap();
        drop(tier);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();

        let entry = tier.get("ns", b"key").unwrap().unwrap();
        assert_eq!(entry, CacheEntry::new(&b"value"[..], 7));
        assert_eq!(tier.entries("ns").unwrap(), 1);
        assert_eq!(tier.bytes("ns").unwrap(), 8);
    }

    #[test]
    fn a_new_format_version_wipes_the_namespace_and_keeps_its_keys() {
        let dir = tempfile::tempdir().unwrap();

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        tier.open_namespace("other", 1).unwrap();
        tier.put("ns", b"a", b"1", 1).unwrap();
        tier.put("ns", b"b", b"2", 1).unwrap();
        tier.put("other", b"a", b"3", 1).unwrap();
        tier.flush().unwrap();
        drop(tier);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 2).unwrap();

        assert_eq!(tier.get("ns", b"a").unwrap(), None);
        assert!(tier.keys("ns").unwrap().is_empty());
        assert_eq!(tier.bytes("ns").unwrap(), 0);
        assert_eq!(
            sorted(tier.previous_keys("ns").unwrap()),
            [Bytes::from("a"), Bytes::from("b")]
        );

        assert_eq!(tier.get("other", b"a").unwrap().unwrap().value, "3");
        assert_eq!(tier.total_bytes().unwrap(), 2);

        tier.put("ns", b"c", b"4", 1).unwrap();
        assert_eq!(tier.keys("ns").unwrap(), [Bytes::from("c")]);
    }

    /// Only a namespace kept under another version, older or newer, is wiped
    /// with a warning, which names it and both versions.
    #[test]
    fn a_new_format_version_logs_one_warning_naming_the_namespace() {
        let tier = RedbCacheTier::memory(MIB).unwrap();

        let warned = warned_while(|| {
            tier.open_namespace("ns", 1).unwrap();
            tier.open_namespace("ns", 1).unwrap();
            tier.open_namespace("other", 1).unwrap();
        });
        assert!(warned.is_empty(), "{warned:?}");

        for (found, expected) in [(1, 2), (2, 1)] {
            let warned = warned_while(|| tier.open_namespace("ns", expected).unwrap());

            assert_eq!(warned.len(), 1, "{found} to {expected}: {warned:?}");
            assert!(
                warned[0].contains(&format!(
                    "namespace=\"ns\" found={found} expected={expected} "
                )),
                "{found} to {expected}: {warned:?}"
            );
        }
    }

    #[test]
    fn the_same_format_version_keeps_the_namespace() {
        let tier = RedbCacheTier::memory(MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        tier.put("ns", b"a", b"1", 1).unwrap();

        tier.open_namespace("ns", 1).unwrap();

        assert!(holds(&tier, "a"));
        assert!(tier.previous_keys("ns").unwrap().is_empty());
    }

    #[test]
    fn the_budget_evicts_the_lowest_cost_per_byte_first() {
        let tier = RedbCacheTier::memory(100).unwrap();

        put(&tier, "large", 60, 60);
        put(&tier, "a", 10, 100);
        put(&tier, "b", 10, 100);
        put(&tier, "c", 10, 100);
        assert_eq!(tier.total_bytes().unwrap(), 90);

        put(&tier, "d", 20, 200);

        assert!(!holds(&tier, "large"));
        assert!(["a", "b", "c", "d"].iter().all(|k| holds(&tier, k)));
        assert_eq!(tier.total_bytes().unwrap(), 50);
        assert_eq!(tier.entries("ns").unwrap(), 4);
    }

    #[test]
    fn an_entry_worth_less_per_byte_pushes_nothing_out() {
        let tier = RedbCacheTier::memory(100).unwrap();

        for key in ["a", "b", "c", "d", "e"] {
            put(&tier, key, 20, 200);
        }

        put(&tier, "large", 50, 50);

        assert!(!holds(&tier, "large"));
        assert!(["a", "b", "c", "d", "e"].iter().all(|k| holds(&tier, k)));
        assert_eq!(tier.total_bytes().unwrap(), 100);
    }

    #[test]
    fn the_least_recently_read_goes_first_among_equals() {
        let tier = RedbCacheTier::memory(30).unwrap();

        put(&tier, "a", 10, 10);
        put(&tier, "b", 10, 10);
        put(&tier, "c", 10, 10);
        assert!(holds(&tier, "a"));

        put(&tier, "d", 10, 10);

        assert!(!holds(&tier, "b"));
        assert!(["a", "c", "d"].iter().all(|k| holds(&tier, k)));
    }

    #[test]
    fn eviction_crosses_namespaces() {
        let tier = RedbCacheTier::memory(30).unwrap();

        tier.put("cheap", b"a", &[0; 19], 1).unwrap();
        put(&tier, "b", 10, 100);
        put(&tier, "c", 20, 200);

        assert_eq!(tier.get("cheap", b"a").unwrap(), None);
        assert_eq!(tier.bytes("cheap").unwrap(), 0);
        assert!(holds(&tier, "b") && holds(&tier, "c"));
    }

    #[test]
    fn a_replacement_releases_the_old_bytes() {
        let tier = RedbCacheTier::memory(100).unwrap();

        put(&tier, "a", 40, 1);
        put(&tier, "a", 70, 1);

        assert_eq!(tier.total_bytes().unwrap(), 70);
        assert_eq!(tier.entries("ns").unwrap(), 1);
    }

    #[test]
    fn a_wipe_records_the_keys_it_drops() {
        let tier = RedbCacheTier::memory(MIB).unwrap();

        put(&tier, "a", 10, 1);
        put(&tier, "b", 10, 1);
        tier.remove("ns", b"b").unwrap();
        tier.wipe("ns").unwrap();

        assert_eq!(tier.previous_keys("ns").unwrap(), [Bytes::from("a")]);
        assert_eq!(tier.total_bytes().unwrap(), 0);

        put(&tier, "c", 10, 1);
        tier.wipe("ns").unwrap();
        assert_eq!(tier.previous_keys("ns").unwrap(), [Bytes::from("c")]);
    }

    /// A hit's access time, still to be written, does not stop a wipe or a
    /// new format version from dropping its namespace.
    #[test]
    fn a_wipe_after_a_hit_drops_the_namespace() {
        let tier = RedbCacheTier::memory(MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        tier.open_namespace("other", 1).unwrap();

        put(&tier, "a", 10, 1);
        tier.put("other", b"x", b"0123456789", 1).unwrap();
        assert!(holds(&tier, "a"));
        assert!(tier.get("other", b"x").unwrap().is_some());

        tier.wipe("ns").unwrap();
        assert_eq!(tier.previous_keys("ns").unwrap(), [Bytes::from("a")]);
        assert!(tier.get("other", b"x").unwrap().is_some());

        put(&tier, "b", 10, 1);
        assert!(holds(&tier, "b"));

        tier.open_namespace("ns", 2).unwrap();
        assert!(tier.keys("ns").unwrap().is_empty());
        assert_eq!(tier.entries("other").unwrap(), 1);
    }

    #[test]
    fn a_corrupt_file_is_recreated_empty() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(FILE_NAME), vec![0xa5; 64 * 1024]).unwrap();

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        assert!(tier.keys("ns").unwrap().is_empty());

        tier.put("ns", b"key", b"value", 1).unwrap();
        assert!(holds(&tier, "key"));
    }

    #[test]
    fn a_file_cut_short_is_recreated_empty() {
        let dir = tempfile::tempdir().unwrap();
        drop(RedbCacheTier::open(dir.path(), MIB).unwrap());

        let file = std::fs::OpenOptions::new()
            .write(true)
            .open(dir.path().join(FILE_NAME))
            .unwrap();
        file.set_len(100).unwrap();
        drop(file);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        put(&tier, "key", 10, 1);
        assert!(holds(&tier, "key"));
    }

    /// redb asserts the file holds the layout its header records before it
    /// reports any error.
    #[test]
    fn a_file_cut_short_past_its_header_is_recreated_empty() {
        for len in [320, 4096, 8192] {
            let dir = tempfile::tempdir().unwrap();
            drop(RedbCacheTier::open(dir.path(), MIB).unwrap());

            let file = std::fs::OpenOptions::new()
                .write(true)
                .open(dir.path().join(FILE_NAME))
                .unwrap();
            file.set_len(len).unwrap();
            drop(file);

            let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
            tier.open_namespace("ns", 1).unwrap();
            put(&tier, "key", 10, 1);
            assert!(holds(&tier, "key"), "cut to {len} bytes");
        }
    }

    #[test]
    fn a_file_not_a_whole_number_of_pages_is_recreated_empty() {
        let dir = tempfile::tempdir().unwrap();
        drop(RedbCacheTier::open(dir.path(), MIB).unwrap());

        let path = dir.path().join(FILE_NAME);
        let len = std::fs::metadata(&path).unwrap().len();
        let file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        file.set_len(len + 100).unwrap();
        drop(file);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        put(&tier, "key", 10, 1);
        assert!(holds(&tier, "key"));
    }

    #[test]
    fn a_file_whose_tables_changed_type_is_recreated_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join(FILE_NAME)).unwrap();
        let wx = db.begin_write().unwrap();
        wx.open_table(TableDefinition::<&str, u64>::new("cache-usage"))
            .unwrap()
            .insert("ns", 1)
            .unwrap();
        wx.commit().unwrap();
        drop(db);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        assert_eq!(tier.entries("ns").unwrap(), 0);
        put(&tier, "key", 10, 1);
        assert!(holds(&tier, "key"));
    }

    #[test]
    fn a_file_whose_namespace_tables_changed_type_is_recreated_empty() {
        let dir = tempfile::tempdir().unwrap();
        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        put(&tier, "key", 10, 1);
        tier.flush().unwrap();
        drop(tier);

        let db = Database::create(dir.path().join(FILE_NAME)).unwrap();
        let wx = db.begin_write().unwrap();
        wx.delete_table(Names::of("ns").entries()).unwrap();
        wx.open_table(TableDefinition::<&[u8], u64>::new("cache-entries:ns"))
            .unwrap()
            .insert(b"key".as_slice(), 1)
            .unwrap();
        wx.commit().unwrap();
        drop(db);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        assert!(tier.keys("ns").unwrap().is_empty());
        put(&tier, "other", 10, 1);
        assert!(holds(&tier, "other"));
        tier.wipe("ns").unwrap();
    }

    /// redb checks a table's kind before its types.
    #[test]
    fn a_file_whose_table_became_a_multimap_is_recreated_empty() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::create(dir.path().join(FILE_NAME)).unwrap();
        let wx = db.begin_write().unwrap();
        wx.open_multimap_table(redb::MultimapTableDefinition::<&str, u64>::new(
            "cache-usage",
        ))
        .unwrap()
        .insert("ns", 1)
        .unwrap();
        wx.commit().unwrap();
        drop(db);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        assert_eq!(tier.entries("ns").unwrap(), 0);
        put(&tier, "key", 10, 1);
        assert!(holds(&tier, "key"));
    }

    #[cfg(unix)]
    #[test]
    fn a_file_it_may_not_write_is_an_error_and_kept() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let dir = tempfile::tempdir().unwrap();
        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        put(&tier, "key", 10, 1);
        tier.flush().unwrap();
        drop(tier);

        let path = dir.path().join(FILE_NAME);
        let inode = std::fs::metadata(&path).unwrap().ino();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();

        // root writes it regardless
        if std::fs::OpenOptions::new().write(true).open(&path).is_ok() {
            return;
        }

        let Err(error) = RedbCacheTier::open(dir.path(), MIB) else {
            panic!("a cache file it may not write opened");
        };
        assert!(matches!(error, CacheError::Disk(_)), "{error}");
        assert!(
            error.to_string().contains(&path.display().to_string()),
            "{error}"
        );
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);

        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        assert!(holds(&tier, "key"));
    }

    #[test]
    fn a_file_in_use_is_an_error_that_names_it() {
        let dir = tempfile::tempdir().unwrap();
        let _held = RedbCacheTier::open(dir.path(), MIB).unwrap();

        let Err(error) = RedbCacheTier::open(dir.path(), MIB) else {
            panic!("a second open of the cache file succeeded");
        };

        let path = dir.path().join(FILE_NAME);
        assert!(
            error.to_string().contains(&path.display().to_string()),
            "{error}"
        );
        assert!(path.exists());
    }

    /// Rewrites the file's version, removing it on `None`.
    fn mark(dir: &Path, version: Option<u32>) {
        let db = Database::create(dir.join(FILE_NAME)).unwrap();
        let wx = db.begin_write().unwrap();

        {
            let mut versions = wx.open_table(VERSIONS).unwrap();

            match version {
                Some(version) => versions.insert(FILE_VERSION_KEY, version).unwrap(),
                None => versions.remove(FILE_VERSION_KEY).unwrap(),
            };
        }

        wx.commit().unwrap();
    }

    fn recorded(dir: &Path) -> Option<u32> {
        let db = Database::create(dir.join(FILE_NAME)).unwrap();
        let rx = db.begin_read().unwrap();
        let versions = rx.open_table(VERSIONS).unwrap();
        let version = versions.get(FILE_VERSION_KEY).unwrap().map(|v| v.value());
        version
    }

    /// A file holding one entry under `ns`, flushed.
    fn holding_an_entry(dir: &Path) {
        let tier = RedbCacheTier::open(dir, MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        put(&tier, "key", 10, 1);
        tier.flush().unwrap();
    }

    #[test]
    fn a_new_file_records_its_version() {
        let dir = tempfile::tempdir().unwrap();
        drop(RedbCacheTier::open(dir.path(), MIB).unwrap());

        assert_eq!(recorded(dir.path()), Some(CACHE_FILE_VERSION));
    }

    #[test]
    fn a_file_of_this_version_keeps_its_entries() {
        let dir = tempfile::tempdir().unwrap();
        holding_an_entry(dir.path());

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        assert!(holds(&tier, "key"));
    }

    #[test]
    fn a_file_of_another_version_is_recreated_empty() {
        for version in [0, CACHE_FILE_VERSION + 1] {
            let dir = tempfile::tempdir().unwrap();
            holding_an_entry(dir.path());
            mark(dir.path(), Some(version));

            let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
            tier.open_namespace("ns", 1).unwrap();
            assert!(!holds(&tier, "key"), "version {version}");
            assert_eq!(tier.total_bytes().unwrap(), 0);
            drop(tier);

            assert_eq!(recorded(dir.path()), Some(CACHE_FILE_VERSION));
        }
    }

    #[test]
    fn a_file_without_a_version_over_namespaces_is_recreated_empty() {
        let dir = tempfile::tempdir().unwrap();
        holding_an_entry(dir.path());
        mark(dir.path(), None);

        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        assert!(!holds(&tier, "key"));
        drop(tier);

        assert_eq!(recorded(dir.path()), Some(CACHE_FILE_VERSION));
    }

    /// As a release before the file version left a cache it never used.
    #[test]
    fn a_file_without_a_version_or_namespaces_is_stamped() {
        let dir = tempfile::tempdir().unwrap();
        drop(RedbCacheTier::open(dir.path(), MIB).unwrap());
        mark(dir.path(), None);

        drop(RedbCacheTier::open(dir.path(), MIB).unwrap());
        assert_eq!(recorded(dir.path()), Some(CACHE_FILE_VERSION));
    }

    #[test]
    fn the_file_version_is_no_namespace() {
        let dir = tempfile::tempdir().unwrap();
        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();

        assert!(matches!(
            tier.open_namespace(FILE_VERSION_KEY, 1),
            Err(CacheError::UnknownNamespace(_))
        ));
    }

    #[test]
    fn a_file_held_by_another_handle_is_a_lock_error_and_kept() {
        let dir = tempfile::tempdir().unwrap();
        holding_an_entry(dir.path());
        mark(dir.path(), Some(CACHE_FILE_VERSION + 1));

        let path = dir.path().join(FILE_NAME);
        // the holder marks the file in use as it opens it
        let held = Database::create(&path).unwrap();
        // Windows locks are mandatory: a held file cannot be read through
        // another handle
        #[cfg(unix)]
        let before = std::fs::read(&path).unwrap();

        assert!(matches!(
            RedbCacheTier::open(dir.path(), MIB),
            Err(CacheError::InUse(_))
        ));
        assert!(matches!(
            RedbCacheTier::check_unheld(dir.path()),
            Err(CacheError::InUse(_))
        ));
        #[cfg(unix)]
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(held);
        assert_eq!(recorded(dir.path()), Some(CACHE_FILE_VERSION + 1));
    }

    #[test]
    fn the_unheld_check_leaves_a_file_of_another_version_as_it_is() {
        let dir = tempfile::tempdir().unwrap();
        holding_an_entry(dir.path());
        mark(dir.path(), Some(CACHE_FILE_VERSION + 1));
        let path = dir.path().join(FILE_NAME);
        let before = std::fs::read(&path).unwrap();

        RedbCacheTier::check_unheld(dir.path()).unwrap();

        assert_eq!(std::fs::read(&path).unwrap(), before);
        assert_eq!(recorded(dir.path()), Some(CACHE_FILE_VERSION + 1));
    }

    #[test]
    fn the_unheld_check_creates_no_file() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("cache");

        RedbCacheTier::check_unheld(&missing).unwrap();

        assert!(!missing.exists());
    }
}
