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
    backends::InMemoryBackend, Database, Durability, ReadOnlyTable, ReadTransaction,
    ReadableDatabase, ReadableTable, TableDefinition, TableError, WriteTransaction,
};
use tracing::warn;

/// The database's own page cache. It never shares one with fjall.
const PAGE_CACHE_BYTES: usize = 32 * 1024 * 1024;

/// Hits whose access time waits for the next write. Past this many, further
/// hits keep their entries' old access times.
const MAX_TOUCHED: usize = 4096;

pub const FILE_NAME: &str = "cache.redb";

/// Namespace → format version.
const VERSIONS: TableDefinition<&str, u32> = TableDefinition::new("cache-versions");

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
    /// values. A file redb finds corrupt or of an old format is deleted and
    /// recreated empty; one another process holds, or that fails to open for
    /// any other reason, such as its permissions, is an error that names it.
    pub fn open(dir: impl AsRef<Path>, budget: u64) -> Result<Self, CacheError> {
        let dir = dir.as_ref();
        let path = dir.join(FILE_NAME);

        std::fs::create_dir_all(dir)
            .map_err(|error| CacheError::Disk(format!("creating {}: {error}", dir.display())))?;

        match Self::create(&path, budget) {
            Ok(tier) => Ok(tier),
            Err(redb::Error::DatabaseAlreadyOpen) => {
                Err(CacheError::InUse(path.display().to_string()))
            }
            Err(error) if !is_corrupt(&error) => Err(CacheError::Disk(format!(
                "opening {}: {error}",
                path.display()
            ))),
            Err(error) => {
                warn!(path = %path.display(), %error, "cache file unreadable, recreating it empty");

                std::fs::remove_file(&path).map_err(|error| {
                    CacheError::Disk(format!("deleting {}: {error}", path.display()))
                })?;

                Self::create(&path, budget).map_err(|error| {
                    CacheError::Disk(format!("creating {}: {error}", path.display()))
                })
            }
        }
    }

    /// A tier that keeps its database in memory, for tests and ephemeral
    /// domains.
    pub fn memory(budget: u64) -> Result<Self, CacheError> {
        let db = Database::builder()
            .set_cache_size(PAGE_CACHE_BYTES)
            .create_with_backend(InMemoryBackend::new())
            .map_err(CacheError::disk)?;

        Self::initialized(db, budget).map_err(CacheError::disk)
    }

    fn create(path: &Path, budget: u64) -> Result<Self, redb::Error> {
        let db = Database::builder()
            .set_cache_size(PAGE_CACHE_BYTES)
            .set_repair_callback(|x| {
                warn!(progress = x.progress() * 100f64, "cache db is repairing")
            })
            .create(path)?;

        Self::initialized(db, budget)
    }

    fn initialized(db: Database, budget: u64) -> Result<Self, redb::Error> {
        let wx = db.begin_write()?;
        wx.open_table(VERSIONS)?;
        wx.open_table(USAGE)?;
        wx.open_table(CLOCK)?;
        wx.open_table(RANKS)?;
        wx.commit()?;

        Ok(Self {
            inner: Arc::new(Inner {
                db,
                budget,
                touched: Mutex::default(),
            }),
        })
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
        })
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

/// Whether an open failed on the file's contents rather than on access to it.
/// redb only reads the contents once it holds the file's lock, so no other
/// process is using a file found corrupt.
fn is_corrupt(error: &redb::Error) -> bool {
    match error {
        redb::Error::Corrupted(_) | redb::Error::UpgradeRequired(_) => true,
        redb::Error::Io(error) => error.kind() == std::io::ErrorKind::InvalidData,
        _ => false,
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

    #[cfg(unix)]
    #[test]
    fn a_file_it_may_not_write_is_an_error_and_kept() {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let dir = tempfile::tempdir().unwrap();
        let tier = RedbCacheTier::open(dir.path(), MIB).unwrap();
        tier.open_namespace("ns", 1).unwrap();
        put(&tier, "key", 10, 1);
        tier.flush().unwrap();

        let path = dir.path().join(FILE_NAME);
        let inode = std::fs::metadata(&path).unwrap().ino();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o444)).unwrap();

        let Err(error) = RedbCacheTier::open(dir.path(), MIB) else {
            panic!("a cache file it may not write opened");
        };
        assert!(
            error.to_string().contains(&path.display().to_string()),
            "{error}"
        );
        assert_eq!(std::fs::metadata(&path).unwrap().ino(), inode);
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
}
