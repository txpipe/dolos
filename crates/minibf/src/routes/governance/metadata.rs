use blockfrost_openapi::models::{DrepsInnerMetadata, DrepsInnerMetadataError};
use futures::{
    future::{BoxFuture, Shared},
    FutureExt as _,
};
use pallas::{
    crypto::hash::{Hash, Hasher},
    ledger::primitives::conway::Anchor,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    future::Future,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Mutex, MutexGuard, OnceLock,
    },
    time::{Duration, Instant, SystemTime},
};

use crate::mapping::{anchor_metadata_from_body, anchor_offchain_body};

// The fetch itself, with its public-address gate, redirect limit, `ipfs://`
// gateways and size cap, is the one `/governance/dreps/{drep_id}/metadata`
// runs (`anchor_offchain_body`). This module only keeps what it returns, since
// a list page asks for up to a hundred anchors at a time.

/// An anchor pins its content by hash, so a verified fetch never has to be
/// repeated: its body goes to the [`OffchainStore`] on disk and the rendered
/// metadata stays in memory within these caps. A failed fetch is kept in
/// memory for `FAILURE_TTL`, so a dead host costs one timeout per window
/// instead of one per page render.
const FAILURE_TTL: Duration = Duration::from_secs(10 * 60);
const MAX_CACHE_ENTRIES: usize = 4096;
const MAX_CACHE_BYTES: usize = 64 * 1024 * 1024;

type CacheKey = (String, Hash<32>);

struct CacheEntry {
    metadata: DrepsInnerMetadata,
    stored_at: Instant,
    size: usize,
}

impl CacheEntry {
    fn is_stale(&self, now: Instant) -> bool {
        // a verified fetch never goes stale: the hash pins the content
        self.metadata.error.is_some() && now.duration_since(self.stored_at) > FAILURE_TTL
    }
}

struct MetadataCache {
    entries: HashMap<CacheKey, CacheEntry>,
    // insertion order, oldest first, for eviction
    order: VecDeque<CacheKey>,
    bytes: usize,
    max_entries: usize,
    max_bytes: usize,
}

impl MetadataCache {
    fn new(max_entries: usize, max_bytes: usize) -> Self {
        Self {
            entries: HashMap::new(),
            order: VecDeque::new(),
            bytes: 0,
            max_entries,
            max_bytes,
        }
    }

    fn get(&self, key: &CacheKey, now: Instant) -> Option<DrepsInnerMetadata> {
        let entry = self.entries.get(key)?;

        (!entry.is_stale(now)).then(|| entry.metadata.clone())
    }

    fn insert(&mut self, key: CacheKey, metadata: DrepsInnerMetadata, now: Instant) {
        // the hex `bytes` dominate an entry, so the budget is a bound on
        // the order of the real footprint rather than an exact figure
        let size = key.0.len() + metadata.bytes.as_ref().map_or(0, |x| x.len());

        let entry = CacheEntry {
            metadata,
            stored_at: now,
            size,
        };

        match self.entries.insert(key.clone(), entry) {
            Some(old) => self.bytes -= old.size,
            None => self.order.push_back(key),
        }

        self.bytes += size;

        while self.entries.len() > self.max_entries || self.bytes > self.max_bytes {
            let Some(oldest) = self.order.pop_front() else {
                break;
            };

            if let Some(old) = self.entries.remove(&oldest) {
                self.bytes -= old.size;
            }
        }
    }
}

fn cache() -> MutexGuard<'static, MetadataCache> {
    static CACHE: OnceLock<Mutex<MetadataCache>> = OnceLock::new();

    CACHE
        .get_or_init(|| Mutex::new(MetadataCache::new(MAX_CACHE_ENTRIES, MAX_CACHE_BYTES)))
        .lock()
        // plain data under the lock: a panic elsewhere leaves nothing half-done
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

type InFlight = Shared<BoxFuture<'static, DrepsInnerMetadata>>;

/// Runs `load` for `key` unless a load of that key is already running, in
/// which case it awaits that one instead. Rows of one page, and pages served
/// at once, often miss on the same anchor before its first fetch is back; this
/// keeps them to one outbound request and one write.
async fn coalesced<F>(key: CacheKey, load: F) -> DrepsInnerMetadata
where
    F: Future<Output = DrepsInnerMetadata> + Send + 'static,
{
    static IN_FLIGHT: OnceLock<Mutex<HashMap<CacheKey, InFlight>>> = OnceLock::new();

    fn in_flight() -> MutexGuard<'static, HashMap<CacheKey, InFlight>> {
        IN_FLIGHT
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    let shared = in_flight()
        .entry(key.clone())
        .or_insert_with(|| {
            async move {
                let metadata = load.await;

                // whoever polls the load to its end retires it, so the next
                // miss after it starts afresh
                in_flight().remove(&key);

                metadata
            }
            .boxed()
            .shared()
        })
        .clone();

    shared.await
}

fn errored(mut out: DrepsInnerMetadata, error: DrepsInnerMetadataError) -> DrepsInnerMetadata {
    out.error = Some(Box::new(error));
    out
}

/// Verified bodies, content-addressed by their hash: one file per hash under
/// `<storage.path>/offchain`, so a fetch outlives the process. The hash check
/// on the way back in guards the file the way it guarded the download, and a
/// failure never reaches the disk.
#[derive(Clone)]
pub struct OffchainStore {
    dir: PathBuf,
    budget: u64,
}

impl OffchainStore {
    pub const DIR: &'static str = "offchain";

    /// `budget` is the disk the store may occupy, in bytes; zero turns it
    /// off and leaves only the in-process cache.
    pub fn new(storage_path: &Path, budget: u64) -> Self {
        Self {
            dir: storage_path.join(Self::DIR),
            budget,
        }
    }

    /// The store under `storage_path`, weighed against `budget` the first
    /// time this process opens it. The write tally that paces later walks
    /// starts from zero with the process, so without this pass a cache left
    /// by an earlier run, or one a lowered budget now overflows, would stay
    /// over the bound until another fifth of the budget had been written, and
    /// for good on a node that only reads.
    pub async fn open(storage_path: &Path, budget: u64) -> Self {
        static OPENED: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();

        let store = Self::new(storage_path, budget);

        let first = OPENED
            .get_or_init(Default::default)
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(store.dir.clone());

        if first {
            store.enforce_budget().await;
        }

        store
    }

    fn path(&self, hash: &Hash<32>) -> PathBuf {
        self.dir.join(hex::encode(hash))
    }

    async fn read(&self, hash: Hash<32>) -> Option<Vec<u8>> {
        let path = self.path(&hash);

        let body = tokio::task::spawn_blocking(move || std::fs::read(path))
            .await
            .ok()?
            .ok()?;

        (Hasher::<256>::hash(&body) == hash).then_some(body)
    }

    /// Weigh the directory and drop the oldest files until it fits the
    /// budget again, taking it a fifth below so the next walk is not
    /// immediate. Age is the order the files were written, not the order
    /// they were last read: the anchor hash pins each body for good, so no
    /// entry is worth more than another and only the bound matters.
    async fn enforce_budget(&self) {
        let dir = self.dir.clone();
        let budget = self.budget;

        let _ = tokio::task::spawn_blocking(move || {
            let mut files: Vec<(SystemTime, u64, PathBuf)> = std::fs::read_dir(&dir)?
                .filter_map(Result::ok)
                .filter_map(|entry| {
                    let meta = entry.metadata().ok()?;

                    if !meta.is_file() {
                        return None;
                    }

                    Some((meta.modified().ok()?, meta.len(), entry.path()))
                })
                .collect();

            let total: u64 = files.iter().map(|(_, size, _)| size).sum();

            let Some(mut excess) = total.checked_sub(budget - budget / 5) else {
                return Ok(());
            };

            files.sort_by_key(|(written, _, _)| *written);

            for (_, size, path) in files {
                if excess == 0 {
                    break;
                }

                if std::fs::remove_file(&path).is_ok() {
                    excess = excess.saturating_sub(size);
                }
            }

            std::io::Result::Ok(())
        })
        .await;
    }

    async fn write(&self, hash: Hash<32>, body: Vec<u8>) {
        static SEQ: AtomicU64 = AtomicU64::new(0);

        // Weighing the directory costs a walk, so it happens once per
        // fifth-of-a-budget written rather than per file, which caps the
        // overshoot at that same fifth.
        static UNWEIGHED: AtomicU64 = AtomicU64::new(0);

        if self.budget == 0 {
            return;
        }

        let dir = self.dir.clone();
        let path = self.path(&hash);
        let size = body.len() as u64;

        // Written beside its final name and renamed into place, so a
        // concurrent reader never sees a partial body. `rename` replaces an
        // existing file on every platform std supports, Windows included,
        // which lets a corrupt body be overwritten and two writers of one
        // hash both land, the last with the same bytes as the first.
        let written = tokio::task::spawn_blocking(move || {
            std::fs::create_dir_all(&dir)?;

            let tmp = dir.join(format!(
                "{}.{}.{}.tmp",
                hex::encode(hash),
                std::process::id(),
                SEQ.fetch_add(1, Ordering::Relaxed)
            ));

            let placed = std::fs::write(&tmp, &body).and_then(|()| std::fs::rename(&tmp, path));

            // a body that never made it into place leaves nothing behind
            if placed.is_err() {
                let _ = std::fs::remove_file(&tmp);
            }

            placed
        })
        .await;

        // the store is a cache: a body that could not be kept is fetched
        // again next time
        if let Err(err) = written.map_err(std::io::Error::other).and_then(|x| x) {
            tracing::warn!(%err, "failed to store off-chain metadata");
            return;
        }

        let unweighed = UNWEIGHED.fetch_add(size, Ordering::Relaxed) + size;

        if unweighed >= self.budget / 5 {
            UNWEIGHED.store(0, Ordering::Relaxed);
            self.enforce_budget().await;
        }
    }
}

/// The metadata of a verified body.
fn verified(out: DrepsInnerMetadata, body: &[u8]) -> DrepsInnerMetadata {
    match anchor_metadata_from_body(&out.url, body) {
        Ok(metadata) => DrepsInnerMetadata {
            json_metadata: Some(metadata.json),
            bytes: Some(metadata.bytes),
            ..out
        },
        // the spec keeps `json_metadata` and `bytes` null on failed
        // validation and reports the failure through `error`
        Err(error) => errored(out, error),
    }
}

/// The metadata of `anchor` before any fetch: the anchor fields alone.
fn unfetched(anchor: &Anchor) -> DrepsInnerMetadata {
    DrepsInnerMetadata {
        url: anchor.url.clone(),
        hash: hex::encode(anchor.content_hash),
        json_metadata: None,
        bytes: None,
        error: None,
    }
}

/// The metadata of `anchor`, as both `/governance/dreps` and
/// `/governance/dreps/{drep_id}/metadata` serve it: from the in-process cache,
/// else from the store on disk, else fetched and kept in both.
pub async fn fetch_drep_metadata(
    store: &OffchainStore,
    ipfs_gateways: &[String],
    anchor: &Anchor,
) -> DrepsInnerMetadata {
    let key = (anchor.url.clone(), anchor.content_hash);

    if let Some(cached) = cache().get(&key, Instant::now()) {
        return cached;
    }

    let load = load_drep_metadata(store.clone(), ipfs_gateways.to_vec(), anchor.clone());

    coalesced(key, load).await
}

async fn load_drep_metadata(
    store: OffchainStore,
    ipfs_gateways: Vec<String>,
    anchor: Anchor,
) -> DrepsInnerMetadata {
    let key = (anchor.url.clone(), anchor.content_hash);

    // a load that finished between the caller's miss and this one has
    // already left its answer here
    if let Some(cached) = cache().get(&key, Instant::now()) {
        return cached;
    }

    let out = unfetched(&anchor);

    let metadata = match store.read(anchor.content_hash).await {
        Some(body) => verified(out, &body),
        None => {
            let fetched =
                anchor_offchain_body(&anchor.url, anchor.content_hash.as_ref(), &ipfs_gateways)
                    .await;

            match fetched {
                Ok(body) => {
                    store.write(anchor.content_hash, body.clone()).await;
                    verified(out, &body)
                }
                Err(error) => errored(out, error),
            }
        }
    };

    cache().insert(key, metadata.clone(), Instant::now());

    metadata
}

/// Keeps `body` in the in-process cache as the verified metadata of
/// `anchor`, as a fetch would.
#[cfg(test)]
pub(super) fn cache_verified_body(anchor: &Anchor, body: &[u8]) {
    let key = (anchor.url.clone(), anchor.content_hash);

    cache().insert(key, verified(unfetched(anchor), body), Instant::now());
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockfrost_openapi::models::dreps_inner_metadata_error::Code as MetadataError;

    fn temp_root(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);

        std::env::temp_dir().join(format!(
            "dolos-offchain-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }

    #[tokio::test]
    async fn store_keeps_verified_bodies_only() {
        let root = temp_root("verified");
        let store = OffchainStore::new(&root, 1024 * 1024);

        let body = br#"{"body":"hello"}"#.to_vec();
        let hash = Hasher::<256>::hash(&body);

        assert_eq!(store.read(hash).await, None);

        store.write(hash, body.clone()).await;
        assert_eq!(store.read(hash).await, Some(body.clone()));

        // a body that no longer matches its name is not served
        std::fs::write(store.path(&hash), b"tampered").unwrap();
        assert_eq!(store.read(hash).await, None);

        // and the next verified write replaces it, on Windows as well
        store.write(hash, body.clone()).await;
        assert_eq!(store.read(hash).await, Some(body));

        std::fs::remove_dir_all(root).unwrap();
    }

    /// A write that cannot be put in place, here because a directory holds
    /// its name, takes its temporary file away with it.
    #[tokio::test]
    async fn a_failed_write_leaves_no_temporary_file() {
        let root = temp_root("failed");
        let store = OffchainStore::new(&root, 1024 * 1024);

        let body = br#"{"body":"blocked"}"#.to_vec();
        let hash = Hasher::<256>::hash(&body);

        std::fs::create_dir_all(store.path(&hash)).unwrap();

        store.write(hash, body).await;

        let leftovers: Vec<_> = std::fs::read_dir(root.join(OffchainStore::DIR))
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .filter(|name| name.to_string_lossy().ends_with(".tmp"))
            .collect();

        assert!(leftovers.is_empty(), "left behind: {leftovers:?}");

        std::fs::remove_dir_all(root).unwrap();
    }

    /// The store is a cache beside the state and archive data, so it has to
    /// stay inside its budget however many anchors the chain carries.
    #[tokio::test]
    async fn store_evicts_the_oldest_bodies_to_stay_inside_its_budget() {
        let root = temp_root("budget");
        // room for two bodies, and the walk takes it a fifth under
        let store = OffchainStore::new(&root, 2048);

        let mut written = vec![];

        for (age, byte) in [(4u64, b'a'), (3, b'b'), (2, b'c'), (1, b'd')] {
            let body = vec![byte; 700];
            let hash = Hasher::<256>::hash(&body);
            store.write(hash, body).await;

            // one file per second apart, oldest first, since a filesystem
            // may not separate four writes in the same instant
            let file = std::fs::File::options()
                .write(true)
                .open(store.path(&hash))
                .unwrap();
            file.set_modified(SystemTime::now() - Duration::from_secs(age))
                .unwrap();

            written.push(hash);
        }

        store.enforce_budget().await;

        let kept: Vec<bool> =
            futures::future::join_all(written.iter().map(|hash| store.read(*hash)))
                .await
                .into_iter()
                .map(|body| body.is_some())
                .collect();

        assert_eq!(kept, vec![false, false, true, true], "oldest go first");

        let total = stored_bytes(&root);
        assert!(total <= 2048, "{total} bytes left behind");

        std::fs::remove_dir_all(root).unwrap();
    }

    /// The bytes the store keeps under `root`.
    fn stored_bytes(root: &Path) -> u64 {
        // the bodies live under the store's own directory; `root` holds only
        // that directory, whose size is the filesystem's (4096 on ext4)
        std::fs::read_dir(root.join(OffchainStore::DIR))
            .unwrap()
            .filter_map(Result::ok)
            .map(|e| e.metadata().unwrap().len())
            .sum()
    }

    /// A restart forgets what the earlier run wrote, so the first opening
    /// weighs what is already there: a cache left over a lowered budget
    /// shrinks back without a single new write.
    #[tokio::test]
    async fn opening_trims_a_cache_left_over_the_budget() {
        let root = temp_root("reopen");
        let roomy = OffchainStore::new(&root, 1024 * 1024);

        for byte in [b'a', b'b', b'c', b'd'] {
            let body = vec![byte; 700];
            roomy.write(Hasher::<256>::hash(&body), body).await;
        }

        assert_eq!(stored_bytes(&root), 2800);

        // reopened under room for two bodies
        OffchainStore::open(&root, 2048).await;

        let total = stored_bytes(&root);
        assert!(total <= 2048, "{total} bytes left behind");

        std::fs::remove_dir_all(root).unwrap();
    }

    /// Misses on one anchor while its load is out wait for that load rather
    /// than starting their own, and the next miss after it loads afresh.
    #[tokio::test]
    async fn concurrent_misses_share_one_load() {
        let loads = std::sync::Arc::new(AtomicU64::new(0));
        let (release, gate) = futures::channel::oneshot::channel::<()>();
        let gate = gate.shared();

        let load = || {
            let loads = loads.clone();
            let gate = gate.clone();

            async move {
                loads.fetch_add(1, Ordering::SeqCst);
                let _ = gate.await;
                metadata("coalesced", None)
            }
        };

        let key = key("coalesced");
        let mut first = Box::pin(coalesced(key.clone(), load()));
        let mut second = Box::pin(coalesced(key.clone(), load()));

        // both are waiting while the first load is held at the gate
        assert!(futures::poll!(&mut first).is_pending());
        assert!(futures::poll!(&mut second).is_pending());

        release.send(()).unwrap();

        assert_eq!(first.await, second.await);
        assert_eq!(loads.load(Ordering::SeqCst), 1);

        coalesced(key, load()).await;
        assert_eq!(loads.load(Ordering::SeqCst), 2);
    }

    /// A zero budget leaves nothing on disk, bodies an earlier run kept
    /// included.
    #[tokio::test]
    async fn opening_with_a_zero_budget_empties_the_store() {
        let root = temp_root("emptied");
        let body = b"{}".to_vec();

        OffchainStore::new(&root, 1024)
            .write(Hasher::<256>::hash(&body), body)
            .await;
        assert_eq!(stored_bytes(&root), 2);

        OffchainStore::open(&root, 0).await;
        assert_eq!(stored_bytes(&root), 0);

        std::fs::remove_dir_all(root).unwrap();
    }

    /// A zero budget is the way to keep metadata out of the data directory
    /// entirely; the in-process cache still answers.
    #[tokio::test]
    async fn a_zero_budget_writes_nothing_to_disk() {
        let root = temp_root("nodisk");
        let store = OffchainStore::new(&root, 0);

        let body = b"{}".to_vec();
        let hash = Hasher::<256>::hash(&body);

        store.write(hash, body).await;

        assert!(store.read(hash).await.is_none());
        assert!(!root.join(OffchainStore::DIR).exists());
    }

    #[test]
    fn verified_bodies_render_json_or_a_decode_error() {
        let out = DrepsInnerMetadata {
            url: "https://x.io/d.json".to_string(),
            hash: String::new(),
            json_metadata: None,
            bytes: None,
            error: None,
        };

        let json = verified(out.clone(), br#"{"a":1}"#);
        assert_eq!(json.json_metadata, Some(serde_json::json!({"a": 1})));
        assert_eq!(json.bytes.as_deref(), Some("\\x7b2261223a317d"));
        assert!(json.error.is_none());

        let broken = verified(out, b"not json");
        assert_eq!(broken.json_metadata, None);
        assert_eq!(broken.bytes, None);
        assert_eq!(broken.error.unwrap().code, MetadataError::DecodeError);
    }

    fn metadata(url: &str, error: Option<DrepsInnerMetadataError>) -> DrepsInnerMetadata {
        DrepsInnerMetadata {
            url: url.to_string(),
            hash: String::new(),
            json_metadata: None,
            bytes: Some("\\x00".repeat(4)),
            error: error.map(Box::new),
        }
    }

    fn key(url: &str) -> CacheKey {
        (url.to_string(), Hash::<32>::from([0u8; 32]))
    }

    #[test]
    fn cache_keeps_verified_fetches_and_expires_failed_ones() {
        let mut cache = MetadataCache::new(16, usize::MAX);
        let now = Instant::now();
        let later = now + FAILURE_TTL + Duration::from_secs(1);

        cache.insert(key("ok"), metadata("ok", None), now);
        cache.insert(
            key("bad"),
            metadata(
                "bad",
                Some(DrepsInnerMetadataError::new(
                    MetadataError::ConnectionError,
                    "bad".to_string(),
                )),
            ),
            now,
        );

        assert!(cache.get(&key("ok"), now).is_some());
        assert!(cache.get(&key("bad"), now).is_some());
        assert!(cache.get(&key("ok"), later).is_some());
        assert!(cache.get(&key("bad"), later).is_none());
        assert!(cache.get(&key("missing"), now).is_none());
    }

    #[test]
    fn cache_evicts_oldest_past_its_caps() {
        let now = Instant::now();

        let mut by_count = MetadataCache::new(2, usize::MAX);
        for url in ["a", "b", "c"] {
            by_count.insert(key(url), metadata(url, None), now);
        }
        assert!(by_count.get(&key("a"), now).is_none());
        assert!(by_count.get(&key("b"), now).is_some());
        assert!(by_count.get(&key("c"), now).is_some());

        // each entry weighs its url plus 16 bytes of hex
        let mut by_bytes = MetadataCache::new(usize::MAX, 40);
        for url in ["a", "b", "c"] {
            by_bytes.insert(key(url), metadata(url, None), now);
        }
        assert!(by_bytes.get(&key("a"), now).is_none());
        assert!(by_bytes.get(&key("b"), now).is_some());
        assert!(by_bytes.get(&key("c"), now).is_some());
        assert_eq!(by_bytes.bytes, 34);

        // replacing an entry swaps its weight instead of adding to it
        by_bytes.insert(key("c"), metadata("c", None), now);
        assert_eq!(by_bytes.bytes, 34);
        assert_eq!(by_bytes.entries.len(), by_bytes.order.len());
    }
}
