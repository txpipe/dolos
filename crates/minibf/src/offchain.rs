use axum::body::Bytes;
use blockfrost_openapi::models::{DrepsInnerMetadata, DrepsInnerMetadataError};
use dolos_core::{CacheEntry, CacheError, CacheNamespace, CacheStore};
use pallas::{
    crypto::hash::Hasher,
    ledger::primitives::{conway::Anchor, PoolMetadata},
};
use std::{convert::Infallible, future::Future, path::Path, time::Duration};

use crate::mapping::{
    anchor_metadata_from_body, anchor_offchain_body, offchain_hash_mismatch_error,
    pool_fetch_error, pool_offchain_body, PoolOffchainMetadata,
};

/// The maximum concurrent fetches for each metadata cache.
pub const MAX_CONCURRENT_FETCHES: usize = 16;

/// The maximum pending fetches for each metadata cache, including those
/// running.
pub const MAX_PENDING_LOADS: usize = 4 * MAX_CONCURRENT_FETCHES;

const FAILURE_TTL: Duration = Duration::from_secs(10 * 60);

/// Verified governance-anchor bodies, keyed by content hash, in memory and on
/// disk.
pub const OFFCHAIN: CacheNamespace = CacheNamespace::immutable("offchain", 1)
    .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_LOADS);

/// Failed governance-anchor fetches, keyed by content hash and URL, in memory
/// only.
pub const OFFCHAIN_FAILURES: CacheNamespace =
    CacheNamespace::tip_scoped("offchain-failures", 1, FAILURE_TTL);

/// Verified pool bodies, keyed by content hash, in memory and on disk.
pub const POOL_OFFCHAIN: CacheNamespace = CacheNamespace::immutable("pool-offchain", 1)
    .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_LOADS);

/// Failed pool fetches, keyed by content hash and URL, in memory only.
pub const POOL_OFFCHAIN_FAILURES: CacheNamespace =
    CacheNamespace::tip_scoped("pool-offchain-failures", 1, FAILURE_TTL);

#[derive(Clone, Copy)]
struct MetadataCache {
    bodies: &'static str,
    failures: &'static str,
}

const ANCHOR_CACHE: MetadataCache = MetadataCache {
    bodies: OFFCHAIN.name,
    failures: OFFCHAIN_FAILURES.name,
};

const POOL_CACHE: MetadataCache = MetadataCache {
    bodies: POOL_OFFCHAIN.name,
    failures: POOL_OFFCHAIN_FAILURES.name,
};

#[derive(Clone)]
struct MetadataLocation {
    url: String,
    hash: Vec<u8>,
}

impl MetadataLocation {
    fn holds_hash(&self, body: &[u8]) -> bool {
        Hasher::<256>::hash(body).as_ref() == self.hash
    }

    fn failure_key(&self) -> Vec<u8> {
        [self.hash.as_slice(), self.url.as_bytes()].concat()
    }
}

impl From<&Anchor> for MetadataLocation {
    fn from(anchor: &Anchor) -> Self {
        Self {
            url: anchor.url.clone(),
            hash: anchor.content_hash.to_vec(),
        }
    }
}

impl From<&PoolMetadata> for MetadataLocation {
    fn from(metadata: &PoolMetadata) -> Self {
        Self {
            url: metadata.url.clone(),
            hash: metadata.hash.to_vec(),
        }
    }
}

const LEGACY_DIR: &str = "offchain";

/// Registers the pool and governance metadata cache namespaces.
pub fn register<C: CacheStore>(cache: &C) {
    for namespace in [
        OFFCHAIN,
        OFFCHAIN_FAILURES,
        POOL_OFFCHAIN,
        POOL_OFFCHAIN_FAILURES,
    ] {
        if let Err(error) = cache.register(namespace.clone()) {
            tracing::warn!(namespace = namespace.name, %error, "cannot register cache namespace");
        }
    }
}

/// Removes the directory earlier versions kept verified bodies in.
pub fn remove_legacy_dir(storage_path: &Path) {
    let dir = storage_path.join(LEGACY_DIR);

    if !dir.is_dir() {
        return;
    }

    tracing::info!(path = %dir.display(), "removing off-chain metadata directory, no longer used");

    if let Err(error) = std::fs::remove_dir_all(&dir) {
        tracing::warn!(path = %dir.display(), %error, "cannot remove off-chain metadata directory");
    }
}

/// What a miss gets while the pending fetch limit is reached.
#[derive(Debug, Clone, Copy)]
pub enum OnOverload {
    /// Return the anchor without metadata.
    Unfetched,
    /// Fetch within the caller's request, under the concurrent fetch limit.
    Inline,
}

#[derive(Debug)]
struct FetchFailed {
    url: String,
    error: Option<DrepsInnerMetadataError>,
}

impl std::fmt::Display for FetchFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.error {
            Some(error) => f.write_str(&error.message),
            None => f.write_str("off-chain metadata fetch failed"),
        }
    }
}

impl std::error::Error for FetchFailed {}

enum MetadataBody {
    Verified(Bytes),
    Failed(Option<DrepsInnerMetadataError>),
    Unfetched,
}

/// The metadata of `anchor` before any fetch: the anchor fields alone.
pub fn unfetched(anchor: &Anchor) -> DrepsInnerMetadata {
    DrepsInnerMetadata {
        url: anchor.url.clone(),
        hash: hex::encode(anchor.content_hash),
        json_metadata: None,
        bytes: None,
        error: None,
    }
}

fn anchor_metadata(anchor: &Anchor, body: MetadataBody) -> DrepsInnerMetadata {
    let mut out = unfetched(anchor);

    match body {
        MetadataBody::Verified(body) => match anchor_metadata_from_body(&anchor.url, &body) {
            Ok(metadata) => {
                out.json_metadata = Some(metadata.json);
                out.bytes = Some(metadata.bytes);
            }
            Err(error) => out.error = Some(Box::new(error)),
        },
        MetadataBody::Failed(error) => out.error = error.map(Box::new),
        MetadataBody::Unfetched => {}
    }

    out
}

fn cached<C: CacheStore>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
) -> Option<MetadataBody> {
    if let Some(body) = cache.get(namespaces.bodies, &location.hash) {
        if location.holds_hash(&body) {
            return Some(MetadataBody::Verified(body));
        }

        tracing::warn!(url = %location.url, "dropping cached off-chain metadata that fails its hash");

        if let Err(error) = cache.remove(namespaces.bodies, &location.hash) {
            tracing::warn!(%error, "cannot remove cached off-chain metadata");
        }
    }

    let failure = cache.get(namespaces.failures, &location.failure_key())?;
    Some(MetadataBody::Failed(serde_json::from_slice(&failure).ok()?))
}

async fn cached_body<C: CacheStore>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
) -> Option<MetadataBody> {
    let (cache, location) = (cache.clone(), location.clone());

    tokio::task::spawn_blocking(move || cached(&cache, &location, namespaces))
        .await
        .ok()
        .flatten()
}

/// Returns cached governance-anchor metadata without a network request.
pub async fn cached_anchor_metadata<C: CacheStore>(
    cache: &C,
    anchor: &Anchor,
) -> Option<DrepsInnerMetadata> {
    cached_body(cache, &MetadataLocation::from(anchor), ANCHOR_CACHE)
        .await
        .map(|body| anchor_metadata(anchor, body))
}

/// Returns governance-anchor metadata from the cache, or fetches and caches its
/// body.
pub async fn fetch_anchor_metadata<C: CacheStore>(
    cache: &C,
    ipfs_gateways: &[String],
    anchor: &Anchor,
    on_overload: OnOverload,
) -> DrepsInnerMetadata {
    let fetch = |anchor: Anchor| {
        let gateways = ipfs_gateways.to_vec();

        async move { anchor_offchain_body(&anchor.url, anchor.content_hash.as_ref(), &gateways).await }
    };

    load_anchor_metadata(cache, anchor, on_overload, fetch).await
}

async fn load_anchor_metadata<C, F, Fut>(
    cache: &C,
    anchor: &Anchor,
    on_overload: OnOverload,
    fetch: F,
) -> DrepsInnerMetadata
where
    C: CacheStore,
    F: Fn(Anchor) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, DrepsInnerMetadataError>> + Send + 'static,
{
    let body = load_body(
        cache,
        &MetadataLocation::from(anchor),
        ANCHOR_CACHE,
        on_overload,
        || {
            let fetched = fetch(anchor.clone());
            async move { fetched.await.map_err(Some) }
        },
    )
    .await;

    anchor_metadata(anchor, body)
}

/// Returns pool metadata and its error from the cache, or fetches and caches
/// its body.
pub async fn fetch_pool_metadata<C: CacheStore>(
    cache: &C,
    metadata: &PoolMetadata,
) -> (
    Option<PoolOffchainMetadata>,
    Option<DrepsInnerMetadataError>,
) {
    load_pool_metadata(cache, metadata, |metadata| async move {
        pool_offchain_body(&metadata.url)
            .await
            .map_err(|error| pool_fetch_error(&metadata.url, error))
    })
    .await
}

async fn load_pool_metadata<C, F, Fut>(
    cache: &C,
    metadata: &PoolMetadata,
    fetch: F,
) -> (
    Option<PoolOffchainMetadata>,
    Option<DrepsInnerMetadataError>,
)
where
    C: CacheStore,
    F: Fn(PoolMetadata) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>> + Send + 'static,
{
    let body = load_body(
        cache,
        &MetadataLocation::from(metadata),
        POOL_CACHE,
        OnOverload::Inline,
        || fetch(metadata.clone()),
    )
    .await;

    match body {
        MetadataBody::Verified(body) => (serde_json::from_slice(&body).ok(), None),
        MetadataBody::Failed(error) => (None, error),
        MetadataBody::Unfetched => (None, None),
    }
}

async fn load_body<C, F, Fut>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
    on_overload: OnOverload,
    fetch: F,
) -> MetadataBody
where
    C: CacheStore,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>> + Send + 'static,
{
    if let Some(cached) = cached_body(cache, location, namespaces).await {
        return cached;
    }

    let fill = fill_body(cache.clone(), location.clone(), namespaces, fetch());

    // A detached fill survives cancellation of the request that started it.
    let filled = match cache
        .get_or_fill_async(namespaces.bodies, &location.hash, fill)
        .await
    {
        Err(CacheError::Overloaded) => match on_overload {
            OnOverload::Unfetched => return MetadataBody::Unfetched,
            OnOverload::Inline => {
                let fill = fill_body(cache.clone(), location.clone(), namespaces, fetch());
                cache
                    .fill_inline(namespaces.bodies, &location.hash, fill)
                    .await
            }
        },
        filled => filled,
    };

    // A joined fetch of the same hash can fail at another URL.
    let filled = match filled {
        Err(error)
            if error
                .downcast_fill::<FetchFailed>()
                .is_some_and(|failed| failed.url != location.url) =>
        {
            let fill = fill_body(cache.clone(), location.clone(), namespaces, fetch());
            cache
                .fill_inline(namespaces.bodies, &location.hash, fill)
                .await
        }
        filled => filled,
    };

    match filled {
        Ok(body) if location.holds_hash(&body) => MetadataBody::Verified(body),
        Ok(_) => MetadataBody::Unfetched,
        Err(error) => match error.downcast_fill::<FetchFailed>() {
            Some(failed) => MetadataBody::Failed(failed.error.clone()),
            None => {
                tracing::warn!(url = %location.url, %error, "off-chain metadata not loaded");
                MetadataBody::Unfetched
            }
        },
    }
}

async fn fill_body<C, Fut>(
    cache: C,
    location: MetadataLocation,
    namespaces: MetadataCache,
    fetch: Fut,
) -> Result<CacheEntry, FetchFailed>
where
    C: CacheStore,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>>,
{
    let fetched = fetch.await.and_then(|body| {
        if location.holds_hash(&body) {
            return Ok(body);
        }

        let actual = Hasher::<256>::hash(&body);
        Err(Some(offchain_hash_mismatch_error(
            &location.url,
            &location.hash,
            actual.as_ref(),
        )))
    });

    match fetched {
        Ok(body) => Ok(CacheEntry::new(body, 0)),
        Err(failure) => {
            keep_failure(&cache, &location, namespaces, &failure);
            Err(FetchFailed {
                url: location.url,
                error: failure,
            })
        }
    }
}

fn keep_failure<C: CacheStore>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
    failure: &Option<DrepsInnerMetadataError>,
) {
    let Ok(value) = serde_json::to_vec(failure) else {
        return;
    };

    let kept = cache.refill(namespaces.failures, &location.failure_key(), move || {
        Ok::<_, Infallible>(CacheEntry::new(value, 0))
    });

    if let Err(error) = kept {
        tracing::warn!(url = %location.url, %error, "cannot keep off-chain metadata failure");
    }
}

/// Keeps `body` in the cache as the verified body of `anchor`, as a fetch
/// would.
#[cfg(test)]
pub(crate) async fn cache_verified_body<C: CacheStore>(cache: &C, anchor: &Anchor, body: &[u8]) {
    let body = body.to_vec();

    cache
        .get_or_fill_async(OFFCHAIN.name, anchor.content_hash.as_ref(), async move {
            Ok::<_, Infallible>(CacheEntry::new(body, 0))
        })
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockfrost_openapi::models::dreps_inner_metadata_error::Code as MetadataError;
    use dolos_core::{config::CacheConfig, Domain as _, QueryLimiter, TieredCache};
    use dolos_testing::toy_domain::ToyDomain;
    use std::{
        path::PathBuf,
        sync::{
            atomic::{AtomicU64, AtomicUsize, Ordering},
            Arc,
        },
    };
    use tokio::sync::Semaphore;

    fn memory_cache() -> impl CacheStore {
        let cache = TieredCache::memory(&CacheConfig::default(), QueryLimiter::default());
        register(&cache);
        cache
    }

    fn temp_root(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);

        std::env::temp_dir().join(format!(
            "dolos-offchain-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ))
    }

    fn anchor(body: &[u8]) -> Anchor {
        Anchor {
            url: format!("https://example.invalid/{}", hex::encode(&body[..4])),
            content_hash: Hasher::<256>::hash(body),
        }
    }

    fn connection_error(url: &str) -> DrepsInnerMetadataError {
        DrepsInnerMetadataError::new(MetadataError::ConnectionError, url.to_string())
    }

    type Fetch = futures::future::BoxFuture<'static, Result<Vec<u8>, DrepsInnerMetadataError>>;

    /// A fetch that counts the calls that ran and answers `body`.
    fn serving(body: &'static [u8], calls: &Arc<AtomicUsize>) -> impl Fn(Anchor) -> Fetch {
        let calls = calls.clone();

        move |_| {
            let calls = calls.clone();

            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(body.to_vec())
            })
        }
    }

    /// A fetch that answers `body` once the gate gives it a permit, keeping
    /// count of those running and the most that ran at once.
    #[derive(Clone)]
    struct Gated {
        gate: Arc<Semaphore>,
        calls: Arc<AtomicUsize>,
        running: Arc<AtomicUsize>,
        most: Arc<AtomicUsize>,
    }

    impl Default for Gated {
        fn default() -> Self {
            Self {
                gate: Arc::new(Semaphore::new(0)),
                calls: Arc::default(),
                running: Arc::default(),
                most: Arc::default(),
            }
        }
    }

    impl Gated {
        fn fetch(&self, body: Vec<u8>) -> impl Fn(Anchor) -> Fetch {
            let this = self.clone();

            move |_| {
                let (this, body) = (this.clone(), body.clone());

                Box::pin(async move {
                    this.calls.fetch_add(1, Ordering::SeqCst);
                    let now = this.running.fetch_add(1, Ordering::SeqCst) + 1;
                    this.most.fetch_max(now, Ordering::SeqCst);
                    let _ = this.gate.acquire().await;
                    this.running.fetch_sub(1, Ordering::SeqCst);
                    Ok(body)
                })
            }
        }

        async fn wait_for_running(&self, n: usize) {
            while self.running.load(Ordering::SeqCst) < n {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        }

        fn open(&self) {
            self.gate.add_permits(Semaphore::MAX_PERMITS / 2);
        }
    }

    const POOL_BODY: &[u8] = br#"{"name":"Pool","description":"A pool","ticker":"POOL","homepage":"https://example.com"}"#;

    fn pool_metadata(body: &[u8]) -> PoolMetadata {
        PoolMetadata {
            url: "https://example.invalid/pool.json".to_string(),
            hash: Hasher::<256>::hash(body).to_vec().into(),
        }
    }

    type PoolFetch =
        futures::future::BoxFuture<'static, Result<Vec<u8>, Option<DrepsInnerMetadataError>>>;

    fn serving_pool(
        body: &'static [u8],
        calls: &Arc<AtomicUsize>,
    ) -> impl Fn(PoolMetadata) -> PoolFetch {
        let calls = calls.clone();
        move |_| {
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(body.to_vec())
            })
        }
    }

    #[tokio::test]
    async fn pool_metadata_survives_restart_and_an_unavailable_url() {
        let dir = temp_root("pool-restart");
        let metadata = pool_metadata(POOL_BODY);
        let calls = Arc::new(AtomicUsize::new(0));
        let domain = persistent_domain(&dir);
        let (fetched, error) =
            load_pool_metadata(domain.cache(), &metadata, serving_pool(POOL_BODY, &calls)).await;
        assert_eq!(fetched.unwrap().ticker, "POOL");
        assert!(error.is_none());
        domain.cache().flush().unwrap();
        drop(domain);

        let domain = persistent_domain(&dir);
        let (kept, error) = fetch_pool_metadata(domain.cache(), &metadata).await;
        assert_eq!(kept.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        drop(domain);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn pool_metadata_uses_the_hash_when_the_url_body_changes() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let calls = Arc::new(AtomicUsize::new(0));
        load_pool_metadata(&cache, &metadata, serving_pool(POOL_BODY, &calls)).await;

        let changed = br#"{"name":"Other","description":"Changed","ticker":"NEW","homepage":"https://example.com"}"#;
        let (kept, error) =
            load_pool_metadata(&cache, &metadata, serving_pool(changed, &calls)).await;
        assert_eq!(kept.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        cache.wipe(POOL_OFFCHAIN.name).unwrap();
        let (missing, error) =
            load_pool_metadata(&cache, &metadata, serving_pool(changed, &calls)).await;
        assert!(missing.is_none());
        assert_eq!(error.unwrap().code, MetadataError::HashMismatch);
        assert!(cache
            .get(POOL_OFFCHAIN.name, metadata.hash.as_slice())
            .is_none());

        let updated = pool_metadata(changed);
        let (fetched, error) =
            load_pool_metadata(&cache, &updated, serving_pool(changed, &calls)).await;
        assert_eq!(fetched.unwrap().ticker, "NEW");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn pool_failures_expire_and_a_different_url_retries_immediately() {
        let ttl = Duration::from_millis(100);
        let cache = memory_cache();
        cache
            .register(CacheNamespace::tip_scoped(
                POOL_OFFCHAIN_FAILURES.name,
                1,
                ttl,
            ))
            .unwrap();
        let metadata = pool_metadata(POOL_BODY);
        let calls = Arc::new(AtomicUsize::new(0));
        let failure = crate::mapping::pool_connection_error(&metadata.url);
        let (missing, error) = load_pool_metadata(&cache, &metadata, |_| {
            let failure = failure.clone();
            async move { Err(Some(failure)) }
        })
        .await;
        assert!(missing.is_none());
        assert_eq!(error.unwrap(), failure);

        let location = MetadataLocation::from(&metadata);
        while cached_body(&cache, &location, POOL_CACHE).await.is_none() {
            tokio::task::yield_now().await;
        }
        let (missing, error) =
            load_pool_metadata(&cache, &metadata, serving_pool(POOL_BODY, &calls)).await;
        assert!(missing.is_none());
        assert_eq!(error.unwrap(), failure);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        tokio::time::sleep(ttl).await;
        let (fetched, error) =
            load_pool_metadata(&cache, &metadata, serving_pool(POOL_BODY, &calls)).await;
        assert_eq!(fetched.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        cache.wipe(POOL_OFFCHAIN.name).unwrap();
        load_pool_metadata(&cache, &metadata, |_| {
            let failure = failure.clone();
            async move { Err(Some(failure)) }
        })
        .await;
        while cached_body(&cache, &location, POOL_CACHE).await.is_none() {
            tokio::task::yield_now().await;
        }
        let mut moved = metadata.clone();
        moved.url.push_str("?mirror");
        let (fetched, error) =
            load_pool_metadata(&cache, &moved, serving_pool(POOL_BODY, &calls)).await;
        assert_eq!(fetched.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn pool_metadata_keeps_silent_failures_and_invalid_json_behavior() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let (missing, error) = load_pool_metadata(&cache, &metadata, |_| async { Err(None) }).await;
        assert!(missing.is_none());
        assert!(error.is_none());
        while cached_body(&cache, &MetadataLocation::from(&metadata), POOL_CACHE)
            .await
            .is_none()
        {
            tokio::task::yield_now().await;
        }
        let calls = Arc::new(AtomicUsize::new(0));
        let (missing, error) =
            load_pool_metadata(&cache, &metadata, serving_pool(POOL_BODY, &calls)).await;
        assert!(missing.is_none());
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        for body in [b"not json".as_slice(), b"{}".as_slice()] {
            let (missing, error) =
                load_pool_metadata(&cache, &pool_metadata(body), serving_pool(body, &calls)).await;
            assert!(missing.is_none());
            assert!(error.is_none());
        }
    }

    #[tokio::test]
    async fn pool_and_drep_fetch_policies_do_not_share_entries_or_failures() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let anchor = Anchor {
            url: metadata.url.clone(),
            content_hash: Hasher::<256>::hash(POOL_BODY),
        };
        cache_verified_body(&cache, &anchor, POOL_BODY).await;
        let (missing, error) = fetch_pool_metadata(&cache, &metadata).await;
        assert!(missing.is_none());
        assert_eq!(error.unwrap().code, MetadataError::ConnectionError);
        let drep = fetch_anchor_metadata(&cache, &[], &anchor, OnOverload::Inline).await;
        assert!(drep.error.is_none());
        assert!(drep.json_metadata.is_some());
    }

    #[tokio::test]
    async fn concurrent_pool_misses_share_one_fetch() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let gated = Gated::default();
        let requests: Vec<_> = (0..8)
            .map(|_| {
                let (cache, metadata, gated) = (cache.clone(), metadata.clone(), gated.clone());
                tokio::spawn(async move {
                    load_pool_metadata(&cache, &metadata, |_| {
                        let fetch = gated.fetch(POOL_BODY.to_vec());
                        let fetched = fetch(anchor(POOL_BODY));
                        async move { fetched.await.map_err(Some) }
                    })
                    .await
                })
            })
            .collect();
        gated.wait_for_running(1).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        gated.open();
        for request in requests {
            let (metadata, error) = request.await.unwrap();
            assert_eq!(metadata.unwrap().ticker, "POOL");
            assert!(error.is_none());
        }
        assert_eq!(gated.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_shared_failure_at_another_url_retries_the_own_url() {
        let cache = memory_cache();
        let dead = pool_metadata(POOL_BODY);
        let mut moved = dead.clone();
        moved.url = "https://mirror.invalid/pool.json".to_string();
        let failure = crate::mapping::pool_connection_error(&dead.url);
        let (started, ready) = tokio::sync::oneshot::channel::<()>();
        let release = Arc::new(tokio::sync::Notify::new());
        let first = tokio::spawn({
            let (cache, dead, failure, release) = (
                cache.clone(),
                dead.clone(),
                failure.clone(),
                release.clone(),
            );
            let started = std::sync::Mutex::new(Some(started));
            async move {
                load_pool_metadata(&cache, &dead, move |_| {
                    let (failure, release) = (failure.clone(), release.clone());
                    let started = started.lock().unwrap().take();
                    async move {
                        if let Some(started) = started {
                            let _ = started.send(());
                        }
                        release.notified().await;
                        Err(Some(failure))
                    }
                })
                .await
            }
        });
        ready.await.unwrap();

        let calls = Arc::new(AtomicUsize::new(0));
        let second = tokio::spawn({
            let (cache, moved, fetch) = (
                cache.clone(),
                moved.clone(),
                serving_pool(POOL_BODY, &calls),
            );
            async move { load_pool_metadata(&cache, &moved, fetch).await }
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        release.notify_one();

        let (missing, error) = first.await.unwrap();
        assert!(missing.is_none());
        assert_eq!(error.unwrap(), failure);
        let (fetched, error) = second.await.unwrap();
        assert_eq!(fetched.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_body_that_fails_its_hash_is_never_kept() {
        let cache = memory_cache();
        let calls = Arc::new(AtomicUsize::new(0));
        let anchor = anchor(br#"{"body":"expected"}"#);

        let fetch = serving(br#"{"body":"tampered"}"#, &calls);
        let got = load_anchor_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await;

        assert_eq!(got.error.unwrap().code, MetadataError::HashMismatch);
        assert!(got.bytes.is_none());
        assert_eq!(cache.get(OFFCHAIN.name, anchor.content_hash.as_ref()), None);
    }

    #[tokio::test]
    async fn a_corrupted_stored_body_is_dropped_on_read() {
        let cache = memory_cache();
        let anchor = anchor(br#"{"body":"hello"}"#);

        cache_verified_body(&cache, &anchor, b"corrupted").await;
        assert!(cache
            .get(OFFCHAIN.name, anchor.content_hash.as_ref())
            .is_some());

        assert_eq!(cached_anchor_metadata(&cache, &anchor).await, None);
        assert_eq!(cache.get(OFFCHAIN.name, anchor.content_hash.as_ref()), None);

        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = serving(br#"{"body":"hello"}"#, &calls);
        let got = load_anchor_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await;

        assert_eq!(
            got.json_metadata,
            Some(serde_json::json!({"body": "hello"}))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    fn persistent_domain(dir: &Path) -> ToyDomain {
        let domain = ToyDomain::new(Some(dolos_testing::fake_genesis_delta(1_000_000)), None)
            .with_persistent_cache(dir)
            .unwrap();

        register(domain.cache());
        domain
    }

    #[tokio::test]
    async fn a_verified_body_survives_a_restart() {
        let dir = temp_root("restart");
        let body: &'static [u8] = br#"{"body":"kept"}"#;
        let anchor = anchor(body);
        let calls = Arc::new(AtomicUsize::new(0));

        let domain = persistent_domain(&dir);
        let fetched = load_anchor_metadata(
            domain.cache(),
            &anchor,
            OnOverload::Unfetched,
            serving(body, &calls),
        )
        .await;
        domain.cache().flush().unwrap();
        drop(domain);

        let domain = persistent_domain(&dir);
        let kept = load_anchor_metadata(
            domain.cache(),
            &anchor,
            OnOverload::Unfetched,
            serving(body, &calls),
        )
        .await;

        assert_eq!(kept, fetched);
        assert!(kept.error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        drop(domain);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_failure_is_kept_for_ten_minutes() {
        assert_eq!(
            OFFCHAIN_FAILURES.validity,
            dolos_core::cache::Validity::TipScoped {
                ttl: Duration::from_secs(600)
            }
        );
    }

    /// A failure answers until its TTL passes, then the anchor is fetched
    /// again; the TTL is shortened here to keep the test fast.
    #[tokio::test]
    async fn a_failure_is_served_until_its_ttl_then_refetched() {
        let ttl = Duration::from_millis(200);
        let cache = memory_cache();
        cache
            .register(CacheNamespace::tip_scoped(OFFCHAIN_FAILURES.name, 1, ttl))
            .unwrap();

        let anchor = anchor(br#"{"body":"later"}"#);
        let calls = Arc::new(AtomicUsize::new(0));

        let failing = {
            let calls = calls.clone();

            move |anchor: Anchor| -> Fetch {
                let calls = calls.clone();

                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Err(connection_error(&anchor.url))
                })
            }
        };

        let failed = load_anchor_metadata(&cache, &anchor, OnOverload::Unfetched, &failing).await;
        assert_eq!(
            failed.error.as_ref().unwrap().code,
            MetadataError::ConnectionError
        );

        // the failure lands from a fill of its own
        while cached_anchor_metadata(&cache, &anchor).await.is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let served = load_anchor_metadata(&cache, &anchor, OnOverload::Unfetched, &failing).await;
        assert_eq!(served, failed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::sleep(ttl).await;

        let fetch = serving(br#"{"body":"later"}"#, &calls);
        let refetched = load_anchor_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await;
        assert!(refetched.error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_misses_on_one_anchor_make_one_fetch() {
        let cache = memory_cache();
        let body = br#"{"body":"shared"}"#.to_vec();
        let anchor = anchor(&body);
        let gated = Gated::default();

        let requests: Vec<_> = (0..8)
            .map(|_| {
                let (cache, anchor, fetch) =
                    (cache.clone(), anchor.clone(), gated.fetch(body.clone()));
                tokio::spawn(async move {
                    load_anchor_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await
                })
            })
            .collect();

        gated.wait_for_running(1).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        gated.open();

        for request in requests {
            let got = request.await.unwrap();
            assert_eq!(
                got.json_metadata,
                Some(serde_json::json!({"body": "shared"}))
            );
        }

        assert_eq!(gated.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn no_more_than_the_fetch_bound_run_at_once() {
        let cache = memory_cache();
        let gated = Gated::default();

        let requests: Vec<_> = (0..MAX_CONCURRENT_FETCHES + 8)
            .map(|i| {
                let body = format!("{{\"body\":{i}}}").into_bytes();
                let (cache, anchor, fetch) = (cache.clone(), anchor(&body), gated.fetch(body));
                tokio::spawn(async move {
                    load_anchor_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await
                })
            })
            .collect();

        gated.wait_for_running(MAX_CONCURRENT_FETCHES).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(gated.most.load(Ordering::SeqCst), MAX_CONCURRENT_FETCHES);
        gated.open();

        for request in requests {
            assert!(request.await.unwrap().error.is_none());
        }

        assert_eq!(
            gated.calls.load(Ordering::SeqCst),
            MAX_CONCURRENT_FETCHES + 8
        );
        assert_eq!(gated.most.load(Ordering::SeqCst), MAX_CONCURRENT_FETCHES);
    }

    /// Past the pending bound, a list row goes out unfetched and starts
    /// nothing, while the single-anchor endpoint fetches within its request;
    /// a miss on the anchor under way still joins its fetch. The bound is
    /// shortened here to one fetch.
    #[tokio::test]
    async fn misses_past_the_pending_bound_answer_by_their_choice() {
        let cache = memory_cache();
        cache
            .register(CacheNamespace::immutable(OFFCHAIN.name, 1).with_fill_limits(2, 1))
            .unwrap();

        let gated = Gated::default();
        let held_body = br#"{"body":"held"}"#.to_vec();
        let held = anchor(&held_body);

        let holding = tokio::spawn({
            let (cache, held, fetch) =
                (cache.clone(), held.clone(), gated.fetch(held_body.clone()));
            async move { load_anchor_metadata(&cache, &held, OnOverload::Unfetched, fetch).await }
        });

        gated.wait_for_running(1).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let body: &'static [u8] = br#"{"body":"past"}"#;
        let past = anchor(body);

        let unfetched_row =
            load_anchor_metadata(&cache, &past, OnOverload::Unfetched, serving(body, &calls)).await;
        assert_eq!(unfetched_row, unfetched(&past));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let inline =
            load_anchor_metadata(&cache, &past, OnOverload::Inline, serving(body, &calls)).await;
        assert_eq!(
            inline.json_metadata,
            Some(serde_json::json!({"body": "past"}))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let joined = tokio::spawn({
            let (cache, held, fetch) = (cache.clone(), held.clone(), gated.fetch(held_body));
            async move { load_anchor_metadata(&cache, &held, OnOverload::Unfetched, fetch).await }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        gated.open();

        assert_eq!(holding.await.unwrap(), joined.await.unwrap());
        assert_eq!(gated.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn inline_fetches_stay_within_the_fetch_bound() {
        let cache = memory_cache();
        cache
            .register(CacheNamespace::immutable(OFFCHAIN.name, 1).with_fill_limits(2, 2))
            .unwrap();

        let gated = Gated::default();

        let load = |i: usize, on_overload: OnOverload| {
            let body = format!("{{\"body\":{i}}}").into_bytes();
            let (cache, anchor, fetch) = (cache.clone(), anchor(&body), gated.fetch(body));
            tokio::spawn(
                async move { load_anchor_metadata(&cache, &anchor, on_overload, fetch).await },
            )
        };

        let mut requests: Vec<_> = (0..2).map(|i| load(i, OnOverload::Unfetched)).collect();
        gated.wait_for_running(2).await;

        requests.extend((2..4).map(|i| load(i, OnOverload::Inline)));
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(gated.calls.load(Ordering::SeqCst), 2);
        gated.open();

        for request in requests {
            assert!(request.await.unwrap().json_metadata.is_some());
        }

        assert_eq!(gated.calls.load(Ordering::SeqCst), 4);
        assert_eq!(gated.most.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn the_legacy_directory_is_removed() {
        let root = temp_root("legacy");
        std::fs::create_dir_all(root.join(LEGACY_DIR)).unwrap();
        std::fs::write(root.join(LEGACY_DIR).join("body"), b"{}").unwrap();

        remove_legacy_dir(&root);
        assert!(!root.join(LEGACY_DIR).exists());

        // a storage path without one is left alone
        remove_legacy_dir(&root);
        assert!(root.exists());

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn verified_bodies_render_json_or_a_decode_error() {
        let anchor = anchor(br#"{"a":1}"#);
        let json = anchor_metadata(
            &anchor,
            MetadataBody::Verified(Bytes::from_static(br#"{"a":1}"#)),
        );
        assert_eq!(json.json_metadata, Some(serde_json::json!({"a": 1})));
        assert_eq!(json.bytes.as_deref(), Some("\\x7b2261223a317d"));
        assert!(json.error.is_none());

        let broken = anchor_metadata(
            &anchor,
            MetadataBody::Verified(Bytes::from_static(b"not json")),
        );
        assert_eq!(broken.json_metadata, None);
        assert_eq!(broken.bytes, None);
        assert_eq!(broken.error.unwrap().code, MetadataError::DecodeError);
    }
}
