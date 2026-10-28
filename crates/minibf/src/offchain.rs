use axum::body::Bytes;
use blockfrost_openapi::models::{DrepsInnerMetadata, DrepsInnerMetadataError};
use dolos_core::{CacheEntry, CacheError, CacheNamespace, CacheStore};
use futures::{stream, FutureExt as _, StreamExt as _};
use pallas::{
    crypto::hash::Hasher,
    ledger::primitives::{conway::Anchor, PoolMetadata},
};
use std::{
    convert::Infallible,
    future::Future,
    path::Path,
    time::{Duration, Instant},
};

use crate::mapping::{
    anchor_fetch_budget, anchor_metadata_from_body, anchor_offchain_body,
    offchain_hash_mismatch_error, pool_fetch_error, pool_offchain_body, PoolOffchainMetadata,
};

/// The concurrent fill limit of the DRep and the proposal bodies namespaces,
/// each.
pub const MAX_CONCURRENT_FETCHES: usize = 16;

/// The pending fill limit of the DRep and the proposal bodies namespaces, each.
pub const MAX_PENDING_LOADS: usize = 4 * MAX_CONCURRENT_FETCHES;

/// The concurrent fill limit of the pool bodies namespace.
pub const MAX_CONCURRENT_POOL_FETCHES: usize = 64;

/// The pending fill limit of the pool bodies namespace.
pub const MAX_PENDING_POOL_LOADS: usize = 2 * MAX_CONCURRENT_POOL_FETCHES;

/// How long a failures namespace answers a failed fetch.
const FAILURE_TTL: Duration = Duration::from_secs(10 * 60);

/// How long a single-pool request waits for its metadata, the wait for a fill
/// permit included.
const POOL_REQUEST_DEADLINE: Duration = Duration::from_secs(10);

/// Verified DRep bodies, keyed by anchor content hash, in both tiers. Every
/// body is kept, and every read checks it against its hash.
pub const OFFCHAIN: CacheNamespace = CacheNamespace::immutable("offchain", 1)
    .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_LOADS);

/// Failed DRep fetches, keyed by content hash and url, in memory only.
pub const OFFCHAIN_FAILURES: CacheNamespace =
    CacheNamespace::tip_scoped("offchain-failures", 1, FAILURE_TTL);

/// Verified proposal bodies, kept and checked as [`OFFCHAIN`] keeps DRep
/// bodies.
pub const PROPOSAL_OFFCHAIN: CacheNamespace = CacheNamespace::immutable("proposal-offchain", 1)
    .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_LOADS);

/// Failed proposal fetches, keyed by content hash and url, in memory only.
pub const PROPOSAL_OFFCHAIN_FAILURES: CacheNamespace =
    CacheNamespace::tip_scoped("proposal-offchain-failures", 1, FAILURE_TTL);

/// Verified pool bodies, keyed by advertised content hash, kept and checked as
/// [`OFFCHAIN`] keeps and checks DRep bodies.
pub const POOL_OFFCHAIN: CacheNamespace = CacheNamespace::immutable("pool-offchain", 1)
    .with_fill_limits(MAX_CONCURRENT_POOL_FETCHES, MAX_PENDING_POOL_LOADS);

/// Failed pool fetches, keyed by content hash and url, in memory only.
pub const POOL_OFFCHAIN_FAILURES: CacheNamespace =
    CacheNamespace::tip_scoped("pool-offchain-failures", 1, FAILURE_TTL);

/// The bodies and failures namespaces of one metadata kind.
#[derive(Clone, Copy)]
struct MetadataCache {
    bodies: &'static str,
    failures: &'static str,
}

const DREP_CACHE: MetadataCache = MetadataCache {
    bodies: OFFCHAIN.name,
    failures: OFFCHAIN_FAILURES.name,
};

const PROPOSAL_CACHE: MetadataCache = MetadataCache {
    bodies: PROPOSAL_OFFCHAIN.name,
    failures: PROPOSAL_OFFCHAIN_FAILURES.name,
};

const POOL_CACHE: MetadataCache = MetadataCache {
    bodies: POOL_OFFCHAIN.name,
    failures: POOL_OFFCHAIN_FAILURES.name,
};

/// The url of a body and the content hash it must match.
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

/// The directory under the storage path that earlier versions used.
const LEGACY_DIR: &str = "offchain";

/// Registers the bodies and failures namespaces of DRep, proposal and pool
/// metadata.
pub fn register<C: CacheStore>(cache: &C) {
    for namespace in [
        OFFCHAIN,
        OFFCHAIN_FAILURES,
        PROPOSAL_OFFCHAIN,
        PROPOSAL_OFFCHAIN_FAILURES,
        POOL_OFFCHAIN,
        POOL_OFFCHAIN_FAILURES,
    ] {
        if let Err(error) = cache.register(namespace.clone()) {
            tracing::warn!(namespace = namespace.name, %error, "cannot register cache namespace");
        }
    }
}

/// Removes [`LEGACY_DIR`] from `storage_path`, if it exists.
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

/// What a miss gets when its namespace is at its pending fill limit.
#[derive(Debug, Clone, Copy)]
pub enum OnOverload {
    /// The row without metadata.
    Unfetched,
    /// A fill in the caller's task, within the concurrent fill limit.
    Inline,
}

/// A failed fetch: the url it failed at, and its error if Blockfrost reports
/// one.
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

/// What a load found for one location.
enum MetadataBody {
    Verified(Bytes),
    Failed(Option<DrepsInnerMetadataError>),
    Unfetched,
}

/// Pool metadata and the error of its last failed fetch.
pub type PoolMetadataLoad = (
    Option<PoolOffchainMetadata>,
    Option<DrepsInnerMetadataError>,
);

/// Runs `loads`, at most `concurrency` at a time, and returns, in order, what
/// each one settled to within `deadline`: `None` for a load still running or
/// never started. Every load is dropped at the deadline, so one that should
/// outlive it has to detach its own work, the way the metadata loaders do.
pub(crate) async fn settle_within<F: Future>(
    loads: Vec<F>,
    concurrency: usize,
    deadline: Duration,
) -> Vec<Option<F::Output>> {
    let mut settled: Vec<_> = loads.iter().map(|_| None).collect();

    let mut running = stream::iter(loads.into_iter().enumerate())
        .map(|(index, load)| load.map(move |value| (index, value)))
        .buffer_unordered(concurrency);

    let _ = tokio::time::timeout(deadline, async {
        while let Some((index, value)) = running.next().await {
            settled[index] = Some(value);
        }
    })
    .await;

    settled
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
            // on a failed validation the spec keeps `json_metadata` and `bytes` null
            Err(error) => out.error = Some(Box::new(error)),
        },
        MetadataBody::Failed(error) => out.error = error.map(Box::new),
        MetadataBody::Unfetched => {}
    }

    out
}

/// The cached body of `location`, else its fresh failure. Removes a body that
/// fails its hash.
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

/// The metadata of `anchor` if the cache holds it, without a fetch.
pub async fn cached_drep_metadata<C: CacheStore>(
    cache: &C,
    anchor: &Anchor,
) -> Option<DrepsInnerMetadata> {
    cached_body(cache, &MetadataLocation::from(anchor), DREP_CACHE)
        .await
        .map(|body| anchor_metadata(anchor, body))
}

/// The metadata of `anchor`, from the cache, else fetched and kept.
pub async fn fetch_drep_metadata<C: CacheStore>(
    cache: &C,
    ipfs_gateways: &[String],
    anchor: &Anchor,
    on_overload: OnOverload,
) -> DrepsInnerMetadata {
    fetch_anchor_metadata(cache, DREP_CACHE, ipfs_gateways, anchor, on_overload).await
}

/// The metadata of a proposal anchor, from the cache, else fetched and kept.
/// Waits at most [`anchor_fetch_budget`], the wait for a fill permit included.
pub async fn fetch_proposal_metadata<C: CacheStore>(
    cache: &C,
    ipfs_gateways: &[String],
    anchor: &Anchor,
) -> DrepsInnerMetadata {
    let load = fetch_anchor_metadata(
        cache,
        PROPOSAL_CACHE,
        ipfs_gateways,
        anchor,
        OnOverload::Inline,
    );

    let deadline = anchor_fetch_budget(&anchor.url, ipfs_gateways);

    tokio::time::timeout(deadline, load)
        .await
        .unwrap_or_else(|_| unfetched(anchor))
}

async fn fetch_anchor_metadata<C: CacheStore>(
    cache: &C,
    namespaces: MetadataCache,
    ipfs_gateways: &[String],
    anchor: &Anchor,
    on_overload: OnOverload,
) -> DrepsInnerMetadata {
    let fetch = |anchor: Anchor| {
        let gateways = ipfs_gateways.to_vec();

        async move { anchor_offchain_body(&anchor.url, anchor.content_hash.as_ref(), &gateways).await }
    };

    load_anchor_metadata(cache, namespaces, anchor, on_overload, fetch).await
}

async fn load_anchor_metadata<C, F, Fut>(
    cache: &C,
    namespaces: MetadataCache,
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
        namespaces,
        on_overload,
        || {
            let fetched = fetch(anchor.clone());
            async move { fetched.await.map_err(Some) }
        },
    )
    .await;

    anchor_metadata(anchor, body)
}

fn pool_metadata_of(body: MetadataBody) -> PoolMetadataLoad {
    match body {
        MetadataBody::Verified(body) => (serde_json::from_slice(&body).ok(), None),
        MetadataBody::Failed(error) => (None, error),
        MetadataBody::Unfetched => (None, None),
    }
}

/// The metadata of a pool and its last fetch error, if the cache holds them.
pub async fn cached_pool_metadata<C: CacheStore>(
    cache: &C,
    metadata: &PoolMetadata,
) -> Option<PoolMetadataLoad> {
    cached_body(cache, &MetadataLocation::from(metadata), POOL_CACHE)
        .await
        .map(pool_metadata_of)
}

/// The metadata of a pool and its last fetch error, for a single-pool request.
/// Waits at most [`POOL_REQUEST_DEADLINE`].
pub async fn fetch_pool_metadata<C: CacheStore>(
    cache: &C,
    metadata: &PoolMetadata,
) -> PoolMetadataLoad {
    load_pool_metadata_within(
        cache,
        metadata,
        POOL_REQUEST_DEADLINE,
        OnOverload::Inline,
        fetch_pool_body,
    )
    .await
}

/// The metadata of a pool and its last fetch error, for a list row. A miss at
/// the pending fill limit gets no metadata.
pub async fn fetch_listed_pool_metadata<C: CacheStore>(
    cache: &C,
    metadata: &PoolMetadata,
) -> PoolMetadataLoad {
    load_pool_metadata(cache, metadata, OnOverload::Unfetched, fetch_pool_body).await
}

async fn fetch_pool_body(
    metadata: PoolMetadata,
) -> Result<Vec<u8>, Option<DrepsInnerMetadataError>> {
    pool_offchain_body(&metadata.url)
        .await
        .map_err(|error| pool_fetch_error(&metadata.url, error))
}

async fn load_pool_metadata_within<C, F, Fut>(
    cache: &C,
    metadata: &PoolMetadata,
    deadline: Duration,
    on_overload: OnOverload,
    fetch: F,
) -> PoolMetadataLoad
where
    C: CacheStore,
    F: Fn(PoolMetadata) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>> + Send + 'static,
{
    tokio::time::timeout(
        deadline,
        load_pool_metadata(cache, metadata, on_overload, fetch),
    )
    .await
    .unwrap_or((None, None))
}

pub(crate) async fn load_pool_metadata<C, F, Fut>(
    cache: &C,
    metadata: &PoolMetadata,
    on_overload: OnOverload,
    fetch: F,
) -> PoolMetadataLoad
where
    C: CacheStore,
    F: Fn(PoolMetadata) -> Fut,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>> + Send + 'static,
{
    let body = load_body(
        cache,
        &MetadataLocation::from(metadata),
        POOL_CACHE,
        on_overload,
        || fetch(metadata.clone()),
    )
    .await;

    pool_metadata_of(body)
}

/// The body of `location`: cached, joined from a flight, or fetched and kept.
/// At the pending fill limit, `on_overload` decides.
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

    let filled = match shared_fill(cache, location, namespaces, on_overload, &fetch).await {
        None => return MetadataBody::Unfetched,
        // a namespace that failed to register, which `register` warned of
        Some(Err(CacheError::UnknownNamespace(_))) => {
            return uncached(cache, location, namespaces, fetch()).await
        }
        Some(filled) => filled,
    };

    // a joined fill can fail at another url: retry the own url in one shared fill
    let filled = if failed_elsewhere(&filled, location) {
        match shared_fill(cache, location, namespaces, on_overload, &fetch).await {
            None => return MetadataBody::Unfetched,
            Some(filled) if failed_elsewhere(&filled, location) => {
                inline_fill(cache, location, namespaces, fetch()).await
            }
            Some(filled) => filled,
        }
    } else {
        filled
    };

    match filled {
        Ok(body) if location.holds_hash(&body) => MetadataBody::Verified(body),
        // the next read drops it
        Ok(_) => MetadataBody::Unfetched,
        Err(error) => match error.downcast_fill::<FetchFailed>() {
            Some(failed) => MetadataBody::Failed(failed.error.clone()),
            // a fill that panicked or was dropped
            None => {
                tracing::warn!(%error, "off-chain metadata cache unavailable, fetching uncached");
                uncached(cache, location, namespaces, fetch()).await
            }
        },
    }
}

/// The body of `location`, fetched without a cache fill.
async fn uncached<C, Fut>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
    fetch: Fut,
) -> MetadataBody
where
    C: CacheStore,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>>,
{
    match fill_body(cache.clone(), location.clone(), namespaces, fetch).await {
        Ok(entry) => MetadataBody::Verified(entry.value),
        Err(failed) => MetadataBody::Failed(failed.error),
    }
}

/// Joins or starts the fill of `location` on its own task. At the pending fill
/// limit, `on_overload` decides, and `None` means unfetched.
async fn shared_fill<C, F, Fut>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
    on_overload: OnOverload,
    fetch: &F,
) -> Option<Result<Bytes, CacheError>>
where
    C: CacheStore,
    F: Fn() -> Fut,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>> + Send + 'static,
{
    let fill = fill_body(cache.clone(), location.clone(), namespaces, fetch());

    match cache
        .get_or_fill_async(namespaces.bodies, &location.hash, fill)
        .await
    {
        Err(CacheError::Overloaded) => match on_overload {
            OnOverload::Unfetched => None,
            OnOverload::Inline => Some(inline_fill(cache, location, namespaces, fetch()).await),
        },
        filled => Some(filled),
    }
}

/// Fills `location` in the caller's task, within the concurrent fill limit.
async fn inline_fill<C, Fut>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
    fetch: Fut,
) -> Result<Bytes, CacheError>
where
    C: CacheStore,
    Fut: Future<Output = Result<Vec<u8>, Option<DrepsInnerMetadataError>>> + Send,
{
    let fill = fill_body(cache.clone(), location.clone(), namespaces, fetch);

    cache
        .fill_inline(namespaces.bodies, &location.hash, fill)
        .await
}

/// Whether `filled` failed at another url than `location`'s.
fn failed_elsewhere(filled: &Result<Bytes, CacheError>, location: &MetadataLocation) -> bool {
    filled.as_ref().is_err_and(|error| {
        error
            .downcast_fill::<FetchFailed>()
            .is_some_and(|failed| failed.url != location.url)
    })
}

/// The entry of `location`'s body, once it matches the hash. A failure goes to
/// the failures namespace.
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
    let started = Instant::now();

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

    // an entry costs the time its fetch took
    let cost = u64::try_from(started.elapsed().as_millis())
        .unwrap_or(u64::MAX)
        .max(1);

    match fetched {
        Ok(body) => Ok(CacheEntry::new(body, cost)),
        Err(failure) => {
            keep_failure(&cache, &location, namespaces, &failure, cost).await;
            Err(FetchFailed {
                url: location.url,
                error: failure,
            })
        }
    }
}

/// Keeps `failure` in the failures namespace before the fill answers.
async fn keep_failure<C: CacheStore>(
    cache: &C,
    location: &MetadataLocation,
    namespaces: MetadataCache,
    failure: &Option<DrepsInnerMetadataError>,
    cost: u64,
) {
    let Ok(value) = serde_json::to_vec(failure) else {
        return;
    };

    let kept = cache
        .fill_inline(namespaces.failures, &location.failure_key(), async move {
            Ok::<_, Infallible>(CacheEntry::new(value, cost))
        })
        .await;

    match kept {
        // a namespace that failed to register, which `register` warned of
        Ok(_) | Err(CacheError::UnknownNamespace(_)) => {}
        Err(error) => {
            tracing::warn!(url = %location.url, %error, "cannot keep off-chain metadata failure")
        }
    }
}

/// Keeps `body` in `namespace` as the verified body of `anchor`, as a fetch
/// would.
#[cfg(test)]
pub(crate) async fn cache_verified_body<C: CacheStore>(
    cache: &C,
    namespace: &str,
    anchor: &Anchor,
    body: &[u8],
) {
    let body = body.to_vec();

    cache
        .get_or_fill_async(namespace, anchor.content_hash.as_ref(), async move {
            Ok::<_, Infallible>(CacheEntry::new(body, 0))
        })
        .await
        .unwrap();
}

/// Keeps `error` as the failure of a pool fetch, as a failed fetch would.
#[cfg(test)]
pub(crate) async fn cache_pool_failure<C: CacheStore>(
    cache: &C,
    metadata: &PoolMetadata,
    error: DrepsInnerMetadataError,
) {
    keep_failure(
        cache,
        &MetadataLocation::from(metadata),
        POOL_CACHE,
        &Some(error),
        1,
    )
    .await;
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

    fn failing_pool(failure: &DrepsInnerMetadataError) -> impl Fn(PoolMetadata) -> PoolFetch {
        let failure = failure.clone();
        move |_| {
            let failure = failure.clone();
            Box::pin(async move { Err(Some(failure)) })
        }
    }

    #[tokio::test]
    async fn pool_metadata_survives_restart_and_an_unavailable_url() {
        let dir = temp_root("pool-restart");
        let metadata = pool_metadata(POOL_BODY);
        let calls = Arc::new(AtomicUsize::new(0));
        let domain = persistent_domain(&dir);
        let (fetched, error) = load_pool_metadata(
            domain.cache(),
            &metadata,
            OnOverload::Inline,
            serving_pool(POOL_BODY, &calls),
        )
        .await;
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
        let load = |metadata, body| {
            load_pool_metadata(
                &cache,
                metadata,
                OnOverload::Inline,
                serving_pool(body, &calls),
            )
        };
        load(&metadata, POOL_BODY).await;

        let changed = br#"{"name":"Other","description":"Changed","ticker":"NEW","homepage":"https://example.com"}"#;
        let (kept, error) = load(&metadata, changed).await;
        assert_eq!(kept.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        cache.wipe(POOL_OFFCHAIN.name).unwrap();
        let (missing, error) = load(&metadata, changed).await;
        assert!(missing.is_none());
        assert_eq!(error.unwrap().code, MetadataError::HashMismatch);
        assert!(cache
            .get(POOL_OFFCHAIN.name, metadata.hash.as_slice())
            .is_none());

        let updated = pool_metadata(changed);
        let (fetched, error) = load(&updated, changed).await;
        assert_eq!(fetched.unwrap().ticker, "NEW");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn pool_failures_answer_until_they_expire_and_a_new_url_retries() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let calls = Arc::new(AtomicUsize::new(0));
        let failure = crate::mapping::pool_connection_error(&metadata.url);
        let (missing, error) = load_pool_metadata(
            &cache,
            &metadata,
            OnOverload::Inline,
            failing_pool(&failure),
        )
        .await;
        assert!(missing.is_none());
        assert_eq!(error.unwrap(), failure);

        // the failure answers without a fetch
        let serve = || serving_pool(POOL_BODY, &calls);
        let (missing, error) =
            load_pool_metadata(&cache, &metadata, OnOverload::Inline, serve()).await;
        assert!(missing.is_none());
        assert_eq!(error.unwrap(), failure);
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        // the failure of one URL does not hold back another URL
        let mut moved = metadata.clone();
        moved.url.push_str("?mirror");
        let (fetched, error) =
            load_pool_metadata(&cache, &moved, OnOverload::Inline, serve()).await;
        assert_eq!(fetched.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // past its TTL the failure no longer answers
        cache.wipe(POOL_OFFCHAIN.name).unwrap();
        cache
            .register(CacheNamespace::tip_scoped(
                POOL_OFFCHAIN_FAILURES.name,
                1,
                Duration::ZERO,
            ))
            .unwrap();
        let (fetched, error) =
            load_pool_metadata(&cache, &metadata, OnOverload::Inline, serve()).await;
        assert_eq!(fetched.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_failure_is_kept_before_the_fetch_answers() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let failure = crate::mapping::pool_connection_error(&metadata.url);
        load_pool_metadata(
            &cache,
            &metadata,
            OnOverload::Inline,
            failing_pool(&failure),
        )
        .await;

        let kept = cached_body(&cache, &MetadataLocation::from(&metadata), POOL_CACHE).await;
        assert!(matches!(kept, Some(MetadataBody::Failed(Some(error))) if error == failure));
    }

    #[tokio::test]
    async fn pool_metadata_keeps_silent_failures_and_invalid_json_behavior() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let (missing, error) =
            load_pool_metadata(&cache, &metadata, OnOverload::Inline, |_| async {
                Err(None)
            })
            .await;
        assert!(missing.is_none());
        assert!(error.is_none());

        let calls = Arc::new(AtomicUsize::new(0));
        let (missing, error) = load_pool_metadata(
            &cache,
            &metadata,
            OnOverload::Inline,
            serving_pool(POOL_BODY, &calls),
        )
        .await;
        assert!(missing.is_none());
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        for body in [b"not json".as_slice(), b"{}".as_slice()] {
            let (missing, error) = load_pool_metadata(
                &cache,
                &pool_metadata(body),
                OnOverload::Inline,
                serving_pool(body, &calls),
            )
            .await;
            assert!(missing.is_none());
            assert!(error.is_none());
        }
    }

    #[tokio::test]
    async fn pool_and_drep_metadata_share_neither_entries_nor_failures() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let anchor = Anchor {
            url: metadata.url.clone(),
            content_hash: Hasher::<256>::hash(POOL_BODY),
        };
        cache_verified_body(&cache, OFFCHAIN.name, &anchor, POOL_BODY).await;

        let failure = crate::mapping::pool_connection_error(&metadata.url);
        let (missing, error) = load_pool_metadata(
            &cache,
            &metadata,
            OnOverload::Inline,
            failing_pool(&failure),
        )
        .await;
        assert!(missing.is_none());
        assert_eq!(error, Some(failure));

        let key = MetadataLocation::from(&metadata).failure_key();
        assert!(cache.get(POOL_OFFCHAIN_FAILURES.name, &key).is_some());
        assert!(cache.get(OFFCHAIN_FAILURES.name, &key).is_none());

        let calls = Arc::new(AtomicUsize::new(0));
        let drep = load_anchor_metadata(
            &cache,
            DREP_CACHE,
            &anchor,
            OnOverload::Inline,
            serving(POOL_BODY, &calls),
        )
        .await;
        assert!(drep.error.is_none());
        assert!(drep.json_metadata.is_some());
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn entries_cost_the_time_their_fetch_took() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let fetch = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok(POOL_BODY.to_vec())
        };

        let entry = fill_body(cache, MetadataLocation::from(&metadata), POOL_CACHE, fetch)
            .await
            .unwrap();
        assert!(entry.cost >= 20, "{}", entry.cost);
    }

    #[tokio::test]
    async fn an_unregistered_namespace_fetches_without_the_cache() {
        let cache = TieredCache::memory(&CacheConfig::default(), QueryLimiter::default());
        let metadata = pool_metadata(POOL_BODY);
        let calls = Arc::new(AtomicUsize::new(0));

        for expected in [1, 2] {
            let (fetched, error) = load_pool_metadata(
                &cache,
                &metadata,
                OnOverload::Inline,
                serving_pool(POOL_BODY, &calls),
            )
            .await;
            assert_eq!(fetched.unwrap().ticker, "POOL");
            assert!(error.is_none());
            assert_eq!(calls.load(Ordering::SeqCst), expected);
        }
    }

    #[tokio::test]
    async fn a_disabled_cache_fetches_on_every_request() {
        let config = CacheConfig {
            enabled: false,
            ..CacheConfig::default()
        };
        let cache = TieredCache::memory(&config, QueryLimiter::default());
        register(&cache);
        let metadata = pool_metadata(POOL_BODY);
        let failure = crate::mapping::pool_connection_error(&metadata.url);
        let calls = Arc::new(AtomicUsize::new(0));

        let (_, error) = load_pool_metadata(
            &cache,
            &metadata,
            OnOverload::Inline,
            failing_pool(&failure),
        )
        .await;
        assert_eq!(error, Some(failure));

        for expected in [1, 2] {
            let (fetched, _) = load_pool_metadata(
                &cache,
                &metadata,
                OnOverload::Inline,
                serving_pool(POOL_BODY, &calls),
            )
            .await;
            assert_eq!(fetched.unwrap().ticker, "POOL");
            assert_eq!(calls.load(Ordering::SeqCst), expected);
        }
    }

    #[tokio::test]
    async fn a_single_pool_request_answers_by_its_deadline() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let release = Arc::new(tokio::sync::Notify::new());
        let held = {
            let release = release.clone();
            move |_| {
                let release = release.clone();
                Box::pin(async move {
                    release.notified().await;
                    Ok(POOL_BODY.to_vec())
                }) as PoolFetch
            }
        };

        let (missing, error) = load_pool_metadata_within(
            &cache,
            &metadata,
            Duration::from_millis(100),
            OnOverload::Inline,
            held,
        )
        .await;
        assert!(missing.is_none());
        assert!(error.is_none());

        // the fetch outlives the request and keeps its body
        release.notify_one();
        let location = MetadataLocation::from(&metadata);
        tokio::time::timeout(Duration::from_secs(5), async {
            while cached_body(&cache, &location, POOL_CACHE).await.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the detached fetch never kept its body");
    }

    #[tokio::test]
    async fn proposal_fetches_do_not_wait_for_drep_fetch_slots() {
        let cache = memory_cache();
        let gated = Gated::default();
        let held: Vec<_> = (0..MAX_CONCURRENT_FETCHES)
            .map(|i| {
                let body = format!("{{\"drep\":{i}}}").into_bytes();
                let (cache, anchor, fetch) = (cache.clone(), anchor(&body), gated.fetch(body));
                tokio::spawn(async move {
                    load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Inline, fetch)
                        .await
                })
            })
            .collect();
        gated.wait_for_running(MAX_CONCURRENT_FETCHES).await;

        let body = br#"{"proposal":1}"#;
        let calls = Arc::new(AtomicUsize::new(0));
        let proposal = tokio::time::timeout(
            Duration::from_secs(1),
            load_anchor_metadata(
                &cache,
                PROPOSAL_CACHE,
                &anchor(body),
                OnOverload::Inline,
                serving(body, &calls),
            ),
        )
        .await
        .expect("the proposal fetch waited for a DRep fetch slot");
        assert_eq!(
            proposal.json_metadata,
            Some(serde_json::json!({"proposal": 1}))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        gated.open();
        for request in held {
            assert!(request.await.unwrap().error.is_none());
        }
    }

    // The test runtime runs one task at a time. `load_body` calls the fetch
    // closure just before it joins the fetch under way, with no await between,
    // so a request that has called its closure has joined.
    #[tokio::test]
    async fn concurrent_pool_misses_share_one_fetch() {
        let cache = memory_cache();
        let metadata = pool_metadata(POOL_BODY);
        let gated = Gated::default();
        let joined = Arc::new(AtomicUsize::new(0));
        let requests: Vec<_> = (0..8)
            .map(|_| {
                let (cache, metadata, gated, joined) = (
                    cache.clone(),
                    metadata.clone(),
                    gated.clone(),
                    joined.clone(),
                );
                tokio::spawn(async move {
                    load_pool_metadata(&cache, &metadata, OnOverload::Inline, |_| {
                        joined.fetch_add(1, Ordering::SeqCst);
                        let fetched = gated.fetch(POOL_BODY.to_vec())(anchor(POOL_BODY));
                        async move { fetched.await.map_err(Some) }
                    })
                    .await
                })
            })
            .collect();
        tokio::time::timeout(Duration::from_secs(5), async {
            while joined.load(Ordering::SeqCst) < 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("a request never reached its fetch");
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
                load_pool_metadata(&cache, &dead, OnOverload::Inline, move |_| {
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

        // see `concurrent_pool_misses_share_one_fetch`: the closure call
        // marks the join
        let calls = Arc::new(AtomicUsize::new(0));
        let (joining, joined) = tokio::sync::oneshot::channel::<()>();
        let joining = std::sync::Mutex::new(Some(joining));
        let serve = serving_pool(POOL_BODY, &calls);
        let second = tokio::spawn({
            let (cache, moved) = (cache.clone(), moved.clone());
            async move {
                load_pool_metadata(&cache, &moved, OnOverload::Inline, move |metadata| {
                    if let Some(joining) = joining.lock().unwrap().take() {
                        let _ = joining.send(());
                    }
                    serve(metadata)
                })
                .await
            }
        });
        joined.await.unwrap();
        release.notify_one();

        let (missing, error) = first.await.unwrap();
        assert!(missing.is_none());
        assert_eq!(error.unwrap(), failure);
        let (fetched, error) = second.await.unwrap();
        assert_eq!(fetched.unwrap().ticker, "POOL");
        assert!(error.is_none());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    /// `wait`'s output, or a test failure after five seconds rather than a
    /// hang.
    async fn within<F: Future>(wait: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(5), wait)
            .await
            .expect("the test waited more than five seconds")
    }

    // see `concurrent_pool_misses_share_one_fetch`: each closure call marks a
    // join, the first of the shared fetch, the second of the shared retry
    #[tokio::test]
    async fn waiters_at_another_url_share_one_retry_that_outlives_them() {
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
                load_pool_metadata(&cache, &dead, OnOverload::Inline, move |_| {
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

        // the fetch at the dead URL holds the flight before any waiter comes
        within(ready).await.unwrap();

        let gated = Gated::default();
        let joined = Arc::new(AtomicUsize::new(0));
        let waiters: Vec<_> = (0..8)
            .map(|_| {
                let (cache, moved, gated, joined) =
                    (cache.clone(), moved.clone(), gated.clone(), joined.clone());
                tokio::spawn(async move {
                    load_pool_metadata(&cache, &moved, OnOverload::Inline, |_| {
                        joined.fetch_add(1, Ordering::SeqCst);
                        let fetched = gated.fetch(POOL_BODY.to_vec())(anchor(POOL_BODY));
                        async move { fetched.await.map_err(Some) }
                    })
                    .await
                })
            })
            .collect();

        let joins = |n: usize| {
            let joined = joined.clone();
            within(async move {
                while joined.load(Ordering::SeqCst) < n {
                    tokio::task::yield_now().await;
                }
            })
        };
        joins(8).await;
        release.notify_one();
        assert_eq!(within(first).await.unwrap().1, Some(failure));
        joins(16).await;

        // every waiter is cancelled, as a page deadline would
        for waiter in waiters {
            waiter.abort();
            assert!(within(waiter).await.unwrap_err().is_cancelled());
        }
        gated.open();

        let location = MetadataLocation::from(&moved);
        within(async {
            while cached_body(&cache, &location, POOL_CACHE).await.is_none() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(gated.calls.load(Ordering::SeqCst), 1);
    }

    /// A page answers by its deadline: a slow load leaves its row unsettled,
    /// and the deadline drops it along with every load not started yet, so a
    /// page leaves nothing of its own running once it has answered.
    #[tokio::test]
    async fn settle_within_answers_by_the_deadline_and_drops_the_rest() {
        let (_release, released) = tokio::sync::oneshot::channel::<()>();
        let (dropped, was_dropped) = tokio::sync::oneshot::channel::<()>();

        let slow = async move {
            // dropped with the load, which closes the channel
            let _dropped = dropped;
            let _ = released.await;
            2
        }
        .boxed();

        let started = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));

        let queued = {
            let started = started.clone();

            async move {
                started.store(true, std::sync::atomic::Ordering::SeqCst);
                3
            }
            .boxed()
        };

        // one at a time: the slow load holds the only slot past the deadline
        let loads = vec![async { 1 }.boxed(), slow, queued];

        let settled = settle_within(loads, 1, Duration::from_millis(100)).await;

        assert_eq!(settled, vec![Some(1), None, None]);

        // the deadline dropped the slow load rather than leaving it running,
        // and the queued one never started
        assert!(was_dropped.await.is_err());
        assert!(!started.load(std::sync::atomic::Ordering::SeqCst));
    }

    #[tokio::test]
    async fn a_body_that_fails_its_hash_is_never_kept() {
        let cache = memory_cache();
        let calls = Arc::new(AtomicUsize::new(0));
        let anchor = anchor(br#"{"body":"expected"}"#);

        let fetch = serving(br#"{"body":"tampered"}"#, &calls);
        let got =
            load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Unfetched, fetch).await;

        assert_eq!(got.error.unwrap().code, MetadataError::HashMismatch);
        assert!(got.bytes.is_none());
        assert_eq!(cache.get(OFFCHAIN.name, anchor.content_hash.as_ref()), None);
    }

    #[tokio::test]
    async fn a_corrupted_stored_body_is_dropped_on_read() {
        let cache = memory_cache();
        let anchor = anchor(br#"{"body":"hello"}"#);

        cache_verified_body(&cache, OFFCHAIN.name, &anchor, b"corrupted").await;
        assert!(cache
            .get(OFFCHAIN.name, anchor.content_hash.as_ref())
            .is_some());

        assert_eq!(cached_drep_metadata(&cache, &anchor).await, None);
        assert_eq!(cache.get(OFFCHAIN.name, anchor.content_hash.as_ref()), None);

        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = serving(br#"{"body":"hello"}"#, &calls);
        let got =
            load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Unfetched, fetch).await;

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
            DREP_CACHE,
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
            DREP_CACHE,
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
    fn metadata_namespaces_keep_their_validity_and_limits() {
        use dolos_core::cache::{FillLimits, Validity};

        for failures in [
            OFFCHAIN_FAILURES,
            PROPOSAL_OFFCHAIN_FAILURES,
            POOL_OFFCHAIN_FAILURES,
        ] {
            let ten_minutes = Validity::TipScoped {
                ttl: Duration::from_secs(600),
            };
            assert_eq!(failures.validity, ten_minutes, "{}", failures.name);
        }

        for (bodies, concurrent, pending) in [
            (OFFCHAIN, 16, 64),
            (PROPOSAL_OFFCHAIN, 16, 64),
            (POOL_OFFCHAIN, 64, 128),
        ] {
            assert_eq!(bodies.validity, Validity::Immutable, "{}", bodies.name);
            let limits = FillLimits {
                concurrent,
                pending,
            };
            assert_eq!(bodies.fill_limits, Some(limits), "{}", bodies.name);
        }
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

        let failed =
            load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Unfetched, &failing)
                .await;
        assert_eq!(
            failed.error.as_ref().unwrap().code,
            MetadataError::ConnectionError
        );

        // the failure is kept before the fetch answers
        assert!(cached_drep_metadata(&cache, &anchor).await.is_some());

        let served =
            load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Unfetched, &failing)
                .await;
        assert_eq!(served, failed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::sleep(ttl).await;

        let fetch = serving(br#"{"body":"later"}"#, &calls);
        let refetched =
            load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Unfetched, fetch).await;
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
                    load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Unfetched, fetch)
                        .await
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
                    load_anchor_metadata(&cache, DREP_CACHE, &anchor, OnOverload::Unfetched, fetch)
                        .await
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
            async move {
                load_anchor_metadata(&cache, DREP_CACHE, &held, OnOverload::Unfetched, fetch).await
            }
        });

        gated.wait_for_running(1).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let body: &'static [u8] = br#"{"body":"past"}"#;
        let past = anchor(body);

        let unfetched_row = load_anchor_metadata(
            &cache,
            DREP_CACHE,
            &past,
            OnOverload::Unfetched,
            serving(body, &calls),
        )
        .await;
        assert_eq!(unfetched_row, unfetched(&past));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let inline = load_anchor_metadata(
            &cache,
            DREP_CACHE,
            &past,
            OnOverload::Inline,
            serving(body, &calls),
        )
        .await;
        assert_eq!(
            inline.json_metadata,
            Some(serde_json::json!({"body": "past"}))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let joined = tokio::spawn({
            let (cache, held, fetch) = (cache.clone(), held.clone(), gated.fetch(held_body));
            async move {
                load_anchor_metadata(&cache, DREP_CACHE, &held, OnOverload::Unfetched, fetch).await
            }
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
            tokio::spawn(async move {
                load_anchor_metadata(&cache, DREP_CACHE, &anchor, on_overload, fetch).await
            })
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
