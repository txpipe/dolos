use blockfrost_openapi::models::{DrepsInnerMetadata, DrepsInnerMetadataError};
use dolos_core::{CacheEntry, CacheError, CacheNamespace, CacheStore};
use pallas::{crypto::hash::Hasher, ledger::primitives::conway::Anchor};
use std::{convert::Infallible, future::Future, path::Path, time::Duration};

use crate::mapping::{
    anchor_metadata_from_body, anchor_offchain_body, offchain_hash_mismatch_error,
};

// The fetch itself, with its public-address gate, redirect limit, `ipfs://`
// gateways and size cap, is the one `/governance/dreps/{drep_id}/metadata`
// runs (`anchor_offchain_body`). This module only keeps what it returns, since
// a list page asks for up to a hundred anchors at a time.

/// How many anchor fetches the whole process runs at a time, whichever
/// requests asked for them. A page answers by its deadline and leaves the
/// fetches it started running, so without this bound pages served back to
/// back could stack outbound connections well past what any one page opens.
/// Cache hits never wait on it.
pub const MAX_CONCURRENT_FETCHES: usize = 16;

/// How many anchor fetches the whole process keeps under way, those waiting
/// to run included. A fetch outlives the page that started it, so without
/// this bound pages served back to back, each missing on other anchors, would
/// pile up detached fetches faster than [`MAX_CONCURRENT_FETCHES`] drains
/// them. A miss past it starts no detached fetch, and its caller decides what
/// it gets instead ([`OnOverload`]); a miss on an anchor already under way
/// still joins that fetch.
pub const MAX_PENDING_LOADS: usize = 4 * MAX_CONCURRENT_FETCHES;

/// A failed fetch is answered from the cache for this long, so a dead host
/// costs one timeout per window instead of one per page render.
const FAILURE_TTL: Duration = Duration::from_secs(10 * 60);

/// Verified bodies, keyed by the content hash of their anchor. The hash pins
/// the content, so a body is immutable input and is kept in both tiers; every
/// read checks it against the hash again.
pub const OFFCHAIN: CacheNamespace = CacheNamespace::immutable("offchain", 1)
    .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_LOADS);

/// Failed fetches, keyed by the content hash and url of their anchor, in
/// memory only.
pub const OFFCHAIN_FAILURES: CacheNamespace =
    CacheNamespace::tip_scoped("offchain-failures", 1, FAILURE_TTL);

/// Where earlier versions kept verified bodies, under the storage path.
const LEGACY_DIR: &str = "offchain";

/// Registers the namespaces this module keeps. Without [`OFFCHAIN`] anchors
/// go out unfetched until restart; without [`OFFCHAIN_FAILURES`] a failing
/// anchor is fetched again on every request.
pub fn register<C: CacheStore>(cache: &C) {
    for namespace in [OFFCHAIN, OFFCHAIN_FAILURES] {
        if let Err(error) = cache.register(namespace.clone()) {
            tracing::warn!(namespace = namespace.name, %error, "cannot register cache namespace");
        }
    }
}

/// Removes the directory earlier versions kept verified bodies in. The cache
/// holds them now, and the directory is disposable.
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

/// What a miss gets while [`MAX_PENDING_LOADS`] fetches are already under way.
#[derive(Debug, Clone, Copy)]
pub enum OnOverload {
    /// Its anchor alone, unfetched, the way a list row past its page deadline
    /// goes out: a page shows what it has rather than wait on a backlog.
    Unfetched,
    /// The fetch all the same, run within the caller's own request rather
    /// than detached, so it lasts no longer than the request does: an
    /// endpoint that serves one anchor owes its client the fetch or its error.
    Inline,
}

/// A failed fetch, as a cache fill reports it.
#[derive(Debug)]
struct FetchFailed(DrepsInnerMetadataError);

impl std::fmt::Display for FetchFailed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.message)
    }
}

impl std::error::Error for FetchFailed {}

fn errored(mut out: DrepsInnerMetadata, error: DrepsInnerMetadataError) -> DrepsInnerMetadata {
    out.error = Some(Box::new(error));
    out
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
pub fn unfetched(anchor: &Anchor) -> DrepsInnerMetadata {
    DrepsInnerMetadata {
        url: anchor.url.clone(),
        hash: hex::encode(anchor.content_hash),
        json_metadata: None,
        bytes: None,
        error: None,
    }
}

fn holds_hash(anchor: &Anchor, body: &[u8]) -> bool {
    Hasher::<256>::hash(body) == anchor.content_hash
}

fn failure_key(anchor: &Anchor) -> Vec<u8> {
    [anchor.content_hash.as_ref(), anchor.url.as_bytes()].concat()
}

/// What the cache holds for `anchor`: its body, else a fresh failure. A body
/// that no longer matches its hash is removed. Reads the disk tier.
fn cached<C: CacheStore>(cache: &C, anchor: &Anchor) -> Option<DrepsInnerMetadata> {
    let key = anchor.content_hash.as_ref();

    if let Some(body) = cache.get(OFFCHAIN.name, key) {
        if holds_hash(anchor, &body) {
            return Some(verified(unfetched(anchor), &body));
        }

        tracing::warn!(url = %anchor.url, "dropping cached off-chain metadata that fails its hash");

        if let Err(error) = cache.remove(OFFCHAIN.name, key) {
            tracing::warn!(%error, "cannot remove cached off-chain metadata");
        }
    }

    let failure = cache.get(OFFCHAIN_FAILURES.name, &failure_key(anchor))?;
    let failure = serde_json::from_slice(&failure).ok()?;

    Some(errored(unfetched(anchor), failure))
}

/// The metadata of `anchor` if the cache holds it already. It never goes to
/// the network, so a page answers these rows without spending a fetch slot
/// on them.
pub async fn cached_drep_metadata<C: CacheStore>(
    cache: &C,
    anchor: &Anchor,
) -> Option<DrepsInnerMetadata> {
    let (cache, anchor) = (cache.clone(), anchor.clone());

    tokio::task::spawn_blocking(move || cached(&cache, &anchor))
        .await
        .ok()
        .flatten()
}

/// The metadata of `anchor`, as both `/governance/dreps` and
/// `/governance/dreps/{drep_id}/metadata` serve it: from the cache, else
/// fetched and kept there.
pub async fn fetch_drep_metadata<C: CacheStore>(
    cache: &C,
    ipfs_gateways: &[String],
    anchor: &Anchor,
    on_overload: OnOverload,
) -> DrepsInnerMetadata {
    let fetch = |anchor: Anchor| {
        let gateways = ipfs_gateways.to_vec();

        async move { anchor_offchain_body(&anchor.url, anchor.content_hash.as_ref(), &gateways).await }
    };

    load_drep_metadata(cache, anchor, on_overload, fetch).await
}

/// [`fetch_drep_metadata`] over `fetch`, the request for an anchor's body.
async fn load_drep_metadata<C, F, Fut>(
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
    if let Some(cached) = cached_drep_metadata(cache, anchor).await {
        return cached;
    }

    let fill = fill_body(cache.clone(), anchor.clone(), fetch(anchor.clone()));
    let key = anchor.content_hash.as_ref();

    // the fill runs on its own task, so it reaches its end, and fills the
    // cache, whether or not anyone still waits for it
    match cache.get_or_fill_async(OFFCHAIN.name, key, fill).await {
        Err(CacheError::Overloaded) => match on_overload {
            OnOverload::Unfetched => unfetched(anchor),
            OnOverload::Inline => {
                // within the same fetch bound as a detached load
                let fill = fill_body(cache.clone(), anchor.clone(), fetch(anchor.clone()));
                rendered(anchor, cache.fill_inline(OFFCHAIN.name, key, fill).await)
            }
        },
        filled => rendered(anchor, filled),
    }
}

/// The metadata of `anchor` as its fill left it.
fn rendered(anchor: &Anchor, filled: Result<impl AsRef<[u8]>, CacheError>) -> DrepsInnerMetadata {
    let out = unfetched(anchor);

    match filled {
        Ok(body) if holds_hash(anchor, body.as_ref()) => verified(out, body.as_ref()),
        // the next read drops it
        Ok(_) => out,
        Err(error) => match error.downcast_fill::<FetchFailed>() {
            Some(FetchFailed(failure)) => errored(out, failure.clone()),
            // a fill that panicked, or a namespace that never registered
            None => {
                tracing::warn!(url = %anchor.url, %error, "off-chain metadata not loaded");
                out
            }
        },
    }
}

/// The fill of `anchor`'s body: what `fetch` returns, once it matches the
/// anchor's hash. A failure is kept in [`OFFCHAIN_FAILURES`] and nothing in
/// [`OFFCHAIN`].
async fn fill_body<C, Fut>(cache: C, anchor: Anchor, fetch: Fut) -> Result<CacheEntry, FetchFailed>
where
    C: CacheStore,
    Fut: Future<Output = Result<Vec<u8>, DrepsInnerMetadataError>>,
{
    let fetched = fetch.await.and_then(|body| {
        if holds_hash(&anchor, &body) {
            return Ok(body);
        }

        let actual = Hasher::<256>::hash(&body);

        Err(offchain_hash_mismatch_error(
            &anchor.url,
            anchor.content_hash.as_ref(),
            actual.as_ref(),
        ))
    });

    match fetched {
        Ok(body) => Ok(CacheEntry::new(body, 0)),
        Err(failure) => {
            keep_failure(&cache, &anchor, &failure);
            Err(FetchFailed(failure))
        }
    }
}

fn keep_failure<C: CacheStore>(cache: &C, anchor: &Anchor, failure: &DrepsInnerMetadataError) {
    let Ok(value) = serde_json::to_vec(failure) else {
        return;
    };

    let kept = cache.refill(OFFCHAIN_FAILURES.name, &failure_key(anchor), move || {
        Ok::<_, Infallible>(CacheEntry::new(value, 0))
    });

    if let Err(error) = kept {
        tracing::warn!(url = %anchor.url, %error, "cannot keep off-chain metadata failure");
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

    #[tokio::test]
    async fn a_body_that_fails_its_hash_is_never_kept() {
        let cache = memory_cache();
        let calls = Arc::new(AtomicUsize::new(0));
        let anchor = anchor(br#"{"body":"expected"}"#);

        let fetch = serving(br#"{"body":"tampered"}"#, &calls);
        let got = load_drep_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await;

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

        assert_eq!(cached_drep_metadata(&cache, &anchor).await, None);
        assert_eq!(cache.get(OFFCHAIN.name, anchor.content_hash.as_ref()), None);

        let calls = Arc::new(AtomicUsize::new(0));
        let fetch = serving(br#"{"body":"hello"}"#, &calls);
        let got = load_drep_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await;

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
        let fetched = load_drep_metadata(
            domain.cache(),
            &anchor,
            OnOverload::Unfetched,
            serving(body, &calls),
        )
        .await;
        domain.cache().flush().unwrap();
        drop(domain);

        let domain = persistent_domain(&dir);
        let kept = load_drep_metadata(
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

        let failed = load_drep_metadata(&cache, &anchor, OnOverload::Unfetched, &failing).await;
        assert_eq!(
            failed.error.as_ref().unwrap().code,
            MetadataError::ConnectionError
        );

        // the failure lands from a fill of its own
        while cached_drep_metadata(&cache, &anchor).await.is_none() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let served = load_drep_metadata(&cache, &anchor, OnOverload::Unfetched, &failing).await;
        assert_eq!(served, failed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        tokio::time::sleep(ttl).await;

        let fetch = serving(br#"{"body":"later"}"#, &calls);
        let refetched = load_drep_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await;
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
                    load_drep_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await
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
                    load_drep_metadata(&cache, &anchor, OnOverload::Unfetched, fetch).await
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
            async move { load_drep_metadata(&cache, &held, OnOverload::Unfetched, fetch).await }
        });

        gated.wait_for_running(1).await;

        let calls = Arc::new(AtomicUsize::new(0));
        let body: &'static [u8] = br#"{"body":"past"}"#;
        let past = anchor(body);

        let unfetched_row =
            load_drep_metadata(&cache, &past, OnOverload::Unfetched, serving(body, &calls)).await;
        assert_eq!(unfetched_row, unfetched(&past));
        assert_eq!(calls.load(Ordering::SeqCst), 0);

        let inline =
            load_drep_metadata(&cache, &past, OnOverload::Inline, serving(body, &calls)).await;
        assert_eq!(
            inline.json_metadata,
            Some(serde_json::json!({"body": "past"}))
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let joined = tokio::spawn({
            let (cache, held, fetch) = (cache.clone(), held.clone(), gated.fetch(held_body));
            async move { load_drep_metadata(&cache, &held, OnOverload::Unfetched, fetch).await }
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
                async move { load_drep_metadata(&cache, &anchor, on_overload, fetch).await },
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
}
