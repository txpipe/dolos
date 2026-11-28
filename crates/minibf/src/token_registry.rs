//! The off-chain token registry that `token_registry_url` names: the metadata
//! of `/assets/{asset}` and the decimals of `/addresses/{address}/extended`.
//!
//! Nothing on chain pins a registry entry, and its owner can change it at any
//! time, so an answer is kept for [`TTL`] rather than checked against a hash.
//! Past it, the old answer is served while one request replaces it, and a
//! failed request keeps it, so a registry outage does not empty the cache.
//! "Not registered" is an answer too, the one most units get.
//!
//! A request that fails is no answer either: the unit's older answer is served
//! if the cache holds one, and no metadata otherwise. The unit is not asked for
//! again for [`FAILURE_TTL`], so a registry that is down costs each unit one
//! request rather than one per response.

use std::{convert::Infallible, sync::OnceLock, time::Duration};

use blockfrost_openapi::models::asset_metadata::AssetMetadata;
use dolos_core::{cache::Cost, CacheEntry, CacheError, CacheNamespace, CacheStore};
use reqwest::StatusCode;
use serde::Deserialize;

/// How long an answer is served before the registry is asked again. "Not
/// registered" is an answer, so a unit registered after it was asked for
/// shows its entry once this has passed.
const TTL: Duration = Duration::from_secs(60 * 60);

/// How long a unit whose request failed is not asked for again.
const FAILURE_TTL: Duration = Duration::from_secs(60);

/// What an entry costs per byte of its key and value. An entry stands for a
/// request to another host and is in no other tier: evicted, it costs that
/// request again, where the other namespaces' entries come back from disk. An
/// unregistered unit's entry, about 100 bytes, counts about 100,000, the sums
/// a `totals` entry must have read to be kept.
///
/// The memory tier makes room only from entries that cost less per byte, then
/// from the least recently used among equals. Every entry here costs the same
/// per byte, so a full tier makes room for a new answer from the least
/// recently used ones, whether they say "not registered" or hold metadata.
const COST_PER_BYTE: Cost = 1_000;

/// How many registry requests the whole process runs at a time, whichever
/// requests asked for them. Every one goes to the same host, so this bounds
/// the load the node puts on it. Cache hits never wait on it.
pub const MAX_CONCURRENT_FETCHES: usize = 16;

/// How many registry requests the whole process keeps under way, those
/// waiting to run included. A request outlives the response that started it,
/// so it fills the cache for the next one. A miss past this bound is asked
/// within its own response instead, under [`MAX_CONCURRENT_FETCHES`] still.
pub const MAX_PENDING_FETCHES: usize = 4 * MAX_CONCURRENT_FETCHES;

/// The registry's answers, keyed by unit: the JSON of the metadata, or `null`
/// for a unit the registry holds no entry for. Kept in memory only.
pub const TOKEN_REGISTRY: CacheNamespace = CacheNamespace::tip_scoped("token-registry", 1, TTL)
    .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_FETCHES);

/// The units whose last request failed. Kept in memory only.
pub const TOKEN_REGISTRY_FAILURES: CacheNamespace =
    CacheNamespace::tip_scoped("token-registry-failures", 1, FAILURE_TTL);

/// Registers the namespaces this module keeps. Without [`TOKEN_REGISTRY`] the
/// registry is asked on every request; without [`TOKEN_REGISTRY_FAILURES`] a
/// unit it fails on is asked for again on every request.
pub fn register<C: CacheStore>(cache: &C) -> Result<(), CacheError> {
    cache.register(TOKEN_REGISTRY)?;
    cache.register(TOKEN_REGISTRY_FAILURES)
}

/// The registry's base URL as the configuration names it: without the slashes
/// it ends in, which would start the request path with two, and none when
/// there is nothing left to ask.
pub fn base_url(configured: Option<&str>) -> Option<String> {
    let url = configured?.trim_end_matches('/');

    (!url.is_empty()).then(|| url.to_string())
}

/// The time limit for one registry request, and for one lookup: a lookup's
/// wait for its turn among the requests under way counts too, so a registry
/// that hangs holds no response longer than one request takes. The client
/// applies this limit. Each request also applies it, so the limit holds even
/// if the client falls back to the default and loses its own limit.
const TIMEOUT: Duration = Duration::from_secs(10);

/// The HTTP client that every registry request shares. `reqwest::Client`
/// holds an internal connection pool, so one instance reuses DNS and TLS across
/// the many concurrent lookups that `/addresses/{address}/extended` starts.
///
/// `get_or_init` builds the client one time. Two concurrent calls do not
/// build two clients. The builder fails only when the TLS backend does not
/// start. In that case, this function uses the default client. As a result,
/// the caller gets a client and does not handle a `Result`. The default
/// client has no time limit, so each request applies [`TIMEOUT`] on its own.
fn client() -> &'static reqwest::Client {
    static CLIENT: OnceLock<reqwest::Client> = OnceLock::new();

    CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .timeout(TIMEOUT)
            .user_agent("Dolos MiniBF")
            .build()
            .unwrap_or_default()
    })
}

#[derive(Debug, Deserialize)]
struct Value<T> {
    value: T,
}

/// A registry entry, as `/metadata/{unit}` serves it.
#[derive(Debug, Deserialize)]
struct Entry {
    name: Option<Value<String>>,
    description: Option<Value<String>>,
    ticker: Option<Value<String>>,
    url: Option<Value<String>>,
    logo: Option<Value<String>>,
    decimals: Option<Value<i32>>,
}

impl Entry {
    /// The entry as Blockfrost serves it, which needs a name and a
    /// description: without either, the unit has no metadata.
    fn into_metadata(self) -> Option<AssetMetadata> {
        Some(AssetMetadata {
            name: self.name?.value,
            description: self.description?.value,
            ticker: self.ticker.map(|x| x.value),
            url: self.url.map(|x| x.value),
            logo: self.logo.map(|x| x.value),
            decimals: self.decimals.map(|x| x.value),
        })
    }
}

/// A registry request that gave no answer to keep: it failed, or the registry
/// answered a status that says nothing about the unit, or a body that does
/// not parse.
#[derive(Debug)]
struct Unanswered(String);

impl std::fmt::Display for Unanswered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "token registry: {}", self.0)
    }
}

impl std::error::Error for Unanswered {}

/// Asks the registry at `url` for `unit`'s entry: `None` for a unit it holds
/// no entry for.
async fn fetch(url: &str, unit: &str) -> Result<Option<AssetMetadata>, Unanswered> {
    let res = client()
        .get(format!("{url}/metadata/{unit}"))
        .timeout(TIMEOUT)
        .send()
        .await
        .map_err(|error| Unanswered(error.to_string()))?;

    match res.status() {
        StatusCode::OK => {}
        // tokens.cardano.org answers 204 for a unit without an entry, the
        // testnet registry 404
        StatusCode::NO_CONTENT | StatusCode::NOT_FOUND => return Ok(None),
        status => return Err(Unanswered(format!("answered {status}"))),
    }

    let entry = res
        .json::<Entry>()
        .await
        .map_err(|error| Unanswered(error.to_string()))?;

    Ok(entry.into_metadata())
}

/// `unit`'s entry holding `value`, at [`COST_PER_BYTE`].
fn costed(unit: &str, value: Vec<u8>) -> CacheEntry {
    let size = (unit.len() + value.len()) as Cost;

    CacheEntry::new(value, size.saturating_mul(COST_PER_BYTE))
}

/// The fill of `unit`'s entry: what the registry at `url` answers, encoded. It
/// is the one place a request is made, so a failure is kept here, once per
/// request, and only when the registry was asked and gave no answer.
async fn fill<C: CacheStore>(
    cache: C,
    url: String,
    unit: String,
) -> Result<CacheEntry, Unanswered> {
    let metadata = fetch(&url, &unit)
        .await
        .inspect_err(|_| keep_failure(&cache, &unit))?;

    let value = serde_json::to_vec(&metadata).map_err(|error| Unanswered(error.to_string()))?;

    Ok(costed(&unit, value))
}

/// Keeps `unit` from being asked for again for [`FAILURE_TTL`].
fn keep_failure<C: CacheStore>(cache: &C, unit: &str) {
    let entry = costed(unit, Vec::new());
    let kept = cache.refill(TOKEN_REGISTRY_FAILURES.name, unit.as_bytes(), || {
        Ok::<_, Infallible>(entry)
    });

    if let Err(error) = kept {
        tracing::warn!(unit, %error, "cannot keep token registry failure");
    }
}

/// The metadata a cached answer holds. One that does not decode is removed,
/// so the next lookup asks again.
fn decoded<C: CacheStore>(cache: &C, unit: &str, value: &[u8]) -> Option<AssetMetadata> {
    serde_json::from_slice(value).unwrap_or_else(|error| {
        tracing::warn!(unit, %error, "dropping a token registry entry that does not decode");

        if let Err(error) = cache.remove(TOKEN_REGISTRY.name, unit.as_bytes()) {
            tracing::warn!(unit, %error, "cannot remove token registry entry");
        }

        None
    })
}

/// `unit`'s entry in the registry at `url`: from the cache, else asked for
/// and kept. A lookup waits at most [`TIMEOUT`] and then gets no metadata,
/// while the request goes on and fills the cache for the next one. A unit
/// whose last request failed is not asked for until [`FAILURE_TTL`] has
/// passed, and gets its older answer meanwhile, if the cache holds one.
pub async fn metadata<C: CacheStore>(cache: &C, url: &str, unit: &str) -> Option<AssetMetadata> {
    metadata_within(cache, url, unit, TIMEOUT).await
}

/// [`metadata`], waiting at most `patience` for the answer.
async fn metadata_within<C: CacheStore>(
    cache: &C,
    url: &str,
    unit: &str,
    patience: Duration,
) -> Option<AssetMetadata> {
    let key = unit.as_bytes();

    // neither namespace is on disk, so these reads do not block
    if cache.get(TOKEN_REGISTRY_FAILURES.name, key).is_some() {
        let value = cache.peek(TOKEN_REGISTRY.name, key)?;
        return decoded(cache, unit, &value);
    }

    // the request runs on a task of its own, shared by every lookup of the
    // unit: a lookup that gives up or goes away leaves it running
    let fill = fill(cache.clone(), url.to_string(), unit.to_string());
    let asked = cache.get_or_fill_async(TOKEN_REGISTRY.name, key, fill);

    match tokio::time::timeout(patience, asked).await {
        Ok(Ok(value)) => decoded(cache, unit, &value),
        Ok(Err(CacheError::Overloaded)) => asked_inline(cache, url, unit, patience).await,
        Ok(Err(CacheError::UnknownNamespace(_))) => fetch(url, unit).await.ok().flatten(),
        Ok(Err(error)) => unanswered(unit, &error),
        Err(_) => None,
    }
}

/// The answer for a unit past [`MAX_PENDING_FETCHES`]: the response needs
/// it, so it asks within its own task, under the same bound on requests at
/// once, and the request stops with the response.
async fn asked_inline<C: CacheStore>(
    cache: &C,
    url: &str,
    unit: &str,
    patience: Duration,
) -> Option<AssetMetadata> {
    let fill = fill(cache.clone(), url.to_string(), unit.to_string());
    let filling = cache.fill_inline(TOKEN_REGISTRY.name, unit.as_bytes(), fill);

    match tokio::time::timeout(patience, filling).await {
        Ok(Ok(value)) => decoded(cache, unit, &value),
        Ok(Err(error)) => unanswered(unit, &error),
        Err(_) => None,
    }
}

/// No metadata for `unit`, logging what the cache did not: it logs a failed
/// fill, and nothing else.
fn unanswered(unit: &str, error: &CacheError) -> Option<AssetMetadata> {
    if error.downcast_fill::<Unanswered>().is_none() {
        tracing::warn!(unit, %error, "token registry lookup failed");
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::Path, routing::get, Router};
    use dolos_core::{config::CacheConfig, QueryLimiter, TieredCache};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
    use tokio::{sync::Semaphore, task::JoinHandle, time::Instant};

    /// A unit with an entry and a unit without one.
    const REGISTERED: &str = "7c7c7c7c";
    const UNREGISTERED: &str = "0000";

    /// The name that makes the registry fail on [`REGISTERED`].
    const FAILING: &str = "fail";

    /// A memory cache that keeps answers under `namespace` and failures
    /// under [`TOKEN_REGISTRY_FAILURES`].
    fn memory_cache(namespace: CacheNamespace) -> impl CacheStore {
        let cache = TieredCache::memory(&CacheConfig::default(), QueryLimiter::default());
        cache.register(namespace).unwrap();
        cache.register(TOKEN_REGISTRY_FAILURES).unwrap();
        cache
    }

    /// Waits until the cache keeps `unit`'s failure, which it does on a task
    /// of its own.
    async fn failure_kept(cache: &impl CacheStore, unit: &str) {
        let deadline = Instant::now() + Duration::from_secs(10);

        while cache
            .get(TOKEN_REGISTRY_FAILURES.name, unit.as_bytes())
            .is_none()
        {
            assert!(Instant::now() < deadline, "the failure of {unit} not kept");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// The URL of a host that refuses connections.
    async fn refusing() -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();

        format!("http://{}", listener.local_addr().unwrap())
    }

    fn entry(name: &str) -> serde_json::Value {
        serde_json::json!({
            "subject": REGISTERED,
            "name": { "value": name, "sequenceNumber": 0, "signatures": [] },
            "description": { "value": "a registered token" },
            "ticker": { "value": "REG" },
            "decimals": { "value": 4 },
        })
    }

    fn metadata_named(name: &str) -> AssetMetadata {
        AssetMetadata {
            name: name.to_string(),
            description: "a registered token".to_string(),
            ticker: Some("REG".to_string()),
            url: None,
            logo: None,
            decimals: Some(4),
        }
    }

    /// A registry that answers [`REGISTERED`] with the entry named `name`, or
    /// 502 while that is [`FAILING`], [`UNREGISTERED`] with 204, `undescribed`
    /// with an entry that has no description, and any other unit with 500. It
    /// records the units it is asked for.
    struct Registry {
        url: String,
        name: Arc<Mutex<String>>,
        asked: Arc<Mutex<Vec<String>>>,
        server: JoinHandle<()>,
    }

    impl Registry {
        async fn start(name: &str) -> Self {
            let name = Arc::new(Mutex::new(name.to_string()));
            let asked: Arc<Mutex<Vec<String>>> = Default::default();

            let router = Router::new().route(
                "/metadata/{unit}",
                get({
                    let (name, asked) = (name.clone(), asked.clone());

                    move |Path(unit): Path<String>| {
                        let (name, asked) = (name.clone(), asked.clone());

                        async move {
                            asked.lock().unwrap().push(unit.clone());
                            let name = name.lock().unwrap().clone();

                            match unit.as_str() {
                                REGISTERED if name == FAILING => {
                                    (StatusCode::BAD_GATEWAY, String::new())
                                }
                                REGISTERED => (StatusCode::OK, entry(&name).to_string()),
                                UNREGISTERED => (StatusCode::NO_CONTENT, String::new()),
                                "undescribed" => (
                                    StatusCode::OK,
                                    serde_json::json!({ "name": { "value": "x" } }).to_string(),
                                ),
                                _ => (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
                            }
                        }
                    }
                }),
            );

            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });

            Self {
                url,
                name,
                asked,
                server,
            }
        }

        fn answer(&self, name: &str) {
            *self.name.lock().unwrap() = name.to_string();
        }

        fn asked(&self, unit: &str) -> usize {
            self.asked
                .lock()
                .unwrap()
                .iter()
                .filter(|x| *x == unit)
                .count()
        }
    }

    impl Drop for Registry {
        fn drop(&mut self) {
            self.server.abort();
        }
    }

    #[tokio::test]
    async fn an_entry_is_asked_for_once() {
        let registry = Registry::start("first").await;
        let cache = memory_cache(TOKEN_REGISTRY);

        for _ in 0..3 {
            let found = metadata(&cache, &registry.url, REGISTERED).await;
            assert_eq!(found, Some(metadata_named("first")));
        }

        assert_eq!(registry.asked(REGISTERED), 1);
    }

    /// 204, and an entry Blockfrost could not serve, are answers: the unit
    /// has no metadata, and the registry is not asked again.
    #[tokio::test]
    async fn not_registered_is_kept_too() {
        let registry = Registry::start("first").await;
        let cache = memory_cache(TOKEN_REGISTRY);

        for unit in [UNREGISTERED, "undescribed"] {
            for _ in 0..3 {
                assert_eq!(metadata(&cache, &registry.url, unit).await, None);
            }

            assert_eq!(registry.asked(unit), 1, "{unit}");
        }
    }

    /// A failed request is no answer: the unit has no metadata for now and
    /// nothing is kept as its entry. It is not asked for again for a while.
    #[tokio::test]
    async fn a_failure_is_no_answer_and_is_not_asked_again_at_once() {
        let registry = Registry::start("first").await;
        let cache = memory_cache(TOKEN_REGISTRY);

        assert_eq!(metadata(&cache, &registry.url, "elsewhere").await, None);
        failure_kept(&cache, "elsewhere").await;

        for _ in 0..3 {
            assert_eq!(metadata(&cache, &registry.url, "elsewhere").await, None);
        }

        assert_eq!(registry.asked("elsewhere"), 1);
        assert_eq!(cache.get(TOKEN_REGISTRY.name, b"elsewhere"), None);

        let refusing = refusing().await;
        assert_eq!(metadata(&cache, &refusing, REGISTERED).await, None);
        assert_eq!(cache.get(TOKEN_REGISTRY.name, REGISTERED.as_bytes()), None);

        // the unit answers once its failure is no longer kept
        failure_kept(&cache, REGISTERED).await;
        assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, None);
        cache
            .remove(TOKEN_REGISTRY_FAILURES.name, REGISTERED.as_bytes())
            .unwrap();

        let found = metadata(&cache, &registry.url, REGISTERED).await;
        assert_eq!(found, Some(metadata_named("first")));
    }

    /// A failure is kept when the lookup that asked is gone before its
    /// request fails: the registry holds the request, the lookup is dropped,
    /// and only then does the registry answer 502.
    #[tokio::test]
    async fn a_failure_is_kept_after_its_lookup_is_gone() {
        let gate = Arc::new(Semaphore::new(0));
        let asked = Arc::new(AtomicUsize::new(0));

        let router = Router::new().route(
            "/metadata/{unit}",
            get({
                let (gate, asked) = (gate.clone(), asked.clone());

                move || {
                    let (gate, asked) = (gate.clone(), asked.clone());

                    async move {
                        asked.fetch_add(1, Ordering::SeqCst);
                        let _permit = gate.acquire().await.unwrap();

                        StatusCode::BAD_GATEWAY
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let cache = memory_cache(TOKEN_REGISTRY);

        let lookup = tokio::spawn({
            let (cache, url) = (cache.clone(), url.clone());

            async move { metadata(&cache, &url, REGISTERED).await }
        });

        let deadline = Instant::now() + Duration::from_secs(10);
        while asked.load(Ordering::SeqCst) == 0 {
            assert!(Instant::now() < deadline, "the registry was not asked");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        lookup.abort();
        assert!(lookup.await.unwrap_err().is_cancelled());

        gate.add_permits(1);
        failure_kept(&cache, REGISTERED).await;

        assert_eq!(metadata(&cache, &url, REGISTERED).await, None);
        assert_eq!(asked.load(Ordering::SeqCst), 1);

        server.abort();
    }

    /// Once the time a failure is kept for has passed, the unit is asked for
    /// again.
    #[tokio::test]
    async fn a_failed_unit_is_asked_for_again_later() {
        let registry = Registry::start("first").await;
        let cache = memory_cache(TOKEN_REGISTRY);
        let passed = CacheNamespace::tip_scoped(TOKEN_REGISTRY_FAILURES.name, 1, Duration::ZERO);
        cache.register(passed).unwrap();

        for _ in 0..3 {
            assert_eq!(metadata(&cache, &registry.url, "elsewhere").await, None);
        }

        assert_eq!(registry.asked("elsewhere"), 3);
    }

    /// A lookup gives up after its patience, whatever the requests ahead of
    /// it take, and keeps no failure: the registry gave no answer yet, and a
    /// unit past the bound on requests under way was never asked for. The
    /// registry accepts connections and never answers.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_lookup_waits_no_longer_than_its_patience() {
        const UNITS: usize = MAX_PENDING_FETCHES + 36;
        const PATIENCE: Duration = Duration::from_millis(300);

        let accepted = Arc::new(AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());

        let registry = tokio::spawn({
            let accepted = accepted.clone();

            async move {
                let mut held = vec![];

                while let Ok((stream, _)) = listener.accept().await {
                    accepted.fetch_add(1, Ordering::SeqCst);
                    held.push(stream);
                }
            }
        });

        let cache = memory_cache(TOKEN_REGISTRY);
        let started = Instant::now();

        let lookups: Vec<_> = (0..UNITS)
            .map(|i| {
                let (cache, url) = (cache.clone(), url.clone());

                tokio::spawn(async move {
                    metadata_within(&cache, &url, &format!("{i:04x}"), PATIENCE).await
                })
            })
            .collect();

        for lookup in lookups {
            assert_eq!(lookup.await.unwrap(), None);
        }

        // far less than the one request the registry is holding takes
        assert!(started.elapsed() < TIMEOUT / 2, "{:?}", started.elapsed());

        // a lookup past the bound on requests under way gives its turn up
        // with its patience, and a request that was waiting takes it
        let asked = accepted.load(Ordering::SeqCst);
        assert!(asked <= 2 * MAX_CONCURRENT_FETCHES, "{asked}");

        for i in 0..UNITS {
            let unit = format!("{i:04x}");
            let kept = cache.get(TOKEN_REGISTRY_FAILURES.name, unit.as_bytes());
            assert_eq!(kept, None, "{unit}");
        }

        // so the next lookup waits for the registry again
        let last = format!("{:04x}", UNITS - 1);
        let again = Instant::now();
        assert_eq!(metadata_within(&cache, &url, &last, PATIENCE).await, None);
        assert!(again.elapsed() >= PATIENCE, "{:?}", again.elapsed());

        registry.abort();
    }

    /// Lookups of one unit share one request, and a lookup that gives up
    /// leaves nothing behind: no task keeps waiting for the request in its
    /// place. The registry holds the request until the lookups are gone.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lookup_that_gives_up_leaves_nothing_behind() {
        const LOOKUPS: usize = 100;
        const PATIENCE: Duration = Duration::from_millis(50);

        let gate = Arc::new(Semaphore::new(0));
        let asked = Arc::new(AtomicUsize::new(0));

        let router = Router::new().route(
            "/metadata/{unit}",
            get({
                let (gate, asked) = (gate.clone(), asked.clone());

                move || {
                    let (gate, asked) = (gate.clone(), asked.clone());

                    async move {
                        asked.fetch_add(1, Ordering::SeqCst);
                        let _permit = gate.acquire().await.unwrap();

                        (StatusCode::OK, entry("first").to_string())
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let cache = memory_cache(TOKEN_REGISTRY);

        let tasks = tokio::runtime::Handle::current().metrics();
        let before = tasks.num_alive_tasks();

        let lookups: Vec<_> = (0..LOOKUPS)
            .map(|_| {
                let (cache, url) = (cache.clone(), url.clone());

                tokio::spawn(
                    async move { metadata_within(&cache, &url, REGISTERED, PATIENCE).await },
                )
            })
            .collect();

        for lookup in lookups {
            assert_eq!(lookup.await.unwrap(), None);
        }

        // what is left is the request: its fill, and a connection task on
        // each side of it
        let deadline = Instant::now() + Duration::from_secs(10);

        loop {
            let left = tasks.num_alive_tasks().saturating_sub(before);

            if left <= 4 {
                break;
            }

            assert!(Instant::now() < deadline, "{left} tasks left");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        gate.add_permits(1);

        while cache
            .get(TOKEN_REGISTRY.name, REGISTERED.as_bytes())
            .is_none()
        {
            assert!(Instant::now() < deadline, "the answer was not kept");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        let found = metadata(&cache, &url, REGISTERED).await;
        assert_eq!(found, Some(metadata_named("first")));
        assert_eq!(asked.load(Ordering::SeqCst), 1);

        server.abort();
    }

    /// An answer is kept in a memory tier that entries with a cost of their
    /// own fill, as `assets` entries do on a node that serves assets.
    #[tokio::test]
    async fn an_answer_is_kept_in_a_full_memory_tier() {
        const FILLER: CacheNamespace =
            CacheNamespace::tip_scoped("filler", 1, Duration::from_secs(3600));

        let config = CacheConfig {
            memory_mb: 1,
            ..CacheConfig::default()
        };

        let cache = TieredCache::memory(&config, QueryLimiter::default());
        cache.register(TOKEN_REGISTRY).unwrap();
        cache.register(TOKEN_REGISTRY_FAILURES).unwrap();
        cache.register(FILLER).unwrap();

        // more than the tier holds, a kibibyte each with its key, so that
        // they leave no room: each costs a hundredth per byte
        for i in 0..1100u16 {
            cache
                .get_or_fill(FILLER.name, &i.to_be_bytes(), || {
                    Ok::<_, Infallible>(CacheEntry::new(vec![0; 1022], 10))
                })
                .await
                .unwrap();
        }

        let held = cache.stats().unwrap();
        let filler = held.namespaces.iter().find(|x| x.name == FILLER.name);
        assert_eq!(filler.unwrap().memory.bytes, 1 << 20, "{held:?}");

        let registry = Registry::start("first").await;

        for unit in [REGISTERED, UNREGISTERED] {
            for _ in 0..3 {
                metadata(&cache, &registry.url, unit).await;
            }

            assert_eq!(registry.asked(unit), 1, "{unit}");
        }
    }

    /// An answer is kept in a memory tier that "not registered" answers fill:
    /// each is smaller than an entry with metadata, which must still find
    /// room among them. The units are spelt in 64 digits, as policy ids are.
    #[tokio::test]
    async fn an_answer_is_kept_in_a_tier_full_of_unregistered_units() {
        const NOT_REGISTERED: &[u8] = b"null";

        let config = CacheConfig {
            memory_mb: 1,
            ..CacheConfig::default()
        };

        let cache = TieredCache::memory(&config, QueryLimiter::default());
        cache.register(TOKEN_REGISTRY).unwrap();
        cache.register(TOKEN_REGISTRY_FAILURES).unwrap();

        // more than the tier holds, so the room they leave is less than one
        // of them takes
        let size = 64 + NOT_REGISTERED.len();

        for i in 0..(1 << 20) / size + 100 {
            let unit = format!("{i:064x}");

            cache
                .get_or_fill_async(TOKEN_REGISTRY.name, unit.clone().as_bytes(), async move {
                    Ok::<_, Infallible>(costed(&unit, NOT_REGISTERED.to_vec()))
                })
                .await
                .unwrap();
        }

        let held = cache.stats().unwrap();
        let answers = held
            .namespaces
            .iter()
            .find(|x| x.name == TOKEN_REGISTRY.name);
        let bytes = answers.unwrap().memory.bytes;
        assert!(bytes > (1 << 20) - size as u64, "{held:?}");

        let registry = Registry::start("first").await;

        for _ in 0..3 {
            let found = metadata(&cache, &registry.url, REGISTERED).await;
            assert_eq!(found, Some(metadata_named("first")));
        }

        assert_eq!(registry.asked(REGISTERED), 1);
    }

    /// An entry that does not decode is removed, and its unit asked for
    /// again.
    #[tokio::test]
    async fn an_entry_that_does_not_decode_is_dropped() {
        let registry = Registry::start("first").await;
        let cache = memory_cache(TOKEN_REGISTRY);
        let key = REGISTERED.as_bytes();

        cache
            .get_or_fill_async(TOKEN_REGISTRY.name, key, async {
                Ok::<_, Infallible>(costed(REGISTERED, b"not json".to_vec()))
            })
            .await
            .unwrap();

        assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, None);
        assert_eq!(cache.get(TOKEN_REGISTRY.name, key), None);
        assert_eq!(registry.asked(REGISTERED), 0);

        let found = metadata(&cache, &registry.url, REGISTERED).await;
        assert_eq!(found, Some(metadata_named("first")));
    }

    #[test]
    fn the_base_url_ends_in_no_slash_and_is_not_empty() {
        assert_eq!(base_url(None), None);
        assert_eq!(base_url(Some("")), None);
        assert_eq!(base_url(Some("/")), None);

        for configured in [
            "https://tokens.example.com",
            "https://tokens.example.com/",
            "https://tokens.example.com//",
        ] {
            let url = base_url(Some(configured));
            assert_eq!(url.as_deref(), Some("https://tokens.example.com"));
        }

        let nested = base_url(Some("https://example.com/registry/"));
        assert_eq!(nested.as_deref(), Some("https://example.com/registry"));
    }

    /// Past its time limit an answer is served while one request replaces
    /// it. A request that fails keeps it, and the unit is not asked for again
    /// for a while: every lookup gets the old answer meanwhile.
    #[tokio::test]
    async fn an_old_answer_is_served_until_a_new_one_arrives() {
        let registry = Registry::start("first").await;
        let stale = CacheNamespace::tip_scoped(TOKEN_REGISTRY.name, 1, Duration::ZERO)
            .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_FETCHES);
        let cache = memory_cache(stale);

        let first = Some(metadata_named("first"));
        assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, first);

        // the registry fails: the lookup that starts the refill gets the old
        // answer, and so does every lookup once the refill failed
        registry.answer(FAILING);
        assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, first);
        failure_kept(&cache, REGISTERED).await;

        for _ in 0..3 {
            assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, first);
        }

        assert_eq!(registry.asked(REGISTERED), 2);

        // the registry answers again, and the failure is no longer kept: the
        // lookup that starts the refill gets the old answer, a later one the
        // new
        registry.answer("second");
        cache
            .remove(TOKEN_REGISTRY_FAILURES.name, REGISTERED.as_bytes())
            .unwrap();

        let second = Some(metadata_named("second"));
        let deadline = Instant::now() + Duration::from_secs(10);

        loop {
            let found = metadata(&cache, &registry.url, REGISTERED).await;

            if found == second {
                break;
            }

            assert_eq!(found, first);
            assert!(Instant::now() < deadline, "the new answer never arrived");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Lookups of many units ask the registry at most
    /// [`MAX_CONCURRENT_FETCHES`] at a time, those past
    /// [`MAX_PENDING_FETCHES`] included, and each gets its answer.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn the_registry_is_asked_a_bounded_number_at_once() {
        const UNITS: usize = MAX_PENDING_FETCHES + 36;

        let gate = Arc::new(Semaphore::new(0));
        let (running, most) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));

        let router = Router::new().route(
            "/metadata/{unit}",
            get({
                let (gate, running, most) = (gate.clone(), running.clone(), most.clone());

                move || {
                    let (gate, running, most) = (gate.clone(), running.clone(), most.clone());

                    async move {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        most.fetch_max(now, Ordering::SeqCst);
                        let _permit = gate.acquire().await.unwrap();
                        running.fetch_sub(1, Ordering::SeqCst);

                        StatusCode::NO_CONTENT
                    }
                }
            }),
        );

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        let cache = memory_cache(TOKEN_REGISTRY);

        let lookups: Vec<_> = (0..UNITS)
            .map(|i| {
                let (cache, url) = (cache.clone(), url.clone());
                tokio::spawn(async move { metadata(&cache, &url, &format!("{i:04x}")).await })
            })
            .collect();

        let deadline = Instant::now() + Duration::from_secs(10);
        while running.load(Ordering::SeqCst) < MAX_CONCURRENT_FETCHES {
            assert!(Instant::now() < deadline, "{running:?}");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // room for any request past the bound to arrive
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(most.load(Ordering::SeqCst), MAX_CONCURRENT_FETCHES);

        gate.add_permits(UNITS);

        for lookup in lookups {
            assert_eq!(lookup.await.unwrap(), None);
        }

        server.abort();
        assert_eq!(most.load(Ordering::SeqCst), MAX_CONCURRENT_FETCHES);
    }

    /// Without the namespace, the registry is asked on every lookup.
    #[tokio::test]
    async fn without_the_namespace_every_lookup_asks() {
        let registry = Registry::start("first").await;
        let cache = TieredCache::memory(&CacheConfig::default(), QueryLimiter::default());

        for _ in 0..2 {
            let found = metadata(&cache, &registry.url, REGISTERED).await;
            assert_eq!(found, Some(metadata_named("first")));
        }

        assert_eq!(registry.asked(REGISTERED), 2);
    }
}
