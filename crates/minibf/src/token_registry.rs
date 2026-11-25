//! The off-chain token registry that `token_registry_url` names: the metadata
//! of `/assets/{asset}` and the decimals of `/addresses/{address}/extended`.
//!
//! Nothing on chain pins a registry entry, and its owner can change it at any
//! time, so an answer is kept for [`TTL`] rather than checked against a hash.
//! Past it, the old answer is served while one request replaces it, and a
//! failed request keeps it, so a registry outage does not empty the cache.
//! "Not registered" is an answer too, the one most units get.

use std::{sync::OnceLock, time::Duration};

use blockfrost_openapi::models::asset_metadata::AssetMetadata;
use dolos_core::{CacheEntry, CacheError, CacheNamespace, CacheStore};
use reqwest::StatusCode;
use serde::Deserialize;

/// How long an answer is served before the registry is asked again.
const TTL: Duration = Duration::from_secs(60 * 60);

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

/// Registers [`TOKEN_REGISTRY`]. Without it the registry is asked on every
/// request.
pub fn register<C: CacheStore>(cache: &C) -> Result<(), CacheError> {
    cache.register(TOKEN_REGISTRY)
}

/// The time limit for one registry request. The client applies this limit.
/// Each request also applies it, so the limit holds even if the client falls
/// back to the default and loses its own limit.
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

/// The fill of `unit`'s entry: what the registry at `url` answers, encoded.
async fn fill(url: String, unit: String) -> Result<CacheEntry, Unanswered> {
    let metadata = fetch(&url, &unit).await?;
    let value = serde_json::to_vec(&metadata).map_err(|error| Unanswered(error.to_string()))?;

    Ok(CacheEntry::new(value, 0))
}

/// `unit`'s entry in the registry at `url`: from the cache, else asked for
/// and kept. A registry that does not answer gives no metadata and keeps
/// nothing, unless the cache holds an older answer, which it serves.
pub async fn metadata<C: CacheStore>(cache: &C, url: &str, unit: &str) -> Option<AssetMetadata> {
    let key = unit.as_bytes();
    let fill = || fill(url.to_string(), unit.to_string());

    // the fill runs on its own task, so it reaches its end, and fills the
    // cache, whether or not anyone still waits for it
    let filled = match cache
        .get_or_fill_async(TOKEN_REGISTRY.name, key, fill())
        .await
    {
        // the response needs the answer, so it asks within its own request,
        // under the same bound on requests at once
        Err(CacheError::Overloaded) => cache.fill_inline(TOKEN_REGISTRY.name, key, fill()).await,
        filled => filled,
    };

    match filled {
        Ok(value) => serde_json::from_slice(&value).unwrap_or_else(|error| {
            tracing::warn!(unit, %error, "dropping a token registry entry that does not decode");
            None
        }),
        Err(CacheError::UnknownNamespace(_)) => fetch(url, unit).await.ok().flatten(),
        // the cache logged why
        Err(_) => None,
    }
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

    fn memory_cache(namespace: CacheNamespace) -> impl CacheStore {
        let cache = TieredCache::memory(&CacheConfig::default(), QueryLimiter::default());
        cache.register(namespace).unwrap();
        cache
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

    /// A failed request is no answer: the unit has no metadata for now, and
    /// the next lookup asks again.
    #[tokio::test]
    async fn a_failure_is_not_kept() {
        let registry = Registry::start("first").await;
        let cache = memory_cache(TOKEN_REGISTRY);

        for _ in 0..3 {
            assert_eq!(metadata(&cache, &registry.url, "elsewhere").await, None);
        }

        assert_eq!(registry.asked("elsewhere"), 3);

        let unreachable = "http://127.0.0.1:1";
        assert_eq!(metadata(&cache, unreachable, REGISTERED).await, None);
        assert_eq!(cache.get(TOKEN_REGISTRY.name, REGISTERED.as_bytes()), None);
    }

    /// Past its time limit an answer is served while one request replaces
    /// it, and a request that fails keeps it.
    #[tokio::test]
    async fn an_old_answer_is_served_until_a_new_one_arrives() {
        let registry = Registry::start("first").await;
        let stale = CacheNamespace::tip_scoped(TOKEN_REGISTRY.name, 1, Duration::ZERO)
            .with_fill_limits(MAX_CONCURRENT_FETCHES, MAX_PENDING_FETCHES);
        let cache = memory_cache(stale);

        let first = Some(metadata_named("first"));
        assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, first);

        // the registry fails: each lookup gets the old answer, and a third
        // request means the refill of the second ended, failed, and kept it
        registry.answer(FAILING);
        let deadline = Instant::now() + Duration::from_secs(10);

        while registry.asked(REGISTERED) < 3 {
            assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, first);
            assert!(Instant::now() < deadline, "no refill started");
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        assert_eq!(metadata(&cache, &registry.url, REGISTERED).await, first);

        // the registry answers again: the lookup that starts the refill gets
        // the old answer, a later one the new
        registry.answer("second");
        let second = Some(metadata_named("second"));

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
