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

