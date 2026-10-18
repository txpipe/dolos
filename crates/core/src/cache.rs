//! The cache store: disposable bytes derived from the other stores (decision
//! 0059).
//!
//! A use registers a [`CacheNamespace`] and keeps entries under it, keyed and
//! valued by bytes in its own encoding. Nothing here enters the WAL, steles,
//! restore or any ledger computation; deleting the cache changes no answer.
//!
//! [`TieredCache`] is the store: the memory tier
//! ([`MemoryCacheTier`]) in front of a [`CacheDiskTier`]. Entries built from
//! immutable input go to both tiers, tip-scoped ones stay in memory.

use std::{
    collections::HashMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, PoisonError, RwLock, Weak,
    },
    time::{Duration, Instant},
};

use bytes::Bytes;
use futures_util::future::{BoxFuture, FutureExt as _, Shared};
use opentelemetry::{
    global,
    metrics::{Counter, Histogram, ObservableGauge},
    KeyValue,
};
use thiserror::Error;
use tokio::{
    runtime::Handle,
    sync::{oneshot, Semaphore},
};

use crate::{builtin::MemoryCacheTier, config::CacheConfig, QueryLimiter};

/// What an entry cost to build, in the unit its use measures.
pub type Cost = u64;

/// What a fill fails with. Every waiter receives it and nothing is cached.
pub type FillError = Box<dyn std::error::Error + Send + Sync>;

/// How long an entry stays valid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Validity {
    /// Built from sealed data only: never stale, at most incomplete. Kept in
    /// both tiers.
    Immutable,
    /// Valid for `ttl`, then served stale while one fill replaces it. Kept in
    /// memory only.
    TipScoped { ttl: Duration },
}

/// Bounds on a namespace's async fills.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FillLimits {
    /// Fills running at once.
    pub concurrent: usize,
    /// Fills under way, those waiting to run included. A miss past this
    /// starts nothing and gets [`CacheError::Overloaded`].
    pub pending: usize,
}

/// A namespace as its use registers it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheNamespace {
    pub name: &'static str,
    /// Bumped whenever the encoding, or the computation behind it, changes.
    pub version: u32,
    pub validity: Validity,
    /// The least cost an entry must have to be kept.
    pub admission: Cost,
    pub fill_limits: Option<FillLimits>,
}

impl CacheNamespace {
    pub const fn immutable(name: &'static str, version: u32) -> Self {
        Self {
            name,
            version,
            validity: Validity::Immutable,
            admission: 0,
            fill_limits: None,
        }
    }

    pub const fn tip_scoped(name: &'static str, version: u32, ttl: Duration) -> Self {
        Self {
            name,
            version,
            validity: Validity::TipScoped { ttl },
            admission: 0,
            fill_limits: None,
        }
    }

    pub const fn with_admission(mut self, admission: Cost) -> Self {
        self.admission = admission;
        self
    }

    pub const fn with_fill_limits(mut self, concurrent: usize, pending: usize) -> Self {
        self.fill_limits = Some(FillLimits {
            concurrent,
            pending,
        });
        self
    }

    fn on_disk(&self) -> bool {
        self.validity == Validity::Immutable
    }
}

/// A value and what it cost to build.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CacheEntry {
    pub value: Bytes,
    pub cost: Cost,
}

impl CacheEntry {
    pub fn new(value: impl Into<Bytes>, cost: Cost) -> Self {
        Self {
            value: value.into(),
            cost,
        }
    }
}

#[derive(Debug, Clone, Error)]
pub enum CacheError {
    #[error("cache namespace {0} is not registered")]
    UnknownNamespace(String),

    #[error("cache fill failed: {0}")]
    Fill(Arc<dyn std::error::Error + Send + Sync>),

    #[error("cache fill could not run: {0}")]
    Limiter(String),

    #[error("cache fill ended without a result")]
    Aborted,

    #[error("too many cache fills under way")]
    Overloaded,

    #[error("cache fills need a Tokio runtime")]
    NoRuntime,

    #[error("cache disk tier: {0}")]
    Disk(String),
}

impl CacheError {
    pub fn disk(error: impl std::fmt::Display) -> Self {
        Self::Disk(error.to_string())
    }

    /// The fill's own error, if it was one of type `E`.
    pub fn downcast_fill<E: std::error::Error + 'static>(&self) -> Option<&E> {
        match self {
            Self::Fill(error) => error.downcast_ref(),
            _ => None,
        }
    }

    fn fill(error: impl Into<FillError>) -> Self {
        Self::Fill(Arc::from(error.into()))
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TierStats {
    pub entries: u64,
    pub bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamespaceStats {
    pub name: &'static str,
    pub version: u32,
    pub memory: TierStats,
    pub disk: TierStats,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CacheStats {
    pub namespaces: Vec<NamespaceStats>,
}

/// A disposable store of derived bytes, by namespace and key.
#[trait_variant::make(Send)]
pub trait CacheStore: Clone + Send + Sync + 'static {
    /// Registers `namespace`, replacing an earlier registration of its name.
    /// Entries kept under another format version are wiped, and its fills
    /// still under way keep nothing.
    fn register(&self, namespace: CacheNamespace) -> Result<(), CacheError>;

    /// The fresh entry under `key`, if a tier holds one. May read the disk
    /// tier, so async code calls it from a blocking task.
    fn get(&self, namespace: &str, key: &[u8]) -> Option<Bytes>;

    fn remove(&self, namespace: &str, key: &[u8]) -> Result<(), CacheError>;

    fn wipe(&self, namespace: &str) -> Result<(), CacheError>;

    fn stats(&self) -> Result<CacheStats, CacheError>;

    /// A durable commit of the disk tier, called at shutdown.
    fn flush(&self) -> Result<(), CacheError>;

    /// The entry under `key`, or the value `fill` builds for it.
    ///
    /// Concurrent misses on one key share one fill. It runs in its own task
    /// under the query limiter, so a caller that goes away does not stop it.
    /// A stale tip-scoped entry is returned while the fill replaces it.
    async fn get_or_fill<F, E>(
        &self,
        namespace: &str,
        key: &[u8],
        fill: F,
    ) -> Result<Bytes, CacheError>
    where
        F: FnOnce() -> Result<CacheEntry, E> + Send + 'static,
        E: Into<FillError> + 'static;

    /// Replaces the entry under `key` with what `fill` builds, in the
    /// background. Does nothing while a fill of the key is under way.
    ///
    /// The fill is spawned on the caller's Tokio runtime, and a runtime that
    /// is shutting down drops it; called outside one, it fails with
    /// [`CacheError::NoRuntime`].
    fn refill<F, E>(&self, namespace: &str, key: &[u8], fill: F) -> Result<(), CacheError>
    where
        F: FnOnce() -> Result<CacheEntry, E> + Send + 'static,
        E: Into<FillError> + 'static;

    /// [`get_or_fill`](Self::get_or_fill) for fills that wait on the network
    /// rather than the store: `fill` runs on its own task, outside the query
    /// limiter and within the namespace's [`FillLimits`].
    async fn get_or_fill_async<Fut, E>(
        &self,
        namespace: &str,
        key: &[u8],
        fill: Fut,
    ) -> Result<Bytes, CacheError>
    where
        Fut: Future<Output = Result<CacheEntry, E>> + Send + 'static,
        E: Into<FillError> + 'static;

    /// The entry under `key`, or the value `fill` builds for it in the
    /// caller's task: the answer to a miss that
    /// [`get_or_fill_async`](Self::get_or_fill_async) refused with
    /// [`CacheError::Overloaded`]. `fill` runs outside the pending limit but
    /// within the concurrent one, joins no flight and stops with its caller.
    async fn fill_inline<Fut, E>(
        &self,
        namespace: &str,
        key: &[u8],
        fill: Fut,
    ) -> Result<Bytes, CacheError>
    where
        Fut: Future<Output = Result<CacheEntry, E>> + Send,
        E: Into<FillError>;
}

/// The persistent tier a backend implements. Calls block on I/O.
pub trait CacheDiskTier: Send + Sync + 'static {
    /// Opens `namespace` at `version`, wiping it if it was kept under another.
    fn open_namespace(&self, namespace: &str, version: u32) -> Result<(), CacheError>;

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<CacheEntry>, CacheError>;

    fn put(&self, namespace: &str, key: &[u8], value: &[u8], cost: Cost) -> Result<(), CacheError>;

    fn remove(&self, namespace: &str, key: &[u8]) -> Result<(), CacheError>;

    /// Drops every entry of `namespace`, recording its keys first for
    /// [`previous_keys`](Self::previous_keys).
    fn wipe(&self, namespace: &str) -> Result<(), CacheError>;

    /// The keys `namespace` held when it was last wiped.
    fn previous_keys(&self, namespace: &str) -> Result<Vec<Bytes>, CacheError>;

    fn keys(&self, namespace: &str) -> Result<Vec<Bytes>, CacheError>;

    fn bytes(&self, namespace: &str) -> Result<u64, CacheError>;

    fn entries(&self, namespace: &str) -> Result<u64, CacheError> {
        Ok(self.keys(namespace)?.len() as u64)
    }

    /// A durable commit of every write so far.
    fn flush(&self) -> Result<(), CacheError>;
}

/// A key under one format version of its namespace, so a miss never joins a
/// fill of another version.
type FlightKey = (&'static str, u32, Bytes);

fn flight_key(ns: &CacheNamespace, key: &[u8]) -> FlightKey {
    (ns.name, ns.version, Bytes::copy_from_slice(key))
}

/// The one fill under way for a key, awaited by every caller that needs it.
type Flight = Shared<BoxFuture<'static, Result<Bytes, CacheError>>>;

struct Registered {
    spec: CacheNamespace,
    permits: Option<Arc<Semaphore>>,
    pending: AtomicUsize,
    /// False once a registration under another format version replaces this
    /// one.
    live: RwLock<bool>,
}

impl Registered {
    /// Runs `keep` unless this registration was replaced. `register` waits
    /// for a running `keep` before it wipes, so nothing lands after the wipe.
    fn keep_while_live(&self, keep: impl FnOnce()) {
        let live = self.live.read().unwrap_or_else(PoisonError::into_inner);

        if *live {
            keep();
        }
    }

    fn retire(&self) {
        *self.live.write().unwrap_or_else(PoisonError::into_inner) = false;
    }
}

enum Lookup {
    Fresh(Bytes),
    Stale(Bytes),
    Miss,
}

/// The cache store over the memory tier and the disk tier `D`.
pub struct TieredCache<D: CacheDiskTier> {
    inner: Arc<Inner<D>>,
}

impl<D: CacheDiskTier> Clone for TieredCache<D> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct Inner<D> {
    enabled: bool,
    admission: HashMap<String, Cost>,
    limiter: QueryLimiter,
    namespaces: Mutex<HashMap<&'static str, Arc<Registered>>>,
    flights: Mutex<HashMap<FlightKey, Flight>>,
    memory: Mutex<MemoryCacheTier>,
    disk: D,
    metrics: Metrics,
}

impl TieredCache<crate::builtin::NoDiskTier> {
    /// A cache with the memory tier only.
    pub fn memory(config: &CacheConfig, limiter: QueryLimiter) -> Self {
        Self::new(config, crate::builtin::NoDiskTier, limiter)
    }
}

impl<D: CacheDiskTier> TieredCache<D> {
    /// Fills run under `limiter`, the domain's query limiter.
    pub fn new(config: &CacheConfig, disk: D, limiter: QueryLimiter) -> Self {
        let memory_budget = if config.enabled {
            config.memory_budget_bytes
        } else {
            0
        };

        let inner = Arc::new_cyclic(|weak: &Weak<Inner<D>>| Inner {
            enabled: config.enabled,
            admission: config.admission.clone().into_iter().collect(),
            limiter,
            namespaces: Mutex::default(),
            flights: Mutex::default(),
            memory: Mutex::new(MemoryCacheTier::new(memory_budget)),
            disk,
            metrics: Metrics::new(weak.clone()),
        });

        Self { inner }
    }

    pub fn disk(&self) -> &D {
        &self.inner.disk
    }

    fn namespace(&self, name: &str) -> Result<Arc<Registered>, CacheError> {
        lock(&self.inner.namespaces)
            .get(name)
            .cloned()
            .ok_or_else(|| CacheError::UnknownNamespace(name.to_string()))
    }

    fn uses_disk(&self, ns: &CacheNamespace) -> bool {
        self.inner.enabled && ns.on_disk()
    }

    fn lookup_memory(&self, ns: &CacheNamespace, key: &[u8]) -> Lookup {
        let found = lock(&self.inner.memory).get(ns.name, key);
        let metrics = &self.inner.metrics;

        let Some((value, stored_at)) = found else {
            metrics.miss(ns.name, Tier::Memory);
            return Lookup::Miss;
        };

        metrics.hit(ns.name, Tier::Memory);

        match ns.validity {
            Validity::TipScoped { ttl } if stored_at.elapsed() >= ttl => Lookup::Stale(value),
            _ => Lookup::Fresh(value),
        }
    }

    /// Reads the disk tier and promotes a hit to memory. A disk error is a
    /// miss: the cache is disposable.
    fn lookup_disk(&self, ns: &Registered, key: &Bytes) -> Option<Bytes> {
        let spec = &ns.spec;

        if !self.uses_disk(spec) {
            return None;
        }

        let metrics = &self.inner.metrics;

        match self.inner.disk.get(spec.name, key) {
            Ok(Some(entry)) => {
                metrics.hit(spec.name, Tier::Disk);
                let value = entry.value.clone();
                ns.keep_while_live(|| self.keep_in_memory(spec, key.clone(), entry));
                Some(value)
            }
            Ok(None) => {
                metrics.miss(spec.name, Tier::Disk);
                None
            }
            Err(error) => {
                tracing::warn!(namespace = spec.name, %error, "cache disk read failed");
                metrics.miss(spec.name, Tier::Disk);
                None
            }
        }
    }

    fn admits(&self, ns: &CacheNamespace, entry: &CacheEntry) -> bool {
        self.inner.enabled && entry.cost >= ns.admission
    }

    fn keep_in_memory(&self, ns: &CacheNamespace, key: Bytes, entry: CacheEntry) {
        let mut memory = lock(&self.inner.memory);

        if !self.admits(ns, &entry) {
            memory.remove(ns.name, &key);
            return;
        }

        let inserted = memory.insert(ns.name, key, entry.value, entry.cost, Instant::now());
        drop(memory);

        for evicted in inserted.evicted {
            self.inner.metrics.evicted(evicted, Tier::Memory);
        }
    }

    /// Keeps a filled entry in memory and, for an immutable namespace, on
    /// disk, unless a new format version replaced `ns` meanwhile. Blocks on
    /// the disk write.
    fn store(&self, ns: &Registered, key: Bytes, entry: CacheEntry) {
        let spec = &ns.spec;
        let on_disk = self.admits(spec, &entry) && self.uses_disk(spec);
        let (value, cost) = (entry.value.clone(), entry.cost);

        ns.keep_while_live(|| {
            self.keep_in_memory(spec, key.clone(), entry);

            if on_disk {
                if let Err(error) = self.inner.disk.put(spec.name, &key, &value, cost) {
                    tracing::warn!(namespace = spec.name, %error, "cache disk write failed");
                }
            }
        });
    }

    /// Records `key`'s flight under the caller's flights lock and releases
    /// the lock before the fill is spawned: a runtime that is shutting down
    /// drops a spawned task unstarted, on the spawning thread, and its
    /// retirement takes the lock.
    fn record_flight(
        &self,
        mut flights: MutexGuard<'_, HashMap<FlightKey, Flight>>,
        key: FlightKey,
        pending: Option<Arc<Registered>>,
    ) -> (Flight, Retire<D>) {
        let (done, result) = oneshot::channel();

        let flight = async move { result.await.unwrap_or(Err(CacheError::Aborted)) }
            .boxed()
            .shared();

        flights.insert(key.clone(), flight.clone());
        drop(flights);

        let retire = Retire {
            cache: self.clone(),
            key,
            pending,
            done: Some(done),
        };

        (flight, retire)
    }

    /// Starts a blocking fill of `key` as the key's flight.
    fn start_fill<F, E>(
        &self,
        flights: MutexGuard<'_, HashMap<FlightKey, Flight>>,
        ns: Arc<Registered>,
        key: FlightKey,
        fill: F,
        read_disk: bool,
    ) -> Result<Flight, CacheError>
    where
        F: FnOnce() -> Result<CacheEntry, E> + Send + 'static,
        E: Into<FillError> + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| CacheError::NoRuntime)?;

        let cache = self.clone();
        let entry_key = key.2.clone();
        let (flight, retire) = self.record_flight(flights, key, None);

        runtime.spawn(async move {
            let limiter = cache.inner.limiter.clone();

            let run = move || {
                if read_disk {
                    if let Some(value) = cache.lookup_disk(&ns, &entry_key) {
                        return Ok(value);
                    }
                }

                let name = ns.spec.name;
                let started = Instant::now();
                let filled = fill();
                cache.inner.metrics.filled(name, started.elapsed());

                let entry = filled.map_err(|error| {
                    let error = CacheError::fill(error);
                    tracing::warn!(namespace = name, %error, "cache fill failed");
                    error
                })?;

                let value = entry.value.clone();
                cache.store(&ns, entry_key, entry);
                Ok(value)
            };

            let filled = limiter
                .run_blocking(run)
                .await
                .unwrap_or_else(|error| Err(CacheError::Limiter(error.to_string())));

            retire.finish(filled);
        });

        Ok(flight)
    }

    /// Starts an async fill of `key`, within the namespace's fill limits, as
    /// the key's flight.
    fn start_async_fill<Fut, E>(
        &self,
        flights: MutexGuard<'_, HashMap<FlightKey, Flight>>,
        ns: Arc<Registered>,
        key: FlightKey,
        fill: Fut,
    ) -> Result<Flight, CacheError>
    where
        Fut: Future<Output = Result<CacheEntry, E>> + Send + 'static,
        E: Into<FillError> + 'static,
    {
        let runtime = Handle::try_current().map_err(|_| CacheError::NoRuntime)?;

        if let Some(limits) = ns.spec.fill_limits {
            if ns.pending.load(Ordering::SeqCst) >= limits.pending {
                return Err(CacheError::Overloaded);
            }
        }

        ns.pending.fetch_add(1, Ordering::SeqCst);

        let cache = self.clone();
        let entry_key = key.2.clone();
        let (flight, retire) = self.record_flight(flights, key, Some(ns.clone()));

        let run = cache.run_async_fill(ns, entry_key, fill);

        runtime.spawn(async move {
            let filled = run.await;
            retire.finish(filled);
        });

        Ok(flight)
    }
    /// An async fill of `key` under the namespace's concurrent limit: the
    /// disk tier first, then `fill`, whose entry is kept.
    async fn run_async_fill<Fut, E>(
        self,
        ns: Arc<Registered>,
        key: Bytes,
        fill: Fut,
    ) -> Result<Bytes, CacheError>
    where
        Fut: Future<Output = Result<CacheEntry, E>>,
        E: Into<FillError>,
    {
        if self.uses_disk(&ns.spec) {
            let (cache, ns, key) = (self.clone(), ns.clone(), key.clone());
            let found = tokio::task::spawn_blocking(move || cache.lookup_disk(&ns, &key))
                .await
                .ok()
                .flatten();

            if let Some(value) = found {
                return Ok(value);
            }
        }

        let _permit =
            match &ns.permits {
                Some(permits) => {
                    Some(permits.clone().acquire_owned().await.map_err(|_| {
                        CacheError::Limiter("cache fill permits closed".to_string())
                    })?)
                }
                None => None,
            };

        let started = Instant::now();
        let filled = fill.await.map_err(CacheError::fill);
        self.inner.metrics.filled(ns.spec.name, started.elapsed());

        let entry = filled.inspect_err(|error| {
            tracing::warn!(namespace = ns.spec.name, %error, "cache fill failed");
        })?;

        let value = entry.value.clone();

        if self.uses_disk(&ns.spec) {
            let stored = tokio::task::spawn_blocking(move || self.store(&ns, key, entry));
            let _ = stored.await;
        } else {
            self.store(&ns, key, entry);
        }

        Ok(value)
    }
}

/// Clears a fill's flight when its task ends, panics, aborts and a task
/// dropped unstarted included, so the next miss starts afresh. The flight's
/// waiters wake after it is cleared.
struct Retire<D: CacheDiskTier> {
    cache: TieredCache<D>,
    key: FlightKey,
    pending: Option<Arc<Registered>>,
    done: Option<oneshot::Sender<Result<Bytes, CacheError>>>,
}

impl<D: CacheDiskTier> Retire<D> {
    fn finish(mut self, filled: Result<Bytes, CacheError>) {
        let done = self.done.take();
        drop(self);

        if let Some(done) = done {
            let _ = done.send(filled);
        }
    }
}

impl<D: CacheDiskTier> Drop for Retire<D> {
    fn drop(&mut self) {
        let mut flights = lock(&self.cache.inner.flights);
        flights.remove(&self.key);

        if let Some(ns) = &self.pending {
            ns.pending.fetch_sub(1, Ordering::SeqCst);
        }
    }
}

impl<D: CacheDiskTier> CacheStore for TieredCache<D> {
    fn register(&self, mut namespace: CacheNamespace) -> Result<(), CacheError> {
        if let Some(admission) = self.inner.admission.get(namespace.name) {
            namespace.admission = *admission;
        }

        let mut namespaces = lock(&self.inner.namespaces);

        let replaced = namespaces
            .get(namespace.name)
            .filter(|known| known.spec.version != namespace.version);

        if let Some(known) = replaced {
            known.retire();
            lock(&self.inner.memory).wipe(namespace.name);
        }

        if self.uses_disk(&namespace) {
            self.inner
                .disk
                .open_namespace(namespace.name, namespace.version)?;
        }

        let permits = namespace
            .fill_limits
            .map(|limits| Arc::new(Semaphore::new(limits.concurrent.max(1))));

        namespaces.insert(
            namespace.name,
            Arc::new(Registered {
                spec: namespace,
                permits,
                pending: AtomicUsize::new(0),
                live: RwLock::new(true),
            }),
        );

        Ok(())
    }

    fn get(&self, namespace: &str, key: &[u8]) -> Option<Bytes> {
        let ns = self.namespace(namespace).ok()?;

        match self.lookup_memory(&ns.spec, key) {
            Lookup::Fresh(value) => Some(value),
            Lookup::Stale(_) => None,
            Lookup::Miss => self.lookup_disk(&ns, &Bytes::copy_from_slice(key)),
        }
    }

    fn remove(&self, namespace: &str, key: &[u8]) -> Result<(), CacheError> {
        let ns = self.namespace(namespace)?;

        lock(&self.inner.memory).remove(ns.spec.name, key);

        if self.uses_disk(&ns.spec) {
            self.inner.disk.remove(ns.spec.name, key)?;
        }

        Ok(())
    }

    fn wipe(&self, namespace: &str) -> Result<(), CacheError> {
        let ns = self.namespace(namespace)?;

        lock(&self.inner.memory).wipe(ns.spec.name);

        if self.uses_disk(&ns.spec) {
            self.inner.disk.wipe(ns.spec.name)?;
        }

        Ok(())
    }

    fn stats(&self) -> Result<CacheStats, CacheError> {
        let mut namespaces: Vec<_> = lock(&self.inner.namespaces).values().cloned().collect();
        namespaces.sort_by_key(|ns| ns.spec.name);

        let mut stats = CacheStats::default();

        for ns in namespaces {
            let memory = lock(&self.inner.memory).usage(ns.spec.name);

            let disk = if self.uses_disk(&ns.spec) {
                TierStats {
                    entries: self.inner.disk.entries(ns.spec.name)?,
                    bytes: self.inner.disk.bytes(ns.spec.name)?,
                }
            } else {
                TierStats::default()
            };

            stats.namespaces.push(NamespaceStats {
                name: ns.spec.name,
                version: ns.spec.version,
                memory,
                disk,
            });
        }

        Ok(stats)
    }

    fn flush(&self) -> Result<(), CacheError> {
        if self.inner.enabled {
            self.inner.disk.flush()?;
        }

        Ok(())
    }

    async fn get_or_fill<F, E>(
        &self,
        namespace: &str,
        key: &[u8],
        fill: F,
    ) -> Result<Bytes, CacheError>
    where
        F: FnOnce() -> Result<CacheEntry, E> + Send + 'static,
        E: Into<FillError> + 'static,
    {
        let ns = self.namespace(namespace)?;
        let key = flight_key(&ns.spec, key);

        let flight = {
            let flights = lock(&self.inner.flights);

            match self.lookup_memory(&ns.spec, &key.2) {
                Lookup::Fresh(value) => return Ok(value),
                Lookup::Stale(value) => {
                    if !flights.contains_key(&key) {
                        drop(self.start_fill(flights, ns, key, fill, false)?);
                    }

                    return Ok(value);
                }
                Lookup::Miss => match flights.get(&key) {
                    Some(flight) => flight.clone(),
                    None => self.start_fill(flights, ns, key, fill, true)?,
                },
            }
        };

        flight.await
    }

    fn refill<F, E>(&self, namespace: &str, key: &[u8], fill: F) -> Result<(), CacheError>
    where
        F: FnOnce() -> Result<CacheEntry, E> + Send + 'static,
        E: Into<FillError> + 'static,
    {
        let ns = self.namespace(namespace)?;
        let key = flight_key(&ns.spec, key);

        let flights = lock(&self.inner.flights);

        if !flights.contains_key(&key) {
            drop(self.start_fill(flights, ns, key, fill, false)?);
        }

        Ok(())
    }

    async fn get_or_fill_async<Fut, E>(
        &self,
        namespace: &str,
        key: &[u8],
        fill: Fut,
    ) -> Result<Bytes, CacheError>
    where
        Fut: Future<Output = Result<CacheEntry, E>> + Send + 'static,
        E: Into<FillError> + 'static,
    {
        let ns = self.namespace(namespace)?;
        let key = flight_key(&ns.spec, key);

        let flight = {
            let flights = lock(&self.inner.flights);

            match self.lookup_memory(&ns.spec, &key.2) {
                Lookup::Fresh(value) => return Ok(value),
                Lookup::Stale(value) => {
                    // past the fill limits the stale value is still an answer
                    if !flights.contains_key(&key) {
                        match self.start_async_fill(flights, ns, key, fill) {
                            Ok(_) | Err(CacheError::Overloaded) => {}
                            Err(error) => return Err(error),
                        }
                    }

                    return Ok(value);
                }
                Lookup::Miss => match flights.get(&key) {
                    Some(flight) => flight.clone(),
                    None => self.start_async_fill(flights, ns, key, fill)?,
                },
            }
        };

        flight.await
    }

    async fn fill_inline<Fut, E>(
        &self,
        namespace: &str,
        key: &[u8],
        fill: Fut,
    ) -> Result<Bytes, CacheError>
    where
        Fut: Future<Output = Result<CacheEntry, E>> + Send,
        E: Into<FillError>,
    {
        let ns = self.namespace(namespace)?;

        if let Lookup::Fresh(value) = self.lookup_memory(&ns.spec, key) {
            return Ok(value);
        }

        self.clone()
            .run_async_fill(ns, Bytes::copy_from_slice(key), fill)
            .await
    }
}

/// Locks a cache structure. No lock is held across an await and every write
/// under one leaves it consistent, so a poisoned lock is still usable.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone, Copy)]
enum Tier {
    Memory,
    Disk,
}

impl Tier {
    fn name(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::Disk => "disk",
        }
    }
}

struct Metrics {
    hits: Counter<u64>,
    misses: Counter<u64>,
    fills: Counter<u64>,
    evictions: Counter<u64>,
    fill_time: Histogram<f64>,
    _bytes: ObservableGauge<u64>,
    _entries: ObservableGauge<u64>,
}

impl Metrics {
    /// The gauges read the cache's stats when the meter collects them.
    fn new<D: CacheDiskTier>(cache: Weak<Inner<D>>) -> Self {
        let meter = global::meter("dolos-cache");

        let gauge = |name: &'static str, unit: &'static str, read: fn(&TierStats) -> u64| {
            let cache = cache.clone();

            meter
                .u64_observable_gauge(name)
                .with_unit(unit)
                .with_callback(move |observer| {
                    let Some(inner) = cache.upgrade() else {
                        return;
                    };

                    let Ok(stats) = (TieredCache { inner }).stats() else {
                        return;
                    };

                    for ns in stats.namespaces {
                        for (tier, usage) in [(Tier::Memory, ns.memory), (Tier::Disk, ns.disk)] {
                            observer.observe(read(&usage), &attributes(ns.name, tier));
                        }
                    }
                })
                .build()
        };

        Self {
            hits: meter.u64_counter("cache_hits").build(),
            misses: meter.u64_counter("cache_misses").build(),
            fills: meter.u64_counter("cache_fills").build(),
            evictions: meter.u64_counter("cache_evictions").build(),
            fill_time: meter
                .f64_histogram("cache_fill_time")
                .with_unit("s")
                .build(),
            _bytes: gauge("cache_bytes", "By", |usage| usage.bytes),
            _entries: gauge("cache_entries", "{entry}", |usage| usage.entries),
        }
    }

    fn hit(&self, namespace: &'static str, tier: Tier) {
        self.hits.add(1, &attributes(namespace, tier));
    }

    fn miss(&self, namespace: &'static str, tier: Tier) {
        self.misses.add(1, &attributes(namespace, tier));
    }

    fn evicted(&self, namespace: &'static str, tier: Tier) {
        self.evictions.add(1, &attributes(namespace, tier));
    }

    fn filled(&self, namespace: &'static str, took: Duration) {
        let attributes = [KeyValue::new("namespace", namespace)];
        self.fills.add(1, &attributes);
        self.fill_time.record(took.as_secs_f64(), &attributes);
    }
}

fn attributes(namespace: &'static str, tier: Tier) -> [KeyValue; 2] {
    [
        KeyValue::new("namespace", namespace),
        KeyValue::new("tier", tier.name()),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_executor::block_on;
    use std::{
        panic::AssertUnwindSafe,
        sync::{atomic::AtomicBool, mpsc, Condvar},
    };

    const SLOW: CacheNamespace = CacheNamespace::immutable("slow", 1);

    fn cache() -> TieredCache<crate::builtin::NoDiskTier> {
        TieredCache::memory(&CacheConfig::default(), QueryLimiter::default())
    }

    fn registered<D: CacheDiskTier>(cache: TieredCache<D>, ns: CacheNamespace) -> TieredCache<D> {
        cache.register(ns).unwrap();
        cache
    }

    fn entry(value: &'static str, cost: Cost) -> Result<CacheEntry, FillError> {
        Ok(CacheEntry::new(value, cost))
    }

    /// Holds blocking fills until the test opens it.
    #[derive(Default)]
    struct Gate {
        open: Mutex<bool>,
        cv: Condvar,
    }

    impl Gate {
        fn wait(&self) {
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.cv.wait(open).unwrap();
            }
        }

        fn open(&self) {
            *self.open.lock().unwrap() = true;
            self.cv.notify_all();
        }
    }

    async fn wait_for_calls(calls: &AtomicUsize, n: usize) {
        while calls.load(Ordering::SeqCst) < n {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }

    /// Waits for the fill under way for `key`, if any, to end.
    async fn wait_for_fill<D: CacheDiskTier>(cache: &TieredCache<D>, namespace: &str, key: &[u8]) {
        let flight = lock(&cache.inner.flights)
            .iter()
            .find(|((ns, _, k), _)| *ns == namespace && k.as_ref() == key)
            .map(|(_, flight)| flight.clone());

        if let Some(flight) = flight {
            let _ = tokio::time::timeout(Duration::from_secs(5), flight)
                .await
                .expect("timed out waiting for the cache fill");
        }
    }

    /// A disk tier that keeps entries in a map and records what reaches it.
    #[derive(Default)]
    struct RecordingDisk {
        entries: Mutex<HashMap<(String, Bytes), CacheEntry>>,
        puts: Mutex<Vec<String>>,
        opened: Mutex<Vec<(String, u32)>>,
    }

    impl CacheDiskTier for RecordingDisk {
        fn open_namespace(&self, namespace: &str, version: u32) -> Result<(), CacheError> {
            let mut opened = lock(&self.opened);
            let last = opened.iter().rev().find(|(ns, _)| ns == namespace);

            if last.is_some_and(|(_, known)| *known != version) {
                self.wipe(namespace)?;
            }

            opened.push((namespace.to_string(), version));
            Ok(())
        }

        fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<CacheEntry>, CacheError> {
            let key = (namespace.to_string(), Bytes::copy_from_slice(key));
            Ok(lock(&self.entries).get(&key).cloned())
        }

        fn put(
            &self,
            namespace: &str,
            key: &[u8],
            value: &[u8],
            cost: Cost,
        ) -> Result<(), CacheError> {
            lock(&self.puts).push(namespace.to_string());
            lock(&self.entries).insert(
                (namespace.to_string(), Bytes::copy_from_slice(key)),
                CacheEntry::new(value.to_vec(), cost),
            );
            Ok(())
        }

        fn remove(&self, namespace: &str, key: &[u8]) -> Result<(), CacheError> {
            let key = (namespace.to_string(), Bytes::copy_from_slice(key));
            lock(&self.entries).remove(&key);
            Ok(())
        }

        fn wipe(&self, namespace: &str) -> Result<(), CacheError> {
            lock(&self.entries).retain(|(ns, _), _| ns != namespace);
            Ok(())
        }

        fn previous_keys(&self, _namespace: &str) -> Result<Vec<Bytes>, CacheError> {
            Ok(Vec::new())
        }

        fn keys(&self, namespace: &str) -> Result<Vec<Bytes>, CacheError> {
            Ok(lock(&self.entries)
                .keys()
                .filter(|(ns, _)| ns == namespace)
                .map(|(_, key)| key.clone())
                .collect())
        }

        fn bytes(&self, namespace: &str) -> Result<u64, CacheError> {
            Ok(lock(&self.entries)
                .iter()
                .filter(|((ns, _), _)| ns == namespace)
                .map(|((_, key), entry)| (key.len() + entry.value.len()) as u64)
                .sum())
        }

        fn flush(&self) -> Result<(), CacheError> {
            Ok(())
        }
    }

    fn recording(config: &CacheConfig) -> TieredCache<RecordingDisk> {
        TieredCache::new(config, RecordingDisk::default(), QueryLimiter::default())
    }

    #[tokio::test]
    async fn an_entry_below_its_threshold_is_not_kept() {
        let cache = registered(cache(), SLOW.with_admission(10));

        let value = cache.get_or_fill("slow", b"cheap", || entry("cheap", 9));
        assert_eq!(value.await.unwrap(), "cheap");
        assert_eq!(cache.get("slow", b"cheap"), None);

        let value = cache.get_or_fill("slow", b"costly", || entry("costly", 10));
        assert_eq!(value.await.unwrap(), "costly");
        assert_eq!(cache.get("slow", b"costly").unwrap(), "costly");
    }

    #[tokio::test]
    async fn configured_admission_overrides_the_namespace_default() {
        let config = CacheConfig {
            admission: [("slow".to_string(), 100)].into(),
            ..CacheConfig::default()
        };
        let cache = TieredCache::memory(&config, QueryLimiter::default());
        let cache = registered(cache, SLOW.with_admission(10));

        let value = cache.get_or_fill("slow", b"key", || entry("value", 50));
        assert_eq!(value.await.unwrap(), "value");
        assert_eq!(cache.get("slow", b"key"), None);
    }

    #[tokio::test]
    async fn an_unregistered_namespace_is_refused() {
        let cache = cache();

        let res = cache.get_or_fill("nowhere", b"key", || entry("value", 1));
        assert!(matches!(res.await, Err(CacheError::UnknownNamespace(_))));
        assert_eq!(cache.get("nowhere", b"key"), None);
    }

    #[tokio::test]
    async fn concurrent_misses_on_one_key_share_one_fill() {
        let cache = registered(cache(), SLOW);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate::default());

        let requests: Vec<_> = (0..16u64)
            .map(|i| {
                let cache = cache.clone();
                let (calls, gate) = (calls.clone(), gate.clone());
                tokio::spawn(async move {
                    cache
                        .get_or_fill("slow", b"key", move || {
                            calls.fetch_add(1, Ordering::SeqCst);
                            gate.wait();
                            Ok::<_, FillError>(CacheEntry::new(i.to_be_bytes().to_vec(), 1))
                        })
                        .await
                })
            })
            .collect();

        wait_for_calls(&calls, 1).await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        gate.open();

        let mut values = Vec::new();
        for request in requests {
            values.push(request.await.unwrap().unwrap());
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(values.iter().all(|v| *v == values[0]));
    }

    #[tokio::test]
    async fn a_dropped_request_does_not_stop_its_fill() {
        let cache = registered(cache(), SLOW);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate::default());

        let request = tokio::spawn({
            let cache = cache.clone();
            let (calls, gate) = (calls.clone(), gate.clone());
            async move {
                cache
                    .get_or_fill("slow", b"key", move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        gate.wait();
                        entry("first", 1)
                    })
                    .await
            }
        });

        wait_for_calls(&calls, 1).await;
        request.abort();
        let _ = request.await;
        gate.open();
        wait_for_fill(&cache, "slow", b"key").await;

        let value = cache.get_or_fill("slow", b"key", || entry("second", 1));
        assert_eq!(value.await.unwrap(), "first");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_failed_fill_is_retried_by_the_next_request() {
        let cache = registered(cache(), SLOW);

        #[derive(Debug, Error)]
        #[error("boom")]
        struct Boom;

        let res = cache.get_or_fill("slow", b"key", || Err::<CacheEntry, _>(Boom));
        let err = res.await.unwrap_err();
        assert!(err.downcast_fill::<Boom>().is_some(), "{err}");

        let value = cache.get_or_fill("slow", b"key", || entry("ok", 1));
        assert_eq!(value.await.unwrap(), "ok");
    }

    fn slow_and_net() -> TieredCache<crate::builtin::NoDiskTier> {
        let net = CacheNamespace::immutable("net", 1).with_fill_limits(1, 1);
        registered(registered(cache(), SLOW), net)
    }

    /// No flight is left behind or locked, and no async fill counts as
    /// pending.
    fn assert_usable(cache: &TieredCache<crate::builtin::NoDiskTier>) {
        assert!(lock(&cache.inner.flights).is_empty());
        let pending = cache
            .namespace("net")
            .unwrap()
            .pending
            .load(Ordering::SeqCst);
        assert_eq!(pending, 0);
        assert_eq!(cache.get("slow", b"key"), None);
    }

    /// Starts each kind of fill where the runtime drops its tasks unstarted,
    /// as one that is shutting down does, and reports how the checks went.
    /// It waits for each result, since such a runtime may drop a task later,
    /// on one of its workers.
    fn fill_where_tasks_are_dropped(
        cache: TieredCache<crate::builtin::NoDiskTier>,
        done: mpsc::Sender<std::thread::Result<()>>,
    ) {
        let checked = std::panic::catch_unwind(AssertUnwindSafe(|| {
            cache.refill("slow", b"key", || entry("value", 1)).unwrap();

            let filled = cache.get_or_fill("slow", b"key", || entry("value", 1));
            assert!(matches!(block_on(filled), Err(CacheError::Aborted)));

            let fetched = cache.get_or_fill_async("net", b"key", async { entry("value", 1) });
            assert!(matches!(block_on(fetched), Err(CacheError::Aborted)));

            assert_usable(&cache);
        }));

        let _ = done.send(checked);
    }

    /// Fails, rather than hangs, when a cache call on another thread blocks.
    fn finished_in_time(finished: mpsc::Receiver<std::thread::Result<()>>) {
        match finished.recv_timeout(Duration::from_secs(5)) {
            Ok(checked) => checked.unwrap_or_else(|panic| std::panic::resume_unwind(panic)),
            Err(_) => panic!("a cache call hung"),
        }
    }

    #[test]
    fn a_fill_outside_a_runtime_fails_and_leaves_the_cache_usable() {
        let cache = slow_and_net();

        let refilled = cache.refill("slow", b"key", || entry("value", 1));
        assert!(matches!(refilled, Err(CacheError::NoRuntime)));

        let filled = cache.get_or_fill("slow", b"key", || entry("value", 1));
        assert!(matches!(
            filled.now_or_never(),
            Some(Err(CacheError::NoRuntime))
        ));

        let fetched = cache.get_or_fill_async("net", b"key", async { entry("value", 1) });
        assert!(matches!(
            fetched.now_or_never(),
            Some(Err(CacheError::NoRuntime))
        ));

        assert_usable(&cache);
    }

    #[test]
    fn a_fill_on_a_runtime_shutting_down_leaves_the_cache_usable() {
        let cache = slow_and_net();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let (started, running) = mpsc::channel();
        let (shut, closed) = mpsc::channel();
        let (done, finished) = mpsc::channel();

        runtime.spawn_blocking(move || {
            started.send(()).unwrap();
            closed.recv().unwrap();
            fill_where_tasks_are_dropped(cache, done);
        });

        running.recv().unwrap();
        runtime.shutdown_background();
        shut.send(()).unwrap();

        finished_in_time(finished);
    }

    #[test]
    fn a_fill_on_a_dropped_runtime_leaves_the_cache_usable() {
        let cache = slow_and_net();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let handle = runtime.handle().clone();
        drop(runtime);

        let (done, finished) = mpsc::channel();

        std::thread::spawn(move || {
            let _entered = handle.enter();
            fill_where_tasks_are_dropped(cache, done);
        });

        finished_in_time(finished);
    }

    #[tokio::test]
    async fn a_tip_scoped_entry_is_served_stale_while_it_is_refilled() {
        let ttl = Duration::from_millis(100);
        let cache = registered(cache(), CacheNamespace::tip_scoped("tip", 1, ttl));
        let calls = Arc::new(AtomicUsize::new(0));

        let get = |value: &'static str| {
            let calls = calls.clone();
            cache.get_or_fill("tip", b"key", move || {
                calls.fetch_add(1, Ordering::SeqCst);
                entry(value, 1)
            })
        };

        assert_eq!(get("initial").await.unwrap(), "initial");
        assert_eq!(get("unused").await.unwrap(), "initial");

        tokio::time::sleep(ttl * 2).await;
        assert_eq!(get("updated").await.unwrap(), "initial");
        assert_eq!(cache.get("tip", b"key"), None);

        wait_for_fill(&cache, "tip", b"key").await;
        assert_eq!(get("unused").await.unwrap(), "updated");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn refill_replaces_an_entry_and_shares_the_fill_under_way() {
        let cache = registered(cache(), SLOW);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate::default());

        let request = tokio::spawn({
            let cache = cache.clone();
            let (calls, gate) = (calls.clone(), gate.clone());
            async move {
                cache
                    .get_or_fill("slow", b"key", move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        gate.wait();
                        entry("first", 1)
                    })
                    .await
            }
        });

        wait_for_calls(&calls, 1).await;

        let c = calls.clone();
        cache
            .refill("slow", b"key", move || {
                c.fetch_add(1, Ordering::SeqCst);
                entry("joined", 1)
            })
            .unwrap();

        gate.open();
        assert_eq!(request.await.unwrap().unwrap(), "first");
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        cache
            .refill("slow", b"key", || entry("extended", 1))
            .unwrap();
        wait_for_fill(&cache, "slow", b"key").await;
        assert_eq!(cache.get("slow", b"key").unwrap(), "extended");
    }

    #[tokio::test]
    async fn a_new_format_version_wipes_the_namespace() {
        let cache = registered(recording(&CacheConfig::default()), SLOW);

        let value = cache.get_or_fill("slow", b"key", || entry("v1", 1));
        assert_eq!(value.await.unwrap(), "v1");

        cache.register(SLOW).unwrap();
        assert_eq!(cache.get("slow", b"key").unwrap(), "v1");

        cache
            .register(CacheNamespace { version: 2, ..SLOW })
            .unwrap();
        assert_eq!(cache.get("slow", b"key"), None);

        assert_eq!(
            *lock(&cache.disk().opened),
            [
                ("slow".to_string(), 1),
                ("slow".to_string(), 1),
                ("slow".to_string(), 2)
            ]
        );
    }

    #[tokio::test]
    async fn a_fill_under_a_replaced_format_version_is_not_kept() {
        let cache = registered(recording(&CacheConfig::default()), SLOW);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate::default());

        let old = tokio::spawn({
            let cache = cache.clone();
            let (calls, gate) = (calls.clone(), gate.clone());
            async move {
                cache
                    .get_or_fill("slow", b"key", move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        gate.wait();
                        entry("v1", 1)
                    })
                    .await
            }
        });

        wait_for_calls(&calls, 1).await;
        cache
            .register(CacheNamespace { version: 2, ..SLOW })
            .unwrap();

        let value = cache.get_or_fill("slow", b"key", || entry("v2", 1));
        let value = tokio::time::timeout(Duration::from_secs(5), value).await;

        gate.open();
        let value = value.expect("the v2 miss joined the v1 fill");
        assert_eq!(value.unwrap(), "v2");
        assert_eq!(old.await.unwrap().unwrap(), "v1");

        assert_eq!(cache.get("slow", b"key").unwrap(), "v2");
        let on_disk = cache.disk().get("slow", b"key").unwrap().unwrap();
        assert_eq!(on_disk.value, "v2");
    }

    #[tokio::test]
    async fn only_immutable_entries_reach_the_disk_tier() {
        let cache = recording(&CacheConfig::default());
        cache.register(SLOW).unwrap();
        cache
            .register(CacheNamespace::tip_scoped(
                "tip",
                1,
                Duration::from_secs(60),
            ))
            .unwrap();

        let value = cache.get_or_fill("tip", b"key", || entry("tip", 1));
        assert_eq!(value.await.unwrap(), "tip");
        let value = cache.get_or_fill("slow", b"key", || entry("sealed", 1));
        assert_eq!(value.await.unwrap(), "sealed");

        assert_eq!(*lock(&cache.disk().puts), ["slow"]);
        assert_eq!(*lock(&cache.disk().opened), [("slow".to_string(), 1)]);

        let stats = cache.stats().unwrap();
        let slow = stats
            .namespaces
            .iter()
            .find(|ns| ns.name == "slow")
            .unwrap();
        assert_eq!(slow.disk.entries, 1);
        let tip = stats.namespaces.iter().find(|ns| ns.name == "tip").unwrap();
        assert_eq!(tip.disk, TierStats::default());
        assert_eq!(tip.memory.entries, 1);
    }

    #[tokio::test]
    async fn a_disk_hit_is_promoted_to_memory() {
        let cache = registered(recording(&CacheConfig::default()), SLOW);
        cache.disk().put("slow", b"key", b"on disk", 1).unwrap();

        let value = cache.get_or_fill("slow", b"key", || entry("refolded", 1));
        assert_eq!(value.await.unwrap(), "on disk");

        lock(&cache.disk().entries).clear();
        assert_eq!(cache.get("slow", b"key").unwrap(), "on disk");
    }

    #[tokio::test]
    async fn a_disabled_cache_stores_nothing() {
        let config = CacheConfig {
            enabled: false,
            ..CacheConfig::default()
        };
        let cache = registered(recording(&config), SLOW);
        let calls = Arc::new(AtomicUsize::new(0));

        for _ in 0..2 {
            let c = calls.clone();
            let value = cache.get_or_fill("slow", b"key", move || {
                c.fetch_add(1, Ordering::SeqCst);
                entry("value", 1)
            });
            assert_eq!(value.await.unwrap(), "value");
        }

        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert_eq!(cache.get("slow", b"key"), None);
        assert!(lock(&cache.disk().puts).is_empty());
        assert!(lock(&cache.disk().opened).is_empty());

        let stats = cache.stats().unwrap();
        assert_eq!(stats.namespaces[0].memory, TierStats::default());
    }

    #[tokio::test]
    async fn concurrent_async_misses_on_one_key_share_one_fill() {
        let cache = registered(cache(), CacheNamespace::immutable("net", 1));
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));

        let requests: Vec<_> = (0..8)
            .map(|_| {
                let cache = cache.clone();
                let (calls, gate) = (calls.clone(), gate.clone());
                tokio::spawn(async move {
                    cache
                        .get_or_fill_async("net", b"key", async move {
                            calls.fetch_add(1, Ordering::SeqCst);
                            let _ = gate.acquire().await;
                            entry("fetched", 1)
                        })
                        .await
                })
            })
            .collect();

        wait_for_calls(&calls, 1).await;
        tokio::time::sleep(Duration::from_millis(20)).await;
        gate.add_permits(64);

        for request in requests {
            assert_eq!(request.await.unwrap().unwrap(), "fetched");
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn async_fills_stay_within_their_limits() {
        let ns = CacheNamespace::immutable("net", 1).with_fill_limits(2, 4);
        let cache = registered(cache(), ns);
        let running = Arc::new(AtomicUsize::new(0));
        let most = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));

        let fetch = |key: u8| {
            let (cache, gate) = (cache.clone(), gate.clone());
            let (running, most) = (running.clone(), most.clone());
            tokio::spawn(async move {
                cache
                    .get_or_fill_async("net", &[key], async move {
                        let now = running.fetch_add(1, Ordering::SeqCst) + 1;
                        most.fetch_max(now, Ordering::SeqCst);
                        let _ = gate.acquire().await;
                        running.fetch_sub(1, Ordering::SeqCst);
                        entry("fetched", 1)
                    })
                    .await
            })
        };

        let requests: Vec<_> = (0..4).map(fetch).collect();
        wait_for_calls(&running, 2).await;

        let started = Arc::new(AtomicBool::new(false));
        let s = started.clone();
        let past = cache.get_or_fill_async("net", &[9], async move {
            s.store(true, Ordering::SeqCst);
            entry("never", 1)
        });
        assert!(matches!(past.await, Err(CacheError::Overloaded)));

        let joined = tokio::spawn({
            let cache = cache.clone();
            async move {
                cache
                    .get_or_fill_async("net", &[0], async { entry("never", 1) })
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(most.load(Ordering::SeqCst), 2);
        gate.add_permits(64);

        for request in requests {
            assert_eq!(request.await.unwrap().unwrap(), "fetched");
        }

        assert_eq!(joined.await.unwrap().unwrap(), "fetched");
        assert!(!started.load(Ordering::SeqCst));
        assert_eq!(most.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn an_inline_fill_waits_for_a_permit_and_keeps_its_entry() {
        let ns = CacheNamespace::immutable("net", 1).with_fill_limits(1, 1);
        let cache = registered(cache(), ns);
        let running = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Semaphore::new(0));

        let held = tokio::spawn({
            let (cache, running, gate) = (cache.clone(), running.clone(), gate.clone());
            async move {
                cache
                    .get_or_fill_async("net", &[0], async move {
                        running.fetch_add(1, Ordering::SeqCst);
                        let _ = gate.acquire().await;
                        entry("held", 1)
                    })
                    .await
            }
        });

        wait_for_calls(&running, 1).await;

        let past = cache
            .get_or_fill_async("net", &[1], async { entry("never", 1) })
            .await;
        assert!(matches!(past, Err(CacheError::Overloaded)));

        let started = Arc::new(AtomicBool::new(false));

        let inline = tokio::spawn({
            let (cache, started) = (cache.clone(), started.clone());
            async move {
                cache
                    .fill_inline("net", &[1], async move {
                        started.store(true, Ordering::SeqCst);
                        entry("inline", 1)
                    })
                    .await
            }
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(!started.load(Ordering::SeqCst));
        gate.add_permits(64);

        assert_eq!(held.await.unwrap().unwrap(), "held");
        assert_eq!(inline.await.unwrap().unwrap(), "inline");
        assert_eq!(cache.get("net", &[1]).unwrap(), "inline");
    }
}
