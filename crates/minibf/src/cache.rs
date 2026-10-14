use dolos_core::{DomainError, QueryLimiter};
use futures::future::{BoxFuture, FutureExt, Shared};
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// The one fetch in flight for an entry, awaited by every request that needs
/// its result.
type Fetch<T, E> = Shared<BoxFuture<'static, Result<T, CacheError<E>>>>;

/// Internal cache entry holding a value and its metadata.
struct Entry<T, E> {
    value: Option<T>,
    cached_at: Option<Instant>,
    /// Set while a fetch runs; the fetch task clears it when it ends.
    fetch: Option<Fetch<T, E>>,
}

impl<T, E> Default for Entry<T, E> {
    fn default() -> Self {
        Self {
            value: None,
            cached_at: None,
            fetch: None,
        }
    }
}

/// A type-safe cache service that supports stale-while-revalidate semantics.
#[derive(Clone, Default)]
pub struct CacheService {
    map: Arc<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

/// Errors that can occur when fetching from the cache.
#[derive(Debug, Clone)]
pub enum CacheError<E> {
    /// Error from the fetcher function.
    Inner(E),
    /// The query limiter could not run the fetcher, or the fetcher panicked.
    Limiter(Arc<DomainError>),
    /// The fetch task ended without a result.
    Aborted,
}

impl CacheService {
    /// Gets a value from the cache or fetches it using the provided function.
    ///
    /// If the value is fresh (within TTL), returns it immediately.
    /// If stale, returns the stale value and refreshes in the background.
    /// If missing, waits for a fetch and returns its result.
    ///
    /// An entry has at most one fetch in flight. It runs under `limiter` in a
    /// task of its own, so a request that goes away does not stop it.
    pub async fn get_or_fetch_blocking<T, E, F>(
        &self,
        ttl: Duration,
        limiter: &QueryLimiter,
        fetcher: F,
    ) -> Result<T, CacheError<E>>
    where
        T: Clone + Send + Sync + 'static,
        E: Clone + std::fmt::Debug + Send + Sync + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        let entry = self.get_entry::<T, E>().await;

        let fetch = {
            let mut guard = lock(&entry);

            if let Some(value) = Self::get_fresh_value(&guard, ttl) {
                return Ok(value);
            }

            let fetch = match &guard.fetch {
                Some(fetch) => fetch.clone(),
                None => {
                    let fetch = Self::spawn_fetch(entry.clone(), limiter.clone(), fetcher);
                    guard.fetch = Some(fetch.clone());
                    fetch
                }
            };

            if let Some(stale) = &guard.value {
                return Ok(stale.clone());
            }

            fetch
        };

        fetch.await
    }

    /// Checks if there's a fresh cached value (within TTL).
    fn get_fresh_value<T: Clone, E>(guard: &Entry<T, E>, ttl: Duration) -> Option<T> {
        let now = Instant::now();
        let is_fresh = guard
            .cached_at
            .map(|t| now.duration_since(t) < ttl)
            .unwrap_or(false);

        if is_fresh {
            guard.value.clone()
        } else {
            None
        }
    }

    /// Spawns the task that runs the fetcher and stores its value.
    ///
    /// The caller holds the entry's lock while it records the returned fetch,
    /// so the task's clearing of it always comes after.
    fn spawn_fetch<T, E, F>(
        entry: Arc<Mutex<Entry<T, E>>>,
        limiter: QueryLimiter,
        fetcher: F,
    ) -> Fetch<T, E>
    where
        T: Clone + Send + Sync + 'static,
        E: Clone + std::fmt::Debug + Send + Sync + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        let task = tokio::spawn(async move {
            let res = limiter.run_blocking(fetcher).await;

            let mut guard = lock(&entry);
            guard.fetch = None;

            match res {
                Ok(Ok(v)) => {
                    guard.value = Some(v.clone());
                    guard.cached_at = Some(Instant::now());
                    Ok(v)
                }
                Ok(Err(e)) => {
                    tracing::error!(error = ?e, "cache fetch failed");
                    Err(CacheError::Inner(e))
                }
                Err(e) => {
                    tracing::error!(error = ?e, "cache fetch could not run");
                    Err(CacheError::Limiter(Arc::new(e)))
                }
            }
        });

        task.map(|res| res.unwrap_or(Err(CacheError::Aborted)))
            .boxed()
            .shared()
    }

    /// Gets or creates the cache entry for the pair of T and E.
    async fn get_entry<T, E>(&self) -> Arc<Mutex<Entry<T, E>>>
    where
        T: Send + Sync + 'static,
        E: Send + Sync + 'static,
    {
        let type_id = TypeId::of::<Entry<T, E>>();

        {
            let map = self.map.read().await;
            if let Some(b) = map.get(&type_id) {
                if let Some(entry) =
                    (b.as_ref() as &dyn Any).downcast_ref::<Arc<Mutex<Entry<T, E>>>>()
                {
                    return entry.clone();
                } else {
                    // This path theoretically shouldn't happen if T maps 1:1 to TypeId
                    eprintln!("CacheService: downcast failed for type {:?}", type_id);
                }
            }
        }

        let mut map = self.map.write().await;
        // Check again
        if let Some(b) = map.get(&type_id) {
            if let Some(entry) = (b.as_ref() as &dyn Any).downcast_ref::<Arc<Mutex<Entry<T, E>>>>()
            {
                return entry.clone();
            }
        }

        let entry = Arc::new(Mutex::new(Entry::<T, E>::default()));
        map.insert(type_id, Box::new(entry.clone()));
        entry
    }
}

/// Locks an entry. The lock is never held across an await, and every write
/// under it leaves the entry consistent, so a poisoned lock is still usable.
fn lock<T, E>(entry: &Mutex<Entry<T, E>>) -> MutexGuard<'_, Entry<T, E>> {
    entry.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Condvar;

    /// Holds blocking fetchers until the test opens it.
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

    #[tokio::test]
    async fn test_cache_basic() {
        let cache = CacheService::default();
        let limiter = QueryLimiter::default();
        let ttl = Duration::from_secs(1);

        let res: Result<String, CacheError<()>> = cache
            .get_or_fetch_blocking(ttl, &limiter, || Ok("hello".to_string()))
            .await;

        assert_eq!(res.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_cache_stale_revalidate() {
        let cache = CacheService::default();
        let limiter = QueryLimiter::default();
        let ttl = Duration::from_millis(100);

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();

        // 1. Initial fetch
        let fetcher = move || {
            c.fetch_add(1, Ordering::SeqCst);
            Ok("initial".to_string())
        };

        let res: Result<String, CacheError<()>> =
            cache.get_or_fetch_blocking(ttl, &limiter, fetcher).await;
        assert_eq!(res.unwrap(), "initial");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        // 2. Wait for TTL to expire
        tokio::time::sleep(Duration::from_millis(200)).await;

        // 3. Fetch again - should return stale "initial" but trigger background refresh
        let c = counter.clone();
        let fetcher = move || {
            c.fetch_add(1, Ordering::SeqCst);
            Ok("updated".to_string())
        };

        let res: Result<String, CacheError<()>> =
            cache.get_or_fetch_blocking(ttl, &limiter, fetcher).await;
        assert_eq!(res.unwrap(), "initial"); // Returns stale!

        // 4. Wait for background refresh to finish
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 5. Fetch again - should return "updated"
        let res: Result<String, CacheError<()>> = cache
            .get_or_fetch_blocking(ttl, &limiter, || Ok("should not run".to_string()))
            .await;
        assert_eq!(res.unwrap(), "updated");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn dropped_request_does_not_freeze_the_entry() {
        let cache = CacheService::default();
        let limiter = QueryLimiter::default();
        let ttl = Duration::from_millis(200);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate::default());

        let request = tokio::spawn({
            let (cache, limiter) = (cache.clone(), limiter.clone());
            let (calls, gate) = (calls.clone(), gate.clone());
            async move {
                cache
                    .get_or_fetch_blocking::<String, (), _>(ttl, &limiter, move || {
                        calls.fetch_add(1, Ordering::SeqCst);
                        gate.wait();
                        Ok("first".to_string())
                    })
                    .await
            }
        });

        wait_for_calls(&calls, 1).await;
        request.abort();
        let _ = request.await;
        gate.open();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let get = |value: &'static str| {
            let calls = calls.clone();
            cache.get_or_fetch_blocking::<String, (), _>(ttl, &limiter, move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(value.to_string())
            })
        };

        assert_eq!(get("first").await.unwrap(), "first");

        tokio::time::sleep(ttl).await;
        assert_eq!(get("second").await.unwrap(), "first");

        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(get("third").await.unwrap(), "second");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn concurrent_cold_requests_share_one_fetch() {
        let cache = CacheService::default();
        let limiter = QueryLimiter::default();
        let ttl = Duration::from_secs(30);
        let calls = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Gate::default());

        let requests: Vec<_> = (0..16u64)
            .map(|i| {
                let (cache, limiter) = (cache.clone(), limiter.clone());
                let (calls, gate) = (calls.clone(), gate.clone());
                tokio::spawn(async move {
                    cache
                        .get_or_fetch_blocking::<u64, (), _>(ttl, &limiter, move || {
                            calls.fetch_add(1, Ordering::SeqCst);
                            gate.wait();
                            Ok(i)
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
    async fn failed_fetch_is_retried_by_the_next_request() {
        let cache = CacheService::default();
        let limiter = QueryLimiter::default();
        let ttl = Duration::from_secs(30);
        let calls = Arc::new(AtomicUsize::new(0));

        let c = calls.clone();
        let res = cache
            .get_or_fetch_blocking::<String, &'static str, _>(ttl, &limiter, move || {
                c.fetch_add(1, Ordering::SeqCst);
                Err("boom")
            })
            .await;
        assert!(matches!(res, Err(CacheError::Inner("boom"))));

        let c = calls.clone();
        let res = cache
            .get_or_fetch_blocking::<String, &'static str, _>(ttl, &limiter, move || {
                c.fetch_add(1, Ordering::SeqCst);
                Ok("ok".to_string())
            })
            .await;
        assert_eq!(res.unwrap(), "ok");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
