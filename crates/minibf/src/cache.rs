use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

/// Internal cache entry holding a value and its metadata.
struct Entry<T> {
    value: Option<T>,
    cached_at: Option<Instant>,
    refreshing: bool,
}

impl<T> Default for Entry<T> {
    fn default() -> Self {
        Self {
            value: None,
            cached_at: None,
            refreshing: false,
        }
    }
}

/// A type-safe cache service that supports stale-while-revalidate semantics.
#[derive(Clone, Default)]
pub struct CacheService {
    map: Arc<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

/// Errors that can occur when fetching from the cache.
#[derive(Debug)]
pub enum CacheError<E> {
    /// Error from the fetcher function.
    Inner(E),
    /// Error when joining the background task.
    JoinError(tokio::task::JoinError),
}

impl CacheService {
    /// Gets a value from the cache or fetches it using the provided function.
    ///
    /// If the value is fresh (within TTL), returns it immediately.
    /// If stale, returns the stale value and refreshes in the background.
    /// If missing, fetches synchronously and populates the cache.
    pub async fn get_or_fetch_blocking<T, E, F>(
        &self,
        ttl: Duration,
        fetcher: F,
    ) -> Result<T, CacheError<E>>
    where
        T: Clone + Send + Sync + 'static,
        E: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        let entry = self.get_entry::<T>().await;
        let entry_clone = entry.clone();
        let guard = entry.lock().await;

        // Check if we have a fresh cached value
        if let Some(value) = Self::get_fresh_value(&guard, ttl) {
            return Ok(value);
        }

        let value_opt = guard.value.clone();
        let is_refreshing = guard.refreshing;

        if is_refreshing {
            // Another task is already refreshing
            return Self::handle_concurrent_refresh(entry_clone, guard, value_opt, fetcher).await;
        }

        // No refresh in progress, we need to handle it
        if let Some(stale_value) = value_opt {
            // Return stale value and refresh in background
            Self::spawn_background_refresh(entry_clone, guard, stale_value.clone(), fetcher);
            Ok(stale_value)
        } else {
            // No value at all, must fetch synchronously
            Self::fetch_and_store_sync(entry_clone, guard, fetcher).await
        }
    }

    /// Checks if there's a fresh cached value (within TTL).
    fn get_fresh_value<T: Clone>(
        guard: &tokio::sync::MutexGuard<'_, Entry<T>>,
        ttl: Duration,
    ) -> Option<T> {
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

    /// Spawns a background task to refresh the cache.
    fn spawn_background_refresh<T, E, F>(
        entry: Arc<Mutex<Entry<T>>>,
        mut guard: tokio::sync::MutexGuard<'_, Entry<T>>,
        _stale_value: T,
        fetcher: F,
    ) where
        T: Clone + Send + Sync + 'static,
        E: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        guard.refreshing = true;
        drop(guard);

        tokio::spawn(async move {
            let res = tokio::task::spawn_blocking(fetcher).await;

            let mut g = entry.lock().await;
            g.refreshing = false;

            match res {
                Ok(Ok(v)) => {
                    g.value = Some(v);
                    g.cached_at = Some(Instant::now());
                }
                Ok(Err(e)) => {
                    tracing::error!("background refresh failed: {:?}", e);
                }
                Err(e) => {
                    tracing::error!("background task join failed: {:?}", e);
                }
            }
        });
    }

    /// Fetches data synchronously and stores it in the cache.
    async fn fetch_and_store_sync<T, E, F>(
        entry: Arc<Mutex<Entry<T>>>,
        mut guard: tokio::sync::MutexGuard<'_, Entry<T>>,
        fetcher: F,
    ) -> Result<T, CacheError<E>>
    where
        T: Clone + Send + Sync + 'static,
        E: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        guard.refreshing = true;
        drop(guard);

        let res = tokio::task::spawn_blocking(fetcher).await;

        // Re-acquire lock
        let mut guard = entry.lock().await;
        guard.refreshing = false;

        match res {
            Ok(Ok(v)) => {
                guard.value = Some(v.clone());
                guard.cached_at = Some(Instant::now());
                Ok(v)
            }
            Ok(Err(e)) => Err(CacheError::Inner(e)),
            Err(e) => {
                tracing::error!("tokio task join error: {:?}", e);
                Err(CacheError::JoinError(e))
            }
        }
    }

    /// Handles the case when another task is already refreshing the cache.
    async fn handle_concurrent_refresh<T, E, F>(
        entry: Arc<Mutex<Entry<T>>>,
        guard: tokio::sync::MutexGuard<'_, Entry<T>>,
        value_opt: Option<T>,
        fetcher: F,
    ) -> Result<T, CacheError<E>>
    where
        T: Clone + Send + Sync + 'static,
        E: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        if let Some(v) = value_opt {
            // We have a stale value, return it
            Ok(v)
        } else {
            // No value and already refreshing - fetch and populate cache
            drop(guard);
            let res = tokio::task::spawn_blocking(fetcher).await;

            match res {
                Ok(Ok(v)) => {
                    let mut g = entry.lock().await;
                    g.value = Some(v.clone());
                    g.cached_at = Some(Instant::now());
                    Ok(v)
                }
                Ok(Err(e)) => Err(CacheError::Inner(e)),
                Err(e) => {
                    tracing::error!("tokio task join error: {:?}", e);
                    Err(CacheError::JoinError(e))
                }
            }
        }
    }

    /// Gets or creates a cache entry for type T.
    async fn get_entry<T: Send + Sync + 'static>(&self) -> Arc<Mutex<Entry<T>>> {
        let type_id = TypeId::of::<T>();

        {
            let map = self.map.read().await;
            if let Some(b) = map.get(&type_id) {
                if let Some(entry) = (b.as_ref() as &dyn Any).downcast_ref::<Arc<Mutex<Entry<T>>>>()
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
            if let Some(entry) = (b.as_ref() as &dyn Any).downcast_ref::<Arc<Mutex<Entry<T>>>>() {
                return entry.clone();
            }
        }

        let entry = Arc::new(Mutex::new(Entry::<T>::default()));
        map.insert(type_id, Box::new(entry.clone()));
        entry
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn test_cache_basic() {
        let cache = CacheService::default();
        let ttl = Duration::from_secs(1);

        let res: Result<String, CacheError<()>> = cache
            .get_or_fetch_blocking(ttl, || Ok("hello".to_string()))
            .await;

        assert_eq!(res.unwrap(), "hello");
    }

    #[tokio::test]
    async fn test_cache_stale_revalidate() {
        let cache = CacheService::default();
        let ttl = Duration::from_millis(100);

        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();

        // 1. Initial fetch
        let fetcher = move || {
            c.fetch_add(1, Ordering::SeqCst);
            Ok("initial".to_string())
        };

        let res: Result<String, CacheError<()>> = cache.get_or_fetch_blocking(ttl, fetcher).await;
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

        let res: Result<String, CacheError<()>> = cache.get_or_fetch_blocking(ttl, fetcher).await;
        assert_eq!(res.unwrap(), "initial"); // Returns stale!

        // 4. Wait for background refresh to finish
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 5. Fetch again - should return "updated"
        let res: Result<String, CacheError<()>> = cache
            .get_or_fetch_blocking(ttl, || Ok("should not run".to_string()))
            .await;
        assert_eq!(res.unwrap(), "updated");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
