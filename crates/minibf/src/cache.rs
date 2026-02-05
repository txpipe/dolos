use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock};

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

#[derive(Clone, Default)]
pub struct CacheService {
    map: Arc<RwLock<HashMap<TypeId, Box<dyn Any + Send + Sync>>>>,
}

impl CacheService {
    pub async fn get_or_fetch_blocking<T, E, F>(&self, ttl: Duration, fetcher: F) -> Result<T, E>
    where
        T: Clone + Send + Sync + 'static,
        E: std::fmt::Debug + Send + 'static,
        F: FnOnce() -> Result<T, E> + Send + 'static,
    {
        let entry = self.get_entry::<T>().await;
        let mut guard = entry.lock().await;

        let now = Instant::now();
        let is_fresh = guard
            .cached_at
            .map(|t| now.duration_since(t) < ttl)
            .unwrap_or(false);

        if is_fresh {
            if let Some(v) = &guard.value {
                return Ok(v.clone());
            }
        }

        let val_opt = guard.value.clone();

        if !guard.refreshing {
            if let Some(val) = val_opt {
                // Stale-while-revalidate
                guard.refreshing = true;
                let entry_clone = entry.clone();

                tokio::spawn(async move {
                    let res = tokio::task::spawn_blocking(fetcher).await;

                    let mut g = entry_clone.lock().await;
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

                Ok(val)
            } else {
                // No value, must wait
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
                    Ok(Err(e)) => Err(e),
                    Err(e) => {
                        tracing::error!("tokio task join error: {:?}", e);
                        panic!("tokio task join error: {:?}", e);
                    }
                }
            }
        } else {
            // Already refreshing
            if let Some(v) = val_opt {
                Ok(v)
            } else {
                drop(guard);
                let res = tokio::task::spawn_blocking(fetcher).await;
                match res {
                    Ok(Ok(v)) => Ok(v),
                    Ok(Err(e)) => Err(e),
                    Err(e) => panic!("tokio task join error: {:?}", e),
                }
            }
        }
    }

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

        // eprintln!("CacheService: creating new entry for type {:?}", type_id);
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

        let res: Result<String, ()> = cache
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

        let res: Result<String, ()> = cache.get_or_fetch_blocking(ttl, fetcher).await;
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

        let res: Result<String, ()> = cache.get_or_fetch_blocking(ttl, fetcher).await;
        assert_eq!(res.unwrap(), "initial"); // Returns stale!

        // 4. Wait for background refresh to finish
        tokio::time::sleep(Duration::from_millis(100)).await;

        // 5. Fetch again - should return "updated"
        let res: Result<String, ()> = cache
            .get_or_fetch_blocking(ttl, || Ok("should not run".to_string()))
            .await;
        assert_eq!(res.unwrap(), "updated");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }
}
