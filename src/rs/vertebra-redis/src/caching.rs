//! RESP3 client-side caching with bounded LRU and push invalidation tracking.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::time::{SystemTime, UNIX_EPOCH};

use lru::LruCache;
use parking_lot::Mutex;

const MAX_INVALIDATION_EVENTS: usize = 4096;

#[derive(Clone, Debug)]
struct CacheEntry {
    value: Vec<u8>,
    expires_at: Option<f64>,
}

/// Thread-safe bounded LRU client-side cache for Redis RESP3 `CLIENT TRACKING`.
pub struct ClientLruCache {
    inner: Mutex<LruCacheState>,
}

struct LruCacheState {
    lru: LruCache<String, CacheEntry>,
    invalidations: VecDeque<String>,
}

impl LruCacheState {
    fn push_invalidation(&mut self, key: String) {
        if self.invalidations.len() >= MAX_INVALIDATION_EVENTS {
            self.invalidations.pop_front();
        }
        self.invalidations.push_back(key);
    }
}

fn current_timestamp_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

impl ClientLruCache {
    pub fn new(capacity: usize) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).unwrap_or(NonZeroUsize::MIN);
        Self {
            inner: Mutex::new(LruCacheState {
                lru: LruCache::new(cap),
                invalidations: VecDeque::new(),
            }),
        }
    }

    /// Retrieve a cached value if present and not expired.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let mut state = self.inner.lock();
        if let Some(entry) = state.lru.get(key) {
            let now = current_timestamp_secs();
            if let Some(exp) = entry.expires_at
                && now >= exp
            {
                state.lru.pop(key);
                return None;
            }
            return Some(entry.value.clone());
        }
        None
    }

    /// Insert or update a cached entry with optional TTL in seconds.
    pub fn put(&self, key: String, value: Vec<u8>, ttl_seconds: Option<f64>) {
        let expires_at = ttl_seconds.map(|ttl| current_timestamp_secs() + ttl);
        let mut state = self.inner.lock();
        state.lru.put(key, CacheEntry { value, expires_at });
    }

    /// Evict keys from the local cache without recording an invalidation event.
    pub fn evict_local(&self, keys: &[String]) {
        let mut state = self.inner.lock();
        for key in keys {
            state.lru.pop(key);
        }
    }

    /// Evict a specific list of keys received from an invalidation push frame.
    pub fn invalidate_keys(&self, keys: &[String]) {
        let mut state = self.inner.lock();
        for key in keys {
            state.lru.pop(key);
            state.push_invalidation(key.clone());
        }
    }

    /// Invalidate all keys (e.g. on `FLUSHDB` or `null` invalidation payload).
    pub fn invalidate_all(&self) {
        let mut state = self.inner.lock();
        let existing: Vec<String> = state.lru.iter().map(|(k, _)| k.clone()).collect();
        state.lru.clear();
        for k in existing {
            state.push_invalidation(k);
        }
    }

    /// Return all key names currently resident in the cache.
    pub fn cached_keys(&self) -> Vec<String> {
        let state = self.inner.lock();
        state.lru.iter().map(|(k, _)| k.clone()).collect()
    }

    /// Drain all queued invalidation events since the last call.
    pub fn drain_invalidations(&self) -> Vec<String> {
        let mut state = self.inner.lock();
        state.invalidations.drain(..).collect()
    }
}
