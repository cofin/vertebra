//! RESP3 `CLIENT TRACKING` L1 LRU cache and `>invalidate` push invalidation.

use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use lru::LruCache;
use parking_lot::Mutex;

use crate::codec::RespValue;

/// Maximum buffered invalidation key names retained before dropping oldest entries.
pub const MAX_INVALIDATION_EVENTS: usize = 4096;

#[derive(Debug, Clone)]
struct CacheEntry {
    value: Bytes,
    expires_at: Option<Instant>,
}

/// Thread-safe bounded L1 client-side cache invalidated by RESP3 push frames.
#[derive(Debug)]
pub struct TrackingCache {
    lru: Mutex<LruCache<Bytes, CacheEntry>>,
    invalidated_keys: Mutex<VecDeque<String>>,
    ttl: Option<Duration>,
    hits: AtomicU64,
    misses: AtomicU64,
    invalidations: AtomicU64,
}

impl TrackingCache {
    /// Construct an L1 cache with bounded entry capacity and optional per-entry TTL.
    #[must_use]
    pub fn new(capacity: usize, ttl: Option<Duration>) -> Self {
        let cap = NonZeroUsize::new(capacity.max(1)).expect("capacity >= 1");
        Self {
            lru: Mutex::new(LruCache::new(cap)),
            invalidated_keys: Mutex::new(VecDeque::new()),
            ttl,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            invalidations: AtomicU64::new(0),
        }
    }

    fn push_invalidation(inv: &mut VecDeque<String>, key: String) {
        if inv.len() >= MAX_INVALIDATION_EVENTS {
            inv.pop_front();
        }
        inv.push_back(key);
    }

    /// Look up `key` in the L1 cache, evicting expired entries if TTL is active.
    #[must_use]
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        let mut guard = self.lru.lock();
        let entry = guard.get(key)?;
        if let Some(exp) = entry.expires_at
            && Instant::now() >= exp
        {
            guard.pop(key);
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }
        let val = entry.value.clone();
        self.hits.fetch_add(1, Ordering::Relaxed);
        Some(val)
    }

    /// Record a cache hit candidate after a server read using the default cache TTL.
    pub fn insert(&self, key: Bytes, value: Bytes) {
        self.insert_with_ttl(key, value, None);
    }

    /// Record a cache entry with an optional per-entry TTL override.
    pub fn insert_with_ttl(&self, key: Bytes, value: Bytes, ttl_override: Option<Duration>) {
        let effective_ttl = match (ttl_override, self.ttl) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        let expires_at = effective_ttl.map(|d| Instant::now() + d);
        let mut guard = self.lru.lock();
        guard.put(key, CacheEntry { value, expires_at });
    }

    /// Evict a key from the local cache without recording an invalidation event.
    pub fn evict_local(&self, key: &[u8]) {
        let mut guard = self.lru.lock();
        guard.pop(key);
    }

    /// Evict `Some(key)` or flush the entire cache when `key` is `None`.
    pub fn invalidate_key(&self, key: Option<&[u8]>) -> usize {
        let mut guard = self.lru.lock();
        let evicted = match key {
            Some(k) => {
                let s = String::from_utf8_lossy(k).into_owned();
                let mut inv = self.invalidated_keys.lock();
                Self::push_invalidation(&mut inv, s);
                if guard.pop(k).is_some() { 1 } else { 0 }
            }
            None => {
                let count = guard.len();
                let mut inv = self.invalidated_keys.lock();
                for (k, _) in guard.iter() {
                    Self::push_invalidation(&mut inv, String::from_utf8_lossy(k).into_owned());
                }
                guard.clear();
                count
            }
        };
        self.invalidations.fetch_add(1, Ordering::Relaxed);
        evicted
    }

    /// Invalidate a collection of keys or all keys if `None`.
    pub fn invalidate_keys(&self, keys: Option<&[Vec<u8>]>) -> usize {
        match keys {
            Some(ks) => {
                let mut count = 0;
                for k in ks {
                    count += self.invalidate_key(Some(k));
                }
                count
            }
            None => self.invalidate_key(None),
        }
    }

    /// Return all key names currently resident in the cache in LRU order (oldest to newest).
    #[must_use]
    pub fn cached_keys(&self) -> Vec<String> {
        let guard = self.lru.lock();
        guard
            .iter()
            .rev()
            .map(|(k, _)| String::from_utf8_lossy(k).into_owned())
            .collect()
    }

    /// Drain all queued invalidation events since the last call.
    pub fn drain_invalidations(&self) -> Vec<String> {
        let mut guard = self.invalidated_keys.lock();
        guard.drain(..).collect()
    }

    /// Return the current number of cached keys.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lru.lock().len()
    }

    /// Return true when the cache has zero entries.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lru.lock().is_empty()
    }

    /// Total L1 cache hits observed.
    #[must_use]
    pub fn hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total L1 cache misses observed.
    #[must_use]
    pub fn misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Total invalidation calls processed.
    #[must_use]
    pub fn invalidations(&self) -> u64 {
        self.invalidations.load(Ordering::Relaxed)
    }

    /// Inspect a RESP3 `Push` frame and process `>invalidate` notifications.
    ///
    /// Returns `true` when `frame` is a RESP3 `invalidate` push message.
    pub fn handle_push_frame(&self, frame: &RespValue) -> bool {
        let RespValue::Push(items) = frame.unattributed() else {
            return false;
        };
        let Some(first) = items.first() else {
            return false;
        };
        let Some(kind) = first.as_bytes() else {
            return false;
        };
        if !kind.eq_ignore_ascii_case(b"invalidate") {
            return false;
        }
        match items.get(1).map(RespValue::unattributed) {
            Some(RespValue::Null) => {
                self.invalidate_key(None);
            }
            Some(RespValue::Array(keys)) => {
                for key_val in keys {
                    if let Some(k) = key_val.as_bytes() {
                        self.invalidate_key(Some(k));
                    }
                }
            }
            _ => {
                self.invalidate_key(None);
            }
        }
        true
    }
}

/// Build `CLIENT TRACKING ON [PREFIX <p> ...] [BCAST]`.
#[must_use]
pub fn build_client_tracking_cmd(prefixes: &[String], bcast: bool) -> Vec<Vec<u8>> {
    let mut cmd = vec![b"CLIENT".to_vec(), b"TRACKING".to_vec(), b"ON".to_vec()];
    for prefix in prefixes {
        if !prefix.is_empty() {
            cmd.push(b"PREFIX".to_vec());
            cmd.push(prefix.as_bytes().to_vec());
        }
    }
    if bcast || !prefixes.is_empty() {
        cmd.push(b"BCAST".to_vec());
    }
    cmd
}
