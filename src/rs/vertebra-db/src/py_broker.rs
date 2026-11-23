//! Shared broker pool registry, SHM Arrow region naming, and free-threaded build flag
//! for `vertebra-*` PyO3 database extensions.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

/// `true` when compiled against a free-threaded Python interpreter (`Py_GIL_DISABLED`).
pub const FREE_THREADED: bool = cfg!(Py_GIL_DISABLED);

static SHM_NAME_SEQ: AtomicU64 = AtomicU64::new(1);

/// Generate a process-unique POSIX shared-memory name `<prefix><separator><pid><separator><seq>`.
///
/// The process id and sequence render as lowercase hex so names stay inside the macOS
/// 31-byte `PSHMNAMLEN` limit for long-lived processes.
pub fn next_shm_name(prefix: &str, separator: char) -> String {
    let seq = SHM_NAME_SEQ.fetch_add(1, Ordering::Relaxed);
    format!(
        "{prefix}{separator}{:x}{separator}{seq:x}",
        std::process::id()
    )
}

/// Thread-safe registry mapping numeric pool IDs to `Arc<T>` pool handles for broker capsules.
pub struct BrokerPoolRegistry<T> {
    next_id: AtomicU64,
    pools: RwLock<HashMap<u64, Arc<T>>>,
}

impl<T> Default for BrokerPoolRegistry<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> BrokerPoolRegistry<T> {
    /// Construct an empty registry whose generated IDs start at `1`.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(1),
            pools: RwLock::new(HashMap::new()),
        }
    }

    /// Insert `pool` under an explicit `pool_id`, replacing any prior entry with the same ID.
    pub fn insert(&self, pool_id: u64, pool: Arc<T>) {
        self.pools
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(pool_id, pool);
    }

    /// Allocate a fresh pool ID, insert `pool`, and return its ID.
    pub fn register(&self, pool: Arc<T>) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        self.insert(id, pool);
        id
    }

    /// Look up `pool_id` and return a cloned `Arc<T>` handle.
    pub fn get(&self, pool_id: u64) -> Option<Arc<T>> {
        self.pools
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .get(&pool_id)
            .cloned()
    }

    /// Remove and return the pool handle registered under `pool_id`, if any.
    pub fn remove(&self, pool_id: u64) -> Option<Arc<T>> {
        self.pools
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(&pool_id)
    }

    /// Return the number of registered pools.
    pub fn len(&self) -> usize {
        self.pools
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .len()
    }

    /// Return `true` when no pools are registered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Remove all registered pool handles.
    pub fn clear(&self) {
        self.pools
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}
