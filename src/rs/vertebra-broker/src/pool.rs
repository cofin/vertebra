use std::collections::HashMap;
use std::sync::Arc;
use async_trait::async_trait;

#[derive(Debug, Clone)]
pub struct PoolStats {
    pub total: usize,
    pub active: usize,
    pub idle: usize,
}

#[async_trait]
pub trait PoolAdapter: Send + Sync {
    /// Acquire a resource from the pool.
    /// Returns an opaque handle or token.
    async fn acquire(&self) -> Result<String, String>;

    /// Release a resource back to the pool.
    async fn release(&self, resource_id: &str) -> Result<(), String>;

    /// Get pool statistics.
    async fn stats(&self) -> PoolStats;
}

pub struct PoolManager {
    pools: std::sync::RwLock<HashMap<String, Arc<dyn PoolAdapter>>>,
}

impl PoolManager {
    pub fn new() -> Self {
        Self {
            pools: std::sync::RwLock::new(HashMap::new()),
        }
    }

    pub fn register(&self, name: &str, pool: Arc<dyn PoolAdapter>) {
        let mut pools = self.pools.write().unwrap();
        pools.insert(name.to_string(), pool);
    }

    pub fn unregister(&self, name: &str) {
        let mut pools = self.pools.write().unwrap();
        pools.remove(name);
    }

    pub fn get(&self, name: &str) -> Option<Arc<dyn PoolAdapter>> {
        let pools = self.pools.read().unwrap();
        pools.get(name).cloned()
    }
}
