//! Shared-memory connection lease broker and seqlock statement plan cache for `vertebra-mssql`.
//!
//! Coordinates cross-worker connection slot leases (`ShmLeaseTable`) and caches `@P1`-rewritten
//! SQL plans in `ShmStatementCache` under [`DIALECT_TAG_MSSQL`] (`5`).

use std::sync::Arc;
use std::time::Duration;

pub use vertebra_core::broker::DIALECT_TAG_MSSQL;
use vertebra_core::broker::{
    LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementCacheStats,
};
use vertebra_core::shm::ShmRegion;
use vertebra_db::rewrite_mssql_placeholders;

use crate::errors::MssqlError;

/// Shared-memory broker backend managing MSSQL connection leases and `@P1` statement plans.
#[derive(Clone)]
pub struct MssqlBrokerBackend {
    lease_table: Arc<ShmLeaseTable>,
    stmt_cache: Option<Arc<ShmStatementCache>>,
}

impl MssqlBrokerBackend {
    /// Initialize a new in-process/shared-memory [`MssqlBrokerBackend`].
    pub fn new(
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> Result<Self, MssqlError> {
        let eff_pool = pool_size.max(1);
        let req_size =
            ShmLeaseTable::required_size_with_statements(eff_pool, max_overflow, stmt_capacity);
        let region = ShmRegion::create(req_size).map_err(|e| {
            MssqlError::config(format!("failed to allocate MSSQL broker ShmRegion: {e}"))
        })?;
        let table = ShmLeaseTable::init_in_region_with_statements(
            region,
            eff_pool,
            max_overflow,
            stmt_capacity,
        )?;
        let cache = table.statement_cache().map(Arc::new);
        Ok(Self {
            lease_table: Arc::new(table),
            stmt_cache: cache,
        })
    }

    /// Acquire a connection lease from the shared-memory broker table.
    pub fn acquire_lease(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        timeout: Duration,
    ) -> Result<LeaseGrant, MssqlError> {
        self.lease_table
            .acquire(worker_pid, ttl_ns, timeout, u64::from(DIALECT_TAG_MSSQL))
            .map_err(MssqlError::from)
    }

    /// Release a previously acquired connection lease.
    pub fn release_lease(&self, grant: LeaseGrant) -> Result<(), MssqlError> {
        self.lease_table
            .release(grant.slot_index, grant.generation)
            .map_err(MssqlError::from)
    }

    /// Look up or compile a SQL statement into `@P1` form, storing it in the seqlock statement cache
    /// under [`DIALECT_TAG_MSSQL`] when it fits within a 224-byte slot.
    pub fn resolve_or_rewrite_sql(&self, raw_sql: &str) -> (String, usize) {
        let dialect = DIALECT_TAG_MSSQL as u8;
        if let Some(ref cache) = self.stmt_cache
            && let Some(entry) = cache.lookup(dialect, raw_sql.as_bytes())
            && let Ok(compiled) = String::from_utf8(entry.compiled_sql)
        {
            return (compiled, usize::from(entry.param_count));
        }

        let (owned, count) = rewrite_mssql_placeholders(raw_sql);
        if let Some(ref cache) = self.stmt_cache
            && let Ok(param_count_u16) = u16::try_from(count)
        {
            let _ = cache.insert(
                dialect,
                raw_sql.as_bytes(),
                param_count_u16,
                owned.as_bytes(),
                &[],
            );
        }
        (owned, count)
    }

    /// Return current lease table telemetry counters.
    pub fn lease_stats(&self) -> LeaseTableStats {
        self.lease_table.stats()
    }

    /// Return current statement cache telemetry counters, if statement caching is enabled.
    pub fn statement_cache_stats(&self) -> Option<StatementCacheStats> {
        self.stmt_cache.as_ref().map(|c| c.stats())
    }

    /// Return the broker dialect tag (`DIALECT_TAG_MSSQL = 5`).
    pub const fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_MSSQL
    }

    /// Return the broker dialect name (`"mssql"`).
    pub const fn dialect(&self) -> &'static str {
        "mssql"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broker_backend_caches_rewritten_mssql_statements_and_tracks_leases() {
        let broker = MssqlBrokerBackend::new(2, 1, 16).expect("create broker");
        let (sql1, count1) =
            broker.resolve_or_rewrite_sql("SELECT id, name FROM users WHERE id = $1 AND active = ?");
        assert_eq!(sql1, "SELECT id, name FROM users WHERE id = @P1 AND active = @P2");
        assert_eq!(count1, 2);

        let (sql2, count2) =
            broker.resolve_or_rewrite_sql("SELECT id, name FROM users WHERE id = $1 AND active = ?");
        assert_eq!(sql2, sql1);
        assert_eq!(count2, 2);

        let cache_stats = broker.statement_cache_stats().expect("cache stats");
        assert_eq!(cache_stats.hits, 1);
        assert_eq!(cache_stats.inserts, 1);

        let grant = broker
            .acquire_lease(std::process::id(), 5_000_000_000, Duration::from_millis(50))
            .expect("acquire lease");
        assert_eq!(broker.lease_stats().active_leases, 1);
        broker.release_lease(grant).expect("release lease");
        assert_eq!(broker.lease_stats().active_leases, 0);
    }
}
