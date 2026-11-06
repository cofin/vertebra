//! Native Rust SQLite WAL / Mmap / Pragma configuration and single-writer / multi-reader SHM pool.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use vertebra_core::broker::{
    LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache, StatementPlanEntry,
};
use vertebra_core::shm::ShmRegion;

/// SQLite dialect tag for `ShmStatementCache`.
pub const SQLITE_DIALECT_TAG: u8 = 3;

/// Configuration for SQLite 3.45+ WAL mode, memory-mapped I/O, and reader pool sizing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteConfig {
    pub path: String,
    pub wal_mode: bool,
    pub synchronous: String,
    pub mmap_size: u64,
    pub busy_timeout_ms: u32,
    pub max_readers: u16,
    pub foreign_keys: bool,
}

impl Default for SqliteConfig {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
            wal_mode: true,
            synchronous: "NORMAL".to_string(),
            mmap_size: 268_435_456,
            busy_timeout_ms: 5000,
            max_readers: 16,
            foreign_keys: true,
        }
    }
}

impl SqliteConfig {
    /// Generate the ordered list of initialization `PRAGMA` statements for this configuration.
    pub fn initialization_pragmas(&self) -> Vec<String> {
        let mut pragmas = Vec::with_capacity(6);
        if self.wal_mode {
            pragmas.push("PRAGMA journal_mode = WAL".to_string());
        } else {
            pragmas.push("PRAGMA journal_mode = MEMORY".to_string());
        }
        let sync_mode = self.synchronous.trim().to_ascii_uppercase();
        pragmas.push(format!("PRAGMA synchronous = {sync_mode}"));
        pragmas.push(format!("PRAGMA mmap_size = {}", self.mmap_size));
        pragmas.push(format!("PRAGMA busy_timeout = {}", self.busy_timeout_ms));
        pragmas.push("PRAGMA temp_store = MEMORY".to_string());
        let fk = if self.foreign_keys { "ON" } else { "OFF" };
        pragmas.push(format!("PRAGMA foreign_keys = {fk}"));
        pragmas
    }
}

/// Shared-memory coordinated SQLite connection pool enforcing single-writer + multi-reader concurrency.
#[derive(Clone)]
pub struct SqlitePool {
    pub config: SqliteConfig,
    writer_table: Arc<ShmLeaseTable>,
    reader_table: Arc<ShmLeaseTable>,
    stmt_cache: Arc<ShmStatementCache>,
}

impl SqlitePool {
    /// Initialize a new `SqlitePool` with a 1-slot writer lease table and `max_readers` reader lease table.
    pub fn new(config: SqliteConfig) -> Result<Self, String> {
        let writer_size = ShmLeaseTable::required_size(1, 0);
        let writer_region = ShmRegion::create(writer_size)
            .map_err(|e| format!("failed to allocate SQLite writer ShmRegion: {e}"))?;
        let writer_table = ShmLeaseTable::init_in_region(writer_region, 1, 0)
            .map_err(|e| format!("failed to init SQLite writer ShmLeaseTable: {e}"))?;

        let readers = (config.max_readers.max(1)) as u32;
        let reader_size = ShmLeaseTable::required_size(readers, 0);
        let reader_region = ShmRegion::create(reader_size)
            .map_err(|e| format!("failed to allocate SQLite reader ShmRegion: {e}"))?;
        let reader_table = ShmLeaseTable::init_in_region(reader_region, readers, 0)
            .map_err(|e| format!("failed to init SQLite reader ShmLeaseTable: {e}"))?;

        let cache_size = ShmStatementCache::required_size(128);
        let cache_region = ShmRegion::create(cache_size)
            .map_err(|e| format!("failed to allocate SQLite stmt cache ShmRegion: {e}"))?;
        let stmt_cache = ShmStatementCache::init_in_region(cache_region, 128)
            .map_err(|e| format!("failed to init SQLite ShmStatementCache: {e}"))?;

        Ok(Self {
            config,
            writer_table: Arc::new(writer_table),
            reader_table: Arc::new(reader_table),
            stmt_cache: Arc::new(stmt_cache),
        })
    }

    /// Acquire the exclusive single-writer lease.
    pub fn acquire_writer(&self, worker_pid: u32, timeout: Duration) -> Result<LeaseGrant, String> {
        self.writer_table
            .acquire(worker_pid, 0, timeout, 0)
            .map_err(|e| e.to_string())
    }

    /// Release the single-writer lease.
    pub fn release_writer(&self, grant: LeaseGrant) -> Result<(), String> {
        self.writer_table
            .release(grant.slot_index, grant.generation)
            .map_err(|e| e.to_string())
    }

    /// Acquire a concurrent reader lease.
    pub fn acquire_reader(&self, worker_pid: u32, timeout: Duration) -> Result<LeaseGrant, String> {
        self.reader_table
            .acquire(worker_pid, 0, timeout, 0)
            .map_err(|e| e.to_string())
    }

    /// Release a concurrent reader lease.
    pub fn release_reader(&self, grant: LeaseGrant) -> Result<(), String> {
        self.reader_table
            .release(grant.slot_index, grant.generation)
            .map_err(|e| e.to_string())
    }

    /// Look up or insert a statement plan in the shared-memory statement cache.
    pub fn lookup_or_insert_statement(
        &self,
        sql: &str,
        param_count: u16,
    ) -> Option<StatementPlanEntry> {
        if let Some(entry) = self.stmt_cache.lookup(SQLITE_DIALECT_TAG, sql.as_bytes()) {
            return Some(entry);
        }
        self.stmt_cache.insert(
            SQLITE_DIALECT_TAG,
            sql.as_bytes(),
            param_count,
            sql.as_bytes(),
            &[],
        );
        self.stmt_cache.lookup(SQLITE_DIALECT_TAG, sql.as_bytes())
    }

    /// Encode rows into MessagePack bytes for compact SHM slot transfer.
    pub fn encode_msgpack_rows(rows: &[Vec<Option<Vec<u8>>>]) -> Result<Vec<u8>, String> {
        rmp_serde::to_vec(rows).map_err(|e| format!("msgpack row encode error: {e}"))
    }

    /// Decode rows from MessagePack bytes.
    pub fn decode_msgpack_rows(buf: &[u8]) -> Result<Vec<Vec<Option<Vec<u8>>>>, String> {
        rmp_serde::from_slice(buf).map_err(|e| format!("msgpack row decode error: {e}"))
    }

    /// Return `(writer_stats, reader_stats)`.
    pub fn stats(&self) -> (LeaseTableStats, LeaseTableStats) {
        (self.writer_table.stats(), self.reader_table.stats())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlite_initialization_pragmas() {
        let cfg = SqliteConfig::default();
        let pragmas = cfg.initialization_pragmas();
        assert_eq!(
            pragmas,
            vec![
                "PRAGMA journal_mode = WAL",
                "PRAGMA synchronous = NORMAL",
                "PRAGMA mmap_size = 268435456",
                "PRAGMA busy_timeout = 5000",
                "PRAGMA temp_store = MEMORY",
                "PRAGMA foreign_keys = ON",
            ]
        );
    }

    #[test]
    fn test_sqlite_single_writer_and_multi_reader_leases() {
        let cfg = SqliteConfig {
            max_readers: 2,
            ..Default::default()
        };
        let pool = SqlitePool::new(cfg).unwrap();

        let w1 = pool.acquire_writer(100, Duration::from_millis(10)).unwrap();
        assert!(pool.acquire_writer(101, Duration::ZERO).is_err());
        pool.release_writer(w1).unwrap();

        let r1 = pool.acquire_reader(100, Duration::from_millis(10)).unwrap();
        let r2 = pool.acquire_reader(101, Duration::from_millis(10)).unwrap();
        assert!(pool.acquire_reader(102, Duration::ZERO).is_err());
        pool.release_reader(r1).unwrap();
        pool.release_reader(r2).unwrap();

        let rows = vec![vec![Some(b"row1".to_vec()), None]];
        let encoded = SqlitePool::encode_msgpack_rows(&rows).unwrap();
        let decoded = SqlitePool::decode_msgpack_rows(&encoded).unwrap();
        assert_eq!(decoded, rows);
    }
}
