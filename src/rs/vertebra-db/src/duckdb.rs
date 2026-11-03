//! SHM-brokered single-writer DuckDB analytical engine, DuckDB 2.0-safe pragmas, and `ShmRegion` Arrow IPC framing.

use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use vertebra_core::broker::{LeaseGrant, ShmLeaseTable};
use vertebra_core::shm::ShmRegion;

pub use crate::arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, ARROW_IPC_EOS_MARKER, SHM_ARROW_MAGIC, ShmArrowFrameHeader,
    ShmArrowStreamWriter, normalize_shm_name, open_shm_arrow_region, read_shm_arrow_frame,
    read_shm_arrow_frame_with, release_shm_arrow_frame, write_shm_arrow_frame,
    write_shm_arrow_region,
};

/// Configuration for the SHM-brokered DuckDB analytical engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DuckdbConfig {
    pub path: String,
    pub memory_limit_mb: usize,
    pub threads: usize,
    pub parquet_metadata_cache: bool,
    pub httpfs_connection_caching: bool,
    pub enable_http_metadata_cache: bool,
}

impl Default for DuckdbConfig {
    fn default() -> Self {
        Self {
            path: ":memory:".to_string(),
            memory_limit_mb: 2048,
            threads: 4,
            parquet_metadata_cache: true,
            httpfs_connection_caching: true,
            enable_http_metadata_cache: true,
        }
    }
}

impl DuckdbConfig {
    /// Generate DuckDB 2.0-safe post-initialization `SET` statements.
    ///
    /// Note: `parquet_metadata_cache` MUST be applied as a post-init `SET` statement
    /// rather than a startup config parameter because DuckDB 2.0 evaluates startup config
    /// options before autoloading the `parquet` extension.
    pub fn bootstrap_statements(&self) -> Vec<String> {
        let mut stmts = Vec::with_capacity(5);
        stmts.push(format!("SET memory_limit = '{}MB'", self.memory_limit_mb));
        stmts.push(format!("SET threads = {}", self.threads.max(1)));
        let pq_cache = if self.parquet_metadata_cache {
            "true"
        } else {
            "false"
        };
        stmts.push(format!("SET parquet_metadata_cache = {pq_cache}"));
        let httpfs_cache = if self.httpfs_connection_caching {
            "true"
        } else {
            "false"
        };
        stmts.push(format!("SET httpfs_connection_caching = {httpfs_cache}"));
        let http_meta = if self.enable_http_metadata_cache {
            "true"
        } else {
            "false"
        };
        stmts.push(format!("SET enable_http_metadata_cache = {http_meta}"));
        stmts
    }
}

/// Single-writer / multi-reader SHM broker coordinator for a shared DuckDB catalog.
#[derive(Clone)]
pub struct DuckdbBrokerPool {
    pub config: DuckdbConfig,
    writer_lease: Arc<ShmLeaseTable>,
    reader_leases: Arc<ShmLeaseTable>,
}

impl DuckdbBrokerPool {
    /// Initialize a new `DuckdbBrokerPool`.
    pub fn new(config: DuckdbConfig, max_readers: u32) -> Result<Self, String> {
        let writer_size = ShmLeaseTable::required_size(1, 0);
        let writer_region = ShmRegion::create(writer_size)
            .map_err(|e| format!("failed to allocate DuckDB writer ShmRegion: {e}"))?;
        let writer_lease = ShmLeaseTable::init_in_region(writer_region, 1, 0)
            .map_err(|e| format!("failed to init DuckDB writer ShmLeaseTable: {e}"))?;

        let readers = max_readers.max(1);
        let reader_size = ShmLeaseTable::required_size(readers, 0);
        let reader_region = ShmRegion::create(reader_size)
            .map_err(|e| format!("failed to allocate DuckDB reader ShmRegion: {e}"))?;
        let reader_leases = ShmLeaseTable::init_in_region(reader_region, readers, 0)
            .map_err(|e| format!("failed to init DuckDB reader ShmLeaseTable: {e}"))?;

        Ok(Self {
            config,
            writer_lease: Arc::new(writer_lease),
            reader_leases: Arc::new(reader_leases),
        })
    }

    /// Acquire the single-writer lease for mutating DDL/DML statements.
    pub fn acquire_writer(&self, worker_pid: u32, timeout: Duration) -> Result<LeaseGrant, String> {
        self.writer_lease
            .acquire(worker_pid, 0, timeout, 0)
            .map_err(|e| e.to_string())
    }

    /// Release the single-writer lease.
    pub fn release_writer(&self, grant: LeaseGrant) -> Result<(), String> {
        self.writer_lease
            .release(grant.slot_index, grant.generation)
            .map_err(|e| e.to_string())
    }

    /// Acquire a concurrent analytical reader lease.
    pub fn acquire_reader(&self, worker_pid: u32, timeout: Duration) -> Result<LeaseGrant, String> {
        self.reader_leases
            .acquire(worker_pid, 0, timeout, 0)
            .map_err(|e| e.to_string())
    }

    /// Release a concurrent analytical reader lease.
    pub fn release_reader(&self, grant: LeaseGrant) -> Result<(), String> {
        self.reader_leases
            .release(grant.slot_index, grant.generation)
            .map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_duckdb_2_0_safe_bootstrap_statements() {
        let cfg = DuckdbConfig::default();
        let stmts = cfg.bootstrap_statements();
        assert_eq!(
            stmts,
            vec![
                "SET memory_limit = '2048MB'",
                "SET threads = 4",
                "SET parquet_metadata_cache = true",
                "SET httpfs_connection_caching = true",
                "SET enable_http_metadata_cache = true",
            ]
        );
    }

    #[test]
    fn test_shm_arrow_frame_write_read_release() {
        let shm_name = format!("/vtb-arrow-test-{}", std::process::id());
        let payload = b"ARROW1\x00\x00mock-ipc-stream-payload";
        let (norm_name, written_len) = write_shm_arrow_frame(&shm_name, payload).unwrap();
        assert_eq!(norm_name, shm_name);
        assert_eq!(written_len, payload.len());

        let read_back = read_shm_arrow_frame(&shm_name).unwrap();
        assert_eq!(read_back, payload);

        assert!(release_shm_arrow_frame(&shm_name));
    }
}
