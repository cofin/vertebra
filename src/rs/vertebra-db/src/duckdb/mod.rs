//! DuckDB 2.0-safe `SET` bootstrap statements and `ShmRegion` Arrow IPC frame re-exports.

use serde::{Deserialize, Serialize};

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
