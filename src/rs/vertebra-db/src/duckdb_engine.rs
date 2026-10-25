//! SHM-brokered single-writer DuckDB analytical engine, DuckDB 2.0-safe pragmas, and `ShmRegion` Arrow IPC framing.

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use vertebra_core::broker::{LeaseGrant, ShmLeaseTable};
use vertebra_core::shm::ShmRegion;

/// 8-byte magic header prefix for shared-memory Arrow IPC frames.
pub const SHM_ARROW_MAGIC: &[u8; 8] = b"VTBARROW";

/// Header written at offset `0` of a shared-memory Arrow IPC `ShmRegion`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShmArrowFrameHeader {
    pub magic: [u8; 8],
    pub byte_len: u64,
}

impl ShmArrowFrameHeader {
    pub const SIZE: usize = 16;

    /// Encode header into 16 bytes.
    pub fn to_bytes(&self) -> [u8; 16] {
        let mut out = [0u8; 16];
        out[0..8].copy_from_slice(&self.magic);
        out[8..16].copy_from_slice(&self.byte_len.to_le_bytes());
        out
    }

    /// Decode header from the first 16 bytes of a slice.
    pub fn from_slice(buf: &[u8]) -> Result<Self, String> {
        if buf.len() < Self::SIZE {
            return Err("buffer too short for 16-byte ShmArrowFrameHeader".to_string());
        }
        let mut magic = [0u8; 8];
        magic.copy_from_slice(&buf[0..8]);
        if &magic != SHM_ARROW_MAGIC {
            return Err("invalid ShmArrowFrameHeader magic (expected VTBARROW)".to_string());
        }
        let byte_len = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        Ok(Self { magic, byte_len })
    }
}

static ACTIVE_ARROW_SHM_REGIONS: LazyLock<DashMap<String, ShmRegion>> = LazyLock::new(DashMap::new);

fn normalize_shm_name(shm_name: &str) -> String {
    let trimmed = shm_name.trim();
    if trimmed.starts_with('/') {
        trimmed.to_string()
    } else {
        format!("/{trimmed}")
    }
}

/// Write an Arrow IPC byte stream into a shared-memory `ShmRegion`, returning `(normalized_shm_name, payload_len)`.
pub fn write_shm_arrow_frame(shm_name: &str, ipc_bytes: &[u8]) -> Result<(String, usize), String> {
    let name = normalize_shm_name(shm_name);
    let total_size = ShmArrowFrameHeader::SIZE + ipc_bytes.len();
    let alloc_size = total_size.max(4096);

    #[cfg(unix)]
    let mut region = match ShmRegion::create_named(&name, alloc_size) {
        Ok(r) => r,
        Err(_) => ShmRegion::create(alloc_size)
            .map_err(|e| format!("failed to create Arrow ShmRegion `{name}`: {e}"))?,
    };
    #[cfg(not(unix))]
    let mut region = ShmRegion::create(alloc_size)
        .map_err(|e| format!("failed to create Arrow ShmRegion `{name}`: {e}"))?;

    let header = ShmArrowFrameHeader {
        magic: *SHM_ARROW_MAGIC,
        byte_len: ipc_bytes.len() as u64,
    };
    let hdr_bytes = header.to_bytes();

    // SAFETY: We exclusively own `region` during initialization before publishing to ACTIVE_ARROW_SHM_REGIONS.
    unsafe {
        let slice = region.as_mut_slice();
        slice[0..16].copy_from_slice(&hdr_bytes);
        slice[16..16 + ipc_bytes.len()].copy_from_slice(ipc_bytes);
    }

    ACTIVE_ARROW_SHM_REGIONS.insert(name.clone(), region);
    Ok((name, ipc_bytes.len()))
}

/// Borrow an Arrow IPC byte stream from a shared-memory `ShmRegion` without intermediate `Vec<u8>` allocation.
pub fn read_shm_arrow_frame_with<R, F>(shm_name: &str, f: F) -> Result<R, String>
where
    F: FnOnce(&[u8]) -> R,
{
    let name = normalize_shm_name(shm_name);
    if let Some(region_ref) = ACTIVE_ARROW_SHM_REGIONS.get(&name) {
        // SAFETY: Region was initialized by `write_shm_arrow_frame` and is immutable after publication.
        let slice = unsafe { region_ref.as_slice() };
        let header = ShmArrowFrameHeader::from_slice(slice)?;
        let len = header.byte_len as usize;
        if slice.len() < ShmArrowFrameHeader::SIZE + len {
            return Err("corrupted ShmArrowFrameHeader byte_len exceeds region size".to_string());
        }
        return Ok(f(&slice[16..16 + len]));
    }

    #[cfg(unix)]
    {
        let probe = ShmRegion::open_named(&name, 4096)
            .map_err(|e| format!("failed to open Arrow ShmRegion `{name}`: {e}"))?;
        let header = unsafe { ShmArrowFrameHeader::from_slice(probe.as_slice()) }?;
        let len = header.byte_len as usize;
        let total_size = ShmArrowFrameHeader::SIZE + len;
        if total_size <= probe.len() {
            let slice = unsafe { probe.as_slice() };
            return Ok(f(&slice[16..16 + len]));
        }
        drop(probe);
        let full = ShmRegion::open_named(&name, total_size)
            .map_err(|e| format!("failed to map full Arrow ShmRegion `{name}`: {e}"))?;
        let slice = unsafe { full.as_slice() };
        Ok(f(&slice[16..16 + len]))
    }
    #[cfg(not(unix))]
    {
        let _ = f;
        Err(format!("Arrow ShmRegion `{name}` not found"))
    }
}

/// Read an Arrow IPC byte stream from a shared-memory `ShmRegion`.
pub fn read_shm_arrow_frame(shm_name: &str) -> Result<Vec<u8>, String> {
    read_shm_arrow_frame_with(shm_name, |slice| slice.to_vec())
}

/// Release and unlink an active shared-memory Arrow IPC `ShmRegion`.
pub fn release_shm_arrow_frame(shm_name: &str) -> bool {
    let name = normalize_shm_name(shm_name);
    ACTIVE_ARROW_SHM_REGIONS.remove(&name).is_some()
}

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
