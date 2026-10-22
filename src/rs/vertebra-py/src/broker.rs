//! PyO3 bindings for shared-memory connection broker and statement cache.

use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use vertebra_core::broker::{
    BrokerError, LeaseGrant, LeaseTableStats, ShmLeaseTable, ShmStatementCache,
    StatementCacheStats, StatementPlanEntry,
};
use vertebra_core::shm::ShmRegion;

fn broker_err_to_py(err: BrokerError) -> PyErr {
    match err {
        BrokerError::InvalidCapacity => pyo3::exceptions::PyValueError::new_err(err.to_string()),
        BrokerError::RegionTooSmall { .. } => {
            pyo3::exceptions::PyValueError::new_err(err.to_string())
        }
        BrokerError::InvalidMagic(_) | BrokerError::UnsupportedAbi(_) => {
            pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
        }
        BrokerError::PoolExhausted => pyo3::exceptions::PyTimeoutError::new_err(err.to_string()),
        BrokerError::LeaseNotFound => pyo3::exceptions::PyKeyError::new_err(err.to_string()),
        BrokerError::GenerationMismatch => {
            pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
        }
    }
}

#[pyclass(frozen, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeLeaseGrant {
    #[pyo3(get)]
    pub slot_index: u32,
    #[pyo3(get)]
    pub generation: u64,
    #[pyo3(get)]
    pub worker_pid: u32,
    #[pyo3(get)]
    pub is_overflow: bool,
    #[pyo3(get)]
    pub acquired_ns: u64,
    #[pyo3(get)]
    pub expiry_ns: u64,
    #[pyo3(get)]
    pub bind_key_hash: u64,
}

impl From<LeaseGrant> for NativeLeaseGrant {
    fn from(g: LeaseGrant) -> Self {
        Self {
            slot_index: g.slot_index,
            generation: g.generation,
            worker_pid: g.worker_pid,
            is_overflow: g.is_overflow,
            acquired_ns: g.acquired_ns,
            expiry_ns: g.expiry_ns,
            bind_key_hash: g.bind_key_hash,
        }
    }
}

#[pyclass(frozen, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeBrokerStats {
    #[pyo3(get)]
    pub pool_size: u32,
    #[pyo3(get)]
    pub max_overflow: u32,
    #[pyo3(get)]
    pub active_leases: u32,
    #[pyo3(get)]
    pub primary_in_use: u32,
    #[pyo3(get)]
    pub overflow_in_use: u32,
    #[pyo3(get)]
    pub waiting_threads: u32,
    #[pyo3(get)]
    pub total_acquired: u64,
    #[pyo3(get)]
    pub total_released: u64,
    #[pyo3(get)]
    pub total_expired: u64,
    #[pyo3(get)]
    pub total_timeouts: u64,
    #[pyo3(get)]
    pub total_reclaimed: u64,
}

impl From<LeaseTableStats> for NativeBrokerStats {
    fn from(s: LeaseTableStats) -> Self {
        Self {
            pool_size: s.pool_size,
            max_overflow: s.max_overflow,
            active_leases: s.active_leases,
            primary_in_use: s.primary_in_use,
            overflow_in_use: s.overflow_in_use,
            waiting_threads: s.waiting_threads,
            total_acquired: s.total_acquired,
            total_released: s.total_released,
            total_expired: s.total_expired,
            total_timeouts: s.total_timeouts,
            total_reclaimed: s.total_reclaimed,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct NativeLeaseBroker {
    table: Arc<ShmLeaseTable>,
}

#[pymethods]
impl NativeLeaseBroker {
    #[new]
    #[pyo3(signature = (pool_size=20, max_overflow=10))]
    pub fn new(pool_size: u32, max_overflow: u32) -> PyResult<Self> {
        let req_size = ShmLeaseTable::required_size(pool_size, max_overflow);
        let region = ShmRegion::create(req_size)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let table = ShmLeaseTable::init_in_region(region, pool_size, max_overflow)
            .map_err(broker_err_to_py)?;
        Ok(Self {
            table: Arc::new(table),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (name, pool_size=20, max_overflow=10))]
    pub fn create_named(name: &str, pool_size: u32, max_overflow: u32) -> PyResult<Self> {
        let req_size = ShmLeaseTable::required_size(pool_size, max_overflow);
        #[cfg(unix)]
        {
            let region = ShmRegion::create_named(name, req_size)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let table = ShmLeaseTable::init_in_region(region, pool_size, max_overflow)
                .map_err(broker_err_to_py)?;
            Ok(Self {
                table: Arc::new(table),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (name, pool_size, max_overflow, req_size);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    #[pyo3(signature = (name, pool_size=20, max_overflow=10))]
    pub fn open_named(name: &str, pool_size: u32, max_overflow: u32) -> PyResult<Self> {
        let req_size = ShmLeaseTable::required_size(pool_size, max_overflow);
        #[cfg(unix)]
        {
            let region = ShmRegion::open_named(name, req_size)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let table = ShmLeaseTable::attach_region(region).map_err(broker_err_to_py)?;
            Ok(Self {
                table: Arc::new(table),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (name, pool_size, max_overflow, req_size);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    pub fn from_fd(fd: i32, size: usize) -> PyResult<Self> {
        #[cfg(unix)]
        {
            let dup_fd = unsafe { libc::dup(fd) };
            if dup_fd < 0 {
                return Err(pyo3::exceptions::PyOSError::new_err(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
            let region = ShmRegion::from_fd(dup_fd, size)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let table = ShmLeaseTable::attach_region(region).map_err(broker_err_to_py)?;
            Ok(Self {
                table: Arc::new(table),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (fd, size);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "from_fd only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    pub fn from_env() -> PyResult<Option<Self>> {
        #[cfg(unix)]
        {
            let fd_val = match std::env::var("VERTEBRA_BROKER_SHM_FD") {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let size_val = match std::env::var("VERTEBRA_BROKER_SHM_SIZE") {
                Ok(v) => v,
                Err(_) => return Ok(None),
            };
            let fd: i32 = fd_val.parse().map_err(|e: std::num::ParseIntError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            let size: usize = size_val.parse().map_err(|e: std::num::ParseIntError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            Self::from_fd(fd, size).map(Some)
        }
        #[cfg(not(unix))]
        {
            Ok(None)
        }
    }

    #[pyo3(signature = (worker_pid, ttl_ns=0, timeout_ms=5000, bind_key_hash=0))]
    pub fn acquire(
        &self,
        py: Python<'_>,
        worker_pid: u32,
        ttl_ns: u64,
        timeout_ms: u64,
        bind_key_hash: u64,
    ) -> PyResult<Option<NativeLeaseGrant>> {
        let table = Arc::clone(&self.table);
        let result = py.detach(move || {
            table.acquire(
                worker_pid,
                ttl_ns,
                Duration::from_millis(timeout_ms),
                bind_key_hash,
            )
        });
        match result {
            Ok(grant) => Ok(Some(NativeLeaseGrant::from(grant))),
            Err(BrokerError::PoolExhausted) => Ok(None),
            Err(err) => Err(broker_err_to_py(err)),
        }
    }

    #[pyo3(signature = (worker_pid, ttl_ns=0, bind_key_hash=0))]
    pub fn try_acquire(
        &self,
        worker_pid: u32,
        ttl_ns: u64,
        bind_key_hash: u64,
    ) -> PyResult<Option<NativeLeaseGrant>> {
        match self.table.try_acquire(worker_pid, ttl_ns, bind_key_hash) {
            Ok(grant) => Ok(Some(NativeLeaseGrant::from(grant))),
            Err(BrokerError::PoolExhausted) => Ok(None),
            Err(err) => Err(broker_err_to_py(err)),
        }
    }

    pub fn release(&self, slot_index: u32, generation: u64) -> PyResult<bool> {
        match self.table.release(slot_index, generation) {
            Ok(()) => Ok(true),
            Err(BrokerError::LeaseNotFound) | Err(BrokerError::GenerationMismatch) => Ok(false),
            Err(err) => Err(broker_err_to_py(err)),
        }
    }

    pub fn reap_expired(&self) -> u32 {
        self.table
            .reap_expired(vertebra_core::broker::monotonic_now_ns())
    }

    pub fn reclaim_worker(&self, worker_pid: u32) -> u32 {
        self.table.reclaim_worker(worker_pid)
    }

    pub fn stats(&self) -> NativeBrokerStats {
        NativeBrokerStats::from(self.table.stats())
    }

    #[getter]
    pub fn pool_size(&self) -> u32 {
        self.table.pool_size()
    }

    #[getter]
    pub fn max_overflow(&self) -> u32 {
        self.table.max_overflow()
    }

    #[getter]
    pub fn total_capacity(&self) -> u32 {
        self.table.total_capacity()
    }
}

// ============================================================================
// Statement Cache PyO3 Bindings (Task 2.1)
// ============================================================================

#[pyclass(frozen, from_py_object)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NativeStatementPlan {
    #[pyo3(get)]
    pub sql_hash: u64,
    #[pyo3(get)]
    pub dialect_tag: u8,
    #[pyo3(get)]
    pub param_count: u16,
    #[pyo3(get)]
    pub compiled_sql: Vec<u8>,
    #[pyo3(get)]
    pub param_plan: Vec<u8>,
}

impl From<StatementPlanEntry> for NativeStatementPlan {
    fn from(e: StatementPlanEntry) -> Self {
        Self {
            sql_hash: e.sql_hash,
            dialect_tag: e.dialect_tag,
            param_count: e.param_count,
            compiled_sql: e.compiled_sql,
            param_plan: e.param_plan,
        }
    }
}

#[pyclass(frozen, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeStatementCacheStats {
    #[pyo3(get)]
    pub capacity: u32,
    #[pyo3(get)]
    pub hits: u64,
    #[pyo3(get)]
    pub misses: u64,
    #[pyo3(get)]
    pub inserts: u64,
    #[pyo3(get)]
    pub evictions: u64,
}

impl From<StatementCacheStats> for NativeStatementCacheStats {
    fn from(s: StatementCacheStats) -> Self {
        Self {
            capacity: s.capacity,
            hits: s.hits,
            misses: s.misses,
            inserts: s.inserts,
            evictions: s.evictions,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct NativeStatementCache {
    cache: Arc<ShmStatementCache>,
}

#[pymethods]
impl NativeStatementCache {
    #[new]
    #[pyo3(signature = (capacity=512))]
    pub fn new(capacity: u32) -> PyResult<Self> {
        let req_size = ShmStatementCache::required_size(capacity);
        let region = ShmRegion::create(req_size)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let cache =
            ShmStatementCache::init_in_region(region, capacity).map_err(broker_err_to_py)?;
        Ok(Self {
            cache: Arc::new(cache),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (name, capacity=512))]
    pub fn create_named(name: &str, capacity: u32) -> PyResult<Self> {
        let req_size = ShmStatementCache::required_size(capacity);
        #[cfg(unix)]
        {
            let region = ShmRegion::create_named(name, req_size)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let cache =
                ShmStatementCache::init_in_region(region, capacity).map_err(broker_err_to_py)?;
            Ok(Self {
                cache: Arc::new(cache),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (name, capacity, req_size);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    #[pyo3(signature = (name, capacity=512))]
    pub fn open_named(name: &str, capacity: u32) -> PyResult<Self> {
        let req_size = ShmStatementCache::required_size(capacity);
        #[cfg(unix)]
        {
            let region = ShmRegion::open_named(name, req_size)
                .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
            let cache =
                ShmStatementCache::attach_region(region, capacity).map_err(broker_err_to_py)?;
            Ok(Self {
                cache: Arc::new(cache),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (name, capacity, req_size);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    pub fn lookup(&self, dialect_tag: u8, raw_sql: &[u8]) -> Option<NativeStatementPlan> {
        self.cache
            .lookup(dialect_tag, raw_sql)
            .map(NativeStatementPlan::from)
    }

    pub fn insert(
        &self,
        dialect_tag: u8,
        raw_sql: &[u8],
        param_count: u16,
        compiled_sql: &[u8],
        param_plan: &[u8],
    ) -> bool {
        self.cache
            .insert(dialect_tag, raw_sql, param_count, compiled_sql, param_plan)
    }

    pub fn stats(&self) -> NativeStatementCacheStats {
        NativeStatementCacheStats::from(self.cache.stats())
    }
}
