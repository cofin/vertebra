//! PyO3 bindings for shared-memory connection broker and statement cache.

use std::collections::BTreeMap;
#[cfg(unix)]
use std::sync::Mutex;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyRuntimeError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyByteArray, PyBytes, PyCapsule, PyString};
use vertebra_core::broker::{
    BrokerError, LeaseGrant as RsLeaseGrant, LeaseTableStats, PartitionStats as RsPartitionStats,
    PoolPartitionSpec, SHM_CACHE_MAX_KEY_BYTES, SHM_CACHE_MAX_VALUE_BYTES, ShmCache as RsShmCache,
    ShmCacheStats as RsShmCacheStats, ShmLeaseTable, ShmStatementCache,
    StatementCacheStats as RsStatementCacheStats, StatementPlanEntry,
};
use vertebra_core::shm::ShmRegion;
use vertebra_db::{BROKER_ENGINE_CAPSULE_NAME, BrokerEngineCapsule, ForeignEngineCapsule};

type PyChannelFrame<'py> = (String, Bound<'py, PyBytes>, u64, Option<String>);

fn extract_bytes_or_str(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(b) = obj.cast::<PyBytes>() {
        Ok(b.as_bytes().to_vec())
    } else if let Ok(s) = obj.extract::<&str>() {
        Ok(s.as_bytes().to_vec())
    } else {
        obj.extract::<Vec<u8>>()
    }
}

/// Compute a deterministic unseeded XXH3-64 digest across Python worker processes.
#[pyfunction]
pub fn stable_hash64(data: &Bound<'_, PyAny>) -> PyResult<u64> {
    if let Ok(b) = data.cast::<PyBytes>() {
        return Ok(vertebra_core::broker::stable_hash64(b.as_bytes()));
    }
    if let Ok(s) = data.cast::<PyString>() {
        let utf8 = s.to_str()?;
        return Ok(vertebra_core::broker::stable_hash64(utf8.as_bytes()));
    }
    if let Ok(ba) = data.cast::<PyByteArray>() {
        // SAFETY: GIL or immutable borrow within Python scope holds `ba` alive cleanly.
        let bytes = unsafe { ba.as_bytes() };
        return Ok(vertebra_core::broker::stable_hash64(bytes));
    }
    if let Ok(buf) = PyBuffer::<u8>::get(data)
        && let Ok(vec) = buf.to_vec(data.py())
    {
        return Ok(vertebra_core::broker::stable_hash64(&vec));
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "stable_hash64 expects bytes, bytearray, memoryview, or str",
    ))
}

#[cfg(unix)]
struct CachedShmPubSubTable {
    fd: i32,
    size: usize,
    dev: u64,
    ino: u64,
    table: Arc<ShmLeaseTable>,
}

#[cfg(unix)]
static SHM_PUBSUB_TABLE_CACHE: Mutex<Option<CachedShmPubSubTable>> = Mutex::new(None);

#[cfg(unix)]
fn resolve_env_lease_table() -> PyResult<Option<Arc<ShmLeaseTable>>> {
    let fd_val = match std::env::var("VERTEBRA_BROKER_SHM_FD") {
        Ok(v) => v,
        Err(_) => return Ok(None),
    };
    let fd: i32 = fd_val
        .parse()
        .map_err(|e: std::num::ParseIntError| PyValueError::new_err(e.to_string()))?;
    let size: usize = match std::env::var("VERTEBRA_BROKER_SHM_SIZE") {
        Ok(v) => v
            .parse()
            .map_err(|e: std::num::ParseIntError| PyValueError::new_err(e.to_string()))?,
        Err(_) => 0,
    };
    // SAFETY: `libc::stat` is a plain C struct safe to zero-initialize before `fstat`.
    let mut stat: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: `stat` points to initialized stack memory for `fstat`.
    if unsafe { libc::fstat(fd, &mut stat) } != 0 {
        return Err(pyo3::exceptions::PyOSError::new_err(
            std::io::Error::last_os_error().to_string(),
        ));
    }
    let dev = stat.st_dev as u64;
    let ino = stat.st_ino as u64;
    {
        let guard = SHM_PUBSUB_TABLE_CACHE
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(ref cached) = *guard
            && cached.fd == fd
            && cached.size == size
            && cached.dev == dev
            && cached.ino == ino
        {
            return Ok(Some(Arc::clone(&cached.table)));
        }
    }
    let broker = LeaseBroker::from_fd(fd, size)?;
    let table = broker.table();
    let mut guard = SHM_PUBSUB_TABLE_CACHE
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(CachedShmPubSubTable {
        fd,
        size,
        dev,
        ino,
        table: Arc::clone(&table),
    });
    Ok(Some(table))
}

fn pubsub_publish_bytes(
    topic: &str,
    payload: &[u8],
    idempotency_key: Option<&str>,
) -> PyResult<Option<u64>> {
    let is_shm = std::env::var("VERTEBRA_TRANSPORT_MODE").as_deref() == Ok("shm");
    if !is_shm && let Some(bridge) = vertebra_core::bridge::BatchBridge::get() {
        return Ok(bridge.publish_pubsub(topic, payload, idempotency_key));
    }

    if is_shm || std::env::var_os("VERTEBRA_BROKER_SHM_FD").is_some() {
        let frame = vertebra_core::bridge::encode_shm_pubsub_frame(topic, payload, idempotency_key)
            .map_err(|len| {
                PyValueError::new_err(format!(
                    "SHM pubsub frame length {len} exceeds {} byte limit",
                    vertebra_core::broker::CHAN_MAX_PAYLOAD_BYTES
                ))
            })?;
        #[cfg(unix)]
        if let Some(table) = resolve_env_lease_table()? {
            table.publish_channel(
                vertebra_core::bridge::SHM_PUBSUB_CHANNEL,
                &frame,
                vertebra_core::broker::monotonic_now_ns(),
                None,
            );
            return Ok(None);
        }
        #[cfg(not(unix))]
        let _ = frame;
        return Ok(None);
    }

    Ok(None)
}

/// Publish `payload` (`Buffer | str`) to `topic` across the active Embedded or SHM Pub/Sub bridge.
#[pyfunction]
#[pyo3(signature = (topic, payload, idempotency_key=None))]
pub fn pubsub_publish(
    py: Python<'_>,
    topic: &str,
    payload: &Bound<'_, PyAny>,
    idempotency_key: Option<&str>,
) -> PyResult<Option<u64>> {
    if let Ok(py_str) = payload.cast::<PyString>() {
        let utf8 = py_str.to_str()?;
        return pubsub_publish_bytes(topic, utf8.as_bytes(), idempotency_key);
    }
    crate::transport::with_buffer_slice(py, payload, |slice| {
        pubsub_publish_bytes(topic, slice, idempotency_key)
    })
}

fn broker_err_to_py(err: BrokerError) -> PyErr {
    match err {
        BrokerError::InvalidCapacity | BrokerError::InvalidPartition(_) => {
            pyo3::exceptions::PyValueError::new_err(err.to_string())
        }
        BrokerError::RegionTooSmall { .. } | BrokerError::MisalignedRegion { .. } => {
            pyo3::exceptions::PyValueError::new_err(err.to_string())
        }
        BrokerError::PoolExhausted | BrokerError::InitTimeout => {
            pyo3::exceptions::PyTimeoutError::new_err(err.to_string())
        }
        BrokerError::LeaseNotFound => pyo3::exceptions::PyKeyError::new_err(err.to_string()),
        BrokerError::SegmentInUse { owner_pid } => pyo3::exceptions::PyFileExistsError::new_err(
            format!("shared memory segment in use by owner_pid={owner_pid}"),
        ),
        BrokerError::NotFound => pyo3::exceptions::PyFileNotFoundError::new_err(err.to_string()),
        BrokerError::PermissionDenied => {
            pyo3::exceptions::PyPermissionError::new_err(err.to_string())
        }
        BrokerError::SystemError(errno) => PyErr::from(std::io::Error::from_raw_os_error(errno)),
        BrokerError::InvalidMagic(_)
        | BrokerError::UnsupportedAbi(_)
        | BrokerError::GenerationMismatch
        | BrokerError::StaleGeneration
        | BrokerError::InvalidState
        | BrokerError::PartitionsExhausted => {
            pyo3::exceptions::PyRuntimeError::new_err(err.to_string())
        }
    }
}

#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseGrant {
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

impl From<RsLeaseGrant> for LeaseGrant {
    fn from(g: RsLeaseGrant) -> Self {
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

#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BrokerStats {
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
    pub quarantined: u32,
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
    #[pyo3(get)]
    pub total_quarantined: u64,
    #[pyo3(get)]
    pub total_disposed: u64,
}

impl From<LeaseTableStats> for BrokerStats {
    fn from(s: LeaseTableStats) -> Self {
        Self {
            pool_size: s.pool_size,
            max_overflow: s.max_overflow,
            active_leases: s.active_leases,
            primary_in_use: s.primary_in_use,
            overflow_in_use: s.overflow_in_use,
            waiting_threads: s.waiting_threads,
            quarantined: s.quarantined,
            total_acquired: s.total_acquired,
            total_released: s.total_released,
            total_expired: s.total_expired,
            total_timeouts: s.total_timeouts,
            total_reclaimed: s.total_reclaimed,
            total_quarantined: s.total_quarantined,
            total_disposed: s.total_disposed,
        }
    }
}

#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartitionStats {
    #[pyo3(get)]
    pub pool_id: u16,
    #[pyo3(get)]
    pub pool_size: u32,
    #[pyo3(get)]
    pub max_overflow: u32,
    #[pyo3(get)]
    pub active_leases: u32,
    #[pyo3(get)]
    pub quarantined: u32,
    #[pyo3(get)]
    pub total_acquired: u64,
    #[pyo3(get)]
    pub total_released: u64,
    #[pyo3(get)]
    pub total_reclaimed: u64,
    #[pyo3(get)]
    pub total_quarantined: u64,
    #[pyo3(get)]
    pub total_disposed: u64,
}

impl From<RsPartitionStats> for PartitionStats {
    fn from(s: RsPartitionStats) -> Self {
        Self {
            pool_id: s.pool_id,
            pool_size: s.pool_size,
            max_overflow: s.max_overflow,
            active_leases: s.active_leases,
            quarantined: s.quarantined,
            total_acquired: s.total_acquired,
            total_released: s.total_released,
            total_reclaimed: s.total_reclaimed,
            total_quarantined: s.total_quarantined,
            total_disposed: s.total_disposed,
        }
    }
}

#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReclaimedLease {
    #[pyo3(get)]
    pub slot_index: u32,
    #[pyo3(get)]
    pub generation: u64,
    #[pyo3(get)]
    pub pool_id: Option<u16>,
}

impl From<vertebra_core::broker::ReclaimedLease> for ReclaimedLease {
    fn from(r: vertebra_core::broker::ReclaimedLease) -> Self {
        Self {
            slot_index: r.slot_index,
            generation: r.generation,
            pool_id: r.pool_id,
        }
    }
}

#[pymethods]
impl ReclaimedLease {
    fn __repr__(&self) -> String {
        let pool_id = self
            .pool_id
            .map_or_else(|| "None".to_string(), |id| id.to_string());
        format!(
            "ReclaimedLease(slot_index={}, generation={}, pool_id={pool_id})",
            self.slot_index, self.generation
        )
    }
}

fn extract_secret_bytes(secret: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    if let Ok(bytes) = secret.extract::<Vec<u8>>() {
        return Ok(bytes);
    }
    if let Ok(text) = secret.extract::<String>() {
        return Ok(text.into_bytes());
    }
    Err(pyo3::exceptions::PyTypeError::new_err(
        "secret must be bytes or str",
    ))
}

fn lease_quarantined_error(py: Python<'_>, slot_index: u32, generation: u64, msg: String) -> PyErr {
    if let Ok(errors_mod) = py.import("vertebra.extensions.broker.errors")
        && let Ok(exc_cls) = errors_mod.getattr("LeaseQuarantinedError")
        && let Ok(inst) = exc_cls.call1((slot_index, generation, msg.as_str()))
    {
        return PyErr::from_value(inst);
    }
    pyo3::exceptions::PyRuntimeError::new_err(msg)
}

fn attach_err_to_py(py: Python<'_>, err: vertebra_core::broker_attach::AttachError) -> PyErr {
    use vertebra_core::broker_attach::AttachError;
    let msg = err.to_string();
    match err {
        AttachError::AuthFailed(_) | AttachError::InsecurePermissions(_) => {
            if let Ok(errors_mod) = py.import("vertebra.extensions.broker.errors")
                && let Ok(exc_cls) = errors_mod.getattr("BrokerAuthenticationError")
                && let Ok(inst) = exc_cls.call1((msg.as_str(),))
            {
                return PyErr::from_value(inst);
            }
            pyo3::exceptions::PyPermissionError::new_err(msg)
        }
        AttachError::Timeout(_) => pyo3::exceptions::PyTimeoutError::new_err(msg),
        AttachError::Protocol(_) | AttachError::Io(_) | AttachError::UnsupportedPlatform => {
            pyo3::exceptions::PyRuntimeError::new_err(msg)
        }
    }
}

#[pyclass(immutable_type)]
pub struct BrokerAttachServer {
    server: std::sync::Mutex<Option<vertebra_core::broker_attach::BrokerAttachServer>>,
    handle: std::sync::Mutex<Option<vertebra_core::broker_attach::AttachServerHandle>>,
    socket_path: String,
}

#[pymethods]
impl BrokerAttachServer {
    #[new]
    #[pyo3(signature = (socket_path, secret, shm_fd, shm_size = 0))]
    pub fn new(
        py: Python<'_>,
        socket_path: &str,
        secret: &Bound<'_, PyAny>,
        shm_fd: i32,
        shm_size: usize,
    ) -> PyResult<Self> {
        let secret_bytes = extract_secret_bytes(secret)?;
        let path_owned = socket_path.to_string();
        let bound = py
            .detach(|| {
                vertebra_core::broker_attach::BrokerAttachServer::bind(
                    &path_owned,
                    &secret_bytes,
                    shm_fd,
                    shm_size,
                )
            })
            .map_err(|err| attach_err_to_py(py, err))?;
        Ok(Self {
            socket_path: bound.socket_path().to_string_lossy().into_owned(),
            server: std::sync::Mutex::new(Some(bound)),
            handle: std::sync::Mutex::new(None),
        })
    }

    #[pyo3(signature = (timeout_ms = 5000))]
    pub fn serve_one(&self, py: Python<'_>, timeout_ms: u64) -> PyResult<()> {
        let dur = Duration::from_millis(timeout_ms);
        let result = py.detach(|| {
            let guard = self
                .server
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(srv) = guard.as_ref() else {
                return Err(vertebra_core::broker_attach::AttachError::Protocol(
                    "attach server already started in background".to_string(),
                ));
            };
            srv.serve_one(dur)
        });
        result.map_err(|err| attach_err_to_py(py, err))
    }

    pub fn start(&self, py: Python<'_>) -> PyResult<()> {
        let srv = {
            let mut guard = self
                .server
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.take()
        };
        let Some(srv) = srv else {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "attach server already started",
            ));
        };
        let handle = py
            .detach(|| srv.start_background())
            .map_err(|err| attach_err_to_py(py, err))?;
        let mut h_guard = self
            .handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *h_guard = Some(handle);
        Ok(())
    }

    pub fn stop(&self, py: Python<'_>) {
        let handle = {
            let mut h_guard = self
                .handle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            h_guard.take()
        };
        let srv = {
            let mut s_guard = self
                .server
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            s_guard.take()
        };
        let socket_path = self.socket_path.clone();
        py.detach(move || {
            if let Some(h) = handle {
                h.stop();
            }
            drop(srv);
            let _ = std::fs::remove_file(socket_path);
        });
    }

    #[getter]
    pub fn socket_path(&self) -> &str {
        &self.socket_path
    }

    pub fn __repr__(&self) -> String {
        format!(
            "BrokerAttachServer(socket_path={:?}, secret='[REDACTED]')",
            self.socket_path
        )
    }
}

#[pyclass(frozen, immutable_type)]
pub struct BrokerAttachClient;

#[pymethods]
impl BrokerAttachClient {
    #[staticmethod]
    #[pyo3(signature = (socket_path, secret, timeout_ms = 5000))]
    pub fn attach(
        py: Python<'_>,
        socket_path: &str,
        secret: &Bound<'_, PyAny>,
        timeout_ms: u64,
    ) -> PyResult<(i32, usize)> {
        let secret_bytes = extract_secret_bytes(secret)?;
        let path_owned = socket_path.to_string();
        let dur = Duration::from_millis(timeout_ms);
        let result = py.detach(|| {
            vertebra_core::broker_attach::BrokerAttachClient::attach(
                &path_owned,
                &secret_bytes,
                dur,
            )
        });
        match result {
            Ok((owned_fd, shm_size)) => {
                #[cfg(unix)]
                let raw_fd =
                    vertebra_core::broker_attach::BrokerAttachClient::into_raw_fd(owned_fd);
                #[cfg(not(unix))]
                let raw_fd = owned_fd;
                Ok((raw_fd, shm_size))
            }
            Err(err) => Err(attach_err_to_py(py, err)),
        }
    }
}

#[pyclass(immutable_type)]
#[derive(Clone)]
pub struct LeaseBroker {
    table: Arc<ShmLeaseTable>,
}

impl LeaseBroker {
    pub fn table(&self) -> Arc<ShmLeaseTable> {
        Arc::clone(&self.table)
    }
}

#[pymethods]
impl LeaseBroker {
    #[new]
    #[pyo3(signature = (pool_size=20, max_overflow=10, stmt_capacity=512))]
    pub fn new(pool_size: u32, max_overflow: u32, stmt_capacity: u32) -> PyResult<Self> {
        let eff_stmt =
            ShmLeaseTable::validate_capacities(pool_size, max_overflow, stmt_capacity, false)
                .map_err(broker_err_to_py)?;
        let req_size =
            ShmLeaseTable::required_size_with_statements(pool_size, max_overflow, eff_stmt);
        let region = ShmRegion::create(req_size)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(e.to_string()))?;
        let table = ShmLeaseTable::init_in_region_with_statements(
            region,
            pool_size,
            max_overflow,
            eff_stmt,
        )
        .map_err(broker_err_to_py)?;
        Ok(Self {
            table: Arc::new(table),
        })
    }

    #[staticmethod]
    #[pyo3(signature = (name, pool_size=20, max_overflow=10, stmt_capacity=512))]
    pub fn create_named(
        py: Python<'_>,
        name: &str,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> PyResult<Self> {
        #[cfg(unix)]
        {
            let table = py
                .detach(|| {
                    ShmLeaseTable::create_with_statements(
                        name,
                        pool_size,
                        max_overflow,
                        stmt_capacity,
                    )
                })
                .map_err(broker_err_to_py)?
                .0;
            Ok(Self {
                table: Arc::new(table),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, pool_size, max_overflow, stmt_capacity);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    #[pyo3(signature = (name, pool_size=None, max_overflow=None, stmt_capacity=None))]
    pub fn open_named(
        py: Python<'_>,
        name: &str,
        pool_size: Option<u32>,
        max_overflow: Option<u32>,
        stmt_capacity: Option<u32>,
    ) -> PyResult<Self> {
        #[cfg(unix)]
        {
            let table = py
                .detach(|| {
                    ShmLeaseTable::open_with_statements_matching_opt(
                        name,
                        pool_size,
                        max_overflow,
                        stmt_capacity,
                    )
                })
                .map_err(broker_err_to_py)?
                .0;
            Ok(Self {
                table: Arc::new(table),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, pool_size, max_overflow, stmt_capacity);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    #[pyo3(signature = (name, pool_size=20, max_overflow=10, stmt_capacity=512))]
    pub fn open_or_create_named(
        py: Python<'_>,
        name: &str,
        pool_size: u32,
        max_overflow: u32,
        stmt_capacity: u32,
    ) -> PyResult<(Self, bool)> {
        #[cfg(unix)]
        {
            let (table, _, created) = py
                .detach(|| {
                    ShmLeaseTable::open_or_create_with_statements(
                        name,
                        pool_size,
                        max_overflow,
                        stmt_capacity,
                    )
                })
                .map_err(broker_err_to_py)?;
            Ok((
                Self {
                    table: Arc::new(table),
                },
                created,
            ))
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, pool_size, max_overflow, stmt_capacity);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    pub fn probe_owner_pid(name: &str) -> Option<u32> {
        #[cfg(unix)]
        {
            vertebra_core::shm::probe_owner_pid(name)
        }
        #[cfg(not(unix))]
        {
            let _ = name;
            None
        }
    }

    pub fn unlink(&self) -> bool {
        self.table.unlink()
    }

    #[getter]
    pub fn owner_pid(&self) -> Option<u32> {
        self.table.owner_pid()
    }

    #[staticmethod]
    #[pyo3(signature = (fd, size=0))]
    pub fn from_fd(fd: i32, size: usize) -> PyResult<Self> {
        #[cfg(unix)]
        {
            // SAFETY: `libc::dup` accepts a raw integer descriptor and returns `-1` with `errno` if invalid.
            let dup_fd = unsafe { libc::dup(fd) };
            if dup_fd < 0 {
                return Err(pyo3::exceptions::PyOSError::new_err(
                    std::io::Error::last_os_error().to_string(),
                ));
            }
            let effective_size = if size > 0 {
                size
            } else {
                // SAFETY: `libc::stat` is a plain C struct safe to zero-initialize before `fstat`.
                let mut stat: libc::stat = unsafe { std::mem::zeroed() };
                // SAFETY: `dup_fd` is a valid duplicated descriptor (`>= 0`) and `stat` is stack-allocated.
                if unsafe { libc::fstat(dup_fd, &mut stat) } != 0 || stat.st_size <= 0 {
                    // SAFETY: `dup_fd` is an open owned descriptor (`>= 0`) not yet transferred to `ShmRegion`.
                    unsafe {
                        libc::close(dup_fd);
                    }
                    return Err(pyo3::exceptions::PyValueError::new_err(
                        "unable to determine SHM region size from fd",
                    ));
                }
                stat.st_size as usize
            };
            let region = ShmRegion::from_fd(dup_fd, effective_size)
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
            let fd: i32 = fd_val.parse().map_err(|e: std::num::ParseIntError| {
                pyo3::exceptions::PyValueError::new_err(e.to_string())
            })?;
            let size: usize = match std::env::var("VERTEBRA_BROKER_SHM_SIZE") {
                Ok(v) => v.parse().map_err(|e: std::num::ParseIntError| {
                    pyo3::exceptions::PyValueError::new_err(e.to_string())
                })?,
                Err(_) => 0,
            };
            Self::from_fd(fd, size).map(Some)
        }
        #[cfg(not(unix))]
        {
            Ok(None)
        }
    }

    pub fn statement_cache(&self) -> Option<StatementCache> {
        self.table
            .statement_cache()
            .map(|c| StatementCache { cache: Arc::new(c) })
    }

    pub fn active_leases_for_worker(
        &self,
        py: Python<'_>,
        worker_pid: u32,
    ) -> Vec<(u16, u32, u64)> {
        let table = Arc::clone(&self.table);
        py.detach(move || table.active_leases_for_worker(worker_pid))
    }

    #[pyo3(signature = (channel, payload, timestamp_ns=None, lease_id=None))]
    pub fn publish_channel(
        &self,
        channel: &Bound<'_, PyAny>,
        payload: &[u8],
        timestamp_ns: Option<u64>,
        lease_id: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let ch_bytes = extract_bytes_or_str(channel)?;
        let lid_bytes = match lease_id {
            Some(obj) => Some(extract_bytes_or_str(obj)?),
            None => None,
        };
        let ts = timestamp_ns.unwrap_or_else(vertebra_core::broker::monotonic_now_ns);
        Ok(self
            .table
            .publish_channel(&ch_bytes, payload, ts, lid_bytes.as_deref()))
    }

    pub fn drain_channel<'py>(
        &self,
        py: Python<'py>,
        channel: &Bound<'py, PyAny>,
    ) -> PyResult<Vec<PyChannelFrame<'py>>> {
        let ch_bytes = extract_bytes_or_str(channel)?;
        let frames = self.table.drain_channel(&ch_bytes);
        Ok(frames
            .into_iter()
            .map(|f| {
                let ch_str = String::from_utf8_lossy(&f.channel).into_owned();
                let py_payload = PyBytes::new(py, &f.payload);
                let lid_str = f.lease_id.map(|b| String::from_utf8_lossy(&b).into_owned());
                (ch_str, py_payload, f.timestamp_ns, lid_str)
            })
            .collect())
    }

    pub fn drain_all_channels<'py>(&self, py: Python<'py>) -> Vec<PyChannelFrame<'py>> {
        let frames = self.table.drain_all_channels();
        frames
            .into_iter()
            .map(|f| {
                let ch_str = String::from_utf8_lossy(&f.channel).into_owned();
                let py_payload = PyBytes::new(py, &f.payload);
                let lid_str = f.lease_id.map(|b| String::from_utf8_lossy(&b).into_owned());
                (ch_str, py_payload, f.timestamp_ns, lid_str)
            })
            .collect()
    }

    #[pyo3(signature = (worker_pid, ttl_ns=0, timeout_ms=5000, bind_key_hash=0))]
    pub fn acquire(
        &self,
        py: Python<'_>,
        worker_pid: u32,
        ttl_ns: u64,
        timeout_ms: u64,
        bind_key_hash: u64,
    ) -> PyResult<Option<LeaseGrant>> {
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
            Ok(grant) => Ok(Some(LeaseGrant::from(grant))),
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
    ) -> PyResult<Option<LeaseGrant>> {
        match self.table.try_acquire(worker_pid, ttl_ns, bind_key_hash) {
            Ok(grant) => Ok(Some(LeaseGrant::from(grant))),
            Err(BrokerError::PoolExhausted) => Ok(None),
            Err(err) => Err(broker_err_to_py(err)),
        }
    }

    #[pyo3(signature = (pool_key_hash, pool_size, max_overflow=0))]
    pub fn register_partition(
        &self,
        pool_key_hash: u64,
        pool_size: u32,
        max_overflow: u32,
    ) -> PyResult<u16> {
        self.table
            .register_partition(PoolPartitionSpec {
                pool_key_hash,
                pool_size,
                max_overflow,
            })
            .map_err(broker_err_to_py)
    }

    pub fn lookup_partition(&self, pool_key_hash: u64) -> Option<u16> {
        self.table.lookup_partition(pool_key_hash)
    }

    pub fn partition_count(&self) -> u32 {
        self.table.partition_count()
    }

    pub fn wake_seq(&self) -> u32 {
        self.table.wake_seq()
    }

    #[pyo3(signature = (expected_seq, timeout_ms=25))]
    pub fn wait_for_wake_async(
        &self,
        py: Python<'_>,
        expected_seq: u32,
        timeout_ms: u64,
    ) -> PyResult<crate::async_bridge::TaskAwaitable> {
        let table = Arc::clone(&self.table);
        let timeout = Duration::from_millis(timeout_ms);
        crate::async_bridge::spawn_rust_db_task(
            py,
            async move {
                tokio::task::spawn_blocking(move || table.wait_for_wake(expected_seq, timeout))
                    .await
                    .map_err(|err| format!("broker wait_for_wake task failed: {err}"))
            },
            |py, seq| Ok(seq.into_pyobject(py)?.into_any().unbind()),
            |_py, err| PyRuntimeError::new_err(err),
        )
    }

    pub fn partition_stats(&self, pool_key_hash: u64) -> Option<PartitionStats> {
        self.table
            .partition_stats(pool_key_hash)
            .map(PartitionStats::from)
    }

    #[pyo3(signature = (partition, worker_pid, ttl_ns=0, bind_key_hash=0))]
    pub fn try_acquire_in(
        &self,
        partition: u16,
        worker_pid: u32,
        ttl_ns: u64,
        bind_key_hash: u64,
    ) -> PyResult<Option<LeaseGrant>> {
        self.table
            .try_acquire_in(partition, worker_pid, ttl_ns, bind_key_hash)
            .map(|opt| opt.map(LeaseGrant::from))
            .map_err(broker_err_to_py)
    }

    #[pyo3(signature = (partition, worker_pid, ttl_ns=0, timeout_ms=5000, bind_key_hash=0))]
    pub fn acquire_in(
        &self,
        py: Python<'_>,
        partition: u16,
        worker_pid: u32,
        ttl_ns: u64,
        timeout_ms: u64,
        bind_key_hash: u64,
    ) -> PyResult<Option<LeaseGrant>> {
        let table = Arc::clone(&self.table);
        let result = py.detach(move || {
            table.acquire_in(
                partition,
                worker_pid,
                ttl_ns,
                Duration::from_millis(timeout_ms),
                bind_key_hash,
            )
        });
        match result {
            Ok(grant) => Ok(Some(LeaseGrant::from(grant))),
            Err(BrokerError::PoolExhausted) => Ok(None),
            Err(err) => Err(broker_err_to_py(err)),
        }
    }

    pub fn heartbeat(&self, slot_index: u32, generation: u64) -> PyResult<()> {
        self.table
            .heartbeat(slot_index, generation)
            .map_err(broker_err_to_py)
    }

    pub fn release(&self, py: Python<'_>, slot_index: u32, generation: u64) -> PyResult<bool> {
        match self.table.release(slot_index, generation) {
            Ok(()) => Ok(true),
            Err(BrokerError::LeaseNotFound) | Err(BrokerError::GenerationMismatch) => Ok(false),
            Err(BrokerError::InvalidState) => Err(lease_quarantined_error(
                py,
                slot_index,
                generation,
                BrokerError::InvalidState.to_string(),
            )),
            Err(err) => Err(broker_err_to_py(err)),
        }
    }

    pub fn quarantine(&self, slot_index: u32, generation: u64) -> PyResult<()> {
        self.table
            .quarantine(slot_index, generation)
            .map_err(broker_err_to_py)
    }

    pub fn dispose_quarantined(&self, slot_index: u32, generation: u64) -> PyResult<()> {
        self.table
            .dispose_quarantined(slot_index, generation)
            .map_err(broker_err_to_py)
    }

    pub fn quarantined_count(&self) -> u32 {
        self.table.quarantined_count()
    }

    pub fn reap_expired(&self) -> u32 {
        self.table
            .reap_expired(vertebra_core::broker::monotonic_now_ns())
    }

    pub fn reclaim_worker(&self, worker_pid: u32) -> u32 {
        self.table.reclaim_worker(worker_pid)
    }

    pub fn reclaim_worker_leases(&self, py: Python<'_>, worker_pid: u32) -> Vec<ReclaimedLease> {
        py.detach(|| {
            self.table
                .reclaim_worker_leases(worker_pid)
                .into_iter()
                .map(ReclaimedLease::from)
                .collect()
        })
    }

    pub fn stats(&self) -> BrokerStats {
        BrokerStats::from(self.table.stats())
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

    #[getter]
    pub fn stmt_capacity(&self) -> u32 {
        self.table.stmt_capacity()
    }

    #[getter]
    pub fn stmt_offset(&self) -> u64 {
        self.table.stmt_offset()
    }

    #[getter]
    pub fn shm_size(&self) -> usize {
        self.table.region_size()
    }

    #[getter]
    pub fn shm_fd(&self) -> Option<i32> {
        #[cfg(unix)]
        {
            self.table.region_handle()
        }
        #[cfg(not(unix))]
        {
            None
        }
    }
}

// ============================================================================
// Statement Cache PyO3 Bindings (Task 2.1)
// ============================================================================

#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementPlan {
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

impl From<StatementPlanEntry> for StatementPlan {
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

#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatementCacheStats {
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

impl From<RsStatementCacheStats> for StatementCacheStats {
    fn from(s: RsStatementCacheStats) -> Self {
        Self {
            capacity: s.capacity,
            hits: s.hits,
            misses: s.misses,
            inserts: s.inserts,
            evictions: s.evictions,
        }
    }
}

#[pyclass(immutable_type)]
#[derive(Clone)]
pub struct StatementCache {
    cache: Arc<ShmStatementCache>,
}

#[pymethods]
impl StatementCache {
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
    pub fn create_named(py: Python<'_>, name: &str, capacity: u32) -> PyResult<Self> {
        let req_size = ShmStatementCache::required_size(capacity);
        #[cfg(unix)]
        {
            let region = py
                .detach(|| ShmRegion::create_named(name, req_size))
                .map_err(|e| match e {
                    vertebra_core::shm::ShmError::InUse { owner_pid } => {
                        broker_err_to_py(BrokerError::SegmentInUse { owner_pid })
                    }
                    vertebra_core::shm::ShmError::InitTimeout => {
                        broker_err_to_py(BrokerError::InitTimeout)
                    }
                    vertebra_core::shm::ShmError::Sys(ref io_err)
                        if io_err.kind() == std::io::ErrorKind::PermissionDenied =>
                    {
                        broker_err_to_py(BrokerError::PermissionDenied)
                    }
                    other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
                })?;
            let cache =
                ShmStatementCache::init_in_region(region, capacity).map_err(broker_err_to_py)?;
            Ok(Self {
                cache: Arc::new(cache),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, capacity, req_size);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    #[pyo3(signature = (name, capacity=512))]
    pub fn open_named(py: Python<'_>, name: &str, capacity: u32) -> PyResult<Self> {
        let req_size = ShmStatementCache::required_size(capacity);
        #[cfg(unix)]
        {
            let region = py
                .detach(|| ShmRegion::open_named(name, req_size))
                .map_err(|e| match e {
                    vertebra_core::shm::ShmError::InUse { owner_pid } => {
                        broker_err_to_py(BrokerError::SegmentInUse { owner_pid })
                    }
                    vertebra_core::shm::ShmError::Sys(ref io_err)
                        if io_err.kind() == std::io::ErrorKind::NotFound =>
                    {
                        broker_err_to_py(BrokerError::NotFound)
                    }
                    vertebra_core::shm::ShmError::Sys(ref io_err)
                        if io_err.kind() == std::io::ErrorKind::PermissionDenied =>
                    {
                        broker_err_to_py(BrokerError::PermissionDenied)
                    }
                    other => pyo3::exceptions::PyRuntimeError::new_err(other.to_string()),
                })?;
            let cache =
                ShmStatementCache::attach_region(region, capacity).map_err(broker_err_to_py)?;
            Ok(Self {
                cache: Arc::new(cache),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, capacity, req_size);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    #[staticmethod]
    #[pyo3(signature = (name, capacity=512))]
    pub fn open_or_create_named(
        py: Python<'_>,
        name: &str,
        capacity: u32,
    ) -> PyResult<(Self, bool)> {
        #[cfg(unix)]
        {
            let (cache, created) = py
                .detach(|| ShmStatementCache::open_or_create_named(name, capacity))
                .map_err(broker_err_to_py)?;
            Ok((
                Self {
                    cache: Arc::new(cache),
                },
                created,
            ))
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, capacity);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "named SHM regions only supported on Unix",
            ))
        }
    }

    pub fn unlink(&self) -> bool {
        self.cache.unlink()
    }

    #[getter]
    pub fn owner_pid(&self) -> Option<u32> {
        self.cache.owner_pid()
    }

    pub fn lookup(&self, dialect_tag: u8, raw_sql: &[u8]) -> Option<StatementPlan> {
        self.cache
            .lookup(dialect_tag, raw_sql)
            .map(StatementPlan::from)
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

    pub fn stats(&self) -> StatementCacheStats {
        StatementCacheStats::from(self.cache.stats())
    }

    pub fn invalidate_all(&self) {
        self.cache.invalidate_all();
    }

    #[getter]
    pub fn capacity(&self) -> u32 {
        self.cache.capacity()
    }
}

#[pyclass(immutable_type)]
#[derive(Debug, Clone, Copy)]
pub struct ShmCacheStats {
    #[pyo3(get)]
    pub capacity: u32,
    #[pyo3(get)]
    pub max_entries: u32,
    #[pyo3(get)]
    pub capacity_bytes: u64,
    #[pyo3(get)]
    pub hits: u64,
    #[pyo3(get)]
    pub misses: u64,
    #[pyo3(get)]
    pub sets: u64,
    #[pyo3(get)]
    pub evictions: u64,
    #[pyo3(get)]
    pub expirations: u64,
}

impl From<RsShmCacheStats> for ShmCacheStats {
    fn from(s: RsShmCacheStats) -> Self {
        Self {
            capacity: s.capacity,
            max_entries: s.max_entries,
            capacity_bytes: s.capacity_bytes,
            hits: s.hits,
            misses: s.misses,
            sets: s.sets,
            evictions: s.evictions,
            expirations: s.expirations,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ParsedTtl {
    Immortal,
    ExpireNow,
    DurationNs(u64),
}

fn parse_py_ttl(ttl: Option<f64>) -> PyResult<ParsedTtl> {
    match ttl {
        None => Ok(ParsedTtl::Immortal),
        Some(t) if !t.is_finite() => Err(PyValueError::new_err("ttl must be a finite number")),
        Some(t) if t <= 0.0 => Ok(ParsedTtl::ExpireNow),
        Some(t) => {
            let scaled = t * 1_000_000_000.0;
            let ns = if scaled >= (u64::MAX as f64) {
                u64::MAX
            } else {
                (scaled.ceil() as u64).max(1)
            };
            Ok(ParsedTtl::DurationNs(ns))
        }
    }
}

#[pyclass(immutable_type)]
#[derive(Clone)]
pub struct ShmCache {
    cache: Arc<RsShmCache>,
}

#[pymethods]
impl ShmCache {
    #[new]
    #[pyo3(signature = (name, capacity_bytes=1_048_576, max_entries=1024))]
    pub fn new(
        py: Python<'_>,
        name: &str,
        capacity_bytes: usize,
        max_entries: u32,
    ) -> PyResult<Self> {
        #[cfg(unix)]
        {
            let (cache, _created) = py
                .detach(|| RsShmCache::open_or_create_named(name, capacity_bytes, max_entries))
                .map_err(broker_err_to_py)?;
            Ok(Self {
                cache: Arc::new(cache),
            })
        }
        #[cfg(not(unix))]
        {
            let _ = (py, name, capacity_bytes, max_entries);
            Err(pyo3::exceptions::PyNotImplementedError::new_err(
                "ShmCache requires POSIX shared memory",
            ))
        }
    }

    #[pyo3(signature = (key))]
    pub fn get<'py>(
        &self,
        py: Python<'py>,
        key: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let key_bytes = extract_bytes_or_str(key)?;
        let val = py.detach(|| self.cache.get(&key_bytes));
        Ok(val.map(|b| PyBytes::new(py, &b)))
    }

    #[pyo3(signature = (key, value, ttl=None))]
    pub fn set(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        ttl: Option<f64>,
    ) -> PyResult<bool> {
        let key_bytes = extract_bytes_or_str(key)?;
        let val_bytes = extract_bytes_or_str(value)?;
        let parsed_ttl = parse_py_ttl(ttl)?;
        let ok = match parsed_ttl {
            ParsedTtl::Immortal => py.detach(|| self.cache.set(&key_bytes, &val_bytes, None)),
            ParsedTtl::DurationNs(ns) => {
                py.detach(|| self.cache.set(&key_bytes, &val_bytes, Some(ns)))
            }
            ParsedTtl::ExpireNow => {
                if key_bytes.is_empty()
                    || key_bytes.len() > SHM_CACHE_MAX_KEY_BYTES
                    || val_bytes.len() > SHM_CACHE_MAX_VALUE_BYTES
                {
                    false
                } else {
                    py.detach(|| {
                        self.cache.delete(&key_bytes);
                        true
                    })
                }
            }
        };
        Ok(ok)
    }

    #[pyo3(signature = (key))]
    pub fn delete(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        let key_bytes = extract_bytes_or_str(key)?;
        let ok = py.detach(|| self.cache.delete(&key_bytes));
        Ok(ok)
    }

    #[pyo3(signature = (key, value, ttl=None))]
    pub fn add(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        ttl: Option<f64>,
    ) -> PyResult<bool> {
        let key_bytes = extract_bytes_or_str(key)?;
        let val_bytes = extract_bytes_or_str(value)?;
        let parsed_ttl = parse_py_ttl(ttl)?;
        let ok = match parsed_ttl {
            ParsedTtl::Immortal => py.detach(|| self.cache.add(&key_bytes, &val_bytes, None)),
            ParsedTtl::DurationNs(ns) => {
                py.detach(|| self.cache.add(&key_bytes, &val_bytes, Some(ns)))
            }
            ParsedTtl::ExpireNow => {
                if key_bytes.is_empty()
                    || key_bytes.len() > SHM_CACHE_MAX_KEY_BYTES
                    || val_bytes.len() > SHM_CACHE_MAX_VALUE_BYTES
                {
                    false
                } else {
                    py.detach(|| !self.cache.has_key(&key_bytes))
                }
            }
        };
        Ok(ok)
    }

    #[pyo3(signature = (key, delta=1))]
    pub fn incr(&self, py: Python<'_>, key: &Bound<'_, PyAny>, delta: i64) -> PyResult<i64> {
        let key_bytes = extract_bytes_or_str(key)?;
        py.detach(|| self.cache.incr(&key_bytes, delta))
            .map_err(broker_err_to_py)
    }

    #[pyo3(signature = (key, delta=1))]
    pub fn decr(&self, py: Python<'_>, key: &Bound<'_, PyAny>, delta: i64) -> PyResult<i64> {
        let key_bytes = extract_bytes_or_str(key)?;
        py.detach(|| self.cache.decr(&key_bytes, delta))
            .map_err(broker_err_to_py)
    }

    #[pyo3(signature = (key, ttl=None))]
    pub fn touch(
        &self,
        py: Python<'_>,
        key: &Bound<'_, PyAny>,
        ttl: Option<f64>,
    ) -> PyResult<bool> {
        let key_bytes = extract_bytes_or_str(key)?;
        let parsed_ttl = parse_py_ttl(ttl)?;
        let ok = match parsed_ttl {
            ParsedTtl::Immortal => py.detach(|| self.cache.touch(&key_bytes, None)),
            ParsedTtl::DurationNs(ns) => py.detach(|| self.cache.touch(&key_bytes, Some(ns))),
            ParsedTtl::ExpireNow => py.detach(|| self.cache.delete(&key_bytes)),
        };
        Ok(ok)
    }

    #[pyo3(signature = (key))]
    pub fn has_key(&self, py: Python<'_>, key: &Bound<'_, PyAny>) -> PyResult<bool> {
        let key_bytes = extract_bytes_or_str(key)?;
        let ok = py.detach(|| self.cache.has_key(&key_bytes));
        Ok(ok)
    }

    #[pyo3(signature = (prefix))]
    pub fn invalidate_prefix(&self, py: Python<'_>, prefix: &Bound<'_, PyAny>) -> PyResult<u32> {
        let prefix_bytes = extract_bytes_or_str(prefix)?;
        let count = py.detach(|| self.cache.invalidate_prefix(&prefix_bytes));
        Ok(count)
    }

    pub fn clear(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| self.cache.clear());
        Ok(())
    }

    pub fn stats(&self, py: Python<'_>) -> PyResult<ShmCacheStats> {
        let s = py.detach(|| self.cache.stats());
        Ok(s.into())
    }

    pub fn close(&self) -> PyResult<()> {
        Ok(())
    }

    pub fn unlink(&self, py: Python<'_>) -> PyResult<bool> {
        let ok = py.detach(|| self.cache.unlink());
        Ok(ok)
    }

    #[pyo3(signature = (broker))]
    pub fn attach_lease_table(&self, broker: &LeaseBroker) -> PyResult<()> {
        self.cache.attach_lease_table(broker.table());
        Ok(())
    }

    #[pyo3(signature = (max_frames=64))]
    pub fn drain_invalidations<'py>(
        &self,
        py: Python<'py>,
        max_frames: usize,
    ) -> PyResult<Vec<Bound<'py, PyBytes>>> {
        let frames = py.detach(|| self.cache.drain_invalidations(max_frames));
        Ok(frames.into_iter().map(|b| PyBytes::new(py, &b)).collect())
    }

    #[getter]
    pub fn capacity(&self) -> u32 {
        self.cache.capacity()
    }
}

static ENGINE_REGISTRY: OnceLock<RwLock<BTreeMap<u16, ForeignEngineCapsule>>> = OnceLock::new();

fn engine_registry() -> &'static RwLock<BTreeMap<u16, ForeignEngineCapsule>> {
    ENGINE_REGISTRY.get_or_init(|| RwLock::new(BTreeMap::new()))
}

fn lookup_engine_handle(dialect_tag: u16) -> PyResult<ForeignEngineCapsule> {
    let guard = engine_registry()
        .read()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    guard.get(&dialect_tag).cloned().ok_or_else(|| {
        PyRuntimeError::new_err(format!(
            "no broker engine registered for dialect_tag {dialect_tag}"
        ))
    })
}

/// Process-wide registry of imported `BrokerEngineCapsule` vtables keyed by dialect tag.
#[pyclass(module = "vertebra._vertebra", frozen, immutable_type)]
pub struct BrokerEngineRegistry;

#[pymethods]
impl BrokerEngineRegistry {
    #[staticmethod]
    #[pyo3(signature = (capsule))]
    pub fn register(capsule: &Bound<'_, PyAny>) -> PyResult<u16> {
        let cap = capsule.cast::<PyCapsule>().map_err(|_| {
            PyTypeError::new_err("capsule must be a vertebra.BrokerEngineCapsule PyCapsule")
        })?;
        let ptr = cap
            .pointer_checked(Some(BROKER_ENGINE_CAPSULE_NAME))
            .map_err(|_| {
                PyTypeError::new_err(
                    "PyCapsule name mismatch; expected vertebra.BrokerEngineCapsule",
                )
            })?
            .as_ptr()
            .cast::<BrokerEngineCapsule>();
        // SAFETY: `pointer_checked` verified `BROKER_ENGINE_CAPSULE_NAME` on a live `PyCapsule`
        // holding a `BrokerEngineCapsule` value initialized by `new_broker_engine_pycapsule`.
        let handle = unsafe { ForeignEngineCapsule::from_capsule_ptr(ptr) }
            .map_err(PyRuntimeError::new_err)?;
        let tag = handle.dialect_tag();
        let replaced = {
            let mut guard = engine_registry()
                .write()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.insert(tag, handle)
        };
        drop(replaced);
        Ok(tag)
    }

    #[staticmethod]
    pub fn registered_tags() -> Vec<u16> {
        let guard = engine_registry()
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.keys().copied().collect()
    }

    #[staticmethod]
    #[pyo3(signature = (dialect_tag, sql))]
    pub fn normalize(py: Python<'_>, dialect_tag: u16, sql: &str) -> PyResult<(String, usize)> {
        let handle = lookup_engine_handle(dialect_tag)?;
        py.detach(|| handle.normalize(sql))
            .map_err(PyRuntimeError::new_err)
    }

    #[staticmethod]
    #[pyo3(signature = (dialect_tag, dsn, max_size))]
    pub fn open_pool(py: Python<'_>, dialect_tag: u16, dsn: &str, max_size: u32) -> PyResult<u64> {
        let handle = lookup_engine_handle(dialect_tag)?;
        py.detach(|| handle.open_pool(dsn, max_size))
            .map_err(PyRuntimeError::new_err)
    }

    #[staticmethod]
    #[pyo3(signature = (dialect_tag, pool_id, frame))]
    pub fn execute<'py>(
        py: Python<'py>,
        dialect_tag: u16,
        pool_id: u64,
        frame: &[u8],
    ) -> PyResult<Bound<'py, PyBytes>> {
        let handle = lookup_engine_handle(dialect_tag)?;
        let response = py
            .detach(|| handle.execute(pool_id, frame))
            .map_err(PyRuntimeError::new_err)?;
        Ok(PyBytes::new(py, &response))
    }

    #[staticmethod]
    #[pyo3(signature = (dialect_tag, pool_id))]
    pub fn close_pool(py: Python<'_>, dialect_tag: u16, pool_id: u64) -> PyResult<()> {
        let handle = lookup_engine_handle(dialect_tag)?;
        py.detach(|| handle.close_pool(pool_id));
        Ok(())
    }
}
