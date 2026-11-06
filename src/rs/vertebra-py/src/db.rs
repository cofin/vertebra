//! PyO3 bindings for `vertebra-db` cloud dialers, wire codecs, and shared-memory Arrow IPC cursors.

use std::collections::HashMap;
use std::ffi::{c_int, c_void};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use pyo3::buffer::PyBuffer;
use pyo3::exceptions::{PyBufferError, PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use vertebra_core::shm::{ShmCapsule, ShmRegion};
use vertebra_db::arrow_shm::{
    ShmArrowFrameHeader, open_shm_arrow_region, read_shm_arrow_frame_with, release_shm_arrow_frame,
    write_shm_arrow_region, write_shm_arrow_region_unregistered,
};
use vertebra_db::dialer::{
    AlloyDbDialer as RsAlloyDbDialer, AuthType, CloudSqlDialer as RsCloudSqlDialer, IpType,
    decode_alloydb_metadata_response as rs_decode_alloydb_metadata_response,
    encode_alloydb_metadata_exchange as rs_encode_alloydb_metadata_exchange,
};
use vertebra_db::duckdb::DuckdbConfig;
use vertebra_db::mssql::{
    encode_tds_sql_batch as rs_encode_tds_sql_batch,
    rewrite_mssql_placeholders as rs_rewrite_mssql_placeholders,
};
use vertebra_db::oracle::{
    OracleSessionAttrs, decode_oracle_vector_binary as rs_decode_oracle_vector_binary,
    decode_oracle_vector_f32 as rs_decode_oracle_vector_f32,
    decode_oracle_vector_f64 as rs_decode_oracle_vector_f64,
    decode_oracle_vector_i8 as rs_decode_oracle_vector_i8,
    decode_oson_payload as rs_decode_oson_payload,
    encode_oracle_vector_binary as rs_encode_oracle_vector_binary,
    encode_oracle_vector_f32 as rs_encode_oracle_vector_f32,
    encode_oracle_vector_f64 as rs_encode_oracle_vector_f64,
    encode_oracle_vector_i8 as rs_encode_oracle_vector_i8,
    encode_oson_payload as rs_encode_oson_payload,
};
use vertebra_db::postgres::{
    decode_pgcopy_binary_rows, decode_pgvector_bit as rs_decode_pgvector_bit,
    decode_pgvector_f16 as rs_decode_pgvector_f16, decode_pgvector_f32 as rs_decode_pgvector_f32,
    encode_pgcopy_binary_rows, encode_pgvector_bit as rs_encode_pgvector_bit,
    encode_pgvector_f16 as rs_encode_pgvector_f16, encode_pgvector_f32 as rs_encode_pgvector_f32,
    strip_pg_jsonb_header as rs_strip_pg_jsonb_header,
};
use vertebra_db::py_async::{SHM_CAPSULE_NAME, wrap_shm_region_capsule};

use crate::vector::VectorView;
use vertebra_db::oracle::tns::{
    parse_tnsnames_ora as rs_parse_tnsnames_ora, resolve_tns_alias as rs_resolve_tns_alias,
};
use vertebra_db::resp3::{
    crc16_xmodem as rs_crc16_xmodem, encode_hello3_handshake as rs_encode_hello3_handshake,
    encode_resp_command as rs_encode_resp_command,
    parse_cluster_redirect as rs_parse_cluster_redirect, redis_hash_slot as rs_redis_hash_slot,
};
use vertebra_db::sqlite::SqliteConfig;

/// Zero-sidecar Google Cloud SQL (`:3307`) mTLS dialer.
#[pyclass(name = "CloudSQLDialer", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct CloudSQLDialer {
    inner: RsCloudSqlDialer,
}

#[pymethods]
impl CloudSQLDialer {
    #[new]
    #[pyo3(signature = (instance_uri, ip_type="PUBLIC", iam_auth=true))]
    pub fn new(instance_uri: &str, ip_type: &str, iam_auth: bool) -> PyResult<Self> {
        let parsed_ip = IpType::parse(ip_type).map_err(PyValueError::new_err)?;
        let auth = if iam_auth {
            AuthType::Iam
        } else {
            AuthType::Password
        };
        let inner =
            RsCloudSqlDialer::new(instance_uri, parsed_ip, auth).map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn admin_api_path(&self) -> String {
        self.inner.admin_api_path()
    }

    #[getter]
    pub fn server_port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn project(&self) -> String {
        self.inner.uri.project.clone()
    }

    #[getter]
    pub fn region(&self) -> String {
        self.inner.uri.region.clone()
    }

    #[getter]
    pub fn instance(&self) -> String {
        self.inner.uri.instance.clone()
    }

    #[getter]
    pub fn expected_server_cn(&self) -> String {
        self.inner.uri.expected_server_cn()
    }

    pub fn generate_ephemeral_keypair(&self, py: Python<'_>) -> PyResult<(String, String)> {
        let dialer = self.inner.clone();
        py.detach(move || dialer.generate_ephemeral_keypair())
            .map_err(pyo3::exceptions::PyRuntimeError::new_err)
    }
}

/// Zero-sidecar Google Cloud AlloyDB (`:5433`) mTLS and `MetadataExchangeRequest` dialer.
#[pyclass(name = "AlloyDBDialer", frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct AlloyDBDialer {
    inner: RsAlloyDbDialer,
}

#[pymethods]
impl AlloyDBDialer {
    #[new]
    #[pyo3(signature = (cluster_uri, ip_type="PRIVATE", iam_auth=true))]
    pub fn new(cluster_uri: &str, ip_type: &str, iam_auth: bool) -> PyResult<Self> {
        let parsed_ip = IpType::parse(ip_type).map_err(PyValueError::new_err)?;
        let auth = if iam_auth {
            AuthType::Iam
        } else {
            AuthType::Password
        };
        let inner =
            RsAlloyDbDialer::new(cluster_uri, parsed_ip, auth).map_err(PyValueError::new_err)?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn server_port(&self) -> u16 {
        self.inner.server_port
    }

    #[getter]
    pub fn admin_api_path(&self) -> String {
        self.inner.uri.admin_api_path()
    }

    #[getter]
    pub fn project(&self) -> String {
        self.inner.uri.project.clone()
    }

    #[getter]
    pub fn location(&self) -> String {
        self.inner.uri.location.clone()
    }

    #[getter]
    pub fn cluster(&self) -> String {
        self.inner.uri.cluster.clone()
    }

    #[getter]
    pub fn instance(&self) -> String {
        self.inner.uri.instance.clone()
    }

    #[pyo3(signature = (user_agent="vertebra-db/0.1", token=""))]
    pub fn encode_metadata_exchange<'py>(
        &self,
        py: Python<'py>,
        user_agent: &str,
        token: &str,
    ) -> PyResult<Bound<'py, PyBytes>> {
        let bytes = self.inner.encode_metadata_exchange(user_agent, token);
        PyBytes::new_with(py, bytes.len(), |buf| {
            buf.copy_from_slice(&bytes);
            Ok(())
        })
    }
}

const MAX_ARROW_CURSOR_BYTE_LEN: usize = (isize::MAX as usize) - ShmArrowFrameHeader::SIZE;

#[inline]
fn checked_arrow_frame_end(byte_len: usize) -> Option<usize> {
    if byte_len > MAX_ARROW_CURSOR_BYTE_LEN {
        return None;
    }
    ShmArrowFrameHeader::SIZE.checked_add(byte_len)
}

#[inline]
fn normalize_optional_shm_name(shm_name: Option<String>) -> String {
    match shm_name {
        Some(name) if !name.is_empty() => {
            if name.starts_with('/') {
                name
            } else {
                format!("/{name}")
            }
        }
        _ => String::new(),
    }
}

#[derive(Debug)]
struct CapsuleBacking {
    ptr: *mut u8,
    len: usize,
    ctx: *mut c_void,
    drop_fn: unsafe extern "C" fn(*mut c_void),
    unlink_fn: Option<unsafe extern "C" fn(*mut c_void) -> bool>,
    unlinked: AtomicBool,
}

// SAFETY: `CapsuleBacking` exclusively owns a thread-safe `ShmCapsule` context (`Arc<ShmRegion>`)
// claimed via atomic `AcqRel` swap, and releases it once in `Drop`.
unsafe impl Send for CapsuleBacking {}
// SAFETY: `ptr` references immutable published `VTBARROW` shared memory, `unlink_now` synchronizes
// via `AtomicBool` and `ShmRegion`'s internal `Mutex`, and `ctx` is only released in `Drop`.
unsafe impl Sync for CapsuleBacking {}

impl CapsuleBacking {
    fn unlink_now(&self) -> bool {
        if self.ctx.is_null() || self.unlinked.swap(true, Ordering::AcqRel) {
            return false;
        }
        let Some(unlink_fn) = self.unlink_fn else {
            return false;
        };
        // SAFETY: `self.ctx` remains live for the lifetime of `self` because `drop_fn` is only
        // invoked in `CapsuleBacking::drop` when the last `Arc<CapsuleBacking>` reference drops.
        unsafe { unlink_fn(self.ctx) }
    }
}

impl Drop for CapsuleBacking {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            // SAFETY: `CapsuleBacking` exclusively owns `self.ctx` after `_from_shm_capsule`
            // atomically clears the `PyCapsule` slot, and drops it once when the last `Arc` drops.
            unsafe {
                (self.drop_fn)(self.ctx);
            }
        }
    }
}

#[derive(Clone, Debug)]
enum ArrowBacking {
    Region(Arc<ShmRegion>),
    Capsule(Arc<CapsuleBacking>),
}

impl ArrowBacking {
    #[inline]
    fn as_ptr_and_len(&self) -> (*mut u8, usize) {
        match self {
            Self::Region(r) => (r.as_ptr(), r.len()),
            Self::Capsule(c) => (c.ptr, c.len),
        }
    }
}

/// Shared-memory Arrow IPC cursor backed by `vertebra_core::shm::ShmRegion`.
#[pyclass(frozen, immutable_type, from_py_object)]
#[derive(Clone, Debug)]
pub struct ArrowCursor {
    #[pyo3(get)]
    pub shm_name: String,
    #[pyo3(get)]
    pub byte_len: usize,
    #[pyo3(get)]
    pub row_count: usize,
    backing: Option<ArrowBacking>,
}

impl ArrowCursor {
    pub fn from_shm_region(
        shm_name: String,
        byte_len: usize,
        row_count: usize,
        region: Arc<ShmRegion>,
    ) -> Self {
        Self {
            shm_name,
            byte_len,
            row_count,
            backing: Some(ArrowBacking::Region(region)),
        }
    }
}

#[pymethods]
impl ArrowCursor {
    #[new]
    #[pyo3(signature = (shm_name, byte_len, row_count=0))]
    pub fn new(shm_name: String, byte_len: usize, row_count: usize) -> PyResult<Self> {
        let required = checked_arrow_frame_end(byte_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;
        let norm_name = normalize_optional_shm_name(Some(shm_name));
        let backing = if norm_name.is_empty() {
            None
        } else {
            open_shm_arrow_region(&norm_name)
                .ok()
                .filter(|(r, _)| r.len() >= required)
                .map(|(r, _)| ArrowBacking::Region(r))
        };
        Ok(Self {
            shm_name: norm_name,
            byte_len,
            row_count,
            backing,
        })
    }

    #[staticmethod]
    #[pyo3(signature = (shm_name, payload, row_count=0))]
    pub fn write_ipc_to_shm(
        py: Python<'_>,
        shm_name: &str,
        payload: &[u8],
        row_count: usize,
    ) -> PyResult<Self> {
        let (norm_name, len, region) = py
            .detach(|| write_shm_arrow_region(shm_name, payload))
            .map_err(PyRuntimeError::new_err)?;
        Ok(Self {
            shm_name: norm_name,
            byte_len: len,
            row_count,
            backing: Some(ArrowBacking::Region(region)),
        })
    }

    #[staticmethod]
    pub fn _allocate_shm_capsule<'py>(
        py: Python<'py>,
        shm_name: &str,
        payload: &[u8],
    ) -> PyResult<(String, usize, Bound<'py, PyAny>)> {
        let (norm_name, len, region) = py
            .detach(|| write_shm_arrow_region_unregistered(shm_name, payload))
            .map_err(PyRuntimeError::new_err)?;
        let capsule = wrap_shm_region_capsule(py, region)?;
        Ok((norm_name, len, capsule))
    }

    #[staticmethod]
    #[pyo3(signature = (shm_name, byte_len, row_count, capsule))]
    pub fn _from_shm_capsule(
        py: Python<'_>,
        shm_name: Option<String>,
        byte_len: usize,
        row_count: usize,
        capsule: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        if byte_len > MAX_ARROW_CURSOR_BYTE_LEN {
            return Err(PyValueError::new_err("ArrowCursor byte_len overflow"));
        }
        let required = ShmArrowFrameHeader::SIZE
            .checked_add(byte_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;

        // SAFETY: `capsule.as_ptr()` is a valid borrowed `PyObject*` and `SHM_CAPSULE_NAME` is a
        // static NUL-terminated C string; CPython checks exact `PyCapsule` type and name.
        let raw = unsafe { ffi::PyCapsule_GetPointer(capsule.as_ptr(), SHM_CAPSULE_NAME.as_ptr()) }
            .cast::<ShmCapsule>();
        if raw.is_null() {
            return Err(PyErr::fetch(py));
        }

        // SAFETY: `raw` points to a valid `#[repr(C)]` `ShmCapsule` kept alive by `capsule`;
        // shared immutable access avoids aliasing UB under concurrent free-threaded calls.
        let cap_ref: &ShmCapsule = unsafe { &*(raw as *const ShmCapsule) };
        if cap_ref.ptr.is_null() {
            return Err(PyValueError::new_err("ShmCapsule mapping pointer is null"));
        }
        if cap_ref.len < required {
            return Err(PyValueError::new_err(
                "ShmCapsule mapping smaller than declared VTBARROW frame length",
            ));
        }

        let ctx = cap_ref.ctx.swap(core::ptr::null_mut(), Ordering::AcqRel);
        if ctx.is_null() {
            return Err(PyValueError::new_err("ShmCapsule already consumed"));
        }

        let norm_name = normalize_optional_shm_name(shm_name);
        Ok(Self {
            shm_name: norm_name,
            byte_len,
            row_count,
            backing: Some(ArrowBacking::Capsule(Arc::new(CapsuleBacking {
                ptr: cap_ref.ptr,
                len: cap_ref.len,
                ctx,
                drop_fn: cap_ref.drop_fn,
                unlink_fn: cap_ref.unlink_fn,
                unlinked: AtomicBool::new(false),
            }))),
        })
    }

    #[staticmethod]
    pub fn read_ipc_from_shm<'py>(
        py: Python<'py>,
        shm_name: &str,
    ) -> PyResult<Bound<'py, PyBytes>> {
        read_shm_arrow_frame_with(shm_name, |slice| {
            PyBytes::new_with(py, slice.len(), |buf| {
                buf.copy_from_slice(slice);
                Ok(())
            })
        })
        .map_err(PyRuntimeError::new_err)?
    }

    pub fn read_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyBytes>> {
        let end = checked_arrow_frame_end(self.byte_len)
            .ok_or_else(|| PyValueError::new_err("ArrowCursor byte_len overflow"))?;
        if let Some(ref backing) = self.backing {
            let (ptr, len) = backing.as_ptr_and_len();
            if !ptr.is_null() && len >= end {
                // SAFETY: `ptr` is non-null, `end <= len`, `end <= isize::MAX`, and the mapping
                // is immutable and anchored for the lifetime of `self.backing`.
                let slice = unsafe { core::slice::from_raw_parts(ptr, end) };
                let payload = &slice[ShmArrowFrameHeader::SIZE..end];
                return PyBytes::new_with(py, payload.len(), |buf| {
                    buf.copy_from_slice(payload);
                    Ok(())
                });
            }
        }
        Self::read_ipc_from_shm(py, &self.shm_name)
    }

    pub fn buffer_view<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        // SAFETY: `slf.as_ptr()` is a valid borrowed `PyObject*` implementing the buffer protocol.
        let ptr = unsafe { ffi::PyMemoryView_FromObject(slf.as_ptr()) };
        if ptr.is_null() {
            return Err(PyErr::fetch(py));
        }
        // SAFETY: `ptr` is a non-null owned `PyObject*` returned by `PyMemoryView_FromObject`.
        Ok(unsafe { Bound::from_owned_ptr(py, ptr) })
    }

    pub fn to_pyarrow<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let pa = py.import("pyarrow")?;
        let mv = Self::buffer_view(slf)?;
        let pa_buf = pa.call_method1("py_buffer", (mv,))?;
        let reader = pa.getattr("ipc")?.call_method1("open_stream", (pa_buf,))?;
        reader.call_method0("read_all")
    }

    pub fn to_arrow_table<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        Self::to_pyarrow(slf)
    }

    pub fn to_polars<'py>(slf: Bound<'py, Self>) -> PyResult<Bound<'py, PyAny>> {
        let py = slf.py();
        let pl = py.import("polars")?;
        let tbl = Self::to_pyarrow(slf)?;
        pl.call_method1("from_arrow", (tbl,))
    }

    pub fn release(&self, py: Python<'_>) -> bool {
        let name = self.shm_name.clone();
        let backing = self.backing.clone();
        py.detach(move || {
            let removed = release_shm_arrow_frame(&name);
            match backing {
                Some(ArrowBacking::Region(region)) => {
                    #[cfg(unix)]
                    let unlinked = region.unlink_now();
                    #[cfg(not(unix))]
                    let unlinked = {
                        let _ = region;
                        false
                    };
                    removed || unlinked
                }
                Some(ArrowBacking::Capsule(cap)) => {
                    let unlinked = cap.unlink_now();
                    removed || unlinked
                }
                None => removed,
            }
        })
    }

    pub fn close(&self, py: Python<'_>) -> bool {
        self.release(py)
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_val=None, exc_tb=None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_val: Option<&Bound<'_, PyAny>>,
        exc_tb: Option<&Bound<'_, PyAny>>,
    ) {
        let _ = (exc_type, exc_val, exc_tb);
        let _ = self.release(py);
    }

    unsafe fn __getbuffer__(
        slf: Bound<'_, Self>,
        view: *mut ffi::Py_buffer,
        flags: c_int,
    ) -> PyResult<()> {
        let py = slf.py();
        if view.is_null() {
            return Err(PyBufferError::new_err("Py_buffer pointer is null"));
        }
        if (flags & ffi::PyBUF_WRITABLE) == ffi::PyBUF_WRITABLE {
            return Err(PyBufferError::new_err("ArrowCursor buffer is read-only"));
        }
        let this = slf.get();
        let Some(ref backing) = this.backing else {
            return Err(PyBufferError::new_err(
                "ArrowCursor has no anchored ShmRegion",
            ));
        };
        let Some(end) = checked_arrow_frame_end(this.byte_len) else {
            return Err(PyBufferError::new_err("ArrowCursor byte_len overflow"));
        };
        let (base_ptr, region_len) = backing.as_ptr_and_len();
        if base_ptr.is_null() || region_len < end {
            return Err(PyBufferError::new_err(
                "ArrowCursor ShmRegion smaller than declared byte_len",
            ));
        }
        // SAFETY: `base_ptr` is non-null with `region_len >= end = 16 + byte_len` and
        // `end <= isize::MAX as usize`, `view` is a valid non-null `Py_buffer*`, and `slf.as_ptr()`
        // is a borrowed reference whose refcount `PyBuffer_FillInfo` increments to keep
        // `this.backing` alive until the exported buffer is released.
        let ret = unsafe {
            let ptr = base_ptr.add(ShmArrowFrameHeader::SIZE).cast::<c_void>();
            ffi::PyBuffer_FillInfo(
                view,
                slf.as_ptr(),
                ptr,
                this.byte_len as ffi::Py_ssize_t,
                1,
                flags,
            )
        };
        if ret == -1 {
            return Err(PyErr::fetch(py));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

/// Encode a 4-byte length-prefixed AlloyDB `MetadataExchangeRequest` protobuf wire frame.
#[pyfunction]
#[pyo3(signature = (user_agent="vertebra-db/0.1", oauth2_token="", iam_auth=true))]
pub fn encode_alloydb_metadata_exchange<'py>(
    py: Python<'py>,
    user_agent: &str,
    oauth2_token: &str,
    iam_auth: bool,
) -> PyResult<Bound<'py, PyBytes>> {
    let auth = if iam_auth {
        AuthType::Iam
    } else {
        AuthType::Password
    };
    let bytes = rs_encode_alloydb_metadata_exchange(user_agent, oauth2_token, auth);
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode a 4-byte length-prefixed AlloyDB `MetadataExchangeResponse` protobuf wire frame.
#[pyfunction]
pub fn decode_alloydb_metadata_response(data: &[u8]) -> PyResult<(u32, String)> {
    rs_decode_alloydb_metadata_response(data).map_err(PyValueError::new_err)
}

/// Strip the 1-byte PostgreSQL binary `JSONB` version header (`0x01`).
#[pyfunction]
pub fn strip_pg_jsonb_header<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let stripped = rs_strip_pg_jsonb_header(data).map_err(PyValueError::new_err)?;
    PyBytes::new_with(py, stripped.len(), |buf| {
        buf.copy_from_slice(stripped);
        Ok(())
    })
}

/// Prepend the 1-byte PostgreSQL binary `JSONB` version header (`0x01`).
#[pyfunction]
pub fn encode_pg_jsonb<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    PyBytes::new_with(py, 1 + data.len(), |buf| {
        buf[0] = 0x01;
        buf[1..].copy_from_slice(data);
        Ok(())
    })
}

/// Type alias for a batch of binary PostgreSQL `COPY` rows.
pub type PgCopyRowBatch = Vec<Vec<Option<Vec<u8>>>>;

/// Type alias for a resolved Oracle TNS alias tuple `(host, port, service_name, sid, security_cert_dn)`.
pub type ResolvedTnsAlias = (String, u16, Option<String>, Option<String>, Option<String>);

/// Encode rows into a PostgreSQL binary `COPY` stream (`PGCOPY\n\xff\r\n\0`).
#[pyfunction]
pub fn encode_pgcopy_rows<'py>(
    py: Python<'py>,
    rows: PgCopyRowBatch,
) -> PyResult<Bound<'py, PyBytes>> {
    let encoded = py.detach(move || encode_pgcopy_binary_rows(&rows));
    PyBytes::new_with(py, encoded.len(), |buf| {
        buf.copy_from_slice(&encoded);
        Ok(())
    })
}

/// Decode a PostgreSQL binary `COPY` stream (`PGCOPY\n\xff\r\n\0`) into rows of optional byte slices.
#[pyfunction]
pub fn decode_pgcopy_rows(py: Python<'_>, data: &[u8]) -> PyResult<PgCopyRowBatch> {
    py.detach(|| decode_pgcopy_binary_rows(data))
        .map_err(PyValueError::new_err)
}

/// Parse an Oracle `tnsnames.ora` file into a dictionary of uppercase alias -> descriptor string.
#[pyfunction]
pub fn parse_tnsnames_ora(py: Python<'_>, contents: &str) -> HashMap<String, String> {
    py.detach(|| rs_parse_tnsnames_ora(contents))
}

/// Resolve a TNS alias from `tnsnames.ora` into `(host, port, service_name, sid, security_cert_dn)`.
#[pyfunction]
pub fn resolve_tns_alias(py: Python<'_>, contents: &str, alias: &str) -> Option<ResolvedTnsAlias> {
    py.detach(|| rs_resolve_tns_alias(contents, alias))
        .map(|d| (d.host, d.port, d.service_name, d.sid, d.security_cert_dn))
}

fn with_f32_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[f32]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        let slice = view_ref.row_f32()?;
        return f(slice).map_err(PyValueError::new_err);
    }
    if let Ok(buf) = PyBuffer::<f32>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const f32;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `f32` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<f32> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

fn with_f64_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[f64]) -> Result<R, String>,
{
    if let Ok(buf) = PyBuffer::<f64>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const f64;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `f64` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<f64> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

fn with_i8_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[i8]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        let slice = view_ref.row_i8()?;
        return f(slice).map_err(PyValueError::new_err);
    }
    if let Ok(buf) = PyBuffer::<i8>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const i8;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `i8` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<i8> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

fn with_u8_slice_or_vec<R, F>(obj: &Bound<'_, PyAny>, f: F) -> PyResult<R>
where
    F: FnOnce(&[u8]) -> Result<R, String>,
{
    if let Ok(view_ref) = obj.extract::<PyRef<'_, VectorView>>() {
        let slice = view_ref.row_bytes()?;
        return f(slice).map_err(PyValueError::new_err);
    }
    if let Ok(bytes) = obj.extract::<&[u8]>() {
        return f(bytes).map_err(PyValueError::new_err);
    }
    if let Ok(buf) = PyBuffer::<u8>::get(obj)
        && buf.is_c_contiguous()
    {
        let len = buf.item_count();
        let ptr = buf.buf_ptr() as *const u8;
        let slice = if len == 0 || ptr.is_null() {
            &[]
        } else {
            // SAFETY: `buf` pins a contiguous `u8` PEP 3118 buffer while the GIL is held in `f`.
            unsafe { std::slice::from_raw_parts(ptr, len) }
        };
        return f(slice).map_err(PyValueError::new_err);
    }
    let vec: Vec<u8> = obj.extract()?;
    f(&vec).map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
#[pyfunction]
pub fn encode_oracle_vector_f32<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f32_slice_or_vec(values, |slice| Ok(rs_encode_oracle_vector_f32(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, FLOAT32)` binary frame.
#[pyfunction]
pub fn decode_oracle_vector_f32(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f32>> {
    py.detach(|| rs_decode_oracle_vector_f32(data))
        .map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
#[pyfunction]
pub fn encode_oracle_vector_f64<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f64_slice_or_vec(values, |slice| Ok(rs_encode_oracle_vector_f64(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, FLOAT64)` binary frame.
#[pyfunction]
pub fn decode_oracle_vector_f64(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f64>> {
    py.detach(|| rs_decode_oracle_vector_f64(data))
        .map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
#[pyfunction]
pub fn encode_oracle_vector_i8<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_i8_slice_or_vec(values, |slice| Ok(rs_encode_oracle_vector_i8(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, INT8)` binary frame.
#[pyfunction]
pub fn decode_oracle_vector_i8(py: Python<'_>, data: &[u8]) -> PyResult<Vec<i8>> {
    py.detach(|| rs_decode_oracle_vector_i8(data))
        .map_err(PyValueError::new_err)
}

/// Encode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame.
#[pyfunction]
pub fn encode_oracle_vector_binary<'py>(
    py: Python<'py>,
    bits: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_u8_slice_or_vec(bits, |slice| Ok(rs_encode_oracle_vector_binary(slice)))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode an Oracle 26ai `VECTOR(*, BINARY)` packed bit-vector binary frame.
#[pyfunction]
pub fn decode_oracle_vector_binary(py: Python<'_>, data: &[u8]) -> PyResult<Vec<u8>> {
    py.detach(|| rs_decode_oracle_vector_binary(data))
        .map_err(PyValueError::new_err)
}

/// Encode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
#[pyfunction]
pub fn encode_pgvector_f32<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f32_slice_or_vec(values, rs_encode_pgvector_f32)?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode a `pgvector` binary `vector` (`[u16 dim BE][u16 unused=0 BE][f32 BE; dim]`).
#[pyfunction]
pub fn decode_pgvector_f32(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f32>> {
    py.detach(|| rs_decode_pgvector_f32(data))
        .map_err(PyValueError::new_err)
}

/// Encode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
#[pyfunction]
pub fn encode_pgvector_f16<'py>(
    py: Python<'py>,
    values: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_f32_slice_or_vec(values, rs_encode_pgvector_f16)?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode a `pgvector` binary `halfvec` (`[u16 dim BE][u16 unused=0 BE][f16 BE; dim]`).
#[pyfunction]
pub fn decode_pgvector_f16(py: Python<'_>, data: &[u8]) -> PyResult<Vec<f32>> {
    py.detach(|| rs_decode_pgvector_f16(data))
        .map_err(PyValueError::new_err)
}

/// Encode a PostgreSQL / `pgvector` binary `bit` (`VARBIT`).
#[pyfunction]
pub fn encode_pgvector_bit<'py>(
    py: Python<'py>,
    packed_bytes: &Bound<'py, PyAny>,
    bit_len: usize,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = with_u8_slice_or_vec(packed_bytes, |slice| rs_encode_pgvector_bit(slice, bit_len))?;
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Decode a PostgreSQL / `pgvector` binary `bit` (`VARBIT`) into `(packed_bytes, bit_len)`.
#[pyfunction]
pub fn decode_pgvector_bit<'py>(
    py: Python<'py>,
    data: &[u8],
) -> PyResult<(Bound<'py, PyBytes>, usize)> {
    let (raw, bit_len) = py
        .detach(|| rs_decode_pgvector_bit(data))
        .map_err(PyValueError::new_err)?;
    let py_bytes = PyBytes::new_with(py, raw.len(), |buf| {
        buf.copy_from_slice(&raw);
        Ok(())
    })?;
    Ok((py_bytes, bit_len))
}

/// Encode JSON bytes with an Oracle 26ai `OSON` binary header (`\xff\x4a\x5a`).
#[pyfunction]
pub fn encode_oson_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(|| rs_encode_oson_payload(data));
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Validate an Oracle 26ai `OSON` binary header and return the underlying JSON payload bytes.
#[pyfunction]
pub fn decode_oson_payload<'py>(py: Python<'py>, data: &[u8]) -> PyResult<Bound<'py, PyBytes>> {
    let slice = rs_decode_oson_payload(data).map_err(PyValueError::new_err)?;
    PyBytes::new_with(py, slice.len(), |buf| {
        buf.copy_from_slice(slice);
        Ok(())
    })
}

/// Encode zero-RTT piggybacked Oracle TTC session attributes (`CLIENT_IDENTIFIER`, `MODULE`, `ACTION`, `ECID`).
#[pyfunction]
#[pyo3(signature = (client_id=None, module=None, action=None, ecid=None))]
pub fn encode_ttc_session_attrs<'py>(
    py: Python<'py>,
    client_id: Option<String>,
    module: Option<String>,
    action: Option<String>,
    ecid: Option<String>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(move || {
        let attrs = OracleSessionAttrs {
            client_identifier: client_id,
            module,
            action,
            ecid,
        };
        attrs.encode_ttc_piggyback_session_attrs()
    });
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Rewrite `?` and `$1`/`$2` placeholders to SQL Server `@P1, @P2, ...` while preserving literals and comments.
#[pyfunction]
pub fn rewrite_mssql_placeholders(py: Python<'_>, sql: &str) -> (String, usize) {
    py.detach(|| rs_rewrite_mssql_placeholders(sql))
}

/// Encode a TDS 7.4 `SQLBatch` packet with `ALL_HEADERS` `TransactionDescriptor` and UTF-16LE SQL text.
#[pyfunction]
#[pyo3(signature = (sql, tx_descriptor=0))]
pub fn encode_tds_sql_batch<'py>(
    py: Python<'py>,
    sql: &str,
    tx_descriptor: u64,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(|| rs_encode_tds_sql_batch(sql, tx_descriptor));
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Generate DuckDB 2.0-safe post-initialization `SET` statements.
#[pyfunction]
#[pyo3(signature = (
    path=":memory:",
    memory_limit_mb=2048,
    threads=4,
    parquet_metadata_cache=true,
    httpfs_connection_caching=true,
    enable_http_metadata_cache=true
))]
pub fn duckdb_bootstrap_statements(
    path: &str,
    memory_limit_mb: usize,
    threads: usize,
    parquet_metadata_cache: bool,
    httpfs_connection_caching: bool,
    enable_http_metadata_cache: bool,
) -> Vec<String> {
    let cfg = DuckdbConfig {
        path: path.to_string(),
        memory_limit_mb,
        threads,
        parquet_metadata_cache,
        httpfs_connection_caching,
        enable_http_metadata_cache,
    };
    cfg.bootstrap_statements()
}

/// Generate SQLite 3.45+ initialization `PRAGMA` statements.
#[pyfunction]
#[pyo3(signature = (
    path=":memory:",
    wal_mode=true,
    synchronous="NORMAL",
    mmap_size=268_435_456,
    busy_timeout_ms=5000,
    max_readers=16,
    foreign_keys=true
))]
pub fn sqlite_initialization_pragmas(
    path: &str,
    wal_mode: bool,
    synchronous: &str,
    mmap_size: u64,
    busy_timeout_ms: u32,
    max_readers: u16,
    foreign_keys: bool,
) -> Vec<String> {
    let cfg = SqliteConfig {
        path: path.to_string(),
        wal_mode,
        synchronous: synchronous.to_string(),
        mmap_size,
        busy_timeout_ms,
        max_readers,
        foreign_keys,
    };
    cfg.initialization_pragmas()
}

/// Encode a Redis or Valkey RESP3 bulk-string array command.
#[pyfunction]
pub fn encode_resp_command<'py>(
    py: Python<'py>,
    args: Vec<Vec<u8>>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(move || {
        let refs: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
        rs_encode_resp_command(&refs)
    });
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Encode a Redis / Valkey `HELLO 3` handshake frame (with optional pipelined `CLIENT TRACKING ON ... BCAST`).
#[pyfunction]
#[pyo3(signature = (
    client_name="vertebra",
    username=None,
    password=None,
    enable_tracking_bcast=false,
    prefixes=Vec::new()
))]
pub fn encode_hello3_handshake<'py>(
    py: Python<'py>,
    client_name: &str,
    username: Option<&str>,
    password: Option<&str>,
    enable_tracking_bcast: bool,
    prefixes: Vec<String>,
) -> PyResult<Bound<'py, PyBytes>> {
    let bytes = py.detach(|| {
        let prefix_refs: Vec<&str> = prefixes.iter().map(|s| s.as_str()).collect();
        rs_encode_hello3_handshake(
            client_name,
            username,
            password,
            enable_tracking_bcast,
            &prefix_refs,
        )
    });
    PyBytes::new_with(py, bytes.len(), |buf| {
        buf.copy_from_slice(&bytes);
        Ok(())
    })
}

/// Compute CRC16-XMODEM for Redis/Valkey Cluster keys.
#[pyfunction]
pub fn crc16_xmodem(data: &[u8]) -> u16 {
    rs_crc16_xmodem(data)
}

/// Compute Redis/Valkey Cluster hash slot (`0..16383`) supporting `{hash_tag}` extraction.
#[pyfunction]
pub fn redis_hash_slot(key: &[u8]) -> u16 {
    rs_redis_hash_slot(key)
}

/// Parse a Redis/Valkey Cluster `MOVED` or `ASK` redirection error into `(kind, slot, host, port)`.
#[pyfunction]
pub fn parse_cluster_redirect(err_msg: &str) -> Option<(String, u16, String, u16)> {
    rs_parse_cluster_redirect(err_msg)
        .map(|r| (r.kind.as_str().to_string(), r.slot, r.host, r.port))
}

/// Register all `vertebra-db` classes and functions on the `_vertebra` Python module.
pub fn register_db_bindings(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<CloudSQLDialer>()?;
    m.add_class::<AlloyDBDialer>()?;
    m.add_class::<ArrowCursor>()?;

    m.add_function(wrap_pyfunction!(encode_alloydb_metadata_exchange, m)?)?;
    m.add_function(wrap_pyfunction!(decode_alloydb_metadata_response, m)?)?;
    m.add_function(wrap_pyfunction!(strip_pg_jsonb_header, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pg_jsonb, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgcopy_rows, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgcopy_rows, m)?)?;
    m.add_function(wrap_pyfunction!(parse_tnsnames_ora, m)?)?;
    m.add_function(wrap_pyfunction!(resolve_tns_alias, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_f64, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_f64, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_i8, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_i8, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oracle_vector_binary, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oracle_vector_binary, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgvector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgvector_f32, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgvector_f16, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgvector_f16, m)?)?;
    m.add_function(wrap_pyfunction!(encode_pgvector_bit, m)?)?;
    m.add_function(wrap_pyfunction!(decode_pgvector_bit, m)?)?;
    m.add_function(wrap_pyfunction!(encode_oson_payload, m)?)?;
    m.add_function(wrap_pyfunction!(decode_oson_payload, m)?)?;
    m.add_function(wrap_pyfunction!(encode_ttc_session_attrs, m)?)?;
    m.add_function(wrap_pyfunction!(rewrite_mssql_placeholders, m)?)?;
    m.add_function(wrap_pyfunction!(encode_tds_sql_batch, m)?)?;
    m.add_function(wrap_pyfunction!(duckdb_bootstrap_statements, m)?)?;
    m.add_function(wrap_pyfunction!(sqlite_initialization_pragmas, m)?)?;
    m.add_function(wrap_pyfunction!(encode_resp_command, m)?)?;
    m.add_function(wrap_pyfunction!(encode_hello3_handshake, m)?)?;
    m.add_function(wrap_pyfunction!(crc16_xmodem, m)?)?;
    m.add_function(wrap_pyfunction!(redis_hash_slot, m)?)?;
    m.add_function(wrap_pyfunction!(parse_cluster_redirect, m)?)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;

    use super::ArrowCursor;

    #[test]
    fn arrow_cursor_from_shm_capsule_exposes_zero_copy_view_and_unlinks_on_release() {
        Python::initialize();
        Python::attach(|py| {
            let payload = b"VTBARROW-RUST-CAPSULE-TEST-PAYLOAD";
            let unslashed = format!("vtb-py-cap-{}", std::process::id());
            let (norm_name, byte_len, capsule) =
                ArrowCursor::_allocate_shm_capsule(py, &unslashed, payload).unwrap();
            assert_eq!(byte_len, payload.len());
            assert_eq!(norm_name, format!("/{unslashed}"));

            assert!(ArrowCursor::new(norm_name.clone(), usize::MAX, 0).is_err());
            let bad_name = Some(unslashed.clone());
            let bad = ArrowCursor::_from_shm_capsule(py, bad_name, usize::MAX, 5, &capsule);
            assert!(bad.is_err());

            let ok = ArrowCursor::_from_shm_capsule(py, Some(unslashed), byte_len, 5, &capsule);
            let cursor = ok.expect("from capsule after rejected overflow");
            assert_eq!(cursor.shm_name, norm_name);
            assert_eq!(cursor.row_count, 5);
            let dup_name = Some(norm_name.clone());
            let dup = ArrowCursor::_from_shm_capsule(py, dup_name, byte_len, 5, &capsule);
            assert!(dup.is_err());

            let py_cursor = Bound::new(py, cursor).expect("bound cursor");
            let mv = ArrowCursor::buffer_view(py_cursor.clone()).expect("buffer view");
            let view_bytes: Vec<u8> = py
                .import("builtins")
                .unwrap()
                .getattr("bytes")
                .unwrap()
                .call1((&mv,))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(view_bytes, payload);

            assert!(py_cursor.borrow().release(py));
            #[cfg(target_os = "linux")]
            if !norm_name.starts_with("/tmp/") {
                let shm_path = format!("/dev/shm{norm_name}");
                assert!(!std::path::Path::new(&shm_path).exists());
            }

            let still_readable: Vec<u8> = py
                .import("builtins")
                .unwrap()
                .getattr("bytes")
                .unwrap()
                .call1((&mv,))
                .unwrap()
                .extract()
                .unwrap();
            assert_eq!(still_readable, payload);
            assert!(!py_cursor.borrow().release(py));
        });
    }
}
