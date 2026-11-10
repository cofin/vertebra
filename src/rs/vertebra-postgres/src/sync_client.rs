//! Synchronous physical PostgreSQL session client (`vertebra._postgres.SyncPostgresClient`).

use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::os::raw::c_char;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};

use bytes::Bytes;
use pyo3::exceptions::{PyBufferError, PyTypeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyByteArray, PyBytes, PyDict, PyString, PyTuple};
use vertebra_db::py_async::block_on_db;

use crate::arrow::{
    DEFAULT_ARROW_BATCH_ROWS, DEFAULT_MAX_ARROW_RESULT_BYTES, MAX_ARROW_COPY_CALL_BYTES,
    arrow_output_into_py_result, shm_output_into_py_cursor,
};

const _: () = assert!(DEFAULT_MAX_ARROW_RESULT_BYTES == 268_435_456);
const _: () = assert!(DEFAULT_ARROW_BATCH_ROWS == 65_536);
use crate::config::{PostgresConfig, RawPostgresOptions};
use crate::copy::{CopyFromStream, CopyOut};
use crate::errors::{PostgresError, postgres_error_to_py};
use crate::portal::{ArrowPortalStream, Portal, execute_select_to_arrow};
use crate::python_values::{extract_parameters, extract_pg_value, query_result_to_python};
use crate::result::PgValue;
use crate::session::pipeline_scope::{
    PipelineScopeToken, extract_pipeline_batch, outcomes_to_python, pipeline_error_to_py,
};
use crate::session::{ClientBeginConnect, CloseAction, CommandBudget, SessionHandle};

/// Extract a `Mapping[str, str]` snapshot while attached to the Python runtime.
pub(crate) fn extract_string_map(
    obj: Option<&Bound<'_, PyAny>>,
    field_name: &str,
) -> PyResult<Option<HashMap<String, String>>> {
    let Some(mapping) = obj.filter(|v| !v.is_none()) else {
        return Ok(None);
    };
    if mapping.is_instance_of::<PyString>()
        || !mapping.hasattr("items")?
        || !mapping.hasattr("__getitem__")?
    {
        return Err(PyTypeError::new_err(format!(
            "{field_name} must be a Mapping[str, str]"
        )));
    }
    let items = mapping
        .call_method0("items")
        .map_err(|_| PyTypeError::new_err(format!("{field_name} must be a Mapping[str, str]")))?;
    let mut out = HashMap::new();
    for entry in items.try_iter()? {
        let item = entry?;
        let pair = item.cast_into::<PyTuple>().map_err(|_| {
            PyTypeError::new_err(format!("{field_name} items must be (str, str) pairs"))
        })?;
        if pair.len() != 2 {
            return Err(PyTypeError::new_err(format!(
                "{field_name} items must be 2-tuples of (str, str)"
            )));
        }
        let k_obj = pair.get_item(0)?;
        let v_obj = pair.get_item(1)?;
        if !k_obj.is_instance_of::<PyString>() || !v_obj.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err(format!(
                "{field_name} keys and values must be str"
            )));
        }
        out.insert(k_obj.extract::<String>()?, v_obj.extract::<String>()?);
    }
    Ok(Some(out))
}

pub(crate) fn extract_strict_i64(obj: &Bound<'_, PyAny>) -> PyResult<i64> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected int, not bool"));
    }
    obj.extract::<i64>()
        .map_err(|_| PyTypeError::new_err("expected int"))
}

pub(crate) fn extract_strict_u64(obj: &Bound<'_, PyAny>) -> PyResult<u64> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected int, not bool"));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int"))?;
    if val < 0 {
        return Err(postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("savepoint token must be >= 0".to_string()),
        ));
    }
    u64::try_from(val).map_err(|_| {
        postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("savepoint token overflowed u64".to_string()),
        )
    })
}

pub(crate) fn extract_strict_opt_i64(obj: &Bound<'_, PyAny>) -> PyResult<Option<i64>> {
    if obj.is_none() {
        return Ok(None);
    }
    extract_strict_i64(obj).map(Some)
}

pub(crate) fn extract_strict_f64(obj: &Bound<'_, PyAny>) -> PyResult<f64> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected float or int, not bool"));
    }
    obj.extract::<f64>()
        .map_err(|_| PyTypeError::new_err("expected float or int"))
}

pub(crate) fn extract_strict_opt_f64(obj: Option<&Bound<'_, PyAny>>) -> PyResult<Option<f64>> {
    let Some(val) = obj.filter(|v| !v.is_none()) else {
        return Ok(None);
    };
    extract_strict_f64(val).map(Some)
}

pub(crate) fn extract_positive_i32(obj: &Bound<'_, PyAny>, field_name: &str) -> PyResult<i32> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(format!(
            "expected int {field_name}, not bool"
        )));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err(format!("expected int {field_name}")))?;
    if val <= 0 {
        return Err(postgres_error_to_py(
            obj.py(),
            PostgresError::Interface(format!("{field_name} must be >= 1, got {val}")),
        ));
    }
    i32::try_from(val).map_err(|_| {
        postgres_error_to_py(
            obj.py(),
            PostgresError::Interface(format!("{field_name} {val} exceeds i32::MAX")),
        )
    })
}

pub(crate) fn extract_max_rows(obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    extract_positive_i32(obj, "max_rows")
}

pub(crate) fn extract_batch_size(obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    extract_positive_i32(obj, "batch_size")
}

pub(crate) fn extract_threshold_rows(obj: &Bound<'_, PyAny>) -> PyResult<usize> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "expected int threshold_rows, not bool",
        ));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int threshold_rows"))?;
    if val < 0 {
        return Err(PyValueError::new_err(format!(
            "threshold_rows must be >= 0, got {val}"
        )));
    }
    usize::try_from(val)
        .map_err(|_| PyValueError::new_err(format!("threshold_rows {val} exceeds usize::MAX")))
}

pub(crate) fn extract_max_result_bytes(obj: &Bound<'_, PyAny>) -> PyResult<usize> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err(
            "expected int max_result_bytes, not bool",
        ));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int max_result_bytes"))?;
    if val <= 0 {
        return Err(PyValueError::new_err(format!(
            "max_result_bytes must be >= 1, got {val}"
        )));
    }
    usize::try_from(val)
        .map_err(|_| PyValueError::new_err(format!("max_result_bytes {val} exceeds usize::MAX")))
}

pub(crate) fn extract_arrow_batch_size(obj: &Bound<'_, PyAny>) -> PyResult<i32> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected int batch_size, not bool"));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int batch_size"))?;
    if val <= 0 {
        return Err(PyValueError::new_err(format!(
            "batch_size must be >= 1, got {val}"
        )));
    }
    i32::try_from(val)
        .map_err(|_| PyValueError::new_err(format!("batch_size {val} exceeds i32::MAX")))
}

pub(crate) fn extract_strict_bool(obj: &Bound<'_, PyAny>) -> PyResult<bool> {
    if !obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected bool"));
    }
    obj.extract::<bool>()
}

pub(crate) fn extract_scope_id(obj: &Bound<'_, PyAny>) -> PyResult<PipelineScopeToken> {
    if obj.is_instance_of::<PyBool>() {
        return Err(PyTypeError::new_err("expected int scope_id, not bool"));
    }
    let val = obj
        .extract::<i128>()
        .map_err(|_| PyTypeError::new_err("expected int scope_id"))?;
    if val <= 0 {
        return Err(postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("pipeline scope_id must be >= 1".to_string()),
        ));
    }
    let raw = u64::try_from(val).map_err(|_| {
        postgres_error_to_py(
            obj.py(),
            PostgresError::Interface("pipeline scope_id overflowed u64".to_string()),
        )
    })?;
    Ok(PipelineScopeToken(raw))
}

pub(crate) fn extract_session_gucs(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<HashMap<String, String>>> {
    extract_string_map(Some(obj), "session_gucs")
}

pub(crate) fn extract_cloud_auth_options(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<HashMap<String, String>>> {
    extract_string_map(Some(obj), "cloud_auth_options")
}

pub(crate) fn extract_iam_auth_options(
    obj: &Bound<'_, PyAny>,
) -> PyResult<Option<HashMap<String, String>>> {
    extract_string_map(Some(obj), "iam_auth_options")
}

struct PyRawBufferGuard {
    view: ffi::Py_buffer,
}

impl Drop for PyRawBufferGuard {
    fn drop(&mut self) {
        // SAFETY: `self.view` was initialized by a successful `PyObject_GetBuffer` call and is
        // dropped strictly on the calling thread while attached to `Python<'_>`.
        unsafe { ffi::PyBuffer_Release(&raw mut self.view) };
    }
}

/// Validate and copy a contiguous Python `Buffer` (`<= 16 MiB`) into owned Rust bytes while
/// attached on the caller thread, checking length before allocating and protecting writable
/// buffer exports under free-threaded Python (`Py_GIL_DISABLED`).
pub(crate) fn extract_ipc_buffer_bytes(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    field_name: &str,
) -> PyResult<Vec<u8>> {
    if let Ok(py_bytes) = obj.cast::<PyBytes>() {
        let slice = py_bytes.as_bytes();
        if slice.len() > MAX_ARROW_COPY_CALL_BYTES {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Data(format!(
                    "{field_name} byte length {} exceeds maximum Arrow COPY call limit ({MAX_ARROW_COPY_CALL_BYTES} bytes)",
                    slice.len()
                )),
            ));
        }
        return Ok(slice.to_vec());
    }

    if let Ok(ba) = obj.cast::<PyByteArray>() {
        let copied = pyo3::sync::critical_section::with_critical_section(ba.as_any(), || {
            let len = ba.len();
            if len > MAX_ARROW_COPY_CALL_BYTES {
                return Err(len);
            }
            // SAFETY: Holding CPython's per-object critical section on `ba` serializes against
            // concurrent `bytearray` resize or in-place mutation under `Py_GIL_DISABLED` (and
            // the GIL serializes standard builds) across both the length check and immediate copy.
            Ok(unsafe { ba.as_bytes() }.to_vec())
        });
        return copied.map_err(|len| {
            postgres_error_to_py(
                py,
                PostgresError::Data(format!(
                    "{field_name} byte length {len} exceeds maximum Arrow COPY call limit ({MAX_ARROW_COPY_CALL_BYTES} bytes)"
                )),
            )
        });
    }

    let mut raw_view = MaybeUninit::<ffi::Py_buffer>::zeroed();
    // SAFETY: `obj.as_ptr()` is a live borrowed PyObject while attached to `py`, and
    // `PyBUF_CONTIG_RO` requests a C-contiguous byte export without mutating data.
    let rc = unsafe {
        ffi::PyObject_GetBuffer(obj.as_ptr(), raw_view.as_mut_ptr(), ffi::PyBUF_CONTIG_RO)
    };
    if rc != 0 {
        return Err(PyErr::fetch(py));
    }
    // SAFETY: `PyObject_GetBuffer` returned 0, fully initializing `raw_view`.
    let guard = PyRawBufferGuard {
        view: unsafe { raw_view.assume_init() },
    };
    // SAFETY: `guard.view` is an initialized `Py_buffer` checked before reading `guard.view.buf`.
    if unsafe { ffi::PyBuffer_IsContiguous(&raw const guard.view, b'C' as c_char) } == 0 {
        return Err(PyBufferError::new_err(format!(
            "{field_name} must be a C-contiguous buffer"
        )));
    }
    let len = usize::try_from(guard.view.len).map_err(|_| {
        postgres_error_to_py(
            py,
            PostgresError::Data(format!(
                "{field_name} exported negative buffer length {}",
                guard.view.len
            )),
        )
    })?;
    if len > MAX_ARROW_COPY_CALL_BYTES {
        return Err(postgres_error_to_py(
            py,
            PostgresError::Data(format!(
                "{field_name} byte length {len} exceeds maximum Arrow COPY call limit ({MAX_ARROW_COPY_CALL_BYTES} bytes)"
            )),
        ));
    }
    if len == 0 {
        return Ok(Vec::new());
    }
    if guard.view.buf.is_null() {
        return Err(postgres_error_to_py(
            py,
            PostgresError::Data(format!(
                "{field_name} exported null buffer pointer for length {len}"
            )),
        ));
    }
    let owned = if guard.view.readonly != 0 {
        // SAFETY: `PyBUF_CONTIG_RO`, `PyBuffer_IsContiguous('C')`, and `readonly != 0` guarantee
        // an immutable C-contiguous `len`-byte region at non-null `guard.view.buf` pinned until
        // `PyBuffer_Release` runs in `drop(guard)`.
        unsafe { std::slice::from_raw_parts(guard.view.buf.cast::<u8>(), len) }.to_vec()
    } else {
        let cs_target: Borrowed<'_, '_, PyAny> = if !guard.view.obj.is_null() {
            // SAFETY: A non-null `guard.view.obj` holds a strong exporter reference owned by
            // `guard.view` until `PyBuffer_Release` runs in `drop(guard)` below.
            unsafe { Borrowed::from_ptr(py, guard.view.obj) }
        } else {
            obj.as_borrowed()
        };
        let base = guard.view.buf.cast::<AtomicU8>().cast_const();
        pyo3::sync::critical_section::with_critical_section(&cs_target, || {
            let mut out = Vec::with_capacity(len);
            for idx in 0..len {
                // SAFETY: `guard` pins a valid C-contiguous `len`-byte buffer at non-null `base`
                // (`AtomicU8` has identical size and 1-byte alignment to `u8`). Copying via
                // relaxed atomic byte loads inside `cs_target`'s per-object critical section never
                // forms `&[u8]` over writable memory, avoiding Rust aliasing/data-race UB even if
                // external code mutates unlocked backing bytes under `Py_GIL_DISABLED`.
                let byte = unsafe { &*base.add(idx) }.load(Ordering::Relaxed);
                out.push(byte);
            }
            out
        })
    };
    drop(guard);
    Ok(owned)
}

fn ensure_python_sequence(obj: &Bound<'_, PyAny>, field_name: &str) -> PyResult<()> {
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyByteArray>()
        || obj.is_instance_of::<PyDict>()
        || obj.hasattr("keys")?
        || !obj.hasattr("__len__")?
        || !obj.hasattr("__getitem__")?
    {
        return Err(PyTypeError::new_err(format!(
            "{field_name} must be a sequence"
        )));
    }
    Ok(())
}

/// Extract optional `columns: Sequence[str] | None` while attached on the caller thread.
pub(crate) fn extract_opt_copy_columns(
    obj: Option<&Bound<'_, PyAny>>,
) -> PyResult<Option<Vec<String>>> {
    let Some(seq) = obj.filter(|v| !v.is_none()) else {
        return Ok(None);
    };
    ensure_python_sequence(seq, "columns")?;
    let mut cols = Vec::new();
    for entry in seq.try_iter()? {
        let item = entry?;
        if !item.is_instance_of::<PyString>() {
            return Err(PyTypeError::new_err("columns entries must be str"));
        }
        cols.push(item.extract::<String>()?);
    }
    Ok(Some(cols))
}

/// Extract `(column_oids, rows)` for binary `COPY ... FROM STDIN` while attached on the caller thread.
pub(crate) fn extract_copy_rows_and_oids(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    column_oids: &Bound<'_, PyAny>,
) -> PyResult<(Vec<u32>, Vec<Vec<PgValue>>)> {
    ensure_python_sequence(column_oids, "column_oids")?;
    ensure_python_sequence(rows, "rows")?;

    let mut oids = Vec::new();
    for entry in column_oids.try_iter()? {
        let item = entry?;
        if item.is_instance_of::<PyBool>() {
            return Err(PyTypeError::new_err(
                "column_oids entries must be int, not bool",
            ));
        }
        let raw = item
            .extract::<i128>()
            .map_err(|_| PyTypeError::new_err("column_oids entries must be int"))?;
        let oid = u32::try_from(raw).ok().filter(|&v| v > 0).ok_or_else(|| {
            postgres_error_to_py(
                py,
                PostgresError::Data(format!("column_oids entry {raw} must be in 1..=u32::MAX")),
            )
        })?;
        oids.push(oid);
    }
    if oids.is_empty() {
        return Err(postgres_error_to_py(
            py,
            PostgresError::Data("column_oids cannot be empty".to_string()),
        ));
    }

    let mut pg_rows = Vec::new();
    for row_entry in rows.try_iter()? {
        let row_obj = row_entry?;
        ensure_python_sequence(&row_obj, "COPY row")?;
        let mut cells = Vec::with_capacity(oids.len());
        for cell_entry in row_obj.try_iter()? {
            cells.push(cell_entry?);
        }
        if cells.len() != oids.len() {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Data(format!(
                    "COPY row length {} does not match column_oids length {}",
                    cells.len(),
                    oids.len()
                )),
            ));
        }
        let mut pg_row = Vec::with_capacity(oids.len());
        for (cell, &oid) in cells.iter().zip(oids.iter()) {
            let val =
                extract_pg_value(py, cell, Some(oid)).map_err(|e| postgres_error_to_py(py, e))?;
            pg_row.push(val);
        }
        pg_rows.push(pg_row);
    }
    Ok((oids, pg_rows))
}

/// Convert one decoded `CopyOut` binary tuple into `tuple[bytes | None, ...]`.
pub(crate) fn copy_out_row_to_python(
    py: Python<'_>,
    fields: Vec<Option<Bytes>>,
) -> PyResult<Py<PyAny>> {
    let mut items = Vec::with_capacity(fields.len());
    for cell in fields {
        match cell {
            Some(buf) => items.push(PyBytes::new(py, &buf).into_any()),
            None => items.push(py.None().into_bound(py)),
        }
    }
    Ok(PyTuple::new(py, items)?.into_any().unbind())
}

pub(crate) struct BorrowedBusyGuard<'a> {
    flag: &'a AtomicBool,
}

impl<'a> BorrowedBusyGuard<'a> {
    pub(crate) fn try_enter(py: Python<'_>, flag: &'a AtomicBool, kind: &str) -> PyResult<Self> {
        if flag
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(format!("overlapping {kind} operation is not allowed")),
            ));
        }
        Ok(Self { flag })
    }
}

impl Drop for BorrowedBusyGuard<'_> {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

pub(crate) struct OwnedBusyGuard {
    flag: Arc<AtomicBool>,
}

impl OwnedBusyGuard {
    pub(crate) fn try_enter(py: Python<'_>, flag: &Arc<AtomicBool>, kind: &str) -> PyResult<Self> {
        if flag
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(format!("overlapping {kind} operation is not allowed")),
            ));
        }
        Ok(Self {
            flag: Arc::clone(flag),
        })
    }
}

impl Drop for OwnedBusyGuard {
    fn drop(&mut self) {
        self.flag.store(false, Ordering::Release);
    }
}

/// Shared 25-parameter constructor validator used by both `SyncPostgresClient` and `AsyncPostgresClient`.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_validated_config(
    py: Python<'_>,
    user: String,
    database: String,
    host: String,
    port: i64,
    password: Option<String>,
    unix_socket: Option<String>,
    sslmode: String,
    ca_pem: Option<Vec<u8>>,
    client_cert_pem: Option<Vec<u8>>,
    client_key_pem: Option<Vec<u8>>,
    server_name: Option<String>,
    channel_binding: String,
    connect_timeout: f64,
    application_name: String,
    session_gucs: Option<HashMap<String, String>>,
    keepalives: bool,
    keepalives_idle: Option<i64>,
    keepalives_interval: Option<i64>,
    keepalives_count: Option<i64>,
    cloud_sql_instance: Option<String>,
    cloud_sql_ip_type: String,
    alloydb_instance_uri: Option<String>,
    alloydb_ip_type: String,
    enable_iam_auth: bool,
    cloud_auth_options: Option<HashMap<String, String>>,
    iam_auth_options: Option<HashMap<String, String>>,
) -> PyResult<PostgresConfig> {
    let raw = RawPostgresOptions {
        user,
        database,
        host,
        port,
        password,
        unix_socket,
        sslmode,
        ca_pem,
        client_cert_pem,
        client_key_pem,
        server_name,
        channel_binding,
        connect_timeout,
        application_name,
        session_gucs: session_gucs.unwrap_or_default(),
        keepalives,
        keepalives_idle,
        keepalives_interval,
        keepalives_count,
        cloud_sql_instance,
        cloud_sql_ip_type,
        alloydb_instance_uri,
        alloydb_ip_type,
        enable_iam_auth,
        cloud_auth_options: cloud_auth_options.unwrap_or_default(),
        iam_auth_options: iam_auth_options.unwrap_or_default(),
        ..RawPostgresOptions::default()
    };

    PostgresConfig::validate(raw).map_err(|e| postgres_error_to_py(py, e))
}

/// Physical synchronous native PostgreSQL client (`vertebra._postgres.SyncPostgresClient`).
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        name = "SyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type,
        freelist = 128
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(
        name = "SyncPostgresClient",
        module = "vertebra._postgres",
        frozen,
        immutable_type
    )
)]
pub struct PySyncPostgresClient {
    pub(crate) session: Arc<SessionHandle>,
}

impl PySyncPostgresClient {
    /// Wrap an existing [`SessionHandle`] (such as a checked-out pool lease).
    pub fn from_session(session: Arc<SessionHandle>) -> Self {
        Self { session }
    }

    /// Register synchronous client, portal, Arrow stream, and COPY types on `vertebra._postgres`.
    pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_class::<Self>()?;
        m.add_class::<PySyncPostgresPortal>()?;
        m.add_class::<PySyncPostgresArrowStream>()?;
        m.add_class::<PySyncPostgresCopyOut>()?;
        m.add_class::<PySyncPostgresCopyIn>()
    }
}

/// Exclusive synchronous binary `COPY ... TO STDOUT` iterator (`vertebra._postgres.SyncPostgresCopyOut`).
#[pyclass(
    name = "SyncPostgresCopyOut",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PySyncPostgresCopyOut {
    stream: tokio::sync::Mutex<CopyOut>,
    exhausted_flag: Arc<AtomicBool>,
    closed_flag: Arc<AtomicBool>,
    busy: AtomicBool,
}

impl PySyncPostgresCopyOut {
    pub(crate) fn from_stream(stream: CopyOut) -> Self {
        let (exhausted_flag, closed_flag) = stream.status_flags();
        Self {
            stream: tokio::sync::Mutex::new(stream),
            exhausted_flag,
            closed_flag,
            busy: AtomicBool::new(false),
        }
    }
}

#[pymethods]
impl PySyncPostgresCopyOut {
    /// Return the stream itself as a synchronous row iterator.
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Yield the next decoded binary row as `tuple[bytes | None, ...]`, or stop on `CopyDone`.
    pub fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let _busy = BorrowedBusyGuard::try_enter(py, &self.busy, "COPY TO")?;
        let next = py
            .detach(|| {
                block_on_db(async {
                    let mut guard = self.stream.lock().await;
                    guard.next_row().await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        match next {
            Some(fields) => Ok(Some(copy_out_row_to_python(py, fields)?)),
            None => Ok(None),
        }
    }

    /// Close an unfinished `COPY TO` stream via `CancelRequest` drain/retire, or no-op when closed.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.stream.lock().await;
                guard.close().await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Enter a synchronous context-manager scope returning this `SyncPostgresCopyOut`.
    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Idempotently close the stream when leaving a `with` block without suppressing exceptions.
    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_val: Option<&Bound<'_, PyAny>>,
        exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_type, exc_val, exc_tb);
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!(
            "SyncPostgresCopyOut(exhausted={}, closed={})",
            self.exhausted_flag.load(Ordering::Acquire),
            self.closed_flag.load(Ordering::Acquire),
        )
    }
}

/// Exclusive synchronous binary `COPY ... FROM STDIN` stream (`vertebra._postgres.SyncPostgresCopyIn`).
#[pyclass(
    name = "SyncPostgresCopyIn",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PySyncPostgresCopyIn {
    stream: tokio::sync::Mutex<CopyFromStream>,
    closed_flag: Arc<AtomicBool>,
    busy: AtomicBool,
}

impl PySyncPostgresCopyIn {
    pub(crate) fn from_stream(stream: CopyFromStream) -> Self {
        let closed_flag = stream.closed_flag();
        Self {
            stream: tokio::sync::Mutex::new(stream),
            closed_flag,
            busy: AtomicBool::new(false),
        }
    }
}

#[pymethods]
impl PySyncPostgresCopyIn {
    /// Encode and flush binary `COPY` rows against explicit PostgreSQL target `column_oids`.
    pub fn write(
        &self,
        py: Python<'_>,
        rows: &Bound<'_, PyAny>,
        column_oids: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let _busy = BorrowedBusyGuard::try_enter(py, &self.busy, "COPY FROM")?;
        if self.closed_flag.load(Ordering::Acquire) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(
                    "cannot call write() on a closed COPY FROM stream".to_string(),
                ),
            ));
        }
        let (oids, pg_rows) = match extract_copy_rows_and_oids(py, rows, column_oids) {
            Ok(extracted) => extracted,
            Err(err) => {
                let _ = py.detach(|| {
                    block_on_db(async {
                        let mut guard = self.stream.lock().await;
                        guard
                            .abort("COPY FROM aborted due to invalid input rows")
                            .await
                    })
                });
                return Err(err);
            }
        };
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.stream.lock().await;
                guard.write_rows(&oids, &pg_rows).await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Encode and flush a bounded (`<= 16 MiB`) Arrow IPC stream chunk without materializing Python rows.
    pub fn write_arrow_ipc(&self, py: Python<'_>, batch_ipc: &Bound<'_, PyAny>) -> PyResult<()> {
        let _busy = BorrowedBusyGuard::try_enter(py, &self.busy, "COPY FROM")?;
        if self.closed_flag.load(Ordering::Acquire) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(
                    "cannot call write_arrow_ipc() on a closed COPY FROM stream".to_string(),
                ),
            ));
        }
        let ipc_bytes = match extract_ipc_buffer_bytes(py, batch_ipc, "batch_ipc") {
            Ok(buf) => buf,
            Err(err) => {
                let _ = py.detach(|| {
                    block_on_db(async {
                        let mut guard = self.stream.lock().await;
                        guard
                            .abort("COPY FROM Arrow aborted due to invalid IPC buffer")
                            .await
                    })
                });
                return Err(err);
            }
        };
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.stream.lock().await;
                guard.write_arrow_ipc(&ipc_bytes).await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Emit the binary `-1i16` trailer and `CopyDone`, commit COPY-owned scope, and return row count.
    pub fn finish(&self, py: Python<'_>) -> PyResult<u64> {
        let _busy = BorrowedBusyGuard::try_enter(py, &self.busy, "COPY FROM")?;
        if self.closed_flag.load(Ordering::Acquire) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Interface(
                    "cannot call finish() on a closed COPY FROM stream".to_string(),
                ),
            ));
        }
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.stream.lock().await;
                guard.finish().await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Abort an unfinished `COPY FROM` stream via `CopyFail` + rollback, or no-op once finished/closed.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.stream.lock().await;
                guard.abort("COPY FROM closed before finish").await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Enter a synchronous context-manager scope returning this `SyncPostgresCopyIn`.
    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Abort via `close()` if leaving `with` unfinished or on exception without suppressing exceptions.
    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_val: Option<&Bound<'_, PyAny>>,
        exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_type, exc_val, exc_tb);
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!(
            "SyncPostgresCopyIn(closed={})",
            self.closed_flag.load(Ordering::Acquire),
        )
    }
}

/// Session-pinned synchronous PostgreSQL Protocol 3.0 portal (`vertebra._postgres.SyncPostgresPortal`).
#[pyclass(
    name = "SyncPostgresPortal",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PySyncPostgresPortal {
    portal: tokio::sync::Mutex<Portal>,
    exhausted_flag: Arc<AtomicBool>,
    closed_flag: Arc<AtomicBool>,
}

impl PySyncPostgresPortal {
    pub(crate) fn from_portal(portal: Portal) -> Self {
        let (exhausted_flag, closed_flag) = portal.status_flags();
        Self {
            portal: tokio::sync::Mutex::new(portal),
            exhausted_flag,
            closed_flag,
        }
    }
}

#[pymethods]
impl PySyncPostgresPortal {
    /// Fetch up to `max_rows` rows (`>= 1`) from the suspended server portal as a `PostgresQueryResult`.
    pub fn fetch(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_max_rows)] max_rows: i32,
    ) -> PyResult<Py<PyAny>> {
        let res = py
            .detach(|| {
                block_on_db(async {
                    let mut guard = self.portal.lock().await;
                    guard.fetch(max_rows).await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        query_result_to_python(py, &res)
    }

    /// Close the portal idempotently and complete/roll back its pinned `TransactionScope`.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.portal.lock().await;
                guard.close().await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Return `True` once the server portal has reached `CommandComplete` or `EmptyQueryResponse`.
    #[getter]
    pub fn exhausted(&self) -> bool {
        self.exhausted_flag.load(Ordering::Acquire)
    }

    /// Return `True` once the portal has been closed or exhausted.
    #[getter]
    pub fn closed(&self) -> bool {
        self.closed_flag.load(Ordering::Acquire)
    }

    fn __repr__(&self) -> String {
        format!(
            "SyncPostgresPortal(exhausted={}, closed={})",
            self.exhausted(),
            self.closed()
        )
    }
}

/// Pinned synchronous `VTBARROW` shared-memory Arrow IPC batch iterator (`vertebra._postgres.SyncPostgresArrowStream`).
#[pyclass(
    name = "SyncPostgresArrowStream",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PySyncPostgresArrowStream {
    stream: tokio::sync::Mutex<ArrowPortalStream>,
    exhausted_flag: Arc<AtomicBool>,
    closed_flag: Arc<AtomicBool>,
    busy: AtomicBool,
}

impl PySyncPostgresArrowStream {
    pub(crate) fn from_stream(stream: ArrowPortalStream) -> Self {
        let (exhausted_flag, closed_flag) = stream.status_flags();
        Self {
            stream: tokio::sync::Mutex::new(stream),
            exhausted_flag,
            closed_flag,
            busy: AtomicBool::new(false),
        }
    }
}

#[pymethods]
impl PySyncPostgresArrowStream {
    /// Return the stream itself as a synchronous iterator of `ArrowCursor` batches.
    pub fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Advance the server portal and return the next `vertebra._vertebra.ArrowCursor` SHM batch.
    pub fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let _busy = BorrowedBusyGuard::try_enter(py, &self.busy, "Arrow portal stream")?;
        let next = py
            .detach(|| {
                block_on_db(async {
                    let mut guard = self.stream.lock().await;
                    guard.next_batch_shm().await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        match next {
            Some(shm_out) => shm_output_into_py_cursor(py, shm_out).map(Some),
            None => Ok(None),
        }
    }

    /// Close the pinned Arrow portal stream idempotently, rolling back unfinished scopes or cancelling mid-`Execute` pauses.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let _busy = BorrowedBusyGuard::try_enter(py, &self.busy, "Arrow portal stream")?;
        py.detach(|| {
            block_on_db(async {
                let mut guard = self.stream.lock().await;
                guard.close().await
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Enter a synchronous context-manager scope returning this `SyncPostgresArrowStream`.
    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Idempotently close the stream on context exit without suppressing exceptions.
    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_val: Option<&Bound<'_, PyAny>>,
        exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        let _ = (exc_type, exc_val, exc_tb);
        self.close(py)?;
        Ok(false)
    }

    fn __repr__(&self) -> String {
        format!(
            "SyncPostgresArrowStream(exhausted={}, closed={})",
            self.exhausted_flag.load(Ordering::Acquire),
            self.closed_flag.load(Ordering::Acquire),
        )
    }
}

#[pymethods]
impl PySyncPostgresClient {
    #[new]
    #[pyo3(signature = (
        *,
        user,
        database,
        host = "localhost",
        port = 5432,
        password = None,
        unix_socket = None,
        sslmode = "verify-full",
        ca_pem = None,
        client_cert_pem = None,
        client_key_pem = None,
        server_name = None,
        channel_binding = "prefer",
        connect_timeout = 10.0,
        application_name = "vertebra",
        session_gucs = None,
        keepalives = true,
        keepalives_idle = None,
        keepalives_interval = None,
        keepalives_count = None,
        cloud_sql_instance = None,
        cloud_sql_ip_type = "PRIVATE",
        alloydb_instance_uri = None,
        alloydb_ip_type = "PRIVATE",
        enable_iam_auth = false,
        cloud_auth_options = None,
        iam_auth_options = None
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        user: String,
        database: String,
        host: &str,
        #[pyo3(from_py_with = extract_strict_i64)] port: i64,
        password: Option<String>,
        unix_socket: Option<String>,
        sslmode: &str,
        ca_pem: Option<Vec<u8>>,
        client_cert_pem: Option<Vec<u8>>,
        client_key_pem: Option<Vec<u8>>,
        server_name: Option<String>,
        channel_binding: &str,
        #[pyo3(from_py_with = extract_strict_f64)] connect_timeout: f64,
        application_name: &str,
        #[pyo3(from_py_with = extract_session_gucs)] session_gucs: Option<HashMap<String, String>>,
        keepalives: bool,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_idle: Option<i64>,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_interval: Option<i64>,
        #[pyo3(from_py_with = extract_strict_opt_i64)] keepalives_count: Option<i64>,
        cloud_sql_instance: Option<String>,
        cloud_sql_ip_type: &str,
        alloydb_instance_uri: Option<String>,
        alloydb_ip_type: &str,
        enable_iam_auth: bool,
        #[pyo3(from_py_with = extract_cloud_auth_options)] cloud_auth_options: Option<
            HashMap<String, String>,
        >,
        #[pyo3(from_py_with = extract_iam_auth_options)] iam_auth_options: Option<
            HashMap<String, String>,
        >,
    ) -> PyResult<Self> {
        let cfg = build_validated_config(
            py,
            user,
            database,
            host.to_string(),
            port,
            password,
            unix_socket,
            sslmode.to_string(),
            ca_pem,
            client_cert_pem,
            client_key_pem,
            server_name,
            channel_binding.to_string(),
            connect_timeout,
            application_name.to_string(),
            session_gucs,
            keepalives,
            keepalives_idle,
            keepalives_interval,
            keepalives_count,
            cloud_sql_instance,
            cloud_sql_ip_type.to_string(),
            alloydb_instance_uri,
            alloydb_ip_type.to_string(),
            enable_iam_auth,
            cloud_auth_options,
            iam_auth_options,
        )?;
        Ok(Self {
            session: Arc::new(SessionHandle::from_config(cfg)),
        })
    }

    /// Establish a physical PostgreSQL session, releasing the GIL while awaiting startup completion.
    pub fn connect(&self, py: Python<'_>) -> PyResult<()> {
        match self
            .session
            .begin_client_connect()
            .map_err(|e| postgres_error_to_py(py, e))?
        {
            ClientBeginConnect::AlreadyReady => Ok(()),
            ClientBeginConnect::Attempt(attempt) => {
                let pending = py
                    .detach(|| block_on_db(attempt.run()))
                    .map_err(|e| postgres_error_to_py(py, e))?;
                pending.commit().map_err(|e| postgres_error_to_py(py, e))
            }
        }
    }

    /// Close the physical PostgreSQL session idempotently, sending `Terminate` (`'X'`) once if direct or releasing if pooled.
    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let action = self
            .session
            .begin_close_action()
            .map_err(|e| postgres_error_to_py(py, e))?;
        match action {
            CloseAction::Immediate(Some(backing)) => py
                .detach(|| block_on_db(async move { backing.close().await }))
                .map_err(|e| postgres_error_to_py(py, e))?,
            CloseAction::Immediate(None) => {}
            CloseAction::WaitInFlight(notified) => {
                let timeout = self.session.config().connect_timeout;
                py.detach(|| {
                    block_on_db(async move {
                        let _ = tokio::time::timeout(timeout, notified).await;
                    })
                });
            }
        }
        Ok(())
    }

    /// Trigger an out-of-band generation-fenced protocol `CancelRequest` for any currently active operation without waiting on the session execution lock.
    pub fn cancel(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        py.detach(move || {
            block_on_db(async move {
                let _ = session.cancel().await;
            });
        });
        Ok(())
    }

    /// Execute a single-statement PostgreSQL Protocol 3.0 extended query, returning `PostgresQueryResult`.
    #[pyo3(signature = (sql, parameters = None, *, parameter_oids = None, timeout = None))]
    pub fn query(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        parameter_oids: Option<&Bound<'_, PyAny>>,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        let res = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .query_as_with_generation_and_budget(
                            op_gen,
                            None,
                            &sql_owned,
                            &params,
                            oids.as_deref(),
                            budget,
                        )
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        query_result_to_python(py, &res)
    }

    /// Execute a multi-statement PostgreSQL Protocol 3.0 simple query (`'Q'`), returning `tuple[PostgresQueryResult, ...]`.
    #[pyo3(signature = (sql, *, timeout = None))]
    pub fn query_multi(
        &self,
        py: Python<'_>,
        sql: &str,
        timeout: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        let results = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .simple_query_with_generation_and_budget(op_gen, &sql_owned, budget)
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        let mut py_results = Vec::with_capacity(results.len());
        for res in &results {
            py_results.push(query_result_to_python(py, res)?);
        }
        Ok(PyTuple::new(py, py_results)?.into_any().unbind())
    }

    /// Open a session-pinned PostgreSQL portal inside an explicit transaction or nested savepoint scope.
    #[pyo3(signature = (sql, parameters = None, *, parameter_oids = None, batch_size = 65536))]
    pub fn open_portal(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        parameter_oids: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = extract_batch_size)] batch_size: i32,
    ) -> PyResult<PySyncPostgresPortal> {
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        let portal = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .open_portal_with_generation(
                            op_gen,
                            &sql_owned,
                            &params,
                            oids.as_deref(),
                            batch_size,
                        )
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PySyncPostgresPortal::from_portal(portal))
    }

    /// Execute `sql` directly into an inline or `VTBARROW` shared-memory `PostgresArrowResult` without materializing Python rows.
    #[pyo3(signature = (
        sql,
        parameters = None,
        *,
        parameter_oids = None,
        timeout = None,
        threshold_rows = 256,
        max_result_bytes = 268435456
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        parameter_oids: Option<&Bound<'_, PyAny>>,
        timeout: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = extract_threshold_rows)] threshold_rows: usize,
        #[pyo3(from_py_with = extract_max_result_bytes)] max_result_bytes: usize,
    ) -> PyResult<Py<PyAny>> {
        let timeout_secs = extract_strict_opt_f64(timeout)?;
        let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
            .map_err(|e| postgres_error_to_py(py, e))?;
        if sql.as_bytes().contains(&0) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Data(
                    "SQL query string cannot contain embedded NUL byte".to_string(),
                ),
            ));
        }
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let budget = match CommandBudget::start_from_duration(timeout_dur) {
            Ok(b) => b,
            Err(err) => {
                session.unreserve_operation_generation(op_gen);
                return Err(postgres_error_to_py(py, err));
            }
        };
        let sql_owned = sql.to_owned();
        let output = py
            .detach(move || {
                block_on_db(async move {
                    execute_select_to_arrow(
                        &session,
                        op_gen,
                        &sql_owned,
                        &params,
                        oids.as_deref(),
                        None,
                        budget,
                        threshold_rows,
                        max_result_bytes,
                    )
                    .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        arrow_output_into_py_result(py, output)
    }

    /// Open a session-pinned `SyncPostgresArrowStream` yielding `VTBARROW` shared-memory `ArrowCursor` batches inside a single portal `TransactionScope`.
    #[pyo3(signature = (
        sql,
        parameters = None,
        *,
        parameter_oids = None,
        batch_size = 65536,
        columns = None
    ))]
    pub fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        parameter_oids: Option<&Bound<'_, PyAny>>,
        #[pyo3(from_py_with = extract_arrow_batch_size)] batch_size: i32,
        columns: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<PySyncPostgresArrowStream> {
        let cols_owned = extract_opt_copy_columns(columns)?;
        if sql.as_bytes().contains(&0) {
            return Err(postgres_error_to_py(
                py,
                PostgresError::Data(
                    "SQL query string cannot contain embedded NUL byte".to_string(),
                ),
            ));
        }
        self.session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e))?;
        let none_bound = py.None().into_bound(py);
        let params_ref = parameters.unwrap_or(&none_bound);
        let (params, oids) = extract_parameters(py, params_ref, parameter_oids)
            .map_err(|e| postgres_error_to_py(py, e))?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        let stream = py
            .detach(move || {
                block_on_db(async move {
                    ArrowPortalStream::open_as_with_generation(
                        &session,
                        op_gen,
                        None,
                        &sql_owned,
                        &params,
                        oids.as_deref(),
                        batch_size,
                        cols_owned,
                    )
                    .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PySyncPostgresArrowStream::from_stream(stream))
    }

    /// Open an exclusive binary `COPY ... TO STDOUT (FORMAT binary)` streaming iterator.
    pub fn copy_to(&self, py: Python<'_>, sql: &str) -> PyResult<PySyncPostgresCopyOut> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        let stream = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .copy_to_with_generation_as(op_gen, &sql_owned, None)
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PySyncPostgresCopyOut::from_stream(stream))
    }

    /// Open an exclusive binary `COPY ... FROM STDIN (FORMAT binary)` write stream.
    pub fn copy_from(&self, py: Python<'_>, sql: &str) -> PyResult<PySyncPostgresCopyIn> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let sql_owned = sql.to_owned();
        let stream = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .copy_from_with_generation_as(op_gen, &sql_owned, None)
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PySyncPostgresCopyIn::from_stream(stream))
    }

    /// Validate schema against `pg_catalog`, open a binary `COPY ... FROM STDIN` stream, and flush `first_batch_ipc`.
    #[pyo3(signature = (table_name, first_batch_ipc, *, columns = None, schema_name = None))]
    pub fn copy_from_arrow(
        &self,
        py: Python<'_>,
        table_name: &str,
        first_batch_ipc: &Bound<'_, PyAny>,
        columns: Option<&Bound<'_, PyAny>>,
        schema_name: Option<&str>,
    ) -> PyResult<PySyncPostgresCopyIn> {
        let cols_owned = extract_opt_copy_columns(columns)?;
        let ipc_owned = extract_ipc_buffer_bytes(py, first_batch_ipc, "first_batch_ipc")?;
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        let table_owned = table_name.to_owned();
        let schema_owned = schema_name.map(str::to_owned);
        let stream = py
            .detach(move || {
                block_on_db(async move {
                    session
                        .copy_from_arrow_with_generation_as(
                            op_gen,
                            &table_owned,
                            schema_owned.as_deref(),
                            &ipc_owned,
                            cols_owned.as_deref(),
                            None,
                        )
                        .await
                })
            })
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(PySyncPostgresCopyIn::from_stream(stream))
    }

    /// Open a retained pipeline transaction (`BEGIN` at `'I'`) or savepoint (`SAVEPOINT` at `'T'`) and return its integer scope identifier.
    pub fn pipeline_begin(&self, py: Python<'_>) -> PyResult<u64> {
        let session = Arc::clone(&self.session);
        let token = py
            .detach(move || block_on_db(async move { session.pipeline_begin().await }))
            .map_err(|e| postgres_error_to_py(py, e))?;
        Ok(token.0)
    }

    /// Submit `operations` inside `scope_id` as one single-`Sync` extended pipeline batch while retaining the open transaction/savepoint scope.
    pub fn pipeline_results(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_scope_id)] scope_id: PipelineScopeToken,
        operations: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        let batch = match extract_pipeline_batch(py, operations) {
            Ok(b) => b,
            Err(err) => {
                let _ = self
                    .session
                    .mark_pipeline_scope_failed(scope_id, err.clone());
                return Err(pipeline_error_to_py(py, err));
            }
        };
        let session = Arc::clone(&self.session);
        let outcomes = py
            .detach(move || {
                block_on_db(async move { session.pipeline_results(scope_id, batch).await })
            })
            .map_err(|e| pipeline_error_to_py(py, e))?;
        outcomes_to_python(py, &outcomes)
    }

    /// Commit/release (`success=True` with no prior failure) or roll back (`success=False` or persistent failure) the retained pipeline scope `scope_id`.
    pub fn pipeline_finish(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_scope_id)] scope_id: PipelineScopeToken,
        #[pyo3(from_py_with = extract_strict_bool)] success: bool,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        py.detach(move || {
            block_on_db(async move { session.pipeline_finish(scope_id, success).await })
        })
        .map_err(|e| pipeline_error_to_py(py, e))
    }

    /// Start an explicit transaction (`BEGIN`).
    pub fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session.begin_as_with_generation(op_gen, None).await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Commit the current transaction (`COMMIT`).
    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session.commit_as_with_generation(op_gen, None).await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Roll back the current transaction (`ROLLBACK`).
    pub fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        py.detach(move || {
            block_on_db(async move {
                session.rollback_client().await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Create a numeric savepoint (`SAVEPOINT vtb_sp_<token>`).
    pub fn savepoint(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session
                    .savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Release an active numeric savepoint (`RELEASE SAVEPOINT vtb_sp_<token>`).
    pub fn release_savepoint(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session
                    .release_savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Roll back to an active numeric savepoint (`ROLLBACK TO SAVEPOINT vtb_sp_<token>`).
    pub fn rollback_to_savepoint(
        &self,
        py: Python<'_>,
        #[pyo3(from_py_with = extract_strict_u64)] token: u64,
    ) -> PyResult<()> {
        let session = Arc::clone(&self.session);
        let op_gen = session.reserve_operation_generation();
        py.detach(move || {
            block_on_db(async move {
                session
                    .rollback_to_savepoint_as_with_generation(op_gen, None, token)
                    .await?;
                Ok::<(), PostgresError>(())
            })
        })
        .map_err(|e| postgres_error_to_py(py, e))
    }

    /// Return `True` unless an authenticated session has reached `ReadyForQuery(Idle)` and has not been closed.
    #[getter]
    pub fn closed(&self) -> bool {
        self.session.is_client_closed()
    }

    /// Return a newly allocated dictionary copy of the session's `ParameterStatus` values.
    #[getter]
    pub fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (k, v) in self.session.parameters() {
            dict.set_item(k, v)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        format!(
            "SyncPostgresClient(host={:?}, port={}, database={:?}, user={:?}, closed={})",
            self.session.config().host,
            self.session.config().port,
            self.session.config().database,
            self.session.config().user,
            self.session.is_client_closed(),
        )
    }
}
