//! PyO3 client bindings for `SyncSQLiteClient`, `SyncSQLiteCursor`, `AsyncSQLiteClient`, `AsyncSQLiteCursor`, and `broker_engine_capsule`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};

use parking_lot::{Mutex, RwLock};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyDict, PyList, PySequence, PyString, PyTuple};
use vertebra_core::broker::DIALECT_TAG_SQLITE;
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, normalize_shm_name, release_shm_arrow_frame,
    write_shm_arrow_region_unregistered,
};
use vertebra_db::broker_capsule::{BrokerEngineBackend, BrokerPoolBackend};
use vertebra_db::py_async::{
    ReadyAwaitable, RustDbTaskAwaitable, construct_py_arrow_cursor, ready_ok,
    spawn_rust_db_task_with_attr_and_cancel,
};
use vertebra_db::{
    DbRpcEncodedResponse, DbRpcScalar, PlaceholderStyle, decode_dbrpc_request_frame,
    encode_dbrpc_response_frame, new_broker_engine_pycapsule, normalize_placeholders,
};

use crate::arrow_bridge::ArrowResultPayload;
use crate::config::{SqliteClientConfig, SqliteSynchronous};
use crate::errors::SqliteClientError;
use crate::value::{BoundParams, SqliteValue, StatementOutcome};
use crate::worker::SqlitePool;

const SQLITE_COMPLETION_DRAINER_ATTR: &str = "_vertebra_sqlite_completion_drainer";

fn scalar_to_sqlite_value(scalar: &DbRpcScalar) -> SqliteValue {
    match scalar {
        DbRpcScalar::Null => SqliteValue::Null,
        DbRpcScalar::Bool(b) => SqliteValue::Integer(if *b { 1 } else { 0 }),
        DbRpcScalar::Int(i) => SqliteValue::Integer(*i),
        DbRpcScalar::UInt(u) => SqliteValue::Integer(*u as i64),
        DbRpcScalar::Float(f) => SqliteValue::Real(*f),
        DbRpcScalar::Str(s) => SqliteValue::Text(s.clone()),
        DbRpcScalar::Bytes(b) => SqliteValue::Blob(b.clone()),
    }
}

fn sqlite_err_class(err: &SqliteClientError) -> &'static str {
    match err {
        SqliteClientError::InvalidInput(_) => "InterfaceError",
        SqliteClientError::Operational { .. } | SqliteClientError::ShmArrow(_) => {
            "OperationalError"
        }
        SqliteClientError::Integrity { .. } => "IntegrityError",
        SqliteClientError::Programming { .. } => "ProgrammingError",
        SqliteClientError::Data { .. } => "DataError",
        SqliteClientError::NotFound(_) => "NotFoundError",
        SqliteClientError::Cancelled(_) => "QueryCancelledError",
    }
}

fn validate_savepoint_name(raw: &str) -> Result<String, SqliteClientError> {
    let trimmed = raw.trim();
    if trimmed.is_empty()
        || !trimmed
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        return Err(SqliteClientError::Programming {
            message: format!("invalid SQLite savepoint identifier `{raw}`"),
            code: None,
            sqlstate: Some("42000"),
        });
    }
    Ok(trimmed.to_string())
}

/// Shared-memory broker backend for SQLite (`DIALECT_TAG_SQLITE = 3`).
pub struct SqliteBrokerBackend {
    pools: RwLock<HashMap<u64, Arc<SqlitePool>>>,
    active_shm: Mutex<HashMap<String, (u64, Arc<ShmRegion>)>>,
    next_pool_id: std::sync::atomic::AtomicU64,
}

impl Default for SqliteBrokerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl SqliteBrokerBackend {
    /// Construct a new [`SqliteBrokerBackend`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: RwLock::new(HashMap::new()),
            active_shm: Mutex::new(HashMap::new()),
            next_pool_id: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Retain `region` in the active SHM map until explicit release or backend/pool close.
    pub fn retain_shm(&self, pool_id: u64, shm_name: &str, region: Arc<ShmRegion>) -> String {
        let norm = normalize_shm_name(shm_name);
        ACTIVE_ARROW_SHM_REGIONS.insert(norm.clone(), Arc::clone(&region));
        self.active_shm
            .lock()
            .insert(norm.clone(), (pool_id, region));
        norm
    }

    /// Release and unlink a retained `VTBARROW` shared-memory segment by name.
    pub fn release_shm(&self, shm_name: &str) -> bool {
        let norm = normalize_shm_name(shm_name);
        let removed_local = self.active_shm.lock().remove(&norm);
        let removed_global = release_shm_arrow_frame(&norm);
        if let Some((_, region)) = removed_local {
            #[cfg(unix)]
            let _ = region.unlink_now();
            drop(region);
            true
        } else {
            removed_global
        }
    }

    /// Return the number of retained `VTBARROW` SHM segments currently held open.
    #[must_use]
    pub fn active_shm_count(&self) -> usize {
        self.active_shm.lock().len()
    }

    fn release_pool_shm(&self, pool_id: u64) {
        let names: Vec<String> = {
            let guard = self.active_shm.lock();
            guard
                .iter()
                .filter_map(|(k, (pid, _))| (*pid == pool_id).then_some(k.clone()))
                .collect()
        };
        for name in names {
            self.release_shm(&name);
        }
    }

    /// Close all pools and release all retained shared-memory segments.
    pub fn close(&self) {
        let pools: Vec<Arc<SqlitePool>> =
            self.pools.write().drain().map(|(_, pool)| pool).collect();
        for pool in pools {
            pool.close();
        }
        let shms: Vec<(String, Arc<ShmRegion>)> = self
            .active_shm
            .lock()
            .drain()
            .map(|(k, (_, region))| (k, region))
            .collect();
        for (name, region) in shms {
            let _ = release_shm_arrow_frame(&name);
            #[cfg(unix)]
            let _ = region.unlink_now();
            drop(region);
        }
    }
}

impl Drop for SqliteBrokerBackend {
    fn drop(&mut self) {
        self.close();
    }
}

impl BrokerEngineBackend for SqliteBrokerBackend {
    fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_SQLITE
    }

    fn dialect_name(&self) -> &'static str {
        "sqlite"
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        normalize_placeholders(sql, PlaceholderStyle::QuestionMark)
    }
}

impl BrokerPoolBackend for SqliteBrokerBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        let trimmed = dsn.trim();
        let path = if trimmed.is_empty() || trimmed == ":memory:" {
            ":memory:".to_string()
        } else if let Some((scheme, rest)) = trimmed.split_once("://") {
            let lower = scheme.to_ascii_lowercase();
            if lower != "sqlite" && lower != "file" {
                return Err(format!("invalid SQLite DSN scheme `{scheme}://`"));
            }
            rest.split_once('?')
                .map_or(rest, |(p, _)| p)
                .trim()
                .to_string()
        } else {
            trimmed.to_string()
        };

        let config = SqliteClientConfig {
            path,
            max_readers: (max_size as usize).max(1),
            ..Default::default()
        };

        let pool = SqlitePool::open(config).map_err(|e| e.to_string())?;
        let pool_id = self.next_pool_id.fetch_add(1, Ordering::Relaxed);
        self.pools.write().insert(pool_id, pool);
        Ok(pool_id)
    }

    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        let pool = {
            let guard = self.pools.read();
            guard
                .get(&pool_id)
                .cloned()
                .ok_or_else(|| format!("unknown SQLite broker pool_id {pool_id}"))?
        };

        let req = decode_dbrpc_request_frame(frame)
            .map_err(|e| format!("DBRPC request decode error: {e:?}"))?;

        match req.op {
            DbRpcOp::Begin => {
                let mode = match req.sql.as_deref() {
                    Some(raw) => {
                        let trimmed = raw.trim().trim_end_matches(';').trim();
                        let upper = trimmed.to_ascii_uppercase();
                        if upper == "BEGIN" {
                            String::new()
                        } else if let Some(rest) = upper.strip_prefix("BEGIN ") {
                            rest.trim().to_string()
                        } else {
                            trimmed.to_string()
                        }
                    }
                    None => "IMMEDIATE".to_string(),
                };
                return match pool.begin(mode) {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req.request_id, 0, None),
                    )),
                    Err(e) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                        req.request_id,
                        sqlite_err_class(&e),
                        e.to_string(),
                    ))),
                };
            }
            DbRpcOp::Commit => {
                return match pool.commit() {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req.request_id, 0, None),
                    )),
                    Err(e) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                        req.request_id,
                        sqlite_err_class(&e),
                        e.to_string(),
                    ))),
                };
            }
            DbRpcOp::Rollback => {
                let res = if let Some(sp) = req.savepoint.as_deref().filter(|s| !s.is_empty()) {
                    validate_savepoint_name(sp).and_then(|ident| {
                        pool.execute(
                            format!("ROLLBACK TO SAVEPOINT \"{ident}\""),
                            BoundParams::Empty,
                        )
                        .map(|_| ())
                    })
                } else {
                    pool.rollback()
                };
                return match res {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req.request_id, 0, None),
                    )),
                    Err(e) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                        req.request_id,
                        sqlite_err_class(&e),
                        e.to_string(),
                    ))),
                };
            }
            DbRpcOp::Savepoint => {
                let raw_sp = req
                    .savepoint
                    .as_deref()
                    .or(req.sql.as_deref())
                    .ok_or_else(|| "DBRPC Savepoint frame missing savepoint name".to_string())?;
                let res = validate_savepoint_name(raw_sp).and_then(|ident| {
                    pool.execute(format!("SAVEPOINT \"{ident}\""), BoundParams::Empty)
                        .map(|_| ())
                });
                return match res {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req.request_id, 0, None),
                    )),
                    Err(e) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                        req.request_id,
                        sqlite_err_class(&e),
                        e.to_string(),
                    ))),
                };
            }
            DbRpcOp::FetchChunk => {
                if let Some(shm_name) = req.savepoint.as_deref().or(req.sql.as_deref())
                    && !shm_name.trim().is_empty()
                {
                    self.release_shm(shm_name);
                }
                return Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req.request_id, 0, None),
                ));
            }
            DbRpcOp::Cancel => {
                pool.cancel();
                return Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req.request_id, 0, None),
                ));
            }
            DbRpcOp::Close => {
                self.close_pool(pool_id);
                return Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req.request_id, 0, None),
                ));
            }
            DbRpcOp::Execute | DbRpcOp::ExecuteMany => {}
        }

        let raw_sql = req
            .sql
            .as_deref()
            .ok_or_else(|| "DBRPC request frame missing SQL".to_string())?;
        let (norm_sql, _) = self.normalize_statement(raw_sql);
        let params = if req.params.is_empty() {
            BoundParams::Empty
        } else {
            BoundParams::Positional(req.params.iter().map(scalar_to_sqlite_value).collect())
        };

        let trimmed = norm_sql.trim_start();
        let upper = trimmed.to_ascii_uppercase();
        let is_query = req.op == DbRpcOp::Execute
            && (upper.starts_with("SELECT")
                || upper.starts_with("EXPLAIN")
                || upper.starts_with("WITH"));

        if is_query {
            match pool.select_to_arrow(norm_sql, params, 8192) {
                Ok(ArrowResultPayload::Shm {
                    shm_name,
                    row_count,
                    region,
                    ..
                }) => {
                    let retained_name = self.retain_shm(pool_id, &shm_name, region);
                    Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_arrow_shm(
                            req.request_id,
                            row_count as i64,
                            Vec::new(),
                            retained_name,
                        ),
                    ))
                }
                Ok(ArrowResultPayload::Inline {
                    ipc_bytes,
                    row_count,
                    ..
                }) => {
                    static INLINE_BROKER_SEQ: std::sync::atomic::AtomicU64 =
                        std::sync::atomic::AtomicU64::new(1);
                    let seq = INLINE_BROKER_SEQ.fetch_add(1, Ordering::Relaxed);
                    let candidate = format!("/vtb_sqlite_bkr_{}_{seq}", std::process::id());
                    match write_shm_arrow_region_unregistered(&candidate, &ipc_bytes) {
                        Ok((name, _, region)) => {
                            let retained_name = self.retain_shm(pool_id, &name, region);
                            Ok(encode_dbrpc_response_frame(
                                &DbRpcEncodedResponse::ok_arrow_shm(
                                    req.request_id,
                                    row_count as i64,
                                    Vec::new(),
                                    retained_name,
                                ),
                            ))
                        }
                        Err(err) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                            req.request_id,
                            "OperationalError",
                            err,
                        ))),
                    }
                }
                Err(e) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                    req.request_id,
                    sqlite_err_class(&e),
                    e.to_string(),
                ))),
            }
        } else {
            match pool.execute(norm_sql, params) {
                Ok(affected) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req.request_id, affected as i64, None),
                )),
                Err(e) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                    req.request_id,
                    sqlite_err_class(&e),
                    e.to_string(),
                ))),
            }
        }
    }

    fn close_pool(&self, pool_id: u64) {
        if let Some(pool) = self.pools.write().remove(&pool_id) {
            pool.close();
        }
        self.release_pool_shm(pool_id);
    }
}

/// Return a PyCapsule wrapping a BrokerPoolBackend for SQLite.
#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>> {
    new_broker_engine_pycapsule(py, Arc::new(SqliteBrokerBackend::new()))
}

#[allow(clippy::too_many_arguments)]
fn parse_config(
    path: &str,
    wal_mode: bool,
    synchronous: &str,
    mmap_size: u64,
    busy_timeout_ms: u32,
    max_readers: usize,
    foreign_keys: bool,
    enable_jsonb: bool,
) -> Result<SqliteClientConfig, SqliteClientError> {
    let sync_parsed: SqliteSynchronous = synchronous.parse()?;
    Ok(SqliteClientConfig {
        path: path.to_string(),
        wal_mode,
        synchronous: sync_parsed,
        mmap_size,
        busy_timeout_ms,
        max_readers,
        foreign_keys,
        enable_jsonb,
    })
}

fn extract_operations(
    operations: &Bound<'_, PyAny>,
) -> Result<Vec<(bool, String, BoundParams)>, SqliteClientError> {
    let seq = operations.clone().cast_into::<PySequence>().map_err(|_| {
        SqliteClientError::InvalidInput("operations must be a sequence of tuples".to_string())
    })?;
    let len = seq.len().map_err(|e| {
        SqliteClientError::InvalidInput(format!("failed to get operations len: {e}"))
    })?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let item = seq.get_item(i).map_err(|e| {
            SqliteClientError::InvalidInput(format!("failed to get operation {i}: {e}"))
        })?;
        let tuple = item.cast_into::<PyTuple>().map_err(|_| {
            SqliteClientError::InvalidInput(format!("operation {i} must be a tuple"))
        })?;
        if tuple.len() != 3 {
            return Err(SqliteClientError::InvalidInput(format!(
                "operation {i} must be a 3-tuple (is_query, sql, params)"
            )));
        }
        let is_query: bool = tuple
            .get_item(0)
            .and_then(|v| v.extract())
            .map_err(|e| SqliteClientError::InvalidInput(format!("operation {i} is_query: {e}")))?;
        let sql: String = tuple
            .get_item(1)
            .and_then(|v| v.extract())
            .map_err(|e| SqliteClientError::InvalidInput(format!("operation {i} sql: {e}")))?;
        let params_obj = tuple.get_item(2).ok();
        let params = BoundParams::from_py(params_obj.as_ref())?;
        out.push((is_query, sql, params));
    }
    Ok(out)
}

fn extract_params_sequence(
    params_seq: &Bound<'_, PyAny>,
) -> Result<Vec<BoundParams>, SqliteClientError> {
    let seq = params_seq.clone().cast_into::<PySequence>().map_err(|_| {
        SqliteClientError::InvalidInput("params_seq must be a sequence".to_string())
    })?;
    let len = seq.len().map_err(|e| {
        SqliteClientError::InvalidInput(format!("failed to get params_seq len: {e}"))
    })?;
    let mut out = Vec::with_capacity(len);
    for i in 0..len {
        let item = seq.get_item(i).map_err(|e| {
            SqliteClientError::InvalidInput(format!("failed to get param item {i}: {e}"))
        })?;
        out.push(BoundParams::from_py(Some(&item))?);
    }
    Ok(out)
}

fn arrow_payload_to_py_cursor(py: Python<'_>, payload: ArrowResultPayload) -> PyResult<Py<PyAny>> {
    match payload {
        ArrowResultPayload::Shm {
            shm_name,
            byte_len,
            row_count,
            region,
        } => construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region),
        ArrowResultPayload::Inline {
            ipc_bytes,
            row_count,
            ..
        } => {
            static INLINE_CURSOR_SEQ: std::sync::atomic::AtomicU64 =
                std::sync::atomic::AtomicU64::new(1);
            let seq = INLINE_CURSOR_SEQ.fetch_add(1, Ordering::Relaxed);
            let shm_name = format!("/vtb_sqlite_inline_{}_{seq}", std::process::id());
            let (name, len, region) = write_shm_arrow_region_unregistered(&shm_name, &ipc_bytes)
                .map_err(SqliteClientError::ShmArrow)
                .map_err(|e| e.into_py_err(py))?;
            construct_py_arrow_cursor(py, &name, len, row_count, region)
        }
    }
}

/// Reason a cursor rejects a call because of its own state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CursorStateError {
    Closed,
    InvalidArraysize,
    NegativeFetchSize,
}

/// Buffered result and PEP 249 attributes shared by the sync and async cursors.
struct CursorState {
    closed: bool,
    columns: Option<Vec<String>>,
    description: Option<Py<PyTuple>>,
    rows: VecDeque<Vec<SqliteValue>>,
    rowcount: i64,
    lastrowid: Option<i64>,
    arraysize: usize,
}

impl CursorState {
    fn new() -> Self {
        Self {
            closed: false,
            columns: None,
            description: None,
            rows: VecDeque::new(),
            rowcount: -1,
            lastrowid: None,
            arraysize: 1,
        }
    }

    fn require_open(&self) -> Result<(), CursorStateError> {
        if self.closed {
            Err(CursorStateError::Closed)
        } else {
            Ok(())
        }
    }

    /// Replace the buffered result with `outcome` and drop the cached description.
    fn install(&mut self, outcome: StatementOutcome) {
        self.columns = outcome.columns;
        self.description = None;
        self.rows = VecDeque::from(outcome.rows);
        self.rowcount = outcome.rowcount;
        self.lastrowid = outcome.lastrowid;
    }

    /// Drop the buffered result and reset the statement attributes.
    fn clear_result(&mut self) {
        self.columns = None;
        self.description = None;
        self.rows = VecDeque::new();
        self.rowcount = -1;
        self.lastrowid = None;
    }

    fn take_one(&mut self) -> Result<Option<Vec<SqliteValue>>, CursorStateError> {
        self.require_open()?;
        Ok(self.rows.pop_front())
    }

    fn take_many(&mut self, size: Option<i64>) -> Result<Vec<Vec<SqliteValue>>, CursorStateError> {
        self.require_open()?;
        let wanted = match size {
            None => self.arraysize,
            Some(n) => usize::try_from(n).map_err(|_| CursorStateError::NegativeFetchSize)?,
        };
        let count = wanted.min(self.rows.len());
        Ok(self.rows.drain(..count).collect())
    }

    fn take_all(&mut self) -> Result<Vec<Vec<SqliteValue>>, CursorStateError> {
        self.require_open()?;
        Ok(std::mem::take(&mut self.rows).into())
    }

    fn set_arraysize(&mut self, value: i64) -> Result<(), CursorStateError> {
        match usize::try_from(value) {
            Ok(size) if size >= 1 => {
                self.arraysize = size;
                Ok(())
            }
            _ => Err(CursorStateError::InvalidArraysize),
        }
    }

    /// Close the cursor; a second call changes nothing.
    fn close(&mut self) {
        self.closed = true;
        self.clear_result();
    }
}

fn cursor_state_err(py: Python<'_>, err: CursorStateError) -> PyErr {
    let programming = |message: &str| SqliteClientError::Programming {
        message: message.to_string(),
        code: None,
        sqlstate: None,
    };
    let client_err = match err {
        CursorStateError::Closed => SqliteClientError::InvalidInput("cursor is closed".to_string()),
        CursorStateError::InvalidArraysize => programming("arraysize must be at least 1"),
        CursorStateError::NegativeFetchSize => programming("fetchmany size must not be negative"),
    };
    client_err.into_py_err(py)
}

fn row_to_py_tuple<'py>(py: Python<'py>, row: Vec<SqliteValue>) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(py, row.into_iter().map(|value| value.into_py(py)))
}

fn rows_to_py_list<'py>(
    py: Python<'py>,
    rows: Vec<Vec<SqliteValue>>,
) -> PyResult<Bound<'py, PyList>> {
    let tuples = rows
        .into_iter()
        .map(|row| row_to_py_tuple(py, row))
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, tuples)
}

fn description_tuple<'py>(py: Python<'py>, columns: &[String]) -> PyResult<Bound<'py, PyTuple>> {
    let entries = columns
        .iter()
        .map(|name| {
            let name_obj = name.as_str().into_pyobject(py)?.into_any().unbind();
            PyTuple::new(
                py,
                [
                    name_obj,
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                    py.None(),
                ],
            )
        })
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, entries)
}

fn names_to_py_tuple<'py>(py: Python<'py>, columns: &[String]) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(
        py,
        columns
            .iter()
            .map(|name| PyString::intern(py, name.as_str())),
    )
}

fn outcome_to_rows<'py>(
    py: Python<'py>,
    outcome: StatementOutcome,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyTuple>)> {
    let Some(columns) = outcome.columns else {
        return Ok((PyList::empty(py), PyTuple::empty(py)));
    };
    let names = names_to_py_tuple(py, &columns)?;
    let rows = rows_to_py_list(py, outcome.rows)?;
    Ok((rows, names))
}

fn outcome_to_dicts<'py>(
    py: Python<'py>,
    outcome: StatementOutcome,
) -> PyResult<Bound<'py, PyList>> {
    let Some(columns) = outcome.columns else {
        return Ok(PyList::empty(py));
    };
    let keys = names_to_py_tuple(py, &columns)?;
    let mut dicts = Vec::with_capacity(outcome.rows.len());
    for row in outcome.rows {
        let dict = PyDict::new(py);
        for (key, value) in keys.iter_borrowed().zip(row) {
            dict.set_item(key, value.into_py(py))?;
        }
        dicts.push(dict);
    }
    PyList::new(py, dicts)
}

fn outcome_to_value(
    py: Python<'_>,
    outcome: StatementOutcome,
) -> Result<Py<PyAny>, SqliteClientError> {
    if outcome.columns.is_none() {
        return Err(SqliteClientError::NotFound(
            "Query returned no rows".to_string(),
        ));
    }
    let Some(first_row) = outcome.rows.into_iter().next() else {
        return Err(SqliteClientError::NotFound(
            "Query returned no rows".to_string(),
        ));
    };
    Ok(first_row
        .into_iter()
        .next()
        .map_or_else(|| py.None(), |value| value.into_py(py)))
}

const fn outcome_to_dml(outcome: &StatementOutcome) -> (i64, Option<i64>) {
    let changes = if outcome.rowcount < 0 {
        0
    } else {
        outcome.rowcount
    };
    (changes, outcome.lastrowid)
}

/// Held execute lock that can cross the `py.detach` boundary.
///
/// A `parking_lot` guard is not `Send`, so it cannot be returned from a detached closure.
/// This gate takes the lock while detached and releases it on drop, after the caller has
/// installed or cleared the result with the interpreter attached.
struct ExecuteGate<'a>(&'a Mutex<()>);

impl<'a> ExecuteGate<'a> {
    fn acquire(lock: &'a Mutex<()>) -> Self {
        std::mem::forget(lock.lock());
        Self(lock)
    }
}

impl Drop for ExecuteGate<'_> {
    fn drop(&mut self) {
        // SAFETY: `acquire` locked this mutex and forgot its guard, so this gate owns the lock.
        unsafe { self.0.force_unlock() }
    }
}

/// Synchronous PEP 249 cursor over a native SQLite worker pool.
#[pyclass(name = "SyncSQLiteCursor", frozen, immutable_type)]
pub struct SyncSQLiteCursor {
    pool: Arc<SqlitePool>,
    state: Mutex<CursorState>,
    execute_lock: Mutex<()>,
    lazy_begin: bool,
}

impl SyncSQLiteCursor {
    fn new(pool: Arc<SqlitePool>, lazy_begin: bool) -> Self {
        Self {
            pool,
            state: Mutex::new(CursorState::new()),
            execute_lock: Mutex::new(()),
            lazy_begin,
        }
    }

    fn require_open(&self, py: Python<'_>) -> PyResult<()> {
        self.state
            .lock()
            .require_open()
            .map_err(|e| cursor_state_err(py, e))
    }

    /// Clear the buffered result and convert `err` for raising.
    fn fail(&self, py: Python<'_>, err: SqliteClientError) -> PyErr {
        self.state.lock().clear_result();
        err.into_py_err(py)
    }

    /// Run one statement call under the execute lock and leave its outcome on the cursor.
    ///
    /// The execute lock is taken with the interpreter detached and released after the
    /// result is installed or cleared. The state lock is taken only after re-attaching.
    fn run_and_install<F>(&self, py: Python<'_>, begin: bool, run: F) -> PyResult<()>
    where
        F: FnOnce(&SqlitePool) -> Result<StatementOutcome, SqliteClientError> + Send,
    {
        let pool = &self.pool;
        let execute_lock = &self.execute_lock;
        let (gate, result) = py.detach(move || {
            let gate = ExecuteGate::acquire(execute_lock);
            let result = if begin && !pool.in_transaction() {
                pool.begin("IMMEDIATE".to_string()).and_then(|()| run(pool))
            } else {
                run(pool)
            };
            (gate, result)
        });
        let mut state = self.state.lock();
        let raised = match result {
            Ok(outcome) => {
                if state.closed {
                    state.clear_result();
                } else {
                    state.install(outcome);
                }
                None
            }
            Err(err) => {
                state.clear_result();
                Some(err)
            }
        };
        drop(state);
        drop(gate);
        raised.map_or(Ok(()), |err| Err(err.into_py_err(py)))
    }
}

#[pymethods]
impl SyncSQLiteCursor {
    #[getter]
    pub fn description(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        let columns = {
            let state = self.state.lock();
            if let Some(cached) = &state.description {
                return Ok(Some(cached.clone_ref(py)));
            }
            match &state.columns {
                Some(columns) => columns.clone(),
                None => return Ok(None),
            }
        };
        let built = description_tuple(py, &columns)?.unbind();
        let mut state = self.state.lock();
        if let Some(cached) = &state.description {
            return Ok(Some(cached.clone_ref(py)));
        }
        if state.columns.as_deref() == Some(columns.as_slice()) {
            state.description = Some(built.clone_ref(py));
        }
        Ok(Some(built))
    }

    #[getter]
    pub fn rowcount(&self) -> i64 {
        self.state.lock().rowcount
    }

    #[getter]
    pub fn lastrowid(&self) -> Option<i64> {
        self.state.lock().lastrowid
    }

    #[getter]
    pub fn arraysize(&self) -> usize {
        self.state.lock().arraysize
    }

    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state
            .lock()
            .set_arraysize(value)
            .map_err(|e| cursor_state_err(py, e))
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.require_open(py)?;
        let bound = BoundParams::from_py(params).map_err(|e| this.fail(py, e))?;
        this.run_and_install(py, this.lazy_begin, move |pool| pool.run(sql, bound))?;
        Ok(slf.clone())
    }

    pub fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.require_open(py)?;
        let params_seq = extract_params_sequence(seq_of_params).map_err(|e| this.fail(py, e))?;
        let begin = this.lazy_begin && !params_seq.is_empty();
        let sql_owned = sql.to_string();
        this.run_and_install(py, begin, move |pool| pool.run_many(sql_owned, params_seq))?;
        Ok(slf.clone())
    }

    pub fn fetchone<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let row = self
            .state
            .lock()
            .take_one()
            .map_err(|e| cursor_state_err(py, e))?;
        row.map(|values| row_to_py_tuple(py, values)).transpose()
    }

    #[pyo3(signature = (size = None))]
    pub fn fetchmany<'py>(
        &self,
        py: Python<'py>,
        size: Option<i64>,
    ) -> PyResult<Bound<'py, PyList>> {
        let rows = self
            .state
            .lock()
            .take_many(size)
            .map_err(|e| cursor_state_err(py, e))?;
        rows_to_py_list(py, rows)
    }

    pub fn fetchall<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let rows = self
            .state
            .lock()
            .take_all()
            .map_err(|e| cursor_state_err(py, e))?;
        rows_to_py_list(py, rows)
    }

    pub fn close(&self) {
        self.state.lock().close();
    }
}

/// In-flight marker of one async cursor statement.
///
/// The guard travels with the task output into the completion mapper, which installs or
/// clears the result through `settle_with`. A guard dropped unsettled (cancelled awaitable,
/// panicked task, failed bind, or failed spawn) clears the result itself. The in-flight flag
/// is released last, so a later statement never races this one for the cursor state.
struct ExecutingGuard {
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
    settled: Arc<AtomicBool>,
}

impl ExecutingGuard {
    fn settle_with(self, apply: impl FnOnce(&mut CursorState)) {
        let mut state = self.state.lock();
        apply(&mut state);
        self.settled.store(true, Ordering::Release);
    }
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if !self.settled.load(Ordering::Acquire) {
            let mut state = self.state.lock();
            state.clear_result();
            self.settled.store(true, Ordering::Release);
        }
        self.executing.store(false, Ordering::Release);
    }
}

/// Awaitable PEP 249-shaped cursor over a native SQLite worker pool.
#[pyclass(name = "AsyncSQLiteCursor", frozen, immutable_type)]
pub struct AsyncSQLiteCursor {
    pool: Arc<SqlitePool>,
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
    lazy_begin: bool,
}

impl AsyncSQLiteCursor {
    fn new(pool: Arc<SqlitePool>, lazy_begin: bool) -> Self {
        Self {
            pool,
            state: Arc::new(Mutex::new(CursorState::new())),
            executing: Arc::new(AtomicBool::new(false)),
            lazy_begin,
        }
    }

    /// Check that the cursor is open and idle, then mark it in flight.
    fn begin_statement(&self, py: Python<'_>) -> PyResult<ExecutingGuard> {
        self.state
            .lock()
            .require_open()
            .map_err(|e| cursor_state_err(py, e))?;
        if self
            .executing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(
                SqliteClientError::InvalidInput("cursor is already executing".to_string())
                    .into_py_err(py),
            );
        }
        Ok(ExecutingGuard {
            state: Arc::clone(&self.state),
            executing: Arc::clone(&self.executing),
            settled: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Spawn one statement call and resolve its awaitable to the cursor object.
    ///
    /// Cancelling the awaitable interrupts the operation and clears the result at once;
    /// the completion mapper does not run for a cancelled awaitable.
    fn spawn_statement<'py, F>(
        slf: &Bound<'py, Self>,
        guard: ExecutingGuard,
        begin: bool,
        run: F,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
    where
        F: FnOnce(&SqlitePool, u64) -> Result<StatementOutcome, SqliteClientError> + Send + 'static,
    {
        let this = slf.get();
        let pool = Arc::clone(&this.pool);
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);
        let cancel_state = Arc::clone(&this.state);
        let cancel_settled = Arc::clone(&guard.settled);
        let cursor = slf.clone().unbind();

        spawn_rust_db_task_with_attr_and_cancel(
            slf.py(),
            async move {
                let joined = tokio::task::spawn_blocking(move || {
                    if begin && !pool.in_transaction() {
                        pool.begin_with_op_id(op_id, "IMMEDIATE".to_string())?;
                    }
                    run(&pool, op_id)
                })
                .await;
                let result = joined.unwrap_or_else(|e| {
                    Err(SqliteClientError::Operational {
                        message: e.to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })
                });
                match result {
                    Ok(outcome) => Ok((outcome, guard)),
                    Err(err) => Err((err, guard)),
                }
            },
            move |_py, (outcome, guard): (StatementOutcome, ExecutingGuard)| {
                guard.settle_with(|state| {
                    if state.closed {
                        state.clear_result();
                    } else {
                        state.install(outcome);
                    }
                });
                Ok(cursor.into_any())
            },
            |py, (err, guard): (SqliteClientError, ExecutingGuard)| {
                guard.settle_with(CursorState::clear_result);
                err.into_py_err(py)
            },
            move || {
                cancel_pool.cancel_operation(op_id);
                let mut state = cancel_state.lock();
                if !cancel_settled.load(Ordering::Acquire) {
                    state.clear_result();
                }
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl AsyncSQLiteCursor {
    #[getter]
    pub fn description(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        let columns = {
            let state = self.state.lock();
            if let Some(cached) = &state.description {
                return Ok(Some(cached.clone_ref(py)));
            }
            match &state.columns {
                Some(columns) => columns.clone(),
                None => return Ok(None),
            }
        };
        let built = description_tuple(py, &columns)?.unbind();
        let mut state = self.state.lock();
        if let Some(cached) = &state.description {
            return Ok(Some(cached.clone_ref(py)));
        }
        if state.columns.as_deref() == Some(columns.as_slice()) {
            state.description = Some(built.clone_ref(py));
        }
        Ok(Some(built))
    }

    #[getter]
    pub fn rowcount(&self) -> i64 {
        self.state.lock().rowcount
    }

    #[getter]
    pub fn lastrowid(&self) -> Option<i64> {
        self.state.lock().lastrowid
    }

    #[getter]
    pub fn arraysize(&self) -> usize {
        self.state.lock().arraysize
    }

    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state
            .lock()
            .set_arraysize(value)
            .map_err(|e| cursor_state_err(py, e))
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let this = slf.get();
        let guard = this.begin_statement(py)?;
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let sql_owned = sql.to_string();
        Self::spawn_statement(slf, guard, this.lazy_begin, move |pool, op_id| {
            pool.run_with_op_id(op_id, &sql_owned, bound)
        })
    }

    pub fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let this = slf.get();
        let guard = this.begin_statement(py)?;
        let params_seq = extract_params_sequence(seq_of_params).map_err(|e| e.into_py_err(py))?;
        let begin = this.lazy_begin && !params_seq.is_empty();
        let sql_owned = sql.to_string();
        Self::spawn_statement(slf, guard, begin, move |pool, op_id| {
            pool.run_many_with_op_id(op_id, sql_owned, params_seq)
        })
    }

    pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let row = self
            .state
            .lock()
            .take_one()
            .map_err(|e| cursor_state_err(py, e))?;
        let value = match row {
            Some(values) => row_to_py_tuple(py, values)?.into_any().unbind(),
            None => py.None(),
        };
        ready_ok(py, value)
    }

    #[pyo3(signature = (size = None))]
    pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = self
            .state
            .lock()
            .take_many(size)
            .map_err(|e| cursor_state_err(py, e))?;
        let value = rows_to_py_list(py, rows)?.into_any().unbind();
        ready_ok(py, value)
    }

    pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = self
            .state
            .lock()
            .take_all()
            .map_err(|e| cursor_state_err(py, e))?;
        let value = rows_to_py_list(py, rows)?.into_any().unbind();
        ready_ok(py, value)
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        self.state.lock().close();
        ready_ok(py, py.None())
    }
}

/// Synchronous native SQLite 3.45+ client bridged via dedicated worker threads.
#[pyclass(name = "SyncSQLiteClient", frozen, immutable_type)]
pub struct SyncSQLiteClient {
    pool: Arc<SqlitePool>,
    last_statement_rowid: AtomicI64,
}

impl SyncSQLiteClient {
    fn run_once(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<StatementOutcome> {
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let pool = &self.pool;
        let outcome = py
            .detach(|| pool.run(sql, bound))
            .map_err(|e| e.into_py_err(py))?;
        self.last_statement_rowid
            .store(outcome.lastrowid.unwrap_or(0), Ordering::Release);
        Ok(outcome)
    }
}

#[pymethods]
impl SyncSQLiteClient {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        path = ":memory:",
        *,
        wal_mode = true,
        synchronous = "NORMAL",
        mmap_size = 268435456,
        busy_timeout_ms = 5000,
        max_readers = 8,
        foreign_keys = true,
        enable_jsonb = true
    ))]
    pub fn new(
        py: Python<'_>,
        path: &str,
        wal_mode: bool,
        synchronous: &str,
        mmap_size: u64,
        busy_timeout_ms: u32,
        max_readers: usize,
        foreign_keys: bool,
        enable_jsonb: bool,
    ) -> PyResult<Self> {
        let config = parse_config(
            path,
            wal_mode,
            synchronous,
            mmap_size,
            busy_timeout_ms,
            max_readers,
            foreign_keys,
            enable_jsonb,
        )
        .map_err(|e| e.into_py_err(py))?;

        let pool = py
            .detach(|| SqlitePool::open(config))
            .map_err(|e| e.into_py_err(py))?;
        Ok(Self {
            pool,
            last_statement_rowid: AtomicI64::new(0),
        })
    }

    #[getter]
    pub fn path(&self) -> String {
        self.pool.path().to_string()
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.pool.in_transaction()
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.pool.is_closed()
    }

    #[getter]
    pub fn last_insert_rowid(&self) -> i64 {
        self.pool.last_insert_rowid()
    }

    #[getter]
    pub fn last_statement_rowid(&self) -> Option<i64> {
        match self.last_statement_rowid.load(Ordering::Acquire) {
            0 => None,
            rowid => Some(rowid),
        }
    }

    #[pyo3(signature = (sql, params = None))]
    pub fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<usize> {
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        py.detach(move || pool.execute(sql_owned, bound))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        params_seq: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let seq = extract_params_sequence(params_seq).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        py.detach(move || pool.execute_many(sql_owned, seq))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn execute_script(&self, py: Python<'_>, sql_script: &str) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        let script_owned = sql_script.to_string();
        py.detach(move || pool.execute_script(script_owned))
            .map_err(|e| e.into_py_err(py))
    }

    #[pyo3(signature = (operations, *, use_savepoint = false))]
    pub fn execute_pipeline(
        &self,
        py: Python<'_>,
        operations: &Bound<'_, PyAny>,
        use_savepoint: bool,
    ) -> PyResult<Py<PyList>> {
        let ops = extract_operations(operations).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let results = py
            .detach(move || pool.execute_pipeline(ops, use_savepoint))
            .map_err(|e| e.into_py_err(py))?;

        let py_list = PyList::empty(py);
        for item in results {
            py_list.append(item.into_py(py)?)?;
        }
        Ok(py_list.unbind())
    }

    #[pyo3(signature = (sql, params = None, *, batch_size = 8192))]
    pub fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<Py<PyAny>> {
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        let payload = py
            .detach(move || pool.select_to_arrow(sql_owned, bound, batch_size))
            .map_err(|e| e.into_py_err(py))?;
        arrow_payload_to_py_cursor(py, payload)
    }

    pub fn load_from_arrow(
        &self,
        py: Python<'_>,
        table_name: &str,
        arrow_ipc_bytes: &Bound<'_, PyAny>,
    ) -> PyResult<usize> {
        let bytes: Vec<u8> = arrow_ipc_bytes.extract()?;
        let pool = Arc::clone(&self.pool);
        let tbl_owned = table_name.to_string();
        py.detach(move || pool.load_from_arrow(tbl_owned, bytes))
            .map_err(|e| e.into_py_err(py))
    }

    #[pyo3(signature = (target_path, *, pages_per_step = 256, sleep_ms = 0))]
    pub fn backup_to(
        &self,
        py: Python<'_>,
        target_path: &str,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        let target_owned = target_path.to_string();
        py.detach(move || pool.backup_to(target_owned, pages_per_step, sleep_ms))
            .map_err(|e| e.into_py_err(py))
    }

    #[pyo3(signature = (source_path, *, pages_per_step = 256, sleep_ms = 0))]
    pub fn restore_from(
        &self,
        py: Python<'_>,
        source_path: &str,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        let source_owned = source_path.to_string();
        py.detach(move || pool.restore_from(source_owned, pages_per_step, sleep_ms))
            .map_err(|e| e.into_py_err(py))
    }

    #[pyo3(signature = (mode = "IMMEDIATE"))]
    pub fn begin(&self, py: Python<'_>, mode: &str) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        let mode_owned = mode.to_string();
        py.detach(move || pool.begin(mode_owned))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        py.detach(move || pool.commit())
            .map_err(|e| e.into_py_err(py))
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        py.detach(move || pool.rollback())
            .map_err(|e| e.into_py_err(py))
    }

    pub fn cancel(&self) -> PyResult<()> {
        self.pool.cancel();
        Ok(())
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let pool = Arc::clone(&self.pool);
        py.detach(move || pool.close());
        Ok(())
    }

    pub fn to_async(&self) -> AsyncSQLiteClient {
        AsyncSQLiteClient {
            pool: Arc::clone(&self.pool),
            last_statement_rowid: Arc::new(AtomicI64::new(0)),
        }
    }

    #[pyo3(signature = (*, lazy_begin = false))]
    pub fn cursor(&self, lazy_begin: bool) -> SyncSQLiteCursor {
        SyncSQLiteCursor::new(Arc::clone(&self.pool), lazy_begin)
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyTuple>)> {
        let outcome = self.run_once(py, sql, params)?;
        outcome_to_rows(py, outcome)
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_dicts<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyList>> {
        let outcome = self.run_once(py, sql, params)?;
        outcome_to_dicts(py, outcome)
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_value(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let outcome = self.run_once(py, sql, params)?;
        outcome_to_value(py, outcome).map_err(|e| e.into_py_err(py))
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute_dml(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<(i64, Option<i64>)> {
        let outcome = self.run_once(py, sql, params)?;
        Ok(outcome_to_dml(&outcome))
    }
}

/// Asynchronous native SQLite 3.45+ client bridged via Tokio and completion drainer.
#[pyclass(name = "AsyncSQLiteClient", frozen, immutable_type)]
pub struct AsyncSQLiteClient {
    pool: Arc<SqlitePool>,
    last_statement_rowid: Arc<AtomicI64>,
}

impl AsyncSQLiteClient {
    fn spawn_single_call<'py, M>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        map_ok: M,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>>
    where
        M: FnOnce(Python<'_>, StatementOutcome) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);
        let last_rowid = Arc::clone(&self.last_statement_rowid);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || pool.run_with_op_id(op_id, &sql_owned, bound))
                    .await
                    .map_err(|e| SqliteClientError::Operational {
                        message: e.to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })?
            },
            move |py, outcome| {
                last_rowid.store(outcome.lastrowid.unwrap_or(0), Ordering::Release);
                map_ok(py, outcome)
            },
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl AsyncSQLiteClient {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        path = ":memory:",
        *,
        wal_mode = true,
        synchronous = "NORMAL",
        mmap_size = 268435456,
        busy_timeout_ms = 5000,
        max_readers = 8,
        foreign_keys = true,
        enable_jsonb = true
    ))]
    pub fn new(
        py: Python<'_>,
        path: &str,
        wal_mode: bool,
        synchronous: &str,
        mmap_size: u64,
        busy_timeout_ms: u32,
        max_readers: usize,
        foreign_keys: bool,
        enable_jsonb: bool,
    ) -> PyResult<Self> {
        let config = parse_config(
            path,
            wal_mode,
            synchronous,
            mmap_size,
            busy_timeout_ms,
            max_readers,
            foreign_keys,
            enable_jsonb,
        )
        .map_err(|e| e.into_py_err(py))?;

        let pool = py
            .detach(|| SqlitePool::open(config))
            .map_err(|e| e.into_py_err(py))?;
        Ok(Self {
            pool,
            last_statement_rowid: Arc::new(AtomicI64::new(0)),
        })
    }

    #[getter]
    pub fn path(&self) -> String {
        self.pool.path().to_string()
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.pool.in_transaction()
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.pool.is_closed()
    }

    #[getter]
    pub fn last_insert_rowid(&self) -> i64 {
        self.pool.last_insert_rowid()
    }

    #[getter]
    pub fn last_statement_rowid(&self) -> Option<i64> {
        match self.last_statement_rowid.load(Ordering::Acquire) {
            0 => None,
            rowid => Some(rowid),
        }
    }

    pub fn cancel(&self) -> PyResult<()> {
        self.pool.cancel();
        Ok(())
    }

    #[pyo3(signature = (sql, params = None))]
    pub fn execute<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.execute_with_op_id(op_id, sql_owned, bound)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn execute_many<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params_seq: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let seq = extract_params_sequence(params_seq).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.execute_many_with_op_id(op_id, sql_owned, seq)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn execute_script<'py>(
        &self,
        py: Python<'py>,
        sql_script: &str,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pool = Arc::clone(&self.pool);
        let script_owned = sql_script.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.execute_script_with_op_id(op_id, script_owned)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (operations, *, use_savepoint = false))]
    pub fn execute_pipeline<'py>(
        &self,
        py: Python<'py>,
        operations: &Bound<'_, PyAny>,
        use_savepoint: bool,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let ops = extract_operations(operations).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.execute_pipeline_with_op_id(op_id, ops, use_savepoint)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, results| {
                let py_list = PyList::empty(py);
                for item in results {
                    py_list.append(item.into_py(py)?)?;
                }
                Ok(py_list.unbind().into_any())
            },
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, params = None, *, batch_size = 8192))]
    pub fn select_to_arrow<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.select_to_arrow_with_op_id(op_id, sql_owned, bound, batch_size)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            arrow_payload_to_py_cursor,
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn load_from_arrow<'py>(
        &self,
        py: Python<'py>,
        table_name: &str,
        arrow_ipc_bytes: &Bound<'_, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let bytes: Vec<u8> = arrow_ipc_bytes.extract()?;
        let pool = Arc::clone(&self.pool);
        let tbl_owned = table_name.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.load_from_arrow_with_op_id(op_id, tbl_owned, bytes)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (target_path, *, pages_per_step = 256, sleep_ms = 0))]
    pub fn backup_to<'py>(
        &self,
        py: Python<'py>,
        target_path: &str,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pool = Arc::clone(&self.pool);
        let target_owned = target_path.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.backup_to_with_op_id(op_id, target_owned, pages_per_step, sleep_ms)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (source_path, *, pages_per_step = 256, sleep_ms = 0))]
    pub fn restore_from<'py>(
        &self,
        py: Python<'py>,
        source_path: &str,
        pages_per_step: i32,
        sleep_ms: u64,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pool = Arc::clone(&self.pool);
        let source_owned = source_path.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.restore_from_with_op_id(op_id, source_owned, pages_per_step, sleep_ms)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (mode = "IMMEDIATE"))]
    pub fn begin<'py>(
        &self,
        py: Python<'py>,
        mode: &str,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pool = Arc::clone(&self.pool);
        let mode_owned = mode.to_string();
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || pool.begin_with_op_id(op_id, mode_owned))
                    .await
                    .map_err(|e| SqliteClientError::Operational {
                        message: e.to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn commit<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pool = Arc::clone(&self.pool);
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || pool.commit_with_op_id(op_id))
                    .await
                    .map_err(|e| SqliteClientError::Operational {
                        message: e.to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn rollback<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pool = Arc::clone(&self.pool);
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || pool.rollback_with_op_id(op_id))
                    .await
                    .map_err(|e| SqliteClientError::Operational {
                        message: e.to_string(),
                        code: None,
                        sqlstate: Some("08000"),
                        is_transient: false,
                    })?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn close<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let pool = Arc::clone(&self.pool);
        let op_id = pool.next_op_id();
        let cancel_pool = Arc::clone(&pool);

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                tokio::task::spawn_blocking(move || {
                    pool.close();
                    Ok::<(), SqliteClientError>(())
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            move || {
                cancel_pool.cancel_operation(op_id);
            },
            SQLITE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn to_sync(&self) -> SyncSQLiteClient {
        SyncSQLiteClient {
            pool: Arc::clone(&self.pool),
            last_statement_rowid: AtomicI64::new(0),
        }
    }

    #[pyo3(signature = (*, lazy_begin = false))]
    pub fn cursor(&self, lazy_begin: bool) -> AsyncSQLiteCursor {
        AsyncSQLiteCursor::new(Arc::clone(&self.pool), lazy_begin)
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_rows<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_single_call(py, sql, params, |py, outcome| {
            let (rows, names) = outcome_to_rows(py, outcome)?;
            Ok((rows, names).into_pyobject(py)?.into_any().unbind())
        })
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_dicts<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_single_call(py, sql, params, |py, outcome| {
            Ok(outcome_to_dicts(py, outcome)?.into_any().unbind())
        })
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_value<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_single_call(py, sql, params, |py, outcome| {
            outcome_to_value(py, outcome).map_err(|e| e.into_py_err(py))
        })
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute_dml<'py>(
        &self,
        py: Python<'py>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        self.spawn_single_call(py, sql, params, |py, outcome| {
            Ok(outcome_to_dml(&outcome)
                .into_pyobject(py)?
                .into_any()
                .unbind())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vertebra_core::dbrpc::{DBRPC_VERSION, DbRpcHeader};
    use vertebra_db::arrow_shm::read_shm_arrow_frame;

    fn make_dbrpc_frame(
        op: DbRpcOp,
        request_id: u64,
        sql: Option<&str>,
        savepoint: Option<&str>,
    ) -> Vec<u8> {
        let hdr = DbRpcHeader::new(op, request_id, 0, 1);
        let mut frame = Vec::new();
        frame.extend_from_slice(&hdr.encode());
        frame.push(0x99); // 9-element array
        frame.push(DBRPC_VERSION as u8);
        frame.push(op as u8);
        frame.push(request_id as u8);
        frame.push(0);
        frame.push(1);
        match sql {
            Some(s) => {
                let b = s.as_bytes();
                frame.push(0xd9);
                frame.push(b.len() as u8);
                frame.extend_from_slice(b);
            }
            None => frame.push(0xc0),
        }
        frame.push(0x90); // empty params
        match savepoint {
            Some(sp) => {
                let b = sp.as_bytes();
                frame.push(0xd9);
                frame.push(b.len() as u8);
                frame.extend_from_slice(b);
            }
            None => frame.push(0xc0),
        }
        frame.push(0); // chunk_rows
        frame
    }

    #[test]
    fn test_sqlite_broker_retains_shm_and_dispatches_control_ops_without_sql() {
        let backend = SqliteBrokerBackend::new();
        let pool_id = backend.open_pool(":memory:", 2).unwrap();

        // Control ops without `req.sql` must succeed instead of failing with missing SQL.
        let begin_resp = backend
            .execute(pool_id, &make_dbrpc_frame(DbRpcOp::Begin, 1, None, None))
            .unwrap();
        assert!(!begin_resp.is_empty());

        let create_resp = backend
            .execute(
                pool_id,
                &make_dbrpc_frame(
                    DbRpcOp::Execute,
                    2,
                    Some("CREATE TABLE items (id INT PRIMARY KEY, name TEXT)"),
                    None,
                ),
            )
            .unwrap();
        assert!(!create_resp.is_empty());

        let sp_resp = backend
            .execute(
                pool_id,
                &make_dbrpc_frame(DbRpcOp::Savepoint, 3, None, Some("sp1")),
            )
            .unwrap();
        assert!(!sp_resp.is_empty());

        backend
            .execute(
                pool_id,
                &make_dbrpc_frame(
                    DbRpcOp::Execute,
                    4,
                    Some("INSERT INTO items VALUES (1, 'discarded')"),
                    None,
                ),
            )
            .unwrap();

        backend
            .execute(
                pool_id,
                &make_dbrpc_frame(DbRpcOp::Rollback, 5, None, Some("sp1")),
            )
            .unwrap();

        backend
            .execute(
                pool_id,
                &make_dbrpc_frame(
                    DbRpcOp::Execute,
                    6,
                    Some("INSERT INTO items VALUES (2, 'kept')"),
                    None,
                ),
            )
            .unwrap();

        backend
            .execute(pool_id, &make_dbrpc_frame(DbRpcOp::Commit, 7, None, None))
            .unwrap();

        // SELECT over DBRPC must retain the Arrow SHM region until explicit release or close.
        let sel_resp = backend
            .execute(
                pool_id,
                &make_dbrpc_frame(
                    DbRpcOp::Execute,
                    8,
                    Some("SELECT id, name FROM items"),
                    None,
                ),
            )
            .unwrap();
        assert_eq!(backend.active_shm_count(), 1);
        let shm_name = {
            let guard = backend.active_shm.lock();
            guard.keys().next().unwrap().clone()
        };
        assert!(!sel_resp.is_empty());
        let ipc = read_shm_arrow_frame(&shm_name).expect("SHM segment must remain attached");
        assert!(!ipc.is_empty());

        // Release via FetchChunk control frame with shm_name in savepoint.
        backend
            .execute(
                pool_id,
                &make_dbrpc_frame(DbRpcOp::FetchChunk, 9, None, Some(&shm_name)),
            )
            .unwrap();
        assert_eq!(backend.active_shm_count(), 0);

        backend
            .execute(pool_id, &make_dbrpc_frame(DbRpcOp::Cancel, 10, None, None))
            .unwrap();
        backend
            .execute(pool_id, &make_dbrpc_frame(DbRpcOp::Close, 11, None, None))
            .unwrap();
        assert!(backend.pools.read().is_empty());
    }
}
