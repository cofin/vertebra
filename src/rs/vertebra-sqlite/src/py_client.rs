//! PyO3 client bindings for `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use parking_lot::{Mutex, RwLock};
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PySequence, PyTuple};
use vertebra_core::broker::DIALECT_TAG_SQLITE;
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, normalize_shm_name, release_shm_arrow_frame,
    write_shm_arrow_region_unregistered,
};
use vertebra_db::broker_capsule::{BrokerEngineBackend, BrokerPoolBackend};
use vertebra_db::py_async::{
    RustDbTaskAwaitable, construct_py_arrow_cursor, spawn_rust_db_task_with_attr_and_cancel,
};
use vertebra_db::{
    DbRpcEncodedResponse, DbRpcScalar, PlaceholderStyle, decode_dbrpc_request_frame,
    encode_dbrpc_response_frame, new_broker_engine_pycapsule, normalize_placeholders,
};

use crate::arrow_bridge::ArrowResultPayload;
use crate::config::{SqliteClientConfig, SqliteSynchronous};
use crate::errors::SqliteClientError;
use crate::value::{BoundParams, SqliteValue};
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

/// Synchronous native SQLite 3.45+ client bridged via dedicated worker threads.
#[pyclass(name = "SyncSQLiteClient", frozen, immutable_type)]
pub struct SyncSQLiteClient {
    pool: Arc<SqlitePool>,
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
        Ok(Self { pool })
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

    #[pyo3(signature = (sql, params = None))]
    pub fn fetch_all(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound = BoundParams::from_py(params).map_err(|e| e.into_py_err(py))?;
        let pool = Arc::clone(&self.pool);
        let sql_owned = sql.to_string();
        let qr = py
            .detach(move || pool.fetch_all(sql_owned, bound))
            .map_err(|e| e.into_py_err(py))?;
        qr.into_py(py)
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
        }
    }
}

/// Asynchronous native SQLite 3.45+ client bridged via Tokio and completion drainer.
#[pyclass(name = "AsyncSQLiteClient", frozen, immutable_type)]
pub struct AsyncSQLiteClient {
    pool: Arc<SqlitePool>,
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
        Ok(Self { pool })
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

    #[pyo3(signature = (sql, params = None))]
    pub fn fetch_all<'py>(
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
                    pool.fetch_all_with_op_id(op_id, sql_owned, bound)
                })
                .await
                .map_err(|e| SqliteClientError::Operational {
                    message: e.to_string(),
                    code: None,
                    sqlstate: Some("08000"),
                    is_transient: false,
                })?
            },
            |py, qr| qr.into_py(py),
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
        }
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
