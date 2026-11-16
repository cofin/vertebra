//! PyO3 client bindings for `SyncSQLiteClient`, `AsyncSQLiteClient`, and `broker_engine_capsule`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use parking_lot::RwLock;
use pyo3::prelude::*;
use pyo3::types::{PyCapsule, PyList, PySequence, PyTuple};
use vertebra_core::broker::DIALECT_TAG_SQLITE;
use vertebra_db::arrow_shm::write_shm_arrow_region_unregistered;
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

/// Shared-memory broker backend for SQLite (`DIALECT_TAG_SQLITE = 3`).
pub struct SqliteBrokerBackend {
    pools: RwLock<HashMap<u64, Arc<SqlitePool>>>,
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
            next_pool_id: std::sync::atomic::AtomicU64::new(1),
        }
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
        let is_query = upper.starts_with("SELECT")
            || upper.starts_with("EXPLAIN")
            || upper.starts_with("WITH");

        if is_query {
            match pool.select_to_arrow(norm_sql, params, 8192) {
                Ok(ArrowResultPayload::Shm {
                    shm_name,
                    row_count,
                    ..
                }) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_arrow_shm(
                        req.request_id,
                        row_count as i64,
                        Vec::new(),
                        shm_name,
                    ),
                )),
                Ok(ArrowResultPayload::Inline { .. }) => {
                    Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                        req.request_id,
                        "SqliteError",
                        "inline arrow not supported over broker DBRPC".to_string(),
                    )))
                }
                Err(e) => Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
                    req.request_id,
                    "SqliteError",
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
                    "SqliteError",
                    e.to_string(),
                ))),
            }
        }
    }

    fn close_pool(&self, pool_id: u64) {
        if let Some(pool) = self.pools.write().remove(&pool_id) {
            pool.close();
        }
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
            let shm_seq = std::process::id();
            let shm_name = format!("/vtb_sqlite_inline_{shm_seq}");
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

        let pool = SqlitePool::open(config).map_err(|e| e.into_py_err(py))?;
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

    pub fn close(&self) -> PyResult<()> {
        self.pool.close();
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

        let pool = SqlitePool::open(config).map_err(|e| e.into_py_err(py))?;
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
                tokio::task::spawn_blocking(move || pool.execute(sql_owned, bound))
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
                tokio::task::spawn_blocking(move || pool.execute_many(sql_owned, seq))
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
                tokio::task::spawn_blocking(move || pool.execute_script(script_owned))
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
                tokio::task::spawn_blocking(move || pool.execute_pipeline(ops, use_savepoint))
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
                tokio::task::spawn_blocking(move || pool.fetch_all(sql_owned, bound))
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
                    pool.select_to_arrow(sql_owned, bound, batch_size)
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
                tokio::task::spawn_blocking(move || pool.load_from_arrow(tbl_owned, bytes))
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
                    pool.backup_to(target_owned, pages_per_step, sleep_ms)
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
                    pool.restore_from(source_owned, pages_per_step, sleep_ms)
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
                tokio::task::spawn_blocking(move || pool.begin(mode_owned))
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
                tokio::task::spawn_blocking(move || pool.commit())
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
                tokio::task::spawn_blocking(move || pool.rollback())
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
