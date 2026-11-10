//! Native DuckDB 2.0 client extension for Vertebra (`vertebra._duckdb`).
//!
//! Provides [`SyncDuckDBClient`] (bridged through [`SyncCompletionQueue`]) and
//! [`AsyncDuckDBClient`] (bridged through [`AsyncCompletionQueue`] + [`Notifier`]
//! via `vertebra_db::py_async::spawn_rust_db_task_with_attr` with
//! `"_vertebra_duckdb_completion_drainer"`), zero-copy `VTBARROW` shared-memory
//! Arrow IPC cursors, and native `duckdb::Appender` bulk ingestion.

pub mod errors;

use std::collections::HashMap;
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};

use arrow_array::RecordBatch;
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use duckdb::types::{TimeUnit as DuckTimeUnit, Value as DuckValue};
use duckdb::{AccessMode, Config, Connection, InterruptHandle, params_from_iter};
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple,
};
use vertebra_core::notify::SyncCompletionQueue;
use vertebra_core::shm::ShmRegion;
use vertebra_db::arrow_shm::{ShmArrowFrameHeader, ShmArrowStreamWriter};
use vertebra_db::duckdb::DuckdbConfig;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, construct_py_arrow_cursor, spawn_rust_db_task_with_attr,
};

use crate::errors::DuckDBClientError;

const DUCKDB_COMPLETION_DRAINER_ATTR: &str = "_vertebra_duckdb_completion_drainer";
static ARROW_SHM_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Whether this build targets free-threaded Python (3.14t+).
#[cfg(Py_GIL_DISABLED)]
pub const FREE_THREADED: bool = true;
#[cfg(not(Py_GIL_DISABLED))]
pub const FREE_THREADED: bool = false;

/// Parameter collection accepted by DuckDB statement execution.
#[derive(Clone, Debug, Default)]
pub enum BoundParams {
    /// No bound parameters.
    #[default]
    Empty,
    /// Positional parameters (`?`, `?1`, `$1`).
    Positional(Vec<DuckValue>),
    /// Named parameters (`$name` or `:name`).
    Named(HashMap<String, DuckValue>),
}

impl BoundParams {
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Positional(v) => v.is_empty(),
            Self::Named(m) => m.is_empty(),
        }
    }
}

/// Result row representation returned from worker threads before GIL re-acquisition.
#[derive(Clone, Debug)]
pub struct QueryResultSet {
    /// Ordered column names.
    pub columns: Vec<String>,
    /// Row values aligned with `columns`.
    pub rows: Vec<Vec<DuckValue>>,
}

/// Unregistered or inline Arrow IPC payload ready to wrap into `ArrowCursor`.
#[derive(Clone, Debug)]
pub enum ArrowResultPayload {
    /// Zero-copy `VTBARROW` shared-memory frame.
    Shm {
        shm_name: String,
        byte_len: usize,
        row_count: usize,
        region: Arc<ShmRegion>,
    },
    /// In-memory Arrow IPC stream bytes (used below `threshold_rows`).
    Inline {
        ipc_bytes: Vec<u8>,
        row_count: usize,
        columns: Vec<String>,
    },
}

/// Unregistered shared-memory Arrow IPC payload ready to wrap into `ArrowCursor`.
#[derive(Clone, Debug)]
pub struct ShmArrowPayload {
    pub shm_name: String,
    pub byte_len: usize,
    pub row_count: usize,
    pub region: Arc<ShmRegion>,
}

/// Bulk ingestion payload accepted by `load_from_arrow`.
#[derive(Clone, Debug)]
pub enum BulkLoadSource {
    /// Encapsulated Arrow IPC stream or file bytes.
    ArrowIpc(Vec<u8>),
    /// Row-oriented fallback values (`(column_names, rows)`).
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<DuckValue>>,
    },
}

enum WorkerTask {
    Run(Box<dyn FnOnce(&mut Connection) + Send + 'static>),
    Shutdown,
}

struct WorkerSlot {
    sender: mpsc::Sender<WorkerTask>,
    interrupt_handle: Arc<InterruptHandle>,
    busy: AtomicBool,
    active_cancel: Mutex<Option<Arc<AtomicBool>>>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
}

impl WorkerSlot {
    fn spawn(mut conn: Connection) -> Self {
        let interrupt_handle = conn.interrupt_handle();
        let (tx, rx) = mpsc::channel::<WorkerTask>();
        let join_handle = thread::Builder::new()
            .name("vtb-duckdb-worker".to_string())
            .spawn(move || {
                while let Ok(task) = rx.recv() {
                    match task {
                        WorkerTask::Run(job) => job(&mut conn),
                        WorkerTask::Shutdown => break,
                    }
                }
            })
            .expect("failed to spawn vtb-duckdb-worker thread");
        Self {
            sender: tx,
            interrupt_handle,
            busy: AtomicBool::new(false),
            active_cancel: Mutex::new(None),
            join_handle: Mutex::new(Some(join_handle)),
        }
    }

    fn interrupt_if_matching(&self, cancel_flag: &Arc<AtomicBool>) -> bool {
        let matches = self
            .active_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .as_ref()
            .is_some_and(|active| Arc::ptr_eq(active, cancel_flag));
        if matches {
            self.interrupt_handle.interrupt();
        }
        matches
    }

    fn clear_active_cancel(&self, cancel_flag: &Arc<AtomicBool>) {
        let mut active = self
            .active_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if active
            .as_ref()
            .is_some_and(|cur| Arc::ptr_eq(cur, cancel_flag))
        {
            *active = None;
        }
    }

    fn execute<T, F>(&self, cancel_flag: &Arc<AtomicBool>, f: F) -> Result<T, DuckDBClientError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, DuckDBClientError> + Send + 'static,
    {
        if cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB query cancelled before execution".to_string(),
            ));
        }
        let completion = Arc::new(SyncCompletionQueue::<Result<T, DuckDBClientError>>::new());
        let completion_writer = Arc::clone(&completion);
        let flag_clone = Arc::clone(cancel_flag);
        self.busy.store(true, Ordering::Release);
        *self
            .active_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(cancel_flag));
        let send_res = self.sender.send(WorkerTask::Run(Box::new(move |conn| {
            let outcome = if flag_clone.load(Ordering::Acquire) {
                Err(DuckDBClientError::QueryTimeout(
                    "DuckDB query interrupted".to_string(),
                ))
            } else {
                f(conn)
            };
            completion_writer.push(outcome);
        })));
        if send_res.is_err() {
            self.clear_active_cancel(cancel_flag);
            self.busy.store(false, Ordering::Release);
            return Err(DuckDBClientError::Operational(
                "DuckDB worker thread is closed".to_string(),
            ));
        }
        let outcome = completion.pop_blocking();
        self.clear_active_cancel(cancel_flag);
        self.busy.store(false, Ordering::Release);
        if outcome.is_err() && cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB query interrupted".to_string(),
            ));
        }
        outcome
    }

    fn shutdown(&self) {
        self.interrupt_handle.interrupt();
        let _ = self.sender.send(WorkerTask::Shutdown);
        if let Some(handle) = self
            .join_handle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        {
            let _ = handle.join();
        }
    }
}

#[derive(Clone, Debug)]
enum TxLogOp {
    Execute {
        sql: String,
        params: BoundParams,
    },
    ExecuteMany {
        sql: String,
        param_sets: Vec<BoundParams>,
    },
    LoadArrow {
        table: String,
        source: BulkLoadSource,
        columns: Option<Vec<String>>,
        create_if_missing: bool,
    },
}

#[derive(Default)]
struct TxJournal {
    ops: Vec<TxLogOp>,
    savepoints: Vec<(String, usize)>,
}

/// Shared multi-connection DuckDB session pool state.
pub struct DuckDBInner {
    database: String,
    read_only: bool,
    is_in_memory: bool,
    max_connections: usize,
    bootstrap_statements: Vec<String>,
    workers: Vec<Arc<WorkerSlot>>,
    reader_cursor: AtomicUsize,
    in_transaction: AtomicBool,
    closed: AtomicBool,
    active_cancels: Mutex<Vec<Arc<AtomicBool>>>,
    tx_journal: Mutex<TxJournal>,
}

impl std::fmt::Debug for DuckDBInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DuckDBInner")
            .field("database", &self.database)
            .field("read_only", &self.read_only)
            .field("is_in_memory", &self.is_in_memory)
            .field("max_connections", &self.max_connections)
            .field(
                "in_transaction",
                &self.in_transaction.load(Ordering::Acquire),
            )
            .field("closed", &self.closed.load(Ordering::Acquire))
            .finish()
    }
}

impl DuckDBInner {
    #[allow(clippy::too_many_arguments)]
    pub fn open(
        database: &str,
        read_only: bool,
        max_connections: usize,
        max_memory_bytes: Option<u64>,
        threads: Option<usize>,
        temp_directory: Option<String>,
        parquet_metadata_cache: bool,
        httpfs_connection_caching: bool,
        enable_http_metadata_cache: bool,
        custom_config: Option<HashMap<String, String>>,
        extensions: Option<Vec<String>>,
        init_statements: Option<Vec<String>>,
    ) -> Result<Arc<Self>, DuckDBClientError> {
        if database.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "DuckDB database path must not contain NUL bytes".to_string(),
            ));
        }
        if max_connections == 0 {
            return Err(DuckDBClientError::InvalidInput(
                "DuckDB max_connections must be >= 1".to_string(),
            ));
        }
        if let Some(ref td) = temp_directory
            && td.contains('\0')
        {
            return Err(DuckDBClientError::InvalidInput(
                "DuckDB temp_directory must not contain NUL bytes".to_string(),
            ));
        }
        if let Some(ref cfg_map) = custom_config {
            for (k, v) in cfg_map {
                if k.trim().is_empty() || k.contains('\0') || v.contains('\0') {
                    return Err(DuckDBClientError::InvalidInput(
                        "DuckDB config key and value must not be empty or contain NUL bytes"
                            .to_string(),
                    ));
                }
            }
        }
        if let Some(ref exts) = extensions {
            for ext in exts {
                if ext.contains('\0') {
                    return Err(DuckDBClientError::InvalidInput(
                        "DuckDB extension spec must not contain NUL bytes".to_string(),
                    ));
                }
            }
        }
        if let Some(ref inits) = init_statements {
            for stmt in inits {
                if stmt.contains('\0') {
                    return Err(DuckDBClientError::InvalidInput(
                        "DuckDB init_statement must not contain NUL bytes".to_string(),
                    ));
                }
            }
        }

        let trimmed = database.trim();
        let normalized_db = if trimmed.is_empty() {
            ":memory:".to_string()
        } else {
            trimmed.to_string()
        };
        let is_in_memory = normalized_db == ":memory:"
            || normalized_db.eq_ignore_ascii_case("memory")
            || normalized_db.starts_with(":memory:");
        let pool_size = max_connections;

        let memory_limit_mb = max_memory_bytes.map_or(2048, |bytes| {
            usize::try_from((bytes / (1024 * 1024)).max(64)).unwrap_or(2048)
        });
        let worker_threads = threads.unwrap_or(4).max(1);

        let duck_cfg = DuckdbConfig {
            path: normalized_db.clone(),
            memory_limit_mb,
            threads: worker_threads,
            parquet_metadata_cache,
            httpfs_connection_caching,
            enable_http_metadata_cache,
        };

        let mut bootstrap_stmts = duck_cfg.bootstrap_statements();
        if let Some(ref temp_dir) = temp_directory {
            let td = temp_dir.trim();
            if !td.is_empty() {
                let escaped = td.replace('\'', "''");
                bootstrap_stmts.push(format!("SET temp_directory = '{escaped}'"));
            }
        }

        let mut engine_config = Config::default();
        if read_only && !is_in_memory {
            engine_config = engine_config
                .access_mode(AccessMode::ReadOnly)
                .map_err(DuckDBClientError::from_duckdb)?;
        }
        if let Some(ref cfg_map) = custom_config {
            for (k, v) in cfg_map {
                let key_lower = k.trim().to_ascii_lowercase();
                if key_lower == "access_mode" && is_in_memory {
                    continue;
                }
                engine_config = engine_config
                    .with(k.as_str(), v.as_str())
                    .map_err(DuckDBClientError::from_duckdb)?;
            }
        }

        let primary_conn = if is_in_memory {
            Connection::open_in_memory_with_flags(engine_config)
                .map_err(DuckDBClientError::from_duckdb)?
        } else {
            Connection::open_with_flags(&normalized_db, engine_config)
                .map_err(DuckDBClientError::from_duckdb)?
        };

        let mut connections = Vec::with_capacity(pool_size);
        connections.push(primary_conn);
        for _ in 1..pool_size {
            let cloned = connections[0]
                .try_clone()
                .map_err(DuckDBClientError::from_duckdb)?;
            connections.push(cloned);
        }

        for conn in &mut connections {
            apply_session_bootstrap(
                conn,
                &bootstrap_stmts,
                extensions.as_deref(),
                init_statements.as_deref(),
            )?;
        }

        let workers = connections
            .into_iter()
            .map(|c| Arc::new(WorkerSlot::spawn(c)))
            .collect();

        Ok(Arc::new(Self {
            database: normalized_db,
            read_only,
            is_in_memory,
            max_connections: pool_size,
            bootstrap_statements: bootstrap_stmts,
            workers,
            reader_cursor: AtomicUsize::new(0),
            in_transaction: AtomicBool::new(false),
            closed: AtomicBool::new(false),
            active_cancels: Mutex::new(Vec::new()),
            tx_journal: Mutex::new(TxJournal::default()),
        }))
    }

    fn ensure_open(&self) -> Result<(), DuckDBClientError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DuckDBClientError::Operational(
                "DuckDB client is closed".to_string(),
            ));
        }
        Ok(())
    }

    fn clear_tx_journal(&self) {
        let mut journal = self
            .tx_journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        journal.ops.clear();
        journal.savepoints.clear();
    }

    fn record_tx_op(&self, op: TxLogOp) {
        if self.in_transaction.load(Ordering::Acquire) {
            let mut journal = self
                .tx_journal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            journal.ops.push(op);
        }
    }

    fn register_cancel_flag(&self) -> Arc<AtomicBool> {
        let flag = Arc::new(AtomicBool::new(false));
        let mut guard = self
            .active_cancels
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        guard.retain(|f| Arc::strong_count(f) > 1);
        guard.push(Arc::clone(&flag));
        flag
    }

    pub fn interrupt_matching(&self, cancel_flag: &Arc<AtomicBool>) {
        cancel_flag.store(true, Ordering::Release);
        for worker in &self.workers {
            worker.interrupt_if_matching(cancel_flag);
        }
    }

    pub fn interrupt_all(&self) {
        {
            let guard = self
                .active_cancels
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for flag in guard.iter() {
                flag.store(true, Ordering::Release);
            }
        }
        for worker in &self.workers {
            worker.interrupt_handle.interrupt();
        }
    }

    pub fn close_all(&self) {
        if !self.closed.swap(true, Ordering::AcqRel) {
            self.in_transaction.store(false, Ordering::Release);
            self.clear_tx_journal();
            self.interrupt_all();
            for worker in &self.workers {
                worker.shutdown();
            }
        }
    }

    fn select_worker(&self, is_read_only_query: bool) -> &Arc<WorkerSlot> {
        if !is_read_only_query
            || self.in_transaction.load(Ordering::Acquire)
            || self.workers.len() == 1
        {
            return &self.workers[0];
        }
        if !self.workers[0].busy.load(Ordering::Acquire) {
            return &self.workers[0];
        }
        let reader_count = self.workers.len() - 1;
        let start = self.reader_cursor.fetch_add(1, Ordering::Relaxed) % reader_count;
        for offset in 0..reader_count {
            let idx = 1 + ((start + offset) % reader_count);
            if !self.workers[idx].busy.load(Ordering::Acquire) {
                return &self.workers[idx];
            }
        }
        let fallback_idx = 1 + (start % reader_count);
        &self.workers[fallback_idx]
    }

    pub fn run_on_pool<T, F>(
        &self,
        is_read_only_query: bool,
        cancel_flag: &Arc<AtomicBool>,
        f: F,
    ) -> Result<T, DuckDBClientError>
    where
        T: Send + 'static,
        F: FnOnce(&mut Connection) -> Result<T, DuckDBClientError> + Send + 'static,
    {
        self.ensure_open()?;
        let worker = self.select_worker(is_read_only_query);
        worker.execute(cancel_flag, f)
    }

    pub fn execute_sql(
        &self,
        sql: String,
        params: BoundParams,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<i64, DuckDBClientError> {
        self.ensure_open()?;
        if sql.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "SQL statement must not contain NUL bytes".to_string(),
            ));
        }
        if self.read_only && is_mutating_sql(&sql) {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let trimmed_upper = trimmed.to_ascii_uppercase();
        if trimmed_upper == "BEGIN"
            || trimmed_upper == "BEGIN TRANSACTION"
            || trimmed_upper == "START TRANSACTION"
        {
            self.begin_tx(cancel_flag)?;
            return Ok(0);
        }
        if trimmed_upper == "COMMIT" || trimmed_upper == "END TRANSACTION" {
            self.commit_tx(cancel_flag)?;
            return Ok(0);
        }
        if trimmed_upper == "ROLLBACK" || trimmed_upper == "ABORT" {
            self.rollback_tx(cancel_flag)?;
            return Ok(0);
        }
        if trimmed_upper.starts_with("SAVEPOINT ") {
            let sp_name = trimmed["SAVEPOINT ".len()..].trim();
            self.savepoint_tx(sp_name)?;
            return Ok(0);
        }
        if trimmed_upper.starts_with("ROLLBACK TO ") {
            let after_to = trimmed["ROLLBACK TO ".len()..].trim();
            let sp_name = if after_to.to_ascii_uppercase().starts_with("SAVEPOINT ") {
                after_to["SAVEPOINT ".len()..].trim()
            } else {
                after_to
            };
            self.rollback_to_savepoint_tx(sp_name, cancel_flag)?;
            return Ok(0);
        }
        if trimmed_upper.starts_with("RELEASE ") {
            let after_rel = trimmed["RELEASE ".len()..].trim();
            let sp_name = if after_rel.to_ascii_uppercase().starts_with("SAVEPOINT ") {
                after_rel["SAVEPOINT ".len()..].trim()
            } else {
                after_rel
            };
            self.release_savepoint_tx(sp_name)?;
            return Ok(0);
        }

        let is_read = is_read_only_sql(&sql);
        let is_mut = is_mutating_sql(&sql);
        let log_sql = sql.clone();
        let log_params = params.clone();
        let changed = self.run_on_pool(is_read, cancel_flag, move |conn| {
            execute_statement_on_conn(conn, &sql, &params)
        })?;
        if is_mut || !is_read {
            self.record_tx_op(TxLogOp::Execute {
                sql: log_sql,
                params: log_params,
            });
        }
        Ok(changed)
    }

    pub fn execute_many_sql(
        &self,
        sql: String,
        param_sets: Vec<BoundParams>,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<i64, DuckDBClientError> {
        self.ensure_open()?;
        if sql.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "SQL statement must not contain NUL bytes".to_string(),
            ));
        }
        if self.read_only && is_mutating_sql(&sql) {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        if param_sets.is_empty() {
            return Ok(0);
        }
        let log_sql = sql.clone();
        let log_sets = param_sets.clone();
        let flag_for_worker = Arc::clone(cancel_flag);
        let total = self.run_on_pool(false, cancel_flag, move |conn| {
            let mut total: i64 = 0;
            for params in &param_sets {
                if flag_for_worker.load(Ordering::Acquire) {
                    return Err(DuckDBClientError::QueryTimeout(
                        "DuckDB execute_many interrupted".to_string(),
                    ));
                }
                total += execute_statement_on_conn(conn, &sql, params)?;
            }
            Ok(total)
        })?;
        self.record_tx_op(TxLogOp::ExecuteMany {
            sql: log_sql,
            param_sets: log_sets,
        });
        Ok(total)
    }

    pub fn query_sql(
        &self,
        sql: String,
        params: BoundParams,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<QueryResultSet, DuckDBClientError> {
        self.ensure_open()?;
        if sql.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "SQL statement must not contain NUL bytes".to_string(),
            ));
        }
        if self.read_only && is_mutating_sql(&sql) {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        let is_read = is_read_only_sql(&sql);
        let is_mut = is_mutating_sql(&sql);
        let log_sql = sql.clone();
        let log_params = params.clone();
        let flag_for_worker = Arc::clone(cancel_flag);
        let res = self.run_on_pool(is_read, cancel_flag, move |conn| {
            query_rows_on_conn(conn, &sql, &params, &flag_for_worker)
        })?;
        if is_mut {
            self.record_tx_op(TxLogOp::Execute {
                sql: log_sql,
                params: log_params,
            });
        }
        Ok(res)
    }

    pub fn query_arrow_payload(
        &self,
        sql: String,
        params: BoundParams,
        batch_size: usize,
        threshold_rows: usize,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<ArrowResultPayload, DuckDBClientError> {
        self.ensure_open()?;
        if sql.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "SQL statement must not contain NUL bytes".to_string(),
            ));
        }
        let is_mut = is_mutating_sql(&sql);
        if self.read_only && is_mut {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        let is_read = is_read_only_sql(&sql);
        let log_sql = sql.clone();
        let log_params = params.clone();
        let flag_for_worker = Arc::clone(cancel_flag);
        let payload = self.run_on_pool(is_read, cancel_flag, move |conn| {
            query_arrow_payload_on_conn(
                conn,
                &sql,
                &params,
                batch_size,
                threshold_rows,
                &flag_for_worker,
            )
        })?;
        if is_mut {
            self.record_tx_op(TxLogOp::Execute {
                sql: log_sql,
                params: log_params,
            });
        }
        Ok(payload)
    }

    pub fn query_arrow_shm(
        &self,
        sql: String,
        params: BoundParams,
        batch_size: usize,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<ShmArrowPayload, DuckDBClientError> {
        match self.query_arrow_payload(sql, params, batch_size, 0, cancel_flag)? {
            ArrowResultPayload::Shm {
                shm_name,
                byte_len,
                row_count,
                region,
            } => Ok(ShmArrowPayload {
                shm_name,
                byte_len,
                row_count,
                region,
            }),
            ArrowResultPayload::Inline { .. } => Err(DuckDBClientError::ShmArrow(
                "Expected shared-memory VTBARROW payload".to_string(),
            )),
        }
    }

    pub fn stream_arrow_shm_batches(
        &self,
        sql: String,
        params: BoundParams,
        batch_size: usize,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<Vec<ShmArrowPayload>, DuckDBClientError> {
        self.ensure_open()?;
        if sql.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "SQL statement must not contain NUL bytes".to_string(),
            ));
        }
        let is_mut = is_mutating_sql(&sql);
        if self.read_only && is_mut {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        let is_read = is_read_only_sql(&sql);
        let log_sql = sql.clone();
        let log_params = params.clone();
        let flag_for_worker = Arc::clone(cancel_flag);
        let payloads = self.run_on_pool(is_read, cancel_flag, move |conn| {
            stream_arrow_shm_batches_on_conn(conn, &sql, &params, batch_size, &flag_for_worker)
        })?;
        if is_mut {
            self.record_tx_op(TxLogOp::Execute {
                sql: log_sql,
                params: log_params,
            });
        }
        Ok(payloads)
    }

    pub fn load_arrow_bulk(
        &self,
        table: String,
        source: BulkLoadSource,
        columns: Option<Vec<String>>,
        create_if_missing: bool,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<i64, DuckDBClientError> {
        self.ensure_open()?;
        if table.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "Table name must not contain NUL bytes".to_string(),
            ));
        }
        if self.read_only {
            return Err(DuckDBClientError::Programming(
                "Cannot bulk load in read-only mode".to_string(),
            ));
        }
        let log_table = table.clone();
        let log_source = source.clone();
        let log_cols = columns.clone();
        let flag_for_worker = Arc::clone(cancel_flag);
        let loaded = self.run_on_pool(false, cancel_flag, move |conn| {
            load_arrow_on_conn(
                conn,
                &table,
                source,
                columns.as_deref(),
                create_if_missing,
                &flag_for_worker,
            )
        })?;
        self.record_tx_op(TxLogOp::LoadArrow {
            table: log_table,
            source: log_source,
            columns: log_cols,
            create_if_missing,
        });
        Ok(loaded)
    }

    pub fn begin_tx(&self, cancel_flag: &Arc<AtomicBool>) -> Result<(), DuckDBClientError> {
        self.ensure_open()?;
        if self.in_transaction.load(Ordering::Acquire) {
            return Err(DuckDBClientError::Transaction(
                "Cannot start a transaction within an active transaction".to_string(),
            ));
        }
        self.workers[0].execute(cancel_flag, |conn| {
            conn.execute_batch("BEGIN TRANSACTION")
                .map_err(DuckDBClientError::from_duckdb)
        })?;
        self.clear_tx_journal();
        self.in_transaction.store(true, Ordering::Release);
        Ok(())
    }

    pub fn commit_tx(&self, cancel_flag: &Arc<AtomicBool>) -> Result<(), DuckDBClientError> {
        self.ensure_open()?;
        let res = self.workers[0].execute(cancel_flag, |conn| {
            conn.execute_batch("COMMIT")
                .map_err(DuckDBClientError::from_duckdb)
        });
        let should_clear = match &res {
            Ok(()) => true,
            Err(err) => err
                .to_string()
                .to_ascii_lowercase()
                .contains("no transaction is active"),
        };
        if should_clear {
            self.clear_tx_journal();
            self.in_transaction.store(false, Ordering::Release);
        }
        res
    }

    pub fn rollback_tx(&self, cancel_flag: &Arc<AtomicBool>) -> Result<(), DuckDBClientError> {
        self.ensure_open()?;
        let res = self.workers[0].execute(cancel_flag, |conn| {
            conn.execute_batch("ROLLBACK")
                .map_err(DuckDBClientError::from_duckdb)
        });
        let should_clear = match &res {
            Ok(()) => true,
            Err(err) => err
                .to_string()
                .to_ascii_lowercase()
                .contains("no transaction is active"),
        };
        if should_clear {
            self.clear_tx_journal();
            self.in_transaction.store(false, Ordering::Release);
        }
        res
    }

    pub fn savepoint_tx(&self, name: &str) -> Result<(), DuckDBClientError> {
        self.ensure_open()?;
        validate_sql_identifier(name, "savepoint")?;
        if !self.in_transaction.load(Ordering::Acquire) {
            return Err(DuckDBClientError::Programming(
                "savepoint requires an active transaction".to_string(),
            ));
        }
        let clean = name.trim().to_string();
        let mut journal = self
            .tx_journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let op_idx = journal.ops.len();
        journal.savepoints.push((clean, op_idx));
        Ok(())
    }

    pub fn release_savepoint_tx(&self, name: &str) -> Result<(), DuckDBClientError> {
        self.ensure_open()?;
        validate_sql_identifier(name, "savepoint")?;
        if !self.in_transaction.load(Ordering::Acquire) {
            return Err(DuckDBClientError::Programming(
                "release_savepoint requires an active transaction".to_string(),
            ));
        }
        let clean = name.trim();
        let mut journal = self
            .tx_journal
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(pos) = journal
            .savepoints
            .iter()
            .rposition(|(sp_name, _)| sp_name == clean)
        else {
            return Err(DuckDBClientError::Programming(format!(
                "Savepoint {clean:?} does not exist"
            )));
        };
        journal.savepoints.truncate(pos);
        Ok(())
    }

    pub fn rollback_to_savepoint_tx(
        &self,
        name: &str,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<(), DuckDBClientError> {
        self.ensure_open()?;
        validate_sql_identifier(name, "savepoint")?;
        if !self.in_transaction.load(Ordering::Acquire) {
            return Err(DuckDBClientError::Programming(
                "rollback_to_savepoint requires an active transaction".to_string(),
            ));
        }
        let clean = name.trim();
        let replay_ops = {
            let mut journal = self
                .tx_journal
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let Some(pos) = journal
                .savepoints
                .iter()
                .rposition(|(sp_name, _)| sp_name == clean)
            else {
                return Err(DuckDBClientError::Programming(format!(
                    "Savepoint {clean:?} does not exist"
                )));
            };
            let target_op_len = journal.savepoints[pos].1;
            journal.savepoints.truncate(pos + 1);
            journal.ops.truncate(target_op_len);
            journal.ops.clone()
        };
        let flag_for_worker = Arc::clone(cancel_flag);
        self.workers[0].execute(cancel_flag, move |conn| {
            conn.execute_batch("ROLLBACK; BEGIN TRANSACTION")
                .map_err(DuckDBClientError::from_duckdb)?;
            for op in replay_ops {
                match op {
                    TxLogOp::Execute { sql, params } => {
                        execute_statement_on_conn(conn, &sql, &params)?;
                    }
                    TxLogOp::ExecuteMany { sql, param_sets } => {
                        for params in &param_sets {
                            execute_statement_on_conn(conn, &sql, params)?;
                        }
                    }
                    TxLogOp::LoadArrow {
                        table,
                        source,
                        columns,
                        create_if_missing,
                    } => {
                        load_arrow_on_conn(
                            conn,
                            &table,
                            source,
                            columns.as_deref(),
                            create_if_missing,
                            &flag_for_worker,
                        )?;
                    }
                }
            }
            Ok(())
        })
    }
}

impl Drop for DuckDBInner {
    fn drop(&mut self) {
        self.close_all();
    }
}

fn apply_session_bootstrap(
    conn: &mut Connection,
    bootstrap_stmts: &[String],
    extensions: Option<&[String]>,
    init_statements: Option<&[String]>,
) -> Result<(), DuckDBClientError> {
    for stmt in bootstrap_stmts {
        let lower = stmt.trim().to_ascii_lowercase();
        if lower.contains("memory_limit")
            || lower.contains("threads")
            || lower.contains("temp_directory")
        {
            conn.execute_batch(stmt)
                .map_err(DuckDBClientError::from_duckdb)?;
        } else {
            let _ = conn.execute_batch(stmt);
        }
    }
    if let Some(exts) = extensions {
        for ext in exts {
            let clean = ext.trim();
            if !clean.is_empty() {
                let parts: Vec<&str> = clean.split_whitespace().collect();
                if parts.len() == 3 && parts[1].eq_ignore_ascii_case("FROM") {
                    validate_sql_identifier(parts[0], "extension")?;
                    validate_sql_identifier(parts[2], "extension repository")?;
                    let install_load_sql =
                        format!("INSTALL {} FROM {}; LOAD {}", parts[0], parts[2], parts[0]);
                    conn.execute_batch(&install_load_sql)
                        .map_err(DuckDBClientError::from_duckdb)?;
                } else {
                    validate_sql_identifier(clean, "extension")?;
                    if conn.execute_batch(&format!("LOAD {clean}")).is_err() {
                        conn.execute_batch(&format!("INSTALL {clean}; LOAD {clean}"))
                            .map_err(DuckDBClientError::from_duckdb)?;
                    }
                }
            }
        }
    }
    if let Some(inits) = init_statements {
        for stmt in inits {
            let clean = stmt.trim();
            if !clean.is_empty() {
                conn.execute_batch(clean)
                    .map_err(DuckDBClientError::from_duckdb)?;
            }
        }
    }
    Ok(())
}

/// Validate a SQL identifier (extension name, savepoint, or table component) against injection.
pub fn validate_sql_identifier(ident: &str, label: &str) -> Result<(), DuckDBClientError> {
    if ident.contains('\0') {
        return Err(DuckDBClientError::InvalidInput(format!(
            "DuckDB {label} must not contain NUL bytes"
        )));
    }
    let trimmed = ident.trim();
    if trimmed.is_empty() {
        return Err(DuckDBClientError::InvalidInput(format!(
            "DuckDB {label} must not be empty"
        )));
    }
    let mut chars = trimmed.chars();
    let Some(first) = chars.next() else {
        return Err(DuckDBClientError::InvalidInput(format!(
            "DuckDB {label} must not be empty"
        )));
    };
    if !(first.is_ascii_alphabetic() || first == '_')
        || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    {
        return Err(DuckDBClientError::InvalidInput(format!(
            "Invalid DuckDB {label}: {ident:?}"
        )));
    }
    Ok(())
}

#[must_use]
fn is_read_only_sql(sql: &str) -> bool {
    if is_mutating_sql(sql) {
        return false;
    }
    let first_word = leading_sql_keyword(sql);
    matches!(
        first_word.as_str(),
        "SELECT" | "WITH" | "SHOW" | "DESCRIBE" | "DESC" | "EXPLAIN" | "SUMMARIZE" | "VALUES"
    )
}

#[must_use]
fn is_mutating_sql(sql: &str) -> bool {
    let tokens = scan_sql_keywords_outside_literals(sql);
    let Some(first) = tokens.first().map(String::as_str) else {
        return false;
    };
    if is_mutating_verb(first) {
        return true;
    }
    if first == "WITH" {
        return tokens.iter().skip(1).any(|tok| is_mutating_verb(tok));
    }
    false
}

#[must_use]
fn is_mutating_verb(word: &str) -> bool {
    matches!(
        word,
        "INSERT"
            | "UPDATE"
            | "DELETE"
            | "CREATE"
            | "DROP"
            | "ALTER"
            | "TRUNCATE"
            | "MERGE"
            | "COPY"
            | "VACUUM"
            | "ATTACH"
            | "DETACH"
    )
}

fn leading_sql_keyword(sql: &str) -> String {
    scan_sql_keywords_outside_literals(sql)
        .into_iter()
        .next()
        .unwrap_or_default()
}

fn scan_sql_keywords_outside_literals(sql: &str) -> Vec<String> {
    let bytes = sql.as_bytes();
    let mut tokens = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b.is_ascii_whitespace() {
            i += 1;
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            i = (i + 2).min(bytes.len());
            continue;
        }
        if b == b'\'' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\'' {
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'\'' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if b == b'"' {
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'"' {
                    i += 1;
                    if i < bytes.len() && bytes[i] == b'"' {
                        i += 1;
                    } else {
                        break;
                    }
                } else {
                    i += 1;
                }
            }
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            i += 1;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            tokens.push(sql[start..i].to_ascii_uppercase());
            continue;
        }
        i += 1;
    }
    tokens
}

/// Rewrite `:name` bind placeholders into `$name` outside string literals, quoted identifiers,
/// comments, and `::` type casts so DuckDB's native `$name` parser handles both syntaxes.
#[must_use]
pub fn normalize_named_sql(sql: &str) -> String {
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\'' {
            out.push('\'');
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c as char);
                i += 1;
                if c == b'\'' {
                    if i < bytes.len() && bytes[i] == b'\'' {
                        out.push('\'');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if b == b'"' {
            out.push('"');
            i += 1;
            while i < bytes.len() {
                let c = bytes[i];
                out.push(c as char);
                i += 1;
                if c == b'"' {
                    if i < bytes.len() && bytes[i] == b'"' {
                        out.push('"');
                        i += 1;
                    } else {
                        break;
                    }
                }
            }
            continue;
        }
        if b == b'-' && i + 1 < bytes.len() && bytes[i + 1] == b'-' {
            while i < bytes.len() && bytes[i] != b'\n' {
                out.push(bytes[i] as char);
                i += 1;
            }
            continue;
        }
        if b == b'/' && i + 1 < bytes.len() && bytes[i + 1] == b'*' {
            out.push('/');
            out.push('*');
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                out.push(bytes[i] as char);
                i += 1;
            }
            if i + 1 < bytes.len() {
                out.push('*');
                out.push('/');
                i += 2;
            }
            continue;
        }
        if b == b':' {
            if i + 1 < bytes.len() && (bytes[i + 1] == b':' || bytes[i + 1] == b'=') {
                out.push(':');
                out.push(bytes[i + 1] as char);
                i += 2;
                continue;
            }
            if i + 1 < bytes.len() && (bytes[i + 1].is_ascii_alphabetic() || bytes[i + 1] == b'_') {
                out.push('$');
                i += 1;
                while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                    out.push(bytes[i] as char);
                    i += 1;
                }
                continue;
            }
        }
        let ch = sql[i..].chars().next().unwrap_or('\0');
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

fn resolve_ordered_params<'a>(
    stmt: &duckdb::Statement<'_>,
    params: &'a BoundParams,
) -> Result<Vec<&'a DuckValue>, DuckDBClientError> {
    let expected = stmt.parameter_count();
    match params {
        BoundParams::Empty => {
            if expected != 0 {
                return Err(DuckDBClientError::Programming(format!(
                    "Statement expects {expected} parameters, but none were supplied"
                )));
            }
            Ok(Vec::new())
        }
        BoundParams::Positional(values) => {
            if expected == 0 && values.is_empty() {
                return Ok(Vec::new());
            }
            let mut has_explicit_indices = true;
            let mut resolved = Vec::with_capacity(expected);
            for idx in 1..=expected {
                if let Ok(name) = stmt.parameter_name(idx)
                    && let Ok(parsed_pos) = name.parse::<usize>()
                    && parsed_pos >= 1
                    && parsed_pos <= values.len()
                {
                    resolved.push(&values[parsed_pos - 1]);
                } else {
                    has_explicit_indices = false;
                    break;
                }
            }
            if has_explicit_indices && resolved.len() == expected {
                return Ok(resolved);
            }
            if values.len() != expected {
                return Err(DuckDBClientError::Programming(format!(
                    "Invalid parameter count: passed {}, expected {expected}",
                    values.len()
                )));
            }
            Ok(values.iter().collect())
        }
        BoundParams::Named(map) => {
            let mut ordered = Vec::with_capacity(expected);
            for idx in 1..=expected {
                let raw_name = stmt
                    .parameter_name(idx)
                    .map_err(DuckDBClientError::from_duckdb)?;
                let clean = raw_name
                    .strip_prefix('$')
                    .or_else(|| raw_name.strip_prefix(':'))
                    .unwrap_or(&raw_name);
                let val = map
                    .get(clean)
                    .or_else(|| map.get(&format!("${clean}")))
                    .or_else(|| map.get(&format!(":{clean}")))
                    .ok_or_else(|| {
                        DuckDBClientError::Programming(format!(
                            "Missing named parameter {clean:?} for statement"
                        ))
                    })?;
                ordered.push(val);
            }
            Ok(ordered)
        }
    }
}

fn execute_statement_on_conn(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
) -> Result<i64, DuckDBClientError> {
    let effective_sql = match params {
        BoundParams::Named(_) => normalize_named_sql(sql),
        _ => sql.to_string(),
    };
    let mut stmt = match conn.prepare(&effective_sql) {
        Ok(s) => s,
        Err(duckdb::Error::MultipleStatement) if params.is_empty() => {
            conn.execute_batch(&effective_sql)
                .map_err(DuckDBClientError::from_duckdb)?;
            return Ok(0);
        }
        Err(e) => return Err(DuckDBClientError::from_duckdb(e)),
    };
    let ordered = resolve_ordered_params(&stmt, params)?;
    let changed = stmt
        .execute(params_from_iter(ordered))
        .map_err(DuckDBClientError::from_duckdb)?;
    Ok(i64::try_from(changed).unwrap_or(i64::MAX))
}

fn query_rows_on_conn(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
    cancel_flag: &Arc<AtomicBool>,
) -> Result<QueryResultSet, DuckDBClientError> {
    let effective_sql = match params {
        BoundParams::Named(_) => normalize_named_sql(sql),
        _ => sql.to_string(),
    };
    let mut stmt = conn
        .prepare(&effective_sql)
        .map_err(DuckDBClientError::from_duckdb)?;
    let ordered = resolve_ordered_params(&stmt, params)?;
    let mut rows = stmt
        .query(params_from_iter(ordered))
        .map_err(DuckDBClientError::from_duckdb)?;

    let mut out_rows = Vec::new();
    while let Some(row) = rows.next().map_err(DuckDBClientError::from_duckdb)? {
        if cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB query interrupted while fetching rows".to_string(),
            ));
        }
        let row_col_count = row.as_ref().column_count();
        let mut row_vals = Vec::with_capacity(row_col_count);
        for idx in 0..row_col_count {
            let val: DuckValue = row.get(idx).map_err(DuckDBClientError::from_duckdb)?;
            row_vals.push(val);
        }
        out_rows.push(row_vals);
    }

    let col_count = rows.as_ref().map_or(0, duckdb::Statement::column_count);
    let mut columns = Vec::with_capacity(col_count);
    if let Some(stmt_ref) = rows.as_ref() {
        for idx in 0..col_count {
            let col_name = stmt_ref
                .column_name(idx)
                .map_or_else(|_| format!("col_{idx}"), ToString::to_string);
            columns.push(col_name);
        }
    }
    drop(rows);

    if columns.is_empty()
        && is_read_only_sql(sql)
        && let Ok(ordered_arrow) = resolve_ordered_params(&stmt, params)
        && let Ok(arrow_iter) = stmt.query_arrow(params_from_iter(ordered_arrow))
    {
        let schema = arrow_iter.get_schema();
        for field in schema.fields() {
            columns.push(field.name().clone());
        }
    }

    Ok(QueryResultSet {
        columns,
        rows: out_rows,
    })
}

fn serialize_batches_to_ipc_bytes(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<(Vec<u8>, usize), DuckDBClientError> {
    let total_rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    let mut ipc_buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut ipc_buf, schema)
            .map_err(|e| DuckDBClientError::ShmArrow(e.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| DuckDBClientError::ShmArrow(e.to_string()))?;
        }
        writer
            .finish()
            .map_err(|e| DuckDBClientError::ShmArrow(e.to_string()))?;
    }
    Ok((ipc_buf, total_rows))
}

fn serialize_batches_to_shm_payload(
    schema: &Arc<Schema>,
    batches: &[RecordBatch],
) -> Result<ShmArrowPayload, DuckDBClientError> {
    let (ipc_buf, total_rows) = serialize_batches_to_ipc_bytes(schema, batches)?;
    let seq = ARROW_SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let seg_name = format!("/vtb-dd-{:x}-{seq:x}", std::process::id());
    let mut shm_writer = ShmArrowStreamWriter::create(&seg_name, ipc_buf.len() + 64, &[])
        .map_err(DuckDBClientError::ShmArrow)?;
    shm_writer
        .append_raw_stream_chunk(&ipc_buf, total_rows)
        .map_err(DuckDBClientError::ShmArrow)?;
    let (shm_name, byte_len, region) = shm_writer
        .finish_unregistered()
        .map_err(DuckDBClientError::ShmArrow)?;
    Ok(ShmArrowPayload {
        shm_name,
        byte_len,
        row_count: total_rows,
        region,
    })
}

fn rechunk_record_batches(batches: Vec<RecordBatch>, target_batch_size: usize) -> Vec<RecordBatch> {
    if target_batch_size == 0 || batches.is_empty() {
        return batches;
    }
    let mut out = Vec::new();
    for batch in batches {
        let num_rows = batch.num_rows();
        if num_rows <= target_batch_size {
            out.push(batch);
        } else {
            let mut offset = 0;
            while offset < num_rows {
                let len = (num_rows - offset).min(target_batch_size);
                out.push(batch.slice(offset, len));
                offset += len;
            }
        }
    }
    out
}

fn query_arrow_payload_on_conn(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
    batch_size: usize,
    threshold_rows: usize,
    cancel_flag: &Arc<AtomicBool>,
) -> Result<ArrowResultPayload, DuckDBClientError> {
    let effective_sql = match params {
        BoundParams::Named(_) => normalize_named_sql(sql),
        _ => sql.to_string(),
    };
    let mut stmt = conn
        .prepare(&effective_sql)
        .map_err(DuckDBClientError::from_duckdb)?;
    let ordered = resolve_ordered_params(&stmt, params)?;
    let arrow_iter = stmt
        .query_arrow(params_from_iter(ordered))
        .map_err(DuckDBClientError::from_duckdb)?;
    let schema = arrow_iter.get_schema();
    let mut raw_batches = Vec::new();
    for batch in arrow_iter {
        if cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB Arrow query interrupted".to_string(),
            ));
        }
        raw_batches.push(batch);
    }
    let batches = rechunk_record_batches(raw_batches, batch_size);
    let (ipc_buf, total_rows) = serialize_batches_to_ipc_bytes(&schema, &batches)?;
    if threshold_rows > 0 && total_rows < threshold_rows && ipc_buf.len() < 65_536 {
        let columns = schema.fields().iter().map(|f| f.name().clone()).collect();
        return Ok(ArrowResultPayload::Inline {
            ipc_bytes: ipc_buf,
            row_count: total_rows,
            columns,
        });
    }
    let seq = ARROW_SHM_COUNTER.fetch_add(1, Ordering::Relaxed);
    let seg_name = format!("/vtb-dd-{:x}-{seq:x}", std::process::id());
    let mut shm_writer = ShmArrowStreamWriter::create(&seg_name, ipc_buf.len() + 64, &[])
        .map_err(DuckDBClientError::ShmArrow)?;
    shm_writer
        .append_raw_stream_chunk(&ipc_buf, total_rows)
        .map_err(DuckDBClientError::ShmArrow)?;
    let (shm_name, byte_len, region) = shm_writer
        .finish_unregistered()
        .map_err(DuckDBClientError::ShmArrow)?;
    Ok(ArrowResultPayload::Shm {
        shm_name,
        byte_len,
        row_count: total_rows,
        region,
    })
}

fn stream_arrow_shm_batches_on_conn(
    conn: &mut Connection,
    sql: &str,
    params: &BoundParams,
    batch_size: usize,
    cancel_flag: &Arc<AtomicBool>,
) -> Result<Vec<ShmArrowPayload>, DuckDBClientError> {
    let effective_sql = match params {
        BoundParams::Named(_) => normalize_named_sql(sql),
        _ => sql.to_string(),
    };
    let mut stmt = conn
        .prepare(&effective_sql)
        .map_err(DuckDBClientError::from_duckdb)?;
    let ordered = resolve_ordered_params(&stmt, params)?;
    let arrow_iter = stmt
        .query_arrow(params_from_iter(ordered))
        .map_err(DuckDBClientError::from_duckdb)?;
    let schema = arrow_iter.get_schema();
    let mut raw_batches = Vec::new();
    for batch in arrow_iter {
        if cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB Arrow stream interrupted".to_string(),
            ));
        }
        raw_batches.push(batch);
    }
    let chunks = rechunk_record_batches(raw_batches, batch_size.max(1));
    if chunks.is_empty() {
        return Ok(vec![serialize_batches_to_shm_payload(&schema, &[])?]);
    }
    let mut payloads = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        payloads.push(serialize_batches_to_shm_payload(
            &schema,
            std::slice::from_ref(&chunk),
        )?);
    }
    Ok(payloads)
}

fn arrow_type_to_duckdb_ddl(dt: &DataType) -> String {
    match dt {
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Int8 => "TINYINT".to_string(),
        DataType::Int16 => "SMALLINT".to_string(),
        DataType::Int32 => "INTEGER".to_string(),
        DataType::Int64 => "BIGINT".to_string(),
        DataType::UInt8 => "UTINYINT".to_string(),
        DataType::UInt16 => "USMALLINT".to_string(),
        DataType::UInt32 => "UINTEGER".to_string(),
        DataType::UInt64 => "UBIGINT".to_string(),
        DataType::Float16 | DataType::Float32 => "FLOAT".to_string(),
        DataType::Float64 => "DOUBLE".to_string(),
        DataType::Decimal128(p, s) | DataType::Decimal256(p, s) => format!("DECIMAL({p}, {s})"),
        DataType::Date32 | DataType::Date64 => "DATE".to_string(),
        DataType::Time32(_) | DataType::Time64(_) => "TIME".to_string(),
        DataType::Timestamp(_, Some(_)) => "TIMESTAMPTZ".to_string(),
        DataType::Timestamp(_, None) => "TIMESTAMP".to_string(),
        DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::FixedSizeBinary(_) => "BLOB".to_string(),
        _ => "VARCHAR".to_string(),
    }
}

#[allow(clippy::type_complexity)]
fn quote_qualified_table(
    table: &str,
) -> Result<(Option<String>, Option<String>, String, String), DuckDBClientError> {
    let parts: Vec<&str> = table
        .split('.')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if parts.is_empty() || parts.len() > 3 {
        return Err(DuckDBClientError::InvalidInput(format!(
            "Invalid DuckDB table name: {table:?}"
        )));
    }
    for part in &parts {
        let unquoted = part.trim_matches('"');
        validate_sql_identifier(unquoted, "table identifier")?;
    }
    let clean_parts: Vec<String> = parts
        .iter()
        .map(|p| p.trim_matches('"').to_string())
        .collect();
    let quoted_sql = clean_parts
        .iter()
        .map(|p| format!("\"{p}\""))
        .collect::<Vec<_>>()
        .join(".");
    match clean_parts.len() {
        1 => Ok((None, None, clean_parts[0].clone(), quoted_sql)),
        2 => Ok((
            None,
            Some(clean_parts[0].clone()),
            clean_parts[1].clone(),
            quoted_sql,
        )),
        _ => Ok((
            Some(clean_parts[0].clone()),
            Some(clean_parts[1].clone()),
            clean_parts[2].clone(),
            quoted_sql,
        )),
    }
}

fn parse_arrow_ipc_batches(
    bytes: &[u8],
) -> Result<(Arc<Schema>, Vec<RecordBatch>), DuckDBClientError> {
    let payload = if bytes.len() >= 8 && &bytes[..8] == b"VTBARROW" {
        let header = ShmArrowFrameHeader::from_slice(bytes).map_err(DuckDBClientError::Data)?;
        let byte_len = usize::try_from(header.byte_len).map_err(|_| {
            DuckDBClientError::Data("VTBARROW frame byte_len overflows usize".to_string())
        })?;
        let frame_end = ShmArrowFrameHeader::SIZE
            .checked_add(byte_len)
            .ok_or_else(|| DuckDBClientError::Data("VTBARROW frame size overflow".to_string()))?;
        bytes
            .get(ShmArrowFrameHeader::SIZE..frame_end)
            .ok_or_else(|| {
                DuckDBClientError::Data(format!(
                    "VTBARROW frame declares {frame_end} bytes but buffer holds {}",
                    bytes.len()
                ))
            })?
    } else {
        bytes
    };
    if payload.is_empty() {
        return Err(DuckDBClientError::InvalidInput(
            "Arrow IPC payload is empty".to_string(),
        ));
    }
    if payload.starts_with(b"ARROW1") {
        let reader = FileReader::try_new(Cursor::new(payload), None)
            .map_err(|e| DuckDBClientError::Data(format!("Invalid Arrow IPC file: {e}")))?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch_res in reader {
            let batch = batch_res
                .map_err(|e| DuckDBClientError::Data(format!("Invalid Arrow IPC batch: {e}")))?;
            batches.push(batch);
        }
        Ok((schema, batches))
    } else {
        let reader = StreamReader::try_new(Cursor::new(payload), None)
            .map_err(|e| DuckDBClientError::Data(format!("Invalid Arrow IPC stream: {e}")))?;
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch_res in reader {
            let batch = batch_res
                .map_err(|e| DuckDBClientError::Data(format!("Invalid Arrow IPC batch: {e}")))?;
            batches.push(batch);
        }
        Ok((schema, batches))
    }
}

fn project_batch_columns(
    batch: &RecordBatch,
    target_columns: &[String],
) -> Result<RecordBatch, DuckDBClientError> {
    let schema = batch.schema();
    let mut indices = Vec::with_capacity(target_columns.len());
    for col in target_columns {
        let idx = schema.index_of(col).map_err(|_| {
            DuckDBClientError::Programming(format!(
                "Arrow RecordBatch does not contain requested column {col:?}"
            ))
        })?;
        indices.push(idx);
    }
    batch
        .project(&indices)
        .map_err(|e| DuckDBClientError::Data(format!("Failed to project Arrow batch: {e}")))
}

fn load_arrow_on_conn(
    conn: &mut Connection,
    table: &str,
    source: BulkLoadSource,
    columns: Option<&[String]>,
    create_if_missing: bool,
    cancel_flag: &Arc<AtomicBool>,
) -> Result<i64, DuckDBClientError> {
    let (catalog_opt, schema_opt, table_name, quoted_table) = quote_qualified_table(table)?;

    match source {
        BulkLoadSource::ArrowIpc(ipc_bytes) => {
            let (schema, batches) = parse_arrow_ipc_batches(&ipc_bytes)?;
            let effective_fields: Vec<Field> = if let Some(cols) = columns {
                let mut fields = Vec::with_capacity(cols.len());
                for col in cols {
                    let f = schema.field_with_name(col).map_err(|_| {
                        DuckDBClientError::Programming(format!(
                            "Arrow schema does not contain column {col:?}"
                        ))
                    })?;
                    fields.push(f.clone());
                }
                fields
            } else {
                schema.fields().iter().map(|f| (**f).clone()).collect()
            };

            if create_if_missing && !effective_fields.is_empty() {
                let col_defs = effective_fields
                    .iter()
                    .map(|f| {
                        let clean_col = f.name().replace('"', "\"\"");
                        let sql_ty = arrow_type_to_duckdb_ddl(f.data_type());
                        format!("\"{clean_col}\" {sql_ty}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let ddl = format!("CREATE TABLE IF NOT EXISTS {quoted_table} ({col_defs})");
                conn.execute_batch(&ddl)
                    .map_err(DuckDBClientError::from_duckdb)?;
            }

            let col_names: Vec<&str> = effective_fields.iter().map(|f| f.name().as_str()).collect();
            let mut appender = open_table_appender(
                conn,
                catalog_opt.as_deref(),
                schema_opt.as_deref(),
                &table_name,
                if columns.is_some() {
                    Some(col_names.as_slice())
                } else {
                    None
                },
            )?;

            let mut total_rows: i64 = 0;
            for batch in batches {
                if cancel_flag.load(Ordering::Acquire) {
                    return Err(DuckDBClientError::QueryTimeout(
                        "DuckDB bulk load interrupted".to_string(),
                    ));
                }
                if batch.num_rows() == 0 {
                    continue;
                }
                let projected = if let Some(cols) = columns {
                    project_batch_columns(&batch, cols)?
                } else {
                    batch
                };
                total_rows += i64::try_from(projected.num_rows()).unwrap_or(0);
                appender
                    .append_record_batch(projected)
                    .map_err(DuckDBClientError::from_duckdb)?;
            }
            appender.flush().map_err(DuckDBClientError::from_duckdb)?;
            Ok(total_rows)
        }
        BulkLoadSource::Rows {
            columns: row_cols,
            rows,
        } => {
            let active_cols: Vec<String> = if let Some(cols) = columns {
                cols.to_vec()
            } else {
                row_cols
            };
            if create_if_missing && !active_cols.is_empty() {
                let col_defs = active_cols
                    .iter()
                    .enumerate()
                    .map(|(idx, col_name)| {
                        let clean_col = col_name.replace('"', "\"\"");
                        let inferred_ty = rows
                            .first()
                            .and_then(|r| r.get(idx))
                            .map_or("VARCHAR", infer_duck_value_ddl);
                        format!("\"{clean_col}\" {inferred_ty}")
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                let ddl = format!("CREATE TABLE IF NOT EXISTS {quoted_table} ({col_defs})");
                conn.execute_batch(&ddl)
                    .map_err(DuckDBClientError::from_duckdb)?;
            }
            if rows.is_empty() || active_cols.is_empty() {
                return Ok(0);
            }
            let quoted_cols = active_cols
                .iter()
                .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
                .collect::<Vec<_>>()
                .join(", ");
            let placeholders = vec!["?"; active_cols.len()].join(", ");
            let insert_sql =
                format!("INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})");
            let mut stmt = conn
                .prepare(&insert_sql)
                .map_err(DuckDBClientError::from_duckdb)?;
            let mut count: i64 = 0;
            for row in &rows {
                if cancel_flag.load(Ordering::Acquire) {
                    return Err(DuckDBClientError::QueryTimeout(
                        "DuckDB bulk load interrupted".to_string(),
                    ));
                }
                stmt.execute(params_from_iter(row.iter()))
                    .map_err(DuckDBClientError::from_duckdb)?;
                count += 1;
            }
            Ok(count)
        }
    }
}

fn open_table_appender<'a>(
    conn: &'a Connection,
    catalog: Option<&str>,
    schema: Option<&str>,
    table: &str,
    columns: Option<&[&str]>,
) -> Result<duckdb::Appender<'a>, DuckDBClientError> {
    let res = match (catalog, schema, columns) {
        (Some(cat), Some(sch), Some(cols)) => {
            conn.appender_with_columns_to_catalog_and_db(table, cat, sch, cols)
        }
        (None, Some(sch), Some(cols)) => conn.appender_with_columns_to_db(table, sch, cols),
        (None, None, Some(cols)) => conn.appender_with_columns(table, cols),
        (Some(cat), Some(sch), None) => conn.appender_to_catalog_and_db(table, cat, sch),
        (None, Some(sch), None) => conn.appender_to_db(table, sch),
        _ => conn.appender(table),
    };
    res.map_err(DuckDBClientError::from_duckdb)
}

const fn infer_duck_value_ddl(val: &DuckValue) -> &'static str {
    match val {
        DuckValue::Boolean(_) => "BOOLEAN",
        DuckValue::TinyInt(_)
        | DuckValue::SmallInt(_)
        | DuckValue::Int(_)
        | DuckValue::BigInt(_) => "BIGINT",
        DuckValue::UTinyInt(_)
        | DuckValue::USmallInt(_)
        | DuckValue::UInt(_)
        | DuckValue::UBigInt(_) => "UBIGINT",
        DuckValue::Float(_) | DuckValue::Double(_) => "DOUBLE",
        DuckValue::Blob(_) => "BLOB",
        _ => "VARCHAR",
    }
}

fn py_to_duck_value(obj: &Bound<'_, PyAny>) -> Result<DuckValue, DuckDBClientError> {
    if obj.is_none() {
        return Ok(DuckValue::Null);
    }
    if obj.is_instance_of::<PyBool>() {
        let b: bool = obj
            .extract::<bool>()
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        return Ok(DuckValue::Boolean(b));
    }
    if obj.is_instance_of::<PyInt>() {
        if let Ok(v) = obj.extract::<i64>() {
            return Ok(DuckValue::BigInt(v));
        }
        if let Ok(v) = obj.extract::<i128>() {
            return Ok(DuckValue::HugeInt(v));
        }
        if let Ok(v) = obj.extract::<u64>() {
            return Ok(DuckValue::UBigInt(v));
        }
    }
    if obj.is_instance_of::<PyFloat>() {
        let v: f64 = obj
            .extract::<f64>()
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        return Ok(DuckValue::Double(v));
    }
    if let Ok(b) = obj.cast::<PyBytes>() {
        return Ok(DuckValue::Blob(b.as_bytes().to_vec()));
    }
    if let Ok(ba) = obj.cast::<PyByteArray>() {
        let bytes = pyo3::sync::critical_section::with_critical_section(ba.as_any(), || {
            // SAFETY: `with_critical_section` locks `PyByteArray` on free-threaded Python and we
            // hold the GIL on standard builds while copying the buffer synchronously.
            unsafe { ba.as_bytes() }.to_vec()
        });
        return Ok(DuckValue::Blob(bytes));
    }
    if let Ok(s) = obj.cast::<PyString>() {
        let text = s
            .to_str()
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        if text.contains('\0') {
            return Err(DuckDBClientError::InvalidInput(
                "String parameter must not contain NUL bytes".to_string(),
            ));
        }
        return Ok(DuckValue::Text(text.to_string()));
    }
    if obj.is_instance_of::<PyDict>()
        || obj.is_instance_of::<PyList>()
        || obj.is_instance_of::<PyTuple>()
    {
        let py = obj.py();
        let serialized = if let Ok(serializers_mod) = py.import("vertebra.utils.serializers") {
            serializers_mod
                .call_method1("encode_json", (obj,))
                .and_then(|v| v.extract::<String>())
                .map_err(|e: PyErr| DuckDBClientError::Data(e.to_string()))?
        } else {
            let json_mod = py
                .import("json")
                .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
            json_mod
                .call_method1("dumps", (obj,))
                .and_then(|v| v.extract::<String>())
                .map_err(|e: PyErr| DuckDBClientError::Data(e.to_string()))?
        };
        return Ok(DuckValue::Text(serialized));
    }
    if obj.hasattr("isoformat").unwrap_or(false) {
        let iso: String = obj
            .call_method0("isoformat")
            .and_then(|v| v.extract::<String>())
            .map_err(|e: PyErr| DuckDBClientError::Data(e.to_string()))?;
        return Ok(DuckValue::Text(iso));
    }
    let text: String = obj
        .str()
        .and_then(|s| s.extract::<String>())
        .map_err(|e: PyErr| DuckDBClientError::Data(e.to_string()))?;
    Ok(DuckValue::Text(text))
}

fn extract_bound_params(
    parameters: Option<&Bound<'_, PyAny>>,
) -> Result<BoundParams, DuckDBClientError> {
    let Some(obj) = parameters else {
        return Ok(BoundParams::Empty);
    };
    if obj.is_none() {
        return Ok(BoundParams::Empty);
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        if dict.is_empty() {
            return Ok(BoundParams::Empty);
        }
        let mut map = HashMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key: String = k
                .extract::<String>()
                .map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
            map.insert(key, py_to_duck_value(&v)?);
        }
        return Ok(BoundParams::Named(map));
    }
    if let Ok(list) = obj.cast::<PyList>() {
        let mut vals = Vec::with_capacity(list.len());
        for item in list.iter() {
            vals.push(py_to_duck_value(&item)?);
        }
        return Ok(BoundParams::Positional(vals));
    }
    if let Ok(tup) = obj.cast::<PyTuple>() {
        let mut vals = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            vals.push(py_to_duck_value(&item)?);
        }
        return Ok(BoundParams::Positional(vals));
    }
    let iter = obj.try_iter().map_err(|_| {
        DuckDBClientError::InvalidInput(
            "SQL parameters must be a sequence (list/tuple) or mapping (dict)".to_string(),
        )
    })?;
    let mut vals = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
        vals.push(py_to_duck_value(&item)?);
    }
    Ok(BoundParams::Positional(vals))
}

fn extract_parameter_sets(
    parameter_sets: &Bound<'_, PyAny>,
) -> Result<Vec<BoundParams>, DuckDBClientError> {
    let iter = parameter_sets.try_iter().map_err(|_| {
        DuckDBClientError::InvalidInput(
            "execute_many parameter_sets must be an iterable of parameter sets".to_string(),
        )
    })?;
    let mut sets = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
        sets.push(extract_bound_params(Some(&item))?);
    }
    Ok(sets)
}

fn extract_bulk_load_source(
    py: Python<'_>,
    source: &Bound<'_, PyAny>,
    columns: Option<&[String]>,
) -> Result<BulkLoadSource, DuckDBClientError> {
    if let Ok(b) = source.cast::<PyBytes>() {
        return Ok(BulkLoadSource::ArrowIpc(b.as_bytes().to_vec()));
    }
    if let Ok(ba) = source.cast::<PyByteArray>() {
        let bytes = pyo3::sync::critical_section::with_critical_section(ba.as_any(), || {
            // SAFETY: `with_critical_section` locks `PyByteArray` on free-threaded Python and we
            // hold the GIL on standard builds while copying the buffer synchronously.
            unsafe { ba.as_bytes() }.to_vec()
        });
        return Ok(BulkLoadSource::ArrowIpc(bytes));
    }
    if source.hasattr("read_bytes").unwrap_or(false) {
        let raw: Vec<u8> = source
            .call_method0("read_bytes")
            .and_then(|b| b.extract::<Vec<u8>>())
            .map_err(|e: PyErr| DuckDBClientError::Data(e.to_string()))?;
        return Ok(BulkLoadSource::ArrowIpc(raw));
    }
    if let Ok(buf) = PyBuffer::<u8>::get(source) {
        let bytes = buf
            .to_vec(py)
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        return Ok(BulkLoadSource::ArrowIpc(bytes));
    }
    if source.hasattr("schema").unwrap_or(false)
        && let Ok(pa_ipc) = py.import("pyarrow.ipc")
        && let Ok(pa) = py.import("pyarrow")
    {
        let sink = pa
            .call_method0("BufferOutputStream")
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        let schema = source
            .getattr("schema")
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        let writer = pa_ipc
            .call_method1("new_stream", (&sink, &schema))
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        if source.hasattr("to_batches").unwrap_or(false) {
            writer
                .call_method1("write_table", (source,))
                .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        } else if source.hasattr("read_next_batch").unwrap_or(false) {
            let tbl = source
                .call_method0("read_all")
                .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
            writer
                .call_method1("write_table", (tbl,))
                .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        } else {
            writer
                .call_method1("write_batch", (source,))
                .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        }
        writer
            .call_method0("close")
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        let py_buf = sink
            .call_method0("getvalue")
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?;
        let raw_bytes: Vec<u8> = py_buf
            .call_method0("to_pybytes")
            .and_then(|b| b.extract::<Vec<u8>>())
            .map_err(|e: PyErr| DuckDBClientError::Data(e.to_string()))?;
        return Ok(BulkLoadSource::ArrowIpc(raw_bytes));
    }

    let row_list_obj = if source.hasattr("to_pylist").unwrap_or(false) {
        source
            .call_method0("to_pylist")
            .map_err(|e| DuckDBClientError::Data(e.to_string()))?
    } else {
        source.clone()
    };

    let iter = row_list_obj.try_iter().map_err(|_| {
        DuckDBClientError::InvalidInput(
            "load_from_arrow source must be an ArrowCursor, Arrow IPC bytes, PyArrow object, or sequence of dicts"
                .to_string(),
        )
    })?;
    let mut inferred_cols: Vec<String> = columns.map_or_else(Vec::new, <[String]>::to_vec);
    let mut rows_out: Vec<Vec<DuckValue>> = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
        if let Ok(dict) = item.cast::<PyDict>() {
            if inferred_cols.is_empty() {
                for k in dict.keys() {
                    let col_name: String = k
                        .extract::<String>()
                        .map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
                    inferred_cols.push(col_name);
                }
            }
            let mut row_vals = Vec::with_capacity(inferred_cols.len());
            for col in &inferred_cols {
                match dict.get_item(col) {
                    Ok(Some(v)) => row_vals.push(py_to_duck_value(&v)?),
                    Ok(None) => row_vals.push(DuckValue::Null),
                    Err(e) => return Err(DuckDBClientError::InvalidInput(e.to_string())),
                }
            }
            rows_out.push(row_vals);
        } else {
            return Err(DuckDBClientError::InvalidInput(
                "Row items in load_from_arrow must be dictionaries".to_string(),
            ));
        }
    }
    Ok(BulkLoadSource::Rows {
        columns: inferred_cols,
        rows: rows_out,
    })
}

fn duck_value_to_py(py: Python<'_>, val: DuckValue) -> PyResult<Py<PyAny>> {
    match val {
        DuckValue::Null => Ok(py.None()),
        DuckValue::Boolean(b) => Ok(PyBool::new(py, b).to_owned().into_any().unbind()),
        DuckValue::TinyInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::SmallInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::Int(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::BigInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::HugeInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::UTinyInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::USmallInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::UInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::UBigInt(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::Float(v) => Ok(f64::from(v).into_pyobject(py)?.into_any().unbind()),
        DuckValue::Double(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        DuckValue::Decimal(d) => {
            let dec_mod = py.import("decimal")?;
            let dec_obj = dec_mod.call_method1("Decimal", (d.to_string(),))?;
            Ok(dec_obj.unbind())
        }
        DuckValue::Text(s) | DuckValue::Enum(s) => Ok(s.into_pyobject(py)?.into_any().unbind()),
        DuckValue::Blob(b) => Ok(PyBytes::new(py, &b).into_any().unbind()),
        DuckValue::Date32(days) => {
            let dt_mod = py.import("datetime")?;
            let ordinal = 719_163_i64 + i64::from(days);
            let date_obj = dt_mod
                .getattr("date")?
                .call_method1("fromordinal", (ordinal,))?;
            Ok(date_obj.unbind())
        }
        DuckValue::Time64(unit, raw) => {
            let micros = match unit {
                DuckTimeUnit::Second => raw.saturating_mul(1_000_000),
                DuckTimeUnit::Millisecond => raw.saturating_mul(1_000),
                DuckTimeUnit::Microsecond => raw,
                DuckTimeUnit::Nanosecond => raw / 1_000,
            };
            let rem_day = micros.rem_euclid(86_400_000_000);
            let hours = rem_day / 3_600_000_000;
            let mins = (rem_day % 3_600_000_000) / 60_000_000;
            let secs = (rem_day % 60_000_000) / 1_000_000;
            let us = rem_day % 1_000_000;
            let dt_mod = py.import("datetime")?;
            let time_obj = dt_mod.call_method1("time", (hours, mins, secs, us))?;
            Ok(time_obj.unbind())
        }
        DuckValue::Timestamp(unit, raw) => {
            let micros = match unit {
                DuckTimeUnit::Second => raw.saturating_mul(1_000_000),
                DuckTimeUnit::Millisecond => raw.saturating_mul(1_000),
                DuckTimeUnit::Microsecond => raw,
                DuckTimeUnit::Nanosecond => raw / 1_000,
            };
            let dt_mod = py.import("datetime")?;
            let utc = dt_mod.getattr("timezone")?.getattr("utc")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("tzinfo", utc)?;
            let epoch = dt_mod
                .getattr("datetime")?
                .call((1970, 1, 1), Some(&kwargs))?;
            let delta = dt_mod.getattr("timedelta")?.call1((0, 0, micros))?;
            let ts_obj = epoch.call_method1("__add__", (delta,))?;
            Ok(ts_obj.unbind())
        }
        DuckValue::Interval {
            months,
            days,
            nanos,
        } => {
            let dt_mod = py.import("datetime")?;
            let total_days = i64::from(months) * 30 + i64::from(days);
            let micros = nanos / 1_000;
            let delta = dt_mod
                .getattr("timedelta")?
                .call1((total_days, 0, micros))?;
            Ok(delta.unbind())
        }
        DuckValue::List(items) | DuckValue::Array(items) => {
            let py_list = PyList::empty(py);
            for item in items {
                py_list.append(duck_value_to_py(py, item)?)?;
            }
            Ok(py_list.into_any().unbind())
        }
        DuckValue::Struct(fields) => {
            let py_dict = PyDict::new(py);
            for (k, v) in fields.iter() {
                py_dict.set_item(k, duck_value_to_py(py, v.clone())?)?;
            }
            Ok(py_dict.into_any().unbind())
        }
        DuckValue::Map(entries) => {
            let py_dict = PyDict::new(py);
            for (k, v) in entries.iter() {
                let py_k = duck_value_to_py(py, k.clone())?;
                let py_v = duck_value_to_py(py, v.clone())?;
                py_dict.set_item(py_k, py_v)?;
            }
            Ok(py_dict.into_any().unbind())
        }
        DuckValue::Union(inner) => duck_value_to_py(py, *inner),
        _ => Ok(format!("{val:?}").into_pyobject(py)?.into_any().unbind()),
    }
}

fn result_set_to_py_dicts(py: Python<'_>, result: QueryResultSet) -> PyResult<Py<PyAny>> {
    let py_list = PyList::empty(py);
    let col_names: Vec<Bound<'_, PyString>> = result
        .columns
        .iter()
        .map(|c| PyString::new(py, c))
        .collect();
    for row in result.rows {
        let dict = PyDict::new(py);
        for (idx, val) in row.into_iter().enumerate() {
            if let Some(key) = col_names.get(idx) {
                dict.set_item(key, duck_value_to_py(py, val)?)?;
            }
        }
        py_list.append(dict)?;
    }
    Ok(py_list.into_any().unbind())
}

fn result_set_to_py_tuples(py: Python<'_>, result: QueryResultSet) -> PyResult<Py<PyAny>> {
    let py_list = PyList::empty(py);
    for row in result.rows {
        let mut items = Vec::with_capacity(row.len());
        for val in row {
            items.push(duck_value_to_py(py, val)?);
        }
        py_list.append(PyTuple::new(py, items)?)?;
    }
    Ok(py_list.into_any().unbind())
}

fn result_set_to_py_with_description(
    py: Python<'_>,
    result: QueryResultSet,
) -> PyResult<Py<PyAny>> {
    let cols_tuple = PyTuple::new(py, &result.columns)?;
    let rows_list = result_set_to_py_tuples(py, result)?;
    let pair = PyTuple::new(py, [rows_list.bind(py), cols_tuple.as_any()])?;
    Ok(pair.into_any().unbind())
}

fn result_set_to_single_dict(
    py: Python<'_>,
    mut result: QueryResultSet,
    optional: bool,
) -> PyResult<Py<PyAny>> {
    if result.rows.is_empty() {
        if optional {
            return Ok(py.None());
        }
        return Err(
            DuckDBClientError::NotFound("Query returned no rows".to_string()).into_py_err(py),
        );
    }
    if result.rows.len() > 1 {
        return Err(DuckDBClientError::Data(format!(
            "Expected at most 1 row, got {}",
            result.rows.len()
        ))
        .into_py_err(py));
    }
    let row = result.rows.swap_remove(0);
    let dict = PyDict::new(py);
    for (col, val) in result.columns.into_iter().zip(row) {
        dict.set_item(col, duck_value_to_py(py, val)?)?;
    }
    Ok(dict.into_any().unbind())
}

fn arrow_result_payload_to_py_cursor(
    py: Python<'_>,
    payload: ArrowResultPayload,
) -> PyResult<Py<PyAny>> {
    match payload {
        ArrowResultPayload::Shm {
            shm_name,
            byte_len,
            row_count,
            region,
        } => construct_py_arrow_cursor(py, &shm_name, byte_len, row_count, region),
        ArrowResultPayload::Inline {
            ipc_bytes,
            row_count: _,
            columns,
        } => {
            let py_bytes = PyBytes::new(py, &ipc_bytes);
            if let Ok(arrow_mod) = py.import("vertebra.db.arrow")
                && let Ok(cursor_cls) = arrow_mod.getattr("ArrowCursor")
            {
                let kwargs = PyDict::new(py);
                kwargs.set_item("columns", PyTuple::new(py, &columns)?)?;
                kwargs.set_item("used_shm", false)?;
                let cursor = cursor_cls.call((py_bytes,), Some(&kwargs))?;
                return Ok(cursor.unbind());
            }
            Ok(py_bytes.into_any().unbind())
        }
    }
}

struct CancelOnDropGuard {
    inner: Arc<DuckDBInner>,
    cancel_flag: Arc<AtomicBool>,
    completed: bool,
}

impl CancelOnDropGuard {
    fn new(inner: Arc<DuckDBInner>, cancel_flag: Arc<AtomicBool>) -> Self {
        Self {
            inner,
            cancel_flag,
            completed: false,
        }
    }

    fn mark_completed(&mut self) {
        self.completed = true;
    }
}

impl Drop for CancelOnDropGuard {
    fn drop(&mut self) {
        if !self.completed {
            self.inner.interrupt_matching(&self.cancel_flag);
        }
    }
}

/// Native synchronous DuckDB 2.0 client bridged via [`SyncCompletionQueue`].
#[pyclass(frozen, immutable_type)]
pub struct SyncDuckDBClient {
    inner: Arc<DuckDBInner>,
}

#[pymethods]
impl SyncDuckDBClient {
    #[new]
    #[pyo3(signature = (
        database = ":memory:",
        *,
        read_only = false,
        max_connections = 4,
        max_memory_bytes = None,
        threads = None,
        temp_directory = None,
        parquet_metadata_cache = true,
        httpfs_connection_caching = true,
        enable_http_metadata_cache = true,
        config = None,
        extensions = None,
        init_statements = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        database: &str,
        read_only: bool,
        max_connections: usize,
        max_memory_bytes: Option<u64>,
        threads: Option<usize>,
        temp_directory: Option<String>,
        parquet_metadata_cache: bool,
        httpfs_connection_caching: bool,
        enable_http_metadata_cache: bool,
        config: Option<HashMap<String, String>>,
        extensions: Option<Vec<String>>,
        init_statements: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let db_str = database.to_string();
        let inner = py
            .detach(move || {
                DuckDBInner::open(
                    &db_str,
                    read_only,
                    max_connections,
                    max_memory_bytes,
                    threads,
                    temp_directory,
                    parquet_metadata_cache,
                    httpfs_connection_caching,
                    enable_http_metadata_cache,
                    config,
                    extensions,
                    init_statements,
                )
            })
            .map_err(|e| e.into_py_err(py))?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn database(&self) -> &str {
        &self.inner.database
    }

    #[getter]
    pub fn read_only(&self) -> bool {
        self.inner.read_only
    }

    #[getter]
    pub fn is_in_memory(&self) -> bool {
        self.inner.is_in_memory
    }

    #[getter]
    pub fn max_connections(&self) -> usize {
        self.inner.max_connections
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.inner.in_transaction.load(Ordering::Acquire)
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    #[getter]
    pub fn bootstrap_statements(&self) -> Vec<String> {
        self.inner.bootstrap_statements.clone()
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<i64> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || inner.execute_sql(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameter_sets: &Bound<'_, PyAny>,
    ) -> PyResult<i64> {
        let bound_sets = extract_parameter_sets(parameter_sets).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || inner.execute_many_sql(sql_owned, bound_sets, &cancel_flag))
            .map_err(|e| e.into_py_err(py))
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute_tuples(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let res = py
            .detach(move || inner.query_sql(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))?;
        result_set_to_py_tuples(py, res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute_with_description(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let res = py
            .detach(move || inner.query_sql(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))?;
        result_set_to_py_with_description(py, res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_all(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let res = py
            .detach(move || inner.query_sql(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))?;
        result_set_to_py_dicts(py, res)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_one(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let res = py
            .detach(move || inner.query_sql(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))?;
        result_set_to_single_dict(py, res, false)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_optional(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let res = py
            .detach(move || inner.query_sql(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))?;
        result_set_to_single_dict(py, res, true)
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn select_value(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut res = py
            .detach(move || inner.query_sql(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))?;
        if res.rows.is_empty() || res.rows[0].is_empty() {
            return Err(
                DuckDBClientError::NotFound("Query returned no scalar value".to_string())
                    .into_py_err(py),
            );
        }
        let val = res.rows.swap_remove(0).swap_remove(0);
        duck_value_to_py(py, val)
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536, threshold_rows=0))]
    pub fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
        threshold_rows: usize,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let payload = py
            .detach(move || {
                inner.query_arrow_payload(
                    sql_owned,
                    bound,
                    batch_size,
                    threshold_rows,
                    &cancel_flag,
                )
            })
            .map_err(|e| e.into_py_err(py))?;
        arrow_result_payload_to_py_cursor(py, payload)
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536))]
    pub fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<Py<PyAny>> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let payloads = py
            .detach(move || {
                inner.stream_arrow_shm_batches(sql_owned, bound, batch_size, &cancel_flag)
            })
            .map_err(|e| e.into_py_err(py))?;
        let out = PyList::empty(py);
        for payload in payloads {
            out.append(construct_py_arrow_cursor(
                py,
                &payload.shm_name,
                payload.byte_len,
                payload.row_count,
                payload.region,
            )?)?;
        }
        Ok(out.into_any().unbind())
    }

    #[pyo3(signature = (table, source, *, columns=None, create_if_missing=true))]
    pub fn load_from_arrow(
        &self,
        py: Python<'_>,
        table: &str,
        source: &Bound<'_, PyAny>,
        columns: Option<Vec<String>>,
        create_if_missing: bool,
    ) -> PyResult<i64> {
        let load_source = extract_bulk_load_source(py, source, columns.as_deref())
            .map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let table_owned = table.to_string();
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || {
            inner.load_arrow_bulk(
                table_owned,
                load_source,
                columns,
                create_if_missing,
                &cancel_flag,
            )
        })
        .map_err(|e| e.into_py_err(py))
    }

    pub fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || inner.begin_tx(&cancel_flag))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || inner.commit_tx(&cancel_flag))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || inner.rollback_tx(&cancel_flag))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn savepoint(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let name_owned = name.to_string();
        py.detach(move || inner.savepoint_tx(&name_owned))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn rollback_to_savepoint(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let name_owned = name.to_string();
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || inner.rollback_to_savepoint_tx(&name_owned, &cancel_flag))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn release_savepoint(&self, py: Python<'_>, name: &str) -> PyResult<()> {
        let inner = Arc::clone(&self.inner);
        let name_owned = name.to_string();
        py.detach(move || inner.release_savepoint_tx(&name_owned))
            .map_err(|e| e.into_py_err(py))
    }

    pub fn interrupt(&self) {
        self.inner.interrupt_all();
    }

    pub fn cancel(&self) {
        self.inner.interrupt_all();
    }

    pub fn close(&self, py: Python<'_>) {
        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.close_all());
    }

    pub fn to_async(&self) -> AsyncDuckDBClient {
        AsyncDuckDBClient {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "SyncDuckDBClient(database={:?}, read_only={}, max_connections={}, in_transaction={}, closed={})",
            self.inner.database,
            self.inner.read_only,
            self.inner.max_connections,
            self.in_transaction(),
            self.closed()
        )
    }

    pub fn __enter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    pub fn __exit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) {
        self.close(py);
    }
}

/// Native asynchronous DuckDB 2.0 client bridged via [`AsyncCompletionQueue`] + [`Notifier`].
#[pyclass(frozen, immutable_type)]
pub struct AsyncDuckDBClient {
    inner: Arc<DuckDBInner>,
}

#[pymethods]
impl AsyncDuckDBClient {
    #[new]
    #[pyo3(signature = (
        database = ":memory:",
        *,
        read_only = false,
        max_connections = 4,
        max_memory_bytes = None,
        threads = None,
        temp_directory = None,
        parquet_metadata_cache = true,
        httpfs_connection_caching = true,
        enable_http_metadata_cache = true,
        config = None,
        extensions = None,
        init_statements = None,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        database: &str,
        read_only: bool,
        max_connections: usize,
        max_memory_bytes: Option<u64>,
        threads: Option<usize>,
        temp_directory: Option<String>,
        parquet_metadata_cache: bool,
        httpfs_connection_caching: bool,
        enable_http_metadata_cache: bool,
        config: Option<HashMap<String, String>>,
        extensions: Option<Vec<String>>,
        init_statements: Option<Vec<String>>,
    ) -> PyResult<Self> {
        let db_str = database.to_string();
        let inner = py
            .detach(move || {
                DuckDBInner::open(
                    &db_str,
                    read_only,
                    max_connections,
                    max_memory_bytes,
                    threads,
                    temp_directory,
                    parquet_metadata_cache,
                    httpfs_connection_caching,
                    enable_http_metadata_cache,
                    config,
                    extensions,
                    init_statements,
                )
            })
            .map_err(|e| e.into_py_err(py))?;
        Ok(Self { inner })
    }

    #[getter]
    pub fn database(&self) -> &str {
        &self.inner.database
    }

    #[getter]
    pub fn read_only(&self) -> bool {
        self.inner.read_only
    }

    #[getter]
    pub fn is_in_memory(&self) -> bool {
        self.inner.is_in_memory
    }

    #[getter]
    pub fn max_connections(&self) -> usize {
        self.inner.max_connections
    }

    #[getter]
    pub fn in_transaction(&self) -> bool {
        self.inner.in_transaction.load(Ordering::Acquire)
    }

    #[getter]
    pub fn closed(&self) -> bool {
        self.inner.closed.load(Ordering::Acquire)
    }

    #[getter]
    pub fn bootstrap_statements(&self) -> Vec<String> {
        self.inner.bootstrap_statements.clone()
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.execute_sql(sql_owned, bound, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameter_sets: &Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound_sets = extract_parameter_sets(parameter_sets).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.execute_many_sql(sql_owned, bound_sets, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute_tuples(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.query_sql(sql_owned, bound, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            result_set_to_py_tuples,
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute_with_description(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.query_sql(sql_owned, bound, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            result_set_to_py_with_description,
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_all(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.query_sql(sql_owned, bound, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            result_set_to_py_dicts,
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_one(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.query_sql(sql_owned, bound, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, result_set| result_set_to_single_dict(py, result_set, false),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn fetch_optional(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.query_sql(sql_owned, bound, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, result_set| result_set_to_single_dict(py, result_set, true),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None))]
    pub fn select_value(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.query_sql(sql_owned, bound, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, mut result_set| {
                if result_set.rows.is_empty() || result_set.rows[0].is_empty() {
                    return Err(DuckDBClientError::NotFound(
                        "Query returned no scalar value".to_string(),
                    )
                    .into_py_err(py));
                }
                let val = result_set.rows.swap_remove(0).swap_remove(0);
                duck_value_to_py(py, val)
            },
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536, threshold_rows=0))]
    pub fn select_to_arrow(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
        threshold_rows: usize,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.query_arrow_payload(
                        sql_owned,
                        bound,
                        batch_size,
                        threshold_rows,
                        &cancel_flag,
                    )
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            arrow_result_payload_to_py_cursor,
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536))]
    pub fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.stream_arrow_shm_batches(sql_owned, bound, batch_size, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, payloads| {
                let out = PyList::empty(py);
                for payload in payloads {
                    out.append(construct_py_arrow_cursor(
                        py,
                        &payload.shm_name,
                        payload.byte_len,
                        payload.row_count,
                        payload.region,
                    )?)?;
                }
                Ok(out.into_any().unbind())
            },
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (table, source, *, columns=None, create_if_missing=true))]
    pub fn load_from_arrow(
        &self,
        py: Python<'_>,
        table: &str,
        source: &Bound<'_, PyAny>,
        columns: Option<Vec<String>>,
        create_if_missing: bool,
    ) -> PyResult<RustDbTaskAwaitable> {
        let load_source = extract_bulk_load_source(py, source, columns.as_deref())
            .map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let table_owned = table.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.load_arrow_bulk(
                        table_owned,
                        load_source,
                        columns,
                        create_if_missing,
                        &cancel_flag,
                    )
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, rows| Ok(rows.into_pyobject(py)?.into_any().unbind()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn begin(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || inner.begin_tx(&cancel_flag))
                    .await
                    .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || inner.commit_tx(&cancel_flag))
                    .await
                    .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || inner.rollback_tx(&cancel_flag))
                    .await
                    .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn savepoint(&self, py: Python<'_>, name: &str) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let name_owned = name.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move {
                tokio::task::spawn_blocking(move || inner.savepoint_tx(&name_owned))
                    .await
                    .map_err(|e| DuckDBClientError::Operational(e.to_string()))?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn rollback_to_savepoint(
        &self,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let name_owned = name.to_string();
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || {
                    inner.rollback_to_savepoint_tx(&name_owned, &cancel_flag)
                })
                .await
                .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn release_savepoint(&self, py: Python<'_>, name: &str) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        let name_owned = name.to_string();
        spawn_rust_db_task_with_attr(
            py,
            async move {
                tokio::task::spawn_blocking(move || inner.release_savepoint_tx(&name_owned))
                    .await
                    .map_err(|e| DuckDBClientError::Operational(e.to_string()))?
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn interrupt(&self) {
        self.inner.interrupt_all();
    }

    pub fn cancel(&self) {
        self.inner.interrupt_all();
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        let inner = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let _ = tokio::task::spawn_blocking(move || inner.close_all()).await;
                Ok::<(), DuckDBClientError>(())
            },
            |py, ()| Ok(py.None()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn to_sync(&self) -> SyncDuckDBClient {
        SyncDuckDBClient {
            inner: Arc::clone(&self.inner),
        }
    }

    pub fn __repr__(&self) -> String {
        format!(
            "AsyncDuckDBClient(database={:?}, read_only={}, max_connections={}, in_transaction={}, closed={})",
            self.inner.database,
            self.inner.read_only,
            self.inner.max_connections,
            self.in_transaction(),
            self.closed()
        )
    }

    pub fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        spawn_rust_db_task_with_attr(
            py,
            async move { Ok::<Py<Self>, DuckDBClientError>(slf) },
            |_, obj| Ok(obj.into_any()),
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    #[pyo3(signature = (_exc_type=None, _exc_val=None, _exc_tb=None))]
    pub fn __aexit__(
        &self,
        py: Python<'_>,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_val: Option<&Bound<'_, PyAny>>,
        _exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        self.close(py)
    }
}

/// Native `vertebra._duckdb` Python extension module.
#[pymodule(gil_used = false)]
pub fn _duckdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_class::<SyncDuckDBClient>()?;
    m.add_class::<AsyncDuckDBClient>()?;
    m.add(
        "__all__",
        vec![
            "FREE_THREADED",
            "GIL_USED",
            "AsyncDuckDBClient",
            "SyncDuckDBClient",
            "__version__",
        ],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_pool(max_connections: usize) -> Arc<DuckDBInner> {
        DuckDBInner::open(
            ":memory:",
            false,
            max_connections,
            Some(256 * 1024 * 1024),
            Some(2),
            None,
            true,
            true,
            true,
            None,
            None,
            None,
        )
        .expect("in-memory pool should open")
    }

    #[test]
    fn test_pool_bootstrap_and_shared_catalog() {
        let pool = make_pool(4);
        assert!(pool.is_in_memory);
        assert_eq!(pool.max_connections, 4);
        assert!(
            pool.bootstrap_statements
                .iter()
                .any(|s| s.contains("memory_limit"))
        );

        let flag = pool.register_cancel_flag();
        pool.execute_sql(
            "CREATE TABLE items (id INTEGER PRIMARY KEY, label VARCHAR, score DOUBLE)".to_string(),
            BoundParams::Empty,
            &flag,
        )
        .expect("DDL should succeed");

        let sets = vec![
            BoundParams::Positional(vec![
                DuckValue::Int(1),
                DuckValue::Text("alpha".to_string()),
                DuckValue::Double(1.5),
            ]),
            BoundParams::Positional(vec![
                DuckValue::Int(2),
                DuckValue::Text("beta".to_string()),
                DuckValue::Double(2.5),
            ]),
        ];
        let inserted = pool
            .execute_many_sql(
                "INSERT INTO items VALUES (?, ?, ?)".to_string(),
                sets,
                &flag,
            )
            .expect("execute_many should succeed");
        assert_eq!(inserted, 2);

        let mut named = HashMap::new();
        named.insert("min_score".to_string(), DuckValue::Double(2.0));
        let res = pool
            .query_sql(
                "SELECT id, label FROM items WHERE score >= :min_score".to_string(),
                BoundParams::Named(named),
                &flag,
            )
            .expect("named query should succeed");
        assert_eq!(res.columns, vec!["id", "label"]);
        assert_eq!(res.rows.len(), 1);
    }

    #[test]
    fn test_transactions_and_error_classification() {
        let pool = make_pool(2);
        let flag = pool.register_cancel_flag();
        pool.execute_sql(
            "CREATE TABLE accounts (id INTEGER PRIMARY KEY, balance INTEGER)".to_string(),
            BoundParams::Empty,
            &flag,
        )
        .unwrap();

        pool.begin_tx(&flag).unwrap();
        assert!(pool.in_transaction.load(Ordering::Acquire));
        pool.execute_sql(
            "INSERT INTO accounts VALUES (1, 100)".to_string(),
            BoundParams::Empty,
            &flag,
        )
        .unwrap();
        pool.rollback_tx(&flag).unwrap();
        assert!(!pool.in_transaction.load(Ordering::Acquire));

        let res = pool
            .query_sql(
                "SELECT COUNT(*) AS cnt FROM accounts".to_string(),
                BoundParams::Empty,
                &flag,
            )
            .unwrap();
        assert_eq!(res.rows[0][0], DuckValue::BigInt(0));

        pool.begin_tx(&flag).unwrap();
        pool.execute_sql(
            "INSERT INTO accounts VALUES (1, 250)".to_string(),
            BoundParams::Empty,
            &flag,
        )
        .unwrap();
        pool.commit_tx(&flag).unwrap();

        let dup_err = pool
            .execute_sql(
                "INSERT INTO accounts VALUES (1, 500)".to_string(),
                BoundParams::Empty,
                &flag,
            )
            .unwrap_err();
        assert!(matches!(dup_err, DuckDBClientError::Integrity(_)));

        let syntax_err = pool
            .execute_sql("SELEC BROKEN".to_string(), BoundParams::Empty, &flag)
            .unwrap_err();
        assert!(matches!(syntax_err, DuckDBClientError::Programming(_)));
    }

    #[test]
    fn test_arrow_shm_and_appender_roundtrip() {
        let pool = make_pool(2);
        let flag = pool.register_cancel_flag();
        pool.execute_sql(
            "CREATE TABLE metrics (id INTEGER, name VARCHAR, val DOUBLE)".to_string(),
            BoundParams::Empty,
            &flag,
        )
        .unwrap();
        pool.execute_sql(
            "INSERT INTO metrics VALUES (1, 'cpu', 0.75), (2, 'mem', 0.50), (3, 'io', 0.25)"
                .to_string(),
            BoundParams::Empty,
            &flag,
        )
        .unwrap();

        let payload = pool
            .query_arrow_shm(
                "SELECT id, name, val FROM metrics ORDER BY id".to_string(),
                BoundParams::Empty,
                2,
                &flag,
            )
            .expect("query_arrow_shm should succeed");
        assert_eq!(payload.row_count, 3);
        assert!(payload.byte_len > 0);

        assert!(payload.shm_name.len() <= 31);
        let full_region_len = payload.region.len();
        assert!(full_region_len >= ShmArrowFrameHeader::SIZE + payload.byte_len);
        // SAFETY: `payload.region` maps `full_region_len` readable bytes immobilized by `Arc`.
        let full_slice =
            unsafe { std::slice::from_raw_parts(payload.region.as_ptr(), full_region_len) };
        assert_eq!(&full_slice[..8], b"VTBARROW");

        let loaded = pool
            .load_arrow_bulk(
                "metrics_copy".to_string(),
                BulkLoadSource::ArrowIpc(full_slice.to_vec()),
                None,
                true,
                &flag,
            )
            .expect("load_arrow_bulk should succeed on page-padded VTBARROW slice");
        assert_eq!(loaded, 3);

        let verify = pool
            .query_sql(
                "SELECT COUNT(*) FROM metrics_copy".to_string(),
                BoundParams::Empty,
                &flag,
            )
            .unwrap();
        assert_eq!(verify.rows[0][0], DuckValue::BigInt(3));
    }

    #[test]
    fn test_mutating_cte_journal_and_arrow_ipc_file_padding() {
        let pool = make_pool(2);
        let flag = pool.register_cancel_flag();
        pool.execute_sql(
            "CREATE TABLE cte_tbl (id INTEGER PRIMARY KEY, label VARCHAR)".to_string(),
            BoundParams::Empty,
            &flag,
        )
        .unwrap();

        pool.begin_tx(&flag).unwrap();
        pool.savepoint_tx("sp_before_cte").unwrap();
        let cte_res = pool
            .query_sql(
                "/* lead comment */ WITH src AS (SELECT 10 AS id, 'cte_row' AS label) INSERT INTO cte_tbl SELECT * FROM src RETURNING id".to_string(),
                BoundParams::Empty,
                &flag,
            )
            .unwrap();
        assert_eq!(cte_res.rows.len(), 1);
        pool.savepoint_tx("sp_after_cte").unwrap();
        pool.execute_sql(
            "INSERT INTO cte_tbl VALUES (20, 'discarded')".to_string(),
            BoundParams::Empty,
            &flag,
        )
        .unwrap();
        pool.rollback_to_savepoint_tx("sp_after_cte", &flag)
            .unwrap();
        pool.commit_tx(&flag).unwrap();

        let kept = pool
            .query_sql(
                "SELECT id, label FROM cte_tbl ORDER BY id".to_string(),
                BoundParams::Empty,
                &flag,
            )
            .unwrap();
        assert_eq!(kept.rows.len(), 1);
        assert_eq!(kept.rows[0][0], DuckValue::Int(10));

        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow_array::Int32Array::from(vec![7, 8]))],
        )
        .unwrap();
        let mut file_bytes = Vec::new();
        {
            let mut fw = arrow_ipc::writer::FileWriter::try_new(&mut file_bytes, &schema).unwrap();
            fw.write(&batch).unwrap();
            fw.finish().unwrap();
        }
        let header = ShmArrowFrameHeader {
            magic: *b"VTBARROW",
            byte_len: file_bytes.len() as u64,
        };
        let mut padded_frame = Vec::with_capacity(4096);
        padded_frame.extend_from_slice(&header.to_bytes());
        padded_frame.extend_from_slice(&file_bytes);
        padded_frame.resize(4096, 0);
        let (parsed_schema, parsed_batches) = parse_arrow_ipc_batches(&padded_frame)
            .expect("page-padded VTBARROW wrapping Arrow IPC File should parse");
        assert_eq!(parsed_schema.fields().len(), 1);
        assert_eq!(parsed_batches[0].num_rows(), 2);
    }

    #[test]
    fn test_shared_secret_catalog_across_pooled_workers() {
        let pool = make_pool(3);
        let flag = pool.register_cancel_flag();
        pool.execute_sql(
            "CREATE OR REPLACE SECRET test_http_secret (TYPE http, BEARER_TOKEN 'tok_v1')"
                .to_string(),
            BoundParams::Empty,
            &flag,
        )
        .expect("CREATE OR REPLACE SECRET TYPE http should succeed");

        for _ in 0..4 {
            let res = pool
                .query_sql(
                    "SELECT name, type FROM duckdb_secrets() WHERE name = 'test_http_secret'"
                        .to_string(),
                    BoundParams::Empty,
                    &flag,
                )
                .expect("reader worker should see shared SecretManager catalog");
            assert_eq!(res.rows.len(), 1);
            assert_eq!(
                res.rows[0][0],
                DuckValue::Text("test_http_secret".to_string())
            );
            assert_eq!(res.rows[0][1], DuckValue::Text("http".to_string()));
        }
    }
}
