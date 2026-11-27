//! Native DuckDB 2.0 client extension for Vertebra (`vertebra._duckdb`).
//!
//! Provides [`SyncDuckDBClient`] (executed on the calling thread with the GIL released) and
//! [`AsyncDuckDBClient`] (bridged through [`AsyncCompletionQueue`] + [`Notifier`]
//! via `vertebra_db::py_async::spawn_rust_db_task_with_attr` with
//! `"_vertebra_duckdb_completion_drainer"`), zero-copy `VTBARROW` shared-memory
//! Arrow IPC cursors, and native `duckdb::Appender` bulk ingestion.

mod cursor;
pub mod errors;

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::io::Cursor;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use arrow_array::RecordBatch;
use arrow_ipc::reader::{FileReader, StreamReader};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use duckdb::types::{TimeUnit as DuckTimeUnit, Value as DuckValue};
use duckdb::{AccessMode, Config, Connection, InterruptHandle, params_from_iter};
use pyo3::buffer::PyBuffer;
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyCapsule, PyDict, PyFloat, PyInt, PyList, PyMapping, PyString,
    PyTuple,
};
use vertebra_core::broker::DIALECT_TAG_DUCKDB;
use vertebra_core::dbrpc::DbRpcOp;
use vertebra_core::shm::ShmRegion;
use vertebra_core::text::classify_sql_statement;
use vertebra_db::arrow_shm::{
    ACTIVE_ARROW_SHM_REGIONS, ShmArrowFrameHeader, ShmArrowStreamWriter, release_shm_arrow_frame,
};
use vertebra_db::duckdb::DuckdbConfig;
use vertebra_db::py_async::{
    TaskAwaitable, construct_py_arrow_cursor, spawn_rust_db_task_with_attr,
};
pub use vertebra_db::py_broker::FREE_THREADED;
use vertebra_db::py_broker::{BrokerPoolRegistry, next_shm_name};
use vertebra_db::{
    BrokerEngineBackend, BrokerPoolBackend, DbRpcEncodedResponse, DbRpcScalar, PlaceholderStyle,
    decode_dbrpc_request_frame, encode_dbrpc_response_frame, new_broker_engine_pycapsule,
    normalize_placeholders,
};

pub use crate::cursor::{AsyncDuckDBCursor, SyncDuckDBCursor};
use crate::cursor::{outcome_to_dicts, outcome_to_dml, outcome_to_rows, outcome_to_value};
use crate::errors::DuckDBClientError;

const DUCKDB_COMPLETION_DRAINER_ATTR: &str = "_vertebra_duckdb_completion_drainer";

/// Maximum number of distinct SQL texts whose facts a pool caches before the cache is cleared.
pub const STATEMENT_FACTS_CAPACITY: usize = 256;

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

/// Facts derived from one SQL text, scanned once and cached per pool.
#[derive(Clone, Debug)]
pub struct StatementFacts {
    /// The statement only reads and may run on a reader worker.
    pub read_only: bool,
    /// The statement changes data or catalog state.
    pub mutating: bool,
    /// The statement produces a result set.
    pub returns_rows: bool,
    /// The leading keyword may start a transaction-control statement.
    pub maybe_tx_control: bool,
    /// The SQL text with `:name` placeholders rewritten to `$name`.
    pub named_sql: Arc<str>,
}

impl StatementFacts {
    fn scan(sql: &str) -> Self {
        let maybe_tx_control = matches!(
            leading_sql_keyword(sql).as_str(),
            "BEGIN" | "START" | "COMMIT" | "END" | "ROLLBACK" | "ABORT" | "SAVEPOINT" | "RELEASE"
        );
        Self {
            read_only: is_read_only_sql(sql),
            mutating: is_mutating_sql(sql),
            returns_rows: sql_returns_rows(sql),
            maybe_tx_control,
            named_sql: Arc::from(normalize_named_sql(sql)),
        }
    }
}

/// Columns, rows, and changed-row count of one statement execution.
#[derive(Clone, Debug)]
pub struct StatementOutcome {
    /// Ordered column names, or `None` when the statement produces no result set.
    pub columns: Option<Vec<String>>,
    /// Row values aligned with `columns`.
    pub rows: Vec<Vec<DuckValue>>,
    /// Changed-row count, or `-1` for a non-mutating result set.
    pub rowcount: i64,
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

struct WorkerSlot {
    conn: Mutex<Option<Connection>>,
    interrupt_handle: Arc<InterruptHandle>,
    busy: AtomicBool,
    active_cancel: Mutex<Option<Arc<AtomicBool>>>,
}

impl WorkerSlot {
    fn spawn(conn: Connection) -> Self {
        let interrupt_handle = conn.interrupt_handle();
        Self {
            conn: Mutex::new(Some(conn)),
            interrupt_handle,
            busy: AtomicBool::new(false),
            active_cancel: Mutex::new(None),
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
        F: FnOnce(&mut Connection) -> Result<T, DuckDBClientError>,
    {
        if cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB query cancelled before execution".to_string(),
            ));
        }
        let mut conn_guard = self
            .conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(conn) = conn_guard.as_mut() else {
            return Err(DuckDBClientError::Operational(
                "DuckDB worker connection is closed".to_string(),
            ));
        };
        if cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB query interrupted".to_string(),
            ));
        }
        self.busy.store(true, Ordering::Release);
        *self
            .active_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(Arc::clone(cancel_flag));
        let outcome = f(conn);
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
        let _ = self
            .conn
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
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
    statement_facts: RwLock<HashMap<Box<str>, StatementFacts>>,
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
            statement_facts: RwLock::new(HashMap::new()),
        }))
    }

    fn ensure_open(&self) -> Result<(), DuckDBClientError> {
        if self.closed.load(Ordering::Acquire) {
            return Err(DuckDBClientError::InvalidInput(
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

    /// Return the cached facts for `sql`, scanning the text only on a cache miss.
    pub fn statement_facts(&self, sql: &str) -> StatementFacts {
        {
            let cache = self
                .statement_facts
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(facts) = cache.get(sql) {
                return facts.clone();
            }
        }
        let facts = StatementFacts::scan(sql);
        let mut cache = self
            .statement_facts
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if cache.len() >= STATEMENT_FACTS_CAPACITY && !cache.contains_key(sql) {
            cache.clear();
        }
        cache.insert(Box::from(sql), facts.clone());
        facts
    }

    /// Number of SQL texts currently held in the facts cache.
    pub fn statement_facts_len(&self) -> usize {
        self.statement_facts
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    /// Run one statement and report its columns, rows, and changed-row count.
    pub fn run_statement(
        &self,
        sql: String,
        params: BoundParams,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<StatementOutcome, DuckDBClientError> {
        let facts = self.statement_facts(&sql);
        if !facts.returns_rows {
            let changed = self.execute_sql(sql, params, cancel_flag)?;
            return Ok(StatementOutcome {
                columns: None,
                rows: Vec::new(),
                rowcount: changed,
            });
        }
        let result = self.query_sql(sql, params, cancel_flag)?;
        let rowcount = if facts.mutating {
            i64::try_from(result.rows.len()).unwrap_or(i64::MAX)
        } else {
            -1
        };
        Ok(StatementOutcome {
            columns: Some(result.columns),
            rows: result.rows,
            rowcount,
        })
    }

    /// Run one statement once per parameter set and report the total changed-row count.
    pub fn run_statement_many(
        &self,
        sql: String,
        param_sets: Vec<BoundParams>,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<StatementOutcome, DuckDBClientError> {
        let total = self.execute_many_sql(sql, param_sets, cancel_flag)?;
        Ok(StatementOutcome {
            columns: None,
            rows: Vec::new(),
            rowcount: total,
        })
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
        let facts = self.statement_facts(&sql);
        if self.read_only && facts.mutating {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        if facts.maybe_tx_control && self.run_tx_control(&sql, cancel_flag)? {
            return Ok(0);
        }

        let is_read = facts.read_only;
        let is_mut = facts.mutating;
        let named_sql = facts.named_sql;
        let needs_tx_log = (is_mut || !is_read) && self.in_transaction.load(Ordering::Acquire);
        let log_payload = if needs_tx_log {
            Some((sql.clone(), params.clone()))
        } else {
            None
        };
        let changed = self.run_on_pool(is_read, cancel_flag, move |conn| {
            execute_statement_on_conn(conn, &sql, &named_sql, &params)
        })?;
        if let Some((log_sql, log_params)) = log_payload {
            self.record_tx_op(TxLogOp::Execute {
                sql: log_sql,
                params: log_params,
            });
        }
        Ok(changed)
    }

    /// Handle `sql` as a transaction-control statement, returning whether it was one.
    fn run_tx_control(
        &self,
        sql: &str,
        cancel_flag: &Arc<AtomicBool>,
    ) -> Result<bool, DuckDBClientError> {
        let trimmed = sql.trim().trim_end_matches(';').trim();
        let trimmed_upper = trimmed.to_ascii_uppercase();
        if trimmed_upper == "BEGIN"
            || trimmed_upper == "BEGIN TRANSACTION"
            || trimmed_upper == "START TRANSACTION"
        {
            self.begin_tx(cancel_flag)?;
            return Ok(true);
        }
        if trimmed_upper == "COMMIT" || trimmed_upper == "END TRANSACTION" {
            self.commit_tx(cancel_flag)?;
            return Ok(true);
        }
        if trimmed_upper == "ROLLBACK" || trimmed_upper == "ABORT" {
            self.rollback_tx(cancel_flag)?;
            return Ok(true);
        }
        if trimmed_upper.starts_with("SAVEPOINT ") {
            let sp_name = trimmed["SAVEPOINT ".len()..].trim();
            self.savepoint_tx(sp_name)?;
            return Ok(true);
        }
        if trimmed_upper.starts_with("ROLLBACK TO ") {
            let after_to = trimmed["ROLLBACK TO ".len()..].trim();
            let sp_name = if after_to.to_ascii_uppercase().starts_with("SAVEPOINT ") {
                after_to["SAVEPOINT ".len()..].trim()
            } else {
                after_to
            };
            self.rollback_to_savepoint_tx(sp_name, cancel_flag)?;
            return Ok(true);
        }
        if trimmed_upper.starts_with("RELEASE ") {
            let after_rel = trimmed["RELEASE ".len()..].trim();
            let sp_name = if after_rel.to_ascii_uppercase().starts_with("SAVEPOINT ") {
                after_rel["SAVEPOINT ".len()..].trim()
            } else {
                after_rel
            };
            self.release_savepoint_tx(sp_name)?;
            return Ok(true);
        }
        Ok(false)
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
        let facts = self.statement_facts(&sql);
        if self.read_only && facts.mutating {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        if param_sets.is_empty() {
            return Ok(0);
        }
        let named_sql = facts.named_sql;
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
                total += execute_statement_on_conn(conn, &sql, &named_sql, params)?;
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
        let facts = self.statement_facts(&sql);
        if self.read_only && facts.mutating {
            return Err(DuckDBClientError::Programming(
                "Cannot execute mutating SQL statement in read-only mode".to_string(),
            ));
        }
        let is_read = facts.read_only;
        let is_mut = facts.mutating;
        let named_sql = facts.named_sql;
        let needs_tx_log = is_mut && self.in_transaction.load(Ordering::Acquire);
        let log_payload = if needs_tx_log {
            Some((sql.clone(), params.clone()))
        } else {
            None
        };
        let flag_for_worker = Arc::clone(cancel_flag);
        let res = self.run_on_pool(is_read, cancel_flag, move |conn| {
            query_rows_on_conn(conn, &sql, &named_sql, &params, &flag_for_worker)
        })?;
        if let Some((log_sql, log_params)) = log_payload {
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
        let replay_ops: Vec<(TxLogOp, Option<Arc<str>>)> = replay_ops
            .into_iter()
            .map(|op| {
                let named_sql = match &op {
                    TxLogOp::Execute { sql, .. } | TxLogOp::ExecuteMany { sql, .. } => {
                        Some(self.statement_facts(sql).named_sql)
                    }
                    TxLogOp::LoadArrow { .. } => None,
                };
                (op, named_sql)
            })
            .collect();
        let flag_for_worker = Arc::clone(cancel_flag);
        self.workers[0].execute(cancel_flag, move |conn| {
            conn.execute_batch("ROLLBACK; BEGIN TRANSACTION")
                .map_err(DuckDBClientError::from_duckdb)?;
            for (op, named_sql) in replay_ops {
                let named_sql = named_sql.as_deref().unwrap_or_default();
                match op {
                    TxLogOp::Execute { sql, params } => {
                        execute_statement_on_conn(conn, &sql, named_sql, &params)?;
                    }
                    TxLogOp::ExecuteMany { sql, param_sets } => {
                        for params in &param_sets {
                            execute_statement_on_conn(conn, &sql, named_sql, params)?;
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
    named_sql: &str,
    params: &BoundParams,
) -> Result<i64, DuckDBClientError> {
    let effective_sql = match params {
        BoundParams::Named(_) => named_sql,
        _ => sql,
    };
    let mut stmt = match conn.prepare(effective_sql) {
        Ok(s) => s,
        Err(duckdb::Error::MultipleStatement) if params.is_empty() => {
            conn.execute_batch(effective_sql)
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
    named_sql: &str,
    params: &BoundParams,
    cancel_flag: &Arc<AtomicBool>,
) -> Result<QueryResultSet, DuckDBClientError> {
    let effective_sql = match params {
        BoundParams::Named(_) => named_sql,
        _ => sql,
    };
    let mut stmt = conn
        .prepare(effective_sql)
        .map_err(DuckDBClientError::from_duckdb)?;
    let ordered = resolve_ordered_params(&stmt, params)?;
    let mut rows = stmt
        .query(params_from_iter(ordered))
        .map_err(DuckDBClientError::from_duckdb)?;

    let mut out_rows = Vec::new();
    let mut null_columns: Option<Vec<bool>> = None;
    while let Some(row) = rows.next().map_err(DuckDBClientError::from_duckdb)? {
        if cancel_flag.load(Ordering::Acquire) {
            return Err(DuckDBClientError::QueryTimeout(
                "DuckDB query interrupted while fetching rows".to_string(),
            ));
        }
        let is_null_col = null_columns.get_or_insert_with(|| {
            row.as_ref()
                .schema()
                .fields()
                .iter()
                .map(|field| matches!(field.data_type(), DataType::Null))
                .collect()
        });
        let row_col_count = row.as_ref().column_count();
        let mut row_vals = Vec::with_capacity(row_col_count);
        for idx in 0..row_col_count {
            let val: DuckValue = if is_null_col.get(idx).copied().unwrap_or(false) {
                DuckValue::Null
            } else {
                row.get(idx).map_err(DuckDBClientError::from_duckdb)?
            };
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
    let seg_name = next_shm_name("/vtb-dd", '-');
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
    let seg_name = next_shm_name("/vtb-dd", '-');
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
    if obj
        .get_type()
        .name()
        .is_ok_and(|name| name == "TypedParameter")
        && let Ok(inner) = obj.getattr("value")
    {
        return py_to_duck_value(&inner);
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
    if let Ok(tup) = obj.cast_exact::<PyTuple>() {
        if tup.is_empty() {
            return Ok(BoundParams::Empty);
        }
        let mut vals = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            vals.push(py_to_duck_value(&item)?);
        }
        return Ok(BoundParams::Positional(vals));
    }
    if let Ok(list) = obj.cast_exact::<PyList>() {
        if list.is_empty() {
            return Ok(BoundParams::Empty);
        }
        let mut vals = Vec::with_capacity(list.len());
        for item in list.iter() {
            vals.push(py_to_duck_value(&item)?);
        }
        return Ok(BoundParams::Positional(vals));
    }
    if let Ok(dict) = obj.cast_exact::<PyDict>() {
        if dict.is_empty() {
            return Ok(BoundParams::Empty);
        }
        let map = RefCell::new(HashMap::with_capacity(dict.len()));
        let first_err = RefCell::new(None);
        if let Err(py_err) = dict.locked_for_each(|k, v| {
            let key: String = match k.extract::<String>() {
                Ok(key) => key,
                Err(e) => {
                    *first_err.borrow_mut() = Some(DuckDBClientError::InvalidInput(e.to_string()));
                    return Err(e);
                }
            };
            match py_to_duck_value(&v) {
                Ok(val) => {
                    map.borrow_mut().insert(key, val);
                    Ok(())
                }
                Err(e) => {
                    let msg = e.to_string();
                    *first_err.borrow_mut() = Some(e);
                    Err(pyo3::exceptions::PyValueError::new_err(msg))
                }
            }
        }) {
            return Err(first_err
                .into_inner()
                .unwrap_or_else(|| DuckDBClientError::InvalidInput(py_err.to_string())));
        }
        return Ok(BoundParams::Named(map.into_inner()));
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
        if list.is_empty() {
            return Ok(BoundParams::Empty);
        }
        let mut vals = Vec::with_capacity(list.len());
        for item in list.iter() {
            vals.push(py_to_duck_value(&item)?);
        }
        return Ok(BoundParams::Positional(vals));
    }
    if let Ok(tup) = obj.cast::<PyTuple>() {
        if tup.is_empty() {
            return Ok(BoundParams::Empty);
        }
        let mut vals = Vec::with_capacity(tup.len());
        for item in tup.iter() {
            vals.push(py_to_duck_value(&item)?);
        }
        return Ok(BoundParams::Positional(vals));
    }
    if let Ok(mapping) = obj.cast::<PyMapping>() {
        let keys = mapping
            .keys()
            .map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
        if keys.is_empty() {
            return Ok(BoundParams::Empty);
        }
        let mut map = HashMap::with_capacity(keys.len());
        for k in keys.iter() {
            let key: String = k
                .extract::<String>()
                .map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
            let val = mapping
                .get_item(&k)
                .map_err(|e| DuckDBClientError::InvalidInput(e.to_string()))?;
            map.insert(key, py_to_duck_value(&val)?);
        }
        return Ok(BoundParams::Named(map));
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

fn shm_payloads_to_py_list(py: Python<'_>, payloads: Vec<ShmArrowPayload>) -> PyResult<Py<PyAny>> {
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

macro_rules! impl_duckdb_client_pymethods {
    (
        $client:ident,
        $name:literal,
        $cursor:ident,
        { $($extra:tt)* }
    ) => {
        #[pymethods]
        impl $client {
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

            pub fn interrupt(&self) {
                self.inner.interrupt_all();
            }

            pub fn cancel(&self) {
                self.inner.interrupt_all();
            }

            #[pyo3(signature = (*, lazy_begin = false))]
            pub fn cursor(&self, lazy_begin: bool) -> $cursor {
                $cursor::new(Arc::clone(&self.inner), lazy_begin)
            }

            pub fn __repr__(&self) -> String {
                format!(
                    "{}(database={:?}, read_only={}, max_connections={}, in_transaction={}, closed={})",
                    $name,
                    self.inner.database,
                    self.inner.read_only,
                    self.inner.max_connections,
                    self.in_transaction(),
                    self.closed()
                )
            }

            $($extra)*
        }
    };
}

/// Native synchronous DuckDB 2.0 client executed on the calling thread.
#[pyclass(frozen, immutable_type)]
pub struct SyncDuckDBClient {
    inner: Arc<DuckDBInner>,
}

impl SyncDuckDBClient {
    fn run_once(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<StatementOutcome> {
        let bound = extract_bound_params(params).map_err(|e| e.into_py_err(py))?;
        let inner = Arc::clone(&self.inner);
        let sql_owned = sql.to_string();
        let cancel_flag = inner.register_cancel_flag();
        py.detach(move || inner.run_statement(sql_owned, bound, &cancel_flag))
            .map_err(|e| e.into_py_err(py))
    }
}

impl_duckdb_client_pymethods!(SyncDuckDBClient, "SyncDuckDBClient", SyncDuckDBCursor, {
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
        shm_payloads_to_py_list(py, payloads)
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

    pub fn close(&self, py: Python<'_>) {
        let inner = Arc::clone(&self.inner);
        py.detach(move || inner.close_all());
    }

    pub fn to_async(&self) -> AsyncDuckDBClient {
        AsyncDuckDBClient {
            inner: Arc::clone(&self.inner),
        }
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
        outcome_to_value(py, outcome)
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
});

/// Native asynchronous DuckDB 2.0 client bridged via [`AsyncCompletionQueue`] + [`Notifier`].
#[pyclass(frozen, immutable_type)]
pub struct AsyncDuckDBClient {
    inner: Arc<DuckDBInner>,
}

impl AsyncDuckDBClient {
    fn spawn_cancellable<T, W, M>(
        &self,
        py: Python<'_>,
        work: W,
        map_ok: M,
    ) -> PyResult<TaskAwaitable>
    where
        T: Send + 'static,
        W: FnOnce(Arc<DuckDBInner>, Arc<AtomicBool>) -> Result<T, DuckDBClientError>
            + Send
            + 'static,
        M: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = tokio::task::spawn_blocking(move || work(inner, cancel_flag))
                    .await
                    .map_err(|e| DuckDBClientError::Operational(e.to_string()))?;
                drop_guard.mark_completed();
                res
            },
            map_ok,
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    fn spawn_blocking_op<T, W, M>(
        &self,
        py: Python<'_>,
        work: W,
        map_ok: M,
    ) -> PyResult<TaskAwaitable>
    where
        T: Send + 'static,
        W: FnOnce(Arc<DuckDBInner>) -> Result<T, DuckDBClientError> + Send + 'static,
        M: FnOnce(Python<'_>, T) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let inner = Arc::clone(&self.inner);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                tokio::task::spawn_blocking(move || work(inner))
                    .await
                    .map_err(|e| DuckDBClientError::Operational(e.to_string()))?
            },
            map_ok,
            |py, err| err.into_py_err(py),
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }

    fn spawn_single_call<M>(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        map_ok: M,
    ) -> PyResult<TaskAwaitable>
    where
        M: FnOnce(Python<'_>, StatementOutcome) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let bound = extract_bound_params(params).map_err(|e| e.into_py_err(py))?;
        let sql_owned = sql.to_string();
        self.spawn_cancellable(
            py,
            move |inner, cancel_flag| inner.run_statement(sql_owned, bound, &cancel_flag),
            map_ok,
        )
    }
}

impl_duckdb_client_pymethods!(AsyncDuckDBClient, "AsyncDuckDBClient", AsyncDuckDBCursor, {
    #[pyo3(signature = (sql, parameters=None))]
    pub fn execute(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<TaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let sql_owned = sql.to_string();
        self.spawn_cancellable(
            py,
            move |inner, cancel_flag| inner.execute_sql(sql_owned, bound, &cancel_flag),
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn execute_many(
        &self,
        py: Python<'_>,
        sql: &str,
        parameter_sets: &Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let bound_sets = extract_parameter_sets(parameter_sets).map_err(|e| e.into_py_err(py))?;
        let sql_owned = sql.to_string();
        self.spawn_cancellable(
            py,
            move |inner, cancel_flag| inner.execute_many_sql(sql_owned, bound_sets, &cancel_flag),
            |py, count| Ok(count.into_pyobject(py)?.into_any().unbind()),
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
    ) -> PyResult<TaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let sql_owned = sql.to_string();
        self.spawn_cancellable(
            py,
            move |inner, cancel_flag| {
                inner.query_arrow_payload(
                    sql_owned,
                    bound,
                    batch_size,
                    threshold_rows,
                    &cancel_flag,
                )
            },
            arrow_result_payload_to_py_cursor,
        )
    }

    #[pyo3(signature = (sql, parameters=None, *, batch_size=65536))]
    pub fn stream_arrow_batches(
        &self,
        py: Python<'_>,
        sql: &str,
        parameters: Option<&Bound<'_, PyAny>>,
        batch_size: usize,
    ) -> PyResult<TaskAwaitable> {
        let bound = extract_bound_params(parameters).map_err(|e| e.into_py_err(py))?;
        let sql_owned = sql.to_string();
        self.spawn_cancellable(
            py,
            move |inner, cancel_flag| {
                inner.stream_arrow_shm_batches(sql_owned, bound, batch_size, &cancel_flag)
            },
            shm_payloads_to_py_list,
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
    ) -> PyResult<TaskAwaitable> {
        let load_source = extract_bulk_load_source(py, source, columns.as_deref())
            .map_err(|e| e.into_py_err(py))?;
        let table_owned = table.to_string();
        self.spawn_cancellable(
            py,
            move |inner, cancel_flag| {
                inner.load_arrow_bulk(
                    table_owned,
                    load_source,
                    columns,
                    create_if_missing,
                    &cancel_flag,
                )
            },
            |py, rows| Ok(rows.into_pyobject(py)?.into_any().unbind()),
        )
    }

    pub fn begin(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        self.spawn_cancellable(
            py,
            |inner, cancel_flag| inner.begin_tx(&cancel_flag),
            |py, ()| Ok(py.None()),
        )
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        self.spawn_cancellable(
            py,
            |inner, cancel_flag| inner.commit_tx(&cancel_flag),
            |py, ()| Ok(py.None()),
        )
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
        self.spawn_cancellable(
            py,
            |inner, cancel_flag| inner.rollback_tx(&cancel_flag),
            |py, ()| Ok(py.None()),
        )
    }

    pub fn savepoint(&self, py: Python<'_>, name: &str) -> PyResult<TaskAwaitable> {
        let name_owned = name.to_string();
        self.spawn_blocking_op(
            py,
            move |inner| inner.savepoint_tx(&name_owned),
            |py, ()| Ok(py.None()),
        )
    }

    pub fn rollback_to_savepoint(&self, py: Python<'_>, name: &str) -> PyResult<TaskAwaitable> {
        let name_owned = name.to_string();
        self.spawn_cancellable(
            py,
            move |inner, cancel_flag| inner.rollback_to_savepoint_tx(&name_owned, &cancel_flag),
            |py, ()| Ok(py.None()),
        )
    }

    pub fn release_savepoint(&self, py: Python<'_>, name: &str) -> PyResult<TaskAwaitable> {
        let name_owned = name.to_string();
        self.spawn_blocking_op(
            py,
            move |inner| inner.release_savepoint_tx(&name_owned),
            |py, ()| Ok(py.None()),
        )
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<TaskAwaitable> {
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

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_rows(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<TaskAwaitable> {
        self.spawn_single_call(py, sql, params, |py, outcome| {
            let (rows, names) = outcome_to_rows(py, outcome)?;
            Ok((rows, names).into_pyobject(py)?.into_any().unbind())
        })
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_dicts(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<TaskAwaitable> {
        self.spawn_single_call(py, sql, params, |py, outcome| {
            Ok(outcome_to_dicts(py, outcome)?.into_any().unbind())
        })
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn fetch_value(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<TaskAwaitable> {
        self.spawn_single_call(py, sql, params, outcome_to_value)
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute_dml(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<TaskAwaitable> {
        self.spawn_single_call(py, sql, params, |py, outcome| {
            Ok(outcome_to_dml(&outcome)
                .into_pyobject(py)?
                .into_any()
                .unbind())
        })
    }

    pub fn __aenter__(slf: Py<Self>, py: Python<'_>) -> PyResult<TaskAwaitable> {
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
    ) -> PyResult<TaskAwaitable> {
        self.close(py)
    }
});

const DUCKDB_BROKER_ARROW_SPILL_ROWS: usize = 256;
const DUCKDB_MAX_ACTIVE_SHM: usize = 64;

struct DuckDBPoolEntry {
    inner: Arc<DuckDBInner>,
    active_shm: Mutex<VecDeque<String>>,
}

impl DuckDBPoolEntry {
    fn register_active_shm(&self, shm_name: &str, region: Arc<ShmRegion>) {
        let key = shm_name.to_string();
        ACTIVE_ARROW_SHM_REGIONS.insert(key.clone(), region);
        let evicted = {
            let mut guard = self
                .active_shm
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.retain(|existing| ACTIVE_ARROW_SHM_REGIONS.contains_key(existing));
            let oldest = (guard.len() >= DUCKDB_MAX_ACTIVE_SHM)
                .then(|| guard.pop_front())
                .flatten();
            guard.push_back(key);
            oldest
        };
        if let Some(old_name) = evicted {
            let _ = release_shm_arrow_frame(&old_name);
        }
    }
}

/// Shared-memory broker backend managing single-writer DuckDB pools (`DIALECT_TAG_DUCKDB = 6`).
pub struct DuckDBBrokerBackend {
    pools: BrokerPoolRegistry<DuckDBPoolEntry>,
}

impl Default for DuckDBBrokerBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl DuckDBBrokerBackend {
    /// Construct a new [`DuckDBBrokerBackend`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            pools: BrokerPoolRegistry::new(),
        }
    }
}

fn parse_duckdb_broker_dsn(dsn: &str) -> Result<String, String> {
    let trimmed = dsn.trim();
    if trimmed.is_empty() || trimmed.contains('\0') {
        return Err("DuckDB DSN must not be empty or contain NUL bytes".to_string());
    }
    if trimmed == ":memory:" {
        return Ok(":memory:".to_string());
    }
    if let Some((scheme, rest)) = trimmed.split_once("://") {
        let lower_scheme = scheme.to_ascii_lowercase();
        if lower_scheme != "duckdb" && lower_scheme != "file" {
            return Err(format!("invalid DuckDB DSN scheme `{scheme}://`"));
        }
        let target = rest.split_once('?').map_or(rest, |(path, _)| path).trim();
        if target.is_empty()
            || target == ":memory:"
            || target == "/:memory:"
            || target.eq_ignore_ascii_case("memory")
        {
            return Ok(":memory:".to_string());
        }
        return Ok(target.to_string());
    }
    Ok(trimmed.to_string())
}

fn scalar_to_duck_value(scalar: &DbRpcScalar) -> DuckValue {
    match scalar {
        DbRpcScalar::Null => DuckValue::Null,
        DbRpcScalar::Bool(b) => DuckValue::Boolean(*b),
        DbRpcScalar::Int(v) => DuckValue::BigInt(*v),
        DbRpcScalar::UInt(v) => DuckValue::UBigInt(*v),
        DbRpcScalar::Float(v) => DuckValue::Double(*v),
        DbRpcScalar::Str(s) => DuckValue::Text(s.clone()),
        DbRpcScalar::Bytes(b) => DuckValue::Blob(b.clone()),
    }
}

fn duck_value_to_scalar(val: &DuckValue) -> DbRpcScalar {
    match val {
        DuckValue::Null => DbRpcScalar::Null,
        DuckValue::Boolean(b) => DbRpcScalar::Bool(*b),
        DuckValue::TinyInt(v) => DbRpcScalar::Int(i64::from(*v)),
        DuckValue::SmallInt(v) => DbRpcScalar::Int(i64::from(*v)),
        DuckValue::Int(v) => DbRpcScalar::Int(i64::from(*v)),
        DuckValue::BigInt(v) => DbRpcScalar::Int(*v),
        DuckValue::HugeInt(v) => match i64::try_from(*v) {
            Ok(i) => DbRpcScalar::Int(i),
            Err(_) => DbRpcScalar::Str(v.to_string()),
        },
        DuckValue::UTinyInt(v) => DbRpcScalar::Int(i64::from(*v)),
        DuckValue::USmallInt(v) => DbRpcScalar::Int(i64::from(*v)),
        DuckValue::UInt(v) => DbRpcScalar::Int(i64::from(*v)),
        DuckValue::UBigInt(v) => match i64::try_from(*v) {
            Ok(i) => DbRpcScalar::Int(i),
            Err(_) => DbRpcScalar::UInt(*v),
        },
        DuckValue::Float(v) => DbRpcScalar::Float(f64::from(*v)),
        DuckValue::Double(v) => DbRpcScalar::Float(*v),
        DuckValue::Decimal(d) => DbRpcScalar::Str(d.to_string()),
        DuckValue::Text(s) | DuckValue::Enum(s) => DbRpcScalar::Str(s.clone()),
        DuckValue::Blob(b) => DbRpcScalar::Bytes(b.clone()),
        DuckValue::Union(inner) => duck_value_to_scalar(inner),
        other => DbRpcScalar::Str(format!("{other:?}")),
    }
}

fn sql_returns_rows(sql: &str) -> bool {
    classify_sql_statement(sql).0
        || (!is_mutating_sql(sql) && leading_sql_keyword(sql) == "SUMMARIZE")
}

fn encode_duckdb_err(request_id: u64, err: &DuckDBClientError) -> Vec<u8> {
    encode_dbrpc_response_frame(&DbRpcEncodedResponse::error(
        request_id,
        "DuckDBError",
        err.to_string(),
    ))
}

impl BrokerEngineBackend for DuckDBBrokerBackend {
    fn dialect_tag(&self) -> u16 {
        DIALECT_TAG_DUCKDB
    }

    fn dialect_name(&self) -> &'static str {
        "duckdb"
    }

    fn normalize_statement(&self, sql: &str) -> (String, usize) {
        normalize_placeholders(sql, PlaceholderStyle::QuestionMark)
    }
}

impl BrokerPoolBackend for DuckDBBrokerBackend {
    fn open_pool(&self, dsn: &str, max_size: u32) -> Result<u64, String> {
        if max_size == 0 {
            return Err("DuckDB pool max_size must be >= 1".to_string());
        }
        let db_path = parse_duckdb_broker_dsn(dsn)?;
        let inner = DuckDBInner::open(
            &db_path,
            false,
            max_size as usize,
            None,
            None,
            None,
            true,
            true,
            true,
            None,
            None,
            None,
        )
        .map_err(|e| e.to_string())?;
        let entry = Arc::new(DuckDBPoolEntry {
            inner,
            active_shm: Mutex::new(VecDeque::new()),
        });
        Ok(self.pools.register(entry))
    }

    fn execute(&self, pool_id: u64, frame: &[u8]) -> Result<Vec<u8>, String> {
        let entry = self
            .pools
            .get(pool_id)
            .ok_or_else(|| format!("unknown pool id {pool_id}"))?;

        let req = decode_dbrpc_request_frame(frame)?;
        let req_id = req.request_id;
        let cancel_flag = entry.inner.register_cancel_flag();
        match req.op {
            DbRpcOp::Execute => {
                let raw_sql = req
                    .sql
                    .as_deref()
                    .ok_or_else(|| "DBRPC Execute frame missing SQL".to_string())?;
                let (norm_sql, _) = self.normalize_statement(raw_sql);
                let bound = if req.params.is_empty() {
                    BoundParams::Empty
                } else {
                    BoundParams::Positional(req.params.iter().map(scalar_to_duck_value).collect())
                };
                if sql_returns_rows(&norm_sql) {
                    if req.chunk_rows > 0 {
                        let batch_size = usize::try_from(req.chunk_rows.max(1024)).unwrap_or(65536);
                        return match entry.inner.query_arrow_payload(
                            norm_sql,
                            bound,
                            batch_size,
                            0,
                            &cancel_flag,
                        ) {
                            Ok(ArrowResultPayload::Shm {
                                shm_name,
                                row_count,
                                region,
                                ..
                            }) => {
                                entry.register_active_shm(&shm_name, region);
                                Ok(encode_dbrpc_response_frame(
                                    &DbRpcEncodedResponse::ok_arrow_shm(
                                        req_id,
                                        i64::try_from(row_count).unwrap_or(i64::MAX),
                                        Vec::new(),
                                        shm_name,
                                    ),
                                ))
                            }
                            Ok(ArrowResultPayload::Inline { .. }) => {
                                unreachable!("threshold_rows=0 enforces SHM")
                            }
                            Err(err) => Ok(encode_duckdb_err(req_id, &err)),
                        };
                    }
                    match entry
                        .inner
                        .query_sql(norm_sql.clone(), bound.clone(), &cancel_flag)
                    {
                        Ok(res) if res.rows.len() > DUCKDB_BROKER_ARROW_SPILL_ROWS => {
                            let columns = res.columns;
                            match entry.inner.query_arrow_payload(
                                norm_sql,
                                bound,
                                1024,
                                0,
                                &cancel_flag,
                            ) {
                                Ok(ArrowResultPayload::Shm {
                                    shm_name,
                                    row_count,
                                    region,
                                    ..
                                }) => {
                                    entry.register_active_shm(&shm_name, region);
                                    Ok(encode_dbrpc_response_frame(
                                        &DbRpcEncodedResponse::ok_arrow_shm(
                                            req_id,
                                            i64::try_from(row_count).unwrap_or(i64::MAX),
                                            columns,
                                            shm_name,
                                        ),
                                    ))
                                }
                                Ok(ArrowResultPayload::Inline { .. }) => {
                                    unreachable!("threshold_rows=0 enforces SHM")
                                }
                                Err(err) => Ok(encode_duckdb_err(req_id, &err)),
                            }
                        }
                        Ok(res) => {
                            let rows: Vec<Vec<DbRpcScalar>> = res
                                .rows
                                .iter()
                                .map(|row| row.iter().map(duck_value_to_scalar).collect())
                                .collect();
                            let rowcount = i64::try_from(rows.len()).unwrap_or(i64::MAX);
                            Ok(encode_dbrpc_response_frame(&DbRpcEncodedResponse::ok_rows(
                                req_id,
                                rowcount,
                                res.columns,
                                rows,
                            )))
                        }
                        Err(err) => Ok(encode_duckdb_err(req_id, &err)),
                    }
                } else {
                    match entry.inner.execute_sql(norm_sql, bound, &cancel_flag) {
                        Ok(changed) => Ok(encode_dbrpc_response_frame(
                            &DbRpcEncodedResponse::ok_count(req_id, changed, None),
                        )),
                        Err(err) => Ok(encode_duckdb_err(req_id, &err)),
                    }
                }
            }
            DbRpcOp::Begin => match entry.inner.begin_tx(&cancel_flag) {
                Ok(()) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                )),
                Err(err) => Ok(encode_duckdb_err(req_id, &err)),
            },
            DbRpcOp::Commit => match entry.inner.commit_tx(&cancel_flag) {
                Ok(()) => Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                )),
                Err(err) => Ok(encode_duckdb_err(req_id, &err)),
            },
            DbRpcOp::Rollback => {
                let res = if let Some(sp) = req.savepoint.as_deref() {
                    entry.inner.rollback_to_savepoint_tx(sp, &cancel_flag)
                } else {
                    entry.inner.rollback_tx(&cancel_flag)
                };
                match res {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_duckdb_err(req_id, &err)),
                }
            }
            DbRpcOp::Savepoint => {
                let sp = req
                    .savepoint
                    .as_deref()
                    .ok_or_else(|| "DBRPC Savepoint frame missing savepoint name".to_string())?;
                match entry.inner.savepoint_tx(sp) {
                    Ok(()) => Ok(encode_dbrpc_response_frame(
                        &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                    )),
                    Err(err) => Ok(encode_duckdb_err(req_id, &err)),
                }
            }
            DbRpcOp::Cancel => {
                entry.inner.interrupt_all();
                Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                ))
            }
            DbRpcOp::Close => {
                self.close_pool(pool_id);
                Ok(encode_dbrpc_response_frame(
                    &DbRpcEncodedResponse::ok_count(req_id, 0, None),
                ))
            }
            other => Err(format!(
                "unsupported DBRPC op {other:?} in DuckDB broker pool"
            )),
        }
    }

    fn close_pool(&self, pool_id: u64) {
        if let Some(entry) = self.pools.remove(pool_id) {
            entry.inner.close_all();
            let pending: Vec<String> = entry
                .active_shm
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .drain(..)
                .collect();
            for shm_name in pending {
                let _ = release_shm_arrow_frame(&shm_name);
            }
        }
    }
}

/// Export the `vertebra.BrokerEngineCapsule` PyCapsule for DuckDB (`DIALECT_TAG_DUCKDB = 6`).
#[pyfunction]
pub fn broker_engine_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyCapsule>> {
    new_broker_engine_pycapsule(py, Arc::new(DuckDBBrokerBackend::new()))
}

/// Native `vertebra._duckdb` Python extension module.
#[pymodule(gil_used = false)]
pub fn _duckdb(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("FREE_THREADED", FREE_THREADED)?;
    m.add("GIL_USED", false)?;
    m.add_function(wrap_pyfunction!(broker_engine_capsule, m)?)?;
    m.add_class::<SyncDuckDBClient>()?;
    m.add_class::<AsyncDuckDBClient>()?;
    m.add_class::<SyncDuckDBCursor>()?;
    m.add_class::<AsyncDuckDBCursor>()?;
    m.add(
        "__all__",
        vec![
            "FREE_THREADED",
            "GIL_USED",
            "AsyncDuckDBClient",
            "AsyncDuckDBCursor",
            "SyncDuckDBClient",
            "SyncDuckDBCursor",
            "__version__",
            "broker_engine_capsule",
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

    fn run(pool: &DuckDBInner, sql: &str, params: BoundParams) -> StatementOutcome {
        let flag = pool.register_cancel_flag();
        pool.run_statement(sql.to_string(), params, &flag)
            .unwrap_or_else(|err| panic!("{sql} should succeed: {err}"))
    }

    #[test]
    fn test_run_statement_outcomes() {
        let pool = make_pool(2);

        let query = run(&pool, "SELECT 1 AS a", BoundParams::Empty);
        assert_eq!(query.columns, Some(vec!["a".to_string()]));
        assert_eq!(query.rows.len(), 1);
        assert_eq!(query.rowcount, -1);

        let ddl = run(
            &pool,
            "CREATE TABLE outcome_tbl (id INTEGER PRIMARY KEY, label VARCHAR)",
            BoundParams::Empty,
        );
        assert_eq!(ddl.columns, None);
        assert!(ddl.rows.is_empty());
        assert_eq!(ddl.rowcount, 0);

        let empty = run(
            &pool,
            "SELECT id, label FROM outcome_tbl WHERE 1 = 0",
            BoundParams::Empty,
        );
        assert_eq!(
            empty.columns,
            Some(vec!["id".to_string(), "label".to_string()])
        );
        assert!(empty.rows.is_empty());
        assert_eq!(empty.rowcount, -1);

        let insert = run(
            &pool,
            "INSERT INTO outcome_tbl VALUES (1, 'a'), (2, 'b')",
            BoundParams::Empty,
        );
        assert_eq!(insert.columns, None);
        assert!(insert.rows.is_empty());
        assert_eq!(insert.rowcount, 2);

        let returning = run(
            &pool,
            "INSERT INTO outcome_tbl VALUES (3, 'c'), (4, 'd') RETURNING id",
            BoundParams::Empty,
        );
        assert_eq!(returning.columns, Some(vec!["id".to_string()]));
        assert_eq!(returning.rows.len(), 2);
        assert_eq!(returning.rowcount, 2);

        let mut named = HashMap::new();
        named.insert("v".to_string(), DuckValue::Int(7));
        let named_res = run(&pool, "SELECT $v AS v", BoundParams::Named(named));
        assert_eq!(named_res.columns, Some(vec!["v".to_string()]));
        assert_eq!(named_res.rows, vec![vec![DuckValue::Int(7)]]);

        let mut colon_named = HashMap::new();
        colon_named.insert("min_id".to_string(), DuckValue::Int(3));
        let colon_res = run(
            &pool,
            "SELECT id FROM outcome_tbl WHERE id >= :min_id ORDER BY id",
            BoundParams::Named(colon_named),
        );
        assert_eq!(colon_res.rows.len(), 2);
    }

    #[test]
    fn test_run_statement_transaction_control_and_many() {
        let pool = make_pool(2);
        run(
            &pool,
            "CREATE TABLE tx_tbl (id INTEGER PRIMARY KEY)",
            BoundParams::Empty,
        );

        let begin = run(&pool, "BEGIN", BoundParams::Empty);
        assert_eq!(begin.columns, None);
        assert_eq!(begin.rowcount, 0);
        assert!(pool.in_transaction.load(Ordering::Acquire));
        let inserted = run(&pool, "INSERT INTO tx_tbl VALUES (1)", BoundParams::Empty);
        assert_eq!(inserted.rowcount, 1);
        let rollback = run(&pool, "ROLLBACK;", BoundParams::Empty);
        assert_eq!(rollback.columns, None);
        assert_eq!(rollback.rowcount, 0);
        assert!(!pool.in_transaction.load(Ordering::Acquire));
        let count = run(&pool, "SELECT COUNT(*) FROM tx_tbl", BoundParams::Empty);
        assert_eq!(count.rows[0][0], DuckValue::BigInt(0));

        let flag = pool.register_cancel_flag();
        let sets = (1..=3)
            .map(|id| BoundParams::Positional(vec![DuckValue::Int(id)]))
            .collect();
        let many = pool
            .run_statement_many("INSERT INTO tx_tbl VALUES (?)".to_string(), sets, &flag)
            .expect("run_statement_many should succeed");
        assert_eq!(many.columns, None);
        assert!(many.rows.is_empty());
        assert_eq!(many.rowcount, 3);

        let none = pool
            .run_statement_many(
                "INSERT INTO tx_tbl VALUES (?)".to_string(),
                Vec::new(),
                &flag,
            )
            .expect("empty run_statement_many should succeed");
        assert_eq!(none.rowcount, 0);
        let count = run(&pool, "SELECT COUNT(*) FROM tx_tbl", BoundParams::Empty);
        assert_eq!(count.rows[0][0], DuckValue::BigInt(3));
    }

    #[test]
    fn test_run_statement_rejects_mutation_on_read_only_database() {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let path = dir.path().join("ro.duckdb");
        let path_str = path.to_str().expect("tempdir path should be UTF-8");
        let open = |read_only: bool| {
            DuckDBInner::open(
                path_str, read_only, 1, None, None, None, true, true, true, None, None, None,
            )
            .expect("file-backed pool should open")
        };
        let writer = open(false);
        run(
            &writer,
            "CREATE TABLE ro_tbl (id INTEGER)",
            BoundParams::Empty,
        );
        writer.close_all();
        drop(writer);

        let reader = open(true);
        let flag = reader.register_cancel_flag();
        for sql in [
            "INSERT INTO ro_tbl VALUES (1)",
            "INSERT INTO ro_tbl VALUES (1) RETURNING id",
        ] {
            let err = reader
                .run_statement(sql.to_string(), BoundParams::Empty, &flag)
                .unwrap_err();
            assert!(
                matches!(err, DuckDBClientError::Programming(_)),
                "{sql}: {err}"
            );
        }
        let many_err = reader
            .run_statement_many(
                "INSERT INTO ro_tbl VALUES (?)".to_string(),
                vec![BoundParams::Positional(vec![DuckValue::Int(1)])],
                &flag,
            )
            .unwrap_err();
        assert!(matches!(many_err, DuckDBClientError::Programming(_)));
        let rows = run(&reader, "SELECT COUNT(*) FROM ro_tbl", BoundParams::Empty);
        assert_eq!(rows.rows[0][0], DuckValue::BigInt(0));
    }

    #[test]
    fn test_statement_facts_cache() {
        let pool = make_pool(1);
        assert_eq!(pool.statement_facts_len(), 0);
        run(&pool, "SELECT 1 AS a", BoundParams::Empty);
        run(&pool, "SELECT 1 AS a", BoundParams::Empty);
        assert_eq!(pool.statement_facts_len(), 1);

        let select = pool.statement_facts("SELECT :v AS v");
        assert!(select.read_only && select.returns_rows);
        assert!(!select.mutating && !select.maybe_tx_control);
        assert_eq!(&*select.named_sql, "SELECT $v AS v");
        let insert = pool.statement_facts("INSERT INTO t VALUES (1)");
        assert!(insert.mutating && !insert.read_only && !insert.returns_rows);
        for sql in [
            "BEGIN",
            "START TRANSACTION",
            "COMMIT",
            "END TRANSACTION",
            "ROLLBACK TO sp",
            "ABORT",
            "SAVEPOINT sp",
            "RELEASE sp",
        ] {
            assert!(pool.statement_facts(sql).maybe_tx_control, "{sql}");
        }

        let fresh = make_pool(1);
        for idx in 0..=STATEMENT_FACTS_CAPACITY {
            fresh.statement_facts(&format!("SELECT {idx}"));
        }
        assert_eq!(fresh.statement_facts_len(), 1);
    }

    #[test]
    fn test_extract_bound_params_exact_containers() {
        Python::attach(|py| {
            let expected_positional = vec![DuckValue::BigInt(1), DuckValue::Text("a".to_string())];
            for expr in [c"(1, 'a')", c"[1, 'a']"] {
                let obj = py.eval(expr, None, None).expect("eval should succeed");
                let bound = extract_bound_params(Some(&obj))
                    .expect("exact tuple/list extraction should succeed");
                match bound {
                    BoundParams::Positional(vals) => assert_eq!(vals, expected_positional),
                    other => panic!("expected Positional for {expr:?}, got {other:?}"),
                }
            }

            let dict_obj = py
                .eval(c"{'k': 1}", None, None)
                .expect("eval should succeed");
            let bound = extract_bound_params(Some(&dict_obj))
                .expect("exact dict extraction should succeed");
            match bound {
                BoundParams::Named(map) => {
                    assert_eq!(map.len(), 1);
                    assert_eq!(map.get("k"), Some(&DuckValue::BigInt(1)));
                }
                other => panic!("expected Named for exact dict, got {other:?}"),
            }
        });
    }

    #[test]
    fn test_extract_bound_params_empty_containers_are_empty() {
        Python::attach(|py| {
            let none_bound = extract_bound_params(None).expect("None option should succeed");
            assert!(
                matches!(none_bound, BoundParams::Empty),
                "expected Empty for None option, got {none_bound:?}"
            );

            for expr in [c"()", c"[]", c"{}", c"None"] {
                let obj = py.eval(expr, None, None).expect("eval should succeed");
                let bound = extract_bound_params(Some(&obj))
                    .expect("empty container extraction should succeed");
                assert!(
                    matches!(bound, BoundParams::Empty),
                    "expected Empty for {expr:?}, got {bound:?}"
                );
            }
        });
    }

    #[test]
    fn test_extract_bound_params_subclasses_and_mappings() {
        Python::attach(|py| {
            let locals = PyDict::new(py);
            py.run(
                c"
import collections, types

Pair = collections.namedtuple('Pair', ['x', 'y'])
named_tup = Pair(1, 'a')
EmptyTup = collections.namedtuple('EmptyTup', [])
empty_named_tup = EmptyTup()

class MyList(list):
    pass

sub_list = MyList([1, 'a'])
empty_sub_list = MyList()

class MyDict(dict):
    pass

sub_dict = MyDict(k=1)
empty_sub_dict = MyDict()

proxy = types.MappingProxyType({'k': 1})
empty_proxy = types.MappingProxyType({})
",
                None,
                Some(&locals),
            )
            .expect("python setup should succeed");

            let expected_pos = vec![DuckValue::BigInt(1), DuckValue::Text("a".to_string())];
            for name in ["named_tup", "sub_list"] {
                let obj = locals
                    .get_item(name)
                    .expect("lookup should succeed")
                    .expect("key should exist");
                let bound = extract_bound_params(Some(&obj))
                    .unwrap_or_else(|err| panic!("{name} extraction should succeed: {err}"));
                match bound {
                    BoundParams::Positional(vals) => assert_eq!(vals, expected_pos, "{name}"),
                    other => panic!("expected Positional for {name}, got {other:?}"),
                }
            }

            for name in ["sub_dict", "proxy"] {
                let obj = locals
                    .get_item(name)
                    .expect("lookup should succeed")
                    .expect("key should exist");
                let bound = extract_bound_params(Some(&obj))
                    .unwrap_or_else(|err| panic!("{name} extraction should succeed: {err}"));
                match bound {
                    BoundParams::Named(map) => {
                        assert_eq!(map.len(), 1, "{name}");
                        assert_eq!(map.get("k"), Some(&DuckValue::BigInt(1)), "{name}");
                    }
                    other => panic!("expected Named for {name}, got {other:?}"),
                }
            }

            for name in [
                "empty_named_tup",
                "empty_sub_list",
                "empty_sub_dict",
                "empty_proxy",
            ] {
                let obj = locals
                    .get_item(name)
                    .expect("lookup should succeed")
                    .expect("key should exist");
                let bound = extract_bound_params(Some(&obj))
                    .unwrap_or_else(|err| panic!("{name} extraction should succeed: {err}"));
                assert!(
                    matches!(bound, BoundParams::Empty),
                    "expected Empty for {name}, got {bound:?}"
                );
            }
        });
    }

    #[test]
    fn test_extract_bound_params_generator_and_bad_key() {
        Python::attach(|py| {
            let generator = py
                .eval(c"(x for x in [1])", None, None)
                .expect("eval should succeed");
            let bound = extract_bound_params(Some(&generator)).expect("generator should succeed");
            match bound {
                BoundParams::Positional(vals) => assert_eq!(vals, vec![DuckValue::BigInt(1)]),
                other => panic!("expected Positional for generator, got {other:?}"),
            }

            let bad_dict = py
                .eval(c"{1: 'a'}", None, None)
                .expect("eval should succeed");
            let bad_dict_err = extract_bound_params(Some(&bad_dict)).unwrap_err();
            assert!(
                matches!(bad_dict_err, DuckDBClientError::InvalidInput(_)),
                "expected InvalidInput for non-str dict key, got {bad_dict_err:?}"
            );

            let bad_proxy = py
                .eval(
                    c"__import__('types').MappingProxyType({1: 'a'})",
                    None,
                    None,
                )
                .expect("eval should succeed");
            let bad_proxy_err = extract_bound_params(Some(&bad_proxy)).unwrap_err();
            assert!(
                matches!(bad_proxy_err, DuckDBClientError::InvalidInput(_)),
                "expected InvalidInput for non-str mapping key, got {bad_proxy_err:?}"
            );

            let bad_int = py.eval(c"5", None, None).expect("eval should succeed");
            let bad_int_err = extract_bound_params(Some(&bad_int)).unwrap_err();
            match bad_int_err {
                DuckDBClientError::InvalidInput(msg) => {
                    assert!(
                        msg.contains("sequence (list/tuple) or mapping (dict)"),
                        "unexpected message: {msg}"
                    );
                }
                other => panic!("expected InvalidInput for integer 5, got {other:?}"),
            }
        });
    }
}
