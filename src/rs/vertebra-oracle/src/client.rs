//! Per-connection Rust worker thread, completion bridge, and out-of-band `break_execution()` cancellation.

use std::collections::{BTreeMap, HashMap};
use std::net::{Shutdown, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, LazyLock, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict, PyList, PyString, PyTuple};
use tokio::sync::oneshot;
use vertebra_core::notify::SyncCompletionQueue;
use vertebra_db::arrow_shm::write_shm_arrow_region_unregistered;
use vertebra_db::lease_guard::LeaseGuard;
use vertebra_db::oracle::OracleSessionAttrs;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, construct_py_arrow_cursor, spawn_rust_db_task_with_attr,
};
use vertebra_db::py_broker::next_shm_name;
use vertebra_db::py_cursor::{DEFAULT_PLAN_CACHE_CAPACITY, PlanLruCache};

use crate::config::{NativeOracleConfig, RawOracleOptions};
use crate::cursor::{
    AsyncOracleCursor, BuiltReply, SyncOracleCursor, prepare_execute, reply_dicts, reply_dml,
    reply_first_value, reply_rows_and_names, run_single_call_sync, spawn_single_call,
};
use crate::dialer::{connect_oracledb, connect_oracledb_session};
use crate::errors::OracleError;
use crate::notify::{
    AsyncOracleListener, SyncOracleListener, enqueue_notification_sync, validate_aq_identifier,
};
use crate::types::{
    OracleCellValue, OracleColumn, OracleQueryResult, OracleValueOptions, VectorReturnFormat,
    cell_to_oracledb_bind, decode_oracledb_row, extract_bind_value, normalize_named_bind_map,
    out_bind_type_placeholder,
};

/// Attribute name used to attach `vertebra._oracle`'s `AsyncCompletionQueue` drainer to an `asyncio` event loop.
pub const ORACLE_COMPLETION_DRAINER_ATTR: &str = "_vertebra_oracle_completion_drainer";

/// Capability constant indicating whether `oracledb` exposes non-blocking TTC wire pipelining (`false` on `=26.0.0-beta.5`).
pub const ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool = false;

const ARROW_SHM_THRESHOLD_ROWS: usize = 256;
static NEXT_WORKER_ID: AtomicU64 = AtomicU64::new(1);

/// Validate an Oracle savepoint identifier against ASCII `[A-Za-z_][A-Za-z0-9_$#]*`.
pub fn validate_savepoint_identifier(name: &str) -> Result<(), OracleError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 128 {
        return Err(OracleError::Server {
            code: 900,
            message: format!("ORA-00900: invalid savepoint identifier `{name}`"),
        });
    }
    if !matches!(bytes[0], b'A'..=b'Z' | b'a'..=b'z' | b'_') {
        return Err(OracleError::Server {
            code: 900,
            message: format!("ORA-00900: invalid savepoint identifier `{name}`"),
        });
    }
    if !bytes[1..]
        .iter()
        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'$' | b'#'))
    {
        return Err(OracleError::Server {
            code: 900,
            message: format!("ORA-00900: invalid savepoint identifier `{name}`"),
        });
    }
    Ok(())
}

pub(crate) type BreakCallback = Arc<dyn Fn() + Send + Sync>;

/// Build an out-of-band break callback that cancels active SQL/PLSQL on `(sid, serial)` via a helper session.
pub(crate) fn spawn_cancel_sql_callback(
    cfg: Arc<NativeOracleConfig>,
    sid: usize,
    serial: usize,
    fallback_socket: Option<Arc<TcpStream>>,
) -> BreakCallback {
    Arc::new(move || {
        let cfg = Arc::clone(&cfg);
        let fallback = fallback_socket.clone();
        let _ = thread::Builder::new()
            .name("vtb-ora-break".to_owned())
            .spawn(move || {
                let mut cancel_cfg = (*cfg).clone();
                cancel_cfg.cclass = None;
                cancel_cfg.connection_tag = None;
                cancel_cfg.call_timeout_ms = Some(3_000);
                let mut cancelled_server_side = false;
                if let Ok(mut helper) = connect_oracledb(&cancel_cfg) {
                    let cancel_sql = format!("ALTER SYSTEM CANCEL SQL '{sid}, {serial}'");
                    if helper.execute(&cancel_sql, &[]).is_ok() {
                        cancelled_server_side = true;
                    }
                    let _ = helper.close();
                }
                if !cancelled_server_side && let Some(ref sock) = fallback {
                    let _ = sock.shutdown(Shutdown::Both);
                }
            });
    })
}

/// Out-of-band cancellation handle shared between the caller and the worker thread.
pub struct OracleBreakHandle {
    in_flight: AtomicBool,
    cancelled: AtomicBool,
    wait_lock: Mutex<()>,
    wait_cvar: Condvar,
    callback: Mutex<Option<BreakCallback>>,
}

impl Default for OracleBreakHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl OracleBreakHandle {
    /// Construct a new idle [`OracleBreakHandle`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            in_flight: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
            wait_lock: Mutex::new(()),
            wait_cvar: Condvar::new(),
            callback: Mutex::new(None),
        }
    }

    /// Register an optional out-of-band break callback invoked when [`Self::break_execution`] fires.
    pub fn set_callback(&self, cb: Option<BreakCallback>) {
        let mut guard = self.callback.lock().unwrap_or_else(|e| e.into_inner());
        *guard = cb;
    }

    /// Arm the break handle before dispatching an operation to the worker thread.
    pub fn begin_operation(&self) {
        self.cancelled.store(false, Ordering::Release);
        self.in_flight.store(true, Ordering::Release);
    }

    /// Disarm the break handle when an operation completes on the worker thread.
    pub fn end_operation(&self) {
        self.in_flight.store(false, Ordering::Release);
        self.cancelled.store(false, Ordering::Release);
    }

    /// Trigger an out-of-band `break_execution()` if an operation is currently in flight.
    pub fn break_execution(&self) {
        if !self.in_flight.load(Ordering::Acquire) {
            return;
        }
        self.cancelled.store(true, Ordering::SeqCst);
        {
            let _guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
            self.wait_cvar.notify_all();
        }
        let cb = self
            .callback
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if let Some(f) = cb {
            f();
        }
    }

    /// Return `true` if the active operation has been cancelled.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Consume any pending cancellation flag and return `Err(ORA-01013)` if set.
    pub fn check_cancelled(&self) -> Result<(), OracleError> {
        if self.cancelled.swap(false, Ordering::AcqRel) {
            return Err(OracleError::Server {
                code: 1013,
                message: "ORA-01013: user requested cancel of current operation".to_owned(),
            });
        }
        Ok(())
    }

    /// Wait up to `duration` or abort immediately with `ORA-01013` if [`Self::break_execution`] is called.
    pub fn wait_or_cancelled(&self, duration: Duration) -> Result<(), OracleError> {
        let start = Instant::now();
        let mut guard = self.wait_lock.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if self.cancelled.swap(false, Ordering::AcqRel) {
                return Err(OracleError::Server {
                    code: 1013,
                    message: "ORA-01013: user requested cancel of current operation".to_owned(),
                });
            }
            let elapsed = start.elapsed();
            if elapsed >= duration {
                return Ok(());
            }
            let remaining = duration.saturating_sub(elapsed);
            let (next_guard, _) = self
                .wait_cvar
                .wait_timeout(guard, remaining)
                .unwrap_or_else(|e| e.into_inner());
            guard = next_guard;
        }
    }
}

/// Owned bind parameter collection lowered on the caller thread before worker dispatch.
#[derive(Clone, Debug)]
pub enum BindArgs {
    Positional(Vec<OracleCellValue>),
    Named(Vec<(String, OracleCellValue)>),
}

impl Default for BindArgs {
    fn default() -> Self {
        Self::Positional(Vec::new())
    }
}

/// Owned output bind declaration for `DML RETURNING INTO` or PL/SQL `OUT` binds.
#[derive(Clone, Debug, Default)]
pub enum OutBindSpec {
    #[default]
    None,
    Positional(Vec<String>),
    Named(Vec<(String, String)>),
}

/// Owned output bind values returned from a worker execution.
#[derive(Clone, Debug, Default)]
pub enum OutBindsPayload {
    #[default]
    None,
    Positional(Vec<OracleCellValue>),
    Named(Vec<(String, OracleCellValue)>),
}

impl OutBindsPayload {
    fn into_py_object(
        self,
        py: Python<'_>,
        fetch_decimals: bool,
    ) -> Result<Option<Py<PyAny>>, OracleError> {
        match self {
            Self::None => Ok(None),
            Self::Positional(vals) => {
                let mut py_vals = Vec::with_capacity(vals.len());
                for v in vals {
                    py_vals.push(v.into_py_object(py, fetch_decimals)?);
                }
                let tup = PyTuple::new(py, py_vals)
                    .map_err(|e| OracleError::data_error(e.to_string()))?;
                Ok(Some(tup.into_any().unbind()))
            }
            Self::Named(pairs) => {
                let dict = PyDict::new(py);
                for (k, v) in pairs {
                    let py_val = v.into_py_object(py, fetch_decimals)?;
                    dict.set_item(k, py_val)
                        .map_err(|e| OracleError::data_error(e.to_string()))?;
                }
                Ok(Some(dict.into_any().unbind()))
            }
        }
    }
}

/// Owned worker query/DML result transferable across threads without holding the GIL.
#[derive(Clone, Debug)]
pub struct WorkerQueryOutcome {
    pub columns: Vec<OracleColumn>,
    pub rows: Vec<Vec<OracleCellValue>>,
    pub rowcount: usize,
    pub out_binds: OutBindsPayload,
    pub lastrowid: Option<String>,
    pub fetch_decimals: bool,
}

impl WorkerQueryOutcome {
    fn into_py_query_result(self, py: Python<'_>) -> PyResult<OracleQueryResult> {
        let mut py_cols = Vec::with_capacity(self.columns.len());
        for col in self.columns {
            py_cols.push(Py::new(py, col)?);
        }
        let mut py_rows = Vec::with_capacity(self.rows.len());
        for row in self.rows {
            let mut cells = Vec::with_capacity(row.len());
            for cell in row {
                cells.push(
                    cell.into_py_object(py, self.fetch_decimals)
                        .map_err(|e| e.to_py_err(py))?,
                );
            }
            let tup = PyTuple::new(py, cells)?;
            py_rows.push(tup.unbind());
        }
        let out_binds = self
            .out_binds
            .into_py_object(py, self.fetch_decimals)
            .map_err(|e| e.to_py_err(py))?;
        Ok(OracleQueryResult {
            columns: py_cols,
            rows: py_rows,
            rowcount: self.rowcount,
            lastrowid: self.lastrowid,
            out_binds,
        })
    }
}

/// Per-operation outcome returned by [`SyncOracleClient::execute_pipeline_batch`] and [`AsyncOracleClient::execute_pipeline_batch`].
#[pyclass(
    name = "OraclePipelineOutcome",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct OraclePipelineOutcome {
    #[pyo3(get)]
    pub status: String,
    #[pyo3(get)]
    pub result: Option<Py<OracleQueryResult>>,
    #[pyo3(get)]
    pub error: Option<Py<PyAny>>,
}

#[pymethods]
impl OraclePipelineOutcome {
    #[new]
    #[pyo3(signature = (status, result=None, error=None))]
    pub fn new(
        status: String,
        result: Option<Py<OracleQueryResult>>,
        error: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            status,
            result,
            error,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct PipelineOpSpec {
    pub sql: String,
    pub params: BindArgs,
    pub returns_rows: Option<bool>,
    pub fetch_decimals: bool,
    pub lowercase_column_names: bool,
}

#[derive(Clone, Debug)]
pub(crate) enum WorkerPipelineStepOutcome {
    Completed(WorkerQueryOutcome),
    Error(OracleError),
    Skipped,
}

impl WorkerPipelineStepOutcome {
    fn into_py_outcome(self, py: Python<'_>) -> PyResult<OraclePipelineOutcome> {
        match self {
            Self::Completed(outcome) => {
                let qres = outcome.into_py_query_result(py)?;
                Ok(OraclePipelineOutcome {
                    status: "completed".to_owned(),
                    result: Some(Py::new(py, qres)?),
                    error: None,
                })
            }
            Self::Error(err) => {
                let py_err = err.to_py_err(py);
                let exc_obj = py_err.value(py).clone().into_any().unbind();
                Ok(OraclePipelineOutcome {
                    status: "error".to_owned(),
                    result: None,
                    error: Some(exc_obj),
                })
            }
            Self::Skipped => Ok(OraclePipelineOutcome {
                status: "skipped".to_owned(),
                result: None,
                error: None,
            }),
        }
    }
}

pub(crate) fn extract_pipeline_ops(
    operations: &Bound<'_, PyAny>,
    default_fetch_decimals: bool,
    default_lowercase: bool,
) -> Result<Vec<PipelineOpSpec>, OracleError> {
    if operations.is_instance_of::<PyString>()
        || operations.is_instance_of::<PyBytes>()
        || operations.is_instance_of::<PyByteArray>()
    {
        return Err(OracleError::Interface(
            "Oracle pipeline operations must be a Sequence of operation tuples".to_owned(),
        ));
    }
    let iter = operations.try_iter().map_err(|_| {
        OracleError::Interface(
            "Oracle pipeline operations must be a Sequence of operation tuples".to_owned(),
        )
    })?;
    let mut ops = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| OracleError::data_error(e.to_string()))?;
        let item_len = item.len().map_err(|_| {
            OracleError::Interface("Each pipeline operation must be a tuple".to_owned())
        })?;
        if item_len == 0 {
            return Err(OracleError::Interface(
                "Pipeline operation tuple cannot be empty".to_owned(),
            ));
        }
        let sql_obj = item
            .get_item(0)
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let sql = sql_obj
            .extract::<String>()
            .map_err(|e| OracleError::data_error(e.to_string()))?;
        let params = if item_len >= 2 {
            let p_obj = item
                .get_item(1)
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            extract_bind_args(Some(&p_obj))?
        } else {
            BindArgs::Positional(Vec::new())
        };
        let returns_rows = if item_len >= 3 {
            let r_obj = item
                .get_item(2)
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            if r_obj.is_none() {
                None
            } else {
                Some(
                    r_obj
                        .extract::<bool>()
                        .map_err(|e| OracleError::data_error(e.to_string()))?,
                )
            }
        } else {
            None
        };
        ops.push(PipelineOpSpec {
            sql,
            params,
            returns_rows,
            fetch_decimals: default_fetch_decimals,
            lowercase_column_names: default_lowercase,
        });
    }
    Ok(ops)
}

pub(crate) fn extract_bind_args(
    params: Option<&Bound<'_, PyAny>>,
) -> Result<BindArgs, OracleError> {
    let Some(obj) = params else {
        return Ok(BindArgs::Positional(Vec::new()));
    };
    if obj.is_none() {
        return Ok(BindArgs::Positional(Vec::new()));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut map = HashMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key_str = k
                .extract::<String>()
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            let cell = extract_bind_value(&v)?;
            map.insert(key_str, cell);
        }
        return Ok(BindArgs::Named(normalize_named_bind_map(map)));
    }
    if obj.hasattr("keys").unwrap_or(false)
        && obj.hasattr("items").unwrap_or(false)
        && let Ok(items) = obj.call_method0("items")
        && let Ok(iter) = items.try_iter()
    {
        let mut map = HashMap::new();
        for pair_res in iter {
            let pair = pair_res.map_err(|e| OracleError::data_error(e.to_string()))?;
            let key_obj = pair
                .get_item(0)
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            let val_obj = pair
                .get_item(1)
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            let key_str = key_obj
                .extract::<String>()
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            let cell = extract_bind_value(&val_obj)?;
            map.insert(key_str, cell);
        }
        return Ok(BindArgs::Named(normalize_named_bind_map(map)));
    }
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyByteArray>()
    {
        return Err(OracleError::Interface(
            "Oracle bind parameters must be a Sequence or Mapping, not str/bytes".to_owned(),
        ));
    }
    let iter = obj.try_iter().map_err(|_| {
        OracleError::Interface("Oracle bind parameters must be a Sequence or Mapping".to_owned())
    })?;
    let mut vals = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| OracleError::data_error(e.to_string()))?;
        vals.push(extract_bind_value(&item)?);
    }
    Ok(BindArgs::Positional(vals))
}

pub(crate) fn extract_out_bind_spec(
    out_binds: Option<&Bound<'_, PyAny>>,
) -> Result<OutBindSpec, OracleError> {
    let Some(obj) = out_binds else {
        return Ok(OutBindSpec::None);
    };
    if obj.is_none() {
        return Ok(OutBindSpec::None);
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut pairs = Vec::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            let key_str = k
                .extract::<String>()
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            let val_str = v
                .extract::<String>()
                .unwrap_or_else(|_| "VARCHAR2".to_owned());
            let clean = key_str.strip_prefix(':').unwrap_or(&key_str).to_owned();
            pairs.push((clean, val_str));
        }
        return Ok(OutBindSpec::Named(pairs));
    }
    if obj.hasattr("items").unwrap_or(false)
        && let Ok(items) = obj.call_method0("items")
        && let Ok(iter) = items.try_iter()
    {
        let mut pairs = Vec::new();
        for item_res in iter {
            let item = item_res.map_err(|e| OracleError::data_error(e.to_string()))?;
            let k = item
                .get_item(0)
                .and_then(|x| x.extract::<String>())
                .map_err(|e| OracleError::data_error(e.to_string()))?;
            let v = item
                .get_item(1)
                .and_then(|x| x.extract::<String>())
                .unwrap_or_else(|_| "VARCHAR2".to_owned());
            let clean = k.strip_prefix(':').unwrap_or(&k).to_owned();
            pairs.push((clean, v));
        }
        return Ok(OutBindSpec::Named(pairs));
    }
    let iter = obj.try_iter().map_err(|_| {
        OracleError::Interface("out_binds must be a Sequence or Mapping".to_owned())
    })?;
    let mut hints = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| OracleError::data_error(e.to_string()))?;
        let hint = item
            .extract::<String>()
            .unwrap_or_else(|_| "VARCHAR2".to_owned());
        hints.push(hint);
    }
    Ok(OutBindSpec::Positional(hints))
}

pub(crate) fn extract_bind_seq(
    params_seq: &Bound<'_, PyAny>,
) -> Result<Vec<BindArgs>, OracleError> {
    if params_seq.is_instance_of::<PyString>()
        || params_seq.is_instance_of::<PyBytes>()
        || params_seq.is_instance_of::<PyByteArray>()
    {
        return Err(OracleError::Interface(
            "execute_many parameter_sets must be a Sequence of parameter sets".to_owned(),
        ));
    }
    let iter = params_seq.try_iter().map_err(|_| {
        OracleError::Interface(
            "execute_many parameter_sets must be an iterable of parameter sets".to_owned(),
        )
    })?;
    let mut out = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| OracleError::data_error(e.to_string()))?;
        out.push(extract_bind_args(Some(&item))?);
    }
    Ok(out)
}

fn build_py_arrow_cursor(
    py: Python<'_>,
    ipc_bytes: Vec<u8>,
    row_count: usize,
    force_shm: Option<bool>,
) -> PyResult<Py<PyAny>> {
    let use_shm = force_shm
        .unwrap_or(row_count >= ARROW_SHM_THRESHOLD_ROWS || ipc_bytes.len() >= 65_536)
        && !ipc_bytes.is_empty();
    let arrow_mod = py.import("vertebra.db.arrow")?;
    let arrow_cls = arrow_mod.getattr("ArrowCursor")?;
    if use_shm {
        let shm_name = next_shm_name("/vtb-ora-arrow", '-');
        let (norm_name, byte_len, region) =
            write_shm_arrow_region_unregistered(&shm_name, &ipc_bytes)
                .map_err(|e| OracleError::operational(e).to_py_err(py))?;
        let native_cur = construct_py_arrow_cursor(py, &norm_name, byte_len, row_count, region)?;
        let kwargs = PyDict::new(py);
        kwargs.set_item("used_shm", true)?;
        kwargs.set_item("shm_cursor", native_cur.bind(py))?;
        let cur = arrow_cls.call((native_cur.bind(py),), Some(&kwargs))?;
        Ok(cur.unbind())
    } else {
        let py_bytes = PyBytes::new(py, &ipc_bytes);
        let kwargs = PyDict::new(py);
        kwargs.set_item("used_shm", false)?;
        let cur = arrow_cls.call((py_bytes,), Some(&kwargs))?;
        Ok(cur.unbind())
    }
}

pub(crate) enum WorkerCommand {
    Connect,
    Ping,
    Begin,
    Commit,
    Rollback,
    ResetIfDirty,
    Savepoint(String),
    RollbackToSavepoint(String),
    Query {
        sql: String,
        params: BindArgs,
        out_binds: OutBindSpec,
        fetch_decimals: bool,
        lowercase_column_names: bool,
        max_rows: Option<usize>,
    },
    Execute {
        sql: String,
        params: BindArgs,
        out_binds: OutBindSpec,
        fetch_decimals: bool,
        lowercase_column_names: bool,
    },
    ExecuteMany {
        sql: String,
        params_seq: Vec<BindArgs>,
    },
    QueryArrow {
        sql: String,
        params: BindArgs,
        batch_size: usize,
        lowercase_column_names: bool,
    },
    PipelineBatch {
        ops: Vec<PipelineOpSpec>,
        continue_on_error: bool,
    },
    Close,
}

pub(crate) enum WorkerReply {
    Unit,
    RowCount(usize),
    QueryOutcome(WorkerQueryOutcome),
    ArrowIpc {
        ipc_bytes: Vec<u8>,
        row_count: usize,
    },
    PipelineOutcomes(Vec<WorkerPipelineStepOutcome>),
}

enum ReplyChannel {
    Sync(Arc<SyncCompletionQueue<Result<WorkerReply, OracleError>>>),
    Async(oneshot::Sender<Result<WorkerReply, OracleError>>),
}

impl ReplyChannel {
    fn send(self, result: Result<WorkerReply, OracleError>) {
        match self {
            Self::Sync(queue) => queue.push(result),
            Self::Async(tx) => {
                let _ = tx.send(result);
            }
        }
    }
}

struct WorkerEnvelope {
    cmd: WorkerCommand,
    reply: ReplyChannel,
}

#[derive(Clone, Debug)]
struct ReturningClause {
    columns: Vec<String>,
    out_vars: Vec<String>,
}

fn parse_returning_clause(sql: &str) -> Option<ReturningClause> {
    let upper = sql.to_ascii_uppercase();
    let ret_pos = find_keyword_outside_quotes(&upper, "RETURNING")
        .or_else(|| find_keyword_outside_quotes(&upper, "RETURN"))?;
    let kw_len = if upper[ret_pos..].starts_with("RETURNING") {
        "RETURNING".len()
    } else {
        "RETURN".len()
    };
    let after_ret_offset = ret_pos + kw_len;
    let after_ret_upper = &upper[after_ret_offset..];
    let into_rel = find_keyword_outside_quotes(after_ret_upper, "INTO")?;
    let into_pos = after_ret_offset + into_rel;
    let cols_part = sql[after_ret_offset..into_pos].trim();
    let vars_part = sql[into_pos + "INTO".len()..]
        .trim()
        .trim_end_matches(';')
        .trim();
    if cols_part.is_empty() || vars_part.is_empty() {
        return None;
    }
    let columns: Vec<String> = split_top_level_commas(cols_part)
        .into_iter()
        .map(|c| extract_column_alias_or_name(c.trim()))
        .collect();
    let out_vars: Vec<String> = split_top_level_commas(vars_part)
        .into_iter()
        .map(|v| v.trim().strip_prefix(':').unwrap_or(v.trim()).to_owned())
        .collect();
    Some(ReturningClause { columns, out_vars })
}

fn find_keyword_outside_quotes(upper_sql: &str, keyword: &str) -> Option<usize> {
    let bytes = upper_sql.as_bytes();
    let kw = keyword.as_bytes();
    let mut in_single = false;
    let mut in_double = false;
    let mut i = 0;
    while i + kw.len() <= bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_double {
            in_single = !in_single;
            i += 1;
            continue;
        }
        if b == b'"' && !in_single {
            in_double = !in_double;
            i += 1;
            continue;
        }
        if !in_single && !in_double && &bytes[i..i + kw.len()] == kw {
            let prev_ok = i == 0 || !is_ident_char(bytes[i - 1]);
            let next_ok = i + kw.len() == bytes.len() || !is_ident_char(bytes[i + kw.len()]);
            if prev_ok && next_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn is_ident_char(b: u8) -> bool {
    matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'$' | b'#' | b':')
}

fn split_top_level_commas(input: &str) -> Vec<&str> {
    let mut out = Vec::new();
    let bytes = input.as_bytes();
    let mut depth = 0i32;
    let mut in_single = false;
    let mut start = 0usize;
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'\'' {
            in_single = !in_single;
        } else if !in_single {
            if b == b'(' {
                depth += 1;
            } else if b == b')' {
                depth = (depth - 1).max(0);
            } else if b == b',' && depth == 0 {
                out.push(&input[start..idx]);
                start = idx + 1;
            }
        }
    }
    out.push(&input[start..]);
    out
}

fn extract_column_alias_or_name(expr: &str) -> String {
    let trimmed = expr.trim();
    let upper = trimmed.to_ascii_uppercase();
    if let Some(as_pos) = find_keyword_outside_quotes(&upper, "AS") {
        let alias = trimmed[as_pos + 2..].trim().trim_matches('"');
        if !alias.is_empty() {
            return alias.to_ascii_uppercase();
        }
    }
    let clean = trimmed.trim_matches('"');
    let after_dot = clean.rsplit('.').next().unwrap_or(clean).trim();
    after_dot.to_ascii_uppercase()
}

fn strip_leading_sql_comments(sql: &str) -> &str {
    let mut s = sql.trim();
    loop {
        if s.starts_with("/*")
            && let Some(end) = s.find("*/")
        {
            s = s[end + 2..].trim();
            continue;
        }
        if s.starts_with("--") {
            if let Some(nl) = s.find('\n') {
                s = s[nl + 1..].trim();
                continue;
            }
            return "";
        }
        break;
    }
    s
}

/// High-level Oracle statement kind used by the worker and native cursor.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum StatementKind {
    Query,
    DmlOrPlsql,
}

/// Cached per-SQL-text classification facts computed without Python scanning.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) struct StatementFacts {
    pub kind: StatementKind,
    pub touches_session_context: bool,
}

fn starts_with_ascii_ci(s: &str, prefix: &str) -> bool {
    s.get(..prefix.len())
        .is_some_and(|head| head.eq_ignore_ascii_case(prefix))
}

fn compute_statement_facts(sql: &str) -> StatementFacts {
    let trimmed = strip_leading_sql_comments(sql);
    let kind = if starts_with_ascii_ci(trimmed, "SELECT") || starts_with_ascii_ci(trimmed, "WITH") {
        StatementKind::Query
    } else {
        StatementKind::DmlOrPlsql
    };
    let upper = sql.to_ascii_uppercase();
    let touches_session_context = upper.contains("DBMS_SESSION")
        || upper.contains("SET_IDENTIFIER")
        || upper.contains("CLEAR_IDENTIFIER");
    StatementFacts {
        kind,
        touches_session_context,
    }
}

static STATEMENT_FACTS_CACHE: LazyLock<Mutex<PlanLruCache<StatementFacts>>> =
    LazyLock::new(|| Mutex::new(PlanLruCache::new(DEFAULT_PLAN_CACHE_CAPACITY)));

/// Return cached [`StatementFacts`] for `sql`, computing and caching on a miss (LRU capacity 512).
pub(crate) fn statement_facts(sql: &str) -> StatementFacts {
    {
        let mut cache = STATEMENT_FACTS_CACHE
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(hit) = cache.get(sql) {
            return hit;
        }
    }
    let facts = compute_statement_facts(sql);
    let mut cache = STATEMENT_FACTS_CACHE
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    cache.insert(sql.into(), facts)
}

fn command_may_open_txn(cmd: &WorkerCommand) -> bool {
    match cmd {
        WorkerCommand::Connect
        | WorkerCommand::Ping
        | WorkerCommand::Commit
        | WorkerCommand::Rollback
        | WorkerCommand::ResetIfDirty
        | WorkerCommand::Close => false,
        WorkerCommand::Begin
        | WorkerCommand::Savepoint(_)
        | WorkerCommand::RollbackToSavepoint(_)
        | WorkerCommand::Query { .. }
        | WorkerCommand::Execute { .. }
        | WorkerCommand::ExecuteMany { .. }
        | WorkerCommand::QueryArrow { .. }
        | WorkerCommand::PipelineBatch { .. } => true,
    }
}

struct WorkerSession {
    config: Arc<NativeOracleConfig>,
    conn: Option<oracledb::Connection>,
    upstream_socket: Option<Arc<TcpStream>>,
    savepoints: Vec<String>,
    in_transaction: Arc<AtomicBool>,
    may_have_open_txn: bool,
    closed: Arc<AtomicBool>,
    break_handle: Arc<OracleBreakHandle>,
    pending_session_attrs: Arc<Mutex<Option<OracleSessionAttrs>>>,
    active_session_attrs: Arc<Mutex<OracleSessionAttrs>>,
    last_piggyback_frame: Arc<Mutex<Option<Vec<u8>>>>,
    #[cfg(test)]
    reset_rollbacks: usize,
}

impl Drop for WorkerSession {
    fn drop(&mut self) {
        self.break_handle.set_callback(None);
    }
}

fn is_unsupported_arrow_fetch_err(err: &oracledb::Error, mapped: &OracleError) -> bool {
    if matches!(mapped, OracleError::Server { .. }) {
        return false;
    }
    let msg = err.to_string();
    let lower = msg.to_ascii_lowercase();
    msg.contains("DB_TYPE_")
        || lower.contains("unsupported")
        || (lower.contains("not supported") && lower.contains("type"))
        || lower.contains("fetching columns of type")
}

impl WorkerSession {
    fn discard_dead_connection_if_broken(&mut self, mapped: &OracleError) {
        if matches!(
            mapped,
            OracleError::Transport { .. }
                | OracleError::Server {
                    code: 3113 | 3114 | 3135 | 12537,
                    ..
                }
        ) {
            self.break_handle.set_callback(None);
            self.conn = None;
            self.upstream_socket = None;
            self.savepoints.clear();
            self.in_transaction.store(false, Ordering::Release);
            self.may_have_open_txn = false;
        }
    }

    fn note_autocommit(&mut self, statement_ok: bool, commit_ok: bool) {
        if statement_ok && commit_ok {
            self.may_have_open_txn = false;
        }
    }

    fn run_rollback(&mut self) -> Result<WorkerReply, OracleError> {
        let res = self.conn.as_ref().map(|conn| conn.rollback());
        if let Some(Err(e)) = res {
            return Err(self.map_call_err(&e));
        }
        self.savepoints.clear();
        self.in_transaction.store(false, Ordering::Release);
        self.may_have_open_txn = false;
        Ok(WorkerReply::Unit)
    }

    fn map_call_err(&mut self, err: &oracledb::Error) -> OracleError {
        let mapped = OracleError::from_oracledb(err);
        self.discard_dead_connection_if_broken(&mapped);
        if let Err(cancel_err) = self.break_handle.check_cancelled() {
            return cancel_err;
        }
        mapped
    }

    fn ensure_connected(&mut self) -> Result<&oracledb::Connection, OracleError> {
        if self.conn.is_none() {
            let dialed = connect_oracledb_session(&self.config)?;
            let fallback = dialed.upstream_socket.clone();
            if let (Ok(sid), Ok(serial)) = (dialed.conn.session_id(), dialed.conn.serial_num()) {
                let cb = spawn_cancel_sql_callback(Arc::clone(&self.config), sid, serial, fallback);
                self.break_handle.set_callback(Some(cb));
            } else if let Some(sock) = fallback {
                self.break_handle.set_callback(Some(Arc::new(move || {
                    let _ = sock.shutdown(Shutdown::Both);
                })));
            }
            self.upstream_socket = dialed.upstream_socket;
            self.conn = Some(dialed.conn);
        }
        self.conn.as_ref().ok_or_else(|| {
            OracleError::Interface("Oracle worker connection unavailable".to_owned())
        })
    }

    fn ensure_connected_and_flush_attrs(&mut self) -> Result<&oracledb::Connection, OracleError> {
        let _ = self.ensure_connected()?;
        let staged = {
            let mut guard = self
                .pending_session_attrs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(attrs) = staged
            && (attrs.client_identifier.is_some()
                || attrs.module.is_some()
                || attrs.action.is_some()
                || attrs.ecid.is_some())
        {
            if let Some(ref conn) = self.conn {
                if let Some(ref cid) = attrs.client_identifier {
                    conn.set_client_identifier(cid)
                        .map_err(|e| OracleError::from_oracledb(&e))?;
                }
                if let Some(ref m) = attrs.module {
                    conn.set_module(m)
                        .map_err(|e| OracleError::from_oracledb(&e))?;
                }
                if let Some(ref a) = attrs.action {
                    conn.set_action(a)
                        .map_err(|e| OracleError::from_oracledb(&e))?;
                }
            }
            let frame = attrs.encode_ttc_piggyback_session_attrs();
            {
                let mut last_guard = self
                    .last_piggyback_frame
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                *last_guard = Some(frame.to_vec());
            }
            {
                let mut active_guard = self
                    .active_session_attrs
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                if let Some(cid) = attrs.client_identifier {
                    active_guard.client_identifier = (!cid.is_empty()).then_some(cid);
                }
                if let Some(m) = attrs.module {
                    active_guard.module = (!m.is_empty()).then_some(m);
                }
                if let Some(a) = attrs.action {
                    active_guard.action = (!a.is_empty()).then_some(a);
                }
                if let Some(e) = attrs.ecid {
                    active_guard.ecid = (!e.is_empty()).then_some(e);
                }
            }
        }
        self.conn.as_ref().ok_or_else(|| {
            OracleError::Interface("Oracle worker connection unavailable".to_owned())
        })
    }

    fn handle_command(&mut self, cmd: WorkerCommand) -> Result<WorkerReply, OracleError> {
        self.break_handle.check_cancelled()?;
        if command_may_open_txn(&cmd) {
            self.may_have_open_txn = true;
        }
        match cmd {
            WorkerCommand::Connect => {
                let _ = self.ensure_connected()?;
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Ping => {
                let res = {
                    let conn = self.ensure_connected_and_flush_attrs()?;
                    conn.ping()
                };
                if let Err(e) = res {
                    return Err(self.map_call_err(&e));
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Begin => {
                self.in_transaction.store(true, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Commit => {
                let res = self.conn.as_ref().map(|conn| conn.commit());
                if let Some(Err(e)) = res {
                    return Err(self.map_call_err(&e));
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
                self.may_have_open_txn = false;
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Rollback => self.run_rollback(),
            WorkerCommand::ResetIfDirty => {
                if self.in_transaction.load(Ordering::Acquire) || self.may_have_open_txn {
                    #[cfg(test)]
                    {
                        self.reset_rollbacks += 1;
                    }
                    self.run_rollback()
                } else {
                    Ok(WorkerReply::Unit)
                }
            }
            WorkerCommand::Savepoint(name) => {
                validate_savepoint_identifier(&name)?;
                self.in_transaction.store(true, Ordering::Release);
                let stmt = format!("SAVEPOINT {name}");
                let res = {
                    let conn = self.ensure_connected_and_flush_attrs()?;
                    conn.execute(&stmt, &[])
                };
                if let Err(e) = res {
                    return Err(self.map_call_err(&e));
                }
                if !self
                    .savepoints
                    .iter()
                    .any(|s| s.eq_ignore_ascii_case(&name))
                {
                    self.savepoints.push(name);
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::RollbackToSavepoint(name) => {
                validate_savepoint_identifier(&name)?;
                let stmt = format!("ROLLBACK TO SAVEPOINT {name}");
                match self.conn.as_ref().map(|conn| conn.execute(&stmt, &[])) {
                    Some(res) => {
                        if let Err(e) = res {
                            return Err(self.map_call_err(&e));
                        }
                        if let Some(pos) = self
                            .savepoints
                            .iter()
                            .position(|s| s.eq_ignore_ascii_case(&name))
                        {
                            self.savepoints.truncate(pos + 1);
                        }
                        self.in_transaction.store(true, Ordering::Release);
                        Ok(WorkerReply::Unit)
                    }
                    None => Err(OracleError::Server {
                        code: 1086,
                        message: format!(
                            "ORA-01086: savepoint '{name}' never established in this session or is invalid"
                        ),
                    }),
                }
            }
            WorkerCommand::Query {
                sql,
                params,
                out_binds,
                fetch_decimals,
                lowercase_column_names,
                max_rows,
            } => {
                let outcome = self.run_query_or_execute(
                    &sql,
                    &params,
                    &out_binds,
                    fetch_decimals,
                    lowercase_column_names,
                    max_rows,
                    true,
                )?;
                Ok(WorkerReply::QueryOutcome(outcome))
            }
            WorkerCommand::Execute {
                sql,
                params,
                out_binds,
                fetch_decimals,
                lowercase_column_names,
            } => {
                let outcome = self.run_query_or_execute(
                    &sql,
                    &params,
                    &out_binds,
                    fetch_decimals,
                    lowercase_column_names,
                    None,
                    false,
                )?;
                Ok(WorkerReply::QueryOutcome(outcome))
            }
            WorkerCommand::ExecuteMany { sql, params_seq } => {
                if params_seq.is_empty() {
                    return Ok(WorkerReply::RowCount(0));
                }
                let mut total = 0usize;
                for item_params in &params_seq {
                    let outcome = self.run_query_or_execute(
                        &sql,
                        item_params,
                        &OutBindSpec::None,
                        false,
                        self.config.enable_lowercase_column_names,
                        None,
                        false,
                    )?;
                    total = total.saturating_add(outcome.rowcount);
                }
                self.may_have_open_txn = true;
                Ok(WorkerReply::RowCount(total))
            }
            WorkerCommand::QueryArrow {
                sql,
                params,
                batch_size,
                lowercase_column_names,
            } => {
                let arrow_attempt = {
                    let break_clone = Arc::clone(&self.break_handle);
                    let res = {
                        let conn = self.ensure_connected_and_flush_attrs()?;
                        match &params {
                            BindArgs::Positional(vals) => {
                                let mut boxed = Vec::with_capacity(vals.len());
                                for v in vals {
                                    boxed.push(cell_to_oracledb_bind(v)?);
                                }
                                let refs: Vec<&dyn oracledb::ToDbValue> = boxed
                                    .iter()
                                    .map(|b| &**b as &dyn oracledb::ToDbValue)
                                    .collect();
                                let rows_slice = [refs.as_slice()];
                                Some(conn.query_arrow(&sql, rows_slice.as_slice()))
                            }
                            BindArgs::Named(pairs) if pairs.is_empty() => {
                                let empty_row: [&dyn oracledb::ToDbValue; 0] = [];
                                let rows_slice = [empty_row.as_slice()];
                                Some(conn.query_arrow(&sql, rows_slice.as_slice()))
                            }
                            BindArgs::Named(_) => None,
                        }
                    };
                    match res {
                        Some(Ok(batch)) => {
                            break_clone.check_cancelled()?;
                            let (ipc_bytes, row_count) =
                                crate::arrow::oracledb_batch_to_arrow_ipc_chunked(
                                    &batch,
                                    lowercase_column_names,
                                    batch_size,
                                )?;
                            Ok(Some((ipc_bytes, row_count)))
                        }
                        Some(Err(err)) => {
                            let mapped = OracleError::from_oracledb(&err);
                            if is_unsupported_arrow_fetch_err(&err, &mapped) {
                                break_clone.check_cancelled()?;
                                Ok(None)
                            } else {
                                self.discard_dead_connection_if_broken(&mapped);
                                break_clone.check_cancelled()?;
                                Err(mapped)
                            }
                        }
                        None => Ok(None),
                    }
                }?;

                if let Some((ipc_bytes, row_count)) = arrow_attempt {
                    return Ok(WorkerReply::ArrowIpc {
                        ipc_bytes,
                        row_count,
                    });
                }

                let outcome = self.run_query_or_execute(
                    &sql,
                    &params,
                    &OutBindSpec::None,
                    false,
                    lowercase_column_names,
                    None,
                    true,
                )?;
                let (ipc_bytes, row_count) = crate::arrow::rows_to_arrow_ipc_chunked(
                    &outcome.columns,
                    &outcome.rows,
                    lowercase_column_names,
                    batch_size,
                )?;
                self.may_have_open_txn = true;
                Ok(WorkerReply::ArrowIpc {
                    ipc_bytes,
                    row_count,
                })
            }
            WorkerCommand::PipelineBatch {
                ops,
                continue_on_error,
            } => {
                let mut outcomes = Vec::with_capacity(ops.len());
                let mut aborted = false;
                for op in ops {
                    if aborted && !continue_on_error {
                        outcomes.push(WorkerPipelineStepOutcome::Skipped);
                        continue;
                    }
                    let clean = strip_leading_sql_comments(&op.sql).trim();
                    let upper = clean.to_ascii_uppercase();
                    let is_query = op.returns_rows.unwrap_or_else(|| {
                        upper.starts_with("SELECT") || upper.starts_with("WITH")
                    });
                    match self.run_query_or_execute(
                        &op.sql,
                        &op.params,
                        &OutBindSpec::None,
                        op.fetch_decimals,
                        op.lowercase_column_names,
                        None,
                        is_query,
                    ) {
                        Ok(outcome) => outcomes.push(WorkerPipelineStepOutcome::Completed(outcome)),
                        Err(err) => {
                            outcomes.push(WorkerPipelineStepOutcome::Error(err));
                            aborted = true;
                        }
                    }
                }
                self.may_have_open_txn = true;
                Ok(WorkerReply::PipelineOutcomes(outcomes))
            }
            WorkerCommand::Close => {
                self.break_handle.set_callback(None);
                self.upstream_socket = None;
                if let Some(mut conn) = self.conn.take() {
                    let _ = conn.close();
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
                self.may_have_open_txn = false;
                self.closed.store(true, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn run_query_or_execute(
        &mut self,
        sql: &str,
        params: &BindArgs,
        out_binds: &OutBindSpec,
        fetch_decimals: bool,
        lowercase_column_names: bool,
        max_rows: Option<usize>,
        is_query: bool,
    ) -> Result<WorkerQueryOutcome, OracleError> {
        self.may_have_open_txn = true;
        let auto_commit = !self.in_transaction.load(Ordering::Acquire);
        let break_handle = Arc::clone(&self.break_handle);
        let clean = strip_leading_sql_comments(sql);
        let starts_select = clean
            .get(..6)
            .is_some_and(|p| p.eq_ignore_ascii_case("SELECT"))
            && !clean.as_bytes().get(6).is_some_and(|&b| is_ident_char(b));
        let starts_with = clean
            .get(..4)
            .is_some_and(|p| p.eq_ignore_ascii_case("WITH"))
            && !clean.as_bytes().get(4).is_some_and(|&b| is_ident_char(b));
        let is_select =
            matches!(out_binds, OutBindSpec::None) && (is_query || starts_select || starts_with);
        if is_select && parse_returning_clause(clean).is_none() {
            let query_outcome: Result<(Vec<OracleColumn>, Vec<Vec<OracleCellValue>>), OracleError> = {
                let conn = self.ensure_connected_and_flush_attrs()?;
                let cursor_res = match params {
                    BindArgs::Positional(vals) => {
                        let mut boxed = Vec::with_capacity(vals.len());
                        for v in vals {
                            boxed.push(cell_to_oracledb_bind(v)?);
                        }
                        let refs: Vec<&dyn oracledb::ToDbValue> = boxed
                            .iter()
                            .map(|b| &**b as &dyn oracledb::ToDbValue)
                            .collect();
                        conn.query(sql, &refs)
                    }
                    BindArgs::Named(pairs) => {
                        let mut boxed = Vec::with_capacity(pairs.len());
                        for (k, v) in pairs {
                            boxed.push((k.as_str(), cell_to_oracledb_bind(v)?));
                        }
                        let refs: Vec<(&str, &dyn oracledb::ToDbValue)> = boxed
                            .iter()
                            .map(|(k, b)| (*k, &**b as &dyn oracledb::ToDbValue))
                            .collect();
                        conn.query_named(sql, &refs)
                    }
                };
                match cursor_res {
                    Err(e) => Err(OracleError::from_oracledb(&e)),
                    Ok(cursor) => {
                        let columns: Vec<OracleColumn> = cursor
                            .columns()
                            .iter()
                            .map(|m| OracleColumn::from_metadata(m, lowercase_column_names))
                            .collect();
                        let mut rows = Vec::new();
                        let mut fetch_err: Option<OracleError> = None;
                        for row_res in cursor {
                            if let Err(ce) = break_handle.check_cancelled() {
                                fetch_err = Some(ce);
                                break;
                            }
                            match row_res {
                                Err(e) => {
                                    fetch_err = Some(OracleError::from_oracledb(&e));
                                    break;
                                }
                                Ok(mut r) => {
                                    rows.push(decode_oracledb_row(&mut r, &columns)?);
                                    if let Some(lim) = max_rows
                                        && rows.len() >= lim
                                    {
                                        break;
                                    }
                                }
                            }
                        }
                        match fetch_err {
                            Some(err) => Err(err),
                            None => Ok((columns, rows)),
                        }
                    }
                }
            };
            match query_outcome {
                Ok((columns, rows)) => {
                    let rowcount = rows.len();
                    return Ok(WorkerQueryOutcome {
                        columns,
                        rows,
                        rowcount,
                        out_binds: OutBindsPayload::None,
                        lastrowid: None,
                        fetch_decimals,
                    });
                }
                Err(mapped) => {
                    self.discard_dead_connection_if_broken(&mapped);
                    self.break_handle.check_cancelled()?;
                    return Err(mapped);
                }
            }
        }

        let returning = parse_returning_clause(clean);
        let (exec_attempt, autocommit_ok) = {
            let conn = self.ensure_connected_and_flush_attrs()?;
            let res = match params {
                BindArgs::Positional(vals) => {
                    let mut boxed = Vec::with_capacity(vals.len());
                    for v in vals {
                        boxed.push(cell_to_oracledb_bind(v)?);
                    }
                    if let OutBindSpec::Positional(hints) = out_binds {
                        for h in hints {
                            boxed.push(out_bind_type_placeholder(h));
                        }
                    } else if let Some(ref ret) = returning {
                        for _ in &ret.out_vars {
                            boxed.push(out_bind_type_placeholder("VARCHAR2"));
                        }
                    }
                    let refs: Vec<&dyn oracledb::ToDbValue> = boxed
                        .iter()
                        .map(|b| &**b as &dyn oracledb::ToDbValue)
                        .collect();
                    conn.execute(sql, &refs)
                }
                BindArgs::Named(pairs) => {
                    let mut boxed: Vec<(String, Box<dyn oracledb::ToDbValue + Send + Sync>)> =
                        Vec::with_capacity(pairs.len());
                    for (k, v) in pairs {
                        boxed.push((k.clone(), cell_to_oracledb_bind(v)?));
                    }
                    if let OutBindSpec::Named(out_pairs) = out_binds {
                        for (k, h) in out_pairs {
                            let clean_k = k.strip_prefix(':').unwrap_or(k).to_owned();
                            if !boxed
                                .iter()
                                .any(|(ek, _)| ek.eq_ignore_ascii_case(&clean_k))
                            {
                                boxed.push((clean_k, out_bind_type_placeholder(h)));
                            }
                        }
                    } else if let Some(ref ret) = returning {
                        for var_name in &ret.out_vars {
                            if !boxed.iter().any(|(k, _)| k.eq_ignore_ascii_case(var_name)) {
                                boxed.push((
                                    var_name.clone(),
                                    out_bind_type_placeholder("VARCHAR2"),
                                ));
                            }
                        }
                    }
                    let refs: Vec<(&str, &dyn oracledb::ToDbValue)> = boxed
                        .iter()
                        .map(|(k, b)| (k.as_str(), &**b as &dyn oracledb::ToDbValue))
                        .collect();
                    conn.execute_named(sql, &refs)
                }
            };
            let commit_ok = if res.is_ok() && auto_commit {
                conn.commit().is_ok()
            } else {
                false
            };
            (res, commit_ok)
        };
        if auto_commit {
            self.note_autocommit(exec_attempt.is_ok(), autocommit_ok);
        }
        let mut exec_res = match exec_attempt {
            Ok(r) => r,
            Err(e) => return Err(self.map_call_err(&e)),
        };
        break_handle.check_cancelled()?;

        let rowcount = exec_res.rows_affected() as usize;
        let mut columns = Vec::new();
        let mut rows = Vec::new();
        let mut out_payload = OutBindsPayload::None;

        let returned_rows = exec_res.returned_data();
        if !returned_rows.is_empty() {
            for (ridx, mut r) in returned_rows.into_iter().enumerate() {
                if ridx == 0 {
                    columns = r
                        .columns()
                        .iter()
                        .enumerate()
                        .map(|(i, m)| {
                            let mut col = OracleColumn::from_metadata(m, lowercase_column_names);
                            if let Some(ref ret) = returning
                                && let Some(ret_col) = ret.columns.get(i)
                            {
                                col.name = if lowercase_column_names {
                                    ret_col.to_ascii_lowercase()
                                } else {
                                    ret_col.to_ascii_uppercase()
                                };
                            }
                            col
                        })
                        .collect();
                }
                rows.push(decode_oracledb_row(&mut r, &columns)?);
            }
            if let Some(first_row) = rows.first() {
                if let OutBindSpec::Named(out_pairs) = out_binds {
                    let pairs = out_pairs
                        .iter()
                        .map(|(k, _)| k.strip_prefix(':').unwrap_or(k).to_owned())
                        .zip(first_row.iter().cloned())
                        .collect();
                    out_payload = OutBindsPayload::Named(pairs);
                } else if let Some(ref ret) = returning
                    && ret.out_vars.iter().any(|v| v.parse::<usize>().is_err())
                {
                    let pairs = ret
                        .out_vars
                        .iter()
                        .cloned()
                        .zip(first_row.iter().cloned())
                        .collect();
                    out_payload = OutBindsPayload::Named(pairs);
                } else {
                    out_payload = OutBindsPayload::Positional(first_row.clone());
                }
            }
        } else {
            let mut out_row = exec_res.out_bind_data();
            if !out_row.columns().is_empty() {
                let raw_names: Vec<String> = out_row
                    .columns()
                    .iter()
                    .map(|m| m.name().to_owned())
                    .collect();
                let out_cols: Vec<OracleColumn> = out_row
                    .columns()
                    .iter()
                    .map(|m| OracleColumn::from_metadata(m, lowercase_column_names))
                    .collect();
                let decoded = decode_oracledb_row(&mut out_row, &out_cols)?;
                match out_binds {
                    OutBindSpec::Named(out_pairs) => {
                        let mut named = Vec::with_capacity(decoded.len());
                        for (meta_name, val) in raw_names.into_iter().zip(decoded) {
                            let key = out_pairs
                                .iter()
                                .find_map(|(k, _)| {
                                    let clean = k.strip_prefix(':').unwrap_or(k);
                                    clean
                                        .eq_ignore_ascii_case(&meta_name)
                                        .then(|| clean.to_owned())
                                })
                                .unwrap_or_else(|| meta_name.to_ascii_lowercase());
                            named.push((key, val));
                        }
                        out_payload = OutBindsPayload::Named(named);
                    }
                    OutBindSpec::Positional(_) => {
                        out_payload = OutBindsPayload::Positional(decoded);
                    }
                    OutBindSpec::None => {
                        if raw_names.iter().any(|n| n.parse::<usize>().is_err()) {
                            let named = raw_names
                                .into_iter()
                                .map(|n| n.to_ascii_lowercase())
                                .zip(decoded)
                                .collect();
                            out_payload = OutBindsPayload::Named(named);
                        } else {
                            out_payload = OutBindsPayload::Positional(decoded);
                        }
                    }
                }
            }
        }

        Ok(WorkerQueryOutcome {
            columns,
            rows,
            rowcount,
            out_binds: out_payload,
            lastrowid: None,
            fetch_decimals,
        })
    }
}

struct CancelOnDrop {
    break_handle: Arc<OracleBreakHandle>,
    armed: bool,
}

impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        if self.armed {
            self.break_handle.break_execution();
        }
    }
}

/// Handle to one `oracledb::Connection` session shared between synchronous callers, which run commands
/// on their own thread, and a dedicated `"vtb-ora-worker"` OS thread that serves asynchronous callers.
pub struct OracleWorkerHandle {
    id: u64,
    config: Arc<NativeOracleConfig>,
    session: Arc<Mutex<WorkerSession>>,
    sender: Mutex<Option<mpsc::Sender<WorkerEnvelope>>>,
    join_handle: Mutex<Option<JoinHandle<()>>>,
    break_handle: Arc<OracleBreakHandle>,
    closed: Arc<AtomicBool>,
    in_transaction: Arc<AtomicBool>,
    created_at: Instant,
    last_used_at: Mutex<Instant>,
    last_health_check_at: Mutex<Instant>,
    purity: Mutex<String>,
    connection_tag: Mutex<Option<String>>,
    tag_matched: AtomicBool,
    pending_session_attrs: Arc<Mutex<Option<OracleSessionAttrs>>>,
    active_session_attrs: Arc<Mutex<OracleSessionAttrs>>,
    last_piggyback_frame: Arc<Mutex<Option<Vec<u8>>>>,
}

impl OracleWorkerHandle {
    /// Spawn a dedicated worker thread for `config` without opening a socket at construction time.
    pub fn spawn(config: NativeOracleConfig) -> Result<Self, OracleError> {
        let id = NEXT_WORKER_ID.fetch_add(1, Ordering::Relaxed);
        let now = Instant::now();
        let initial_pending = if config.client_identifier.is_some()
            || config.module.is_some()
            || config.action.is_some()
            || config.ecid.is_some()
        {
            Some(OracleSessionAttrs {
                client_identifier: config.client_identifier.clone(),
                module: config.module.clone(),
                action: config.action.clone(),
                ecid: config.ecid.clone(),
            })
        } else {
            None
        };
        let initial_purity = config.purity.as_str().to_owned();
        let initial_tag = config.connection_tag.clone();
        let cfg = Arc::new(config);
        let break_handle = Arc::new(OracleBreakHandle::new());
        let closed = Arc::new(AtomicBool::new(false));
        let in_transaction = Arc::new(AtomicBool::new(false));
        let pending_session_attrs = Arc::new(Mutex::new(initial_pending));
        let active_session_attrs = Arc::new(Mutex::new(OracleSessionAttrs::default()));
        let last_piggyback_frame = Arc::new(Mutex::new(None));
        let (tx, rx) = mpsc::channel::<WorkerEnvelope>();

        let cfg_clone = Arc::clone(&cfg);
        let break_clone = Arc::clone(&break_handle);
        let closed_clone = Arc::clone(&closed);
        let in_tx_clone = Arc::clone(&in_transaction);
        let pending_clone = Arc::clone(&pending_session_attrs);
        let active_clone = Arc::clone(&active_session_attrs);
        let last_frame_clone = Arc::clone(&last_piggyback_frame);

        let session = Arc::new(Mutex::new(WorkerSession {
            config: cfg_clone,
            conn: None,
            upstream_socket: None,
            savepoints: Vec::new(),
            in_transaction: in_tx_clone,
            may_have_open_txn: false,
            closed: closed_clone,
            break_handle: break_clone,
            pending_session_attrs: pending_clone,
            active_session_attrs: active_clone,
            last_piggyback_frame: last_frame_clone,
            #[cfg(test)]
            reset_rollbacks: 0,
        }));
        let worker_session = Arc::clone(&session);
        let worker_break = Arc::clone(&break_handle);

        let join_handle = thread::Builder::new()
            .name("vtb-ora-worker".to_owned())
            .spawn(move || {
                while let Ok(envelope) = rx.recv() {
                    let is_close = matches!(envelope.cmd, WorkerCommand::Close);
                    let outcome = worker_session
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .handle_command(envelope.cmd);
                    worker_break.end_operation();
                    envelope.reply.send(outcome);
                    if is_close {
                        break;
                    }
                }
            })
            .map_err(|e| {
                OracleError::Interface(format!("failed to spawn Oracle worker thread: {e}"))
            })?;

        Ok(Self {
            id,
            config: cfg,
            session,
            sender: Mutex::new(Some(tx)),
            join_handle: Mutex::new(Some(join_handle)),
            break_handle,
            closed,
            in_transaction,
            created_at: now,
            last_used_at: Mutex::new(now),
            last_health_check_at: Mutex::new(now),
            purity: Mutex::new(initial_purity),
            connection_tag: Mutex::new(initial_tag),
            tag_matched: AtomicBool::new(false),
            pending_session_attrs,
            active_session_attrs,
            last_piggyback_frame,
        })
    }

    /// Return the unique numeric ID of this worker handle.
    #[must_use]
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Return the timestamp when this worker session was created.
    #[must_use]
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// Return the timestamp when this worker session was last returned to or checked out from a pool.
    #[must_use]
    pub fn last_used_at(&self) -> Instant {
        *self.last_used_at.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Update the `last_used_at` timestamp to `Instant::now()`.
    pub fn touch_last_used(&self) {
        let mut guard = self.last_used_at.lock().unwrap_or_else(|e| e.into_inner());
        *guard = Instant::now();
    }

    /// Return the timestamp when this worker session last passed a health check.
    #[must_use]
    pub fn last_health_check_at(&self) -> Instant {
        *self
            .last_health_check_at
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    /// Update the `last_health_check_at` timestamp to `Instant::now()`.
    pub fn touch_health_check(&self) {
        let mut guard = self
            .last_health_check_at
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = Instant::now();
    }

    /// Return the DRCP connection class (`cclass`) if configured.
    #[must_use]
    pub fn cclass(&self) -> Option<String> {
        self.config.cclass.clone()
    }

    /// Return the active DRCP purity (`"SELF"` or `"NEW"`).
    #[must_use]
    pub fn purity(&self) -> String {
        self.purity
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Set the active DRCP purity (`"SELF"` or `"NEW"`).
    pub fn set_purity(&self, purity: String) {
        let mut guard = self.purity.lock().unwrap_or_else(|e| e.into_inner());
        *guard = purity;
    }

    /// Return the current DRCP connection tag.
    #[must_use]
    pub fn connection_tag(&self) -> Option<String> {
        self.connection_tag
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Update the DRCP connection tag.
    pub fn set_connection_tag(&self, tag: Option<String>) {
        let mut guard = self
            .connection_tag
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        *guard = tag;
    }

    /// Return `true` if the most recent pool checkout matched the requested `connection_tag`.
    #[must_use]
    pub fn tag_matched(&self) -> bool {
        self.tag_matched.load(Ordering::Acquire)
    }

    /// Set whether the most recent pool checkout matched the requested `connection_tag`.
    pub fn set_tag_matched(&self, matched: bool) {
        self.tag_matched.store(matched, Ordering::Release);
    }

    /// Clear prior session tag and active session attributes when checked out with `purity="NEW"`.
    pub fn reset_for_new_purity(&self) {
        self.set_connection_tag(None);
        self.set_tag_matched(false);
        let had_active = {
            let active = self
                .active_session_attrs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            active.clone()
        };
        {
            let mut pending = self
                .pending_session_attrs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *pending = Some(OracleSessionAttrs {
                client_identifier: had_active.client_identifier.map(|_| String::new()),
                module: had_active.module.map(|_| String::new()),
                action: had_active.action.map(|_| String::new()),
                ecid: had_active.ecid.map(|_| String::new()),
            });
        }
        {
            let mut active = self
                .active_session_attrs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *active = OracleSessionAttrs::default();
        }
        {
            let mut frame = self
                .last_piggyback_frame
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *frame = None;
        }
    }

    /// Stage resets for any session attributes changed during an untagged pool lease back to pool defaults.
    pub fn reset_untagged_session_attrs(&self) {
        let active = self
            .active_session_attrs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let pending = self
            .pending_session_attrs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();

        let eff_cid = pending
            .as_ref()
            .and_then(|p| p.client_identifier.as_ref())
            .or(active.client_identifier.as_ref());
        let eff_mod = pending
            .as_ref()
            .and_then(|p| p.module.as_ref())
            .or(active.module.as_ref());
        let eff_act = pending
            .as_ref()
            .and_then(|p| p.action.as_ref())
            .or(active.action.as_ref());
        let eff_ecid = pending
            .as_ref()
            .and_then(|p| p.ecid.as_ref())
            .or(active.ecid.as_ref());

        let reset_cid = (eff_cid != self.config.client_identifier.as_ref())
            .then(|| self.config.client_identifier.clone().unwrap_or_default());
        let reset_mod = (eff_mod != self.config.module.as_ref())
            .then(|| self.config.module.clone().unwrap_or_default());
        let reset_act = (eff_act != self.config.action.as_ref())
            .then(|| self.config.action.clone().unwrap_or_default());
        let reset_ecid = (eff_ecid != self.config.ecid.as_ref())
            .then(|| self.config.ecid.clone().unwrap_or_default());

        if reset_cid.is_some() || reset_mod.is_some() || reset_act.is_some() || reset_ecid.is_some()
        {
            let mut guard = self
                .pending_session_attrs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *guard = Some(OracleSessionAttrs {
                client_identifier: reset_cid,
                module: reset_mod,
                action: reset_act,
                ecid: reset_ecid,
            });
        }
    }

    /// Stage end-to-end tracing and session attributes in memory to be flushed on the next statement.
    pub fn stage_session_attrs(
        &self,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) {
        if client_identifier.is_none() && module.is_none() && action.is_none() && ecid.is_none() {
            return;
        }
        let mut guard = self
            .pending_session_attrs
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let entry = guard.get_or_insert_with(OracleSessionAttrs::default);
        if let Some(cid) = client_identifier {
            entry.client_identifier = Some(cid);
        }
        if let Some(m) = module {
            entry.module = Some(m);
        }
        if let Some(a) = action {
            entry.action = Some(a);
        }
        if let Some(e) = ecid {
            entry.ecid = Some(e);
        }
    }

    /// Return a snapshot of any staged `OracleSessionAttrs` awaiting flush.
    #[must_use]
    pub fn pending_session_attrs(&self) -> Option<OracleSessionAttrs> {
        self.pending_session_attrs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Return a snapshot of the currently active `OracleSessionAttrs` on the session.
    #[must_use]
    pub fn active_session_attrs(&self) -> OracleSessionAttrs {
        self.active_session_attrs
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Return the raw bytes of the most recently flushed `[0x11, 0x6e, count, ...]` TTC piggyback frame.
    #[must_use]
    pub fn last_piggyback_frame(&self) -> Option<Vec<u8>> {
        self.last_piggyback_frame
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Return a reference to the validated [`NativeOracleConfig`].
    #[must_use]
    pub fn config(&self) -> &NativeOracleConfig {
        &self.config
    }

    /// Return a shared [`Arc<NativeOracleConfig>`] for this worker session.
    #[must_use]
    pub fn config_arc(&self) -> Arc<NativeOracleConfig> {
        Arc::clone(&self.config)
    }

    /// Return a shared reference to the out-of-band [`OracleBreakHandle`].
    #[must_use]
    pub fn break_handle(&self) -> &Arc<OracleBreakHandle> {
        &self.break_handle
    }

    /// Return `true` if this client has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Return `true` if a transaction is currently open on this client.
    #[must_use]
    pub fn is_in_transaction(&self) -> bool {
        self.in_transaction.load(Ordering::Acquire)
    }

    /// Trigger an out-of-band `break_execution()` on any in-flight command.
    pub fn cancel(&self) {
        if !self.is_closed() {
            self.break_handle.break_execution();
        }
    }

    fn ensure_open(&self) -> Result<(), OracleError> {
        if self.is_closed() {
            return Err(OracleError::Interface("Oracle client is closed".to_owned()));
        }
        Ok(())
    }

    pub(crate) fn dispatch_sync(&self, cmd: WorkerCommand) -> Result<WorkerReply, OracleError> {
        if !matches!(cmd, WorkerCommand::Close) {
            self.ensure_open()?;
        }
        let tx = {
            let guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        let Some(sender) = tx else {
            if matches!(cmd, WorkerCommand::Close) {
                self.closed.store(true, Ordering::Release);
                self.in_transaction.store(false, Ordering::Release);
                return Ok(WorkerReply::Unit);
            }
            return Err(OracleError::Interface("Oracle client is closed".to_owned()));
        };

        if !matches!(cmd, WorkerCommand::Close) {
            drop(sender);
            self.break_handle.begin_operation();
            let outcome = self
                .session
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .handle_command(cmd);
            self.break_handle.end_operation();
            return outcome;
        }

        let queue = Arc::new(SyncCompletionQueue::new());
        self.break_handle.begin_operation();
        if sender
            .send(WorkerEnvelope {
                cmd,
                reply: ReplyChannel::Sync(Arc::clone(&queue)),
            })
            .is_err()
        {
            self.break_handle.end_operation();
            return Err(OracleError::Interface(
                "Oracle worker thread terminated".to_owned(),
            ));
        }
        queue.pop_blocking()
    }

    pub(crate) fn dispatch_async<M>(
        &self,
        py: Python<'_>,
        cmd: WorkerCommand,
        map_ok: M,
    ) -> PyResult<RustDbTaskAwaitable>
    where
        M: FnOnce(Python<'_>, WorkerReply) -> PyResult<Py<PyAny>> + Send + 'static,
    {
        let is_close = matches!(cmd, WorkerCommand::Close);
        if !is_close && let Err(err) = self.ensure_open() {
            return Err(err.to_py_err(py));
        }

        let tx_opt = {
            let mut guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            if is_close {
                self.closed.store(true, Ordering::Release);
                self.in_transaction.store(false, Ordering::Release);
                guard.take()
            } else {
                guard.clone()
            }
        };

        let Some(sender) = tx_opt else {
            if is_close {
                return spawn_rust_db_task_with_attr(
                    py,
                    async { Ok::<WorkerReply, OracleError>(WorkerReply::Unit) },
                    map_ok,
                    |py, err| err.to_py_err(py),
                    ORACLE_COMPLETION_DRAINER_ATTR,
                );
            }
            return Err(OracleError::Interface("Oracle client is closed".to_owned()).to_py_err(py));
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.break_handle.begin_operation();
        if sender
            .send(WorkerEnvelope {
                cmd,
                reply: ReplyChannel::Async(reply_tx),
            })
            .is_err()
        {
            self.break_handle.end_operation();
            return Err(
                OracleError::Interface("Oracle worker thread terminated".to_owned()).to_py_err(py),
            );
        }

        let break_for_drop = Arc::clone(&self.break_handle);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let mut cancel_guard = CancelOnDrop {
                    break_handle: break_for_drop,
                    armed: true,
                };
                let res = match reply_rx.await {
                    Ok(outcome) => outcome,
                    Err(_) => Err(OracleError::Interface(
                        "Oracle worker channel closed before reply".to_owned(),
                    )),
                };
                cancel_guard.armed = false;
                res
            },
            map_ok,
            |py, err| err.to_py_err(py),
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub(crate) async fn dispatch_async_raw(
        &self,
        cmd: WorkerCommand,
    ) -> Result<WorkerReply, OracleError> {
        let is_close = matches!(cmd, WorkerCommand::Close);
        if !is_close {
            self.ensure_open()?;
        }

        let tx_opt = {
            let mut guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            if is_close {
                self.closed.store(true, Ordering::Release);
                self.in_transaction.store(false, Ordering::Release);
                guard.take()
            } else {
                guard.clone()
            }
        };

        let Some(sender) = tx_opt else {
            if is_close {
                return Ok(WorkerReply::Unit);
            }
            return Err(OracleError::Interface("Oracle client is closed".to_owned()));
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.break_handle.begin_operation();
        if sender
            .send(WorkerEnvelope {
                cmd,
                reply: ReplyChannel::Async(reply_tx),
            })
            .is_err()
        {
            self.break_handle.end_operation();
            return Err(OracleError::Interface(
                "Oracle worker thread terminated".to_owned(),
            ));
        }

        let mut cancel_guard = CancelOnDrop {
            break_handle: Arc::clone(&self.break_handle),
            armed: true,
        };
        let res = match reply_rx.await {
            Ok(outcome) => outcome,
            Err(_) => Err(OracleError::Interface(
                "Oracle worker channel closed before reply".to_owned(),
            )),
        };
        cancel_guard.armed = false;
        res
    }

    /// Roll back any uncommitted transaction state within `timeout`, or close the worker if it times out.
    pub fn rollback_with_timeout(&self, timeout: Duration) -> Result<(), OracleError> {
        self.dispatch_channel_with_timeout(WorkerCommand::Rollback, timeout)
    }

    /// Roll back uncommitted or potentially open transaction state within `timeout`, skipping the
    /// server round trip when the session is provably clean.
    pub fn reset_with_timeout(&self, timeout: Duration) -> Result<(), OracleError> {
        self.dispatch_channel_with_timeout(WorkerCommand::ResetIfDirty, timeout)
    }

    #[cfg(test)]
    fn reset_rollbacks(&self) -> usize {
        self.session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .reset_rollbacks
    }

    fn dispatch_channel_with_timeout(
        &self,
        cmd: WorkerCommand,
        timeout: Duration,
    ) -> Result<(), OracleError> {
        self.ensure_open()?;
        let tx = {
            let guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        let Some(sender) = tx else {
            return Err(OracleError::Interface("Oracle client is closed".to_owned()));
        };
        let queue = Arc::new(SyncCompletionQueue::new());
        self.break_handle.begin_operation();
        if sender
            .send(WorkerEnvelope {
                cmd,
                reply: ReplyChannel::Sync(Arc::clone(&queue)),
            })
            .is_err()
        {
            self.break_handle.end_operation();
            return Err(OracleError::Interface(
                "Oracle worker thread terminated".to_owned(),
            ));
        }
        match queue.pop_timeout(timeout) {
            Some(Ok(_)) => Ok(()),
            Some(Err(err)) => {
                let _ = self.close_sync();
                Err(err)
            }
            None => {
                let _ = self.close_sync();
                Err(OracleError::operational(
                    "Oracle session reset timed out during rollback",
                ))
            }
        }
    }

    /// Synchronously close the worker session and join the worker thread.
    pub fn close_sync(&self) -> Result<(), OracleError> {
        if self.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        self.in_transaction.store(false, Ordering::Release);
        self.break_handle.break_execution();
        let sender = {
            let mut guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(tx) = sender {
            let queue = Arc::new(SyncCompletionQueue::new());
            if tx
                .send(WorkerEnvelope {
                    cmd: WorkerCommand::Close,
                    reply: ReplyChannel::Sync(Arc::clone(&queue)),
                })
                .is_ok()
            {
                let _ = queue.pop_timeout(Duration::from_secs(2));
            }
        }
        let join = {
            let mut guard = self.join_handle.lock().unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        if let Some(handle) = join {
            let _ = handle.join();
        }
        Ok(())
    }
}

impl Drop for OracleWorkerHandle {
    fn drop(&mut self) {
        let _ = self.close_sync();
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn build_raw_oracle_options(
    user: Option<String>,
    password: Option<String>,
    dsn: Option<String>,
    host: String,
    port: i32,
    service_name: String,
    sid: Option<String>,
    tns_admin: Option<String>,
    connect_descriptor: Option<String>,
    protocol: String,
    ssl_server_dn_match: bool,
    ssl_server_cert_dn: Option<String>,
    wallet_location: Option<String>,
    wallet_password: Option<String>,
    pem_cert_data: Option<Vec<u8>>,
    cwallet_sso_data: Option<Vec<u8>>,
    auth_mode: String,
    access_token: Option<String>,
    token_auth_options: Option<BTreeMap<String, String>>,
    cclass: Option<String>,
    purity: String,
    connection_tag: Option<String>,
    stmtcachesize: i32,
    prefetchrows: i64,
    arraysize: i64,
    call_timeout_ms: Option<i64>,
    connect_timeout: f64,
    enable_lowercase_column_names: bool,
    client_identifier: Option<String>,
    module: Option<String>,
    action: Option<String>,
    ecid: Option<String>,
) -> RawOracleOptions {
    RawOracleOptions {
        user,
        password,
        dsn,
        host,
        port,
        service_name,
        sid,
        tns_admin,
        connect_descriptor,
        protocol,
        ssl_server_dn_match,
        ssl_server_cert_dn,
        wallet_location,
        wallet_password,
        pem_cert_data,
        cwallet_sso_data,
        auth_mode,
        access_token,
        token_auth_options,
        cclass,
        purity,
        connection_tag,
        stmtcachesize,
        prefetchrows,
        arraysize,
        call_timeout_ms,
        connect_timeout,
        enable_lowercase_column_names,
        client_identifier,
        module,
        action,
        ecid,
    }
}

fn session_attrs_to_py_dict(py: Python<'_>, attrs: &OracleSessionAttrs) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("client_identifier", attrs.client_identifier.as_deref())?;
    dict.set_item("module", attrs.module.as_deref())?;
    dict.set_item("action", attrs.action.as_deref())?;
    dict.set_item("ecid", attrs.ecid.as_deref())?;
    Ok(dict.unbind())
}

fn reply_to_query_result(py: Python<'_>, reply: WorkerReply) -> PyResult<OracleQueryResult> {
    match reply {
        WorkerReply::QueryOutcome(outcome) => outcome.into_py_query_result(py),
        WorkerReply::RowCount(n) => Ok(OracleQueryResult::new(
            py,
            Vec::new(),
            Vec::new(),
            n,
            None,
            None,
        )),
        _ => Ok(OracleQueryResult::new(
            py,
            Vec::new(),
            Vec::new(),
            0,
            None,
            None,
        )),
    }
}

macro_rules! impl_oracle_client_pymethods {
    (
        $client:ident,
        cursor = $cursor:ident,
        unit_ret = $unit_ret:ty,
        single_ret = $single_ret:ty,
        pipeline_ret = $pipeline_ret:ty,
        single_calls = [ $( $sc_name:ident => $sc_shape:path ),* $(,)? ],
        extra = { $($extra:tt)* }
    ) => {
        impl $client {
            /// Construct a client wrapping an existing [`OracleWorkerHandle`].
            #[must_use]
            pub fn from_worker(worker: Arc<OracleWorkerHandle>) -> Self {
                Self {
                    worker,
                    lease: Arc::new(Mutex::new(None)),
                }
            }

            /// Return a reference to the underlying [`OracleWorkerHandle`].
            #[must_use]
            pub fn worker(&self) -> &Arc<OracleWorkerHandle> {
                &self.worker
            }
        }

        #[pymethods]
        impl $client {
            #[new]
            #[pyo3(signature = (
                *,
                user=None,
                password=None,
                dsn=None,
                host="localhost",
                port=1521,
                service_name="FREEPDB1",
                sid=None,
                tns_admin=None,
                connect_descriptor=None,
                protocol="tcp",
                ssl_server_dn_match=true,
                ssl_server_cert_dn=None,
                wallet_location=None,
                wallet_password=None,
                pem_cert_data=None,
                cwallet_sso_data=None,
                auth_mode="password",
                access_token=None,
                token_auth_options=None,
                cclass=None,
                purity="SELF",
                connection_tag=None,
                stmtcachesize=64,
                prefetchrows=256,
                arraysize=256,
                call_timeout_ms=None,
                connect_timeout=10.0,
                enable_lowercase_column_names=true,
                client_identifier=None,
                module=None,
                action=None,
                ecid=None,
                enable_reset_relay=true,
            ))]
            #[allow(clippy::too_many_arguments)]
            pub fn new(
                py: Python<'_>,
                user: Option<String>,
                password: Option<String>,
                dsn: Option<String>,
                host: &str,
                port: i32,
                service_name: &str,
                sid: Option<String>,
                tns_admin: Option<String>,
                connect_descriptor: Option<String>,
                protocol: &str,
                ssl_server_dn_match: bool,
                ssl_server_cert_dn: Option<String>,
                wallet_location: Option<String>,
                wallet_password: Option<String>,
                pem_cert_data: Option<Vec<u8>>,
                cwallet_sso_data: Option<Vec<u8>>,
                auth_mode: &str,
                access_token: Option<String>,
                token_auth_options: Option<BTreeMap<String, String>>,
                cclass: Option<String>,
                purity: &str,
                connection_tag: Option<String>,
                stmtcachesize: i32,
                prefetchrows: i64,
                arraysize: i64,
                call_timeout_ms: Option<i64>,
                connect_timeout: f64,
                enable_lowercase_column_names: bool,
                client_identifier: Option<String>,
                module: Option<String>,
                action: Option<String>,
                ecid: Option<String>,
                enable_reset_relay: bool,
            ) -> PyResult<Self> {
                let raw = build_raw_oracle_options(
                    user,
                    password,
                    dsn,
                    host.to_owned(),
                    port,
                    service_name.to_owned(),
                    sid,
                    tns_admin,
                    connect_descriptor,
                    protocol.to_owned(),
                    ssl_server_dn_match,
                    ssl_server_cert_dn,
                    wallet_location,
                    wallet_password,
                    pem_cert_data,
                    cwallet_sso_data,
                    auth_mode.to_owned(),
                    access_token,
                    token_auth_options,
                    cclass,
                    purity.to_owned(),
                    connection_tag,
                    stmtcachesize,
                    prefetchrows,
                    arraysize,
                    call_timeout_ms,
                    connect_timeout,
                    enable_lowercase_column_names,
                    client_identifier,
                    module,
                    action,
                    ecid,
                );
                let mut cfg = NativeOracleConfig::validate(raw).map_err(|e| e.to_py_err(py))?;
                cfg.enable_reset_relay = enable_reset_relay;
                let worker = OracleWorkerHandle::spawn(cfg).map_err(|e| e.to_py_err(py))?;
                Ok(Self {
                    worker: Arc::new(worker),
                    lease: Arc::new(Mutex::new(None)),
                })
            }

            #[getter]
            pub fn closed(&self) -> bool {
                self.worker.is_closed()
            }

            #[getter]
            pub fn in_transaction(&self) -> bool {
                self.worker.is_in_transaction()
            }

            #[getter]
            pub fn cclass(&self) -> Option<String> {
                self.worker.cclass()
            }

            #[getter]
            pub fn purity(&self) -> String {
                self.worker.purity()
            }

            #[getter]
            pub fn connection_tag(&self) -> Option<String> {
                self.worker.connection_tag()
            }

            #[getter]
            pub fn tag_matched(&self) -> bool {
                self.worker.tag_matched()
            }

            #[getter]
            pub fn pending_session_attrs(&self, py: Python<'_>) -> PyResult<Option<Py<PyDict>>> {
                match self.worker.pending_session_attrs() {
                    Some(attrs) => Ok(Some(session_attrs_to_py_dict(py, &attrs)?)),
                    None => Ok(None),
                }
            }

            #[getter]
            pub fn active_session_attrs(&self, py: Python<'_>) -> PyResult<Py<PyDict>> {
                let attrs = self.worker.active_session_attrs();
                session_attrs_to_py_dict(py, &attrs)
            }

            #[getter]
            pub fn last_piggyback_frame(&self, py: Python<'_>) -> Option<Py<PyBytes>> {
                self.worker
                    .last_piggyback_frame()
                    .map(|bytes| PyBytes::new(py, &bytes).unbind())
            }

            #[pyo3(signature = (
                *,
                client_identifier=None,
                module=None,
                action=None,
                ecid=None,
            ))]
            pub fn set_session_attrs(
                &self,
                client_identifier: Option<String>,
                module: Option<String>,
                action: Option<String>,
                ecid: Option<String>,
            ) {
                self.worker
                    .stage_session_attrs(client_identifier, module, action, ecid);
            }

            pub fn connect(&self, py: Python<'_>) -> PyResult<$unit_ret> {
                self.dispatch_unit(py, WorkerCommand::Connect)
            }

            pub fn attach_lease(&self, py: Python<'_>, release: Py<PyAny>) {
                let previous = {
                    let mut slot = self.lease.lock().unwrap_or_else(|e| e.into_inner());
                    slot.replace(LeaseGuard::new(release))
                };
                if let Some(mut guard) = previous {
                    guard.release(py);
                }
            }

            pub fn release_lease(&self, py: Python<'_>) {
                let previous = {
                    let mut slot = self.lease.lock().unwrap_or_else(|e| e.into_inner());
                    slot.take()
                };
                if let Some(mut guard) = previous {
                    guard.release(py);
                }
            }

            #[pyo3(signature = (
                *,
                enable_uuid_binary = true,
                fetch_decimals = false,
                vector_return_format = "list",
                enable_lowercase_column_names = true,
                lazy_begin = false,
            ))]
            pub fn cursor(
                &self,
                py: Python<'_>,
                enable_uuid_binary: bool,
                fetch_decimals: bool,
                vector_return_format: &str,
                enable_lowercase_column_names: bool,
                lazy_begin: bool,
            ) -> PyResult<$cursor> {
                let parsed_format = VectorReturnFormat::parse(vector_return_format)
                    .map_err(|e| e.to_py_err(py))?;
                let options = OracleValueOptions {
                    enable_uuid_binary,
                    fetch_decimals,
                    vector_return_format: parsed_format,
                };
                Ok($cursor::new(
                    Arc::clone(&self.worker),
                    options,
                    enable_lowercase_column_names,
                    lazy_begin,
                ))
            }

            $(
                #[pyo3(
                    signature = (
                        sql,
                        params = None,
                        *,
                        out_binds = None,
                        enable_uuid_binary = true,
                        fetch_decimals = false,
                        vector_return_format = "list",
                        enable_lowercase_column_names = true,
                    ),
                    text_signature = "($self, sql, params=(), *, out_binds=None, enable_uuid_binary=True, fetch_decimals=False, vector_return_format='list', enable_lowercase_column_names=True)"
                )]
                #[allow(clippy::too_many_arguments)]
                pub fn $sc_name(
                    &self,
                    py: Python<'_>,
                    sql: &str,
                    params: Option<&Bound<'_, PyAny>>,
                    out_binds: Option<&Bound<'_, PyAny>>,
                    enable_uuid_binary: bool,
                    fetch_decimals: bool,
                    vector_return_format: &str,
                    enable_lowercase_column_names: bool,
                ) -> PyResult<$single_ret> {
                    self.single_call(
                        py,
                        sql,
                        params,
                        out_binds,
                        enable_uuid_binary,
                        fetch_decimals,
                        vector_return_format,
                        enable_lowercase_column_names,
                        $sc_shape,
                    )
                }
            )*

            pub fn ping(&self, py: Python<'_>) -> PyResult<$unit_ret> {
                self.dispatch_unit(py, WorkerCommand::Ping)
            }

            pub fn cancel(&self) {
                self.worker.cancel();
            }

            pub fn begin(&self, py: Python<'_>) -> PyResult<$unit_ret> {
                self.dispatch_unit(py, WorkerCommand::Begin)
            }

            pub fn commit(&self, py: Python<'_>) -> PyResult<$unit_ret> {
                self.dispatch_unit(py, WorkerCommand::Commit)
            }

            pub fn rollback(&self, py: Python<'_>) -> PyResult<$unit_ret> {
                self.dispatch_unit(py, WorkerCommand::Rollback)
            }

            pub fn savepoint(&self, py: Python<'_>, name: String) -> PyResult<$unit_ret> {
                validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
                self.dispatch_unit(py, WorkerCommand::Savepoint(name))
            }

            pub fn rollback_to_savepoint(
                &self,
                py: Python<'_>,
                name: String,
            ) -> PyResult<$unit_ret> {
                validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
                self.dispatch_unit(py, WorkerCommand::RollbackToSavepoint(name))
            }

            #[pyo3(signature = (operations, *, continue_on_error=false))]
            pub fn run_pipeline(
                &self,
                py: Python<'_>,
                operations: &Bound<'_, PyAny>,
                continue_on_error: bool,
            ) -> PyResult<$pipeline_ret> {
                self.execute_pipeline_batch(py, operations, continue_on_error)
            }

            $($extra)*
        }
    };
}

/// Synchronous physical Oracle session client (`vertebra._oracle.SyncOracleClient`).
#[derive(Clone)]
#[pyclass(
    name = "SyncOracleClient",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct SyncOracleClient {
    worker: Arc<OracleWorkerHandle>,
    lease: Arc<Mutex<Option<LeaseGuard>>>,
}

impl SyncOracleClient {
    fn dispatch_unit(&self, py: Python<'_>, cmd: WorkerCommand) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(cmd))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    #[allow(clippy::too_many_arguments)]
    fn single_call(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        out_binds: Option<&Bound<'_, PyAny>>,
        enable_uuid_binary: bool,
        fetch_decimals: bool,
        vector_return_format: &str,
        enable_lowercase_column_names: bool,
        shape: fn(Python<'_>, BuiltReply) -> PyResult<Py<PyAny>>,
    ) -> PyResult<Py<PyAny>> {
        let parsed_format =
            VectorReturnFormat::parse(vector_return_format).map_err(|e| e.to_py_err(py))?;
        let options = OracleValueOptions {
            enable_uuid_binary,
            fetch_decimals,
            vector_return_format: parsed_format,
        };
        let prepared = prepare_execute(
            py,
            sql,
            params,
            out_binds,
            options,
            enable_lowercase_column_names,
        )?;
        run_single_call_sync(py, &self.worker, prepared, shape)
    }
}

impl_oracle_client_pymethods!(
    SyncOracleClient,
    cursor = SyncOracleCursor,
    unit_ret = (),
    single_ret = Py<PyAny>,
    pipeline_ret = Vec<OraclePipelineOutcome>,
    single_calls = [
        fetch_rows => reply_rows_and_names,
        fetch_dicts => reply_dicts,
        fetch_value => reply_first_value,
        execute_dml => reply_dml,
    ],
    extra = {
        pub fn close(&self, py: Python<'_>) -> PyResult<()> {
            let worker = Arc::clone(&self.worker);
            let res = py
                .detach(|| worker.close_sync())
                .map_err(|e| e.to_py_err(py));
            self.release_lease(py);
            res
        }

        #[pyo3(signature = (
            sql,
            params=None,
            *,
            out_binds=None,
            fetch_decimals=false,
            enable_lowercase_column_names=None,
        ))]
        pub fn execute(
            &self,
            py: Python<'_>,
            sql: String,
            params: Option<&Bound<'_, PyAny>>,
            out_binds: Option<&Bound<'_, PyAny>>,
            fetch_decimals: bool,
            enable_lowercase_column_names: Option<bool>,
        ) -> PyResult<OracleQueryResult> {
            let bind_args = extract_bind_args(params).map_err(|e| e.to_py_err(py))?;
            let out_spec = extract_out_bind_spec(out_binds).map_err(|e| e.to_py_err(py))?;
            let lowercase = enable_lowercase_column_names
                .unwrap_or(self.worker.config().enable_lowercase_column_names);
            let worker = Arc::clone(&self.worker);
            let reply = py
                .detach(|| {
                    worker.dispatch_sync(WorkerCommand::Execute {
                        sql,
                        params: bind_args,
                        out_binds: out_spec,
                        fetch_decimals,
                        lowercase_column_names: lowercase,
                    })
                })
                .map_err(|e| e.to_py_err(py))?;
            reply_to_query_result(py, reply)
        }

        pub fn execute_many(
            &self,
            py: Python<'_>,
            sql: String,
            params_seq: &Bound<'_, PyAny>,
        ) -> PyResult<usize> {
            self.worker.ensure_open().map_err(|e| e.to_py_err(py))?;
            let seq = extract_bind_seq(params_seq).map_err(|e| e.to_py_err(py))?;
            if seq.is_empty() {
                return Ok(0);
            }
            let worker = Arc::clone(&self.worker);
            let reply = py
                .detach(|| {
                    worker.dispatch_sync(WorkerCommand::ExecuteMany {
                        sql,
                        params_seq: seq,
                    })
                })
                .map_err(|e| e.to_py_err(py))?;
            match reply {
                WorkerReply::RowCount(n) => Ok(n),
                _ => Ok(0),
            }
        }

        #[pyo3(signature = (
            sql,
            params=None,
            *,
            batch_size=8192,
            force_shm=None,
            enable_lowercase_column_names=None,
        ))]
        pub fn query_arrow(
            &self,
            py: Python<'_>,
            sql: String,
            params: Option<&Bound<'_, PyAny>>,
            batch_size: usize,
            force_shm: Option<bool>,
            enable_lowercase_column_names: Option<bool>,
        ) -> PyResult<Py<PyAny>> {
            let bind_args = extract_bind_args(params).map_err(|e| e.to_py_err(py))?;
            let lowercase = enable_lowercase_column_names
                .unwrap_or(self.worker.config().enable_lowercase_column_names);
            let worker = Arc::clone(&self.worker);
            let reply = py
                .detach(|| {
                    worker.dispatch_sync(WorkerCommand::QueryArrow {
                        sql,
                        params: bind_args,
                        batch_size: batch_size.max(1),
                        lowercase_column_names: lowercase,
                    })
                })
                .map_err(|e| e.to_py_err(py))?;
            match reply {
                WorkerReply::ArrowIpc {
                    ipc_bytes,
                    row_count,
                } => build_py_arrow_cursor(py, ipc_bytes, row_count, force_shm),
                _ => build_py_arrow_cursor(py, Vec::new(), 0, force_shm),
            }
        }

        #[pyo3(signature = (operations, *, continue_on_error=false))]
        pub fn execute_pipeline_batch(
            &self,
            py: Python<'_>,
            operations: &Bound<'_, PyAny>,
            continue_on_error: bool,
        ) -> PyResult<Vec<OraclePipelineOutcome>> {
            self.worker.ensure_open().map_err(|e| e.to_py_err(py))?;
            let lowercase = self.worker.config().enable_lowercase_column_names;
            let ops = extract_pipeline_ops(operations, false, lowercase)
                .map_err(|e| e.to_py_err(py))?;
            if ops.is_empty() {
                return Ok(Vec::new());
            }
            let worker = Arc::clone(&self.worker);
            let reply = py
                .detach(|| {
                    worker.dispatch_sync(WorkerCommand::PipelineBatch {
                        ops,
                        continue_on_error,
                    })
                })
                .map_err(|e| e.to_py_err(py))?;
            match reply {
                WorkerReply::PipelineOutcomes(steps) => {
                    let mut py_outcomes = Vec::with_capacity(steps.len());
                    for step in steps {
                        py_outcomes.push(step.into_py_outcome(py)?);
                    }
                    Ok(py_outcomes)
                }
                _ => Ok(Vec::new()),
            }
        }

        #[pyo3(signature = (
            *,
            queue_name,
            payload,
            consumer_name=None,
            correlation_id=None,
        ))]
        pub fn enqueue_notification<'py>(
            &self,
            py: Python<'py>,
            queue_name: String,
            payload: Vec<u8>,
            consumer_name: Option<String>,
            correlation_id: Option<String>,
        ) -> PyResult<Bound<'py, PyBytes>> {
            let worker = Arc::clone(&self.worker);
            let msg_id = py
                .detach(|| {
                    enqueue_notification_sync(
                        &worker,
                        &queue_name,
                        &payload,
                        consumer_name.as_deref(),
                        correlation_id.as_deref(),
                    )
                })
                .map_err(|e| e.to_py_err(py))?;
            Ok(PyBytes::new(py, &msg_id))
        }

        #[pyo3(signature = (
            *,
            queue_name,
            consumer_name=None,
            wait_timeout_seconds=5,
            max_buffered=1024,
        ))]
        pub fn open_listener(
            &self,
            py: Python<'_>,
            queue_name: String,
            consumer_name: Option<String>,
            wait_timeout_seconds: u32,
            max_buffered: usize,
        ) -> PyResult<SyncOracleListener> {
            self.worker.ensure_open().map_err(|e| e.to_py_err(py))?;
            validate_aq_identifier(&queue_name, "queue_name").map_err(|e| e.to_py_err(py))?;
            if let Some(ref c) = consumer_name {
                validate_aq_identifier(c, "consumer_name").map_err(|e| e.to_py_err(py))?;
            }
            if max_buffered == 0 {
                return Err(OracleError::Config(
                    "SyncOracleListener max_buffered must be >= 1".to_owned(),
                )
                .to_py_err(py));
            }
            let cfg = self.worker.config_arc();
            SyncOracleListener::spawn(
                cfg,
                queue_name,
                consumer_name,
                wait_timeout_seconds,
                max_buffered,
            )
            .map_err(|e| e.to_py_err(py))
        }
    }
);

/// Asynchronous physical Oracle session client (`vertebra._oracle.AsyncOracleClient`).
#[derive(Clone)]
#[pyclass(
    name = "AsyncOracleClient",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct AsyncOracleClient {
    worker: Arc<OracleWorkerHandle>,
    lease: Arc<Mutex<Option<LeaseGuard>>>,
}

impl AsyncOracleClient {
    fn dispatch_unit(&self, py: Python<'_>, cmd: WorkerCommand) -> PyResult<RustDbTaskAwaitable> {
        self.worker.dispatch_async(py, cmd, |py, _| Ok(py.None()))
    }

    #[allow(clippy::too_many_arguments)]
    fn single_call(
        &self,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        out_binds: Option<&Bound<'_, PyAny>>,
        enable_uuid_binary: bool,
        fetch_decimals: bool,
        vector_return_format: &str,
        enable_lowercase_column_names: bool,
        shape: fn(Python<'_>, BuiltReply) -> PyResult<Py<PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let parsed_format =
            VectorReturnFormat::parse(vector_return_format).map_err(|e| e.to_py_err(py))?;
        let options = OracleValueOptions {
            enable_uuid_binary,
            fetch_decimals,
            vector_return_format: parsed_format,
        };
        let prepared = prepare_execute(
            py,
            sql,
            params,
            out_binds,
            options,
            enable_lowercase_column_names,
        )?;
        spawn_single_call(py, Arc::clone(&self.worker), prepared, shape)
    }
}

impl_oracle_client_pymethods!(
    AsyncOracleClient,
    cursor = AsyncOracleCursor,
    unit_ret = RustDbTaskAwaitable,
    single_ret = RustDbTaskAwaitable,
    pipeline_ret = RustDbTaskAwaitable,
    single_calls = [
        fetch_rows => reply_rows_and_names,
        fetch_dicts => reply_dicts,
        fetch_value => reply_first_value,
        execute_dml => reply_dml,
    ],
    extra = {
        pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
            let previous = {
                let mut slot = self.lease.lock().unwrap_or_else(|e| e.into_inner());
                slot.take()
            };
            self.worker
                .dispatch_async(py, WorkerCommand::Close, move |py, _| {
                    if let Some(mut guard) = previous {
                        guard.release(py);
                    }
                    Ok(py.None())
                })
        }

        #[pyo3(signature = (
            sql,
            params=None,
            *,
            out_binds=None,
            fetch_decimals=false,
            enable_lowercase_column_names=None,
        ))]
        pub fn execute(
            &self,
            py: Python<'_>,
            sql: String,
            params: Option<&Bound<'_, PyAny>>,
            out_binds: Option<&Bound<'_, PyAny>>,
            fetch_decimals: bool,
            enable_lowercase_column_names: Option<bool>,
        ) -> PyResult<RustDbTaskAwaitable> {
            let bind_args = extract_bind_args(params).map_err(|e| e.to_py_err(py))?;
            let out_spec = extract_out_bind_spec(out_binds).map_err(|e| e.to_py_err(py))?;
            let lowercase = enable_lowercase_column_names
                .unwrap_or(self.worker.config().enable_lowercase_column_names);
            self.worker.dispatch_async(
                py,
                WorkerCommand::Execute {
                    sql,
                    params: bind_args,
                    out_binds: out_spec,
                    fetch_decimals,
                    lowercase_column_names: lowercase,
                },
                |py, reply| {
                    let qres = reply_to_query_result(py, reply)?;
                    Ok(Py::new(py, qres)?.into_any())
                },
            )
        }

        pub fn execute_many(
            &self,
            py: Python<'_>,
            sql: String,
            params_seq: &Bound<'_, PyAny>,
        ) -> PyResult<RustDbTaskAwaitable> {
            self.worker.ensure_open().map_err(|e| e.to_py_err(py))?;
            let seq = extract_bind_seq(params_seq).map_err(|e| e.to_py_err(py))?;
            if seq.is_empty() {
                return spawn_rust_db_task_with_attr(
                    py,
                    async { Ok::<usize, OracleError>(0) },
                    |py, n| Ok(n.into_pyobject(py)?.into_any().unbind()),
                    |py, err| err.to_py_err(py),
                    ORACLE_COMPLETION_DRAINER_ATTR,
                );
            }
            self.worker.dispatch_async(
                py,
                WorkerCommand::ExecuteMany {
                    sql,
                    params_seq: seq,
                },
                |py, reply| match reply {
                    WorkerReply::RowCount(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
                    _ => Ok(0usize.into_pyobject(py)?.into_any().unbind()),
                },
            )
        }

        #[pyo3(signature = (
            sql,
            params=None,
            *,
            batch_size=8192,
            force_shm=None,
            enable_lowercase_column_names=None,
        ))]
        pub fn query_arrow(
            &self,
            py: Python<'_>,
            sql: String,
            params: Option<&Bound<'_, PyAny>>,
            batch_size: usize,
            force_shm: Option<bool>,
            enable_lowercase_column_names: Option<bool>,
        ) -> PyResult<RustDbTaskAwaitable> {
            let bind_args = extract_bind_args(params).map_err(|e| e.to_py_err(py))?;
            let lowercase = enable_lowercase_column_names
                .unwrap_or(self.worker.config().enable_lowercase_column_names);
            self.worker.dispatch_async(
                py,
                WorkerCommand::QueryArrow {
                    sql,
                    params: bind_args,
                    batch_size: batch_size.max(1),
                    lowercase_column_names: lowercase,
                },
                move |py, reply| match reply {
                    WorkerReply::ArrowIpc {
                        ipc_bytes,
                        row_count,
                    } => build_py_arrow_cursor(py, ipc_bytes, row_count, force_shm),
                    _ => build_py_arrow_cursor(py, Vec::new(), 0, force_shm),
                },
            )
        }

        #[pyo3(signature = (operations, *, continue_on_error=false))]
        pub fn execute_pipeline_batch(
            &self,
            py: Python<'_>,
            operations: &Bound<'_, PyAny>,
            continue_on_error: bool,
        ) -> PyResult<RustDbTaskAwaitable> {
            self.worker.ensure_open().map_err(|e| e.to_py_err(py))?;
            let lowercase = self.worker.config().enable_lowercase_column_names;
            let ops = extract_pipeline_ops(operations, false, lowercase)
                .map_err(|e| e.to_py_err(py))?;
            if ops.is_empty() {
                return spawn_rust_db_task_with_attr(
                    py,
                    async { Ok::<Vec<WorkerPipelineStepOutcome>, OracleError>(Vec::new()) },
                    |py, _| Ok(PyList::empty(py).into_any().unbind()),
                    |py, err| err.to_py_err(py),
                    ORACLE_COMPLETION_DRAINER_ATTR,
                );
            }
            self.worker.dispatch_async(
                py,
                WorkerCommand::PipelineBatch {
                    ops,
                    continue_on_error,
                },
                |py, reply| match reply {
                    WorkerReply::PipelineOutcomes(steps) => {
                        let mut py_outcomes = Vec::with_capacity(steps.len());
                        for step in steps {
                            let out = step.into_py_outcome(py)?;
                            py_outcomes.push(Py::new(py, out)?);
                        }
                        let list = PyList::new(py, py_outcomes)?;
                        Ok(list.into_any().unbind())
                    }
                    _ => Ok(PyList::empty(py).into_any().unbind()),
                },
            )
        }

        #[pyo3(signature = (
            *,
            queue_name,
            consumer_name=None,
            wait_timeout_seconds=5,
            max_buffered=1024,
        ))]
        pub fn open_listener(
            &self,
            py: Python<'_>,
            queue_name: String,
            consumer_name: Option<String>,
            wait_timeout_seconds: u32,
            max_buffered: usize,
        ) -> PyResult<RustDbTaskAwaitable> {
            self.worker.ensure_open().map_err(|e| e.to_py_err(py))?;
            validate_aq_identifier(&queue_name, "queue_name").map_err(|e| e.to_py_err(py))?;
            if let Some(ref c) = consumer_name {
                validate_aq_identifier(c, "consumer_name").map_err(|e| e.to_py_err(py))?;
            }
            if max_buffered == 0 {
                return Err(OracleError::Config(
                    "AsyncOracleListener max_buffered must be >= 1".to_owned(),
                )
                .to_py_err(py));
            }
            let cfg = self.worker.config_arc();
            spawn_rust_db_task_with_attr(
                py,
                async move {
                    AsyncOracleListener::spawn(
                        cfg,
                        queue_name,
                        consumer_name,
                        wait_timeout_seconds,
                        max_buffered,
                    )
                },
                |py, listener| Ok(Py::new(py, listener)?.into_any()),
                |py, err| err.to_py_err(py),
                ORACLE_COMPLETION_DRAINER_ATTR,
            )
        }

        #[pyo3(signature = (
            *,
            queue_name,
            payload,
            consumer_name=None,
            correlation_id=None,
        ))]
        pub fn enqueue_notification(
            &self,
            py: Python<'_>,
            queue_name: String,
            payload: Vec<u8>,
            consumer_name: Option<String>,
            correlation_id: Option<String>,
        ) -> PyResult<RustDbTaskAwaitable> {
            self.worker.ensure_open().map_err(|e| e.to_py_err(py))?;
            validate_aq_identifier(&queue_name, "queue_name").map_err(|e| e.to_py_err(py))?;
            if let Some(ref c) = consumer_name {
                validate_aq_identifier(c, "consumer_name").map_err(|e| e.to_py_err(py))?;
            }
            let worker = Arc::clone(&self.worker);
            spawn_rust_db_task_with_attr(
                py,
                async move {
                    tokio::task::spawn_blocking(move || {
                        enqueue_notification_sync(
                            &worker,
                            &queue_name,
                            &payload,
                            consumer_name.as_deref(),
                            correlation_id.as_deref(),
                        )
                    })
                    .await
                    .map_err(|e| OracleError::Interface(format!("enqueue task join error: {e}")))?
                },
                |py, msg_id| Ok(PyBytes::new(py, &msg_id).into_any().unbind()),
                |py, err| err.to_py_err(py),
                ORACLE_COMPLETION_DRAINER_ATTR,
            )
        }
    }
);

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::Duration;

    use tokio::sync::oneshot;
    use vertebra_db::py_cursor::{DEFAULT_PLAN_CACHE_CAPACITY, PlanLruCache};

    use super::{
        BindArgs, OracleBreakHandle, OracleWorkerHandle, OutBindSpec, ReplyChannel, StatementFacts,
        StatementKind, WorkerCommand, WorkerEnvelope, command_may_open_txn,
        compute_statement_facts, statement_facts, validate_savepoint_identifier,
    };
    use crate::config::{NativeOracleConfig, RawOracleOptions};
    use crate::errors::OracleError;

    #[test]
    fn savepoint_identifier_validation_enforces_oracle_rules() {
        assert!(validate_savepoint_identifier("sp_1").is_ok());
        assert!(validate_savepoint_identifier("_vtb_pipe_1$#").is_ok());
        assert!(matches!(
            validate_savepoint_identifier(""),
            Err(OracleError::Server { code: 900, .. })
        ));
        assert!(matches!(
            validate_savepoint_identifier("1bad"),
            Err(OracleError::Server { code: 900, .. })
        ));
        assert!(matches!(
            validate_savepoint_identifier("sp;drop"),
            Err(OracleError::Server { code: 900, .. })
        ));
    }

    #[test]
    fn worker_zero_io_spawn_session_attr_staging_and_break_handle_lifecycle() {
        let cfg = NativeOracleConfig::validate(RawOracleOptions {
            host: "localhost".to_owned(),
            ..Default::default()
        })
        .expect("valid oracle config");
        let worker = OracleWorkerHandle::spawn(cfg).expect("zero-I/O worker spawn");
        assert!(!worker.is_closed());
        assert!(!worker.is_in_transaction());

        worker.stage_session_attrs(
            Some("tenant-42".to_owned()),
            Some("vtb-test".to_owned()),
            Some("unit".to_owned()),
            None,
        );

        worker
            .dispatch_sync(WorkerCommand::Begin)
            .expect("begin marks worker in transaction without socket I/O");
        assert!(worker.is_in_transaction());
        worker
            .dispatch_sync(WorkerCommand::Commit)
            .expect("commit clears transaction flag on unconnected session");
        assert!(!worker.is_in_transaction());
        worker.close_sync().expect("close worker");
        assert!(worker.is_closed());

        let break_handle = Arc::new(OracleBreakHandle::new());
        let cb_fired = Arc::new(AtomicBool::new(false));
        let cb_flag = Arc::clone(&cb_fired);
        break_handle.set_callback(Some(Arc::new(move || {
            cb_flag.store(true, Ordering::SeqCst);
        })));
        break_handle.begin_operation();
        let bh_for_cancel = Arc::clone(&break_handle);
        let canceller = thread::spawn(move || {
            thread::sleep(Duration::from_millis(25));
            bh_for_cancel.break_execution();
        });
        let res = break_handle.wait_or_cancelled(Duration::from_secs(2));
        canceller.join().expect("join canceller");
        assert!(cb_fired.load(Ordering::SeqCst));
        assert!(matches!(res, Err(OracleError::Server { code: 1013, .. })));
        break_handle.end_operation();
        assert!(!break_handle.is_cancelled());
    }

    #[test]
    fn statement_facts_classifies_queries_dml_plsql_and_session_context() {
        let select_facts = statement_facts("  /* c */ select 1 from dual");
        assert_eq!(
            select_facts,
            StatementFacts {
                kind: StatementKind::Query,
                touches_session_context: false,
            }
        );

        let with_facts = statement_facts("WITH x AS (SELECT 1 FROM dual) SELECT * FROM x");
        assert_eq!(
            with_facts,
            StatementFacts {
                kind: StatementKind::Query,
                touches_session_context: false,
            }
        );

        let plsql_facts = statement_facts("BEGIN NULL; END;");
        assert_eq!(
            plsql_facts,
            StatementFacts {
                kind: StatementKind::DmlOrPlsql,
                touches_session_context: false,
            }
        );

        let dml_facts = statement_facts("insert into t values (1)");
        assert_eq!(
            dml_facts,
            StatementFacts {
                kind: StatementKind::DmlOrPlsql,
                touches_session_context: false,
            }
        );

        let session_facts = statement_facts("BEGIN DBMS_SESSION.SET_IDENTIFIER(:value); END;");
        assert_eq!(
            session_facts,
            StatementFacts {
                kind: StatementKind::DmlOrPlsql,
                touches_session_context: true,
            }
        );

        let clear_facts = statement_facts("BEGIN clear_identifier; END;");
        assert!(clear_facts.touches_session_context);
    }

    #[test]
    fn statement_facts_lru_caches_without_rescan_and_evicts_at_capacity() {
        let mut lru: PlanLruCache<StatementFacts> = PlanLruCache::new(DEFAULT_PLAN_CACHE_CAPACITY);
        let sql = "SELECT 42 FROM dual";
        let first = lru.insert(sql.into(), compute_statement_facts(sql));
        let second = lru.get(sql).expect("cache hit");
        assert_eq!(first, second);
        assert_eq!(first.kind, StatementKind::Query);
        assert_eq!(lru.len(), 1);

        let mut small: PlanLruCache<StatementFacts> = PlanLruCache::new(2);
        small.insert(
            "SELECT 1 FROM dual".into(),
            compute_statement_facts("SELECT 1 FROM dual"),
        );
        small.insert(
            "SELECT 2 FROM dual".into(),
            compute_statement_facts("SELECT 2 FROM dual"),
        );
        assert!(small.get("SELECT 1 FROM dual").is_some());
        small.insert(
            "SELECT 3 FROM dual".into(),
            compute_statement_facts("SELECT 3 FROM dual"),
        );
        assert_eq!(small.len(), 2);
        assert!(small.get("SELECT 1 FROM dual").is_some());
        assert!(small.get("SELECT 2 FROM dual").is_none());
        assert!(small.get("SELECT 3 FROM dual").is_some());
    }

    fn spawn_unconnected_worker() -> OracleWorkerHandle {
        let cfg = NativeOracleConfig::validate(RawOracleOptions {
            host: "localhost".to_owned(),
            ..Default::default()
        })
        .expect("valid oracle config");
        OracleWorkerHandle::spawn(cfg).expect("zero-I/O worker spawn")
    }

    #[test]
    fn reset_with_timeout_skips_rollback_on_clean_worker() {
        let worker = spawn_unconnected_worker();
        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("reset on fresh worker succeeds without round trip");
        assert_eq!(worker.reset_rollbacks(), 0);
        assert!(!worker.is_in_transaction());
        worker.close_sync().expect("close worker");
    }

    #[test]
    fn reset_with_timeout_rolls_back_abandoned_transaction_and_clears_dirty_state() {
        let worker = spawn_unconnected_worker();
        worker
            .dispatch_sync(WorkerCommand::Begin)
            .expect("begin marks worker in transaction");
        assert!(worker.is_in_transaction());

        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("reset rolls back abandoned transaction");
        assert_eq!(worker.reset_rollbacks(), 1);
        assert!(!worker.is_in_transaction());

        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("subsequent reset on now-clean worker skips rollback");
        assert_eq!(worker.reset_rollbacks(), 1);

        worker
            .session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .may_have_open_txn = true;
        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("reset rolls back when may_have_open_txn is set without explicit begin");
        assert_eq!(worker.reset_rollbacks(), 2);
        worker.close_sync().expect("close worker");
    }

    #[test]
    fn reset_with_timeout_skips_rollback_after_commit_or_rollback() {
        let worker = spawn_unconnected_worker();
        worker
            .dispatch_sync(WorkerCommand::Begin)
            .expect("begin transaction");
        worker
            .dispatch_sync(WorkerCommand::Commit)
            .expect("commit clears dirty transaction state");
        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("reset after commit skips rollback");
        assert_eq!(worker.reset_rollbacks(), 0);
        assert!(!worker.is_in_transaction());

        worker
            .dispatch_sync(WorkerCommand::Begin)
            .expect("begin second transaction");
        worker
            .dispatch_sync(WorkerCommand::Rollback)
            .expect("rollback clears dirty transaction state");
        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("reset after rollback skips rollback");
        assert_eq!(worker.reset_rollbacks(), 0);
        assert!(!worker.is_in_transaction());
        worker.close_sync().expect("close worker");
    }

    #[test]
    fn reset_with_timeout_runs_through_worker_channel_after_queued_commands() {
        let worker = spawn_unconnected_worker();
        let tx = worker
            .sender
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
            .expect("worker sender");
        let (reply_tx, reply_rx) = oneshot::channel();
        worker.break_handle.begin_operation();
        tx.send(WorkerEnvelope {
            cmd: WorkerCommand::Begin,
            reply: ReplyChannel::Async(reply_tx),
        })
        .expect("enqueue begin on worker channel");

        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("reset drains behind queued begin");
        assert!(reply_rx.blocking_recv().expect("begin reply").is_ok());
        assert_eq!(worker.reset_rollbacks(), 1);
        assert!(!worker.is_in_transaction());
        worker.close_sync().expect("close worker");
    }

    #[test]
    fn note_autocommit_clears_dirty_flag_only_when_statement_and_commit_succeed() {
        let worker = spawn_unconnected_worker();
        let cases = [
            (false, false, true),
            (false, true, true),
            (true, false, true),
            (true, true, false),
        ];
        let mut expected_rollbacks = 0usize;
        for (statement_ok, commit_ok, expected_dirty) in cases {
            {
                let mut session = worker.session.lock().unwrap_or_else(|e| e.into_inner());
                session.may_have_open_txn = true;
                session.note_autocommit(statement_ok, commit_ok);
                assert_eq!(
                    session.may_have_open_txn, expected_dirty,
                    "unexpected dirty state for statement_ok={statement_ok}, commit_ok={commit_ok}"
                );
            }
            worker
                .reset_with_timeout(Duration::from_secs(2))
                .expect("reset after note_autocommit");
            if expected_dirty {
                expected_rollbacks += 1;
            }
            assert_eq!(worker.reset_rollbacks(), expected_rollbacks);
        }
        worker.close_sync().expect("close worker");
    }

    #[test]
    fn command_may_open_txn_classifies_every_worker_command() {
        assert!(!command_may_open_txn(&WorkerCommand::Connect));
        assert!(!command_may_open_txn(&WorkerCommand::Ping));
        assert!(!command_may_open_txn(&WorkerCommand::Commit));
        assert!(!command_may_open_txn(&WorkerCommand::Rollback));
        assert!(!command_may_open_txn(&WorkerCommand::Close));
        assert!(!command_may_open_txn(&WorkerCommand::ResetIfDirty));

        assert!(command_may_open_txn(&WorkerCommand::Begin));
        assert!(command_may_open_txn(&WorkerCommand::Savepoint(
            "sp1".to_owned()
        )));
        assert!(command_may_open_txn(&WorkerCommand::RollbackToSavepoint(
            "sp1".to_owned()
        )));
        assert!(command_may_open_txn(&WorkerCommand::Query {
            sql: "SELECT 1 FROM dual".to_owned(),
            params: BindArgs::Positional(Vec::new()),
            out_binds: OutBindSpec::None,
            fetch_decimals: false,
            lowercase_column_names: true,
            max_rows: None,
        }));
        assert!(command_may_open_txn(&WorkerCommand::Execute {
            sql: "INSERT INTO t VALUES (1)".to_owned(),
            params: BindArgs::Positional(Vec::new()),
            out_binds: OutBindSpec::None,
            fetch_decimals: false,
            lowercase_column_names: true,
        }));
        assert!(command_may_open_txn(&WorkerCommand::ExecuteMany {
            sql: "INSERT INTO t VALUES (:1)".to_owned(),
            params_seq: Vec::new(),
        }));
        assert!(command_may_open_txn(&WorkerCommand::QueryArrow {
            sql: "SELECT 1 FROM dual".to_owned(),
            params: BindArgs::Positional(Vec::new()),
            batch_size: 1024,
            lowercase_column_names: true,
        }));
        assert!(command_may_open_txn(&WorkerCommand::PipelineBatch {
            ops: Vec::new(),
            continue_on_error: false,
        }));
    }

    #[test]
    fn reset_with_timeout_rolls_back_after_async_begin() {
        let worker = Arc::new(spawn_unconnected_worker());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");

        rt.block_on(async {
            worker
                .dispatch_async_raw(WorkerCommand::Begin)
                .await
                .expect("async begin succeeds");
        });
        assert!(worker.is_in_transaction());

        worker
            .reset_with_timeout(Duration::from_secs(2))
            .expect("reset rolls back after async begin");
        assert_eq!(worker.reset_rollbacks(), 1);
        assert!(!worker.is_in_transaction());

        worker.close_sync().expect("close worker");
    }

    #[test]
    fn reset_with_timeout_rolls_back_behind_queued_async_command() {
        use std::future::Future;
        use std::task::{Context, Poll, Waker};

        let worker = Arc::new(spawn_unconnected_worker());
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime");

        rt.block_on(async {
            let mut begin_fut = Box::pin(worker.dispatch_async_raw(WorkerCommand::Begin));
            let mut cx = Context::from_waker(Waker::noop());
            let initial_poll = begin_fut.as_mut().poll(&mut cx);

            let worker_for_reset = Arc::clone(&worker);
            tokio::task::spawn_blocking(move || {
                worker_for_reset.reset_with_timeout(Duration::from_secs(2))
            })
            .await
            .expect("blocking reset task joined")
            .expect("reset drains behind queued async begin");

            assert_eq!(worker.reset_rollbacks(), 1);
            assert!(!worker.is_in_transaction());

            if matches!(initial_poll, Poll::Pending) {
                begin_fut.await.expect("queued async begin completed");
            }
        });

        worker.close_sync().expect("close worker");
    }
}
