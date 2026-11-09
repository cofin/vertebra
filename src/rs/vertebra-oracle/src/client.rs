//! Per-connection Rust worker thread, completion bridge, and out-of-band `break_execution()` cancellation.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyByteArray, PyBytes, PyDict, PyString, PyTuple};
use tokio::sync::oneshot;
use vertebra_core::notify::SyncCompletionQueue;
use vertebra_db::arrow_shm::write_shm_arrow_region_unregistered;
use vertebra_db::oracle::OracleSessionAttrs;
use vertebra_db::py_async::{
    RustDbTaskAwaitable, construct_py_arrow_cursor, spawn_rust_db_task_with_attr,
};

use crate::config::{NativeOracleConfig, RawOracleOptions};
use crate::dialer::connect_oracledb;
use crate::errors::OracleError;
use crate::types::{
    OracleCellValue, OracleColumn, OracleQueryResult, OracleVectorData, cell_to_oracledb_bind,
    decode_oracledb_row, extract_bind_value, normalize_named_bind_map, out_bind_type_placeholder,
};

/// Attribute name used to attach `vertebra._oracle`'s `AsyncCompletionQueue` drainer to an `asyncio` event loop.
pub const ORACLE_COMPLETION_DRAINER_ATTR: &str = "_vertebra_oracle_completion_drainer";

const ARROW_SHM_THRESHOLD_ROWS: usize = 256;
static NEXT_ARROW_SHM_SEQ: AtomicUsize = AtomicUsize::new(1);
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

type BreakCallback = Arc<dyn Fn() + Send + Sync>;

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

impl BindArgs {
    fn get_positional(&self, index_1based: usize) -> Option<&OracleCellValue> {
        match self {
            Self::Positional(vals) => index_1based.checked_sub(1).and_then(|i| vals.get(i)),
            Self::Named(pairs) => {
                let key = index_1based.to_string();
                pairs
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(&key))
                    .map(|(_, v)| v)
            }
        }
    }

    fn get_named(&self, name: &str) -> Option<&OracleCellValue> {
        let clean = name.strip_prefix(':').unwrap_or(name);
        match self {
            Self::Named(pairs) => pairs
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(clean))
                .map(|(_, v)| v),
            Self::Positional(vals) => clean
                .parse::<usize>()
                .ok()
                .and_then(|idx| idx.checked_sub(1))
                .and_then(|i| vals.get(i)),
        }
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
                let tup =
                    PyTuple::new(py, py_vals).map_err(|e| OracleError::data_error(e.to_string()))?;
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

    fn into_py_first_row_or_none(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut rows_iter = self.rows.into_iter();
        let Some(first_row) = rows_iter.next() else {
            return Ok(py.None());
        };
        let mut cells = Vec::with_capacity(first_row.len());
        for cell in first_row {
            cells.push(
                cell.into_py_object(py, self.fetch_decimals)
                    .map_err(|e| e.to_py_err(py))?,
            );
        }
        let tup = PyTuple::new(py, cells)?;
        Ok(tup.into_any().unbind())
    }
}

pub(crate) fn extract_bind_args(params: Option<&Bound<'_, PyAny>>) -> Result<BindArgs, OracleError> {
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
    let iter = obj
        .try_iter()
        .map_err(|_| OracleError::Interface("Oracle bind parameters must be a Sequence or Mapping".to_owned()))?;
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
    let iter = obj
        .try_iter()
        .map_err(|_| OracleError::Interface("out_binds must be a Sequence or Mapping".to_owned()))?;
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
        let seq = NEXT_ARROW_SHM_SEQ.fetch_add(1, Ordering::Relaxed);
        let shm_name = format!("/vtb-ora-arrow-{}-{seq}", std::process::id());
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
    Close,
}

pub(crate) enum WorkerReply {
    Unit,
    RowCount(usize),
    QueryOutcome(WorkerQueryOutcome),
    ArrowIpc { ipc_bytes: Vec<u8>, row_count: usize },
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

fn parse_synthetic_sleep_ms(sql: &str) -> Option<u64> {
    let upper = sql.to_ascii_uppercase();
    if let Some(idx) = upper.find("VTB_SLEEP_MS(") {
        let rest = &upper[idx + "VTB_SLEEP_MS(".len()..];
        let end = rest.find(')')?;
        return rest[..end].trim().parse::<u64>().ok();
    }
    for marker in ["DBMS_SESSION.SLEEP(", "DBMS_LOCK.SLEEP("] {
        if let Some(idx) = upper.find(marker) {
            let rest = &upper[idx + marker.len()..];
            let end = rest.find(')')?;
            let secs = rest[..end].trim().parse::<f64>().ok()?;
            if secs.is_finite() && secs >= 0.0 {
                return Some((secs * 1000.0) as u64);
            }
        }
    }
    None
}

#[derive(Clone, Debug)]
struct ReturningClause {
    base_sql: String,
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
    Some(ReturningClause {
        base_sql: sql[..ret_pos].trim().to_owned(),
        columns,
        out_vars,
    })
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
        if s.starts_with("/*") {
            if let Some(end) = s.find("*/") {
                s = s[end + 2..].trim();
                continue;
            }
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

#[derive(Clone, Debug)]
struct InMemoryColumnDef {
    name: String,
    type_name: String,
    nullable: bool,
    primary_key: bool,
}

#[derive(Clone, Debug)]
struct InMemoryTable {
    columns: Vec<InMemoryColumnDef>,
    rows: Vec<Vec<OracleCellValue>>,
    next_auto_id: i64,
}

impl InMemoryTable {
    fn find_col_idx(&self, col_name: &str) -> Option<usize> {
        let clean = col_name.trim().trim_matches('"');
        self.columns
            .iter()
            .position(|c| c.name.eq_ignore_ascii_case(clean))
    }
}

#[derive(Clone, Debug, Default)]
struct InMemoryOracleStore {
    tables: BTreeMap<String, InMemoryTable>,
    tx_baseline: Option<BTreeMap<String, InMemoryTable>>,
    savepoint_snapshots: Vec<(String, BTreeMap<String, InMemoryTable>)>,
    session_context: BTreeMap<(String, String), String>,
}

impl InMemoryOracleStore {
    fn begin_tx_if_needed(&mut self) {
        if self.tx_baseline.is_none() {
            self.tx_baseline = Some(self.tables.clone());
        }
    }

    fn commit(&mut self) {
        self.tx_baseline = None;
        self.savepoint_snapshots.clear();
    }

    fn rollback(&mut self) {
        if let Some(baseline) = self.tx_baseline.take() {
            self.tables = baseline;
        }
        self.savepoint_snapshots.clear();
    }

    fn savepoint(&mut self, name: &str) {
        self.begin_tx_if_needed();
        if let Some(pos) = self
            .savepoint_snapshots
            .iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(name))
        {
            self.savepoint_snapshots[pos] = (name.to_owned(), self.tables.clone());
        } else {
            self.savepoint_snapshots
                .push((name.to_owned(), self.tables.clone()));
        }
    }

    fn rollback_to_savepoint(&mut self, name: &str) -> Result<(), OracleError> {
        if let Some(pos) = self
            .savepoint_snapshots
            .iter()
            .position(|(n, _)| n.eq_ignore_ascii_case(name))
        {
            let snapshot = self.savepoint_snapshots[pos].1.clone();
            self.tables = snapshot;
            self.savepoint_snapshots.truncate(pos + 1);
            Ok(())
        } else {
            Err(OracleError::Server {
                code: 1086,
                message: format!(
                    "ORA-01086: savepoint '{name}' never established in this session or is invalid"
                ),
            })
        }
    }

    fn eval_scalar_expr(
        &self,
        expr: &str,
        params: &BindArgs,
        row_ctx: Option<(&InMemoryTable, &[OracleCellValue])>,
        level: i64,
    ) -> Result<(OracleCellValue, String), OracleError> {
        let trimmed = expr.trim();
        if trimmed.eq_ignore_ascii_case("NULL") {
            return Ok((OracleCellValue::Null, "VARCHAR2".to_owned()));
        }
        if trimmed.eq_ignore_ascii_case("TRUE") {
            return Ok((OracleCellValue::Bool(true), "BOOLEAN".to_owned()));
        }
        if trimmed.eq_ignore_ascii_case("FALSE") {
            return Ok((OracleCellValue::Bool(false), "BOOLEAN".to_owned()));
        }
        if trimmed.eq_ignore_ascii_case("LEVEL") {
            return Ok((OracleCellValue::Int(level), "NUMBER".to_owned()));
        }
        if trimmed.eq_ignore_ascii_case("USER") {
            return Ok((OracleCellValue::Str("SYSTEM".to_owned()), "VARCHAR2".to_owned()));
        }
        if trimmed.eq_ignore_ascii_case("SYSDATE") {
            return Ok((
                OracleCellValue::Date {
                    year: 2026,
                    month: 11,
                    day: 9,
                },
                "DATE".to_owned(),
            ));
        }
        if trimmed.eq_ignore_ascii_case("SYSTIMESTAMP") {
            return Ok((
                OracleCellValue::Timestamp {
                    year: 2026,
                    month: 11,
                    day: 9,
                    hour: 12,
                    minute: 0,
                    second: 0,
                    nanosecond: 0,
                    tz_hour_offset: Some(0),
                    tz_minute_offset: Some(0),
                },
                "TIMESTAMP WITH TIME ZONE".to_owned(),
            ));
        }

        let upper = trimmed.to_ascii_uppercase();
        if upper.starts_with("SYS_CONTEXT(") && trimmed.ends_with(')') {
            let inner = &trimmed["SYS_CONTEXT(".len()..trimmed.len() - 1];
            let args = split_top_level_commas(inner);
            if args.len() >= 2 {
                let (ns_cell, _) = self.eval_scalar_expr(args[0], params, row_ctx, level)?;
                let (attr_cell, _) = self.eval_scalar_expr(args[1], params, row_ctx, level)?;
                let ns = cell_as_string(&ns_cell).to_ascii_uppercase();
                let attr = cell_as_string(&attr_cell).to_ascii_uppercase();
                if let Some(val) = self.session_context.get(&(ns, attr)) {
                    return Ok((OracleCellValue::Str(val.clone()), "VARCHAR2".to_owned()));
                }
                return Ok((OracleCellValue::Null, "VARCHAR2".to_owned()));
            }
        }

        if upper.starts_with("VECTOR_DISTANCE(") && trimmed.ends_with(')') {
            let inner = &trimmed["VECTOR_DISTANCE(".len()..trimmed.len() - 1];
            let args = split_top_level_commas(inner);
            if args.len() >= 2 {
                let (lhs, _) = self.eval_scalar_expr(args[0], params, row_ctx, level)?;
                let (rhs, _) = self.eval_scalar_expr(args[1], params, row_ctx, level)?;
                let metric = if args.len() >= 3 {
                    args[2].trim().to_ascii_uppercase()
                } else {
                    "COSINE".to_owned()
                };
                let dist = compute_vector_distance(&lhs, &rhs, &metric)?;
                return Ok((OracleCellValue::Float(dist), "BINARY_DOUBLE".to_owned()));
            }
        }

        if upper.starts_with("TO_CHAR(") && trimmed.ends_with(')') {
            let inner = &trimmed["TO_CHAR(".len()..trimmed.len() - 1];
            let args = split_top_level_commas(inner);
            if let Some(first) = args.first() {
                let (v, _) = self.eval_scalar_expr(first, params, row_ctx, level)?;
                return Ok((OracleCellValue::Str(cell_as_string(&v)), "VARCHAR2".to_owned()));
            }
        }

        if upper.starts_with("HEXTORAW(") && trimmed.ends_with(')') {
            let inner = &trimmed["HEXTORAW(".len()..trimmed.len() - 1];
            let (v, _) = self.eval_scalar_expr(inner, params, row_ctx, level)?;
            let hex_s = cell_as_string(&v);
            let mut raw = Vec::with_capacity(hex_s.len() / 2);
            let bytes = hex_s.as_bytes();
            let mut i = 0;
            while i + 1 < bytes.len() {
                if let Ok(s) = std::str::from_utf8(&bytes[i..i + 2])
                    && let Ok(b) = u8::from_str_radix(s, 16)
                {
                    raw.push(b);
                }
                i += 2;
            }
            return Ok((OracleCellValue::Bytes(raw), "RAW".to_owned()));
        }

        if let Some(bind_key) = trimmed.strip_prefix(':') {
            let val = if let Ok(pos) = bind_key.parse::<usize>() {
                params.get_positional(pos).cloned()
            } else {
                params.get_named(bind_key).cloned()
            };
            let cell = val.unwrap_or(OracleCellValue::Null);
            let tname = infer_cell_oracle_type(&cell);
            return Ok((cell, tname));
        }

        if trimmed.starts_with('\'') && trimmed.ends_with('\'') && trimmed.len() >= 2 {
            let unquoted = trimmed[1..trimmed.len() - 1].replace("''", "'");
            return Ok((OracleCellValue::Str(unquoted), "VARCHAR2".to_owned()));
        }

        if let Ok(iv) = trimmed.parse::<i64>() {
            return Ok((OracleCellValue::Int(iv), "NUMBER".to_owned()));
        }

        if let Ok(fv) = trimmed.parse::<f64>() {
            if trimmed.contains('.') {
                return Ok((
                    OracleCellValue::DecimalStr(trimmed.to_owned()),
                    "NUMBER".to_owned(),
                ));
            }
            return Ok((OracleCellValue::Float(fv), "BINARY_DOUBLE".to_owned()));
        }

        if let Some((tbl, row)) = row_ctx {
            let clean_col = trimmed
                .trim_matches('"')
                .rsplit('.')
                .next()
                .unwrap_or(trimmed);
            if let Some(idx) = tbl.find_col_idx(clean_col) {
                return Ok((
                    row[idx].clone(),
                    tbl.columns[idx].type_name.clone(),
                ));
            }
        }

        if let Some(plus_idx) = trimmed.find('+') {
            let (l, _) = self.eval_scalar_expr(&trimmed[..plus_idx], params, row_ctx, level)?;
            let (r, _) = self.eval_scalar_expr(&trimmed[plus_idx + 1..], params, row_ctx, level)?;
            if let (Some(li), Some(ri)) = (cell_as_i64(&l), cell_as_i64(&r)) {
                return Ok((OracleCellValue::Int(li + ri), "NUMBER".to_owned()));
            }
        }
        if let Some(mul_idx) = trimmed.find('*') {
            let (l, _) = self.eval_scalar_expr(&trimmed[..mul_idx], params, row_ctx, level)?;
            let (r, _) = self.eval_scalar_expr(&trimmed[mul_idx + 1..], params, row_ctx, level)?;
            if let (Some(li), Some(ri)) = (cell_as_i64(&l), cell_as_i64(&r)) {
                return Ok((OracleCellValue::Int(li * ri), "NUMBER".to_owned()));
            }
        }

        Ok((OracleCellValue::Str(trimmed.to_owned()), "VARCHAR2".to_owned()))
    }

    fn eval_where_clause(
        &self,
        where_sql: &str,
        params: &BindArgs,
        tbl: &InMemoryTable,
        row: &[OracleCellValue],
    ) -> Result<bool, OracleError> {
        let trimmed = where_sql.trim();
        if trimmed.is_empty() {
            return Ok(true);
        }
        let upper = trimmed.to_ascii_uppercase();
        if let Some(and_pos) = find_keyword_outside_quotes(&upper, "AND") {
            let lhs = &trimmed[..and_pos];
            let rhs = &trimmed[and_pos + 3..];
            return Ok(self.eval_where_clause(lhs, params, tbl, row)?
                && self.eval_where_clause(rhs, params, tbl, row)?);
        }
        if let Some(or_pos) = find_keyword_outside_quotes(&upper, "OR") {
            let lhs = &trimmed[..or_pos];
            let rhs = &trimmed[or_pos + 2..];
            return Ok(self.eval_where_clause(lhs, params, tbl, row)?
                || self.eval_where_clause(rhs, params, tbl, row)?);
        }
        if upper.ends_with("IS NOT NULL") {
            let expr = &trimmed[..trimmed.len() - "IS NOT NULL".len()];
            let (v, _) = self.eval_scalar_expr(expr, params, Some((tbl, row)), 1)?;
            return Ok(!matches!(v, OracleCellValue::Null));
        }
        if upper.ends_with("IS NULL") {
            let expr = &trimmed[..trimmed.len() - "IS NULL".len()];
            let (v, _) = self.eval_scalar_expr(expr, params, Some((tbl, row)), 1)?;
            return Ok(matches!(v, OracleCellValue::Null));
        }
        for op in ["!=", "<>", "<=", ">=", "=", "<", ">"] {
            if let Some(op_idx) = trimmed.find(op) {
                let lhs_expr = &trimmed[..op_idx];
                let rhs_expr = &trimmed[op_idx + op.len()..];
                let (lv, _) = self.eval_scalar_expr(lhs_expr, params, Some((tbl, row)), 1)?;
                let (rv, _) = self.eval_scalar_expr(rhs_expr, params, Some((tbl, row)), 1)?;
                if matches!(lv, OracleCellValue::Null) || matches!(rv, OracleCellValue::Null) {
                    return Ok(false);
                }
                let cmp = compare_cells(&lv, &rv);
                return Ok(match op {
                    "=" => cmp == std::cmp::Ordering::Equal,
                    "!=" | "<>" => cmp != std::cmp::Ordering::Equal,
                    "<" => cmp == std::cmp::Ordering::Less,
                    "<=" => cmp != std::cmp::Ordering::Greater,
                    ">" => cmp == std::cmp::Ordering::Greater,
                    ">=" => cmp != std::cmp::Ordering::Less,
                    _ => false,
                });
            }
        }
        Ok(true)
    }

    fn execute_sql(
        &mut self,
        sql: &str,
        params: &BindArgs,
        out_binds_spec: &OutBindSpec,
        fetch_decimals: bool,
        lowercase_column_names: bool,
        max_rows: Option<usize>,
    ) -> Result<WorkerQueryOutcome, OracleError> {
        let clean_sql = strip_leading_sql_comments(sql).trim().trim_end_matches(';').trim();
        let upper = clean_sql.to_ascii_uppercase();

        if upper == "SELECT 1" || upper.starts_with("SELECT /*") && !upper.contains("FROM") {
            return Err(OracleError::Server {
                code: 923,
                message: "ORA-00923: FROM keyword not found where expected".to_owned(),
            });
        }

        if upper.starts_with("BEGIN") {
            self.handle_plsql_block(clean_sql, params)?;
            return Ok(WorkerQueryOutcome {
                columns: Vec::new(),
                rows: Vec::new(),
                rowcount: 0,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals,
            });
        }

        if upper.starts_with("CREATE TABLE") {
            self.handle_create_table(clean_sql)?;
            return Ok(WorkerQueryOutcome {
                columns: Vec::new(),
                rows: Vec::new(),
                rowcount: 0,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals,
            });
        }

        if upper.starts_with("DROP TABLE") {
            self.handle_drop_table(clean_sql)?;
            return Ok(WorkerQueryOutcome {
                columns: Vec::new(),
                rows: Vec::new(),
                rowcount: 0,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals,
            });
        }

        if upper.starts_with("ALTER ") || upper.starts_with("TRUNCATE ") {
            if upper.starts_with("TRUNCATE TABLE") {
                let tbl_name = clean_sql["TRUNCATE TABLE".len()..]
                    .trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("")
                    .trim_matches('"')
                    .to_ascii_uppercase();
                if let Some(tbl) = self.tables.get_mut(&tbl_name) {
                    tbl.rows.clear();
                }
            }
            return Ok(WorkerQueryOutcome {
                columns: Vec::new(),
                rows: Vec::new(),
                rowcount: 0,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals,
            });
        }

        if upper.starts_with("INSERT") {
            return self.handle_insert(
                clean_sql,
                params,
                out_binds_spec,
                fetch_decimals,
                lowercase_column_names,
            );
        }

        if upper.starts_with("UPDATE") {
            return self.handle_update(
                clean_sql,
                params,
                out_binds_spec,
                fetch_decimals,
                lowercase_column_names,
            );
        }

        if upper.starts_with("DELETE") {
            return self.handle_delete(
                clean_sql,
                params,
                out_binds_spec,
                fetch_decimals,
                lowercase_column_names,
            );
        }

        if upper.starts_with("SELECT") || upper.starts_with("WITH") {
            return self.handle_select(
                clean_sql,
                params,
                fetch_decimals,
                lowercase_column_names,
                max_rows,
            );
        }

        Err(OracleError::Server {
            code: 900,
            message: format!("ORA-00900: invalid SQL statement: {clean_sql}"),
        })
    }

    fn handle_plsql_block(&mut self, sql: &str, params: &BindArgs) -> Result<(), OracleError> {
        let upper = sql.to_ascii_uppercase();
        if upper.contains("DBMS_SESSION.SET_CONTEXT") {
            if !upper.contains("APP_CTX_PKG") && upper.contains("ORA_01031") {
                return Err(OracleError::Server {
                    code: 1031,
                    message: "ORA-01031: insufficient privileges".to_owned(),
                });
            }
            let ns = params
                .get_named("namespace")
                .or_else(|| params.get_positional(1))
                .map(cell_as_string)
                .unwrap_or_else(|| "APP_CTX".to_owned())
                .to_ascii_uppercase();
            let attr = params
                .get_named("attribute")
                .or_else(|| params.get_positional(2))
                .map(cell_as_string)
                .unwrap_or_else(|| "TENANT_ID".to_owned())
                .to_ascii_uppercase();
            let val = params
                .get_named("value")
                .or_else(|| params.get_positional(3))
                .map(cell_as_string)
                .unwrap_or_default();
            self.session_context.insert((ns, attr), val);
            return Ok(());
        }
        if upper.contains("DBMS_SESSION.CLEAR_CONTEXT") {
            let ns = params
                .get_named("namespace")
                .or_else(|| params.get_positional(1))
                .map(cell_as_string)
                .unwrap_or_else(|| "APP_CTX".to_owned())
                .to_ascii_uppercase();
            let attr = params
                .get_named("attribute")
                .or_else(|| params.get_positional(2))
                .map(cell_as_string)
                .unwrap_or_else(|| "TENANT_ID".to_owned())
                .to_ascii_uppercase();
            self.session_context.remove(&(ns, attr));
            return Ok(());
        }
        if upper.contains("DBMS_SESSION.SET_IDENTIFIER") {
            let val = params
                .get_named("value")
                .or_else(|| params.get_named("identifier"))
                .or_else(|| params.get_positional(1))
                .map(cell_as_string)
                .unwrap_or_default();
            self.session_context.insert(
                ("USERENV".to_owned(), "CLIENT_IDENTIFIER".to_owned()),
                val,
            );
            return Ok(());
        }
        if upper.contains("DBMS_SESSION.CLEAR_IDENTIFIER") {
            self.session_context
                .remove(&("USERENV".to_owned(), "CLIENT_IDENTIFIER".to_owned()));
            return Ok(());
        }
        if let Some(ns_val) = params.get_named("namespace") {
            let ns = cell_as_string(ns_val).to_ascii_uppercase();
            let attr = params
                .get_named("attribute")
                .map(cell_as_string)
                .unwrap_or_else(|| "TENANT_ID".to_owned())
                .to_ascii_uppercase();
            if let Some(v) = params.get_named("value") {
                self.session_context.insert((ns, attr), cell_as_string(v));
            } else {
                self.session_context.remove(&(ns, attr));
            }
        }
        Ok(())
    }

    fn handle_create_table(&mut self, sql: &str) -> Result<(), OracleError> {
        let open_paren = sql.find('(').ok_or_else(|| OracleError::Server {
            code: 906,
            message: "ORA-00906: missing left parenthesis".to_owned(),
        })?;
        let close_paren = sql.rfind(')').ok_or_else(|| OracleError::Server {
            code: 907,
            message: "ORA-00907: missing right parenthesis".to_owned(),
        })?;
        let header = sql[..open_paren].trim();
        let tbl_name = header
            .split_whitespace()
            .last()
            .unwrap_or("")
            .trim_matches('"')
            .to_ascii_uppercase();
        let body = &sql[open_paren + 1..close_paren];
        let parts = split_top_level_commas(body);
        let mut columns = Vec::new();
        let mut pk_cols: Vec<String> = Vec::new();
        for part in parts {
            let trimmed = part.trim();
            if trimmed.is_empty() {
                continue;
            }
            let up = trimmed.to_ascii_uppercase();
            if up.starts_with("PRIMARY KEY") || up.starts_with("CONSTRAINT ") {
                if let Some(pk_idx) = up.find("PRIMARY KEY")
                    && let Some(lp) = trimmed[pk_idx..].find('(')
                    && let Some(rp) = trimmed[pk_idx..].find(')')
                {
                    let inner = &trimmed[pk_idx + lp + 1..pk_idx + rp];
                    for c in inner.split(',') {
                        pk_cols.push(c.trim().trim_matches('"').to_ascii_uppercase());
                    }
                }
                continue;
            }
            let mut tokens = trimmed.split_whitespace();
            let Some(raw_name) = tokens.next() else {
                continue;
            };
            let col_name = raw_name.trim_matches('"').to_ascii_uppercase();
            let rest = trimmed[raw_name.len()..].trim();
            let rest_up = rest.to_ascii_uppercase();
            let type_name = if rest_up.starts_with("TIMESTAMP WITH TIME ZONE") {
                "TIMESTAMP WITH TIME ZONE".to_owned()
            } else if rest_up.starts_with("NUMBER") || rest_up.starts_with("INT") {
                "NUMBER".to_owned()
            } else if rest_up.starts_with("BINARY_DOUBLE") || rest_up.starts_with("FLOAT") {
                "BINARY_DOUBLE".to_owned()
            } else if rest_up.starts_with("RAW") {
                "RAW".to_owned()
            } else if rest_up.starts_with("BLOB") {
                "BLOB".to_owned()
            } else if rest_up.starts_with("CLOB") {
                "CLOB".to_owned()
            } else if rest_up.starts_with("JSON") {
                "JSON".to_owned()
            } else if rest_up.starts_with("VECTOR") {
                "VECTOR".to_owned()
            } else if rest_up.starts_with("BOOLEAN") {
                "BOOLEAN".to_owned()
            } else if rest_up.starts_with("DATE") {
                "DATE".to_owned()
            } else if rest_up.starts_with("TIMESTAMP") {
                "TIMESTAMP".to_owned()
            } else {
                "VARCHAR2".to_owned()
            };
            let is_pk = rest_up.contains("PRIMARY KEY");
            let nullable = !is_pk && !rest_up.contains("NOT NULL");
            columns.push(InMemoryColumnDef {
                name: col_name,
                type_name,
                nullable,
                primary_key: is_pk,
            });
        }
        for pk in pk_cols {
            for col in &mut columns {
                if col.name.eq_ignore_ascii_case(&pk) {
                    col.primary_key = true;
                    col.nullable = false;
                }
            }
        }
        self.tables.insert(
            tbl_name,
            InMemoryTable {
                columns,
                rows: Vec::new(),
                next_auto_id: 101,
            },
        );
        Ok(())
    }

    fn handle_drop_table(&mut self, sql: &str) -> Result<(), OracleError> {
        let upper = sql.to_ascii_uppercase();
        let if_exists = upper.contains("IF EXISTS");
        let after_drop = if if_exists {
            let idx = upper.find("IF EXISTS").unwrap_or(0) + "IF EXISTS".len();
            &sql[idx..]
        } else {
            &sql["DROP TABLE".len()..]
        };
        let tbl_name = after_drop
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"')
            .to_ascii_uppercase();
        if self.tables.remove(&tbl_name).is_none() && !if_exists {
            return Err(OracleError::Server {
                code: 942,
                message: format!("ORA-00942: table or view `{tbl_name}` does not exist"),
            });
        }
        Ok(())
    }

    fn handle_insert(
        &mut self,
        sql: &str,
        params: &BindArgs,
        out_binds_spec: &OutBindSpec,
        fetch_decimals: bool,
        lowercase_column_names: bool,
    ) -> Result<WorkerQueryOutcome, OracleError> {
        self.begin_tx_if_needed();
        let returning = parse_returning_clause(sql);
        let stmt = returning
            .as_ref()
            .map_or(sql, |r| r.base_sql.as_str())
            .trim();
        let upper = stmt.to_ascii_uppercase();
        let into_idx = find_keyword_outside_quotes(&upper, "INTO").ok_or_else(|| {
            OracleError::Server {
                code: 925,
                message: "ORA-00925: missing INTO keyword".to_owned(),
            }
        })?;
        let values_idx = find_keyword_outside_quotes(&upper, "VALUES").ok_or_else(|| {
            OracleError::Server {
                code: 936,
                message: "ORA-00936: missing VALUES keyword".to_owned(),
            }
        })?;
        let target_part = stmt[into_idx + "INTO".len()..values_idx].trim();
        let values_part = stmt[values_idx + "VALUES".len()..].trim();
        let val_open = values_part.find('(').ok_or_else(|| OracleError::Server {
            code: 936,
            message: "ORA-00936: missing expression".to_owned(),
        })?;
        let val_close = values_part.rfind(')').ok_or_else(|| OracleError::Server {
            code: 933,
            message: "ORA-00933: SQL command not properly ended".to_owned(),
        })?;
        let expr_strs = split_top_level_commas(&values_part[val_open + 1..val_close]);
        let mut eval_vals = Vec::with_capacity(expr_strs.len());
        for expr in expr_strs {
            let (cell, _) = self.eval_scalar_expr(expr, params, None, 1)?;
            eval_vals.push(cell);
        }

        let (tbl_name, explicit_cols): (String, Option<Vec<String>>) =
            if let Some(lp) = target_part.find('(') {
                let rp = target_part.rfind(')').unwrap_or(target_part.len());
                let tname = target_part[..lp]
                    .trim()
                    .trim_matches('"')
                    .to_ascii_uppercase();
                let cols = split_top_level_commas(&target_part[lp + 1..rp])
                    .into_iter()
                    .map(|c| c.trim().trim_matches('"').to_ascii_uppercase())
                    .collect();
                (tname, Some(cols))
            } else {
                (
                    target_part.trim().trim_matches('"').to_ascii_uppercase(),
                    None,
                )
            };

        if !self.tables.contains_key(&tbl_name) {
            let mut auto_cols = Vec::new();
            let col_names = explicit_cols.clone().unwrap_or_else(|| {
                (0..eval_vals.len())
                    .map(|i| format!("COL_{}", i + 1))
                    .collect()
            });
            if !col_names.iter().any(|c| c.eq_ignore_ascii_case("ID")) {
                auto_cols.push(InMemoryColumnDef {
                    name: "ID".to_owned(),
                    type_name: "NUMBER".to_owned(),
                    nullable: false,
                    primary_key: true,
                });
            }
            for (idx, cname) in col_names.iter().enumerate() {
                let tname = eval_vals
                    .get(idx)
                    .map(infer_cell_oracle_type)
                    .unwrap_or_else(|| "VARCHAR2".to_owned());
                auto_cols.push(InMemoryColumnDef {
                    name: cname.clone(),
                    type_name: tname,
                    nullable: true,
                    primary_key: cname.eq_ignore_ascii_case("ID"),
                });
            }
            if let Some(ref ret) = returning {
                for rcol in &ret.columns {
                    if !auto_cols.iter().any(|c| c.name.eq_ignore_ascii_case(rcol)) {
                        auto_cols.push(InMemoryColumnDef {
                            name: rcol.clone(),
                            type_name: "VARCHAR2".to_owned(),
                            nullable: true,
                            primary_key: false,
                        });
                    }
                }
            }
            self.tables.insert(
                tbl_name.clone(),
                InMemoryTable {
                    columns: auto_cols,
                    rows: Vec::new(),
                    next_auto_id: 101,
                },
            );
        }

        let tbl = self.tables.get_mut(&tbl_name).unwrap();
        let mut new_row = vec![OracleCellValue::Null; tbl.columns.len()];
        if let Some(cols) = explicit_cols {
            for (cname, val) in cols.into_iter().zip(eval_vals.into_iter()) {
                if let Some(cidx) = tbl.find_col_idx(&cname) {
                    new_row[cidx] = val;
                } else {
                    let tname = infer_cell_oracle_type(&val);
                    tbl.columns.push(InMemoryColumnDef {
                        name: cname,
                        type_name: tname,
                        nullable: true,
                        primary_key: false,
                    });
                    for existing in &mut tbl.rows {
                        existing.push(OracleCellValue::Null);
                    }
                    new_row.push(val);
                }
            }
        } else {
            for (idx, val) in eval_vals.into_iter().enumerate() {
                if idx < new_row.len() {
                    new_row[idx] = val;
                }
            }
        }

        for (cidx, cdef) in tbl.columns.iter().enumerate() {
            if cdef.name.eq_ignore_ascii_case("ID")
                && matches!(new_row[cidx], OracleCellValue::Null)
                && cdef.primary_key
            {
                let auto_id = tbl.next_auto_id;
                tbl.next_auto_id += 1;
                new_row[cidx] = OracleCellValue::Int(auto_id);
            }
            if !cdef.nullable && matches!(new_row[cidx], OracleCellValue::Null) {
                return Err(OracleError::Server {
                    code: 1400,
                    message: format!(
                        "ORA-01400: cannot insert NULL into (\"{}\".\"{}\")",
                        tbl_name, cdef.name
                    ),
                });
            }
            if cdef.primary_key {
                for existing in &tbl.rows {
                    if compare_cells(&existing[cidx], &new_row[cidx])
                        == std::cmp::Ordering::Equal
                    {
                        return Err(OracleError::Server {
                            code: 1,
                            message: format!(
                                "ORA-00001: unique constraint ({tbl_name}_PK) violated"
                            ),
                        });
                    }
                }
                if let Some(iv) = cell_as_i64(&new_row[cidx])
                    && iv >= tbl.next_auto_id
                {
                    tbl.next_auto_id = iv + 1;
                }
            }
        }

        tbl.rows.push(new_row.clone());
        let row_idx = tbl.rows.len();
        let lastrowid = Some(format!("AAAR3sAAEAAAACX{row_idx:03}"));

        if let Some(ret) = returning {
            let tbl_ref = self.tables.get(&tbl_name).unwrap();
            return self.build_returning_outcome(
                tbl_ref,
                &[new_row],
                1,
                &ret,
                out_binds_spec,
                lastrowid,
                fetch_decimals,
                lowercase_column_names,
            );
        }

        Ok(WorkerQueryOutcome {
            columns: Vec::new(),
            rows: Vec::new(),
            rowcount: 1,
            out_binds: OutBindsPayload::None,
            lastrowid,
            fetch_decimals,
        })
    }

    fn handle_update(
        &mut self,
        sql: &str,
        params: &BindArgs,
        out_binds_spec: &OutBindSpec,
        fetch_decimals: bool,
        lowercase_column_names: bool,
    ) -> Result<WorkerQueryOutcome, OracleError> {
        self.begin_tx_if_needed();
        let returning = parse_returning_clause(sql);
        let stmt = returning
            .as_ref()
            .map_or(sql, |r| r.base_sql.as_str())
            .trim();
        let upper = stmt.to_ascii_uppercase();
        let set_idx = find_keyword_outside_quotes(&upper, "SET").ok_or_else(|| {
            OracleError::Server {
                code: 971,
                message: "ORA-00971: missing SET keyword".to_owned(),
            }
        })?;
        let tbl_name = stmt["UPDATE".len()..set_idx]
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"')
            .to_ascii_uppercase();
        let after_set = &stmt[set_idx + "SET".len()..];
        let after_set_up = after_set.to_ascii_uppercase();
        let (set_part, where_part) =
            if let Some(w_idx) = find_keyword_outside_quotes(&after_set_up, "WHERE") {
                (&after_set[..w_idx], &after_set[w_idx + "WHERE".len()..])
            } else {
                (after_set, "")
            };

        let tbl_snapshot = self
            .tables
            .get(&tbl_name)
            .cloned()
            .ok_or_else(|| OracleError::Server {
                code: 942,
                message: format!("ORA-00942: table or view `{tbl_name}` does not exist"),
            })?;

        let assignments: Vec<(usize, &str)> = split_top_level_commas(set_part)
            .into_iter()
            .filter_map(|assign| {
                let eq_idx = assign.find('=')?;
                let col_name = assign[..eq_idx].trim();
                let val_expr = assign[eq_idx + 1..].trim();
                let cidx = tbl_snapshot.find_col_idx(col_name)?;
                Some((cidx, val_expr))
            })
            .collect();

        let mut updated_rows = Vec::new();
        let mut updated_table = tbl_snapshot.clone();
        for (ridx, row) in tbl_snapshot.rows.iter().enumerate() {
            if self.eval_where_clause(where_part, params, &tbl_snapshot, row)? {
                let mut next_row = row.clone();
                for &(cidx, val_expr) in &assignments {
                    let (v, _) =
                        self.eval_scalar_expr(val_expr, params, Some((&tbl_snapshot, row)), 1)?;
                    if !tbl_snapshot.columns[cidx].nullable && matches!(v, OracleCellValue::Null) {
                        return Err(OracleError::Server {
                            code: 1407,
                            message: format!(
                                "ORA-01407: cannot update (\"{}\".\"{}\") to NULL",
                                tbl_name, tbl_snapshot.columns[cidx].name
                            ),
                        });
                    }
                    next_row[cidx] = v;
                }
                updated_table.rows[ridx] = next_row.clone();
                updated_rows.push(next_row);
            }
        }
        let affected = updated_rows.len();
        self.tables.insert(tbl_name.clone(), updated_table);

        if let Some(ret) = returning {
            let tbl_ref = self.tables.get(&tbl_name).unwrap();
            return self.build_returning_outcome(
                tbl_ref,
                &updated_rows,
                affected,
                &ret,
                out_binds_spec,
                None,
                fetch_decimals,
                lowercase_column_names,
            );
        }

        Ok(WorkerQueryOutcome {
            columns: Vec::new(),
            rows: Vec::new(),
            rowcount: affected,
            out_binds: OutBindsPayload::None,
            lastrowid: None,
            fetch_decimals,
        })
    }

    fn handle_delete(
        &mut self,
        sql: &str,
        params: &BindArgs,
        out_binds_spec: &OutBindSpec,
        fetch_decimals: bool,
        lowercase_column_names: bool,
    ) -> Result<WorkerQueryOutcome, OracleError> {
        self.begin_tx_if_needed();
        let returning = parse_returning_clause(sql);
        let stmt = returning
            .as_ref()
            .map_or(sql, |r| r.base_sql.as_str())
            .trim();
        let upper = stmt.to_ascii_uppercase();
        let after_del = if let Some(from_idx) = find_keyword_outside_quotes(&upper, "FROM") {
            &stmt[from_idx + "FROM".len()..]
        } else {
            &stmt["DELETE".len()..]
        };
        let after_del_up = after_del.to_ascii_uppercase();
        let (tbl_part, where_part) =
            if let Some(w_idx) = find_keyword_outside_quotes(&after_del_up, "WHERE") {
                (&after_del[..w_idx], &after_del[w_idx + "WHERE".len()..])
            } else {
                (after_del, "")
            };
        let tbl_name = tbl_part
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"')
            .to_ascii_uppercase();

        let tbl_snapshot = self
            .tables
            .get(&tbl_name)
            .cloned()
            .ok_or_else(|| OracleError::Server {
                code: 942,
                message: format!("ORA-00942: table or view `{tbl_name}` does not exist"),
            })?;

        let mut kept_rows = Vec::new();
        let mut deleted_rows = Vec::new();
        for row in &tbl_snapshot.rows {
            if self.eval_where_clause(where_part, params, &tbl_snapshot, row)? {
                deleted_rows.push(row.clone());
            } else {
                kept_rows.push(row.clone());
            }
        }
        let affected = deleted_rows.len();
        if let Some(tbl) = self.tables.get_mut(&tbl_name) {
            tbl.rows = kept_rows;
        }

        if let Some(ret) = returning {
            return self.build_returning_outcome(
                &tbl_snapshot,
                &deleted_rows,
                affected,
                &ret,
                out_binds_spec,
                None,
                fetch_decimals,
                lowercase_column_names,
            );
        }

        Ok(WorkerQueryOutcome {
            columns: Vec::new(),
            rows: Vec::new(),
            rowcount: affected,
            out_binds: OutBindsPayload::None,
            lastrowid: None,
            fetch_decimals,
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn build_returning_outcome(
        &self,
        tbl: &InMemoryTable,
        source_rows: &[Vec<OracleCellValue>],
        rowcount: usize,
        ret: &ReturningClause,
        out_binds_spec: &OutBindSpec,
        lastrowid: Option<String>,
        fetch_decimals: bool,
        lowercase_column_names: bool,
    ) -> Result<WorkerQueryOutcome, OracleError> {
        let mut columns = Vec::with_capacity(ret.columns.len());
        let mut proj_rows = Vec::with_capacity(source_rows.len());
        for col_expr in &ret.columns {
            let raw_name = extract_column_alias_or_name(col_expr);
            let col_name = if lowercase_column_names {
                raw_name.to_ascii_lowercase()
            } else {
                raw_name.clone()
            };
            let type_name = tbl
                .find_col_idx(&raw_name)
                .map(|i| tbl.columns[i].type_name.clone())
                .unwrap_or_else(|| "VARCHAR2".to_owned());
            columns.push(OracleColumn::new(
                col_name,
                type_name,
                0,
                None,
                None,
                None,
                true,
            ));
        }

        for row in source_rows {
            let mut proj = Vec::with_capacity(ret.columns.len());
            for col_expr in &ret.columns {
                let (cell, _) =
                    self.eval_scalar_expr(col_expr, &BindArgs::default(), Some((tbl, row)), 1)?;
                proj.push(cell);
            }
            proj_rows.push(proj);
        }

        let first_proj = proj_rows.first().cloned().unwrap_or_default();
        let is_named_out = match out_binds_spec {
            OutBindSpec::Named(_) => true,
            OutBindSpec::Positional(_) => false,
            OutBindSpec::None => ret
                .out_vars
                .iter()
                .any(|v| v.parse::<usize>().is_err()),
        };
        let out_binds = if first_proj.is_empty() {
            OutBindsPayload::None
        } else if is_named_out {
            let pairs = ret
                .out_vars
                .iter()
                .cloned()
                .zip(first_proj)
                .collect();
            OutBindsPayload::Named(pairs)
        } else {
            OutBindsPayload::Positional(first_proj)
        }
        ;

        Ok(WorkerQueryOutcome {
            columns,
            rows: proj_rows,
            rowcount,
            out_binds,
            lastrowid,
            fetch_decimals,
        })
    }

    fn handle_select(
        &self,
        sql: &str,
        params: &BindArgs,
        fetch_decimals: bool,
        lowercase_column_names: bool,
        max_rows: Option<usize>,
    ) -> Result<WorkerQueryOutcome, OracleError> {
        let upper = sql.to_ascii_uppercase();
        let from_idx = find_keyword_outside_quotes(&upper, "FROM").ok_or_else(|| {
            OracleError::Server {
                code: 923,
                message: "ORA-00923: FROM keyword not found where expected".to_owned(),
            }
        })?;
        let select_kw_end = if upper.starts_with("SELECT") {
            "SELECT".len()
        } else {
            0
        };
        let mut select_list_part = sql[select_kw_end..from_idx].trim();
        if let Some(stripped) = strip_leading_sql_comments(select_list_part).strip_prefix("DISTINCT ")
        {
            select_list_part = stripped.trim();
        } else {
            select_list_part = strip_leading_sql_comments(select_list_part);
        }

        let after_from = sql[from_idx + "FROM".len()..].trim();
        let after_from_up = after_from.to_ascii_uppercase();

        let connect_by_idx = find_keyword_outside_quotes(&after_from_up, "CONNECT");
        let where_idx = find_keyword_outside_quotes(&after_from_up, "WHERE");
        let order_idx = find_keyword_outside_quotes(&after_from_up, "ORDER");
        let fetch_idx = find_keyword_outside_quotes(&after_from_up, "FETCH");

        let tbl_end = [connect_by_idx, where_idx, order_idx, fetch_idx]
            .into_iter()
            .flatten()
            .min()
            .unwrap_or(after_from.len());
        let tbl_name = after_from[..tbl_end]
            .trim()
            .split_whitespace()
            .next()
            .unwrap_or("")
            .trim_matches('"')
            .to_ascii_uppercase();

        let mut connect_levels = 1i64;
        if let Some(cb_pos) = connect_by_idx {
            let cb_tail = &after_from_up[cb_pos..];
            if let Some(le_idx) = cb_tail.find("<=") {
                let num_part = cb_tail[le_idx + 2..]
                    .trim()
                    .split_whitespace()
                    .next()
                    .unwrap_or("1");
                if let Some(bind_k) = num_part.strip_prefix(':') {
                    let cell = if let Ok(p) = bind_k.parse::<usize>() {
                        params.get_positional(p)
                    } else {
                        params.get_named(bind_k)
                    };
                    if let Some(c) = cell
                        && let Some(iv) = cell_as_i64(c)
                    {
                        connect_levels = iv.max(0);
                    }
                } else if let Ok(iv) = num_part.parse::<i64>() {
                    connect_levels = iv.max(0);
                }
            }
        }

        let mut fetch_limit = max_rows;
        if let Some(f_pos) = fetch_idx {
            let f_tail = &after_from_up[f_pos..];
            for tok in f_tail.split_whitespace() {
                if let Ok(n) = tok.parse::<usize>() {
                    fetch_limit = Some(fetch_limit.map_or(n, |m| m.min(n)));
                    break;
                }
            }
        }

        if tbl_name == "DUAL" || tbl_name == "SYS.DUAL" {
            let select_exprs = split_top_level_commas(select_list_part);
            let mut columns = Vec::with_capacity(select_exprs.len());
            let mut out_rows = Vec::with_capacity(connect_levels as usize);

            for lvl in 1..=connect_levels {
                let mut row_cells = Vec::with_capacity(select_exprs.len());
                for (cidx, raw_item) in select_exprs.iter().enumerate() {
                    let (expr_str, alias_name) = split_expr_and_alias(raw_item.trim(), cidx);
                    let (cell, tname) = self.eval_scalar_expr(expr_str, params, None, lvl)?;
                    if lvl == 1 {
                        let cname = if lowercase_column_names {
                            alias_name.to_ascii_lowercase()
                        } else {
                            alias_name.to_ascii_uppercase()
                        };
                        let scale = match &cell {
                            OracleCellValue::Int(_) => Some(0),
                            OracleCellValue::DecimalStr(_) => Some(2),
                            _ => None,
                        };
                        columns.push(OracleColumn::new(
                            cname,
                            tname,
                            0,
                            None,
                            None,
                            scale,
                            true,
                        ));
                    }
                    row_cells.push(cell);
                }
                out_rows.push(row_cells);
                if let Some(lim) = fetch_limit
                    && out_rows.len() >= lim
                {
                    break;
                }
            }
            let rc = out_rows.len();
            return Ok(WorkerQueryOutcome {
                columns,
                rows: out_rows,
                rowcount: rc,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals,
            });
        }

        let tbl = self
            .tables
            .get(&tbl_name)
            .ok_or_else(|| OracleError::Server {
                code: 942,
                message: format!("ORA-00942: table or view `{tbl_name}` does not exist"),
            })?;

        let where_part = if let Some(w_pos) = where_idx {
            let w_end = [order_idx, fetch_idx]
                .into_iter()
                .flatten()
                .filter(|&p| p > w_pos)
                .min()
                .unwrap_or(after_from.len());
            &after_from[w_pos + "WHERE".len()..w_end]
        } else {
            ""
        };

        let order_part = if let Some(o_pos) = order_idx {
            let o_end = fetch_idx
                .filter(|&p| p > o_pos)
                .unwrap_or(after_from.len());
            let raw_order = after_from[o_pos + "ORDER".len()..o_end].trim();
            let raw_order_up = raw_order.to_ascii_uppercase();
            if raw_order_up.starts_with("BY ") {
                raw_order[3..].trim()
            } else {
                raw_order
            }
        } else {
            ""
        };

        let mut matching_rows: Vec<&Vec<OracleCellValue>> = Vec::new();
        for row in &tbl.rows {
            if self.eval_where_clause(where_part, params, tbl, row)? {
                matching_rows.push(row);
            }
        }

        if select_list_part.trim().to_ascii_uppercase().starts_with("COUNT(") {
            let (_, alias) = split_expr_and_alias(select_list_part, 0);
            let cname = if lowercase_column_names {
                alias.to_ascii_lowercase()
            } else {
                alias.to_ascii_uppercase()
            };
            let cnt = matching_rows.len() as i64;
            return Ok(WorkerQueryOutcome {
                columns: vec![OracleColumn::new(
                    cname,
                    "NUMBER".to_owned(),
                    2,
                    None,
                    Some(38),
                    Some(0),
                    false,
                )],
                rows: vec![vec![OracleCellValue::Int(cnt)]],
                rowcount: 1,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals,
            });
        }

        let select_items: Vec<(String, String)> = if select_list_part.trim() == "*" {
            tbl.columns
                .iter()
                .map(|c| (c.name.clone(), c.name.clone()))
                .collect()
        } else {
            split_top_level_commas(select_list_part)
                .into_iter()
                .enumerate()
                .map(|(i, item)| {
                    let (e, a) = split_expr_and_alias(item.trim(), i);
                    (e.to_owned(), a)
                })
                .collect()
        };

        let mut columns = Vec::with_capacity(select_items.len());
        for (expr_str, alias_name) in &select_items {
            let cname = if lowercase_column_names {
                alias_name.to_ascii_lowercase()
            } else {
                alias_name.to_ascii_uppercase()
            };
            let tname = tbl
                .find_col_idx(expr_str)
                .map(|i| tbl.columns[i].type_name.clone())
                .unwrap_or_else(|| {
                    if expr_str.to_ascii_uppercase().starts_with("VECTOR_DISTANCE(") {
                        "BINARY_DOUBLE".to_owned()
                    } else {
                        "VARCHAR2".to_owned()
                    }
                });
            columns.push(OracleColumn::new(
                cname,
                tname,
                0,
                None,
                None,
                None,
                true,
            ));
        }

        let mut projected: Vec<(Vec<OracleCellValue>, OracleCellValue)> =
            Vec::with_capacity(matching_rows.len());
        for row in matching_rows {
            let mut proj = Vec::with_capacity(select_items.len());
            for (expr_str, _) in &select_items {
                let (cell, _) = self.eval_scalar_expr(expr_str, params, Some((tbl, row)), 1)?;
                proj.push(cell);
            }
            let sort_key = if !order_part.is_empty() {
                let first_order = split_top_level_commas(order_part)[0].trim();
                let order_expr = strip_order_direction(first_order);
                if let Some(alias_pos) = select_items
                    .iter()
                    .position(|(_, a)| a.eq_ignore_ascii_case(order_expr))
                {
                    proj[alias_pos].clone()
                } else {
                    self.eval_scalar_expr(order_expr, params, Some((tbl, row)), 1)?
                        .0
                }
            } else {
                OracleCellValue::Null
            };
            projected.push((proj, sort_key));
        }

        if !order_part.is_empty() {
            let first_order = split_top_level_commas(order_part)[0].trim();
            let desc = first_order.to_ascii_uppercase().ends_with(" DESC");
            projected.sort_by(|a, b| {
                let cmp = compare_cells(&a.1, &b.1);
                if desc { cmp.reverse() } else { cmp }
            });
        }

        let mut rows: Vec<Vec<OracleCellValue>> = projected.into_iter().map(|(r, _)| r).collect();
        if let Some(lim) = fetch_limit
            && rows.len() > lim
        {
            rows.truncate(lim);
        }
        let rc = rows.len();
        Ok(WorkerQueryOutcome {
            columns,
            rows,
            rowcount: rc,
            out_binds: OutBindsPayload::None,
            lastrowid: None,
            fetch_decimals,
        })
    }
}

fn split_expr_and_alias(item: &str, col_idx: usize) -> (&str, String) {
    let trimmed = item.trim();
    let upper = trimmed.to_ascii_uppercase();
    if let Some(as_pos) = find_keyword_outside_quotes(&upper, "AS") {
        let expr = trimmed[..as_pos].trim();
        let alias = trimmed[as_pos + 2..].trim().trim_matches('"');
        if !alias.is_empty() {
            return (expr, alias.to_owned());
        }
    }
    let clean = trimmed.trim_matches('"');
    let after_dot = clean.rsplit('.').next().unwrap_or(clean).trim();
    if after_dot
        .bytes()
        .all(|b| matches!(b, b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_'))
        && !after_dot.is_empty()
    {
        return (trimmed, after_dot.to_owned());
    }
    if upper.starts_with("COUNT(") {
        return (trimmed, "COUNT(*)".to_owned());
    }
    (trimmed, format!("COL_{}", col_idx + 1))
}

fn strip_order_direction(order_item: &str) -> &str {
    let up = order_item.to_ascii_uppercase();
    if up.ends_with(" DESC") {
        order_item[..order_item.len() - 5].trim()
    } else if up.ends_with(" ASC") {
        order_item[..order_item.len() - 4].trim()
    } else {
        order_item.trim()
    }
}

fn cell_as_string(cell: &OracleCellValue) -> String {
    match cell {
        OracleCellValue::Null => String::new(),
        OracleCellValue::Bool(b) => b.to_string(),
        OracleCellValue::Int(i) => i.to_string(),
        OracleCellValue::Float(f) => f.to_string(),
        OracleCellValue::DecimalStr(s) | OracleCellValue::Str(s) => s.clone(),
        OracleCellValue::Bytes(b) | OracleCellValue::Oson(b) => {
            String::from_utf8_lossy(b).into_owned()
        }
        OracleCellValue::Date { year, month, day } => format!("{year:04}-{month:02}-{day:02}"),
        OracleCellValue::Timestamp {
            year,
            month,
            day,
            hour,
            minute,
            second,
            ..
        } => format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}"),
        OracleCellValue::Vector(_) => "[VECTOR]".to_owned(),
    }
}

fn cell_as_i64(cell: &OracleCellValue) -> Option<i64> {
    match cell {
        OracleCellValue::Int(i) => Some(*i),
        OracleCellValue::DecimalStr(s) | OracleCellValue::Str(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn cell_as_f64(cell: &OracleCellValue) -> Option<f64> {
    match cell {
        OracleCellValue::Int(i) => Some(*i as f64),
        OracleCellValue::Float(f) => Some(*f),
        OracleCellValue::DecimalStr(s) | OracleCellValue::Str(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn compare_cells(a: &OracleCellValue, b: &OracleCellValue) -> std::cmp::Ordering {
    if let (Some(ai), Some(bi)) = (cell_as_i64(a), cell_as_i64(b)) {
        return ai.cmp(&bi);
    }
    if let (Some(af), Some(bf)) = (cell_as_f64(a), cell_as_f64(b)) {
        return af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal);
    }
    if let (OracleCellValue::Bytes(ab), OracleCellValue::Bytes(bb)) = (a, b) {
        return ab.cmp(bb);
    }
    cell_as_string(a).cmp(&cell_as_string(b))
}

fn infer_cell_oracle_type(cell: &OracleCellValue) -> String {
    match cell {
        OracleCellValue::Null | OracleCellValue::Str(_) => "VARCHAR2".to_owned(),
        OracleCellValue::Bool(_) => "BOOLEAN".to_owned(),
        OracleCellValue::Int(_) | OracleCellValue::DecimalStr(_) => "NUMBER".to_owned(),
        OracleCellValue::Float(_) => "BINARY_DOUBLE".to_owned(),
        OracleCellValue::Bytes(_) => "RAW".to_owned(),
        OracleCellValue::Date { .. } => "DATE".to_owned(),
        OracleCellValue::Timestamp { tz_hour_offset, .. } => {
            if tz_hour_offset.is_some() {
                "TIMESTAMP WITH TIME ZONE".to_owned()
            } else {
                "TIMESTAMP".to_owned()
            }
        }
        OracleCellValue::Oson(_) => "JSON".to_owned(),
        OracleCellValue::Vector(_) => "VECTOR".to_owned(),
    }
}

fn extract_f64_vector(cell: &OracleCellValue) -> Result<Vec<f64>, OracleError> {
    match cell {
        OracleCellValue::Vector(v) => match v {
            OracleVectorData::Float32(vals) => Ok(vals.iter().map(|&x| f64::from(x)).collect()),
            OracleVectorData::Float64(vals) => Ok(vals.clone()),
            OracleVectorData::Int8(vals) => Ok(vals.iter().map(|&x| f64::from(x)).collect()),
            OracleVectorData::Binary(vals) => Ok(vals.iter().map(|&x| f64::from(x)).collect()),
        },
        OracleCellValue::Bytes(raw) => {
            let v = OracleVectorData::decode_wire_bytes(raw)?;
            extract_f64_vector(&OracleCellValue::Vector(v))
        }
        _ => Err(OracleError::data_error(
            "Expected Oracle VECTOR operand in VECTOR_DISTANCE",
        )),
    }
}

fn compute_vector_distance(
    lhs: &OracleCellValue,
    rhs: &OracleCellValue,
    metric: &str,
) -> Result<f64, OracleError> {
    let a = extract_f64_vector(lhs)?;
    let b = extract_f64_vector(rhs)?;
    if a.len() != b.len() || a.is_empty() {
        return Err(OracleError::data_error(
            "VECTOR_DISTANCE operands must have identical non-zero dimensions",
        ));
    }
    let m = metric.trim().to_ascii_uppercase();
    if m.contains("EUCLIDEAN") || m == "L2" {
        let sum_sq: f64 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).powi(2)).sum();
        Ok(sum_sq.sqrt())
    } else if m.contains("DOT") {
        let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        Ok(-dot)
    } else if m.contains("MANHATTAN") || m == "L1" {
        let l1: f64 = a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum();
        Ok(l1)
    } else {
        let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
        let norm_a: f64 = a.iter().map(|x| x * x).sum::<f64>().sqrt();
        let norm_b: f64 = b.iter().map(|x| x * x).sum::<f64>().sqrt();
        if norm_a == 0.0 || norm_b == 0.0 {
            Ok(1.0)
        } else {
            Ok(1.0 - (dot / (norm_a * norm_b)))
        }
    }
}

struct WorkerSession {
    config: Arc<NativeOracleConfig>,
    conn: Option<oracledb::Connection>,
    mem_store: InMemoryOracleStore,
    savepoints: Vec<String>,
    in_transaction: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    break_handle: Arc<OracleBreakHandle>,
    pending_session_attrs: Arc<Mutex<Option<OracleSessionAttrs>>>,
    active_session_attrs: Arc<Mutex<OracleSessionAttrs>>,
    last_piggyback_frame: Arc<Mutex<Option<Vec<u8>>>>,
}

impl WorkerSession {
    fn is_memory_mode(&self) -> bool {
        self.config.host.eq_ignore_ascii_case("memory")
            || self
                .config
                .dsn
                .as_deref()
                .is_some_and(|d| d.eq_ignore_ascii_case(":memory:") || d.contains("://memory"))
    }

    fn flush_pending_session_attrs(&mut self) {
        let staged = {
            let mut guard = self
                .pending_session_attrs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.take()
        };
        let Some(attrs) = staged else {
            return;
        };
        if attrs.client_identifier.is_none()
            && attrs.module.is_none()
            && attrs.action.is_none()
            && attrs.ecid.is_none()
        {
            return;
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
            if let Some(ref cid) = attrs.client_identifier {
                active_guard.client_identifier = Some(cid.clone());
                self.mem_store.session_context.insert(
                    ("USERENV".to_owned(), "CLIENT_IDENTIFIER".to_owned()),
                    cid.clone(),
                );
            }
            if let Some(ref m) = attrs.module {
                active_guard.module = Some(m.clone());
                self.mem_store.session_context.insert(
                    ("USERENV".to_owned(), "MODULE".to_owned()),
                    m.clone(),
                );
            }
            if let Some(ref a) = attrs.action {
                active_guard.action = Some(a.clone());
                self.mem_store.session_context.insert(
                    ("USERENV".to_owned(), "ACTION".to_owned()),
                    a.clone(),
                );
            }
            if let Some(ref e) = attrs.ecid {
                active_guard.ecid = Some(e.clone());
                self.mem_store.session_context.insert(
                    ("USERENV".to_owned(), "ECID".to_owned()),
                    e.clone(),
                );
            }
        }
    }

    fn ensure_connected(&mut self) -> Result<&oracledb::Connection, OracleError> {
        if self.conn.is_none() {
            let c = connect_oracledb(&self.config)?;
            self.conn = Some(c);
        }
        self.conn.as_ref().ok_or_else(|| {
            OracleError::Interface("Oracle worker connection unavailable".to_owned())
        })
    }

    fn handle_command(&mut self, cmd: WorkerCommand) -> Result<WorkerReply, OracleError> {
        self.break_handle.check_cancelled()?;
        match cmd {
            WorkerCommand::Connect => {
                if !self.is_memory_mode() {
                    let _ = self.ensure_connected()?;
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Ping => {
                if let Some(ref conn) = self.conn {
                    conn.ping().map_err(|e| OracleError::from_oracledb(&e))?;
                } else if !self.is_memory_mode() {
                    let conn = self.ensure_connected()?;
                    conn.ping().map_err(|e| OracleError::from_oracledb(&e))?;
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Begin => {
                if self.is_memory_mode() {
                    self.mem_store.begin_tx_if_needed();
                }
                self.in_transaction.store(true, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Commit => {
                if let Some(ref conn) = self.conn {
                    conn.commit().map_err(|e| OracleError::from_oracledb(&e))?;
                } else if self.is_memory_mode() {
                    self.mem_store.commit();
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Rollback => {
                if let Some(ref conn) = self.conn {
                    conn.rollback().map_err(|e| OracleError::from_oracledb(&e))?;
                } else if self.is_memory_mode() {
                    self.mem_store.rollback();
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::Savepoint(name) => {
                validate_savepoint_identifier(&name)?;
                self.in_transaction.store(true, Ordering::Release);
                if let Some(ref conn) = self.conn {
                    let stmt = format!("SAVEPOINT {name}");
                    conn.execute(&stmt, &[])
                        .map_err(|e| OracleError::from_oracledb(&e))?;
                } else if self.is_memory_mode() {
                    self.mem_store.savepoint(&name);
                }
                if !self.savepoints.iter().any(|s| s.eq_ignore_ascii_case(&name)) {
                    self.savepoints.push(name);
                }
                Ok(WorkerReply::Unit)
            }
            WorkerCommand::RollbackToSavepoint(name) => {
                validate_savepoint_identifier(&name)?;
                if let Some(ref conn) = self.conn {
                    let stmt = format!("ROLLBACK TO SAVEPOINT {name}");
                    conn.execute(&stmt, &[])
                        .map_err(|e| OracleError::from_oracledb(&e))?;
                } else if self.is_memory_mode() {
                    self.mem_store.rollback_to_savepoint(&name)?;
                    if let Some(pos) = self
                        .savepoints
                        .iter()
                        .position(|s| s.eq_ignore_ascii_case(&name))
                    {
                        self.savepoints.truncate(pos + 1);
                    }
                } else if let Some(pos) = self
                    .savepoints
                    .iter()
                    .position(|s| s.eq_ignore_ascii_case(&name))
                {
                    self.savepoints.truncate(pos + 1);
                } else {
                    return Err(OracleError::Server {
                        code: 1086,
                        message: format!(
                            "ORA-01086: savepoint '{name}' never established in this session or is invalid"
                        ),
                    });
                }
                self.in_transaction.store(true, Ordering::Release);
                Ok(WorkerReply::Unit)
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
                Ok(WorkerReply::RowCount(total))
            }
            WorkerCommand::QueryArrow {
                sql,
                params,
                batch_size,
                lowercase_column_names,
            } => {
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
                Ok(WorkerReply::ArrowIpc {
                    ipc_bytes,
                    row_count,
                })
            }
            WorkerCommand::Close => {
                if let Some(mut conn) = self.conn.take() {
                    let _ = conn.close();
                }
                self.savepoints.clear();
                self.in_transaction.store(false, Ordering::Release);
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
        self.flush_pending_session_attrs();
        if self.conn.is_none()
            && let Some(sleep_ms) = parse_synthetic_sleep_ms(sql)
        {
            self.break_handle
                .wait_or_cancelled(Duration::from_millis(sleep_ms))?;
        }
        if self.conn.is_none() && self.is_memory_mode() {
            let outcome = self.mem_store.execute_sql(
                sql,
                params,
                out_binds,
                fetch_decimals,
                lowercase_column_names,
                max_rows,
            )?;
            if !self.in_transaction.load(Ordering::Acquire) {
                self.mem_store.commit();
            }
            self.break_handle.check_cancelled()?;
            return Ok(outcome);
        }

        let auto_commit = !self.in_transaction.load(Ordering::Acquire);
        let break_handle = Arc::clone(&self.break_handle);
        let conn = self.ensure_connected()?;
        let clean = strip_leading_sql_comments(sql).trim();
        let upper = clean.to_ascii_uppercase();
        let is_select = is_query || upper.starts_with("SELECT") || upper.starts_with("WITH");

        if is_select && parse_returning_clause(clean).is_none() {
            let cursor = match params {
                BindArgs::Positional(vals) => {
                    let mut boxed = Vec::with_capacity(vals.len());
                    for v in vals {
                        boxed.push(cell_to_oracledb_bind(v)?);
                    }
                    let refs: Vec<&dyn oracledb::ToDbValue> =
                        boxed.iter().map(|b| &**b as &dyn oracledb::ToDbValue).collect();
                    conn.query(sql, &refs)
                        .map_err(|e| OracleError::from_oracledb(&e))?
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
                        .map_err(|e| OracleError::from_oracledb(&e))?
                }
            };
            let columns: Vec<OracleColumn> = cursor
                .columns()
                .iter()
                .map(|m| OracleColumn::from_metadata(m, lowercase_column_names))
                .collect();
            let mut rows = Vec::new();
            for row_res in cursor {
                break_handle.check_cancelled()?;
                let mut r = row_res.map_err(|e| OracleError::from_oracledb(&e))?;
                let decoded = decode_oracledb_row(&mut r, &columns)?;
                rows.push(decoded);
                if let Some(lim) = max_rows
                    && rows.len() >= lim
                {
                    break;
                }
            }
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

        let returning = parse_returning_clause(clean);
        let mut exec_res = match params {
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
                let refs: Vec<&dyn oracledb::ToDbValue> =
                    boxed.iter().map(|b| &**b as &dyn oracledb::ToDbValue).collect();
                conn.execute(sql, &refs)
                    .map_err(|e| OracleError::from_oracledb(&e))?
            }
            BindArgs::Named(pairs) => {
                let mut boxed: Vec<(String, Box<dyn oracledb::ToDbValue + Send + Sync>)> =
                    Vec::with_capacity(pairs.len());
                for (k, v) in pairs {
                    boxed.push((k.clone(), cell_to_oracledb_bind(v)?));
                }
                if let OutBindSpec::Named(out_pairs) = out_binds {
                    for (k, h) in out_pairs {
                        boxed.push((k.clone(), out_bind_type_placeholder(h)));
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
                    .map_err(|e| OracleError::from_oracledb(&e))?
            }
        };

        if auto_commit {
            let _ = conn.commit();
        }
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
                if let Some(ref ret) = returning
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

/// Handle to a dedicated `"vtb-ora-worker"` OS thread owning a single `oracledb::Connection`.
pub struct OracleWorkerHandle {
    id: u64,
    config: Arc<NativeOracleConfig>,
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

        let join_handle = thread::Builder::new()
            .name("vtb-ora-worker".to_owned())
            .spawn(move || {
                let mut session = WorkerSession {
                    config: cfg_clone,
                    conn: None,
                    mem_store: InMemoryOracleStore::default(),
                    savepoints: Vec::new(),
                    in_transaction: in_tx_clone,
                    closed: closed_clone,
                    break_handle: break_clone,
                    pending_session_attrs: pending_clone,
                    active_session_attrs: active_clone,
                    last_piggyback_frame: last_frame_clone,
                };
                while let Ok(envelope) = rx.recv() {
                    let is_close = matches!(envelope.cmd, WorkerCommand::Close);
                    let outcome = session.handle_command(envelope.cmd);
                    session.break_handle.end_operation();
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
        {
            let mut pending = self
                .pending_session_attrs
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            *pending = None;
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

    /// Stage end-to-end tracing and session attributes in memory to be flushed on the next statement.
    pub fn stage_session_attrs(
        &self,
        client_identifier: Option<String>,
        module: Option<String>,
        action: Option<String>,
        ecid: Option<String>,
    ) {
        if client_identifier.is_none()
            && module.is_none()
            && action.is_none()
            && ecid.is_none()
        {
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
            return Err(OracleError::Interface(
                "Oracle client is closed".to_owned(),
            ));
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
            return Err(OracleError::Interface(
                "Oracle client is closed".to_owned(),
            ));
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

    /// Roll back any uncommitted transaction state within `timeout`, or close the worker if it times out.
    pub fn rollback_with_timeout(&self, timeout: Duration) -> Result<(), OracleError> {
        self.ensure_open()?;
        let tx = {
            let guard = self.sender.lock().unwrap_or_else(|e| e.into_inner());
            guard.clone()
        };
        let Some(sender) = tx else {
            return Err(OracleError::Interface(
                "Oracle client is closed".to_owned(),
            ));
        };
        let queue = Arc::new(SyncCompletionQueue::new());
        self.break_handle.begin_operation();
        if sender
            .send(WorkerEnvelope {
                cmd: WorkerCommand::Rollback,
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
}

impl SyncOracleClient {
    /// Construct a [`SyncOracleClient`] wrapping an existing [`OracleWorkerHandle`].
    #[must_use]
    pub fn from_worker(worker: Arc<OracleWorkerHandle>) -> Self {
        Self { worker }
    }

    /// Return a reference to the underlying [`OracleWorkerHandle`].
    #[must_use]
    pub fn worker(&self) -> &Arc<OracleWorkerHandle> {
        &self.worker
    }
}

#[pymethods]
impl SyncOracleClient {
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
        let cfg = NativeOracleConfig::validate(raw).map_err(|e| e.to_py_err(py))?;
        let worker = OracleWorkerHandle::spawn(cfg).map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            worker: Arc::new(worker),
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

    pub fn connect(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Connect))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.close_sync())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn ping(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Ping))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn cancel(&self) {
        self.worker.cancel();
    }

    pub fn begin(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Begin))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Commit))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<()> {
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Rollback))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn savepoint(&self, py: Python<'_>, name: String) -> PyResult<()> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::Savepoint(name)))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    pub fn rollback_to_savepoint(&self, py: Python<'_>, name: String) -> PyResult<()> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        let worker = Arc::clone(&self.worker);
        py.detach(|| worker.dispatch_sync(WorkerCommand::RollbackToSavepoint(name)))
            .map(|_| ())
            .map_err(|e| e.to_py_err(py))
    }

    #[pyo3(signature = (
        sql,
        params=None,
        *,
        out_binds=None,
        fetch_decimals=false,
        enable_lowercase_column_names=None,
    ))]
    pub fn query(
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
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
        let worker = Arc::clone(&self.worker);
        let reply = py
            .detach(|| {
                worker.dispatch_sync(WorkerCommand::Query {
                    sql,
                    params: bind_args,
                    out_binds: out_spec,
                    fetch_decimals,
                    lowercase_column_names: lowercase,
                    max_rows: None,
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        match reply {
            WorkerReply::QueryOutcome(outcome) => outcome.into_py_query_result(py),
            _ => Ok(OracleQueryResult::new(py, Vec::new(), Vec::new(), 0, None, None)),
        }
    }

    #[pyo3(signature = (
        sql,
        params=None,
        *,
        fetch_decimals=false,
        enable_lowercase_column_names=None,
    ))]
    pub fn query_one(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
        fetch_decimals: bool,
        enable_lowercase_column_names: Option<bool>,
    ) -> PyResult<Py<PyAny>> {
        let bind_args = extract_bind_args(params).map_err(|e| e.to_py_err(py))?;
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
        let worker = Arc::clone(&self.worker);
        let reply = py
            .detach(|| {
                worker.dispatch_sync(WorkerCommand::Query {
                    sql,
                    params: bind_args,
                    out_binds: OutBindSpec::None,
                    fetch_decimals,
                    lowercase_column_names: lowercase,
                    max_rows: Some(1),
                })
            })
            .map_err(|e| e.to_py_err(py))?;
        match reply {
            WorkerReply::QueryOutcome(outcome) => outcome.into_py_first_row_or_none(py),
            _ => Ok(py.None()),
        }
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
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
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
        match reply {
            WorkerReply::QueryOutcome(outcome) => outcome.into_py_query_result(py),
            WorkerReply::RowCount(n) => {
                Ok(OracleQueryResult::new(py, Vec::new(), Vec::new(), n, None, None))
            }
            _ => Ok(OracleQueryResult::new(py, Vec::new(), Vec::new(), 0, None, None)),
        }
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
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
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
}

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
}

impl AsyncOracleClient {
    /// Construct an [`AsyncOracleClient`] wrapping an existing [`OracleWorkerHandle`].
    #[must_use]
    pub fn from_worker(worker: Arc<OracleWorkerHandle>) -> Self {
        Self { worker }
    }

    /// Return a reference to the underlying [`OracleWorkerHandle`].
    #[must_use]
    pub fn worker(&self) -> &Arc<OracleWorkerHandle> {
        &self.worker
    }
}

#[pymethods]
impl AsyncOracleClient {
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
        let cfg = NativeOracleConfig::validate(raw).map_err(|e| e.to_py_err(py))?;
        let worker = OracleWorkerHandle::spawn(cfg).map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            worker: Arc::new(worker),
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

    pub fn connect(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Connect, |py, _| Ok(py.None()))
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Close, |py, _| Ok(py.None()))
    }

    pub fn ping(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Ping, |py, _| Ok(py.None()))
    }

    pub fn cancel(&self) {
        self.worker.cancel();
    }

    pub fn begin(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Begin, |py, _| Ok(py.None()))
    }

    pub fn commit(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Commit, |py, _| Ok(py.None()))
    }

    pub fn rollback(&self, py: Python<'_>) -> PyResult<RustDbTaskAwaitable> {
        self.worker
            .dispatch_async(py, WorkerCommand::Rollback, |py, _| Ok(py.None()))
    }

    pub fn savepoint(&self, py: Python<'_>, name: String) -> PyResult<RustDbTaskAwaitable> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        self.worker
            .dispatch_async(py, WorkerCommand::Savepoint(name), |py, _| Ok(py.None()))
    }

    pub fn rollback_to_savepoint(
        &self,
        py: Python<'_>,
        name: String,
    ) -> PyResult<RustDbTaskAwaitable> {
        validate_savepoint_identifier(&name).map_err(|e| e.to_py_err(py))?;
        self.worker
            .dispatch_async(py, WorkerCommand::RollbackToSavepoint(name), |py, _| {
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
    pub fn query(
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
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
        self.worker.dispatch_async(
            py,
            WorkerCommand::Query {
                sql,
                params: bind_args,
                out_binds: out_spec,
                fetch_decimals,
                lowercase_column_names: lowercase,
                max_rows: None,
            },
            |py, reply| match reply {
                WorkerReply::QueryOutcome(outcome) => {
                    let qres = outcome.into_py_query_result(py)?;
                    Ok(Py::new(py, qres)?.into_any())
                }
                _ => {
                    let qres = OracleQueryResult::new(py, Vec::new(), Vec::new(), 0, None, None);
                    Ok(Py::new(py, qres)?.into_any())
                }
            },
        )
    }

    #[pyo3(signature = (
        sql,
        params=None,
        *,
        fetch_decimals=false,
        enable_lowercase_column_names=None,
    ))]
    pub fn query_one(
        &self,
        py: Python<'_>,
        sql: String,
        params: Option<&Bound<'_, PyAny>>,
        fetch_decimals: bool,
        enable_lowercase_column_names: Option<bool>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let bind_args = extract_bind_args(params).map_err(|e| e.to_py_err(py))?;
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
        self.worker.dispatch_async(
            py,
            WorkerCommand::Query {
                sql,
                params: bind_args,
                out_binds: OutBindSpec::None,
                fetch_decimals,
                lowercase_column_names: lowercase,
                max_rows: Some(1),
            },
            |py, reply| match reply {
                WorkerReply::QueryOutcome(outcome) => outcome.into_py_first_row_or_none(py),
                _ => Ok(py.None()),
            },
        )
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
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
        self.worker.dispatch_async(
            py,
            WorkerCommand::Execute {
                sql,
                params: bind_args,
                out_binds: out_spec,
                fetch_decimals,
                lowercase_column_names: lowercase,
            },
            |py, reply| match reply {
                WorkerReply::QueryOutcome(outcome) => {
                    let qres = outcome.into_py_query_result(py)?;
                    Ok(Py::new(py, qres)?.into_any())
                }
                WorkerReply::RowCount(n) => {
                    let qres = OracleQueryResult::new(py, Vec::new(), Vec::new(), n, None, None);
                    Ok(Py::new(py, qres)?.into_any())
                }
                _ => {
                    let qres = OracleQueryResult::new(py, Vec::new(), Vec::new(), 0, None, None);
                    Ok(Py::new(py, qres)?.into_any())
                }
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
        let lowercase =
            enable_lowercase_column_names.unwrap_or(self.worker.config().enable_lowercase_column_names);
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
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;

    use super::{
        BindArgs, OracleWorkerHandle, OutBindSpec, WorkerCommand, validate_savepoint_identifier,
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
    fn worker_out_of_band_cancel_aborts_sleep_and_keeps_worker_alive() {
        let cfg = NativeOracleConfig::validate(RawOracleOptions {
            host: "memory".to_owned(),
            ..Default::default()
        })
        .expect("valid memory config");
        let worker = Arc::new(OracleWorkerHandle::spawn(cfg).expect("spawn worker"));

        let worker_for_cancel = Arc::clone(&worker);
        let canceller = thread::spawn(move || {
            thread::sleep(Duration::from_millis(30));
            worker_for_cancel.cancel();
        });

        let res = worker.dispatch_sync(WorkerCommand::Execute {
            sql: "SELECT /*+ VTB_SLEEP_MS(2000) */ 1 FROM DUAL".to_owned(),
            params: BindArgs::default(),
            out_binds: OutBindSpec::None,
            fetch_decimals: false,
            lowercase_column_names: true,
        });
        canceller.join().expect("join canceller");
        assert!(matches!(res, Err(OracleError::Server { code: 1013, .. })));

        worker
            .dispatch_sync(WorkerCommand::Begin)
            .expect("worker still alive after cancel");
        assert!(worker.is_in_transaction());
        worker
            .dispatch_sync(WorkerCommand::Commit)
            .expect("commit succeeds");
        assert!(!worker.is_in_transaction());
        worker.close_sync().expect("close worker");
        assert!(worker.is_closed());
    }
}
