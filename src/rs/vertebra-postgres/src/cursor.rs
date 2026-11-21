//! Shared PEP 249 cursor state, description builder, row tuple builder, state-error helpers,
//! and native cursor classes for `vertebra-postgres`.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use pyo3::exceptions::{PyTypeError, PyValueError};
#[cfg(unix)]
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyWeakrefMethods,
    PyWeakrefReference,
};
#[cfg(unix)]
use vertebra_db::CancelHandle;
#[cfg(unix)]
use vertebra_db::py_async::db_runtime_handle;
use vertebra_db::py_async::{
    ReadyAwaitable, RustDbTaskAwaitable, block_on_db, ready_err, ready_ok,
    spawn_rust_db_task_with_attr_and_cancel,
};

use crate::Parameter;
use crate::async_client::POSTGRES_COMPLETION_DRAINER_ATTR;
use crate::bind_plan::{order_parameters, plan_for};
use crate::errors::{PostgresError, instantiate_exc, postgres_error_to_py, resolve_py_exceptions};
use crate::pool::PostgresPool;
#[cfg(unix)]
use crate::pool::{LoopReaderSlot, PooledConnection, is_clean_oneshot_query};
use crate::python_values::{extract_parameters, pg_value_to_python};
use crate::result::{Column, PgValue, QueryResult, TransactionStatus};
use crate::session::cancel::OperationGeneration;
use crate::session::{CommandBudget, SessionHandle};
#[cfg(unix)]
use crate::session::{
    ExchangeState, LoopCachedExchange, LoopPrepareOutcome, LoopReadStep, LoopWriteOutcome,
};
use crate::sync_client::extract_strict_opt_f64;

/// Backing execution source for PostgreSQL cursors (session-pinned or pool one-shot).
#[derive(Clone)]
pub(crate) enum CursorSource {
    Session(Arc<SessionHandle>),
    Pool(Arc<PostgresPool>),
}

/// Buffered result set and PEP 249 state shared by sync and async PostgreSQL cursors.
pub(crate) struct CursorState {
    pub(crate) rows: Vec<Py<PyTuple>>,
    pub(crate) pos: usize,
    pub(crate) description: Option<Py<PyTuple>>,
    pub(crate) rowcount: i64,
    pub(crate) lastrowid: Option<Py<PyAny>>,
    pub(crate) arraysize: usize,
    pub(crate) executed: bool,
    pub(crate) closed: bool,
}

impl CursorState {
    /// Construct a fresh, unexecuted cursor state (`rowcount = -1`, `arraysize = 1`).
    pub(crate) const fn new() -> Self {
        Self {
            rows: Vec::new(),
            pos: 0,
            description: None,
            rowcount: -1,
            lastrowid: None,
            arraysize: 1,
            executed: false,
            closed: false,
        }
    }

    /// Return `InterfaceError` when the cursor has been closed.
    pub(crate) fn ensure_open(&self, py: Python<'_>) -> PyResult<()> {
        if self.closed {
            return Err(cursor_interface_err(py, "cursor is closed"));
        }
        Ok(())
    }

    fn ensure_result_set(&self, py: Python<'_>) -> PyResult<()> {
        self.ensure_open(py)?;
        if !self.executed || self.description.is_none() {
            return Err(cursor_programming_err(
                py,
                "no result set available to fetch from",
            ));
        }
        Ok(())
    }

    /// Replace the buffered statement result and discard any previously unread rows.
    pub(crate) fn store(
        &mut self,
        description: Option<Py<PyTuple>>,
        rows: Vec<Py<PyTuple>>,
        rowcount: i64,
        lastrowid: Option<Py<PyAny>>,
    ) {
        self.rows = rows;
        self.pos = 0;
        self.description = description;
        self.rowcount = rowcount;
        self.lastrowid = lastrowid;
        self.executed = true;
    }

    /// Reset buffered rows and statement metadata (used on failed execute and close).
    pub(crate) fn clear(&mut self) {
        self.rows.clear();
        self.pos = 0;
        self.description = None;
        self.rowcount = -1;
        self.lastrowid = None;
    }

    /// Return the next row tuple, or `None` when the buffered result set is exhausted.
    pub(crate) fn fetchone(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        self.ensure_result_set(py)?;
        if self.pos >= self.rows.len() {
            return Ok(None);
        }
        let row = self.rows[self.pos].clone_ref(py);
        self.pos += 1;
        Ok(Some(row))
    }

    /// Return up to `size.unwrap_or(self.arraysize)` remaining row tuples.
    pub(crate) fn fetchmany(
        &mut self,
        py: Python<'_>,
        size: Option<usize>,
    ) -> PyResult<Vec<Py<PyTuple>>> {
        self.ensure_result_set(py)?;
        let limit = size.unwrap_or(self.arraysize);
        if limit == 0 || self.pos >= self.rows.len() {
            return Ok(Vec::new());
        }
        let end = self.pos.saturating_add(limit).min(self.rows.len());
        let out = self.rows[self.pos..end]
            .iter()
            .map(|row| row.clone_ref(py))
            .collect();
        self.pos = end;
        Ok(out)
    }

    /// Return all remaining row tuples and mark the result set exhausted.
    pub(crate) fn fetchall(&mut self, py: Python<'_>) -> PyResult<Vec<Py<PyTuple>>> {
        self.ensure_result_set(py)?;
        if self.pos >= self.rows.len() {
            return Ok(Vec::new());
        }
        let out = self.rows[self.pos..]
            .iter()
            .map(|row| row.clone_ref(py))
            .collect();
        self.pos = self.rows.len();
        Ok(out)
    }

    /// Validate and update `arraysize` (`>= 1`), raising `ProgrammingError` otherwise.
    pub(crate) fn set_arraysize(&mut self, py: Python<'_>, value: i64) -> PyResult<()> {
        if value < 1 {
            return Err(cursor_programming_err(py, "arraysize must be at least 1"));
        }
        self.arraysize = usize::try_from(value)
            .map_err(|_| cursor_programming_err(py, "arraysize must be at least 1"))?;
        Ok(())
    }

    /// Close the cursor and release buffered rows. Idempotent.
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.clear();
    }
}

/// Build the PEP 249 `description` tuple of 7-item `(name, type_oid, None, None, None, None, None)` tuples.
pub(crate) fn describe_columns(py: Python<'_>, columns: &[Column]) -> PyResult<Py<PyTuple>> {
    let mut entries = Vec::with_capacity(columns.len());
    for col in columns {
        let name = PyString::new(py, &col.name).into_any();
        let type_oid = col.type_oid.into_pyobject(py)?.into_any();
        let entry = PyTuple::new(
            py,
            [
                name,
                type_oid,
                py.None().into_bound(py),
                py.None().into_bound(py),
                py.None().into_bound(py),
                py.None().into_bound(py),
                py.None().into_bound(py),
            ],
        )?;
        entries.push(entry);
    }
    Ok(PyTuple::new(py, entries)?.unbind())
}

/// Return `true` when `raw` is an RFC 8259 JSON `null` literal with optional JSON whitespace.
fn is_rfc8259_json_null(raw: &[u8]) -> bool {
    let start = raw
        .iter()
        .position(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
        .unwrap_or(raw.len());
    let end = raw
        .iter()
        .rposition(|b| !matches!(b, b' ' | b'\t' | b'\n' | b'\r'))
        .map_or(0, |idx| idx + 1);
    start < end && &raw[start..end] == b"null"
}

/// Convert one decoded PostgreSQL cell into a public Python object matching `_project_public_row`.
pub(crate) fn cell_to_py(py: Python<'_>, cell: &PgValue) -> PyResult<Py<PyAny>> {
    match cell {
        PgValue::Null => Ok(py.None()),
        PgValue::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().into_any().unbind()),
        PgValue::Int2(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Int4(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Int8(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Float4(v) => Ok(f64::from(*v).into_pyobject(py)?.into_any().unbind()),
        PgValue::Float8(v) => Ok(v.into_pyobject(py)?.into_any().unbind()),
        PgValue::Text(s) => Ok(s.as_str().into_pyobject(py)?.into_any().unbind()),
        PgValue::Bytes(b) => Ok(PyBytes::new(py, b).into_any().unbind()),
        PgValue::Json(raw) if is_rfc8259_json_null(raw) => Ok(py.None()),
        other => pg_value_to_python(py, other),
    }
}

/// Convert one decoded PostgreSQL row into a public Python tuple matching `_project_public_row`.
pub(crate) fn row_to_tuple(py: Python<'_>, row: &[PgValue]) -> PyResult<Py<PyTuple>> {
    let mut cells = Vec::with_capacity(row.len());
    for cell in row {
        cells.push(cell_to_py(py, cell)?);
    }
    Ok(PyTuple::new(py, cells)?.unbind())
}

/// Build `(list_of_tuples, tuple_of_names)` for a [`QueryResult`], or `([], ())` when it has no columns.
pub(crate) fn rows_and_names(py: Python<'_>, result: &QueryResult) -> PyResult<Py<PyAny>> {
    if result.columns.is_empty() {
        let empty_rows = PyList::empty(py).into_any();
        let empty_names = PyTuple::empty(py).into_any();
        return Ok(PyTuple::new(py, [empty_rows, empty_names])?
            .into_any()
            .unbind());
    }
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        rows.push(row_to_tuple(py, row)?);
    }
    let rows_list = PyList::new(py, rows)?.into_any();
    let mut names = Vec::with_capacity(result.columns.len());
    for col in &result.columns {
        names.push(PyString::new(py, &col.name));
    }
    let names_tuple = PyTuple::new(py, names)?.into_any();
    Ok(PyTuple::new(py, [rows_list, names_tuple])?
        .into_any()
        .unbind())
}

/// Build a Python list of row dictionaries for a [`QueryResult`], reusing column name strings across rows.
pub(crate) fn row_dicts(py: Python<'_>, result: &QueryResult) -> PyResult<Py<PyAny>> {
    if result.columns.is_empty() || result.rows.is_empty() {
        return Ok(PyList::empty(py).into_any().unbind());
    }
    let mut keys = Vec::with_capacity(result.columns.len());
    for col in &result.columns {
        keys.push(PyString::new(py, &col.name));
    }
    let mut dicts = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let dict = PyDict::new(py);
        for (key, cell) in keys.iter().zip(row.iter()) {
            let val = cell_to_py(py, cell)?;
            dict.set_item(key, val)?;
        }
        dicts.push(dict);
    }
    Ok(PyList::new(py, dicts)?.into_any().unbind())
}

/// Return the first cell of the first row, `None` for an empty row, or `NotFoundError` when no row exists.
pub(crate) fn first_value(py: Python<'_>, result: &QueryResult) -> PyResult<Py<PyAny>> {
    let Some(first_row) = result.rows.first() else {
        return Err(cursor_not_found_err(py, "Query returned no rows"));
    };
    let Some(first_cell) = first_row.first() else {
        return Ok(py.None());
    };
    cell_to_py(py, first_cell)
}

/// Build `(rowcount, None)` for a [`QueryResult`], clamping negative row counts to `0`.
pub(crate) fn dml_outcome(py: Python<'_>, result: &QueryResult) -> PyResult<Py<PyAny>> {
    let rowcount = rowcount_for_result(result).max(0);
    let py_count = rowcount.into_pyobject(py)?.into_any();
    let py_none = py.None().into_bound(py);
    Ok(PyTuple::new(py, [py_count, py_none])?.into_any().unbind())
}

/// Return `true` when a PostgreSQL `CommandComplete` tag carries an affected-row count.
fn command_tag_reports_rows(tag: &str) -> bool {
    let mut parts = tag.split_whitespace();
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some("INSERT"), Some(_), Some(_), None)
            | (
                Some("DELETE" | "UPDATE" | "SELECT" | "MOVE" | "FETCH" | "COPY" | "MERGE"),
                Some(_),
                None,
                None,
            )
    )
}

/// Compute the PEP 249 `rowcount` for a [`QueryResult`].
pub(crate) fn rowcount_for_result(result: &QueryResult) -> i64 {
    if !result.columns.is_empty() {
        return i64::try_from(result.rows.len()).unwrap_or(i64::MAX);
    }
    if command_tag_reports_rows(&result.command_tag) {
        return i64::try_from(result.affected_rows).unwrap_or(i64::MAX);
    }
    -1
}

/// Construct a cursor `InterfaceError` from the shared `errors.rs` exception cache.
pub(crate) fn cursor_interface_err(py: Python<'_>, message: &str) -> PyErr {
    match resolve_py_exceptions(py) {
        Ok(types) => instantiate_exc(py, &types.interface, message, None, false),
        Err(err) => err,
    }
}

/// Construct a cursor `ProgrammingError` from the shared `errors.rs` exception cache.
pub(crate) fn cursor_programming_err(py: Python<'_>, message: &str) -> PyErr {
    match resolve_py_exceptions(py) {
        Ok(types) => instantiate_exc(py, &types.programming, message, None, false),
        Err(err) => err,
    }
}

/// Construct a cursor `NotFoundError` from the shared `errors.rs` exception cache.
pub(crate) fn cursor_not_found_err(py: Python<'_>, message: &str) -> PyErr {
    match resolve_py_exceptions(py) {
        Ok(types) => instantiate_exc(py, &types.not_found, message, None, false),
        Err(err) => err,
    }
}

static NORMALIZE_PARAM_FN: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

fn resolve_normalizer(py: Python<'_>) -> PyResult<&'static Py<PyAny>> {
    NORMALIZE_PARAM_FN.get_or_try_init(py, || {
        let module = py.import("vertebra.db.adapters.postgres.binds")?;
        let func = module.getattr("normalize_postgres_parameter")?;
        Ok(func.unbind())
    })
}

fn is_exact_primitive(obj: &Bound<'_, PyAny>) -> bool {
    obj.is_none()
        || obj.is_exact_instance_of::<PyBool>()
        || obj.is_exact_instance_of::<PyInt>()
        || obj.is_exact_instance_of::<PyFloat>()
        || obj.is_exact_instance_of::<PyString>()
        || obj.is_exact_instance_of::<PyBytes>()
}

pub(crate) fn convert_param_err(py: Python<'_>, err: PyErr) -> PyErr {
    if err.is_instance_of::<PyTypeError>(py) || err.is_instance_of::<PyValueError>(py) {
        let msg = err
            .value(py)
            .str()
            .ok()
            .and_then(|s| s.to_str().ok().map(str::to_owned))
            .unwrap_or_else(|| err.to_string());
        cursor_interface_err(py, &msg)
    } else {
        err
    }
}

fn normalize_ordered_tuple<'py>(
    py: Python<'py>,
    ordered: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    let tuple = ordered.cast::<PyTuple>()?;
    let len = tuple.len();
    let mut needs_norm = false;
    for idx in 0..len {
        let item = tuple.get_item(idx)?;
        if !is_exact_primitive(&item) {
            needs_norm = true;
            break;
        }
    }
    if !needs_norm {
        return Ok(ordered);
    }
    let normalizer = resolve_normalizer(py)?.bind(py);
    let mut out = Vec::with_capacity(len);
    for idx in 0..len {
        let item = tuple.get_item(idx)?;
        if is_exact_primitive(&item) {
            out.push(item);
        } else {
            let normalized = normalizer
                .call1((item,))
                .map_err(|e| convert_param_err(py, e))?;
            out.push(normalized);
        }
    }
    Ok(PyTuple::new(py, out)?.into_any())
}

pub(crate) fn extract_cursor_parameters(
    py: Python<'_>,
    plan: &crate::bind_plan::BindPlan,
    params: &Bound<'_, PyAny>,
    parameter_oids: Option<&Bound<'_, PyAny>>,
) -> PyResult<(Vec<Parameter>, Option<Vec<u32>>)> {
    let ordered = order_parameters(py, plan, params).map_err(|e| convert_param_err(py, e))?;
    let normalized = normalize_ordered_tuple(py, ordered)?;
    extract_parameters(py, &normalized, parameter_oids).map_err(|e| postgres_error_to_py(py, e))
}

/// A validated statement and bound parameters ready for execution on a session or pool.
pub(crate) struct StatementCall {
    pub(crate) sql: String,
    pub(crate) params: Vec<Parameter>,
    pub(crate) oids: Option<Vec<u32>>,
    pub(crate) timeout: Option<Duration>,
}

/// Validate timeout, resolve cached bind plan, and extract bound parameters for one statement call.
pub(crate) fn prepare_statement_call(
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
    parameter_oids: Option<&Bound<'_, PyAny>>,
    timeout: Option<&Bound<'_, PyAny>>,
) -> PyResult<StatementCall> {
    let timeout_secs = extract_strict_opt_f64(timeout).map_err(|e| convert_param_err(py, e))?;
    let timeout_dur = CommandBudget::validate_optional_seconds(timeout_secs)
        .map_err(|e| postgres_error_to_py(py, e))?;
    let plan = plan_for(sql).map_err(|e| postgres_error_to_py(py, e))?;
    let none_bound = py.None().into_bound(py);
    let params_ref = params.unwrap_or(&none_bound);
    let (bound_params, oids) = extract_cursor_parameters(py, &plan, params_ref, parameter_oids)?;
    Ok(StatementCall {
        sql: plan.sql.to_string(),
        params: bound_params,
        oids,
        timeout: timeout_dur,
    })
}

/// Ensure a [`CursorSource`] is open and ready to accept an ordinary statement before detaching.
pub(crate) fn ensure_source_ready(
    py: Python<'_>,
    source: &CursorSource,
    closed_pool_message: &str,
) -> PyResult<()> {
    match source {
        CursorSource::Session(session) => session
            .ensure_ordinary_reservable()
            .map_err(|e| postgres_error_to_py(py, e)),
        CursorSource::Pool(pool) => {
            if pool.is_closed() {
                return Err(cursor_interface_err(py, closed_pool_message));
            }
            Ok(())
        }
    }
}

/// Execute one prepared [`StatementCall`] against a session or pool, optionally lazily beginning a transaction.
pub(crate) async fn run_statement(
    source: CursorSource,
    call: StatementCall,
    lazy_begin: bool,
) -> Result<QueryResult, PostgresError> {
    match source {
        CursorSource::Session(session) => {
            if lazy_begin && session.transaction_status() == TransactionStatus::Idle {
                let begin_gen = session.reserve_operation_generation();
                session.begin_as_with_generation(begin_gen, None).await?;
            }
            let op_gen = session.reserve_operation_generation();
            let budget = match CommandBudget::start_from_duration(call.timeout) {
                Ok(b) => b,
                Err(err) => {
                    session.unreserve_operation_generation(op_gen);
                    return Err(err);
                }
            };
            session
                .query_as_with_generation_and_budget(
                    op_gen,
                    None,
                    &call.sql,
                    &call.params,
                    call.oids.as_deref(),
                    budget,
                )
                .await
        }
        CursorSource::Pool(pool) => {
            let budget = CommandBudget::start_from_duration(call.timeout)?;
            pool.query_oneshot(&call.sql, &call.params, call.oids.as_deref(), budget)
                .await
        }
    }
}

/// Prepare, run, and materialize one synchronous single-call statement without creating cursor state.
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_single_call(
    py: Python<'_>,
    source: CursorSource,
    closed_pool_message: &str,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
    parameter_oids: Option<&Bound<'_, PyAny>>,
    timeout: Option<&Bound<'_, PyAny>>,
    build: fn(Python<'_>, &QueryResult) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let call = prepare_statement_call(py, sql, params, parameter_oids, timeout)?;
    ensure_source_ready(py, &source, closed_pool_message)?;
    let result = py
        .detach(move || block_on_db(run_statement(source, call, false)))
        .map_err(|err| postgres_error_to_py(py, err))?;
    build(py, &result)
}

struct BuiltCursorResult {
    description: Option<Py<PyTuple>>,
    rows: Vec<Py<PyTuple>>,
    rowcount: i64,
}

fn build_cursor_result(py: Python<'_>, result: &QueryResult) -> PyResult<BuiltCursorResult> {
    let description = if result.columns.is_empty() {
        None
    } else {
        Some(describe_columns(py, &result.columns)?)
    };
    let mut rows = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        rows.push(row_to_tuple(py, row)?);
    }
    let rowcount = rowcount_for_result(result);
    Ok(BuiltCursorResult {
        description,
        rows,
        rowcount,
    })
}

/// Synchronous PEP 249 cursor over a [`SessionHandle`] or [`PostgresPool`]
/// (`vertebra._postgres.SyncPostgresCursor`).
#[pyclass(
    name = "SyncPostgresCursor",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PySyncPostgresCursor {
    source: CursorSource,
    state: Mutex<CursorState>,
    execute_lock: Arc<tokio::sync::Mutex<()>>,
    lazy_begin: bool,
}

impl PySyncPostgresCursor {
    /// Construct a cursor bound to `session` without performing any network I/O.
    pub(crate) fn new(session: Arc<SessionHandle>, lazy_begin: bool) -> Self {
        Self {
            source: CursorSource::Session(session),
            state: Mutex::new(CursorState::new()),
            execute_lock: Arc::new(tokio::sync::Mutex::new(())),
            lazy_begin,
        }
    }

    /// Construct a pool-backed cursor using the one-shot query path without performing any I/O.
    pub(crate) fn from_pool(pool: Arc<PostgresPool>) -> Self {
        Self {
            source: CursorSource::Pool(pool),
            state: Mutex::new(CursorState::new()),
            execute_lock: Arc::new(tokio::sync::Mutex::new(())),
            lazy_begin: false,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, CursorState> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn clear_state(&self) {
        self.lock_state().clear();
    }
}

#[pymethods]
impl PySyncPostgresCursor {
    /// Return the 7-item PEP 249 column description tuple, or `None` when no result set is present.
    #[getter]
    pub fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        self.lock_state()
            .description
            .as_ref()
            .map(|desc| desc.clone_ref(py))
    }

    /// Return the PEP 249 row count (`-1` before execute or when the command reports no count).
    #[getter]
    pub fn rowcount(&self) -> i64 {
        self.lock_state().rowcount
    }

    /// Return `None` (PostgreSQL does not populate a PEP 249 `lastrowid`).
    #[getter]
    pub fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock_state()
            .lastrowid
            .as_ref()
            .map(|val| val.clone_ref(py))
    }

    /// Return the default batch size used by `fetchmany()`.
    #[getter]
    pub fn arraysize(&self) -> usize {
        self.lock_state().arraysize
    }

    /// Set the default batch size used by `fetchmany()` (`>= 1`).
    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.lock_state().set_arraysize(py, value)
    }

    /// Execute one SQL statement on the client session and buffer its result set on this cursor.
    #[pyo3(
        signature = (sql, params = None, *, parameter_oids = None, timeout = None),
        text_signature = "($self, sql, params=(), *, parameter_oids=None, timeout=None)"
    )]
    pub fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
        parameter_oids: Option<&Bound<'py, PyAny>>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.lock_state().ensure_open(py)?;

        let call =
            prepare_statement_call(py, sql, params, parameter_oids, timeout).inspect_err(|_| {
                this.clear_state();
            })?;
        ensure_source_ready(
            py,
            &this.source,
            "cannot execute on a cursor whose pool is closed",
        )
        .inspect_err(|_| {
            this.clear_state();
        })?;

        let source = this.source.clone();
        let exec_lock = Arc::clone(&this.execute_lock);
        let lazy_begin = this.lazy_begin;
        let (gate, res) = py.detach(move || {
            block_on_db(async move {
                let gate = exec_lock.lock_owned().await;
                let query_res = run_statement(source, call, lazy_begin).await;
                (gate, query_res)
            })
        });

        let outcome = match res {
            Ok(query_result) => match build_cursor_result(py, &query_result) {
                Ok(built) => {
                    let mut state = this.lock_state();
                    if state.closed {
                        state.clear();
                    } else {
                        state.store(built.description, built.rows, built.rowcount, None);
                    }
                    Ok(())
                }
                Err(err) => {
                    this.clear_state();
                    Err(err)
                }
            },
            Err(err) => {
                this.clear_state();
                Err(postgres_error_to_py(py, err))
            }
        };
        drop(gate);
        outcome?;
        Ok(slf.clone())
    }

    /// Execute one SQL statement once per parameter set and sum the affected row counts.
    pub fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.lock_state().ensure_open(py)?;

        let plan = plan_for(sql).map_err(|e| {
            this.clear_state();
            postgres_error_to_py(py, e)
        })?;
        let iter = seq_of_params.try_iter().map_err(|e| {
            this.clear_state();
            convert_param_err(py, e)
        })?;
        let mut param_batches: Vec<Vec<Parameter>> = Vec::new();
        for entry in iter {
            let item = entry.map_err(|e| {
                this.clear_state();
                convert_param_err(py, e)
            })?;
            let (bound_params, _) =
                extract_cursor_parameters(py, &plan, &item, None).inspect_err(|_| {
                    this.clear_state();
                })?;
            param_batches.push(bound_params);
        }

        if param_batches.is_empty() {
            let mut state = this.lock_state();
            if state.closed {
                state.clear();
            } else {
                state.store(None, Vec::new(), 0, None);
            }
            return Ok(slf.clone());
        }

        match &this.source {
            CursorSource::Session(session) => {
                session.ensure_ordinary_reservable().map_err(|e| {
                    this.clear_state();
                    postgres_error_to_py(py, e)
                })?;
            }
            CursorSource::Pool(pool) => {
                if pool.is_closed() {
                    this.clear_state();
                    return Err(cursor_interface_err(
                        py,
                        "cannot execute on a cursor whose pool is closed",
                    ));
                }
            }
        }

        let source = this.source.clone();
        let exec_lock = Arc::clone(&this.execute_lock);
        let lazy_begin = this.lazy_begin;
        let sql_owned = plan.sql.to_string();
        let (gate, res) = py.detach(move || {
            block_on_db(async move {
                let gate = exec_lock.lock_owned().await;
                let outcome = match source {
                    CursorSource::Session(session) => {
                        if lazy_begin && session.transaction_status() == TransactionStatus::Idle {
                            let begin_gen = session.reserve_operation_generation();
                            if let Err(err) =
                                session.begin_as_with_generation(begin_gen, None).await
                            {
                                return (gate, Err(err));
                            }
                        }
                        let mut total_rows: i64 = 0;
                        for params in &param_batches {
                            let op_gen = session.reserve_operation_generation();
                            let budget = match CommandBudget::start_from_duration(None) {
                                Ok(b) => b,
                                Err(err) => {
                                    session.unreserve_operation_generation(op_gen);
                                    return (gate, Err(err));
                                }
                            };
                            match session
                                .query_as_with_generation_and_budget(
                                    op_gen, None, &sql_owned, params, None, budget,
                                )
                                .await
                            {
                                Ok(query_result) => {
                                    let count = rowcount_for_result(&query_result);
                                    if count > 0 {
                                        total_rows = total_rows.saturating_add(count);
                                    }
                                }
                                Err(err) => return (gate, Err(err)),
                            }
                        }
                        Ok::<i64, PostgresError>(total_rows)
                    }
                    CursorSource::Pool(pool) => {
                        let mut lease = match pool.acquire().await {
                            Ok(l) => l,
                            Err(err) => return (gate, Err(err)),
                        };
                        let mut total_rows: i64 = 0;
                        for params in &param_batches {
                            match lease.query(&sql_owned, params, None).await {
                                Ok(query_result) => {
                                    let count = rowcount_for_result(&query_result);
                                    if count > 0 {
                                        total_rows = total_rows.saturating_add(count);
                                    }
                                }
                                Err(err) => {
                                    if lease.is_closed() {
                                        lease.mark_retire();
                                    }
                                    let _ = lease.release().await;
                                    return (gate, Err(err));
                                }
                            }
                        }
                        let _ = lease.release().await;
                        Ok::<i64, PostgresError>(total_rows)
                    }
                };
                (gate, outcome)
            })
        });

        let outcome = match res {
            Ok(total_rows) => {
                let mut state = this.lock_state();
                if state.closed {
                    state.clear();
                } else {
                    state.store(None, Vec::new(), total_rows, None);
                }
                Ok(())
            }
            Err(err) => {
                this.clear_state();
                Err(postgres_error_to_py(py, err))
            }
        };
        drop(gate);
        outcome?;
        Ok(slf.clone())
    }

    /// Return the next buffered row tuple, or `None` when exhausted.
    pub fn fetchone(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        self.lock_state().fetchone(py)
    }

    /// Return up to `size` (or `arraysize` when `size` is `None`) buffered row tuples.
    #[pyo3(signature = (size = None))]
    pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Vec<Py<PyTuple>>> {
        let mut state = self.lock_state();
        state.ensure_open(py)?;
        let count = match size {
            Some(n) if n < 0 => {
                return Err(cursor_programming_err(
                    py,
                    "fetchmany size must be non-negative",
                ));
            }
            Some(n) => Some(usize::try_from(n).unwrap_or(usize::MAX)),
            None => None,
        };
        state.fetchmany(py, count)
    }

    /// Return all remaining buffered row tuples and leave the cursor exhausted.
    pub fn fetchall(&self, py: Python<'_>) -> PyResult<Vec<Py<PyTuple>>> {
        self.lock_state().fetchall(py)
    }

    /// Close the cursor and release buffered rows. Idempotent.
    pub fn close(&self) {
        self.lock_state().close();
    }
}

fn lock_cursor_state(state: &Mutex<CursorState>) -> std::sync::MutexGuard<'_, CursorState> {
    state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) struct AsyncOpControl {
    active_gen: Mutex<Option<OperationGeneration>>,
    cancelled: AtomicBool,
}

impl AsyncOpControl {
    pub(crate) fn new() -> Self {
        Self {
            active_gen: Mutex::new(None),
            cancelled: AtomicBool::new(false),
        }
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn register_op_gen(&self, op_gen: OperationGeneration) -> bool {
        let mut slot = self
            .active_gen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if self.cancelled.load(Ordering::Acquire) {
            return false;
        }
        *slot = Some(op_gen);
        true
    }

    pub(crate) fn clear_op_gen(&self) {
        let mut slot = self
            .active_gen
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot = None;
    }

    pub(crate) fn cancel(&self, session: Option<&SessionHandle>) {
        let active = {
            let mut slot = self
                .active_gen
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.cancelled.store(true, Ordering::Release);
            slot.take()
        };
        if let (Some(session), Some(op_gen)) = (session, active) {
            session.trigger_async_cancel(op_gen);
        }
    }
}

/// Execute one prepared [`StatementCall`] with cooperative cancellation checks and optional lazy `BEGIN`.
pub(crate) async fn run_statement_cancellable(
    source: CursorSource,
    call: StatementCall,
    budget: CommandBudget,
    lazy_begin: bool,
    control: Arc<AsyncOpControl>,
) -> Result<Option<QueryResult>, PostgresError> {
    if control.is_cancelled() {
        return Ok(None);
    }
    let query_res = match source {
        CursorSource::Session(session) => {
            if lazy_begin && session.transaction_status() == TransactionStatus::Idle {
                let begin_gen = session.reserve_operation_generation();
                if !control.register_op_gen(begin_gen) {
                    session.unreserve_operation_generation(begin_gen);
                    return Ok(None);
                }
                let begin_res = session.begin_as_with_generation(begin_gen, None).await;
                control.clear_op_gen();
                begin_res?;
                if control.is_cancelled() {
                    return Ok(None);
                }
            }
            let op_gen = session.reserve_operation_generation();
            if !control.register_op_gen(op_gen) {
                session.unreserve_operation_generation(op_gen);
                return Ok(None);
            }
            let res = session
                .query_as_with_generation_and_budget(
                    op_gen,
                    None,
                    &call.sql,
                    &call.params,
                    call.oids.as_deref(),
                    budget,
                )
                .await;
            control.clear_op_gen();
            res
        }
        CursorSource::Pool(pool) => {
            pool.query_oneshot(&call.sql, &call.params, call.oids.as_deref(), budget)
                .await
        }
    };
    query_res.map(Some)
}

/// Prepare and spawn one asynchronous single-call statement that materializes its result on completion.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_single_call<'py>(
    py: Python<'py>,
    source: CursorSource,
    closed_pool_message: &str,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
    parameter_oids: Option<&Bound<'py, PyAny>>,
    timeout: Option<&Bound<'py, PyAny>>,
    build: fn(Python<'_>, &QueryResult) -> PyResult<Py<PyAny>>,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
    let call = prepare_statement_call(py, sql, params, parameter_oids, timeout)?;
    ensure_source_ready(py, &source, closed_pool_message)?;
    spawn_single_call_prepared(py, source, call, build)
}

fn spawn_single_call_prepared<'py>(
    py: Python<'py>,
    source: CursorSource,
    call: StatementCall,
    build: fn(Python<'_>, &QueryResult) -> PyResult<Py<PyAny>>,
) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
    let budget = CommandBudget::start_from_duration(call.timeout)
        .map_err(|e| postgres_error_to_py(py, e))?;
    let cancel_session = match &source {
        CursorSource::Session(s) => Some(Arc::clone(s)),
        CursorSource::Pool(_) => None,
    };
    let control = Arc::new(AsyncOpControl::new());
    let control_cancel = Arc::clone(&control);
    spawn_rust_db_task_with_attr_and_cancel(
        py,
        run_statement_cancellable(source, call, budget, false, control),
        move |py, maybe_result| match maybe_result {
            Some(result) => build(py, &result),
            None => Ok(py.None()),
        },
        postgres_error_to_py,
        move || {
            control_cancel.cancel(cancel_session.as_deref());
        },
        POSTGRES_COMPLETION_DRAINER_ATTR,
    )
}

#[cfg(unix)]
static PG_GET_RUNNING_LOOP: PyOnceLock<Py<PyAny>> = PyOnceLock::new();

#[cfg(unix)]
fn pg_get_running_loop<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
    let getter = PG_GET_RUNNING_LOOP.get_or_try_init(py, || -> PyResult<Py<PyAny>> {
        Ok(py.import("asyncio")?.getattr("get_running_loop")?.unbind())
    })?;
    getter.bind(py).call0()
}

#[cfg(unix)]
struct PersistentLoopReader {
    loop_ptr: usize,
    loop_weakref: Py<PyWeakrefReference>,
    fd: std::os::fd::RawFd,
    reader: Py<PgLoopOneshot>,
    registered: Arc<AtomicBool>,
}

#[cfg(unix)]
impl std::fmt::Debug for PersistentLoopReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PersistentLoopReader")
            .field("fd", &self.fd)
            .field("registered", &self.registered.load(Ordering::Acquire))
            .finish()
    }
}

#[cfg(unix)]
impl PersistentLoopReader {
    fn matches_loop(&self, py: Python<'_>, loop_obj: &Bound<'_, PyAny>) -> bool {
        if self.loop_ptr != loop_obj.as_ptr() as usize || !self.registered.load(Ordering::Acquire) {
            return false;
        }
        self.loop_weakref
            .bind(py)
            .upgrade()
            .is_some_and(|live| live.is(loop_obj))
    }

    fn detach_on_loop(&self, py: Python<'_>, loop_obj: &Bound<'_, PyAny>) {
        if self.registered.swap(false, Ordering::AcqRel) {
            let _ = loop_obj.call_method1(intern!(py, "remove_reader"), (self.fd,));
        }
    }
}

#[cfg(unix)]
impl Drop for PersistentLoopReader {
    fn drop(&mut self) {
        if !self.registered.swap(false, Ordering::AcqRel) {
            return;
        }
        let fd = self.fd;
        let _ = Python::try_attach(|py| {
            if let Some(loop_obj) = self.loop_weakref.bind(py).upgrade()
                && let Ok(remove_fn) = loop_obj.getattr(intern!(py, "remove_reader"))
            {
                let _ = loop_obj.call_method1(intern!(py, "call_soon_threadsafe"), (remove_fn, fd));
            }
        });
    }
}

#[cfg(unix)]
impl LoopReaderSlot for PersistentLoopReader {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[cfg(unix)]
fn detach_lease_reader_on_loop(
    py: Python<'_>,
    lease: &mut PooledConnection,
    loop_obj: &Bound<'_, PyAny>,
) {
    if let Some(slot) = lease.take_loop_reader() {
        if let Some(reg) = slot.as_any().downcast_ref::<PersistentLoopReader>()
            && reg.loop_ptr == loop_obj.as_ptr() as usize
            && reg
                .loop_weakref
                .bind(py)
                .upgrade()
                .is_some_and(|live| live.is(loop_obj))
        {
            reg.detach_on_loop(py, loop_obj);
        }
        drop(slot);
    }
}

#[cfg(unix)]
fn cancel_and_retire_loop_lease(mut lease: PooledConnection) {
    let cancel_handle = lease.postgres_cancel_handle();
    lease.mark_retire();
    db_runtime_handle().spawn(async move {
        let _ = cancel_handle.cancel().await;
        let _ = lease.release().await;
    });
}

#[cfg(unix)]
struct InFlightLoopOneshot {
    lease: Option<PooledConnection>,
    loop_obj: Py<PyAny>,
    future: Py<PyAny>,
    exchange: Box<LoopCachedExchange>,
    build: fn(Python<'_>, &QueryResult) -> PyResult<Py<PyAny>>,
}

#[cfg(unix)]
impl Drop for InFlightLoopOneshot {
    fn drop(&mut self) {
        if let Some(mut lease) = self.lease.take() {
            if lease.exchange_state != ExchangeState::Idle {
                lease.mark_retire();
            }
            drop(lease);
        }
    }
}

#[cfg(unix)]
#[pyclass(
    name = "_PgLoopOneshot",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
struct PgLoopOneshot {
    fd: std::os::fd::RawFd,
    loop_weakref: Py<PyWeakrefReference>,
    registered: Arc<AtomicBool>,
    state: Mutex<Option<InFlightLoopOneshot>>,
}

#[cfg(unix)]
#[pymethods]
impl PgLoopOneshot {
    #[pyo3(signature = (done_fut = None))]
    fn __call__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        done_fut: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        if let Some(fut) = done_fut {
            let cancelled = fut
                .call_method0(intern!(py, "cancelled"))
                .and_then(|v| v.extract::<bool>())
                .unwrap_or(false);
            if cancelled {
                let taken = slf
                    .get()
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .take();
                if let Some(mut in_flight) = taken
                    && let Some(mut lease) = in_flight.lease.take()
                {
                    detach_lease_reader_on_loop(py, &mut lease, in_flight.loop_obj.bind(py));
                    cancel_and_retire_loop_lease(lease);
                }
            }
            return Ok(());
        }

        let Some(mut in_flight) = slf
            .get()
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
        else {
            if slf.get().registered.swap(false, Ordering::AcqRel)
                && let Some(loop_obj) = slf.get().loop_weakref.bind(py).upgrade()
            {
                let _ = loop_obj.call_method1(intern!(py, "remove_reader"), (slf.get().fd,));
            }
            return Ok(());
        };

        let fut_is_done = in_flight
            .future
            .bind(py)
            .call_method0(intern!(py, "done"))
            .and_then(|v| v.extract::<bool>())
            .unwrap_or(true);
        if fut_is_done {
            if let Some(mut lease) = in_flight.lease.take() {
                detach_lease_reader_on_loop(py, &mut lease, in_flight.loop_obj.bind(py));
                cancel_and_retire_loop_lease(lease);
            }
            return Ok(());
        }

        let step = {
            let lease = in_flight
                .lease
                .as_mut()
                .expect("in-flight loop query owns a lease");
            lease.drive_loop_cached_read(&mut in_flight.exchange)
        };

        match step {
            LoopReadStep::Pending => {
                let mut guard = slf
                    .get()
                    .state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                *guard = Some(in_flight);
                Ok(())
            }
            LoopReadStep::Done(outcome) => {
                let fut = in_flight.future.bind(py);
                let _ = fut.call_method1(intern!(py, "remove_done_callback"), (slf,));
                let mut lease = in_flight
                    .lease
                    .take()
                    .expect("in-flight loop query owns a lease");
                match outcome {
                    Ok(query_result) => {
                        let is_clean = !in_flight.exchange.saw_session_change
                            && !lease.is_closed()
                            && lease.exchange_state == ExchangeState::Idle
                            && lease.transaction_status == TransactionStatus::Idle
                            && lease.open_portals.is_empty()
                            && lease.notifications.is_empty()
                            && is_clean_oneshot_query(
                                &in_flight.exchange.sql,
                                &query_result.command_tag,
                            );
                        if is_clean {
                            lease.return_clean();
                        } else {
                            detach_lease_reader_on_loop(
                                py,
                                &mut lease,
                                in_flight.loop_obj.bind(py),
                            );
                            db_runtime_handle().spawn(async move {
                                let _ = lease.release().await;
                            });
                        }
                        match (in_flight.build)(py, &query_result) {
                            Ok(val) => {
                                let _ = fut.call_method1(intern!(py, "set_result"), (val,));
                            }
                            Err(py_err) => {
                                let _ = fut.call_method1(
                                    intern!(py, "set_exception"),
                                    (py_err.into_value(py),),
                                );
                            }
                        }
                    }
                    Err(pg_err) => {
                        detach_lease_reader_on_loop(py, &mut lease, in_flight.loop_obj.bind(py));
                        if lease.is_closed() {
                            lease.mark_retire();
                        }
                        db_runtime_handle().spawn(async move {
                            let _ = lease.release().await;
                        });
                        let py_err = postgres_error_to_py(py, pg_err);
                        let _ = fut
                            .call_method1(intern!(py, "set_exception"), (py_err.into_value(py),));
                    }
                }
                Ok(())
            }
        }
    }
}

/// Run one pooled async single-call statement, using the loop-driven non-blocking fast path when
/// an idle plain-socket connection already has the statement cached, and falling back to Tokio
/// otherwise.
#[allow(clippy::too_many_arguments)]
pub(crate) fn spawn_pool_single_call<'py>(
    py: Python<'py>,
    pool: Arc<PostgresPool>,
    closed_pool_message: &str,
    sql: &str,
    params: Option<&Bound<'py, PyAny>>,
    parameter_oids: Option<&Bound<'py, PyAny>>,
    timeout: Option<&Bound<'py, PyAny>>,
    build: fn(Python<'_>, &QueryResult) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let call = prepare_statement_call(py, sql, params, parameter_oids, timeout)?;
    let source = CursorSource::Pool(Arc::clone(&pool));
    ensure_source_ready(py, &source, closed_pool_message)?;

    #[cfg(unix)]
    if call.timeout.is_none() && call.oids.is_none() {
        let loop_obj = pg_get_running_loop(py)?;
        let cached_loop_io = loop_obj
            .getattr(intern!(py, "_vertebra_pg_loop_io"))
            .ok()
            .and_then(|flag| flag.is_truthy().ok());
        if cached_loop_io != Some(false)
            && let Ok(Some(mut lease)) = pool.try_acquire_idle()
        {
            match lease.prepare_loop_cached_query(&call.sql, &call.params) {
                Ok(LoopPrepareOutcome::Ready { fd, exchange }) => {
                    let existing_reader: Option<Py<PgLoopOneshot>> = lease
                        .loop_reader()
                        .and_then(|slot| slot.as_any().downcast_ref::<PersistentLoopReader>())
                        .filter(|reg| reg.fd == fd && reg.matches_loop(py, &loop_obj))
                        .map(|reg| reg.reader.clone_ref(py));

                    let driver_opt: Option<Py<PgLoopOneshot>> = if let Some(reader) =
                        existing_reader
                    {
                        Some(reader)
                    } else {
                        detach_lease_reader_on_loop(py, &mut lease, &loop_obj);
                        let loop_weak = PyWeakrefReference::new(&loop_obj)?;
                        let registered = Arc::new(AtomicBool::new(false));
                        let driver_py = Py::new(
                            py,
                            PgLoopOneshot {
                                fd,
                                loop_weakref: loop_weak.clone().unbind(),
                                registered: Arc::clone(&registered),
                                state: Mutex::new(None),
                            },
                        )?;
                        if loop_obj
                            .call_method1(intern!(py, "add_reader"), (fd, driver_py.bind(py)))
                            .is_err()
                        {
                            let _ = loop_obj.setattr(intern!(py, "_vertebra_pg_loop_io"), false);
                            None
                        } else {
                            registered.store(true, Ordering::Release);
                            if cached_loop_io.is_none() {
                                let _ = loop_obj.setattr(intern!(py, "_vertebra_pg_loop_io"), true);
                            }
                            lease.set_loop_reader(Box::new(PersistentLoopReader {
                                loop_ptr: loop_obj.as_ptr() as usize,
                                loop_weakref: loop_weak.unbind(),
                                fd,
                                reader: driver_py.clone_ref(py),
                                registered,
                            }));
                            Some(driver_py)
                        }
                    };

                    let Some(driver_py) = driver_opt else {
                        lease.return_clean();
                        return Ok(spawn_single_call_prepared(py, source, call, build)?
                            .into_any()
                            .unbind());
                    };
                    match lease.try_write_prepared_loop_query() {
                        LoopWriteOutcome::SentAll => {
                            let fut = loop_obj.call_method0(intern!(py, "create_future"))?;
                            {
                                let mut guard = driver_py
                                    .get()
                                    .state
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                *guard = Some(InFlightLoopOneshot {
                                    lease: Some(lease),
                                    loop_obj: loop_obj.clone().unbind(),
                                    future: fut.clone().unbind(),
                                    exchange,
                                    build,
                                });
                            }
                            fut.call_method1(
                                intern!(py, "add_done_callback"),
                                (driver_py.bind(py),),
                            )?;
                            return Ok(fut.unbind());
                        }
                        LoopWriteOutcome::WouldBlock => {
                            detach_lease_reader_on_loop(py, &mut lease, &loop_obj);
                            lease.return_clean();
                        }
                        LoopWriteOutcome::PartialWrite => {
                            detach_lease_reader_on_loop(py, &mut lease, &loop_obj);
                            let cancel_handle = lease.postgres_cancel_handle();
                            let sql_owned = exchange.sql.clone();
                            let awaitable = spawn_rust_db_task_with_attr_and_cancel(
                                py,
                                async move {
                                    let outcome =
                                        lease.finish_partial_loop_cached_query(*exchange).await;
                                    match outcome {
                                        Ok((query_result, saw_change)) => {
                                            let is_clean = !saw_change
                                                && !lease.is_closed()
                                                && lease.exchange_state == ExchangeState::Idle
                                                && lease.transaction_status
                                                    == TransactionStatus::Idle
                                                && lease.open_portals.is_empty()
                                                && lease.notifications.is_empty()
                                                && is_clean_oneshot_query(
                                                    &sql_owned,
                                                    &query_result.command_tag,
                                                );
                                            if is_clean {
                                                lease.return_clean();
                                            } else {
                                                let _ = lease.release().await;
                                            }
                                            Ok(query_result)
                                        }
                                        Err(err) => {
                                            if lease.is_closed() {
                                                lease.mark_retire();
                                            }
                                            let _ = lease.release().await;
                                            Err(err)
                                        }
                                    }
                                },
                                move |py, result| build(py, &result),
                                postgres_error_to_py,
                                move || {
                                    db_runtime_handle().spawn(async move {
                                        let _ = cancel_handle.cancel().await;
                                    });
                                },
                                POSTGRES_COMPLETION_DRAINER_ATTR,
                            )?;
                            return Ok(awaitable.into_any().unbind());
                        }
                        LoopWriteOutcome::IoError(err) => {
                            detach_lease_reader_on_loop(py, &mut lease, &loop_obj);
                            lease.mark_retire();
                            db_runtime_handle().spawn(async move {
                                let _ = lease.release().await;
                            });
                            return Ok(ready_err(py, postgres_error_to_py(py, err))?.into_any());
                        }
                    }
                }
                Ok(LoopPrepareOutcome::Fallback) => {
                    detach_lease_reader_on_loop(py, &mut lease, &loop_obj);
                    lease.return_clean();
                }
                Err(err) => {
                    lease.return_clean();
                    return Ok(ready_err(py, postgres_error_to_py(py, err))?.into_any());
                }
            }
        }
    }

    Ok(spawn_single_call_prepared(py, source, call, build)?
        .into_any()
        .unbind())
}

fn ready_from(py: Python<'_>, outcome: PyResult<Py<PyAny>>) -> PyResult<Py<ReadyAwaitable>> {
    match outcome {
        Ok(value) => ready_ok(py, value),
        Err(err) => ready_err(py, err),
    }
}

struct ExecReleaseGuard {
    in_flight: Arc<Mutex<Option<u64>>>,
    exec_id: u64,
}

impl ExecReleaseGuard {
    fn is_active(&self) -> bool {
        let slot = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *slot == Some(self.exec_id)
    }

    fn release(&self) {
        let mut slot = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if *slot == Some(self.exec_id) {
            *slot = None;
        }
    }
}

impl Drop for ExecReleaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

/// Asynchronous PEP 249 cursor over a [`SessionHandle`] or [`PostgresPool`]
/// (`vertebra._postgres.AsyncPostgresCursor`).
#[pyclass(
    name = "AsyncPostgresCursor",
    module = "vertebra._postgres",
    frozen,
    immutable_type
)]
pub struct PyAsyncPostgresCursor {
    source: CursorSource,
    state: Arc<Mutex<CursorState>>,
    execute_lock: Arc<tokio::sync::Mutex<()>>,
    in_flight: Arc<Mutex<Option<u64>>>,
    next_exec_id: AtomicU64,
    lazy_begin: bool,
}

impl PyAsyncPostgresCursor {
    /// Construct an async cursor bound to `session` without performing any network I/O.
    pub(crate) fn new(session: Arc<SessionHandle>, lazy_begin: bool) -> Self {
        Self {
            source: CursorSource::Session(session),
            state: Arc::new(Mutex::new(CursorState::new())),
            execute_lock: Arc::new(tokio::sync::Mutex::new(())),
            in_flight: Arc::new(Mutex::new(None)),
            next_exec_id: AtomicU64::new(1),
            lazy_begin,
        }
    }

    /// Construct a pool-backed async cursor using the one-shot query path without performing any I/O.
    pub(crate) fn from_pool(pool: Arc<PostgresPool>) -> Self {
        Self {
            source: CursorSource::Pool(pool),
            state: Arc::new(Mutex::new(CursorState::new())),
            execute_lock: Arc::new(tokio::sync::Mutex::new(())),
            in_flight: Arc::new(Mutex::new(None)),
            next_exec_id: AtomicU64::new(1),
            lazy_begin: false,
        }
    }

    fn lock_state(&self) -> std::sync::MutexGuard<'_, CursorState> {
        lock_cursor_state(&self.state)
    }

    fn clear_state(&self) {
        self.lock_state().clear();
    }

    fn ensure_not_executing(&self, py: Python<'_>) -> PyResult<()> {
        let slot = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_some() {
            return Err(cursor_interface_err(py, "cursor is already executing"));
        }
        Ok(())
    }

    fn try_claim_execute(&self, py: Python<'_>) -> PyResult<ExecReleaseGuard> {
        let mut slot = self
            .in_flight
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if slot.is_some() {
            return Err(cursor_interface_err(py, "cursor is already executing"));
        }
        let exec_id = self.next_exec_id.fetch_add(1, Ordering::Relaxed);
        *slot = Some(exec_id);
        Ok(ExecReleaseGuard {
            in_flight: Arc::clone(&self.in_flight),
            exec_id,
        })
    }
}

#[pymethods]
impl PyAsyncPostgresCursor {
    /// Return the 7-item PEP 249 column description tuple, or `None` when no result set is present.
    #[getter]
    pub fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        self.lock_state()
            .description
            .as_ref()
            .map(|desc| desc.clone_ref(py))
    }

    /// Return the PEP 249 row count (`-1` before execute or when the command reports no count).
    #[getter]
    pub fn rowcount(&self) -> i64 {
        self.lock_state().rowcount
    }

    /// Return `None` (PostgreSQL does not populate a PEP 249 `lastrowid`).
    #[getter]
    pub fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.lock_state()
            .lastrowid
            .as_ref()
            .map(|val| val.clone_ref(py))
    }

    /// Return the default batch size used by `fetchmany()`.
    #[getter]
    pub fn arraysize(&self) -> usize {
        self.lock_state().arraysize
    }

    /// Set the default batch size used by `fetchmany()` (`>= 1`).
    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.lock_state().set_arraysize(py, value)
    }

    /// Execute one SQL statement asynchronously on the client session and buffer its result set.
    #[pyo3(
        signature = (sql, params = None, *, parameter_oids = None, timeout = None),
        text_signature = "($self, sql, params=(), *, parameter_oids=None, timeout=None)"
    )]
    pub fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
        parameter_oids: Option<&Bound<'py, PyAny>>,
        timeout: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let this = slf.get();
        this.lock_state().ensure_open(py)?;
        this.ensure_not_executing(py)?;

        let call =
            prepare_statement_call(py, sql, params, parameter_oids, timeout).inspect_err(|_| {
                this.clear_state();
            })?;
        ensure_source_ready(
            py,
            &this.source,
            "cannot execute on a cursor whose pool is closed",
        )
        .inspect_err(|_| {
            this.clear_state();
        })?;

        let guard = this.try_claim_execute(py)?;
        let exec_id = guard.exec_id;
        let budget = match CommandBudget::start_from_duration(call.timeout) {
            Ok(b) => b,
            Err(err) => {
                drop(guard);
                this.clear_state();
                return Err(postgres_error_to_py(py, err));
            }
        };

        let source = this.source.clone();
        let cancel_session = match &this.source {
            CursorSource::Session(s) => Some(Arc::clone(s)),
            CursorSource::Pool(_) => None,
        };
        let exec_lock = Arc::clone(&this.execute_lock);
        let state_ok = Arc::clone(&this.state);
        let state_err = Arc::clone(&this.state);
        let state_cancel = Arc::clone(&this.state);
        let in_flight_cancel = Arc::clone(&this.in_flight);
        let control = Arc::new(AsyncOpControl::new());
        let control_ok = Arc::clone(&control);
        let control_cancel = Arc::clone(&control);
        let lazy_begin = this.lazy_begin;
        let cursor_py: Py<Self> = slf.clone().unbind();

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                let _wire_gate = exec_lock.lock_owned().await;
                match run_statement_cancellable(source, call, budget, lazy_begin, control).await {
                    Ok(maybe_res) => Ok((maybe_res, guard)),
                    Err(err) => Err((err, guard)),
                }
            },
            move |py, (maybe_result, guard)| {
                let Some(query_result) = maybe_result else {
                    return Ok(cursor_py.into_any());
                };
                if control_ok.is_cancelled() || !guard.is_active() {
                    return Ok(cursor_py.into_any());
                }
                match build_cursor_result(py, &query_result) {
                    Ok(built) => {
                        let mut state = lock_cursor_state(&state_ok);
                        if state.closed {
                            state.clear();
                        } else {
                            state.store(built.description, built.rows, built.rowcount, None);
                        }
                        drop(state);
                        drop(guard);
                        Ok(cursor_py.into_any())
                    }
                    Err(err) => {
                        lock_cursor_state(&state_ok).clear();
                        drop(guard);
                        Err(err)
                    }
                }
            },
            move |py, (err, guard)| {
                if guard.is_active() {
                    lock_cursor_state(&state_err).clear();
                }
                drop(guard);
                postgres_error_to_py(py, err)
            },
            move || {
                control_cancel.cancel(cancel_session.as_deref());
                let mut slot = in_flight_cancel
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if *slot == Some(exec_id) {
                    *slot = None;
                    lock_cursor_state(&state_cancel).clear();
                }
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Execute one SQL statement asynchronously once per parameter set and sum affected row counts.
    pub fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, RustDbTaskAwaitable>> {
        let py = slf.py();
        let this = slf.get();
        this.lock_state().ensure_open(py)?;
        this.ensure_not_executing(py)?;

        let plan = plan_for(sql).map_err(|e| {
            this.clear_state();
            postgres_error_to_py(py, e)
        })?;
        let iter = seq_of_params.try_iter().map_err(|e| {
            this.clear_state();
            convert_param_err(py, e)
        })?;
        let mut param_batches: Vec<Vec<Parameter>> = Vec::new();
        for entry in iter {
            let item = entry.map_err(|e| {
                this.clear_state();
                convert_param_err(py, e)
            })?;
            let (bound_params, _) =
                extract_cursor_parameters(py, &plan, &item, None).inspect_err(|_| {
                    this.clear_state();
                })?;
            param_batches.push(bound_params);
        }

        if !param_batches.is_empty() {
            match &this.source {
                CursorSource::Session(session) => {
                    session.ensure_ordinary_reservable().map_err(|e| {
                        this.clear_state();
                        postgres_error_to_py(py, e)
                    })?;
                }
                CursorSource::Pool(pool) => {
                    if pool.is_closed() {
                        this.clear_state();
                        return Err(cursor_interface_err(
                            py,
                            "cannot execute on a cursor whose pool is closed",
                        ));
                    }
                }
            }
        }

        let guard = this.try_claim_execute(py)?;
        let exec_id = guard.exec_id;
        let source = this.source.clone();
        let cancel_session = match &this.source {
            CursorSource::Session(s) => Some(Arc::clone(s)),
            CursorSource::Pool(_) => None,
        };
        let exec_lock = Arc::clone(&this.execute_lock);
        let state_ok = Arc::clone(&this.state);
        let state_err = Arc::clone(&this.state);
        let state_cancel = Arc::clone(&this.state);
        let in_flight_cancel = Arc::clone(&this.in_flight);
        let control = Arc::new(AsyncOpControl::new());
        let control_ok = Arc::clone(&control);
        let control_cancel = Arc::clone(&control);
        let lazy_begin = this.lazy_begin;
        let sql_owned = plan.sql.to_string();
        let cursor_py: Py<Self> = slf.clone().unbind();

        spawn_rust_db_task_with_attr_and_cancel(
            py,
            async move {
                if param_batches.is_empty() {
                    return Ok((Some(0i64), guard));
                }
                let _wire_gate = exec_lock.lock_owned().await;
                if control.is_cancelled() {
                    return Ok((None, guard));
                }
                match source {
                    CursorSource::Session(session) => {
                        if lazy_begin && session.transaction_status() == TransactionStatus::Idle {
                            let begin_gen = session.reserve_operation_generation();
                            if !control.register_op_gen(begin_gen) {
                                session.unreserve_operation_generation(begin_gen);
                                return Ok((None, guard));
                            }
                            let begin_res = session.begin_as_with_generation(begin_gen, None).await;
                            control.clear_op_gen();
                            if let Err(err) = begin_res {
                                return Err((err, guard));
                            }
                            if control.is_cancelled() {
                                return Ok((None, guard));
                            }
                        }
                        let mut total_rows: i64 = 0;
                        for params in &param_batches {
                            if control.is_cancelled() {
                                return Ok((None, guard));
                            }
                            let op_gen = session.reserve_operation_generation();
                            if !control.register_op_gen(op_gen) {
                                session.unreserve_operation_generation(op_gen);
                                return Ok((None, guard));
                            }
                            let budget = match CommandBudget::start_from_duration(None) {
                                Ok(b) => b,
                                Err(err) => {
                                    control.clear_op_gen();
                                    session.unreserve_operation_generation(op_gen);
                                    return Err((err, guard));
                                }
                            };
                            let query_res = session
                                .query_as_with_generation_and_budget(
                                    op_gen, None, &sql_owned, params, None, budget,
                                )
                                .await;
                            control.clear_op_gen();
                            match query_res {
                                Ok(query_result) => {
                                    let count = rowcount_for_result(&query_result);
                                    if count > 0 {
                                        total_rows = total_rows.saturating_add(count);
                                    }
                                }
                                Err(err) => return Err((err, guard)),
                            }
                        }
                        Ok((Some(total_rows), guard))
                    }
                    CursorSource::Pool(pool) => {
                        let mut lease = match pool.acquire().await {
                            Ok(l) => l,
                            Err(err) => return Err((err, guard)),
                        };
                        let mut total_rows: i64 = 0;
                        for params in &param_batches {
                            if control.is_cancelled() {
                                lease.mark_retire();
                                let _ = lease.release().await;
                                return Ok((None, guard));
                            }
                            match lease.query(&sql_owned, params, None).await {
                                Ok(query_result) => {
                                    let count = rowcount_for_result(&query_result);
                                    if count > 0 {
                                        total_rows = total_rows.saturating_add(count);
                                    }
                                }
                                Err(err) => {
                                    if lease.is_closed() {
                                        lease.mark_retire();
                                    }
                                    let _ = lease.release().await;
                                    return Err((err, guard));
                                }
                            }
                        }
                        let _ = lease.release().await;
                        Ok((Some(total_rows), guard))
                    }
                }
            },
            move |_py, (maybe_total, guard)| {
                let Some(total_rows) = maybe_total else {
                    return Ok(cursor_py.into_any());
                };
                if control_ok.is_cancelled() || !guard.is_active() {
                    return Ok(cursor_py.into_any());
                }
                let mut state = lock_cursor_state(&state_ok);
                if state.closed {
                    state.clear();
                } else {
                    state.store(None, Vec::new(), total_rows, None);
                }
                drop(state);
                drop(guard);
                Ok(cursor_py.into_any())
            },
            move |py, (err, guard)| {
                if guard.is_active() {
                    lock_cursor_state(&state_err).clear();
                }
                drop(guard);
                postgres_error_to_py(py, err)
            },
            move || {
                control_cancel.cancel(cancel_session.as_deref());
                let mut slot = in_flight_cancel
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if *slot == Some(exec_id) {
                    *slot = None;
                    lock_cursor_state(&state_cancel).clear();
                }
            },
            POSTGRES_COMPLETION_DRAINER_ATTR,
        )
    }

    /// Return the next buffered row tuple, or `None` when exhausted, without network I/O.
    pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let outcome = lock_cursor_state(&self.state)
            .fetchone(py)
            .map(|row| row.map_or_else(|| py.None(), Py::into_any));
        ready_from(py, outcome)
    }

    /// Return up to `size` (or `arraysize` when `size` is `None`) buffered row tuples without I/O.
    #[pyo3(signature = (size = None))]
    pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>> {
        let outcome = (|| -> PyResult<Py<PyAny>> {
            let mut locked = lock_cursor_state(&self.state);
            locked.ensure_open(py)?;
            let count = match size {
                Some(n) if n < 0 => {
                    return Err(cursor_programming_err(
                        py,
                        "fetchmany size must be non-negative",
                    ));
                }
                Some(n) => Some(usize::try_from(n).unwrap_or(usize::MAX)),
                None => None,
            };
            let rows = locked.fetchmany(py, count)?;
            Ok(rows.into_pyobject(py)?.into_any().unbind())
        })();
        ready_from(py, outcome)
    }

    /// Return all remaining buffered row tuples and leave the cursor exhausted without I/O.
    pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let outcome = (|| -> PyResult<Py<PyAny>> {
            let rows = lock_cursor_state(&self.state).fetchall(py)?;
            Ok(rows.into_pyobject(py)?.into_any().unbind())
        })();
        ready_from(py, outcome)
    }

    /// Close the cursor and release buffered rows without I/O. Idempotent.
    pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        lock_cursor_state(&self.state).close();
        ready_ok(py, py.None())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::{PostgresError, postgres_error_to_py};
    use crate::result::{Format, TransactionStatus};

    fn sample_rows(py: Python<'_>, count: i64) -> Vec<Py<PyTuple>> {
        (1..=count)
            .map(|i| PyTuple::new(py, [i]).expect("tuple").unbind())
            .collect()
    }

    fn sample_description(py: Python<'_>) -> Py<PyTuple> {
        let cols = vec![Column::try_new("id", 23, Format::Binary, -1, 0, 0).expect("column")];
        describe_columns(py, &cols).expect("describe_columns")
    }

    fn err_type_name(py: Python<'_>, err: &PyErr) -> String {
        err.get_type(py)
            .name()
            .expect("type name")
            .to_str()
            .expect("utf8")
            .to_owned()
    }

    #[test]
    fn cursor_fresh_state_defaults() {
        let state = CursorState::new();
        assert_eq!(state.rowcount, -1);
        assert_eq!(state.arraysize, 1);
        assert!(state.description.is_none());
        assert!(state.lastrowid.is_none());
        assert!(!state.executed);
        assert!(!state.closed);
    }

    #[test]
    fn cursor_fetch_before_execute_raises_programming_error() {
        Python::attach(|py| {
            let mut state = CursorState::new();
            let err = state
                .fetchone(py)
                .expect_err("fetchone before execute must fail");
            assert_eq!(err_type_name(py, &err), "ProgrammingError");
        });
    }

    #[test]
    fn cursor_fetch_after_command_raises_programming_error_and_preserves_rowcount() {
        Python::attach(|py| {
            let mut state = CursorState::new();
            state.store(None, Vec::new(), 3, None);
            assert_eq!(state.rowcount, 3);
            assert!(state.description.is_none());
            let err = state
                .fetchall(py)
                .expect_err("fetchall after command must fail");
            assert_eq!(err_type_name(py, &err), "ProgrammingError");
            assert_eq!(state.rowcount, 3);
        });
    }

    #[test]
    fn cursor_empty_result_set_returns_empty_with_description() {
        Python::attach(|py| {
            let mut state = CursorState::new();
            let desc = sample_description(py);
            state.store(Some(desc), Vec::new(), 0, None);
            assert!(state.description.is_some());
            assert_eq!(state.rowcount, 0);
            let rows = state.fetchall(py).expect("fetchall on empty result set");
            assert!(rows.is_empty());
        });
    }

    #[test]
    fn cursor_fetchmany_default_uses_arraysize() {
        Python::attach(|py| {
            let mut state = CursorState::new();
            state.set_arraysize(py, 2).expect("arraysize 2");
            let desc = sample_description(py);
            state.store(Some(desc), sample_rows(py, 5), 5, None);

            let b1 = state.fetchmany(py, None).expect("batch 1");
            assert_eq!(b1.len(), 2);
            let b2 = state.fetchmany(py, None).expect("batch 2");
            assert_eq!(b2.len(), 2);
            let b3 = state.fetchmany(py, None).expect("batch 3");
            assert_eq!(b3.len(), 1);
            let b4 = state.fetchmany(py, None).expect("batch 4");
            assert!(b4.is_empty());
        });
    }

    #[test]
    fn cursor_fetchall_returns_remainder_then_empty() {
        Python::attach(|py| {
            let mut state = CursorState::new();
            let desc = sample_description(py);
            state.store(Some(desc), sample_rows(py, 5), 5, None);

            let first = state.fetchone(py).expect("fetchone").expect("row 1");
            assert_eq!(
                first
                    .bind(py)
                    .get_item(0)
                    .expect("item")
                    .extract::<i64>()
                    .expect("i64"),
                1
            );

            let rest = state.fetchall(py).expect("fetchall remainder");
            assert_eq!(rest.len(), 4);
            let vals: Vec<i64> = rest
                .iter()
                .map(|r| {
                    r.bind(py)
                        .get_item(0)
                        .expect("item")
                        .extract::<i64>()
                        .expect("i64")
                })
                .collect();
            assert_eq!(vals, vec![2, 3, 4, 5]);

            let exhausted = state.fetchall(py).expect("fetchall exhausted");
            assert!(exhausted.is_empty());
        });
    }

    #[test]
    fn cursor_close_raises_interface_error_and_is_idempotent() {
        Python::attach(|py| {
            let mut state = CursorState::new();
            state.close();
            assert!(state.closed);
            state.close();
            assert!(state.closed);

            let err = state.fetchone(py).expect_err("fetchone after close");
            assert_eq!(err_type_name(py, &err), "InterfaceError");
        });
    }

    #[test]
    fn cursor_describe_columns_builds_seven_item_tuples_with_oid() {
        Python::attach(|py| {
            let cols = vec![
                Column::try_new("id", 23, Format::Binary, -1, 0, 0).expect("col 0"),
                Column::try_new("name", 25, Format::Text, -1, 0, 0).expect("col 1"),
            ];
            let desc = describe_columns(py, &cols).expect("describe_columns");
            let outer = desc.bind(py);
            assert_eq!(outer.len(), 2);

            let expected = [("id", 23u32), ("name", 25u32)];
            for (idx, (exp_name, exp_oid)) in expected.iter().enumerate() {
                let entry_obj = outer.get_item(idx).expect("entry");
                let entry = entry_obj.cast::<PyTuple>().expect("entry tuple");
                assert_eq!(entry.len(), 7);
                assert_eq!(
                    entry
                        .get_item(0)
                        .expect("name")
                        .extract::<String>()
                        .expect("str"),
                    *exp_name
                );
                assert_eq!(
                    entry
                        .get_item(1)
                        .expect("oid")
                        .extract::<u32>()
                        .expect("u32"),
                    *exp_oid
                );
                for pos in 2..7 {
                    assert!(entry.get_item(pos).expect("none slot").is_none());
                }
            }
        });
    }

    #[test]
    fn cursor_row_to_tuple_projects_values_and_top_level_json_null() {
        Python::attach(|py| {
            let row = vec![
                PgValue::Int4(42),
                PgValue::Text("hello".to_owned()),
                PgValue::Null,
                PgValue::Json(b"null".to_vec()),
                PgValue::Json(b"  null \n".to_vec()),
            ];
            let py_row = row_to_tuple(py, &row).expect("row_to_tuple");
            let bound = py_row.bind(py);
            assert_eq!(bound.len(), 5);
            assert_eq!(
                bound
                    .get_item(0)
                    .expect("c0")
                    .extract::<i32>()
                    .expect("i32"),
                42
            );
            assert_eq!(
                bound
                    .get_item(1)
                    .expect("c1")
                    .extract::<String>()
                    .expect("str"),
                "hello"
            );
            assert!(bound.get_item(2).expect("c2").is_none());
            assert!(bound.get_item(3).expect("c3").is_none());
            assert!(bound.get_item(4).expect("c4").is_none());
        });
    }

    #[test]
    fn cursor_rowcount_for_result_handles_sets_commands_and_ddl() {
        let col = Column::try_new("id", 23, Format::Binary, -1, 0, 0).expect("col");
        let select_res = QueryResult::try_new(
            vec![col],
            vec![vec![PgValue::Int4(1)], vec![PgValue::Int4(2)]],
            2,
            "SELECT 2",
            TransactionStatus::Idle,
        )
        .expect("select");
        assert_eq!(rowcount_for_result(&select_res), 2);

        let insert_res = QueryResult::try_new(
            Vec::new(),
            Vec::new(),
            3,
            "INSERT 0 3",
            TransactionStatus::Idle,
        )
        .expect("insert");
        assert_eq!(rowcount_for_result(&insert_res), 3);

        let ddl_res = QueryResult::try_new(
            Vec::new(),
            Vec::new(),
            0,
            "CREATE TABLE",
            TransactionStatus::Idle,
        )
        .expect("ddl");
        assert_eq!(rowcount_for_result(&ddl_res), -1);
    }

    #[test]
    fn cursor_state_error_classes_and_messages() {
        Python::attach(|py| {
            let iface = cursor_interface_err(py, "x");
            assert_eq!(err_type_name(py, &iface), "InterfaceError");
            assert_eq!(
                iface.value(py).str().expect("str").to_str().expect("utf8"),
                "x"
            );

            let prog = cursor_programming_err(py, "x");
            assert_eq!(err_type_name(py, &prog), "ProgrammingError");
            assert_eq!(
                prog.value(py).str().expect("str").to_str().expect("utf8"),
                "x"
            );
        });
    }

    #[test]
    fn cursor_server_error_sets_sqlstate_and_is_transient() {
        Python::attach(|py| {
            let err = PostgresError::server("ERROR", "40001", "serialization failure", Vec::new())
                .expect("server error");
            let py_err = postgres_error_to_py(py, err);
            assert_eq!(err_type_name(py, &py_err), "SerializationFailure");
            let val = py_err.value(py);
            assert_eq!(
                val.getattr("sqlstate")
                    .expect("sqlstate")
                    .extract::<String>()
                    .expect("str"),
                "40001"
            );
            assert!(
                val.getattr("is_transient")
                    .expect("is_transient")
                    .extract::<bool>()
                    .expect("bool")
            );
        });
    }

    #[test]
    fn single_call_builders_cover_no_columns_no_rows_dml_and_ddl() {
        Python::attach(|py| {
            let col_a = Column::try_new("a", 23, Format::Binary, -1, 0, 0).expect("col a");
            let col_b = Column::try_new("b", 25, Format::Text, -1, 0, 0).expect("col b");
            let col_a_dup = Column::try_new("a", 23, Format::Binary, -1, 0, 0).expect("col a dup");

            let select_res = QueryResult::try_new(
                vec![col_a.clone(), col_b.clone()],
                vec![
                    vec![PgValue::Int4(1), PgValue::Text("x".to_owned())],
                    vec![PgValue::Null, PgValue::Text("y".to_owned())],
                ],
                2,
                "SELECT 2",
                TransactionStatus::Idle,
            )
            .expect("select_res");

            let rn_obj = rows_and_names(py, &select_res).expect("rows_and_names select");
            let rn = rn_obj.bind(py).cast::<PyTuple>().expect("rn tuple");
            let rows = rn
                .get_item(0)
                .expect("rows")
                .cast_into::<PyList>()
                .expect("list");
            let names = rn
                .get_item(1)
                .expect("names")
                .cast_into::<PyTuple>()
                .expect("tuple");
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows.get_item(0)
                    .expect("r0")
                    .extract::<(i32, String)>()
                    .expect("r0 tuple"),
                (1, "x".to_owned())
            );
            assert_eq!(
                rows.get_item(1)
                    .expect("r1")
                    .extract::<(Option<i32>, String)>()
                    .expect("r1 tuple"),
                (None, "y".to_owned())
            );
            assert_eq!(
                names.extract::<(String, String)>().expect("names"),
                ("a".to_owned(), "b".to_owned())
            );

            let dicts_obj = row_dicts(py, &select_res).expect("row_dicts select");
            let dicts = dicts_obj.bind(py).cast::<PyList>().expect("dicts list");
            assert_eq!(dicts.len(), 2);
            let d0 = dicts
                .get_item(0)
                .expect("d0")
                .cast_into::<PyDict>()
                .expect("d0");
            assert_eq!(
                d0.get_item("a")
                    .expect("a")
                    .expect("some")
                    .extract::<i32>()
                    .expect("i32"),
                1
            );
            assert_eq!(
                d0.get_item("b")
                    .expect("b")
                    .expect("some")
                    .extract::<String>()
                    .expect("str"),
                "x"
            );

            let dup_res = QueryResult::try_new(
                vec![col_a.clone(), col_a_dup],
                vec![vec![PgValue::Int4(10), PgValue::Int4(20)]],
                1,
                "SELECT 1",
                TransactionStatus::Idle,
            )
            .expect("dup_res");
            let dup_dicts_obj = row_dicts(py, &dup_res).expect("row_dicts dup");
            let dup_dicts = dup_dicts_obj.bind(py).cast::<PyList>().expect("dup list");
            let dup_d0 = dup_dicts
                .get_item(0)
                .expect("dup d0")
                .cast_into::<PyDict>()
                .expect("dup dict");
            assert_eq!(
                dup_d0
                    .get_item("a")
                    .expect("a")
                    .expect("some")
                    .extract::<i32>()
                    .expect("i32"),
                20
            );

            let val_obj = first_value(py, &select_res).expect("first_value select");
            assert_eq!(val_obj.bind(py).extract::<i32>().expect("i32"), 1);

            let null_row_res = QueryResult::try_new(
                vec![col_a.clone()],
                vec![vec![PgValue::Null]],
                1,
                "SELECT 1",
                TransactionStatus::Idle,
            )
            .expect("null_row_res");
            assert!(
                first_value(py, &null_row_res)
                    .expect("first_value null")
                    .bind(py)
                    .is_none()
            );

            let zero_col_row_res = QueryResult {
                columns: Vec::new(),
                rows: vec![Vec::new()],
                affected_rows: 1,
                command_tag: "SELECT 1".to_owned(),
                transaction_status: TransactionStatus::Idle,
            };
            assert!(
                first_value(py, &zero_col_row_res)
                    .expect("first_value empty row")
                    .bind(py)
                    .is_none()
            );

            let no_rows_res = QueryResult::try_new(
                vec![col_a.clone()],
                Vec::new(),
                0,
                "SELECT 0",
                TransactionStatus::Idle,
            )
            .expect("no_rows_res");
            let no_rows_rn_obj = rows_and_names(py, &no_rows_res).expect("rows_and_names no rows");
            let no_rows_rn = no_rows_rn_obj
                .bind(py)
                .cast::<PyTuple>()
                .expect("no_rows rn");
            assert_eq!(
                no_rows_rn
                    .get_item(0)
                    .expect("rows")
                    .cast::<PyList>()
                    .expect("list")
                    .len(),
                0
            );
            assert_eq!(
                no_rows_rn
                    .get_item(1)
                    .expect("names")
                    .extract::<(String,)>()
                    .expect("names"),
                ("a".to_owned(),)
            );
            assert_eq!(
                row_dicts(py, &no_rows_res)
                    .expect("row_dicts no rows")
                    .bind(py)
                    .cast::<PyList>()
                    .expect("list")
                    .len(),
                0
            );
            let nf_err = first_value(py, &no_rows_res).expect_err("first_value no rows");
            assert_eq!(err_type_name(py, &nf_err), "NotFoundError");
            assert_eq!(
                nf_err.value(py).str().expect("str").to_str().expect("utf8"),
                "Query returned no rows"
            );

            let insert_res = QueryResult::try_new(
                Vec::new(),
                Vec::new(),
                3,
                "INSERT 0 3",
                TransactionStatus::Idle,
            )
            .expect("insert_res");
            let insert_rn_obj = rows_and_names(py, &insert_res).expect("rows_and_names insert");
            let insert_rn = insert_rn_obj.bind(py).cast::<PyTuple>().expect("insert rn");
            assert_eq!(
                insert_rn
                    .get_item(0)
                    .expect("rows")
                    .cast::<PyList>()
                    .expect("list")
                    .len(),
                0
            );
            assert_eq!(
                insert_rn
                    .get_item(1)
                    .expect("names")
                    .cast::<PyTuple>()
                    .expect("tuple")
                    .len(),
                0
            );
            assert_eq!(
                row_dicts(py, &insert_res)
                    .expect("row_dicts insert")
                    .bind(py)
                    .cast::<PyList>()
                    .expect("list")
                    .len(),
                0
            );
            let insert_dml = dml_outcome(py, &insert_res).expect("dml_outcome insert");
            assert_eq!(
                insert_dml
                    .bind(py)
                    .extract::<(i64, Option<i64>)>()
                    .expect("dml tuple"),
                (3, None)
            );

            let ddl_res = QueryResult::try_new(
                Vec::new(),
                Vec::new(),
                0,
                "CREATE TABLE",
                TransactionStatus::Idle,
            )
            .expect("ddl_res");
            let ddl_rn_obj = rows_and_names(py, &ddl_res).expect("rows_and_names ddl");
            let ddl_rn = ddl_rn_obj.bind(py).cast::<PyTuple>().expect("ddl rn");
            assert_eq!(
                ddl_rn
                    .get_item(0)
                    .expect("rows")
                    .cast::<PyList>()
                    .expect("list")
                    .len(),
                0
            );
            assert_eq!(
                ddl_rn
                    .get_item(1)
                    .expect("names")
                    .cast::<PyTuple>()
                    .expect("tuple")
                    .len(),
                0
            );
            assert_eq!(
                row_dicts(py, &ddl_res)
                    .expect("row_dicts ddl")
                    .bind(py)
                    .cast::<PyList>()
                    .expect("list")
                    .len(),
                0
            );
            let ddl_nf = first_value(py, &ddl_res).expect_err("first_value ddl");
            assert_eq!(err_type_name(py, &ddl_nf), "NotFoundError");
            let ddl_dml = dml_outcome(py, &ddl_res).expect("dml_outcome ddl");
            assert_eq!(
                ddl_dml
                    .bind(py)
                    .extract::<(i64, Option<i64>)>()
                    .expect("dml tuple"),
                (0, None)
            );

            let returning_res = QueryResult::try_new(
                vec![col_a],
                vec![vec![PgValue::Int4(7)]],
                1,
                "INSERT 0 1",
                TransactionStatus::Idle,
            )
            .expect("returning_res");
            let returning_dml = dml_outcome(py, &returning_res).expect("dml_outcome returning");
            assert_eq!(
                returning_dml
                    .bind(py)
                    .extract::<(i64, Option<i64>)>()
                    .expect("dml tuple"),
                (1, None)
            );
        });
    }
}
