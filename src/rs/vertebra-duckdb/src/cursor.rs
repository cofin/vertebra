//! PEP 249 cursors over the native DuckDB worker pool.
//!
//! [`SyncDuckDBCursor`] buffers one statement result at a time and serializes
//! `execute` / `executemany` calls behind an execute lock taken with the
//! interpreter detached.
//!
//! [`AsyncDuckDBCursor`] shares the same buffered state, returns awaitables from
//! its six methods, and rejects a statement call while another is in flight.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use duckdb::types::Value as DuckValue;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use vertebra_db::py_async::{
    ReadyAwaitable, RustDbTaskAwaitable, ready_ok, spawn_rust_db_task_with_attr,
};

use super::{
    CancelOnDropGuard, DUCKDB_COMPLETION_DRAINER_ATTR, DuckDBInner, StatementOutcome,
    duck_value_to_py, extract_bound_params, extract_parameter_sets,
};
use crate::errors::DuckDBClientError;

/// Reason a cursor rejects a call because of its own state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CursorStateError {
    Closed,
    InvalidArraysize,
    NegativeFetchSize,
}

/// Buffered result and PEP 249 attributes of one cursor.
pub(crate) struct CursorState {
    closed: bool,
    columns: Option<Vec<String>>,
    description: Option<Py<PyTuple>>,
    rows: VecDeque<Vec<DuckValue>>,
    rowcount: i64,
    arraysize: usize,
}

impl CursorState {
    pub(crate) const fn new() -> Self {
        Self {
            closed: false,
            columns: None,
            description: None,
            rows: VecDeque::new(),
            rowcount: -1,
            arraysize: 1,
        }
    }

    pub(crate) const fn require_open(&self) -> Result<(), CursorStateError> {
        if self.closed {
            Err(CursorStateError::Closed)
        } else {
            Ok(())
        }
    }

    /// Replace the buffered result with `outcome` and drop the cached description.
    pub(crate) fn install(&mut self, outcome: StatementOutcome) {
        self.columns = outcome.columns;
        self.description = None;
        self.rows = VecDeque::from(outcome.rows);
        self.rowcount = outcome.rowcount;
    }

    /// Drop the buffered result and reset the statement attributes.
    pub(crate) fn clear_result(&mut self) {
        self.columns = None;
        self.description = None;
        self.rows = VecDeque::new();
        self.rowcount = -1;
    }

    pub(crate) fn take_one(&mut self) -> Result<Option<Vec<DuckValue>>, CursorStateError> {
        self.require_open()?;
        Ok(self.rows.pop_front())
    }

    pub(crate) fn take_many(
        &mut self,
        size: Option<i64>,
    ) -> Result<Vec<Vec<DuckValue>>, CursorStateError> {
        self.require_open()?;
        let wanted = match size {
            None => self.arraysize,
            Some(n) => usize::try_from(n).map_err(|_| CursorStateError::NegativeFetchSize)?,
        };
        let count = wanted.min(self.rows.len());
        Ok(self.rows.drain(..count).collect())
    }

    pub(crate) fn take_all(&mut self) -> Result<Vec<Vec<DuckValue>>, CursorStateError> {
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
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.clear_result();
    }
}

pub(crate) fn cursor_state_err(py: Python<'_>, err: CursorStateError) -> PyErr {
    let client_err = match err {
        CursorStateError::Closed => DuckDBClientError::InvalidInput("cursor is closed".to_string()),
        CursorStateError::InvalidArraysize => {
            DuckDBClientError::Programming("arraysize must be at least 1".to_string())
        }
        CursorStateError::NegativeFetchSize => {
            DuckDBClientError::Programming("fetchmany size must not be negative".to_string())
        }
    };
    client_err.into_py_err(py)
}

pub(crate) fn row_to_py_tuple(py: Python<'_>, row: Vec<DuckValue>) -> PyResult<Bound<'_, PyTuple>> {
    let values = row
        .into_iter()
        .map(|value| duck_value_to_py(py, value))
        .collect::<PyResult<Vec<_>>>()?;
    PyTuple::new(py, values)
}

pub(crate) fn rows_to_py_list(
    py: Python<'_>,
    rows: Vec<Vec<DuckValue>>,
) -> PyResult<Bound<'_, PyList>> {
    let tuples = rows
        .into_iter()
        .map(|row| row_to_py_tuple(py, row))
        .collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, tuples)
}

pub(crate) fn names_to_py_tuple<'py>(
    py: Python<'py>,
    columns: &[String],
) -> PyResult<Bound<'py, PyTuple>> {
    let names = columns
        .iter()
        .map(|name| PyString::intern(py, name.as_str()));
    PyTuple::new(py, names)
}

pub(crate) fn outcome_to_rows<'py>(
    py: Python<'py>,
    outcome: StatementOutcome,
) -> PyResult<(Bound<'py, PyList>, Bound<'py, PyTuple>)> {
    let Some(columns) = outcome.columns else {
        return Ok((PyList::empty(py), PyTuple::empty(py)));
    };
    let rows = rows_to_py_list(py, outcome.rows)?;
    let names = names_to_py_tuple(py, &columns)?;
    Ok((rows, names))
}

pub(crate) fn outcome_to_dicts<'py>(
    py: Python<'py>,
    outcome: StatementOutcome,
) -> PyResult<Bound<'py, PyList>> {
    let Some(columns) = outcome.columns else {
        return Ok(PyList::empty(py));
    };
    if outcome.rows.is_empty() {
        return Ok(PyList::empty(py));
    }
    let keys: Vec<Bound<'py, PyString>> = columns
        .iter()
        .map(|name| PyString::intern(py, name.as_str()))
        .collect();
    let mut dicts = Vec::with_capacity(outcome.rows.len());
    for row in outcome.rows {
        let dict = PyDict::new(py);
        for (key, value) in keys.iter().zip(row) {
            dict.set_item(key, duck_value_to_py(py, value)?)?;
        }
        dicts.push(dict);
    }
    PyList::new(py, dicts)
}

pub(crate) fn outcome_to_value(py: Python<'_>, outcome: StatementOutcome) -> PyResult<Py<PyAny>> {
    if outcome.columns.is_none() {
        return Err(
            DuckDBClientError::NotFound("Query returned no rows".to_string()).into_py_err(py),
        );
    }
    let Some(first_row) = outcome.rows.into_iter().next() else {
        return Err(
            DuckDBClientError::NotFound("Query returned no rows".to_string()).into_py_err(py),
        );
    };
    match first_row.into_iter().next() {
        Some(value) => duck_value_to_py(py, value),
        None => Ok(py.None()),
    }
}

pub(crate) const fn outcome_to_dml(outcome: &StatementOutcome) -> (i64, Option<i64>) {
    let rowcount = if outcome.rowcount < 0 {
        0
    } else {
        outcome.rowcount
    };
    (rowcount, None)
}

pub(crate) fn description_tuple<'py>(
    py: Python<'py>,
    columns: &[String],
) -> PyResult<Bound<'py, PyTuple>> {
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

/// Synchronous PEP 249 cursor over a native DuckDB worker pool.
#[pyclass(name = "SyncDuckDBCursor", frozen, immutable_type)]
pub struct SyncDuckDBCursor {
    inner: Arc<DuckDBInner>,
    state: Mutex<CursorState>,
    execute_lock: Mutex<()>,
    lazy_begin: bool,
}

impl SyncDuckDBCursor {
    pub(crate) const fn new(inner: Arc<DuckDBInner>, lazy_begin: bool) -> Self {
        Self {
            inner,
            state: Mutex::new(CursorState::new()),
            execute_lock: Mutex::new(()),
            lazy_begin,
        }
    }

    fn state(&self) -> MutexGuard<'_, CursorState> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn require_open(&self, py: Python<'_>) -> PyResult<()> {
        self.state()
            .require_open()
            .map_err(|e| cursor_state_err(py, e))
    }

    /// Clear the buffered result and convert `err` for raising.
    fn fail(&self, py: Python<'_>, err: DuckDBClientError) -> PyErr {
        self.state().clear_result();
        err.into_py_err(py)
    }

    /// Run one statement call under the execute lock and leave its outcome on the cursor.
    ///
    /// The execute lock is taken with the interpreter detached and held until the result
    /// is installed or cleared. The cached description is moved out of the state while
    /// detached and dropped only after the interpreter is attached again.
    fn run_and_install<F>(&self, py: Python<'_>, begin: bool, run: F) -> PyResult<()>
    where
        F: FnOnce(&DuckDBInner, &Arc<AtomicBool>) -> Result<StatementOutcome, DuckDBClientError>
            + Send,
    {
        let inner = &self.inner;
        let state_lock = &self.state;
        let execute_lock = &self.execute_lock;
        let cancel_flag = inner.register_cancel_flag();
        let (stale_description, raised) = py.detach(move || {
            let _serial = execute_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let result = if begin && !inner.in_transaction.load(Ordering::Acquire) {
                inner
                    .begin_tx(&cancel_flag)
                    .and_then(|()| run(inner, &cancel_flag))
            } else {
                run(inner, &cancel_flag)
            };
            let mut state = state_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let stale_description = state.description.take();
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
            (stale_description, raised)
        });
        drop(stale_description);
        raised.map_or(Ok(()), |err| Err(err.into_py_err(py)))
    }
}

#[pymethods]
impl SyncDuckDBCursor {
    #[getter]
    pub fn description(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        let columns = {
            let state = self.state();
            if let Some(cached) = &state.description {
                return Ok(Some(cached.clone_ref(py)));
            }
            match &state.columns {
                Some(columns) => columns.clone(),
                None => return Ok(None),
            }
        };
        let built = description_tuple(py, &columns)?.unbind();
        let mut state = self.state();
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
        self.state().rowcount
    }

    #[getter]
    #[allow(clippy::unused_self)]
    pub fn lastrowid(&self) -> Option<i64> {
        None
    }

    #[getter]
    pub fn arraysize(&self) -> usize {
        self.state().arraysize
    }

    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state()
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
        let bound = extract_bound_params(params).map_err(|e| this.fail(py, e))?;
        let sql_owned = sql.to_string();
        this.run_and_install(py, this.lazy_begin, move |inner, cancel_flag| {
            inner.run_statement(sql_owned, bound, cancel_flag)
        })?;
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
        let param_sets = extract_parameter_sets(seq_of_params).map_err(|e| this.fail(py, e))?;
        let begin = this.lazy_begin && !param_sets.is_empty();
        let sql_owned = sql.to_string();
        this.run_and_install(py, begin, move |inner, cancel_flag| {
            if param_sets.is_empty() {
                return Ok(StatementOutcome {
                    columns: None,
                    rows: Vec::new(),
                    rowcount: 0,
                });
            }
            inner.run_statement_many(sql_owned, param_sets, cancel_flag)
        })?;
        Ok(slf.clone())
    }

    pub fn fetchone<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        let row = self
            .state()
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
            .state()
            .take_many(size)
            .map_err(|e| cursor_state_err(py, e))?;
        rows_to_py_list(py, rows)
    }

    pub fn fetchall<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        let rows = self
            .state()
            .take_all()
            .map_err(|e| cursor_state_err(py, e))?;
        rows_to_py_list(py, rows)
    }

    pub fn close(&self) {
        self.state().close();
    }
}

fn lock_state(state: &Mutex<CursorState>) -> MutexGuard<'_, CursorState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
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
    settled: bool,
}

impl ExecutingGuard {
    fn settle_with(mut self, apply: impl FnOnce(&mut CursorState)) {
        apply(&mut lock_state(&self.state));
        self.settled = true;
    }
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if !self.settled {
            lock_state(&self.state).clear_result();
        }
        self.executing.store(false, Ordering::Release);
    }
}

/// Awaitable PEP 249-shaped cursor over a native DuckDB worker pool.
#[pyclass(name = "AsyncDuckDBCursor", frozen, immutable_type)]
pub struct AsyncDuckDBCursor {
    inner: Arc<DuckDBInner>,
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
    lazy_begin: bool,
}

impl AsyncDuckDBCursor {
    pub(crate) fn new(inner: Arc<DuckDBInner>, lazy_begin: bool) -> Self {
        Self {
            inner,
            state: Arc::new(Mutex::new(CursorState::new())),
            executing: Arc::new(AtomicBool::new(false)),
            lazy_begin,
        }
    }

    fn state(&self) -> MutexGuard<'_, CursorState> {
        lock_state(&self.state)
    }

    /// Check that the cursor is open and idle, then mark it in flight.
    fn begin_statement(&self, py: Python<'_>) -> PyResult<ExecutingGuard> {
        self.state()
            .require_open()
            .map_err(|e| cursor_state_err(py, e))?;
        if self
            .executing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(
                DuckDBClientError::InvalidInput("cursor is already executing".to_string())
                    .into_py_err(py),
            );
        }
        Ok(ExecutingGuard {
            state: Arc::clone(&self.state),
            executing: Arc::clone(&self.executing),
            settled: false,
        })
    }

    /// Spawn one statement call and resolve its awaitable to the cursor object.
    ///
    /// Dropping the future before the blocking call returns interrupts the statement
    /// through the cancel-on-drop guard and clears the result through the in-flight guard.
    fn spawn_statement<F>(
        slf: &Bound<'_, Self>,
        guard: ExecutingGuard,
        begin: bool,
        run: F,
    ) -> PyResult<RustDbTaskAwaitable>
    where
        F: FnOnce(&DuckDBInner, &Arc<AtomicBool>) -> Result<StatementOutcome, DuckDBClientError>
            + Send
            + 'static,
    {
        let inner = Arc::clone(&slf.get().inner);
        let cancel_flag = inner.register_cancel_flag();
        let mut drop_guard = CancelOnDropGuard::new(Arc::clone(&inner), Arc::clone(&cancel_flag));
        let cursor = slf.clone().unbind();
        spawn_rust_db_task_with_attr(
            slf.py(),
            async move {
                let joined = tokio::task::spawn_blocking(move || {
                    if begin && !inner.in_transaction.load(Ordering::Acquire) {
                        inner.begin_tx(&cancel_flag)?;
                    }
                    run(&inner, &cancel_flag)
                })
                .await;
                let result = match joined {
                    Ok(result) => {
                        drop_guard.mark_completed();
                        result
                    }
                    Err(e) => Err(DuckDBClientError::Operational(e.to_string())),
                };
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
            |py, (err, guard): (DuckDBClientError, ExecutingGuard)| {
                guard.settle_with(CursorState::clear_result);
                err.into_py_err(py)
            },
            DUCKDB_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl AsyncDuckDBCursor {
    #[getter]
    pub fn description(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        let columns = {
            let state = self.state();
            if let Some(cached) = &state.description {
                return Ok(Some(cached.clone_ref(py)));
            }
            match &state.columns {
                Some(columns) => columns.clone(),
                None => return Ok(None),
            }
        };
        let built = description_tuple(py, &columns)?.unbind();
        let mut state = self.state();
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
        self.state().rowcount
    }

    #[getter]
    #[allow(clippy::unused_self)]
    pub fn lastrowid(&self) -> Option<i64> {
        None
    }

    #[getter]
    pub fn arraysize(&self) -> usize {
        self.state().arraysize
    }

    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state()
            .set_arraysize(value)
            .map_err(|e| cursor_state_err(py, e))
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        let guard = this.begin_statement(py)?;
        let bound = extract_bound_params(params).map_err(|e| e.into_py_err(py))?;
        let sql_owned = sql.to_string();
        Self::spawn_statement(slf, guard, this.lazy_begin, move |inner, cancel_flag| {
            inner.run_statement(sql_owned, bound, cancel_flag)
        })
    }

    pub fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        let guard = this.begin_statement(py)?;
        let param_sets = extract_parameter_sets(seq_of_params).map_err(|e| e.into_py_err(py))?;
        let begin = this.lazy_begin && !param_sets.is_empty();
        let sql_owned = sql.to_string();
        Self::spawn_statement(slf, guard, begin, move |inner, cancel_flag| {
            if param_sets.is_empty() {
                return Ok(StatementOutcome {
                    columns: None,
                    rows: Vec::new(),
                    rowcount: 0,
                });
            }
            inner.run_statement_many(sql_owned, param_sets, cancel_flag)
        })
    }

    pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let row = self
            .state()
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
            .state()
            .take_many(size)
            .map_err(|e| cursor_state_err(py, e))?;
        let value = rows_to_py_list(py, rows)?.into_any().unbind();
        ready_ok(py, value)
    }

    pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = self
            .state()
            .take_all()
            .map_err(|e| cursor_state_err(py, e))?;
        let value = rows_to_py_list(py, rows)?.into_any().unbind();
        ready_ok(py, value)
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        self.state().close();
        ready_ok(py, py.None())
    }
}
