//! Shared PEP 249 cursor state, description/row builders, metadata helper, state-error helpers,
//! and sync/async cursor classes for `vertebra-bigquery`.

use std::collections::HashMap;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyMapping, PyString, PyTuple};
use vertebra_db::py_async::{
    ReadyAwaitable, TaskAwaitable, block_on_db, ready_ok, spawn_rust_db_task_with_attr,
};
use vertebra_db::py_cursor::{BufferedCursorState, build_description_7tuple};
#[cfg(test)]
use vertebra_db::py_errors::cursor_programming_err;
pub(crate) use vertebra_db::py_errors::{cursor_interface_err, cursor_not_supported_err};

use crate::client::{BigQueryClient as NativeBigQueryClient, BigQueryError};
use crate::codec::{
    BigQueryFieldSchema, BigQueryQueryOptions, BigQueryQueryParameter, BigQueryQueryResult,
    parse_query_parameters_json,
};
use crate::errors::bigquery_error_to_py;
use crate::{BIGQUERY_COMPLETION_DRAINER_ATTR, cell_to_py};

const NEED_ENCODER_MSG: &str = "BigQuery cursor needs encoded parameters or a param_encoder";

/// Extract a Python `Mapping[str, str] | None` into a Rust `HashMap<String, String>`.
pub(crate) fn extract_labels(obj: Option<&Bound<'_, PyAny>>) -> PyResult<HashMap<String, String>> {
    let Some(val) = obj else {
        return Ok(HashMap::new());
    };
    if val.is_none() {
        return Ok(HashMap::new());
    }
    if let Ok(dict) = val.cast::<PyDict>() {
        let mut out = HashMap::with_capacity(dict.len());
        for (k, v) in dict.iter() {
            out.insert(k.extract::<String>()?, v.extract::<String>()?);
        }
        return Ok(out);
    }
    let mapping = val.cast::<PyMapping>()?;
    let items = mapping.items()?;
    let mut out = HashMap::with_capacity(items.len());
    for pair in items.iter() {
        let (k, v): (String, String) = pair.extract()?;
        out.insert(k, v);
    }
    Ok(out)
}

/// Buffered result and PEP 249 state shared by `SyncBigQueryCursor` and `AsyncBigQueryCursor`.
pub(crate) struct CursorState {
    pub(crate) buf: BufferedCursorState,
    pub(crate) session_id: Option<String>,
    pub(crate) query_metadata: Option<Py<PyDict>>,
}

impl Deref for CursorState {
    type Target = BufferedCursorState;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for CursorState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

impl CursorState {
    /// Construct a fresh, open cursor state with `rowcount = -1` and `arraysize = 1`.
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self {
            buf: BufferedCursorState::new(),
            session_id: None,
            query_metadata: None,
        }
    }

    /// Construct a fresh, open cursor state with an optional initial BigQuery session ID.
    pub(crate) fn with_session_id(session_id: Option<String>) -> Self {
        Self {
            buf: BufferedCursorState::new(),
            session_id,
            query_metadata: None,
        }
    }

    /// Clear buffered rows and reset statement metadata after a failed call or `close()`.
    pub(crate) fn clear_result(&mut self) {
        self.buf.clear_result();
        self.query_metadata = None;
    }

    /// Store a decoded [`BigQueryQueryResult`] using BigQuery's schema, DML count, and metadata.
    pub(crate) fn store_query_result(
        &mut self,
        py: Python<'_>,
        result: &BigQueryQueryResult,
    ) -> PyResult<()> {
        let meta = query_metadata_to_py(py, result)?;
        if result.schema.is_empty() {
            let rowcount = result.num_dml_affected_rows.unwrap_or(-1);
            self.store(None, Vec::new(), rowcount, None);
        } else {
            let desc = describe_fields(py, &result.schema)?;
            let rows = rows_to_tuples(py, result)?;
            let rowcount = result
                .num_dml_affected_rows
                .unwrap_or_else(|| i64::try_from(rows.len()).unwrap_or(i64::MAX));
            self.store(Some(desc), rows, rowcount, None);
        }
        self.query_metadata = Some(meta);
        if let Some(ref sid) = result.session_id
            && !sid.is_empty()
        {
            self.session_id = Some(sid.clone());
        }
        Ok(())
    }

    /// Close the cursor idempotently and release buffered rows.
    pub(crate) fn close(&mut self) {
        self.buf.close();
        self.query_metadata = None;
    }
}

/// Build the PEP 249 7-item `description` tuple from BigQuery field schemas.
pub(crate) fn describe_fields(
    py: Python<'_>,
    schema: &[BigQueryFieldSchema],
) -> PyResult<Py<PyTuple>> {
    let mut cols = Vec::with_capacity(schema.len());
    for field in schema {
        let type_code = PyString::new(py, &field.field_type).into_any().unbind();
        let null_ok = if field.mode.eq_ignore_ascii_case("NULLABLE") {
            Some(true)
        } else if field.mode.eq_ignore_ascii_case("REQUIRED") {
            Some(false)
        } else {
            None
        };
        cols.push((field.name.as_str(), Some(type_code), null_ok));
    }
    build_description_7tuple(py, cols)
}

/// Convert every decoded row in a [`BigQueryQueryResult`] into a Python tuple once.
pub(crate) fn rows_to_tuples(
    py: Python<'_>,
    result: &BigQueryQueryResult,
) -> PyResult<Vec<Py<PyTuple>>> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let mut cells: Vec<Py<PyAny>> = Vec::with_capacity(row.len());
        for cell in row {
            cells.push(cell_to_py(py, cell)?);
        }
        out.push(PyTuple::new(py, cells)?.unbind());
    }
    Ok(out)
}

/// Build the Python metadata dictionary for a BigQuery query reply.
pub(crate) fn query_metadata_to_py(
    py: Python<'_>,
    result: &BigQueryQueryResult,
) -> PyResult<Py<PyDict>> {
    let dict = PyDict::new(py);
    dict.set_item("job_id", &result.job_id)?;
    dict.set_item("session_id", &result.session_id)?;
    dict.set_item("total_rows", result.total_rows)?;
    dict.set_item("total_bytes_processed", result.total_bytes_processed)?;
    dict.set_item("total_bytes_billed", result.total_bytes_billed)?;
    dict.set_item("slot_ms", result.slot_ms)?;
    dict.set_item("cache_hit", result.cache_hit)?;
    dict.set_item("num_dml_affected_rows", result.num_dml_affected_rows)?;
    Ok(dict.unbind())
}

/// Pure-Rust query job options copied into background tasks for a BigQuery cursor.
#[derive(Clone)]
pub(crate) struct CursorQueryOptions {
    pub(crate) destination_table: Option<String>,
    pub(crate) write_disposition: Option<String>,
    pub(crate) maximum_bytes_billed: Option<i64>,
    pub(crate) job_timeout_ms: Option<u64>,
    pub(crate) priority: Option<String>,
    pub(crate) labels: HashMap<String, String>,
}

impl CursorQueryOptions {
    fn to_query_options(&self, session_id: Option<String>) -> BigQueryQueryOptions {
        BigQueryQueryOptions {
            session_id,
            create_session: false,
            dry_run: false,
            destination_table: self.destination_table.clone(),
            write_disposition: self.write_disposition.clone(),
            maximum_bytes_billed: self.maximum_bytes_billed,
            job_timeout_ms: self.job_timeout_ms,
            max_results: None,
            priority: self.priority.clone(),
            labels: self.labels.clone(),
        }
    }
}

fn encode_with_encoder(
    py: Python<'_>,
    sql: &str,
    obj: &Bound<'_, PyAny>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<(String, Vec<BigQueryQueryParameter>)> {
    let Some(enc) = encoder else {
        return Err(cursor_interface_err(py, NEED_ENCODER_MSG));
    };
    let encoded = enc.bind(py).call1((sql, obj))?;
    let (rewritten_sql, params_json): (String, Option<String>) =
        encoded.extract().map_err(|err| {
            cursor_interface_err(
                py,
                &format!("param_encoder must return (str, str | None): {err}"),
            )
        })?;
    let parsed = match params_json.as_deref() {
        Some(text) if !text.trim().is_empty() => {
            parse_query_parameters_json(Some(text)).map_err(|e| bigquery_error_to_py(py, e))?
        }
        _ => Vec::new(),
    };
    Ok((rewritten_sql, parsed))
}

fn encode_sql_and_params(
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<(String, Vec<BigQueryQueryParameter>)> {
    let Some(obj) = params else {
        return Ok((sql.to_string(), Vec::new()));
    };
    if obj.is_none() {
        return Ok((sql.to_string(), Vec::new()));
    }
    if let Ok(tup) = obj.cast_exact::<PyTuple>() {
        if tup.is_empty() {
            return Ok((sql.to_string(), Vec::new()));
        }
        return encode_with_encoder(py, sql, obj, encoder);
    }
    if let Ok(list) = obj.cast_exact::<PyList>() {
        if list.is_empty() {
            return Ok((sql.to_string(), Vec::new()));
        }
        return encode_with_encoder(py, sql, obj, encoder);
    }
    if let Ok(dict) = obj.cast_exact::<PyDict>() {
        if dict.is_empty() {
            return Ok((sql.to_string(), Vec::new()));
        }
        return encode_with_encoder(py, sql, obj, encoder);
    }
    if let Ok(text_obj) = obj.cast::<PyString>() {
        let text = text_obj.to_str()?;
        if text.trim().is_empty() {
            return Ok((sql.to_string(), Vec::new()));
        }
        let parsed =
            parse_query_parameters_json(Some(text)).map_err(|e| bigquery_error_to_py(py, e))?;
        return Ok((sql.to_string(), parsed));
    }
    if let Ok(0) = obj.len() {
        return Ok((sql.to_string(), Vec::new()));
    }
    encode_with_encoder(py, sql, obj, encoder)
}

fn encode_executemany_statements(
    py: Python<'_>,
    sql: &str,
    seq_of_params: &Bound<'_, PyAny>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<Vec<(String, Vec<BigQueryQueryParameter>)>> {
    let iter = seq_of_params.try_iter().map_err(|err| {
        if err.is_instance_of::<pyo3::exceptions::PyTypeError>(py)
            || err.is_instance_of::<pyo3::exceptions::PyValueError>(py)
        {
            cursor_interface_err(py, &err.to_string())
        } else {
            err
        }
    })?;
    let mut statements = Vec::new();
    for item in iter {
        let item = item?;
        statements.push(encode_sql_and_params(py, sql, Some(&item), encoder)?);
    }
    Ok(statements)
}

fn lock_state(state: &Mutex<CursorState>) -> MutexGuard<'_, CursorState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

enum BigQueryCursorOutcome {
    Single(BigQueryQueryResult),
    Many {
        total_dml: i64,
        session_id: Option<String>,
        last_result: Option<BigQueryQueryResult>,
    },
}

impl BigQueryCursorOutcome {
    fn install_into(self, py: Python<'_>, state: &mut CursorState) -> PyResult<()> {
        match self {
            Self::Single(result) => state.store_query_result(py, &result),
            Self::Many {
                total_dml,
                session_id,
                last_result,
            } => {
                let meta = match last_result {
                    Some(ref res) => Some(query_metadata_to_py(py, res)?),
                    None => None,
                };
                state.store(None, Vec::new(), total_dml, None);
                state.query_metadata = meta;
                if let Some(sid) = session_id {
                    state.session_id = Some(sid);
                }
                Ok(())
            }
        }
    }
}

async fn run_execute_job(
    client: Arc<NativeBigQueryClient>,
    options: CursorQueryOptions,
    session_id: Option<String>,
    sql: String,
    params: Vec<BigQueryQueryParameter>,
) -> Result<BigQueryCursorOutcome, BigQueryError> {
    client.ensure_open()?;
    let opts = options.to_query_options(session_id);
    let res = client.query_with_options(&sql, &params, &opts).await?;
    Ok(BigQueryCursorOutcome::Single(res))
}

async fn run_executemany_jobs(
    client: Arc<NativeBigQueryClient>,
    options: CursorQueryOptions,
    mut session_id: Option<String>,
    statements: Vec<(String, Vec<BigQueryQueryParameter>)>,
) -> Result<BigQueryCursorOutcome, BigQueryError> {
    let mut total_dml: i64 = 0;
    let mut last_result: Option<BigQueryQueryResult> = None;
    for (stmt_sql, stmt_params) in statements {
        client.ensure_open()?;
        let opts = options.to_query_options(session_id.clone());
        let res = client
            .query_with_options(&stmt_sql, &stmt_params, &opts)
            .await?;
        if let Some(ref sid) = res.session_id
            && !sid.is_empty()
        {
            session_id = Some(sid.clone());
        }
        total_dml = total_dml.saturating_add(res.num_dml_affected_rows.unwrap_or(0));
        last_result = Some(res);
    }
    Ok(BigQueryCursorOutcome::Many {
        total_dml,
        session_id,
        last_result,
    })
}

/// Synchronous PEP 249 cursor backed by a Google Cloud BigQuery client.
#[pyclass(
    name = "SyncBigQueryCursor",
    module = "vertebra._bigquery",
    frozen,
    immutable_type
)]
pub struct PySyncBigQueryCursor {
    client: Arc<NativeBigQueryClient>,
    options: CursorQueryOptions,
    param_encoder: Option<Py<PyAny>>,
    state: Mutex<CursorState>,
    execute_lock: Mutex<()>,
}

impl PySyncBigQueryCursor {
    pub(crate) fn new(
        client: Arc<NativeBigQueryClient>,
        session_id: Option<String>,
        options: CursorQueryOptions,
        param_encoder: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            client,
            options,
            param_encoder,
            state: Mutex::new(CursorState::with_session_id(session_id)),
            execute_lock: Mutex::new(()),
        }
    }

    fn state(&self) -> MutexGuard<'_, CursorState> {
        lock_state(&self.state)
    }

    fn run_and_install<F, Fut>(&self, py: Python<'_>, build_fut: F) -> PyResult<()>
    where
        F: FnOnce(Option<String>) -> Fut + Send,
        Fut: std::future::Future<Output = Result<BigQueryCursorOutcome, BigQueryError>> + Send,
    {
        let execute_lock = &self.execute_lock;
        let state_lock = &self.state;
        py.detach(move || {
            let _serial = execute_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let session_id = lock_state(state_lock).session_id.clone();
            let outcome = block_on_db(build_fut(session_id));
            Python::attach(|py| {
                let mut state = lock_state(state_lock);
                match outcome {
                    Ok(result) => {
                        if state.closed {
                            state.clear_result();
                            Ok(())
                        } else if let Err(err) = result.install_into(py, &mut state) {
                            state.clear_result();
                            Err(err)
                        } else {
                            Ok(())
                        }
                    }
                    Err(err) => {
                        state.clear_result();
                        Err(bigquery_error_to_py(py, err))
                    }
                }
            })
        })
    }
}

#[pymethods]
impl PySyncBigQueryCursor {
    #[getter]
    fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        self.state().description.as_ref().map(|d| d.clone_ref(py))
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.state().rowcount
    }

    #[getter]
    fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.state().lastrowid.as_ref().map(|v| v.clone_ref(py))
    }

    #[getter]
    fn query_metadata(&self, py: Python<'_>) -> Option<Py<PyDict>> {
        self.state()
            .query_metadata
            .as_ref()
            .map(|m| m.clone_ref(py))
    }

    #[getter]
    fn arraysize(&self) -> usize {
        self.state().arraysize
    }

    #[setter]
    fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state().set_arraysize(py, value)
    }

    #[pyo3(signature = (sql, params=None), text_signature = "($self, sql, params=())")]
    fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.state().ensure_open(py)?;
        let (resolved_sql, encoded_params) =
            match encode_sql_and_params(py, sql, params, this.param_encoder.as_ref()) {
                Ok(pair) => pair,
                Err(err) => {
                    this.state().clear_result();
                    return Err(err);
                }
            };
        let client = Arc::clone(&this.client);
        let options = this.options.clone();
        this.run_and_install(py, move |session_id| {
            run_execute_job(client, options, session_id, resolved_sql, encoded_params)
        })?;
        Ok(slf.clone())
    }

    fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.state().ensure_open(py)?;
        let statements = match encode_executemany_statements(
            py,
            sql,
            seq_of_params,
            this.param_encoder.as_ref(),
        ) {
            Ok(stmts) => stmts,
            Err(err) => {
                this.state().clear_result();
                return Err(err);
            }
        };
        let client = Arc::clone(&this.client);
        let options = this.options.clone();
        this.run_and_install(py, move |session_id| {
            run_executemany_jobs(client, options, session_id, statements)
        })?;
        Ok(slf.clone())
    }

    fn fetchone(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        self.state().fetchone(py)
    }

    #[pyo3(signature = (size=None))]
    fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<PyList>> {
        self.state().fetchmany(py, size)
    }

    fn fetchall(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.state().fetchall(py)
    }

    fn close(&self) {
        self.state().close();
    }

    fn __enter__<'py>(slf: Bound<'py, Self>) -> Bound<'py, Self> {
        slf
    }

    #[pyo3(signature = (exc_type=None, exc_value=None, traceback=None))]
    fn __exit__(
        &self,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_value: Option<&Bound<'_, PyAny>>,
        traceback: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        let _ = (exc_type, exc_value, traceback);
        self.state().close();
        false
    }
}

struct ExecutingGuard {
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
    settled: bool,
}

impl ExecutingGuard {
    fn settle_with<R>(mut self, apply: impl FnOnce(&mut CursorState) -> R) -> R {
        let out = apply(&mut lock_state(&self.state));
        self.settled = true;
        out
    }
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if !self.settled {
            Python::attach(|_| {
                lock_state(&self.state).clear_result();
            });
        }
        self.executing.store(false, Ordering::Release);
    }
}

/// Asynchronous PEP 249 cursor backed by a Google Cloud BigQuery client.
#[pyclass(
    name = "AsyncBigQueryCursor",
    module = "vertebra._bigquery",
    frozen,
    immutable_type
)]
pub struct PyAsyncBigQueryCursor {
    client: Arc<NativeBigQueryClient>,
    options: CursorQueryOptions,
    param_encoder: Option<Py<PyAny>>,
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
}

impl PyAsyncBigQueryCursor {
    pub(crate) fn new(
        client: Arc<NativeBigQueryClient>,
        session_id: Option<String>,
        options: CursorQueryOptions,
        param_encoder: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            client,
            options,
            param_encoder,
            state: Arc::new(Mutex::new(CursorState::with_session_id(session_id))),
            executing: Arc::new(AtomicBool::new(false)),
        }
    }

    fn state(&self) -> MutexGuard<'_, CursorState> {
        lock_state(&self.state)
    }

    fn begin_statement(&self, py: Python<'_>) -> PyResult<ExecutingGuard> {
        self.state().ensure_open(py)?;
        if self
            .executing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(cursor_interface_err(py, "cursor is already executing"));
        }
        Ok(ExecutingGuard {
            state: Arc::clone(&self.state),
            executing: Arc::clone(&self.executing),
            settled: false,
        })
    }

    fn spawn_statement<Fut>(
        slf: &Bound<'_, Self>,
        guard: ExecutingGuard,
        fut: Fut,
    ) -> PyResult<TaskAwaitable>
    where
        Fut: std::future::Future<Output = Result<BigQueryCursorOutcome, BigQueryError>>
            + Send
            + 'static,
    {
        let cursor = slf.clone().unbind();
        spawn_rust_db_task_with_attr(
            slf.py(),
            async move {
                match fut.await {
                    Ok(outcome) => Ok((outcome, guard)),
                    Err(err) => Err((err, guard)),
                }
            },
            move |py, (outcome, guard): (BigQueryCursorOutcome, ExecutingGuard)| {
                let install_res = guard.settle_with(|state| {
                    if state.closed {
                        state.clear_result();
                        Ok(())
                    } else if let Err(err) = outcome.install_into(py, state) {
                        state.clear_result();
                        Err(err)
                    } else {
                        Ok(())
                    }
                });
                install_res?;
                Ok(cursor.into_any())
            },
            |py, (err, guard): (BigQueryError, ExecutingGuard)| {
                guard.settle_with(CursorState::clear_result);
                bigquery_error_to_py(py, err)
            },
            BIGQUERY_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl PyAsyncBigQueryCursor {
    #[getter]
    fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        self.state().description.as_ref().map(|d| d.clone_ref(py))
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.state().rowcount
    }

    #[getter]
    fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.state().lastrowid.as_ref().map(|v| v.clone_ref(py))
    }

    #[getter]
    fn query_metadata(&self, py: Python<'_>) -> Option<Py<PyDict>> {
        self.state()
            .query_metadata
            .as_ref()
            .map(|m| m.clone_ref(py))
    }

    #[getter]
    fn arraysize(&self) -> usize {
        self.state().arraysize
    }

    #[setter]
    fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state().set_arraysize(py, value)
    }

    #[pyo3(signature = (sql, params=None), text_signature = "($self, sql, params=())")]
    fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<TaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        let guard = this.begin_statement(py)?;
        let (resolved_sql, encoded_params) =
            encode_sql_and_params(py, sql, params, this.param_encoder.as_ref())?;
        let session_id = this.state().session_id.clone();
        let client = Arc::clone(&this.client);
        let options = this.options.clone();
        Self::spawn_statement(
            slf,
            guard,
            run_execute_job(client, options, session_id, resolved_sql, encoded_params),
        )
    }

    fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        let guard = this.begin_statement(py)?;
        let statements =
            encode_executemany_statements(py, sql, seq_of_params, this.param_encoder.as_ref())?;
        let session_id = this.state().session_id.clone();
        let client = Arc::clone(&this.client);
        let options = this.options.clone();
        Self::spawn_statement(
            slf,
            guard,
            run_executemany_jobs(client, options, session_id, statements),
        )
    }

    fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let row = self.state().fetchone(py)?;
        ready_ok(py, row.map_or_else(|| py.None(), Py::into_any))
    }

    #[pyo3(signature = (size=None))]
    fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = self.state().fetchmany(py, size)?;
        ready_ok(py, rows.into_any())
    }

    fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = self.state().fetchall(py)?;
        ready_ok(py, rows.into_any())
    }

    fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        self.state().close();
        ready_ok(py, py.None())
    }
}

/// Register BigQuery cursor classes on `vertebra._bigquery`.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySyncBigQueryCursor>()?;
    m.add_class::<PyAsyncBigQueryCursor>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList, PyString, PyTuple};

    use crate::codec::{BigQueryCellValue, BigQueryFieldSchema, BigQueryQueryResult};

    use super::{
        CursorState, NEED_ENCODER_MSG, cursor_interface_err, cursor_programming_err,
        describe_fields, encode_sql_and_params, query_metadata_to_py, rows_to_tuples,
    };

    fn make_row(py: Python<'_>, id: i64) -> Py<PyTuple> {
        PyTuple::new(py, [id]).unwrap().unbind()
    }

    fn sample_schema() -> Vec<BigQueryFieldSchema> {
        vec![
            BigQueryFieldSchema {
                name: "id".to_string(),
                field_type: "INT64".to_string(),
                mode: "REQUIRED".to_string(),
                fields: Vec::new(),
            },
            BigQueryFieldSchema {
                name: "label".to_string(),
                field_type: "STRING".to_string(),
                mode: "NULLABLE".to_string(),
                fields: Vec::new(),
            },
        ]
    }

    fn empty_result() -> BigQueryQueryResult {
        BigQueryQueryResult {
            columns: Vec::new(),
            schema: Vec::new(),
            rows: Vec::new(),
            total_rows: 0,
            total_bytes_processed: 0,
            total_bytes_billed: 0,
            slot_ms: 0,
            cache_hit: false,
            job_id: None,
            session_id: None,
            num_dml_affected_rows: None,
        }
    }

    #[test]
    fn cursor_state_fresh_and_fetch_without_result_set() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();
            assert_eq!(state.rowcount, -1);
            assert_eq!(state.arraysize, 1);
            assert!(state.description.is_none());
            assert!(state.lastrowid.is_none());
            assert!(!state.executed);
            assert!(!state.closed);

            assert!(state.fetchone(py).unwrap().is_none());
            assert!(state.fetchmany(py, None).unwrap().bind(py).is_empty());
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());

            state.store(None, Vec::new(), 3, None);
            assert_eq!(state.rowcount, 3);
            assert!(state.executed);
            assert!(state.fetchone(py).unwrap().is_none());
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());
        });
    }

    #[test]
    fn cursor_state_empty_result_set_keeps_description() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();
            let desc = describe_fields(py, &sample_schema()).unwrap();
            state.store(Some(desc), Vec::new(), 0, None);
            assert!(state.description.is_some());
            assert_eq!(state.rowcount, 0);
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());
            assert!(state.fetchone(py).unwrap().is_none());
        });
    }

    #[test]
    fn cursor_state_fetchmany_default_and_fetchall_remainder() {
        Python::initialize();
        Python::attach(|py| {
            let desc = describe_fields(py, &sample_schema()).unwrap();
            let rows = (1..=5).map(|i| make_row(py, i)).collect();

            let mut state = CursorState::new();
            state.set_arraysize(py, 2).unwrap();
            state.store(Some(desc.clone_ref(py)), rows, 5, None);

            assert!(state.fetchmany(py, Some(0)).unwrap().bind(py).is_empty());
            assert_eq!(state.fetchmany(py, None).unwrap().bind(py).len(), 2);
            assert_eq!(state.fetchmany(py, None).unwrap().bind(py).len(), 2);
            assert_eq!(state.fetchmany(py, None).unwrap().bind(py).len(), 1);
            assert!(state.fetchmany(py, None).unwrap().bind(py).is_empty());

            let rows2 = (1..=5).map(|i| make_row(py, i)).collect();
            state.store(Some(desc), rows2, 5, None);
            let first = state.fetchone(py).unwrap().unwrap();
            assert_eq!(
                first
                    .bind(py)
                    .get_item(0)
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1
            );
            let rest = state.fetchall(py).unwrap();
            assert_eq!(rest.bind(py).len(), 4);
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());

            let err = state.set_arraysize(py, 0).unwrap_err();
            assert_eq!(
                err.get_type(py).name().unwrap().to_str().unwrap(),
                "ProgrammingError"
            );
        });
    }

    #[test]
    fn cursor_state_closed_and_clear_result() {
        Python::initialize();
        Python::attach(|py| {
            let desc = describe_fields(py, &sample_schema()).unwrap();
            let mut state = CursorState::new();
            state.store(Some(desc), vec![make_row(py, 1)], 1, None);
            state.clear_result();
            assert!(state.description.is_none());
            assert_eq!(state.rowcount, -1);
            assert!(state.lastrowid.is_none());
            assert!(state.rows.is_empty());

            state.close();
            state.close();
            assert!(state.closed);
            let err = state.fetchone(py).unwrap_err();
            assert_eq!(
                err.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
            let err_many = state.fetchmany(py, None).unwrap_err();
            assert_eq!(
                err_many.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
            let err_all = state.fetchall(py).unwrap_err();
            assert_eq!(
                err_all.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
        });
    }

    #[test]
    fn cursor_describe_fields_sets_null_ok_by_mode() {
        Python::initialize();
        Python::attach(|py| {
            let mut schema = sample_schema();
            schema.push(BigQueryFieldSchema {
                name: "tags".to_string(),
                field_type: "STRING".to_string(),
                mode: "REPEATED".to_string(),
                fields: Vec::new(),
            });
            let desc = describe_fields(py, &schema).unwrap();
            let bound = desc.bind(py);
            assert_eq!(bound.len(), 3);

            let first = bound.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
            assert_eq!(first.len(), 7);
            assert_eq!(
                first.get_item(0).unwrap().extract::<String>().unwrap(),
                "id"
            );
            assert_eq!(
                first.get_item(1).unwrap().extract::<String>().unwrap(),
                "INT64"
            );
            for idx in 2..6 {
                assert!(first.get_item(idx).unwrap().is_none());
            }
            assert_eq!(
                first
                    .get_item(6)
                    .unwrap()
                    .extract::<Option<bool>>()
                    .unwrap(),
                Some(false)
            );

            let second = bound.get_item(1).unwrap().cast_into::<PyTuple>().unwrap();
            assert_eq!(second.len(), 7);
            assert_eq!(
                second.get_item(0).unwrap().extract::<String>().unwrap(),
                "label"
            );
            assert_eq!(
                second.get_item(1).unwrap().extract::<String>().unwrap(),
                "STRING"
            );
            assert_eq!(
                second
                    .get_item(6)
                    .unwrap()
                    .extract::<Option<bool>>()
                    .unwrap(),
                Some(true)
            );

            let third = bound.get_item(2).unwrap().cast_into::<PyTuple>().unwrap();
            assert_eq!(third.len(), 7);
            assert!(third.get_item(6).unwrap().is_none());
        });
    }

    #[test]
    fn cursor_rows_to_tuples_preserves_nested_and_repeated_shapes() {
        Python::initialize();
        Python::attach(|py| {
            let mut result = empty_result();
            result.schema = sample_schema();
            result.rows = vec![vec![
                BigQueryCellValue::Array(vec![
                    BigQueryCellValue::Int64(10),
                    BigQueryCellValue::Int64(20),
                ]),
                BigQueryCellValue::Struct(vec![
                    ("k".to_string(), BigQueryCellValue::String("v".to_string())),
                    ("n".to_string(), BigQueryCellValue::Null),
                ]),
            ]];

            let rows = rows_to_tuples(py, &result).unwrap();
            assert_eq!(rows.len(), 1);
            let row = rows[0].bind(py);
            let arr = row.get_item(0).unwrap().cast_into::<PyList>().unwrap();
            assert_eq!(arr.extract::<Vec<i64>>().unwrap(), vec![10, 20]);
            let st = row.get_item(1).unwrap().cast_into::<PyDict>().unwrap();
            assert_eq!(
                st.get_item("k")
                    .unwrap()
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "v"
            );
            assert!(st.get_item("n").unwrap().unwrap().is_none());
        });
    }

    #[test]
    fn cursor_store_query_result_dml_ddl_and_select() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();

            let mut dml = empty_result();
            dml.num_dml_affected_rows = Some(4);
            state.store_query_result(py, &dml).unwrap();
            assert_eq!(state.rowcount, 4);
            assert!(state.description.is_none());
            assert!(state.lastrowid.is_none());

            let ddl = empty_result();
            state.store_query_result(py, &ddl).unwrap();
            assert_eq!(state.rowcount, -1);
            assert!(state.description.is_none());
            assert!(state.lastrowid.is_none());

            let mut select = empty_result();
            select.schema = sample_schema();
            select.rows = vec![
                vec![
                    BigQueryCellValue::Int64(1),
                    BigQueryCellValue::String("a".to_string()),
                ],
                vec![
                    BigQueryCellValue::Int64(2),
                    BigQueryCellValue::String("b".to_string()),
                ],
            ];
            state.store_query_result(py, &select).unwrap();
            assert_eq!(state.rowcount, 2);
            assert!(state.description.is_some());
            assert!(state.lastrowid.is_none());
            assert_eq!(state.fetchall(py).unwrap().bind(py).len(), 2);
        });
    }

    #[test]
    fn cursor_query_metadata_to_py_exposes_all_keys() {
        Python::initialize();
        Python::attach(|py| {
            let mut result = empty_result();
            result.job_id = Some("job-123".to_string());
            result.session_id = Some("sess-456".to_string());
            result.total_rows = 42;
            result.total_bytes_processed = 1024;
            result.total_bytes_billed = 2048;
            result.slot_ms = 55;
            result.cache_hit = true;
            result.num_dml_affected_rows = Some(4);

            let meta = query_metadata_to_py(py, &result).unwrap();
            let bound = meta.bind(py);
            assert_eq!(
                bound
                    .get_item("job_id")
                    .unwrap()
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("job-123")
            );
            assert_eq!(
                bound
                    .get_item("session_id")
                    .unwrap()
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("sess-456")
            );
            assert_eq!(
                bound
                    .get_item("total_rows")
                    .unwrap()
                    .unwrap()
                    .extract::<u64>()
                    .unwrap(),
                42
            );
            assert_eq!(
                bound
                    .get_item("total_bytes_processed")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                1024
            );
            assert_eq!(
                bound
                    .get_item("total_bytes_billed")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                2048
            );
            assert_eq!(
                bound
                    .get_item("slot_ms")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                55
            );
            assert!(
                bound
                    .get_item("cache_hit")
                    .unwrap()
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
            assert_eq!(
                bound
                    .get_item("num_dml_affected_rows")
                    .unwrap()
                    .unwrap()
                    .extract::<Option<i64>>()
                    .unwrap(),
                Some(4)
            );
        });
    }

    #[test]
    fn cursor_state_error_classes_and_messages() {
        Python::initialize();
        Python::attach(|py| {
            let iface = cursor_interface_err(py, "x");
            assert_eq!(
                iface.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
            assert_eq!(iface.value(py).str().unwrap().to_str().unwrap(), "x");

            let prog = cursor_programming_err(py, "x");
            assert_eq!(
                prog.get_type(py).name().unwrap().to_str().unwrap(),
                "ProgrammingError"
            );
            assert_eq!(prog.value(py).str().unwrap().to_str().unwrap(), "x");
        });
    }

    #[test]
    fn cursor_encode_empty_exact_containers_need_no_encoder() {
        Python::initialize();
        Python::attach(|py| {
            let empty_tuple = PyTuple::empty(py).into_any();
            let empty_list = PyList::empty(py).into_any();
            let empty_dict = PyDict::new(py).into_any();

            for container in [empty_tuple, empty_list, empty_dict] {
                let (sql, params) =
                    encode_sql_and_params(py, "SELECT 1", Some(&container), None).unwrap();
                assert_eq!(sql, "SELECT 1");
                assert!(params.is_empty());
            }
        });
    }

    #[test]
    fn cursor_encode_non_empty_exact_containers_call_encoder() {
        Python::initialize();
        Python::attach(|py| {
            let encoder = py
                .eval(c"lambda sql, params: (sql + ' /*enc*/', None)", None, None)
                .unwrap()
                .unbind();

            let non_empty_tuple = PyTuple::new(py, [1_i64]).unwrap().into_any();
            let non_empty_list = PyList::new(py, [1_i64]).unwrap().into_any();
            let non_empty_dict = PyDict::new(py);
            non_empty_dict.set_item("a", 1_i64).unwrap();
            let non_empty_dict = non_empty_dict.into_any();

            for container in [&non_empty_tuple, &non_empty_list, &non_empty_dict] {
                let (sql, params) =
                    encode_sql_and_params(py, "SELECT 1", Some(container), Some(&encoder)).unwrap();
                assert!(sql.ends_with(" /*enc*/"));
                assert!(params.is_empty());
            }

            let err =
                encode_sql_and_params(py, "SELECT 1", Some(&non_empty_tuple), None).unwrap_err();
            assert_eq!(
                err.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
            assert_eq!(
                err.value(py).str().unwrap().to_str().unwrap(),
                NEED_ENCODER_MSG
            );
        });
    }

    #[test]
    fn cursor_encode_text_and_subclass_inputs() {
        Python::initialize();
        Python::attach(|py| {
            let encoder = py
                .eval(c"lambda sql, params: (sql + ' /*enc*/', None)", None, None)
                .unwrap()
                .unbind();

            let text_params = PyString::new(py, "[]").into_any();
            let (text_sql, parsed_params) =
                encode_sql_and_params(py, "SELECT 1", Some(&text_params), Some(&encoder)).unwrap();
            assert_eq!(text_sql, "SELECT 1");
            assert!(parsed_params.is_empty());

            let collections = py.import("collections").unwrap();
            let empty_od = collections.getattr("OrderedDict").unwrap().call0().unwrap();
            let (od_sql, od_params) =
                encode_sql_and_params(py, "SELECT 1", Some(&empty_od), Some(&encoder)).unwrap();
            assert_eq!(od_sql, "SELECT 1");
            assert!(od_params.is_empty());

            let row_cls = collections
                .getattr("namedtuple")
                .unwrap()
                .call1(("Row", ["a"]))
                .unwrap();
            let row_inst = row_cls.call1((1_i64,)).unwrap();
            let (nt_sql, nt_params) =
                encode_sql_and_params(py, "SELECT 1", Some(&row_inst), Some(&encoder)).unwrap();
            assert!(nt_sql.ends_with(" /*enc*/"));
            assert!(nt_params.is_empty());
        });
    }
}
