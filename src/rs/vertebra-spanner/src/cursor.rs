//! Shared PEP 249 cursor state, statement kind cache, and sync/async cursor classes for `vertebra-spanner`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use vertebra_db::py_async::{
    ReadyAwaitable, TaskAwaitable, block_on_db, ready_ok, spawn_rust_db_task_with_attr,
};
use vertebra_db::py_cursor::{
    BufferedCursorState, DEFAULT_PLAN_CACHE_CAPACITY, PlanLruCache, build_description_7tuple,
};
#[cfg(test)]
use vertebra_db::py_errors::cursor_programming_err;
pub(crate) use vertebra_db::py_errors::{cursor_interface_err, cursor_not_supported_err};

use crate::client::{
    SpannerBeginTransactionOptions, SpannerClient as NativeSpannerClient, SpannerError,
    SpannerReadWriteTx,
};
use crate::codec::{
    SpannerBatchDmlStatement, SpannerQueryParameter, SpannerQueryResult, SpannerStaleness,
    SpannerStructField, parse_spanner_parameters_json,
};
use crate::errors::spanner_error_to_py;
use crate::{SPANNER_COMPLETION_DRAINER_ATTR, spanner_val_to_py};

/// Statement kind used to route Spanner execution before sending the RPC.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StatementKind {
    Query,
    Dml,
    Ddl,
}

static STATEMENT_KIND_CACHE: OnceLock<Mutex<PlanLruCache<StatementKind>>> = OnceLock::new();

fn kind_cache() -> &'static Mutex<PlanLruCache<StatementKind>> {
    STATEMENT_KIND_CACHE.get_or_init(|| Mutex::new(PlanLruCache::new(DEFAULT_PLAN_CACHE_CAPACITY)))
}

fn skip_hint_block(bytes: &[u8], start: usize) -> usize {
    let mut i = start + 2;
    let mut depth: usize = 1;
    let mut quote: Option<u8> = None;
    while i < bytes.len() && depth > 0 {
        let b = bytes[i];
        if let Some(q) = quote {
            if b == b'\\' && i + 1 < bytes.len() {
                i += 2;
                continue;
            }
            if b == q {
                quote = None;
            }
            i += 1;
            continue;
        }
        match b {
            b'\'' | b'"' => {
                quote = Some(b);
                i += 1;
            }
            b'{' => {
                depth += 1;
                i += 1;
            }
            b'}' => {
                depth -= 1;
                i += 1;
            }
            _ => {
                i += 1;
            }
        }
    }
    i
}

fn classify_statement_uncached(sql: &str) -> StatementKind {
    let bytes = sql.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        while i < bytes.len() && bytes[i].is_ascii_whitespace() {
            i += 1;
        }
        if i + 1 < bytes.len() && bytes[i] == b'-' && bytes[i + 1] == b'-' {
            i += 2;
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'/' && bytes[i + 1] == b'*' {
            i += 2;
            while i + 1 < bytes.len() && !(bytes[i] == b'*' && bytes[i + 1] == b'/') {
                i += 1;
            }
            if i + 1 < bytes.len() {
                i += 2;
            } else {
                i = bytes.len();
            }
            continue;
        }
        if i + 1 < bytes.len() && bytes[i] == b'@' && bytes[i + 1] == b'{' {
            i = skip_hint_block(bytes, i);
            continue;
        }
        break;
    }

    let word_start = i;
    while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
        i += 1;
    }
    let keyword = &sql[word_start..i];
    if keyword.eq_ignore_ascii_case("INSERT")
        || keyword.eq_ignore_ascii_case("UPDATE")
        || keyword.eq_ignore_ascii_case("DELETE")
    {
        StatementKind::Dml
    } else if keyword.eq_ignore_ascii_case("CREATE")
        || keyword.eq_ignore_ascii_case("ALTER")
        || keyword.eq_ignore_ascii_case("DROP")
    {
        StatementKind::Ddl
    } else {
        StatementKind::Query
    }
}

/// Return the cached [`StatementKind`] for `sql`, computing and caching it on a miss.
pub(crate) fn statement_kind(sql: &str) -> StatementKind {
    {
        let mut cache = kind_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(hit) = cache.get(sql) {
            return hit;
        }
    }

    let kind = classify_statement_uncached(sql);

    let mut cache = kind_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    cache.insert(sql.into(), kind)
}

pub(crate) type CursorState = BufferedCursorState;

/// Store a decoded [`SpannerQueryResult`] according to its [`StatementKind`].
pub(crate) fn store_query_result(
    state: &mut CursorState,
    py: Python<'_>,
    kind: StatementKind,
    result: &SpannerQueryResult,
) -> PyResult<()> {
    match kind {
        StatementKind::Ddl => {
            state.store(None, Vec::new(), -1, None);
        }
        StatementKind::Dml => {
            let rowcount = result
                .row_count_exact
                .or(result.row_count_lower_bound)
                .unwrap_or_else(|| i64::try_from(result.rows.len()).unwrap_or(i64::MAX));
            if result.schema.is_empty() {
                state.store(None, Vec::new(), rowcount, None);
            } else {
                let desc = describe_fields(py, &result.schema)?;
                let rows = rows_to_tuples(py, result)?;
                state.store(Some(desc), rows, rowcount, None);
            }
        }
        StatementKind::Query => {
            let desc = describe_fields(py, &result.schema)?;
            let rows = rows_to_tuples(py, result)?;
            let rowcount = i64::try_from(rows.len()).unwrap_or(i64::MAX);
            state.store(Some(desc), rows, rowcount, None);
        }
    }
    Ok(())
}

/// Build a PEP 249 `description` tuple of 7-item tuples
/// `(name, type_name, None, None, None, None, None)` from Spanner result fields.
pub(crate) fn describe_fields(
    py: Python<'_>,
    fields: &[SpannerStructField],
) -> PyResult<Py<PyTuple>> {
    let mut cols = Vec::with_capacity(fields.len());
    for field in fields {
        let type_code = PyString::new(py, field.field_type.code.as_str())
            .into_any()
            .unbind();
        cols.push((field.name.as_str(), Some(type_code), None));
    }
    build_description_7tuple(py, cols)
}

/// Convert all rows in a [`SpannerQueryResult`] into Python tuples via [`spanner_val_to_py`].
pub(crate) fn rows_to_tuples(
    py: Python<'_>,
    result: &SpannerQueryResult,
) -> PyResult<Vec<Py<PyTuple>>> {
    let mut out = Vec::with_capacity(result.rows.len());
    for row in &result.rows {
        let mut cells = Vec::with_capacity(row.len());
        for cell in row {
            cells.push(spanner_val_to_py(py, cell)?);
        }
        out.push(PyTuple::new(py, cells)?.unbind());
    }
    Ok(out)
}

const DDL_IN_RW_TX_MSG: &str =
    "DDL statements cannot be executed inside a Spanner ReadWrite transaction";
const NEED_ENCODER_MSG: &str = "Spanner cursor needs encoded parameters or a param_encoder";

/// Return whether a DML statement includes a `THEN RETURN` (GoogleSQL) or `RETURNING` (PostgreSQL) clause.
pub(crate) fn dml_has_returning_clause(sql: &str) -> bool {
    let bytes = sql.as_bytes();
    let mut i = 0;
    let mut prev_was_then = false;
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
        if matches!(b, b'\'' | b'"' | b'`') {
            let quote = b;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == b'\\' && quote != b'`' && i + 1 < bytes.len() {
                    i += 2;
                    continue;
                }
                if bytes[i] == quote {
                    i += 1;
                    if i < bytes.len() && bytes[i] == quote {
                        i += 1;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            prev_was_then = false;
            continue;
        }
        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            while i < bytes.len() && (bytes[i].is_ascii_alphanumeric() || bytes[i] == b'_') {
                i += 1;
            }
            let word = &sql[start..i];
            if word.eq_ignore_ascii_case("RETURNING")
                || (prev_was_then && word.eq_ignore_ascii_case("RETURN"))
            {
                return true;
            }
            prev_was_then = word.eq_ignore_ascii_case("THEN");
            continue;
        }
        prev_was_then = false;
        i += 1;
    }
    false
}

fn encode_with_encoder(
    py: Python<'_>,
    obj: &Bound<'_, PyAny>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<Vec<SpannerQueryParameter>> {
    let Some(enc) = encoder else {
        return Err(cursor_interface_err(py, NEED_ENCODER_MSG));
    };
    let encoded = enc.bind(py).call1((obj,))?;
    if encoded.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(text_obj) = encoded.cast::<PyString>() {
        let text = text_obj.to_str()?;
        if text.trim().is_empty() {
            return Ok(Vec::new());
        }
        return parse_spanner_parameters_json(Some(text)).map_err(|e| spanner_error_to_py(py, e));
    }
    Err(cursor_interface_err(
        py,
        "param_encoder must return str or None",
    ))
}

fn encode_one_param_set(
    py: Python<'_>,
    params: Option<&Bound<'_, PyAny>>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<Vec<SpannerQueryParameter>> {
    let Some(obj) = params else {
        return Ok(Vec::new());
    };
    if obj.is_none() {
        return Ok(Vec::new());
    }
    if let Ok(tuple) = obj.cast_exact::<PyTuple>() {
        if tuple.is_empty() {
            return Ok(Vec::new());
        }
        return encode_with_encoder(py, obj, encoder);
    }
    if let Ok(list) = obj.cast_exact::<PyList>() {
        if list.is_empty() {
            return Ok(Vec::new());
        }
        return encode_with_encoder(py, obj, encoder);
    }
    if let Ok(dict) = obj.cast_exact::<PyDict>() {
        if dict.is_empty() {
            return Ok(Vec::new());
        }
        return encode_with_encoder(py, obj, encoder);
    }
    if let Ok(text_obj) = obj.cast::<PyString>() {
        let text = text_obj.to_str()?;
        if text.trim().is_empty() {
            return Ok(Vec::new());
        }
        return parse_spanner_parameters_json(Some(text)).map_err(|e| spanner_error_to_py(py, e));
    }
    if let Ok(0) = obj.len() {
        return Ok(Vec::new());
    }
    encode_with_encoder(py, obj, encoder)
}

fn encode_batch_dml_statements(
    py: Python<'_>,
    sql: &str,
    seq_of_params: &Bound<'_, PyAny>,
    encoder: Option<&Py<PyAny>>,
) -> PyResult<Vec<SpannerBatchDmlStatement>> {
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
        let params = encode_one_param_set(py, Some(&item), encoder)?;
        statements.push(SpannerBatchDmlStatement {
            sql: sql.to_string(),
            params,
        });
    }
    Ok(statements)
}

fn lock_state(state: &Mutex<CursorState>) -> MutexGuard<'_, CursorState> {
    state.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Backing handle for a Spanner cursor: either an autocommit client or a read-write transaction.
#[derive(Clone)]
pub(crate) enum CursorSource {
    Client(Arc<NativeSpannerClient>),
    ReadWriteTx(Box<SpannerReadWriteTx>),
}

/// Pure-Rust RPC options copied into background tasks for a Spanner cursor.
#[derive(Clone)]
pub(crate) struct CursorRpcOptions {
    pub(crate) staleness: SpannerStaleness,
    pub(crate) request_tag: Option<String>,
    pub(crate) transaction_tag: Option<String>,
}

enum SpannerCursorOutcome {
    QueryResult(StatementKind, SpannerQueryResult),
    DmlCount(i64),
    Ddl,
}

impl SpannerCursorOutcome {
    fn install_into(self, py: Python<'_>, state: &mut CursorState) -> PyResult<()> {
        match self {
            Self::QueryResult(kind, result) => store_query_result(state, py, kind, &result),
            Self::DmlCount(count) => {
                state.store(None, Vec::new(), count, None);
                Ok(())
            }
            Self::Ddl => {
                state.store(None, Vec::new(), -1, None);
                Ok(())
            }
        }
    }
}

async fn run_execute_rpc(
    source: CursorSource,
    options: CursorRpcOptions,
    kind: StatementKind,
    sql: String,
    params: Vec<SpannerQueryParameter>,
) -> Result<SpannerCursorOutcome, SpannerError> {
    match (source, kind) {
        (CursorSource::Client(client), StatementKind::Query) => {
            let res = client.query_owned(&sql, params, options.staleness).await?;
            Ok(SpannerCursorOutcome::QueryResult(StatementKind::Query, res))
        }
        (CursorSource::Client(client), StatementKind::Dml) => {
            if dml_has_returning_clause(&sql) {
                let tx = client
                    .begin_read_write_transaction(SpannerBeginTransactionOptions {
                        transaction_tag: options.transaction_tag.clone(),
                        ..Default::default()
                    })
                    .await?;
                let res = match tx
                    .query(&sql, &params, options.request_tag.as_deref())
                    .await
                {
                    Ok(res) => res,
                    Err(err) => {
                        let _ = tx.rollback().await;
                        return Err(err);
                    }
                };
                tx.commit(&[]).await?;
                Ok(SpannerCursorOutcome::QueryResult(StatementKind::Dml, res))
            } else {
                let count = client
                    .execute_dml_owned(
                        &sql,
                        params,
                        options.request_tag.as_deref(),
                        options.transaction_tag.as_deref(),
                    )
                    .await?;
                Ok(SpannerCursorOutcome::DmlCount(count))
            }
        }
        (CursorSource::Client(client), StatementKind::Ddl) => {
            let _op = client.update_database_ddl(&[sql]).await?;
            Ok(SpannerCursorOutcome::Ddl)
        }
        (CursorSource::ReadWriteTx(tx), StatementKind::Query) => {
            let res = tx
                .query(&sql, &params, options.request_tag.as_deref())
                .await?;
            Ok(SpannerCursorOutcome::QueryResult(StatementKind::Query, res))
        }
        (CursorSource::ReadWriteTx(tx), StatementKind::Dml) => {
            if dml_has_returning_clause(&sql) {
                let res = tx
                    .query(&sql, &params, options.request_tag.as_deref())
                    .await?;
                Ok(SpannerCursorOutcome::QueryResult(StatementKind::Dml, res))
            } else {
                let count = tx
                    .execute_dml(&sql, &params, options.request_tag.as_deref())
                    .await?;
                Ok(SpannerCursorOutcome::DmlCount(count))
            }
        }
        (CursorSource::ReadWriteTx(_), StatementKind::Ddl) => {
            Err(SpannerError::InvalidArgument(DDL_IN_RW_TX_MSG.to_string()))
        }
    }
}

async fn run_executemany_rpc(
    source: CursorSource,
    options: CursorRpcOptions,
    statements: Vec<SpannerBatchDmlStatement>,
) -> Result<SpannerCursorOutcome, SpannerError> {
    let counts = match source {
        CursorSource::Client(client) => {
            client
                .execute_batch_dml_owned(
                    statements,
                    options.request_tag.as_deref(),
                    options.transaction_tag.as_deref(),
                )
                .await?
        }
        CursorSource::ReadWriteTx(tx) => {
            tx.execute_batch_dml(&statements, options.request_tag.as_deref())
                .await?
        }
    };
    let total = counts.into_iter().fold(0_i64, i64::saturating_add);
    Ok(SpannerCursorOutcome::DmlCount(total))
}

/// Synchronous PEP 249 cursor backed by a Cloud Spanner client or read-write transaction.
#[pyclass(
    name = "SyncSpannerCursor",
    module = "vertebra._spanner",
    frozen,
    immutable_type
)]
pub struct PySyncSpannerCursor {
    source: CursorSource,
    options: CursorRpcOptions,
    param_encoder: Option<Py<PyAny>>,
    state: Mutex<CursorState>,
    execute_lock: Mutex<()>,
}

impl PySyncSpannerCursor {
    pub(crate) fn new(
        source: CursorSource,
        options: CursorRpcOptions,
        param_encoder: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            source,
            options,
            param_encoder,
            state: Mutex::new(CursorState::new()),
            execute_lock: Mutex::new(()),
        }
    }

    fn state(&self) -> MutexGuard<'_, CursorState> {
        lock_state(&self.state)
    }

    fn run_and_install<Fut>(&self, py: Python<'_>, fut: Fut) -> PyResult<()>
    where
        Fut: std::future::Future<Output = Result<SpannerCursorOutcome, SpannerError>> + Send,
    {
        let execute_lock = &self.execute_lock;
        let state_lock = &self.state;
        py.detach(move || {
            let _serial = execute_lock.lock().unwrap_or_else(PoisonError::into_inner);
            let outcome = block_on_db(fut);
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
                        Err(spanner_error_to_py(py, err))
                    }
                }
            })
        })
    }
}

#[pymethods]
impl PySyncSpannerCursor {
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
        let kind = statement_kind(sql);
        if matches!(this.source, CursorSource::ReadWriteTx(_)) && kind == StatementKind::Ddl {
            this.state().clear_result();
            return Err(cursor_not_supported_err(py, DDL_IN_RW_TX_MSG));
        }
        let encoded_params = match encode_one_param_set(py, params, this.param_encoder.as_ref()) {
            Ok(p) => p,
            Err(err) => {
                this.state().clear_result();
                return Err(err);
            }
        };
        let source = this.source.clone();
        let options = this.options.clone();
        let sql_owned = sql.to_string();
        this.run_and_install(
            py,
            run_execute_rpc(source, options, kind, sql_owned, encoded_params),
        )?;
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
        let kind = statement_kind(sql);
        if kind != StatementKind::Dml {
            this.state().clear_result();
            return Err(cursor_not_supported_err(
                py,
                "executemany only supports DML statements on Spanner",
            ));
        }
        let statements = match encode_batch_dml_statements(
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
        let source = this.source.clone();
        let options = this.options.clone();
        this.run_and_install(py, run_executemany_rpc(source, options, statements))?;
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

    #[pyo3(signature = (_exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        _exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> bool {
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

/// Asynchronous PEP 249 cursor backed by a Cloud Spanner client or read-write transaction.
#[pyclass(
    name = "AsyncSpannerCursor",
    module = "vertebra._spanner",
    frozen,
    immutable_type
)]
pub struct PyAsyncSpannerCursor {
    source: CursorSource,
    options: CursorRpcOptions,
    param_encoder: Option<Py<PyAny>>,
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
}

impl PyAsyncSpannerCursor {
    pub(crate) fn new(
        source: CursorSource,
        options: CursorRpcOptions,
        param_encoder: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            source,
            options,
            param_encoder,
            state: Arc::new(Mutex::new(CursorState::new())),
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
        Fut: std::future::Future<Output = Result<SpannerCursorOutcome, SpannerError>>
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
            move |py, (outcome, guard): (SpannerCursorOutcome, ExecutingGuard)| {
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
            |py, (err, guard): (SpannerError, ExecutingGuard)| {
                guard.settle_with(CursorState::clear_result);
                spanner_error_to_py(py, err)
            },
            SPANNER_COMPLETION_DRAINER_ATTR,
        )
    }
}

#[pymethods]
impl PyAsyncSpannerCursor {
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
        let kind = statement_kind(sql);
        if matches!(this.source, CursorSource::ReadWriteTx(_)) && kind == StatementKind::Ddl {
            return Err(cursor_not_supported_err(py, DDL_IN_RW_TX_MSG));
        }
        let encoded_params = encode_one_param_set(py, params, this.param_encoder.as_ref())?;
        let source = this.source.clone();
        let options = this.options.clone();
        let sql_owned = sql.to_string();
        Self::spawn_statement(
            slf,
            guard,
            run_execute_rpc(source, options, kind, sql_owned, encoded_params),
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
        let kind = statement_kind(sql);
        if kind != StatementKind::Dml {
            return Err(cursor_not_supported_err(
                py,
                "executemany only supports DML statements on Spanner",
            ));
        }
        let statements =
            encode_batch_dml_statements(py, sql, seq_of_params, this.param_encoder.as_ref())?;
        let source = this.source.clone();
        let options = this.options.clone();
        Self::spawn_statement(slf, guard, run_executemany_rpc(source, options, statements))
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

/// Register Spanner cursor classes on `vertebra._spanner`.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySyncSpannerCursor>()?;
    m.add_class::<PyAsyncSpannerCursor>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList, PyString, PyTuple};
    use vertebra_db::gcp::GcpErrorInfo;
    use vertebra_db::py_cursor::PlanLruCache;

    use crate::client::SpannerError;
    use crate::codec::{
        SpannerQueryResult, SpannerStructField, SpannerType, SpannerTypeCode, SpannerValue,
    };
    use crate::errors::spanner_error_to_py;

    use super::{
        CursorState, StatementKind, classify_statement_uncached, cursor_interface_err,
        cursor_not_supported_err, cursor_programming_err, describe_fields,
        dml_has_returning_clause, encode_one_param_set, rows_to_tuples, statement_kind,
        store_query_result,
    };

    fn make_row(py: Python<'_>, v: i64) -> Py<PyTuple> {
        PyTuple::new(py, [v]).expect("tuple").unbind()
    }

    fn make_desc(py: Python<'_>) -> Py<PyTuple> {
        let fields = vec![SpannerStructField {
            name: "id".to_string(),
            field_type: SpannerType::scalar(SpannerTypeCode::Int64),
        }];
        describe_fields(py, &fields).expect("describe_fields")
    }

    #[test]
    fn cursor_state_fresh_and_fetch_rules() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();
            assert_eq!(state.rowcount, -1);
            assert_eq!(state.arraysize, 1);
            assert!(state.description.is_none());
            assert!(state.lastrowid.is_none());
            assert!(!state.executed);
            assert!(!state.closed);

            assert!(state.fetchone(py).expect("fetchone fresh").is_none());
            assert!(
                state
                    .fetchmany(py, None)
                    .expect("fetchmany fresh")
                    .bind(py)
                    .is_empty()
            );
            assert!(
                state
                    .fetchall(py)
                    .expect("fetchall fresh")
                    .bind(py)
                    .is_empty()
            );

            state.store(None, Vec::new(), 3, None);
            assert_eq!(state.rowcount, 3);
            assert!(state.description.is_none());
            assert!(
                state
                    .fetchall(py)
                    .expect("fetchall after dml")
                    .bind(py)
                    .is_empty()
            );

            let desc = make_desc(py);
            state.store(Some(desc), Vec::new(), 0, None);
            assert!(state.description.is_some());
            assert!(
                state
                    .fetchall(py)
                    .expect("fetchall empty")
                    .bind(py)
                    .is_empty()
            );
        });
    }

    #[test]
    fn cursor_state_fetchmany_and_fetchall_pagination() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();
            let desc = make_desc(py);
            let rows = (1..=5).map(|i| make_row(py, i)).collect();
            state.store(Some(desc), rows, 5, None);
            state.set_arraysize(py, 2).expect("set_arraysize");
            assert_eq!(state.arraysize, 2);

            assert!(
                state
                    .fetchmany(py, Some(0))
                    .expect("fetchmany 0")
                    .bind(py)
                    .is_empty()
            );
            let chunk1 = state.fetchmany(py, None).expect("chunk 1");
            let chunk2 = state.fetchmany(py, None).expect("chunk 2");
            let chunk3 = state.fetchmany(py, None).expect("chunk 3");
            let chunk4 = state.fetchmany(py, None).expect("chunk 4");
            assert_eq!(chunk1.bind(py).len(), 2);
            assert_eq!(chunk2.bind(py).len(), 2);
            assert_eq!(chunk3.bind(py).len(), 1);
            assert!(chunk4.bind(py).is_empty());

            let desc2 = make_desc(py);
            let rows2 = (10..=14).map(|i| make_row(py, i)).collect();
            state.store(Some(desc2), rows2, 5, None);
            let first = state.fetchone(py).expect("fetchone").expect("row");
            assert_eq!(
                first
                    .bind(py)
                    .get_item(0)
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                10
            );
            let rem = state.fetchall(py).expect("fetchall remainder");
            assert_eq!(rem.bind(py).len(), 4);
            assert!(
                state
                    .fetchall(py)
                    .expect("fetchall exhausted")
                    .bind(py)
                    .is_empty()
            );
        });
    }

    #[test]
    fn cursor_state_closed_and_invalid_arraysize() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();
            let err = state.set_arraysize(py, 0).expect_err("arraysize 0");
            assert_eq!(
                err.get_type(py).name().unwrap().to_str().unwrap(),
                "ProgrammingError"
            );

            state.close();
            state.close();
            assert!(state.closed);
            let closed_err = state.fetchone(py).expect_err("fetchone closed");
            assert_eq!(
                closed_err.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
        });
    }

    #[test]
    fn describe_fields_and_rows_to_tuples_and_store_variants() {
        Python::initialize();
        Python::attach(|py| {
            let fields = vec![
                SpannerStructField {
                    name: "id".to_string(),
                    field_type: SpannerType::scalar(SpannerTypeCode::Int64),
                },
                SpannerStructField {
                    name: "label".to_string(),
                    field_type: SpannerType::scalar(SpannerTypeCode::String),
                },
            ];
            let desc = describe_fields(py, &fields).expect("describe_fields");
            let bound = desc.bind(py);
            assert_eq!(bound.len(), 2);
            let col0 = bound.get_item(0).unwrap().cast_into::<PyTuple>().unwrap();
            assert_eq!(col0.len(), 7);
            assert_eq!(col0.get_item(0).unwrap().extract::<String>().unwrap(), "id");
            assert_eq!(
                col0.get_item(1).unwrap().extract::<String>().unwrap(),
                "INT64"
            );
            for idx in 2..7 {
                assert!(col0.get_item(idx).unwrap().is_none());
            }

            let query_result = SpannerQueryResult {
                columns: vec!["id".to_string(), "label".to_string()],
                schema: fields.clone(),
                rows: vec![vec![
                    SpannerValue::Int64(7),
                    SpannerValue::String("ok".to_string()),
                ]],
                row_count_exact: None,
                row_count_lower_bound: None,
            };
            let tuples = rows_to_tuples(py, &query_result).expect("rows_to_tuples");
            assert_eq!(tuples.len(), 1);
            assert_eq!(
                tuples[0]
                    .bind(py)
                    .get_item(0)
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                7
            );
            assert_eq!(
                tuples[0]
                    .bind(py)
                    .get_item(1)
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "ok"
            );

            let mut state = CursorState::new();
            store_query_result(&mut state, py, StatementKind::Query, &query_result)
                .expect("store query");
            assert!(state.description.is_some());
            assert_eq!(state.rowcount, 1);
            assert!(state.lastrowid.is_none());

            let dml_returning = SpannerQueryResult {
                columns: vec!["id".to_string(), "label".to_string()],
                schema: fields,
                rows: vec![vec![SpannerValue::Int64(9), SpannerValue::Null]],
                row_count_exact: Some(1),
                row_count_lower_bound: None,
            };
            store_query_result(&mut state, py, StatementKind::Dml, &dml_returning)
                .expect("store dml then return");
            assert!(state.description.is_some());
            assert_eq!(state.rowcount, 1);
            assert!(state.lastrowid.is_none());

            let dml_plain = SpannerQueryResult {
                columns: Vec::new(),
                schema: Vec::new(),
                rows: Vec::new(),
                row_count_exact: Some(4),
                row_count_lower_bound: None,
            };
            store_query_result(&mut state, py, StatementKind::Dml, &dml_plain).expect("store dml");
            assert!(state.description.is_none());
            assert_eq!(state.rowcount, 4);
            assert!(state.lastrowid.is_none());

            store_query_result(&mut state, py, StatementKind::Ddl, &dml_plain).expect("store ddl");
            assert!(state.description.is_none());
            assert_eq!(state.rowcount, -1);
            assert!(state.lastrowid.is_none());
        });
    }

    #[test]
    fn statement_kind_classification_rules() {
        assert_eq!(statement_kind("SELECT 1"), StatementKind::Query);
        assert_eq!(
            statement_kind("WITH x AS (SELECT 1) SELECT * FROM x"),
            StatementKind::Query
        );
        assert_eq!(
            statement_kind("  -- c\n update t set a = 1 where true"),
            StatementKind::Dml
        );
        assert_eq!(
            statement_kind("/* block */ INSERT INTO t VALUES (1)"),
            StatementKind::Dml
        );
        assert_eq!(
            statement_kind("DELETE FROM t WHERE true"),
            StatementKind::Dml
        );
        assert_eq!(
            statement_kind("CREATE TABLE t (id INT64) PRIMARY KEY (id)"),
            StatementKind::Ddl
        );
        assert_eq!(
            statement_kind("ALTER TABLE t ADD COLUMN c STRING(MAX)"),
            StatementKind::Ddl
        );
        assert_eq!(statement_kind("DROP INDEX idx"), StatementKind::Ddl);
        assert_eq!(
            statement_kind("@{FORCE_INDEX=idx} SELECT * FROM t"),
            StatementKind::Query
        );
        assert_eq!(
            statement_kind(
                "  -- hint\n@{OPTIMIZER_VERSION=1} /* c */ UPDATE t SET a = 1 WHERE true"
            ),
            StatementKind::Dml
        );
    }

    #[test]
    fn statement_kind_lru_cache_deduplicates_and_evicts() {
        let mut cache = PlanLruCache::new(2);
        let sql = "SELECT 42";
        let k1 = classify_statement_uncached(sql);
        cache.insert(sql.into(), k1);
        cache.insert(sql.into(), k1);
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get(sql), Some(StatementKind::Query));

        cache.insert("UPDATE t SET a = 1".into(), StatementKind::Dml);
        assert_eq!(cache.len(), 2);
        let _ = cache.get(sql);
        cache.insert("DROP TABLE t".into(), StatementKind::Ddl);
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.get(sql), Some(StatementKind::Query));
        assert_eq!(cache.get("UPDATE t SET a = 1"), None);
    }

    #[test]
    fn cursor_state_error_classes_and_spanner_error_attributes() {
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

            let aborted =
                spanner_error_to_py(py, SpannerError::Gcp(GcpErrorInfo::new(10, "aborted")));
            let val = aborted.value(py);
            assert_eq!(val.getattr("code").unwrap().extract::<i32>().unwrap(), 10);
            assert!(
                val.getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
            assert_eq!(
                val.getattr("sqlstate")
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("40001")
            );
            let not_supported = cursor_not_supported_err(py, "no");
            assert_eq!(
                not_supported.get_type(py).name().unwrap().to_str().unwrap(),
                "NotSupportedError"
            );
            assert_eq!(
                not_supported.value(py).str().unwrap().to_str().unwrap(),
                "no"
            );
        });
    }

    #[test]
    fn dml_returning_clause_detection() {
        assert!(dml_has_returning_clause(
            "INSERT INTO t (id) VALUES (1) THEN RETURN id"
        ));
        assert!(dml_has_returning_clause(
            "UPDATE t SET v = 1 WHERE true then\n  return *"
        ));
        assert!(dml_has_returning_clause(
            "DELETE FROM t WHERE id = $1 RETURNING id"
        ));
        assert!(!dml_has_returning_clause("UPDATE t SET v = 1 WHERE true"));
        assert!(!dml_has_returning_clause(
            "UPDATE t SET v = 'THEN RETURN id' WHERE true"
        ));
        assert!(!dml_has_returning_clause(
            "UPDATE t SET returning_col = 1 -- THEN RETURN id\nWHERE true"
        ));
        assert!(!dml_has_returning_clause(
            "UPDATE t SET v = 1 /* RETURNING id */ WHERE true"
        ));
    }

    #[test]
    fn encode_one_param_set_empty_exact_containers_need_no_encoder() {
        Python::initialize();
        Python::attach(|py| {
            let empty_tuple = PyTuple::empty(py).into_any();
            let empty_list = PyList::empty(py).into_any();
            let empty_dict = PyDict::new(py).into_any();

            assert!(
                encode_one_param_set(py, Some(&empty_tuple), None)
                    .expect("empty tuple")
                    .is_empty()
            );
            assert!(
                encode_one_param_set(py, Some(&empty_list), None)
                    .expect("empty list")
                    .is_empty()
            );
            assert!(
                encode_one_param_set(py, Some(&empty_dict), None)
                    .expect("empty dict")
                    .is_empty()
            );
        });
    }

    #[test]
    fn encode_one_param_set_non_empty_exact_containers_call_encoder() {
        Python::initialize();
        Python::attach(|py| {
            let calls = PyList::empty(py);
            let encoder = calls.getattr("append").expect("list.append").unbind();

            let tup = PyTuple::new(py, [1_i64]).expect("tuple").into_any();
            let lst = PyList::new(py, [1_i64]).expect("list").into_any();
            let dct = PyDict::new(py);
            dct.set_item("a", 1_i64).expect("set_item");
            let dct = dct.into_any();

            assert!(
                encode_one_param_set(py, Some(&tup), Some(&encoder))
                    .expect("non-empty tuple")
                    .is_empty()
            );
            assert!(
                encode_one_param_set(py, Some(&lst), Some(&encoder))
                    .expect("non-empty list")
                    .is_empty()
            );
            assert!(
                encode_one_param_set(py, Some(&dct), Some(&encoder))
                    .expect("non-empty dict")
                    .is_empty()
            );

            assert_eq!(calls.len(), 3);
            assert!(calls.get_item(0).expect("item 0").eq(&tup).expect("eq 0"));
            assert!(calls.get_item(1).expect("item 1").eq(&lst).expect("eq 1"));
            assert!(calls.get_item(2).expect("item 2").eq(&dct).expect("eq 2"));

            let err = encode_one_param_set(py, Some(&tup), None)
                .expect_err("non-empty tuple without encoder");
            assert_eq!(
                err.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
            assert_eq!(
                err.value(py).str().unwrap().to_str().unwrap(),
                "Spanner cursor needs encoded parameters or a param_encoder"
            );
        });
    }

    #[test]
    fn encode_one_param_set_text_and_subclass_inputs() {
        Python::initialize();
        Python::attach(|py| {
            let calls = PyList::empty(py);
            let encoder = calls.getattr("append").expect("list.append").unbind();

            let text = PyString::new(py, "{}").into_any();
            assert!(
                encode_one_param_set(py, Some(&text), Some(&encoder))
                    .expect("json text")
                    .is_empty()
            );
            assert_eq!(calls.len(), 0);

            let collections = py.import("collections").expect("import collections");
            let ordered = collections
                .getattr("OrderedDict")
                .expect("OrderedDict")
                .call0()
                .expect("empty OrderedDict");
            assert!(
                encode_one_param_set(py, Some(&ordered), Some(&encoder))
                    .expect("empty OrderedDict")
                    .is_empty()
            );
            assert_eq!(calls.len(), 0);

            let nt_cls = collections
                .getattr("namedtuple")
                .expect("namedtuple")
                .call1(("Pair", ["x"]))
                .expect("define namedtuple");
            let nt = nt_cls.call1((1_i64,)).expect("instantiate namedtuple");
            assert!(
                encode_one_param_set(py, Some(&nt), Some(&encoder))
                    .expect("non-empty namedtuple")
                    .is_empty()
            );
            assert_eq!(calls.len(), 1);
            assert!(calls.get_item(0).expect("item 0").eq(&nt).expect("eq nt"));
        });
    }
}
