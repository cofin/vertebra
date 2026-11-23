//! Shared PEP 249 cursor state, placeholder rewrite plan cache, and sync/async cursor classes for `vertebra-mssql`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PySequence, PyString, PyTuple};
use tiberius::ColumnType;
use tokio::sync::Mutex as AsyncMutex;
use vertebra_db::py_async::{
    ReadyAwaitable, RustDbTaskAwaitable, ready_err, ready_ok, spawn_rust_db_task_with_attr,
};
use vertebra_db::py_cursor::{
    BufferedCursorState, DEFAULT_PLAN_CACHE_CAPACITY, PlanLruCache, build_description_7tuple,
};
use vertebra_db::py_errors::cursor_interface_err;
use vertebra_db::rewrite_mssql_placeholders;

use crate::client::MssqlClient;
use crate::codecs::{MssqlCellValue, MssqlColumnMeta, cell_to_py};
use crate::errors::{MssqlError, mssql_error_to_py};
use crate::params::{
    MssqlParam, prepare_many_sql_and_params, prepare_sql_and_params, py_any_to_mssql_param,
};
use crate::{MSSQL_COMPLETION_DRAINER_ATTR, run_sync_with_completion_queue};

/// Cached placeholder rewrite result for a SQL string.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct RewritePlan {
    pub(crate) sql: Box<str>,
    pub(crate) param_count: usize,
}

static REWRITE_PLAN_CACHE: OnceLock<Mutex<PlanLruCache<Arc<RewritePlan>>>> = OnceLock::new();

fn plan_cache() -> &'static Mutex<PlanLruCache<Arc<RewritePlan>>> {
    REWRITE_PLAN_CACHE.get_or_init(|| Mutex::new(PlanLruCache::new(DEFAULT_PLAN_CACHE_CAPACITY)))
}

/// Return the cached [`RewritePlan`] for `sql`, computing and inserting it on a cache miss.
pub(crate) fn rewrite_plan_for(sql: &str) -> Arc<RewritePlan> {
    {
        let mut cache = plan_cache()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(hit) = cache.get(sql) {
            return hit;
        }
    }

    let (rewritten, param_count) = rewrite_mssql_placeholders(sql);
    let plan = Arc::new(RewritePlan {
        sql: rewritten.into_boxed_str(),
        param_count,
    });

    let mut cache = plan_cache()
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    cache.insert(sql.into(), plan)
}

fn bind_exact_items<'py>(
    plan: Arc<RewritePlan>,
    len: usize,
    items: impl Iterator<Item = Bound<'py, PyAny>>,
) -> Result<(Arc<RewritePlan>, Vec<MssqlParam>), MssqlError> {
    if len != plan.param_count {
        return Err(MssqlError::programming(format!(
            "MSSQL parameter count mismatch: expected {} positional parameter(s), got {len}",
            plan.param_count
        )));
    }

    let mut out = Vec::with_capacity(len);
    for item in items {
        out.push(py_any_to_mssql_param(&item)?);
    }

    Ok((plan, out))
}

/// Resolve the cached rewrite plan for `sql` and convert positional Python parameters.
///
/// Mapping parameters are handled separately by `prepare_sql_and_params`.
pub(crate) fn bind_positional(
    _py: Python<'_>,
    sql: &str,
    params: &Bound<'_, PyAny>,
) -> Result<(Arc<RewritePlan>, Vec<MssqlParam>), MssqlError> {
    let plan = rewrite_plan_for(sql);

    if params.is_none() {
        if plan.param_count != 0 {
            return Err(MssqlError::programming(format!(
                "MSSQL parameter count mismatch: expected {} positional parameter(s), got 0",
                plan.param_count
            )));
        }
        return Ok((plan, Vec::new()));
    }

    if let Ok(tuple) = params.cast_exact::<PyTuple>() {
        return bind_exact_items(plan, tuple.len(), tuple.iter());
    }

    if let Ok(list) = params.cast_exact::<PyList>() {
        return bind_exact_items(plan, list.len(), list.iter());
    }

    if params.is_instance_of::<PyDict>()
        || params.is_instance_of::<PyString>()
        || params.is_instance_of::<PyBytes>()
    {
        return Err(MssqlError::programming(
            "MSSQL query parameters must be a sequence (list/tuple) or mapping (dict)",
        ));
    }

    let seq = params.cast::<PySequence>().map_err(|_| {
        MssqlError::programming(
            "MSSQL query parameters must be a sequence (list/tuple) or mapping (dict)",
        )
    })?;
    let len = seq
        .len()
        .map_err(|e| MssqlError::programming(e.to_string()))?;

    if len != plan.param_count {
        return Err(MssqlError::programming(format!(
            "MSSQL parameter count mismatch: expected {} positional parameter(s), got {len}",
            plan.param_count
        )));
    }

    let mut out = Vec::with_capacity(len);
    for idx in 0..len {
        let item = seq
            .get_item(idx)
            .map_err(|e| MssqlError::programming(e.to_string()))?;
        out.push(py_any_to_mssql_param(&item)?);
    }

    Ok((plan, out))
}

/// Map a TDS [`ColumnType`] to its SQL Server type name, or `None` when untyped (`Null`).
pub(crate) const fn column_type_name(col_type: ColumnType) -> Option<&'static str> {
    match col_type {
        ColumnType::Null => None,
        ColumnType::Bit | ColumnType::Bitn => Some("bit"),
        ColumnType::Int1 => Some("tinyint"),
        ColumnType::Int2 => Some("smallint"),
        ColumnType::Int4 | ColumnType::Intn => Some("int"),
        ColumnType::Int8 => Some("bigint"),
        ColumnType::Float4 => Some("real"),
        ColumnType::Float8 | ColumnType::Floatn => Some("float"),
        ColumnType::Money => Some("money"),
        ColumnType::Money4 => Some("smallmoney"),
        ColumnType::Decimaln => Some("decimal"),
        ColumnType::Numericn => Some("numeric"),
        ColumnType::Guid => Some("uniqueidentifier"),
        ColumnType::Daten => Some("date"),
        ColumnType::Timen => Some("time"),
        ColumnType::Datetime | ColumnType::Datetimen => Some("datetime"),
        ColumnType::Datetime4 => Some("smalldatetime"),
        ColumnType::Datetime2 => Some("datetime2"),
        ColumnType::DatetimeOffsetn => Some("datetimeoffset"),
        ColumnType::Xml => Some("xml"),
        ColumnType::BigVarBin => Some("varbinary"),
        ColumnType::BigBinary => Some("binary"),
        ColumnType::Image => Some("image"),
        ColumnType::BigVarChar => Some("varchar"),
        ColumnType::BigChar => Some("char"),
        ColumnType::NVarchar => Some("nvarchar"),
        ColumnType::NChar => Some("nchar"),
        ColumnType::Text => Some("text"),
        ColumnType::NText => Some("ntext"),
        ColumnType::Udt => Some("udt"),
        ColumnType::SSVariant => Some("sql_variant"),
    }
}

/// Build the PEP 249 `description` tuple of 7-item column tuples, or `None` when empty.
pub(crate) fn describe_columns(
    py: Python<'_>,
    columns: &[MssqlColumnMeta],
) -> PyResult<Option<Py<PyTuple>>> {
    if columns.is_empty() {
        return Ok(None);
    }
    let mut items = Vec::with_capacity(columns.len());
    for col in columns {
        let type_obj = match column_type_name(col.column_type) {
            Some(type_name) => Some(type_name.into_pyobject(py)?.into_any().unbind()),
            None => None,
        };
        items.push((col.name.as_str(), type_obj, None));
    }
    Ok(Some(build_description_7tuple(py, items)?))
}

/// Materialize decoded SQL Server rows into Python tuples once.
pub(crate) fn rows_to_tuples(
    py: Python<'_>,
    rows: &[Vec<MssqlCellValue>],
) -> PyResult<Vec<Py<PyTuple>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(row.len());
        for cell in row {
            cells.push(cell_to_py(py, cell)?);
        }
        out.push(PyTuple::new(py, cells)?.unbind());
    }
    Ok(out)
}

fn prepare_cursor_execute(
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Vec<MssqlParam>)> {
    let res = match params {
        None => bind_positional(py, sql, py.None().bind(py))
            .map(|(plan, bound)| (plan.sql.to_string(), bound)),
        Some(p) if p.is_none() => {
            bind_positional(py, sql, p).map(|(plan, bound)| (plan.sql.to_string(), bound))
        }
        Some(p) if p.is_exact_instance_of::<PyTuple>() || p.is_exact_instance_of::<PyList>() => {
            bind_positional(py, sql, p).map(|(plan, bound)| (plan.sql.to_string(), bound))
        }
        Some(p) => match p.cast::<PyDict>() {
            Ok(dict) => {
                if dict.is_empty() && p.is_exact_instance_of::<PyDict>() {
                    bind_positional(py, sql, py.None().bind(py))
                        .map(|(plan, bound)| (plan.sql.to_string(), bound))
                } else {
                    prepare_sql_and_params(sql, Some(p))
                }
            }
            Err(_) => {
                bind_positional(py, sql, p).map(|(plan, bound)| (plan.sql.to_string(), bound))
            }
        },
    };
    res.map_err(|err| mssql_error_to_py(py, err))
}

struct InFlightGuard(Arc<AtomicBool>);

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Synchronous PEP 249 cursor backed by a shared [`MssqlClient`] handle (`vertebra._mssql.SyncMSSQLCursor`).
#[pyclass(
    name = "SyncMSSQLCursor",
    module = "vertebra._mssql",
    frozen,
    immutable_type
)]
pub struct PySyncMSSQLCursor {
    client: Arc<MssqlClient>,
    state: Arc<Mutex<BufferedCursorState>>,
    exec_lock: Arc<AsyncMutex<()>>,
    lazy_begin: bool,
}

impl PySyncMSSQLCursor {
    /// Construct a new synchronous cursor over `client`.
    pub(crate) fn new(client: Arc<MssqlClient>, lazy_begin: bool) -> Self {
        Self {
            client,
            state: Arc::new(Mutex::new(BufferedCursorState::new())),
            exec_lock: Arc::new(AsyncMutex::new(())),
            lazy_begin,
        }
    }
}

#[pymethods]
impl PySyncMSSQLCursor {
    #[getter]
    fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .description
            .as_ref()
            .map(|d| d.clone_ref(py))
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rowcount
    }

    #[getter]
    fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lastrowid
            .as_ref()
            .map(|v| v.clone_ref(py))
    }

    #[getter]
    fn arraysize(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .arraysize
    }

    #[setter]
    fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set_arraysize(py, value)
    }

    #[pyo3(signature = (sql, params = None))]
    fn execute(
        slf: Py<Self>,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Py<Self>> {
        let (exec_lock, client, state, lazy_begin) = {
            let this = slf.borrow(py);
            (
                Arc::clone(&this.exec_lock),
                Arc::clone(&this.client),
                Arc::clone(&this.state),
                this.lazy_begin,
            )
        };

        let _exec_guard = py.detach(|| exec_lock.blocking_lock_owned());

        {
            let st = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.ensure_open(py)?;
        }

        let (rewritten, bound_params) = match prepare_cursor_execute(py, sql, params) {
            Ok(prepared) => prepared,
            Err(err) => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear_result();
                return Err(err);
            }
        };

        let res = run_sync_with_completion_queue(py, async move {
            if lazy_begin && !client.in_transaction() {
                client.begin_transaction(None).await?;
            }
            client.query(rewritten, bound_params).await
        });

        match res {
            Ok(query_res) => {
                let desc = match describe_columns(py, &query_res.columns) {
                    Ok(d) => d,
                    Err(err) => {
                        state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clear_result();
                        return Err(err);
                    }
                };
                let rows = match rows_to_tuples(py, &query_res.rows) {
                    Ok(r) => r,
                    Err(err) => {
                        state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clear_result();
                        return Err(err);
                    }
                };
                let mut st = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                st.store(desc, rows, query_res.rows_affected as i64, None);
                Ok(slf)
            }
            Err(err) => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear_result();
                Err(mssql_error_to_py(py, err))
            }
        }
    }

    fn executemany(
        slf: Py<Self>,
        py: Python<'_>,
        sql: &str,
        seq_of_params: &Bound<'_, PyAny>,
    ) -> PyResult<Py<Self>> {
        let (exec_lock, client, state, lazy_begin) = {
            let this = slf.borrow(py);
            (
                Arc::clone(&this.exec_lock),
                Arc::clone(&this.client),
                Arc::clone(&this.state),
                this.lazy_begin,
            )
        };

        let _exec_guard = py.detach(|| exec_lock.blocking_lock_owned());

        {
            let st = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.ensure_open(py)?;
        }

        let batches = match prepare_many_sql_and_params(sql, seq_of_params) {
            Ok(b) => b,
            Err(err) => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear_result();
                return Err(mssql_error_to_py(py, err));
            }
        };

        let res = run_sync_with_completion_queue(py, async move {
            if lazy_begin && !client.in_transaction() {
                client.begin_transaction(None).await?;
            }
            client.execute_many(batches).await
        });

        match res {
            Ok(total) => {
                let mut st = state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                st.store(None, Vec::new(), total as i64, None);
                Ok(slf)
            }
            Err(err) => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear_result();
                Err(mssql_error_to_py(py, err))
            }
        }
    }

    fn fetchone(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fetchone(py)
    }

    #[pyo3(signature = (size = None))]
    fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<PyList>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fetchmany(py, size)
    }

    fn fetchall(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fetchall(py)
    }

    fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .close();
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn __exit__(
        &self,
        _exc_type: &Bound<'_, PyAny>,
        _exc_val: &Bound<'_, PyAny>,
        _exc_tb: &Bound<'_, PyAny>,
    ) {
        self.close();
    }
}

/// Asynchronous PEP 249 cursor backed by a shared [`MssqlClient`] handle (`vertebra._mssql.AsyncMSSQLCursor`).
#[pyclass(
    name = "AsyncMSSQLCursor",
    module = "vertebra._mssql",
    frozen,
    immutable_type
)]
pub struct PyAsyncMSSQLCursor {
    client: Arc<MssqlClient>,
    state: Arc<Mutex<BufferedCursorState>>,
    in_flight: Arc<AtomicBool>,
    lazy_begin: bool,
}

impl PyAsyncMSSQLCursor {
    /// Construct a new asynchronous cursor over `client`.
    pub(crate) fn new(client: Arc<MssqlClient>, lazy_begin: bool) -> Self {
        Self {
            client,
            state: Arc::new(Mutex::new(BufferedCursorState::new())),
            in_flight: Arc::new(AtomicBool::new(false)),
            lazy_begin,
        }
    }
}

#[pymethods]
impl PyAsyncMSSQLCursor {
    #[getter]
    fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .description
            .as_ref()
            .map(|d| d.clone_ref(py))
    }

    #[getter]
    fn rowcount(&self) -> i64 {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .rowcount
    }

    #[getter]
    fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .lastrowid
            .as_ref()
            .map(|v| v.clone_ref(py))
    }

    #[getter]
    fn arraysize(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .arraysize
    }

    #[setter]
    fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .set_arraysize(py, value)
    }

    #[pyo3(signature = (sql, params = None))]
    fn execute(
        slf: Py<Self>,
        py: Python<'_>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (client, state, in_flight, lazy_begin) = {
            let this = slf.borrow(py);
            (
                Arc::clone(&this.client),
                Arc::clone(&this.state),
                Arc::clone(&this.in_flight),
                this.lazy_begin,
            )
        };

        {
            let st = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.ensure_open(py)?;
        }

        if in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(cursor_interface_err(py, "cursor is already executing"));
        }
        let guard = InFlightGuard(in_flight);

        let (rewritten, bound_params) = match prepare_cursor_execute(py, sql, params) {
            Ok(prepared) => prepared,
            Err(err) => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear_result();
                return Err(err);
            }
        };

        let state_for_task = Arc::clone(&state);
        let state_for_ok = Arc::clone(&state);
        let slf_for_ok = slf.clone_ref(py);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = async {
                    if lazy_begin && !client.in_transaction() {
                        client.begin_transaction(None).await?;
                    }
                    client.query(rewritten, bound_params).await
                }
                .await;
                match res {
                    Ok(query_res) => Ok((query_res, guard)),
                    Err(err) => {
                        state_for_task
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clear_result();
                        drop(guard);
                        Err(err)
                    }
                }
            },
            move |py, (query_res, _guard)| {
                let desc = match describe_columns(py, &query_res.columns) {
                    Ok(d) => d,
                    Err(err) => {
                        state_for_ok
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clear_result();
                        return Err(err);
                    }
                };
                let rows = match rows_to_tuples(py, &query_res.rows) {
                    Ok(r) => r,
                    Err(err) => {
                        state_for_ok
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clear_result();
                        return Err(err);
                    }
                };
                let mut st = state_for_ok
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                st.store(desc, rows, query_res.rows_affected as i64, None);
                Ok(slf_for_ok.into_any())
            },
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn executemany(
        slf: Py<Self>,
        py: Python<'_>,
        sql: &str,
        seq_of_params: &Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let (client, state, in_flight, lazy_begin) = {
            let this = slf.borrow(py);
            (
                Arc::clone(&this.client),
                Arc::clone(&this.state),
                Arc::clone(&this.in_flight),
                this.lazy_begin,
            )
        };

        {
            let st = state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.ensure_open(py)?;
        }

        if in_flight
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(cursor_interface_err(py, "cursor is already executing"));
        }
        let guard = InFlightGuard(in_flight);

        let batches = match prepare_many_sql_and_params(sql, seq_of_params) {
            Ok(b) => b,
            Err(err) => {
                state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clear_result();
                return Err(mssql_error_to_py(py, err));
            }
        };

        let state_for_task = Arc::clone(&state);
        let state_for_ok = Arc::clone(&state);
        let slf_for_ok = slf.clone_ref(py);
        spawn_rust_db_task_with_attr(
            py,
            async move {
                let res = async {
                    if lazy_begin && !client.in_transaction() {
                        client.begin_transaction(None).await?;
                    }
                    client.execute_many(batches).await
                }
                .await;
                match res {
                    Ok(total) => Ok((total, guard)),
                    Err(err) => {
                        state_for_task
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .clear_result();
                        drop(guard);
                        Err(err)
                    }
                }
            },
            move |_py, (total, _guard)| {
                let mut st = state_for_ok
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                st.store(None, Vec::new(), total as i64, None);
                Ok(slf_for_ok.into_any())
            },
            mssql_error_to_py,
            MSSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let res = {
            let mut st = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.fetchone(py)
        };
        match res {
            Ok(Some(tuple)) => ready_ok(py, tuple.into_any()),
            Ok(None) => ready_ok(py, py.None()),
            Err(err) => ready_err(py, err),
        }
    }

    #[pyo3(signature = (size = None))]
    fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fetchmany(py, size)?;
        ready_ok(py, rows.into_any())
    }

    fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let res = {
            let mut st = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.fetchall(py)
        };
        match res {
            Ok(rows) => ready_ok(py, rows.into_any()),
            Err(err) => ready_err(py, err),
        }
    }

    fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        {
            let mut st = self
                .state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            st.close();
        }
        ready_ok(py, py.None())
    }
}

/// Register `SyncMSSQLCursor` and `AsyncMSSQLCursor` on `vertebra._mssql`.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<PySyncMSSQLCursor>()?;
    m.add_class::<PyAsyncMSSQLCursor>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::errors::MssqlErrorKind;
    use vertebra_db::py_errors::cursor_programming_err;

    fn make_row(py: Python<'_>, value: i64) -> Py<PyTuple> {
        PyTuple::new(py, [value]).unwrap().unbind()
    }

    fn make_desc(py: Python<'_>) -> Py<PyTuple> {
        let cols = vec![MssqlColumnMeta {
            name: "id".to_string(),
            column_type: ColumnType::Int4,
        }];
        describe_columns(py, &cols).unwrap().unwrap()
    }

    #[test]
    fn cursor_state_fresh_and_fetch_without_result_set() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = BufferedCursorState::new();
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
            assert!(state.description.is_none());
            assert!(state.executed);
            assert!(state.fetchone(py).unwrap().is_none());
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());
        });
    }

    #[test]
    fn cursor_state_empty_result_set_and_fetch_pagination() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = BufferedCursorState::new();
            let desc = make_desc(py);
            state.store(Some(desc.clone_ref(py)), Vec::new(), 0, None);
            assert!(state.description.is_some());
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());

            let rows: Vec<Py<PyTuple>> = (1..=5).map(|v| make_row(py, v)).collect();
            state.store(Some(desc.clone_ref(py)), rows, 5, None);
            state.set_arraysize(py, 2).unwrap();
            assert_eq!(state.arraysize, 2);
            assert!(state.fetchmany(py, Some(0)).unwrap().bind(py).is_empty());

            let batch1 = state.fetchmany(py, None).unwrap();
            let batch2 = state.fetchmany(py, None).unwrap();
            let batch3 = state.fetchmany(py, None).unwrap();
            let batch4 = state.fetchmany(py, None).unwrap();
            assert_eq!(batch1.bind(py).len(), 2);
            assert_eq!(batch2.bind(py).len(), 2);
            assert_eq!(batch3.bind(py).len(), 1);
            assert!(batch4.bind(py).is_empty());

            let rows2: Vec<Py<PyTuple>> = (10..=14).map(|v| make_row(py, v)).collect();
            state.store(Some(desc), rows2, 5, None);
            let first = state.fetchone(py).unwrap().unwrap();
            assert_eq!(
                first
                    .bind(py)
                    .get_item(0)
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                10
            );
            let rest = state.fetchall(py).unwrap();
            assert_eq!(rest.bind(py).len(), 4);
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());
        });
    }

    #[test]
    fn cursor_state_close_clear_and_invalid_arraysize() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = BufferedCursorState::new();
            let err = state.set_arraysize(py, 0).unwrap_err();
            assert_eq!(
                err.get_type(py).name().unwrap().to_str().unwrap(),
                "ProgrammingError"
            );

            let desc = make_desc(py);
            state.store(Some(desc), vec![make_row(py, 1)], 1, None);
            state.clear_result();
            assert!(state.description.is_none());
            assert_eq!(state.rowcount, -1);
            assert!(state.lastrowid.is_none());
            assert!(state.rows.is_empty());

            state.close();
            state.close();
            let closed_err = state.fetchone(py).unwrap_err();
            assert_eq!(
                closed_err.get_type(py).name().unwrap().to_str().unwrap(),
                "InterfaceError"
            );
        });
    }

    #[test]
    fn describe_columns_and_rows_to_tuples_match_pep249() {
        Python::initialize();
        Python::attach(|py| {
            assert!(describe_columns(py, &[]).unwrap().is_none());

            let cols = vec![
                MssqlColumnMeta {
                    name: "id".to_string(),
                    column_type: ColumnType::Int4,
                },
                MssqlColumnMeta {
                    name: "label".to_string(),
                    column_type: ColumnType::NVarchar,
                },
                MssqlColumnMeta {
                    name: "empty".to_string(),
                    column_type: ColumnType::Null,
                },
            ];
            let desc = describe_columns(py, &cols).unwrap().unwrap();
            let desc_bound = desc.bind(py);
            assert_eq!(desc_bound.len(), 3);

            let c0 = desc_bound
                .get_item(0)
                .unwrap()
                .cast_into::<PyTuple>()
                .unwrap();
            assert_eq!(c0.len(), 7);
            assert_eq!(c0.get_item(0).unwrap().extract::<String>().unwrap(), "id");
            assert_eq!(c0.get_item(1).unwrap().extract::<String>().unwrap(), "int");
            for idx in 2..7 {
                assert!(c0.get_item(idx).unwrap().is_none());
            }

            let c1 = desc_bound
                .get_item(1)
                .unwrap()
                .cast_into::<PyTuple>()
                .unwrap();
            assert_eq!(c1.len(), 7);
            assert_eq!(
                c1.get_item(0).unwrap().extract::<String>().unwrap(),
                "label"
            );
            assert_eq!(
                c1.get_item(1).unwrap().extract::<String>().unwrap(),
                "nvarchar"
            );

            let c2 = desc_bound
                .get_item(2)
                .unwrap()
                .cast_into::<PyTuple>()
                .unwrap();
            assert_eq!(c2.len(), 7);
            assert_eq!(
                c2.get_item(0).unwrap().extract::<String>().unwrap(),
                "empty"
            );
            assert!(c2.get_item(1).unwrap().is_none());

            let rows = vec![vec![
                MssqlCellValue::Int64(42),
                MssqlCellValue::String("ok".to_string()),
                MssqlCellValue::Null,
            ]];
            let tuples = rows_to_tuples(py, &rows).unwrap();
            assert_eq!(tuples.len(), 1);
            let row0 = tuples[0].bind(py);
            assert_eq!(row0.get_item(0).unwrap().extract::<i64>().unwrap(), 42);
            assert_eq!(row0.get_item(1).unwrap().extract::<String>().unwrap(), "ok");
            assert!(row0.get_item(2).unwrap().is_none());
        });
    }

    #[test]
    fn rewrite_plan_caches_arc_and_matches_rewriter() {
        let sql = "SELECT ? + ?";
        let plan1 = rewrite_plan_for(sql);
        let plan2 = rewrite_plan_for(sql);
        assert!(Arc::ptr_eq(&plan1, &plan2));

        let (expected_sql, expected_count) = rewrite_mssql_placeholders(sql);
        assert_eq!(plan1.sql.as_ref(), expected_sql);
        assert_eq!(plan1.param_count, expected_count);
        assert_eq!(plan1.param_count, 2);

        let literal_sql = "SELECT '?'";
        let literal_plan = rewrite_plan_for(literal_sql);
        assert_eq!(literal_plan.sql.as_ref(), literal_sql);
        assert_eq!(literal_plan.param_count, 0);
    }

    #[test]
    fn bind_positional_converts_values_and_rejects_count_mismatch() {
        Python::initialize();
        Python::attach(|py| {
            let good_params = PyTuple::new(py, [1_i64, 2_i64]).unwrap();
            let (plan, bound) = bind_positional(py, "SELECT ? + ?", good_params.as_any()).unwrap();
            assert_eq!(plan.sql.as_ref(), "SELECT @P1 + @P2");
            assert_eq!(plan.param_count, 2);
            assert_eq!(bound.len(), 2);

            let short_params = PyTuple::new(py, [1_i64]).unwrap();
            let err = bind_positional(py, "SELECT ? + ?", short_params.as_any()).unwrap_err();
            assert_eq!(err.kind, MssqlErrorKind::Programming);

            let at_p_params = PyTuple::new(py, [1_i64, 2_i64]).unwrap();
            let (at_p_plan, at_p_bound) = bind_positional(
                py,
                "INSERT INTO t (a, b, c) VALUES (@P1, @p2, @P1)",
                at_p_params.as_any(),
            )
            .unwrap();
            assert_eq!(
                at_p_plan.sql.as_ref(),
                "INSERT INTO t (a, b, c) VALUES (@P1, @P2, @P1)"
            );
            assert_eq!(at_p_plan.param_count, 2);
            assert_eq!(at_p_bound.len(), 2);

            let at_p_short = PyTuple::new(py, [1_i64]).unwrap();
            let at_p_err = bind_positional(
                py,
                "INSERT INTO t (a, b) VALUES (@P1, @P2)",
                at_p_short.as_any(),
            )
            .unwrap_err();
            assert_eq!(at_p_err.kind, MssqlErrorKind::Programming);
            assert!(
                at_p_err
                    .message
                    .contains("expected 2 positional parameter(s), got 1"),
                "unexpected message: {}",
                at_p_err.message
            );
        });
    }

    #[test]
    fn bind_positional_exact_tuple_and_list_match_sequence_path() {
        Python::initialize();
        Python::attach(|py| {
            let tuple_params = PyTuple::new(py, [1_i64, 2_i64]).unwrap();
            let (tuple_plan, tuple_bound) =
                bind_positional(py, "SELECT ? + ?", tuple_params.as_any()).unwrap();
            assert_eq!(tuple_plan.sql.as_ref(), "SELECT @P1 + @P2");
            assert_eq!(tuple_bound.len(), 2);
            assert!(matches!(tuple_bound[0], MssqlParam::I64(1)));
            assert!(matches!(tuple_bound[1], MssqlParam::I64(2)));

            let list_params = PyList::new(py, [1_i64, 2_i64]).unwrap();
            let (list_plan, list_bound) =
                bind_positional(py, "SELECT ? + ?", list_params.as_any()).unwrap();
            assert_eq!(list_plan.sql.as_ref(), "SELECT @P1 + @P2");
            assert_eq!(list_bound.len(), 2);
            assert!(matches!(list_bound[0], MssqlParam::I64(1)));
            assert!(matches!(list_bound[1], MssqlParam::I64(2)));

            let empty_tuple = PyTuple::empty(py);
            let (et_plan, et_bound) =
                bind_positional(py, "SELECT 1", empty_tuple.as_any()).unwrap();
            assert_eq!(et_plan.sql.as_ref(), "SELECT 1");
            assert!(et_bound.is_empty());

            let empty_list = PyList::empty(py);
            let (el_plan, el_bound) = bind_positional(py, "SELECT 1", empty_list.as_any()).unwrap();
            assert_eq!(el_plan.sql.as_ref(), "SELECT 1");
            assert!(el_bound.is_empty());

            let err = bind_positional(py, "SELECT ?", empty_tuple.as_any()).unwrap_err();
            assert_eq!(err.kind, MssqlErrorKind::Programming);
            assert!(
                err.message.ends_with("got 0"),
                "unexpected message: {}",
                err.message
            );

            let collections = py.import("collections").unwrap();
            let pair_cls = collections
                .call_method1("namedtuple", ("Pair", ("a", "b")))
                .unwrap();
            let pair = pair_cls.call1((1_i64, 2_i64)).unwrap();
            let (nt_plan, nt_bound) = bind_positional(py, "SELECT ? + ?", &pair).unwrap();
            assert_eq!(nt_plan.sql.as_ref(), tuple_plan.sql.as_ref());
            assert_eq!(nt_bound.len(), 2);
            assert!(matches!(nt_bound[0], MssqlParam::I64(1)));
            assert!(matches!(nt_bound[1], MssqlParam::I64(2)));

            let (none_sql, none_bound) = prepare_cursor_execute(py, "SELECT 1", None).unwrap();
            let empty_dict = PyDict::new(py);
            let (dict_sql, dict_bound) =
                prepare_cursor_execute(py, "SELECT 1", Some(empty_dict.as_any())).unwrap();
            assert_eq!(dict_sql, none_sql);
            assert!(none_bound.is_empty());
            assert!(dict_bound.is_empty());

            let (et_exec_sql, et_exec_bound) =
                prepare_cursor_execute(py, "SELECT 1", Some(empty_tuple.as_any())).unwrap();
            assert_eq!(et_exec_sql, none_sql);
            assert!(et_exec_bound.is_empty());

            let (el_exec_sql, el_exec_bound) =
                prepare_cursor_execute(py, "SELECT 1", Some(empty_list.as_any())).unwrap();
            assert_eq!(el_exec_sql, none_sql);
            assert!(el_exec_bound.is_empty());

            let dict_mismatch =
                prepare_cursor_execute(py, "SELECT ?", Some(empty_dict.as_any())).unwrap_err();
            assert_eq!(
                dict_mismatch.get_type(py).name().unwrap().to_str().unwrap(),
                "ProgrammingError"
            );
        });
    }

    #[test]
    fn cursor_state_error_helpers_use_cached_classes() {
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
}
