//! Shared cursor state, named-parameter plan cache, value coercion, state-error helpers,
//! and PEP 249 cursor classes (`SyncMySQLCursor`, `AsyncMySQLCursor`) for `vertebra-mysql`.

use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use mysql_async::Params;
use pyo3::exceptions::{PyKeyError, PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyString, PyTuple, PyType,
};
use vertebra_db::py_async::{
    ReadyAwaitable, TaskAwaitable, block_on_db, ready_ok, spawn_rust_db_task_with_attr,
};
use vertebra_db::py_cursor::{
    BufferedCursorState, DEFAULT_PLAN_CACHE_CAPACITY, PlanLruCache, build_description_7tuple,
};
use vertebra_db::py_errors::{cursor_interface_err, cursor_programming_err};

use crate::client::{
    DecodedCell, DecodedResultSet, MYSQL_COMPLETION_DRAINER_ATTR, MySQLClientCore,
    py_param_to_mysql_value,
};
use crate::errors::MySQLError;

/// Convert a [`DecodedResultSet`] into buffered cursor state according to PEP 249 rules.
pub(crate) fn store_result_set(
    state: &mut BufferedCursorState,
    py: Python<'_>,
    rs: DecodedResultSet,
) -> PyResult<()> {
    let lastrowid = match rs.last_insert_id {
        Some(id) if id > 0 => Some(id.into_pyobject(py)?.into_any().unbind()),
        _ => None,
    };
    if rs.columns.is_empty() {
        let rowcount = i64::try_from(rs.affected_rows).unwrap_or(i64::MAX);
        state.store(None, Vec::new(), rowcount, lastrowid);
    } else {
        let description = describe_columns(py, &rs.columns)?;
        let rowcount = i64::try_from(rs.rows.len()).unwrap_or(i64::MAX);
        let rows = rows_to_tuples(py, rs.rows)?;
        state.store(Some(description), rows, rowcount, lastrowid);
    }
    Ok(())
}

/// Build a PEP 249 `description` tuple of 7-item column tuples `(name, type_code, None, None, None, None, None)`.
pub(crate) fn describe_columns(py: Python<'_>, columns: &[(String, u8)]) -> PyResult<Py<PyTuple>> {
    let mut entries = Vec::with_capacity(columns.len());
    for (name, type_code) in columns {
        let tc = (*type_code).into_pyobject(py)?.into_any().unbind();
        entries.push((name.as_str(), Some(tc), None));
    }
    build_description_7tuple(py, entries)
}

/// Convert decoded MySQL rows into Python tuple objects once.
pub(crate) fn rows_to_tuples(
    py: Python<'_>,
    rows: Vec<Vec<DecodedCell>>,
) -> PyResult<Vec<Py<PyTuple>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let mut cells = Vec::with_capacity(row.len());
        for cell in row {
            cells.push(cell.into_py_any(py)?);
        }
        out.push(PyTuple::new(py, cells)?.unbind());
    }
    Ok(out)
}

/// Compiled named-parameter rewrite plan for a single SQL string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NamedPlan {
    pub(crate) sql: Box<str>,
    pub(crate) names: Box<[Box<str>]>,
}

static NAMED_PLAN_CACHE: LazyLock<Mutex<PlanLruCache<Arc<NamedPlan>>>> =
    LazyLock::new(|| Mutex::new(PlanLruCache::new(DEFAULT_PLAN_CACHE_CAPACITY)));

fn consume_literal_or_comment(sql: &str, index: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let ch = bytes[index];
    if ch == b'\'' || ch == b'"' || ch == b'`' {
        let quote = ch;
        let mut cursor = index + 1;
        while cursor < len {
            let c = bytes[cursor];
            if c == b'\\' && quote != b'`' && cursor + 1 < len {
                let next_byte = bytes[cursor + 1];
                if next_byte < 0x80 {
                    cursor += 2;
                } else {
                    let step = sql[cursor + 1..].chars().next().map_or(1, char::len_utf8);
                    cursor += 1 + step;
                }
                continue;
            }
            if c == quote {
                if cursor + 1 < len && bytes[cursor + 1] == quote {
                    cursor += 2;
                    continue;
                }
                return Some(cursor + 1);
            }
            cursor += 1;
        }
        return Some(len);
    }
    if (ch == b'-' && index + 1 < len && bytes[index + 1] == b'-') || ch == b'#' {
        let start = index + if ch == b'-' { 2 } else { 1 };
        return Some(match sql[start..].find('\n') {
            Some(rel) => start + rel + 1,
            None => len,
        });
    }
    if ch == b'/' && index + 1 < len && bytes[index + 1] == b'*' {
        let start = index + 2;
        return Some(match sql[start..].find("*/") {
            Some(rel) => start + rel + 2,
            None => len,
        });
    }
    None
}

fn compile_named_plan(sql: &str) -> NamedPlan {
    let bytes = sql.as_bytes();
    let len = bytes.len();
    let mut out = String::with_capacity(len);
    let mut names: Vec<Box<str>> = Vec::new();
    let mut i = 0usize;

    while i < len {
        let b = bytes[i];
        if matches!(b, b'\'' | b'"' | b'`' | b'-' | b'#' | b'/')
            && let Some(next_i) = consume_literal_or_comment(sql, i)
        {
            out.push_str(&sql[i..next_i]);
            i = next_i;
            continue;
        }
        if b == b'%'
            && i + 2 < len
            && bytes[i + 1] == b'('
            && let Some(rel_close) = sql[i + 2..].find(")s")
        {
            let close_idx = i + 2 + rel_close;
            let name = &sql[i + 2..close_idx];
            names.push(name.into());
            out.push('?');
            i = close_idx + 2;
            continue;
        }
        if b == b':' && i + 1 < len {
            let mut chars = sql[i + 1..].char_indices();
            if let Some((_, first)) = chars.next()
                && (first.is_alphabetic() || first == '_')
            {
                if i > 0 && bytes[i - 1] == b':' {
                    out.push(':');
                    i += 1;
                    continue;
                }
                let mut end = i + 1 + first.len_utf8();
                for (rel_idx, ch) in chars {
                    if ch.is_alphanumeric() || ch == '_' {
                        end = i + 1 + rel_idx + ch.len_utf8();
                    } else {
                        break;
                    }
                }
                let name = &sql[i + 1..end];
                names.push(name.into());
                out.push('?');
                i = end;
                continue;
            }
        }
        if b < 0x80 {
            out.push(b as char);
            i += 1;
        } else if let Some(ch) = sql[i..].chars().next() {
            out.push(ch);
            i += ch.len_utf8();
        } else {
            break;
        }
    }

    NamedPlan {
        sql: out.into_boxed_str(),
        names: names.into_boxed_slice(),
    }
}

/// Return a cached [`NamedPlan`] rewriting `:name` and `%(name)s` placeholders to `?`.
pub(crate) fn named_plan_for(sql: &str) -> Result<Arc<NamedPlan>, MySQLError> {
    {
        let mut cache = NAMED_PLAN_CACHE
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if let Some(plan) = cache.get(sql) {
            return Ok(plan);
        }
    }

    let plan = Arc::new(compile_named_plan(sql));
    let mut cache = NAMED_PLAN_CACHE
        .lock()
        .unwrap_or_else(PoisonError::into_inner);
    Ok(cache.insert(sql.into(), plan))
}

struct CoerceTypes {
    mapping_abc: Py<PyType>,
    memoryview_type: Py<PyType>,
    uuid_type: Py<PyType>,
    msgspec_struct: Option<Py<PyType>>,
    encode_json: Py<PyAny>,
}

static COERCE_TYPES: PyOnceLock<CoerceTypes> = PyOnceLock::new();

fn resolve_coerce_types(py: Python<'_>) -> PyResult<&'static CoerceTypes> {
    COERCE_TYPES.get_or_try_init(py, || {
        let mapping_abc = py
            .import("collections.abc")?
            .getattr("Mapping")?
            .cast_into::<PyType>()?
            .unbind();
        let memoryview_type = py
            .import("builtins")?
            .getattr("memoryview")?
            .cast_into::<PyType>()?
            .unbind();
        let uuid_type = py
            .import("uuid")?
            .getattr("UUID")?
            .cast_into::<PyType>()?
            .unbind();
        let msgspec_struct = py
            .import("msgspec")
            .and_then(|m| m.getattr("Struct"))
            .and_then(|t| t.cast_into::<PyType>().map_err(Into::into))
            .ok()
            .map(Bound::unbind);
        let encode_json = match py
            .import("vertebra.utils.serializers")
            .and_then(|m| m.getattr("encode_json"))
        {
            Ok(func) => func.unbind(),
            Err(_) => py.import("json")?.getattr("dumps")?.unbind(),
        };
        Ok(CoerceTypes {
            mapping_abc,
            memoryview_type,
            uuid_type,
            msgspec_struct,
            encode_json,
        })
    })
}

fn missing_bind_err(py: Python<'_>, name: &str) -> PyErr {
    let repr_name = PyString::new(py, name)
        .repr()
        .map_or_else(|_| format!("'{name}'"), |r| r.to_string());
    cursor_programming_err(py, &format!("Missing MySQL bind parameter {repr_name}"))
}

fn bind_named_dict<'py, 's>(
    py: Python<'py>,
    sql: &'s str,
    dict: &Bound<'py, PyDict>,
) -> PyResult<(Cow<'s, str>, Vec<Bound<'py, PyAny>>)> {
    let plan = named_plan_for(sql).map_err(|e| e.to_py_err(py))?;
    if plan.names.is_empty() {
        return Ok((Cow::Borrowed(sql), Vec::new()));
    }
    let mut values = Vec::with_capacity(plan.names.len());
    for name in &plan.names {
        match dict.get_item(name.as_ref())? {
            Some(val) => values.push(val),
            None => return Err(missing_bind_err(py, name)),
        }
    }
    Ok((Cow::Owned(plan.sql.to_string()), values))
}

/// Bind positional or named parameters against `sql`, returning the SQL to execute and ordered values.
pub(crate) fn bind_parameters<'py, 's>(
    py: Python<'py>,
    sql: &'s str,
    params: &Bound<'py, PyAny>,
) -> PyResult<(Cow<'s, str>, Vec<Bound<'py, PyAny>>)> {
    if params.is_none() {
        return Ok((Cow::Borrowed(sql), Vec::new()));
    }

    if let Ok(tuple) = params.cast_exact::<PyTuple>() {
        let compiled = normalize_positional_sql(sql);
        if tuple.is_empty() {
            return Ok((compiled, Vec::new()));
        }
        return Ok((compiled, tuple.iter().collect()));
    }

    if let Ok(list) = params.cast_exact::<PyList>() {
        let compiled = normalize_positional_sql(sql);
        if list.is_empty() {
            return Ok((compiled, Vec::new()));
        }
        return Ok((compiled, list.iter().collect()));
    }

    if let Ok(dict) = params.cast_exact::<PyDict>() {
        if dict.is_empty() {
            return Ok((Cow::Borrowed(sql), Vec::new()));
        }
        return bind_named_dict(py, sql, dict);
    }

    if let Ok(dict) = params.cast::<PyDict>() {
        return bind_named_dict(py, sql, dict);
    }

    if let Ok(tuple) = params.cast::<PyTuple>() {
        let compiled = normalize_positional_sql(sql);
        if tuple.is_empty() {
            return Ok((compiled, Vec::new()));
        }
        let values = tuple.iter().collect();
        return Ok((compiled, values));
    }

    if let Ok(list) = params.cast::<PyList>() {
        let compiled = normalize_positional_sql(sql);
        if list.is_empty() {
            return Ok((compiled, Vec::new()));
        }
        let values = list.iter().collect();
        return Ok((compiled, values));
    }

    let types = resolve_coerce_types(py)?;
    if params.is_instance(types.mapping_abc.bind(py))? {
        let plan = named_plan_for(sql).map_err(|e| e.to_py_err(py))?;
        if plan.names.is_empty() {
            return Ok((Cow::Borrowed(sql), Vec::new()));
        }
        let mut values = Vec::with_capacity(plan.names.len());
        for name in &plan.names {
            match params.get_item(name.as_ref()) {
                Ok(val) => values.push(val),
                Err(err) if err.is_instance_of::<PyKeyError>(py) => {
                    return Err(missing_bind_err(py, name));
                }
                Err(err) => return Err(err),
            }
        }
        return Ok((Cow::Owned(plan.sql.to_string()), values));
    }

    if params.is_instance_of::<PyString>()
        || params.is_instance_of::<PyBytes>()
        || params.is_instance_of::<PyByteArray>()
    {
        return Err(cursor_interface_err(
            py,
            "parameters must be a sequence or mapping",
        ));
    }

    let iter = params
        .try_iter()
        .map_err(|_| cursor_interface_err(py, "parameters must be a sequence or mapping"))?;
    let mut values = Vec::new();
    for item in iter {
        values.push(item?);
    }
    Ok((normalize_positional_sql(sql), values))
}

fn normalize_positional_sql(sql: &str) -> Cow<'_, str> {
    if sql.as_bytes().contains(&b'%') {
        Cow::Owned(crate::protocol::normalize_mysql_placeholders(sql).0)
    } else {
        Cow::Borrowed(sql)
    }
}

/// Coerce a Python bind value into a MySQL-compatible parameter value.
pub(crate) fn coerce_value<'py>(
    py: Python<'py>,
    value: Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyAny>> {
    if value.is_none()
        || value.is_exact_instance_of::<PyBool>()
        || value.is_exact_instance_of::<PyInt>()
        || value.is_exact_instance_of::<PyFloat>()
        || value.is_exact_instance_of::<PyString>()
        || value.is_exact_instance_of::<PyBytes>()
    {
        return Ok(value);
    }

    if value
        .get_type()
        .name()
        .is_ok_and(|name| name == "TypedParameter")
    {
        let inner = value.getattr("value")?;
        return coerce_value(py, inner);
    }

    if let Ok(ba) = value.cast::<PyByteArray>() {
        return Ok(PyBytes::new(py, &ba.to_vec()).into_any());
    }

    let types = resolve_coerce_types(py)?;
    if value.is_instance(types.memoryview_type.bind(py))? {
        return py.get_type::<PyBytes>().call1((value,));
    }

    if value.is_instance(types.uuid_type.bind(py))? {
        return Ok(value.str()?.into_any());
    }

    if value.is_instance_of::<PyDict>() || value.is_instance_of::<PyList>() {
        return types.encode_json.bind(py).call1((value,));
    }

    if let Some(ref struct_ty) = types.msgspec_struct
        && value.is_instance(struct_ty.bind(py))?
    {
        return types.encode_json.bind(py).call1((value,));
    }

    Ok(value)
}

fn remap_param_conversion_err(py: Python<'_>, err: PyErr) -> PyErr {
    if err.is_instance_of::<PyTypeError>(py) || err.is_instance_of::<PyValueError>(py) {
        cursor_interface_err(py, &err.to_string())
    } else {
        err
    }
}

fn prepare_cursor_params(
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Params)> {
    let Some(params_obj) = params else {
        return Ok((sql.to_owned(), Params::Empty));
    };
    let (rewritten_sql, raw_values) =
        bind_parameters(py, sql, params_obj).map_err(|err| remap_param_conversion_err(py, err))?;
    if raw_values.is_empty() {
        return Ok((rewritten_sql.into_owned(), Params::Empty));
    }
    let mut mysql_vals = Vec::with_capacity(raw_values.len());
    for raw in raw_values {
        let coerced = coerce_value(py, raw).map_err(|err| remap_param_conversion_err(py, err))?;
        let val = py_param_to_mysql_value(&coerced).map_err(|e| e.to_py_err(py))?;
        mysql_vals.push(val);
    }
    Ok((rewritten_sql.into_owned(), Params::Positional(mysql_vals)))
}

fn prepare_cursor_many_params(
    py: Python<'_>,
    sql: &str,
    seq_of_params: &Bound<'_, PyAny>,
) -> PyResult<(String, Vec<Params>)> {
    if seq_of_params.is_instance_of::<PyString>()
        || seq_of_params.is_instance_of::<PyBytes>()
        || seq_of_params.is_instance_of::<PyByteArray>()
    {
        return Err(cursor_interface_err(
            py,
            "seq_of_params must be a sequence of parameter sequences or mappings",
        ));
    }
    let iter = seq_of_params
        .try_iter()
        .map_err(|err| remap_param_conversion_err(py, err))?;
    let mut normalized_sql: Option<String> = None;
    let mut compiled_sets = Vec::new();
    for item in iter {
        let item = item.map_err(|err| remap_param_conversion_err(py, err))?;
        let (rewritten, params) = prepare_cursor_params(py, sql, Some(&item))?;
        if normalized_sql.is_none() {
            normalized_sql = Some(rewritten);
        }
        compiled_sets.push(params);
    }
    Ok((
        normalized_sql.unwrap_or_else(|| sql.to_owned()),
        compiled_sets,
    ))
}

/// Rewrite named MySQL parameters (`:name`, `%(name)s`) to `?` and coerce bind values.
#[pyfunction]
#[pyo3(signature = (sql, parameters = None))]
pub fn compile_binds(
    py: Python<'_>,
    sql: &str,
    parameters: Option<&Bound<'_, PyAny>>,
) -> PyResult<(String, Py<PyTuple>)> {
    let Some(params) = parameters else {
        return Ok((sql.to_owned(), PyTuple::empty(py).unbind()));
    };
    let (rewritten_sql, raw_values) =
        bind_parameters(py, sql, params).map_err(|err| remap_param_conversion_err(py, err))?;
    let mut coerced = Vec::with_capacity(raw_values.len());
    for val in raw_values {
        coerced.push(coerce_value(py, val).map_err(|err| remap_param_conversion_err(py, err))?);
    }
    let tuple = PyTuple::new(py, coerced)?.unbind();
    Ok((rewritten_sql.into_owned(), tuple))
}

/// Synchronous PEP 249 cursor backed by [`MySQLClientCore`].
#[pyclass(module = "vertebra._mysql", frozen, immutable_type)]
pub struct SyncMySQLCursor {
    core: Arc<MySQLClientCore>,
    state: Mutex<BufferedCursorState>,
    execute_lock: Mutex<()>,
    lazy_begin: bool,
}

impl SyncMySQLCursor {
    pub(crate) const fn new(core: Arc<MySQLClientCore>, lazy_begin: bool) -> Self {
        Self {
            core,
            state: Mutex::new(BufferedCursorState::new()),
            execute_lock: Mutex::new(()),
            lazy_begin,
        }
    }

    fn ensure_open(&self, py: Python<'_>) -> PyResult<()> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.ensure_open(py)
    }

    fn clear_on_error(&self, err: PyErr) -> PyErr {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.clear_result();
        err
    }
}

#[pymethods]
impl SyncMySQLCursor {
    #[getter]
    pub fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.description.as_ref().map(|d| d.clone_ref(py))
    }

    #[getter]
    pub fn rowcount(&self) -> i64 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .rowcount
    }

    #[getter]
    pub fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.lastrowid.as_ref().map(|id| id.clone_ref(py))
    }

    #[getter]
    pub fn arraysize(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .arraysize
    }

    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_arraysize(py, value)
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.ensure_open(py)?;
        let (norm_sql, bound_params) =
            prepare_cursor_params(py, sql, params).map_err(|err| this.clear_on_error(err))?;

        let core = Arc::clone(&this.core);
        let lazy_begin = this.lazy_begin;
        let execute_lock = &this.execute_lock;
        let result = py.detach(|| {
            let _gate = execute_lock.lock().unwrap_or_else(PoisonError::into_inner);
            block_on_db(async move {
                if lazy_begin && !core.in_transaction() {
                    core.begin(None, false).await?;
                }
                core.execute_result_set(norm_sql, bound_params).await
            })
        });

        let mut state = this.state.lock().unwrap_or_else(PoisonError::into_inner);
        match result {
            Ok(rs) => {
                if state.closed {
                    state.clear_result();
                } else if let Err(err) = store_result_set(&mut state, py, rs) {
                    state.clear_result();
                    return Err(err);
                }
                Ok(slf.clone())
            }
            Err(err) => {
                state.clear_result();
                Err(err.to_py_err(py))
            }
        }
    }

    pub fn executemany<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        seq_of_params: &Bound<'py, PyAny>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.ensure_open(py)?;
        let (norm_sql, compiled_sets) = prepare_cursor_many_params(py, sql, seq_of_params)
            .map_err(|err| this.clear_on_error(err))?;

        if compiled_sets.is_empty() {
            let mut state = this.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.ensure_open(py)?;
            state.store(None, Vec::new(), 0, None);
            return Ok(slf.clone());
        }

        let core = Arc::clone(&this.core);
        let lazy_begin = this.lazy_begin;
        let execute_lock = &this.execute_lock;
        let result = py.detach(|| {
            let _gate = execute_lock.lock().unwrap_or_else(PoisonError::into_inner);
            block_on_db(async move {
                if lazy_begin && !core.in_transaction() {
                    core.begin(None, false).await?;
                }
                core.execute_many_batch(norm_sql, compiled_sets).await
            })
        });

        let mut state = this.state.lock().unwrap_or_else(PoisonError::into_inner);
        match result {
            Ok(total) => {
                if state.closed {
                    state.clear_result();
                } else {
                    let rowcount = i64::try_from(total).unwrap_or(i64::MAX);
                    state.store(None, Vec::new(), rowcount, None);
                }
                Ok(slf.clone())
            }
            Err(err) => {
                state.clear_result();
                Err(err.to_py_err(py))
            }
        }
    }

    pub fn fetchone(&self, py: Python<'_>) -> PyResult<Option<Py<PyTuple>>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.fetchone(py)
    }

    #[pyo3(signature = (size = None))]
    pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<PyList>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.fetchmany(py, size)
    }

    pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<PyList>> {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.fetchall(py)
    }

    pub fn close(&self) {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .close();
    }

    pub fn __enter__<'py>(slf: &Bound<'py, Self>) -> Bound<'py, Self> {
        slf.clone()
    }

    #[pyo3(signature = (exc_type = None, exc_val = None, exc_tb = None))]
    pub fn __exit__(
        &self,
        exc_type: Option<&Bound<'_, PyAny>>,
        exc_val: Option<&Bound<'_, PyAny>>,
        exc_tb: Option<&Bound<'_, PyAny>>,
    ) -> bool {
        let _ = (exc_type, exc_val, exc_tb);
        self.close();
        false
    }
}

struct ExecutingGuard {
    state: Arc<Mutex<BufferedCursorState>>,
    executing: Arc<AtomicBool>,
    settled: bool,
}

impl ExecutingGuard {
    fn try_acquire(
        py: Python<'_>,
        state: &Arc<Mutex<BufferedCursorState>>,
        executing: &Arc<AtomicBool>,
    ) -> PyResult<Self> {
        if executing
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return Err(cursor_interface_err(py, "cursor is already executing"));
        }
        Ok(Self {
            state: Arc::clone(state),
            executing: Arc::clone(executing),
            settled: false,
        })
    }

    fn settle_with<F>(mut self, apply: F) -> PyResult<()>
    where
        F: FnOnce(&mut BufferedCursorState) -> PyResult<()>,
    {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        let res = apply(&mut state);
        if res.is_err() {
            state.clear_result();
        }
        self.settled = true;
        drop(state);
        res
    }

    fn clear_and_settle(mut self) {
        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.clear_result();
        self.settled = true;
    }
}

impl Drop for ExecutingGuard {
    fn drop(&mut self) {
        if !self.settled {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.clear_result();
            self.settled = true;
        }
        self.executing.store(false, Ordering::Release);
    }
}

/// Asynchronous PEP 249 cursor backed by [`MySQLClientCore`] and `vertebra_db::py_async`.
#[pyclass(module = "vertebra._mysql", frozen, immutable_type)]
pub struct AsyncMySQLCursor {
    core: Arc<MySQLClientCore>,
    state: Arc<Mutex<BufferedCursorState>>,
    executing: Arc<AtomicBool>,
    lazy_begin: bool,
}

impl AsyncMySQLCursor {
    pub(crate) fn new(core: Arc<MySQLClientCore>, lazy_begin: bool) -> Self {
        Self {
            core,
            state: Arc::new(Mutex::new(BufferedCursorState::new())),
            executing: Arc::new(AtomicBool::new(false)),
            lazy_begin,
        }
    }

    fn ensure_open(&self, py: Python<'_>) -> PyResult<()> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.ensure_open(py)
    }
}

#[pymethods]
impl AsyncMySQLCursor {
    #[getter]
    pub fn description(&self, py: Python<'_>) -> Option<Py<PyTuple>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.description.as_ref().map(|d| d.clone_ref(py))
    }

    #[getter]
    pub fn rowcount(&self) -> i64 {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .rowcount
    }

    #[getter]
    pub fn lastrowid(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.lastrowid.as_ref().map(|id| id.clone_ref(py))
    }

    #[getter]
    pub fn arraysize(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .arraysize
    }

    #[setter]
    pub fn set_arraysize(&self, py: Python<'_>, value: i64) -> PyResult<()> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .set_arraysize(py, value)
    }

    #[pyo3(signature = (sql, params = None), text_signature = "($self, sql, params=())")]
    pub fn execute(
        slf: &Bound<'_, Self>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<TaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        this.ensure_open(py)?;
        let guard = ExecutingGuard::try_acquire(py, &this.state, &this.executing)?;
        let (norm_sql, bound_params) = prepare_cursor_params(py, sql, params)?;

        let cursor_obj = slf.clone().into_any().unbind();
        let core = Arc::clone(&this.core);
        let lazy_begin = this.lazy_begin;
        spawn_rust_db_task_with_attr(
            py,
            async move {
                if lazy_begin
                    && !core.in_transaction()
                    && let Err(err) = core.begin(None, false).await
                {
                    return Err((err, guard));
                }
                match core.execute_result_set(norm_sql, bound_params).await {
                    Ok(rs) => Ok((rs, guard)),
                    Err(err) => Err((err, guard)),
                }
            },
            move |py, (rs, guard): (DecodedResultSet, ExecutingGuard)| {
                guard.settle_with(|state| {
                    if state.closed {
                        state.clear_result();
                        Ok(())
                    } else {
                        store_result_set(state, py, rs)
                    }
                })?;
                Ok(cursor_obj)
            },
            |py, (err, guard): (MySQLError, ExecutingGuard)| {
                guard.clear_and_settle();
                err.to_py_err(py)
            },
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn executemany(
        slf: &Bound<'_, Self>,
        sql: &str,
        seq_of_params: &Bound<'_, PyAny>,
    ) -> PyResult<TaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        this.ensure_open(py)?;
        let guard = ExecutingGuard::try_acquire(py, &this.state, &this.executing)?;
        let (norm_sql, compiled_sets) = prepare_cursor_many_params(py, sql, seq_of_params)?;

        let cursor_obj = slf.clone().into_any().unbind();
        if compiled_sets.is_empty() {
            return spawn_rust_db_task_with_attr(
                py,
                async move { Ok::<(u64, ExecutingGuard), (MySQLError, ExecutingGuard)>((0, guard)) },
                move |_py, (total, guard): (u64, ExecutingGuard)| {
                    guard.settle_with(|state| {
                        if state.closed {
                            state.clear_result();
                        } else {
                            let rowcount = i64::try_from(total).unwrap_or(i64::MAX);
                            state.store(None, Vec::new(), rowcount, None);
                        }
                        Ok(())
                    })?;
                    Ok(cursor_obj)
                },
                |py, (err, guard): (MySQLError, ExecutingGuard)| {
                    guard.clear_and_settle();
                    err.to_py_err(py)
                },
                MYSQL_COMPLETION_DRAINER_ATTR,
            );
        }

        let core = Arc::clone(&this.core);
        let lazy_begin = this.lazy_begin;
        spawn_rust_db_task_with_attr(
            py,
            async move {
                if lazy_begin
                    && !core.in_transaction()
                    && let Err(err) = core.begin(None, false).await
                {
                    return Err((err, guard));
                }
                match core.execute_many_batch(norm_sql, compiled_sets).await {
                    Ok(total) => Ok((total, guard)),
                    Err(err) => Err((err, guard)),
                }
            },
            move |_py, (total, guard): (u64, ExecutingGuard)| {
                guard.settle_with(|state| {
                    if state.closed {
                        state.clear_result();
                    } else {
                        let rowcount = i64::try_from(total).unwrap_or(i64::MAX);
                        state.store(None, Vec::new(), rowcount, None);
                    }
                    Ok(())
                })?;
                Ok(cursor_obj)
            },
            |py, (err, guard): (MySQLError, ExecutingGuard)| {
                guard.clear_and_settle();
                err.to_py_err(py)
            },
            MYSQL_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let row = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.fetchone(py)?
        };
        ready_ok(py, row.map_or_else(|| py.None(), Py::into_any))
    }

    #[pyo3(signature = (size = None))]
    pub fn fetchmany(&self, py: Python<'_>, size: Option<i64>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.fetchmany(py, size)?
        };
        ready_ok(py, rows.into_any())
    }

    pub fn fetchall(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let rows = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.fetchall(py)?
        };
        ready_ok(py, rows.into_any())
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .close();
        ready_ok(py, py.None())
    }
}

/// Register cursor classes and `compile_binds` on the `vertebra._mysql` module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SyncMySQLCursor>()?;
    m.add_class::<AsyncMySQLCursor>()?;
    m.add_function(wrap_pyfunction!(compile_binds, m)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn err_type_name(py: Python<'_>, err: &PyErr) -> String {
        err.get_type(py)
            .name()
            .map_or_else(|_| String::new(), |n| n.to_string())
    }

    fn make_row(py: Python<'_>, v: i64) -> Py<PyTuple> {
        PyTuple::new(py, [v]).unwrap().unbind()
    }

    #[test]
    fn test_cursor_state_fresh_and_fetch_before_execute() {
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
        });
    }

    #[test]
    fn test_cursor_state_fetch_after_command_and_empty_result_set() {
        Python::attach(|py| {
            let mut state = BufferedCursorState::new();
            state.store(None, Vec::new(), 3, None);
            assert_eq!(state.rowcount, 3);
            assert!(state.fetchone(py).unwrap().is_none());
            assert!(state.fetchall(py).unwrap().bind(py).is_empty());

            let desc = describe_columns(py, &[("id".to_string(), 3)]).unwrap();
            state.store(Some(desc), Vec::new(), 0, None);
            assert!(state.description.is_some());
            assert_eq!(state.rowcount, 0);
            let rows = state.fetchall(py).unwrap();
            assert!(rows.bind(py).is_empty());
        });
    }

    #[test]
    fn test_cursor_state_fetchmany_default_and_fetchall_remainder() {
        Python::attach(|py| {
            let desc = describe_columns(py, &[("v".to_string(), 3)]).unwrap();
            let rows: Vec<Py<PyTuple>> = (1..=5).map(|i| make_row(py, i)).collect();

            let mut state = BufferedCursorState::new();
            state.arraysize = 2;
            state.store(Some(desc.clone_ref(py)), rows, 5, None);

            assert_eq!(state.fetchmany(py, Some(0)).unwrap().bind(py).len(), 0);
            assert_eq!(state.fetchmany(py, None).unwrap().bind(py).len(), 2);
            assert_eq!(state.fetchmany(py, None).unwrap().bind(py).len(), 2);
            assert_eq!(state.fetchmany(py, None).unwrap().bind(py).len(), 1);
            assert_eq!(state.fetchmany(py, None).unwrap().bind(py).len(), 0);

            let rows2: Vec<Py<PyTuple>> = (1..=5).map(|i| make_row(py, i)).collect();
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
        });
    }

    #[test]
    fn test_cursor_state_closed_is_idempotent_and_rejects_fetch() {
        Python::attach(|py| {
            let mut state = BufferedCursorState::new();
            state.close();
            state.close();
            assert!(state.closed);
            let err = state.fetchone(py).unwrap_err();
            assert_eq!(err_type_name(py, &err), "InterfaceError");
        });
    }

    #[test]
    fn test_cursor_describe_columns_and_rows_to_tuples() {
        Python::attach(|py| {
            let cols = vec![("id".to_string(), 3u8), ("name".to_string(), 253u8)];
            let desc = describe_columns(py, &cols).unwrap();
            let desc_bound = desc.bind(py);
            assert_eq!(desc_bound.len(), 2);

            let first = desc_bound
                .get_item(0)
                .unwrap()
                .cast_into::<PyTuple>()
                .unwrap();
            assert_eq!(first.len(), 7);
            assert_eq!(
                first.get_item(0).unwrap().extract::<String>().unwrap(),
                "id"
            );
            assert_eq!(first.get_item(1).unwrap().extract::<u8>().unwrap(), 3);
            for idx in 2..7 {
                assert!(first.get_item(idx).unwrap().is_none());
            }

            let second = desc_bound
                .get_item(1)
                .unwrap()
                .cast_into::<PyTuple>()
                .unwrap();
            assert_eq!(second.len(), 7);
            assert_eq!(
                second.get_item(0).unwrap().extract::<String>().unwrap(),
                "name"
            );
            assert_eq!(second.get_item(1).unwrap().extract::<u8>().unwrap(), 253);
            for idx in 2..7 {
                assert!(second.get_item(idx).unwrap().is_none());
            }

            let rows = rows_to_tuples(
                py,
                vec![vec![
                    DecodedCell::Int(7),
                    DecodedCell::Text("alice".to_string()),
                ]],
            )
            .unwrap();
            assert_eq!(rows.len(), 1);
            let row0 = rows[0].bind(py);
            assert_eq!(row0.get_item(0).unwrap().extract::<i64>().unwrap(), 7);
            assert_eq!(
                row0.get_item(1).unwrap().extract::<String>().unwrap(),
                "alice"
            );
        });
    }

    #[test]
    fn test_cursor_store_result_set_lastrowid_zero_and_nonzero() {
        Python::attach(|py| {
            let mut state = BufferedCursorState::new();
            store_result_set(
                &mut state,
                py,
                DecodedResultSet {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    affected_rows: 2,
                    last_insert_id: Some(0),
                },
            )
            .unwrap();
            assert!(state.description.is_none());
            assert_eq!(state.rowcount, 2);
            assert!(state.lastrowid.is_none());

            store_result_set(
                &mut state,
                py,
                DecodedResultSet {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    affected_rows: 1,
                    last_insert_id: Some(99),
                },
            )
            .unwrap();
            assert_eq!(
                state
                    .lastrowid
                    .as_ref()
                    .unwrap()
                    .bind(py)
                    .extract::<u64>()
                    .unwrap(),
                99
            );
        });
    }

    #[test]
    fn test_cursor_named_plan_rewrite_literals_and_cache() {
        let sql = "SELECT * FROM t WHERE a = :a AND b = %(b)s AND c = :a";
        let plan1 = named_plan_for(sql).unwrap();
        assert_eq!(
            plan1.sql.as_ref(),
            "SELECT * FROM t WHERE a = ? AND b = ? AND c = ?"
        );
        let names: Vec<&str> = plan1.names.iter().map(AsRef::as_ref).collect();
        assert_eq!(names, vec!["a", "b", "a"]);

        let plan2 = named_plan_for(sql).unwrap();
        assert!(Arc::ptr_eq(&plan1, &plan2));

        let lit_sql = "SELECT ':x', `:y`, /* :z */ 1";
        let lit_plan = named_plan_for(lit_sql).unwrap();
        assert_eq!(lit_plan.sql.as_ref(), lit_sql);
        assert!(lit_plan.names.is_empty());
    }

    #[test]
    fn test_cursor_bind_parameters_positional_and_missing_name() {
        Python::attach(|py| {
            let sql = "SELECT * FROM t WHERE a = ?";
            let tup = PyTuple::new(py, [42i64]).unwrap();
            let (bound_sql, vals) = bind_parameters(py, sql, tup.as_any()).unwrap();
            assert!(matches!(bound_sql, Cow::Borrowed(_)));
            assert_eq!(bound_sql.as_ref(), sql);
            assert_eq!(vals.len(), 1);
            assert_eq!(vals[0].extract::<i64>().unwrap(), 42);

            let wrong_map = PyDict::new(py);
            wrong_map.set_item("b", 1i64).unwrap();
            let err = bind_parameters(py, "SELECT :a", wrong_map.as_any()).unwrap_err();
            assert_eq!(err_type_name(py, &err), "ProgrammingError");
            assert_eq!(
                err.value(py).to_string(),
                "Missing MySQL bind parameter 'a'"
            );
        });
    }

    #[test]
    fn test_cursor_bind_parameters_exact_empty_containers() {
        Python::attach(|py| {
            let empty_tuple = PyTuple::empty(py);
            let empty_list = PyList::empty(py);
            let empty_dict = PyDict::new(py);

            let sql = "SELECT 1";
            for container in [
                empty_tuple.as_any(),
                empty_list.as_any(),
                empty_dict.as_any(),
            ] {
                let (bound_sql, vals) = bind_parameters(py, sql, container).unwrap();
                assert!(matches!(bound_sql, Cow::Borrowed(_)));
                assert_eq!(bound_sql.as_ref(), sql);
                assert!(vals.is_empty());
            }

            let pct_sql = "SELECT '50%%'";
            let (bound_pct, pct_vals) = bind_parameters(py, pct_sql, empty_tuple.as_any()).unwrap();
            assert!(matches!(bound_pct, Cow::Owned(_)));
            assert_eq!(
                bound_pct.as_ref(),
                normalize_positional_sql(pct_sql).as_ref()
            );
            assert!(pct_vals.is_empty());

            let named_sql = "SELECT :a";
            let (bound_named, named_vals) =
                bind_parameters(py, named_sql, empty_dict.as_any()).unwrap();
            assert!(matches!(bound_named, Cow::Borrowed(_)));
            assert_eq!(bound_named.as_ref(), named_sql);
            assert!(named_vals.is_empty());
        });
    }

    #[test]
    fn test_cursor_bind_parameters_exact_and_subclass_containers() {
        Python::attach(|py| {
            let sql_pos = "SELECT ?, ?";
            let list = PyList::new(py, [1i64, 2i64]).unwrap();
            let (bound_list_sql, list_vals) = bind_parameters(py, sql_pos, list.as_any()).unwrap();
            assert!(matches!(bound_list_sql, Cow::Borrowed(_)));
            assert_eq!(bound_list_sql.as_ref(), sql_pos);
            let extracted_list: Vec<i64> = list_vals
                .iter()
                .map(|v| v.extract::<i64>().unwrap())
                .collect();
            assert_eq!(extracted_list, vec![1, 2]);

            let sql_named = "SELECT :a";
            let dict = PyDict::new(py);
            dict.set_item("a", 1i64).unwrap();
            let (bound_dict_sql, dict_vals) =
                bind_parameters(py, sql_named, dict.as_any()).unwrap();
            assert_eq!(bound_dict_sql.as_ref(), "SELECT ?");
            assert_eq!(dict_vals.len(), 1);
            assert_eq!(dict_vals[0].extract::<i64>().unwrap(), 1);

            let collections = py.import("collections").unwrap();
            let pair_cls = collections
                .getattr("namedtuple")
                .unwrap()
                .call1(("Pair", ("x", "y")))
                .unwrap();
            let pair = pair_cls.call1((1i64, 2i64)).unwrap();
            let (bound_nt_sql, nt_vals) = bind_parameters(py, sql_pos, &pair).unwrap();
            assert!(matches!(bound_nt_sql, Cow::Borrowed(_)));
            assert_eq!(bound_nt_sql.as_ref(), sql_pos);
            let extracted_nt: Vec<i64> = nt_vals
                .iter()
                .map(|v| v.extract::<i64>().unwrap())
                .collect();
            assert_eq!(extracted_nt, vec![1, 2]);

            let ordered_cls = collections.getattr("OrderedDict").unwrap();
            let ordered = ordered_cls.call1((vec![("a", 1i64)],)).unwrap();
            let (bound_od_sql, od_vals) = bind_parameters(py, sql_named, &ordered).unwrap();
            assert_eq!(bound_od_sql.as_ref(), "SELECT ?");
            assert_eq!(od_vals.len(), 1);
            assert_eq!(od_vals[0].extract::<i64>().unwrap(), 1);
        });
    }

    #[test]
    fn test_cursor_coerce_value_types() {
        Python::attach(|py| {
            let ba = PyByteArray::new(py, b"abc");
            let coerced_ba = coerce_value(py, ba.into_any()).unwrap();
            assert!(coerced_ba.is_exact_instance_of::<PyBytes>());
            assert_eq!(coerced_ba.extract::<Vec<u8>>().unwrap(), b"abc");

            let uuid_mod = py.import("uuid").unwrap();
            let uuid_val = uuid_mod
                .getattr("UUID")
                .unwrap()
                .call1(("12345678-1234-5678-1234-567812345678",))
                .unwrap();
            let coerced_uuid = coerce_value(py, uuid_val).unwrap();
            assert!(coerced_uuid.is_exact_instance_of::<PyString>());
            assert_eq!(
                coerced_uuid.extract::<String>().unwrap(),
                "12345678-1234-5678-1234-567812345678"
            );

            let dict = PyDict::new(py);
            dict.set_item("k", 1i64).unwrap();
            let coerced_dict = coerce_value(py, dict.into_any()).unwrap();
            assert!(coerced_dict.is_exact_instance_of::<PyString>());
            let json_text = coerced_dict.extract::<String>().unwrap();
            let parsed: serde_json::Value = serde_json::from_str(&json_text).unwrap();
            assert_eq!(parsed["k"], 1);
        });
    }

    #[test]
    fn test_cursor_state_error_classes_and_messages() {
        Python::attach(|py| {
            let iface = cursor_interface_err(py, "x");
            assert_eq!(err_type_name(py, &iface), "InterfaceError");
            assert_eq!(iface.value(py).to_string(), "x");

            let prog = cursor_programming_err(py, "x");
            assert_eq!(err_type_name(py, &prog), "ProgrammingError");
            assert_eq!(prog.value(py).to_string(), "x");
        });
    }
}
