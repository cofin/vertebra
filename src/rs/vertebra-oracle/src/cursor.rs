//! Shared PEP 249 cursor state, description and tuple builders, state-error helpers, and
//! `SyncOracleCursor` / `AsyncOracleCursor` for `vertebra-oracle`.

use std::collections::{HashMap, HashSet};
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyNone, PyString, PyTuple, PyType,
};
use vertebra_db::py_async::{
    ReadyAwaitable, RustDbTaskAwaitable, ready_ok, spawn_rust_db_task_with_attr,
};
use vertebra_db::py_cursor::{BufferedCursorState, build_description_7tuple};
#[cfg(test)]
use vertebra_db::py_errors::cursor_programming_err;
pub(crate) use vertebra_db::py_errors::{cursor_interface_err, cursor_not_supported_err};
use vertebra_db::py_errors::{instantiate_db_exc, resolve_db_exceptions};

use crate::client::{
    BindArgs, ORACLE_COMPLETION_DRAINER_ATTR, OracleWorkerHandle, OutBindSpec, OutBindsPayload,
    StatementFacts, StatementKind, WorkerCommand, WorkerReply, extract_out_bind_spec,
    statement_facts,
};
use crate::errors::OracleError;
use crate::types::{
    OracleCellValue, OracleColumn, OracleValueOptions, coerce_bind_from_py, coerce_cell_to_py,
    normalize_named_bind_map,
};

/// Build a `NotFoundError` from the shared exception cache in `vertebra_db::py_errors`.
pub(crate) fn cursor_not_found_err(py: Python<'_>, message: &str) -> PyErr {
    match resolve_db_exceptions(py) {
        Ok(types) => instantiate_db_exc(py, &types.not_found, message, None, None, false),
        Err(py_err) => py_err,
    }
}

struct OracleOutBindVarState {
    type_code: Py<PyAny>,
    size: i64,
    arraysize: i64,
    outconverter: Option<Py<PyAny>>,
    values: Py<PyList>,
}

/// Mutable PEP 249 output bind variable returned by `cursor.var()`.
#[pyclass(
    name = "OracleOutBindVar",
    module = "vertebra._oracle",
    frozen,
    immutable_type
)]
pub struct OracleOutBindVar {
    state: Mutex<OracleOutBindVarState>,
}

#[pymethods]
impl OracleOutBindVar {
    #[new]
    #[pyo3(signature = (type_code, size = 0, arraysize = 1, outconverter = None))]
    pub fn new(
        py: Python<'_>,
        type_code: Py<PyAny>,
        size: i64,
        arraysize: i64,
        outconverter: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let effective_arraysize = arraysize.max(1);
        let count = usize::try_from(effective_arraysize).unwrap_or(1);
        let none = py.None();
        let items: Vec<Py<PyAny>> = (0..count).map(|_| none.clone_ref(py)).collect();
        let values = PyList::new(py, items)?.unbind();
        Ok(Self {
            state: Mutex::new(OracleOutBindVarState {
                type_code,
                size,
                arraysize: effective_arraysize,
                outconverter,
                values,
            }),
        })
    }

    #[getter]
    pub fn type_code(&self, py: Python<'_>) -> Py<PyAny> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .type_code
            .clone_ref(py)
    }

    #[setter]
    pub fn set_type_code(&self, value: Py<PyAny>) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .type_code = value;
    }

    #[getter]
    pub fn size(&self) -> i64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).size
    }

    #[setter]
    pub fn set_size(&self, value: i64) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).size = value;
    }

    #[getter]
    pub fn arraysize(&self) -> i64 {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .arraysize
    }

    #[setter]
    pub fn set_arraysize(&self, value: i64) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .arraysize = value;
    }

    #[getter]
    pub fn outconverter(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .outconverter
            .as_ref()
            .map(|cb| cb.clone_ref(py))
    }

    #[setter]
    pub fn set_outconverter(&self, value: Option<Py<PyAny>>) {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .outconverter = value;
    }

    #[getter]
    pub fn values(&self, py: Python<'_>) -> Py<PyList> {
        self.state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values
            .clone_ref(py)
    }

    #[setter]
    pub fn set_values(&self, value: Py<PyList>) {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).values = value;
    }

    #[pyo3(signature = (pos = 0))]
    pub fn getvalue(&self, py: Python<'_>, pos: isize) -> PyResult<Py<PyAny>> {
        let (values, outconverter) = {
            let guard = self.state.lock().unwrap_or_else(|e| e.into_inner());
            (
                guard.values.clone_ref(py),
                guard.outconverter.as_ref().map(|cb| cb.clone_ref(py)),
            )
        };
        let list = values.bind(py);
        let val = list.as_any().get_item(pos)?;
        if !val.is_none()
            && let Some(conv) = outconverter
        {
            return Ok(conv.bind(py).call1((val,))?.unbind());
        }
        Ok(val.unbind())
    }

    pub fn setvalue(&self, py: Python<'_>, pos: isize, value: Py<PyAny>) -> PyResult<()> {
        let values = self
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values
            .clone_ref(py);
        let list = values.bind(py);
        if pos >= 0 {
            let idx = pos as usize;
            let current_len = list.len();
            if idx >= current_len {
                let none = py.None();
                for _ in current_len..=idx {
                    list.append(none.clone_ref(py))?;
                }
            }
        }
        list.as_any().set_item(pos, value)?;
        Ok(())
    }
}

/// Buffered result and PEP 249 state shared by `SyncOracleCursor` and `AsyncOracleCursor`.
pub(crate) struct CursorState {
    pub(crate) buf: BufferedCursorState,
    pub(crate) touches_session_context: bool,
    pub(crate) out_binds: Option<Py<PyAny>>,
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
    pub(crate) fn new() -> Self {
        Self {
            buf: BufferedCursorState::new(),
            touches_session_context: false,
            out_binds: None,
        }
    }

    /// Clear buffered rows and reset statement metadata after a failed call or `close()`.
    pub(crate) fn clear_result(&mut self) {
        self.buf.clear_result();
        self.touches_session_context = false;
        self.out_binds = None;
    }

    /// Replace the buffered result set and update statement attributes.
    pub(crate) fn store(
        &mut self,
        description: Option<Py<PyTuple>>,
        rows: Vec<Py<PyTuple>>,
        rowcount: i64,
        lastrowid: Option<Py<PyAny>>,
    ) {
        self.buf.store(description, rows, rowcount, lastrowid);
        self.out_binds = None;
    }

    /// Close the cursor idempotently and release buffered rows.
    pub(crate) fn close(&mut self) {
        self.buf.close();
        self.touches_session_context = false;
        self.out_binds = None;
    }
}

/// Build the PEP 249 7-item column description tuple for an Oracle result set.
///
/// Each item has the shape `(name, type_name, None, None, None, None, None)`, where
/// `name` is lower-cased when `lowercase` is true and `type_name` is the canonical
/// Oracle type name as a Python `str`.
pub(crate) fn describe_columns(
    py: Python<'_>,
    columns: &[OracleColumn],
    lowercase: bool,
) -> PyResult<Py<PyTuple>> {
    let mut cols = Vec::with_capacity(columns.len());
    for col in columns {
        let col_name = if lowercase {
            col.name.to_ascii_lowercase()
        } else {
            col.name.clone()
        };
        let type_code = col
            .type_name
            .as_str()
            .into_pyobject(py)?
            .into_any()
            .unbind();
        cols.push((col_name, Some(type_code), None));
    }
    build_description_7tuple(py, cols)
}

/// Pack already-converted Python cells for a single row into a `Py<PyTuple>`.
pub(crate) fn cells_to_tuple(py: Python<'_>, cells: Vec<Py<PyAny>>) -> PyResult<Py<PyTuple>> {
    let bound_cells: Vec<Bound<'_, PyAny>> = cells.into_iter().map(|c| c.into_bound(py)).collect();
    Ok(PyTuple::new(py, bound_cells)?.unbind())
}

/// Pack already-converted Python rows into a `Vec<Py<PyTuple>>`.
pub(crate) fn rows_to_tuples(
    py: Python<'_>,
    rows: Vec<Vec<Py<PyAny>>>,
) -> PyResult<Vec<Py<PyTuple>>> {
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        out.push(cells_to_tuple(py, row)?);
    }
    Ok(out)
}

fn remap_bind_py_err(py: Python<'_>, err: PyErr) -> PyErr {
    if err.is_instance_of::<PyTypeError>(py) || err.is_instance_of::<PyValueError>(py) {
        let msg = err.value(py).to_string();
        return cursor_interface_err(py, &msg);
    }
    err
}

fn map_bind_oracle_err(py: Python<'_>, err: OracleError) -> PyErr {
    if let OracleError::Server {
        code: 1722,
        ref message,
    } = err
        && (message.contains("TypeError")
            || message.contains("ValueError")
            || message.contains("Unsupported Oracle bind type"))
    {
        return cursor_interface_err(py, message);
    }
    err.to_py_err(py)
}

fn coerce_single_bind(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
    options: OracleValueOptions,
) -> PyResult<OracleCellValue> {
    if !value.is_none()
        && !value.is_exact_instance_of::<PyBool>()
        && !value.is_exact_instance_of::<PyInt>()
        && !value.is_exact_instance_of::<PyFloat>()
        && !value.is_exact_instance_of::<PyString>()
        && !value.is_exact_instance_of::<PyBytes>()
        && value
            .get_type()
            .name()
            .is_ok_and(|name| name == "TypedParameter")
        && let Ok(inner) = value.getattr("value")
    {
        return coerce_single_bind(py, &inner, options);
    }
    coerce_bind_from_py(py, value, options).map_err(|e| map_bind_oracle_err(py, e))
}

pub(crate) enum OutBindVars {
    None,
    Named(Vec<(String, Py<PyAny>, String)>),
    Positional(Vec<(Py<PyAny>, String)>),
}

fn is_out_bind_var(value: &Bound<'_, PyAny>) -> bool {
    if value.is_none()
        || value.is_exact_instance_of::<PyBool>()
        || value.is_exact_instance_of::<PyInt>()
        || value.is_exact_instance_of::<PyFloat>()
        || value.is_exact_instance_of::<PyString>()
        || value.is_exact_instance_of::<PyBytes>()
    {
        return false;
    }
    let Ok(setvalue) = value.getattr("setvalue") else {
        return false;
    };
    setvalue.is_callable() && value.hasattr("type_code").unwrap_or(false)
}

fn out_bind_type_hint(var: &Bound<'_, PyAny>) -> String {
    let Ok(type_code) = var.getattr("type_code") else {
        return "VARCHAR2".to_owned();
    };
    let raw_name = if let Ok(name_attr) = type_code.getattr("_name")
        && !name_attr.is_none()
    {
        name_attr
            .str()
            .ok()
            .and_then(|s| s.to_str().ok().map(str::to_owned))
            .unwrap_or_default()
    } else if type_code.is_instance_of::<PyType>() {
        type_code
            .getattr("__name__")
            .ok()
            .and_then(|n| n.str().ok())
            .and_then(|s| s.to_str().ok().map(str::to_owned))
            .unwrap_or_default()
    } else if type_code.is_none() {
        String::new()
    } else {
        type_code
            .str()
            .ok()
            .and_then(|s| s.to_str().ok().map(str::to_owned))
            .unwrap_or_default()
    };
    match raw_name.to_ascii_uppercase().as_str() {
        "BLOB" => "BLOB".to_owned(),
        "CLOB" | "NCLOB" => "CLOB".to_owned(),
        "BINARY" | "BFILE" | "BYTES" | "RAW" => "RAW".to_owned(),
        "NUMBER" | "INT" | "FLOAT" | "DECIMAL" => "NUMBER".to_owned(),
        "DATETIME" | "DATE" | "TIMESTAMP" => "TIMESTAMP".to_owned(),
        "BOOL" | "BOOLEAN" => "BOOLEAN".to_owned(),
        _ => "VARCHAR2".to_owned(),
    }
}

fn parse_explicit_out_binds(
    py: Python<'_>,
    out_binds: Option<&Bound<'_, PyAny>>,
) -> PyResult<OutBindSpec> {
    let Some(obj) = out_binds else {
        return Ok(OutBindSpec::None);
    };
    if obj.is_none() {
        return Ok(OutBindSpec::None);
    }
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyByteArray>()
    {
        return Err(cursor_interface_err(
            py,
            "out_binds must be a Sequence or Mapping, not str/bytes",
        ));
    }
    extract_out_bind_spec(Some(obj)).map_err(|e| e.to_py_err(py))
}

fn has_out_binds(spec: &OutBindSpec) -> bool {
    match spec {
        OutBindSpec::None => false,
        OutBindSpec::Positional(v) => !v.is_empty(),
        OutBindSpec::Named(v) => !v.is_empty(),
    }
}

fn coerce_bind_args(
    py: Python<'_>,
    params: Option<&Bound<'_, PyAny>>,
    options: OracleValueOptions,
) -> PyResult<BindArgs> {
    let (bind_args, _, _) = prepare_bind_args_and_out_binds(py, params, None, options, true)?;
    Ok(bind_args)
}

fn collect_positional_binds<'py>(
    py: Python<'py>,
    iter: impl Iterator<Item = PyResult<Bound<'py, PyAny>>>,
    capacity_hint: usize,
    explicit_spec: OutBindSpec,
    options: OracleValueOptions,
    reject_out_bind_vars: bool,
) -> PyResult<(BindArgs, OutBindSpec, OutBindVars)> {
    let mut vals = Vec::with_capacity(capacity_hint);
    let (mut merged_hints, fallback_spec) = match explicit_spec {
        OutBindSpec::Positional(hints) => (hints, OutBindSpec::None),
        other => (Vec::new(), other),
    };
    let mut pos_vars: Vec<(Py<PyAny>, String)> = Vec::new();
    let mut out_var_idx = 0usize;
    for item_res in iter {
        let item = item_res.map_err(|e| remap_bind_py_err(py, e))?;
        if is_out_bind_var(&item) {
            if reject_out_bind_vars {
                return Err(cursor_not_supported_err(
                    py,
                    "out-bind variables are not supported in executemany",
                ));
            }
            let hint = if let Some(existing) = merged_hints.get(out_var_idx) {
                existing.clone()
            } else {
                let inferred = out_bind_type_hint(&item);
                merged_hints.push(inferred.clone());
                inferred
            };
            out_var_idx += 1;
            pos_vars.push((item.unbind(), hint));
        } else {
            vals.push(coerce_single_bind(py, &item, options)?);
        }
    }
    let out_spec = if !pos_vars.is_empty() || !merged_hints.is_empty() {
        OutBindSpec::Positional(merged_hints)
    } else {
        fallback_spec
    };
    let out_vars = if pos_vars.is_empty() {
        OutBindVars::None
    } else {
        OutBindVars::Positional(pos_vars)
    };
    Ok((BindArgs::Positional(vals), out_spec, out_vars))
}

fn collect_dict_binds(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    explicit_spec: OutBindSpec,
    options: OracleValueOptions,
    reject_out_bind_vars: bool,
) -> PyResult<(BindArgs, OutBindSpec, OutBindVars)> {
    let mut map = HashMap::with_capacity(dict.len());
    let (mut merged_hints, fallback_spec) = match explicit_spec {
        OutBindSpec::Named(pairs) => (pairs, OutBindSpec::None),
        other => (Vec::new(), other),
    };
    let mut named_vars: Vec<(String, Py<PyAny>, String)> = Vec::new();
    for (k, v) in dict.iter() {
        let key_str = k
            .extract::<String>()
            .map_err(|e| remap_bind_py_err(py, e))?;
        let clean_key = key_str.strip_prefix(':').unwrap_or(&key_str).to_owned();
        if is_out_bind_var(&v) {
            if reject_out_bind_vars {
                return Err(cursor_not_supported_err(
                    py,
                    "out-bind variables are not supported in executemany",
                ));
            }
            let hint = if let Some((_, existing)) = merged_hints
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case(&clean_key))
            {
                existing.clone()
            } else {
                let inferred = out_bind_type_hint(&v);
                merged_hints.push((clean_key.clone(), inferred.clone()));
                inferred
            };
            named_vars.push((clean_key, v.unbind(), hint));
        } else {
            let cell = coerce_single_bind(py, &v, options)?;
            map.insert(clean_key, cell);
        }
    }
    let out_spec = if !named_vars.is_empty() || !merged_hints.is_empty() {
        OutBindSpec::Named(merged_hints)
    } else {
        fallback_spec
    };
    let out_vars = if named_vars.is_empty() {
        OutBindVars::None
    } else {
        OutBindVars::Named(named_vars)
    };
    Ok((
        BindArgs::Named(normalize_named_bind_map(map)),
        out_spec,
        out_vars,
    ))
}

fn prepare_bind_args_and_out_binds(
    py: Python<'_>,
    params: Option<&Bound<'_, PyAny>>,
    explicit_out_binds: Option<&Bound<'_, PyAny>>,
    options: OracleValueOptions,
    reject_out_bind_vars: bool,
) -> PyResult<(BindArgs, OutBindSpec, OutBindVars)> {
    let explicit_spec = parse_explicit_out_binds(py, explicit_out_binds)?;
    let Some(obj) = params else {
        return Ok((
            BindArgs::Positional(Vec::new()),
            explicit_spec,
            OutBindVars::None,
        ));
    };
    if obj.is_none() {
        return Ok((
            BindArgs::Positional(Vec::new()),
            explicit_spec,
            OutBindVars::None,
        ));
    }
    if obj.is_exact_instance_of::<PyTuple>() {
        let tuple = obj.cast::<PyTuple>()?;
        if tuple.is_empty() {
            return Ok((
                BindArgs::Positional(Vec::new()),
                explicit_spec,
                OutBindVars::None,
            ));
        }
        return collect_positional_binds(
            obj.py(),
            tuple.iter().map(Ok),
            tuple.len(),
            explicit_spec,
            options,
            reject_out_bind_vars,
        );
    }
    if obj.is_exact_instance_of::<PyList>() {
        let list = obj.cast::<PyList>()?;
        if list.is_empty() {
            return Ok((
                BindArgs::Positional(Vec::new()),
                explicit_spec,
                OutBindVars::None,
            ));
        }
        return collect_positional_binds(
            obj.py(),
            list.iter().map(Ok),
            list.len(),
            explicit_spec,
            options,
            reject_out_bind_vars,
        );
    }
    if obj.is_exact_instance_of::<PyDict>() {
        let dict = obj.cast::<PyDict>()?;
        if dict.is_empty() {
            return Ok((
                BindArgs::Positional(Vec::new()),
                explicit_spec,
                OutBindVars::None,
            ));
        }
        return collect_dict_binds(py, dict, explicit_spec, options, reject_out_bind_vars);
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        return collect_dict_binds(py, dict, explicit_spec, options, reject_out_bind_vars);
    }
    if obj.hasattr("keys").unwrap_or(false)
        && obj.hasattr("items").unwrap_or(false)
        && let Ok(items) = obj.call_method0("items")
        && let Ok(iter) = items.try_iter()
    {
        let mut map = HashMap::new();
        let (mut merged_hints, fallback_spec) = match explicit_spec {
            OutBindSpec::Named(pairs) => (pairs, OutBindSpec::None),
            other => (Vec::new(), other),
        };
        let mut named_vars: Vec<(String, Py<PyAny>, String)> = Vec::new();
        for pair_res in iter {
            let pair = pair_res.map_err(|e| remap_bind_py_err(py, e))?;
            let key_obj = pair.get_item(0).map_err(|e| remap_bind_py_err(py, e))?;
            let val_obj = pair.get_item(1).map_err(|e| remap_bind_py_err(py, e))?;
            let key_str = key_obj
                .extract::<String>()
                .map_err(|e| remap_bind_py_err(py, e))?;
            let clean_key = key_str.strip_prefix(':').unwrap_or(&key_str).to_owned();
            if is_out_bind_var(&val_obj) {
                if reject_out_bind_vars {
                    return Err(cursor_not_supported_err(
                        py,
                        "out-bind variables are not supported in executemany",
                    ));
                }
                let hint = if let Some((_, existing)) = merged_hints
                    .iter()
                    .find(|(name, _)| name.eq_ignore_ascii_case(&clean_key))
                {
                    existing.clone()
                } else {
                    let inferred = out_bind_type_hint(&val_obj);
                    merged_hints.push((clean_key.clone(), inferred.clone()));
                    inferred
                };
                named_vars.push((clean_key, val_obj.unbind(), hint));
            } else {
                let cell = coerce_single_bind(py, &val_obj, options)?;
                map.insert(clean_key, cell);
            }
        }
        let out_spec = if !named_vars.is_empty() || !merged_hints.is_empty() {
            OutBindSpec::Named(merged_hints)
        } else {
            fallback_spec
        };
        let out_vars = if named_vars.is_empty() {
            OutBindVars::None
        } else {
            OutBindVars::Named(named_vars)
        };
        return Ok((
            BindArgs::Named(normalize_named_bind_map(map)),
            out_spec,
            out_vars,
        ));
    }
    if obj.is_instance_of::<PyString>()
        || obj.is_instance_of::<PyBytes>()
        || obj.is_instance_of::<PyByteArray>()
    {
        return Err(cursor_interface_err(
            py,
            "Oracle bind parameters must be a Sequence or Mapping, not str/bytes",
        ));
    }
    let iter = obj.try_iter().map_err(|_| {
        cursor_interface_err(py, "Oracle bind parameters must be a Sequence or Mapping")
    })?;
    collect_positional_binds(
        obj.py(),
        iter,
        0,
        explicit_spec,
        options,
        reject_out_bind_vars,
    )
}

fn coerce_bind_seq(
    py: Python<'_>,
    seq_of_params: &Bound<'_, PyAny>,
    options: OracleValueOptions,
) -> PyResult<Vec<BindArgs>> {
    if seq_of_params.is_instance_of::<PyString>()
        || seq_of_params.is_instance_of::<PyBytes>()
        || seq_of_params.is_instance_of::<PyByteArray>()
        || seq_of_params.is_instance_of::<PyDict>()
    {
        return Err(cursor_interface_err(
            py,
            "Oracle executemany parameters must be a Sequence of parameter sets",
        ));
    }
    let iter = seq_of_params.try_iter().map_err(|_| {
        cursor_interface_err(
            py,
            "Oracle executemany parameters must be a Sequence of parameter sets",
        )
    })?;
    let mut out = Vec::new();
    for item_res in iter {
        let item = item_res.map_err(|e| remap_bind_py_err(py, e))?;
        out.push(coerce_bind_args(py, Some(&item), options)?);
    }
    Ok(out)
}

type OutBindRowEntry = (String, String, Py<PyAny>);

fn describe_out_bind_columns(
    py: Python<'_>,
    entries: &[OutBindRowEntry],
    lowercase: bool,
) -> PyResult<Py<PyTuple>> {
    let none = PyNone::get(py);
    let mut cols = Vec::with_capacity(entries.len());
    for (name, type_name, _) in entries {
        let col_name = if lowercase {
            name.to_ascii_lowercase()
        } else {
            name.clone()
        };
        let item = PyTuple::new(
            py,
            [
                col_name.into_pyobject(py)?.into_any(),
                type_name.as_str().into_pyobject(py)?.into_any(),
                none.as_any().clone(),
                none.as_any().clone(),
                none.as_any().clone(),
                none.as_any().clone(),
                none.as_any().clone(),
            ],
        )?;
        cols.push(item.into_any());
    }
    Ok(PyTuple::new(py, cols)?.unbind())
}

fn process_out_binds(
    py: Python<'_>,
    payload: OutBindsPayload,
    out_spec: &OutBindSpec,
    out_vars: &OutBindVars,
    options: OracleValueOptions,
) -> PyResult<(Option<Py<PyAny>>, Vec<OutBindRowEntry>)> {
    match payload {
        OutBindsPayload::None => Ok((None, Vec::new())),
        OutBindsPayload::Named(pairs) => {
            let spec_hints: HashMap<String, &str> = match out_spec {
                OutBindSpec::Named(spec_pairs) => spec_pairs
                    .iter()
                    .map(|(k, v)| {
                        (
                            k.strip_prefix(':').unwrap_or(k).to_ascii_lowercase(),
                            v.as_str(),
                        )
                    })
                    .collect(),
                _ => HashMap::new(),
            };
            let mut reply_entries: Vec<(String, Option<OracleCellValue>)> = pairs
                .into_iter()
                .map(|(k, v)| (k.strip_prefix(':').unwrap_or(&k).to_owned(), Some(v)))
                .collect();
            let mut lower_index: HashMap<String, usize> =
                HashMap::with_capacity(reply_entries.len());
            for (idx, (name, _)) in reply_entries.iter().enumerate() {
                lower_index.entry(name.to_ascii_lowercase()).or_insert(idx);
            }

            let dict = PyDict::new(py);
            let mut row_entries: Vec<(String, String, Py<PyAny>)> = Vec::new();
            let mut handled_lower: HashSet<String> = HashSet::new();

            if let OutBindVars::Named(vars) = out_vars {
                for (clean_k, var_obj, hint) in vars {
                    let lower_k = clean_k.to_ascii_lowercase();
                    let cell = lower_index
                        .get(&lower_k)
                        .and_then(|&idx| reply_entries[idx].1.take())
                        .unwrap_or(OracleCellValue::Null);
                    let coerced = coerce_cell_to_py(py, cell, Some(hint.as_str()), options)?;
                    var_obj
                        .bind(py)
                        .call_method1("setvalue", (0_usize, coerced.bind(py)))?;
                    dict.set_item(clean_k, coerced.bind(py))?;
                    row_entries.push((clean_k.clone(), hint.clone(), coerced));
                    handled_lower.insert(lower_k);
                }
            }

            for (norm_k, maybe_cell) in reply_entries {
                let lower_k = norm_k.to_ascii_lowercase();
                if handled_lower.contains(&lower_k) {
                    continue;
                }
                let cell = maybe_cell.unwrap_or(OracleCellValue::Null);
                let hint = spec_hints.get(&lower_k).copied();
                let coerced = coerce_cell_to_py(py, cell, hint, options)?;
                dict.set_item(&norm_k, coerced.bind(py))?;
                row_entries.push((norm_k, hint.unwrap_or("VARCHAR2").to_owned(), coerced));
                handled_lower.insert(lower_k);
            }

            Ok((Some(dict.into_any().unbind()), row_entries))
        }
        OutBindsPayload::Positional(vals) => {
            let spec_hints: &[String] = match out_spec {
                OutBindSpec::Positional(hints) => hints.as_slice(),
                _ => &[],
            };
            let vars_slice: &[(Py<PyAny>, String)] = match out_vars {
                OutBindVars::Positional(vars) => vars.as_slice(),
                _ => &[],
            };
            let mut coerced_vals = Vec::with_capacity(vals.len());
            let mut row_entries = Vec::with_capacity(vals.len());
            for (idx, cell) in vals.into_iter().enumerate() {
                let hint = vars_slice
                    .get(idx)
                    .map(|(_, h)| h.as_str())
                    .or_else(|| spec_hints.get(idx).map(String::as_str));
                let coerced = coerce_cell_to_py(py, cell, hint, options)?;
                if let Some((var_obj, _)) = vars_slice.get(idx) {
                    var_obj
                        .bind(py)
                        .call_method1("setvalue", (0_usize, coerced.bind(py)))?;
                }
                row_entries.push((
                    format!("out_{}", idx + 1),
                    hint.unwrap_or("VARCHAR2").to_owned(),
                    coerced.clone_ref(py),
                ));
                coerced_vals.push(coerced);
            }
            let tup = cells_to_tuple(py, coerced_vals)?;
            Ok((Some(tup.into_any()), row_entries))
        }
    }
}

/// A statement prepared for worker dispatch together with its reply-coercion context.
pub(crate) struct PreparedExecute {
    pub(crate) cmd: WorkerCommand,
    pub(crate) facts: StatementFacts,
    pub(crate) out_spec: OutBindSpec,
    pub(crate) out_vars: OutBindVars,
    pub(crate) options: OracleValueOptions,
    pub(crate) lowercase: bool,
}

/// Prepare bind arguments, out-binds, statement facts, and the worker command for a statement.
pub(crate) fn prepare_execute(
    py: Python<'_>,
    sql: &str,
    params: Option<&Bound<'_, PyAny>>,
    out_binds: Option<&Bound<'_, PyAny>>,
    options: OracleValueOptions,
    lowercase: bool,
) -> PyResult<PreparedExecute> {
    let (bind_args, out_spec, out_vars) =
        prepare_bind_args_and_out_binds(py, params, out_binds, options, false)?;
    let facts = statement_facts(sql);
    let cmd = build_execute_command(
        sql.to_owned(),
        bind_args,
        out_spec.clone(),
        facts,
        options,
        lowercase,
    );
    Ok(PreparedExecute {
        cmd,
        facts,
        out_spec,
        out_vars,
        options,
        lowercase,
    })
}

/// Coerced Python values built from a single worker reply.
pub(crate) struct BuiltReply {
    pub(crate) description: Option<Py<PyTuple>>,
    pub(crate) rows: Vec<Py<PyTuple>>,
    pub(crate) rowcount: i64,
    pub(crate) lastrowid: Option<Py<PyAny>>,
    pub(crate) out_binds: Option<Py<PyAny>>,
    pub(crate) touches_session_context: bool,
}

/// Coerce a worker reply into Python objects and populate any out-bind variables in place.
#[allow(clippy::too_many_arguments)]
pub(crate) fn build_reply(
    py: Python<'_>,
    reply: WorkerReply,
    facts: StatementFacts,
    options: OracleValueOptions,
    lowercase: bool,
    out_spec: &OutBindSpec,
    out_vars: &OutBindVars,
) -> PyResult<BuiltReply> {
    match reply {
        WorkerReply::QueryOutcome(outcome) => {
            let (coerced_out_binds, out_row_entries) =
                process_out_binds(py, outcome.out_binds, out_spec, out_vars, options)?;
            let rowcount = i64::try_from(outcome.rowcount).unwrap_or(i64::MAX);
            let lastrowid = outcome
                .lastrowid
                .map(|s| PyString::new(py, &s).into_any().unbind());
            let (description, rows) = if !outcome.rows.is_empty() {
                let desc = describe_columns(py, &outcome.columns, lowercase)?;
                let col_types: Vec<&str> = outcome
                    .columns
                    .iter()
                    .map(|c| c.type_name.as_str())
                    .collect();
                let mut converted_rows = Vec::with_capacity(outcome.rows.len());
                for row in outcome.rows {
                    let mut cells = Vec::with_capacity(row.len());
                    for (idx, cell) in row.into_iter().enumerate() {
                        let col_type = col_types.get(idx).copied();
                        cells.push(coerce_cell_to_py(py, cell, col_type, options)?);
                    }
                    converted_rows.push(cells);
                }
                let py_rows = rows_to_tuples(py, converted_rows)?;
                (Some(desc), py_rows)
            } else if !out_row_entries.is_empty() {
                let desc = if !outcome.columns.is_empty() {
                    describe_columns(py, &outcome.columns, lowercase)?
                } else {
                    describe_out_bind_columns(py, &out_row_entries, lowercase)?
                };
                let single_row =
                    cells_to_tuple(py, out_row_entries.into_iter().map(|(_, _, v)| v).collect())?;
                (Some(desc), vec![single_row])
            } else if facts.kind == StatementKind::Query || !outcome.columns.is_empty() {
                let desc = describe_columns(py, &outcome.columns, lowercase)?;
                (Some(desc), Vec::new())
            } else {
                (None, Vec::new())
            };
            Ok(BuiltReply {
                description,
                rows,
                rowcount,
                lastrowid,
                out_binds: coerced_out_binds,
                touches_session_context: facts.touches_session_context,
            })
        }
        WorkerReply::RowCount(n) => {
            let rowcount = i64::try_from(n).unwrap_or(i64::MAX);
            Ok(BuiltReply {
                description: None,
                rows: Vec::new(),
                rowcount,
                lastrowid: None,
                out_binds: None,
                touches_session_context: facts.touches_session_context,
            })
        }
        _ => Ok(BuiltReply {
            description: None,
            rows: Vec::new(),
            rowcount: 0,
            lastrowid: None,
            out_binds: None,
            touches_session_context: facts.touches_session_context,
        }),
    }
}

fn column_names_from_description<'py>(
    py: Python<'py>,
    description: &Py<PyTuple>,
) -> PyResult<Vec<Bound<'py, PyAny>>> {
    let desc = description.bind(py);
    let mut names = Vec::with_capacity(desc.len());
    for entry in desc.iter() {
        names.push(entry.get_item(0)?);
    }
    Ok(names)
}

/// Shape a built reply into `(list_of_row_tuples, tuple_of_column_names)`.
pub(crate) fn reply_rows_and_names(py: Python<'_>, built: BuiltReply) -> PyResult<Py<PyAny>> {
    let Some(desc) = built.description else {
        return Ok((PyList::empty(py), PyTuple::empty(py))
            .into_pyobject(py)?
            .into_any()
            .unbind());
    };
    let names = PyTuple::new(py, column_names_from_description(py, &desc)?)?;
    let rows = PyList::new(py, built.rows)?;
    Ok((rows, names).into_pyobject(py)?.into_any().unbind())
}

/// Shape a built reply into a list of column-keyed row dictionaries (last column wins on duplicates).
pub(crate) fn reply_dicts(py: Python<'_>, built: BuiltReply) -> PyResult<Py<PyAny>> {
    let Some(desc) = built.description else {
        return Ok(PyList::empty(py).into_any().unbind());
    };
    if built.rows.is_empty() {
        return Ok(PyList::empty(py).into_any().unbind());
    }
    let names = column_names_from_description(py, &desc)?;
    let mut dicts = Vec::with_capacity(built.rows.len());
    for row in built.rows {
        let row_bound = row.bind(py);
        let dict = PyDict::new(py);
        for (col_name, cell) in names.iter().zip(row_bound.iter()) {
            dict.set_item(col_name, cell)?;
        }
        dicts.push(dict);
    }
    Ok(PyList::new(py, dicts)?.into_any().unbind())
}

/// Shape a built reply into the first cell of the first row, or raise `NotFoundError` when empty.
pub(crate) fn reply_first_value(py: Python<'_>, built: BuiltReply) -> PyResult<Py<PyAny>> {
    let Some(first_row) = built.rows.into_iter().next() else {
        return Err(cursor_not_found_err(py, "Query returned no rows"));
    };
    let bound_row = first_row.bind(py);
    if bound_row.is_empty() {
        return Ok(py.None());
    }
    Ok(bound_row.get_item(0)?.unbind())
}

/// Shape a built reply into `(rowcount, lastrowid, out_binds, touches_session_context)`.
pub(crate) fn reply_dml(py: Python<'_>, built: BuiltReply) -> PyResult<Py<PyAny>> {
    let rowcount = built.rowcount.max(0);
    let lastrowid = built.lastrowid.unwrap_or_else(|| py.None());
    let out_binds = built.out_binds.unwrap_or_else(|| py.None());
    Ok((
        rowcount,
        lastrowid,
        out_binds,
        built.touches_session_context,
    )
        .into_pyobject(py)?
        .into_any()
        .unbind())
}

/// Dispatch a prepared statement synchronously without creating a cursor, then shape the reply.
pub(crate) fn run_single_call_sync(
    py: Python<'_>,
    worker: &Arc<OracleWorkerHandle>,
    prepared: PreparedExecute,
    shape: fn(Python<'_>, BuiltReply) -> PyResult<Py<PyAny>>,
) -> PyResult<Py<PyAny>> {
    let PreparedExecute {
        cmd,
        facts,
        out_spec,
        out_vars,
        options,
        lowercase,
    } = prepared;
    let reply = py
        .detach(|| worker.dispatch_sync(cmd))
        .map_err(|e| e.to_py_err(py))?;
    let built = build_reply(py, reply, facts, options, lowercase, &out_spec, &out_vars)?;
    shape(py, built)
}

/// Dispatch a prepared statement asynchronously without creating a cursor, then shape the reply.
pub(crate) fn spawn_single_call(
    py: Python<'_>,
    worker: Arc<OracleWorkerHandle>,
    prepared: PreparedExecute,
    shape: fn(Python<'_>, BuiltReply) -> PyResult<Py<PyAny>>,
) -> PyResult<RustDbTaskAwaitable> {
    let PreparedExecute {
        cmd,
        facts,
        out_spec,
        out_vars,
        options,
        lowercase,
    } = prepared;
    spawn_rust_db_task_with_attr(
        py,
        async move { worker.dispatch_async_raw(cmd).await },
        move |py, reply| {
            let built = build_reply(py, reply, facts, options, lowercase, &out_spec, &out_vars)?;
            shape(py, built)
        },
        |py, err| err.to_py_err(py),
        ORACLE_COMPLETION_DRAINER_ATTR,
    )
}

#[allow(clippy::too_many_arguments)]
fn store_reply(
    py: Python<'_>,
    state: &mut CursorState,
    reply: WorkerReply,
    facts: StatementFacts,
    options: OracleValueOptions,
    lowercase: bool,
    out_spec: &OutBindSpec,
    out_vars: &OutBindVars,
) -> PyResult<()> {
    let built = build_reply(py, reply, facts, options, lowercase, out_spec, out_vars)?;
    state.store(
        built.description,
        built.rows,
        built.rowcount,
        built.lastrowid,
    );
    state.out_binds = built.out_binds;
    state.touches_session_context = built.touches_session_context;
    Ok(())
}

fn build_execute_command(
    sql: String,
    params: BindArgs,
    out_binds: OutBindSpec,
    facts: StatementFacts,
    options: OracleValueOptions,
    lowercase: bool,
) -> WorkerCommand {
    if has_out_binds(&out_binds) {
        return WorkerCommand::Execute {
            sql,
            params,
            out_binds,
            fetch_decimals: options.fetch_decimals,
            lowercase_column_names: lowercase,
        };
    }
    match facts.kind {
        StatementKind::Query => WorkerCommand::Query {
            sql,
            params,
            out_binds: OutBindSpec::None,
            fetch_decimals: options.fetch_decimals,
            lowercase_column_names: lowercase,
            max_rows: None,
        },
        StatementKind::DmlOrPlsql => WorkerCommand::Execute {
            sql,
            params,
            out_binds: OutBindSpec::None,
            fetch_decimals: options.fetch_decimals,
            lowercase_column_names: lowercase,
        },
    }
}

/// Synchronous PEP 249 cursor backed by [`OracleWorkerHandle`].
#[pyclass(module = "vertebra._oracle", frozen, immutable_type)]
pub struct SyncOracleCursor {
    worker: Arc<OracleWorkerHandle>,
    options: OracleValueOptions,
    lowercase: bool,
    lazy_begin: bool,
    state: Mutex<CursorState>,
    execute_lock: Mutex<()>,
}

impl SyncOracleCursor {
    pub(crate) fn new(
        worker: Arc<OracleWorkerHandle>,
        options: OracleValueOptions,
        lowercase: bool,
        lazy_begin: bool,
    ) -> Self {
        Self {
            worker,
            options,
            lowercase,
            lazy_begin,
            state: Mutex::new(CursorState::new()),
            execute_lock: Mutex::new(()),
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
impl SyncOracleCursor {
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
    pub fn touches_session_context(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .touches_session_context
    }

    #[getter]
    pub fn out_binds(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.out_binds.as_ref().map(|ob| ob.clone_ref(py))
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

    #[pyo3(
        signature = (sql, params = None, *, out_binds = None),
        text_signature = "($self, sql, params=(), *, out_binds=None)"
    )]
    pub fn execute<'py>(
        slf: &Bound<'py, Self>,
        sql: &str,
        params: Option<&Bound<'py, PyAny>>,
        out_binds: Option<&Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, Self>> {
        let py = slf.py();
        let this = slf.get();
        this.ensure_open(py)?;
        let prepared = prepare_execute(py, sql, params, out_binds, this.options, this.lowercase)
            .map_err(|err| this.clear_on_error(err))?;

        let worker = Arc::clone(&this.worker);
        let lazy_begin = this.lazy_begin;
        let execute_lock = &this.execute_lock;
        let cmd = prepared.cmd;
        let result = py.detach(|| {
            let _gate = execute_lock.lock().unwrap_or_else(PoisonError::into_inner);
            if lazy_begin && !worker.is_in_transaction() {
                worker.dispatch_sync(WorkerCommand::Begin)?;
            }
            worker.dispatch_sync(cmd)
        });

        let mut state = this.state.lock().unwrap_or_else(PoisonError::into_inner);
        match result {
            Ok(reply) => {
                if state.closed {
                    state.clear_result();
                } else if let Err(err) = store_reply(
                    py,
                    &mut state,
                    reply,
                    prepared.facts,
                    prepared.options,
                    prepared.lowercase,
                    &prepared.out_spec,
                    &prepared.out_vars,
                ) {
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
        let params_seq = coerce_bind_seq(py, seq_of_params, this.options)
            .map_err(|err| this.clear_on_error(err))?;
        let facts = statement_facts(sql);

        if params_seq.is_empty() {
            let mut state = this.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.ensure_open(py)?;
            state.store(None, Vec::new(), 0, None);
            state.touches_session_context = facts.touches_session_context;
            return Ok(slf.clone());
        }

        let worker = Arc::clone(&this.worker);
        let lazy_begin = this.lazy_begin;
        let execute_lock = &this.execute_lock;
        let sql_owned = sql.to_owned();
        let result = py.detach(|| {
            let _gate = execute_lock.lock().unwrap_or_else(PoisonError::into_inner);
            if lazy_begin && !worker.is_in_transaction() {
                worker.dispatch_sync(WorkerCommand::Begin)?;
            }
            worker.dispatch_sync(WorkerCommand::ExecuteMany {
                sql: sql_owned,
                params_seq,
            })
        });

        let mut state = this.state.lock().unwrap_or_else(PoisonError::into_inner);
        match result {
            Ok(reply) => {
                if state.closed {
                    state.clear_result();
                } else {
                    let rowcount = match reply {
                        WorkerReply::RowCount(n) => i64::try_from(n).unwrap_or(i64::MAX),
                        _ => 0,
                    };
                    state.store(None, Vec::new(), rowcount, None);
                    state.touches_session_context = facts.touches_session_context;
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

    #[pyo3(signature = (type_code, size = 0, arraysize = 1, outconverter = None))]
    pub fn var(
        &self,
        py: Python<'_>,
        type_code: Py<PyAny>,
        size: i64,
        arraysize: i64,
        outconverter: Option<Py<PyAny>>,
    ) -> PyResult<OracleOutBindVar> {
        self.ensure_open(py)?;
        OracleOutBindVar::new(py, type_code, size, arraysize, outconverter)
    }

    #[pyo3(signature = (*sizes))]
    pub fn setinputsizes(&self, sizes: &Bound<'_, PyTuple>) {
        let _ = sizes;
    }

    #[pyo3(signature = (size, column = None))]
    pub fn setoutputsize(&self, size: &Bound<'_, PyAny>, column: Option<&Bound<'_, PyAny>>) {
        let _ = (size, column);
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
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
    settled: bool,
}

impl ExecutingGuard {
    fn try_acquire(
        py: Python<'_>,
        state: &Arc<Mutex<CursorState>>,
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
        F: FnOnce(&mut CursorState) -> PyResult<()>,
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

/// Asynchronous PEP 249 cursor backed by [`OracleWorkerHandle`] and `vertebra_db::py_async`.
#[pyclass(module = "vertebra._oracle", frozen, immutable_type)]
pub struct AsyncOracleCursor {
    worker: Arc<OracleWorkerHandle>,
    options: OracleValueOptions,
    lowercase: bool,
    lazy_begin: bool,
    state: Arc<Mutex<CursorState>>,
    executing: Arc<AtomicBool>,
}

impl AsyncOracleCursor {
    pub(crate) fn new(
        worker: Arc<OracleWorkerHandle>,
        options: OracleValueOptions,
        lowercase: bool,
        lazy_begin: bool,
    ) -> Self {
        Self {
            worker,
            options,
            lowercase,
            lazy_begin,
            state: Arc::new(Mutex::new(CursorState::new())),
            executing: Arc::new(AtomicBool::new(false)),
        }
    }

    fn ensure_open(&self, py: Python<'_>) -> PyResult<()> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.ensure_open(py)
    }
}

#[pymethods]
impl AsyncOracleCursor {
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
    pub fn touches_session_context(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .touches_session_context
    }

    #[getter]
    pub fn out_binds(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        let state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
        state.out_binds.as_ref().map(|ob| ob.clone_ref(py))
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

    #[pyo3(
        signature = (sql, params = None, *, out_binds = None),
        text_signature = "($self, sql, params=(), *, out_binds=None)"
    )]
    pub fn execute(
        slf: &Bound<'_, Self>,
        sql: &str,
        params: Option<&Bound<'_, PyAny>>,
        out_binds: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        this.ensure_open(py)?;
        let guard = ExecutingGuard::try_acquire(py, &this.state, &this.executing)?;
        let PreparedExecute {
            cmd,
            facts,
            out_spec,
            out_vars,
            options,
            lowercase,
        } = prepare_execute(py, sql, params, out_binds, this.options, this.lowercase)?;

        let cursor_obj = slf.clone().into_any().unbind();
        let worker = Arc::clone(&this.worker);
        let lazy_begin = this.lazy_begin;

        spawn_rust_db_task_with_attr(
            py,
            async move {
                if lazy_begin
                    && !worker.is_in_transaction()
                    && let Err(err) = worker.dispatch_async_raw(WorkerCommand::Begin).await
                {
                    return Err((err, guard));
                }
                match worker.dispatch_async_raw(cmd).await {
                    Ok(reply) => Ok((reply, guard)),
                    Err(err) => Err((err, guard)),
                }
            },
            move |py, (reply, guard): (WorkerReply, ExecutingGuard)| {
                guard.settle_with(|state| {
                    if state.closed {
                        state.clear_result();
                        Ok(())
                    } else {
                        store_reply(
                            py, state, reply, facts, options, lowercase, &out_spec, &out_vars,
                        )
                    }
                })?;
                Ok(cursor_obj)
            },
            |py, (err, guard): (OracleError, ExecutingGuard)| {
                guard.clear_and_settle();
                err.to_py_err(py)
            },
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn executemany(
        slf: &Bound<'_, Self>,
        sql: &str,
        seq_of_params: &Bound<'_, PyAny>,
    ) -> PyResult<RustDbTaskAwaitable> {
        let py = slf.py();
        let this = slf.get();
        this.ensure_open(py)?;
        let guard = ExecutingGuard::try_acquire(py, &this.state, &this.executing)?;
        let params_seq = coerce_bind_seq(py, seq_of_params, this.options)?;
        let facts = statement_facts(sql);
        let cursor_obj = slf.clone().into_any().unbind();

        if params_seq.is_empty() {
            return spawn_rust_db_task_with_attr(
                py,
                async move { Ok::<(usize, ExecutingGuard), (OracleError, ExecutingGuard)>((0, guard)) },
                move |_py, (total, guard): (usize, ExecutingGuard)| {
                    guard.settle_with(|state| {
                        if state.closed {
                            state.clear_result();
                        } else {
                            let rowcount = i64::try_from(total).unwrap_or(i64::MAX);
                            state.store(None, Vec::new(), rowcount, None);
                            state.touches_session_context = facts.touches_session_context;
                        }
                        Ok(())
                    })?;
                    Ok(cursor_obj)
                },
                |py, (err, guard): (OracleError, ExecutingGuard)| {
                    guard.clear_and_settle();
                    err.to_py_err(py)
                },
                ORACLE_COMPLETION_DRAINER_ATTR,
            );
        }

        let worker = Arc::clone(&this.worker);
        let lazy_begin = this.lazy_begin;
        let sql_owned = sql.to_owned();
        spawn_rust_db_task_with_attr(
            py,
            async move {
                if lazy_begin
                    && !worker.is_in_transaction()
                    && let Err(err) = worker.dispatch_async_raw(WorkerCommand::Begin).await
                {
                    return Err((err, guard));
                }
                match worker
                    .dispatch_async_raw(WorkerCommand::ExecuteMany {
                        sql: sql_owned,
                        params_seq,
                    })
                    .await
                {
                    Ok(reply) => Ok((reply, guard)),
                    Err(err) => Err((err, guard)),
                }
            },
            move |_py, (reply, guard): (WorkerReply, ExecutingGuard)| {
                guard.settle_with(|state| {
                    if state.closed {
                        state.clear_result();
                    } else {
                        let rowcount = match reply {
                            WorkerReply::RowCount(n) => i64::try_from(n).unwrap_or(i64::MAX),
                            _ => 0,
                        };
                        state.store(None, Vec::new(), rowcount, None);
                        state.touches_session_context = facts.touches_session_context;
                    }
                    Ok(())
                })?;
                Ok(cursor_obj)
            },
            |py, (err, guard): (OracleError, ExecutingGuard)| {
                guard.clear_and_settle();
                err.to_py_err(py)
            },
            ORACLE_COMPLETION_DRAINER_ATTR,
        )
    }

    pub fn fetchone(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        let row = {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            state.fetchone(py)?
        };
        let value = match row {
            Some(tuple) => tuple.into_any(),
            None => py.None(),
        };
        ready_ok(py, value)
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

    #[pyo3(signature = (type_code, size = 0, arraysize = 1, outconverter = None))]
    pub fn var(
        &self,
        py: Python<'_>,
        type_code: Py<PyAny>,
        size: i64,
        arraysize: i64,
        outconverter: Option<Py<PyAny>>,
    ) -> PyResult<OracleOutBindVar> {
        self.ensure_open(py)?;
        OracleOutBindVar::new(py, type_code, size, arraysize, outconverter)
    }

    #[pyo3(signature = (*sizes))]
    pub fn setinputsizes(&self, sizes: &Bound<'_, PyTuple>) {
        let _ = sizes;
    }

    #[pyo3(signature = (size, column = None))]
    pub fn setoutputsize(&self, size: &Bound<'_, PyAny>, column: Option<&Bound<'_, PyAny>>) {
        let _ = (size, column);
    }

    pub fn close(&self, py: Python<'_>) -> PyResult<Py<ReadyAwaitable>> {
        self.state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .close();
        ready_ok(py, py.None())
    }
}

/// Register cursor classes on the `vertebra._oracle` Python module.
pub(crate) fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<SyncOracleCursor>()?;
    m.add_class::<AsyncOracleCursor>()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::PyTuple;

    use super::{
        CursorState, cells_to_tuple, cursor_interface_err, cursor_programming_err,
        describe_columns, rows_to_tuples,
    };
    use crate::errors::OracleError;
    use crate::types::OracleColumn;

    fn make_row(py: Python<'_>, value: i64) -> Py<PyTuple> {
        PyTuple::new(py, [value]).expect("row tuple").unbind()
    }

    fn make_desc(py: Python<'_>) -> Py<PyTuple> {
        let cols = vec![OracleColumn::new(
            "ID".to_owned(),
            "NUMBER".to_owned(),
            2,
            None,
            None,
            None,
            true,
        )];
        describe_columns(py, &cols, true).expect("describe_columns")
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

            assert!(state.fetchone(py).expect("fetchone").is_none());
            assert!(
                state
                    .fetchmany(py, None)
                    .expect("fetchmany")
                    .bind(py)
                    .is_empty()
            );
            assert!(state.fetchall(py).expect("fetchall").bind(py).is_empty());

            state.store(None, Vec::new(), 3, None);
            assert_eq!(state.rowcount, 3);
            assert!(state.description.is_none());
            assert!(state.lastrowid.is_none());
            assert!(state.executed);
            assert!(state.fetchone(py).expect("fetchone after DML").is_none());
            assert!(
                state
                    .fetchall(py)
                    .expect("fetchall after DML")
                    .bind(py)
                    .is_empty()
            );
        });
    }

    #[test]
    fn cursor_state_empty_result_set_and_fetch_pagination() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();
            let desc = make_desc(py);
            state.store(Some(desc.clone_ref(py)), Vec::new(), 0, None);
            assert!(state.description.is_some());
            assert!(
                state
                    .fetchall(py)
                    .expect("empty fetchall")
                    .bind(py)
                    .is_empty()
            );

            let rows: Vec<Py<PyTuple>> = (1..=5).map(|v| make_row(py, v)).collect();
            state.store(Some(desc.clone_ref(py)), rows, 5, None);
            state.set_arraysize(py, 2).expect("set_arraysize");
            assert_eq!(state.arraysize, 2);
            assert!(
                state
                    .fetchmany(py, Some(0))
                    .expect("fetchmany(0)")
                    .bind(py)
                    .is_empty()
            );

            let batch1 = state.fetchmany(py, None).expect("batch1");
            let batch2 = state.fetchmany(py, None).expect("batch2");
            let batch3 = state.fetchmany(py, None).expect("batch3");
            let batch4 = state.fetchmany(py, None).expect("batch4");
            assert_eq!(batch1.bind(py).len(), 2);
            assert_eq!(batch2.bind(py).len(), 2);
            assert_eq!(batch3.bind(py).len(), 1);
            assert!(batch4.bind(py).is_empty());

            let rows2: Vec<Py<PyTuple>> = (10..=14).map(|v| make_row(py, v)).collect();
            state.store(Some(desc), rows2, 5, None);
            let first = state
                .fetchone(py)
                .expect("fetchone")
                .expect("first row present");
            assert_eq!(
                first
                    .bind(py)
                    .get_item(0)
                    .expect("item 0")
                    .extract::<i64>()
                    .expect("extract i64"),
                10
            );
            let rest = state.fetchall(py).expect("fetchall remainder");
            assert_eq!(rest.bind(py).len(), 4);
            assert!(
                state
                    .fetchall(py)
                    .expect("exhausted fetchall")
                    .bind(py)
                    .is_empty()
            );
        });
    }

    #[test]
    fn cursor_state_close_clear_and_invalid_arraysize() {
        Python::initialize();
        Python::attach(|py| {
            let mut state = CursorState::new();
            let err = state
                .set_arraysize(py, 0)
                .expect_err("arraysize 0 must fail");
            assert_eq!(
                err.get_type(py)
                    .name()
                    .expect("type name")
                    .to_str()
                    .expect("str"),
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
            let closed_err = state.fetchone(py).expect_err("closed fetchone must fail");
            assert_eq!(
                closed_err
                    .get_type(py)
                    .name()
                    .expect("type name")
                    .to_str()
                    .expect("str"),
                "InterfaceError"
            );
        });
    }

    #[test]
    fn describe_columns_respects_lowercase_flag_and_pep249_shape() {
        Python::initialize();
        Python::attach(|py| {
            let cols = vec![
                OracleColumn::new(
                    "ID".to_owned(),
                    "NUMBER".to_owned(),
                    2,
                    None,
                    None,
                    None,
                    false,
                ),
                OracleColumn::new(
                    "LABEL".to_owned(),
                    "VARCHAR2".to_owned(),
                    1,
                    Some(64),
                    None,
                    None,
                    true,
                ),
            ];

            let lower = describe_columns(py, &cols, true).expect("describe lowercase");
            let lower_bound = lower.bind(py);
            assert_eq!(lower_bound.len(), 2);
            let c0 = lower_bound
                .get_item(0)
                .expect("c0")
                .cast_into::<PyTuple>()
                .expect("tuple");
            assert_eq!(c0.len(), 7);
            assert_eq!(
                c0.get_item(0)
                    .expect("name")
                    .extract::<String>()
                    .expect("str"),
                "id"
            );
            assert_eq!(
                c0.get_item(1)
                    .expect("type_name")
                    .extract::<String>()
                    .expect("str"),
                "NUMBER"
            );
            for idx in 2..7 {
                assert!(c0.get_item(idx).expect("slot").is_none());
            }

            let upper = describe_columns(py, &cols, false).expect("describe uppercase");
            let upper_bound = upper.bind(py);
            assert_eq!(upper_bound.len(), 2);
            let u0 = upper_bound
                .get_item(0)
                .expect("u0")
                .cast_into::<PyTuple>()
                .expect("tuple");
            assert_eq!(u0.len(), 7);
            assert_eq!(
                u0.get_item(0)
                    .expect("name")
                    .extract::<String>()
                    .expect("str"),
                "ID"
            );
            assert_eq!(
                u0.get_item(1)
                    .expect("type_name")
                    .extract::<String>()
                    .expect("str"),
                "NUMBER"
            );
            for idx in 2..7 {
                assert!(u0.get_item(idx).expect("slot").is_none());
            }
        });
    }

    #[test]
    fn tuple_builders_pack_converted_python_cells() {
        Python::initialize();
        Python::attach(|py| {
            let single = cells_to_tuple(
                py,
                vec![
                    42_i64.into_pyobject(py).expect("i64").into_any().unbind(),
                    "ok".into_pyobject(py).expect("str").into_any().unbind(),
                ],
            )
            .expect("cells_to_tuple");
            let sb = single.bind(py);
            assert_eq!(sb.len(), 2);
            assert_eq!(
                sb.get_item(0).expect("0").extract::<i64>().expect("i64"),
                42
            );
            assert_eq!(
                sb.get_item(1).expect("1").extract::<String>().expect("str"),
                "ok"
            );

            let rows = rows_to_tuples(
                py,
                vec![
                    vec![1_i64.into_pyobject(py).expect("i64").into_any().unbind()],
                    vec![2_i64.into_pyobject(py).expect("i64").into_any().unbind()],
                ],
            )
            .expect("rows_to_tuples");
            assert_eq!(rows.len(), 2);
            assert_eq!(
                rows[1]
                    .bind(py)
                    .get_item(0)
                    .expect("0")
                    .extract::<i64>()
                    .expect("i64"),
                2
            );
        });
    }

    #[test]
    fn cursor_state_error_helpers_and_ora_00001_attributes() {
        Python::initialize();
        Python::attach(|py| {
            let iface = cursor_interface_err(py, "x");
            assert_eq!(
                iface
                    .get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "InterfaceError"
            );
            assert_eq!(
                iface.value(py).str().expect("str").to_str().expect("str"),
                "x"
            );

            let prog = cursor_programming_err(py, "x");
            assert_eq!(
                prog.get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "ProgrammingError"
            );
            assert_eq!(
                prog.value(py).str().expect("str").to_str().expect("str"),
                "x"
            );

            let dup = OracleError::Server {
                code: 1,
                message: "ORA-00001: unique constraint violated".to_owned(),
            }
            .to_py_err(py);
            let dup_val = dup.value(py);
            assert_eq!(
                dup_val
                    .get_type()
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "IntegrityError"
            );
            assert_eq!(
                dup_val
                    .getattr("sqlstate")
                    .expect("sqlstate")
                    .extract::<Option<String>>()
                    .expect("extract")
                    .as_deref(),
                Some("23505")
            );
            assert_eq!(
                dup_val
                    .getattr("vendor_code")
                    .expect("vendor_code")
                    .extract::<Option<i32>>()
                    .expect("extract"),
                Some(1)
            );
        });
    }

    #[test]
    fn cursor_bind_coercion_named_colon_strip_typed_param_and_errors() {
        use pyo3::types::PyDict;

        use super::{coerce_bind_args, coerce_bind_seq};
        use crate::client::BindArgs;
        use crate::types::{OracleCellValue, OracleValueOptions};

        Python::initialize();
        Python::attach(|py| {
            let opts = OracleValueOptions::default();

            let named = PyDict::new(py);
            named.set_item("a", 1_i64).expect("set a");
            named.set_item(":b", 2_i64).expect("set :b");
            let parsed = coerce_bind_args(py, Some(named.as_any()), opts).expect("named binds");
            match parsed {
                BindArgs::Named(pairs) => {
                    assert_eq!(pairs.len(), 2);
                    assert_eq!(pairs[0].0, "a");
                    assert_eq!(pairs[1].0, "b");
                    assert!(matches!(pairs[0].1, OracleCellValue::Int(1)));
                    assert!(matches!(pairs[1].1, OracleCellValue::Int(2)));
                }
                BindArgs::Positional(_) => panic!("expected Named binds"),
            }

            let locals = PyDict::new(py);
            py.run(
                c"class TypedParameter:\n    def __init__(self, value):\n        self.value = value\ntp = TypedParameter(42)\n",
                None,
                Some(&locals),
            )
            .expect("define TypedParameter");
            let tp = locals.get_item("tp").expect("get").expect("tp");
            let seq = PyTuple::new(py, [tp]).expect("tuple");
            let unwrapped = coerce_bind_args(py, Some(seq.as_any()), opts).expect("unwrapped");
            match unwrapped {
                BindArgs::Positional(vals) => {
                    assert_eq!(vals.len(), 1);
                    assert!(matches!(vals[0], OracleCellValue::Int(42)));
                }
                BindArgs::Named(_) => panic!("expected Positional binds"),
            }

            let bad_str = pyo3::types::PyString::new(py, "not-a-seq");
            let str_err = coerce_bind_args(py, Some(bad_str.as_any()), opts)
                .expect_err("str params must fail");
            assert_eq!(
                str_err
                    .get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "InterfaceError"
            );

            let bad_key = PyDict::new(py);
            bad_key.set_item(123_i64, "v").expect("set int key");
            let key_err = coerce_bind_args(py, Some(bad_key.as_any()), opts)
                .expect_err("int dict key must fail");
            assert_eq!(
                key_err
                    .get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "InterfaceError"
            );

            let many = PyTuple::new(
                py,
                [
                    PyTuple::new(py, [1_i64]).expect("r1"),
                    PyTuple::new(py, [2_i64]).expect("r2"),
                    PyTuple::new(py, [3_i64]).expect("r3"),
                ],
            )
            .expect("many");
            let coerced_many = coerce_bind_seq(py, many.as_any(), opts).expect("executemany binds");
            assert_eq!(coerced_many.len(), 3);
        });
    }

    #[test]
    fn cursor_store_reply_matrix_select_decimal_uuid_ddl_dml_returning_plsql_and_session() {
        use super::{OutBindVars, build_execute_command, store_reply};
        use crate::client::{
            BindArgs, OutBindSpec, OutBindsPayload, WorkerCommand, WorkerQueryOutcome, WorkerReply,
            statement_facts,
        };
        use crate::types::{OracleCellValue, OracleValueOptions};

        Python::initialize();
        Python::attach(|py| {
            let opts = OracleValueOptions::default();
            let mut state = CursorState::new();

            let select_sql = "SELECT 1 AS a, 'x' AS b FROM dual";
            let facts = statement_facts(select_sql);
            assert!(matches!(
                build_execute_command(
                    select_sql.to_owned(),
                    BindArgs::Positional(Vec::new()),
                    OutBindSpec::None,
                    facts,
                    opts,
                    true,
                ),
                WorkerCommand::Query { .. }
            ));

            let outcome = WorkerQueryOutcome {
                columns: vec![
                    OracleColumn::new(
                        "A".to_owned(),
                        "NUMBER".to_owned(),
                        2,
                        None,
                        None,
                        None,
                        true,
                    ),
                    OracleColumn::new(
                        "B".to_owned(),
                        "VARCHAR2".to_owned(),
                        1,
                        Some(1),
                        None,
                        None,
                        true,
                    ),
                ],
                rows: vec![vec![
                    OracleCellValue::Int(1),
                    OracleCellValue::Str("x".to_owned()),
                ]],
                rowcount: 1,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals: false,
            };
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(outcome),
                facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("store select");
            assert!(!state.touches_session_context);
            assert!(state.out_binds.is_none());
            let desc = state.description.as_ref().expect("desc").bind(py);
            assert_eq!(
                desc.get_item(0)
                    .expect("c0")
                    .get_item(0)
                    .expect("name")
                    .extract::<String>()
                    .expect("str"),
                "a"
            );
            let rows = state.fetchall(py).expect("fetchall");
            let rows_list = rows.bind(py);
            assert_eq!(rows_list.len(), 1);
            assert_eq!(
                rows_list
                    .get_item(0)
                    .expect("row 0")
                    .get_item(0)
                    .expect("0")
                    .extract::<i64>()
                    .expect("int"),
                1
            );

            let dec_opts = OracleValueOptions {
                fetch_decimals: true,
                ..OracleValueOptions::default()
            };
            let dec_outcome = WorkerQueryOutcome {
                columns: vec![OracleColumn::new(
                    "V".to_owned(),
                    "NUMBER".to_owned(),
                    2,
                    None,
                    None,
                    None,
                    true,
                )],
                rows: vec![vec![OracleCellValue::Int(5)]],
                rowcount: 1,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals: true,
            };
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(dec_outcome),
                statement_facts("SELECT 5 FROM dual"),
                dec_opts,
                false,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("store decimal");
            let upper_desc = state.description.as_ref().expect("desc").bind(py);
            assert_eq!(
                upper_desc
                    .get_item(0)
                    .expect("c0")
                    .get_item(0)
                    .expect("name")
                    .extract::<String>()
                    .expect("str"),
                "V"
            );
            let dec_row = state.fetchone(py).expect("fetchone").expect("row");
            let dec_cell = dec_row.bind(py).get_item(0).expect("cell");
            assert_eq!(
                dec_cell
                    .get_type()
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "Decimal"
            );
            assert_eq!(dec_cell.str().expect("str").to_str().expect("str"), "5");

            let uuid_bytes: Vec<u8> = (0u8..16u8).collect();
            let uuid_outcome = WorkerQueryOutcome {
                columns: vec![OracleColumn::new(
                    "ID".to_owned(),
                    "RAW".to_owned(),
                    23,
                    Some(16),
                    None,
                    None,
                    false,
                )],
                rows: vec![vec![OracleCellValue::Bytes(uuid_bytes.clone())]],
                rowcount: 1,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals: false,
            };
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(uuid_outcome),
                statement_facts("SELECT id FROM t"),
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("store uuid");
            let uuid_row = state.fetchone(py).expect("fetchone").expect("row");
            let uuid_cell = uuid_row.bind(py).get_item(0).expect("cell");
            assert_eq!(
                uuid_cell
                    .get_type()
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "UUID"
            );
            assert_eq!(
                uuid_cell
                    .getattr("bytes")
                    .expect("bytes")
                    .extract::<Vec<u8>>()
                    .expect("vec"),
                uuid_bytes
            );

            let ddl_facts = statement_facts("CREATE TABLE t (id NUMBER)");
            assert!(matches!(
                build_execute_command(
                    "CREATE TABLE t (id NUMBER)".to_owned(),
                    BindArgs::Positional(Vec::new()),
                    OutBindSpec::None,
                    ddl_facts,
                    opts,
                    true,
                ),
                WorkerCommand::Execute { .. }
            ));
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(WorkerQueryOutcome {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rowcount: 0,
                    out_binds: OutBindsPayload::None,
                    lastrowid: None,
                    fetch_decimals: false,
                }),
                ddl_facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("store ddl");
            assert!(state.description.is_none());
            assert_eq!(state.rowcount, 0);

            let returning_facts =
                statement_facts("INSERT INTO t (id) VALUES (1) RETURNING id INTO :out_id");
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(WorkerQueryOutcome {
                    columns: vec![OracleColumn::new(
                        "ID".to_owned(),
                        "NUMBER".to_owned(),
                        2,
                        None,
                        None,
                        None,
                        false,
                    )],
                    rows: vec![vec![OracleCellValue::Int(1)]],
                    rowcount: 1,
                    out_binds: OutBindsPayload::None,
                    lastrowid: None,
                    fetch_decimals: false,
                }),
                returning_facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("store dml returning");
            assert!(state.description.is_some());
            assert_eq!(state.rowcount, 1);

            let sess_facts = statement_facts("BEGIN DBMS_SESSION.SET_IDENTIFIER('u1'); END;");
            store_reply(
                py,
                &mut state,
                WorkerReply::RowCount(0),
                sess_facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("store session block");
            assert!(state.description.is_none());
            assert!(state.touches_session_context);
        });
    }

    #[test]
    fn cursor_out_binds_matrix_named_positional_returning_hints_and_case_insensitive() {
        use pyo3::types::PyDict;

        use super::{
            OracleOutBindVar, build_execute_command, coerce_bind_seq,
            prepare_bind_args_and_out_binds, store_reply,
        };
        use crate::client::{
            BindArgs, OutBindSpec, OutBindsPayload, WorkerCommand, WorkerQueryOutcome, WorkerReply,
            statement_facts,
        };
        use crate::types::{OracleCellValue, OracleValueOptions};

        Python::initialize();
        Python::attach(|py| {
            let opts = OracleValueOptions::default();

            let var_num = Py::new(
                py,
                OracleOutBindVar::new(
                    py,
                    "NUMBER".into_pyobject(py).expect("str").into_any().unbind(),
                    0,
                    1,
                    None,
                )
                .expect("var"),
            )
            .expect("py var");
            let named_params = PyDict::new(py);
            named_params.set_item("a", 41_i64).expect("set a");
            named_params
                .set_item(":out", var_num.bind(py))
                .expect("set out");

            let (bind_args, out_spec, out_vars) =
                prepare_bind_args_and_out_binds(py, Some(named_params.as_any()), None, opts, false)
                    .expect("prepare named");
            match bind_args {
                BindArgs::Named(ref pairs) => {
                    assert_eq!(pairs.len(), 1);
                    assert_eq!(pairs[0].0, "a");
                }
                BindArgs::Positional(_) => panic!("expected Named"),
            }
            match &out_spec {
                OutBindSpec::Named(hints) => {
                    assert_eq!(hints, &vec![("out".to_owned(), "NUMBER".to_owned())]);
                }
                _ => panic!("expected Named OutBindSpec"),
            }

            let select_facts = statement_facts("SELECT 1 FROM dual");
            assert!(matches!(
                build_execute_command(
                    "SELECT 1 FROM dual".to_owned(),
                    bind_args.clone(),
                    out_spec.clone(),
                    select_facts,
                    opts,
                    true,
                ),
                WorkerCommand::Execute { .. }
            ));

            let mut state = CursorState::new();
            let plsql_facts = statement_facts("BEGIN :out := :a + 1; END;");
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(WorkerQueryOutcome {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rowcount: 0,
                    out_binds: OutBindsPayload::Named(vec![(
                        ":out".to_owned(),
                        OracleCellValue::Int(42),
                    )]),
                    lastrowid: None,
                    fetch_decimals: false,
                }),
                plsql_facts,
                opts,
                true,
                &out_spec,
                &out_vars,
            )
            .expect("store named out_bind");
            assert_eq!(
                var_num
                    .borrow(py)
                    .getvalue(py, 0)
                    .expect("getvalue")
                    .extract::<i64>(py)
                    .expect("i64"),
                42
            );
            let ob_dict = state
                .out_binds
                .as_ref()
                .expect("out_binds")
                .bind(py)
                .cast::<PyDict>()
                .expect("dict");
            assert_eq!(
                ob_dict
                    .get_item("out")
                    .expect("get")
                    .expect("val")
                    .extract::<i64>()
                    .expect("i64"),
                42
            );
            let synth_rows = state.fetchall(py).expect("fetchall");
            let synth_list = synth_rows.bind(py);
            assert_eq!(synth_list.len(), 1);
            assert_eq!(
                synth_list
                    .get_item(0)
                    .expect("row 0")
                    .get_item(0)
                    .expect("0")
                    .extract::<i64>()
                    .expect("i64"),
                42
            );
            let synth_desc = state.description.as_ref().expect("desc").bind(py);
            assert_eq!(
                synth_desc
                    .get_item(0)
                    .expect("c0")
                    .get_item(0)
                    .expect("name")
                    .extract::<String>()
                    .expect("str"),
                "out"
            );

            let var_upper = Py::new(
                py,
                OracleOutBindVar::new(
                    py,
                    "STRING".into_pyobject(py).expect("str").into_any().unbind(),
                    0,
                    1,
                    None,
                )
                .expect("var"),
            )
            .expect("py var");
            let case_params = PyDict::new(py);
            case_params
                .set_item("OUT", var_upper.bind(py))
                .expect("set OUT");
            let explicit_hints = PyDict::new(py);
            explicit_hints.set_item("out", "NUMBER").expect("set hint");
            let (_, case_spec, case_vars) = prepare_bind_args_and_out_binds(
                py,
                Some(case_params.as_any()),
                Some(explicit_hints.as_any()),
                opts,
                false,
            )
            .expect("prepare case");
            match &case_spec {
                OutBindSpec::Named(hints) => {
                    assert_eq!(hints, &vec![("out".to_owned(), "NUMBER".to_owned())]);
                }
                _ => panic!("expected Named OutBindSpec"),
            }
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(WorkerQueryOutcome {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rowcount: 1,
                    out_binds: OutBindsPayload::Named(vec![(
                        "out".to_owned(),
                        OracleCellValue::DecimalStr("99".to_owned()),
                    )]),
                    lastrowid: None,
                    fetch_decimals: false,
                }),
                statement_facts("INSERT INTO t (name) VALUES ('a') RETURNING id INTO :OUT"),
                opts,
                true,
                &case_spec,
                &case_vars,
            )
            .expect("store case-insensitive");
            assert_eq!(
                var_upper
                    .borrow(py)
                    .getvalue(py, 0)
                    .expect("getvalue")
                    .extract::<i64>(py)
                    .expect("i64"),
                99
            );

            let var_pos = Py::new(
                py,
                OracleOutBindVar::new(
                    py,
                    "NUMBER".into_pyobject(py).expect("str").into_any().unbind(),
                    0,
                    1,
                    None,
                )
                .expect("var"),
            )
            .expect("py var");
            let pos_params = PyTuple::new(
                py,
                [
                    10_i64.into_pyobject(py).expect("i64").into_any(),
                    var_pos.bind(py).clone().into_any(),
                ],
            )
            .expect("pos_params");
            let (pos_binds, pos_spec, pos_vars) =
                prepare_bind_args_and_out_binds(py, Some(pos_params.as_any()), None, opts, false)
                    .expect("prepare pos");
            match pos_binds {
                BindArgs::Positional(vals) => assert_eq!(vals.len(), 1),
                BindArgs::Named(_) => panic!("expected Positional"),
            }
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(WorkerQueryOutcome {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rowcount: 1,
                    out_binds: OutBindsPayload::Positional(vec![OracleCellValue::Int(77)]),
                    lastrowid: None,
                    fetch_decimals: false,
                }),
                statement_facts("INSERT INTO t (v) VALUES (:1) RETURNING id INTO :2"),
                opts,
                true,
                &pos_spec,
                &pos_vars,
            )
            .expect("store pos");
            assert_eq!(
                var_pos
                    .borrow(py)
                    .getvalue(py, 0)
                    .expect("getvalue")
                    .extract::<i64>(py)
                    .expect("i64"),
                77
            );
            let ob_tup = state
                .out_binds
                .as_ref()
                .expect("out_binds")
                .bind(py)
                .cast::<PyTuple>()
                .expect("tuple");
            assert_eq!(ob_tup.len(), 1);
            assert_eq!(
                ob_tup
                    .get_item(0)
                    .expect("0")
                    .extract::<i64>()
                    .expect("i64"),
                77
            );
            let pos_desc = state.description.as_ref().expect("desc").bind(py);
            assert_eq!(
                pos_desc
                    .get_item(0)
                    .expect("c0")
                    .get_item(0)
                    .expect("name")
                    .extract::<String>()
                    .expect("str"),
                "out_1"
            );

            let bad_many = PyTuple::new(py, [pos_params]).expect("bad_many");
            let err = coerce_bind_seq(py, bad_many.as_any(), opts)
                .expect_err("executemany must reject out-bind vars");
            assert_eq!(
                err.get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "NotSupportedError"
            );

            let locals = PyDict::new(py);
            py.run(
                c"class DuckVar:\n    def __init__(self, tc):\n        self.type_code = tc\n        self.seen = None\n    def setvalue(self, pos, val):\n        self.seen = (pos, val)\ndv = DuckVar('BLOB')\n",
                None,
                Some(&locals),
            )
            .expect("define DuckVar");
            let dv = locals.get_item("dv").expect("get").expect("dv");
            let duck_params = PyDict::new(py);
            duck_params.set_item("b", &dv).expect("set b");
            let (_, duck_spec, duck_vars) =
                prepare_bind_args_and_out_binds(py, Some(duck_params.as_any()), None, opts, false)
                    .expect("prepare duck");
            match &duck_spec {
                OutBindSpec::Named(hints) => {
                    assert_eq!(hints, &vec![("b".to_owned(), "BLOB".to_owned())]);
                }
                _ => panic!("expected Named OutBindSpec"),
            }
            store_reply(
                py,
                &mut state,
                WorkerReply::QueryOutcome(WorkerQueryOutcome {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rowcount: 0,
                    out_binds: OutBindsPayload::Named(vec![(
                        "b".to_owned(),
                        OracleCellValue::Bytes(vec![1, 2, 3]),
                    )]),
                    lastrowid: None,
                    fetch_decimals: false,
                }),
                statement_facts("BEGIN :b := HEXTORAW('010203'); END;"),
                opts,
                true,
                &duck_spec,
                &duck_vars,
            )
            .expect("store duck");
            let seen = dv.getattr("seen").expect("seen");
            assert_eq!(
                seen.get_item(0)
                    .expect("pos")
                    .extract::<usize>()
                    .expect("usize"),
                0
            );
            assert_eq!(
                seen.get_item(1)
                    .expect("val")
                    .extract::<Vec<u8>>()
                    .expect("bytes"),
                vec![1, 2, 3]
            );
        });
    }

    #[test]
    fn cursor_bind_fast_path_exact_empty_subclass_and_out_bind() {
        use pyo3::types::{PyDict, PyList, PyTuple};

        use super::{OracleOutBindVar, OutBindVars, prepare_bind_args_and_out_binds};
        use crate::client::{BindArgs, OutBindSpec};
        use crate::types::{OracleCellValue, OracleValueOptions};

        Python::initialize();
        Python::attach(|py| {
            let opts = OracleValueOptions::default();

            let exact_tuple = PyTuple::new(
                py,
                [
                    1_i64.into_pyobject(py).expect("i64").into_any(),
                    "a".into_pyobject(py).expect("str").into_any(),
                ],
            )
            .expect("tuple");
            let exact_list = PyList::new(
                py,
                [
                    1_i64.into_pyobject(py).expect("i64").into_any(),
                    "a".into_pyobject(py).expect("str").into_any(),
                ],
            )
            .expect("list");
            let (tup_binds, tup_spec, tup_vars) =
                prepare_bind_args_and_out_binds(py, Some(exact_tuple.as_any()), None, opts, false)
                    .expect("exact tuple");
            let (list_binds, list_spec, list_vars) =
                prepare_bind_args_and_out_binds(py, Some(exact_list.as_any()), None, opts, false)
                    .expect("exact list");
            assert!(matches!(tup_spec, OutBindSpec::None));
            assert!(matches!(list_spec, OutBindSpec::None));
            assert!(matches!(tup_vars, OutBindVars::None));
            assert!(matches!(list_vars, OutBindVars::None));
            match (&tup_binds, &list_binds) {
                (BindArgs::Positional(tv), BindArgs::Positional(lv)) => {
                    assert_eq!(tv.len(), 2);
                    assert_eq!(lv.len(), 2);
                    assert!(matches!(tv[0], OracleCellValue::Int(1)));
                    assert!(matches!(lv[0], OracleCellValue::Int(1)));
                    assert!(matches!(&tv[1], OracleCellValue::Str(s) if s == "a"));
                    assert!(matches!(&lv[1], OracleCellValue::Str(s) if s == "a"));
                }
                _ => panic!("expected Positional for both exact tuple and list"),
            }

            let exact_dict = PyDict::new(py);
            exact_dict.set_item("a", 1_i64).expect("set a");
            exact_dict.set_item(":b", 2_i64).expect("set :b");
            let (dict_binds, _, _) =
                prepare_bind_args_and_out_binds(py, Some(exact_dict.as_any()), None, opts, false)
                    .expect("exact dict");
            match dict_binds {
                BindArgs::Named(pairs) => {
                    let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
                    assert_eq!(keys, vec!["a", "b"]);
                    assert!(matches!(pairs[0].1, OracleCellValue::Int(1)));
                    assert!(matches!(pairs[1].1, OracleCellValue::Int(2)));
                }
                BindArgs::Positional(_) => panic!("expected Named for exact dict"),
            }

            let empty_tuple = PyTuple::empty(py);
            let empty_list = PyList::empty(py);
            let empty_dict = PyDict::new(py);
            for empty_param in [
                Some(empty_tuple.as_any()),
                Some(empty_list.as_any()),
                Some(empty_dict.as_any()),
                None,
            ] {
                let (binds, spec, vars) =
                    prepare_bind_args_and_out_binds(py, empty_param, None, opts, false)
                        .expect("empty container");
                assert!(matches!(binds, BindArgs::Positional(ref v) if v.is_empty()));
                assert!(matches!(spec, OutBindSpec::None));
                assert!(matches!(vars, OutBindVars::None));
            }

            let locals = PyDict::new(py);
            py.run(
                c"from collections import OrderedDict, namedtuple\nfrom types import MappingProxyType\nPair = namedtuple('Pair', ['x', 'y'])\nnt = Pair(1, 'a')\nod = OrderedDict([('a', 1), (':b', 2)])\nmp = MappingProxyType({'a': 1, ':b': 2})\n",
                None,
                Some(&locals),
            )
            .expect("build subclasses");
            let nt = locals.get_item("nt").expect("get").expect("nt");
            let (nt_binds, _, _) =
                prepare_bind_args_and_out_binds(py, Some(&nt), None, opts, false)
                    .expect("namedtuple");
            assert!(matches!(nt_binds, BindArgs::Positional(ref v) if v.len() == 2));

            let od = locals.get_item("od").expect("get").expect("od");
            let (od_binds, _, _) =
                prepare_bind_args_and_out_binds(py, Some(&od), None, opts, false)
                    .expect("OrderedDict");
            match od_binds {
                BindArgs::Named(pairs) => {
                    let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
                    assert_eq!(keys, vec!["a", "b"]);
                }
                BindArgs::Positional(_) => panic!("expected Named for OrderedDict"),
            }

            let mp = locals.get_item("mp").expect("get").expect("mp");
            let (mp_binds, _, _) =
                prepare_bind_args_and_out_binds(py, Some(&mp), None, opts, false)
                    .expect("MappingProxyType");
            match mp_binds {
                BindArgs::Named(pairs) => {
                    let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
                    assert_eq!(keys, vec!["a", "b"]);
                }
                BindArgs::Positional(_) => panic!("expected Named for MappingProxyType"),
            }

            let text = "abc".into_pyobject(py).expect("str");
            let err = prepare_bind_args_and_out_binds(py, Some(text.as_any()), None, opts, false)
                .err()
                .expect("str must fail");
            assert_eq!(
                err.get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "InterfaceError"
            );
            assert_eq!(
                err.value(py).to_string(),
                "Oracle bind parameters must be a Sequence or Mapping, not str/bytes"
            );

            let out_var = Py::new(
                py,
                OracleOutBindVar::new(
                    py,
                    "NUMBER".into_pyobject(py).expect("str").into_any().unbind(),
                    0,
                    1,
                    None,
                )
                .expect("var"),
            )
            .expect("py var");
            let out_tuple = PyTuple::new(py, [out_var.bind(py).clone().into_any()]).expect("tuple");
            let (out_binds, out_spec, out_vars) =
                prepare_bind_args_and_out_binds(py, Some(out_tuple.as_any()), None, opts, false)
                    .expect("out-bind tuple");
            assert!(matches!(out_binds, BindArgs::Positional(ref v) if v.is_empty()));
            assert!(matches!(out_spec, OutBindSpec::Positional(ref h) if h == &["NUMBER"]));
            assert!(matches!(out_vars, OutBindVars::Positional(ref v) if v.len() == 1));
        });
    }

    #[test]
    fn cursor_single_call_shapers_matrix_select_case_ddl_dml_out_binds_and_session() {
        use pyo3::types::{PyDict, PyList, PyTuple};

        use super::{
            BuiltReply, OracleOutBindVar, OutBindVars, build_reply, prepare_execute, reply_dicts,
            reply_dml, reply_first_value, reply_rows_and_names,
        };
        use crate::client::{
            OutBindSpec, OutBindsPayload, WorkerQueryOutcome, WorkerReply, statement_facts,
        };
        use crate::types::{OracleCellValue, OracleColumn, OracleValueOptions};

        Python::initialize();
        Python::attach(|py| {
            let opts = OracleValueOptions::default();

            let make_select_outcome = || WorkerQueryOutcome {
                columns: vec![
                    OracleColumn::new(
                        "A".to_owned(),
                        "NUMBER".to_owned(),
                        2,
                        None,
                        None,
                        None,
                        false,
                    ),
                    OracleColumn::new(
                        "B".to_owned(),
                        "VARCHAR2".to_owned(),
                        1,
                        Some(10),
                        None,
                        None,
                        false,
                    ),
                ],
                rows: vec![
                    vec![
                        OracleCellValue::Int(1),
                        OracleCellValue::Str("x".to_owned()),
                    ],
                    vec![
                        OracleCellValue::Int(2),
                        OracleCellValue::Str("y".to_owned()),
                    ],
                ],
                rowcount: 2,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals: false,
            };
            let select_facts = statement_facts("SELECT a, b FROM t");

            let built_rows = build_reply(
                py,
                WorkerReply::QueryOutcome(make_select_outcome()),
                select_facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("build select");
            let shaped_rows = reply_rows_and_names(py, built_rows).expect("rows_and_names");
            let pair = shaped_rows.bind(py).cast::<PyTuple>().expect("pair");
            let rows_list = pair
                .get_item(0)
                .expect("rows")
                .cast_into::<PyList>()
                .expect("list");
            let names_tup = pair
                .get_item(1)
                .expect("names")
                .cast_into::<PyTuple>()
                .expect("tuple");
            assert_eq!(rows_list.len(), 2);
            assert_eq!(
                rows_list
                    .get_item(0)
                    .expect("r0")
                    .extract::<(i64, String)>()
                    .expect("r0 tup"),
                (1, "x".to_owned())
            );
            assert_eq!(
                rows_list
                    .get_item(1)
                    .expect("r1")
                    .extract::<(i64, String)>()
                    .expect("r1 tup"),
                (2, "y".to_owned())
            );
            assert_eq!(
                names_tup.extract::<(String, String)>().expect("names"),
                ("a".to_owned(), "b".to_owned())
            );

            let built_dicts = build_reply(
                py,
                WorkerReply::QueryOutcome(make_select_outcome()),
                select_facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("build dicts");
            let shaped_dicts = reply_dicts(py, built_dicts).expect("reply_dicts");
            let dicts_list = shaped_dicts.bind(py).cast::<PyList>().expect("list");
            assert_eq!(dicts_list.len(), 2);
            let d0 = dicts_list
                .get_item(0)
                .expect("d0")
                .cast_into::<PyDict>()
                .expect("dict");
            assert_eq!(
                d0.get_item("a")
                    .expect("a")
                    .expect("v")
                    .extract::<i64>()
                    .expect("i64"),
                1
            );
            assert_eq!(
                d0.get_item("b")
                    .expect("b")
                    .expect("v")
                    .extract::<String>()
                    .expect("str"),
                "x"
            );
            let d1 = dicts_list
                .get_item(1)
                .expect("d1")
                .cast_into::<PyDict>()
                .expect("dict");
            assert_eq!(
                d1.get_item("a")
                    .expect("a")
                    .expect("v")
                    .extract::<i64>()
                    .expect("i64"),
                2
            );
            assert_eq!(
                d1.get_item("b")
                    .expect("b")
                    .expect("v")
                    .extract::<String>()
                    .expect("str"),
                "y"
            );

            let dup_outcome = WorkerQueryOutcome {
                columns: vec![
                    OracleColumn::new(
                        "A".to_owned(),
                        "NUMBER".to_owned(),
                        2,
                        None,
                        None,
                        None,
                        false,
                    ),
                    OracleColumn::new(
                        "A".to_owned(),
                        "NUMBER".to_owned(),
                        2,
                        None,
                        None,
                        None,
                        false,
                    ),
                ],
                rows: vec![vec![OracleCellValue::Int(1), OracleCellValue::Int(9)]],
                rowcount: 1,
                out_binds: OutBindsPayload::None,
                lastrowid: None,
                fetch_decimals: false,
            };
            let built_dup = build_reply(
                py,
                WorkerReply::QueryOutcome(dup_outcome),
                select_facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("build dup");
            let dup_dicts = reply_dicts(py, built_dup).expect("dup dicts");
            let dup_d0 = dup_dicts
                .bind(py)
                .cast::<PyList>()
                .expect("list")
                .get_item(0)
                .expect("d0")
                .cast_into::<PyDict>()
                .expect("dict");
            assert_eq!(
                dup_d0
                    .get_item("a")
                    .expect("a")
                    .expect("v")
                    .extract::<i64>()
                    .expect("i64"),
                9
            );

            let built_val = build_reply(
                py,
                WorkerReply::QueryOutcome(make_select_outcome()),
                select_facts,
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("build val");
            let shaped_val = reply_first_value(py, built_val).expect("reply_first_value");
            assert_eq!(shaped_val.extract::<i64>(py).expect("i64"), 1);

            let empty_cell_built = BuiltReply {
                description: None,
                rows: vec![PyTuple::empty(py).unbind()],
                rowcount: 1,
                lastrowid: None,
                out_binds: None,
                touches_session_context: false,
            };
            let none_val = reply_first_value(py, empty_cell_built).expect("empty cell row");
            assert!(none_val.is_none(py));

            let built_upper = build_reply(
                py,
                WorkerReply::QueryOutcome(make_select_outcome()),
                select_facts,
                opts,
                false,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("build upper");
            let upper_pair = reply_rows_and_names(py, built_upper).expect("upper");
            let upper_names = upper_pair
                .bind(py)
                .cast::<PyTuple>()
                .expect("pair")
                .get_item(1)
                .expect("names")
                .extract::<(String, String)>()
                .expect("names");
            assert_eq!(upper_names, ("A".to_owned(), "B".to_owned()));

            let ddl_facts = statement_facts("CREATE TABLE t (id NUMBER)");
            let make_ddl = || {
                build_reply(
                    py,
                    WorkerReply::QueryOutcome(WorkerQueryOutcome {
                        columns: Vec::new(),
                        rows: Vec::new(),
                        rowcount: 0,
                        out_binds: OutBindsPayload::None,
                        lastrowid: None,
                        fetch_decimals: false,
                    }),
                    ddl_facts,
                    opts,
                    true,
                    &OutBindSpec::None,
                    &OutBindVars::None,
                )
                .expect("build ddl")
            };
            let ddl_rows = reply_rows_and_names(py, make_ddl()).expect("ddl rows");
            let ddl_pair = ddl_rows.bind(py).cast::<PyTuple>().expect("pair");
            assert_eq!(ddl_pair.get_item(0).expect("0").len().expect("len"), 0);
            assert_eq!(ddl_pair.get_item(1).expect("1").len().expect("len"), 0);

            let ddl_dicts = reply_dicts(py, make_ddl()).expect("ddl dicts");
            assert_eq!(ddl_dicts.bind(py).len().expect("len"), 0);

            let ddl_err =
                reply_first_value(py, make_ddl()).expect_err("ddl must raise NotFoundError");
            assert_eq!(
                ddl_err
                    .get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "NotFoundError"
            );
            assert_eq!(ddl_err.value(py).to_string(), "Query returned no rows");

            let ddl_dml = reply_dml(py, make_ddl()).expect("ddl dml");
            let ddl_dml_tup = ddl_dml.bind(py).cast::<PyTuple>().expect("tuple");
            assert_eq!(
                ddl_dml_tup
                    .get_item(0)
                    .expect("0")
                    .extract::<i64>()
                    .expect("i64"),
                0
            );
            assert!(ddl_dml_tup.get_item(1).expect("1").is_none());
            assert!(ddl_dml_tup.get_item(2).expect("2").is_none());
            assert!(
                !ddl_dml_tup
                    .get_item(3)
                    .expect("3")
                    .extract::<bool>()
                    .expect("bool")
            );

            let dml_built = build_reply(
                py,
                WorkerReply::RowCount(2),
                statement_facts("UPDATE t SET v = 1"),
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("build dml");
            let dml_res = reply_dml(py, dml_built).expect("reply_dml");
            let dml_tup = dml_res.bind(py).cast::<PyTuple>().expect("tuple");
            assert_eq!(
                dml_tup
                    .get_item(0)
                    .expect("0")
                    .extract::<i64>()
                    .expect("i64"),
                2
            );
            assert!(dml_tup.get_item(1).expect("1").is_none());
            assert!(dml_tup.get_item(2).expect("2").is_none());
            assert!(
                !dml_tup
                    .get_item(3)
                    .expect("3")
                    .extract::<bool>()
                    .expect("bool")
            );

            let var_out = Py::new(
                py,
                OracleOutBindVar::new(
                    py,
                    "NUMBER".into_pyobject(py).expect("str").into_any().unbind(),
                    0,
                    1,
                    None,
                )
                .expect("var"),
            )
            .expect("py var");
            let out_params = PyDict::new(py);
            out_params
                .set_item("out", var_out.bind(py))
                .expect("set out");
            let prepared_out = prepare_execute(
                py,
                "BEGIN :out := 99; END;",
                Some(out_params.as_any()),
                None,
                opts,
                true,
            )
            .expect("prepare out");
            let out_built = build_reply(
                py,
                WorkerReply::QueryOutcome(WorkerQueryOutcome {
                    columns: Vec::new(),
                    rows: Vec::new(),
                    rowcount: 0,
                    out_binds: OutBindsPayload::Named(vec![(
                        "out".to_owned(),
                        OracleCellValue::Int(99),
                    )]),
                    lastrowid: None,
                    fetch_decimals: false,
                }),
                prepared_out.facts,
                prepared_out.options,
                prepared_out.lowercase,
                &prepared_out.out_spec,
                &prepared_out.out_vars,
            )
            .expect("build out");
            let out_dml = reply_dml(py, out_built).expect("out dml");
            let out_dml_tup = out_dml.bind(py).cast::<PyTuple>().expect("tuple");
            let out_dict = out_dml_tup
                .get_item(2)
                .expect("2")
                .cast_into::<PyDict>()
                .expect("dict");
            assert_eq!(
                out_dict
                    .get_item("out")
                    .expect("out")
                    .expect("v")
                    .extract::<i64>()
                    .expect("i64"),
                99
            );
            assert_eq!(
                var_out
                    .borrow(py)
                    .getvalue(py, 0)
                    .expect("getvalue")
                    .extract::<i64>(py)
                    .expect("i64"),
                99
            );

            let sess_built = build_reply(
                py,
                WorkerReply::RowCount(0),
                statement_facts("BEGIN DBMS_SESSION.SET_IDENTIFIER(:v); END;"),
                opts,
                true,
                &OutBindSpec::None,
                &OutBindVars::None,
            )
            .expect("build session");
            let sess_dml = reply_dml(py, sess_built).expect("session dml");
            let sess_tup = sess_dml.bind(py).cast::<PyTuple>().expect("tuple");
            assert!(
                sess_tup
                    .get_item(3)
                    .expect("3")
                    .extract::<bool>()
                    .expect("bool")
            );
        });
    }

    #[test]
    fn async_cursor_fetch_and_close_return_completed_ready_awaitable_and_raise_at_call() {
        use std::sync::{Arc, PoisonError};

        use pyo3::exceptions::PyStopIteration;

        use super::AsyncOracleCursor;
        use crate::client::OracleWorkerHandle;
        use crate::config::{NativeOracleConfig, RawOracleOptions};
        use crate::types::OracleValueOptions;

        fn drive_ready(py: Python<'_>, aw: &Bound<'_, PyAny>) -> Py<PyAny> {
            assert_eq!(
                aw.get_type()
                    .name()
                    .expect("type name")
                    .to_str()
                    .expect("utf8"),
                "ReadyAwaitable"
            );
            let iter = aw.call_method0("__await__").expect("__await__");
            let next_fn = py
                .import("builtins")
                .expect("builtins")
                .getattr("next")
                .expect("next");
            let stop_err = next_fn
                .call1((iter,))
                .expect_err("first next must raise StopIteration without yielding");
            assert!(stop_err.is_instance_of::<PyStopIteration>(py));
            stop_err
                .value(py)
                .getattr("value")
                .expect("StopIteration.value")
                .unbind()
        }

        Python::initialize();
        let cfg = NativeOracleConfig::validate(RawOracleOptions {
            host: "localhost".to_owned(),
            ..Default::default()
        })
        .expect("valid oracle config");
        let worker = Arc::new(OracleWorkerHandle::spawn(cfg).expect("zero-I/O worker spawn"));

        Python::attach(|py| {
            let cursor = AsyncOracleCursor::new(
                Arc::clone(&worker),
                OracleValueOptions::default(),
                true,
                false,
            );
            {
                let mut state = cursor.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.store(
                    Some(make_desc(py)),
                    vec![make_row(py, 10), make_row(py, 20), make_row(py, 30)],
                    -1,
                    None,
                );
            }

            let aw_all = cursor.fetchall(py).expect("fetchall");
            let all_val = drive_ready(py, aw_all.bind(py).as_any());
            assert_eq!(
                all_val.extract::<Vec<(i64,)>>(py).expect("rows"),
                vec![(10,), (20,), (30,)]
            );

            {
                let mut state = cursor.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.store(
                    Some(make_desc(py)),
                    vec![make_row(py, 1), make_row(py, 2), make_row(py, 3)],
                    -1,
                    None,
                );
            }

            let aw_one = cursor.fetchone(py).expect("fetchone");
            let one_val = drive_ready(py, aw_one.bind(py).as_any());
            assert_eq!(one_val.extract::<(i64,)>(py).expect("row"), (1,));

            let aw_many = cursor.fetchmany(py, Some(2)).expect("fetchmany");
            let many_val = drive_ready(py, aw_many.bind(py).as_any());
            assert_eq!(
                many_val.extract::<Vec<(i64,)>>(py).expect("rows"),
                vec![(2,), (3,)]
            );

            let aw_none = cursor.fetchone(py).expect("fetchone exhausted");
            let none_val = drive_ready(py, aw_none.bind(py).as_any());
            assert!(none_val.is_none(py));

            let bad_size_err = cursor
                .fetchmany(py, Some(-1))
                .expect_err("negative fetchmany size must fail at call");
            assert_eq!(
                bad_size_err
                    .get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "ProgrammingError"
            );

            let aw_close = cursor.close(py).expect("close");
            let close_val = drive_ready(py, aw_close.bind(py).as_any());
            assert!(close_val.is_none(py));

            let closed_err = cursor
                .fetchall(py)
                .expect_err("fetchall on closed cursor must fail at call");
            assert_eq!(
                closed_err
                    .get_type(py)
                    .name()
                    .expect("name")
                    .to_str()
                    .expect("str"),
                "InterfaceError"
            );
        });

        worker.close_sync().expect("close worker");
    }
}
