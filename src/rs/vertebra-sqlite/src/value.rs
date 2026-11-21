//! SQLite parameter binding and result value conversions.

use std::cell::RefCell;
use std::collections::HashMap;

use pyo3::prelude::*;
use pyo3::types::{
    PyBool, PyByteArray, PyBytes, PyDict, PyFloat, PyInt, PyList, PyMapping, PySequence, PyString,
    PyTuple,
};
use rusqlite::ToSql;
use rusqlite::types::{ToSqlOutput, ValueRef};

use crate::errors::SqliteClientError;

/// Scalar SQLite data value exchanged between worker threads and PyO3.
#[derive(Clone, Debug, PartialEq)]
pub enum SqliteValue {
    /// Null SQL value.
    Null,
    /// 64-bit signed integer (`INTEGER`).
    Integer(i64),
    /// 64-bit IEEE floating point (`REAL`).
    Real(f64),
    /// UTF-8 encoded text string (`TEXT`).
    Text(String),
    /// Raw binary blob (`BLOB` or SQLite 3.45+ `JSONB`).
    Blob(Vec<u8>),
}

impl ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            Self::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            Self::Integer(i) => Ok(ToSqlOutput::from(*i)),
            Self::Real(r) => Ok(ToSqlOutput::from(*r)),
            Self::Text(s) => Ok(ToSqlOutput::from(s.as_str())),
            Self::Blob(b) => Ok(ToSqlOutput::from(b.as_slice())),
        }
    }
}

impl From<ValueRef<'_>> for SqliteValue {
    fn from(v: ValueRef<'_>) -> Self {
        match v {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(i) => Self::Integer(i),
            ValueRef::Real(r) => Self::Real(r),
            ValueRef::Text(s) => Self::Text(String::from_utf8_lossy(s).into_owned()),
            ValueRef::Blob(b) => Self::Blob(b.to_vec()),
        }
    }
}

impl SqliteValue {
    /// Convert this `SqliteValue` into a Python object.
    pub fn into_py(self, py: Python<'_>) -> Py<PyAny> {
        match self {
            Self::Null => py.None(),
            Self::Integer(i) => i.into_pyobject(py).unwrap().into_any().unbind(),
            Self::Real(r) => r.into_pyobject(py).unwrap().into_any().unbind(),
            Self::Text(s) => s.into_pyobject(py).unwrap().into_any().unbind(),
            Self::Blob(b) => PyBytes::new(py, &b).into_any().unbind(),
        }
    }

    /// Extract a `SqliteValue` from a Python object.
    pub fn from_py(obj: &Bound<'_, PyAny>) -> Result<Self, SqliteClientError> {
        if obj.is_none() {
            return Ok(Self::Null);
        }
        if let Ok(s) = obj.cast_exact::<PyString>() {
            return Ok(Self::Text(s.to_string()));
        }
        if let Ok(i) = obj.cast_exact::<PyInt>()
            && let Ok(val) = i.extract::<i64>()
        {
            return Ok(Self::Integer(val));
        }
        if let Ok(f) = obj.cast_exact::<PyFloat>() {
            return Ok(Self::Real(f.value()));
        }
        if let Ok(b) = obj.cast_exact::<PyBytes>() {
            return Ok(Self::Blob(b.as_bytes().to_vec()));
        }
        if let Ok(b) = obj.cast::<PyBool>() {
            return Ok(Self::Integer(if b.is_true() { 1 } else { 0 }));
        }
        if let Ok(i) = obj.extract::<i64>() {
            return Ok(Self::Integer(i));
        }
        if let Ok(f) = obj.cast::<PyFloat>() {
            return Ok(Self::Real(f.value()));
        }
        if let Ok(s) = obj.cast::<PyString>() {
            return Ok(Self::Text(s.to_string()));
        }
        if let Ok(b) = obj.cast::<PyBytes>() {
            return Ok(Self::Blob(b.as_bytes().to_vec()));
        }
        if let Ok(ba) = obj.cast::<PyByteArray>() {
            return Ok(Self::Blob(unsafe { ba.as_bytes() }.to_vec()));
        }
        // If object has `isoformat()`, call it (datetime.datetime, datetime.date, datetime.time)
        if let Ok(iso_fn) = obj.getattr("isoformat")
            && let Ok(iso_res) = iso_fn.call0()
            && let Ok(s) = iso_res.extract::<String>()
        {
            return Ok(Self::Text(s));
        }
        let type_name = obj
            .get_type()
            .name()
            .map_or_else(|_| "unknown".to_string(), |n| n.to_string());
        if type_name == "TypedParameter"
            && let Ok(inner) = obj.getattr("value")
        {
            return Self::from_py(&inner);
        }
        if type_name == "Decimal" || type_name == "UUID" {
            return Ok(Self::Text(obj.to_string()));
        }
        // If a dict or list is passed for JSON/JSONB binding, serialize it
        if obj.is_instance_of::<PyDict>() || obj.is_instance_of::<PyList>() {
            let json_mod = obj.py().import("json").map_err(|e| {
                SqliteClientError::InvalidInput(format!("failed to import json: {e}"))
            })?;
            let serialized: String = json_mod
                .call_method1("dumps", (obj,))
                .map_err(|e| {
                    SqliteClientError::InvalidInput(format!(
                        "failed to serialize JSON parameter: {e}"
                    ))
                })?
                .extract()
                .map_err(|e| {
                    SqliteClientError::InvalidInput(format!("failed to extract JSON string: {e}"))
                })?;
            return Ok(Self::Text(serialized));
        }

        let type_name = obj
            .get_type()
            .name()
            .map_or_else(|_| "unknown".to_string(), |n| n.to_string());
        Err(SqliteClientError::InvalidInput(format!(
            "unsupported Python parameter type `{type_name}` for SQLite binding"
        )))
    }
}

/// Parameter collection bound to a prepared SQLite statement.
#[derive(Clone, Debug, Default)]
pub enum BoundParams {
    /// No bound parameters.
    #[default]
    Empty,
    /// Positional parameters (`?` or `?NNN`).
    Positional(Vec<SqliteValue>),
    /// Named parameters (`:name`, `@name`, or `$name`).
    Named(HashMap<String, SqliteValue>),
}

impl BoundParams {
    /// Return whether this parameter collection is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Empty => true,
            Self::Positional(p) => p.is_empty(),
            Self::Named(m) => m.is_empty(),
        }
    }

    /// Extract `BoundParams` from an optional Python parameter collection.
    pub fn from_py(parameters: Option<&Bound<'_, PyAny>>) -> Result<Self, SqliteClientError> {
        let Some(param_obj) = parameters else {
            return Ok(Self::Empty);
        };
        if param_obj.is_none() {
            return Ok(Self::Empty);
        }

        if let Ok(tuple) = param_obj.cast_exact::<PyTuple>() {
            let len = tuple.len();
            if len == 0 {
                return Ok(Self::Empty);
            }
            let mut positional = Vec::with_capacity(len);
            for item in tuple.iter() {
                positional.push(SqliteValue::from_py(&item)?);
            }
            return Ok(Self::Positional(positional));
        }

        if let Ok(list) = param_obj.cast_exact::<PyList>() {
            let len = list.len();
            if len == 0 {
                return Ok(Self::Empty);
            }
            let mut positional = Vec::with_capacity(len);
            for item in list.iter() {
                positional.push(SqliteValue::from_py(&item)?);
            }
            return Ok(Self::Positional(positional));
        }

        if let Ok(dict) = param_obj.cast_exact::<PyDict>() {
            let len = dict.len();
            if len == 0 {
                return Ok(Self::Empty);
            }
            let named = RefCell::new(HashMap::with_capacity(len));
            let first_err = RefCell::new(None);
            if let Err(py_err) = dict.locked_for_each(|key_obj, val_obj| {
                let key: String = match key_obj.extract() {
                    Ok(key) => key,
                    Err(e) => {
                        *first_err.borrow_mut() = Some(SqliteClientError::InvalidInput(format!(
                            "parameter key must be str: {e}"
                        )));
                        return Err(e);
                    }
                };
                match SqliteValue::from_py(&val_obj) {
                    Ok(val) => {
                        named.borrow_mut().insert(key, val);
                        Ok(())
                    }
                    Err(err) => {
                        let msg = err.to_string();
                        *first_err.borrow_mut() = Some(err);
                        Err(pyo3::exceptions::PyValueError::new_err(msg))
                    }
                }
            }) {
                return Err(first_err.into_inner().unwrap_or_else(|| {
                    SqliteClientError::InvalidInput(format!("failed to iterate dict: {py_err}"))
                }));
            }
            return Ok(Self::Named(named.into_inner()));
        }

        // Check if Mapping / Dict
        if let Ok(mapping) = param_obj.clone().cast_into::<PyMapping>() {
            let keys = mapping.keys().map_err(|e| {
                SqliteClientError::InvalidInput(format!("failed to extract mapping keys: {e}"))
            })?;
            let mut named = HashMap::new();
            let len = keys.len();
            for i in 0..len {
                let key_obj = keys.get_item(i).map_err(|e| {
                    SqliteClientError::InvalidInput(format!("failed to get key {i}: {e}"))
                })?;
                let key: String = key_obj.extract().map_err(|e| {
                    SqliteClientError::InvalidInput(format!("parameter key must be str: {e}"))
                })?;
                let val_obj = mapping.get_item(&key).map_err(|e| {
                    SqliteClientError::InvalidInput(format!("failed to get value for {key}: {e}"))
                })?;
                let val = SqliteValue::from_py(&val_obj)?;
                named.insert(key, val);
            }
            return Ok(Self::Named(named));
        }

        // Check if Sequence
        if let Ok(seq) = param_obj.clone().cast_into::<PySequence>() {
            let len = seq.len().unwrap_or(0);
            let mut positional = Vec::with_capacity(len);
            for i in 0..len {
                let item = seq.get_item(i).map_err(|e| {
                    SqliteClientError::InvalidInput(format!("failed to get item {i}: {e}"))
                })?;
                positional.push(SqliteValue::from_py(&item)?);
            }
            return Ok(Self::Positional(positional));
        }

        Err(SqliteClientError::InvalidInput(
            "parameters must be a sequence or mapping".to_string(),
        ))
    }

    /// Bind this parameter collection to a prepared statement.
    pub fn bind_to_stmt(
        &self,
        stmt: &mut rusqlite::Statement<'_>,
    ) -> Result<(), SqliteClientError> {
        match self {
            Self::Empty => Ok(()),
            Self::Positional(vals) => {
                for (idx, val) in vals.iter().enumerate() {
                    let col_idx = idx + 1;
                    stmt.raw_bind_parameter(col_idx, val)
                        .map_err(SqliteClientError::from_rusqlite)?;
                }
                Ok(())
            }
            Self::Named(map) => {
                for (key, val) in map {
                    let find_idx =
                        |name: &str| -> Option<usize> { stmt.parameter_index(name).ok().flatten() };
                    let idx = find_idx(key)
                        .or_else(|| find_idx(&format!(":{key}")))
                        .or_else(|| find_idx(&format!("${key}")))
                        .or_else(|| find_idx(&format!("@{key}")));
                    match idx {
                        Some(i) => {
                            stmt.raw_bind_parameter(i, val)
                                .map_err(SqliteClientError::from_rusqlite)?;
                        }
                        None => {
                            return Err(SqliteClientError::Programming {
                                message: format!("unknown parameter name `{key}`"),
                                code: None,
                                sqlstate: Some("42000"),
                            });
                        }
                    }
                }
                Ok(())
            }
        }
    }
}

/// Query result set returned from a worker thread.
#[derive(Clone, Debug, Default)]
pub struct QueryResult {
    /// Ordered column names.
    pub columns: Vec<String>,
    /// Rows aligned with `columns`.
    pub rows: Vec<Vec<SqliteValue>>,
}

/// Outcome of one statement reported by the engine.
#[derive(Clone, Debug, Default)]
pub struct StatementOutcome {
    /// Ordered column names, or `None` when the statement produces no result set.
    pub columns: Option<Vec<String>>,
    /// Rows aligned with `columns`; empty when the statement produces no result set.
    pub rows: Vec<Vec<SqliteValue>>,
    /// Changed-row count, `0` when nothing changed, or `-1` for a read-only statement.
    pub rowcount: i64,
    /// Rowid of the row inserted by this statement, when it is an insert that changed rows.
    pub lastrowid: Option<i64>,
}

impl QueryResult {
    /// Convert this `QueryResult` into a Python `(columns, rows)` tuple.
    pub fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let col_list = pyo3::types::PyList::empty(py);
        for col in self.columns {
            col_list.append(col)?;
        }

        let row_list = pyo3::types::PyList::empty(py);
        for row in self.rows {
            let mut py_items = Vec::with_capacity(row.len());
            for val in row {
                py_items.push(val.into_py(py));
            }
            let tuple = pyo3::types::PyTuple::new(py, py_items)?;
            row_list.append(tuple)?;
        }

        let result_tuple = pyo3::types::PyTuple::new(py, [col_list.as_any(), row_list.as_any()])?;
        Ok(result_tuple.unbind().into_any())
    }

    /// Convert this `QueryResult` into a Python list of dicts.
    pub fn to_py_dicts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let list = pyo3::types::PyList::empty(py);
        for row in &self.rows {
            let dict = pyo3::types::PyDict::new(py);
            for (col, val) in self.columns.iter().zip(row.iter()) {
                dict.set_item(col, val.clone().into_py(py))?;
            }
            list.append(dict)?;
        }
        Ok(list.unbind().into_any())
    }
}

/// Pipeline result item representing either an affected row count or a query result set.
#[derive(Clone, Debug)]
pub enum PipelineResultItem {
    /// Row count modified by DDL or DML statement.
    RowCount(usize),
    /// Query result set returned by SELECT statement.
    Query(QueryResult),
}

impl PipelineResultItem {
    /// Convert this pipeline item into a Python object.
    pub fn into_py(self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self {
            Self::RowCount(n) => Ok(n.into_pyobject(py)?.into_any().unbind()),
            Self::Query(qr) => qr.to_py_dicts(py),
        }
    }
}
