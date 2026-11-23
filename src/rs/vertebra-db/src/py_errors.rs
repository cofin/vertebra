//! Shared Python database exception resolution and instantiation for `vertebra-*` PyO3 crates.
//!
//! Resolves the canonical `VertebraError` / `DatabaseError` / Google Cloud exception hierarchy from
//! `vertebra._vertebra` and `vertebra.typing` via [`PyOnceLock`], falling back to matching stand-in
//! subclasses during standalone Rust unit tests when `vertebra._vertebra` is unavailable.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};

/// Cached canonical Python exception classes shared across all Vertebra database extensions.
pub struct DbPyExceptionTypes {
    pub database: Py<PyType>,
    pub interface: Py<PyType>,
    pub data: Py<PyType>,
    pub not_found: Py<PyType>,
    pub operational: Py<PyType>,
    pub connection_failure: Py<PyType>,
    pub pool_exhausted: Py<PyType>,
    pub query_cancelled: Py<PyType>,
    pub serialization_failure: Py<PyType>,
    pub integrity: Py<PyType>,
    pub internal: Py<PyType>,
    pub programming: Py<PyType>,
    pub not_supported: Py<PyType>,
    pub improper_configuration: Py<PyType>,
    pub google_cloud_auth: Py<PyType>,
    pub bigquery: Py<PyType>,
    pub bigquery_row: Py<PyType>,
    pub spanner: Py<PyType>,
    pub spanner_aborted: Py<PyType>,
}

static DB_PY_EXCEPTIONS: PyOnceLock<DbPyExceptionTypes> = PyOnceLock::new();

fn new_stand_in_class<'py>(
    py: Python<'py>,
    name: &str,
    bases: &Bound<'py, PyTuple>,
) -> PyResult<Py<PyType>> {
    let dict = PyDict::new(py);
    dict.set_item("__module__", "vertebra._vertebra")?;
    let ty = py
        .get_type::<PyType>()
        .call1((name, bases, dict))?
        .cast_into::<PyType>()?;
    Ok(ty.unbind())
}

fn resolve_root_error<'py>(py: Python<'py>) -> Bound<'py, PyType> {
    for module_name in ["vertebra._typing", "vertebra.typing"] {
        if let Ok(ty) = py
            .import(module_name)
            .and_then(|m| m.getattr("VertebraError"))
            .and_then(|v| v.cast_into::<PyType>().map_err(Into::into))
        {
            return ty;
        }
    }
    py.get_type::<pyo3::exceptions::PyException>()
}

fn resolve_improper_configuration(
    py: Python<'_>,
    root: &Bound<'_, PyType>,
) -> PyResult<Py<PyType>> {
    for module_name in ["vertebra.typing", "vertebra._typing"] {
        if let Ok(ty) = py
            .import(module_name)
            .and_then(|m| m.getattr("ImproperConfigurationError"))
            .and_then(|v| v.cast_into::<PyType>().map_err(Into::into))
        {
            return Ok(ty.unbind());
        }
    }
    new_stand_in_class(py, "ImproperConfigurationError", &PyTuple::new(py, [root])?)
}

fn build_stand_in_exceptions(py: Python<'_>) -> PyResult<DbPyExceptionTypes> {
    let root = resolve_root_error(py);
    let root_base = PyTuple::new(py, [&root])?;
    let database = new_stand_in_class(py, "DatabaseError", &root_base)?;
    let db_base = PyTuple::new(py, [database.bind(py)])?;

    let interface = new_stand_in_class(py, "InterfaceError", &db_base)?;
    let data = new_stand_in_class(py, "DataError", &db_base)?;
    let data_base = PyTuple::new(py, [data.bind(py)])?;
    let not_found = new_stand_in_class(py, "NotFoundError", &data_base)?;

    let operational = new_stand_in_class(py, "OperationalError", &db_base)?;
    let op_base = PyTuple::new(py, [operational.bind(py)])?;
    let connection_failure = new_stand_in_class(py, "ConnectionFailure", &op_base)?;
    let pool_exhausted = new_stand_in_class(py, "PoolExhaustedError", &op_base)?;
    let query_cancelled = new_stand_in_class(py, "QueryCancelledError", &op_base)?;
    let serialization_failure = new_stand_in_class(py, "SerializationFailure", &op_base)?;

    let integrity = new_stand_in_class(py, "IntegrityError", &db_base)?;
    let internal = new_stand_in_class(py, "InternalError", &db_base)?;
    let programming = new_stand_in_class(py, "ProgrammingError", &db_base)?;
    let not_supported = new_stand_in_class(py, "NotSupportedError", &db_base)?;
    let improper_configuration = resolve_improper_configuration(py, &root)?;

    let gc = new_stand_in_class(py, "GoogleCloudError", &root_base)?;
    let google_cloud_auth = new_stand_in_class(
        py,
        "GoogleCloudAuthError",
        &PyTuple::new(py, [gc.bind(py), operational.bind(py)])?,
    )?;
    let bigquery = new_stand_in_class(
        py,
        "BigQueryError",
        &PyTuple::new(py, [gc.bind(py), database.bind(py)])?,
    )?;
    let bigquery_row = new_stand_in_class(
        py,
        "BigQueryRowError",
        &PyTuple::new(py, [bigquery.bind(py), data.bind(py)])?,
    )?;
    let spanner = new_stand_in_class(
        py,
        "SpannerError",
        &PyTuple::new(py, [gc.bind(py), database.bind(py)])?,
    )?;
    let spanner_aborted = new_stand_in_class(
        py,
        "SpannerAbortedError",
        &PyTuple::new(py, [spanner.bind(py), serialization_failure.bind(py)])?,
    )?;

    Ok(DbPyExceptionTypes {
        database,
        interface,
        data,
        not_found,
        operational,
        connection_failure,
        pool_exhausted,
        query_cancelled,
        serialization_failure,
        integrity,
        internal,
        programming,
        not_supported,
        improper_configuration,
        google_cloud_auth,
        bigquery,
        bigquery_row,
        spanner,
        spanner_aborted,
    })
}

fn load_from_core(py: Python<'_>) -> PyResult<DbPyExceptionTypes> {
    let core_mod = py.import("vertebra._vertebra")?;
    let get_ty = |name: &str| -> PyResult<Py<PyType>> {
        Ok(core_mod.getattr(name)?.cast_into::<PyType>()?.unbind())
    };
    let root = resolve_root_error(py);
    let database = get_ty("DatabaseError")?;
    let db_base = PyTuple::new(py, [database.bind(py)])?;
    let operational = get_ty("OperationalError")?;
    let op_base = PyTuple::new(py, [operational.bind(py)])?;
    let data = get_ty("DataError")?;
    let data_base = PyTuple::new(py, [data.bind(py)])?;
    let serialization_failure = get_ty("SerializationFailure")?;

    let get_or_standin = |name: &str, bases: &Bound<'_, PyTuple>| -> PyResult<Py<PyType>> {
        match core_mod.getattr(name) {
            Ok(attr) => Ok(attr.cast_into::<PyType>()?.unbind()),
            Err(_) => new_stand_in_class(py, name, bases),
        }
    };

    let root_base = PyTuple::new(py, [&root])?;
    let gc = get_or_standin("GoogleCloudError", &root_base)?;
    let bigquery = get_or_standin(
        "BigQueryError",
        &PyTuple::new(py, [gc.bind(py), database.bind(py)])?,
    )?;
    let spanner = get_or_standin(
        "SpannerError",
        &PyTuple::new(py, [gc.bind(py), database.bind(py)])?,
    )?;

    Ok(DbPyExceptionTypes {
        interface: get_or_standin("InterfaceError", &db_base)?,
        not_found: get_or_standin("NotFoundError", &data_base)?,
        connection_failure: get_or_standin("ConnectionFailure", &op_base)?,
        pool_exhausted: get_or_standin("PoolExhaustedError", &op_base)?,
        query_cancelled: get_or_standin("QueryCancelledError", &op_base)?,
        integrity: get_or_standin("IntegrityError", &db_base)?,
        internal: get_or_standin("InternalError", &db_base)?,
        programming: get_or_standin("ProgrammingError", &db_base)?,
        not_supported: get_or_standin("NotSupportedError", &db_base)?,
        improper_configuration: resolve_improper_configuration(py, &root)?,
        google_cloud_auth: get_or_standin(
            "GoogleCloudAuthError",
            &PyTuple::new(py, [gc.bind(py), operational.bind(py)])?,
        )?,
        bigquery_row: get_or_standin(
            "BigQueryRowError",
            &PyTuple::new(py, [bigquery.bind(py), data.bind(py)])?,
        )?,
        spanner_aborted: get_or_standin(
            "SpannerAbortedError",
            &PyTuple::new(py, [spanner.bind(py), serialization_failure.bind(py)])?,
        )?,
        database,
        data,
        operational,
        serialization_failure,
        bigquery,
        spanner,
    })
}

/// Resolve and cache the canonical Vertebra Python database exception classes.
pub fn resolve_db_exceptions(py: Python<'_>) -> PyResult<&'static DbPyExceptionTypes> {
    DB_PY_EXCEPTIONS.get_or_try_init(py, || match load_from_core(py) {
        Ok(types) => Ok(types),
        Err(_) => build_stand_in_exceptions(py),
    })
}

/// Instantiate a Python exception carrying `.sqlstate`, numeric `.vendor_code`, and `.is_transient`.
pub fn instantiate_db_exc(
    py: Python<'_>,
    ty: &Py<PyType>,
    message: &str,
    sqlstate: Option<&str>,
    vendor_code: Option<i64>,
    is_transient: bool,
) -> PyErr {
    match ty.bind(py).call1((message,)) {
        Ok(inst) => {
            if let Some(code) = sqlstate {
                let _ = inst.setattr("sqlstate", code);
            } else {
                let _ = inst.setattr("sqlstate", py.None());
            }
            if let Some(code) = vendor_code {
                let _ = inst.setattr("vendor_code", code);
            } else {
                let _ = inst.setattr("vendor_code", py.None());
            }
            let _ = inst.setattr("is_transient", is_transient);
            PyErr::from_value(inst)
        }
        Err(err) => err,
    }
}

/// Instantiate a Python exception carrying `.sqlstate`, string `.vendor_code`, and `.is_transient`.
pub fn instantiate_db_exc_str_vendor(
    py: Python<'_>,
    ty: &Py<PyType>,
    message: &str,
    sqlstate: Option<&str>,
    vendor_code: Option<&str>,
    is_transient: bool,
) -> PyErr {
    match ty.bind(py).call1((message,)) {
        Ok(inst) => {
            if let Some(code) = sqlstate {
                let _ = inst.setattr("sqlstate", code);
            } else {
                let _ = inst.setattr("sqlstate", py.None());
            }
            if let Some(code) = vendor_code {
                let _ = inst.setattr("vendor_code", code);
            } else {
                let _ = inst.setattr("vendor_code", py.None());
            }
            let _ = inst.setattr("is_transient", is_transient);
            PyErr::from_value(inst)
        }
        Err(err) => err,
    }
}

/// Instantiate a `InterfaceError` for PEP 249 cursor lifecycle violations.
pub fn cursor_interface_err(py: Python<'_>, message: &str) -> PyErr {
    match resolve_db_exceptions(py) {
        Ok(types) => instantiate_db_exc(py, &types.interface, message, None, None, false),
        Err(err) => err,
    }
}

/// Instantiate a `ProgrammingError` for PEP 249 cursor usage errors.
pub fn cursor_programming_err(py: Python<'_>, message: &str) -> PyErr {
    match resolve_db_exceptions(py) {
        Ok(types) => instantiate_db_exc(py, &types.programming, message, None, None, false),
        Err(err) => err,
    }
}

/// Instantiate a `NotSupportedError` for unsupported cursor operations.
pub fn cursor_not_supported_err(py: Python<'_>, message: &str) -> PyErr {
    match resolve_db_exceptions(py) {
        Ok(types) => instantiate_db_exc(py, &types.not_supported, message, None, None, false),
        Err(err) => err,
    }
}
