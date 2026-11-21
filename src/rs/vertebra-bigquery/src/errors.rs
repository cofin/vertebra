//! Python exception mapping for `vertebra._bigquery`.
//!
//! Resolves `GoogleCloudAuthError`, `BigQueryError`, and `BigQueryRowError` from
//! `vertebra._vertebra` via [`PyOnceLock`] so Python callers catch the exact shared exception
//! hierarchy, falling back to local stand-in subclasses during standalone Rust unit tests.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyList, PyTuple, PyType};
use vertebra_db::gcp::{GcpErrorInfo, code_name, code_number, gax_error_info, http_code};

use crate::client::{BigQueryError, BigQueryRowError};

/// Cached Python exception classes used by `vertebra._bigquery`.
pub struct BigQueryExceptionTypes {
    pub auth: Py<PyType>,
    pub bigquery: Py<PyType>,
    pub bigquery_row: Py<PyType>,
    pub interface: Py<PyType>,
    pub programming: Py<PyType>,
}

static BIGQUERY_EXCEPTION_TYPES: PyOnceLock<BigQueryExceptionTypes> = PyOnceLock::new();

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

fn stand_in_exception_types(py: Python<'_>) -> PyResult<BigQueryExceptionTypes> {
    let root = match py
        .import("vertebra._typing")
        .and_then(|m| m.getattr("VertebraError"))
        .and_then(|v| v.cast_into::<PyType>().map_err(Into::into))
    {
        Ok(ty) => ty,
        Err(_) => py.get_type::<pyo3::exceptions::PyException>(),
    };
    let db = new_stand_in_class(py, "DatabaseError", &PyTuple::new(py, [&root])?)?;
    let operational =
        new_stand_in_class(py, "OperationalError", &PyTuple::new(py, [db.bind(py)])?)?;
    let data = new_stand_in_class(py, "DataError", &PyTuple::new(py, [db.bind(py)])?)?;
    let gc = new_stand_in_class(py, "GoogleCloudError", &PyTuple::new(py, [&root])?)?;
    let auth = new_stand_in_class(
        py,
        "GoogleCloudAuthError",
        &PyTuple::new(py, [gc.bind(py), operational.bind(py)])?,
    )?;
    let bigquery = new_stand_in_class(
        py,
        "BigQueryError",
        &PyTuple::new(py, [gc.bind(py), db.bind(py)])?,
    )?;
    let bigquery_row = new_stand_in_class(
        py,
        "BigQueryRowError",
        &PyTuple::new(py, [bigquery.bind(py), data.bind(py)])?,
    )?;
    let interface = new_stand_in_class(py, "InterfaceError", &PyTuple::new(py, [&root])?)?;
    let programming =
        new_stand_in_class(py, "ProgrammingError", &PyTuple::new(py, [db.bind(py)])?)?;
    Ok(BigQueryExceptionTypes {
        auth,
        bigquery,
        bigquery_row,
        interface,
        programming,
    })
}

pub(crate) fn resolve_exception_types(py: Python<'_>) -> PyResult<&'static BigQueryExceptionTypes> {
    BIGQUERY_EXCEPTION_TYPES.get_or_try_init(py, || match py.import("vertebra._vertebra") {
        Ok(core_mod) => {
            let auth = core_mod
                .getattr("GoogleCloudAuthError")?
                .cast_into::<PyType>()?
                .unbind();
            let bigquery = core_mod
                .getattr("BigQueryError")?
                .cast_into::<PyType>()?
                .unbind();
            let bigquery_row = core_mod
                .getattr("BigQueryRowError")?
                .cast_into::<PyType>()?
                .unbind();
            let interface = core_mod
                .getattr("InterfaceError")?
                .cast_into::<PyType>()?
                .unbind();
            let programming = core_mod
                .getattr("ProgrammingError")?
                .cast_into::<PyType>()?
                .unbind();
            Ok(BigQueryExceptionTypes {
                auth,
                bigquery,
                bigquery_row,
                interface,
                programming,
            })
        }
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyModuleNotFoundError>(py) => {
            stand_in_exception_types(py)
        }
        Err(err) => Err(err),
    })
}

fn attach_gcp_attrs(py: Python<'_>, err: PyErr, info: GcpErrorInfo) -> PyErr {
    let is_transient = matches!(info.code, 4 | 8 | 10 | 14);
    let sqlstate = (info.code == 10).then_some("40001");
    let cname = code_name(info.code);
    let val = err.value(py);
    if let Err(attr_err) = val
        .setattr("code", info.code)
        .and_then(|()| val.setattr("code_name", cname))
        .and_then(|()| val.setattr("message", info.message))
        .and_then(|()| val.setattr("http_status", info.http_status))
        .and_then(|()| val.setattr("retry_delay_ms", info.retry_delay_ms))
        .and_then(|()| val.setattr("reason", info.reason))
        .and_then(|()| val.setattr("vendor_code", i64::from(info.code)))
        .and_then(|()| val.setattr("sqlstate", sqlstate))
        .and_then(|()| val.setattr("is_transient", is_transient))
    {
        return attr_err;
    }
    err
}

fn row_errors_to_py(
    py: Python<'_>,
    types: &BigQueryExceptionTypes,
    rows: Vec<BigQueryRowError>,
) -> PyErr {
    let summary = format!("BigQuery Storage Write rejected {} row(s)", rows.len());
    let list = PyList::empty(py);
    for item in &rows {
        let dict = PyDict::new(py);
        if let Err(e) = dict
            .set_item("index", item.index)
            .and_then(|()| dict.set_item("code", item.code))
            .and_then(|()| dict.set_item("message", &item.message))
            .and_then(|()| list.append(dict))
        {
            return e;
        }
    }
    let err = PyErr::from_type(types.bigquery_row.bind(py).clone(), (summary.clone(),));
    let err = attach_gcp_attrs(py, err, GcpErrorInfo::new(3, summary));
    if let Err(e) = err.value(py).setattr("row_errors", list) {
        return e;
    }
    err
}

fn bigquery_error_with(
    py: Python<'_>,
    types: &BigQueryExceptionTypes,
    err: BigQueryError,
) -> PyErr {
    match err {
        BigQueryError::Auth(message) => {
            let py_err = PyErr::from_type(types.auth.bind(py).clone(), (message.clone(),));
            attach_gcp_attrs(py, py_err, GcpErrorInfo::new(16, message))
        }
        BigQueryError::Api {
            status,
            message,
            reason,
            canonical_status,
        } => {
            let code = canonical_status
                .as_deref()
                .and_then(|raw| {
                    if raw.trim().eq_ignore_ascii_case("UNKNOWN") {
                        Some(2)
                    } else {
                        let parsed = code_number(raw);
                        (parsed != 2).then_some(parsed)
                    }
                })
                .unwrap_or_else(|| http_code(status));
            let ty = if code == 16 {
                &types.auth
            } else {
                &types.bigquery
            };
            let py_err = PyErr::from_type(ty.bind(py).clone(), (message.clone(),));
            attach_gcp_attrs(
                py,
                py_err,
                GcpErrorInfo {
                    code,
                    message,
                    http_status: Some(status),
                    retry_delay_ms: None,
                    reason,
                },
            )
        }
        BigQueryError::Client(gax_err) => {
            let info = gax_error_info(&gax_err);
            let ty = if info.code == 16 {
                &types.auth
            } else {
                &types.bigquery
            };
            let py_err = PyErr::from_type(ty.bind(py).clone(), (info.message.clone(),));
            attach_gcp_attrs(py, py_err, info)
        }
        BigQueryError::RowErrors(rows) => row_errors_to_py(py, types, rows),
        BigQueryError::Http(message) => {
            let py_err = PyErr::from_type(types.bigquery.bind(py).clone(), (message.clone(),));
            attach_gcp_attrs(py, py_err, GcpErrorInfo::new(14, message))
        }
        BigQueryError::Shm(message) | BigQueryError::AppendVerification(message) => {
            let py_err = PyErr::from_type(types.bigquery.bind(py).clone(), (message.clone(),));
            attach_gcp_attrs(py, py_err, GcpErrorInfo::new(13, message))
        }
        BigQueryError::InvalidArgument(message) => {
            let py_err = PyErr::from_type(types.bigquery.bind(py).clone(), (message.clone(),));
            attach_gcp_attrs(py, py_err, GcpErrorInfo::new(3, message))
        }
    }
}

/// Convert a native [`BigQueryError`] into a typed Python `BigQueryError` / `GoogleCloudAuthError`.
pub fn bigquery_error_to_py(py: Python<'_>, err: BigQueryError) -> PyErr {
    match resolve_exception_types(py) {
        Ok(types) => bigquery_error_with(py, types, err),
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};

    use crate::client::{BigQueryError, BigQueryRowError};

    use super::{bigquery_error_with, stand_in_exception_types};

    #[test]
    fn rest_api_error_prefers_canonical_status() {
        Python::initialize();
        Python::attach(|py| {
            let types = stand_in_exception_types(py).expect("types");
            let err = bigquery_error_with(
                py,
                &types,
                BigQueryError::Api {
                    status: 403,
                    message: "Access Denied".into(),
                    reason: Some("accessDenied".into()),
                    canonical_status: Some("PERMISSION_DENIED".into()),
                },
            );
            assert!(err.is_instance(py, types.bigquery.bind(py)));
            let val = err.value(py);
            assert_eq!(val.getattr("code").unwrap().extract::<i32>().unwrap(), 7);
            assert_eq!(
                val.getattr("code_name")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "PERMISSION_DENIED"
            );
            assert_eq!(
                val.getattr("http_status")
                    .unwrap()
                    .extract::<Option<u16>>()
                    .unwrap(),
                Some(403)
            );
            assert_eq!(
                val.getattr("reason")
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("accessDenied")
            );
        });
    }

    #[test]
    fn rest_api_error_maps_http_status() {
        Python::initialize();
        Python::attach(|py| {
            let types = stand_in_exception_types(py).expect("types");
            let rate_err = bigquery_error_with(
                py,
                &types,
                BigQueryError::Api {
                    status: 429,
                    message: "Quota exceeded".into(),
                    reason: Some("rateLimitExceeded".into()),
                    canonical_status: None,
                },
            );
            let val = rate_err.value(py);
            assert_eq!(val.getattr("code").unwrap().extract::<i32>().unwrap(), 8);
            assert_eq!(
                val.getattr("code_name")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "RESOURCE_EXHAUSTED"
            );
            assert!(
                val.getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );

            let auth_err = bigquery_error_with(
                py,
                &types,
                BigQueryError::Api {
                    status: 401,
                    message: "Invalid Credentials".into(),
                    reason: Some("authError".into()),
                    canonical_status: None,
                },
            );
            assert!(auth_err.is_instance(py, types.auth.bind(py)));
            assert_eq!(
                auth_err
                    .value(py)
                    .getattr("code")
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                16
            );
        });
    }

    #[test]
    fn row_errors_map_to_bigquery_row_error() {
        Python::initialize();
        Python::attach(|py| {
            let types = stand_in_exception_types(py).expect("types");
            let err = bigquery_error_with(
                py,
                &types,
                BigQueryError::RowErrors(vec![BigQueryRowError {
                    index: 2,
                    code: 3,
                    message: "invalid row".into(),
                }]),
            );
            assert!(err.is_instance(py, types.bigquery_row.bind(py)));
            let val = err.value(py);
            assert_eq!(val.getattr("code").unwrap().extract::<i32>().unwrap(), 3);
            let rows = val
                .getattr("row_errors")
                .unwrap()
                .cast_into::<PyList>()
                .unwrap();
            assert_eq!(rows.len(), 1);
            let first = rows.get_item(0).unwrap().cast_into::<PyDict>().unwrap();
            assert_eq!(
                first
                    .get_item("index")
                    .unwrap()
                    .unwrap()
                    .extract::<i64>()
                    .unwrap(),
                2
            );
        });
    }
}
