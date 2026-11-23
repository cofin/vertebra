//! Python exception mapping for `vertebra._bigquery`.
//!
//! Resolves `GoogleCloudAuthError`, `BigQueryError`, and `BigQueryRowError` via
//! `vertebra_db::py_errors` so Python callers catch the exact shared exception hierarchy.

use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyType};
use vertebra_db::gcp::{GcpErrorInfo, code_name, code_number, gax_error_info, http_code};
use vertebra_db::py_errors::{DbPyExceptionTypes, instantiate_db_exc, resolve_db_exceptions};

use crate::client::{BigQueryError, BigQueryRowError};

fn attach_gcp_attrs(py: Python<'_>, err: PyErr, info: GcpErrorInfo) -> PyErr {
    let cname = code_name(info.code);
    let val = err.value(py);
    if let Err(attr_err) = val
        .setattr("code", info.code)
        .and_then(|()| val.setattr("code_name", cname))
        .and_then(|()| val.setattr("message", info.message))
        .and_then(|()| val.setattr("http_status", info.http_status))
        .and_then(|()| val.setattr("retry_delay_ms", info.retry_delay_ms))
        .and_then(|()| val.setattr("reason", info.reason))
    {
        return attr_err;
    }
    err
}

fn instantiate_gcp_exc(py: Python<'_>, ty: &Py<PyType>, info: GcpErrorInfo) -> PyErr {
    let is_transient = matches!(info.code, 4 | 8 | 10 | 14);
    let sqlstate = (info.code == 10).then_some("40001");
    let err = instantiate_db_exc(
        py,
        ty,
        &info.message,
        sqlstate,
        Some(i64::from(info.code)),
        is_transient,
    );
    attach_gcp_attrs(py, err, info)
}

fn row_errors_to_py(
    py: Python<'_>,
    types: &DbPyExceptionTypes,
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
    let err = instantiate_gcp_exc(py, &types.bigquery_row, GcpErrorInfo::new(3, summary));
    if let Err(e) = err.value(py).setattr("row_errors", list) {
        return e;
    }
    err
}

fn bigquery_error_with(py: Python<'_>, types: &DbPyExceptionTypes, err: BigQueryError) -> PyErr {
    match err {
        BigQueryError::Auth(message) => {
            instantiate_gcp_exc(py, &types.google_cloud_auth, GcpErrorInfo::new(16, message))
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
                &types.google_cloud_auth
            } else {
                &types.bigquery
            };
            instantiate_gcp_exc(
                py,
                ty,
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
                &types.google_cloud_auth
            } else {
                &types.bigquery
            };
            instantiate_gcp_exc(py, ty, info)
        }
        BigQueryError::RowErrors(rows) => row_errors_to_py(py, types, rows),
        BigQueryError::Http(message) => {
            instantiate_gcp_exc(py, &types.bigquery, GcpErrorInfo::new(14, message))
        }
        BigQueryError::Shm(message) | BigQueryError::AppendVerification(message) => {
            instantiate_gcp_exc(py, &types.bigquery, GcpErrorInfo::new(13, message))
        }
        BigQueryError::InvalidArgument(message) => {
            instantiate_gcp_exc(py, &types.bigquery, GcpErrorInfo::new(3, message))
        }
    }
}

/// Convert a native [`BigQueryError`] into a typed Python `BigQueryError` / `GoogleCloudAuthError`.
pub fn bigquery_error_to_py(py: Python<'_>, err: BigQueryError) -> PyErr {
    match resolve_db_exceptions(py) {
        Ok(types) => bigquery_error_with(py, types, err),
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};
    use vertebra_db::py_errors::resolve_db_exceptions;

    use crate::client::{BigQueryError, BigQueryRowError};

    use super::bigquery_error_with;

    #[test]
    fn rest_api_error_prefers_canonical_status() {
        Python::initialize();
        Python::attach(|py| {
            let types = resolve_db_exceptions(py).expect("types");
            let err = bigquery_error_with(
                py,
                types,
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
            let types = resolve_db_exceptions(py).expect("types");
            let rate_err = bigquery_error_with(
                py,
                types,
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
                types,
                BigQueryError::Api {
                    status: 401,
                    message: "Invalid Credentials".into(),
                    reason: Some("authError".into()),
                    canonical_status: None,
                },
            );
            assert!(auth_err.is_instance(py, types.google_cloud_auth.bind(py)));
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
            let types = resolve_db_exceptions(py).expect("types");
            let err = bigquery_error_with(
                py,
                types,
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
