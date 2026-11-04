//! Typed Google Cloud exception hierarchy and gRPC status code mappers for `vertebra._vertebra`.
//!
//! Creates `GoogleCloudError(VertebraError)` and its subclasses (`GoogleCloudAuthError`,
//! `SpannerError`, `SpannerAbortedError`, `BigQueryError`, `BigQueryRowError`) at module
//! initialization and maps Rust `SpannerError`, `BigQueryError`, and `GcpTokenCache` errors
//! onto structured Python exceptions carrying canonical gRPC status codes (`0`..=`16`).

use google_cloud_gax::error::rpc::StatusDetails;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyList, PyType};
use vertebra_db::bigquery::BigQueryError as BigQueryClientError;
use vertebra_db::spanner::SpannerError as SpannerClientError;

const CODE_NAMES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

fn code_name(code: i32) -> &'static str {
    if (0..=16).contains(&code) {
        CODE_NAMES[code as usize]
    } else {
        "UNKNOWN"
    }
}

fn code_number(name: &str) -> i32 {
    let trimmed = name.trim();
    for (idx, candidate) in CODE_NAMES.iter().enumerate() {
        if candidate.eq_ignore_ascii_case(trimmed) {
            return idx as i32;
        }
    }
    2
}

fn http_code(status: u16) -> i32 {
    match status {
        400 => 3,
        401 => 16,
        403 => 7,
        404 => 5,
        409 => 10,
        412 => 9,
        416 => 11,
        429 => 8,
        499 => 1,
        500 => 13,
        501 => 12,
        503 => 14,
        504 => 4,
        _ => 2,
    }
}

/// Cached Python exception type objects for the Google Cloud exception hierarchy.
pub struct GcpExceptionTypes {
    pub google_cloud: Py<PyType>,
    pub auth: Py<PyType>,
    pub spanner: Py<PyType>,
    pub spanner_aborted: Py<PyType>,
    pub bigquery: Py<PyType>,
    pub bigquery_row: Py<PyType>,
}

static EXCEPTION_TYPES: PyOnceLock<GcpExceptionTypes> = PyOnceLock::new();

fn exception_types_with_base(
    py: Python<'_>,
    base: &Bound<'_, PyType>,
) -> PyResult<GcpExceptionTypes> {
    let google_cloud = PyErr::new_type(
        py,
        c"vertebra._vertebra.GoogleCloudError",
        Some(c"Error returned by a Google Cloud client; carries the canonical gRPC status in `code`, `code_name`, `message`, `http_status`, `retry_delay_ms`, and `reason`."),
        Some(base),
        None,
    )?;
    let gc_bound = google_cloud.bind(py);
    gc_bound.setattr("code", 2i32)?;
    gc_bound.setattr("code_name", "UNKNOWN")?;
    gc_bound.setattr("message", "")?;
    gc_bound.setattr("http_status", py.None())?;
    gc_bound.setattr("retry_delay_ms", py.None())?;
    gc_bound.setattr("reason", py.None())?;

    let auth = PyErr::new_type(
        py,
        c"vertebra._vertebra.GoogleCloudAuthError",
        Some(c"Credentials could not be obtained or were rejected."),
        Some(gc_bound),
        None,
    )?;
    auth.bind(py).setattr("code", 16i32)?;
    auth.bind(py).setattr("code_name", "UNAUTHENTICATED")?;

    let spanner = PyErr::new_type(
        py,
        c"vertebra._vertebra.SpannerError",
        Some(c"Error returned by a Cloud Spanner operation."),
        Some(gc_bound),
        None,
    )?;

    let spanner_aborted = PyErr::new_type(
        py,
        c"vertebra._vertebra.SpannerAbortedError",
        Some(c"Spanner aborted the transaction; the whole transaction can be retried."),
        Some(spanner.bind(py)),
        None,
    )?;
    spanner_aborted.bind(py).setattr("code", 10i32)?;
    spanner_aborted.bind(py).setattr("code_name", "ABORTED")?;

    let bigquery = PyErr::new_type(
        py,
        c"vertebra._vertebra.BigQueryError",
        Some(c"Error returned by a BigQuery operation."),
        Some(gc_bound),
        None,
    )?;

    let bigquery_row = PyErr::new_type(
        py,
        c"vertebra._vertebra.BigQueryRowError",
        Some(c"BigQuery Storage Write rejected rows; see `row_errors`."),
        Some(bigquery.bind(py)),
        None,
    )?;
    bigquery_row.bind(py).setattr("code", 3i32)?;
    bigquery_row
        .bind(py)
        .setattr("code_name", "INVALID_ARGUMENT")?;
    bigquery_row
        .bind(py)
        .setattr("row_errors", PyList::empty(py))?;

    Ok(GcpExceptionTypes {
        google_cloud,
        auth,
        spanner,
        spanner_aborted,
        bigquery,
        bigquery_row,
    })
}

/// Resolve or initialize the cached Google Cloud exception hierarchy rooted at `VertebraError`.
pub fn exception_types(py: Python<'_>) -> PyResult<&'static GcpExceptionTypes> {
    EXCEPTION_TYPES.get_or_try_init(py, || {
        let base = py
            .import("vertebra._typing")?
            .getattr("VertebraError")?
            .cast_into::<PyType>()?;
        exception_types_with_base(py, &base)
    })
}

struct GcpErrorInfo {
    code: i32,
    message: String,
    http_status: Option<u16>,
    retry_delay_ms: Option<u64>,
    reason: Option<String>,
}

impl GcpErrorInfo {
    fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            http_status: None,
            retry_delay_ms: None,
            reason: None,
        }
    }
}

fn gax_error_info(err: &google_cloud_gax::error::Error) -> GcpErrorInfo {
    let http_status = err.http_status_code();
    if let Some(status) = err.status() {
        let code = code_number(status.code.name());
        let message = if status.message.is_empty() {
            err.to_string()
        } else {
            status.message.clone()
        };
        let mut retry_delay_ms = None;
        let mut reason = None;
        for detail in &status.details {
            match detail {
                StatusDetails::RetryInfo(retry) if retry_delay_ms.is_none() => {
                    if let Some(delay) = &retry.retry_delay {
                        let secs = delay.seconds().max(0) as u64;
                        let nanos = delay.nanos().max(0) as u64;
                        retry_delay_ms = Some(secs * 1000 + nanos / 1_000_000);
                    }
                }
                StatusDetails::ErrorInfo(info) if reason.is_none() && !info.reason.is_empty() => {
                    reason = Some(info.reason.clone());
                }
                _ => {}
            }
        }
        GcpErrorInfo {
            code,
            message,
            http_status,
            retry_delay_ms,
            reason,
        }
    } else {
        let code = if err.is_authentication() {
            16
        } else if err.is_timeout() {
            4
        } else if err.is_transport() || err.is_io() || err.is_connect() {
            14
        } else if let Some(http) = http_status {
            http_code(http)
        } else {
            2
        };
        GcpErrorInfo {
            code,
            message: err.to_string(),
            http_status,
            retry_delay_ms: None,
            reason: None,
        }
    }
}

fn raise(py: Python<'_>, ty: &Py<PyType>, info: GcpErrorInfo) -> PyErr {
    let name = code_name(info.code);
    let err = PyErr::from_type(ty.bind(py).clone(), (info.message.clone(),));
    let val = err.value(py);
    if let Err(e) = val
        .setattr("code", info.code)
        .and_then(|()| val.setattr("code_name", name))
        .and_then(|()| val.setattr("message", info.message))
        .and_then(|()| val.setattr("http_status", info.http_status))
        .and_then(|()| val.setattr("retry_delay_ms", info.retry_delay_ms))
        .and_then(|()| val.setattr("reason", info.reason))
    {
        return e;
    }
    err
}

fn spanner_error_with(py: Python<'_>, types: &GcpExceptionTypes, err: SpannerClientError) -> PyErr {
    match err {
        SpannerClientError::InvalidArgument(msg) => {
            raise(py, &types.spanner, GcpErrorInfo::new(3, msg))
        }
        SpannerClientError::Auth(msg) => raise(py, &types.auth, GcpErrorInfo::new(16, msg)),
        SpannerClientError::Client(gax_err) => {
            let info = gax_error_info(&gax_err);
            let ty = if info.code == 10 {
                &types.spanner_aborted
            } else {
                &types.spanner
            };
            raise(py, ty, info)
        }
        SpannerClientError::Protocol(msg) => raise(py, &types.spanner, GcpErrorInfo::new(13, msg)),
        SpannerClientError::Ddl {
            operation,
            code,
            message,
        } => raise(
            py,
            &types.spanner,
            GcpErrorInfo::new(code, format!("{operation}: {message}")),
        ),
        other @ SpannerClientError::DdlTimeout { .. } => {
            raise(py, &types.spanner, GcpErrorInfo::new(4, other.to_string()))
        }
    }
}

/// Convert a Rust `vertebra_db::spanner::SpannerError` into a typed Python `PyErr`.
pub fn spanner_error_to_py(py: Python<'_>, err: SpannerClientError) -> PyErr {
    match exception_types(py) {
        Ok(types) => spanner_error_with(py, types, err),
        Err(e) => e,
    }
}

fn bigquery_error_with(
    py: Python<'_>,
    types: &GcpExceptionTypes,
    err: BigQueryClientError,
) -> PyErr {
    match err {
        BigQueryClientError::Auth(msg) => raise(py, &types.auth, GcpErrorInfo::new(16, msg)),
        BigQueryClientError::Http(msg) => raise(py, &types.bigquery, GcpErrorInfo::new(14, msg)),
        BigQueryClientError::Api {
            status,
            message,
            reason,
            canonical_status,
        } => {
            let code = canonical_status
                .as_deref()
                .map(code_number)
                .unwrap_or_else(|| http_code(status));
            raise(
                py,
                &types.bigquery,
                GcpErrorInfo {
                    code,
                    message,
                    http_status: Some(status),
                    retry_delay_ms: None,
                    reason,
                },
            )
        }
        BigQueryClientError::Client(gax_err) => {
            let info = gax_error_info(&gax_err);
            raise(py, &types.bigquery, info)
        }
        BigQueryClientError::Shm(msg) | BigQueryClientError::AppendVerification(msg) => {
            raise(py, &types.bigquery, GcpErrorInfo::new(13, msg))
        }
        BigQueryClientError::InvalidArgument(msg) => {
            raise(py, &types.bigquery, GcpErrorInfo::new(3, msg))
        }
        BigQueryClientError::RowErrors(rows) => {
            let message = format!("BigQuery Storage Write rejected {} row(s)", rows.len());
            let row_tuples: Vec<(i64, i32, String)> = rows
                .into_iter()
                .map(|r| (r.index, r.code, r.message))
                .collect();
            let err = raise(py, &types.bigquery_row, GcpErrorInfo::new(3, message));
            if let Err(e) = err.value(py).setattr("row_errors", row_tuples) {
                return e;
            }
            err
        }
    }
}

/// Convert a Rust `vertebra_db::bigquery::BigQueryError` into a typed Python `PyErr`.
pub fn bigquery_error_to_py(py: Python<'_>, err: BigQueryClientError) -> PyErr {
    match exception_types(py) {
        Ok(types) => bigquery_error_with(py, types, err),
        Err(e) => e,
    }
}

fn auth_error_with(py: Python<'_>, types: &GcpExceptionTypes, message: String) -> PyErr {
    raise(py, &types.auth, GcpErrorInfo::new(16, message))
}

/// Convert a `GcpTokenCache` refresh error message into a Python `GoogleCloudAuthError`.
pub fn auth_error_to_py(py: Python<'_>, message: String) -> PyErr {
    match exception_types(py) {
        Ok(types) => auth_error_with(py, types, message),
        Err(e) => e,
    }
}

/// Register the Google Cloud exception classes on `vertebra._vertebra`.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let types = exception_types(py)?;
    m.add("GoogleCloudError", types.google_cloud.bind(py))?;
    m.add("GoogleCloudAuthError", types.auth.bind(py))?;
    m.add("SpannerError", types.spanner.bind(py))?;
    m.add("SpannerAbortedError", types.spanner_aborted.bind(py))?;
    m.add("BigQueryError", types.bigquery.bind(py))?;
    m.add("BigQueryRowError", types.bigquery_row.bind(py))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
    use pyo3::prelude::*;
    use pyo3::types::PyType;
    use vertebra_db::bigquery::{BigQueryError as BigQueryClientError, BigQueryRowError};
    use vertebra_db::spanner::SpannerError as SpannerClientError;

    use super::{
        GcpExceptionTypes, bigquery_error_with, exception_types_with_base, spanner_error_with,
    };

    fn stand_in_types(py: Python<'_>) -> (Bound<'_, PyType>, GcpExceptionTypes) {
        let base = PyErr::new_type(
            py,
            c"tests.VertebraError",
            None,
            Some(&py.get_type::<pyo3::exceptions::PyException>()),
            None,
        )
        .expect("base")
        .into_bound(py);
        let types = exception_types_with_base(py, &base).expect("types");
        (base, types)
    }

    #[test]
    fn hierarchy_derives_from_stand_in_base() {
        Python::attach(|py| {
            let (base, types) = stand_in_types(py);
            assert!(
                types
                    .google_cloud
                    .bind(py)
                    .is_subclass(&base)
                    .expect("subclass")
            );
            assert!(
                types
                    .auth
                    .bind(py)
                    .is_subclass(types.google_cloud.bind(py))
                    .expect("subclass")
            );
            assert!(
                types
                    .spanner
                    .bind(py)
                    .is_subclass(types.google_cloud.bind(py))
                    .expect("subclass")
            );
            assert!(
                types
                    .spanner_aborted
                    .bind(py)
                    .is_subclass(types.spanner.bind(py))
                    .expect("subclass")
            );
            assert!(
                types
                    .bigquery
                    .bind(py)
                    .is_subclass(types.google_cloud.bind(py))
                    .expect("subclass")
            );
            assert!(
                types
                    .bigquery_row
                    .bind(py)
                    .is_subclass(types.bigquery.bind(py))
                    .expect("subclass")
            );
        });
    }

    #[test]
    fn aborted_status_maps_to_spanner_aborted_error() {
        Python::attach(|py| {
            let (_base, types) = stand_in_types(py);
            let status = Status::default()
                .set_code(Code::Aborted)
                .set_message("txn aborted")
                .set_details([StatusDetails::RetryInfo(
                    google_cloud_rpc::model::RetryInfo::new().set_retry_delay(
                        google_cloud_wkt::Duration::new(0, 250_000_000).expect("duration"),
                    ),
                )]);
            let err = spanner_error_with(
                py,
                &types,
                SpannerClientError::Client(google_cloud_gax::error::Error::service(status)),
            );
            assert!(err.is_instance(py, types.spanner_aborted.bind(py)));
            let val = err.value(py);
            assert_eq!(
                val.getattr("code")
                    .expect("code")
                    .extract::<i32>()
                    .expect("i32"),
                10
            );
            assert_eq!(
                val.getattr("code_name")
                    .expect("code_name")
                    .extract::<String>()
                    .expect("str"),
                "ABORTED"
            );
            assert_eq!(
                val.getattr("retry_delay_ms")
                    .expect("retry_delay_ms")
                    .extract::<Option<u64>>()
                    .expect("u64"),
                Some(250)
            );
        });
    }

    #[test]
    fn rest_api_error_prefers_canonical_status() {
        Python::attach(|py| {
            let (_base, types) = stand_in_types(py);
            let err = bigquery_error_with(
                py,
                &types,
                BigQueryClientError::Api {
                    status: 400,
                    message: "m".into(),
                    reason: Some("notFound".into()),
                    canonical_status: Some("NOT_FOUND".into()),
                },
            );
            let val = err.value(py);
            assert_eq!(
                val.getattr("code")
                    .expect("code")
                    .extract::<i32>()
                    .expect("i32"),
                5
            );
            assert_eq!(
                val.getattr("http_status")
                    .expect("http_status")
                    .extract::<Option<u16>>()
                    .expect("u16"),
                Some(400)
            );
            assert_eq!(
                val.getattr("reason")
                    .expect("reason")
                    .extract::<Option<String>>()
                    .expect("str"),
                Some("notFound".to_string())
            );
        });
    }

    #[test]
    fn rest_api_error_maps_http_status() {
        Python::attach(|py| {
            let (_base, types) = stand_in_types(py);
            let err = bigquery_error_with(
                py,
                &types,
                BigQueryClientError::Api {
                    status: 429,
                    message: "rateLimitExceeded".into(),
                    reason: Some("rateLimitExceeded".into()),
                    canonical_status: None,
                },
            );
            let val = err.value(py);
            assert_eq!(
                val.getattr("code")
                    .expect("code")
                    .extract::<i32>()
                    .expect("i32"),
                8
            );
            assert_eq!(
                val.getattr("code_name")
                    .expect("code_name")
                    .extract::<String>()
                    .expect("str"),
                "RESOURCE_EXHAUSTED"
            );
        });
    }

    #[test]
    fn row_errors_map_to_bigquery_row_error() {
        Python::attach(|py| {
            let (_base, types) = stand_in_types(py);
            let err = bigquery_error_with(
                py,
                &types,
                BigQueryClientError::RowErrors(vec![BigQueryRowError {
                    index: 0,
                    code: 1,
                    message: "bad field".into(),
                }]),
            );
            assert!(err.is_instance(py, types.bigquery_row.bind(py)));
            let val = err.value(py);
            let rows: Vec<(i64, i32, String)> = val
                .getattr("row_errors")
                .expect("row_errors")
                .extract()
                .expect("extract");
            assert_eq!(rows, vec![(0, 1, "bad field".to_string())]);
        });
    }

    #[test]
    fn ddl_timeout_maps_to_deadline_exceeded() {
        Python::attach(|py| {
            let (_base, types) = stand_in_types(py);
            let err = spanner_error_with(
                py,
                &types,
                SpannerClientError::DdlTimeout {
                    operation: "operations/op1".into(),
                    timeout_ms: 120_000,
                },
            );
            let val = err.value(py);
            assert_eq!(
                val.getattr("code")
                    .expect("code")
                    .extract::<i32>()
                    .expect("i32"),
                4
            );
            assert_eq!(
                val.getattr("code_name")
                    .expect("code_name")
                    .extract::<String>()
                    .expect("str"),
                "DEADLINE_EXCEEDED"
            );
        });
    }
}
