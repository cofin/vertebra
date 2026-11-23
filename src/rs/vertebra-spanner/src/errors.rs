//! Python exception mapping for `vertebra._spanner`.
//!
//! Resolves `GoogleCloudAuthError`, `SpannerError`, and `SpannerAbortedError` via
//! `vertebra_db::py_errors` so Python callers catch the exact shared exception hierarchy.

use pyo3::prelude::*;
use pyo3::types::PyType;
use vertebra_db::gcp::{GcpErrorInfo, code_name, gax_error_info};
use vertebra_db::py_errors::{DbPyExceptionTypes, instantiate_db_exc, resolve_db_exceptions};

use crate::client::SpannerError;

fn select_spanner_type(types: &DbPyExceptionTypes, code: i32) -> &Py<PyType> {
    match code {
        10 => &types.spanner_aborted,
        16 => &types.google_cloud_auth,
        _ => &types.spanner,
    }
}

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

fn spanner_error_with(py: Python<'_>, types: &DbPyExceptionTypes, err: SpannerError) -> PyErr {
    let info = match err {
        SpannerError::InvalidArgument(msg) => GcpErrorInfo::new(3, msg),
        SpannerError::Auth(msg) => GcpErrorInfo::new(16, msg),
        SpannerError::Client(gax_err) => gax_error_info(&gax_err),
        SpannerError::Gcp(info) => info,
        SpannerError::Protocol(msg) => GcpErrorInfo::new(13, msg),
        SpannerError::Ddl {
            operation,
            code,
            message,
        } => GcpErrorInfo::new(
            code,
            format!("UpdateDatabaseDdl operation {operation} failed: {message}"),
        ),
        SpannerError::DdlTimeout { .. } => GcpErrorInfo::new(4, err.to_string()),
    };
    let ty = select_spanner_type(types, info.code);
    let is_transient = matches!(info.code, 4 | 8 | 10 | 14);
    let sqlstate = (info.code == 10).then_some("40001");
    let py_err = instantiate_db_exc(
        py,
        ty,
        &info.message,
        sqlstate,
        Some(i64::from(info.code)),
        is_transient,
    );
    attach_gcp_attrs(py, py_err, info)
}

/// Convert a native [`SpannerError`] into a typed Python `SpannerError` / `SpannerAbortedError` / `GoogleCloudAuthError`.
pub fn spanner_error_to_py(py: Python<'_>, err: SpannerError) -> PyErr {
    match resolve_db_exceptions(py) {
        Ok(types) => spanner_error_with(py, types, err),
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
    use pyo3::prelude::*;
    use vertebra_db::py_errors::resolve_db_exceptions;

    use crate::client::SpannerError;

    use super::spanner_error_with;

    #[test]
    fn aborted_status_maps_to_spanner_aborted_error() {
        Python::initialize();
        Python::attach(|py| {
            let types = resolve_db_exceptions(py).expect("types");
            let status = Status::default()
                .set_code(Code::Aborted)
                .set_message("Transaction was aborted")
                .set_details([StatusDetails::RetryInfo(
                    google_cloud_rpc::model::RetryInfo::new().set_retry_delay(
                        google_cloud_wkt::Duration::new(0, 75_000_000).expect("duration"),
                    ),
                )]);
            let gax_err = google_cloud_gax::error::Error::service(status);
            let err = spanner_error_with(py, types, SpannerError::Client(gax_err));
            assert!(err.is_instance(py, types.spanner_aborted.bind(py)));
            assert!(err.is_instance(py, types.spanner.bind(py)));
            let val = err.value(py);
            assert_eq!(val.getattr("code").unwrap().extract::<i32>().unwrap(), 10);
            assert_eq!(
                val.getattr("code_name")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "ABORTED"
            );
            assert_eq!(
                val.getattr("retry_delay_ms")
                    .unwrap()
                    .extract::<Option<u64>>()
                    .unwrap(),
                Some(75)
            );
            assert_eq!(
                val.getattr("sqlstate")
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("40001")
            );
            assert!(
                val.getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
        });
    }

    #[test]
    fn ddl_timeout_maps_to_deadline_exceeded() {
        Python::initialize();
        Python::attach(|py| {
            let types = resolve_db_exceptions(py).expect("types");
            let err = spanner_error_with(
                py,
                types,
                SpannerError::DdlTimeout {
                    operation: "operations/op-1".into(),
                    timeout_ms: 500,
                },
            );
            assert!(err.is_instance(py, types.spanner.bind(py)));
            let val = err.value(py);
            assert_eq!(val.getattr("code").unwrap().extract::<i32>().unwrap(), 4);
            assert_eq!(
                val.getattr("code_name")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "DEADLINE_EXCEEDED"
            );
            assert!(
                val.getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
        });
    }
}
