//! Python exception mapping for `vertebra._spanner`.
//!
//! Resolves `GoogleCloudAuthError`, `SpannerError`, and `SpannerAbortedError` from
//! `vertebra._vertebra` via [`PyOnceLock`] so Python callers catch the exact shared exception
//! hierarchy, falling back to local stand-in subclasses during standalone Rust unit tests.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};
use vertebra_db::gcp::{GcpErrorInfo, code_name, gax_error_info};

use crate::client::SpannerError;

/// Cached Python exception classes used by `vertebra._spanner`.
pub struct SpannerExceptionTypes {
    pub auth: Py<PyType>,
    pub spanner: Py<PyType>,
    pub spanner_aborted: Py<PyType>,
}

static SPANNER_EXCEPTION_TYPES: PyOnceLock<SpannerExceptionTypes> = PyOnceLock::new();

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

fn stand_in_exception_types(py: Python<'_>) -> PyResult<SpannerExceptionTypes> {
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
    let serialization_failure = new_stand_in_class(
        py,
        "SerializationFailure",
        &PyTuple::new(py, [operational.bind(py)])?,
    )?;
    let gc = new_stand_in_class(py, "GoogleCloudError", &PyTuple::new(py, [&root])?)?;
    let auth = new_stand_in_class(
        py,
        "GoogleCloudAuthError",
        &PyTuple::new(py, [gc.bind(py), operational.bind(py)])?,
    )?;
    let spanner = new_stand_in_class(
        py,
        "SpannerError",
        &PyTuple::new(py, [gc.bind(py), db.bind(py)])?,
    )?;
    let spanner_aborted = new_stand_in_class(
        py,
        "SpannerAbortedError",
        &PyTuple::new(py, [spanner.bind(py), serialization_failure.bind(py)])?,
    )?;
    Ok(SpannerExceptionTypes {
        auth,
        spanner,
        spanner_aborted,
    })
}

fn resolve_exception_types(py: Python<'_>) -> PyResult<&'static SpannerExceptionTypes> {
    SPANNER_EXCEPTION_TYPES.get_or_try_init(py, || match py.import("vertebra._vertebra") {
        Ok(core_mod) => {
            let auth = core_mod
                .getattr("GoogleCloudAuthError")?
                .cast_into::<PyType>()?
                .unbind();
            let spanner = core_mod
                .getattr("SpannerError")?
                .cast_into::<PyType>()?
                .unbind();
            let spanner_aborted = core_mod
                .getattr("SpannerAbortedError")?
                .cast_into::<PyType>()?
                .unbind();
            Ok(SpannerExceptionTypes {
                auth,
                spanner,
                spanner_aborted,
            })
        }
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyModuleNotFoundError>(py) => {
            stand_in_exception_types(py)
        }
        Err(err) => Err(err),
    })
}

fn select_spanner_type(types: &SpannerExceptionTypes, code: i32) -> &Py<PyType> {
    match code {
        10 => &types.spanner_aborted,
        16 => &types.auth,
        _ => &types.spanner,
    }
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

fn spanner_error_with(py: Python<'_>, types: &SpannerExceptionTypes, err: SpannerError) -> PyErr {
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
    let py_err = PyErr::from_type(ty.bind(py).clone(), (info.message.clone(),));
    attach_gcp_attrs(py, py_err, info)
}

/// Convert a native [`SpannerError`] into a typed Python `SpannerError` / `SpannerAbortedError` / `GoogleCloudAuthError`.
pub fn spanner_error_to_py(py: Python<'_>, err: SpannerError) -> PyErr {
    match resolve_exception_types(py) {
        Ok(types) => spanner_error_with(py, types, err),
        Err(e) => e,
    }
}

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
    use pyo3::prelude::*;

    use crate::client::SpannerError;

    use super::{spanner_error_with, stand_in_exception_types};

    #[test]
    fn aborted_status_maps_to_spanner_aborted_error() {
        Python::initialize();
        Python::attach(|py| {
            let types = stand_in_exception_types(py).expect("types");
            let status = Status::default()
                .set_code(Code::Aborted)
                .set_message("Transaction was aborted")
                .set_details([StatusDetails::RetryInfo(
                    google_cloud_rpc::model::RetryInfo::new().set_retry_delay(
                        google_cloud_wkt::Duration::new(0, 75_000_000).expect("duration"),
                    ),
                )]);
            let gax_err = google_cloud_gax::error::Error::service(status);
            let err = spanner_error_with(py, &types, SpannerError::Client(gax_err));
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
            let types = stand_in_exception_types(py).expect("types");
            let err = spanner_error_with(
                py,
                &types,
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
