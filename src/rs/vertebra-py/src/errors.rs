//! Typed database and Google Cloud exception hierarchies for `vertebra._vertebra`.
//!
//! Registers the unified `DatabaseError(VertebraError)` PEP 249 / transient hierarchy alongside
//! `GoogleCloudError(VertebraError)` and multi-inheritance subclasses (`NotFoundError`,
//! `SpannerError`, `SpannerAbortedError`, `BigQueryError`, `BigQueryRowError`) using safe
//! `PyType` construction (zero `unsafe`).

use google_cloud_gax::error::rpc::StatusDetails;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyList, PyTuple, PyType};
use vertebra_db::bigquery::BigQueryError as BigQueryClientError;
use vertebra_db::connection::TransportError;
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

/// Cached Python exception type objects for the `DatabaseError` hierarchy.
pub struct DbExceptionTypes {
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

struct AllExceptionTypes {
    db: DbExceptionTypes,
    gcp: GcpExceptionTypes,
}

static ALL_EXCEPTION_TYPES: PyOnceLock<AllExceptionTypes> = PyOnceLock::new();

fn new_exception_class<'py>(
    py: Python<'py>,
    name: &str,
    bases: &Bound<'py, PyTuple>,
    doc: &str,
    attrs: &[(&str, Bound<'py, PyAny>)],
) -> PyResult<Py<PyType>> {
    let dict = PyDict::new(py);
    dict.set_item("__module__", "vertebra._vertebra")?;
    dict.set_item("__doc__", doc)?;
    for (key, value) in attrs {
        dict.set_item(*key, value)?;
    }
    let ty = py
        .get_type::<PyType>()
        .call1((name, bases, dict))?
        .cast_into::<PyType>()?;
    Ok(ty.unbind())
}

fn build_all_exception_types(
    py: Python<'_>,
    base: &Bound<'_, PyType>,
) -> PyResult<AllExceptionTypes> {
    let none_val = py.None().into_bound(py);
    let false_val = false.into_pyobject(py)?.to_owned().into_any();
    let true_val = true.into_pyobject(py)?.to_owned().into_any();

    let database = new_exception_class(
        py,
        "DatabaseError",
        &PyTuple::new(py, [base])?,
        "Base error raised for database driver, wire-protocol, or query execution failures.",
        &[
            ("sqlstate", none_val.clone()),
            ("vendor_code", none_val.clone()),
            ("is_transient", false_val.clone()),
        ],
    )?;
    let db_bound = database.bind(py);

    let interface = new_exception_class(
        py,
        "InterfaceError",
        &PyTuple::new(py, [db_bound])?,
        "Raised when a driver interface, configuration, or connection handle is misused locally.",
        &[],
    )?;

    let data = new_exception_class(
        py,
        "DataError",
        &PyTuple::new(py, [db_bound])?,
        "Raised for value range, division-by-zero, encoding, or scalar cardinality errors.",
        &[],
    )?;
    let data_bound = data.bind(py);

    let py_value_err = py.get_type::<PyValueError>();
    let not_found = new_exception_class(
        py,
        "NotFoundError",
        &PyTuple::new(py, [data_bound, &py_value_err])?,
        "Raised by `select_one` or `select_value` when a query returns zero rows.",
        &[],
    )?;

    let operational = new_exception_class(
        py,
        "OperationalError",
        &PyTuple::new(py, [db_bound])?,
        "Raised for database operational failures outside direct application control.",
        &[],
    )?;
    let op_bound = operational.bind(py);

    let connection_failure = new_exception_class(
        py,
        "ConnectionFailure",
        &PyTuple::new(py, [op_bound])?,
        "Transient transport, socket, TLS dial, or lost-connection error.",
        &[("is_transient", true_val.clone())],
    )?;

    let pool_exhausted = new_exception_class(
        py,
        "PoolExhaustedError",
        &PyTuple::new(py, [op_bound])?,
        "Transient error raised when a connection or lease pool times out acquiring a slot.",
        &[("is_transient", true_val.clone())],
    )?;

    let query_cancelled = new_exception_class(
        py,
        "QueryCancelledError",
        &PyTuple::new(py, [op_bound])?,
        "Raised when an active statement is cancelled by user request or statement timeout (`57014`).",
        &[
            ("sqlstate", "57014".into_pyobject(py)?.into_any()),
            ("is_transient", false_val.clone()),
        ],
    )?;

    let serialization_failure = new_exception_class(
        py,
        "SerializationFailure",
        &PyTuple::new(py, [op_bound])?,
        "Transient serialization or deadlock failure (`40001`); retry the whole transaction.",
        &[
            ("sqlstate", "40001".into_pyobject(py)?.into_any()),
            ("is_transient", true_val.clone()),
        ],
    )?;
    let ser_bound = serialization_failure.bind(py);

    let integrity = new_exception_class(
        py,
        "IntegrityError",
        &PyTuple::new(py, [db_bound])?,
        "Raised when a relational integrity constraint (unique, foreign-key, check, not-null) fails.",
        &[],
    )?;

    let internal = new_exception_class(
        py,
        "InternalError",
        &PyTuple::new(py, [db_bound])?,
        "Raised when the database engine reports an internal state or cursor synchronization error.",
        &[],
    )?;

    let programming = new_exception_class(
        py,
        "ProgrammingError",
        &PyTuple::new(py, [db_bound])?,
        "Raised for SQL syntax errors, missing tables/columns, or invalid parameter bindings.",
        &[],
    )?;

    let not_supported = new_exception_class(
        py,
        "NotSupportedError",
        &PyTuple::new(py, [db_bound])?,
        "Raised when a requested capability or SQL feature is not supported by the database engine.",
        &[],
    )?;

    let google_cloud = new_exception_class(
        py,
        "GoogleCloudError",
        &PyTuple::new(py, [base])?,
        "Error returned by a Google Cloud client; carries the canonical gRPC status in `code`, `code_name`, `message`, `http_status`, `retry_delay_ms`, and `reason`.",
        &[
            ("code", 2i32.into_pyobject(py)?.into_any()),
            ("code_name", "UNKNOWN".into_pyobject(py)?.into_any()),
            ("message", "".into_pyobject(py)?.into_any()),
            ("http_status", none_val.clone()),
            ("retry_delay_ms", none_val.clone()),
            ("reason", none_val),
        ],
    )?;
    let gc_bound = google_cloud.bind(py);

    let auth = new_exception_class(
        py,
        "GoogleCloudAuthError",
        &PyTuple::new(py, [gc_bound])?,
        "Credentials could not be obtained or were rejected.",
        &[
            ("code", 16i32.into_pyobject(py)?.into_any()),
            ("code_name", "UNAUTHENTICATED".into_pyobject(py)?.into_any()),
        ],
    )?;

    let spanner = new_exception_class(
        py,
        "SpannerError",
        &PyTuple::new(py, [gc_bound, db_bound])?,
        "Error returned by a Cloud Spanner operation.",
        &[],
    )?;
    let spanner_bound = spanner.bind(py);

    let spanner_aborted = new_exception_class(
        py,
        "SpannerAbortedError",
        &PyTuple::new(py, [spanner_bound, ser_bound])?,
        "Spanner aborted the transaction; the whole transaction can be retried.",
        &[
            ("code", 10i32.into_pyobject(py)?.into_any()),
            ("code_name", "ABORTED".into_pyobject(py)?.into_any()),
            ("sqlstate", "40001".into_pyobject(py)?.into_any()),
            ("is_transient", true_val),
        ],
    )?;

    let bigquery = new_exception_class(
        py,
        "BigQueryError",
        &PyTuple::new(py, [gc_bound, db_bound])?,
        "Error returned by a BigQuery operation.",
        &[],
    )?;
    let bq_bound = bigquery.bind(py);

    let bigquery_row = new_exception_class(
        py,
        "BigQueryRowError",
        &PyTuple::new(py, [bq_bound, data_bound])?,
        "BigQuery Storage Write rejected rows; see `row_errors`.",
        &[
            ("code", 3i32.into_pyobject(py)?.into_any()),
            (
                "code_name",
                "INVALID_ARGUMENT".into_pyobject(py)?.into_any(),
            ),
            ("row_errors", PyList::empty(py).into_any()),
        ],
    )?;

    Ok(AllExceptionTypes {
        db: DbExceptionTypes {
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
        },
        gcp: GcpExceptionTypes {
            google_cloud,
            auth,
            spanner,
            spanner_aborted,
            bigquery,
            bigquery_row,
        },
    })
}

fn all_exception_types(py: Python<'_>) -> PyResult<&'static AllExceptionTypes> {
    ALL_EXCEPTION_TYPES.get_or_try_init(py, || {
        let base = py
            .import("vertebra._typing")?
            .getattr("VertebraError")?
            .cast_into::<PyType>()?;
        build_all_exception_types(py, &base)
    })
}

/// Resolve or initialize the cached `DatabaseError` hierarchy rooted at `VertebraError`.
pub fn db_exception_types(py: Python<'_>) -> PyResult<&'static DbExceptionTypes> {
    Ok(&all_exception_types(py)?.db)
}

/// Resolve or initialize the cached Google Cloud exception hierarchy rooted at `VertebraError`.
pub fn exception_types(py: Python<'_>) -> PyResult<&'static GcpExceptionTypes> {
    Ok(&all_exception_types(py)?.gcp)
}

fn db_error_with(
    py: Python<'_>,
    types: &DbExceptionTypes,
    sqlstate: Option<&str>,
    vendor_code: Option<i64>,
    is_transient_override: Option<bool>,
    message: String,
) -> PyErr {
    let ty = match sqlstate {
        Some("40001" | "40P01") => &types.serialization_failure,
        Some("57014") => &types.query_cancelled,
        Some("53300") => &types.pool_exhausted,
        Some(s) if s.starts_with("08") => &types.connection_failure,
        Some(s) if s.starts_with("23") => &types.integrity,
        Some(s) if s.starts_with("22") => &types.data,
        Some(s) if s.starts_with("42") => &types.programming,
        Some(s) if s.starts_with("0A") => &types.not_supported,
        Some(s) if s.starts_with("XX") => &types.internal,
        Some(s)
            if s.starts_with("53")
                || s.starts_with("54")
                || s.starts_with("57")
                || s.starts_with("58") =>
        {
            &types.operational
        }
        _ => &types.database,
    };
    let err = PyErr::from_type(ty.bind(py).clone(), (message,));
    let val = err.value(py);
    if let Some(state) = sqlstate
        && let Err(e) = val.setattr("sqlstate", state)
    {
        return e;
    }
    if let Some(code) = vendor_code
        && let Err(e) = val.setattr("vendor_code", code)
    {
        return e;
    }
    if let Some(transient) = is_transient_override
        && let Err(e) = val.setattr("is_transient", transient)
    {
        return e;
    }
    err
}

/// Construct a typed Python `DatabaseError` subclass from SQLSTATE, vendor code, and transience metadata.
pub fn db_error_to_py(
    py: Python<'_>,
    sqlstate: Option<&str>,
    vendor_code: Option<i64>,
    is_transient_override: Option<bool>,
    message: impl Into<String>,
) -> PyErr {
    match db_exception_types(py) {
        Ok(types) => db_error_with(
            py,
            types,
            sqlstate,
            vendor_code,
            is_transient_override,
            message.into(),
        ),
        Err(e) => e,
    }
}

fn transport_error_with(py: Python<'_>, types: &DbExceptionTypes, err: &TransportError) -> PyErr {
    let ty = match err {
        TransportError::Cancelled => &types.query_cancelled,
        TransportError::InvalidConfig(_) => &types.interface,
        TransportError::Io(_)
        | TransportError::Tls(_)
        | TransportError::Timeout(_)
        | TransportError::Dialer(_)
        | TransportError::Closed => &types.connection_failure,
    };
    let py_err = PyErr::from_type(ty.bind(py).clone(), (err.to_string(),));
    let val = py_err.value(py);
    if let Err(e) = val
        .setattr("sqlstate", err.sqlstate())
        .and_then(|()| val.setattr("is_transient", err.is_transient()))
    {
        return e;
    }
    py_err
}

/// Convert a [`TransportError`] into a typed Python `DatabaseError` subclass.
pub fn transport_error_to_py(py: Python<'_>, err: &TransportError) -> PyErr {
    match db_exception_types(py) {
        Ok(types) => transport_error_with(py, types, err),
        Err(e) => e,
    }
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

/// Register the `DatabaseError` and `GoogleCloudError` hierarchies on `vertebra._vertebra`.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    let py = m.py();
    let all = all_exception_types(py)?;
    m.add("DatabaseError", all.db.database.bind(py))?;
    m.add("InterfaceError", all.db.interface.bind(py))?;
    m.add("DataError", all.db.data.bind(py))?;
    m.add("NotFoundError", all.db.not_found.bind(py))?;
    m.add("OperationalError", all.db.operational.bind(py))?;
    m.add("ConnectionFailure", all.db.connection_failure.bind(py))?;
    m.add("PoolExhaustedError", all.db.pool_exhausted.bind(py))?;
    m.add("QueryCancelledError", all.db.query_cancelled.bind(py))?;
    m.add(
        "SerializationFailure",
        all.db.serialization_failure.bind(py),
    )?;
    m.add("IntegrityError", all.db.integrity.bind(py))?;
    m.add("InternalError", all.db.internal.bind(py))?;
    m.add("ProgrammingError", all.db.programming.bind(py))?;
    m.add("NotSupportedError", all.db.not_supported.bind(py))?;

    m.add("GoogleCloudError", all.gcp.google_cloud.bind(py))?;
    m.add("GoogleCloudAuthError", all.gcp.auth.bind(py))?;
    m.add("SpannerError", all.gcp.spanner.bind(py))?;
    m.add("SpannerAbortedError", all.gcp.spanner_aborted.bind(py))?;
    m.add("BigQueryError", all.gcp.bigquery.bind(py))?;
    m.add("BigQueryRowError", all.gcp.bigquery_row.bind(py))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use google_cloud_gax::error::rpc::{Code, Status, StatusDetails};
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::{PyTuple, PyType};
    use vertebra_db::bigquery::{BigQueryError as BigQueryClientError, BigQueryRowError};
    use vertebra_db::connection::TransportError;
    use vertebra_db::spanner::SpannerError as SpannerClientError;

    use super::{
        AllExceptionTypes, bigquery_error_with, build_all_exception_types, db_error_with,
        new_exception_class, spanner_error_with, transport_error_with,
    };

    fn stand_in_types(py: Python<'_>) -> (Bound<'_, PyType>, AllExceptionTypes) {
        let exc_base = py.get_type::<pyo3::exceptions::PyException>();
        let base = new_exception_class(
            py,
            "VertebraError",
            &PyTuple::new(py, [&exc_base]).expect("tuple"),
            "Stand-in VertebraError",
            &[],
        )
        .expect("base")
        .into_bound(py);
        let all = build_all_exception_types(py, &base).expect("types");
        (base, all)
    }

    #[test]
    fn database_and_gcp_multi_inheritance_hierarchy_and_defaults() {
        Python::attach(|py| {
            let (base, all) = stand_in_types(py);
            let db = all.db.database.bind(py);
            let data = all.db.data.bind(py);
            let not_found = all.db.not_found.bind(py);
            let ser_fail = all.db.serialization_failure.bind(py);
            let gc = all.gcp.google_cloud.bind(py);

            assert!(db.is_subclass(&base).unwrap());
            assert!(not_found.is_subclass(data).unwrap());
            assert!(not_found.is_subclass(db).unwrap());
            assert!(
                not_found
                    .is_subclass(&py.get_type::<PyValueError>())
                    .unwrap()
            );

            assert!(all.gcp.spanner.bind(py).is_subclass(gc).unwrap());
            assert!(all.gcp.spanner.bind(py).is_subclass(db).unwrap());
            assert!(
                all.gcp
                    .spanner_aborted
                    .bind(py)
                    .is_subclass(all.gcp.spanner.bind(py))
                    .unwrap()
            );
            assert!(
                all.gcp
                    .spanner_aborted
                    .bind(py)
                    .is_subclass(ser_fail)
                    .unwrap()
            );
            assert_eq!(
                all.gcp
                    .spanner_aborted
                    .bind(py)
                    .getattr("sqlstate")
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("40001")
            );
            assert!(
                all.gcp
                    .spanner_aborted
                    .bind(py)
                    .getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );

            assert!(all.gcp.bigquery.bind(py).is_subclass(gc).unwrap());
            assert!(all.gcp.bigquery.bind(py).is_subclass(db).unwrap());
            assert!(
                all.gcp
                    .bigquery_row
                    .bind(py)
                    .is_subclass(all.gcp.bigquery.bind(py))
                    .unwrap()
            );
            assert!(all.gcp.bigquery_row.bind(py).is_subclass(data).unwrap());

            assert!(
                all.db
                    .connection_failure
                    .bind(py)
                    .getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
            assert!(
                all.db
                    .pool_exhausted
                    .bind(py)
                    .getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
            assert_eq!(
                all.db
                    .query_cancelled
                    .bind(py)
                    .getattr("sqlstate")
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("57014")
            );
            assert!(
                !all.db
                    .query_cancelled
                    .bind(py)
                    .getattr("is_transient")
                    .unwrap()
                    .extract::<bool>()
                    .unwrap()
            );
        });
    }

    #[test]
    fn db_and_transport_error_mappers_select_expected_subclasses() {
        Python::attach(|py| {
            let (_base, all) = stand_in_types(py);
            let dup = db_error_with(
                py,
                &all.db,
                Some("23505"),
                Some(23505),
                None,
                "duplicate key".into(),
            );
            assert!(dup.is_instance(py, all.db.integrity.bind(py)));
            assert_eq!(
                dup.value(py)
                    .getattr("vendor_code")
                    .unwrap()
                    .extract::<Option<i64>>()
                    .unwrap(),
                Some(23505)
            );

            let cancelled = transport_error_with(py, &all.db, &TransportError::Cancelled);
            assert!(cancelled.is_instance(py, all.db.query_cancelled.bind(py)));
            assert_eq!(
                cancelled
                    .value(py)
                    .getattr("sqlstate")
                    .unwrap()
                    .extract::<Option<String>>()
                    .unwrap()
                    .as_deref(),
                Some("57014")
            );
        });
    }

    #[test]
    fn aborted_status_maps_to_spanner_aborted_error() {
        Python::attach(|py| {
            let (_base, all) = stand_in_types(py);
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
                &all.gcp,
                SpannerClientError::Client(google_cloud_gax::error::Error::service(status)),
            );
            assert!(err.is_instance(py, all.gcp.spanner_aborted.bind(py)));
            assert!(err.is_instance(py, all.db.serialization_failure.bind(py)));
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
            let (_base, all) = stand_in_types(py);
            let err = bigquery_error_with(
                py,
                &all.gcp,
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
            let (_base, all) = stand_in_types(py);
            let err = bigquery_error_with(
                py,
                &all.gcp,
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
            let (_base, all) = stand_in_types(py);
            let err = bigquery_error_with(
                py,
                &all.gcp,
                BigQueryClientError::RowErrors(vec![BigQueryRowError {
                    index: 0,
                    code: 1,
                    message: "bad field".into(),
                }]),
            );
            assert!(err.is_instance(py, all.gcp.bigquery_row.bind(py)));
            assert!(err.is_instance(py, all.db.data.bind(py)));
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
            let (_base, all) = stand_in_types(py);
            let err = spanner_error_with(
                py,
                &all.gcp,
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
