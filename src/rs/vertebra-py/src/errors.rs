//! Typed database and Google Cloud exception hierarchies for `vertebra._vertebra`.
//!
//! Registers the unified `DatabaseError(VertebraError)` PEP 249 / transient hierarchy alongside
//! `GoogleCloudError(VertebraError)` and multi-inheritance subclasses (`NotFoundError`,
//! `SpannerError`, `SpannerAbortedError`, `BigQueryError`, `BigQueryRowError`) using safe
//! `PyType` construction (zero `unsafe` and zero `google-cloud-*` dependencies).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};
use vertebra_db::connection::TransportError;

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
    let serialization_bound = serialization_failure.bind(py);

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
        &PyTuple::new(py, [gc_bound, op_bound])?,
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
        &PyTuple::new(py, [spanner_bound, serialization_bound])?,
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
            ("row_errors", PyTuple::empty(py).into_any()),
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

fn auth_error_with(py: Python<'_>, types: &GcpExceptionTypes, message: String) -> PyErr {
    let err = PyErr::from_type(types.auth.bind(py).clone(), (message.clone(),));
    let val = err.value(py);
    if let Err(e) = val
        .setattr("code", 16i32)
        .and_then(|()| val.setattr("code_name", "UNAUTHENTICATED"))
        .and_then(|()| val.setattr("message", message))
        .and_then(|()| val.setattr("http_status", Option::<u16>::None))
        .and_then(|()| val.setattr("retry_delay_ms", Option::<u64>::None))
        .and_then(|()| val.setattr("reason", Option::<String>::None))
    {
        return e;
    }
    err
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
    use pyo3::exceptions::PyValueError;
    use pyo3::prelude::*;
    use pyo3::types::{PyTuple, PyType};
    use vertebra_db::connection::TransportError;

    use super::{
        AllExceptionTypes, auth_error_with, build_all_exception_types, db_error_with,
        new_exception_class, transport_error_with,
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

            let auth_err = auth_error_with(py, &all.gcp, "bad token".into());
            assert!(auth_err.is_instance(py, all.gcp.auth.bind(py)));
            assert!(auth_err.is_instance(py, all.db.operational.bind(py)));
            assert!(auth_err.is_instance(py, db));
            assert_eq!(
                auth_err
                    .value(py)
                    .getattr("code")
                    .unwrap()
                    .extract::<i32>()
                    .unwrap(),
                16
            );
            assert!(
                all.gcp
                    .bigquery_row
                    .bind(py)
                    .getattr("row_errors")
                    .unwrap()
                    .is_instance_of::<PyTuple>()
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
}
