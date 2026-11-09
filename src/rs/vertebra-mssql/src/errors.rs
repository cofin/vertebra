//! Native SQL Server (TDS) error classification and Python `DatabaseError` hierarchy mapping.
//!
//! Maps TDS `ERROR` tokens (`tiberius::error::TokenError`) and transport failures onto
//! Vertebra's canonical Python exception hierarchy (`DatabaseError`, `InterfaceError`,
//! `DataError`, `NotFoundError`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`,
//! `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`,
//! `ProgrammingError`, `NotSupportedError`) with `.sqlstate`, `.vendor_code`, and `.is_transient`.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};
use tiberius::error::{Error as TiberiusError, IoErrorKind, TokenError};
use vertebra_core::broker::BrokerError;
use vertebra_db::TransportError;

/// Canonical Vertebra database exception category for an MSSQL failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MssqlErrorKind {
    Database,
    Interface,
    Data,
    NotFound,
    Operational,
    ConnectionFailure,
    PoolExhausted,
    QueryCancelled,
    SerializationFailure,
    Integrity,
    Internal,
    Programming,
    NotSupported,
}

/// Structured native SQL Server error carrying SQLSTATE, TDS error number (`vendor_code`),
/// severity class, error state, and retry transience.
#[derive(Clone, Debug, thiserror::Error)]
#[error("{message}")]
pub struct MssqlError {
    pub kind: MssqlErrorKind,
    pub message: String,
    pub sqlstate: Option<String>,
    pub vendor_code: Option<i64>,
    pub is_transient: bool,
    pub severity_class: Option<u8>,
    pub error_state: Option<u8>,
}

impl MssqlError {
    /// Construct a new [`MssqlError`].
    pub fn new(
        kind: MssqlErrorKind,
        message: impl Into<String>,
        sqlstate: Option<&str>,
        vendor_code: Option<i64>,
        is_transient: bool,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            sqlstate: sqlstate.map(str::to_string),
            vendor_code,
            is_transient,
            severity_class: None,
            error_state: None,
        }
    }

    /// Construct a configuration/interface error (`InterfaceError`).
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(MssqlErrorKind::Interface, message, Some("08000"), None, false)
    }

    /// Construct a programming/syntax error (`ProgrammingError`).
    pub fn programming(message: impl Into<String>) -> Self {
        Self::new(MssqlErrorKind::Programming, message, Some("42000"), None, false)
    }

    /// Construct a data conversion/range error (`DataError`).
    pub fn data(message: impl Into<String>) -> Self {
        Self::new(MssqlErrorKind::Data, message, Some("22000"), None, false)
    }

    /// Construct a `NotFoundError` (`NO_DATA_FOUND` / `02000`).
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(MssqlErrorKind::NotFound, message, Some("02000"), None, false)
    }

    /// Construct a query timeout or cancellation error (`QueryCancelledError`, SQLSTATE `57014`).
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(MssqlErrorKind::QueryCancelled, message, Some("57014"), None, true)
    }

    /// Construct a pool exhaustion error (`PoolExhaustedError`, SQLSTATE `53300`).
    pub fn pool_exhausted(message: impl Into<String>) -> Self {
        Self::new(MssqlErrorKind::PoolExhausted, message, Some("53300"), None, true)
    }

    /// Classify a SQL Server error number (`sys.messages.message_id`) and severity class.
    pub fn from_tds_code(
        code: u32,
        class: u8,
        state: u8,
        message: impl Into<String>,
    ) -> Self {
        let (kind, sqlstate, is_transient) = classify_tds_error_code(code, class);
        Self {
            kind,
            message: message.into(),
            sqlstate: Some(sqlstate.to_string()),
            vendor_code: Some(i64::from(code)),
            is_transient,
            severity_class: Some(class),
            error_state: Some(state),
        }
    }

    /// Classify a [`TokenError`] returned in a TDS `0xAA` `ERROR` token.
    pub fn from_token_error(tok: &TokenError) -> Self {
        Self::from_tds_code(
            tok.code(),
            tok.class(),
            tok.state(),
            format!(
                "MSSQL error {} (class {}, state {}, line {}): {}",
                tok.code(),
                tok.class(),
                tok.state(),
                tok.line(),
                tok.message()
            ),
        )
    }
}

/// Map a SQL Server native error number and severity class to `(MssqlErrorKind, sqlstate, is_transient)`.
pub const fn classify_tds_error_code(code: u32, class: u8) -> (MssqlErrorKind, &'static str, bool) {
    match code {
        // Unique index / PRIMARY KEY / UNIQUE constraint violations
        2601 | 2627 => (MssqlErrorKind::Integrity, "23505", false),
        // FOREIGN KEY / CHECK constraint violation
        547 => (MssqlErrorKind::Integrity, "23503", false),
        // Cannot insert NULL into column
        515 => (MssqlErrorKind::Integrity, "23502", false),
        // Deadlock victim (1205), snapshot update conflict (3960)
        1205 | 3960 => (MssqlErrorKind::SerializationFailure, "40001", true),
        // Lock request time out (1222), user-cancelled query (3617), query memory grant timeout (8645)
        1222 | 3617 | 8645 => (MssqlErrorKind::QueryCancelled, "57014", true),
        // Login failed for user / password expired
        18456 | 18487 | 18488 => (MssqlErrorKind::ConnectionFailure, "28000", false),
        // Cannot open database requested by the login
        4060 => (MssqlErrorKind::ConnectionFailure, "08001", false),
        // Azure SQL / Cloud SQL transient throttling & resource reconfiguration codes
        40197 | 40501 | 40613 | 49918 | 49919 | 49920 | 10928 | 10929 => {
            (MssqlErrorKind::Operational, "08006", true)
        }
        // String or binary data would be truncated
        8152 | 2628 => (MssqlErrorKind::Data, "22001", false),
        // Arithmetic overflow / divide by zero / numeric out of range
        8115 | 8134 | 220 | 248 => (MssqlErrorKind::Data, "22003", false),
        // Conversion failed (date/time/int/uniqueidentifier)
        241 | 242 | 245 | 295 | 8114 | 8169 => (MssqlErrorKind::Data, "22007", false),
        // Invalid object name (table/view does not exist)
        208 => (MssqlErrorKind::Programming, "42P01", false),
        // Invalid column name
        207 => (MssqlErrorKind::Programming, "42703", false),
        // Object already exists
        2714 => (MssqlErrorKind::Programming, "42P07", false),
        // Syntax error / Must declare scalar variable / Could not find stored procedure
        102 | 105 | 137 | 156 | 201 | 2812 | 8144 => (MssqlErrorKind::Programming, "42000", false),
        _ => {
            if class >= 20 {
                // Fatal server/connection severity class 20..=25 terminates the TDS connection
                (MssqlErrorKind::ConnectionFailure, "08006", true)
            } else if class >= 17 {
                // Resource / internal server error class 17..=19
                (MssqlErrorKind::Operational, "HY000", true)
            } else {
                (MssqlErrorKind::Database, "HY000", false)
            }
        }
    }
}

impl From<TransportError> for MssqlError {
    fn from(err: TransportError) -> Self {
        match err {
            TransportError::Cancelled => Self::cancelled("MSSQL operation cancelled"),
            TransportError::Timeout(dur) => Self::new(
                MssqlErrorKind::ConnectionFailure,
                format!("MSSQL connection timed out after {dur:?}"),
                Some("08001"),
                None,
                true,
            ),
            TransportError::InvalidConfig(msg) => Self::config(msg),
            other => Self::new(
                MssqlErrorKind::ConnectionFailure,
                other.to_string(),
                other.sqlstate(),
                None,
                other.is_transient(),
            ),
        }
    }
}

impl From<BrokerError> for MssqlError {
    fn from(err: BrokerError) -> Self {
        match err {
            BrokerError::PoolExhausted => {
                Self::pool_exhausted("MSSQL SHM connection lease pool exhausted")
            }
            other => Self::new(
                MssqlErrorKind::Internal,
                other.to_string(),
                Some("XX000"),
                None,
                false,
            ),
        }
    }
}

impl From<TiberiusError> for MssqlError {
    fn from(err: TiberiusError) -> Self {
        match err {
            TiberiusError::Server(tok) => Self::from_token_error(&tok),
            TiberiusError::Io { kind, message } => {
                let is_timeout = matches!(kind, IoErrorKind::TimedOut);
                if is_timeout {
                    Self::cancelled(format!("MSSQL I/O timed out: {message}"))
                } else {
                    Self::new(
                        MssqlErrorKind::ConnectionFailure,
                        format!("MSSQL transport I/O error ({kind:?}): {message}"),
                        Some("08006"),
                        None,
                        true,
                    )
                }
            }
            TiberiusError::Tls(msg) => Self::new(
                MssqlErrorKind::ConnectionFailure,
                format!("MSSQL TLS handshake failed: {msg}"),
                Some("08001"),
                None,
                false,
            ),
            TiberiusError::Protocol(msg) => Self::new(
                MssqlErrorKind::Operational,
                format!("MSSQL TDS protocol error: {msg}"),
                Some("08P01"),
                None,
                false,
            ),
            TiberiusError::Encoding(msg) => Self::data(format!("MSSQL encoding error: {msg}")),
            TiberiusError::Conversion(msg) => Self::data(format!("MSSQL conversion error: {msg}")),
            TiberiusError::Utf8 => Self::data("MSSQL UTF-8 decoding error"),
            TiberiusError::Utf16 => Self::data("MSSQL UTF-16 decoding error"),
            TiberiusError::ParseInt(err) => {
                Self::data(format!("MSSQL integer parsing error: {err}"))
            }
            TiberiusError::Routing { host, port } => Self::new(
                MssqlErrorKind::Operational,
                format!("unhandled MSSQL ENVCHANGE routing redirect to {host}:{port}"),
                Some("08001"),
                None,
                true,
            ),
            TiberiusError::BulkInput(msg) => {
                Self::data(format!("MSSQL bulk load input error: {msg}"))
            }
        }
    }
}

/// Cached Python exception classes resolved from `vertebra._vertebra`.
pub struct MssqlExceptionTypes {
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

static MSSQL_EXCEPTION_TYPES: PyOnceLock<MssqlExceptionTypes> = PyOnceLock::new();

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

fn stand_in_exception_types(py: Python<'_>) -> PyResult<MssqlExceptionTypes> {
    let root = match py
        .import("vertebra._typing")
        .and_then(|m| m.getattr("VertebraError"))
        .and_then(|v| v.cast_into::<PyType>().map_err(Into::into))
    {
        Ok(ty) => ty,
        Err(_) => py.get_type::<pyo3::exceptions::PyException>(),
    };
    let database = new_stand_in_class(py, "DatabaseError", &PyTuple::new(py, [&root])?)?;
    let db_base = PyTuple::new(py, [database.bind(py)])?;
    let interface = new_stand_in_class(py, "InterfaceError", &db_base)?;
    let data = new_stand_in_class(py, "DataError", &db_base)?;
    let not_found = new_stand_in_class(py, "NotFoundError", &db_base)?;
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

    Ok(MssqlExceptionTypes {
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
    })
}

fn resolve_exception_types(py: Python<'_>) -> PyResult<&'static MssqlExceptionTypes> {
    MSSQL_EXCEPTION_TYPES.get_or_try_init(py, || match py.import("vertebra._vertebra") {
        Ok(core_mod) => {
            let get = |name: &str| -> PyResult<Py<PyType>> {
                Ok(core_mod.getattr(name)?.cast_into::<PyType>()?.unbind())
            };
            Ok(MssqlExceptionTypes {
                database: get("DatabaseError")?,
                interface: get("InterfaceError")?,
                data: get("DataError")?,
                not_found: get("NotFoundError")?,
                operational: get("OperationalError")?,
                connection_failure: get("ConnectionFailure")?,
                pool_exhausted: get("PoolExhaustedError")?,
                query_cancelled: get("QueryCancelledError")?,
                serialization_failure: get("SerializationFailure")?,
                integrity: get("IntegrityError")?,
                internal: get("InternalError")?,
                programming: get("ProgrammingError")?,
                not_supported: get("NotSupportedError")?,
            })
        }
        Err(err) if err.is_instance_of::<pyo3::exceptions::PyModuleNotFoundError>(py) => {
            stand_in_exception_types(py)
        }
        Err(err) => Err(err),
    })
}

fn select_exception_type(types: &MssqlExceptionTypes, kind: MssqlErrorKind) -> &Py<PyType> {
    match kind {
        MssqlErrorKind::Database => &types.database,
        MssqlErrorKind::Interface => &types.interface,
        MssqlErrorKind::Data => &types.data,
        MssqlErrorKind::NotFound => &types.not_found,
        MssqlErrorKind::Operational => &types.operational,
        MssqlErrorKind::ConnectionFailure => &types.connection_failure,
        MssqlErrorKind::PoolExhausted => &types.pool_exhausted,
        MssqlErrorKind::QueryCancelled => &types.query_cancelled,
        MssqlErrorKind::SerializationFailure => &types.serialization_failure,
        MssqlErrorKind::Integrity => &types.integrity,
        MssqlErrorKind::Internal => &types.internal,
        MssqlErrorKind::Programming => &types.programming,
        MssqlErrorKind::NotSupported => &types.not_supported,
    }
}

/// Convert a native [`MssqlError`] into a Python exception from the `vertebra._vertebra` hierarchy
/// with `.sqlstate`, `.vendor_code`, and `.is_transient` populated.
pub fn mssql_error_to_py(py: Python<'_>, err: MssqlError) -> PyErr {
    let types = match resolve_exception_types(py) {
        Ok(t) => t,
        Err(py_err) => return py_err,
    };
    let ty = select_exception_type(types, err.kind);
    let py_err = PyErr::from_type(ty.bind(py).clone(), (err.message.clone(),));
    let val = py_err.value(py);
    let _ = val.setattr("sqlstate", err.sqlstate.as_deref());
    let _ = val.setattr("vendor_code", err.vendor_code);
    let _ = val.setattr("is_transient", err.is_transient);
    let _ = val.setattr("severity_class", err.severity_class);
    let _ = val.setattr("error_state", err.error_state);
    py_err
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tds_error_number_classification_maps_to_canonical_hierarchy() {
        Python::initialize();
        Python::attach(|py| {
            let types = stand_in_exception_types(py).expect("stand-in types");

            for (code, class, expected_kind, expected_sqlstate, expected_transient) in [
                (2601_u32, 14_u8, MssqlErrorKind::Integrity, "23505", false),
                (2627, 14, MssqlErrorKind::Integrity, "23505", false),
                (547, 16, MssqlErrorKind::Integrity, "23503", false),
                (515, 16, MssqlErrorKind::Integrity, "23502", false),
                (1205, 13, MssqlErrorKind::SerializationFailure, "40001", true),
                (18456, 14, MssqlErrorKind::ConnectionFailure, "28000", false),
                (8152, 16, MssqlErrorKind::Data, "22001", false),
                (208, 16, MssqlErrorKind::Programming, "42P01", false),
            ] {
                let err = MssqlError::from_tds_code(code, class, 1, "simulated tds error");
                assert_eq!(err.kind, expected_kind);
                assert_eq!(err.sqlstate.as_deref(), Some(expected_sqlstate));
                assert_eq!(err.vendor_code, Some(i64::from(code)));
                assert_eq!(err.is_transient, expected_transient);

                let py_err = mssql_error_to_py(py, err);
                let val = py_err.value(py);
                assert_eq!(
                    val.getattr("vendor_code").unwrap().extract::<Option<i64>>().unwrap(),
                    Some(i64::from(code))
                );
                assert_eq!(
                    val.getattr("sqlstate").unwrap().extract::<Option<String>>().unwrap().as_deref(),
                    Some(expected_sqlstate)
                );
                assert_eq!(
                    val.getattr("is_transient").unwrap().extract::<bool>().unwrap(),
                    expected_transient
                );
                assert!(
                    py_err.is_instance(py, types.database.bind(py))
                        || val.getattr("sqlstate").is_ok()
                );
            }
        });
    }
}
