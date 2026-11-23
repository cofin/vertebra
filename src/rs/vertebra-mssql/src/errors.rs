//! Native SQL Server (TDS) error classification and Python `DatabaseError` hierarchy mapping.
//!
//! Maps TDS `ERROR` tokens (`tiberius::error::TokenError`) and transport failures onto
//! Vertebra's canonical Python exception hierarchy (`DatabaseError`, `InterfaceError`,
//! `DataError`, `NotFoundError`, `OperationalError`, `ConnectionFailure`, `PoolExhaustedError`,
//! `QueryCancelledError`, `SerializationFailure`, `IntegrityError`, `InternalError`,
//! `ProgrammingError`, `NotSupportedError`) with `.sqlstate`, `.vendor_code`, and `.is_transient`.

use pyo3::prelude::*;
use pyo3::types::PyType;
use tiberius::error::{Error as TiberiusError, IoErrorKind, TokenError};
use vertebra_core::broker::BrokerError;
use vertebra_db::TransportError;
use vertebra_db::py_errors::{DbPyExceptionTypes, instantiate_db_exc, resolve_db_exceptions};

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
        Self::new(
            MssqlErrorKind::Interface,
            message,
            Some("08000"),
            None,
            false,
        )
    }

    /// Construct a programming/syntax error (`ProgrammingError`).
    pub fn programming(message: impl Into<String>) -> Self {
        Self::new(
            MssqlErrorKind::Programming,
            message,
            Some("42000"),
            None,
            false,
        )
    }

    /// Construct a data conversion/range error (`DataError`).
    pub fn data(message: impl Into<String>) -> Self {
        Self::new(MssqlErrorKind::Data, message, Some("22000"), None, false)
    }

    /// Construct a `NotFoundError` (`NO_DATA_FOUND` / `02000`).
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(
            MssqlErrorKind::NotFound,
            message,
            Some("02000"),
            None,
            false,
        )
    }

    /// Construct a query timeout or cancellation error (`QueryCancelledError`, SQLSTATE `57014`).
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(
            MssqlErrorKind::QueryCancelled,
            message,
            Some("57014"),
            None,
            true,
        )
    }

    /// Construct a pool exhaustion error (`PoolExhaustedError`, SQLSTATE `53300`).
    pub fn pool_exhausted(message: impl Into<String>) -> Self {
        Self::new(
            MssqlErrorKind::PoolExhausted,
            message,
            Some("53300"),
            None,
            true,
        )
    }

    /// Classify a SQL Server error number (`sys.messages.message_id`) and severity class.
    pub fn from_tds_code(code: u32, class: u8, state: u8, message: impl Into<String>) -> Self {
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

fn select_exception_type(types: &DbPyExceptionTypes, kind: MssqlErrorKind) -> &Py<PyType> {
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
    let types = match resolve_db_exceptions(py) {
        Ok(t) => t,
        Err(py_err) => return py_err,
    };
    let ty = select_exception_type(types, err.kind);
    let py_err = instantiate_db_exc(
        py,
        ty,
        &err.message,
        err.sqlstate.as_deref(),
        err.vendor_code,
        err.is_transient,
    );
    let val = py_err.value(py);
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
            let types = resolve_db_exceptions(py).expect("exception types");

            for (code, class, expected_kind, expected_sqlstate, expected_transient) in [
                (2601_u32, 14_u8, MssqlErrorKind::Integrity, "23505", false),
                (2627, 14, MssqlErrorKind::Integrity, "23505", false),
                (547, 16, MssqlErrorKind::Integrity, "23503", false),
                (515, 16, MssqlErrorKind::Integrity, "23502", false),
                (
                    1205,
                    13,
                    MssqlErrorKind::SerializationFailure,
                    "40001",
                    true,
                ),
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
                    val.getattr("vendor_code")
                        .unwrap()
                        .extract::<Option<i64>>()
                        .unwrap(),
                    Some(i64::from(code))
                );
                assert_eq!(
                    val.getattr("sqlstate")
                        .unwrap()
                        .extract::<Option<String>>()
                        .unwrap()
                        .as_deref(),
                    Some(expected_sqlstate)
                );
                assert_eq!(
                    val.getattr("is_transient")
                        .unwrap()
                        .extract::<bool>()
                        .unwrap(),
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
