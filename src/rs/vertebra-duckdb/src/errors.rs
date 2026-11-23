//! Error taxonomy for `vertebra-duckdb`, mapping DuckDB engine failures into
//! `vertebra.db.DatabaseError` subclasses resolved from `vertebra._vertebra`.

use pyo3::prelude::*;
use thiserror::Error;
use vertebra_db::py_errors::{instantiate_db_exc, resolve_db_exceptions};

/// Classified error emitted by the native DuckDB client.
#[derive(Debug, Error)]
pub enum DuckDBClientError {
    /// Invalid configuration or client argument.
    #[error("{0}")]
    InvalidInput(String),

    /// Connection or session setup error.
    #[error("{0}")]
    Operational(String),

    /// SQL syntax, unknown table/column, or invalid parameter binding error.
    #[error("{0}")]
    Programming(String),

    /// Constraint violation (primary key, unique, foreign key, not null, check).
    #[error("{0}")]
    Integrity(String),

    /// Numeric overflow, division by zero, type conversion, or domain error.
    #[error("{0}")]
    Data(String),

    /// Expected row or resource not found.
    #[error("{0}")]
    NotFound(String),

    /// Transaction conflict or invalid transaction state error.
    #[error("{0}")]
    Transaction(String),

    /// Query execution interrupted or cancelled.
    #[error("{0}")]
    QueryTimeout(String),

    /// Pool checkout or resource exhaustion error.
    #[error("{0}")]
    PoolExhausted(String),

    /// Feature not supported by DuckDB or driver configuration.
    #[error("{0}")]
    NotSupported(String),

    /// Shared-memory Arrow IPC error.
    #[error("shared-memory Arrow IPC error: {0}")]
    ShmArrow(String),
}

impl DuckDBClientError {
    /// Return the canonical SQLSTATE code associated with this error variant.
    #[must_use]
    pub const fn sqlstate(&self) -> Option<&'static str> {
        match self {
            Self::InvalidInput(_) => Some("HY000"),
            Self::Operational(_) | Self::ShmArrow(_) => Some("08000"),
            Self::Programming(_) => Some("42000"),
            Self::Integrity(_) => Some("23000"),
            Self::Data(_) => Some("22000"),
            Self::NotFound(_) => Some("02000"),
            Self::Transaction(_) => Some("40001"),
            Self::QueryTimeout(_) => Some("57014"),
            Self::PoolExhausted(_) => Some("53300"),
            Self::NotSupported(_) => Some("0A000"),
        }
    }

    /// Return the vendor-specific error code if available.
    #[must_use]
    pub const fn vendor_code(&self) -> Option<i32> {
        None
    }

    /// Return whether the error represents a transient condition eligible for retry.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        matches!(
            self,
            Self::PoolExhausted(_) | Self::QueryTimeout(_) | Self::Transaction(_)
        )
    }

    /// Classify a [`duckdb::Error`] into [`DuckDBClientError`] by inspecting its variant and message.
    #[must_use]
    pub fn from_duckdb(err: duckdb::Error) -> Self {
        match err {
            duckdb::Error::DuckDBFailure(raw, msg_opt) => {
                let text = msg_opt.unwrap_or_else(|| raw.to_string());
                Self::classify_message(&text)
            }
            duckdb::Error::FromSqlConversionFailure(_, _, inner) => {
                Self::Data(format!("SQL value conversion failure: {inner}"))
            }
            duckdb::Error::IntegralValueOutOfRange(col, val) => {
                Self::Data(format!("Integral value {val} out of range at column {col}"))
            }
            duckdb::Error::Utf8Error(e) => Self::Data(format!("Invalid UTF-8 value: {e}")),
            duckdb::Error::NulError(e) => {
                Self::InvalidInput(format!("SQL string contains interior NUL byte: {e}"))
            }
            duckdb::Error::InvalidParameterName(name) => {
                Self::Programming(format!("Invalid or unknown SQL parameter name: {name}"))
            }
            duckdb::Error::InvalidPath(path) => {
                Self::Operational(format!("Invalid database file path: {}", path.display()))
            }
            duckdb::Error::ExecuteReturnedResults => Self::Programming(
                "Execute called on a query that returns result rows; use fetch/select instead"
                    .to_string(),
            ),
            duckdb::Error::QueryReturnedNoRows => {
                Self::NotFound("Query returned no rows".to_string())
            }
            duckdb::Error::InvalidColumnIndex(idx) => {
                Self::Programming(format!("Invalid column index: {idx}"))
            }
            duckdb::Error::InvalidColumnName(name) => {
                Self::Programming(format!("Invalid column name: {name}"))
            }
            duckdb::Error::InvalidColumnType(idx, name, ty) => Self::Data(format!(
                "Invalid column type {ty} for column {idx} ({name})"
            )),
            duckdb::Error::ArrowTypeToDuckdbType(col, dt) => Self::Data(format!(
                "Unsupported Arrow data type {dt:?} for column {col}"
            )),
            duckdb::Error::StatementChangedRows(rows) => {
                Self::Programming(format!("Statement unexpectedly modified {rows} rows"))
            }
            duckdb::Error::ToSqlConversionFailure(inner) => {
                Self::Data(format!("Parameter conversion failure: {inner}"))
            }
            duckdb::Error::InvalidQuery => Self::Programming("Invalid SQL query".to_string()),
            duckdb::Error::MultipleStatement => Self::Programming(
                "Multiple statements are not permitted in a single prepared query".to_string(),
            ),
            duckdb::Error::InvalidParameterCount(passed, expected) => Self::Programming(format!(
                "Invalid parameter count: passed {passed}, expected {expected}"
            )),
            duckdb::Error::AppendError => {
                Self::Operational("DuckDB Appender operation failed".to_string())
            }
            _ => Self::classify_message(&err.to_string()),
        }
    }

    /// Classify a raw DuckDB diagnostic message string into the canonical [`DuckDBClientError`] variant.
    #[must_use]
    pub fn classify_message(message: &str) -> Self {
        let lower = message.to_ascii_lowercase();
        if lower.contains("interrupt") || lower.contains("canceled") || lower.contains("cancelled")
        {
            return Self::QueryTimeout(message.to_string());
        }
        if lower.contains("constraint error")
            || lower.contains("duplicate key")
            || lower.contains("violates primary key")
            || lower.contains("violates unique constraint")
            || lower.contains("violates foreign key")
            || lower.contains("not null constraint")
            || lower.contains("check constraint")
        {
            return Self::Integrity(message.to_string());
        }
        if lower.contains("transactioncontext error")
            || lower.contains("transaction conflict")
            || lower.contains("write-write conflict")
            || lower.contains("cannot start a transaction within a transaction")
            || lower.contains("cannot commit - no transaction is active")
            || lower.contains("cannot rollback - no transaction is active")
            || lower.contains("current transaction is aborted")
        {
            return Self::Transaction(message.to_string());
        }
        if lower.contains("conversion error")
            || lower.contains("out of range error")
            || lower.contains("divide by zero")
            || lower.contains("division by zero")
            || lower.contains("numeric value out of range")
            || lower.contains("invalid unicode")
            || lower.contains("invalid input syntax")
        {
            return Self::Data(message.to_string());
        }
        if lower.contains("parser error")
            || lower.contains("syntax error")
            || lower.contains("catalog error")
            || lower.contains("binder error")
            || lower.contains("table with name")
            || lower.contains("does not exist")
            || lower.contains("referenced column")
            || lower.contains("not found in from clause")
            || lower.contains("read-only mode")
            || lower.contains("cannot execute statement of type")
        {
            return Self::Programming(message.to_string());
        }
        if lower.contains("not implemented error") || lower.contains("unsupported") {
            return Self::NotSupported(message.to_string());
        }
        Self::Operational(message.to_string())
    }

    /// Convert this error into a Python `DatabaseError` subclass instance.
    #[must_use]
    pub fn into_py_err(self, py: Python<'_>) -> PyErr {
        let msg = self.to_string();
        let sqlstate = self.sqlstate();
        let vendor_code = self.vendor_code().map(i64::from);
        let is_transient = self.is_transient();
        let Ok(types) = resolve_db_exceptions(py) else {
            return pyo3::exceptions::PyRuntimeError::new_err(msg);
        };
        let ty = match self {
            Self::InvalidInput(_) => &types.interface,
            Self::Programming(_) => &types.programming,
            Self::Operational(_) | Self::ShmArrow(_) => &types.operational,
            Self::Integrity(_) => &types.integrity,
            Self::Data(_) => &types.data,
            Self::NotFound(_) => &types.not_found,
            Self::Transaction(_) => &types.serialization_failure,
            Self::QueryTimeout(_) => &types.query_cancelled,
            Self::PoolExhausted(_) => &types.pool_exhausted,
            Self::NotSupported(_) => &types.not_supported,
        };
        instantiate_db_exc(py, ty, &msg, sqlstate, vendor_code, is_transient)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_message_taxonomy() {
        assert!(matches!(
            DuckDBClientError::classify_message("Interrupt Error: Interrupted!"),
            DuckDBClientError::QueryTimeout(_)
        ));
        assert!(matches!(
            DuckDBClientError::classify_message(
                "Constraint Error: Duplicate key \"id: 1\" violates primary key constraint"
            ),
            DuckDBClientError::Integrity(_)
        ));
        assert!(matches!(
            DuckDBClientError::classify_message("Parser Error: syntax error at or near \"SELEC\""),
            DuckDBClientError::Programming(_)
        ));
        assert!(matches!(
            DuckDBClientError::classify_message(
                "Catalog Error: Table with name missing_tbl does not exist!"
            ),
            DuckDBClientError::Programming(_)
        ));
        assert!(matches!(
            DuckDBClientError::classify_message(
                "Conversion Error: Could not convert string 'abc' to INT32"
            ),
            DuckDBClientError::Data(_)
        ));
        assert!(matches!(
            DuckDBClientError::classify_message(
                "TransactionContext Error: cannot commit - no transaction is active"
            ),
            DuckDBClientError::Transaction(_)
        ));
        assert!(matches!(
            DuckDBClientError::classify_message("IO Error: Cannot open file \"/nonexistent/db\""),
            DuckDBClientError::Operational(_)
        ));
    }
}
