//! Error taxonomy for `vertebra-duckdb`, mapping DuckDB engine failures into
//! `vertebra.db.DatabaseError` subclasses resolved from `vertebra._vertebra`.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};
use thiserror::Error;

/// Cached Python exception types from `vertebra._vertebra`.
pub struct DuckDBExceptionTypes {
    pub database: Py<PyType>,
    pub operational: Py<PyType>,
    pub integrity: Py<PyType>,
    pub programming: Py<PyType>,
    pub data: Py<PyType>,
    pub transaction: Py<PyType>,
    pub query_timeout: Py<PyType>,
    pub pool_exhausted: Py<PyType>,
    pub not_supported: Py<PyType>,
}

static DUCKDB_EXCEPTION_TYPES: PyOnceLock<DuckDBExceptionTypes> = PyOnceLock::new();

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

fn stand_in_exception_types(py: Python<'_>) -> PyResult<DuckDBExceptionTypes> {
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
    let operational = new_stand_in_class(py, "OperationalError", &db_base)?;
    let op_base = PyTuple::new(py, [operational.bind(py)])?;
    let integrity = new_stand_in_class(py, "IntegrityError", &db_base)?;
    let programming = new_stand_in_class(py, "ProgrammingError", &db_base)?;
    let data = new_stand_in_class(py, "DataError", &db_base)?;
    let transaction = new_stand_in_class(py, "TransactionError", &db_base)?;
    let query_timeout = new_stand_in_class(py, "QueryTimeoutError", &op_base)?;
    let pool_exhausted = new_stand_in_class(py, "PoolExhaustedError", &op_base)?;
    let not_supported = new_stand_in_class(py, "NotSupportedError", &db_base)?;
    Ok(DuckDBExceptionTypes {
        database,
        operational,
        integrity,
        programming,
        data,
        transaction,
        query_timeout,
        pool_exhausted,
        not_supported,
    })
}

fn resolve_exception_types(py: Python<'_>) -> PyResult<&'static DuckDBExceptionTypes> {
    DUCKDB_EXCEPTION_TYPES.get_or_try_init(py, || match py.import("vertebra._vertebra") {
        Ok(core_mod) => Ok(DuckDBExceptionTypes {
            database: core_mod
                .getattr("DatabaseError")?
                .cast_into::<PyType>()?
                .unbind(),
            operational: core_mod
                .getattr("OperationalError")?
                .cast_into::<PyType>()?
                .unbind(),
            integrity: core_mod
                .getattr("IntegrityError")?
                .cast_into::<PyType>()?
                .unbind(),
            programming: core_mod
                .getattr("ProgrammingError")?
                .cast_into::<PyType>()?
                .unbind(),
            data: core_mod
                .getattr("DataError")?
                .cast_into::<PyType>()?
                .unbind(),
            transaction: core_mod
                .getattr("TransactionError")?
                .cast_into::<PyType>()?
                .unbind(),
            query_timeout: core_mod
                .getattr("QueryTimeoutError")?
                .cast_into::<PyType>()?
                .unbind(),
            pool_exhausted: core_mod
                .getattr("PoolExhaustedError")?
                .cast_into::<PyType>()?
                .unbind(),
            not_supported: core_mod
                .getattr("NotSupportedError")?
                .cast_into::<PyType>()?
                .unbind(),
        }),
        Err(_) => stand_in_exception_types(py),
    })
}

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
                Self::Programming(format!("SQL string contains interior NUL byte: {e}"))
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
            duckdb::Error::QueryReturnedNoRows => Self::Data("Query returned no rows".to_string()),
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
        let Ok(types) = resolve_exception_types(py) else {
            return pyo3::exceptions::PyRuntimeError::new_err(msg);
        };
        let ty = match self {
            Self::InvalidInput(_) | Self::Programming(_) => &types.programming,
            Self::Operational(_) | Self::ShmArrow(_) => &types.operational,
            Self::Integrity(_) => &types.integrity,
            Self::Data(_) => &types.data,
            Self::Transaction(_) => &types.transaction,
            Self::QueryTimeout(_) => &types.query_timeout,
            Self::PoolExhausted(_) => &types.pool_exhausted,
            Self::NotSupported(_) => &types.not_supported,
        };
        PyErr::from_type(ty.bind(py).clone(), (msg,))
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
