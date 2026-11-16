//! Error mapping for `vertebra-sqlite`, translating rusqlite and SQLite FFI errors
//! into `vertebra._vertebra` exception types with canonical SQLSTATE and transience metadata.

use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};
use thiserror::Error;

/// Cached Python exception types resolved from `vertebra._vertebra`.
pub struct SqliteExceptionTypes {
    pub database: Py<PyType>,
    pub interface: Py<PyType>,
    pub operational: Py<PyType>,
    pub integrity: Py<PyType>,
    pub programming: Py<PyType>,
    pub data: Py<PyType>,
    pub not_found: Py<PyType>,
    pub query_cancelled: Py<PyType>,
}

static SQLITE_EXCEPTION_TYPES: PyOnceLock<SqliteExceptionTypes> = PyOnceLock::new();

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

fn stand_in_exception_types(py: Python<'_>) -> PyResult<SqliteExceptionTypes> {
    let root = match py
        .import("vertebra.typing")
        .and_then(|m| m.getattr("VertebraError"))
        .and_then(|v| v.cast_into::<PyType>().map_err(Into::into))
    {
        Ok(ty) => ty,
        Err(_) => py.get_type::<pyo3::exceptions::PyException>(),
    };
    let database = new_stand_in_class(py, "DatabaseError", &PyTuple::new(py, [&root])?)?;
    let db_base = PyTuple::new(py, [database.bind(py)])?;
    let interface = new_stand_in_class(py, "InterfaceError", &db_base)?;
    let operational = new_stand_in_class(py, "OperationalError", &db_base)?;
    let op_base = PyTuple::new(py, [operational.bind(py)])?;
    let integrity = new_stand_in_class(py, "IntegrityError", &db_base)?;
    let programming = new_stand_in_class(py, "ProgrammingError", &db_base)?;
    let data = new_stand_in_class(py, "DataError", &db_base)?;
    let data_base = PyTuple::new(py, [data.bind(py)])?;
    let not_found = new_stand_in_class(py, "NotFoundError", &data_base)?;
    let query_cancelled = new_stand_in_class(py, "QueryCancelledError", &op_base)?;

    Ok(SqliteExceptionTypes {
        database,
        interface,
        operational,
        integrity,
        programming,
        data,
        not_found,
        query_cancelled,
    })
}

fn resolve_exception_types(py: Python<'_>) -> PyResult<&'static SqliteExceptionTypes> {
    SQLITE_EXCEPTION_TYPES.get_or_try_init(py, || match py.import("vertebra._vertebra") {
        Ok(core_mod) => Ok(SqliteExceptionTypes {
            database: core_mod
                .getattr("DatabaseError")?
                .cast_into::<PyType>()?
                .unbind(),
            interface: core_mod
                .getattr("InterfaceError")?
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
            not_found: core_mod
                .getattr("NotFoundError")?
                .cast_into::<PyType>()?
                .unbind(),
            query_cancelled: core_mod
                .getattr("QueryCancelledError")?
                .cast_into::<PyType>()?
                .unbind(),
        }),
        Err(_) => stand_in_exception_types(py),
    })
}

/// Classified error emitted by the SQLite client.
#[derive(Debug, Error)]
pub enum SqliteClientError {
    /// Invalid configuration, argument, or parameter.
    #[error("{0}")]
    InvalidInput(String),

    /// Operational error (connection, locked, busy, interrupt, disk I/O).
    #[error("{message}")]
    Operational {
        message: String,
        code: Option<i32>,
        sqlstate: Option<&'static str>,
        is_transient: bool,
    },

    /// Constraint violation (unique, primary key, foreign key, not null, check).
    #[error("{message}")]
    Integrity {
        message: String,
        code: Option<i32>,
        sqlstate: Option<&'static str>,
    },

    /// SQL syntax, missing table/column, or invalid SQL command.
    #[error("{message}")]
    Programming {
        message: String,
        code: Option<i32>,
        sqlstate: Option<&'static str>,
    },

    /// Value conversion, type mismatch, or range error.
    #[error("{message}")]
    Data {
        message: String,
        code: Option<i32>,
        sqlstate: Option<&'static str>,
    },

    /// Query returned no rows when exactly one was expected.
    #[error("{0}")]
    NotFound(String),

    /// Shared-memory Arrow IPC streaming error.
    #[error("Arrow SHM error: {0}")]
    ShmArrow(String),

    /// Query or operation cancelled out of band.
    #[error("query cancelled: {0}")]
    Cancelled(String),
}

impl SqliteClientError {
    /// Return the canonical SQLSTATE code associated with this error variant.
    #[must_use]
    pub const fn sqlstate(&self) -> Option<&'static str> {
        match self {
            Self::InvalidInput(_) => Some("HY000"),
            Self::Operational { sqlstate, .. } => *sqlstate,
            Self::Integrity { sqlstate, .. } => *sqlstate,
            Self::Programming { sqlstate, .. } => *sqlstate,
            Self::Data { sqlstate, .. } => *sqlstate,
            Self::NotFound(_) => Some("02000"),
            Self::ShmArrow(_) => Some("08000"),
            Self::Cancelled(_) => Some("57014"),
        }
    }

    /// Return the vendor-specific SQLite error code if available.
    #[must_use]
    pub const fn vendor_code(&self) -> Option<i32> {
        match self {
            Self::Operational { code, .. }
            | Self::Integrity { code, .. }
            | Self::Programming { code, .. }
            | Self::Data { code, .. } => *code,
            _ => None,
        }
    }

    /// Return whether the error represents a transient condition eligible for retry.
    #[must_use]
    pub const fn is_transient(&self) -> bool {
        match self {
            Self::Operational { is_transient, .. } => *is_transient,
            _ => false,
        }
    }

    /// Classify a [`rusqlite::Error`] into a [`SqliteClientError`].
    #[must_use]
    pub fn from_rusqlite(err: rusqlite::Error) -> Self {
        match err {
            rusqlite::Error::SqliteFailure(ffi_err, msg_opt) => {
                let code = ffi_err.extended_code;
                let message = msg_opt.unwrap_or_else(|| ffi_err.to_string());
                Self::from_extended_code(code, message)
            }
            rusqlite::Error::FromSqlConversionFailure(_, _, inner) => Self::Data {
                message: format!("SQL conversion failure: {inner}"),
                code: None,
                sqlstate: Some("22000"),
            },
            rusqlite::Error::IntegralValueOutOfRange(col, val) => Self::Data {
                message: format!("integral value {val} out of range at column {col}"),
                code: None,
                sqlstate: Some("22000"),
            },
            rusqlite::Error::Utf8Error(col, e) => Self::Data {
                message: format!("invalid UTF-8 sequence at column {col}: {e}"),
                code: None,
                sqlstate: Some("22000"),
            },
            rusqlite::Error::NulError(e) => Self::InvalidInput(format!("string contains interior NUL: {e}")),
            rusqlite::Error::InvalidParameterName(name) => Self::Programming {
                message: format!("invalid SQL parameter name `{name}`"),
                code: None,
                sqlstate: Some("42000"),
            },
            rusqlite::Error::InvalidPath(path) => Self::Operational {
                message: format!("invalid database file path `{}`", path.display()),
                code: None,
                sqlstate: Some("08000"),
                is_transient: false,
            },
            rusqlite::Error::ExecuteReturnedResults => Self::Programming {
                message: "execute() called on a statement that returns rows; use fetch_all or select_to_arrow instead".to_string(),
                code: None,
                sqlstate: Some("42000"),
            },
            rusqlite::Error::QueryReturnedNoRows => Self::NotFound("query returned no rows".to_string()),
            rusqlite::Error::InvalidColumnIndex(idx) => Self::Programming {
                message: format!("invalid column index {idx}"),
                code: None,
                sqlstate: Some("42000"),
            },
            rusqlite::Error::InvalidColumnName(name) => Self::Programming {
                message: format!("invalid column name `{name}`"),
                code: None,
                sqlstate: Some("42000"),
            },
            rusqlite::Error::InvalidColumnType(idx, name, ty) => Self::Data {
                message: format!("invalid column type {ty} for column {idx} ({name})"),
                code: None,
                sqlstate: Some("22000"),
            },
            other => Self::Operational {
                message: other.to_string(),
                code: None,
                sqlstate: Some("08000"),
                is_transient: false,
            },
        }
    }

    /// Classify an extended SQLite error code and message.
    #[must_use]
    pub fn from_extended_code(code: i32, message: String) -> Self {
        match code {
            // Unique / primary key / rowid constraint violations
            2067 /* SQLITE_CONSTRAINT_UNIQUE */
            | 1555 /* SQLITE_CONSTRAINT_PRIMARYKEY */
            | 2579 /* SQLITE_CONSTRAINT_ROWID */ => Self::Integrity {
                message,
                code: Some(code),
                sqlstate: Some("23505"),
            },
            // Foreign key constraint violation
            787 /* SQLITE_CONSTRAINT_FOREIGNKEY */ => Self::Integrity {
                message,
                code: Some(code),
                sqlstate: Some("23503"),
            },
            // Not null constraint violation
            1299 /* SQLITE_CONSTRAINT_NOTNULL */ => Self::Integrity {
                message,
                code: Some(code),
                sqlstate: Some("23502"),
            },
            // Check constraint violation
            275 /* SQLITE_CONSTRAINT_CHECK */ => Self::Integrity {
                message,
                code: Some(code),
                sqlstate: Some("23514"),
            },
            // Busy / lock conditions (transient)
            5 /* SQLITE_BUSY */
            | 261 /* SQLITE_BUSY_RECOVERY */
            | 517 /* SQLITE_BUSY_SNAPSHOT */
            | 6 /* SQLITE_LOCKED */ => Self::Operational {
                message,
                code: Some(code),
                sqlstate: Some("55P03"),
                is_transient: true,
            },
            // Interrupt / query cancel (transient)
            9 /* SQLITE_INTERRUPT */ => Self::Operational {
                message,
                code: Some(code),
                sqlstate: Some("57014"),
                is_transient: true,
            },
            // Generic SQLite error (syntax / unknown table / etc)
            1 /* SQLITE_ERROR */ => Self::Programming {
                message,
                code: Some(code),
                sqlstate: Some("42000"),
            },
            // Data type mismatch
            20 /* SQLITE_DATATYPE */ => Self::Data {
                message,
                code: Some(code),
                sqlstate: Some("22000"),
            },
            // Any other constraint error
            c if (c & 0xFF) == 19 /* SQLITE_CONSTRAINT */ => Self::Integrity {
                message,
                code: Some(code),
                sqlstate: Some("23000"),
            },
            _ => Self::Operational {
                message,
                code: Some(code),
                sqlstate: Some("08000"),
                is_transient: false,
            },
        }
    }

    /// Convert this error into a Python `DatabaseError` subclass instance.
    #[must_use]
    pub fn into_py_err(self, py: Python<'_>) -> PyErr {
        let msg = self.to_string();
        let sqlstate = self.sqlstate();
        let vendor_code = self.vendor_code();
        let is_transient = self.is_transient();

        let Ok(types) = resolve_exception_types(py) else {
            return pyo3::exceptions::PyRuntimeError::new_err(msg);
        };

        let ty = match self {
            Self::InvalidInput(_) => &types.interface,
            Self::Programming { .. } => &types.programming,
            Self::Operational { .. } | Self::ShmArrow(_) => &types.operational,
            Self::Integrity { .. } => &types.integrity,
            Self::Data { .. } => &types.data,
            Self::NotFound(_) => &types.not_found,
            Self::Cancelled(_) => &types.query_cancelled,
        };

        let py_err = PyErr::from_type(ty.bind(py).clone(), (msg,));
        let inst = py_err.value(py);
        let _ = inst.setattr("sqlstate", sqlstate);
        let _ = inst.setattr("vendor_code", vendor_code);
        let _ = inst.setattr("is_transient", is_transient);
        py_err
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extended_error_codes() {
        let uq = SqliteClientError::from_extended_code(
            2067,
            "UNIQUE constraint failed: t.a".to_string(),
        );
        assert!(matches!(uq, SqliteClientError::Integrity { .. }));
        assert_eq!(uq.sqlstate(), Some("23505"));
        assert_eq!(uq.vendor_code(), Some(2067));
        assert!(!uq.is_transient());

        let pk =
            SqliteClientError::from_extended_code(1555, "PRIMARY KEY must be unique".to_string());
        assert!(matches!(pk, SqliteClientError::Integrity { .. }));
        assert_eq!(pk.sqlstate(), Some("23505"));

        let fk =
            SqliteClientError::from_extended_code(787, "FOREIGN KEY constraint failed".to_string());
        assert!(matches!(fk, SqliteClientError::Integrity { .. }));
        assert_eq!(fk.sqlstate(), Some("23503"));

        let nn =
            SqliteClientError::from_extended_code(1299, "NOT NULL constraint failed".to_string());
        assert!(matches!(nn, SqliteClientError::Integrity { .. }));
        assert_eq!(nn.sqlstate(), Some("23502"));

        let chk = SqliteClientError::from_extended_code(275, "CHECK constraint failed".to_string());
        assert!(matches!(chk, SqliteClientError::Integrity { .. }));
        assert_eq!(chk.sqlstate(), Some("23514"));

        let busy = SqliteClientError::from_extended_code(5, "database is locked".to_string());
        assert!(matches!(busy, SqliteClientError::Operational { .. }));
        assert_eq!(busy.sqlstate(), Some("55P03"));
        assert!(busy.is_transient());

        let interrupt = SqliteClientError::from_extended_code(9, "interrupted".to_string());
        assert!(matches!(interrupt, SqliteClientError::Operational { .. }));
        assert_eq!(interrupt.sqlstate(), Some("57014"));
        assert!(interrupt.is_transient());

        let syntax =
            SqliteClientError::from_extended_code(1, "near \"SELEC\": syntax error".to_string());
        assert!(matches!(syntax, SqliteClientError::Programming { .. }));
        assert_eq!(syntax.sqlstate(), Some("42000"));

        let datatype = SqliteClientError::from_extended_code(20, "datatype mismatch".to_string());
        assert!(matches!(datatype, SqliteClientError::Data { .. }));
        assert_eq!(datatype.sqlstate(), Some("22000"));
    }
}
