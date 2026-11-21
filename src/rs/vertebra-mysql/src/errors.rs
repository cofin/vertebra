//! MySQL and MariaDB error classification and Python `vertebra.db.exceptions` mapping.

use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};
use thiserror::Error;

/// Cached Python exception types resolved from `vertebra.db.driver` / `vertebra._vertebra`.
pub(crate) struct MySQLExceptionTypes {
    pub(crate) improper_configuration: Py<PyType>,
    pub(crate) connection_failure: Py<PyType>,
    pub(crate) integrity: Py<PyType>,
    pub(crate) serialization_failure: Py<PyType>,
    pub(crate) query_cancelled: Py<PyType>,
    pub(crate) data: Py<PyType>,
    pub(crate) programming: Py<PyType>,
    pub(crate) operational: Py<PyType>,
    pub(crate) not_found: Py<PyType>,
    pub(crate) interface: Py<PyType>,
}

static MYSQL_EXCEPTION_TYPES: PyOnceLock<MySQLExceptionTypes> = PyOnceLock::new();

fn load_exc_type(py: Python<'_>, primary_mod: &str, exc_name: &str) -> PyResult<Py<PyType>> {
    py.import(primary_mod)
        .and_then(|m| m.getattr(exc_name))
        .or_else(|_| {
            py.import("vertebra._vertebra")
                .and_then(|m| m.getattr(exc_name))
        })
        .and_then(|cls| cls.cast_into::<PyType>().map_err(Into::into))
        .map(Bound::unbind)
}

fn new_stand_in_class<'py>(
    py: Python<'py>,
    name: &str,
    bases: &Bound<'py, PyTuple>,
) -> PyResult<Py<PyType>> {
    let dict = PyDict::new(py);
    dict.set_item("__module__", "vertebra.db.driver")?;
    let ty = py
        .get_type::<PyType>()
        .call1((name, bases, dict))?
        .cast_into::<PyType>()?;
    Ok(ty.unbind())
}

fn build_stand_in_exception_types(py: Python<'_>) -> PyResult<MySQLExceptionTypes> {
    let root = py.get_type::<PyException>();
    let root_bases = PyTuple::new(py, [&root])?;
    let improper_configuration = new_stand_in_class(py, "ImproperConfigurationError", &root_bases)?;
    let database = new_stand_in_class(py, "DatabaseError", &root_bases)?;
    let db_bases = PyTuple::new(py, [database.bind(py)])?;
    let interface = new_stand_in_class(py, "InterfaceError", &db_bases)?;
    let operational = new_stand_in_class(py, "OperationalError", &db_bases)?;
    let op_bases = PyTuple::new(py, [operational.bind(py)])?;
    let connection_failure = new_stand_in_class(py, "ConnectionFailure", &op_bases)?;
    let serialization_failure = new_stand_in_class(py, "SerializationFailure", &op_bases)?;
    let query_cancelled = new_stand_in_class(py, "QueryCancelledError", &op_bases)?;
    let integrity = new_stand_in_class(py, "IntegrityError", &db_bases)?;
    let programming = new_stand_in_class(py, "ProgrammingError", &db_bases)?;
    let data = new_stand_in_class(py, "DataError", &db_bases)?;
    let data_bases = PyTuple::new(py, [data.bind(py)])?;
    let not_found = new_stand_in_class(py, "NotFoundError", &data_bases)?;
    Ok(MySQLExceptionTypes {
        improper_configuration,
        connection_failure,
        integrity,
        serialization_failure,
        query_cancelled,
        data,
        programming,
        operational,
        not_found,
        interface,
    })
}

fn load_exception_types(py: Python<'_>) -> PyResult<MySQLExceptionTypes> {
    Ok(MySQLExceptionTypes {
        improper_configuration: load_exc_type(py, "vertebra.typing", "ImproperConfigurationError")?,
        connection_failure: load_exc_type(py, "vertebra.db.driver", "ConnectionFailure")?,
        integrity: load_exc_type(py, "vertebra.db.driver", "IntegrityError")?,
        serialization_failure: load_exc_type(py, "vertebra.db.driver", "SerializationFailure")?,
        query_cancelled: load_exc_type(py, "vertebra.db.driver", "QueryCancelledError")?,
        data: load_exc_type(py, "vertebra.db.driver", "DataError")?,
        programming: load_exc_type(py, "vertebra.db.driver", "ProgrammingError")?,
        operational: load_exc_type(py, "vertebra.db.driver", "OperationalError")?,
        not_found: load_exc_type(py, "vertebra.db.driver", "NotFoundError")?,
        interface: load_exc_type(py, "vertebra.db.driver", "InterfaceError")?,
    })
}

pub(crate) fn resolve_exception_types(py: Python<'_>) -> PyResult<&'static MySQLExceptionTypes> {
    MYSQL_EXCEPTION_TYPES.get_or_try_init(py, || match load_exception_types(py) {
        Ok(types) => Ok(types),
        Err(_) => build_stand_in_exception_types(py),
    })
}

/// Canonical Vertebra database exception category for a MySQL or MariaDB failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MySQLErrorKind {
    ImproperConfiguration,
    ConnectionFailure,
    IntegrityError,
    SerializationFailure,
    QueryCancelled,
    DataError,
    ProgrammingError,
    OperationalError,
    NotFound,
}

impl MySQLErrorKind {
    /// Returns the exception class name in `vertebra.db.exceptions`.
    pub const fn python_exception_name(self) -> &'static str {
        match self {
            Self::ImproperConfiguration => "ImproperConfigurationError",
            Self::ConnectionFailure => "ConnectionFailure",
            Self::IntegrityError => "IntegrityError",
            Self::SerializationFailure => "SerializationFailure",
            Self::QueryCancelled => "QueryCancelledError",
            Self::DataError => "DataError",
            Self::ProgrammingError => "ProgrammingError",
            Self::OperationalError => "OperationalError",
            Self::NotFound => "NotFoundError",
        }
    }
}

/// Classifies a MySQL/MariaDB server error number (`errno`) and optional 5-character `SQLSTATE`
/// into the corresponding [`MySQLErrorKind`].
pub fn classify_mysql_error(errno: u16, sqlstate: Option<&str>) -> MySQLErrorKind {
    match errno {
        1062 | 1216 | 1217 | 1451 | 1452 | 1048 | 1364 | 3819 | 4025 => {
            return MySQLErrorKind::IntegrityError;
        }
        1213 | 1205 => {
            return MySQLErrorKind::SerializationFailure;
        }
        1317 | 3024 | 1969 => {
            return MySQLErrorKind::QueryCancelled;
        }
        1040 | 1042 | 1043 | 1047 | 1053 | 1152..=1161 | 2002 | 2003 | 2006 | 2013 => {
            return MySQLErrorKind::ConnectionFailure;
        }
        1264 | 1265 | 1292 | 1366 | 1406 | 1690 => {
            return MySQLErrorKind::DataError;
        }
        1044 | 1045 | 1049 | 1050 | 1051 | 1054 | 1064 | 1142 | 1146 | 1305 => {
            return MySQLErrorKind::ProgrammingError;
        }
        _ => {}
    }

    if let Some(state) = sqlstate {
        if state == "40001" {
            return MySQLErrorKind::SerializationFailure;
        }
        if state == "70100" || state == "HY008" {
            return MySQLErrorKind::QueryCancelled;
        }
        if state.starts_with("23") {
            return MySQLErrorKind::IntegrityError;
        }
        if state.starts_with("08") {
            return MySQLErrorKind::ConnectionFailure;
        }
        if state.starts_with("22") {
            return MySQLErrorKind::DataError;
        }
        if state.starts_with("42") || state.starts_with("28") {
            return MySQLErrorKind::ProgrammingError;
        }
    }

    MySQLErrorKind::OperationalError
}

/// Structured error returned by `vertebra-mysql` operations.
#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct MySQLError {
    pub kind: MySQLErrorKind,
    pub message: String,
    pub errno: Option<u16>,
    pub sqlstate: Option<String>,
}

impl MySQLError {
    pub fn new(
        kind: MySQLErrorKind,
        message: impl Into<String>,
        errno: Option<u16>,
        sqlstate: Option<String>,
    ) -> Self {
        Self {
            kind,
            message: message.into(),
            errno,
            sqlstate,
        }
    }

    pub fn config(message: impl Into<String>) -> Self {
        Self::new(MySQLErrorKind::ImproperConfiguration, message, None, None)
    }

    pub fn connection(message: impl Into<String>) -> Self {
        Self::new(
            MySQLErrorKind::ConnectionFailure,
            message,
            Some(2003),
            Some("08001".to_string()),
        )
    }

    pub fn operational(message: impl Into<String>) -> Self {
        Self::new(MySQLErrorKind::OperationalError, message, None, None)
    }

    pub fn protocol(message: impl Into<String>) -> Self {
        Self::operational(message)
    }

    pub fn programming(message: impl Into<String>) -> Self {
        Self::new(
            MySQLErrorKind::ProgrammingError,
            message,
            Some(1064),
            Some("42000".to_string()),
        )
    }

    pub fn data(message: impl Into<String>) -> Self {
        Self::new(
            MySQLErrorKind::DataError,
            message,
            Some(1406),
            Some("22001".to_string()),
        )
    }

    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(
            MySQLErrorKind::QueryCancelled,
            message,
            Some(1317),
            Some("70100".to_string()),
        )
    }

    pub fn not_found(message: impl Into<String>) -> Self {
        Self::new(
            MySQLErrorKind::NotFound,
            message,
            None,
            Some("02000".to_string()),
        )
    }

    pub fn from_server_error(
        errno: u16,
        sqlstate: Option<&str>,
        message: impl Into<String>,
    ) -> Self {
        let kind = classify_mysql_error(errno, sqlstate);
        Self {
            kind,
            message: message.into(),
            errno: Some(errno),
            sqlstate: sqlstate.map(ToOwned::to_owned),
        }
    }

    /// Converts this [`MySQLError`] into a Python `vertebra.db.driver` / `vertebra.typing` exception instance.
    pub fn to_py_err(&self, py: Python<'_>) -> PyErr {
        let cls_res = resolve_exception_types(py).map(|types| match self.kind {
            MySQLErrorKind::ImproperConfiguration => types.improper_configuration.bind(py),
            MySQLErrorKind::ConnectionFailure => types.connection_failure.bind(py),
            MySQLErrorKind::IntegrityError => types.integrity.bind(py),
            MySQLErrorKind::SerializationFailure => types.serialization_failure.bind(py),
            MySQLErrorKind::QueryCancelled => types.query_cancelled.bind(py),
            MySQLErrorKind::DataError => types.data.bind(py),
            MySQLErrorKind::ProgrammingError => types.programming.bind(py),
            MySQLErrorKind::OperationalError => types.operational.bind(py),
            MySQLErrorKind::NotFound => types.not_found.bind(py),
        });
        match cls_res.and_then(|cls| cls.call1((self.message.as_str(),))) {
            Ok(instance) => {
                if let Some(ref state) = self.sqlstate {
                    let _ = instance.setattr("sqlstate", state.as_str());
                }
                if let Some(code) = self.errno {
                    let _ = instance.setattr("vendor_code", code);
                    let _ = instance.setattr("errno", code);
                }
                let is_transient = matches!(
                    self.kind,
                    MySQLErrorKind::ConnectionFailure | MySQLErrorKind::SerializationFailure
                );
                let _ = instance.setattr("is_transient", is_transient);
                PyErr::from_value(instance)
            }
            Err(_) => PyRuntimeError::new_err(self.message.clone()),
        }
    }
}

impl From<mysql_async::Error> for MySQLError {
    fn from(err: mysql_async::Error) -> Self {
        match err {
            mysql_async::Error::Server(server_err) => {
                let code = server_err.code;
                let state = server_err.state.clone();
                let msg = format!("ERROR {} ({}): {}", code, state, server_err.message);
                Self::from_server_error(code, Some(state.as_str()), msg)
            }
            mysql_async::Error::Io(io_err) => {
                let text = io_err.to_string();
                Self::connection(format!("MySQL I/O failure: {text}"))
            }
            mysql_async::Error::Url(url_err) => {
                Self::config(format!("Invalid MySQL connection URL: {url_err}"))
            }
            mysql_async::Error::Driver(driver_err) => {
                let text = driver_err.to_string();
                if matches!(
                    driver_err,
                    mysql_async::DriverError::ConnectionClosed
                        | mysql_async::DriverError::PoolDisconnected
                ) {
                    Self::connection(format!("MySQL connection error: {text}"))
                } else {
                    Self::operational(format!("MySQL driver error: {text}"))
                }
            }
            other => Self::operational(other.to_string()),
        }
    }
}

impl From<MySQLError> for PyErr {
    fn from(err: MySQLError) -> Self {
        Python::attach(|py| err.to_py_err(py))
    }
}

/// Maps a MySQL `errno`, optional `sqlstate`, and error message onto a `vertebra.db.exceptions`
/// exception object without raising it.
#[pyfunction]
#[pyo3(signature = (errno, sqlstate=None, message="MySQL error"))]
pub fn map_mysql_error(
    py: Python<'_>,
    errno: u16,
    sqlstate: Option<&str>,
    message: &str,
) -> PyResult<Py<PyAny>> {
    let err = MySQLError::from_server_error(errno, sqlstate, message);
    let py_err = err.to_py_err(py);
    Ok(py_err.value(py).clone().unbind().into_any())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_classify_mysql_error_codes_and_sqlstates() {
        assert_eq!(
            classify_mysql_error(1062, Some("23000")),
            MySQLErrorKind::IntegrityError
        );
        assert_eq!(
            classify_mysql_error(1452, Some("23000")),
            MySQLErrorKind::IntegrityError
        );
        assert_eq!(
            classify_mysql_error(1213, Some("40001")),
            MySQLErrorKind::SerializationFailure
        );
        assert_eq!(
            classify_mysql_error(1205, Some("HY000")),
            MySQLErrorKind::SerializationFailure
        );
        assert_eq!(
            classify_mysql_error(1317, Some("70100")),
            MySQLErrorKind::QueryCancelled
        );
        assert_eq!(
            classify_mysql_error(3024, Some("HY000")),
            MySQLErrorKind::QueryCancelled
        );
        assert_eq!(
            classify_mysql_error(2006, Some("08S01")),
            MySQLErrorKind::ConnectionFailure
        );
        assert_eq!(
            classify_mysql_error(1406, Some("22001")),
            MySQLErrorKind::DataError
        );
        assert_eq!(
            classify_mysql_error(1064, Some("42000")),
            MySQLErrorKind::ProgrammingError
        );
        assert_eq!(
            classify_mysql_error(1146, Some("42S02")),
            MySQLErrorKind::ProgrammingError
        );
        assert_eq!(
            classify_mysql_error(9999, Some("HY000")),
            MySQLErrorKind::OperationalError
        );
    }
}
