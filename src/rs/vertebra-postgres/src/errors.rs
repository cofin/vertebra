//! Structured PostgreSQL error categories, SQLSTATE retention, and diagnostic redaction.

use std::fmt;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyType;

/// Sanitized PostgreSQL backend `ErrorResponse` record retaining 5-byte ASCII SQLSTATE and raw diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    pub severity: String,
    pub sqlstate: String,
    pub message: String,
    pub diagnostics: Vec<(u8, String)>,
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.severity, self.sqlstate, self.message)
    }
}

/// Canonical error categories emitted by `vertebra-postgres`.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PostgresError {
    #[error("invalid PostgreSQL configuration: {0}")]
    Config(String),
    #[error("PostgreSQL interface error: {0}")]
    Interface(String),
    #[error("PostgreSQL data error: {0}")]
    Data(String),
    #[error("PostgreSQL authentication error: {0}")]
    Auth(String),
    #[error("unsupported PostgreSQL authentication: {0}")]
    UnsupportedAuth(String),
    #[error("unsupported PostgreSQL feature: {0}")]
    UnsupportedFeature(String),
    #[error("PostgreSQL transport error: {0}")]
    Transport(String),
    #[error("PostgreSQL protocol error: {0}")]
    Protocol(String),
    #[error("{0}")]
    Server(ServerError),
}

impl PostgresError {
    /// Construct a [`PostgresError::Server`] after validating that `sqlstate` is an exact 5-byte ASCII alphanumeric code.
    pub fn server(
        severity: impl Into<String>,
        sqlstate: &str,
        message: impl Into<String>,
        diagnostics: Vec<(u8, String)>,
    ) -> Result<Self, Self> {
        if sqlstate.len() != 5 || !sqlstate.bytes().all(|b| b.is_ascii_alphanumeric()) {
            return Err(Self::Protocol(format!(
                "invalid 5-byte ASCII SQLSTATE code `{sqlstate}`"
            )));
        }
        Ok(Self::Server(ServerError {
            severity: severity.into(),
            sqlstate: sqlstate.to_owned(),
            message: message.into(),
            diagnostics,
        }))
    }

    /// Return the 5-character SQLSTATE code when this error originated from a server `ErrorResponse`.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Server(err) => Some(err.sqlstate.as_str()),
            _ => None,
        }
    }
}

struct PostgresPyExceptionTypes {
    database: Py<PyType>,
    interface: Py<PyType>,
    data: Py<PyType>,
    integrity: Py<PyType>,
    programming: Py<PyType>,
    not_supported: Py<PyType>,
    connection_failure: Py<PyType>,
    query_cancelled: Py<PyType>,
    serialization_failure: Py<PyType>,
    google_cloud_auth: Py<PyType>,
}

static POSTGRES_PY_EXCEPTIONS: PyOnceLock<PostgresPyExceptionTypes> = PyOnceLock::new();

fn resolve_py_exceptions(py: Python<'_>) -> PyResult<&'static PostgresPyExceptionTypes> {
    POSTGRES_PY_EXCEPTIONS.get_or_try_init(py, || {
        let core_mod = py.import("vertebra._vertebra")?;
        let get_ty = |name: &str| -> PyResult<Py<PyType>> {
            Ok(core_mod.getattr(name)?.cast_into::<PyType>()?.unbind())
        };
        Ok(PostgresPyExceptionTypes {
            database: get_ty("DatabaseError")?,
            interface: get_ty("InterfaceError")?,
            data: get_ty("DataError")?,
            integrity: get_ty("IntegrityError")?,
            programming: get_ty("ProgrammingError")?,
            not_supported: get_ty("NotSupportedError")?,
            connection_failure: get_ty("ConnectionFailure")?,
            query_cancelled: get_ty("QueryCancelledError")?,
            serialization_failure: get_ty("SerializationFailure")?,
            google_cloud_auth: get_ty("GoogleCloudAuthError")?,
        })
    })
}

fn instantiate_exc(
    py: Python<'_>,
    ty: &Py<PyType>,
    message: &str,
    sqlstate: Option<&str>,
) -> PyErr {
    match ty.bind(py).call1((message,)) {
        Ok(inst) => {
            if let Some(code) = sqlstate {
                let _ = inst.setattr("sqlstate", code);
            } else {
                let _ = inst.setattr("sqlstate", py.None());
            }
            PyErr::from_value(inst)
        }
        Err(err) => err,
    }
}

/// Convert a [`PostgresError`] into a canonical `vertebra._vertebra` Python exception.
pub fn postgres_error_to_py(py: Python<'_>, err: PostgresError) -> PyErr {
    if let PostgresError::Config(msg) = err {
        return PyValueError::new_err(msg);
    }
    let types = match resolve_py_exceptions(py) {
        Ok(types) => types,
        Err(import_err) => return import_err,
    };
    match err {
        PostgresError::Config(msg) => PyValueError::new_err(msg),
        PostgresError::Interface(msg) => instantiate_exc(py, &types.interface, &msg, None),
        PostgresError::Data(msg) => instantiate_exc(py, &types.data, &msg, None),
        PostgresError::UnsupportedAuth(msg) | PostgresError::UnsupportedFeature(msg) => {
            instantiate_exc(py, &types.not_supported, &msg, None)
        }
        PostgresError::Transport(msg) | PostgresError::Protocol(msg) => {
            instantiate_exc(py, &types.connection_failure, &msg, None)
        }
        PostgresError::Auth(msg) => {
            let py_err = instantiate_exc(py, &types.google_cloud_auth, &msg, None);
            let _ = py_err.value(py).setattr("message", msg);
            py_err
        }
        PostgresError::Server(srv) => {
            let code = srv.sqlstate.as_str();
            let target_ty = match code {
                "40001" | "40P01" => &types.serialization_failure,
                "57014" => &types.query_cancelled,
                "28000" | "28P01" => &types.database,
                c if c.starts_with("08") => &types.connection_failure,
                c if c.starts_with("22") => &types.data,
                c if c.starts_with("23") => &types.integrity,
                c if c.starts_with("42") => &types.programming,
                _ => &types.database,
            };
            instantiate_exc(py, target_ty, &srv.message, Some(code))
        }
    }
}
