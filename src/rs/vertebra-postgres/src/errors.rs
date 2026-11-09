//! Structured PostgreSQL error categories, SQLSTATE retention, and diagnostic redaction.

use std::fmt;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyTuple, PyType};

/// Sanitized PostgreSQL backend `ErrorResponse` record retaining 5-byte ASCII SQLSTATE and raw diagnostics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    pub severity: String,
    pub sqlstate: String,
    pub message: String,
    pub diagnostics: Vec<(u8, String)>,
}

impl ServerError {
    /// Look up a specific PostgreSQL `ErrorResponse` field byte (e.g. `b'D'` for detail, `b'n'` for constraint).
    pub fn diagnostic(&self, tag: u8) -> Option<&str> {
        self.diagnostics
            .iter()
            .find_map(|(k, v)| (*k == tag).then_some(v.as_str()))
    }
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({}): {}", self.severity, self.sqlstate, self.message)
    }
}

/// Canonical error categories emitted by `vertebra-postgres`.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
pub enum PostgresError {
    #[error("invalid PostgreSQL configuration: {0}")]
    Config(String),
    #[error("PostgreSQL interface error: {0}")]
    Interface(String),
    #[error("PostgreSQL data error: {0}")]
    Data(String),
    #[error("PostgreSQL operational error: {0}")]
    Operational(String),
    #[error("PostgreSQL connection pool exhausted: {0}")]
    PoolExhausted(String),
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
    #[error("{source}")]
    AfterCompletion {
        #[source]
        source: Box<PostgresError>,
        completed_result: crate::result::QueryResult,
    },
    #[error("pipeline validation failed at operation {failing_index}: {source}")]
    PipelineValidation {
        #[source]
        source: Box<PostgresError>,
        failing_index: usize,
    },
    #[error("pipeline failed at operation {}: {}", .0.failing_index, .0.first_error)]
    PipelineAborted(Box<crate::pipeline::PipelineFailure>),
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
            Self::AfterCompletion { source, .. } | Self::PipelineValidation { source, .. } => {
                source.sqlstate()
            }
            Self::PipelineAborted(fail) => fail.first_error.sqlstate(),
            _ => None,
        }
    }

    /// Return `true` when this error represents a transient transport, pool, or server condition.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Transport(_) | Self::PoolExhausted(_) => true,
            Self::Server(err) => {
                err.sqlstate == "40001"
                    || err.sqlstate == "40P01"
                    || (err.sqlstate.starts_with("08") && err.sqlstate != "08P01")
                    || err.sqlstate == "53300"
                    || err.sqlstate.starts_with("57P")
            }
            Self::AfterCompletion { source, .. } | Self::PipelineValidation { source, .. } => {
                source.is_transient()
            }
            Self::PipelineAborted(fail) => fail.first_error.is_transient(),
            _ => false,
        }
    }
}

struct PostgresPyExceptionTypes {
    database: Py<PyType>,
    interface: Py<PyType>,
    data: Py<PyType>,
    operational: Py<PyType>,
    pool_exhausted: Py<PyType>,
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
            operational: get_ty("OperationalError")?,
            pool_exhausted: get_ty("PoolExhaustedError")?,
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
    is_transient: bool,
) -> PyErr {
    match ty.bind(py).call1((message,)) {
        Ok(inst) => {
            if let Some(code) = sqlstate {
                let _ = inst.setattr("sqlstate", code);
                let _ = inst.setattr("vendor_code", code);
            } else {
                let _ = inst.setattr("sqlstate", py.None());
                let _ = inst.setattr("vendor_code", py.None());
            }
            let _ = inst.setattr("is_transient", is_transient);
            let _ = inst.setattr("completed_result", py.None());
            PyErr::from_value(inst)
        }
        Err(err) => err,
    }
}

fn attach_server_diagnostics(py: Python<'_>, inst: &Bound<'_, PyAny>, srv: &ServerError) {
    let _ = inst.setattr("severity", srv.severity.as_str());
    let set_opt = |attr: &str, tag: u8| {
        if let Some(val) = srv.diagnostic(tag) {
            let _ = inst.setattr(attr, val);
        } else {
            let _ = inst.setattr(attr, py.None());
        }
    };
    set_opt("detail", b'D');
    set_opt("hint", b'H');
    set_opt("schema_name", b's');
    set_opt("table_name", b't');
    set_opt("column_name", b'c');
    set_opt("datatype_name", b'd');
    set_opt("constraint_name", b'n');
    set_opt("where_context", b'W');
    if let Some(pos_str) = srv.diagnostic(b'P') {
        if let Ok(pos_int) = pos_str.parse::<i32>() {
            let _ = inst.setattr("position", pos_int);
        } else {
            let _ = inst.setattr("position", pos_str);
        }
    } else {
        let _ = inst.setattr("position", py.None());
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
    let transient = err.is_transient();
    match err {
        PostgresError::Config(msg) => PyValueError::new_err(msg),
        PostgresError::Interface(msg) => {
            instantiate_exc(py, &types.interface, &msg, None, transient)
        }
        PostgresError::Data(msg) => instantiate_exc(py, &types.data, &msg, None, transient),
        PostgresError::Operational(msg) => {
            instantiate_exc(py, &types.operational, &msg, None, transient)
        }
        PostgresError::PoolExhausted(msg) => {
            instantiate_exc(py, &types.pool_exhausted, &msg, None, true)
        }
        PostgresError::UnsupportedAuth(msg) | PostgresError::UnsupportedFeature(msg) => {
            instantiate_exc(py, &types.not_supported, &msg, None, transient)
        }
        PostgresError::Transport(msg) | PostgresError::Protocol(msg) => {
            instantiate_exc(py, &types.connection_failure, &msg, None, true)
        }
        PostgresError::Auth(msg) => {
            let py_err = instantiate_exc(py, &types.google_cloud_auth, &msg, None, transient);
            let _ = py_err.value(py).setattr("message", msg);
            py_err
        }
        PostgresError::Server(srv) => {
            let code = srv.sqlstate.as_str();
            let target_ty = match code {
                "40001" | "40P01" => &types.serialization_failure,
                "57014" => &types.query_cancelled,
                "28000" | "28P01" => &types.database,
                "08P01" | "26000" => &types.programming,
                c if c.starts_with("0A") => &types.not_supported,
                c if c.starts_with("08") => &types.connection_failure,
                c if c.starts_with("22") => &types.data,
                c if c.starts_with("23") => &types.integrity,
                c if c.starts_with("42") => &types.programming,
                c if c.starts_with("53") || c.starts_with("57P") => &types.operational,
                _ => &types.database,
            };
            let py_err = instantiate_exc(py, target_ty, &srv.message, Some(code), transient);
            attach_server_diagnostics(py, py_err.value(py), &srv);
            py_err
        }
        PostgresError::AfterCompletion {
            source,
            completed_result,
        } => {
            let py_err = postgres_error_to_py(py, *source);
            if let Ok(py_res) = crate::python_values::query_result_to_python(py, &completed_result)
            {
                let _ = py_err.value(py).setattr("completed_result", py_res);
            }
            py_err
        }
        PostgresError::PipelineValidation {
            source,
            failing_index,
        } => {
            let py_err = postgres_error_to_py(py, *source);
            let _ = py_err.value(py).setattr("outcomes", PyTuple::empty(py));
            let _ = py_err.value(py).setattr("failing_index", failing_index);
            py_err
        }
        PostgresError::PipelineAborted(failure) => {
            let PipelineAbortedParts {
                first_error,
                failing_index,
                outcomes,
            } = PipelineAbortedParts::from(*failure);
            let py_err = postgres_error_to_py(py, first_error);
            if let Ok(py_outcomes) =
                crate::python_values::pipeline_outcomes_to_python(py, &outcomes)
            {
                let _ = py_err.value(py).setattr("outcomes", py_outcomes);
            } else {
                let _ = py_err.value(py).setattr("outcomes", PyTuple::empty(py));
            }
            let _ = py_err.value(py).setattr("failing_index", failing_index);
            py_err
        }
    }
}

struct PipelineAbortedParts {
    first_error: PostgresError,
    failing_index: usize,
    outcomes: Vec<crate::pipeline::PipelineOutcome>,
}

impl From<crate::pipeline::PipelineFailure> for PipelineAbortedParts {
    fn from(f: crate::pipeline::PipelineFailure) -> Self {
        Self {
            first_error: f.first_error,
            failing_index: f.failing_index,
            outcomes: f.outcomes,
        }
    }
}
