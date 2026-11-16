//! Redis error classification and Python `vertebra._vertebra` / `vertebra.typing` exception mapping.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyTuple, PyType};
use redis::ServerErrorKind;
use thiserror::Error;

/// Structured error returned by `vertebra-redis` operations.
#[derive(Debug, Clone, Error)]
pub enum RedisDriverError {
    #[error("ImproperConfigurationError: {0}")]
    Config(String),
    #[error("OperationalError: {message}")]
    Operational { message: String, is_transient: bool },
    #[error("ProgrammingError: {message}")]
    Programming { message: String, is_transient: bool },
    #[error("DataError: {message}")]
    Data { message: String, is_transient: bool },
    #[error("InterfaceError: {message}")]
    Interface { message: String, is_transient: bool },
}

impl RedisDriverError {
    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub fn operational(msg: impl Into<String>, is_transient: bool) -> Self {
        Self::Operational {
            message: msg.into(),
            is_transient,
        }
    }

    pub fn programming(msg: impl Into<String>, is_transient: bool) -> Self {
        Self::Programming {
            message: msg.into(),
            is_transient,
        }
    }

    pub fn data(msg: impl Into<String>, is_transient: bool) -> Self {
        Self::Data {
            message: msg.into(),
            is_transient,
        }
    }

    pub fn interface(msg: impl Into<String>, is_transient: bool) -> Self {
        Self::Interface {
            message: msg.into(),
            is_transient,
        }
    }

    pub fn is_transient(&self) -> bool {
        match self {
            Self::Config(_) => false,
            Self::Operational { is_transient, .. }
            | Self::Programming { is_transient, .. }
            | Self::Data { is_transient, .. }
            | Self::Interface { is_transient, .. } => *is_transient,
        }
    }

    pub fn message(&self) -> &str {
        match self {
            Self::Config(msg) => msg.as_str(),
            Self::Operational { message, .. }
            | Self::Programming { message, .. }
            | Self::Data { message, .. }
            | Self::Interface { message, .. } => message.as_str(),
        }
    }

    pub fn into_pyerr(self, py: Python<'_>) -> PyErr {
        redis_error_to_py(py, self)
    }
}

/// Classify a `redis::RedisError` into a structured [`RedisDriverError`].
pub fn classify_redis_error(err: &redis::RedisError) -> RedisDriverError {
    let msg = err.to_string();
    let upper = msg.to_uppercase();

    if upper.contains("NOAUTH") || upper.contains("WRONGPASS") || upper.contains("NOPERM") {
        return RedisDriverError::programming(msg, false);
    }
    if upper.contains("CROSSSLOT") || upper.contains("NOSCRIPT") {
        return RedisDriverError::programming(msg, false);
    }
    if upper.contains("WRONGTYPE") {
        return RedisDriverError::data(msg, false);
    }
    if upper.contains("READONLY")
        || upper.contains("BUSY ")
        || upper.contains("LOADING")
        || upper.contains("CLUSTERDOWN")
        || upper.contains("TRYAGAIN")
    {
        return RedisDriverError::operational(msg, true);
    }

    match err.kind() {
        redis::ErrorKind::Server(ServerErrorKind::ResponseError) => {
            if upper.contains("ERR SYNTAX")
                || upper.contains("ERR UNKNOWN")
                || upper.contains("ERR WRONG NUMBER OF ARGUMENTS")
            {
                RedisDriverError::programming(msg, false)
            } else {
                RedisDriverError::operational(msg, true)
            }
        }
        redis::ErrorKind::Server(ServerErrorKind::NoPerm)
        | redis::ErrorKind::Server(ServerErrorKind::NoScript)
        | redis::ErrorKind::Server(ServerErrorKind::CrossSlot)
        | redis::ErrorKind::Server(ServerErrorKind::ExecAbort) => {
            RedisDriverError::programming(msg, false)
        }
        redis::ErrorKind::Server(ServerErrorKind::BusyLoading)
        | redis::ErrorKind::Server(ServerErrorKind::ClusterDown)
        | redis::ErrorKind::Server(ServerErrorKind::MasterDown)
        | redis::ErrorKind::Server(ServerErrorKind::ReadOnly)
        | redis::ErrorKind::Server(ServerErrorKind::TryAgain)
        | redis::ErrorKind::Server(ServerErrorKind::Moved)
        | redis::ErrorKind::Server(ServerErrorKind::Ask) => {
            RedisDriverError::operational(msg, true)
        }
        redis::ErrorKind::AuthenticationFailed => RedisDriverError::programming(msg, false),
        redis::ErrorKind::UnexpectedReturnType | redis::ErrorKind::Parse => {
            RedisDriverError::data(msg, false)
        }
        redis::ErrorKind::InvalidClientConfig => RedisDriverError::config(msg),
        redis::ErrorKind::Io
        | redis::ErrorKind::MasterNameNotFoundBySentinel
        | redis::ErrorKind::NoValidReplicasFoundBySentinel
        | redis::ErrorKind::EmptySentinelList
        | redis::ErrorKind::ClusterConnectionNotFound => {
            RedisDriverError::operational(msg, true)
        }
        _ => {
            if err.is_io_error() || err.is_cluster_error() || err.is_connection_refusal() {
                RedisDriverError::operational(msg, true)
            } else {
                RedisDriverError::operational(msg, false)
            }
        }
    }
}

impl From<redis::RedisError> for RedisDriverError {
    fn from(err: redis::RedisError) -> Self {
        classify_redis_error(&err)
    }
}

struct RedisPyExceptions {
    improper_config: Py<PyType>,
    operational: Py<PyType>,
    programming: Py<PyType>,
    data: Py<PyType>,
    interface: Py<PyType>,
}

static REDIS_PY_EXCEPTIONS: PyOnceLock<RedisPyExceptions> = PyOnceLock::new();

fn new_stand_in_class(
    py: Python<'_>,
    name: &str,
    bases: &Bound<'_, PyTuple>,
) -> PyResult<Py<PyType>> {
    let dict = PyDict::new(py);
    dict.set_item("__module__", "vertebra.db.driver")?;
    let ty = py
        .get_type::<PyType>()
        .call1((name, bases, dict))?
        .cast_into::<PyType>()?;
    Ok(ty.unbind())
}

fn build_stand_in_py_exceptions(py: Python<'_>) -> PyResult<RedisPyExceptions> {
    let root = py.get_type::<pyo3::exceptions::PyException>();
    let database = new_stand_in_class(py, "DatabaseError", &PyTuple::new(py, [&root])?)?;
    let db_bound = database.bind(py);
    let improper_config =
        new_stand_in_class(py, "ImproperConfigurationError", &PyTuple::new(py, [&root])?)?;
    let operational = new_stand_in_class(py, "OperationalError", &PyTuple::new(py, [db_bound])?)?;
    let programming = new_stand_in_class(py, "ProgrammingError", &PyTuple::new(py, [db_bound])?)?;
    let data = new_stand_in_class(py, "DataError", &PyTuple::new(py, [db_bound])?)?;
    let interface = new_stand_in_class(py, "InterfaceError", &PyTuple::new(py, [db_bound])?)?;
    Ok(RedisPyExceptions {
        improper_config,
        operational,
        programming,
        data,
        interface,
    })
}

fn load_py_exceptions_from_core(py: Python<'_>) -> PyResult<RedisPyExceptions> {
    let typing_mod = py.import("vertebra.typing")?;
    let improper_config = typing_mod
        .getattr("ImproperConfigurationError")?
        .cast_into::<PyType>()?
        .unbind();

    let core_mod = py.import("vertebra._vertebra")?;
    let get_ty = |name: &str| -> PyResult<Py<PyType>> {
        Ok(core_mod.getattr(name)?.cast_into::<PyType>()?.unbind())
    };
    Ok(RedisPyExceptions {
        improper_config,
        operational: get_ty("OperationalError")?,
        programming: get_ty("ProgrammingError")?,
        data: get_ty("DataError")?,
        interface: get_ty("InterfaceError")?,
    })
}

fn resolve_py_exceptions(py: Python<'_>) -> PyResult<&'static RedisPyExceptions> {
    REDIS_PY_EXCEPTIONS.get_or_try_init(py, || match load_py_exceptions_from_core(py) {
        Ok(types) => Ok(types),
        Err(_) => build_stand_in_py_exceptions(py),
    })
}

fn instantiate_exc(py: Python<'_>, ty: &Py<PyType>, message: &str, is_transient: bool) -> PyErr {
    match ty.bind(py).call1((message,)) {
        Ok(inst) => {
            let _ = inst.setattr("is_transient", is_transient);
            PyErr::from_value(inst)
        }
        Err(err) => err,
    }
}

/// Convert a [`RedisDriverError`] into a Python exception.
pub fn redis_error_to_py(py: Python<'_>, err: RedisDriverError) -> PyErr {
    let types = match resolve_py_exceptions(py) {
        Ok(types) => types,
        Err(_) => {
            return match err {
                RedisDriverError::Config(msg) => PyValueError::new_err(msg),
                other => PyValueError::new_err(other.to_string()),
            };
        }
    };

    match err {
        RedisDriverError::Config(msg) => types
            .improper_config
            .bind(py)
            .call1((msg,))
            .map_or_else(|e| e, PyErr::from_value),
        RedisDriverError::Operational {
            message,
            is_transient,
        } => instantiate_exc(py, &types.operational, &message, is_transient),
        RedisDriverError::Programming {
            message,
            is_transient,
        } => instantiate_exc(py, &types.programming, &message, is_transient),
        RedisDriverError::Data {
            message,
            is_transient,
        } => instantiate_exc(py, &types.data, &message, is_transient),
        RedisDriverError::Interface {
            message,
            is_transient,
        } => instantiate_exc(py, &types.interface, &message, is_transient),
    }
}
