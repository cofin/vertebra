//! Valkey error classification and mapping onto canonical `vertebra._vertebra` exceptions.

use std::collections::HashMap;
use std::ffi::CString;

use pyo3::PyTypeInfo;
use pyo3::exceptions::{PyException, PyRuntimeError};
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyDict, PyType};
use thiserror::Error;

/// Canonical exception category mapped onto `vertebra._vertebra` or `vertebra.typing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValkeyErrorKind {
    ImproperConfiguration,
    Interface,
    Data,
    Operational { is_transient: bool },
    ConnectionFailure,
    QueryCancelled,
    Integrity,
    Programming,
    NotSupported,
    GoogleCloudAuth { is_transient: bool },
}

impl ValkeyErrorKind {
    /// Returns the Python exception class name for this category.
    #[must_use]
    pub const fn python_exception_name(self) -> &'static str {
        match self {
            Self::ImproperConfiguration => "ImproperConfigurationError",
            Self::Interface => "InterfaceError",
            Self::Data => "DataError",
            Self::Operational { .. } => "OperationalError",
            Self::ConnectionFailure => "ConnectionFailure",
            Self::QueryCancelled => "QueryCancelledError",
            Self::Integrity => "IntegrityError",
            Self::Programming => "ProgrammingError",
            Self::NotSupported => "NotSupportedError",
            Self::GoogleCloudAuth { .. } => "GoogleCloudAuthError",
        }
    }

    /// Returns whether callers can safely treat this failure as transient for retry decisions.
    #[must_use]
    pub const fn is_transient(self) -> bool {
        match self {
            Self::Operational { is_transient } | Self::GoogleCloudAuth { is_transient } => {
                is_transient
            }
            Self::ConnectionFailure => true,
            Self::ImproperConfiguration
            | Self::Interface
            | Self::Data
            | Self::QueryCancelled
            | Self::Integrity
            | Self::Programming
            | Self::NotSupported => false,
        }
    }
}

/// Classify a RESP2/RESP3 server error line into [`ValkeyErrorKind`].
#[must_use]
pub fn classify_server_error(raw_message: &str) -> ValkeyErrorKind {
    let trimmed = raw_message.trim().trim_start_matches('-').trim();
    let first_token = trimmed
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let lower = trimmed.to_ascii_lowercase();

    match first_token.as_str() {
        "WRONGPASS" | "NOAUTH" | "NOPERM" | "UNAUTH" => ValkeyErrorKind::Operational {
            is_transient: false,
        },
        "READONLY" | "MASTERDOWN" | "CLUSTERDOWN" | "LOADING" | "TRYAGAIN" | "BUSY" | "MOVED"
        | "ASK" => ValkeyErrorKind::Operational { is_transient: true },
        "WRONGTYPE" | "BUSYKEY" | "BUSYGROUP" | "CROSSSLOT" | "NOSCRIPT" | "NOGROUP" => {
            ValkeyErrorKind::Programming
        }
        "EXECABORT" | "MISCONF" | "OOM" => ValkeyErrorKind::Operational {
            is_transient: false,
        },
        "ERR" => classify_generic_err(&lower),
        _ => {
            if lower.contains("wrongpass") || lower.contains("noauth") {
                ValkeyErrorKind::Operational {
                    is_transient: false,
                }
            } else if lower.contains("readonly")
                || lower.contains("masterdown")
                || lower.contains("clusterdown")
                || lower.contains("loading")
                || lower.contains("tryagain")
            {
                ValkeyErrorKind::Operational { is_transient: true }
            } else if lower.contains("wrongtype")
                || lower.contains("busykey")
                || lower.contains("crossslot")
                || lower.contains("noscript")
                || lower.contains("syntax")
            {
                ValkeyErrorKind::Programming
            } else {
                ValkeyErrorKind::Operational {
                    is_transient: false,
                }
            }
        }
    }
}

fn classify_generic_err(lower: &str) -> ValkeyErrorKind {
    if lower.contains("syntax")
        || lower.contains("wrong number of arguments")
        || lower.contains("unknown command")
        || lower.contains("unknown subcommand")
        || lower.contains("unsupported")
        || lower.contains("crossslot")
        || lower.contains("invalid argument")
    {
        ValkeyErrorKind::Programming
    } else if lower.contains("not an integer")
        || lower.contains("not a valid float")
        || lower.contains("out of range")
        || lower.contains("value is not valid")
        || lower.contains("invalid stream id")
    {
        ValkeyErrorKind::Data
    } else {
        ValkeyErrorKind::Operational {
            is_transient: false,
        }
    }
}

/// Structured error produced by `vertebra-valkey`.
#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct ValkeyError {
    pub kind: ValkeyErrorKind,
    pub message: String,
    pub code: Option<String>,
}

impl ValkeyError {
    #[must_use]
    pub fn new(kind: ValkeyErrorKind, message: impl Into<String>, code: Option<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code,
        }
    }

    #[must_use]
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(ValkeyErrorKind::ImproperConfiguration, message, None)
    }

    #[must_use]
    pub fn interface(message: impl Into<String>) -> Self {
        Self::new(ValkeyErrorKind::Interface, message, None)
    }

    #[must_use]
    pub fn data(message: impl Into<String>) -> Self {
        Self::new(ValkeyErrorKind::Data, message, None)
    }

    #[must_use]
    pub fn operational(message: impl Into<String>, is_transient: bool) -> Self {
        Self::new(ValkeyErrorKind::Operational { is_transient }, message, None)
    }

    #[must_use]
    pub fn connection(message: impl Into<String>) -> Self {
        Self::new(ValkeyErrorKind::ConnectionFailure, message, None)
    }

    #[must_use]
    pub fn programming(message: impl Into<String>) -> Self {
        Self::new(ValkeyErrorKind::Programming, message, None)
    }

    #[must_use]
    pub fn not_supported(message: impl Into<String>) -> Self {
        Self::new(ValkeyErrorKind::NotSupported, message, None)
    }

    #[must_use]
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ValkeyErrorKind::QueryCancelled, message, None)
    }

    #[must_use]
    pub fn gcp_auth(message: impl Into<String>, is_transient: bool) -> Self {
        Self::new(
            ValkeyErrorKind::GoogleCloudAuth { is_transient },
            message,
            None,
        )
    }

    #[must_use]
    pub fn from_server_error(raw_message: impl Into<String>) -> Self {
        let message = raw_message.into();
        let trimmed = message.trim().trim_start_matches('-').trim();
        let code = trimmed
            .split_whitespace()
            .next()
            .filter(|s| !s.is_empty())
            .map(str::to_ascii_uppercase);
        let kind = classify_server_error(trimmed);
        Self {
            kind,
            message: trimmed.to_string(),
            code,
        }
    }

    /// Returns whether this error is a Cluster redirection (`MOVED` or `ASK`).
    #[must_use]
    pub fn is_cluster_redirect(&self) -> bool {
        matches!(self.code.as_deref(), Some("MOVED" | "ASK"))
    }

    /// Returns whether this error indicates authentication failure (`WRONGPASS`/`NOAUTH`).
    #[must_use]
    pub fn is_auth_error(&self) -> bool {
        matches!(
            self.code.as_deref(),
            Some("WRONGPASS" | "NOAUTH" | "NOPERM")
        )
    }

    /// Returns whether this error indicates missing Lua SHA (`NOSCRIPT`).
    #[must_use]
    pub fn is_noscript(&self) -> bool {
        self.code.as_deref() == Some("NOSCRIPT")
    }

    /// Convert into the canonical Python exception on `vertebra._vertebra` or `vertebra.typing`.
    pub fn to_py_err(&self, py: Python<'_>) -> PyErr {
        if self.kind == ValkeyErrorKind::ImproperConfiguration {
            let cls = resolve_improper_configuration_class(py);
            return match cls.call1((self.message.as_str(),)) {
                Ok(instance) => PyErr::from_value(instance),
                Err(_) => PyRuntimeError::new_err(self.message.clone()),
            };
        }

        let exc_name = self.kind.python_exception_name();
        let cls = resolve_database_error_class(py, exc_name);
        let instance_res = cls.call1((self.message.as_str(),));
        match instance_res {
            Ok(instance) => {
                let _ = instance.setattr("is_transient", self.kind.is_transient());
                let _ = instance.setattr("sqlstate", py.None());
                let _ = instance.setattr("vendor_code", py.None());
                PyErr::from_value(instance)
            }
            Err(_) => PyRuntimeError::new_err(self.message.clone()),
        }
    }
}

impl From<std::io::Error> for ValkeyError {
    fn from(err: std::io::Error) -> Self {
        Self::connection(format!("Valkey socket I/O error: {err}"))
    }
}

impl From<ValkeyError> for PyErr {
    fn from(err: ValkeyError) -> Self {
        Python::attach(|py| err.to_py_err(py))
    }
}

static STANDIN_ERRORS: PyOnceLock<HashMap<String, Py<PyType>>> = PyOnceLock::new();

fn resolve_improper_configuration_class<'py>(py: Python<'py>) -> Bound<'py, PyType> {
    if let Ok(typing_mod) = py.import("vertebra.typing")
        && let Ok(attr) = typing_mod.getattr("ImproperConfigurationError")
        && let Ok(cls) = attr.cast::<PyType>()
    {
        return cls.clone();
    }
    standin_class(py, "ImproperConfigurationError")
}

fn resolve_database_error_class<'py>(py: Python<'py>, exc_name: &str) -> Bound<'py, PyType> {
    if let Ok(ext_mod) = py.import("vertebra._vertebra")
        && let Ok(attr) = ext_mod.getattr(exc_name)
        && let Ok(cls) = attr.cast::<PyType>()
    {
        return cls.clone();
    }
    standin_class(py, exc_name)
}

fn standin_class<'py>(py: Python<'py>, name: &str) -> Bound<'py, PyType> {
    let map = STANDIN_ERRORS.get_or_init(py, || build_standin_errors(py));
    if let Some(ty) = map.get(name) {
        return ty.bind(py).clone();
    }
    PyException::type_object(py)
}

fn build_standin_errors(py: Python<'_>) -> HashMap<String, Py<PyType>> {
    let mut map = HashMap::new();
    let builtins = match py.import("builtins") {
        Ok(b) => b,
        Err(_) => return map,
    };
    let exc_base = PyException::type_object(py);

    let db_err = make_exc_type(py, &builtins, "DatabaseError", &exc_base);
    let op_err = make_exc_type(py, &builtins, "OperationalError", &db_err);
    let conn_err = make_exc_type(py, &builtins, "ConnectionFailure", &op_err);
    let cancel_err = make_exc_type(py, &builtins, "QueryCancelledError", &op_err);
    let gcp_err = make_exc_type(py, &builtins, "GoogleCloudAuthError", &op_err);
    let iface_err = make_exc_type(py, &builtins, "InterfaceError", &db_err);
    let data_err = make_exc_type(py, &builtins, "DataError", &db_err);
    let integ_err = make_exc_type(py, &builtins, "IntegrityError", &db_err);
    let prog_err = make_exc_type(py, &builtins, "ProgrammingError", &db_err);
    let notsup_err = make_exc_type(py, &builtins, "NotSupportedError", &db_err);
    let cfg_err = make_exc_type(py, &builtins, "ImproperConfigurationError", &exc_base);

    for (name, ty) in [
        ("DatabaseError", db_err),
        ("OperationalError", op_err),
        ("ConnectionFailure", conn_err),
        ("QueryCancelledError", cancel_err),
        ("GoogleCloudAuthError", gcp_err),
        ("InterfaceError", iface_err),
        ("DataError", data_err),
        ("IntegrityError", integ_err),
        ("ProgrammingError", prog_err),
        ("NotSupportedError", notsup_err),
        ("ImproperConfigurationError", cfg_err),
    ] {
        map.insert(name.to_string(), ty.unbind());
    }
    map
}

fn make_exc_type<'py>(
    py: Python<'py>,
    builtins: &Bound<'py, PyModule>,
    name: &str,
    base: &Bound<'py, PyType>,
) -> Bound<'py, PyType> {
    let ns = PyDict::new(py);
    let _ = ns.set_item("is_transient", false);
    let _ = ns.set_item("sqlstate", py.None());
    let _ = ns.set_item("vendor_code", py.None());
    let _ = ns.set_item("__module__", "vertebra._vertebra");
    if let Ok(cls_obj) = builtins.call_method1("type", (name, (base,), ns))
        && let Ok(ty) = cls_obj.cast::<PyType>()
    {
        return ty.clone();
    }
    let _ = CString::new(name);
    PyException::type_object(py)
}
