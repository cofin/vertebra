//! RESP error classification and mapping onto canonical `vertebra._vertebra` exceptions.

#[cfg(feature = "pyo3")]
use pyo3::exceptions::PyRuntimeError;
#[cfg(feature = "pyo3")]
use pyo3::prelude::*;
use thiserror::Error;
#[cfg(feature = "pyo3")]
use vertebra_db::py_errors::{instantiate_db_exc, resolve_db_exceptions};

/// Canonical exception category mapped onto `vertebra._vertebra` or `vertebra.typing`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespErrorKind {
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

/// Backward-compatible alias for Valkey callers.
pub type ValkeyErrorKind = RespErrorKind;

impl RespErrorKind {
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

/// Classify a RESP2/RESP3 server error line into [`RespErrorKind`].
#[must_use]
pub fn classify_server_error(raw_message: &str) -> RespErrorKind {
    let trimmed = raw_message.trim().trim_start_matches('-').trim();
    let first_token = trimmed
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_ascii_uppercase();
    let lower = trimmed.to_ascii_lowercase();

    match first_token.as_str() {
        "WRONGPASS" | "NOAUTH" | "NOPERM" | "UNAUTH" => RespErrorKind::Operational {
            is_transient: false,
        },
        "READONLY" | "MASTERDOWN" | "CLUSTERDOWN" | "LOADING" | "TRYAGAIN" | "BUSY" | "MOVED"
        | "ASK" => RespErrorKind::Operational { is_transient: true },
        "WRONGTYPE" | "BUSYKEY" | "BUSYGROUP" | "CROSSSLOT" | "NOSCRIPT" | "NOGROUP" => {
            RespErrorKind::Programming
        }
        "EXECABORT" | "MISCONF" | "OOM" => RespErrorKind::Operational {
            is_transient: false,
        },
        "ERR" => classify_generic_err(&lower),
        _ => {
            if lower.contains("wrongpass") || lower.contains("noauth") {
                RespErrorKind::Operational {
                    is_transient: false,
                }
            } else if lower.contains("readonly")
                || lower.contains("masterdown")
                || lower.contains("clusterdown")
                || lower.contains("loading")
                || lower.contains("tryagain")
            {
                RespErrorKind::Operational { is_transient: true }
            } else if lower.contains("wrongtype")
                || lower.contains("busykey")
                || lower.contains("crossslot")
                || lower.contains("noscript")
                || lower.contains("syntax")
            {
                RespErrorKind::Programming
            } else {
                RespErrorKind::Operational {
                    is_transient: false,
                }
            }
        }
    }
}

fn classify_generic_err(lower: &str) -> RespErrorKind {
    if lower.contains("syntax")
        || lower.contains("wrong number of arguments")
        || lower.contains("unknown command")
        || lower.contains("unknown subcommand")
        || lower.contains("unsupported")
        || lower.contains("crossslot")
        || lower.contains("invalid argument")
    {
        RespErrorKind::Programming
    } else if lower.contains("not an integer")
        || lower.contains("not a valid float")
        || lower.contains("out of range")
        || lower.contains("value is not valid")
        || lower.contains("invalid stream id")
    {
        RespErrorKind::Data
    } else {
        RespErrorKind::Operational {
            is_transient: false,
        }
    }
}

/// Structured error produced by `vertebra-resp`.
#[derive(Debug, Clone, Error)]
#[error("{message}")]
pub struct RespError {
    pub kind: RespErrorKind,
    pub message: String,
    pub code: Option<String>,
}

/// Backward-compatible alias for Valkey callers.
pub type ValkeyError = RespError;

impl RespError {
    #[must_use]
    pub fn new(kind: RespErrorKind, message: impl Into<String>, code: Option<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            code,
        }
    }

    #[must_use]
    pub fn config(message: impl Into<String>) -> Self {
        Self::new(RespErrorKind::ImproperConfiguration, message, None)
    }

    #[must_use]
    pub fn interface(message: impl Into<String>) -> Self {
        Self::new(RespErrorKind::Interface, message, None)
    }

    #[must_use]
    pub fn data(message: impl Into<String>) -> Self {
        Self::new(RespErrorKind::Data, message, None)
    }

    #[must_use]
    pub fn operational(message: impl Into<String>, is_transient: bool) -> Self {
        Self::new(RespErrorKind::Operational { is_transient }, message, None)
    }

    #[must_use]
    pub fn connection(message: impl Into<String>) -> Self {
        Self::new(RespErrorKind::ConnectionFailure, message, None)
    }

    #[must_use]
    pub fn programming(message: impl Into<String>) -> Self {
        Self::new(RespErrorKind::Programming, message, None)
    }

    #[must_use]
    pub fn not_supported(message: impl Into<String>) -> Self {
        Self::new(RespErrorKind::NotSupported, message, None)
    }

    #[must_use]
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(RespErrorKind::QueryCancelled, message, None)
    }

    #[must_use]
    pub fn gcp_auth(message: impl Into<String>, is_transient: bool) -> Self {
        Self::new(
            RespErrorKind::GoogleCloudAuth { is_transient },
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

    /// Returns whether this error indicates authentication failure (`WRONGPASS`/`NOAUTH`/`NOPERM`).
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

    /// Convert into the canonical Python exception via `vertebra_db::py_errors`.
    #[cfg(feature = "pyo3")]
    pub fn to_py_err(&self, py: Python<'_>) -> PyErr {
        let Ok(types) = resolve_db_exceptions(py) else {
            return PyRuntimeError::new_err(self.message.clone());
        };
        if self.kind == RespErrorKind::ImproperConfiguration {
            return match types
                .improper_configuration
                .bind(py)
                .call1((self.message.as_str(),))
            {
                Ok(instance) => PyErr::from_value(instance),
                Err(_) => PyRuntimeError::new_err(self.message.clone()),
            };
        }
        let ty = match self.kind {
            RespErrorKind::ImproperConfiguration => unreachable!(),
            RespErrorKind::Interface => &types.interface,
            RespErrorKind::Data => &types.data,
            RespErrorKind::Operational { .. } => &types.operational,
            RespErrorKind::ConnectionFailure => &types.connection_failure,
            RespErrorKind::QueryCancelled => &types.query_cancelled,
            RespErrorKind::Integrity => &types.integrity,
            RespErrorKind::Programming => &types.programming,
            RespErrorKind::NotSupported => &types.not_supported,
            RespErrorKind::GoogleCloudAuth { .. } => &types.google_cloud_auth,
        };
        instantiate_db_exc(py, ty, &self.message, None, None, self.kind.is_transient())
    }
}

impl From<std::io::Error> for RespError {
    fn from(err: std::io::Error) -> Self {
        Self::connection(format!("RESP socket I/O error: {err}"))
    }
}

#[cfg(feature = "pyo3")]
impl From<RespError> for PyErr {
    fn from(err: RespError) -> Self {
        Python::attach(|py| err.to_py_err(py))
    }
}
