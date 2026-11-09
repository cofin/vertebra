//! Structured PostgreSQL error categories, SQLSTATE retention, and diagnostic redaction.

use std::fmt;

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
        write!(
            f,
            "{} ({}): {}",
            self.severity, self.sqlstate, self.message
        )
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
