//! Owned PostgreSQL error categories and server diagnostics.

use std::fmt;

/// Server fields retained for structured consumers, excluding diagnostics from Display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServerError {
    pub severity: String,
    pub sqlstate: String,
    pub message: String,
    pub diagnostics: Vec<(u8, String)>,
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for ch in self.severity.chars() {
            write!(f, "{}", if ch.is_control() { ' ' } else { ch })?;
        }
        write!(f, " [{}]: ", self.sqlstate)?;
        for ch in self.message.chars() {
            write!(f, "{}", if ch.is_control() { ' ' } else { ch })?;
        }
        Ok(())
    }
}

/// Native failures before Python exception mapping.
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
    /// Preserve server diagnostics after validating its SQLSTATE.
    pub fn server(
        severity: impl Into<String>,
        sqlstate: &str,
        message: impl Into<String>,
        diagnostics: Vec<(u8, String)>,
    ) -> Result<Self, Self> {
        if sqlstate.len() != 5 || !sqlstate.bytes().all(|byte| byte.is_ascii_alphanumeric()) {
            return Err(Self::Protocol("invalid server SQLSTATE".into()));
        }
        Ok(Self::Server(ServerError {
            severity: severity.into(),
            sqlstate: sqlstate.into(),
            message: message.into(),
            diagnostics,
        }))
    }

    /// Return a server-provided SQLSTATE without fabricating codes for local errors.
    pub fn sqlstate(&self) -> Option<&str> {
        match self {
            Self::Server(error) => Some(&error.sqlstate),
            _ => None,
        }
    }
}
