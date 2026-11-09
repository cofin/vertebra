//! PostgreSQL startup authentication state machine (trust, cleartext password, and MD5).

use bytes::BytesMut;
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::config::PostgresConfig;
use crate::errors::PostgresError;

#[derive(Debug, PartialEq, Eq)]
enum AuthPhase {
    Initial,
    PasswordSent,
    Complete,
}

/// Startup authentication state machine for a single connection attempt.
#[derive(Debug)]
pub struct Authentication {
    phase: AuthPhase,
}

impl Default for Authentication {
    fn default() -> Self {
        Self::new()
    }
}

impl Authentication {
    /// Create a new startup authentication state machine in `Initial` phase.
    pub fn new() -> Self {
        Self {
            phase: AuthPhase::Initial,
        }
    }

    /// Return `true` after `AuthenticationOk` has been verified.
    pub fn is_complete(&self) -> bool {
        self.phase == AuthPhase::Complete
    }

    /// Process one authentication [`Message`], appending any frontend response to `out`.
    ///
    /// Returns `Ok(true)` when `AuthenticationOk` completes the authentication exchange.
    pub fn handle_message(
        &mut self,
        msg: &Message,
        config: &PostgresConfig,
        out: &mut BytesMut,
    ) -> Result<bool, PostgresError> {
        match (&self.phase, msg) {
            (AuthPhase::Initial | AuthPhase::PasswordSent, Message::AuthenticationOk) => {
                self.phase = AuthPhase::Complete;
                Ok(true)
            }
            (AuthPhase::Complete, Message::AuthenticationOk) => Err(PostgresError::Protocol(
                "received duplicate AuthenticationOk".to_string(),
            )),
            (AuthPhase::Initial, Message::AuthenticationCleartextPassword) => {
                let password = config.password.as_deref().ok_or_else(|| {
                    PostgresError::Config(
                        "server requested cleartext password authentication, but password is None"
                            .to_string(),
                    )
                })?;
                frontend::password_message(password.as_bytes(), out).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode PasswordMessage: {e}"))
                })?;
                self.phase = AuthPhase::PasswordSent;
                Ok(false)
            }
            (AuthPhase::Initial, Message::AuthenticationMd5Password(body)) => {
                let password = config.password.as_deref().ok_or_else(|| {
                    PostgresError::Config(
                        "server requested MD5 password authentication, but password is None"
                            .to_string(),
                    )
                })?;
                let digest = md5_hash(config.user.as_bytes(), password.as_bytes(), body.salt());
                frontend::password_message(digest.as_bytes(), out).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode MD5 PasswordMessage: {e}"))
                })?;
                self.phase = AuthPhase::PasswordSent;
                Ok(false)
            }
            (
                AuthPhase::PasswordSent | AuthPhase::Complete,
                Message::AuthenticationCleartextPassword | Message::AuthenticationMd5Password(_),
            ) => Err(PostgresError::Protocol(
                "received unexpected second password challenge after PasswordMessage".to_string(),
            )),
            (
                _,
                Message::AuthenticationKerberosV5
                | Message::AuthenticationScmCredential
                | Message::AuthenticationGss
                | Message::AuthenticationSspi
                | Message::AuthenticationGssContinue(_)
                | Message::AuthenticationSasl(_)
                | Message::AuthenticationSaslContinue(_)
                | Message::AuthenticationSaslFinal(_),
            ) => Err(PostgresError::UnsupportedAuth(
                "unsupported PostgreSQL authentication mechanism requested by server".to_string(),
            )),
            _ => Err(PostgresError::Protocol(
                "unexpected backend frame during startup authentication".to_string(),
            )),
        }
    }
}
