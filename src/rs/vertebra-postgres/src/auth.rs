//! Ordered trust, cleartext, and MD5 startup authentication.

use bytes::BytesMut;
use postgres_protocol::{
    authentication::md5_hash,
    message::{backend::Message, frontend::password_message},
};

use crate::{config::PostgresConfig, errors::PostgresError};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuthPhase {
    Initial,
    PasswordSent,
    Complete,
}

pub(crate) struct Authentication {
    phase: AuthPhase,
    md5_response: Option<String>,
}

impl Authentication {
    pub(crate) fn new() -> Self {
        Self {
            phase: AuthPhase::Initial,
            md5_response: None,
        }
    }

    pub(crate) fn is_complete(&self) -> bool {
        self.phase == AuthPhase::Complete
    }

    pub(crate) fn redact_server_text(&self, text: &str, config: &PostgresConfig) -> String {
        let mut redacted = text.to_owned();
        if let Some(response) = &self.md5_response {
            redacted = redacted.replace(response, "<redacted>");
        }
        if let Some(password) = config.password.as_deref().filter(|value| !value.is_empty()) {
            redacted = redacted.replace(password, "<redacted>");
        }
        redacted
    }

    pub(crate) fn handle_message(
        &mut self,
        msg: &Message,
        config: &PostgresConfig,
        out: &mut BytesMut,
    ) -> Result<bool, PostgresError> {
        match msg {
            Message::AuthenticationOk => {
                if self.is_complete() {
                    return Err(PostgresError::Protocol(
                        "duplicate authentication completion".into(),
                    ));
                }
                self.phase = AuthPhase::Complete;
            }
            Message::AuthenticationCleartextPassword | Message::AuthenticationMd5Password(_) => {
                if self.phase != AuthPhase::Initial {
                    return Err(PostgresError::Protocol(
                        "unexpected authentication challenge order".into(),
                    ));
                }
                let password = config.password.as_deref().ok_or_else(|| {
                    PostgresError::Config("password required by server authentication".into())
                })?;
                if let Message::AuthenticationMd5Password(body) = msg {
                    let digest = md5_hash(config.user.as_bytes(), password.as_bytes(), body.salt());
                    password_message(digest.as_bytes(), out).map_err(|_| {
                        PostgresError::Config("invalid authentication password".into())
                    })?;
                    self.md5_response = Some(digest);
                } else {
                    password_message(password.as_bytes(), out).map_err(|_| {
                        PostgresError::Config("invalid authentication password".into())
                    })?;
                }
                self.phase = AuthPhase::PasswordSent;
            }
            Message::AuthenticationKerberosV5
            | Message::AuthenticationScmCredential
            | Message::AuthenticationGss
            | Message::AuthenticationSspi
            | Message::AuthenticationGssContinue(_)
            | Message::AuthenticationSasl(_) => {
                return Err(PostgresError::UnsupportedAuth(
                    "server authentication mechanism is unavailable".into(),
                ));
            }
            Message::AuthenticationSaslContinue(_) | Message::AuthenticationSaslFinal(_) => {
                return Err(PostgresError::Protocol(
                    "unexpected SASL authentication continuation".into(),
                ));
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
