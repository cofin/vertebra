//! Ordered trust, password, and SCRAM startup authentication.

use bytes::BytesMut;
use postgres_protocol::{
    authentication::{
        md5_hash,
        sasl::{ChannelBinding, SCRAM_SHA_256, SCRAM_SHA_256_PLUS, ScramSha256},
    },
    message::{
        backend::Message,
        frontend::{password_message, sasl_initial_response, sasl_response},
    },
};

use crate::{
    config::{ChannelBindingPolicy, PostgresConfig},
    errors::PostgresError,
};

enum AuthPhase {
    Initial,
    PasswordSent,
    ScramStarted {
        scram: ScramSha256,
        client_nonce: String,
    },
    ScramFinalSent(ScramSha256),
    ScramVerifiedAwaitingOk,
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
        matches!(self.phase, AuthPhase::Complete)
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
        tls_server_end_point: Option<&[u8]>,
        out: &mut BytesMut,
    ) -> Result<bool, PostgresError> {
        if config.channel_binding == ChannelBindingPolicy::Require
            && (tls_server_end_point.is_none()
                || (matches!(self.phase, AuthPhase::Initial)
                    && matches!(
                        msg,
                        Message::AuthenticationOk
                            | Message::AuthenticationCleartextPassword
                            | Message::AuthenticationMd5Password(_)
                    )))
        {
            return Err(PostgresError::UnsupportedAuth(
                "server authentication requires channel binding".into(),
            ));
        }
        match msg {
            Message::AuthenticationOk => {
                if !matches!(
                    self.phase,
                    AuthPhase::Initial
                        | AuthPhase::PasswordSent
                        | AuthPhase::ScramVerifiedAwaitingOk
                ) {
                    return Err(PostgresError::Protocol(
                        "unexpected authentication completion".into(),
                    ));
                }
                self.phase = AuthPhase::Complete;
            }
            Message::AuthenticationCleartextPassword | Message::AuthenticationMd5Password(_) => {
                if !matches!(self.phase, AuthPhase::Initial) {
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
            Message::AuthenticationSasl(body) => {
                if !matches!(self.phase, AuthPhase::Initial) {
                    return Err(PostgresError::Protocol(
                        "unexpected SASL challenge order".into(),
                    ));
                }
                use fallible_iterator::FallibleIterator;
                let mut mechanisms = body.mechanisms();
                let mut has_plain = false;
                let mut has_plus = false;
                let mut has_mechanism = false;
                while let Some(mechanism) = mechanisms
                    .next()
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?
                {
                    has_mechanism = true;
                    has_plain |= mechanism == SCRAM_SHA_256;
                    has_plus |= mechanism == SCRAM_SHA_256_PLUS;
                }
                if !has_mechanism {
                    return Err(PostgresError::Protocol("empty SASL mechanism list".into()));
                }
                let (mechanism, binding) = match (config.channel_binding, tls_server_end_point) {
                    (
                        ChannelBindingPolicy::Prefer | ChannelBindingPolicy::Require,
                        Some(digest),
                    ) if has_plus => (
                        SCRAM_SHA_256_PLUS,
                        ChannelBinding::tls_server_end_point(digest.to_vec()),
                    ),
                    (ChannelBindingPolicy::Prefer, Some(_)) if has_plain => {
                        (SCRAM_SHA_256, ChannelBinding::unrequested())
                    }
                    (ChannelBindingPolicy::Disable | ChannelBindingPolicy::Prefer, _)
                        if has_plain =>
                    {
                        (SCRAM_SHA_256, ChannelBinding::unsupported())
                    }
                    _ => {
                        return Err(PostgresError::UnsupportedAuth(
                            "server SASL mechanism is unavailable for channel binding policy"
                                .into(),
                        ));
                    }
                };
                let password = config.password.as_deref().ok_or_else(|| {
                    PostgresError::Config("password required by server authentication".into())
                })?;
                let scram = ScramSha256::new(password.as_bytes(), binding);
                let client_nonce = std::str::from_utf8(scram.message())
                    .ok()
                    .and_then(|message| message.rsplit_once(",r="))
                    .map(|(_, nonce)| nonce.to_owned())
                    .ok_or_else(|| {
                        PostgresError::Protocol("invalid initial SCRAM message".into())
                    })?;
                sasl_initial_response(mechanism, scram.message(), out)
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
                self.phase = AuthPhase::ScramStarted {
                    scram,
                    client_nonce,
                };
            }
            Message::AuthenticationSaslContinue(body) => {
                let AuthPhase::ScramStarted {
                    mut scram,
                    client_nonce,
                } = std::mem::replace(&mut self.phase, AuthPhase::Complete)
                else {
                    return Err(PostgresError::Protocol(
                        "unexpected SASL continuation order".into(),
                    ));
                };
                let message = std::str::from_utf8(body.data())
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
                let mut counts = message
                    .split(',')
                    .filter_map(|attribute| attribute.strip_prefix("i="));
                let count = counts
                    .next()
                    .filter(|value| {
                        !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit())
                    })
                    .and_then(|value| value.parse::<u32>().ok());
                if !count.is_some_and(|value| (1..=100_000).contains(&value))
                    || counts.next().is_some()
                {
                    return Err(PostgresError::Protocol(
                        "invalid SCRAM iteration count".into(),
                    ));
                }
                let nonce = message
                    .split(',')
                    .find_map(|attribute| attribute.strip_prefix("r="));
                if !nonce.is_some_and(|nonce| {
                    nonce.len() > client_nonce.len() && nonce.starts_with(&client_nonce)
                }) {
                    return Err(PostgresError::Protocol("invalid SCRAM server nonce".into()));
                }
                scram
                    .update(body.data())
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
                sasl_response(scram.message(), out)
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
                self.phase = AuthPhase::ScramFinalSent(scram);
            }
            Message::AuthenticationSaslFinal(body) => {
                let AuthPhase::ScramFinalSent(mut scram) =
                    std::mem::replace(&mut self.phase, AuthPhase::Complete)
                else {
                    return Err(PostgresError::Protocol(
                        "unexpected SASL final order".into(),
                    ));
                };
                scram
                    .finish(body.data())
                    .map_err(|error| PostgresError::Protocol(error.to_string()))?;
                self.phase = AuthPhase::ScramVerifiedAwaitingOk;
            }
            Message::AuthenticationKerberosV5
            | Message::AuthenticationScmCredential
            | Message::AuthenticationGss
            | Message::AuthenticationSspi
            | Message::AuthenticationGssContinue(_) => {
                return Err(PostgresError::UnsupportedAuth(
                    "server authentication mechanism is unavailable".into(),
                ));
            }
            _ => return Ok(false),
        }
        Ok(true)
    }
}
