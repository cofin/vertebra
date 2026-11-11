//! PostgreSQL startup authentication state machine (trust, cleartext password, MD5, SCRAM-SHA-256, and SCRAM-SHA-256-PLUS).

use bytes::BytesMut;
use fallible_iterator::FallibleIterator;
use postgres_protocol::authentication::md5_hash;
use postgres_protocol::authentication::sasl::{ChannelBinding, ScramSha256};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;

use crate::cloud::material::{Clock, SystemClock};
use crate::config::{ChannelBindingPolicy, PostgresConfig};
use crate::credentials::ScopedLoginSnapshot;
use crate::errors::PostgresError;

/// Maximum permitted SCRAM-SHA-256 PBKDF2 iteration count (`100_000`).
pub const MAX_SCRAM_ITERATIONS: u32 = 100_000;

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

impl std::fmt::Debug for AuthPhase {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Initial => write!(f, "Initial"),
            Self::PasswordSent => write!(f, "PasswordSent"),
            Self::ScramStarted { client_nonce, .. } => f
                .debug_struct("ScramStarted")
                .field("client_nonce", client_nonce)
                .finish_non_exhaustive(),
            Self::ScramFinalSent(_) => write!(f, "ScramFinalSent(..)"),
            Self::ScramVerifiedAwaitingOk => write!(f, "ScramVerifiedAwaitingOk"),
            Self::Complete => write!(f, "Complete"),
        }
    }
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
    pub const fn is_complete(&self) -> bool {
        matches!(self.phase, AuthPhase::Complete)
    }

    /// Process one authentication [`Message`], appending any frontend response to `out`.
    ///
    /// Returns `Ok(true)` when `AuthenticationOk` completes the authentication exchange.
    pub fn handle_message(
        &mut self,
        msg: &Message,
        config: &PostgresConfig,
        tls_server_end_point: Option<&[u8]>,
        out: &mut BytesMut,
    ) -> Result<bool, PostgresError> {
        self.handle_message_with_login(msg, config, tls_server_end_point, None, &SystemClock, out)
    }

    /// Process one authentication [`Message`] with an optional pre-resolved [`ScopedLoginSnapshot`],
    /// verifying `ScopedLoginSnapshot::ensure_usable` immediately before emitting password or SASL bytes.
    pub(crate) fn handle_message_with_login(
        &mut self,
        msg: &Message,
        config: &PostgresConfig,
        tls_server_end_point: Option<&[u8]>,
        login_snapshot: Option<&ScopedLoginSnapshot>,
        clock: &dyn Clock,
        out: &mut BytesMut,
    ) -> Result<bool, PostgresError> {
        let resolve_effective_password = || -> Result<&str, PostgresError> {
            if let Some(snap) = login_snapshot {
                snap.ensure_usable(clock.now_unix_ms())?;
                return Ok(snap.access_token());
            }
            config.password.as_deref().ok_or_else(|| {
                PostgresError::Config(
                    "server requested password authentication, but password is None".to_string(),
                )
            })
        };

        let phase = std::mem::replace(&mut self.phase, AuthPhase::Initial);
        match (phase, msg) {
            (AuthPhase::Initial | AuthPhase::PasswordSent, Message::AuthenticationOk) => {
                if config.channel_binding == ChannelBindingPolicy::Require {
                    return Err(PostgresError::UnsupportedAuth(
                        "channel_binding='require' requires SCRAM-SHA-256-PLUS authentication"
                            .to_string(),
                    ));
                }
                self.phase = AuthPhase::Complete;
                Ok(true)
            }
            (AuthPhase::ScramVerifiedAwaitingOk, Message::AuthenticationOk) => {
                self.phase = AuthPhase::Complete;
                Ok(true)
            }
            (
                AuthPhase::ScramStarted { .. } | AuthPhase::ScramFinalSent(_),
                Message::AuthenticationOk,
            ) => Err(PostgresError::Protocol(
                "received AuthenticationOk before AuthenticationSASLFinal verification".to_string(),
            )),
            (AuthPhase::Complete, Message::AuthenticationOk) => Err(PostgresError::Protocol(
                "received duplicate AuthenticationOk".to_string(),
            )),
            (AuthPhase::Initial, Message::AuthenticationCleartextPassword) => {
                if config.channel_binding == ChannelBindingPolicy::Require {
                    return Err(PostgresError::UnsupportedAuth(
                        "channel_binding='require' rejected cleartext password challenge"
                            .to_string(),
                    ));
                }
                let password = resolve_effective_password()?;
                frontend::password_message(password.as_bytes(), out).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode PasswordMessage: {e}"))
                })?;
                self.phase = AuthPhase::PasswordSent;
                Ok(false)
            }
            (AuthPhase::Initial, Message::AuthenticationMd5Password(body)) => {
                if config.channel_binding == ChannelBindingPolicy::Require {
                    return Err(PostgresError::UnsupportedAuth(
                        "channel_binding='require' rejected MD5 password challenge".to_string(),
                    ));
                }
                let password = resolve_effective_password()?;
                let digest = md5_hash(config.user.as_bytes(), password.as_bytes(), body.salt());
                frontend::password_message(digest.as_bytes(), out).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode MD5 PasswordMessage: {e}"))
                })?;
                self.phase = AuthPhase::PasswordSent;
                Ok(false)
            }
            (AuthPhase::Initial, Message::AuthenticationSasl(body)) => {
                let mut has_scram_sha256 = false;
                let mut has_scram_sha256_plus = false;
                let mut mechs = body.mechanisms();
                while let Some(mech) = mechs.next().map_err(|e| {
                    PostgresError::Protocol(format!(
                        "invalid AuthenticationSASL mechanism list: {e}"
                    ))
                })? {
                    match mech {
                        "SCRAM-SHA-256" => has_scram_sha256 = true,
                        "SCRAM-SHA-256-PLUS" => has_scram_sha256_plus = true,
                        _ => {}
                    }
                }

                let (mechanism, channel_binding) = match config.channel_binding {
                    ChannelBindingPolicy::Require => {
                        let end_point = tls_server_end_point.ok_or_else(|| {
                            PostgresError::UnsupportedAuth(
                                "channel_binding='require' requires TLS with a bindable peer leaf certificate"
                                    .to_string(),
                            )
                        })?;
                        if !has_scram_sha256_plus {
                            return Err(PostgresError::UnsupportedAuth(
                                "channel_binding='require' requires server to advertise SCRAM-SHA-256-PLUS"
                                    .to_string(),
                            ));
                        }
                        (
                            "SCRAM-SHA-256-PLUS",
                            ChannelBinding::tls_server_end_point(end_point.to_vec()),
                        )
                    }
                    ChannelBindingPolicy::Prefer => {
                        if let Some(end_point) = tls_server_end_point {
                            if has_scram_sha256_plus {
                                (
                                    "SCRAM-SHA-256-PLUS",
                                    ChannelBinding::tls_server_end_point(end_point.to_vec()),
                                )
                            } else if has_scram_sha256 {
                                ("SCRAM-SHA-256", ChannelBinding::unrequested())
                            } else {
                                return Err(PostgresError::UnsupportedAuth(
                                    "server did not advertise SCRAM-SHA-256 or SCRAM-SHA-256-PLUS"
                                        .to_string(),
                                ));
                            }
                        } else if has_scram_sha256 {
                            ("SCRAM-SHA-256", ChannelBinding::unsupported())
                        } else {
                            return Err(PostgresError::UnsupportedAuth(
                                "server did not advertise SCRAM-SHA-256 on unbound transport"
                                    .to_string(),
                            ));
                        }
                    }
                    ChannelBindingPolicy::Disable => {
                        if has_scram_sha256 {
                            ("SCRAM-SHA-256", ChannelBinding::unsupported())
                        } else {
                            return Err(PostgresError::UnsupportedAuth(
                                "channel_binding='disable' requires server to advertise SCRAM-SHA-256"
                                    .to_string(),
                            ));
                        }
                    }
                };

                let password = resolve_effective_password()?;

                let scram = ScramSha256::new(password.as_bytes(), channel_binding);
                let initial_msg = std::str::from_utf8(scram.message()).map_err(|_| {
                    PostgresError::Protocol(
                        "SCRAM client-first-message is not valid UTF-8".to_string(),
                    )
                })?;
                let client_nonce = initial_msg
                    .rsplit_once(",r=")
                    .map(|(_, nonce)| nonce.to_string())
                    .ok_or_else(|| {
                        PostgresError::Protocol(
                            "failed to extract client nonce from SCRAM client-first-message"
                                .to_string(),
                        )
                    })?;

                frontend::sasl_initial_response(mechanism, scram.message(), out).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode SASLInitialResponse: {e}"))
                })?;
                self.phase = AuthPhase::ScramStarted {
                    scram,
                    client_nonce,
                };
                Ok(false)
            }
            (
                AuthPhase::ScramStarted {
                    mut scram,
                    client_nonce,
                },
                Message::AuthenticationSaslContinue(body),
            ) => {
                validate_scram_server_first(body.data(), &client_nonce)?;
                scram.update(body.data()).map_err(|e| {
                    PostgresError::Protocol(format!("invalid SCRAM-SHA-256 server-first: {e}"))
                })?;
                frontend::sasl_response(scram.message(), out).map_err(|e| {
                    PostgresError::Protocol(format!("failed to encode SASLResponse: {e}"))
                })?;
                self.phase = AuthPhase::ScramFinalSent(scram);
                Ok(false)
            }
            (AuthPhase::ScramFinalSent(mut scram), Message::AuthenticationSaslFinal(body)) => {
                validate_scram_server_final(body.data())?;
                scram.finish(body.data()).map_err(|e| {
                    PostgresError::Protocol(format!(
                        "SCRAM-SHA-256 server signature verification failed: {e}"
                    ))
                })?;
                self.phase = AuthPhase::ScramVerifiedAwaitingOk;
                Ok(false)
            }
            (
                _,
                Message::AuthenticationSasl(_)
                | Message::AuthenticationSaslContinue(_)
                | Message::AuthenticationSaslFinal(_),
            ) => Err(PostgresError::Protocol(
                "received out-of-order SASL authentication frame".to_string(),
            )),
            (
                AuthPhase::PasswordSent
                | AuthPhase::ScramStarted { .. }
                | AuthPhase::ScramFinalSent(_)
                | AuthPhase::ScramVerifiedAwaitingOk
                | AuthPhase::Complete,
                Message::AuthenticationCleartextPassword | Message::AuthenticationMd5Password(_),
            ) => Err(PostgresError::Protocol(
                "received unexpected second password challenge after authentication started"
                    .to_string(),
            )),
            (
                _,
                Message::AuthenticationKerberosV5
                | Message::AuthenticationScmCredential
                | Message::AuthenticationGss
                | Message::AuthenticationSspi
                | Message::AuthenticationGssContinue(_),
            ) => Err(PostgresError::UnsupportedAuth(
                "unsupported PostgreSQL authentication mechanism requested by server".to_string(),
            )),
            _ => Err(PostgresError::Protocol(
                "unexpected backend frame during startup authentication".to_string(),
            )),
        }
    }
}

fn validate_scram_server_first(data: &[u8], client_nonce: &str) -> Result<(), PostgresError> {
    let text = std::str::from_utf8(data).map_err(|_| {
        PostgresError::Protocol("SCRAM server-first-message is not valid UTF-8".to_string())
    })?;
    let mut r_val: Option<&str> = None;
    let mut s_val: Option<&str> = None;
    let mut i_val: Option<&str> = None;

    for part in text.split(',') {
        if let Some(rest) = part.strip_prefix("r=") {
            if r_val.replace(rest).is_some() {
                return Err(PostgresError::Protocol(
                    "duplicate r= attribute in SCRAM server-first-message".to_string(),
                ));
            }
        } else if let Some(rest) = part.strip_prefix("s=") {
            if s_val.replace(rest).is_some() {
                return Err(PostgresError::Protocol(
                    "duplicate s= attribute in SCRAM server-first-message".to_string(),
                ));
            }
        } else if let Some(rest) = part.strip_prefix("i=")
            && i_val.replace(rest).is_some()
        {
            return Err(PostgresError::Protocol(
                "duplicate i= attribute in SCRAM server-first-message".to_string(),
            ));
        }
    }

    let server_nonce = r_val.ok_or_else(|| {
        PostgresError::Protocol("missing r= nonce in SCRAM server-first-message".to_string())
    })?;
    if !server_nonce.starts_with(client_nonce) || server_nonce.len() <= client_nonce.len() {
        return Err(PostgresError::Protocol(
            "SCRAM server nonce does not extend client nonce".to_string(),
        ));
    }

    let salt = s_val.ok_or_else(|| {
        PostgresError::Protocol("missing s= salt in SCRAM server-first-message".to_string())
    })?;
    if salt.is_empty() {
        return Err(PostgresError::Protocol(
            "empty s= salt in SCRAM server-first-message".to_string(),
        ));
    }

    let iter_str = i_val.ok_or_else(|| {
        PostgresError::Protocol(
            "missing i= iteration count in SCRAM server-first-message".to_string(),
        )
    })?;
    if iter_str.is_empty() || !iter_str.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PostgresError::Protocol(
            "SCRAM iteration count i= must be non-empty ASCII decimal digits".to_string(),
        ));
    }
    let iterations: u32 = iter_str.parse().map_err(|_| {
        PostgresError::Protocol("SCRAM iteration count i= overflows u32".to_string())
    })?;
    if !(1..=MAX_SCRAM_ITERATIONS).contains(&iterations) {
        return Err(PostgresError::Protocol(format!(
            "SCRAM iteration count {iterations} is outside permitted range 1..={MAX_SCRAM_ITERATIONS}"
        )));
    }
    Ok(())
}

fn validate_scram_server_final(data: &[u8]) -> Result<(), PostgresError> {
    let text = std::str::from_utf8(data).map_err(|_| {
        PostgresError::Protocol("SCRAM server-final-message is not valid UTF-8".to_string())
    })?;
    let Some(verifier) = text.strip_prefix("v=") else {
        return Err(PostgresError::Protocol(
            "SCRAM server-final-message does not begin with v=".to_string(),
        ));
    };
    if verifier.is_empty() {
        return Err(PostgresError::Protocol(
            "SCRAM server-final-message contains empty v= verifier".to_string(),
        ));
    }
    Ok(())
}
