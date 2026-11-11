//! PostgreSQL `SSLRequest` (`80877103`) negotiation and `TransportStream` TLS upgrade.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::Instant;
use vertebra_db::connection::{ConnectOptions, EndpointTarget, TlsMode, TransportStream};

use crate::config::PostgresConfig;
use crate::errors::PostgresError;

/// PostgreSQL `SSLRequest` protocol code (`80877103`, `0x04d2_162f`).
pub const SSL_REQUEST_CODE: i32 = 80_877_103;

/// Canonical 8-byte PostgreSQL `SSLRequest` packet (`[0, 0, 0, 8, 4, 210, 22, 47]`).
pub const SSL_REQUEST_FRAME: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

/// Connected transport stream plus negotiated TLS state.
#[derive(Debug)]
pub struct DialedTransport {
    pub stream: TransportStream,
    pub tls_negotiated: bool,
    pub tls_server_end_point: Option<Vec<u8>>,
}

/// Dial a direct PostgreSQL endpoint and execute `SSLRequest` negotiation when `sslmode != Disable`.
pub async fn connect(
    config: &PostgresConfig,
    deadline: Instant,
) -> Result<DialedTransport, PostgresError> {
    let remaining = remaining_budget(deadline)?;
    let tcp_opts = ConnectOptions::tcp(config.host.clone(), config.port)
        .with_tls_mode(TlsMode::Disable)
        .with_connect_timeout(remaining);

    let mut stream = tokio::time::timeout_at(deadline, TransportStream::connect(&tcp_opts))
        .await
        .map_err(|_| PostgresError::Transport("TCP connect timed out".to_string()))?
        .map_err(|e| PostgresError::Transport(e.to_string()))?;

    if config.sslmode == TlsMode::Disable {
        return Ok(DialedTransport {
            stream,
            tls_negotiated: false,
            tls_server_end_point: None,
        });
    }

    tokio::time::timeout_at(deadline, async {
        stream.write_all(&SSL_REQUEST_FRAME).await?;
        stream.flush().await
    })
    .await
    .map_err(|_| PostgresError::Transport("SSLRequest write timed out".to_string()))?
    .map_err(|e| PostgresError::Transport(format!("SSLRequest write failed: {e}")))?;

    let mut reply = [0u8; 1];
    let read_res = tokio::time::timeout_at(deadline, stream.read_exact(&mut reply)).await;
    match read_res {
        Err(_) => {
            return Err(PostgresError::Transport(
                "SSLRequest response timed out".to_string(),
            ));
        }
        Ok(Err(err)) => {
            return Err(PostgresError::Transport(format!(
                "SSLRequest response read failed: {err}"
            )));
        }
        Ok(Ok(_)) => {}
    }

    match reply[0] {
        b'S' => {
            let tcp = match stream {
                TransportStream::PlainTcp(tcp) => tcp,
                _ => {
                    return Err(PostgresError::Protocol(
                        "expected PlainTcp stream prior to SSLRequest upgrade".to_string(),
                    ));
                }
            };
            let rem = remaining_budget(deadline)?;
            let mut tls_opts = ConnectOptions::tcp(config.host.clone(), config.port)
                .with_tls_mode(config.sslmode)
                .with_connect_timeout(rem);
            if matches!(config.sslmode, TlsMode::VerifyCa | TlsMode::VerifyFull)
                && let Some(ref ca) = config.ca_pem
            {
                tls_opts = tls_opts.with_ca_pem(ca.clone());
            }
            if let (Some(cert), Some(key)) = (&config.client_cert_pem, &config.client_key_pem) {
                tls_opts = tls_opts.with_client_identity(cert.clone(), key.clone());
            }
            if let Some(ref sni) = config.server_name {
                tls_opts = tls_opts.with_server_name(sni.clone());
            }
            let upgraded = tokio::time::timeout_at(
                deadline,
                TransportStream::upgrade_tls(tcp, &config.host, &tls_opts),
            )
            .await
            .map_err(|_| PostgresError::Transport("TLS handshake timed out".to_string()))?
            .map_err(|e| PostgresError::Transport(e.to_string()))?;

            let tls_server_end_point = extract_tls_server_end_point(&upgraded)?;

            Ok(DialedTransport {
                stream: upgraded,
                tls_negotiated: true,
                tls_server_end_point,
            })
        }
        b'N' => {
            if config.sslmode == TlsMode::Prefer {
                Ok(DialedTransport {
                    stream,
                    tls_negotiated: false,
                    tls_server_end_point: None,
                })
            } else {
                Err(PostgresError::Transport(format!(
                    "PostgreSQL server rejected SSLRequest ('N') under sslmode {:?}",
                    config.sslmode
                )))
            }
        }
        other => Err(PostgresError::Protocol(format!(
            "unexpected SSLRequest response byte 0x{other:02x}"
        ))),
    }
}

pub(crate) fn extract_tls_server_end_point(
    stream: &TransportStream,
) -> Result<Option<Vec<u8>>, PostgresError> {
    match stream {
        TransportStream::TlsTcp(tls) => {
            if let Some(leaf) = tls.get_ref().1.peer_certificates().and_then(|c| c.first()) {
                match crate::binding::tls_server_end_point(leaf.as_ref()) {
                    Ok(digest) => Ok(Some(digest)),
                    Err(PostgresError::UnsupportedAuth(_)) => Ok(None),
                    Err(err) => Err(err),
                }
            } else {
                Ok(None)
            }
        }
        _ => Ok(None),
    }
}

fn remaining_budget(deadline: Instant) -> Result<Duration, PostgresError> {
    let now = Instant::now();
    if now >= deadline {
        return Err(PostgresError::Transport(
            "connection deadline exceeded".to_string(),
        ));
    }
    let _ = EndpointTarget::Unix {
        path: std::path::PathBuf::new(),
    };
    Ok(deadline.duration_since(now))
}
