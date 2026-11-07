//! PostgreSQL SSLRequest negotiation over the shared TCP and rustls transport.

use bytes::BytesMut;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::{Instant, timeout_at},
};
use vertebra_db::connection::{
    ConnectOptions, TlsMode, TransportStream, install_rustls_ring_provider,
};

use crate::{config::PostgresConfig, errors::PostgresError};

pub(crate) struct DialedTransport {
    pub stream: TransportStream,
    pub tls_server_end_point: Option<Vec<u8>>,
}

pub(crate) async fn connect(
    config: &PostgresConfig,
    deadline: Instant,
) -> Result<DialedTransport, PostgresError> {
    install_rustls_ring_provider();
    let full_options = ConnectOptions {
        tls_mode: config.sslmode,
        ca_pem: config.ca_pem.clone(),
        client_cert_pem: config.client_cert_pem.clone(),
        client_key_pem: config.client_key_pem.clone(),
        server_name: config.server_name.clone(),
        connect_timeout: deadline.saturating_duration_since(Instant::now()),
        ..ConnectOptions::tcp(&config.host, config.port)
    };
    let dial_options = ConnectOptions {
        tls_mode: TlsMode::Disable,
        ..full_options.clone()
    };
    let mut stream = TransportStream::connect(&dial_options)
        .await
        .map_err(|error| PostgresError::Transport(error.to_string()))?;
    if config.sslmode != TlsMode::Disable {
        stream = timeout_at(deadline, async {
            let mut request = BytesMut::new();
            postgres_protocol::message::frontend::ssl_request(&mut request);
            stream
                .write_all(&request)
                .await
                .map_err(|error| PostgresError::Transport(error.to_string()))?;
            stream
                .flush()
                .await
                .map_err(|error| PostgresError::Transport(error.to_string()))?;
            let response = stream
                .read_u8()
                .await
                .map_err(|error| PostgresError::Transport(error.to_string()))?;
            match response {
                b'S' => {
                    let TransportStream::PlainTcp(tcp) = stream else {
                        return Err(PostgresError::Protocol(
                            "SSLRequest requires a plain TCP stream".into(),
                        ));
                    };
                    TransportStream::upgrade_tls(tcp, &config.host, &full_options)
                        .await
                        .map_err(|error| PostgresError::Transport(error.to_string()))
                }
                b'N' if config.sslmode == TlsMode::Prefer => Ok(stream),
                b'N' => Err(PostgresError::Transport("server refused TLS".into())),
                _ => Err(PostgresError::Protocol(
                    "invalid SSLRequest response".into(),
                )),
            }
        })
        .await
        .map_err(|_| PostgresError::Transport("connection deadline elapsed".into()))??;
    }
    Ok(DialedTransport {
        stream,
        tls_server_end_point: None,
    })
}
