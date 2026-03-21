//! TLS acceptor using rustls with ALPN for h2 and http/1.1 negotiation.
//!
//! Only compiled when the `tls` feature is enabled.

use std::io::BufReader;
use std::sync::Arc;

use rustls::ServerConfig as RustlsServerConfig;
use tokio::net::TcpStream;
use tokio_rustls::server::TlsStream;

use crate::config::TlsConfig;

/// Wraps a `tokio_rustls::TlsAcceptor` with the server ALPN configuration.
#[derive(Clone)]
pub struct TlsAcceptor {
    inner: tokio_rustls::TlsAcceptor,
}

impl TlsAcceptor {
    /// Build a `TlsAcceptor` from a `TlsConfig`.
    ///
    /// ALPN is configured to advertise `h2` first, then `http/1.1`.
    pub fn from_config(cfg: &TlsConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let cert_file = std::fs::File::open(&cfg.cert_path)?;
        let key_file = std::fs::File::open(&cfg.key_path)?;

        let certs: Vec<_> =
            rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<_, _>>()?;
        let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
            .ok_or("no private key found in key file")?;

        let mut tls_config = RustlsServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)?;

        tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];

        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls_config));
        Ok(Self { inner: acceptor })
    }

    /// Perform the TLS handshake on an accepted TCP stream.
    pub async fn accept(&self, stream: TcpStream) -> Result<TlsStream<TcpStream>, std::io::Error> {
        self.inner.accept(stream).await
    }
}
