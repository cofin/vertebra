//! TLS acceptor, mTLS `WebPkiClientVerifier`, X.509 client identity extraction,
//! and CIDR-verified trusted-proxy auth header normalization.

use std::net::{IpAddr, SocketAddr};

use http::HeaderMap;
use vertebra_core::util::sha256_hex;

/// Client certificate verification mode for mTLS connections.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ClientAuthMode {
    /// Do not request or verify client certificates.
    #[default]
    None,
    /// Request a client certificate and verify it if presented, but allow unauthenticated clients.
    Optional,
    /// Require a valid client certificate signed by the configured client CA bundle.
    Required,
}

/// Mutual TLS (mTLS) configuration.
#[derive(Debug, Clone, Default)]
pub struct MtlsConfig {
    /// Client authentication mode.
    pub mode: ClientAuthMode,
    /// Optional path to a PEM-encoded client CA certificate bundle.
    pub client_ca_path: Option<std::path::PathBuf>,
}

/// IPv4 or IPv6 CIDR network block for trusted-proxy verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IpCidr {
    pub network: IpAddr,
    pub prefix_len: u8,
}

impl IpCidr {
    /// Construct a new CIDR range.
    pub const fn new(network: IpAddr, prefix_len: u8) -> Self {
        Self {
            network,
            prefix_len,
        }
    }

    /// Return `true` if `addr` falls within this CIDR block.
    pub fn contains(&self, addr: IpAddr) -> bool {
        match (self.network, addr) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let shift = 32u8.saturating_sub(self.prefix_len.min(32));
                let mask = if shift == 32 { 0 } else { !0u32 << shift };
                (u32::from(net) & mask) == (u32::from(ip) & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                if self.prefix_len == 0 {
                    return true;
                }
                let shift = 128u8.saturating_sub(self.prefix_len.min(128));
                let mask = if shift == 128 { 0 } else { !0u128 << shift };
                (u128::from(net) & mask) == (u128::from(ip) & mask)
            }
            _ => false,
        }
    }
}

/// Configuration for CIDR-verified reverse proxies permitted to forward identity headers.
#[derive(Debug, Clone, Default)]
pub struct TrustedProxyConfig {
    pub trusted_cidrs: Vec<IpCidr>,
}

impl TrustedProxyConfig {
    /// Return `true` if `peer` belongs to any configured trusted proxy CIDR.
    pub fn is_trusted(&self, peer: SocketAddr) -> bool {
        let ip = peer.ip();
        self.trusted_cidrs.iter().any(|cidr| cidr.contains(ip))
    }
}

/// Extracted X.509 client certificate identity attributes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClientIdentity {
    pub subject_cn: Option<String>,
    pub subject_alt_names: Vec<String>,
    pub sha256_fingerprint: String,
}

/// Normalized authentication metadata extracted from trusted-proxy headers.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProxyAuthMetadata {
    pub iap_jwt_assertion: Option<String>,
    pub xfcc_raw: Option<String>,
    pub xfcc_subject: Option<String>,
    pub xfcc_uri: Option<String>,
    pub xfcc_hash: Option<String>,
    pub bearer_token: Option<String>,
}

/// Extract X.509 subject Common Name (CN), Subject Alternative Names (dNSName / URI),
/// and lowercase hex SHA-256 fingerprint from a DER-encoded certificate.
pub fn extract_client_identity_from_der(der: &[u8]) -> Result<ClientIdentity, String> {
    if der.len() < 16 || der[0] != 0x30 {
        return Err("invalid X.509 DER sequence".to_string());
    }
    let sha256_fingerprint = sha256_hex(der);
    let subject_cn = extract_der_common_name(der);
    let subject_alt_names = extract_der_subject_alt_names(der);

    Ok(ClientIdentity {
        subject_cn,
        subject_alt_names,
        sha256_fingerprint,
    })
}

/// Normalize `x-goog-iap-jwt-assertion`, `x-forwarded-client-cert` (`XFCC`), and
/// `authorization` headers.
///
/// When `peer` does not match `proxy_config.trusted_cidrs`, proxy-only identity headers
/// (`x-goog-iap-jwt-assertion` and `x-forwarded-client-cert`) are stripped from `headers`
/// to prevent spoofing.
pub fn normalize_trusted_proxy_headers(
    headers: &mut HeaderMap,
    peer: SocketAddr,
    proxy_config: &TrustedProxyConfig,
) -> ProxyAuthMetadata {
    let bearer_token = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| {
            s.strip_prefix("Bearer ")
                .or_else(|| s.strip_prefix("bearer "))
        })
        .map(|s| s.trim().to_string());

    if !proxy_config.is_trusted(peer) {
        headers.remove("x-goog-iap-jwt-assertion");
        headers.remove("x-forwarded-client-cert");
        return ProxyAuthMetadata {
            bearer_token,
            ..Default::default()
        };
    }

    let iap_jwt_assertion = headers
        .get("x-goog-iap-jwt-assertion")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string());

    let xfcc_raw = headers
        .get("x-forwarded-client-cert")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.trim().to_string());

    let (xfcc_subject, xfcc_uri, xfcc_hash) = match xfcc_raw.as_deref() {
        Some(raw) => parse_xfcc_fields(raw),
        None => (None, None, None),
    };

    ProxyAuthMetadata {
        iap_jwt_assertion,
        xfcc_raw,
        xfcc_subject,
        xfcc_uri,
        xfcc_hash,
        bearer_token,
    }
}

fn parse_xfcc_fields(raw: &str) -> (Option<String>, Option<String>, Option<String>) {
    let mut subject = None;
    let mut uri = None;
    let mut hash = None;

    for part in raw.split(';') {
        let part = part.trim();
        if let Some(val) = part.strip_prefix("Subject=") {
            subject = Some(val.trim_matches('"').to_string());
        } else if let Some(val) = part.strip_prefix("URI=") {
            uri = Some(val.trim_matches('"').to_string());
        } else if let Some(val) = part.strip_prefix("Hash=") {
            hash = Some(val.trim_matches('"').to_string());
        }
    }
    (subject, uri, hash)
}

fn extract_der_common_name(der: &[u8]) -> Option<String> {
    const OID_COMMON_NAME: [u8; 5] = [0x06, 0x03, 0x55, 0x04, 0x03];
    let mut last_cn = None;
    let mut i = 0;
    while i + 7 < der.len() {
        if der[i..i + 5] == OID_COMMON_NAME {
            let str_tag = der[i + 5];
            let str_len = der[i + 6] as usize;
            if matches!(str_tag, 0x0C | 0x13 | 0x16)
                && i + 7 + str_len <= der.len()
                && let Ok(cn) = std::str::from_utf8(&der[i + 7..i + 7 + str_len])
            {
                last_cn = Some(cn.to_string());
                i += 7 + str_len;
                continue;
            }
        }
        i += 1;
    }
    last_cn
}

fn extract_der_subject_alt_names(der: &[u8]) -> Vec<String> {
    const OID_SUBJECT_ALT_NAME: [u8; 5] = [0x06, 0x03, 0x55, 0x1D, 0x11];
    let mut sans = Vec::new();
    let mut i = 0;
    while i + 5 < der.len() {
        if der[i..i + 5] == OID_SUBJECT_ALT_NAME {
            let mut j = i + 5;
            while j + 2 < der.len() {
                let tag = der[j];
                let len = der[j + 1] as usize;
                if (tag == 0x82 || tag == 0x86)
                    && len > 0
                    && j + 2 + len <= der.len()
                    && let Ok(s) = std::str::from_utf8(&der[j + 2..j + 2 + len])
                    && s.chars().all(|c| c.is_ascii_graphic())
                {
                    sans.push(s.to_string());
                    j += 2 + len;
                    continue;
                }
                j += 1;
            }
            break;
        }
        i += 1;
    }
    sans
}

#[cfg(feature = "tls")]
mod rustls_acceptor {
    use std::io::BufReader;
    use std::sync::Arc;

    use rustls::RootCertStore;
    use rustls::ServerConfig as RustlsServerConfig;
    use rustls::server::WebPkiClientVerifier;
    use tokio::net::TcpStream;
    use tokio_rustls::server::TlsStream;

    use super::{ClientAuthMode, ClientIdentity, extract_client_identity_from_der};
    use crate::config::TlsConfig;

    /// Wraps a `tokio_rustls::TlsAcceptor` with ALPN (`h2`, `http/1.1`) and optional mTLS verification.
    #[derive(Clone)]
    pub struct TlsAcceptor {
        inner: tokio_rustls::TlsAcceptor,
        rustls_config: Arc<RustlsServerConfig>,
    }

    impl TlsAcceptor {
        /// Build a `TlsAcceptor` from `TlsConfig`, configuring `WebPkiClientVerifier` when mTLS is enabled.
        pub fn from_config(
            cfg: &TlsConfig,
        ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
            let _ = rustls::crypto::ring::default_provider().install_default();
            let cert_file = std::fs::File::open(&cfg.cert_path)?;
            let key_file = std::fs::File::open(&cfg.key_path)?;

            let certs: Vec<_> =
                rustls_pemfile::certs(&mut BufReader::new(cert_file)).collect::<Result<_, _>>()?;
            let key = rustls_pemfile::private_key(&mut BufReader::new(key_file))?
                .ok_or("no private key found in key file")?;

            let mut tls_config = match cfg.mtls.mode {
                ClientAuthMode::None => RustlsServerConfig::builder()
                    .with_no_client_auth()
                    .with_single_cert(certs, key)?,
                ClientAuthMode::Optional | ClientAuthMode::Required => {
                    let ca_path = cfg.mtls.client_ca_path.as_ref().ok_or(
                        "client_ca_path is required when mTLS mode is Optional or Required",
                    )?;
                    let ca_file = std::fs::File::open(ca_path)?;
                    let ca_certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(ca_file))
                        .collect::<Result<_, _>>()?;
                    let mut roots = RootCertStore::empty();
                    for cert in ca_certs {
                        roots.add(cert)?;
                    }
                    let builder = WebPkiClientVerifier::builder(Arc::new(roots));
                    let verifier = if cfg.mtls.mode == ClientAuthMode::Optional {
                        builder.allow_unauthenticated().build()?
                    } else {
                        builder.build()?
                    };
                    RustlsServerConfig::builder()
                        .with_client_cert_verifier(verifier)
                        .with_single_cert(certs, key)?
                }
            };

            tls_config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
            let arc_cfg = Arc::new(tls_config);
            let acceptor = tokio_rustls::TlsAcceptor::from(Arc::clone(&arc_cfg));
            Ok(Self {
                inner: acceptor,
                rustls_config: arc_cfg,
            })
        }

        /// Return the underlying `rustls::ServerConfig` for reuse by HTTP/3 QUIC endpoints.
        pub fn rustls_config(&self) -> Arc<RustlsServerConfig> {
            Arc::clone(&self.rustls_config)
        }

        /// Perform the TLS handshake on an accepted TCP stream.
        pub async fn accept(
            &self,
            stream: TcpStream,
        ) -> Result<TlsStream<TcpStream>, std::io::Error> {
            self.inner.accept(stream).await
        }

        /// Extract `ClientIdentity` from the peer certificate chain of an established `TlsStream`.
        pub fn peer_identity(stream: &TlsStream<TcpStream>) -> Option<ClientIdentity> {
            let (_, conn) = stream.get_ref();
            let certs = conn.peer_certificates()?;
            let leaf = certs.first()?;
            extract_client_identity_from_der(leaf.as_ref()).ok()
        }
    }
}

#[cfg(feature = "tls")]
pub use rustls_acceptor::TlsAcceptor;
