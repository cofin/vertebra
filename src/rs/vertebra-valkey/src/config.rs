//! Valkey connection configuration, URL validation, Rustls TLS/mTLS, and GCP Memorystore IAM auth.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use pyo3::prelude::*;
use pyo3::types::PyCapsule;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use url::Url;
use vertebra_core::gcp_auth::{
    GCP_TOKEN_CACHE_CAPSULE_NAME, GcpTokenCache, GcpTokenCacheCapsule, SharedGcpTokenCacheHandle,
};

use crate::ensure_rustls_provider;
use crate::error::ValkeyError;

/// Internal endpoint target representation for Valkey connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValkeyTarget {
    Tcp { host: String, port: u16 },
    Unix { path: PathBuf },
}

/// Abstracted Google Cloud Memorystore for Valkey IAM token source.
pub enum GcpAuthSource {
    Handle(SharedGcpTokenCacheHandle),
    PythonCallable(Py<PyAny>),
}

impl Clone for GcpAuthSource {
    fn clone(&self) -> Self {
        match self {
            Self::Handle(h) => Self::Handle(h.clone()),
            Self::PythonCallable(obj) => {
                Python::attach(|py| Self::PythonCallable(obj.clone_ref(py)))
            }
        }
    }
}

impl std::fmt::Debug for GcpAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handle(_) => f.write_str("GcpAuthSource::Handle(..)"),
            Self::PythonCallable(_) => f.write_str("GcpAuthSource::PythonCallable(..)"),
        }
    }
}

#[derive(Debug)]
struct InsecureServerCertVerifier;

impl ServerCertVerifier for InsecureServerCertVerifier {
    fn verify_server_cert(
        &self,
        _end_entity: &CertificateDer<'_>,
        _intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn verify_tls13_signature(
        &self,
        _message: &[u8],
        _cert: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        Ok(HandshakeSignatureValid::assertion())
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        rustls::crypto::ring::default_provider()
            .signature_verification_algorithms
            .supported_schemes()
    }
}

/// Core configuration struct backing [`ValkeyClientConfig`].
#[derive(Clone, Debug)]
pub struct ValkeyConfigInner {
    pub url_redacted: String,
    pub target: ValkeyTarget,
    pub host: String,
    pub port: u16,
    pub db: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub tls: bool,
    pub tls_ca_pem: Option<String>,
    pub tls_cert_pem: Option<String>,
    pub tls_key_pem: Option<String>,
    pub tls_server_name: Option<String>,
    pub tls_insecure: bool,
    pub tls_config: Option<Arc<ClientConfig>>,
    pub pool_min_size: usize,
    pub pool_max_size: usize,
    pub connect_timeout: Duration,
    pub command_timeout: Duration,
    pub max_redirections: usize,
    pub cluster_enabled: bool,
    pub cluster_nodes: Vec<String>,
    pub read_from_replicas: bool,
    pub sentinel_service_name: Option<String>,
    pub sentinel_nodes: Vec<String>,
    pub sentinel_username: Option<String>,
    pub sentinel_password: Option<String>,
    pub client_tracking_enabled: bool,
    pub client_tracking_prefixes: Vec<String>,
    pub client_tracking_capacity: usize,
    pub client_tracking_ttl: Option<Duration>,
    pub client_name: Option<String>,
    pub enable_gcp_memorystore_iam: bool,
    pub gcp_auth: Option<GcpAuthSource>,
    pub protocol_resp3: bool,
    pub enable_capa_redirect: bool,
}

impl ValkeyConfigInner {
    /// Build a [`ValkeyConfigInner`] purely in Rust from a URL without Python objects.
    pub fn from_url_str(url_str: &str) -> Result<Self, ValkeyError> {
        Self::build(
            Some(url_str.to_string()),
            "127.0.0.1".to_string(),
            6379,
            0,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            false,
            None,
            1,
            16,
            5000,
            5000,
            8,
            false,
            Vec::new(),
            false,
            None,
            Vec::new(),
            None,
            None,
            false,
            Vec::new(),
            4096,
            None,
            None,
            false,
            None,
            true,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn build(
        url: Option<String>,
        host: String,
        port: u16,
        db: u16,
        username: Option<String>,
        password: Option<String>,
        tls: Option<bool>,
        tls_ca_pem: Option<String>,
        tls_cert_pem: Option<String>,
        tls_key_pem: Option<String>,
        tls_server_name: Option<String>,
        tls_insecure: bool,
        unix_socket_path: Option<String>,
        pool_min_size: usize,
        pool_max_size: usize,
        connect_timeout_ms: u64,
        command_timeout_ms: u64,
        max_redirections: usize,
        cluster_enabled: bool,
        cluster_nodes: Vec<String>,
        read_from_replicas: bool,
        sentinel_service_name: Option<String>,
        sentinel_nodes: Vec<String>,
        sentinel_username: Option<String>,
        sentinel_password: Option<String>,
        client_tracking_enabled: bool,
        client_tracking_prefixes: Vec<String>,
        client_tracking_capacity: usize,
        client_tracking_ttl_ms: Option<u64>,
        client_name: Option<String>,
        enable_gcp_memorystore_iam: bool,
        gcp_auth: Option<GcpAuthSource>,
        protocol_resp3: bool,
        enable_capa_redirect: bool,
    ) -> Result<Self, ValkeyError> {
        ensure_rustls_provider();

        let mut parsed_host = host;
        let mut parsed_port = port;
        let mut parsed_db = db;
        let mut parsed_user = username;
        let mut parsed_pass = password;
        let mut parsed_tls = tls.unwrap_or(false);
        let mut parsed_unix = unix_socket_path;
        let mut parsed_connect_ms = connect_timeout_ms;
        let mut parsed_command_ms = command_timeout_ms;
        let mut parsed_client_name = client_name;

        if let Some(raw_url) = url {
            let trimmed = raw_url.trim();
            if trimmed.is_empty() {
                return Err(ValkeyError::config("Valkey connection URL cannot be empty"));
            }
            let parsed = Url::parse(trimmed)
                .map_err(|e| ValkeyError::config(format!("Invalid Valkey URL '{trimmed}': {e}")))?;
            match parsed.scheme() {
                "valkey" => {
                    parsed_tls = tls == Some(true);
                    let uhost = parsed.host_str().unwrap_or_default().trim();
                    if uhost.is_empty() {
                        return Err(ValkeyError::config(format!(
                            "Hostless Valkey URL is invalid: '{trimmed}'"
                        )));
                    }
                    parsed_host = uhost
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .to_string();
                    parsed_port = parsed.port().unwrap_or(6379);
                    parsed_unix = None;
                }
                "valkeys" => {
                    if tls == Some(false) {
                        return Err(ValkeyError::config(
                            "valkeys:// scheme requires TLS enabled",
                        ));
                    }
                    parsed_tls = true;
                    let uhost = parsed.host_str().unwrap_or_default().trim();
                    if uhost.is_empty() {
                        return Err(ValkeyError::config(format!(
                            "Hostless Valkey URL is invalid: '{trimmed}'"
                        )));
                    }
                    parsed_host = uhost
                        .trim_start_matches('[')
                        .trim_end_matches(']')
                        .to_string();
                    parsed_port = parsed.port().unwrap_or(6379);
                    parsed_unix = None;
                }
                "unix" => {
                    if tls == Some(true) {
                        return Err(ValkeyError::config(
                            "unix:// transport does not support TLS",
                        ));
                    }
                    parsed_tls = false;
                    let sock_path = parsed.path().trim();
                    if sock_path.is_empty() || sock_path == "/" {
                        return Err(ValkeyError::config(format!(
                            "unix:// URL must specify a socket filesystem path: '{trimmed}'"
                        )));
                    }
                    parsed_unix = Some(sock_path.to_string());
                    parsed_host = "localhost".to_string();
                    parsed_port = 0;
                }
                other => {
                    return Err(ValkeyError::config(format!(
                        "Unsupported Valkey URL scheme '{other}://'; expected 'valkey://', 'valkeys://', or 'unix://'"
                    )));
                }
            }

            if !parsed.username().is_empty() {
                parsed_user = Some(percent_decode(parsed.username()));
            }
            if let Some(p) = parsed.password() {
                parsed_pass = Some(percent_decode(p));
            }

            if parsed.scheme() != "unix" {
                let path_trimmed = parsed.path().trim_matches('/');
                if !path_trimmed.is_empty() {
                    parsed_db = path_trimmed.parse::<u16>().map_err(|_| {
                        ValkeyError::config(format!(
                            "Invalid database index '{path_trimmed}' in Valkey URL"
                        ))
                    })?;
                }
            }

            for (k, v) in parsed.query_pairs() {
                match k.as_ref() {
                    "db" => {
                        parsed_db = v.parse::<u16>().map_err(|_| {
                            ValkeyError::config(format!("Invalid db query value '{v}'"))
                        })?;
                    }
                    "connect_timeout_ms" => {
                        parsed_connect_ms = v.parse::<u64>().map_err(|_| {
                            ValkeyError::config(format!("Invalid connect_timeout_ms '{v}'"))
                        })?;
                    }
                    "command_timeout_ms" | "timeout_ms" => {
                        parsed_command_ms = v.parse::<u64>().map_err(|_| {
                            ValkeyError::config(format!("Invalid command_timeout_ms '{v}'"))
                        })?;
                    }
                    "client_name" if !v.is_empty() => {
                        parsed_client_name = Some(v.into_owned());
                    }
                    _ => {}
                }
            }
        } else if parsed_unix.is_none()
            && sentinel_nodes.is_empty()
            && cluster_nodes.is_empty()
            && parsed_host.trim().is_empty()
        {
            return Err(ValkeyError::config(
                "Valkey host cannot be empty unless unix_socket_path, cluster_nodes, or sentinel_nodes is set",
            ));
        }

        if let Some(ref sock) = parsed_unix {
            if sock.trim().is_empty() {
                return Err(ValkeyError::config("unix_socket_path cannot be empty"));
            }
            if cluster_enabled {
                return Err(ValkeyError::config(
                    "Valkey Cluster mode cannot be combined with unix:// socket transport",
                ));
            }
        } else if sentinel_nodes.is_empty() && parsed_port == 0 && cluster_nodes.is_empty() {
            return Err(ValkeyError::config("Valkey port must be 1..=65535"));
        }

        if !sentinel_nodes.is_empty()
            && sentinel_service_name
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(ValkeyError::config(
                "sentinel_service_name is required when sentinel_nodes is provided",
            ));
        }

        if pool_max_size == 0 {
            return Err(ValkeyError::config("pool_max_size must be at least 1"));
        }
        if pool_min_size > pool_max_size {
            return Err(ValkeyError::config(format!(
                "pool_min_size ({pool_min_size}) cannot exceed pool_max_size ({pool_max_size})"
            )));
        }
        if parsed_connect_ms == 0 || parsed_command_ms == 0 {
            return Err(ValkeyError::config(
                "connect_timeout_ms and command_timeout_ms must be greater than 0",
            ));
        }

        if tls_cert_pem.is_some() ^ tls_key_pem.is_some() {
            return Err(ValkeyError::config(
                "Both tls_cert_pem and tls_key_pem must be provided together for mTLS",
            ));
        }

        let tls_config = if parsed_tls {
            Some(build_rustls_client_config(
                tls_ca_pem.as_deref(),
                tls_cert_pem.as_deref(),
                tls_key_pem.as_deref(),
                tls_insecure,
            )?)
        } else {
            None
        };

        let effective_gcp = if enable_gcp_memorystore_iam && gcp_auth.is_none() {
            GcpTokenCache::from_options(&HashMap::new(), &[])
                .map(|c| GcpAuthSource::Handle(SharedGcpTokenCacheHandle::from_arc(c)))
        } else {
            gcp_auth
        };

        let target = if let Some(ref sock) = parsed_unix {
            ValkeyTarget::Unix {
                path: PathBuf::from(sock),
            }
        } else {
            ValkeyTarget::Tcp {
                host: parsed_host.clone(),
                port: parsed_port,
            }
        };

        let url_redacted = format_redacted_url(
            &target,
            parsed_tls,
            parsed_db,
            parsed_user.as_deref(),
            parsed_pass.is_some(),
        );

        Ok(Self {
            url_redacted,
            target,
            host: parsed_host,
            port: parsed_port,
            db: parsed_db,
            username: parsed_user,
            password: parsed_pass,
            tls: parsed_tls,
            tls_ca_pem,
            tls_cert_pem,
            tls_key_pem,
            tls_server_name,
            tls_insecure,
            tls_config,
            pool_min_size,
            pool_max_size,
            connect_timeout: Duration::from_millis(parsed_connect_ms),
            command_timeout: Duration::from_millis(parsed_command_ms),
            max_redirections,
            cluster_enabled,
            cluster_nodes,
            read_from_replicas,
            sentinel_service_name,
            sentinel_nodes,
            sentinel_username,
            sentinel_password,
            client_tracking_enabled,
            client_tracking_prefixes,
            client_tracking_capacity: client_tracking_capacity.max(1),
            client_tracking_ttl: client_tracking_ttl_ms.map(Duration::from_millis),
            client_name: parsed_client_name,
            enable_gcp_memorystore_iam,
            gcp_auth: effective_gcp,
            protocol_resp3,
            enable_capa_redirect,
        })
    }

    /// Resolve `(username, password, token_generation)` for an outgoing Valkey `HELLO`/`AUTH`.
    pub async fn resolve_auth(
        &self,
        invalidate_generation: Option<u64>,
    ) -> Result<(Option<String>, Option<String>, Option<u64>), ValkeyError> {
        if !self.enable_gcp_memorystore_iam {
            return Ok((self.username.clone(), self.password.clone(), None));
        }

        let auth_source = self.gcp_auth.as_ref().ok_or_else(|| {
            ValkeyError::gcp_auth(
                "enable_gcp_memorystore_iam is enabled without a GCP IAM token provider",
                false,
            )
        })?;

        let effective_user = self
            .username
            .clone()
            .unwrap_or_else(|| "default".to_string());

        match auth_source {
            GcpAuthSource::Handle(handle) => {
                if let Some(gen_id) = invalidate_generation {
                    handle.invalidate(gen_id);
                }
                let snap = handle
                    .get_or_refresh_with_force(invalidate_generation.is_some())
                    .await
                    .map_err(|e| {
                        ValkeyError::gcp_auth(format!("GCP Memorystore IAM token error: {e}"), true)
                    })?;
                Ok((
                    Some(effective_user),
                    Some(snap.access_token.clone()),
                    Some(snap.generation),
                ))
            }
            GcpAuthSource::PythonCallable(py_fn) => {
                let callable = Python::attach(|py| py_fn.clone_ref(py));
                let token = tokio::task::spawn_blocking(move || {
                    Python::attach(|py| -> Result<String, ValkeyError> {
                        let res = callable.bind(py).call0().map_err(|e| {
                            ValkeyError::gcp_auth(
                                format!("IAM token_provider callback failed: {e}"),
                                false,
                            )
                        })?;
                        let val = if let Ok(asyncio) = py.import("asyncio") {
                            if let Ok(is_coro) = asyncio.call_method1("iscoroutine", (&res,)) {
                                if is_coro.extract::<bool>().unwrap_or(false) {
                                    asyncio.call_method1("run", (&res,)).map_err(|e| {
                                        ValkeyError::gcp_auth(
                                            format!("IAM async token_provider failed: {e}"),
                                            false,
                                        )
                                    })?
                                } else {
                                    res
                                }
                            } else {
                                res
                            }
                        } else {
                            res
                        };
                        val.extract::<String>().map_err(|e| {
                            ValkeyError::config(format!("IAM token_provider must return str: {e}"))
                        })
                    })
                })
                .await
                .map_err(|e| {
                    ValkeyError::operational(format!("IAM callback panicked: {e}"), false)
                })??;
                if token.is_empty() {
                    return Err(ValkeyError::gcp_auth(
                        "IAM token_provider returned an empty token",
                        false,
                    ));
                }
                Ok((Some(effective_user), Some(token), None))
            }
        }
    }
}

fn percent_decode(input: &str) -> String {
    percent_decode_bytes(input.as_bytes())
}

fn percent_decode_bytes(bytes: &[u8]) -> String {
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 2 < bytes.len()
            && let (Some(hi), Some(lo)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2]))
        {
            out.push((hi << 4) | lo);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn format_redacted_url(
    target: &ValkeyTarget,
    tls: bool,
    db: u16,
    username: Option<&str>,
    has_password: bool,
) -> String {
    match target {
        ValkeyTarget::Unix { path } => format!("unix://{}?db={db}", path.display()),
        ValkeyTarget::Tcp { host, port } => {
            let scheme = if tls { "valkeys" } else { "valkey" };
            let auth = match (username, has_password) {
                (Some(u), true) => format!("{u}:***@"),
                (None, true) => ":***@".to_string(),
                (Some(u), false) => format!("{u}@"),
                (None, false) => String::new(),
            };
            format!("{scheme}://{auth}{host}:{port}/{db}")
        }
    }
}

/// Build a Rustls `ClientConfig` with WebPKI roots, optional custom CA bundle, and optional mTLS identity.
pub fn build_rustls_client_config(
    ca_pem: Option<&str>,
    client_cert_pem: Option<&str>,
    client_key_pem: Option<&str>,
    insecure: bool,
) -> Result<Arc<ClientConfig>, ValkeyError> {
    ensure_rustls_provider();

    let cert_pair = match (client_cert_pem, client_key_pem) {
        (Some(cert_str), Some(key_str)) => {
            let mut cert_reader = Cursor::new(cert_str.as_bytes());
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    ValkeyError::config(format!("Invalid mTLS client certificate PEM: {e}"))
                })?;
            if certs.is_empty() {
                return Err(ValkeyError::config(
                    "mTLS tls_cert_pem contained no certificates",
                ));
            }
            let mut key_reader = Cursor::new(key_str.as_bytes());
            let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|e| {
                    ValkeyError::config(format!("Invalid mTLS client private key PEM: {e}"))
                })?
                .ok_or_else(|| ValkeyError::config("mTLS tls_key_pem contained no private key"))?;
            Some((certs, key))
        }
        (None, None) => None,
        _ => {
            return Err(ValkeyError::config(
                "Both tls_cert_pem and tls_key_pem are required for mTLS",
            ));
        }
    };

    if insecure {
        let builder = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureServerCertVerifier));
        let cfg = match cert_pair {
            Some((certs, key)) => builder.with_client_auth_cert(certs, key).map_err(|e| {
                ValkeyError::config(format!("Invalid mTLS client credentials: {e}"))
            })?,
            None => builder.with_no_client_auth(),
        };
        return Ok(Arc::new(cfg));
    }

    let mut roots = RootCertStore::empty();
    let _ =
        roots.add_parsable_certificates(webpki_root_certs::TLS_SERVER_ROOT_CERTS.iter().cloned());
    if let Some(ca_str) = ca_pem {
        let mut ca_reader = Cursor::new(ca_str.as_bytes());
        let custom_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut ca_reader)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| ValkeyError::config(format!("Invalid tls_ca_pem certificate: {e}")))?;
        if custom_certs.is_empty() {
            return Err(ValkeyError::config("tls_ca_pem contained no certificates"));
        }
        for cert in custom_certs {
            roots
                .add(cert)
                .map_err(|e| ValkeyError::config(format!("Invalid CA certificate: {e}")))?;
        }
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    let cfg = match cert_pair {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| ValkeyError::config(format!("Invalid mTLS client credentials: {e}")))?,
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(cfg))
}

fn extract_gcp_auth_source(
    py: Python<'_>,
    provider_obj: &Bound<'_, PyAny>,
) -> Result<GcpAuthSource, ValkeyError> {
    if let Ok(capsule) = provider_obj.cast::<PyCapsule>() {
        return extract_capsule(capsule);
    }
    if provider_obj.hasattr("_capsule").unwrap_or(false) {
        let cap_obj = provider_obj.call_method0("_capsule").map_err(|e| {
            ValkeyError::config(format!(
                "Failed to inspect _capsule() on token_provider: {e}"
            ))
        })?;
        let capsule = cap_obj.cast::<PyCapsule>().map_err(|_| {
            ValkeyError::config("token_provider._capsule() did not return a PyCapsule")
        })?;
        return extract_capsule(capsule);
    }
    if provider_obj.is_callable() {
        return Ok(GcpAuthSource::PythonCallable(provider_obj.clone().unbind()));
    }
    let _ = py;
    Err(ValkeyError::config(
        "token_provider must be a GcpTokenCache, GcpTokenCacheCapsule PyCapsule, or callable",
    ))
}

fn extract_capsule(capsule: &Bound<'_, PyCapsule>) -> Result<GcpAuthSource, ValkeyError> {
    let expected_name = GCP_TOKEN_CACHE_CAPSULE_NAME;
    let ptr = capsule
        .pointer_checked(Some(expected_name))
        .map_err(|e| ValkeyError::config(format!("Invalid GcpTokenCacheCapsule: {e}")))?;
    let handle = unsafe {
        SharedGcpTokenCacheHandle::from_capsule_ptr(ptr.as_ptr().cast::<GcpTokenCacheCapsule>())
    }
    .map_err(|e| ValkeyError::config(format!("Invalid GcpTokenCacheCapsule: {e}")))?;
    Ok(GcpAuthSource::Handle(handle))
}

/// Configuration object for `SyncValkeyClient`, `AsyncValkeyClient`, listeners, and pipelines.
#[cfg_attr(
    not(Py_GIL_DISABLED),
    pyclass(
        frozen,
        immutable_type,
        from_py_object,
        freelist = 128,
        module = "vertebra._valkey"
    )
)]
#[cfg_attr(
    Py_GIL_DISABLED,
    pyclass(frozen, immutable_type, from_py_object, module = "vertebra._valkey")
)]
#[derive(Clone, Debug)]
pub struct ValkeyClientConfig {
    pub inner: Arc<ValkeyConfigInner>,
}

#[pymethods]
impl ValkeyClientConfig {
    #[new]
    #[pyo3(signature = (
        url = None,
        *,
        host = "127.0.0.1",
        port = 6379,
        db = 0,
        username = None,
        password = None,
        tls = None,
        tls_ca_pem = None,
        tls_cert_pem = None,
        tls_key_pem = None,
        tls_server_name = None,
        tls_insecure = false,
        unix_socket_path = None,
        pool_min_size = 1,
        pool_max_size = 16,
        connect_timeout_ms = 5000,
        command_timeout_ms = 5000,
        max_redirections = 8,
        cluster_enabled = false,
        cluster_nodes = None,
        read_from_replicas = false,
        sentinel_service_name = None,
        sentinel_nodes = None,
        sentinel_username = None,
        sentinel_password = None,
        client_tracking_enabled = false,
        client_tracking_prefixes = None,
        client_tracking_capacity = 4096,
        client_tracking_ttl_ms = None,
        client_name = None,
        enable_gcp_memorystore_iam = false,
        iam_auth_enabled = None,
        token_provider = None,
        iam_token_provider = None,
        protocol_resp3 = true,
        enable_capa_redirect = true,
    ))]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        py: Python<'_>,
        url: Option<String>,
        host: &str,
        port: u16,
        db: u16,
        username: Option<String>,
        password: Option<String>,
        tls: Option<bool>,
        tls_ca_pem: Option<String>,
        tls_cert_pem: Option<String>,
        tls_key_pem: Option<String>,
        tls_server_name: Option<String>,
        tls_insecure: bool,
        unix_socket_path: Option<String>,
        pool_min_size: usize,
        pool_max_size: usize,
        connect_timeout_ms: u64,
        command_timeout_ms: u64,
        max_redirections: usize,
        cluster_enabled: bool,
        cluster_nodes: Option<Vec<String>>,
        read_from_replicas: bool,
        sentinel_service_name: Option<String>,
        sentinel_nodes: Option<Vec<String>>,
        sentinel_username: Option<String>,
        sentinel_password: Option<String>,
        client_tracking_enabled: bool,
        client_tracking_prefixes: Option<Vec<String>>,
        client_tracking_capacity: usize,
        client_tracking_ttl_ms: Option<u64>,
        client_name: Option<String>,
        enable_gcp_memorystore_iam: bool,
        iam_auth_enabled: Option<bool>,
        token_provider: Option<Bound<'_, PyAny>>,
        iam_token_provider: Option<Bound<'_, PyAny>>,
        protocol_resp3: bool,
        enable_capa_redirect: bool,
    ) -> PyResult<Self> {
        let iam_enabled = iam_auth_enabled.unwrap_or(enable_gcp_memorystore_iam);
        let chosen_provider = token_provider.as_ref().or(iam_token_provider.as_ref());
        let gcp_auth = match chosen_provider {
            Some(obj) if !obj.is_none() => Some(extract_gcp_auth_source(py, obj)?),
            _ => None,
        };
        let inner = ValkeyConfigInner::build(
            url,
            host.to_string(),
            port,
            db,
            username,
            password,
            tls,
            tls_ca_pem,
            tls_cert_pem,
            tls_key_pem,
            tls_server_name,
            tls_insecure,
            unix_socket_path,
            pool_min_size,
            pool_max_size,
            connect_timeout_ms,
            command_timeout_ms,
            max_redirections,
            cluster_enabled,
            cluster_nodes.unwrap_or_default(),
            read_from_replicas,
            sentinel_service_name,
            sentinel_nodes.unwrap_or_default(),
            sentinel_username,
            sentinel_password,
            client_tracking_enabled,
            client_tracking_prefixes.unwrap_or_default(),
            client_tracking_capacity,
            client_tracking_ttl_ms,
            client_name,
            iam_enabled,
            gcp_auth,
            protocol_resp3,
            enable_capa_redirect,
        )
        .map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    /// Construct a [`ValkeyClientConfig`] directly from a Valkey connection URL.
    #[staticmethod]
    pub fn from_url(py: Python<'_>, url: String) -> PyResult<Self> {
        let inner = ValkeyConfigInner::from_url_str(&url).map_err(|e| e.to_py_err(py))?;
        Ok(Self {
            inner: Arc::new(inner),
        })
    }

    #[getter]
    pub fn url(&self) -> &str {
        &self.inner.url_redacted
    }

    #[getter]
    pub fn host(&self) -> &str {
        &self.inner.host
    }

    #[getter]
    pub fn port(&self) -> u16 {
        self.inner.port
    }

    #[getter]
    pub fn db(&self) -> u16 {
        self.inner.db
    }

    #[getter]
    pub fn username(&self) -> Option<&str> {
        self.inner.username.as_deref()
    }

    #[getter]
    pub fn tls(&self) -> bool {
        self.inner.tls
    }

    #[getter]
    pub fn unix_socket_path(&self) -> Option<String> {
        match &self.inner.target {
            ValkeyTarget::Unix { path } => Some(path.to_string_lossy().into_owned()),
            ValkeyTarget::Tcp { .. } => None,
        }
    }

    #[getter]
    pub fn cluster_enabled(&self) -> bool {
        self.inner.cluster_enabled
    }

    #[getter]
    pub fn read_from_replicas(&self) -> bool {
        self.inner.read_from_replicas
    }

    #[getter]
    pub fn sentinel_service_name(&self) -> Option<&str> {
        self.inner.sentinel_service_name.as_deref()
    }

    #[getter]
    pub fn client_tracking_enabled(&self) -> bool {
        self.inner.client_tracking_enabled
    }

    #[getter]
    pub fn client_tracking_prefixes(&self) -> Vec<String> {
        self.inner.client_tracking_prefixes.clone()
    }

    #[getter]
    pub fn enable_gcp_memorystore_iam(&self) -> bool {
        self.inner.enable_gcp_memorystore_iam
    }

    #[getter]
    pub fn max_redirections(&self) -> usize {
        self.inner.max_redirections
    }

    #[getter]
    pub fn connect_timeout_ms(&self) -> u64 {
        self.inner.connect_timeout.as_millis() as u64
    }

    #[getter]
    pub fn command_timeout_ms(&self) -> u64 {
        self.inner.command_timeout.as_millis() as u64
    }

    #[getter]
    pub fn pool_min_size(&self) -> usize {
        self.inner.pool_min_size
    }

    #[getter]
    pub fn pool_max_size(&self) -> usize {
        self.inner.pool_max_size
    }

    pub fn __repr__(&self) -> String {
        format!(
            "ValkeyClientConfig(url={:?}, cluster={}, tls={}, iam={})",
            self.inner.url_redacted,
            self.inner.cluster_enabled,
            self.inner.tls,
            self.inner.enable_gcp_memorystore_iam
        )
    }
}
