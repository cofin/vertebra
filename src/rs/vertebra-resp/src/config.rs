//! RESP connection configuration, URL validation, Rustls TLS/mTLS, and GCP Memorystore IAM auth.

use std::collections::HashMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
#[cfg(feature = "pyo3")]
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

#[cfg(feature = "pyo3")]
use parking_lot::{Mutex as SyncMutex, RwLock as SyncRwLock};
#[cfg(feature = "pyo3")]
use pyo3::prelude::*;
#[cfg(feature = "pyo3")]
use pyo3::types::PyCapsule;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime};
use rustls::{ClientConfig, DigitallySignedStruct, RootCertStore, SignatureScheme};
use url::Url;
#[cfg(feature = "pyo3")]
use vertebra_core::gcp_auth::{GCP_TOKEN_CACHE_CAPSULE_NAME, GcpTokenCacheCapsule};
use vertebra_core::gcp_auth::{GcpTokenCache, SharedGcpTokenCacheHandle};

use crate::ensure_rustls_provider;
use crate::error::RespError;

/// Internal endpoint target representation for RESP connections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RespTarget {
    Tcp { host: String, port: u16 },
    Unix { path: PathBuf },
}

/// Backward-compatible alias for Valkey callers.
pub type ValkeyTarget = RespTarget;

/// Redis topology mode descriptor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RedisTopologyMode {
    #[default]
    Standalone,
    Cluster,
    Sentinel,
}

impl RedisTopologyMode {
    /// Parse `"standalone"`, `"cluster"`, or `"sentinel"` (case-insensitive).
    pub fn parse(s: &str) -> Result<Self, RespError> {
        match s.to_ascii_lowercase().as_str() {
            "standalone" => Ok(Self::Standalone),
            "cluster" => Ok(Self::Cluster),
            "sentinel" => Ok(Self::Sentinel),
            other => Err(RespError::config(format!(
                "Invalid Redis topology mode '{other}'; expected 'standalone', 'cluster', or 'sentinel'"
            ))),
        }
    }
}

#[cfg(feature = "pyo3")]
pub type PendingPythonTokenRx = tokio::sync::watch::Receiver<Option<Result<(), RespError>>>;
#[cfg(feature = "pyo3")]
type PendingPythonTokenTx = tokio::sync::watch::Sender<Option<Result<(), RespError>>>;

#[cfg(feature = "pyo3")]
struct PythonTokenState {
    cached: Option<(String, u64)>,
    inflight: Option<PendingPythonTokenRx>,
}

/// Thread-safe cache for a Python `token_provider` callable resolved strictly on Python caller threads.
#[cfg(feature = "pyo3")]
pub struct PythonTokenProvider {
    callable: Py<PyAny>,
    state: SyncRwLock<PythonTokenState>,
    next_generation: AtomicU64,
}

#[cfg(feature = "pyo3")]
impl PythonTokenProvider {
    /// Wrap a Python `token_provider` callable with an initially empty pure-Rust credential cache.
    #[must_use]
    pub fn new(callable: Py<PyAny>) -> Arc<Self> {
        Arc::new(Self {
            callable,
            state: SyncRwLock::new(PythonTokenState {
                cached: None,
                inflight: None,
            }),
            next_generation: AtomicU64::new(1),
        })
    }

    /// Return whether a valid token is currently cached in pure Rust.
    #[must_use]
    pub fn has_cached_token(&self) -> bool {
        self.state.read().cached.is_some()
    }

    /// Store a newly resolved non-empty token and advance the generation counter.
    pub fn store_token(&self, token: String) -> Result<u64, RespError> {
        if token.is_empty() {
            return Err(RespError::gcp_auth(
                "IAM token_provider returned an empty token",
                false,
            ));
        }
        let gen_id = self.next_generation.fetch_add(1, Ordering::Relaxed);
        let mut guard = self.state.write();
        guard.cached = Some((token, gen_id));
        guard.inflight = None;
        Ok(gen_id)
    }

    fn clear_inflight(&self) {
        let mut guard = self.state.write();
        guard.inflight = None;
    }

    /// Read or invalidate the cached token from a Tokio worker without attaching to Python.
    pub fn resolve_cached(
        &self,
        invalidate_generation: Option<u64>,
    ) -> Result<(String, u64), RespError> {
        if let Some(inv_gen) = invalidate_generation {
            let mut guard = self.state.write();
            if guard.cached.as_ref().is_some_and(|(_, g)| *g == inv_gen) {
                guard.cached = None;
            }
            if let Some((ref tok, g)) = guard.cached {
                return Ok((tok.clone(), g));
            }
            return Err(RespError::from_server_error(
                "WRONGPASS IAM token rejected; refresh required".to_string(),
            ));
        }
        let guard = self.state.read();
        if let Some((ref tok, g)) = guard.cached {
            Ok((tok.clone(), g))
        } else {
            Err(RespError::gcp_auth(
                "IAM Python token_provider cache is unprimed",
                true,
            ))
        }
    }

    /// Invoke the Python `token_provider` synchronously on the caller's Python thread before `py.detach`.
    pub fn ensure_token_sync(&self, py: Python<'_>, force_refresh: bool) -> Result<(), RespError> {
        if !force_refresh && self.has_cached_token() {
            return Ok(());
        }
        let res = self.callable.bind(py).call0().map_err(|e| {
            RespError::gcp_auth(format!("IAM token_provider callback failed: {e}"), false)
        })?;
        let val = if res.hasattr("__await__").unwrap_or(false) {
            let asyncio = py.import("asyncio").map_err(|e| {
                RespError::operational(format!("Failed to import asyncio: {e}"), false)
            })?;
            if asyncio.call_method0("get_running_loop").is_ok() {
                return Err(RespError::config(
                    "Async IAM token_provider cannot be invoked from a synchronous Valkey client inside a running event loop",
                ));
            }
            asyncio.call_method1("run", (&res,)).map_err(|e| {
                RespError::gcp_auth(format!("IAM async token_provider failed: {e}"), false)
            })?
        } else {
            res
        };
        let token = val
            .extract::<String>()
            .map_err(|e| RespError::config(format!("IAM token_provider must return str: {e}")))?;
        self.store_token(token)?;
        Ok(())
    }

    /// Prime or schedule the Python `token_provider` on the caller's Python thread before spawning a Tokio task.
    pub fn prepare_token_async(
        self: &Arc<Self>,
        py: Python<'_>,
        force_refresh: bool,
    ) -> Result<Option<PendingPythonTokenRx>, RespError> {
        if !force_refresh {
            let guard = self.state.read();
            if guard.cached.is_some() {
                return Ok(None);
            }
            if let Some(ref rx) = guard.inflight {
                return Ok(Some(rx.clone()));
            }
        }

        let res = self.callable.bind(py).call0().map_err(|e| {
            RespError::gcp_auth(format!("IAM token_provider callback failed: {e}"), false)
        })?;
        if !res.hasattr("__await__").unwrap_or(false) {
            let token = res.extract::<String>().map_err(|e| {
                RespError::config(format!("IAM token_provider must return str: {e}"))
            })?;
            self.store_token(token)?;
            return Ok(None);
        }

        let asyncio = py
            .import("asyncio")
            .map_err(|e| RespError::operational(format!("Failed to import asyncio: {e}"), false))?;
        let py_task = asyncio
            .call_method1("ensure_future", (&res,))
            .map_err(|e| {
                RespError::gcp_auth(
                    format!("Failed to schedule async IAM token_provider: {e}"),
                    false,
                )
            })?;
        let (tx, rx) = tokio::sync::watch::channel(None);
        {
            let mut guard = self.state.write();
            if force_refresh {
                guard.cached = None;
            }
            guard.inflight = Some(rx.clone());
        }
        let cb = Py::new(
            py,
            RespTokenDoneCallback {
                provider: Arc::clone(self),
                tx: SyncMutex::new(Some(tx)),
            },
        )
        .map_err(|e| RespError::operational(format!("Callback alloc failed: {e}"), false))?;
        py_task
            .call_method1("add_done_callback", (cb,))
            .map_err(|e| {
                self.clear_inflight();
                RespError::operational(format!("Failed to attach IAM token callback: {e}"), false)
            })?;
        Ok(Some(rx))
    }
}

/// Await completion of a Python-loop-scheduled async `token_provider` purely in Rust.
#[cfg(feature = "pyo3")]
pub async fn await_pending_python_token(mut rx: PendingPythonTokenRx) -> Result<(), RespError> {
    loop {
        if let Some(res) = rx.borrow_and_update().clone() {
            return res;
        }
        if rx.changed().await.is_err() {
            if let Some(res) = rx.borrow().clone() {
                return res;
            }
            return Err(RespError::cancelled(
                "IAM async token_provider task ended before producing a token",
            ));
        }
    }
}

#[cfg(feature = "pyo3")]
#[pyclass(
    name = "_RespTokenDoneCallback",
    module = "vertebra._valkey",
    frozen,
    immutable_type
)]
struct RespTokenDoneCallback {
    provider: Arc<PythonTokenProvider>,
    tx: SyncMutex<Option<PendingPythonTokenTx>>,
}

#[cfg(feature = "pyo3")]
#[pymethods]
impl RespTokenDoneCallback {
    fn __call__(&self, task: &Bound<'_, PyAny>) -> PyResult<()> {
        let outcome = if task
            .call_method0("cancelled")
            .and_then(|v| v.extract::<bool>())
            .unwrap_or(false)
        {
            self.provider.clear_inflight();
            Err(RespError::cancelled(
                "IAM async token_provider was cancelled",
            ))
        } else {
            match task.call_method0("result") {
                Ok(val) => match val.extract::<String>() {
                    Ok(token) => match self.provider.store_token(token) {
                        Ok(_) => Ok(()),
                        Err(e) => {
                            self.provider.clear_inflight();
                            Err(e)
                        }
                    },
                    Err(e) => {
                        self.provider.clear_inflight();
                        Err(RespError::config(format!(
                            "IAM token_provider must return str: {e}"
                        )))
                    }
                },
                Err(err) => {
                    self.provider.clear_inflight();
                    Err(RespError::gcp_auth(
                        format!("IAM async token_provider failed: {err}"),
                        false,
                    ))
                }
            }
        };
        if let Some(tx) = self.tx.lock().take() {
            let _ = tx.send(Some(outcome));
        }
        Ok(())
    }
}

/// Abstracted Google Cloud Memorystore IAM token source.
#[derive(Clone)]
pub enum GcpAuthSource {
    Handle(SharedGcpTokenCacheHandle),
    #[cfg(feature = "pyo3")]
    PythonCallable(Arc<PythonTokenProvider>),
}

impl std::fmt::Debug for GcpAuthSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Handle(_) => f.write_str("GcpAuthSource::Handle(..)"),
            #[cfg(feature = "pyo3")]
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

/// Core configuration struct backing both Valkey and Redis RESP clients.
#[derive(Clone, Debug)]
pub struct RespConfig {
    pub url_redacted: String,
    pub target: RespTarget,
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
    pub verify_sentinel_role: bool,
}

/// Backward-compatible alias for Valkey callers.
pub type ValkeyConfigInner = RespConfig;

/// Normalize and validate a Redis connection URL scheme (`redis://`, `rediss://`, `unix://`).
pub fn normalize_redis_url(raw: &str) -> Result<String, RespError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(RespError::config("Redis connection URL cannot be empty"));
    }

    let lower = trimmed.to_ascii_lowercase();
    if lower.starts_with("valkey://") || lower.starts_with("valkeys://") {
        return Err(RespError::config(
            "Valkey URLs ('valkey://', 'valkeys://') are not supported by vertebra._redis; install and use vertebra._valkey via vertebra-core[valkey].",
        ));
    }

    if lower.starts_with("memory://") {
        return Err(RespError::config(
            "In-memory scheme 'memory://' is not supported by vertebra._redis.",
        ));
    }

    if let Some(path_part) = trimmed.strip_prefix("unix://") {
        return Ok(format!("redis+unix:///{path_part}"));
    }

    if let Some(path_part) = trimmed.strip_prefix("unix:") {
        return Ok(format!("redis+unix:///{path_part}"));
    }

    if trimmed.starts_with("redis+unix://") || trimmed.starts_with("redis+unix:") {
        return Ok(trimmed.to_string());
    }

    if trimmed.starts_with("redis://") || trimmed.starts_with("rediss://") {
        return Ok(trimmed.to_string());
    }

    Err(RespError::config(format!(
        "Unsupported Redis URL scheme in '{trimmed}'; expected redis://, rediss://, or unix://"
    )))
}

impl RespConfig {
    /// Build a [`RespConfig`] purely in Rust from a Valkey URL without Python objects.
    pub fn from_url_str(url_str: &str) -> Result<Self, RespError> {
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

    /// Build a validated Valkey [`RespConfig`].
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
    ) -> Result<Self, RespError> {
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
                return Err(RespError::config("Valkey connection URL cannot be empty"));
            }
            let parsed = Url::parse(trimmed)
                .map_err(|e| RespError::config(format!("Invalid Valkey URL '{trimmed}': {e}")))?;
            match parsed.scheme() {
                "valkey" => {
                    parsed_tls = tls == Some(true);
                    let uhost = parsed.host_str().unwrap_or_default().trim();
                    if uhost.is_empty() {
                        return Err(RespError::config(format!(
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
                        return Err(RespError::config("valkeys:// scheme requires TLS enabled"));
                    }
                    parsed_tls = true;
                    let uhost = parsed.host_str().unwrap_or_default().trim();
                    if uhost.is_empty() {
                        return Err(RespError::config(format!(
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
                        return Err(RespError::config("unix:// transport does not support TLS"));
                    }
                    parsed_tls = false;
                    let sock_path = parsed.path().trim();
                    if sock_path.is_empty() || sock_path == "/" {
                        return Err(RespError::config(format!(
                            "unix:// URL must specify a socket filesystem path: '{trimmed}'"
                        )));
                    }
                    parsed_unix = Some(sock_path.to_string());
                    parsed_host = "localhost".to_string();
                    parsed_port = 0;
                }
                other => {
                    return Err(RespError::config(format!(
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
                        RespError::config(format!(
                            "Invalid database index '{path_trimmed}' in Valkey URL"
                        ))
                    })?;
                }
            }

            for (k, v) in parsed.query_pairs() {
                match k.as_ref() {
                    "db" => {
                        parsed_db = v.parse::<u16>().map_err(|_| {
                            RespError::config(format!("Invalid db query value '{v}'"))
                        })?;
                    }
                    "connect_timeout_ms" => {
                        parsed_connect_ms = v.parse::<u64>().map_err(|_| {
                            RespError::config(format!("Invalid connect_timeout_ms '{v}'"))
                        })?;
                    }
                    "command_timeout_ms" | "timeout_ms" => {
                        parsed_command_ms = v.parse::<u64>().map_err(|_| {
                            RespError::config(format!("Invalid command_timeout_ms '{v}'"))
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
            return Err(RespError::config(
                "Valkey host cannot be empty unless unix_socket_path, cluster_nodes, or sentinel_nodes is set",
            ));
        }

        if let Some(ref sock) = parsed_unix {
            if sock.trim().is_empty() {
                return Err(RespError::config("unix_socket_path cannot be empty"));
            }
            if cluster_enabled {
                return Err(RespError::config(
                    "Valkey Cluster mode cannot be combined with unix:// socket transport",
                ));
            }
        } else if sentinel_nodes.is_empty() && parsed_port == 0 && cluster_nodes.is_empty() {
            return Err(RespError::config("Valkey port must be 1..=65535"));
        }

        if !sentinel_nodes.is_empty()
            && sentinel_service_name
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(RespError::config(
                "sentinel_service_name is required when sentinel_nodes is provided",
            ));
        }

        if pool_max_size == 0 {
            return Err(RespError::config("pool_max_size must be at least 1"));
        }
        if pool_min_size > pool_max_size {
            return Err(RespError::config(format!(
                "pool_min_size ({pool_min_size}) cannot exceed pool_max_size ({pool_max_size})"
            )));
        }
        if parsed_connect_ms == 0 || parsed_command_ms == 0 {
            return Err(RespError::config(
                "connect_timeout_ms and command_timeout_ms must be greater than 0",
            ));
        }

        if tls_cert_pem.is_some() ^ tls_key_pem.is_some() {
            return Err(RespError::config(
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
            RespTarget::Unix {
                path: PathBuf::from(sock),
            }
        } else {
            RespTarget::Tcp {
                host: parsed_host.clone(),
                port: parsed_port,
            }
        };

        let url_redacted = format_redacted_url(
            &target,
            parsed_tls,
            "valkey",
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
            verify_sentinel_role: true,
        })
    }

    /// Build a validated Redis [`RespConfig`] from Redis client parameters.
    #[allow(clippy::too_many_arguments)]
    pub fn build_redis(
        raw_url: &str,
        username: Option<String>,
        password: Option<String>,
        client_name: String,
        protocol: u8,
        topology: RedisTopologyMode,
        sentinel_master_name: Option<String>,
        sentinel_nodes: Vec<(String, u16)>,
        cluster_nodes: Vec<(String, u16)>,
        max_redirections: usize,
        enable_client_side_caching: bool,
        client_tracking_prefixes: Vec<String>,
        client_cache_capacity: usize,
        client_cache_ttl_seconds: Option<f64>,
        enable_gcp_iam_auth: bool,
        gcp_auth: Option<GcpAuthSource>,
        max_connections: usize,
    ) -> Result<Self, RespError> {
        ensure_rustls_provider();

        let normalized = normalize_redis_url(raw_url)?;
        let is_unix = normalized.starts_with("redis+unix:");

        if is_unix && topology != RedisTopologyMode::Standalone {
            return Err(RespError::config(
                "Unix domain socket URLs cannot be used with cluster or sentinel topology.",
            ));
        }

        if topology == RedisTopologyMode::Sentinel
            && sentinel_master_name
                .as_deref()
                .map(str::trim)
                .unwrap_or("")
                .is_empty()
        {
            return Err(RespError::config(
                "sentinel_master_name is required when topology is 'sentinel'.",
            ));
        }

        let parsed_host: String;
        let parsed_port: u16;
        let mut parsed_db = 0u16;
        let mut url_user: Option<String> = None;
        let mut url_pass: Option<String> = None;
        let mut parsed_tls = false;
        let target: RespTarget;

        if is_unix {
            let trimmed = raw_url.trim();
            let raw_path_and_query = trimmed
                .strip_prefix("redis+unix://")
                .or_else(|| trimmed.strip_prefix("redis+unix:"))
                .or_else(|| trimmed.strip_prefix("unix://"))
                .or_else(|| trimmed.strip_prefix("unix:"))
                .unwrap_or(trimmed);
            let (sock_part, query_part) = raw_path_and_query
                .split_once('?')
                .map_or((raw_path_and_query, ""), |(p, q)| (p, q));
            let sock_path = if sock_part.starts_with("//") {
                format!("/{}", sock_part.trim_start_matches('/'))
            } else if sock_part.starts_with('/') {
                sock_part.to_string()
            } else {
                format!("/{sock_part}")
            };
            if sock_path.trim().is_empty() || sock_path == "/" {
                return Err(RespError::config(format!(
                    "unix:// URL must specify a socket filesystem path: '{trimmed}'"
                )));
            }
            for pair in query_part.split('&') {
                if let Some((k, v)) = pair.split_once('=')
                    && k == "db"
                    && let Ok(db_idx) = v.parse::<u16>()
                {
                    parsed_db = db_idx;
                }
            }
            parsed_host = "localhost".to_string();
            parsed_port = 0;
            target = RespTarget::Unix {
                path: PathBuf::from(sock_path),
            };
        } else {
            let parsed = Url::parse(&normalized)
                .map_err(|e| RespError::config(format!("Invalid Redis URL '{raw_url}': {e}")))?;
            parsed_tls = parsed.scheme() == "rediss";
            let uhost = parsed.host_str().unwrap_or_default().trim();
            if uhost.is_empty() {
                return Err(RespError::config(format!(
                    "Hostless Redis URL is invalid: '{raw_url}'"
                )));
            }
            parsed_host = uhost
                .trim_start_matches('[')
                .trim_end_matches(']')
                .to_string();
            parsed_port = parsed.port().unwrap_or(6379);
            if !parsed.username().is_empty() {
                url_user = Some(percent_decode(parsed.username()));
            }
            if let Some(p) = parsed.password() {
                url_pass = Some(percent_decode(p));
            }
            let path_trimmed = parsed.path().trim_matches('/');
            if !path_trimmed.is_empty() {
                parsed_db = path_trimmed.parse::<u16>().map_err(|_| {
                    RespError::config(format!(
                        "Invalid database index '{path_trimmed}' in Redis URL"
                    ))
                })?;
            }
            for (k, v) in parsed.query_pairs() {
                if k == "db"
                    && let Ok(db_idx) = v.parse::<u16>()
                {
                    parsed_db = db_idx;
                }
            }
            target = RespTarget::Tcp {
                host: parsed_host.clone(),
                port: parsed_port,
            };
        }

        let effective_user = username.or(url_user);
        let effective_pass = password.or(url_pass);

        let tls_config = if parsed_tls {
            Some(build_rustls_client_config(None, None, None, false)?)
        } else {
            None
        };

        let effective_gcp = if enable_gcp_iam_auth && gcp_auth.is_none() {
            GcpTokenCache::from_options(&HashMap::new(), &[])
                .map(|c| GcpAuthSource::Handle(SharedGcpTokenCacheHandle::from_arc(c)))
        } else {
            gcp_auth
        };

        let format_host_port = |(h, p): &(String, u16)| {
            if h.contains(':') && !h.starts_with('[') {
                format!("[{h}]:{p}")
            } else {
                format!("{h}:{p}")
            }
        };

        let cluster_enabled = topology == RedisTopologyMode::Cluster;
        let cluster_node_specs: Vec<String> = if cluster_enabled {
            if cluster_nodes.is_empty() {
                vec![format_host_port(&(parsed_host.clone(), parsed_port))]
            } else {
                cluster_nodes.iter().map(format_host_port).collect()
            }
        } else {
            Vec::new()
        };

        let sentinel_node_specs: Vec<String> = if topology == RedisTopologyMode::Sentinel {
            if sentinel_nodes.is_empty() {
                vec![format_host_port(&(parsed_host.clone(), parsed_port))]
            } else {
                sentinel_nodes.iter().map(format_host_port).collect()
            }
        } else {
            Vec::new()
        };

        let effective_sentinel_name = if topology == RedisTopologyMode::Sentinel {
            sentinel_master_name
        } else {
            None
        };

        let ttl = client_cache_ttl_seconds
            .filter(|s| s.is_finite() && *s > 0.0)
            .map(Duration::from_secs_f64);

        let url_redacted = format_redacted_url(
            &target,
            parsed_tls,
            "redis",
            parsed_db,
            effective_user.as_deref(),
            effective_pass.is_some(),
        );

        Ok(Self {
            url_redacted,
            target,
            host: parsed_host,
            port: parsed_port,
            db: parsed_db,
            username: effective_user,
            password: effective_pass,
            tls: parsed_tls,
            tls_ca_pem: None,
            tls_cert_pem: None,
            tls_key_pem: None,
            tls_server_name: None,
            tls_insecure: false,
            tls_config,
            pool_min_size: 1,
            pool_max_size: max_connections.max(1),
            connect_timeout: Duration::from_millis(1500),
            command_timeout: Duration::from_millis(2000),
            max_redirections,
            cluster_enabled,
            cluster_nodes: cluster_node_specs,
            read_from_replicas: false,
            sentinel_service_name: effective_sentinel_name,
            sentinel_nodes: sentinel_node_specs,
            sentinel_username: None,
            sentinel_password: None,
            client_tracking_enabled: enable_client_side_caching,
            client_tracking_prefixes,
            client_tracking_capacity: client_cache_capacity.max(1),
            client_tracking_ttl: ttl,
            client_name: if client_name.is_empty() {
                None
            } else {
                Some(client_name)
            },
            enable_gcp_memorystore_iam: enable_gcp_iam_auth,
            gcp_auth: effective_gcp,
            protocol_resp3: protocol == 3,
            enable_capa_redirect: false,
            verify_sentinel_role: false,
        })
    }

    /// Resolve `(username, password, token_generation)` for an outgoing `HELLO`/`AUTH`.
    pub async fn resolve_auth(
        &self,
        invalidate_generation: Option<u64>,
    ) -> Result<(Option<String>, Option<String>, Option<u64>), RespError> {
        if !self.enable_gcp_memorystore_iam {
            return Ok((self.username.clone(), self.password.clone(), None));
        }

        let auth_source = self.gcp_auth.as_ref().ok_or_else(|| {
            RespError::gcp_auth(
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
                        RespError::gcp_auth(format!("GCP Memorystore IAM token error: {e}"), true)
                    })?;
                Ok((
                    Some(effective_user),
                    Some(snap.access_token.clone()),
                    Some(snap.generation),
                ))
            }
            #[cfg(feature = "pyo3")]
            GcpAuthSource::PythonCallable(provider) => {
                let (token, gen_id) = provider.resolve_cached(invalidate_generation)?;
                Ok((Some(effective_user), Some(token), Some(gen_id)))
            }
        }
    }

    /// Ensure any Python `token_provider` is resolved on the caller's Python thread before `py.detach`.
    #[cfg(feature = "pyo3")]
    pub fn ensure_python_token_sync(
        &self,
        py: Python<'_>,
        force_refresh: bool,
    ) -> Result<(), RespError> {
        if !self.enable_gcp_memorystore_iam {
            return Ok(());
        }
        if let Some(GcpAuthSource::PythonCallable(ref provider)) = self.gcp_auth {
            provider.ensure_token_sync(py, force_refresh)?;
        }
        Ok(())
    }

    /// Return `true` when IAM auth uses a Python callable whose cached token was invalidated.
    #[cfg(feature = "pyo3")]
    #[must_use]
    pub fn has_unprimed_python_token(&self) -> bool {
        if !self.enable_gcp_memorystore_iam {
            return false;
        }
        matches!(
            self.gcp_auth,
            Some(GcpAuthSource::PythonCallable(ref provider)) if !provider.has_cached_token()
        )
    }

    /// Prime or schedule any Python `token_provider` on the caller's Python thread before spawning a Tokio task.
    #[cfg(feature = "pyo3")]
    pub fn prepare_python_token_async(
        &self,
        py: Python<'_>,
        force_refresh: bool,
    ) -> Result<Option<PendingPythonTokenRx>, RespError> {
        if !self.enable_gcp_memorystore_iam {
            return Ok(None);
        }
        match self.gcp_auth {
            Some(GcpAuthSource::PythonCallable(ref provider)) => {
                provider.prepare_token_async(py, force_refresh)
            }
            _ => Ok(None),
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
    target: &RespTarget,
    tls: bool,
    scheme_base: &str,
    db: u16,
    username: Option<&str>,
    has_password: bool,
) -> String {
    match target {
        RespTarget::Unix { path } => format!("unix://{}?db={db}", path.display()),
        RespTarget::Tcp { host, port } => {
            let scheme = if tls {
                format!("{scheme_base}s")
            } else {
                scheme_base.to_string()
            };
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
) -> Result<Arc<ClientConfig>, RespError> {
    ensure_rustls_provider();

    let cert_pair = match (client_cert_pem, client_key_pem) {
        (Some(cert_str), Some(key_str)) => {
            let mut cert_reader = Cursor::new(cert_str.as_bytes());
            let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut cert_reader)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| {
                    RespError::config(format!("Invalid mTLS client certificate PEM: {e}"))
                })?;
            if certs.is_empty() {
                return Err(RespError::config(
                    "mTLS tls_cert_pem contained no certificates",
                ));
            }
            let mut key_reader = Cursor::new(key_str.as_bytes());
            let key: PrivateKeyDer<'static> = rustls_pemfile::private_key(&mut key_reader)
                .map_err(|e| {
                    RespError::config(format!("Invalid mTLS client private key PEM: {e}"))
                })?
                .ok_or_else(|| RespError::config("mTLS tls_key_pem contained no private key"))?;
            Some((certs, key))
        }
        (None, None) => None,
        _ => {
            return Err(RespError::config(
                "Both tls_cert_pem and tls_key_pem are required for mTLS",
            ));
        }
    };

    if insecure {
        let builder = ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(InsecureServerCertVerifier));
        let cfg = match cert_pair {
            Some((certs, key)) => builder
                .with_client_auth_cert(certs, key)
                .map_err(|e| RespError::config(format!("Invalid mTLS client credentials: {e}")))?,
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
            .map_err(|e| RespError::config(format!("Invalid tls_ca_pem certificate: {e}")))?;
        if custom_certs.is_empty() {
            return Err(RespError::config("tls_ca_pem contained no certificates"));
        }
        for cert in custom_certs {
            roots
                .add(cert)
                .map_err(|e| RespError::config(format!("Invalid CA certificate: {e}")))?;
        }
    }

    let builder = ClientConfig::builder().with_root_certificates(roots);
    let cfg = match cert_pair {
        Some((certs, key)) => builder
            .with_client_auth_cert(certs, key)
            .map_err(|e| RespError::config(format!("Invalid mTLS client credentials: {e}")))?,
        None => builder.with_no_client_auth(),
    };
    Ok(Arc::new(cfg))
}

/// Extract a [`GcpAuthSource`] from a `GcpTokenCache`, `PyCapsule`, or Python callable.
#[cfg(feature = "pyo3")]
pub fn extract_gcp_auth_source(
    py: Python<'_>,
    provider_obj: &Bound<'_, PyAny>,
) -> Result<GcpAuthSource, RespError> {
    if let Ok(capsule) = provider_obj.cast::<PyCapsule>() {
        return extract_capsule(capsule);
    }
    if provider_obj.hasattr("_capsule").unwrap_or(false) {
        let cap_obj = provider_obj.call_method0("_capsule").map_err(|e| {
            RespError::config(format!(
                "Failed to inspect _capsule() on token_provider: {e}"
            ))
        })?;
        let capsule = cap_obj.cast::<PyCapsule>().map_err(|_| {
            RespError::config("token_provider._capsule() did not return a PyCapsule")
        })?;
        return extract_capsule(capsule);
    }
    if provider_obj.is_callable() {
        return Ok(GcpAuthSource::PythonCallable(PythonTokenProvider::new(
            provider_obj.clone().unbind(),
        )));
    }
    let _ = py;
    Err(RespError::config(
        "token_provider must be a GcpTokenCache, GcpTokenCacheCapsule PyCapsule, or callable",
    ))
}

/// Extract a [`GcpAuthSource::Handle`] strictly from a `GcpTokenCache` `PyCapsule`.
#[cfg(feature = "pyo3")]
pub fn extract_token_cache_capsule(
    capsule_any: &Bound<'_, PyAny>,
) -> Result<GcpAuthSource, RespError> {
    let capsule = capsule_any.cast::<PyCapsule>().map_err(|_| {
        RespError::config("token_cache_capsule must be a valid GcpTokenCache PyCapsule")
    })?;
    extract_capsule(capsule)
}

#[cfg(feature = "pyo3")]
fn extract_capsule(capsule: &Bound<'_, PyCapsule>) -> Result<GcpAuthSource, RespError> {
    let expected_name = GCP_TOKEN_CACHE_CAPSULE_NAME;
    let ptr = capsule
        .pointer_checked(Some(expected_name))
        .map_err(|e| RespError::config(format!("Invalid GcpTokenCacheCapsule: {e}")))?;
    // SAFETY: `ptr` was validated against `GCP_TOKEN_CACHE_CAPSULE_NAME` and points to a
    // `#[repr(C)] GcpTokenCacheCapsule` owned by the Python capsule.
    let handle = unsafe {
        SharedGcpTokenCacheHandle::from_capsule_ptr(ptr.as_ptr().cast::<GcpTokenCacheCapsule>())
    }
    .map_err(|e| RespError::config(format!("Invalid GcpTokenCacheCapsule: {e}")))?;
    Ok(GcpAuthSource::Handle(handle))
}
