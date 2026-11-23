//! PyO3 configuration wrapper (`ValkeyClientConfig`) delegating to `vertebra_resp::config`.

use std::sync::Arc;

use pyo3::prelude::*;

pub use vertebra_resp::config::{
    GcpAuthSource, PendingPythonTokenRx, PythonTokenProvider, RespConfig, RespTarget,
    ValkeyConfigInner, ValkeyTarget, await_pending_python_token, extract_gcp_auth_source,
};

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
