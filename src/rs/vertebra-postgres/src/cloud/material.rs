//! Single-flight `CloudMaterialCache` (`240s` refresh lead), bounded `1 MiB` Admin HTTP helpers,
//! and `CloudConnector` orchestration for Cloud SQL (`v1beta4`) and AlloyDB (`v1alpha`).

use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};

use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::time::Instant;
use vertebra_core::gcp_auth::current_unix_ms;
use vertebra_db::connection::{CloudPeerIdentity, EndpointTarget};
use vertebra_db::dialer::{AlloyDbClusterUri, AuthType, CloudSqlInstanceUri, IpType};

use crate::cloud::{alloydb, cloudsql};
use crate::credentials::{
    ALLOYDB_LOGIN_SCOPE, CLOUDSQL_LOGIN_SCOPE, ScopedLoginSnapshot, ScopedTokenProvider,
};
use crate::errors::PostgresError;

/// Refresh lead window (`240_000 ms` = `240s`) before `effective_expiry_ms`.
pub const MATERIAL_REFRESH_LEAD_MS: u64 = 240_000;

/// Maximum buffered HTTP response size (`1_048_576` bytes = `1 MiB`) from Cloud Admin APIs.
pub const MAX_ADMIN_RESPONSE_BYTES: usize = 1_048_576;

/// Default Google Cloud SQL Admin API base endpoint.
pub const DEFAULT_CLOUDSQL_ADMIN_ENDPOINT: &str = "https://sqladmin.googleapis.com";

/// Default Google Cloud AlloyDB Admin API base endpoint.
pub const DEFAULT_ALLOYDB_ADMIN_ENDPOINT: &str = "https://alloydb.googleapis.com";

/// Clock abstraction for deterministic expiry and refresh-lead testing.
pub trait Clock: Send + Sync {
    /// Return current Unix epoch timestamp in milliseconds.
    fn now_unix_ms(&self) -> u64;
}

/// Production wall-clock implementation backed by [`current_unix_ms`].
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_unix_ms(&self) -> u64 {
        current_unix_ms()
    }
}

/// Target cloud database resource (`CloudSql` or `AlloyDb`).
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum CloudResourceTarget {
    CloudSql(CloudSqlInstanceUri),
    AlloyDb(AlloyDbClusterUri),
}

/// Redacted OAuth2 login token snapshot bound to a cloud session or certificate.
#[derive(Clone, PartialEq, Eq)]
pub struct LoginTokenSnapshot {
    pub token: String,
    pub expiry_ms: Option<u64>,
    pub generation: u64,
}

impl fmt::Debug for LoginTokenSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LoginTokenSnapshot")
            .field("token", &"<redacted>")
            .field("expiry_ms", &self.expiry_ms)
            .field("generation", &self.generation)
            .finish()
    }
}

/// Configuration for [`CloudConnector::prepare`].
#[derive(Clone)]
pub struct CloudConnectorConfig {
    pub target: CloudResourceTarget,
    pub ip_type: IpType,
    pub auth_type: AuthType,
    pub cloud_auth_options: HashMap<String, String>,
    pub iam_auth_options: Option<HashMap<String, String>>,
    pub admin_endpoint: String,
    pub refresh_lead_ms: u64,
}

impl CloudConnectorConfig {
    /// Construct a new [`CloudConnectorConfig`] with the service's default HTTPS Admin endpoint.
    pub fn new(target: CloudResourceTarget, ip_type: IpType, auth_type: AuthType) -> Self {
        let admin_endpoint = match &target {
            CloudResourceTarget::CloudSql(_) => DEFAULT_CLOUDSQL_ADMIN_ENDPOINT.to_string(),
            CloudResourceTarget::AlloyDb(_) => DEFAULT_ALLOYDB_ADMIN_ENDPOINT.to_string(),
        };
        Self {
            target,
            ip_type,
            auth_type,
            cloud_auth_options: HashMap::new(),
            iam_auth_options: None,
            admin_endpoint,
            refresh_lead_ms: MATERIAL_REFRESH_LEAD_MS,
        }
    }

    /// Override the Admin API base endpoint (must be `https://` or an explicit loopback `http://`).
    pub fn with_admin_endpoint(mut self, endpoint: impl Into<String>) -> Result<Self, PostgresError> {
        let ep = endpoint.into();
        validate_admin_endpoint(&ep)?;
        self.admin_endpoint = ep.trim_end_matches('/').to_string();
        Ok(self)
    }

    /// Set the Admin API credential option map (`cloud_auth_options`).
    pub fn with_cloud_auth_options(mut self, opts: HashMap<String, String>) -> Self {
        self.cloud_auth_options = opts;
        self
    }

    /// Set the designated database IAM login credential option map (`iam_auth_options`).
    pub fn with_iam_auth_options(mut self, opts: Option<HashMap<String, String>>) -> Self {
        self.iam_auth_options = opts;
        self
    }

    /// Override the refresh lead window in milliseconds (default `240_000`).
    pub fn with_refresh_lead_ms(mut self, lead_ms: u64) -> Self {
        self.refresh_lead_ms = lead_ms;
        self
    }
}

impl fmt::Debug for CloudConnectorConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut cloud_keys: Vec<&str> = self.cloud_auth_options.keys().map(String::as_str).collect();
        cloud_keys.sort_unstable();
        let iam_keys: Option<Vec<&str>> = self.iam_auth_options.as_ref().map(|m| {
            let mut k: Vec<&str> = m.keys().map(String::as_str).collect();
            k.sort_unstable();
            k
        });
        f.debug_struct("CloudConnectorConfig")
            .field("target", &self.target)
            .field("ip_type", &self.ip_type)
            .field("auth_type", &self.auth_type)
            .field("cloud_auth_option_keys", &cloud_keys)
            .field("iam_auth_option_keys", &iam_keys)
            .field("admin_endpoint", &self.admin_endpoint)
            .field("refresh_lead_ms", &self.refresh_lead_ms)
            .finish()
    }
}

/// Validated, cacheable Cloud SQL or AlloyDB endpoint and mTLS material snapshot.
#[derive(Clone, PartialEq, Eq)]
pub struct PreparedCloudMaterial {
    pub host: String,
    pub port: u16,
    pub endpoint_target: EndpointTarget,
    pub peer_dns_name: Option<String>,
    pub expected_cn: Option<String>,
    pub auth_type: AuthType,
    pub server_ca_pem: Vec<u8>,
    pub client_cert_chain_pem: Vec<u8>,
    pub client_key_pem: Vec<u8>,
    pub signed_cert_not_after_ms: u64,
    pub effective_expiry_ms: u64,
    pub login_token: Option<LoginTokenSnapshot>,
    pub(crate) login_snapshot: Option<ScopedLoginSnapshot>,
}

impl fmt::Debug for PreparedCloudMaterial {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PreparedCloudMaterial")
            .field("host", &self.host)
            .field("port", &self.port)
            .field("peer_dns_name", &self.peer_dns_name)
            .field("expected_cn", &self.expected_cn)
            .field("auth_type", &self.auth_type)
            .field("server_ca_pem_len", &self.server_ca_pem.len())
            .field("client_cert_chain_pem_len", &self.client_cert_chain_pem.len())
            .field("client_key_pem", &"<redacted>")
            .field("signed_cert_not_after_ms", &self.signed_cert_not_after_ms)
            .field("effective_expiry_ms", &self.effective_expiry_ms)
            .field("login_token", &self.login_token)
            .finish()
    }
}

/// Composite cache key isolating target, IP mode, auth mode, Admin endpoint, and credential identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CloudMaterialKey {
    pub credential_fingerprint: u64,
    pub target: CloudResourceTarget,
    pub ip_type: IpType,
    pub admin_endpoint: String,
    pub auth_type: AuthType,
}

impl CloudMaterialKey {
    /// Derive a [`CloudMaterialKey`] from a [`CloudConnectorConfig`].
    pub fn from_config(config: &CloudConnectorConfig) -> Self {
        Self {
            credential_fingerprint: compute_credential_fingerprint(
                &config.cloud_auth_options,
                config.iam_auth_options.as_ref(),
            ),
            target: config.target.clone(),
            ip_type: config.ip_type,
            admin_endpoint: config.admin_endpoint.trim_end_matches('/').to_string(),
            auth_type: config.auth_type,
        }
    }
}

type KeySlot = Arc<tokio::sync::Mutex<Option<PreparedCloudMaterial>>>;

/// Single-flight cache of [`PreparedCloudMaterial`] keyed by [`CloudMaterialKey`].
pub struct CloudMaterialCache {
    clock: Arc<dyn Clock>,
    slots: Mutex<HashMap<CloudMaterialKey, KeySlot>>,
}

impl fmt::Debug for CloudMaterialCache {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = self.slots.lock().map(|g| g.len()).unwrap_or(0);
        f.debug_struct("CloudMaterialCache")
            .field("keys", &len)
            .finish()
    }
}

impl CloudMaterialCache {
    /// Create an empty [`CloudMaterialCache`] using `clock`.
    pub fn new(clock: Arc<dyn Clock>) -> Self {
        Self {
            clock,
            slots: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn slot_for(&self, key: CloudMaterialKey) -> KeySlot {
        let mut guard = self.slots.lock().expect("CloudMaterialCache slots lock poisoned");
        Arc::clone(
            guard
                .entry(key)
                .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(None))),
        )
    }

    /// Return a cached [`PreparedCloudMaterial`] only if it is outside the refresh lead window.
    pub fn get_if_fresh(
        &self,
        key: &CloudMaterialKey,
        refresh_lead_ms: u64,
    ) -> Option<PreparedCloudMaterial> {
        let slot = {
            let guard = self.slots.lock().ok()?;
            Arc::clone(guard.get(key)?)
        };
        let entry_guard = slot.try_lock().ok()?;
        let entry = entry_guard.as_ref()?;
        let now_ms = self.clock.now_unix_ms();
        if now_ms.saturating_add(refresh_lead_ms) < entry.effective_expiry_ms {
            Some(entry.clone())
        } else {
            None
        }
    }

    /// Evict any cached material for `key`.
    pub fn invalidate(&self, key: &CloudMaterialKey) {
        if let Ok(mut guard) = self.slots.lock() {
            guard.remove(key);
        }
    }
}

/// Cloud SQL and AlloyDB Admin API discovery and single-flight material coordinator.
pub struct CloudConnector {
    http: reqwest::Client,
    cache: CloudMaterialCache,
    clock: Arc<dyn Clock>,
}

impl Default for CloudConnector {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for CloudConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CloudConnector")
            .field("cache", &self.cache)
            .finish()
    }
}

impl CloudConnector {
    /// Construct a [`CloudConnector`] using [`SystemClock`].
    pub fn new() -> Self {
        Self::with_clock(Arc::new(SystemClock))
    }

    /// Construct a [`CloudConnector`] with an injected [`Clock`].
    pub fn with_clock(clock: Arc<dyn Clock>) -> Self {
        let http = reqwest::Client::builder()
            .no_proxy()
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        let cache = CloudMaterialCache::new(Arc::clone(&clock));
        Self { http, cache, clock }
    }

    /// Return current Unix epoch milliseconds from the connector's [`Clock`].
    pub fn now_unix_ms(&self) -> u64 {
        self.clock.now_unix_ms()
    }

    /// Borrow the underlying [`Clock`].
    pub fn clock(&self) -> &Arc<dyn Clock> {
        &self.clock
    }

    /// Borrow the underlying [`CloudMaterialCache`].
    pub const fn cache(&self) -> &CloudMaterialCache {
        &self.cache
    }

    /// Acquire fresh or cached [`PreparedCloudMaterial`] under `absolute_deadline`.
    pub async fn prepare(
        &self,
        config: &CloudConnectorConfig,
        absolute_deadline: Instant,
    ) -> Result<PreparedCloudMaterial, PostgresError> {
        validate_admin_endpoint(&config.admin_endpoint)?;
        if Instant::now() >= absolute_deadline {
            return Err(PostgresError::Transport(
                "cloud material preparation deadline exceeded".to_string(),
            ));
        }

        let key = CloudMaterialKey::from_config(config);
        let slot = self.cache.slot_for(key);

        let mut guard = tokio::time::timeout_at(absolute_deadline, slot.lock())
            .await
            .map_err(|_| {
                PostgresError::Transport(
                    "timed out waiting for single-flight cloud material lock".to_string(),
                )
            })?;

        let now_ms = self.clock.now_unix_ms();
        if let Some(ref existing) = *guard
            && now_ms.saturating_add(config.refresh_lead_ms) < existing.effective_expiry_ms
        {
            return Ok(existing.clone());
        }

        *guard = None;

        let prepared = self.fetch_fresh_material(config, absolute_deadline).await?;
        *guard = Some(prepared.clone());
        Ok(prepared)
    }

    async fn fetch_fresh_material(
        &self,
        config: &CloudConnectorConfig,
        absolute_deadline: Instant,
    ) -> Result<PreparedCloudMaterial, PostgresError> {
        let provider = ScopedTokenProvider::new(
            Some(config.cloud_auth_options.clone()),
            config.iam_auth_options.clone(),
            Arc::clone(&self.clock),
        );

        match &config.target {
            CloudResourceTarget::CloudSql(uri) => {
                let admin_snap = provider.resolve_admin_token(absolute_deadline).await?;
                let login_snap = if config.auth_type == AuthType::Iam {
                    let required = vec![CLOUDSQL_LOGIN_SCOPE.to_string()];
                    let snap = provider
                        .resolve_login_snapshot(&required, absolute_deadline)
                        .await?;
                    snap.ensure_usable(self.clock.now_unix_ms())?;
                    Some(snap)
                } else {
                    None
                };

                let discovery = cloudsql::fetch_cloudsql_discovery(
                    self,
                    uri,
                    config.ip_type,
                    &config.admin_endpoint,
                    &admin_snap.access_token,
                    login_snap.as_ref().map(ScopedLoginSnapshot::access_token),
                    absolute_deadline,
                )
                .await?;

                let signed_not_after_ms = discovery.client_identity.signed_not_after_ms;
                let effective_expiry_ms = match login_snap
                    .as_ref()
                    .map(|s| s.snapshot.expires_at_unix_ms)
                {
                    Some(tok_exp) => signed_not_after_ms.min(tok_exp),
                    None => signed_not_after_ms,
                };
                let now_after = self.clock.now_unix_ms();
                if effective_expiry_ms <= now_after {
                    return Err(PostgresError::Transport(
                        "Cloud SQL effective material expiry is already in the past".to_string(),
                    ));
                }

                let sock_addr = parse_or_placeholder_socket_addr(&discovery.host, discovery.port);
                let endpoint_target = EndpointTarget::CloudSql {
                    instance: uri.clone(),
                    addr: sock_addr,
                    server_ca_pem: discovery.server_ca_pem.clone(),
                    client_cert_pem: discovery.client_identity.cert_chain_pem.clone(),
                    client_key_pem: discovery.client_identity.private_key_pem.clone(),
                    peer_identity: CloudPeerIdentity::CloudSql {
                        server_name: discovery.peer_dns_name.clone(),
                        expected_cn: discovery.expected_cn.clone(),
                    },
                };

                let login_token = login_snap.as_ref().map(|s| LoginTokenSnapshot {
                    token: s.snapshot.access_token.clone(),
                    expiry_ms: Some(s.snapshot.expires_at_unix_ms),
                    generation: s.snapshot.generation,
                });

                Ok(PreparedCloudMaterial {
                    host: discovery.host,
                    port: discovery.port,
                    endpoint_target,
                    peer_dns_name: discovery.peer_dns_name,
                    expected_cn: Some(discovery.expected_cn),
                    auth_type: config.auth_type,
                    server_ca_pem: discovery.server_ca_pem,
                    client_cert_chain_pem: discovery.client_identity.cert_chain_pem,
                    client_key_pem: discovery.client_identity.private_key_pem,
                    signed_cert_not_after_ms: signed_not_after_ms,
                    effective_expiry_ms,
                    login_token,
                    login_snapshot: login_snap,
                })
            }
            CloudResourceTarget::AlloyDb(uri) => {
                let admin_snap = provider.resolve_admin_token(absolute_deadline).await?;
                let required = vec![ALLOYDB_LOGIN_SCOPE.to_string()];
                let login_snap = provider
                    .resolve_login_snapshot(&required, absolute_deadline)
                    .await?;
                login_snap.ensure_usable(self.clock.now_unix_ms())?;

                let discovery = alloydb::fetch_alloydb_discovery(
                    self,
                    uri,
                    config.ip_type,
                    &config.admin_endpoint,
                    &admin_snap.access_token,
                    absolute_deadline,
                )
                .await?;

                let signed_not_after_ms = discovery.client_identity.signed_not_after_ms;
                let effective_expiry_ms =
                    signed_not_after_ms.min(login_snap.snapshot.expires_at_unix_ms);
                let now_after = self.clock.now_unix_ms();
                if effective_expiry_ms <= now_after {
                    return Err(PostgresError::Transport(
                        "AlloyDB effective material expiry is already in the past".to_string(),
                    ));
                }

                let sock_addr = parse_or_placeholder_socket_addr(&discovery.host, discovery.port);
                let expected_san = discovery
                    .peer_dns_name
                    .clone()
                    .unwrap_or_else(|| discovery.host.clone());
                let endpoint_target = EndpointTarget::AlloyDb {
                    cluster: uri.clone(),
                    addr: sock_addr,
                    server_ca_pem: discovery.server_ca_pem.clone(),
                    client_cert_pem: discovery.client_identity.cert_chain_pem.clone(),
                    client_key_pem: discovery.client_identity.private_key_pem.clone(),
                    user_agent: format!("vertebra-postgres/{}", env!("CARGO_PKG_VERSION")),
                    oauth2_token: login_snap.snapshot.access_token.clone(),
                    auth_type: config.auth_type,
                    peer_identity: CloudPeerIdentity::AlloyDb { expected_san },
                };

                let login_token = LoginTokenSnapshot {
                    token: login_snap.snapshot.access_token.clone(),
                    expiry_ms: Some(login_snap.snapshot.expires_at_unix_ms),
                    generation: login_snap.snapshot.generation,
                };

                Ok(PreparedCloudMaterial {
                    host: discovery.host,
                    port: discovery.port,
                    endpoint_target,
                    peer_dns_name: discovery.peer_dns_name,
                    expected_cn: None,
                    auth_type: config.auth_type,
                    server_ca_pem: discovery.server_ca_pem,
                    client_cert_chain_pem: discovery.client_identity.cert_chain_pem,
                    client_key_pem: discovery.client_identity.private_key_pem,
                    signed_cert_not_after_ms: signed_not_after_ms,
                    effective_expiry_ms,
                    login_token: Some(login_token),
                    login_snapshot: Some(login_snap),
                })
            }
        }
    }

    /// Execute a bounded `GET` JSON request up to [`MAX_ADMIN_RESPONSE_BYTES`] under `absolute_deadline`.
    pub async fn get_json<T: DeserializeOwned>(
        &self,
        url: &str,
        bearer_token: &str,
        absolute_deadline: Instant,
    ) -> Result<T, PostgresError> {
        let req = self.http.get(url).bearer_auth(bearer_token);
        self.execute_bounded_json(req, bearer_token, absolute_deadline)
            .await
    }

    /// Execute a bounded `POST` JSON request up to [`MAX_ADMIN_RESPONSE_BYTES`] under `absolute_deadline`.
    pub async fn post_json<B: Serialize + ?Sized, T: DeserializeOwned>(
        &self,
        url: &str,
        bearer_token: &str,
        body: &B,
        absolute_deadline: Instant,
    ) -> Result<T, PostgresError> {
        let payload = serde_json::to_vec(body).map_err(|e| {
            PostgresError::Protocol(format!("failed to serialize Admin JSON request: {e}"))
        })?;
        let req = self
            .http
            .post(url)
            .bearer_auth(bearer_token)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(payload);
        self.execute_bounded_json(req, bearer_token, absolute_deadline)
            .await
    }

    async fn execute_bounded_json<T: DeserializeOwned>(
        &self,
        req: reqwest::RequestBuilder,
        bearer_token: &str,
        absolute_deadline: Instant,
    ) -> Result<T, PostgresError> {
        if Instant::now() >= absolute_deadline {
            return Err(PostgresError::Transport(
                "Admin HTTP request deadline exceeded before send".to_string(),
            ));
        }

        let mut resp = tokio::time::timeout_at(absolute_deadline, req.send())
            .await
            .map_err(|_| PostgresError::Transport("Admin HTTP request timed out".to_string()))?
            .map_err(|e| {
                PostgresError::Transport(redact_secret(
                    &format!("Admin HTTP request failed: {e}"),
                    bearer_token,
                ))
            })?;

        let status = resp.status();
        if !status.is_success() {
            return Err(PostgresError::Transport(format!(
                "Admin HTTP request failed with status {}",
                status.as_u16()
            )));
        }

        if let Some(content_len) = resp.content_length()
            && content_len > MAX_ADMIN_RESPONSE_BYTES as u64
        {
            return Err(PostgresError::Protocol(format!(
                "Admin HTTP response content-length {content_len} exceeds cap {MAX_ADMIN_RESPONSE_BYTES}"
            )));
        }

        let mut buf = Vec::new();
        loop {
            let chunk_opt = tokio::time::timeout_at(absolute_deadline, resp.chunk())
                .await
                .map_err(|_| {
                    PostgresError::Transport("Admin HTTP response body read timed out".to_string())
                })?
                .map_err(|e| {
                    PostgresError::Transport(redact_secret(
                        &format!("Admin HTTP response body read failed: {e}"),
                        bearer_token,
                    ))
                })?;

            match chunk_opt {
                Some(chunk) => {
                    if buf.len().saturating_add(chunk.len()) > MAX_ADMIN_RESPONSE_BYTES {
                        return Err(PostgresError::Protocol(format!(
                            "Admin HTTP response body exceeds maximum {MAX_ADMIN_RESPONSE_BYTES} bytes"
                        )));
                    }
                    buf.extend_from_slice(&chunk);
                }
                None => break,
            }
        }

        serde_json::from_slice::<T>(&buf).map_err(|e| {
            PostgresError::Protocol(redact_secret(
                &format!("failed to decode Admin JSON response: {e}"),
                bearer_token,
            ))
        })
    }
}

/// Validate that `admin_endpoint` uses `https://` or an explicit loopback `http://` (`127.0.0.1`, `localhost`, `[::1]`).
pub fn validate_admin_endpoint(raw: &str) -> Result<(), PostgresError> {
    let parsed = url::Url::parse(raw.trim()).map_err(|e| {
        PostgresError::Config(format!("invalid cloud admin_endpoint URL `{raw}`: {e}"))
    })?;
    match parsed.scheme() {
        "https" => Ok(()),
        "http" => {
            let is_loopback = parsed.host_str().is_some_and(|h| {
                matches!(h, "127.0.0.1" | "localhost" | "::1" | "[::1]")
            });
            if is_loopback {
                Ok(())
            } else {
                Err(PostgresError::Config(format!(
                    "non-loopback cloud admin_endpoint `{raw}` must use https://"
                )))
            }
        }
        other => Err(PostgresError::Config(format!(
            "unsupported cloud admin_endpoint scheme `{other}`"
        ))),
    }
}

/// Validate and percent-encode a single Cloud Admin URI path segment.
pub fn encode_uri_segment(segment: &str) -> Result<String, PostgresError> {
    let trimmed = segment.trim();
    if trimmed.is_empty()
        || trimmed == "."
        || trimmed == ".."
        || trimmed.contains('/')
        || trimmed.contains('?')
        || trimmed.contains('#')
        || trimmed.contains('\0')
        || trimmed.chars().any(char::is_whitespace)
    {
        return Err(PostgresError::Config(format!(
            "invalid cloud resource identifier segment `{segment}`"
        )));
    }
    let mut out = String::with_capacity(trimmed.len() + 4);
    for b in trimmed.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    Ok(out)
}

fn compute_credential_fingerprint(
    cloud_opts: &HashMap<String, String>,
    iam_opts: Option<&HashMap<String, String>>,
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    let mut cloud_pairs: Vec<(&str, &str)> = cloud_opts
        .iter()
        .map(|(k, v)| (k.as_str(), v.as_str()))
        .collect();
    cloud_pairs.sort_unstable_by_key(|(k, _)| *k);
    for (k, v) in cloud_pairs {
        "cloud".hash(&mut hasher);
        k.hash(&mut hasher);
        v.hash(&mut hasher);
    }
    if let Some(iam) = iam_opts {
        let mut iam_pairs: Vec<(&str, &str)> =
            iam.iter().map(|(k, v)| (k.as_str(), v.as_str())).collect();
        iam_pairs.sort_unstable_by_key(|(k, _)| *k);
        for (k, v) in iam_pairs {
            "iam".hash(&mut hasher);
            k.hash(&mut hasher);
            v.hash(&mut hasher);
        }
    }
    hasher.finish()
}

fn parse_or_placeholder_socket_addr(host: &str, port: u16) -> SocketAddr {
    if let Ok(ip) = host.parse::<IpAddr>() {
        SocketAddr::new(ip, port)
    } else {
        SocketAddr::from(([127, 0, 0, 1], port))
    }
}

fn redact_secret(msg: &str, secret: &str) -> String {
    if secret.is_empty() {
        msg.to_string()
    } else {
        msg.replace(secret, "<redacted>")
    }
}
