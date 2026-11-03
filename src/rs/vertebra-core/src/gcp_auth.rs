//! Pure-Rust Google Cloud Application Default Credentials (ADC) token cache and RS256 JWT signer.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};

pub const DEFAULT_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_GCP_SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];
pub const GCLOUD_CLI_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_TOKEN_TTL_MS: u64 = 3_600_000;
const DEFAULT_OAUTH_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
const DEFAULT_IAM_ENDPOINT: &str = "https://iamcredentials.googleapis.com";

/// How token and IAM HTTP requests reach the network; metadata requests never use a proxy.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Default)]
pub enum ProxyPolicy {
    #[default]
    Environment,
    Direct,
    Explicit(reqwest::Proxy),
}

pub fn default_gcloud_binary() -> PathBuf {
    std::env::var("GCLOUD_BIN")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("gcloud"))
}

fn url_is_loopback(raw_url: &str) -> bool {
    url::Url::parse(raw_url)
        .ok()
        .and_then(|u| {
            u.host_str()
                .map(|h| matches!(h, "127.0.0.1" | "localhost" | "::1" | "[::1]"))
        })
        .unwrap_or(false)
}

fn source_uses_loopback_endpoint(source: &GcpAuthSource) -> bool {
    match source {
        GcpAuthSource::AuthorizedUser { token_uri, .. } => url_is_loopback(token_uri),
        GcpAuthSource::ServiceAccount { key, .. } => url_is_loopback(&key.token_uri),
        GcpAuthSource::ImpersonatedServiceAccount {
            base, iam_endpoint, ..
        } => url_is_loopback(iam_endpoint) || source_uses_loopback_endpoint(base),
        GcpAuthSource::MetadataServer { endpoint, .. } => url_is_loopback(endpoint),
        GcpAuthSource::Static { .. } | GcpAuthSource::GcloudCli { .. } => false,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpTokenSnapshot {
    pub access_token: String,
    pub expires_at_unix_ms: u64,
    pub generation: u64,
    pub quota_project_id: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServiceAccountKey {
    pub client_email: String,
    pub private_key_pem: String,
    pub token_uri: String,
    pub project_id: Option<String>,
    pub quota_project_id: Option<String>,
}

#[derive(Clone, Debug)]
pub enum GcpAuthSource {
    Static {
        access_token: String,
        expires_at_unix_ms: Option<u64>,
        quota_project_id: Option<String>,
    },
    AuthorizedUser {
        client_id: String,
        client_secret: String,
        refresh_token: String,
        quota_project_id: Option<String>,
        token_uri: String,
    },
    ServiceAccount {
        key: ServiceAccountKey,
        scopes: Vec<String>,
    },
    MetadataServer {
        service_account: String,
        quota_project_id: Option<String>,
        endpoint: String,
        scopes: Vec<String>,
    },
    ImpersonatedServiceAccount {
        base: Box<GcpAuthSource>,
        target_service_account: String,
        iam_endpoint: String,
        scopes: Vec<String>,
        quota_project_id: Option<String>,
    },
    GcloudCli {
        binary: PathBuf,
        timeout: Duration,
        quota_project_id: Option<String>,
        impersonate_service_account: Option<String>,
    },
}

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Serialize)]
struct IamGenerateTokenRequest<'a> {
    scope: &'a [String],
}

#[derive(Deserialize)]
struct IamGenerateTokenResponse {
    #[serde(rename = "accessToken")]
    access_token: String,
    #[serde(rename = "expireTime", default)]
    expire_time: Option<String>,
}

#[derive(Deserialize)]
struct AdcCredentialFile {
    #[serde(rename = "type", default)]
    cred_type: String,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    client_secret: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    client_email: Option<String>,
    #[serde(default)]
    private_key: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    quota_project_id: Option<String>,
    #[serde(default)]
    token_uri: Option<String>,
}

#[derive(Serialize)]
struct ServiceAccountJwtClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<String>,
    aud: &'a str,
    iat: u64,
    exp: u64,
}

type TokenFetchResult = Result<(String, u64, Option<String>), String>;
type TokenFetchFuture<'a> =
    std::pin::Pin<Box<dyn std::future::Future<Output = TokenFetchResult> + Send + 'a>>;

pub fn current_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

pub fn discover_adc_file_path(explicit_path: Option<&str>, allow_ambient: bool) -> Option<PathBuf> {
    if let Some(explicit) = explicit_path {
        let trimmed = explicit.trim();
        if !trimmed.is_empty() {
            let p = Path::new(trimmed);
            if p.is_file() {
                return Some(p.to_path_buf());
            }
        }
    }
    if !allow_ambient {
        return None;
    }
    if let Ok(env_p) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS") {
        let trimmed = env_p.trim();
        if !trimmed.is_empty() && Path::new(trimmed).is_file() {
            return Some(PathBuf::from(trimmed));
        }
    }
    if let Ok(cfg_dir) = std::env::var("CLOUDSDK_CONFIG") {
        let trimmed = cfg_dir.trim();
        if !trimmed.is_empty() {
            let p = Path::new(trimmed).join("application_default_credentials.json");
            if p.is_file() {
                return Some(p);
            }
        }
    }
    if let Ok(home) = std::env::var("HOME") {
        let trimmed = home.trim();
        if !trimmed.is_empty() {
            let p = Path::new(trimmed)
                .join(".config")
                .join("gcloud")
                .join("application_default_credentials.json");
            if p.is_file() {
                return Some(p);
            }
        }
    }
    if let Ok(appdata) = std::env::var("APPDATA") {
        let trimmed = appdata.trim();
        if !trimmed.is_empty() {
            let p = Path::new(trimmed)
                .join("gcloud")
                .join("application_default_credentials.json");
            if p.is_file() {
                return Some(p);
            }
        }
    }
    None
}

pub fn parse_service_account_key_json(
    raw_json: &str,
    quota_project_override: Option<&str>,
    token_uri_override: Option<&str>,
) -> Option<ServiceAccountKey> {
    let parsed: AdcCredentialFile = serde_json::from_str(raw_json).ok()?;
    if parsed.cred_type != "service_account" {
        return None;
    }
    let client_email = parsed
        .client_email
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;
    let private_key_pem = parsed
        .private_key
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;
    let token_uri = token_uri_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| parsed.token_uri.filter(|s| !s.trim().is_empty()))
        .unwrap_or_else(|| DEFAULT_OAUTH_TOKEN_URI.to_string());
    let project_id = parsed.project_id.filter(|s| !s.trim().is_empty());
    let quota_project_id = quota_project_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| parsed.quota_project_id.filter(|s| !s.trim().is_empty()));

    Some(ServiceAccountKey {
        client_email,
        private_key_pem,
        token_uri,
        project_id,
        quota_project_id,
    })
}

fn normalize_gcp_options(raw_opts: &HashMap<String, String>) -> HashMap<String, String> {
    let mut opts = HashMap::with_capacity(raw_opts.len());
    for (k, v) in raw_opts {
        let normalized = k
            .strip_prefix("google_")
            .or_else(|| k.strip_prefix("gcs_"))
            .unwrap_or(k.as_str());
        opts.insert(normalized.to_string(), v.clone());
    }
    opts
}

fn parse_scopes_option(opts: &HashMap<String, String>) -> Option<Vec<String>> {
    let raw = opts.get("scopes")?;
    let parsed: Vec<String> = raw
        .split(|c: char| c == ',' || c.is_whitespace())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if parsed.is_empty() {
        None
    } else {
        Some(parsed)
    }
}

pub fn discover_ambient_quota_project(allow_ambient: bool) -> Option<String> {
    if !allow_ambient {
        return None;
    }
    for env_key in [
        "GOOGLE_CLOUD_QUOTA_PROJECT",
        "CLOUDSDK_BILLING_QUOTA_PROJECT",
    ] {
        if let Ok(val) = std::env::var(env_key) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    let config_dir = std::env::var("CLOUDSDK_CONFIG")
        .ok()
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var("HOME")
                .ok()
                .map(|h| Path::new(h.trim()).join(".config").join("gcloud"))
        })?;
    let default_cfg = config_dir.join("configurations").join("config_default");
    let raw = std::fs::read_to_string(default_cfg).ok()?;
    for line in raw.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("quota_project") {
            let val = rest.trim_start().strip_prefix('=')?.trim();
            if !val.is_empty() {
                return Some(val.to_string());
            }
        }
    }
    None
}

pub fn discover_ambient_impersonated_service_account(allow_ambient: bool) -> Option<String> {
    if !allow_ambient {
        return None;
    }
    for env_key in [
        "GOOGLE_IMPERSONATE_SERVICE_ACCOUNT",
        "CLOUDSDK_AUTH_IMPERSONATE_SERVICE_ACCOUNT",
    ] {
        if let Ok(val) = std::env::var(env_key) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

async fn fetch_token_from_gcloud_cli(
    binary: &Path,
    timeout: Duration,
    impersonate_sa: Option<&str>,
) -> Result<String, String> {
    let subcommands: &[&[&str]] = &[
        &["auth", "application-default", "print-access-token"],
        &["auth", "print-access-token"],
    ];
    let mut last_err = String::new();
    for args in subcommands {
        let mut cmd = tokio::process::Command::new(binary);
        cmd.args(*args);
        cmd.arg("--quiet");
        if let Some(sa) = impersonate_sa.filter(|s| !s.trim().is_empty()) {
            cmd.arg(format!("--impersonate-service-account={sa}"));
        }
        cmd.kill_on_drop(true);
        match tokio::time::timeout(timeout, cmd.output()).await {
            Ok(Ok(out)) if out.status.success() => {
                let tok = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !tok.is_empty() {
                    return Ok(tok);
                }
            }
            Ok(Ok(out)) => {
                last_err = String::from_utf8_lossy(&out.stderr).trim().to_string();
            }
            Ok(Err(e)) => {
                return Err(format!("failed to execute `{}` CLI: {e}", binary.display()));
            }
            Err(_) => {
                return Err(format!(
                    "`{}` print-access-token timed out after {} ms",
                    binary.display(),
                    timeout.as_millis()
                ));
            }
        }
    }
    Err(format!(
        "`{}` print-access-token failed: {last_err}",
        binary.display()
    ))
}

pub fn extract_adc_service_account_key(
    raw_opts: &HashMap<String, String>,
    allow_ambient: bool,
) -> Option<ServiceAccountKey> {
    let opts = normalize_gcp_options(raw_opts);
    if opts
        .get("target_service_account")
        .is_some_and(|v| !v.trim().is_empty())
    {
        return None;
    }
    let quota_override = opts.get("quota_project_id").map(String::as_str);
    let token_uri_override = opts.get("token_uri").map(String::as_str);

    if let Some(inline_json) = opts.get("service_account_key")
        && let Some(key) =
            parse_service_account_key_json(inline_json, quota_override, token_uri_override)
    {
        return Some(key);
    }

    if let Some(sa_path_or_json) = opts
        .get("service_account_path")
        .or_else(|| opts.get("service_account"))
    {
        let trimmed = sa_path_or_json.trim();
        if trimmed.starts_with('{') {
            if let Some(key) =
                parse_service_account_key_json(trimmed, quota_override, token_uri_override)
            {
                return Some(key);
            }
        } else if let Ok(raw) = std::fs::read_to_string(trimmed)
            && let Some(key) =
                parse_service_account_key_json(&raw, quota_override, token_uri_override)
        {
            return Some(key);
        }
    }

    let effective_ambient = allow_ambient
        && opts
            .get("allow_ambient_adc")
            .is_none_or(|v| v != "false" && v != "0");
    let explicit_adc = opts
        .get("application_credentials_path")
        .or_else(|| opts.get("application_credentials"))
        .map(String::as_str);
    let adc_path = discover_adc_file_path(explicit_adc, effective_ambient)?;
    let raw = std::fs::read_to_string(adc_path).ok()?;
    parse_service_account_key_json(&raw, quota_override, token_uri_override)
}

pub struct GcpTokenCache {
    source: GcpAuthSource,
    refresh_skew_ms: u64,
    snapshot: RwLock<Option<Arc<GcpTokenSnapshot>>>,
    refresh_lock: tokio::sync::Mutex<()>,
    generation: AtomicU64,
    client: reqwest::Client,
    metadata_client: reqwest::Client,
}

impl std::fmt::Debug for GcpTokenCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcpTokenCache")
            .field("refresh_skew_ms", &self.refresh_skew_ms)
            .field("generation", &self.generation.load(Ordering::Acquire))
            .finish()
    }
}

impl GcpTokenCache {
    pub fn new(source: GcpAuthSource, refresh_skew_ms: u64) -> Self {
        Self::with_proxy_policy(source, refresh_skew_ms, ProxyPolicy::Environment)
    }

    pub fn with_proxy_policy(
        source: GcpAuthSource,
        refresh_skew_ms: u64,
        policy: ProxyPolicy,
    ) -> Self {
        let builder = reqwest::Client::builder().timeout(Duration::from_secs(5));
        let builder = match policy {
            ProxyPolicy::Environment if source_uses_loopback_endpoint(&source) => {
                builder.no_proxy()
            }
            ProxyPolicy::Environment => builder,
            ProxyPolicy::Direct => builder.no_proxy(),
            ProxyPolicy::Explicit(proxy) => builder.no_proxy().proxy(proxy),
        };
        let client = builder.build().unwrap_or_else(|_| reqwest::Client::new());
        let metadata_client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            source,
            refresh_skew_ms,
            snapshot: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            generation: AtomicU64::new(0),
            client,
            metadata_client,
        }
    }

    pub fn from_options(
        raw_opts: &HashMap<String, String>,
        default_scopes: &[&str],
    ) -> Option<Arc<Self>> {
        let opts = normalize_gcp_options(raw_opts);
        let skew_ms = opts
            .get("refresh_skew_ms")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_REFRESH_SKEW_MS);
        let proxy_policy = match opts
            .get("proxy_policy")
            .map(|v| v.trim().to_ascii_lowercase())
            .as_deref()
        {
            Some("direct") => ProxyPolicy::Direct,
            _ => ProxyPolicy::Environment,
        };
        let allow_ambient = opts
            .get("allow_ambient_adc")
            .is_none_or(|v| v != "false" && v != "0");
        let quota_project_id = opts
            .get("quota_project_id")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| discover_ambient_quota_project(allow_ambient));
        let target_service_account = opts
            .get("target_service_account")
            .or_else(|| opts.get("impersonate_service_account"))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| discover_ambient_impersonated_service_account(allow_ambient));
        let custom_scopes = parse_scopes_option(&opts);
        let fallback_scopes = || -> Vec<String> {
            let base = if default_scopes.is_empty() {
                DEFAULT_GCP_SCOPES
            } else {
                default_scopes
            };
            base.iter().map(|s| (*s).to_string()).collect()
        };
        let default_sa_scopes =
            || -> Vec<String> { custom_scopes.clone().unwrap_or_else(&fallback_scopes) };

        let explicit_adc = opts
            .get("application_credentials_path")
            .or_else(|| opts.get("application_credentials"))
            .map(String::as_str);
        let explicit_adc_candidate = discover_adc_file_path(explicit_adc, false);
        let ambient_adc_candidate = if explicit_adc.is_none() {
            discover_adc_file_path(None, allow_ambient)
        } else {
            None
        };
        let token_uri_override = opts.get("token_uri").map(String::as_str);

        let mut base_source: Option<GcpAuthSource> = None;

        if let Some(inline_json) = opts.get("service_account_key")
            && let Some(sa_key) = parse_service_account_key_json(
                inline_json,
                quota_project_id.as_deref(),
                token_uri_override,
            )
        {
            base_source = Some(GcpAuthSource::ServiceAccount {
                key: sa_key,
                scopes: default_sa_scopes(),
            });
        } else if let Some(sa_path_or_json) = opts
            .get("service_account_path")
            .or_else(|| opts.get("service_account"))
        {
            let trimmed = sa_path_or_json.trim();
            let parsed_key = if trimmed.starts_with('{') {
                parse_service_account_key_json(
                    trimmed,
                    quota_project_id.as_deref(),
                    token_uri_override,
                )
            } else {
                std::fs::read_to_string(trimmed).ok().and_then(|raw| {
                    parse_service_account_key_json(
                        &raw,
                        quota_project_id.as_deref(),
                        token_uri_override,
                    )
                })
            };
            if let Some(sa_key) = parsed_key {
                base_source = Some(GcpAuthSource::ServiceAccount {
                    key: sa_key,
                    scopes: default_sa_scopes(),
                });
            }
        }

        let parse_adc_source = |adc_path: &Path| -> Option<GcpAuthSource> {
            let raw = std::fs::read_to_string(adc_path).ok()?;
            if let Some(sa_key) = parse_service_account_key_json(
                &raw,
                quota_project_id.as_deref(),
                token_uri_override,
            ) {
                return Some(GcpAuthSource::ServiceAccount {
                    key: sa_key,
                    scopes: default_sa_scopes(),
                });
            }
            if let Ok(parsed) = serde_json::from_str::<AdcCredentialFile>(&raw)
                && parsed.cred_type == "authorized_user"
                && let (Some(cid), Some(csec), Some(rtok)) =
                    (parsed.client_id, parsed.client_secret, parsed.refresh_token)
            {
                return Some(GcpAuthSource::AuthorizedUser {
                    client_id: cid,
                    client_secret: csec,
                    refresh_token: rtok,
                    quota_project_id: quota_project_id.clone().or(parsed.quota_project_id),
                    token_uri: token_uri_override
                        .map(str::to_string)
                        .or(parsed.token_uri)
                        .unwrap_or_else(|| DEFAULT_OAUTH_TOKEN_URI.to_string()),
                });
            }
            None
        };

        if base_source.is_none() {
            if let (Some(cid), Some(csec), Some(rtok)) = (
                opts.get("client_id"),
                opts.get("client_secret"),
                opts.get("refresh_token"),
            ) {
                let token_uri = opts
                    .get("token_uri")
                    .cloned()
                    .unwrap_or_else(|| DEFAULT_OAUTH_TOKEN_URI.to_string());
                base_source = Some(GcpAuthSource::AuthorizedUser {
                    client_id: cid.clone(),
                    client_secret: csec.clone(),
                    refresh_token: rtok.clone(),
                    quota_project_id: quota_project_id.clone(),
                    token_uri,
                });
            } else if let Some(ref adc_path) = explicit_adc_candidate {
                base_source = parse_adc_source(adc_path);
            } else if let Some(tok) = opts
                .get("access_token")
                .or_else(|| opts.get("bearer_token"))
                .filter(|s| !s.trim().is_empty())
            {
                base_source = Some(GcpAuthSource::Static {
                    access_token: tok.clone(),
                    expires_at_unix_ms: None,
                    quota_project_id: quota_project_id.clone(),
                });
            } else if opts
                .get("use_metadata_server")
                .is_some_and(|v| v == "true" || v == "1")
            {
                let host = opts
                    .get("metadata_host")
                    .cloned()
                    .or_else(|| std::env::var("GCE_METADATA_HOST").ok())
                    .unwrap_or_else(|| "metadata.google.internal".to_string());
                let endpoint = if host.starts_with("http://") || host.starts_with("https://") {
                    host
                } else {
                    format!("http://{host}")
                };
                base_source = Some(GcpAuthSource::MetadataServer {
                    service_account: opts
                        .get("metadata_service_account")
                        .cloned()
                        .unwrap_or_else(|| "default".to_string()),
                    quota_project_id: quota_project_id.clone(),
                    endpoint,
                    scopes: custom_scopes.clone().unwrap_or_default(),
                });
            } else if opts
                .get("use_gcloud_cli")
                .is_some_and(|v| v == "true" || v == "1")
            {
                base_source = Some(GcpAuthSource::GcloudCli {
                    binary: default_gcloud_binary(),
                    timeout: GCLOUD_CLI_TIMEOUT,
                    quota_project_id: quota_project_id.clone(),
                    impersonate_service_account: None,
                });
            } else if let Some(ref adc_path) = ambient_adc_candidate {
                base_source = parse_adc_source(adc_path);
            }
        }

        if base_source.is_none() && target_service_account.is_none() && allow_ambient {
            for env_var in ["GOOGLE_OAUTH_ACCESS_TOKEN", "CLOUDSDK_AUTH_ACCESS_TOKEN"] {
                if let Ok(tok) = std::env::var(env_var) {
                    let trimmed = tok.trim();
                    if !trimmed.is_empty() {
                        base_source = Some(GcpAuthSource::Static {
                            access_token: trimmed.to_string(),
                            expires_at_unix_ms: None,
                            quota_project_id: quota_project_id.clone(),
                        });
                        break;
                    }
                }
            }
        }

        let source = if let Some(target_sa) = target_service_account {
            let resolved_base = base_source.unwrap_or_else(|| {
                let host = opts
                    .get("metadata_host")
                    .cloned()
                    .or_else(|| std::env::var("GCE_METADATA_HOST").ok())
                    .unwrap_or_else(|| "metadata.google.internal".to_string());
                let endpoint = if host.starts_with("http://") || host.starts_with("https://") {
                    host
                } else {
                    format!("http://{host}")
                };
                GcpAuthSource::MetadataServer {
                    service_account: "default".to_string(),
                    quota_project_id: quota_project_id.clone(),
                    endpoint,
                    scopes: custom_scopes.clone().unwrap_or_default(),
                }
            });
            let iam_endpoint = opts
                .get("iam_endpoint")
                .cloned()
                .unwrap_or_else(|| DEFAULT_IAM_ENDPOINT.to_string());
            let impersonation_scopes = custom_scopes.unwrap_or_else(&fallback_scopes);
            GcpAuthSource::ImpersonatedServiceAccount {
                base: Box::new(resolved_base),
                target_service_account: target_sa,
                iam_endpoint,
                scopes: impersonation_scopes,
                quota_project_id,
            }
        } else {
            base_source?
        };

        Some(Arc::new(Self::with_proxy_policy(
            source,
            skew_ms,
            proxy_policy,
        )))
    }

    pub fn service_account_key(&self) -> Option<&ServiceAccountKey> {
        match &self.source {
            GcpAuthSource::ServiceAccount { key, .. } => Some(key),
            _ => None,
        }
    }

    pub fn sign_jwt_for_audience(&self, audience: &str) -> Result<String, String> {
        let key = self.service_account_key().ok_or_else(|| {
            "sign_jwt_for_audience requires ServiceAccount credentials".to_string()
        })?;
        let trimmed_aud = audience.trim();
        if trimmed_aud.is_empty() {
            return Err("audience must not be empty".to_string());
        }
        let now_secs = current_unix_ms() / 1_000;
        let claims = ServiceAccountJwtClaims {
            iss: &key.client_email,
            sub: &key.client_email,
            scope: None,
            aud: trimmed_aud,
            iat: now_secs,
            exp: now_secs + 3600,
        };
        let encoding_key = EncodingKey::from_rsa_pem(key.private_key_pem.as_bytes())
            .map_err(|e| format!("invalid service account RSA private key PEM: {e}"))?;
        let header = Header::new(Algorithm::RS256);
        encode(&header, &claims, &encoding_key)
            .map_err(|e| format!("failed to encode audience JWT: {e}"))
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn refresh_skew_ms(&self) -> u64 {
        self.refresh_skew_ms
    }

    pub fn get_cached(&self, now_unix_ms: u64) -> Option<Arc<GcpTokenSnapshot>> {
        let guard = self.snapshot.read().expect("token snapshot lock poisoned");
        let snap = guard.as_ref()?;
        if now_unix_ms.saturating_add(self.refresh_skew_ms) < snap.expires_at_unix_ms {
            Some(snap.clone())
        } else {
            None
        }
    }

    pub fn needs_refresh(&self, now_unix_ms: u64) -> bool {
        self.get_cached(now_unix_ms).is_none()
    }

    pub fn invalidate(&self, failed_generation: u64) {
        let mut guard = self.snapshot.write().expect("token snapshot lock poisoned");
        if let Some(current) = guard.as_ref()
            && current.generation == failed_generation
        {
            *guard = None;
        }
    }

    pub async fn get_or_refresh_with_force(
        &self,
        force_refresh: bool,
    ) -> Result<Arc<GcpTokenSnapshot>, String> {
        let now = current_unix_ms();
        if !force_refresh && let Some(snap) = self.get_cached(now) {
            return Ok(snap);
        }

        let _flight = self.refresh_lock.lock().await;
        let now_after_lock = current_unix_ms();
        if !force_refresh && let Some(snap) = self.get_cached(now_after_lock) {
            return Ok(snap);
        }

        let (token, expires_at_unix_ms, quota_project_id) =
            self.fetch_from_source(&self.source, now_after_lock).await?;
        let next_gen = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let new_snapshot = Arc::new(GcpTokenSnapshot {
            access_token: token,
            expires_at_unix_ms,
            generation: next_gen,
            quota_project_id,
        });
        {
            let mut guard = self.snapshot.write().expect("token snapshot lock poisoned");
            *guard = Some(new_snapshot.clone());
        }
        Ok(new_snapshot)
    }

    pub async fn get_or_refresh(&self) -> Result<Arc<GcpTokenSnapshot>, String> {
        self.get_or_refresh_with_force(false).await
    }

    fn fetch_from_source<'a>(
        &'a self,
        source: &'a GcpAuthSource,
        now_unix_ms: u64,
    ) -> TokenFetchFuture<'a> {
        Box::pin(async move {
            match source {
                GcpAuthSource::Static {
                    access_token,
                    expires_at_unix_ms,
                    quota_project_id,
                } => Ok((
                    access_token.clone(),
                    expires_at_unix_ms.unwrap_or(u64::MAX / 2),
                    quota_project_id.clone(),
                )),
                GcpAuthSource::AuthorizedUser {
                    client_id,
                    client_secret,
                    refresh_token,
                    quota_project_id,
                    token_uri,
                } => {
                    let form_body = url::form_urlencoded::Serializer::new(String::new())
                        .append_pair("client_id", client_id.as_str())
                        .append_pair("client_secret", client_secret.as_str())
                        .append_pair("refresh_token", refresh_token.as_str())
                        .append_pair("grant_type", "refresh_token")
                        .finish();
                    let mut req = self
                        .client
                        .post(token_uri)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .body(form_body);
                    if let Some(qp) = quota_project_id {
                        req = req.header("x-goog-user-project", qp);
                    }
                    let resp = req
                        .send()
                        .await
                        .map_err(|e| format!("OAuth2 refresh request failed: {e}"))?;
                    if !resp.status().is_success() {
                        let status = resp.status();
                        if token_uri == DEFAULT_OAUTH_TOKEN_URI
                            && let Ok(cli_tok) = fetch_token_from_gcloud_cli(
                                &default_gcloud_binary(),
                                GCLOUD_CLI_TIMEOUT,
                                None,
                            )
                            .await
                        {
                            return Ok((
                                cli_tok,
                                now_unix_ms.saturating_add(DEFAULT_TOKEN_TTL_MS),
                                quota_project_id.clone(),
                            ));
                        }
                        return Err(format!("OAuth2 refresh returned HTTP {status}"));
                    }
                    let parsed: OAuthTokenResponse = resp
                        .json()
                        .await
                        .map_err(|e| format!("invalid OAuth2 token JSON: {e}"))?;
                    let ttl_ms = parsed
                        .expires_in
                        .map(|s| s.saturating_mul(1_000))
                        .unwrap_or(DEFAULT_TOKEN_TTL_MS);
                    Ok((
                        parsed.access_token,
                        now_unix_ms.saturating_add(ttl_ms),
                        quota_project_id.clone(),
                    ))
                }
                GcpAuthSource::ServiceAccount { key, scopes } => {
                    let now_secs = now_unix_ms / 1_000;
                    let scope_str = if scopes.is_empty() {
                        DEFAULT_GCP_SCOPES.join(" ")
                    } else {
                        scopes.join(" ")
                    };
                    let claims = ServiceAccountJwtClaims {
                        iss: &key.client_email,
                        sub: &key.client_email,
                        scope: Some(scope_str),
                        aud: &key.token_uri,
                        iat: now_secs,
                        exp: now_secs + 3600,
                    };
                    let encoding_key = EncodingKey::from_rsa_pem(key.private_key_pem.as_bytes())
                        .map_err(|e| format!("invalid service account RSA private key PEM: {e}"))?;
                    let header = Header::new(Algorithm::RS256);
                    let assertion = encode(&header, &claims, &encoding_key)
                        .map_err(|e| format!("failed to encode service account JWT: {e}"))?;
                    let form_body = url::form_urlencoded::Serializer::new(String::new())
                        .append_pair("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
                        .append_pair("assertion", &assertion)
                        .finish();
                    let mut req = self
                        .client
                        .post(&key.token_uri)
                        .header("Content-Type", "application/x-www-form-urlencoded")
                        .body(form_body);
                    if let Some(ref qp) = key.quota_project_id {
                        req = req.header("x-goog-user-project", qp);
                    }
                    let resp = req
                        .send()
                        .await
                        .map_err(|e| format!("Service account OAuth2 token request failed: {e}"))?;
                    if !resp.status().is_success() {
                        return Err(format!(
                            "Service account OAuth2 token request returned HTTP {}",
                            resp.status()
                        ));
                    }
                    let parsed: OAuthTokenResponse = resp
                        .json()
                        .await
                        .map_err(|e| format!("invalid OAuth2 token JSON: {e}"))?;
                    let ttl_ms = parsed
                        .expires_in
                        .map(|s| s.saturating_mul(1_000))
                        .unwrap_or(DEFAULT_TOKEN_TTL_MS);
                    Ok((
                        parsed.access_token,
                        now_unix_ms.saturating_add(ttl_ms),
                        key.quota_project_id.clone(),
                    ))
                }
                GcpAuthSource::MetadataServer {
                    service_account,
                    quota_project_id,
                    endpoint,
                    scopes,
                } => {
                    let url = if scopes.is_empty() {
                        format!(
                            "{}/computeMetadata/v1/instance/service-accounts/{}/token",
                            endpoint.trim_end_matches('/'),
                            service_account
                        )
                    } else {
                        format!(
                            "{}/computeMetadata/v1/instance/service-accounts/{}/token?scopes={}",
                            endpoint.trim_end_matches('/'),
                            service_account,
                            scopes.join(",")
                        )
                    };
                    let resp = self
                        .metadata_client
                        .get(&url)
                        .header("Metadata-Flavor", "Google")
                        .send()
                        .await
                        .map_err(|e| format!("metadata server request failed: {e}"))?;
                    if !resp.status().is_success() {
                        return Err(format!("metadata server returned HTTP {}", resp.status()));
                    }
                    let parsed: OAuthTokenResponse = resp
                        .json()
                        .await
                        .map_err(|e| format!("invalid metadata token JSON: {e}"))?;
                    let ttl_ms = parsed
                        .expires_in
                        .map(|s| s.saturating_mul(1_000))
                        .unwrap_or(DEFAULT_TOKEN_TTL_MS);
                    Ok((
                        parsed.access_token,
                        now_unix_ms.saturating_add(ttl_ms),
                        quota_project_id.clone(),
                    ))
                }
                GcpAuthSource::ImpersonatedServiceAccount {
                    base,
                    target_service_account,
                    iam_endpoint,
                    scopes,
                    quota_project_id,
                } => {
                    let (source_token, _, base_qp) =
                        self.fetch_from_source(base, now_unix_ms).await?;
                    let url = format!(
                        "{}/v1/projects/-/serviceAccounts/{}:generateAccessToken",
                        iam_endpoint.trim_end_matches('/'),
                        target_service_account
                    );
                    let resp = self
                        .client
                        .post(&url)
                        .bearer_auth(&source_token)
                        .json(&IamGenerateTokenRequest { scope: scopes })
                        .send()
                        .await
                        .map_err(|e| format!("IAM generateAccessToken failed: {e}"))?;
                    if !resp.status().is_success() {
                        return Err(format!(
                            "IAM generateAccessToken returned HTTP {}",
                            resp.status()
                        ));
                    }
                    let parsed: IamGenerateTokenResponse = resp
                        .json()
                        .await
                        .map_err(|e| format!("invalid IAM generateAccessToken JSON: {e}"))?;
                    let expires_at_unix_ms = parsed
                        .expire_time
                        .as_deref()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.timestamp_millis().max(0) as u64)
                        .unwrap_or_else(|| now_unix_ms.saturating_add(DEFAULT_TOKEN_TTL_MS));
                    Ok((
                        parsed.access_token,
                        expires_at_unix_ms,
                        quota_project_id.clone().or(base_qp),
                    ))
                }
                GcpAuthSource::GcloudCli {
                    binary,
                    timeout,
                    quota_project_id,
                    impersonate_service_account,
                } => {
                    let tok = fetch_token_from_gcloud_cli(
                        binary,
                        *timeout,
                        impersonate_service_account.as_deref(),
                    )
                    .await?;
                    Ok((
                        tok,
                        now_unix_ms.saturating_add(DEFAULT_TOKEN_TTL_MS),
                        quota_project_id.clone(),
                    ))
                }
            }
        })
    }
}
