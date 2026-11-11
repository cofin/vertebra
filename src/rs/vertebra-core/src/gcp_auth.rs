//! Pure-Rust Google Cloud Application Default Credentials (ADC) token cache and RS256 JWT signer.

use std::collections::HashMap;
use std::ffi::{CStr, c_void};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};

pub const DEFAULT_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_GCP_SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];
pub const GCLOUD_CLI_TIMEOUT: Duration = Duration::from_secs(30);
pub const GCP_TOKEN_CACHE_CAPSULE_NAME: &CStr = c"vertebra.GcpTokenCacheCapsule";
pub const GCP_TOKEN_CACHE_ABI_VERSION: u32 = 1;
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

    pub fn quota_project_id(&self) -> Option<&str> {
        Self::source_quota_project_id(&self.source)
    }

    fn source_quota_project_id(source: &GcpAuthSource) -> Option<&str> {
        match source {
            GcpAuthSource::Static {
                quota_project_id, ..
            }
            | GcpAuthSource::AuthorizedUser {
                quota_project_id, ..
            }
            | GcpAuthSource::MetadataServer {
                quota_project_id, ..
            }
            | GcpAuthSource::GcloudCli {
                quota_project_id, ..
            } => quota_project_id.as_deref(),
            GcpAuthSource::ServiceAccount { key, .. } => key.quota_project_id.as_deref(),
            GcpAuthSource::ImpersonatedServiceAccount {
                base,
                quota_project_id,
                ..
            } => quota_project_id
                .as_deref()
                .or_else(|| Self::source_quota_project_id(base)),
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

static GCP_HTTP2_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();
static GCP_HTTP2_DIRECT_CLIENT: std::sync::OnceLock<reqwest::Client> = std::sync::OnceLock::new();

fn shared_gcp_http_client() -> &'static reqwest::Client {
    GCP_HTTP2_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

fn shared_gcp_direct_http_client() -> &'static reqwest::Client {
    GCP_HTTP2_DIRECT_CLIENT.get_or_init(|| {
        reqwest::Client::builder()
            .no_proxy()
            .pool_idle_timeout(Duration::from_secs(90))
            .tcp_nodelay(true)
            .build()
            .unwrap_or_else(|_| reqwest::Client::new())
    })
}

async fn send_post_with_client(
    client: &reqwest::Client,
    url: &str,
    headers: &HashMap<String, String>,
    body: bytes::Bytes,
    timeout: Duration,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut req = client.post(url).timeout(timeout).body(body);
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    req.send().await
}

/// Execute a connection-pooled HTTP/2 or HTTP/1.1 unary POST request and return `(status, lowercase_headers, body)`.
pub async fn gcp_http2_post(
    url: &str,
    headers: &HashMap<String, String>,
    body: Vec<u8>,
    timeout_ms: u64,
) -> Result<(u16, HashMap<String, String>, Vec<u8>), String> {
    let timeout = Duration::from_millis(timeout_ms.max(1));
    let payload = bytes::Bytes::from(body);
    let client = if url_is_loopback(url) {
        shared_gcp_direct_http_client()
    } else {
        shared_gcp_http_client()
    };

    let resp = send_post_with_client(client, url, headers, payload, timeout)
        .await
        .map_err(|e| format!("GCP HTTP/2 POST to `{url}` failed: {e}"))?;

    let status = resp.status().as_u16();
    let mut out_headers = HashMap::with_capacity(resp.headers().len() + 2);
    for (name, val) in resp.headers() {
        if let Ok(val_str) = val.to_str() {
            out_headers.insert(name.as_str().to_ascii_lowercase(), val_str.to_string());
        }
    }
    let body_bytes = resp
        .bytes()
        .await
        .map_err(|e| format!("failed to read GCP HTTP/2 response body from `{url}`: {e}"))?;
    Ok((status, out_headers, body_bytes.to_vec()))
}

pub type GcpCachedTokenVisitFn = unsafe extern "C" fn(
    out_ctx: *mut c_void,
    token_ptr: *const u8,
    token_len: usize,
    expires_at_unix_ms: u64,
    generation: u64,
    qp_ptr: *const u8,
    qp_len: usize,
);

pub type GcpRefreshCompleteFn = unsafe extern "C" fn(
    out_ctx: *mut c_void,
    ok: bool,
    ptr: *const u8,
    len: usize,
    expires_at_unix_ms: u64,
    generation: u64,
    qp_ptr: *const u8,
    qp_len: usize,
);

pub type GcpRefreshAsyncFn = unsafe extern "C" fn(
    ctx: *const c_void,
    force_refresh: bool,
    out_ctx: *mut c_void,
    complete_cb: GcpRefreshCompleteFn,
);

pub type GcpServiceAccountVisitFn = unsafe extern "C" fn(
    out_ctx: *mut c_void,
    email_ptr: *const u8,
    email_len: usize,
    pem_ptr: *const u8,
    pem_len: usize,
    token_uri_ptr: *const u8,
    token_uri_len: usize,
    proj_ptr: *const u8,
    proj_len: usize,
    qp_ptr: *const u8,
    qp_len: usize,
);

pub type GcpQuotaProjectVisitFn =
    unsafe extern "C" fn(out_ctx: *mut c_void, qp_ptr: *const u8, qp_len: usize);

/// C-ABI vtable capsule sharing a refcounted [`GcpTokenCache`] across PyO3 `cdylib` extensions.
#[repr(C)]
#[derive(Debug)]
pub struct GcpTokenCacheCapsule {
    pub abi_version: u32,
    pub ctx: *const c_void,
    pub clone_ctx_fn: unsafe extern "C" fn(*const c_void) -> *const c_void,
    pub drop_ctx_fn: unsafe extern "C" fn(*const c_void),
    pub generation_fn: unsafe extern "C" fn(*const c_void) -> u64,
    pub invalidate_fn: unsafe extern "C" fn(*const c_void, u64),
    pub try_cached_fn:
        unsafe extern "C" fn(*const c_void, u64, *mut c_void, GcpCachedTokenVisitFn) -> bool,
    pub refresh_async_fn: GcpRefreshAsyncFn,
    pub service_account_key_fn:
        unsafe extern "C" fn(*const c_void, *mut c_void, GcpServiceAccountVisitFn) -> bool,
    pub quota_project_id_fn:
        unsafe extern "C" fn(*const c_void, *mut c_void, GcpQuotaProjectVisitFn) -> bool,
}

// SAFETY: `GcpTokenCacheCapsule` wraps an atomically refcounted `Arc<GcpTokenCache>` (`Send + Sync`)
// and pure C-ABI function pointers with no thread-local affinity.
unsafe impl Send for GcpTokenCacheCapsule {}
// SAFETY: Concurrent shared access only invokes thread-safe `GcpTokenCache` methods via `ctx`.
unsafe impl Sync for GcpTokenCacheCapsule {}

unsafe extern "C" fn capsule_clone_ctx(ctx: *const c_void) -> *const c_void {
    if ctx.is_null() {
        return std::ptr::null();
    }
    // SAFETY: `ctx` was produced via `Arc::into_raw` on a live `Arc<GcpTokenCache>`; incrementing
    // the strong count creates a new owned raw pointer reference to the same allocation.
    unsafe {
        Arc::increment_strong_count(ctx.cast::<GcpTokenCache>());
    }
    ctx
}

unsafe extern "C" fn capsule_drop_ctx(ctx: *const c_void) {
    if !ctx.is_null() {
        // SAFETY: `ctx` is an owned raw reference created by `Arc::into_raw` or `capsule_clone_ctx`,
        // surrendered once by `Drop`.
        unsafe {
            drop(Arc::from_raw(ctx.cast::<GcpTokenCache>()));
        }
    }
}

unsafe extern "C" fn capsule_generation(ctx: *const c_void) -> u64 {
    if ctx.is_null() {
        return 0;
    }
    // SAFETY: `ctx` points to a live `GcpTokenCache` kept alive by the caller's owned reference.
    let cache = unsafe { &*(ctx.cast::<GcpTokenCache>()) };
    cache.generation()
}

unsafe extern "C" fn capsule_invalidate(ctx: *const c_void, failed_generation: u64) {
    if ctx.is_null() {
        return;
    }
    // SAFETY: `ctx` points to a live `GcpTokenCache` kept alive by the caller's owned reference.
    let cache = unsafe { &*(ctx.cast::<GcpTokenCache>()) };
    cache.invalidate(failed_generation);
}

unsafe extern "C" fn capsule_try_cached(
    ctx: *const c_void,
    now_unix_ms: u64,
    out_ctx: *mut c_void,
    visit_cb: GcpCachedTokenVisitFn,
) -> bool {
    if ctx.is_null() {
        return false;
    }
    // SAFETY: `ctx` points to a live `GcpTokenCache` kept alive by the caller's owned reference.
    let cache = unsafe { &*(ctx.cast::<GcpTokenCache>()) };
    let Some(snap) = cache.get_cached(now_unix_ms) else {
        return false;
    };
    let (qp_ptr, qp_len) = match snap.quota_project_id.as_deref() {
        Some(qp) => (qp.as_ptr(), qp.len()),
        None => (std::ptr::null(), 0),
    };
    // SAFETY: All byte pointers borrow `snap` synchronously for the duration of `visit_cb`.
    unsafe {
        visit_cb(
            out_ctx,
            snap.access_token.as_ptr(),
            snap.access_token.len(),
            snap.expires_at_unix_ms,
            snap.generation,
            qp_ptr,
            qp_len,
        );
    }
    true
}

unsafe extern "C" fn capsule_service_account_key(
    ctx: *const c_void,
    out_ctx: *mut c_void,
    visit_cb: GcpServiceAccountVisitFn,
) -> bool {
    if ctx.is_null() {
        return false;
    }
    // SAFETY: `ctx` points to a live `GcpTokenCache` kept alive by the caller's owned reference.
    let cache = unsafe { &*(ctx.cast::<GcpTokenCache>()) };
    let Some(key) = cache.service_account_key() else {
        return false;
    };
    let (proj_ptr, proj_len) = match key.project_id.as_deref() {
        Some(p) => (p.as_ptr(), p.len()),
        None => (std::ptr::null(), 0),
    };
    let (qp_ptr, qp_len) = match key.quota_project_id.as_deref() {
        Some(qp) => (qp.as_ptr(), qp.len()),
        None => (std::ptr::null(), 0),
    };
    // SAFETY: All byte slices borrow `key` synchronously for the duration of `visit_cb`.
    unsafe {
        visit_cb(
            out_ctx,
            key.client_email.as_ptr(),
            key.client_email.len(),
            key.private_key_pem.as_ptr(),
            key.private_key_pem.len(),
            key.token_uri.as_ptr(),
            key.token_uri.len(),
            proj_ptr,
            proj_len,
            qp_ptr,
            qp_len,
        );
    }
    true
}

unsafe extern "C" fn capsule_quota_project_id(
    ctx: *const c_void,
    out_ctx: *mut c_void,
    visit_cb: GcpQuotaProjectVisitFn,
) -> bool {
    if ctx.is_null() {
        return false;
    }
    // SAFETY: `ctx` points to a live `GcpTokenCache` kept alive by the caller's owned reference.
    let cache = unsafe { &*(ctx.cast::<GcpTokenCache>()) };
    let Some(qp) = cache.quota_project_id() else {
        return false;
    };
    // SAFETY: `qp` borrows `cache` synchronously for the duration of `visit_cb`.
    unsafe {
        visit_cb(out_ctx, qp.as_ptr(), qp.len());
    }
    true
}

/// Helper invoked by a producing extension's `refresh_async_fn` implementation to clone the
/// underlying [`Arc<GcpTokenCache>`] and forward completion across the C-ABI callback boundary.
///
/// # Safety
/// `ctx` must be a live `GcpTokenCache` raw `Arc` pointer created by [`GcpTokenCacheCapsule::new`],
/// and `complete_cb(out_ctx, ...)` must be safe to call exactly once from any thread.
pub unsafe fn invoke_capsule_refresh_on_runtime(
    runtime: &tokio::runtime::Handle,
    ctx: *const c_void,
    force_refresh: bool,
    out_ctx: *mut c_void,
    complete_cb: GcpRefreshCompleteFn,
) {
    struct SendPtr(*mut c_void);
    // SAFETY: `out_ctx` points to a heap-allocated `tokio::sync::oneshot::Sender` owned exclusively
    // by this single callback invocation (`Send`).
    unsafe impl Send for SendPtr {}

    if ctx.is_null() {
        let msg = b"null GcpTokenCacheCapsule context";
        // SAFETY: `complete_cb` is invoked once with valid UTF-8 error bytes.
        unsafe {
            complete_cb(
                out_ctx,
                false,
                msg.as_ptr(),
                msg.len(),
                0,
                0,
                std::ptr::null(),
                0,
            );
        }
        return;
    }

    // SAFETY: `ctx` points to a live `Arc<GcpTokenCache>`; incrementing the strong count before
    // `Arc::from_raw` produces an independent `Arc` for the spawned async task.
    let cache = unsafe {
        Arc::increment_strong_count(ctx.cast::<GcpTokenCache>());
        Arc::from_raw(ctx.cast::<GcpTokenCache>())
    };
    let send_out = SendPtr(out_ctx);
    runtime.spawn(async move {
        let out_ptr = send_out;
        match cache.get_or_refresh_with_force(force_refresh).await {
            Ok(snap) => {
                let (qp_ptr, qp_len) = match snap.quota_project_id.as_deref() {
                    Some(qp) => (qp.as_ptr(), qp.len()),
                    None => (std::ptr::null(), 0),
                };
                // SAFETY: `snap` stays alive across `complete_cb`, and `out_ptr.0` is consumed once.
                unsafe {
                    complete_cb(
                        out_ptr.0,
                        true,
                        snap.access_token.as_ptr(),
                        snap.access_token.len(),
                        snap.expires_at_unix_ms,
                        snap.generation,
                        qp_ptr,
                        qp_len,
                    );
                }
            }
            Err(err) => {
                // SAFETY: `err` stays alive across `complete_cb`, and `out_ptr.0` is consumed once.
                unsafe {
                    complete_cb(
                        out_ptr.0,
                        false,
                        err.as_ptr(),
                        err.len(),
                        0,
                        0,
                        std::ptr::null(),
                        0,
                    );
                }
            }
        }
    });
}

impl GcpTokenCacheCapsule {
    pub fn new(cache: Arc<GcpTokenCache>, refresh_async_fn: GcpRefreshAsyncFn) -> Self {
        Self {
            abi_version: GCP_TOKEN_CACHE_ABI_VERSION,
            ctx: Arc::into_raw(cache).cast::<c_void>(),
            clone_ctx_fn: capsule_clone_ctx,
            drop_ctx_fn: capsule_drop_ctx,
            generation_fn: capsule_generation,
            invalidate_fn: capsule_invalidate,
            try_cached_fn: capsule_try_cached,
            refresh_async_fn,
            service_account_key_fn: capsule_service_account_key,
            quota_project_id_fn: capsule_quota_project_id,
        }
    }
}

impl Drop for GcpTokenCacheCapsule {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            let ptr = self.ctx;
            self.ctx = std::ptr::null();
            // SAFETY: `self` owns one refcounted reference in `ptr`, released once on drop.
            unsafe {
                (self.drop_ctx_fn)(ptr);
            }
        }
    }
}

struct ForeignCapsuleHandle {
    ctx: *const c_void,
    clone_ctx_fn: unsafe extern "C" fn(*const c_void) -> *const c_void,
    drop_ctx_fn: unsafe extern "C" fn(*const c_void),
    generation_fn: unsafe extern "C" fn(*const c_void) -> u64,
    invalidate_fn: unsafe extern "C" fn(*const c_void, u64),
    try_cached_fn:
        unsafe extern "C" fn(*const c_void, u64, *mut c_void, GcpCachedTokenVisitFn) -> bool,
    refresh_async_fn: GcpRefreshAsyncFn,
    service_account_key_fn:
        unsafe extern "C" fn(*const c_void, *mut c_void, GcpServiceAccountVisitFn) -> bool,
    quota_project_id_fn:
        unsafe extern "C" fn(*const c_void, *mut c_void, GcpQuotaProjectVisitFn) -> bool,
}

// SAFETY: `ForeignCapsuleHandle` holds an atomically refcounted foreign `GcpTokenCache` pointer
// (`Send + Sync`) and pure C-ABI vtable functions.
unsafe impl Send for ForeignCapsuleHandle {}
// SAFETY: Every vtable method synchronizes access internally via atomics, `RwLock`, or Tokio `Mutex`.
unsafe impl Sync for ForeignCapsuleHandle {}

impl Clone for ForeignCapsuleHandle {
    fn clone(&self) -> Self {
        // SAFETY: `self.ctx` is a live owned reference; `clone_ctx_fn` increments the underlying
        // `Arc` strong count and returns an independent owned pointer.
        let cloned_ctx = unsafe { (self.clone_ctx_fn)(self.ctx) };
        Self {
            ctx: cloned_ctx,
            clone_ctx_fn: self.clone_ctx_fn,
            drop_ctx_fn: self.drop_ctx_fn,
            generation_fn: self.generation_fn,
            invalidate_fn: self.invalidate_fn,
            try_cached_fn: self.try_cached_fn,
            refresh_async_fn: self.refresh_async_fn,
            service_account_key_fn: self.service_account_key_fn,
            quota_project_id_fn: self.quota_project_id_fn,
        }
    }
}

impl Drop for ForeignCapsuleHandle {
    fn drop(&mut self) {
        if !self.ctx.is_null() {
            let ptr = self.ctx;
            self.ctx = std::ptr::null();
            // SAFETY: `self` owns one refcounted reference in `ptr`, released once on drop.
            unsafe {
                (self.drop_ctx_fn)(ptr);
            }
        }
    }
}

#[derive(Clone)]
enum CacheHandleRepr {
    Local(Arc<GcpTokenCache>),
    Foreign(ForeignCapsuleHandle),
}

/// Safe, clonable handle to either a crate-local [`GcpTokenCache`] or a foreign C-ABI [`GcpTokenCacheCapsule`].
#[derive(Clone)]
pub struct SharedGcpTokenCacheHandle {
    repr: CacheHandleRepr,
}

impl std::fmt::Debug for SharedGcpTokenCacheHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedGcpTokenCacheHandle")
            .field("generation", &self.generation())
            .finish()
    }
}

unsafe fn utf8_string_from_raw(ptr: *const u8, len: usize) -> String {
    if ptr.is_null() || len == 0 {
        return String::new();
    }
    // SAFETY: Caller guarantees `ptr` is valid for `len` initialized bytes for the duration of this copy.
    let bytes = unsafe { std::slice::from_raw_parts(ptr, len) };
    String::from_utf8_lossy(bytes).into_owned()
}

unsafe fn opt_utf8_string_from_raw(ptr: *const u8, len: usize) -> Option<String> {
    if ptr.is_null() {
        None
    } else {
        // SAFETY: Non-null `ptr` with `len` bytes was derived from a valid `Option<&str>` slice.
        Some(unsafe { utf8_string_from_raw(ptr, len) })
    }
}

unsafe extern "C" fn receive_cached_snapshot(
    out_ctx: *mut c_void,
    token_ptr: *const u8,
    token_len: usize,
    expires_at_unix_ms: u64,
    generation: u64,
    qp_ptr: *const u8,
    qp_len: usize,
) {
    // SAFETY: `out_ctx` points to a stack-allocated `Option<Arc<GcpTokenSnapshot>>` in `get_cached`,
    // and all byte pointers borrow the producer's `GcpTokenSnapshot` during this synchronous call.
    unsafe {
        let slot = &mut *(out_ctx.cast::<Option<Arc<GcpTokenSnapshot>>>());
        *slot = Some(Arc::new(GcpTokenSnapshot {
            access_token: utf8_string_from_raw(token_ptr, token_len),
            expires_at_unix_ms,
            generation,
            quota_project_id: opt_utf8_string_from_raw(qp_ptr, qp_len),
        }));
    }
}

unsafe extern "C" fn receive_refresh_result(
    out_ctx: *mut c_void,
    ok: bool,
    ptr: *const u8,
    len: usize,
    expires_at_unix_ms: u64,
    generation: u64,
    qp_ptr: *const u8,
    qp_len: usize,
) {
    if out_ctx.is_null() {
        return;
    }
    // SAFETY: `out_ctx` was created via `Box::into_raw` on a `tokio::sync::oneshot::Sender` and is
    // reclaimed at most once when `refresh_async_fn` invokes this completion callback.
    let tx = unsafe {
        Box::from_raw(
            out_ctx.cast::<tokio::sync::oneshot::Sender<Result<Arc<GcpTokenSnapshot>, String>>>(),
        )
    };
    let outcome = if ok {
        // SAFETY: Byte slices borrow the refreshed `GcpTokenSnapshot` during this callback.
        let snap = unsafe {
            Arc::new(GcpTokenSnapshot {
                access_token: utf8_string_from_raw(ptr, len),
                expires_at_unix_ms,
                generation,
                quota_project_id: opt_utf8_string_from_raw(qp_ptr, qp_len),
            })
        };
        Ok(snap)
    } else {
        // SAFETY: `ptr`/`len` borrow the UTF-8 error message during this callback.
        Err(unsafe { utf8_string_from_raw(ptr, len) })
    };
    let _ = tx.send(outcome);
}

unsafe extern "C" fn receive_service_account_key(
    out_ctx: *mut c_void,
    email_ptr: *const u8,
    email_len: usize,
    pem_ptr: *const u8,
    pem_len: usize,
    token_uri_ptr: *const u8,
    token_uri_len: usize,
    proj_ptr: *const u8,
    proj_len: usize,
    qp_ptr: *const u8,
    qp_len: usize,
) {
    // SAFETY: `out_ctx` points to a stack-allocated `Option<ServiceAccountKey>` and all byte pointers
    // borrow the producer's `ServiceAccountKey` for the duration of this synchronous call.
    unsafe {
        let slot = &mut *(out_ctx.cast::<Option<ServiceAccountKey>>());
        *slot = Some(ServiceAccountKey {
            client_email: utf8_string_from_raw(email_ptr, email_len),
            private_key_pem: utf8_string_from_raw(pem_ptr, pem_len),
            token_uri: utf8_string_from_raw(token_uri_ptr, token_uri_len),
            project_id: opt_utf8_string_from_raw(proj_ptr, proj_len),
            quota_project_id: opt_utf8_string_from_raw(qp_ptr, qp_len),
        });
    }
}

unsafe extern "C" fn receive_quota_project_id(
    out_ctx: *mut c_void,
    qp_ptr: *const u8,
    qp_len: usize,
) {
    // SAFETY: `out_ctx` points to a stack-allocated `Option<String>` and `qp_ptr` is valid for `qp_len` bytes.
    unsafe {
        let slot = &mut *(out_ctx.cast::<Option<String>>());
        *slot = Some(utf8_string_from_raw(qp_ptr, qp_len));
    }
}

impl SharedGcpTokenCacheHandle {
    pub fn from_arc(cache: Arc<GcpTokenCache>) -> Self {
        Self {
            repr: CacheHandleRepr::Local(cache),
        }
    }

    /// Construct an owned [`SharedGcpTokenCacheHandle`] by cloning the refcounted context inside a
    /// validated C-ABI [`GcpTokenCacheCapsule`].
    ///
    /// # Safety
    /// When non-null, `capsule_ptr` must point to a valid, aligned [`GcpTokenCacheCapsule`] whose
    /// `abi_version` and C-ABI vtable pointers were initialized by [`GcpTokenCacheCapsule::new`].
    pub unsafe fn from_capsule_ptr(
        capsule_ptr: *const GcpTokenCacheCapsule,
    ) -> Result<Self, String> {
        if capsule_ptr.is_null() {
            return Err("invalid or incompatible GcpTokenCacheCapsule".to_string());
        }
        // SAFETY: Caller verified the PyCapsule name matches `GCP_TOKEN_CACHE_CAPSULE_NAME`, so
        // `capsule_ptr` points to an initialized `#[repr(C)] GcpTokenCacheCapsule`.
        let capsule = unsafe { &*capsule_ptr };
        if capsule.abi_version != GCP_TOKEN_CACHE_ABI_VERSION || capsule.ctx.is_null() {
            return Err("invalid or incompatible GcpTokenCacheCapsule".to_string());
        }
        // SAFETY: `capsule.ctx` is a live owned pointer; `clone_ctx_fn` increments its refcount.
        let cloned_ctx = unsafe { (capsule.clone_ctx_fn)(capsule.ctx) };
        if cloned_ctx.is_null() {
            return Err("invalid or incompatible GcpTokenCacheCapsule".to_string());
        }
        Ok(Self {
            repr: CacheHandleRepr::Foreign(ForeignCapsuleHandle {
                ctx: cloned_ctx,
                clone_ctx_fn: capsule.clone_ctx_fn,
                drop_ctx_fn: capsule.drop_ctx_fn,
                generation_fn: capsule.generation_fn,
                invalidate_fn: capsule.invalidate_fn,
                try_cached_fn: capsule.try_cached_fn,
                refresh_async_fn: capsule.refresh_async_fn,
                service_account_key_fn: capsule.service_account_key_fn,
                quota_project_id_fn: capsule.quota_project_id_fn,
            }),
        })
    }

    pub fn get_cached(&self, now_unix_ms: u64) -> Option<Arc<GcpTokenSnapshot>> {
        match &self.repr {
            CacheHandleRepr::Local(cache) => cache.get_cached(now_unix_ms),
            CacheHandleRepr::Foreign(foreign) => {
                let mut out: Option<Arc<GcpTokenSnapshot>> = None;
                // SAFETY: `foreign.ctx` is live and `&mut out` is valid for synchronous `receive_cached_snapshot`.
                let hit = unsafe {
                    (foreign.try_cached_fn)(
                        foreign.ctx,
                        now_unix_ms,
                        (&raw mut out).cast::<c_void>(),
                        receive_cached_snapshot,
                    )
                };
                if hit { out } else { None }
            }
        }
    }

    pub async fn get_or_refresh(&self) -> Result<Arc<GcpTokenSnapshot>, String> {
        self.get_or_refresh_with_force(false).await
    }

    pub async fn get_or_refresh_with_force(
        &self,
        force_refresh: bool,
    ) -> Result<Arc<GcpTokenSnapshot>, String> {
        if !force_refresh && let Some(snap) = self.get_cached(current_unix_ms()) {
            return Ok(snap);
        }
        match &self.repr {
            CacheHandleRepr::Local(cache) => cache.get_or_refresh_with_force(force_refresh).await,
            CacheHandleRepr::Foreign(foreign) => {
                let (tx, rx) = tokio::sync::oneshot::channel();
                let out_ctx = Box::into_raw(Box::new(tx)).cast::<c_void>();
                // SAFETY: `foreign.ctx` is live and `out_ctx` transfers ownership of `oneshot::Sender`
                // to `receive_refresh_result`, which reclaims it once upon task completion.
                unsafe {
                    (foreign.refresh_async_fn)(
                        foreign.ctx,
                        force_refresh,
                        out_ctx,
                        receive_refresh_result,
                    );
                }
                rx.await.map_err(|_| {
                    "GCP token cache refresh task cancelled before completion".to_string()
                })?
            }
        }
    }

    pub fn generation(&self) -> u64 {
        match &self.repr {
            CacheHandleRepr::Local(cache) => cache.generation(),
            CacheHandleRepr::Foreign(foreign) => {
                // SAFETY: `foreign.ctx` is a live owned reference.
                unsafe { (foreign.generation_fn)(foreign.ctx) }
            }
        }
    }

    pub fn invalidate(&self, failed_generation: u64) {
        match &self.repr {
            CacheHandleRepr::Local(cache) => cache.invalidate(failed_generation),
            CacheHandleRepr::Foreign(foreign) => {
                // SAFETY: `foreign.ctx` is a live owned reference.
                unsafe { (foreign.invalidate_fn)(foreign.ctx, failed_generation) }
            }
        }
    }

    pub fn service_account_key(&self) -> Option<ServiceAccountKey> {
        match &self.repr {
            CacheHandleRepr::Local(cache) => cache.service_account_key().cloned(),
            CacheHandleRepr::Foreign(foreign) => {
                let mut out: Option<ServiceAccountKey> = None;
                // SAFETY: `foreign.ctx` is live and `&mut out` is valid during synchronous `receive_service_account_key`.
                let ok = unsafe {
                    (foreign.service_account_key_fn)(
                        foreign.ctx,
                        (&raw mut out).cast::<c_void>(),
                        receive_service_account_key,
                    )
                };
                if ok { out } else { None }
            }
        }
    }

    pub fn quota_project_id(&self) -> Option<String> {
        match &self.repr {
            CacheHandleRepr::Local(cache) => cache.quota_project_id().map(str::to_string),
            CacheHandleRepr::Foreign(foreign) => {
                let mut out: Option<String> = None;
                // SAFETY: `foreign.ctx` is live and `&mut out` is valid during synchronous `receive_quota_project_id`.
                let ok = unsafe {
                    (foreign.quota_project_id_fn)(
                        foreign.ctx,
                        (&raw mut out).cast::<c_void>(),
                        receive_quota_project_id,
                    )
                };
                if ok { out } else { None }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    #[tokio::test]
    async fn capsule_handle_warm_refresh_invalidate_and_post_drop_lifetime() {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("addr");
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = Arc::clone(&calls);

        let server = tokio::spawn(async move {
            for idx in 1..=2usize {
                let (mut socket, _) = listener.accept().await.expect("accept");
                let mut buf = vec![0u8; 4096];
                let _ = socket.read(&mut buf).await.expect("read");
                server_calls.fetch_add(1, Ordering::SeqCst);
                let body = format!(
                    "{{\"access_token\":\"tok-{idx}\",\"expires_in\":3600,\"token_type\":\"Bearer\"}}"
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(resp.as_bytes()).await.expect("write");
            }
        });

        let cache = Arc::new(GcpTokenCache::new(
            GcpAuthSource::AuthorizedUser {
                client_id: "cid".to_string(),
                client_secret: "csec".to_string(),
                refresh_token: "rtok".to_string(),
                quota_project_id: Some("qp-123".to_string()),
                token_uri: format!("http://{addr}/token"),
            },
            DEFAULT_REFRESH_SKEW_MS,
        ));

        unsafe extern "C" fn test_refresh_async(
            ctx: *const c_void,
            force_refresh: bool,
            out_ctx: *mut c_void,
            complete_cb: GcpRefreshCompleteFn,
        ) {
            let rt = tokio::runtime::Handle::current();
            // SAFETY: `ctx` and `complete_cb` originate from `GcpTokenCacheCapsule::new` / `SharedGcpTokenCacheHandle`.
            unsafe {
                invoke_capsule_refresh_on_runtime(&rt, ctx, force_refresh, out_ctx, complete_cb);
            }
        }

        let capsule = GcpTokenCacheCapsule::new(Arc::clone(&cache), test_refresh_async);
        // SAFETY: `&capsule` is a live `#[repr(C)] GcpTokenCacheCapsule`.
        let handle = unsafe { SharedGcpTokenCacheHandle::from_capsule_ptr(&raw const capsule) }
            .expect("valid capsule");
        drop(capsule);
        drop(cache);

        let cloned_handle = handle.clone();
        assert_eq!(cloned_handle.quota_project_id().as_deref(), Some("qp-123"));
        assert!(cloned_handle.service_account_key().is_none());

        let snap1 = cloned_handle.get_or_refresh().await.expect("first refresh");
        assert_eq!(snap1.access_token, "tok-1");
        assert_eq!(snap1.generation, 1);
        assert_eq!(snap1.quota_project_id.as_deref(), Some("qp-123"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        let snap_cached = handle.get_or_refresh().await.expect("cached hit");
        assert_eq!(snap_cached.access_token, "tok-1");
        assert_eq!(snap_cached.generation, 1);
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        handle.invalidate(1);
        let snap2 = cloned_handle
            .get_or_refresh()
            .await
            .expect("second refresh");
        assert_eq!(snap2.access_token, "tok-2");
        assert_eq!(snap2.generation, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        server.await.expect("server task");
    }

    #[test]
    fn capsule_handle_exposes_service_account_and_rejects_invalid_pointers() {
        // SAFETY: Null pointer rejection is explicitly tested.
        let null_res = unsafe { SharedGcpTokenCacheHandle::from_capsule_ptr(std::ptr::null()) };
        assert!(null_res.is_err());

        let cache = Arc::new(GcpTokenCache::new(
            GcpAuthSource::ServiceAccount {
                key: ServiceAccountKey {
                    client_email: "sa@proj.iam.gserviceaccount.com".to_string(),
                    private_key_pem: "-----BEGIN PRIVATE KEY-----\ntest\n-----END PRIVATE KEY-----"
                        .to_string(),
                    token_uri: DEFAULT_OAUTH_TOKEN_URI.to_string(),
                    project_id: Some("proj".to_string()),
                    quota_project_id: Some("qp-sa".to_string()),
                },
                scopes: vec![],
            },
            DEFAULT_REFRESH_SKEW_MS,
        ));

        unsafe extern "C" fn noop_refresh(
            _ctx: *const c_void,
            _force_refresh: bool,
            out_ctx: *mut c_void,
            complete_cb: GcpRefreshCompleteFn,
        ) {
            let err = b"unused";
            // SAFETY: Invokes completion callback once with valid static error bytes.
            unsafe {
                complete_cb(
                    out_ctx,
                    false,
                    err.as_ptr(),
                    err.len(),
                    0,
                    0,
                    std::ptr::null(),
                    0,
                );
            }
        }

        let mut bad_capsule = GcpTokenCacheCapsule::new(Arc::clone(&cache), noop_refresh);
        bad_capsule.abi_version = 999;
        // SAFETY: `bad_capsule` is valid memory with an incompatible `abi_version`.
        assert!(
            unsafe { SharedGcpTokenCacheHandle::from_capsule_ptr(&raw const bad_capsule) }.is_err()
        );

        let capsule = GcpTokenCacheCapsule::new(cache, noop_refresh);
        // SAFETY: `capsule` is a valid `GcpTokenCacheCapsule`.
        let handle =
            unsafe { SharedGcpTokenCacheHandle::from_capsule_ptr(&raw const capsule) }.unwrap();
        let sa = handle.service_account_key().expect("service account key");
        assert_eq!(sa.client_email, "sa@proj.iam.gserviceaccount.com");
        assert_eq!(sa.project_id.as_deref(), Some("proj"));
        assert_eq!(handle.quota_project_id().as_deref(), Some("qp-sa"));
    }
}
