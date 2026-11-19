//! Pure-Rust Google Cloud Application Default Credentials (ADC) token cache and RS256 JWT signer.

use std::collections::HashMap;
use std::ffi::{CStr, c_void};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const DEFAULT_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_GCP_SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];
pub const GCLOUD_CLI_TIMEOUT: Duration = Duration::from_secs(30);
pub const GCP_TOKEN_CACHE_CAPSULE_NAME: &CStr = c"vertebra.GcpTokenCacheCapsule";
pub const GCP_TOKEN_CACHE_ABI_VERSION: u32 = 1;
pub(crate) const DEFAULT_TOKEN_TTL_MS: u64 = 3_600_000;
pub(crate) const DEFAULT_OAUTH_TOKEN_URI: &str = "https://oauth2.googleapis.com/token";
pub(crate) const DEFAULT_IAM_ENDPOINT: &str = "https://iamcredentials.googleapis.com";
pub(crate) const DEFAULT_UNIVERSE_DOMAIN: &str = "googleapis.com";
pub(crate) const STS_GRANT_TYPE_TOKEN_EXCHANGE: &str =
    "urn:ietf:params:oauth:grant-type:token-exchange";
pub(crate) const STS_REQUESTED_TOKEN_TYPE_ACCESS: &str =
    "urn:ietf:params:oauth:token-type:access_token";
pub(crate) const AWS_IMDSV2_TTL_SECONDS: u64 = 300;

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

fn url_is_link_local_metadata(raw_url: &str) -> bool {
    url::Url::parse(raw_url)
        .ok()
        .and_then(|u| {
            u.host_str()
                .map(|h| matches!(h, "169.254.169.254" | "metadata.google.internal"))
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
        GcpAuthSource::ExternalAccount {
            token_url,
            service_account_impersonation_url,
            token_info_url,
            credential_source,
            ..
        } => {
            url_is_loopback(token_url)
                || service_account_impersonation_url
                    .as_deref()
                    .is_some_and(url_is_loopback)
                || token_info_url.as_deref().is_some_and(url_is_loopback)
                || match credential_source {
                    ExternalSubjectTokenSource::File { .. } => false,
                    ExternalSubjectTokenSource::Url { url, .. } => url_is_loopback(url),
                    ExternalSubjectTokenSource::Aws {
                        region_url,
                        url,
                        regional_cred_verification_url,
                        imdsv2_session_token_url,
                        ..
                    } => {
                        region_url.as_deref().is_some_and(url_is_loopback)
                            || url.as_deref().is_some_and(url_is_loopback)
                            || imdsv2_session_token_url
                                .as_deref()
                                .is_some_and(url_is_loopback)
                            || url_is_loopback(regional_cred_verification_url)
                    }
                }
        }
        GcpAuthSource::Static { .. } | GcpAuthSource::GcloudCli { .. } => false,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalCredentialFormat {
    Text,
    Json { subject_token_field_name: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalSubjectTokenSource {
    File {
        path: PathBuf,
        format: ExternalCredentialFormat,
    },
    Url {
        url: String,
        headers: HashMap<String, String>,
        format: ExternalCredentialFormat,
    },
    Aws {
        environment_id: String,
        region_url: Option<String>,
        url: Option<String>,
        regional_cred_verification_url: String,
        imdsv2_session_token_url: Option<String>,
    },
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
    ExternalAccount {
        audience: String,
        subject_token_type: String,
        token_url: String,
        credential_source: ExternalSubjectTokenSource,
        service_account_impersonation_url: Option<String>,
        quota_project_id: Option<String>,
        scopes: Vec<String>,
        token_info_url: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
    },
}

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    expires_in: Option<u64>,
    #[serde(default)]
    scope: Option<String>,
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
struct ExternalCredentialFormatFile {
    #[serde(rename = "type", default)]
    format_type: String,
    #[serde(default)]
    subject_token_field_name: Option<String>,
}

#[derive(Deserialize)]
struct ExternalAccountCredentialSourceFile {
    #[serde(default)]
    file: Option<String>,
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    headers: Option<HashMap<String, String>>,
    #[serde(default)]
    format: Option<ExternalCredentialFormatFile>,
    #[serde(default)]
    environment_id: Option<String>,
    #[serde(default)]
    region_url: Option<String>,
    #[serde(default)]
    regional_cred_verification_url: Option<String>,
    #[serde(default)]
    imdsv2_session_token_url: Option<String>,
    #[serde(default)]
    executable: Option<serde_json::Value>,
}

#[derive(Deserialize)]
struct ExternalAccountCredentialFile {
    #[serde(rename = "type", default)]
    cred_type: String,
    #[serde(default)]
    audience: Option<String>,
    #[serde(default)]
    subject_token_type: Option<String>,
    #[serde(default)]
    token_url: Option<String>,
    #[serde(default)]
    credential_source: Option<ExternalAccountCredentialSourceFile>,
    #[serde(default)]
    service_account_impersonation_url: Option<String>,
    #[serde(default)]
    quota_project_id: Option<String>,
    #[serde(default)]
    token_info_url: Option<String>,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    client_secret: Option<String>,
    #[serde(default)]
    universe_domain: Option<String>,
}

#[derive(Deserialize)]
struct AwsImdsCredentials {
    #[serde(rename = "AccessKeyId")]
    access_key_id: String,
    #[serde(rename = "SecretAccessKey")]
    secret_access_key: String,
    #[serde(rename = "Token", default)]
    token: Option<String>,
}

#[derive(Serialize)]
struct AwsSignedHeader {
    key: String,
    value: String,
}

#[derive(Serialize)]
struct AwsSignedRequestPayload {
    url: String,
    method: String,
    headers: Vec<AwsSignedHeader>,
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

pub(crate) struct ExternalAccountStsRequest<'a> {
    pub(crate) audience: &'a str,
    pub(crate) subject_token_type: &'a str,
    pub(crate) token_url: &'a str,
    pub(crate) credential_source: &'a ExternalSubjectTokenSource,
    pub(crate) scopes: &'a [String],
    pub(crate) client_id: Option<&'a str>,
    pub(crate) client_secret: Option<&'a str>,
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

fn validate_http_url(raw: &str, field_name: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err(format!("external_account {field_name} must not be empty"));
    }
    let parsed = url::Url::parse(trimmed)
        .map_err(|e| format!("invalid external_account {field_name} '{trimmed}': {e}"))?;
    if !matches!(parsed.scheme(), "http" | "https") || parsed.host_str().is_none() {
        return Err(format!(
            "external_account {field_name} must be an http or https URL with a host: '{trimmed}'"
        ));
    }
    Ok(trimmed.to_string())
}

fn parse_external_credential_format(
    raw_format: Option<ExternalCredentialFormatFile>,
) -> Result<ExternalCredentialFormat, String> {
    let Some(fmt) = raw_format else {
        return Ok(ExternalCredentialFormat::Text);
    };
    let kind = fmt.format_type.trim().to_ascii_lowercase();
    match kind.as_str() {
        "" | "text" => Ok(ExternalCredentialFormat::Text),
        "json" => {
            let field_name = fmt
                .subject_token_field_name
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    "external_account credential_source.format 'json' requires a non-empty subject_token_field_name".to_string()
                })?;
            Ok(ExternalCredentialFormat::Json {
                subject_token_field_name: field_name,
            })
        }
        other => Err(format!(
            "unsupported external_account credential_source.format.type '{other}'"
        )),
    }
}

pub fn parse_external_account_json(
    raw_json: &str,
    quota_project_override: Option<&str>,
    token_uri_override: Option<&str>,
    universe_domain_override: Option<&str>,
    scopes: Vec<String>,
) -> Result<GcpAuthSource, String> {
    let parsed: ExternalAccountCredentialFile = serde_json::from_str(raw_json)
        .map_err(|e| format!("invalid external_account JSON: {e}"))?;
    if parsed.cred_type != "external_account" {
        return Err(format!(
            "expected credential type 'external_account', found '{}'",
            parsed.cred_type
        ));
    }

    if let Some(ref raw_ud) = parsed.universe_domain
        && raw_ud.trim().is_empty()
    {
        return Err("external_account universe_domain must not be empty".to_string());
    }
    let cred_universe_domain = parsed
        .universe_domain
        .as_deref()
        .map(str::trim)
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN);
    let expected_universe_domain = universe_domain_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or(DEFAULT_UNIVERSE_DOMAIN);
    if cred_universe_domain != expected_universe_domain {
        return Err(format!(
            "external_account universe_domain '{cred_universe_domain}' does not match configured universe_domain '{expected_universe_domain}'"
        ));
    }

    let audience = parsed
        .audience
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "external_account requires a non-empty audience".to_string())?;
    let subject_token_type = parsed
        .subject_token_type
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .ok_or_else(|| "external_account requires a non-empty subject_token_type".to_string())?;
    let raw_token_url = token_uri_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or(parsed.token_url)
        .ok_or_else(|| "external_account requires a non-empty token_url".to_string())?;
    let token_url = validate_http_url(&raw_token_url, "token_url")?;

    let service_account_impersonation_url = match parsed.service_account_impersonation_url {
        Some(raw) => Some(validate_http_url(
            &raw,
            "service_account_impersonation_url",
        )?),
        None => None,
    };
    let token_info_url = match parsed.token_info_url {
        Some(raw) if !raw.trim().is_empty() => Some(validate_http_url(&raw, "token_info_url")?),
        _ => None,
    };
    let client_id = parsed
        .client_id
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let client_secret = parsed
        .client_secret
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    let quota_project_id = quota_project_override
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .or_else(|| parsed.quota_project_id.filter(|s| !s.trim().is_empty()));

    let cs = parsed
        .credential_source
        .ok_or_else(|| "external_account requires credential_source".to_string())?;
    if cs.executable.is_some() {
        return Err(
            "external_account executable credential_source is not supported for security reasons"
                .to_string(),
        );
    }

    let credential_source = if let Some(raw_env_id) = cs.environment_id {
        let env_id = raw_env_id.trim().to_string();
        if !env_id.starts_with("aws1") {
            return Err(format!(
                "unsupported external_account environment_id '{env_id}'"
            ));
        }
        let raw_regional_url = cs
            .regional_cred_verification_url
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                "external_account aws1 credential_source requires regional_cred_verification_url"
                    .to_string()
            })?;
        let probe_url = raw_regional_url.replace("{region}", "us-east-1");
        validate_http_url(
            &probe_url,
            "credential_source.regional_cred_verification_url",
        )?;
        let region_url = match cs.region_url {
            Some(raw) if !raw.trim().is_empty() => {
                Some(validate_http_url(&raw, "credential_source.region_url")?)
            }
            _ => None,
        };
        let url = match cs.url {
            Some(raw) if !raw.trim().is_empty() => {
                Some(validate_http_url(&raw, "credential_source.url")?)
            }
            _ => None,
        };
        let imdsv2_session_token_url = match cs.imdsv2_session_token_url {
            Some(raw) if !raw.trim().is_empty() => Some(validate_http_url(
                &raw,
                "credential_source.imdsv2_session_token_url",
            )?),
            _ => None,
        };
        ExternalSubjectTokenSource::Aws {
            environment_id: env_id,
            region_url,
            url,
            regional_cred_verification_url: raw_regional_url,
            imdsv2_session_token_url,
        }
    } else if let Some(raw_file) = cs.file {
        if cs.url.is_some() {
            return Err(
                "external_account credential_source cannot specify both file and url".to_string(),
            );
        }
        let trimmed = raw_file.trim();
        if trimmed.is_empty() {
            return Err("external_account credential_source.file must not be empty".to_string());
        }
        let format = parse_external_credential_format(cs.format)?;
        ExternalSubjectTokenSource::File {
            path: PathBuf::from(trimmed),
            format,
        }
    } else if let Some(raw_url) = cs.url {
        let url = validate_http_url(&raw_url, "credential_source.url")?;
        let format = parse_external_credential_format(cs.format)?;
        let mut headers = HashMap::new();
        if let Some(raw_headers) = cs.headers {
            for (k, v) in raw_headers {
                let key = k.trim();
                if key.is_empty() {
                    return Err(
                        "external_account credential_source.headers contains an empty header name"
                            .to_string(),
                    );
                }
                headers.insert(key.to_string(), v);
            }
        }
        ExternalSubjectTokenSource::Url {
            url,
            headers,
            format,
        }
    } else {
        return Err(
            "external_account credential_source must specify file, url, or aws1 environment_id"
                .to_string(),
        );
    };

    Ok(GcpAuthSource::ExternalAccount {
        audience,
        subject_token_type,
        token_url,
        credential_source,
        service_account_impersonation_url,
        quota_project_id,
        scopes,
        token_info_url,
        client_id,
        client_secret,
    })
}

fn extract_subject_token(raw: &str, format: &ExternalCredentialFormat) -> Result<String, String> {
    match format {
        ExternalCredentialFormat::Text => {
            let trimmed = raw.trim();
            if trimmed.is_empty() {
                return Err("external_account subject token payload is empty".to_string());
            }
            Ok(trimmed.to_string())
        }
        ExternalCredentialFormat::Json {
            subject_token_field_name,
        } => {
            let parsed: serde_json::Value = serde_json::from_str(raw)
                .map_err(|e| format!("invalid external_account JSON subject token payload: {e}"))?;
            let token = parsed
                .get(subject_token_field_name)
                .and_then(|v| v.as_str())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .ok_or_else(|| {
                    format!(
                        "external_account JSON subject token missing non-empty string field '{subject_token_field_name}'"
                    )
                })?;
            Ok(token.to_string())
        }
    }
}

fn parse_aws_region_from_az(raw: &str) -> Result<String, String> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Err("AWS region_url returned empty region".to_string());
    }
    let bytes = trimmed.as_bytes();
    let region = if bytes.len() >= 2
        && bytes[bytes.len() - 1].is_ascii_lowercase()
        && bytes[bytes.len() - 2].is_ascii_digit()
    {
        &trimmed[..trimmed.len() - 1]
    } else {
        trimmed
    };
    Ok(region.to_string())
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn hmac_sha256(key: &[u8], message: &[u8]) -> Result<Vec<u8>, String> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key)
        .map_err(|e| format!("HMAC-SHA256 initialization failed: {e}"))?;
    mac.update(message);
    Ok(mac.finalize().into_bytes().to_vec())
}

fn aws_sigv4_encode_component(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    for &b in input.as_bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            use std::fmt::Write;
            let _ = write!(&mut out, "%{b:02X}");
        }
    }
    out
}

fn build_aws_sigv4_subject_token(
    regional_cred_verification_url: &str,
    region: &str,
    audience: &str,
    access_key_id: &str,
    secret_access_key: &str,
    session_token: Option<&str>,
    now_unix_ms: u64,
) -> Result<String, String> {
    let target_url = regional_cred_verification_url.replace("{region}", region);
    let parsed = url::Url::parse(&target_url)
        .map_err(|e| format!("invalid AWS regional_cred_verification_url '{target_url}': {e}"))?;
    let host = parsed
        .host_str()
        .ok_or_else(|| "missing host in AWS regional_cred_verification_url".to_string())?;
    let host_header = match parsed.port() {
        Some(port)
            if (parsed.scheme() == "http" && port != 80)
                || (parsed.scheme() == "https" && port != 443) =>
        {
            format!("{host}:{port}")
        }
        _ => host.to_string(),
    };

    let now_secs = i64::try_from(now_unix_ms / 1_000)
        .map_err(|_| "timestamp overflow during AWS SigV4 signing".to_string())?;
    let dt = chrono::DateTime::from_timestamp(now_secs, 0)
        .ok_or_else(|| "invalid timestamp for AWS SigV4 signing".to_string())?;
    let amz_date = dt.format("%Y%m%dT%H%M%SZ").to_string();
    let datestamp = dt.format("%Y%m%d").to_string();

    let canonical_uri = if parsed.path().is_empty() {
        "/"
    } else {
        parsed.path()
    };
    let mut query_pairs: Vec<(String, String)> = parsed
        .query_pairs()
        .map(|(k, v)| {
            (
                aws_sigv4_encode_component(&k),
                aws_sigv4_encode_component(&v),
            )
        })
        .collect();
    query_pairs.sort();
    let canonical_querystring = query_pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");

    let mut signed_header_map = std::collections::BTreeMap::new();
    signed_header_map.insert("host", host_header);
    signed_header_map.insert("x-amz-date", amz_date.clone());
    if let Some(tok) = session_token.map(str::trim).filter(|s| !s.is_empty()) {
        signed_header_map.insert("x-amz-security-token", tok.to_string());
    }
    signed_header_map.insert("x-goog-cloud-target-resource", audience.to_string());

    let mut canonical_headers = String::new();
    let mut signed_header_names = Vec::with_capacity(signed_header_map.len());
    for (k, v) in &signed_header_map {
        use std::fmt::Write;
        let _ = writeln!(&mut canonical_headers, "{k}:{}", v.trim());
        signed_header_names.push(*k);
    }
    let signed_headers = signed_header_names.join(";");
    let payload_hash = sha256_hex(b"");
    let canonical_request = format!(
        "POST\n{canonical_uri}\n{canonical_querystring}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );

    let credential_scope = format!("{datestamp}/{region}/sts/aws4_request");
    let string_to_sign = format!(
        "AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{}",
        sha256_hex(canonical_request.as_bytes())
    );

    let k_date = hmac_sha256(
        format!("AWS4{secret_access_key}").as_bytes(),
        datestamp.as_bytes(),
    )?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, b"sts")?;
    let k_signing = hmac_sha256(&k_service, b"aws4_request")?;
    let signature = hex::encode(hmac_sha256(&k_signing, string_to_sign.as_bytes())?);
    let auth_header = format!(
        "AWS4-HMAC-SHA256 Credential={access_key_id}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    );

    let mut headers = Vec::with_capacity(signed_header_map.len() + 1);
    headers.push(AwsSignedHeader {
        key: "Authorization".to_string(),
        value: auth_header,
    });
    for (k, v) in signed_header_map {
        headers.push(AwsSignedHeader {
            key: k.to_string(),
            value: v,
        });
    }

    let payload = AwsSignedRequestPayload {
        url: target_url,
        method: "POST".to_string(),
        headers,
    };
    serde_json::to_string(&payload)
        .map_err(|e| format!("failed to serialize AWS SigV4 subject_token: {e}"))
}

pub(crate) fn normalize_gcp_options(raw_opts: &HashMap<String, String>) -> HashMap<String, String> {
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
    requested_scopes: &[String],
) -> Result<String, String> {
    let subcommands: &[&[&str]] = if requested_scopes.is_empty() {
        &[
            &["auth", "application-default", "print-access-token"],
            &["auth", "print-access-token"],
        ]
    } else {
        &[&["auth", "application-default", "print-access-token"]]
    };
    let mut last_err = String::new();
    for args in subcommands {
        let mut cmd = tokio::process::Command::new(binary);
        cmd.args(*args);
        cmd.arg("--quiet");
        if !requested_scopes.is_empty() {
            cmd.arg(format!("--scopes={}", requested_scopes.join(",")));
        }
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
    requested_scopes: Vec<String>,
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
            requested_scopes: Vec::new(),
        }
    }

    pub fn with_requested_scopes(mut self, scopes: Vec<String>) -> Self {
        self.requested_scopes = scopes;
        self
    }

    pub fn from_scoped_options(
        raw_opts: Option<&HashMap<String, String>>,
        default_scopes: &[String],
    ) -> Result<Arc<Self>, String> {
        let empty = HashMap::new();
        let raw = raw_opts.unwrap_or(&empty);
        let normalized = normalize_gcp_options(raw);
        let custom_scopes = parse_scopes_option(&normalized);
        let effective_scopes: Vec<String> = custom_scopes.unwrap_or_else(|| {
            default_scopes
                .iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        });

        let mut merged = raw.clone();
        if !effective_scopes.is_empty() && !merged.contains_key("scopes") {
            merged.insert("scopes".to_string(), effective_scopes.join(","));
        }

        let scope_refs: Vec<&str> = effective_scopes.iter().map(String::as_str).collect();
        let cache = Self::from_options_result(&merged, &scope_refs)?;
        if matches!(cache.source, GcpAuthSource::Static { .. }) && !effective_scopes.is_empty() {
            return Err(
                "static access token cannot satisfy explicit non-empty OAuth scopes; configure a refreshable credential source (service_account, authorized_user, external_account, metadata_server, impersonated_service_account, or gcloud_cli)".to_string(),
            );
        }
        let mut inner = Arc::try_unwrap(cache).unwrap_or_else(|arc| Self {
            source: arc.source.clone(),
            refresh_skew_ms: arc.refresh_skew_ms,
            snapshot: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            generation: AtomicU64::new(0),
            client: arc.client.clone(),
            metadata_client: arc.metadata_client.clone(),
            requested_scopes: Vec::new(),
        });
        inner.requested_scopes = effective_scopes;
        Ok(Arc::new(inner))
    }

    pub fn from_options(
        raw_opts: &HashMap<String, String>,
        default_scopes: &[&str],
    ) -> Option<Arc<Self>> {
        Self::from_options_result(raw_opts, default_scopes).ok()
    }

    pub fn from_options_result(
        raw_opts: &HashMap<String, String>,
        default_scopes: &[&str],
    ) -> Result<Arc<Self>, String> {
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
        let universe_domain_override = opts
            .get("universe_domain")
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .or_else(|| {
                if allow_ambient {
                    std::env::var("GOOGLE_CLOUD_UNIVERSE_DOMAIN")
                        .ok()
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                } else {
                    None
                }
            });
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
        let mut explicit_credential_requested = false;

        let parse_adc_source_str = |raw: &str| -> Result<GcpAuthSource, String> {
            if let Some(sa_key) =
                parse_service_account_key_json(raw, quota_project_id.as_deref(), token_uri_override)
            {
                return Ok(GcpAuthSource::ServiceAccount {
                    key: sa_key,
                    scopes: default_sa_scopes(),
                });
            }
            let parsed: AdcCredentialFile = serde_json::from_str(raw)
                .map_err(|e| format!("invalid ADC credentials JSON: {e}"))?;
            match parsed.cred_type.as_str() {
                "authorized_user" => {
                    if let (Some(cid), Some(csec), Some(rtok)) =
                        (parsed.client_id, parsed.client_secret, parsed.refresh_token)
                    {
                        Ok(GcpAuthSource::AuthorizedUser {
                            client_id: cid,
                            client_secret: csec,
                            refresh_token: rtok,
                            quota_project_id: quota_project_id.clone().or(parsed.quota_project_id),
                            token_uri: token_uri_override
                                .map(str::to_string)
                                .or(parsed.token_uri)
                                .unwrap_or_else(|| DEFAULT_OAUTH_TOKEN_URI.to_string()),
                        })
                    } else {
                        Err("authorized_user ADC JSON missing client_id, client_secret, or refresh_token".to_string())
                    }
                }
                "external_account" => parse_external_account_json(
                    raw,
                    quota_project_id.as_deref(),
                    token_uri_override,
                    universe_domain_override.as_deref(),
                    default_sa_scopes(),
                ),
                other => Err(format!("unsupported ADC credential type '{other}'")),
            }
        };

        let parse_adc_source = |adc_path: &Path| -> Result<GcpAuthSource, String> {
            let raw = std::fs::read_to_string(adc_path).map_err(|e| {
                format!(
                    "failed to read ADC credentials file '{}': {e}",
                    adc_path.display()
                )
            })?;
            parse_adc_source_str(&raw)
        };

        let build_metadata_source = || -> GcpAuthSource {
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
                service_account: opts
                    .get("metadata_service_account")
                    .cloned()
                    .unwrap_or_else(|| "default".to_string()),
                quota_project_id: quota_project_id.clone(),
                endpoint,
                scopes: custom_scopes.clone().unwrap_or_default(),
            }
        };

        if let Some(inline_json) = opts
            .get("service_account_key")
            .filter(|v| !v.trim().is_empty())
        {
            explicit_credential_requested = true;
            let sa_key = parse_service_account_key_json(
                inline_json,
                quota_project_id.as_deref(),
                token_uri_override,
            )
            .ok_or_else(|| "invalid service_account_key JSON".to_string())?;
            base_source = Some(GcpAuthSource::ServiceAccount {
                key: sa_key,
                scopes: default_sa_scopes(),
            });
        } else if let Some(sa_path_or_json) = opts
            .get("service_account_path")
            .or_else(|| opts.get("service_account"))
            .filter(|v| !v.trim().is_empty())
        {
            explicit_credential_requested = true;
            let trimmed = sa_path_or_json.trim();
            let sa_key = if trimmed.starts_with('{') {
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
            }
            .ok_or_else(|| "invalid service_account_path or service_account JSON".to_string())?;
            base_source = Some(GcpAuthSource::ServiceAccount {
                key: sa_key,
                scopes: default_sa_scopes(),
            });
        } else if let Some(ext_json) = opts
            .get("external_account_json")
            .filter(|v| !v.trim().is_empty())
        {
            explicit_credential_requested = true;
            base_source = Some(parse_external_account_json(
                ext_json,
                quota_project_id.as_deref(),
                token_uri_override,
                universe_domain_override.as_deref(),
                default_sa_scopes(),
            )?);
        } else if let Some(cred_json) = opts
            .get("credentials_json")
            .filter(|v| !v.trim().is_empty())
        {
            explicit_credential_requested = true;
            base_source = Some(parse_adc_source_str(cred_json)?);
        } else if opts.contains_key("client_id")
            || opts.contains_key("client_secret")
            || opts.contains_key("refresh_token")
        {
            explicit_credential_requested = true;
            let (Some(cid), Some(csec), Some(rtok)) = (
                opts.get("client_id").filter(|v| !v.trim().is_empty()),
                opts.get("client_secret").filter(|v| !v.trim().is_empty()),
                opts.get("refresh_token").filter(|v| !v.trim().is_empty()),
            ) else {
                return Err(
                    "authorized_user options require non-empty client_id, client_secret, and refresh_token".to_string(),
                );
            };
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
        } else if explicit_adc.is_some_and(|v| !v.trim().is_empty()) {
            explicit_credential_requested = true;
            let adc_path = explicit_adc_candidate.as_ref().ok_or_else(|| {
                "explicit application_credentials path does not exist".to_string()
            })?;
            base_source = Some(parse_adc_source(adc_path)?);
        } else if let Some(tok) = opts
            .get("access_token")
            .or_else(|| opts.get("bearer_token"))
            .filter(|s| !s.trim().is_empty())
        {
            explicit_credential_requested = true;
            base_source = Some(GcpAuthSource::Static {
                access_token: tok.clone(),
                expires_at_unix_ms: None,
                quota_project_id: quota_project_id.clone(),
            });
        } else if opts
            .get("use_metadata_server")
            .is_some_and(|v| v == "true" || v == "1")
        {
            explicit_credential_requested = true;
            base_source = Some(build_metadata_source());
        } else if opts
            .get("use_gcloud_cli")
            .is_some_and(|v| v == "true" || v == "1")
        {
            explicit_credential_requested = true;
            base_source = Some(GcpAuthSource::GcloudCli {
                binary: default_gcloud_binary(),
                timeout: GCLOUD_CLI_TIMEOUT,
                quota_project_id: quota_project_id.clone(),
                impersonate_service_account: None,
            });
        }

        if !explicit_credential_requested && base_source.is_none() && allow_ambient {
            if let Some(ref adc_path) = ambient_adc_candidate {
                match parse_adc_source(adc_path) {
                    Ok(src) => base_source = Some(src),
                    Err(err) => {
                        if let Ok(raw) = std::fs::read_to_string(adc_path)
                            && let Ok(parsed) = serde_json::from_str::<AdcCredentialFile>(&raw)
                            && parsed.cred_type == "external_account"
                        {
                            return Err(err);
                        }
                    }
                }
            }
            if base_source.is_none() && target_service_account.is_none() {
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
            let allow_metadata_fallback = !opts
                .get("use_metadata_server")
                .is_some_and(|v| v == "false" || v == "0");
            if base_source.is_none() && target_service_account.is_none() && allow_metadata_fallback
            {
                base_source = Some(build_metadata_source());
            }
        }

        let source = if let Some(target_sa) = target_service_account {
            let resolved_base = base_source.unwrap_or_else(&build_metadata_source);
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
            base_source.ok_or_else(|| {
                "unable to discover GCP credentials (checked gcp_options, GOOGLE_APPLICATION_CREDENTIALS, and ambient ADC)".to_string()
            })?
        };

        Ok(Arc::new(Self::with_proxy_policy(
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

    pub fn auth_source(&self) -> &GcpAuthSource {
        &self.source
    }

    pub(crate) fn http_client(&self) -> &reqwest::Client {
        &self.client
    }

    pub(crate) fn metadata_http_client(&self) -> &reqwest::Client {
        &self.metadata_client
    }

    pub(crate) async fn fetch_source_access_token(
        &self,
        source: &GcpAuthSource,
    ) -> Result<String, String> {
        let (token, _, _) = self.fetch_from_source(source, current_unix_ms()).await?;
        Ok(token)
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
            }
            | GcpAuthSource::ExternalAccount {
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
                    let form_body = {
                        let mut form_ser = url::form_urlencoded::Serializer::new(String::new());
                        form_ser
                            .append_pair("client_id", client_id.as_str())
                            .append_pair("client_secret", client_secret.as_str())
                            .append_pair("refresh_token", refresh_token.as_str())
                            .append_pair("grant_type", "refresh_token");
                        if !self.requested_scopes.is_empty() {
                            form_ser.append_pair("scope", &self.requested_scopes.join(" "));
                        }
                        form_ser.finish()
                    };
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
                        if self.requested_scopes.is_empty()
                            && token_uri == DEFAULT_OAUTH_TOKEN_URI
                            && let Ok(cli_tok) = fetch_token_from_gcloud_cli(
                                &default_gcloud_binary(),
                                GCLOUD_CLI_TIMEOUT,
                                None,
                                &[],
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
                    if !self.requested_scopes.is_empty()
                        && let Some(ref returned_scope) = parsed.scope
                    {
                        let granted: Vec<&str> = returned_scope.split_whitespace().collect();
                        for req_scope in &self.requested_scopes {
                            if !granted.contains(&req_scope.as_str()) {
                                return Err(format!(
                                    "OAuth2 refresh response omitted requested scope '{req_scope}' (granted: '{returned_scope}')"
                                ));
                            }
                        }
                    }
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
                        &self.requested_scopes,
                    )
                    .await?;
                    Ok((
                        tok,
                        now_unix_ms.saturating_add(DEFAULT_TOKEN_TTL_MS),
                        quota_project_id.clone(),
                    ))
                }
                GcpAuthSource::ExternalAccount {
                    audience,
                    subject_token_type,
                    token_url,
                    credential_source,
                    service_account_impersonation_url,
                    quota_project_id,
                    scopes,
                    client_id,
                    client_secret,
                    ..
                } => {
                    let sts_req = ExternalAccountStsRequest {
                        audience,
                        subject_token_type,
                        token_url,
                        credential_source,
                        scopes,
                        client_id: client_id.as_deref(),
                        client_secret: client_secret.as_deref(),
                    };
                    let (sts_token, sts_expires_at_unix_ms) = self
                        .exchange_external_account_sts_token(
                            sts_req,
                            service_account_impersonation_url.is_some(),
                            now_unix_ms,
                        )
                        .await?;
                    let Some(imp_url) = service_account_impersonation_url else {
                        return Ok((sts_token, sts_expires_at_unix_ms, quota_project_id.clone()));
                    };

                    let default_scopes_owned: Vec<String>;
                    let effective_scopes: &[String] = if !self.requested_scopes.is_empty() {
                        &self.requested_scopes
                    } else if !scopes.is_empty() {
                        scopes
                    } else {
                        default_scopes_owned = DEFAULT_GCP_SCOPES
                            .iter()
                            .map(|s| (*s).to_string())
                            .collect();
                        &default_scopes_owned
                    };
                    let mut req = self.client.post(imp_url).bearer_auth(&sts_token).json(
                        &IamGenerateTokenRequest {
                            scope: effective_scopes,
                        },
                    );
                    if let Some(qp) = quota_project_id
                        .as_deref()
                        .map(str::trim)
                        .filter(|s| !s.is_empty())
                    {
                        req = req.header("x-goog-user-project", qp);
                    }
                    let resp = req
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
                    let token = parsed.access_token.trim().to_string();
                    if token.is_empty() {
                        return Err(
                            "IAM generateAccessToken returned empty accessToken".to_string()
                        );
                    }
                    let expires_at_unix_ms = parsed
                        .expire_time
                        .as_deref()
                        .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                        .map(|dt| dt.timestamp_millis().max(0) as u64)
                        .unwrap_or_else(|| now_unix_ms.saturating_add(DEFAULT_TOKEN_TTL_MS));
                    Ok((token, expires_at_unix_ms, quota_project_id.clone()))
                }
            }
        })
    }

    pub(crate) async fn fetch_external_sts_token(
        &self,
        sts_req: ExternalAccountStsRequest<'_>,
    ) -> Result<String, String> {
        let (tok, _) = self
            .exchange_external_account_sts_token(sts_req, true, current_unix_ms())
            .await?;
        Ok(tok)
    }

    async fn exchange_external_account_sts_token(
        &self,
        sts_req: ExternalAccountStsRequest<'_>,
        for_impersonation: bool,
        now_unix_ms: u64,
    ) -> Result<(String, u64), String> {
        let ExternalAccountStsRequest {
            audience,
            subject_token_type,
            token_url,
            credential_source,
            scopes,
            client_id,
            client_secret,
        } = sts_req;
        let subject_token = self
            .fetch_external_subject_token(credential_source, audience, now_unix_ms)
            .await?;

        let sts_scope = if for_impersonation {
            DEFAULT_GCP_SCOPES.join(" ")
        } else if !self.requested_scopes.is_empty() {
            self.requested_scopes.join(" ")
        } else if !scopes.is_empty() {
            scopes.join(" ")
        } else {
            DEFAULT_GCP_SCOPES.join(" ")
        };

        let form_body = {
            let mut form_ser = url::form_urlencoded::Serializer::new(String::new());
            form_ser
                .append_pair("grant_type", STS_GRANT_TYPE_TOKEN_EXCHANGE)
                .append_pair("audience", audience)
                .append_pair("scope", &sts_scope)
                .append_pair("requested_token_type", STS_REQUESTED_TOKEN_TYPE_ACCESS)
                .append_pair("subject_token", &subject_token)
                .append_pair("subject_token_type", subject_token_type);
            if let Some(cid) = client_id.map(str::trim).filter(|s| !s.is_empty())
                && client_secret.map(str::trim).is_none_or(str::is_empty)
            {
                form_ser.append_pair("client_id", cid);
            }
            form_ser.finish()
        };

        let mut req = self
            .client
            .post(token_url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_body);
        if let (Some(cid), Some(csec)) = (
            client_id.map(str::trim).filter(|s| !s.is_empty()),
            client_secret.map(str::trim).filter(|s| !s.is_empty()),
        ) {
            req = req.basic_auth(cid, Some(csec));
        }

        let resp = req
            .send()
            .await
            .map_err(|e| format!("GCP STS token exchange request failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!(
                "GCP STS token exchange returned HTTP {}",
                resp.status()
            ));
        }
        let parsed: OAuthTokenResponse = resp
            .json()
            .await
            .map_err(|e| format!("invalid GCP STS token JSON: {e}"))?;
        let token = parsed.access_token.trim().to_string();
        if token.is_empty() {
            return Err("GCP STS token exchange returned empty access_token".to_string());
        }
        let ttl_ms = parsed
            .expires_in
            .map(|s| s.saturating_mul(1_000))
            .unwrap_or(DEFAULT_TOKEN_TTL_MS);
        Ok((token, now_unix_ms.saturating_add(ttl_ms)))
    }

    async fn fetch_external_subject_token(
        &self,
        credential_source: &ExternalSubjectTokenSource,
        audience: &str,
        now_unix_ms: u64,
    ) -> Result<String, String> {
        match credential_source {
            ExternalSubjectTokenSource::File { path, format } => {
                let raw = std::fs::read_to_string(path).map_err(|e| {
                    format!(
                        "failed to read external_account subject token file '{}': {e}",
                        path.display()
                    )
                })?;
                extract_subject_token(&raw, format)
            }
            ExternalSubjectTokenSource::Url {
                url,
                headers,
                format,
            } => {
                let client = if url_is_link_local_metadata(url) {
                    &self.metadata_client
                } else {
                    &self.client
                };
                let mut req = client.get(url);
                for (k, v) in headers {
                    req = req.header(k.as_str(), v.as_str());
                }
                let resp = req.send().await.map_err(|e| {
                    format!("external_account URL subject token request failed: {e}")
                })?;
                if !resp.status().is_success() {
                    return Err(format!(
                        "external_account URL subject token request returned HTTP {}",
                        resp.status()
                    ));
                }
                let raw = resp.text().await.map_err(|e| {
                    format!("failed to read external_account URL subject token body: {e}")
                })?;
                extract_subject_token(&raw, format)
            }
            ExternalSubjectTokenSource::Aws {
                region_url,
                url,
                regional_cred_verification_url,
                imdsv2_session_token_url,
                ..
            } => {
                let env_region = std::env::var("AWS_REGION")
                    .ok()
                    .or_else(|| std::env::var("AWS_DEFAULT_REGION").ok())
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                let env_ak = std::env::var("AWS_ACCESS_KEY_ID")
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                let env_sk = std::env::var("AWS_SECRET_ACCESS_KEY")
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());
                let env_tok = std::env::var("AWS_SESSION_TOKEN")
                    .ok()
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty());

                let need_imds = env_region.is_none() || env_ak.is_none() || env_sk.is_none();
                let imds_token = if need_imds && let Some(token_url) = imdsv2_session_token_url {
                    let ttl_secs = AWS_IMDSV2_TTL_SECONDS.clamp(1, 21_600);
                    let resp = self
                        .metadata_client
                        .put(token_url)
                        .header("X-aws-ec2-metadata-token-ttl-seconds", ttl_secs.to_string())
                        .send()
                        .await
                        .map_err(|e| format!("AWS IMDSv2 session token request failed: {e}"))?;
                    if !resp.status().is_success() {
                        return Err(format!(
                            "AWS IMDSv2 session token request returned HTTP {}",
                            resp.status()
                        ));
                    }
                    let tok = resp
                        .text()
                        .await
                        .map_err(|e| format!("failed to read AWS IMDSv2 session token: {e}"))?;
                    let trimmed = tok.trim().to_string();
                    if trimmed.is_empty() {
                        return Err("AWS IMDSv2 session token response was empty".to_string());
                    }
                    Some(trimmed)
                } else {
                    None
                };

                let region = match env_region {
                    Some(r) => r,
                    None => {
                        let r_url = region_url.as_deref().ok_or_else(|| {
                            "AWS region not set in AWS_REGION/AWS_DEFAULT_REGION and no region_url configured".to_string()
                        })?;
                        let mut req = self.metadata_client.get(r_url);
                        if let Some(ref tok) = imds_token {
                            req = req.header("X-aws-ec2-metadata-token", tok);
                        }
                        let resp = req
                            .send()
                            .await
                            .map_err(|e| format!("AWS region_url request failed: {e}"))?;
                        if !resp.status().is_success() {
                            return Err(format!("AWS region_url returned HTTP {}", resp.status()));
                        }
                        let body = resp
                            .text()
                            .await
                            .map_err(|e| format!("failed to read AWS region_url response: {e}"))?;
                        parse_aws_region_from_az(&body)?
                    }
                };

                let (access_key_id, secret_access_key, session_token) = if let (
                    Some(ak),
                    Some(sk),
                ) = (env_ak, env_sk)
                {
                    (ak, sk, env_tok)
                } else {
                    let creds_url = url.as_deref().ok_or_else(|| {
                            "AWS credentials not set in AWS_ACCESS_KEY_ID/AWS_SECRET_ACCESS_KEY and no IMDS url configured".to_string()
                        })?;
                    let mut req = self.metadata_client.get(creds_url);
                    if let Some(ref tok) = imds_token {
                        req = req.header("X-aws-ec2-metadata-token", tok);
                    }
                    let resp = req
                        .send()
                        .await
                        .map_err(|e| format!("AWS IMDS credentials request failed: {e}"))?;
                    if !resp.status().is_success() {
                        return Err(format!(
                            "AWS IMDS credentials request returned HTTP {}",
                            resp.status()
                        ));
                    }
                    let first_body = resp
                        .text()
                        .await
                        .map_err(|e| format!("failed to read AWS IMDS response: {e}"))?;
                    let cred_json = if first_body.trim_start().starts_with('{') {
                        first_body
                    } else {
                        let role_name = first_body
                            .lines()
                            .next()
                            .map(str::trim)
                            .filter(|s| !s.is_empty())
                            .ok_or_else(|| {
                                "AWS IMDS role discovery returned empty role name".to_string()
                            })?;
                        let role_url = format!("{}/{}", creds_url.trim_end_matches('/'), role_name);
                        let mut role_req = self.metadata_client.get(&role_url);
                        if let Some(ref tok) = imds_token {
                            role_req = role_req.header("X-aws-ec2-metadata-token", tok);
                        }
                        let role_resp = role_req
                            .send()
                            .await
                            .map_err(|e| format!("AWS IMDS role credentials failed: {e}"))?;
                        if !role_resp.status().is_success() {
                            return Err(format!(
                                "AWS IMDS role credentials returned HTTP {}",
                                role_resp.status()
                            ));
                        }
                        role_resp
                            .text()
                            .await
                            .map_err(|e| format!("failed to read AWS IMDS role credentials: {e}"))?
                    };
                    let parsed: AwsImdsCredentials = serde_json::from_str(&cred_json)
                        .map_err(|e| format!("invalid AWS IMDS credentials JSON: {e}"))?;
                    if parsed.access_key_id.trim().is_empty()
                        || parsed.secret_access_key.trim().is_empty()
                    {
                        return Err(
                            "AWS IMDS credentials contained empty AccessKeyId or SecretAccessKey"
                                .to_string(),
                        );
                    }
                    (
                        parsed.access_key_id,
                        parsed.secret_access_key,
                        parsed.token.filter(|s| !s.trim().is_empty()),
                    )
                };

                build_aws_sigv4_subject_token(
                    regional_cred_verification_url,
                    &region,
                    audience,
                    &access_key_id,
                    &secret_access_key,
                    session_token.as_deref(),
                    now_unix_ms,
                )
            }
        }
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

fn parse_gcp_http_method(method: &str) -> Result<reqwest::Method, String> {
    if method.eq_ignore_ascii_case("GET") {
        Ok(reqwest::Method::GET)
    } else if method.eq_ignore_ascii_case("POST") {
        Ok(reqwest::Method::POST)
    } else if method.eq_ignore_ascii_case("PATCH") {
        Ok(reqwest::Method::PATCH)
    } else if method.eq_ignore_ascii_case("PUT") {
        Ok(reqwest::Method::PUT)
    } else if method.eq_ignore_ascii_case("DELETE") {
        Ok(reqwest::Method::DELETE)
    } else {
        Err(format!(
            "unsupported HTTP method `{method}`; expected GET, POST, PATCH, PUT, or DELETE"
        ))
    }
}

/// Validate and normalize an HTTP method supported by [`gcp_http2_request`].
pub fn validate_gcp_http_method(method: &str) -> Result<&'static str, String> {
    match parse_gcp_http_method(method)? {
        reqwest::Method::GET => Ok("GET"),
        reqwest::Method::POST => Ok("POST"),
        reqwest::Method::PATCH => Ok("PATCH"),
        reqwest::Method::PUT => Ok("PUT"),
        reqwest::Method::DELETE => Ok("DELETE"),
        other => Err(format!("unsupported HTTP method `{other}`")),
    }
}

async fn send_request_with_client(
    client: &reqwest::Client,
    method: reqwest::Method,
    url: &str,
    headers: &HashMap<String, String>,
    body: Option<bytes::Bytes>,
    timeout: Duration,
) -> Result<reqwest::Response, reqwest::Error> {
    let mut req = client.request(method, url).timeout(timeout);
    if let Some(payload) = body {
        req = req.body(payload);
    }
    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    req.send().await
}

/// Execute a connection-pooled HTTP/2 or HTTP/1.1 request and return `(status, lowercase_headers, body)`.
pub async fn gcp_http2_request(
    method: &str,
    url: &str,
    headers: &HashMap<String, String>,
    body: Option<Vec<u8>>,
    timeout_ms: u64,
) -> Result<(u16, HashMap<String, String>, Vec<u8>), String> {
    let parsed_method = parse_gcp_http_method(method)?;
    let method_name = parsed_method.as_str().to_owned();
    let timeout = Duration::from_millis(timeout_ms.max(1));
    let payload = body.map(bytes::Bytes::from);
    let client = if url_is_loopback(url) {
        shared_gcp_direct_http_client()
    } else {
        shared_gcp_http_client()
    };

    let resp = send_request_with_client(client, parsed_method, url, headers, payload, timeout)
        .await
        .map_err(|e| format!("GCP HTTP/2 {method_name} to `{url}` failed: {e}"))?;

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

/// Execute a connection-pooled HTTP/2 or HTTP/1.1 unary POST request and return `(status, lowercase_headers, body)`.
pub async fn gcp_http2_post(
    url: &str,
    headers: &HashMap<String, String>,
    body: Vec<u8>,
    timeout_ms: u64,
) -> Result<(u16, HashMap<String, String>, Vec<u8>), String> {
    gcp_http2_request("POST", url, headers, Some(body), timeout_ms).await
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
