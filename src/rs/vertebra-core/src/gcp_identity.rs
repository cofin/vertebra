//! Pure-Rust Google Cloud OIDC ID token cache across all ADC credential sources.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;

use jsonwebtoken::{Algorithm, EncodingKey, Header, dangerous::insecure_decode, encode};
use serde::{Deserialize, Serialize};

use crate::gcp_auth::{
    ExternalAccountStsRequest, GcpAuthSource, GcpTokenCache, current_unix_ms, normalize_gcp_options,
};

pub const DEFAULT_ID_TOKEN_REFRESH_SKEW_MS: u64 = 300_000;
pub const DEFAULT_MAX_ID_TOKEN_AUDIENCES: usize = 64;
pub const MAX_ID_TOKEN_AUDIENCE_BYTES: usize = 2_048;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MetadataIdTokenFormat {
    Standard,
    #[default]
    Full,
}

impl MetadataIdTokenFormat {
    fn as_query_value(self) -> &'static str {
        match self {
            Self::Standard => "standard",
            Self::Full => "full",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpIdTokenOptions {
    pub refresh_skew_ms: u64,
    pub include_email: bool,
    pub metadata_format: MetadataIdTokenFormat,
    pub max_audiences: usize,
}

impl Default for GcpIdTokenOptions {
    fn default() -> Self {
        Self {
            refresh_skew_ms: DEFAULT_ID_TOKEN_REFRESH_SKEW_MS,
            include_email: false,
            metadata_format: MetadataIdTokenFormat::Full,
            max_audiences: DEFAULT_MAX_ID_TOKEN_AUDIENCES,
        }
    }
}

impl GcpIdTokenOptions {
    pub fn from_options(raw_opts: &HashMap<String, String>) -> Result<Self, String> {
        let opts = normalize_gcp_options(raw_opts);
        let mut parsed = Self::default();

        if let Some(raw_skew) = opts.get("id_token_refresh_skew_ms") {
            let trimmed = raw_skew.trim();
            parsed.refresh_skew_ms = trimmed
                .parse::<u64>()
                .map_err(|e| format!("invalid id_token_refresh_skew_ms `{raw_skew}`: {e}"))?;
        }

        if let Some(raw_include_email) = opts.get("include_email") {
            parsed.include_email = match raw_include_email.trim().to_ascii_lowercase().as_str() {
                "true" | "1" => true,
                "false" | "0" => false,
                _ => {
                    return Err(format!(
                        "invalid include_email `{raw_include_email}`: expected true/false or 1/0"
                    ));
                }
            };
        }

        if let Some(raw_fmt) = opts.get("metadata_id_token_format") {
            parsed.metadata_format = match raw_fmt.trim().to_ascii_lowercase().as_str() {
                "full" => MetadataIdTokenFormat::Full,
                "standard" => MetadataIdTokenFormat::Standard,
                _ => {
                    return Err(format!(
                        "invalid metadata_id_token_format `{raw_fmt}`: expected `full` or `standard`"
                    ));
                }
            };
        }

        if let Some(raw_max) = opts.get("max_id_token_audiences") {
            let trimmed = raw_max.trim();
            let val = trimmed
                .parse::<usize>()
                .map_err(|e| format!("invalid max_id_token_audiences `{raw_max}`: {e}"))?;
            if val == 0 {
                return Err("invalid max_id_token_audiences `0`: must be >= 1".to_string());
            }
            parsed.max_audiences = val;
        }

        Ok(parsed)
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct GcpIdTokenSnapshot {
    pub id_token: String,
    pub audience: String,
    pub expires_at_unix_ms: u64,
    pub generation: u64,
}

impl std::fmt::Debug for GcpIdTokenSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcpIdTokenSnapshot")
            .field("id_token", &"[REDACTED]")
            .field("audience", &self.audience)
            .field("expires_at_unix_ms", &self.expires_at_unix_ms)
            .field("generation", &self.generation)
            .finish()
    }
}

#[derive(Serialize)]
struct ServiceAccountIdTokenClaims<'a> {
    iss: &'a str,
    sub: &'a str,
    aud: &'a str,
    target_audience: &'a str,
    iat: u64,
    exp: u64,
}

#[derive(Deserialize)]
struct OAuthIdTokenResponse {
    #[serde(default)]
    id_token: Option<String>,
}

#[derive(Serialize)]
struct IamGenerateIdTokenRequest<'a> {
    audience: &'a str,
    #[serde(rename = "includeEmail")]
    include_email: bool,
}

#[derive(Deserialize)]
struct IamGenerateIdTokenResponse {
    #[serde(default)]
    token: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum JwtAudience {
    One(String),
    Many(Vec<String>),
}

#[derive(Deserialize)]
struct IdTokenClaims {
    #[serde(default)]
    exp: Option<u64>,
    #[serde(default)]
    aud: Option<JwtAudience>,
}

pub fn validate_id_token_audience(audience: &str) -> Result<&str, String> {
    if audience.bytes().any(|b| b.is_ascii_control()) {
        return Err("ID token audience must not contain ASCII control characters".to_string());
    }
    let trimmed = audience.trim();
    if trimmed.is_empty() {
        return Err("ID token audience must not be empty".to_string());
    }
    if trimmed.len() > MAX_ID_TOKEN_AUDIENCE_BYTES {
        return Err(format!(
            "ID token audience exceeds maximum length of {MAX_ID_TOKEN_AUDIENCE_BYTES} bytes"
        ));
    }
    Ok(trimmed)
}

fn decode_and_validate_id_token(
    raw_token: &str,
    expected_audience: &str,
    now_unix_ms: u64,
) -> Result<u64, String> {
    let token_data = insecure_decode::<IdTokenClaims>(raw_token)
        .map_err(|e| format!("malformed OIDC ID token JWT: {e}"))?;
    let exp_secs = token_data
        .claims
        .exp
        .ok_or_else(|| "OIDC ID token JWT is missing `exp` claim".to_string())?;
    let expires_at_unix_ms = exp_secs.saturating_mul(1_000);
    if expires_at_unix_ms <= now_unix_ms {
        return Err(format!(
            "OIDC ID token JWT for audience `{expected_audience}` is already expired"
        ));
    }
    let aud_claim = token_data
        .claims
        .aud
        .ok_or_else(|| "OIDC ID token JWT is missing `aud` claim".to_string())?;
    let matches_aud = match &aud_claim {
        JwtAudience::One(single) => single == expected_audience,
        JwtAudience::Many(list) => list.iter().any(|item| item == expected_audience),
    };
    if !matches_aud {
        let actual = match aud_claim {
            JwtAudience::One(single) => single,
            JwtAudience::Many(list) => list.join(","),
        };
        return Err(format!(
            "OIDC ID token JWT `aud` mismatch: expected `{expected_audience}`, got `{actual}`"
        ));
    }
    Ok(expires_at_unix_ms)
}

async fn fetch_id_token_from_gcloud_cli(
    binary: &Path,
    timeout: Duration,
    impersonate_sa: Option<&str>,
    audience: &str,
) -> Result<String, String> {
    let mut cmd = tokio::process::Command::new(binary);
    cmd.args(["auth", "print-identity-token"]);
    cmd.arg(format!("--audiences={audience}"));
    cmd.arg("--quiet");
    if let Some(sa) = impersonate_sa.filter(|s| !s.trim().is_empty()) {
        cmd.arg(format!("--impersonate-service-account={sa}"));
    }
    cmd.kill_on_drop(true);
    match tokio::time::timeout(timeout, cmd.output()).await {
        Ok(Ok(out)) if out.status.success() => {
            let tok = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if tok.is_empty() {
                return Err(format!(
                    "`{}` print-identity-token produced empty output",
                    binary.display()
                ));
            }
            Ok(tok)
        }
        Ok(Ok(out)) => {
            let stderr = String::from_utf8_lossy(&out.stderr).trim().to_string();
            Err(format!(
                "`{}` print-identity-token failed: {stderr}",
                binary.display()
            ))
        }
        Ok(Err(e)) => Err(format!("failed to execute `{}` CLI: {e}", binary.display())),
        Err(_) => Err(format!(
            "`{}` print-identity-token timed out after {} ms",
            binary.display(),
            timeout.as_millis()
        )),
    }
}

struct CachedIdTokenEntry {
    snapshot: Arc<GcpIdTokenSnapshot>,
    minted_at_unix_ms: u64,
}

pub struct GcpIdTokenCache {
    credentials: Arc<GcpTokenCache>,
    options: GcpIdTokenOptions,
    entries: RwLock<HashMap<String, CachedIdTokenEntry>>,
    inflight_locks: Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
    generation: AtomicU64,
}

impl std::fmt::Debug for GcpIdTokenCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GcpIdTokenCache")
            .field("options", &self.options)
            .field("generation", &self.generation.load(Ordering::Acquire))
            .finish()
    }
}

impl GcpIdTokenCache {
    pub fn new(
        credentials: Arc<GcpTokenCache>,
        options: GcpIdTokenOptions,
    ) -> Result<Arc<Self>, String> {
        if options.max_audiences == 0 {
            return Err("invalid max_id_token_audiences `0`: must be >= 1".to_string());
        }
        if matches!(credentials.auth_source(), GcpAuthSource::Static { .. }) {
            return Err("static access tokens cannot mint ID tokens".to_string());
        }
        Ok(Arc::new(Self {
            credentials,
            options,
            entries: RwLock::new(HashMap::new()),
            inflight_locks: Mutex::new(HashMap::new()),
            generation: AtomicU64::new(0),
        }))
    }

    pub fn from_options(raw_opts: &HashMap<String, String>) -> Result<Arc<Self>, String> {
        let id_options = GcpIdTokenOptions::from_options(raw_opts)?;
        let credentials = GcpTokenCache::from_options(raw_opts, &[]).ok_or_else(|| {
            "unable to discover GCP credentials (checked gcp_options, GOOGLE_APPLICATION_CREDENTIALS, and ambient ADC)".to_string()
        })?;
        Self::new(credentials, id_options)
    }

    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }

    pub fn refresh_skew_ms(&self) -> u64 {
        self.options.refresh_skew_ms
    }

    pub fn get_cached(&self, audience: &str, now_unix_ms: u64) -> Option<Arc<GcpIdTokenSnapshot>> {
        let aud = validate_id_token_audience(audience).ok()?;
        let guard = self
            .entries
            .read()
            .expect("id token snapshot lock poisoned");
        let entry = guard.get(aud)?;
        let lifetime_ms = entry
            .snapshot
            .expires_at_unix_ms
            .saturating_sub(entry.minted_at_unix_ms);
        let effective_skew_ms = self.options.refresh_skew_ms.min(lifetime_ms / 2);
        if now_unix_ms.saturating_add(effective_skew_ms) < entry.snapshot.expires_at_unix_ms {
            Some(Arc::clone(&entry.snapshot))
        } else {
            None
        }
    }

    pub fn needs_refresh(&self, audience: &str, now_unix_ms: u64) -> bool {
        self.get_cached(audience, now_unix_ms).is_none()
    }

    pub fn invalidate(&self, audience: &str, failed_generation: u64) {
        let Ok(aud) = validate_id_token_audience(audience) else {
            return;
        };
        let mut guard = self
            .entries
            .write()
            .expect("id token snapshot lock poisoned");
        if let Some(current) = guard.get(aud)
            && current.snapshot.generation == failed_generation
        {
            guard.remove(aud);
        }
    }

    pub async fn get_or_refresh(
        &self,
        audience: &str,
        force_refresh: bool,
    ) -> Result<Arc<GcpIdTokenSnapshot>, String> {
        let aud = validate_id_token_audience(audience)?.to_string();
        let now = current_unix_ms();
        if !force_refresh && let Some(snap) = self.get_cached(&aud, now) {
            return Ok(snap);
        }

        let flight_lock = {
            let mut locks = self
                .inflight_locks
                .lock()
                .expect("id token inflight lock map poisoned");
            if locks.len() > self.options.max_audiences {
                locks.retain(|_, lock| Arc::strong_count(lock) > 1);
            }
            Arc::clone(
                locks
                    .entry(aud.clone())
                    .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(()))),
            )
        };

        let _flight = flight_lock.lock().await;
        let now_after_lock = current_unix_ms();
        if !force_refresh && let Some(snap) = self.get_cached(&aud, now_after_lock) {
            return Ok(snap);
        }

        let raw_token = self.mint_raw_id_token(&aud, now_after_lock).await?;
        let expires_at_unix_ms = decode_and_validate_id_token(&raw_token, &aud, now_after_lock)?;
        let next_gen = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        let snapshot = Arc::new(GcpIdTokenSnapshot {
            id_token: raw_token,
            audience: aud.clone(),
            expires_at_unix_ms,
            generation: next_gen,
        });

        {
            let mut guard = self
                .entries
                .write()
                .expect("id token snapshot lock poisoned");
            guard.insert(
                aud.clone(),
                CachedIdTokenEntry {
                    snapshot: Arc::clone(&snapshot),
                    minted_at_unix_ms: now_after_lock,
                },
            );
            while guard.len() > self.options.max_audiences {
                let victim = Self::select_eviction_victim(&guard, &aud, now_after_lock);
                let Some(victim_key) = victim else {
                    break;
                };
                guard.remove(&victim_key);
            }
        }

        Ok(snapshot)
    }

    fn select_eviction_victim(
        entries: &HashMap<String, CachedIdTokenEntry>,
        preserve_key: &str,
        now_unix_ms: u64,
    ) -> Option<String> {
        let expired_victim = entries
            .iter()
            .filter(|(k, v)| {
                k.as_str() != preserve_key && v.snapshot.expires_at_unix_ms <= now_unix_ms
            })
            .min_by(|(ka, va), (kb, vb)| {
                va.snapshot
                    .expires_at_unix_ms
                    .cmp(&vb.snapshot.expires_at_unix_ms)
                    .then_with(|| ka.cmp(kb))
            })
            .map(|(k, _)| k.clone());
        if expired_victim.is_some() {
            return expired_victim;
        }
        entries
            .iter()
            .filter(|(k, _)| k.as_str() != preserve_key)
            .min_by(|(ka, va), (kb, vb)| {
                va.snapshot
                    .expires_at_unix_ms
                    .cmp(&vb.snapshot.expires_at_unix_ms)
                    .then_with(|| ka.cmp(kb))
            })
            .map(|(k, _)| k.clone())
    }

    async fn mint_raw_id_token(&self, audience: &str, now_unix_ms: u64) -> Result<String, String> {
        match self.credentials.auth_source() {
            GcpAuthSource::Static { .. } => {
                Err("static access tokens cannot mint ID tokens".to_string())
            }
            GcpAuthSource::ServiceAccount { key, .. } => {
                let now_secs = now_unix_ms / 1_000;
                let claims = ServiceAccountIdTokenClaims {
                    iss: &key.client_email,
                    sub: &key.client_email,
                    aud: &key.token_uri,
                    target_audience: audience,
                    iat: now_secs,
                    exp: now_secs + 3600,
                };
                let encoding_key = EncodingKey::from_rsa_pem(key.private_key_pem.as_bytes())
                    .map_err(|e| format!("invalid service account RSA private key PEM: {e}"))?;
                let header = Header::new(Algorithm::RS256);
                let assertion = encode(&header, &claims, &encoding_key)
                    .map_err(|e| format!("failed to encode service account ID token JWT: {e}"))?;
                let form_body = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer")
                    .append_pair("assertion", &assertion)
                    .finish();
                let mut req = self
                    .credentials
                    .http_client()
                    .post(&key.token_uri)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(form_body);
                if let Some(ref qp) = key.quota_project_id
                    && !qp.trim().is_empty()
                {
                    req = req.header("x-goog-user-project", qp.trim());
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| format!("Service account OIDC ID token request failed: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!(
                        "Service account OIDC ID token request returned HTTP {}",
                        resp.status()
                    ));
                }
                let parsed: OAuthIdTokenResponse = resp
                    .json()
                    .await
                    .map_err(|e| format!("invalid OIDC ID token JSON: {e}"))?;
                parsed
                    .id_token
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .ok_or_else(|| {
                        "Service account OIDC token response contained no id_token".to_string()
                    })
            }
            GcpAuthSource::MetadataServer {
                service_account,
                endpoint,
                ..
            } => {
                let query = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("audience", audience)
                    .append_pair("format", self.options.metadata_format.as_query_value())
                    .finish();
                let url = format!(
                    "{}/computeMetadata/v1/instance/service-accounts/{}/identity?{}",
                    endpoint.trim_end_matches('/'),
                    service_account,
                    query
                );
                let resp = self
                    .credentials
                    .metadata_http_client()
                    .get(&url)
                    .header("Metadata-Flavor", "Google")
                    .send()
                    .await
                    .map_err(|e| format!("metadata server ID token request failed: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!(
                        "metadata server ID token request returned HTTP {}",
                        resp.status()
                    ));
                }
                let raw = resp
                    .text()
                    .await
                    .map_err(|e| format!("failed to read metadata server ID token body: {e}"))?;
                let trimmed = raw.trim().to_string();
                if trimmed.is_empty() {
                    return Err("metadata server returned empty ID token".to_string());
                }
                Ok(trimmed)
            }
            GcpAuthSource::ImpersonatedServiceAccount {
                base,
                target_service_account,
                iam_endpoint,
                quota_project_id,
                ..
            } => {
                let source_token = self.credentials.fetch_source_access_token(base).await?;
                let url = format!(
                    "{}/v1/projects/-/serviceAccounts/{}:generateIdToken",
                    iam_endpoint.trim_end_matches('/'),
                    target_service_account
                );
                let mut req = self
                    .credentials
                    .http_client()
                    .post(&url)
                    .bearer_auth(&source_token)
                    .json(&IamGenerateIdTokenRequest {
                        audience,
                        include_email: self.options.include_email,
                    });
                if let Some(qp) = quota_project_id.as_deref().filter(|s| !s.trim().is_empty()) {
                    req = req.header("x-goog-user-project", qp.trim());
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| format!("IAM generateIdToken failed: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!(
                        "IAM generateIdToken returned HTTP {}",
                        resp.status()
                    ));
                }
                let parsed: IamGenerateIdTokenResponse = resp
                    .json()
                    .await
                    .map_err(|e| format!("invalid IAM generateIdToken JSON: {e}"))?;
                parsed
                    .token
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .ok_or_else(|| "IAM generateIdToken response contained no token".to_string())
            }
            GcpAuthSource::GcloudCli {
                binary,
                timeout,
                impersonate_service_account,
                ..
            } => {
                fetch_id_token_from_gcloud_cli(
                    binary,
                    *timeout,
                    impersonate_service_account.as_deref(),
                    audience,
                )
                .await
            }
            GcpAuthSource::AuthorizedUser {
                client_id,
                client_secret,
                refresh_token,
                quota_project_id,
                token_uri,
            } => {
                if audience != client_id {
                    return Err(format!(
                        "authorized_user credentials mint ID tokens only for audience '{client_id}' (the ADC client_id), not '{audience}'; set target_service_account to impersonate a service account for any audience, or add '{client_id}' to the IAP programmaticClients allowlist and request audience '{client_id}'"
                    ));
                }
                let form_body = url::form_urlencoded::Serializer::new(String::new())
                    .append_pair("client_id", client_id.as_str())
                    .append_pair("client_secret", client_secret.as_str())
                    .append_pair("refresh_token", refresh_token.as_str())
                    .append_pair("grant_type", "refresh_token")
                    .finish();
                let mut req = self
                    .credentials
                    .http_client()
                    .post(token_uri)
                    .header("Content-Type", "application/x-www-form-urlencoded")
                    .body(form_body);
                if let Some(qp) = quota_project_id.as_deref().filter(|s| !s.trim().is_empty()) {
                    req = req.header("x-goog-user-project", qp.trim());
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| format!("OAuth2 ID token refresh request failed: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!(
                        "OAuth2 ID token refresh returned HTTP {}",
                        resp.status()
                    ));
                }
                let parsed: OAuthIdTokenResponse = resp
                    .json()
                    .await
                    .map_err(|e| format!("invalid OAuth2 ID token JSON: {e}"))?;
                parsed
                    .id_token
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .ok_or_else(|| {
                        "OAuth2 refresh response contained no id_token; re-authenticate with `gcloud auth application-default login` or configure service_account / target_service_account credentials".to_string()
                    })
            }
            GcpAuthSource::ExternalAccount {
                audience: wif_audience,
                subject_token_type,
                token_url,
                credential_source,
                service_account_impersonation_url,
                quota_project_id,
                scopes,
                client_id: ext_client_id,
                client_secret: ext_client_secret,
                ..
            } => {
                let Some(imp_url) = service_account_impersonation_url.as_deref() else {
                    return Err(
                        "external_account credentials require service_account_impersonation_url or target_service_account impersonation to mint OIDC ID tokens"
                            .to_string(),
                    );
                };
                let id_token_url = imp_url
                    .strip_suffix(":generateAccessToken")
                    .map(|prefix| format!("{prefix}:generateIdToken"))
                    .ok_or_else(|| {
                        format!(
                            "invalid external_account service_account_impersonation_url '{imp_url}': expected ':generateAccessToken' suffix"
                        )
                    })?;
                let sts_token = self
                    .credentials
                    .fetch_external_sts_token(ExternalAccountStsRequest {
                        audience: wif_audience,
                        subject_token_type,
                        token_url,
                        credential_source,
                        scopes,
                        client_id: ext_client_id.as_deref(),
                        client_secret: ext_client_secret.as_deref(),
                    })
                    .await?;
                let mut req = self
                    .credentials
                    .http_client()
                    .post(&id_token_url)
                    .bearer_auth(&sts_token)
                    .json(&IamGenerateIdTokenRequest {
                        audience,
                        include_email: self.options.include_email,
                    });
                if let Some(qp) = quota_project_id.as_deref().filter(|s| !s.trim().is_empty()) {
                    req = req.header("x-goog-user-project", qp.trim());
                }
                let resp = req
                    .send()
                    .await
                    .map_err(|e| format!("IAM generateIdToken failed: {e}"))?;
                if !resp.status().is_success() {
                    return Err(format!(
                        "IAM generateIdToken returned HTTP {}",
                        resp.status()
                    ));
                }
                let parsed: IamGenerateIdTokenResponse = resp
                    .json()
                    .await
                    .map_err(|e| format!("invalid IAM generateIdToken JSON: {e}"))?;
                parsed
                    .token
                    .map(|t| t.trim().to_string())
                    .filter(|t| !t.is_empty())
                    .ok_or_else(|| "IAM generateIdToken response contained no token".to_string())
            }
        }
    }
}
