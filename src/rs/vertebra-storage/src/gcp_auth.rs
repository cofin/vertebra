//! Active GCP Application Default Credentials (ADC) token cache and `RefreshingGcpCredentialProvider`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use object_store::CredentialProvider;
use object_store::gcp::GcpCredential;
use serde::{Deserialize, Serialize};

pub const DEFAULT_REFRESH_SKEW_MS: u64 = 300_000;
const DEFAULT_TOKEN_TTL_MS: u64 = 3_600_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GcpTokenSnapshot {
    pub access_token: String,
    pub expires_at_unix_ms: u64,
    pub generation: u64,
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
    MetadataServer {
        service_account: String,
        quota_project_id: Option<String>,
        endpoint: String,
    },
    ImpersonatedServiceAccount {
        base: Box<GcpAuthSource>,
        target_service_account: String,
        iam_endpoint: String,
        scopes: Vec<String>,
        quota_project_id: Option<String>,
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
struct AdcAuthorizedUserFile {
    #[serde(rename = "type", default)]
    cred_type: String,
    #[serde(default)]
    client_id: Option<String>,
    #[serde(default)]
    client_secret: Option<String>,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    quota_project_id: Option<String>,
    #[serde(default)]
    token_uri: Option<String>,
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

pub struct GcpTokenCache {
    source: GcpAuthSource,
    refresh_skew_ms: u64,
    snapshot: RwLock<Option<Arc<GcpTokenSnapshot>>>,
    refresh_lock: tokio::sync::Mutex<()>,
    generation: AtomicU64,
    client: reqwest::Client,
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
        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            source,
            refresh_skew_ms,
            snapshot: RwLock::new(None),
            refresh_lock: tokio::sync::Mutex::new(()),
            generation: AtomicU64::new(0),
            client,
        }
    }

    pub fn from_gcs_options(opts: &HashMap<String, String>) -> Option<Arc<Self>> {
        let skew_ms = opts
            .get("refresh_skew_ms")
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(DEFAULT_REFRESH_SKEW_MS);
        let quota_project_id = opts.get("quota_project_id").cloned();

        let mut base_source: Option<GcpAuthSource> = None;

        let allow_ambient = opts
            .get("allow_ambient_adc")
            .is_none_or(|v| v != "false" && v != "0");

        let adc_file_candidate = opts
            .get("application_credentials_path")
            .or_else(|| opts.get("application_credentials"))
            .cloned()
            .or_else(|| {
                if !allow_ambient {
                    return None;
                }
                if let Ok(env_p) = std::env::var("GOOGLE_APPLICATION_CREDENTIALS")
                    && !env_p.trim().is_empty()
                    && std::path::Path::new(env_p.trim()).is_file()
                {
                    return Some(env_p.trim().to_string());
                }
                if let Ok(cfg_dir) = std::env::var("CLOUDSDK_CONFIG")
                    && !cfg_dir.trim().is_empty()
                {
                    let p = std::path::Path::new(cfg_dir.trim())
                        .join("application_default_credentials.json");
                    if p.is_file() {
                        return Some(p.to_string_lossy().into_owned());
                    }
                }
                if let Ok(home) = std::env::var("HOME") {
                    let p = std::path::Path::new(&home)
                        .join(".config")
                        .join("gcloud")
                        .join("application_default_credentials.json");
                    if p.is_file() {
                        return Some(p.to_string_lossy().into_owned());
                    }
                }
                None
            });

        if let (Some(cid), Some(csec), Some(rtok)) = (
            opts.get("client_id"),
            opts.get("client_secret"),
            opts.get("refresh_token"),
        ) {
            let token_uri = opts
                .get("token_uri")
                .cloned()
                .unwrap_or_else(|| "https://oauth2.googleapis.com/token".to_string());
            base_source = Some(GcpAuthSource::AuthorizedUser {
                client_id: cid.clone(),
                client_secret: csec.clone(),
                refresh_token: rtok.clone(),
                quota_project_id: quota_project_id.clone(),
                token_uri,
            });
        } else if let Some(ref adc_path) = adc_file_candidate
            && let Ok(raw) = std::fs::read_to_string(adc_path)
            && let Ok(parsed) = serde_json::from_str::<AdcAuthorizedUserFile>(&raw)
            && parsed.cred_type == "authorized_user"
            && let (Some(cid), Some(csec), Some(rtok)) =
                (parsed.client_id, parsed.client_secret, parsed.refresh_token)
        {
            base_source = Some(GcpAuthSource::AuthorizedUser {
                client_id: cid,
                client_secret: csec,
                refresh_token: rtok,
                quota_project_id: quota_project_id.clone().or(parsed.quota_project_id),
                token_uri: parsed
                    .token_uri
                    .unwrap_or_else(|| "https://oauth2.googleapis.com/token".to_string()),
            });
        } else if opts.get("use_metadata_server").is_some_and(|v| v == "true") {
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
                service_account: "default".to_string(),
                quota_project_id: quota_project_id.clone(),
                endpoint,
            });
        } else if opts.get("target_service_account").is_none()
            && let Some(tok) = opts
                .get("access_token")
                .or_else(|| opts.get("bearer_token"))
        {
            base_source = Some(GcpAuthSource::Static {
                access_token: tok.clone(),
                expires_at_unix_ms: None,
                quota_project_id: quota_project_id.clone(),
            });
        }

        let source = if let Some(target_sa) = opts.get("target_service_account") {
            let resolved_base = base_source.unwrap_or_else(|| {
                let host = std::env::var("GCE_METADATA_HOST")
                    .unwrap_or_else(|_| "metadata.google.internal".to_string());
                let endpoint = if host.starts_with("http://") || host.starts_with("https://") {
                    host
                } else {
                    format!("http://{host}")
                };
                GcpAuthSource::MetadataServer {
                    service_account: "default".to_string(),
                    quota_project_id: quota_project_id.clone(),
                    endpoint,
                }
            });
            let iam_endpoint = opts
                .get("iam_endpoint")
                .cloned()
                .unwrap_or_else(|| "https://iamcredentials.googleapis.com".to_string());
            GcpAuthSource::ImpersonatedServiceAccount {
                base: Box::new(resolved_base),
                target_service_account: target_sa.clone(),
                iam_endpoint,
                scopes: vec![
                    "https://www.googleapis.com/auth/devstorage.full_control".to_string(),
                    "https://www.googleapis.com/auth/cloud-platform".to_string(),
                ],
                quota_project_id,
            }
        } else {
            base_source?
        };

        Some(Arc::new(Self::new(source, skew_ms)))
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
                        return Err(format!("OAuth2 refresh returned HTTP {}", resp.status()));
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
                GcpAuthSource::MetadataServer {
                    service_account,
                    quota_project_id,
                    endpoint,
                } => {
                    let url = format!(
                        "{}/computeMetadata/v1/instance/service-accounts/{}/token",
                        endpoint.trim_end_matches('/'),
                        service_account
                    );
                    let resp = self
                        .client
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
            }
        })
    }
}

#[derive(Debug, Clone)]
pub struct RefreshingGcpCredentialProvider {
    cache: Arc<GcpTokenCache>,
}

impl RefreshingGcpCredentialProvider {
    pub fn new(cache: Arc<GcpTokenCache>) -> Self {
        Self { cache }
    }

    pub fn cache(&self) -> &Arc<GcpTokenCache> {
        &self.cache
    }
}

#[async_trait]
impl CredentialProvider for RefreshingGcpCredentialProvider {
    type Credential = GcpCredential;

    async fn get_credential(&self) -> object_store::Result<Arc<Self::Credential>> {
        let snapshot =
            self.cache
                .get_or_refresh()
                .await
                .map_err(|msg| object_store::Error::Generic {
                    store: "GCS",
                    source: Box::new(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        msg,
                    )),
                })?;
        Ok(Arc::new(GcpCredential {
            bearer: snapshot.access_token.clone(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    async fn spawn_mock_oauth_server(
        expires_in_secs: Arc<AtomicU64>,
    ) -> (String, Arc<AtomicUsize>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let c = counter_clone.clone();
                let exp = expires_in_secs.clone();
                tokio::spawn(async move {
                    let mut buf = [0u8; 4096];
                    let _ = stream.read(&mut buf).await;
                    let req_str = String::from_utf8_lossy(&buf);
                    let call_no = c.fetch_add(1, Ordering::SeqCst) + 1;
                    let body = if req_str.contains(":generateAccessToken") {
                        format!(
                            r#"{{"accessToken":"mock-oauth-token-imp-{call_no}","expireTime":"2099-01-01T00:00:00Z"}}"#
                        )
                    } else {
                        let ttl = exp.load(Ordering::SeqCst);
                        format!(
                            r#"{{"access_token":"mock-oauth-token-{call_no}","expires_in":{ttl},"token_type":"Bearer"}}"#
                        )
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        });

        (format!("http://127.0.0.1:{}", addr.port()), counter)
    }

    #[tokio::test]
    async fn test_proactive_skew_and_single_flight_refresh() {
        let expires_in = Arc::new(AtomicU64::new(3600));
        let (base_url, calls) = spawn_mock_oauth_server(expires_in.clone()).await;

        let cache = Arc::new(GcpTokenCache::new(
            GcpAuthSource::AuthorizedUser {
                client_id: "cid".to_string(),
                client_secret: "csec".to_string(),
                refresh_token: "rtok".to_string(),
                quota_project_id: Some("proj-1".to_string()),
                token_uri: format!("{base_url}/token"),
            },
            DEFAULT_REFRESH_SKEW_MS,
        ));

        let mut handles = Vec::new();
        for _ in 0..16 {
            let c = cache.clone();
            handles.push(tokio::spawn(
                async move { c.get_or_refresh().await.unwrap() },
            ));
        }
        for h in handles {
            let snap = h.await.unwrap();
            assert_eq!(snap.access_token, "mock-oauth-token-1");
            assert_eq!(snap.generation, 1);
            assert_eq!(snap.quota_project_id.as_deref(), Some("proj-1"));
        }
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        expires_in.store(240, Ordering::SeqCst);
        cache.invalidate(1);
        cache.invalidate(1);

        let snap2 = cache.get_or_refresh().await.unwrap();
        assert_eq!(snap2.access_token, "mock-oauth-token-2");
        assert_eq!(snap2.generation, 2);
        assert_eq!(calls.load(Ordering::SeqCst), 2);

        let snap3 = cache.get_or_refresh().await.unwrap();
        assert_eq!(snap3.access_token, "mock-oauth-token-3");
        assert_eq!(snap3.generation, 3);
        assert_eq!(calls.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_impersonated_service_account_refresh() {
        let expires_in = Arc::new(AtomicU64::new(3600));
        let (base_url, calls) = spawn_mock_oauth_server(expires_in).await;

        let cache = Arc::new(GcpTokenCache::new(
            GcpAuthSource::ImpersonatedServiceAccount {
                base: Box::new(GcpAuthSource::MetadataServer {
                    service_account: "default".to_string(),
                    quota_project_id: None,
                    endpoint: base_url.clone(),
                }),
                target_service_account: "sa@proj.iam.gserviceaccount.com".to_string(),
                iam_endpoint: base_url,
                scopes: vec!["https://www.googleapis.com/auth/cloud-platform".to_string()],
                quota_project_id: Some("proj".to_string()),
            },
            DEFAULT_REFRESH_SKEW_MS,
        ));

        let provider = RefreshingGcpCredentialProvider::new(cache);
        let cred = provider.get_credential().await.unwrap();
        assert_eq!(cred.bearer, "mock-oauth-token-imp-2");
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }
}
