//! Active GCP Application Default Credentials (ADC) token cache and `RefreshingGcpCredentialProvider`.

use std::sync::Arc;

use async_trait::async_trait;
use object_store::CredentialProvider;
use object_store::gcp::GcpCredential;

pub use vertebra_core::gcp_auth::{
    DEFAULT_GCP_SCOPES, DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache, GcpTokenSnapshot,
    ServiceAccountKey, current_unix_ms, discover_adc_file_path, extract_adc_service_account_key,
    parse_service_account_key_json,
};

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
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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
                    scopes: Vec::new(),
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
