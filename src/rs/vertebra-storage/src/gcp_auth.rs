//! Google Cloud Storage OAuth2 scopes and `RefreshingGcpCredentialProvider` over `SharedGcpTokenCacheHandle`.

use std::sync::Arc;

use async_trait::async_trait;
use object_store::CredentialProvider;
use object_store::gcp::GcpCredential;
use vertebra_core::gcp_auth::{GcpTokenCache, SharedGcpTokenCacheHandle};

pub const GCS_OAUTH_SCOPES: &[&str] = &[
    "https://www.googleapis.com/auth/devstorage.full_control",
    "https://www.googleapis.com/auth/cloud-platform",
];

#[derive(Debug, Clone)]
pub struct RefreshingGcpCredentialProvider {
    cache: SharedGcpTokenCacheHandle,
}

impl RefreshingGcpCredentialProvider {
    pub fn new(cache: SharedGcpTokenCacheHandle) -> Self {
        Self { cache }
    }

    pub fn from_arc(cache: Arc<GcpTokenCache>) -> Self {
        Self {
            cache: SharedGcpTokenCacheHandle::from_arc(cache),
        }
    }

    pub fn cache(&self) -> &SharedGcpTokenCacheHandle {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use vertebra_core::gcp_auth::{DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, ProxyPolicy};

    #[tokio::test]
    async fn test_refreshing_gcp_credential_provider_get_credential() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let c = Arc::clone(&counter_clone);
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let _ = stream.read(&mut buf).await;
                    let call_no = c.fetch_add(1, Ordering::SeqCst) + 1;
                    let body = format!(
                        r#"{{"access_token":"tok_{call_no}","expires_in":3600,"token_type":"Bearer"}}"#
                    );
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                });
            }
        });

        let cache = Arc::new(GcpTokenCache::with_proxy_policy(
            GcpAuthSource::AuthorizedUser {
                client_id: "cid".to_string(),
                client_secret: "csec".to_string(),
                refresh_token: "rtok".to_string(),
                quota_project_id: Some("proj-1".to_string()),
                token_uri: format!("http://127.0.0.1:{}/token", addr.port()),
            },
            DEFAULT_REFRESH_SKEW_MS,
            ProxyPolicy::Direct,
        ));

        let provider = RefreshingGcpCredentialProvider::from_arc(Arc::clone(&cache));
        assert_eq!(
            provider.cache().quota_project_id().as_deref(),
            Some("proj-1")
        );

        let cred = provider.get_credential().await.expect("get_credential");
        assert_eq!(cred.bearer, "tok_1");
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        cache.invalidate(1);
        let refreshed = provider
            .get_credential()
            .await
            .expect("refreshed get_credential");
        assert_eq!(refreshed.bearer, "tok_2");
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_refreshing_gcp_credential_provider_propagates_auth_error() {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind loopback");
        let addr = listener.local_addr().expect("local addr");

        tokio::spawn(async move {
            if let Ok((mut stream, _)) = listener.accept().await {
                let mut buf = [0u8; 1024];
                let _ = stream.read(&mut buf).await;
                let body = r#"{"error":"invalid_grant"}"#;
                let resp = format!(
                    "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            }
        });

        let cache = Arc::new(GcpTokenCache::with_proxy_policy(
            GcpAuthSource::AuthorizedUser {
                client_id: "cid".to_string(),
                client_secret: "csec".to_string(),
                refresh_token: "bad".to_string(),
                quota_project_id: None,
                token_uri: format!("http://127.0.0.1:{}/token", addr.port()),
            },
            DEFAULT_REFRESH_SKEW_MS,
            ProxyPolicy::Direct,
        ));
        let provider = RefreshingGcpCredentialProvider::from_arc(cache);
        let err = provider
            .get_credential()
            .await
            .expect_err("403 must fail get_credential");
        assert!(matches!(
            err,
            object_store::Error::Generic { store: "GCS", .. }
        ));
    }
}
