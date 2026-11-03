use std::sync::Arc;

use google_cloud_auth::credentials::{CacheableResource, Credentials};
use http::{Extensions, HeaderMap};
use vertebra_core::gcp_auth::{GcpAuthSource, GcpTokenCache};
use vertebra_db::gcp::{GcpTokenCredentials, install_rustls_ring_provider, official_credentials};

fn static_cache(token: &str, quota_project: Option<&str>) -> Arc<GcpTokenCache> {
    Arc::new(GcpTokenCache::new(
        GcpAuthSource::Static {
            access_token: token.to_string(),
            expires_at_unix_ms: None,
            quota_project_id: quota_project.map(str::to_string),
        },
        60_000,
    ))
}

async fn header_map(credentials: Credentials) -> HeaderMap {
    match credentials
        .headers(Extensions::new())
        .await
        .expect("headers")
    {
        CacheableResource::New { data, .. } => data,
        CacheableResource::NotModified => panic!("expected new headers"),
    }
}

#[tokio::test]
async fn gcp_token_credentials_emit_bearer_and_quota_headers() {
    let headers =
        header_map(GcpTokenCredentials::new(static_cache("tok-123", Some("billing-proj"))).into())
            .await;
    assert_eq!(headers.get("authorization").unwrap(), "Bearer tok-123");
    assert_eq!(headers.get("x-goog-user-project").unwrap(), "billing-proj");
}

#[tokio::test]
async fn gcp_token_credentials_omit_quota_header_without_project() {
    let headers = header_map(GcpTokenCredentials::new(static_cache("tok-456", None)).into()).await;
    assert_eq!(headers.get("authorization").unwrap(), "Bearer tok-456");
    assert!(headers.get("x-goog-user-project").is_none());
}

#[tokio::test]
async fn official_credentials_without_cache_are_anonymous() {
    let headers = header_map(official_credentials(None)).await;
    assert!(headers.get("authorization").is_none());
}

#[test]
fn install_rustls_ring_provider_is_idempotent() {
    install_rustls_ring_provider();
    install_rustls_ring_provider();
    assert!(rustls::crypto::CryptoProvider::get_default().is_some());
}
