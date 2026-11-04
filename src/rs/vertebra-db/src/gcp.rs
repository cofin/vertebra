//! Wiring between Vertebra's `GcpTokenCache` and the official Google Cloud Rust clients.

use std::sync::Arc;

use google_cloud_auth::credentials::{
    CacheableResource, Credentials, CredentialsProvider, EntityTag, anonymous,
};
use google_cloud_auth::errors::CredentialsError;
use http::header::{AUTHORIZATION, HeaderName, HeaderValue};
use http::{Extensions, HeaderMap};
use vertebra_core::gcp_auth::GcpTokenCache;

const QUOTA_PROJECT_HEADER: HeaderName = HeaderName::from_static("x-goog-user-project");

pub use crate::connection::install_rustls_ring_provider;

/// `google-cloud-auth` credentials backed by one shared [`GcpTokenCache`].
#[derive(Clone, Debug)]
pub struct GcpTokenCredentials {
    cache: Arc<GcpTokenCache>,
}

impl GcpTokenCredentials {
    /// Wrap `cache` so official clients reuse its tokens and refresh policy.
    pub fn new(cache: Arc<GcpTokenCache>) -> Self {
        Self { cache }
    }
}

impl CredentialsProvider for GcpTokenCredentials {
    async fn headers(
        &self,
        _extensions: Extensions,
    ) -> Result<CacheableResource<HeaderMap>, CredentialsError> {
        let snapshot = self
            .cache
            .get_or_refresh()
            .await
            .map_err(|message| CredentialsError::from_msg(true, message))?;
        let mut headers = HeaderMap::with_capacity(2);
        let bearer = HeaderValue::from_str(&format!("Bearer {}", snapshot.access_token))
            .map_err(|err| CredentialsError::from_source(false, err))?;
        headers.insert(AUTHORIZATION, bearer);
        if let Some(project) = snapshot
            .quota_project_id
            .as_deref()
            .filter(|p| !p.is_empty())
        {
            let value = HeaderValue::from_str(project)
                .map_err(|err| CredentialsError::from_source(false, err))?;
            headers.insert(QUOTA_PROJECT_HEADER, value);
        }
        Ok(CacheableResource::New {
            entity_tag: EntityTag::new(),
            data: headers,
        })
    }

    async fn universe_domain(&self) -> Option<String> {
        None
    }
}

/// Return credentials for an official client: the shared cache when present, anonymous otherwise.
pub fn official_credentials(cache: Option<Arc<GcpTokenCache>>) -> Credentials {
    match cache {
        Some(cache) => GcpTokenCredentials::new(cache).into(),
        None => anonymous::Builder::new().build(),
    }
}
