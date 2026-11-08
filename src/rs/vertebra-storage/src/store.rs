//! Multi-backend `ObjectStoreEngine` wrapping Apache `object_store`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{
    Attribute, AttributeValue, Attributes, ClientOptions, GetOptions, MultipartUpload, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    StaticCredentialProvider, UpdateVersion,
};
use thiserror::Error;
use url::Url;
use vertebra_core::gcp_auth::{GcpTokenCache, SharedGcpTokenCacheHandle};

use crate::oci::{OciCavageCredentials, OciObjectStore};
use crate::r2::{R2_UNIFORM_PART_SIZE, UniformPartAccumulator, requires_uniform_part_size};

static STORAGE_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let default_threads = std::thread::available_parallelism()
        .map(|n| n.get().clamp(4, 16))
        .unwrap_or(4);
    let worker_threads = std::env::var("VERTEBRA_STORAGE_IO_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(default_threads);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("failed to initialize vertebra-storage Tokio runtime")
});

pub fn runtime_handle() -> tokio::runtime::Handle {
    tokio::runtime::Handle::try_current().unwrap_or_else(|_| STORAGE_RUNTIME.handle().clone())
}

pub fn block_on_storage<F>(future: F) -> F::Output
where
    F: std::future::Future + Send,
    F::Output: Send,
{
    if tokio::runtime::Handle::try_current().is_ok() {
        return std::thread::scope(|s| {
            s.spawn(|| STORAGE_RUNTIME.block_on(future))
                .join()
                .expect("storage runtime thread panicked")
        });
    }
    STORAGE_RUNTIME.block_on(future)
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object already exists at '{path}': {message}")]
    AlreadyExists { path: String, message: String },
    #[error("precondition failed for '{path}': {message}")]
    Precondition { path: String, message: String },
    #[error("invalid argument: {0}")]
    InvalidInput(String),
    #[error("object_store error: {0}")]
    ObjectStore(object_store::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid storage URL '{0}': {1}")]
    InvalidUrl(String, String),
    #[error("signing error: {0}")]
    Signing(String),
}

pub type ObjectStoreError = StorageError;

impl From<object_store::Error> for StorageError {
    fn from(err: object_store::Error) -> Self {
        match err {
            object_store::Error::AlreadyExists { path, source } => Self::AlreadyExists {
                path,
                message: source.to_string(),
            },
            object_store::Error::Precondition { path, source } => Self::Precondition {
                path,
                message: source.to_string(),
            },
            other => Self::ObjectStore(other),
        }
    }
}

impl StorageError {
    pub fn is_already_exists(&self) -> bool {
        matches!(
            self,
            Self::AlreadyExists { .. }
                | Self::ObjectStore(object_store::Error::AlreadyExists { .. })
        )
    }

    pub fn is_precondition(&self) -> bool {
        matches!(
            self,
            Self::Precondition { .. } | Self::ObjectStore(object_store::Error::Precondition { .. })
        )
    }
}

fn reject_traversal_segments(raw: &str) -> Result<(), StorageError> {
    for seg in raw.split(['/', '\\']) {
        if seg == ".."
            || seg.eq_ignore_ascii_case("%2e%2e")
            || seg.eq_ignore_ascii_case(".%2e")
            || seg.eq_ignore_ascii_case("%2e.")
        {
            return Err(StorageError::InvalidInput(format!(
                "invalid storage path '{raw}': parent traversal '..' is not allowed"
            )));
        }
    }
    Ok(())
}

fn validate_object_path(path: &str) -> Result<StorePath, StorageError> {
    reject_traversal_segments(path)?;
    let trimmed = path.trim_matches('/');
    if trimmed.is_empty() {
        return Err(StorageError::InvalidInput(
            "object path must not be empty".to_string(),
        ));
    }
    Ok(StorePath::from(trimmed))
}

fn validate_optional_prefix(prefix: Option<&str>) -> Result<Option<StorePath>, StorageError> {
    match prefix {
        None => Ok(None),
        Some(p) => {
            reject_traversal_segments(p)?;
            let trimmed = p.trim_matches('/');
            if trimmed.is_empty() {
                Ok(None)
            } else {
                Ok(Some(StorePath::from(trimmed)))
            }
        }
    }
}

fn build_store_attributes(
    content_type: Option<&str>,
    user_attributes: &HashMap<String, String>,
) -> Attributes {
    let mut attrs = Attributes::new();
    if let Some(ct) = content_type {
        attrs.insert(Attribute::ContentType, AttributeValue::from(ct.to_string()));
    }
    for (k, v) in user_attributes {
        attrs.insert(
            Attribute::Metadata(k.clone().into()),
            AttributeValue::from(v.clone()),
        );
    }
    attrs
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObjectMeta {
    pub path: String,
    pub size: u64,
    pub last_modified: String,
    pub etag: Option<String>,
    pub version: Option<String>,
    pub content_type: Option<String>,
    pub attributes: HashMap<String, String>,
}

impl From<object_store::ObjectMeta> for StoredObjectMeta {
    fn from(value: object_store::ObjectMeta) -> Self {
        Self {
            path: value.location.to_string(),
            size: value.size,
            last_modified: value.last_modified.to_rfc3339(),
            etag: value.e_tag,
            version: value.version,
            content_type: None,
            attributes: HashMap::new(),
        }
    }
}

impl StoredObjectMeta {
    pub fn from_meta_and_attributes(value: object_store::ObjectMeta, attrs: &Attributes) -> Self {
        let mut content_type = None;
        let mut attributes = HashMap::new();
        for (key, val) in attrs.iter() {
            match key {
                Attribute::ContentType => {
                    content_type = Some(val.as_ref().to_string());
                }
                Attribute::Metadata(name) => {
                    attributes.insert(name.as_ref().to_string(), val.as_ref().to_string());
                }
                _ => {}
            }
        }
        Self {
            path: value.location.to_string(),
            size: value.size,
            last_modified: value.last_modified.to_rfc3339(),
            etag: value.e_tag,
            version: value.version,
            content_type,
            attributes,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutConditionalOptions {
    pub if_not_exists: bool,
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub content_type: Option<String>,
    pub attributes: HashMap<String, String>,
}

/// Stateful incremental multipart upload session.
///
/// S3, Cloudflare R2, and OCI chunks pass through a rolling 8 MiB `UniformPartAccumulator`.
/// This satisfies S3's non-trailing 5 MiB minimum and R2/OCI part uniformity without
/// buffering the entire payload in memory. B2 URLs normalize to the S3 backend.
pub struct MultipartUploadSession {
    path: String,
    upload: Option<Box<dyn MultipartUpload>>,
    accumulator: Option<UniformPartAccumulator>,
    total_bytes: u64,
    flushed_part_sizes: Vec<usize>,
    content_type: Option<String>,
    attributes: HashMap<String, String>,
    had_error: bool,
}

impl MultipartUploadSession {
    pub fn part_sizes_flushed(&self) -> &[usize] {
        &self.flushed_part_sizes
    }

    pub async fn write_chunk(&mut self, chunk: Bytes) -> Result<(), ObjectStoreError> {
        let upload = self.upload.as_mut().ok_or_else(|| {
            StorageError::InvalidInput("multipart upload session is already closed".to_string())
        })?;
        if chunk.is_empty() {
            return Ok(());
        }
        self.total_bytes += chunk.len() as u64;
        if let Some(ref mut acc) = self.accumulator {
            for part in acc.push(chunk) {
                let part_len = part.len();
                if let Err(err) = upload.put_part(PutPayload::from(part)).await {
                    self.had_error = true;
                    return Err(err.into());
                }
                self.flushed_part_sizes.push(part_len);
            }
        } else {
            let part_len = chunk.len();
            if let Err(err) = upload.put_part(PutPayload::from(chunk)).await {
                self.had_error = true;
                return Err(err.into());
            }
            self.flushed_part_sizes.push(part_len);
        }
        Ok(())
    }

    pub async fn complete(&mut self) -> Result<PutResult, ObjectStoreError> {
        let mut upload = self.upload.take().ok_or_else(|| {
            StorageError::InvalidInput("multipart upload session is already closed".to_string())
        })?;
        if let Some(ref mut acc) = self.accumulator
            && let Some(tail) = acc.finish()
        {
            let tail_len = tail.len();
            if let Err(err) = upload.put_part(PutPayload::from(tail)).await {
                self.had_error = true;
                self.upload = Some(upload);
                return Err(err.into());
            }
            self.flushed_part_sizes.push(tail_len);
        }
        if self.flushed_part_sizes.is_empty() {
            if let Err(err) = upload.put_part(PutPayload::new()).await {
                self.had_error = true;
                self.upload = Some(upload);
                return Err(err.into());
            }
            self.flushed_part_sizes.push(0);
        }
        match upload.complete().await {
            Ok(result) => Ok(result),
            Err(err) => {
                self.had_error = true;
                Err(err.into())
            }
        }
    }

    pub async fn complete_with_meta(&mut self) -> Result<StoredObjectMeta, ObjectStoreError> {
        let result = self.complete().await?;
        Ok(StoredObjectMeta {
            path: self.path.clone(),
            size: self.total_bytes,
            last_modified: chrono::Utc::now().to_rfc3339(),
            etag: result.e_tag,
            version: result.version,
            content_type: self.content_type.clone(),
            attributes: self.attributes.clone(),
        })
    }

    pub async fn abort(&mut self) -> Result<(), ObjectStoreError> {
        if let Some(ref mut acc) = self.accumulator {
            acc.clear();
        }
        let Some(mut upload) = self.upload.take() else {
            return Ok(());
        };
        match upload.abort().await {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(_) if self.had_error => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

fn is_loopback_endpoint(endpoint: Option<&str>) -> bool {
    let Some(ep) = endpoint else {
        return false;
    };
    let Ok(parsed) = Url::parse(ep) else {
        return false;
    };
    matches!(
        parsed.host_str(),
        Some("127.0.0.1" | "localhost" | "::1" | "[::1]")
    )
}

const LOOPBACK_PROXY_EXCLUDES: &str = "127.0.0.1,localhost,::1,metadata.google.internal";

fn ambient_proxy_url() -> Option<String> {
    for key in [
        "HTTPS_PROXY",
        "https_proxy",
        "HTTP_PROXY",
        "http_proxy",
        "ALL_PROXY",
        "all_proxy",
    ] {
        if let Ok(val) = std::env::var(key) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return Some(trimmed.to_string());
            }
        }
    }
    None
}

fn build_http_client_options(
    allow_http: bool,
    endpoint: Option<&str>,
    default_headers: Option<http::HeaderMap>,
) -> ClientOptions {
    let mut opts = ClientOptions::new()
        .with_pool_max_idle_per_host(64)
        .with_pool_idle_timeout(Duration::from_secs(90))
        .with_connect_timeout(Duration::from_secs(10))
        .with_timeout(Duration::from_secs(300))
        .with_http2_keep_alive_interval(Duration::from_secs(30))
        .with_allow_http(allow_http);
    if allow_http || is_loopback_endpoint(endpoint) {
        opts = opts.with_proxy_excludes(LOOPBACK_PROXY_EXCLUDES);
        if let Some(proxy_url) = ambient_proxy_url() {
            opts = opts.with_proxy_url(proxy_url);
        }
    }
    if let Some(headers) = default_headers
        && !headers.is_empty()
    {
        opts = opts.with_default_headers(headers);
    }
    opts
}

#[derive(Clone, Debug)]
pub enum BackendConfig {
    Memory,
    Local {
        root: String,
    },
    S3 {
        bucket: String,
        region: Option<String>,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        endpoint: Option<String>,
        allow_http: bool,
    },
    Gcs {
        bucket: String,
        service_account_path: Option<String>,
        service_account_key: Option<String>,
        application_credentials_path: Option<String>,
        access_token: Option<String>,
        credential_cache: Option<SharedGcpTokenCacheHandle>,
        quota_project_id: Option<String>,
        endpoint: Option<String>,
        allow_http: bool,
        skip_signature: bool,
        allow_ambient_adc: bool,
    },
    Azure {
        container: String,
        account: Option<String>,
        access_key: Option<String>,
        endpoint: Option<String>,
        allow_http: bool,
    },
    Oci {
        bucket: String,
        namespace: String,
        region: String,
        endpoint: Option<String>,
        credentials: Option<OciCavageCredentials>,
    },
    R2 {
        bucket: String,
        account_id: String,
        access_key_id: Option<String>,
        secret_access_key: Option<String>,
        jurisdiction: Option<String>,
        endpoint: Option<String>,
        allow_http: bool,
    },
    Http {
        url: String,
        allow_http: bool,
    },
}

#[derive(Clone)]
pub struct ObjectStoreEngine {
    inner: Arc<dyn ObjectStore>,
    scheme: String,
}

impl ObjectStoreEngine {
    pub fn new(config: BackendConfig) -> Result<Self, StorageError> {
        match config {
            BackendConfig::Memory => Ok(Self {
                inner: Arc::new(InMemory::new()),
                scheme: "memory".to_string(),
            }),
            BackendConfig::Local { root } => {
                std::fs::create_dir_all(&root).map_err(|e| {
                    StorageError::InvalidUrl(
                        root.clone(),
                        format!("failed creating directory: {e}"),
                    )
                })?;
                let store = LocalFileSystem::new_with_prefix(&root)?;
                Ok(Self {
                    inner: Arc::new(store),
                    scheme: "file".to_string(),
                })
            }
            BackendConfig::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                endpoint,
                allow_http,
            } => {
                let client_opts = build_http_client_options(allow_http, endpoint.as_deref(), None);
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(&bucket)
                    .with_allow_http(allow_http)
                    .with_client_options(client_opts);
                if let Some(region_val) = region {
                    builder = builder.with_region(region_val);
                }
                if let Some(key_id) = access_key_id {
                    builder = builder.with_access_key_id(key_id);
                }
                if let Some(secret) = secret_access_key {
                    builder = builder.with_secret_access_key(secret);
                }
                if let Some(ep) = endpoint {
                    builder = builder.with_endpoint(ep);
                }
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    scheme: "s3".to_string(),
                })
            }
            BackendConfig::Gcs {
                bucket,
                service_account_path,
                service_account_key,
                application_credentials_path,
                access_token,
                credential_cache,
                quota_project_id,
                endpoint,
                allow_http,
                skip_signature,
                allow_ambient_adc,
            } => {
                let has_explicit_creds = service_account_path.is_some()
                    || service_account_key.is_some()
                    || application_credentials_path.is_some()
                    || access_token.is_some()
                    || credential_cache.is_some();
                let mut builder = if !allow_ambient_adc || skip_signature || has_explicit_creds {
                    GoogleCloudStorageBuilder::new().with_bucket_name(&bucket)
                } else {
                    GoogleCloudStorageBuilder::from_env().with_bucket_name(&bucket)
                };

                let effective_quota_project = quota_project_id
                    .as_deref()
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned)
                    .or_else(|| {
                        credential_cache
                            .as_ref()
                            .and_then(SharedGcpTokenCacheHandle::quota_project_id)
                            .filter(|s| !s.trim().is_empty())
                    });
                let mut default_headers = http::HeaderMap::new();
                if let Some(qp) = effective_quota_project {
                    let header_val = http::HeaderValue::from_str(qp.trim()).map_err(|e| {
                        StorageError::InvalidInput(format!(
                            "invalid quota_project_id header value: {e}"
                        ))
                    })?;
                    default_headers.insert(
                        http::header::HeaderName::from_static("x-goog-user-project"),
                        header_val,
                    );
                }

                if let Some(sa_path) = service_account_path {
                    builder = builder.with_service_account_path(sa_path);
                }
                if let Some(sa_key) = service_account_key {
                    builder = builder.with_service_account_key(sa_key);
                }
                if let Some(cache) = credential_cache {
                    let creds =
                        Arc::new(crate::gcp_auth::RefreshingGcpCredentialProvider::new(cache));
                    builder = builder.with_credentials(creds);
                } else {
                    if let Some(adc_path) = application_credentials_path {
                        builder = builder.with_application_credentials(adc_path);
                    }
                    if let Some(token) = access_token {
                        let creds = Arc::new(StaticCredentialProvider::new(GcpCredential {
                            bearer: token,
                        }));
                        builder = builder.with_credentials(creds);
                    } else if skip_signature && !has_explicit_creds {
                        let creds = Arc::new(StaticCredentialProvider::new(GcpCredential {
                            bearer: String::new(),
                        }));
                        builder = builder.with_credentials(creds);
                    }
                }
                if let Some(ref ep) = endpoint {
                    builder = builder.with_base_url(ep);
                }
                builder = builder.with_client_options(build_http_client_options(
                    allow_http,
                    endpoint.as_deref(),
                    Some(default_headers),
                ));
                if skip_signature {
                    builder = builder.with_skip_signature(true);
                }
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    scheme: "gs".to_string(),
                })
            }
            BackendConfig::Azure {
                container,
                account,
                access_key,
                endpoint,
                allow_http,
            } => {
                let client_opts = build_http_client_options(allow_http, endpoint.as_deref(), None);
                let mut builder = MicrosoftAzureBuilder::new()
                    .with_container_name(&container)
                    .with_allow_http(allow_http)
                    .with_client_options(client_opts);
                if let Some(acc) = account {
                    builder = builder.with_account(acc);
                }
                if let Some(key) = access_key {
                    builder = builder.with_access_key(key);
                }
                if let Some(ep) = endpoint {
                    builder = builder.with_endpoint(ep);
                }
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    scheme: "az".to_string(),
                })
            }
            BackendConfig::Oci {
                bucket,
                namespace,
                region,
                endpoint,
                credentials,
            } => {
                let store = OciObjectStore::new(namespace, bucket, region, endpoint, credentials)?;
                Ok(Self {
                    inner: Arc::new(store),
                    scheme: "oci".to_string(),
                })
            }
            BackendConfig::R2 {
                bucket,
                account_id,
                access_key_id,
                secret_access_key,
                jurisdiction,
                endpoint,
                allow_http,
            } => {
                let resolved_endpoint = endpoint.unwrap_or_else(|| match jurisdiction.as_deref() {
                    Some(j) if !j.trim().is_empty() => {
                        format!(
                            "https://{}.{}.r2.cloudflarestorage.com",
                            account_id,
                            j.trim()
                        )
                    }
                    _ => format!("https://{}.r2.cloudflarestorage.com", account_id),
                });
                let client_opts =
                    build_http_client_options(allow_http, Some(resolved_endpoint.as_str()), None);
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(&bucket)
                    .with_region("auto")
                    .with_endpoint(resolved_endpoint)
                    .with_allow_http(allow_http)
                    .with_client_options(client_opts);
                if let Some(key_id) = access_key_id {
                    builder = builder.with_access_key_id(key_id);
                }
                if let Some(secret) = secret_access_key {
                    builder = builder.with_secret_access_key(secret);
                }
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    scheme: "r2".to_string(),
                })
            }
            BackendConfig::Http { url, allow_http } => {
                let client_opts = build_http_client_options(allow_http, Some(url.as_str()), None);
                let store = HttpBuilder::new()
                    .with_url(url)
                    .with_client_options(client_opts)
                    .build()?;
                Ok(Self {
                    inner: Arc::new(store),
                    scheme: "http".to_string(),
                })
            }
        }
    }

    pub fn from_url(
        url_str: &str,
        options: Option<&HashMap<String, String>>,
    ) -> Result<Self, StorageError> {
        Self::from_url_with_cache(url_str, options.cloned(), None)
    }

    pub fn from_url_with_cache(
        url_str: &str,
        options: Option<HashMap<String, String>>,
        external_token_cache: Option<SharedGcpTokenCacheHandle>,
    ) -> Result<Self, StorageError> {
        if url_str == "memory://" || url_str == "memory:" {
            return Self::new(BackendConfig::Memory);
        }
        let parsed = Url::parse(url_str)
            .map_err(|e| StorageError::InvalidUrl(url_str.to_string(), e.to_string()))?;
        let opts = options.unwrap_or_default();

        match parsed.scheme() {
            "memory" => Self::new(BackendConfig::Memory),
            "file" => {
                let path = parsed.to_file_path().map_err(|()| {
                    StorageError::InvalidUrl(
                        url_str.to_string(),
                        "cannot convert file:// URL to local path".to_string(),
                    )
                })?;
                Self::new(BackendConfig::Local {
                    root: path.to_string_lossy().into_owned(),
                })
            }
            "s3" | "b2" => {
                let bucket = parsed.host_str().unwrap_or_default().to_string();
                let endpoint = if parsed.scheme() == "b2" {
                    let region = opts
                        .get("region")
                        .cloned()
                        .unwrap_or_else(|| "us-west-004".to_string());
                    Some(
                        opts.get("endpoint")
                            .cloned()
                            .unwrap_or_else(|| format!("https://s3.{region}.backblazeb2.com")),
                    )
                } else {
                    opts.get("endpoint").cloned()
                };
                Self::new(BackendConfig::S3 {
                    bucket,
                    region: opts
                        .get("region")
                        .cloned()
                        .or_else(|| Some("us-east-1".to_string())),
                    access_key_id: opts.get("access_key_id").cloned(),
                    secret_access_key: opts.get("secret_access_key").cloned(),
                    endpoint,
                    allow_http: opts.get("allow_http").is_some_and(|v| v == "true"),
                })
            }
            "r2" => {
                let bucket = parsed.host_str().unwrap_or_default().to_string();
                let account_id = opts.get("account_id").cloned().unwrap_or_default();
                Self::new(BackendConfig::R2 {
                    bucket,
                    account_id,
                    access_key_id: opts.get("access_key_id").cloned(),
                    secret_access_key: opts.get("secret_access_key").cloned(),
                    jurisdiction: opts.get("jurisdiction").cloned(),
                    endpoint: opts.get("endpoint").cloned(),
                    allow_http: opts.get("allow_http").is_some_and(|v| v == "true"),
                })
            }
            "oci" => {
                let raw_host = parsed.host_str().unwrap_or_default();
                let (bucket, host_ns) = if !parsed.username().is_empty() {
                    (parsed.username().to_string(), Some(raw_host.to_string()))
                } else {
                    match raw_host.split_once('@') {
                        Some((b, ns)) => (b.to_string(), Some(ns.to_string())),
                        None => (raw_host.to_string(), None),
                    }
                };
                let namespace = opts
                    .get("namespace")
                    .cloned()
                    .or(host_ns)
                    .unwrap_or_else(|| "default_ns".to_string());
                let region = opts
                    .get("region")
                    .cloned()
                    .unwrap_or_else(|| "us-ashburn-1".to_string());
                let credentials = match (
                    opts.get("tenancy_ocid"),
                    opts.get("user_ocid"),
                    opts.get("fingerprint"),
                    opts.get("private_key_pem"),
                ) {
                    (Some(t), Some(u), Some(fp), Some(pem)) => Some(OciCavageCredentials {
                        tenancy_ocid: t.clone(),
                        user_ocid: u.clone(),
                        fingerprint: fp.clone(),
                        private_key_pem: pem.clone(),
                    }),
                    _ => None,
                };
                Self::new(BackendConfig::Oci {
                    bucket,
                    namespace,
                    region,
                    endpoint: opts.get("endpoint").cloned(),
                    credentials,
                })
            }
            "gs" | "gcs" => {
                let bucket = parsed.host_str().unwrap_or_default().to_string();
                let skip_signature = opts.get("skip_signature").is_some_and(|v| v == "true");
                let allow_ambient_adc = !opts
                    .get("allow_ambient_adc")
                    .is_some_and(|v| v.eq_ignore_ascii_case("false") || v == "0");
                let credential_cache = external_token_cache.or_else(|| {
                    if skip_signature {
                        None
                    } else {
                        GcpTokenCache::from_options(&opts, crate::gcp_auth::GCS_OAUTH_SCOPES)
                            .map(SharedGcpTokenCacheHandle::from_arc)
                    }
                });
                Self::new(BackendConfig::Gcs {
                    bucket,
                    service_account_path: opts.get("service_account_path").cloned(),
                    service_account_key: opts.get("service_account_key").cloned(),
                    application_credentials_path: opts
                        .get("application_credentials_path")
                        .or_else(|| opts.get("application_credentials"))
                        .cloned(),
                    access_token: opts
                        .get("access_token")
                        .or_else(|| opts.get("bearer_token"))
                        .cloned(),
                    credential_cache,
                    quota_project_id: opts.get("quota_project_id").cloned(),
                    endpoint: opts
                        .get("endpoint")
                        .or_else(|| opts.get("base_url"))
                        .cloned(),
                    allow_http: opts.get("allow_http").is_some_and(|v| v == "true"),
                    skip_signature,
                    allow_ambient_adc,
                })
            }
            "az" | "azure" | "abfs" => {
                let container = parsed.host_str().unwrap_or_default().to_string();
                let endpoint = opts.get("endpoint").cloned();
                let allow_http = opts.get("allow_http").is_some_and(|v| v == "true")
                    || endpoint
                        .as_deref()
                        .is_some_and(|ep| ep.starts_with("http://"));
                Self::new(BackendConfig::Azure {
                    container,
                    account: opts.get("account").cloned(),
                    access_key: opts.get("access_key").cloned(),
                    endpoint,
                    allow_http,
                })
            }
            "http" | "https" => Self::new(BackendConfig::Http {
                url: url_str.to_string(),
                allow_http: parsed.scheme() == "http"
                    || opts.get("allow_http").is_some_and(|v| v == "true"),
            }),
            other => Err(StorageError::InvalidUrl(
                url_str.to_string(),
                format!("unsupported storage URL scheme '{other}'"),
            )),
        }
    }

    pub fn with_store(inner: Arc<dyn ObjectStore>, scheme: impl Into<String>) -> Self {
        Self {
            inner,
            scheme: scheme.into(),
        }
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub async fn put(&self, path: &str, data: Bytes) -> Result<StoredObjectMeta, StorageError> {
        self.put_opts(path, data, PutConditionalOptions::default())
            .await
    }

    pub async fn put_opts(
        &self,
        path: &str,
        data: Bytes,
        opts: PutConditionalOptions,
    ) -> Result<StoredObjectMeta, ObjectStoreError> {
        let location = validate_object_path(path)?;
        let create_only = opts.if_not_exists || opts.if_none_match.as_deref() == Some("*");
        if create_only && opts.if_match.is_some() {
            return Err(StorageError::InvalidInput(
                "cannot combine if_not_exists or if_none_match='*' with if_match".to_string(),
            ));
        }

        let mode = if create_only {
            PutMode::Create
        } else if let Some(ref etag) = opts.if_match {
            if self.scheme == "gs" {
                let trimmed = etag.trim();
                if !trimmed.is_empty() && trimmed.bytes().all(|b| b.is_ascii_digit()) {
                    PutMode::Update(UpdateVersion {
                        e_tag: None,
                        version: Some(trimmed.to_string()),
                    })
                } else {
                    match self.inner.head(&location).await {
                        Ok(existing) if existing.e_tag.as_deref() == Some(etag.as_str()) => {
                            let Some(ver) = existing.version else {
                                return Err(StorageError::Precondition {
                                    path: location.to_string(),
                                    message: "GCS object has no generation version for conditional update".to_string(),
                                });
                            };
                            PutMode::Update(UpdateVersion {
                                e_tag: None,
                                version: Some(ver),
                            })
                        }
                        Ok(existing) => {
                            return Err(StorageError::Precondition {
                                path: location.to_string(),
                                message: format!(
                                    "ETag mismatch: expected {etag:?}, got {:?}",
                                    existing.e_tag
                                ),
                            });
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            return Err(StorageError::Precondition {
                                path: location.to_string(),
                                message: "object does not exist for if_match update".to_string(),
                            });
                        }
                        Err(err) => return Err(err.into()),
                    }
                }
            } else {
                PutMode::Update(UpdateVersion {
                    e_tag: Some(etag.clone()),
                    version: None,
                })
            }
        } else if let Some(ref none_etag) = opts.if_none_match {
            match self.inner.head(&location).await {
                Ok(existing) if existing.e_tag.as_deref() == Some(none_etag.as_str()) => {
                    return Err(StorageError::Precondition {
                        path: location.to_string(),
                        message: format!("If-None-Match matched existing ETag {none_etag}"),
                    });
                }
                Ok(_) | Err(object_store::Error::NotFound { .. }) => PutMode::Overwrite,
                Err(err) => return Err(err.into()),
            }
        } else {
            PutMode::Overwrite
        };

        let size = data.len() as u64;
        let store_attrs = build_store_attributes(opts.content_type.as_deref(), &opts.attributes);
        let put_options = PutOptions {
            mode: mode.clone(),
            attributes: store_attrs,
            ..Default::default()
        };

        let result = match self
            .inner
            .put_opts(&location, PutPayload::from(data.clone()), put_options)
            .await
        {
            Ok(res) => res,
            Err(object_store::Error::NotImplemented { .. }) if self.scheme == "file" => {
                if let PutMode::Update(ref uv) = mode {
                    match self.inner.head(&location).await {
                        Ok(existing) if existing.e_tag == uv.e_tag => {
                            self.inner.put(&location, PutPayload::from(data)).await?
                        }
                        Ok(existing) => {
                            return Err(StorageError::Precondition {
                                path: location.to_string(),
                                message: format!(
                                    "ETag mismatch: expected {:?}, got {:?}",
                                    uv.e_tag, existing.e_tag
                                ),
                            });
                        }
                        Err(object_store::Error::NotFound { .. }) => {
                            return Err(StorageError::Precondition {
                                path: location.to_string(),
                                message: "object does not exist for if_match update".to_string(),
                            });
                        }
                        Err(err) => return Err(err.into()),
                    }
                } else {
                    self.inner
                        .put_opts(
                            &location,
                            PutPayload::from(data),
                            PutOptions {
                                mode,
                                ..Default::default()
                            },
                        )
                        .await?
                }
            }
            Err(err) => return Err(err.into()),
        };

        Ok(StoredObjectMeta {
            path: location.to_string(),
            size,
            last_modified: chrono::Utc::now().to_rfc3339(),
            etag: result.e_tag,
            version: result.version,
            content_type: opts.content_type,
            attributes: opts.attributes,
        })
    }

    pub async fn put_file(
        &self,
        path: &str,
        file_path: &str,
    ) -> Result<StoredObjectMeta, StorageError> {
        let _ = validate_object_path(path)?;
        let raw = tokio::fs::read(file_path).await?;
        self.put(path, Bytes::from(raw)).await
    }

    pub async fn create_multipart(
        &self,
        path: &str,
        opts: PutConditionalOptions,
    ) -> Result<MultipartUploadSession, ObjectStoreError> {
        let location = validate_object_path(path)?;
        let create_only = opts.if_not_exists || opts.if_none_match.as_deref() == Some("*");
        if create_only && opts.if_match.is_some() {
            return Err(StorageError::InvalidInput(
                "cannot combine if_not_exists or if_none_match='*' with if_match".to_string(),
            ));
        }

        if create_only {
            if self.exists(path).await? {
                return Err(StorageError::AlreadyExists {
                    path: location.to_string(),
                    message: "object already exists".to_string(),
                });
            }
        } else if let Some(ref expected_etag) = opts.if_match {
            match self.inner.head(&location).await {
                Ok(existing) if existing.e_tag.as_deref() == Some(expected_etag.as_str()) => {}
                Ok(existing) => {
                    return Err(StorageError::Precondition {
                        path: location.to_string(),
                        message: format!(
                            "ETag mismatch: expected {expected_etag}, got {:?}",
                            existing.e_tag
                        ),
                    });
                }
                Err(object_store::Error::NotFound { .. }) => {
                    return Err(StorageError::Precondition {
                        path: location.to_string(),
                        message: "object does not exist for if_match update".to_string(),
                    });
                }
                Err(err) => return Err(err.into()),
            }
        } else if let Some(ref none_etag) = opts.if_none_match {
            match self.inner.head(&location).await {
                Ok(existing) if existing.e_tag.as_deref() == Some(none_etag.as_str()) => {
                    return Err(StorageError::Precondition {
                        path: location.to_string(),
                        message: format!("If-None-Match matched existing ETag {none_etag}"),
                    });
                }
                Ok(_) | Err(object_store::Error::NotFound { .. }) => {}
                Err(err) => return Err(err.into()),
            }
        }

        let mp_opts = PutMultipartOptions {
            attributes: build_store_attributes(opts.content_type.as_deref(), &opts.attributes),
            ..Default::default()
        };
        let upload = match self.inner.put_multipart_opts(&location, mp_opts).await {
            Ok(up) => up,
            Err(object_store::Error::NotImplemented { .. }) if self.scheme == "file" => {
                self.inner.put_multipart(&location).await?
            }
            Err(err) => return Err(err.into()),
        };

        let accumulator = if requires_uniform_part_size(&self.scheme) {
            Some(UniformPartAccumulator::new(R2_UNIFORM_PART_SIZE))
        } else {
            None
        };

        Ok(MultipartUploadSession {
            path: location.to_string(),
            upload: Some(upload),
            accumulator,
            total_bytes: 0,
            flushed_part_sizes: Vec::new(),
            content_type: opts.content_type,
            attributes: opts.attributes,
            had_error: false,
        })
    }

    pub async fn put_multipart(
        &self,
        path: &str,
        parts: Vec<Bytes>,
    ) -> Result<StoredObjectMeta, StorageError> {
        let mut session = self
            .create_multipart(path, PutConditionalOptions::default())
            .await?;
        for chunk in parts {
            if let Err(err) = session.write_chunk(chunk).await {
                let _ = session.abort().await;
                return Err(err);
            }
        }
        session.complete_with_meta().await
    }

    pub async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        let location = validate_object_path(path)?;
        let result = self.inner.get(&location).await?;
        Ok(result.bytes().await?)
    }

    pub async fn get_stream(
        &self,
        path: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, ObjectStoreError>>, ObjectStoreError> {
        let location = validate_object_path(path)?;
        let result = self.inner.get(&location).await?;
        Ok(result.into_stream().map_err(StorageError::from).boxed())
    }

    pub async fn get_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, StorageError> {
        let location = validate_object_path(path)?;
        let start = offset;
        let end = offset + length;
        Ok(self.inner.get_range(&location, start..end).await?)
    }

    pub async fn head(&self, path: &str) -> Result<StoredObjectMeta, StorageError> {
        let location = validate_object_path(path)?;
        let result = self
            .inner
            .get_opts(
                &location,
                GetOptions {
                    head: true,
                    ..Default::default()
                },
            )
            .await?;
        Ok(StoredObjectMeta::from_meta_and_attributes(
            result.meta,
            &result.attributes,
        ))
    }

    pub async fn exists(&self, path: &str) -> Result<bool, StorageError> {
        let location = validate_object_path(path)?;
        match self.inner.head(&location).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(err.into()),
        }
    }

    pub async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let location = validate_object_path(path)?;
        self.inner.delete(&location).await?;
        Ok(())
    }

    pub async fn copy(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        let from = validate_object_path(src)?;
        let to = validate_object_path(dst)?;
        self.inner.copy(&from, &to).await?;
        Ok(())
    }

    pub async fn rename(
        &self,
        src: &str,
        dst: &str,
        if_not_exists: bool,
    ) -> Result<(), ObjectStoreError> {
        let from = validate_object_path(src)?;
        let to = validate_object_path(dst)?;
        if if_not_exists {
            self.inner.rename_if_not_exists(&from, &to).await?;
        } else {
            self.inner.rename(&from, &to).await?;
        }
        Ok(())
    }

    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<StoredObjectMeta>, StorageError> {
        let store_prefix = validate_optional_prefix(prefix)?;
        let stream = self.inner.list(store_prefix.as_ref());
        let metas: Vec<object_store::ObjectMeta> = stream.try_collect().await?;
        let mut items: Vec<StoredObjectMeta> =
            metas.into_iter().map(StoredObjectMeta::from).collect();
        items.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(items)
    }

    pub async fn list_page(
        &self,
        prefix: Option<&str>,
        offset: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<StoredObjectMeta>, Option<String>), ObjectStoreError> {
        if limit == 0 {
            return Err(StorageError::InvalidInput("limit must be >= 1".to_string()));
        }
        let store_prefix = validate_optional_prefix(prefix)?;
        let offset_path = match offset {
            Some(o) if !o.is_empty() => Some(validate_object_path(o)?),
            _ => None,
        };

        if self.scheme == "file" {
            let mut all = self.list(prefix).await?;
            if let Some(ref off) = offset_path {
                let off_str = off.as_ref();
                all.retain(|item| item.path.as_str() > off_str);
            }
            let next_cursor = if all.len() > limit {
                all.truncate(limit);
                all.last().map(|m| m.path.clone())
            } else {
                None
            };
            return Ok((all, next_cursor));
        }

        let mut stream = match offset_path.as_ref() {
            Some(off) => self.inner.list_with_offset(store_prefix.as_ref(), off),
            None => self.inner.list(store_prefix.as_ref()),
        };
        let mut page = Vec::with_capacity(limit.min(1024) + 1);
        while page.len() <= limit {
            match stream.try_next().await? {
                Some(meta) => page.push(StoredObjectMeta::from(meta)),
                None => break,
            }
        }
        let next_cursor = if page.len() > limit {
            page.truncate(limit);
            page.last().map(|m| m.path.clone())
        } else {
            None
        };
        Ok((page, next_cursor))
    }

    pub async fn list_with_delimiter(
        &self,
        prefix: Option<&str>,
    ) -> Result<(Vec<StoredObjectMeta>, Vec<String>), ObjectStoreError> {
        let store_prefix = validate_optional_prefix(prefix)?;
        let res = self
            .inner
            .list_with_delimiter(store_prefix.as_ref())
            .await?;
        let mut objects: Vec<StoredObjectMeta> = res
            .objects
            .into_iter()
            .map(StoredObjectMeta::from)
            .collect();
        objects.sort_by(|a, b| a.path.cmp(&b.path));
        let mut prefixes: Vec<String> = res
            .common_prefixes
            .into_iter()
            .map(|p| p.as_ref().trim_end_matches('/').to_string())
            .collect();
        prefixes.sort();
        prefixes.dedup();
        Ok((objects, prefixes))
    }
}
