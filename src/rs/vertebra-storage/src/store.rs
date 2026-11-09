//! Multi-backend `ObjectStoreEngine` wrapping Apache `object_store`.

use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::{GcpCredential, GcpCredentialProvider, GoogleCloudStorageBuilder};
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{
    Attribute, AttributeValue, Attributes, ClientOptions, GetOptions, MultipartUpload, ObjectStore,
    ObjectStoreExt, PutMode, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    StaticCredentialProvider, UpdateVersion,
};
use serde::Deserialize;
use thiserror::Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

/// Files larger than this stream through multipart uploads in chunks of the same size.
const FILE_TRANSFER_CHUNK_SIZE: usize = 8 * 1024 * 1024;

/// Maximum number of source objects accepted by one GCS JSON `compose` call.
const GCS_COMPOSE_MAX_SOURCES: usize = 32;

const DEFAULT_GCS_BASE_URL: &str = "https://storage.googleapis.com";

static PART_FILE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Process-wide sequence that makes GCS compose temporary names unique per call.
static COMPOSE_SEQ: AtomicU64 = AtomicU64::new(0);

static GCS_LOOPBACK_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap_or_default()
});

static GCS_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(300))
        .build()
        .unwrap_or_default()
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
    StorePath::parse(trimmed)
        .map_err(|e| StorageError::InvalidInput(format!("invalid object path '{path}': {e}")))
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
                StorePath::parse(trimmed)
                    .map(Some)
                    .map_err(|e| StorageError::InvalidInput(format!("invalid prefix '{p}': {e}")))
            }
        }
    }
}

type LocalMetadataEntry = (Option<String>, HashMap<String, String>);
type LocalMetadataRegistry = Arc<RwLock<HashMap<String, LocalMetadataEntry>>>;
type BatchOutcome = (String, Option<Result<StoredObjectMeta, StorageError>>);

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
    local_metadata: Option<LocalMetadataRegistry>,
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

    /// Flush any buffered tail and commit the upload.
    ///
    /// A session whose earlier part upload failed never commits: it returns an error and keeps
    /// the upload so `abort` can release it. The upload stays owned by the session until the
    /// commit succeeds, so a failed or cancelled commit can still be aborted.
    pub async fn complete(&mut self) -> Result<PutResult, ObjectStoreError> {
        let upload = self.upload.as_mut().ok_or_else(|| {
            StorageError::InvalidInput("multipart upload session is already closed".to_string())
        })?;
        if self.had_error {
            return Err(StorageError::InvalidInput(format!(
                "multipart upload for '{}' had a failed part and cannot be completed; abort it",
                self.path
            )));
        }
        if let Some(ref mut acc) = self.accumulator
            && let Some(tail) = acc.finish()
        {
            let tail_len = tail.len();
            if let Err(err) = upload.put_part(PutPayload::from(tail)).await {
                self.had_error = true;
                return Err(err.into());
            }
            self.flushed_part_sizes.push(tail_len);
        }
        if self.flushed_part_sizes.is_empty() {
            if let Err(err) = upload.put_part(PutPayload::new()).await {
                self.had_error = true;
                return Err(err.into());
            }
            self.flushed_part_sizes.push(0);
        }
        match upload.complete().await {
            Ok(result) => {
                self.upload = None;
                if let Some(ref registry) = self.local_metadata {
                    let mut guard = registry.write().unwrap_or_else(|e| e.into_inner());
                    if self.content_type.is_some() || !self.attributes.is_empty() {
                        guard.insert(
                            self.path.clone(),
                            (self.content_type.clone(), self.attributes.clone()),
                        );
                    } else {
                        guard.remove(&self.path);
                    }
                }
                Ok(result)
            }
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
        let Some(upload) = self.upload.as_mut() else {
            return Ok(());
        };
        let outcome = upload.abort().await;
        self.upload = None;
        match outcome {
            Ok(()) => Ok(()),
            Err(object_store::Error::NotFound { .. }) => Ok(()),
            Err(_) if self.had_error => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

/// Abort an upload that was neither completed nor aborted before the session is dropped.
///
/// Cancelling a Python awaitable aborts the Tokio task and drops its future mid-`.await`, so
/// explicit error-path aborts never run. The abort is detached onto the storage runtime
/// because `Drop` cannot await.
impl Drop for MultipartUploadSession {
    fn drop(&mut self) {
        if let Some(mut upload) = self.upload.take() {
            drop(runtime_handle().spawn(async move {
                let _ = upload.abort().await;
            }));
        }
    }
}

/// Unlink a `.part` download file when dropped unless disarmed after the final rename.
struct PartFileGuard {
    path: PathBuf,
    armed: bool,
}

impl Drop for PartFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Unlink batch download destinations created by one `get_files` call unless disarmed.
///
/// Only destinations that did not exist before the call are recorded, so pre-existing files
/// that were successfully replaced stay replaced.
struct CreatedFilesGuard {
    created: std::sync::Mutex<Vec<PathBuf>>,
    armed: bool,
}

impl CreatedFilesGuard {
    fn record(&self, path: &str) {
        self.created
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(PathBuf::from(path));
    }
}

impl Drop for CreatedFilesGuard {
    fn drop(&mut self) {
        if self.armed {
            let created = self.created.get_mut().unwrap_or_else(|e| e.into_inner());
            for path in created.iter() {
                let _ = std::fs::remove_file(path);
            }
        }
    }
}

/// Delete GCS compose temporaries on a detached runtime task when dropped with entries left.
///
/// The happy and error paths delete temporaries inline and then clear the list; entries only
/// remain when the compose future is dropped mid-flight.
struct ComposeTemporariesGuard {
    store: Arc<dyn ObjectStore>,
    temporaries: Vec<String>,
}

impl Drop for ComposeTemporariesGuard {
    fn drop(&mut self) {
        if self.temporaries.is_empty() {
            return;
        }
        let store = Arc::clone(&self.store);
        let temporaries = std::mem::take(&mut self.temporaries);
        drop(runtime_handle().spawn(async move {
            let deletions = temporaries.iter().map(|temp| {
                let store = &store;
                async move {
                    if let Ok(location) = StorePath::parse(temp) {
                        let _ = store.delete(&location).await;
                    }
                }
            });
            futures::future::join_all(deletions).await;
        }));
    }
}

fn resolve_s3_endpoint(
    endpoint: Option<String>,
    bucket: &str,
    virtual_hosted_style: bool,
) -> Result<Option<String>, StorageError> {
    let Some(raw) = endpoint else {
        return Ok(None);
    };
    if !virtual_hosted_style {
        return Ok(Some(raw));
    }
    let mut parsed = Url::parse(&raw)
        .map_err(|e| StorageError::InvalidUrl(raw.clone(), format!("invalid S3 endpoint: {e}")))?;
    if matches!(parsed.host(), Some(url::Host::Ipv4(_) | url::Host::Ipv6(_))) {
        return Err(StorageError::InvalidInput(format!(
            "virtual_hosted_style requires a DNS endpoint host; IP-literal endpoint `{raw}` cannot carry a bucket subdomain"
        )));
    }
    if let Some(url::Host::Domain(domain)) = parsed.host() {
        let bucket_dot = format!("{bucket}.");
        if !domain.starts_with(&bucket_dot) {
            let new_host = format!("{bucket}.{domain}");
            parsed.set_host(Some(&new_host)).map_err(|e| {
                StorageError::InvalidUrl(raw, format!("invalid virtual-hosted S3 host: {e}"))
            })?;
        }
    }
    Ok(Some(parsed.to_string().trim_end_matches('/').to_string()))
}

fn is_loopback_endpoint(endpoint: Option<&str>) -> bool {
    let Some(ep) = endpoint else {
        return false;
    };
    let Ok(parsed) = Url::parse(ep) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]") || host.ends_with(".localhost")
}

const LOOPBACK_PROXY_EXCLUDES: &str = "127.0.0.1,localhost,.localhost,::1,metadata.google.internal";

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

fn merged_loopback_proxy_excludes() -> String {
    for key in ["NO_PROXY", "no_proxy"] {
        if let Ok(val) = std::env::var(key) {
            let trimmed = val.trim();
            if !trimmed.is_empty() {
                return format!("{trimmed},{LOOPBACK_PROXY_EXCLUDES}");
            }
        }
    }
    LOOPBACK_PROXY_EXCLUDES.to_string()
}

fn build_http_client_options(
    allow_http: bool,
    endpoint: Option<&str>,
    default_headers: Option<http::HeaderMap>,
    explicit_proxy_url: Option<&str>,
) -> ClientOptions {
    let mut opts = ClientOptions::new()
        .with_pool_max_idle_per_host(64)
        .with_pool_idle_timeout(Duration::from_secs(90))
        .with_connect_timeout(Duration::from_secs(10))
        .with_timeout(Duration::from_secs(300))
        .with_http2_keep_alive_interval(Duration::from_secs(30))
        .with_allow_http(allow_http);
    if let Some(proxy_url) = explicit_proxy_url.map(str::trim).filter(|s| !s.is_empty()) {
        opts = opts.with_proxy_url(proxy_url);
    } else if is_loopback_endpoint(endpoint) {
        opts = opts.with_proxy_excludes(merged_loopback_proxy_excludes());
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
        session_token: Option<String>,
        virtual_hosted_style: bool,
        endpoint: Option<String>,
        proxy_url: Option<String>,
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
        headers: HashMap<String, String>,
    },
}

fn parse_http_headers_from_options(
    opts: &HashMap<String, String>,
) -> Result<HashMap<String, String>, StorageError> {
    let mut headers = HashMap::new();
    if let Some(raw_json) = opts.get("headers") {
        let parsed: HashMap<String, String> = serde_json::from_str(raw_json).map_err(|e| {
            StorageError::InvalidInput(format!("invalid Http headers JSON `{raw_json}`: {e}"))
        })?;
        headers.extend(parsed);
    }
    for (k, v) in opts {
        if k == "allow_http" || k == "headers" {
            continue;
        }
        let header_name = k
            .strip_prefix("header.")
            .or_else(|| k.strip_prefix("http_header."))
            .unwrap_or(k.as_str());
        if !header_name.trim().is_empty() {
            headers.insert(header_name.trim().to_string(), v.clone());
        }
    }
    Ok(headers)
}

/// Connection and credential state for GCS JSON API calls that `object_store` does not expose.
///
/// `credential_cache` is preferred because its snapshots carry a generation that can be
/// invalidated after a `401`; `credentials` is the provider `object_store` resolved for every
/// other credential source (static token, service account, ADC). `skip_signature` sends no
/// `Authorization` header.
#[derive(Clone, Debug)]
struct GcsComposeContext {
    bucket: String,
    base_url: String,
    credential_cache: Option<SharedGcpTokenCacheHandle>,
    credentials: Option<GcpCredentialProvider>,
    quota_project_id: Option<String>,
    skip_signature: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GcsComposeResponse {
    name: Option<String>,
    size: Option<String>,
    etag: Option<String>,
    generation: Option<String>,
    content_type: Option<String>,
    updated: Option<String>,
}

#[derive(Clone)]
pub struct ObjectStoreEngine {
    inner: Arc<dyn ObjectStore>,
    scheme: String,
    local_metadata: Option<LocalMetadataRegistry>,
    gcs_compose: Option<Arc<GcsComposeContext>>,
}

impl ObjectStoreEngine {
    pub fn new(config: BackendConfig) -> Result<Self, StorageError> {
        match config {
            BackendConfig::Memory => Ok(Self {
                inner: Arc::new(InMemory::new()),
                scheme: "memory".to_string(),
                local_metadata: None,
                gcs_compose: None,
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
                    local_metadata: Some(Arc::new(RwLock::new(HashMap::new()))),
                    gcs_compose: None,
                })
            }
            BackendConfig::S3 {
                bucket,
                region,
                access_key_id,
                secret_access_key,
                session_token,
                virtual_hosted_style,
                endpoint,
                proxy_url,
                allow_http,
            } => {
                let resolved_endpoint =
                    resolve_s3_endpoint(endpoint, &bucket, virtual_hosted_style)?;
                let client_opts = build_http_client_options(
                    allow_http,
                    resolved_endpoint.as_deref(),
                    None,
                    proxy_url.as_deref(),
                );
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
                if let Some(tok) = session_token
                    && !tok.trim().is_empty()
                {
                    builder = builder.with_token(tok);
                }
                if virtual_hosted_style {
                    builder = builder.with_virtual_hosted_style_request(true);
                }
                if let Some(ep) = resolved_endpoint {
                    builder = builder.with_endpoint(ep);
                }
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    scheme: "s3".to_string(),
                    local_metadata: None,
                    gcs_compose: None,
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
                if let Some(ref qp) = effective_quota_project {
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

                let compose_cache = credential_cache.clone();
                if let Some(cache) = credential_cache {
                    let creds =
                        Arc::new(crate::gcp_auth::RefreshingGcpCredentialProvider::new(cache));
                    builder = builder.with_credentials(creds);
                } else {
                    if let Some(sa_path) = service_account_path {
                        builder = builder.with_service_account_path(sa_path);
                    }
                    if let Some(sa_key) = service_account_key {
                        builder = builder.with_service_account_key(sa_key);
                    }
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
                    None,
                ));
                if skip_signature {
                    builder = builder.with_skip_signature(true);
                }
                let store = builder.build()?;
                let compose = GcsComposeContext {
                    bucket,
                    base_url: endpoint
                        .as_deref()
                        .map(str::trim)
                        .filter(|ep| !ep.is_empty())
                        .unwrap_or(DEFAULT_GCS_BASE_URL)
                        .trim_end_matches('/')
                        .to_string(),
                    credential_cache: compose_cache,
                    credentials: Some(Arc::clone(store.credentials())),
                    quota_project_id: effective_quota_project
                        .map(|qp| qp.trim().to_string())
                        .filter(|qp| !qp.is_empty()),
                    skip_signature,
                };
                Ok(Self {
                    inner: Arc::new(store),
                    scheme: "gs".to_string(),
                    local_metadata: None,
                    gcs_compose: Some(Arc::new(compose)),
                })
            }
            BackendConfig::Azure {
                container,
                account,
                access_key,
                endpoint,
                allow_http,
            } => {
                let client_opts =
                    build_http_client_options(allow_http, endpoint.as_deref(), None, None);
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
                    local_metadata: None,
                    gcs_compose: None,
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
                    local_metadata: None,
                    gcs_compose: None,
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
                let client_opts = build_http_client_options(
                    allow_http,
                    Some(resolved_endpoint.as_str()),
                    None,
                    None,
                );
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
                    local_metadata: None,
                    gcs_compose: None,
                })
            }
            BackendConfig::Http {
                url,
                allow_http,
                headers,
            } => {
                let mut header_map = http::HeaderMap::new();
                for (name, val) in headers {
                    let hdr_name = http::header::HeaderName::from_bytes(name.trim().as_bytes())
                        .map_err(|e| {
                            StorageError::InvalidInput(format!(
                                "invalid HTTP header name `{name}`: {e}"
                            ))
                        })?;
                    let hdr_val = http::HeaderValue::from_str(&val).map_err(|e| {
                        StorageError::InvalidInput(format!(
                            "invalid HTTP header value for `{name}`: {e}"
                        ))
                    })?;
                    header_map.insert(hdr_name, hdr_val);
                }
                let client_opts = build_http_client_options(
                    allow_http,
                    Some(url.as_str()),
                    Some(header_map),
                    None,
                );
                let store = HttpBuilder::new()
                    .with_url(url)
                    .with_client_options(client_opts)
                    .build()?;
                Ok(Self {
                    inner: Arc::new(store),
                    scheme: "http".to_string(),
                    local_metadata: None,
                    gcs_compose: None,
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
                    session_token: opts
                        .get("session_token")
                        .or_else(|| opts.get("token"))
                        .cloned(),
                    virtual_hosted_style: opts
                        .get("virtual_hosted_style")
                        .is_some_and(|v| v.eq_ignore_ascii_case("true") || v == "1"),
                    endpoint,
                    proxy_url: opts.get("proxy_url").cloned(),
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
                headers: parse_http_headers_from_options(&opts)?,
            }),
            other => Err(StorageError::InvalidUrl(
                url_str.to_string(),
                format!("unsupported storage URL scheme '{other}'"),
            )),
        }
    }

    pub fn with_store(inner: Arc<dyn ObjectStore>, scheme: impl Into<String>) -> Self {
        let scheme = scheme.into();
        let local_metadata = if scheme == "file" {
            Some(Arc::new(RwLock::new(HashMap::new())))
        } else {
            None
        };
        Self {
            inner,
            scheme,
            local_metadata,
            gcs_compose: None,
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

        if let Some(ref registry) = self.local_metadata {
            let mut guard = registry.write().unwrap_or_else(|e| e.into_inner());
            if opts.content_type.is_some() || !opts.attributes.is_empty() {
                guard.insert(
                    location.to_string(),
                    (opts.content_type.clone(), opts.attributes.clone()),
                );
            } else {
                guard.remove(location.as_ref());
            }
        }

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

    /// Upload a local file, streaming files over [`FILE_TRANSFER_CHUNK_SIZE`] as multipart parts.
    ///
    /// Small files take one `put`. Larger files are read in `8 MiB` chunks handed to a
    /// multipart session without buffering the whole file; any read or upload error aborts the
    /// session before the error is returned.
    pub async fn put_file(
        &self,
        path: &str,
        file_path: &str,
    ) -> Result<StoredObjectMeta, StorageError> {
        let _ = validate_object_path(path)?;
        let size = tokio::fs::metadata(file_path).await?.len();
        if size <= FILE_TRANSFER_CHUNK_SIZE as u64 {
            let raw = tokio::fs::read(file_path).await?;
            return self.put(path, Bytes::from(raw)).await;
        }
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut session = self
            .create_multipart(path, PutConditionalOptions::default())
            .await?;
        let uploaded = async {
            Self::stream_file_into_session(&mut file, &mut session).await?;
            session.complete_with_meta().await
        }
        .await;
        if uploaded.is_err() {
            let _ = session.abort().await;
        }
        uploaded
    }

    /// Download an object to `file_path` atomically.
    ///
    /// Chunks stream into a sibling `.{name}.{pid}.{seq}.part` file that is flushed and renamed
    /// over `file_path` only after the full payload arrives, so readers never observe a partial
    /// file. A drop guard unlinks the `.part` file on any error and when the future is cancelled
    /// mid-transfer. Parent directories are created.
    pub async fn get_file(
        &self,
        path: &str,
        file_path: &str,
    ) -> Result<StoredObjectMeta, StorageError> {
        let location = validate_object_path(path)?;
        let target = FsPath::new(file_path);
        let file_name = target.file_name().ok_or_else(|| {
            StorageError::InvalidInput(format!(
                "destination file path '{file_path}' has no file name"
            ))
        })?;
        let parent = target
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .map_or_else(|| PathBuf::from("."), FsPath::to_path_buf);
        let result = self.inner.get(&location).await?;
        let mut meta =
            StoredObjectMeta::from_meta_and_attributes(result.meta.clone(), &result.attributes);
        self.merge_local_metadata(&location, &mut meta);
        tokio::fs::create_dir_all(&parent).await?;
        let mut part = PartFileGuard {
            path: parent.join(format!(
                ".{}.{}.{}.part",
                file_name.to_string_lossy(),
                std::process::id(),
                PART_FILE_SEQ.fetch_add(1, Ordering::Relaxed)
            )),
            armed: true,
        };
        Self::write_stream_to_file(result.into_stream(), &part.path).await?;
        tokio::fs::rename(&part.path, target).await?;
        part.armed = false;
        Ok(meta)
    }

    /// Download `(key, file_path)` pairs with at most `concurrency` transfers in flight.
    ///
    /// Results follow input order. After the first failure no new transfers start. On failure or
    /// cancellation, destinations that did not exist before the call and were written by it are
    /// unlinked; pre-existing destinations that were successfully replaced stay replaced.
    pub async fn get_files(
        &self,
        pairs: Vec<(String, String)>,
        concurrency: usize,
    ) -> Result<Vec<StoredObjectMeta>, StorageError> {
        if concurrency == 0 {
            return Err(StorageError::InvalidInput(
                "concurrency must be >= 1".to_string(),
            ));
        }
        let destinations: Vec<String> = pairs.iter().map(|pair| pair.1.clone()).collect();
        let fresh: std::collections::HashSet<String> = futures::stream::iter(destinations)
            .map(|file_path| async move {
                let existed = tokio::fs::try_exists(&file_path).await.unwrap_or(true);
                (!existed).then_some(file_path)
            })
            .buffer_unordered(concurrency)
            .filter_map(std::future::ready)
            .collect()
            .await;
        let mut created = CreatedFilesGuard {
            created: std::sync::Mutex::new(Vec::new()),
            armed: true,
        };
        let created_ref = &created;
        let fresh_ref = &fresh;
        let outcomes = Self::run_file_batch(pairs, concurrency, |key, file_path| async move {
            let meta = self.get_file(&key, &file_path).await?;
            if fresh_ref.contains(&file_path) {
                created_ref.record(&file_path);
            }
            Ok(meta)
        })
        .await?;
        if !outcomes.iter().any(|(_, o)| matches!(o, Some(Err(_)))) {
            created.armed = false;
        }
        Self::collect_batch_outcomes(outcomes)
    }

    /// Upload `(key, file_path)` pairs with at most `concurrency` transfers in flight.
    ///
    /// Results follow input order. After the first failure no new transfers start and the first
    /// error in input order is returned.
    pub async fn put_files(
        &self,
        pairs: Vec<(String, String)>,
        concurrency: usize,
    ) -> Result<Vec<StoredObjectMeta>, StorageError> {
        let outcomes = Self::run_file_batch(pairs, concurrency, |key, file_path| async move {
            self.put_file(&key, &file_path).await
        })
        .await?;
        Self::collect_batch_outcomes(outcomes)
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
            local_metadata: self.local_metadata.clone(),
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
        let end = offset.checked_add(length).ok_or_else(|| {
            StorageError::InvalidInput("byte range offset + length overflow".to_string())
        })?;
        if length == 0 {
            let _ = self.head(path).await?;
            return Ok(Bytes::new());
        }
        let location = validate_object_path(path)?;
        Ok(self.inner.get_range(&location, offset..end).await?)
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
        let mut meta = StoredObjectMeta::from_meta_and_attributes(result.meta, &result.attributes);
        self.merge_local_metadata(&location, &mut meta);
        Ok(meta)
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
        if let Some(ref registry) = self.local_metadata {
            registry
                .write()
                .unwrap_or_else(|e| e.into_inner())
                .remove(location.as_ref());
        }
        Ok(())
    }

    /// Copy `src` to `dst`; with `if_not_exists`, fail with `AlreadyExists` when `dst` exists.
    ///
    /// Stores without native create-only copy on the `file` scheme fall back to an atomic
    /// create-only put of the source bytes.
    pub async fn copy(
        &self,
        src: &str,
        dst: &str,
        if_not_exists: bool,
    ) -> Result<(), StorageError> {
        let from = validate_object_path(src)?;
        let to = validate_object_path(dst)?;
        if if_not_exists {
            match self.inner.copy_if_not_exists(&from, &to).await {
                Ok(()) => {}
                Err(object_store::Error::NotImplemented { .. }) if self.scheme == "file" => {
                    let payload = self.inner.get(&from).await?.bytes().await?;
                    self.inner
                        .put_opts(
                            &to,
                            PutPayload::from(payload),
                            PutOptions {
                                mode: PutMode::Create,
                                ..Default::default()
                            },
                        )
                        .await?;
                }
                Err(err) => return Err(err.into()),
            }
        } else {
            self.inner.copy(&from, &to).await?;
        }
        if let Some(ref registry) = self.local_metadata {
            let mut guard = registry.write().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = guard.get(from.as_ref()).cloned() {
                guard.insert(to.to_string(), entry);
            } else {
                guard.remove(to.as_ref());
            }
        }
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
        if let Some(ref registry) = self.local_metadata {
            let mut guard = registry.write().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = guard.remove(from.as_ref()) {
                guard.insert(to.to_string(), entry);
            } else if from != to {
                guard.remove(to.as_ref());
            }
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

    /// Concatenate `sources` in order into `dst`.
    ///
    /// On `gs` stores this uses the GCS JSON `compose` API, folding more than
    /// [`GCS_COMPOSE_MAX_SOURCES`] inputs in layers through `{dst}.compose-{unique}-{idx:04}`
    /// temporary objects, where `unique` is the process id plus a per-call sequence in hex.
    /// Temporaries are always deleted afterwards, even when a fold fails or the call is
    /// cancelled. The returned metadata comes from a `head` of `dst` so its `etag` matches
    /// `head` and conditional `put_opts`. Other stores stream each source sequentially into one
    /// multipart upload in 8 MiB parts, aborting it on error or cancellation.
    pub async fn compose(
        &self,
        sources: Vec<String>,
        dst: &str,
        content_type: Option<String>,
    ) -> Result<StoredObjectMeta, StorageError> {
        if sources.is_empty() {
            return Err(StorageError::InvalidInput(
                "compose requires at least one source object".to_string(),
            ));
        }
        let source_keys = sources
            .iter()
            .map(|src| validate_object_path(src).map(|p| p.to_string()))
            .collect::<Result<Vec<_>, _>>()?;
        let dst_key = validate_object_path(dst)?.to_string();
        match self.gcs_compose.clone() {
            Some(ctx) => {
                self.gcs_compose_tree(&ctx, source_keys, &dst_key, content_type)
                    .await
            }
            None => {
                self.multipart_compose(&source_keys, &dst_key, content_type)
                    .await
            }
        }
    }

    fn merge_local_metadata(&self, location: &StorePath, meta: &mut StoredObjectMeta) {
        if let Some(ref registry) = self.local_metadata
            && let Some((ct, attrs)) = registry
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .get(location.as_ref())
                .cloned()
        {
            if meta.content_type.is_none() {
                meta.content_type = ct;
            }
            if meta.attributes.is_empty() {
                meta.attributes = attrs;
            }
        }
    }

    async fn stream_file_into_session(
        file: &mut tokio::fs::File,
        session: &mut MultipartUploadSession,
    ) -> Result<(), StorageError> {
        let mut buf = BytesMut::with_capacity(FILE_TRANSFER_CHUNK_SIZE);
        loop {
            buf.reserve(FILE_TRANSFER_CHUNK_SIZE);
            while buf.len() < FILE_TRANSFER_CHUNK_SIZE {
                let remaining = FILE_TRANSFER_CHUNK_SIZE - buf.len();
                if file.read_buf(&mut (&mut buf).limit(remaining)).await? == 0 {
                    break;
                }
            }
            if buf.is_empty() {
                return Ok(());
            }
            let full = buf.len() == FILE_TRANSFER_CHUNK_SIZE;
            session.write_chunk(buf.split().freeze()).await?;
            if !full {
                return Ok(());
            }
        }
    }

    async fn write_stream_to_file(
        mut stream: BoxStream<'static, object_store::Result<Bytes>>,
        part_path: &FsPath,
    ) -> Result<(), StorageError> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(part_path)
            .await?;
        let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);
        while let Some(chunk) = stream.next().await {
            writer.write_all(&chunk?).await?;
        }
        writer.flush().await?;
        Ok(())
    }

    /// Run `op` over `pairs` with bounded concurrency, returning outcomes in input order.
    ///
    /// Each entry pairs the local file path with `None` when the item was skipped because an
    /// earlier item had already failed.
    async fn run_file_batch<F, Fut>(
        pairs: Vec<(String, String)>,
        concurrency: usize,
        op: F,
    ) -> Result<Vec<BatchOutcome>, StorageError>
    where
        F: Fn(String, String) -> Fut,
        Fut: std::future::Future<Output = Result<StoredObjectMeta, StorageError>>,
    {
        if concurrency == 0 {
            return Err(StorageError::InvalidInput(
                "concurrency must be >= 1".to_string(),
            ));
        }
        let failed = AtomicBool::new(false);
        let failed_ref = &failed;
        let op_ref = &op;
        let mut outcomes: Vec<(usize, BatchOutcome)> =
            futures::stream::iter(pairs.into_iter().enumerate())
                .map(|(idx, (key, file_path))| async move {
                    if failed_ref.load(Ordering::Acquire) {
                        return (idx, (file_path, None));
                    }
                    let outcome = op_ref(key, file_path.clone()).await;
                    if outcome.is_err() {
                        failed_ref.store(true, Ordering::Release);
                    }
                    (idx, (file_path, Some(outcome)))
                })
                .buffer_unordered(concurrency)
                .collect()
                .await;
        outcomes.sort_unstable_by_key(|(idx, _)| *idx);
        Ok(outcomes.into_iter().map(|(_, outcome)| outcome).collect())
    }

    /// Return metadata in input order, or the first error in input order.
    ///
    /// Skipped items only exist after a failure, so dropping them never hides a missing result.
    fn collect_batch_outcomes(
        outcomes: Vec<BatchOutcome>,
    ) -> Result<Vec<StoredObjectMeta>, StorageError> {
        outcomes
            .into_iter()
            .filter_map(|(_, outcome)| outcome)
            .collect()
    }

    /// Stream `sources` sequentially into one multipart upload at `dst`.
    ///
    /// Network chunks are re-framed into [`FILE_TRANSFER_CHUNK_SIZE`] parts so many small source
    /// chunks never become many tiny parts. Errors abort the upload inline; cancellation aborts it
    /// through the session's drop handler.
    async fn multipart_compose(
        &self,
        sources: &[String],
        dst: &str,
        content_type: Option<String>,
    ) -> Result<StoredObjectMeta, StorageError> {
        let mut session = self
            .create_multipart(
                dst,
                PutConditionalOptions {
                    content_type,
                    ..Default::default()
                },
            )
            .await?;
        let mut frames = UniformPartAccumulator::new(FILE_TRANSFER_CHUNK_SIZE);
        let composed = async {
            for src in sources {
                let mut stream = self.get_stream(src).await?;
                while let Some(chunk) = stream.next().await {
                    for frame in frames.push(chunk?) {
                        session.write_chunk(frame).await?;
                    }
                }
            }
            if let Some(tail) = frames.finish() {
                session.write_chunk(tail).await?;
            }
            session.complete_with_meta().await
        }
        .await;
        if composed.is_err() {
            let _ = session.abort().await;
        }
        composed
    }

    async fn gcs_compose_tree(
        &self,
        ctx: &GcsComposeContext,
        sources: Vec<String>,
        dst: &str,
        content_type: Option<String>,
    ) -> Result<StoredObjectMeta, StorageError> {
        let mut guard = ComposeTemporariesGuard {
            store: Arc::clone(&self.inner),
            temporaries: Vec::new(),
        };
        let composed = async {
            let mut level = sources;
            if level.len() > GCS_COMPOSE_MAX_SOURCES {
                let unique = format!(
                    "{:x}{:012x}",
                    std::process::id(),
                    COMPOSE_SEQ.fetch_add(1, Ordering::Relaxed)
                );
                while level.len() > GCS_COMPOSE_MAX_SOURCES {
                    let mut next =
                        Vec::with_capacity(level.len().div_ceil(GCS_COMPOSE_MAX_SOURCES));
                    for group in level.chunks(GCS_COMPOSE_MAX_SOURCES) {
                        if let [single] = group {
                            next.push(single.clone());
                            continue;
                        }
                        let temp = format!("{dst}.compose-{unique}-{:04}", guard.temporaries.len());
                        guard.temporaries.push(temp.clone());
                        Self::gcs_compose_call(ctx, group, &temp, None).await?;
                        next.push(temp);
                    }
                    level = next;
                }
            }
            let response =
                Self::gcs_compose_call(ctx, &level, dst, content_type.as_deref()).await?;
            let mut meta = self.head(dst).await?;
            if meta.content_type.is_none() {
                meta.content_type = response.content_type;
            }
            Ok(meta)
        }
        .await;
        let deletions = guard.temporaries.iter().map(|temp| async move {
            if let Ok(location) = StorePath::parse(temp) {
                let _ = self.inner.delete(&location).await;
            }
        });
        futures::future::join_all(deletions).await;
        guard.temporaries.clear();
        composed
    }

    async fn gcs_compose_call(
        ctx: &GcsComposeContext,
        sources: &[String],
        dst: &str,
        content_type: Option<&str>,
    ) -> Result<StoredObjectMeta, StorageError> {
        let url = format!(
            "{}/storage/v1/b/{}/o/{}/compose",
            ctx.base_url,
            crate::signing::rfc3986_encode(&ctx.bucket, true),
            crate::signing::rfc3986_encode(dst, true)
        );
        let mut destination = serde_json::Map::new();
        if let Some(ct) = content_type {
            destination.insert(
                "contentType".to_string(),
                serde_json::Value::String(ct.to_string()),
            );
        }
        let body = serde_json::json!({
            "sourceObjects": sources
                .iter()
                .map(|name| serde_json::json!({ "name": name }))
                .collect::<Vec<_>>(),
            "destination": destination,
        });
        let client: &reqwest::Client = if is_loopback_endpoint(Some(&ctx.base_url)) {
            &GCS_LOOPBACK_CLIENT
        } else {
            &GCS_HTTP_CLIENT
        };
        let send = |bearer: Option<String>, snapshot_project: Option<String>| {
            let mut req = client.post(&url).json(&body);
            if let Some(token) = bearer.filter(|t| !t.is_empty()) {
                req = req.bearer_auth(token);
            }
            if let Some(project) = ctx.quota_project_id.clone().or(snapshot_project) {
                req = req.header("x-goog-user-project", project);
            }
            req.send()
        };
        let transport_err = |err: reqwest::Error| {
            Self::gcs_generic_error(format!("GCS compose request for '{dst}' failed: {err}"))
        };

        let resp = if ctx.skip_signature {
            send(None, None).await.map_err(transport_err)?
        } else if let Some(ref cache) = ctx.credential_cache {
            let snap = cache
                .get_or_refresh()
                .await
                .map_err(Self::gcs_generic_error)?;
            let first = send(
                Some(snap.access_token.clone()),
                snap.quota_project_id.clone(),
            )
            .await
            .map_err(transport_err)?;
            if first.status() == reqwest::StatusCode::UNAUTHORIZED {
                cache.invalidate(snap.generation);
                let refreshed = cache
                    .get_or_refresh()
                    .await
                    .map_err(Self::gcs_generic_error)?;
                send(
                    Some(refreshed.access_token.clone()),
                    refreshed.quota_project_id.clone(),
                )
                .await
                .map_err(transport_err)?
            } else {
                first
            }
        } else if let Some(ref provider) = ctx.credentials {
            let credential = provider.get_credential().await?;
            send(Some(credential.bearer.clone()), None)
                .await
                .map_err(transport_err)?
        } else {
            send(None, None).await.map_err(transport_err)?
        };

        let status = resp.status();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_default();
            return Err(match status {
                reqwest::StatusCode::NOT_FOUND => {
                    StorageError::ObjectStore(object_store::Error::NotFound {
                        path: dst.to_string(),
                        source: Box::new(std::io::Error::new(
                            std::io::ErrorKind::NotFound,
                            format!("GCS compose source not found: {text}"),
                        )),
                    })
                }
                reqwest::StatusCode::PRECONDITION_FAILED => StorageError::Precondition {
                    path: dst.to_string(),
                    message: text,
                },
                other => Self::gcs_generic_error(format!(
                    "GCS compose for '{dst}' returned HTTP {other}: {text}"
                )),
            });
        }
        let parsed: GcsComposeResponse = resp.json().await.map_err(|err| {
            Self::gcs_generic_error(format!("invalid GCS compose response for '{dst}': {err}"))
        })?;
        let size = parsed
            .size
            .as_deref()
            .map(|s| {
                s.parse::<u64>().map_err(|err| {
                    Self::gcs_generic_error(format!("invalid GCS compose size '{s}': {err}"))
                })
            })
            .transpose()?
            .unwrap_or(0);
        Ok(StoredObjectMeta {
            path: parsed.name.unwrap_or_else(|| dst.to_string()),
            size,
            last_modified: parsed
                .updated
                .unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
            etag: parsed.etag,
            version: parsed.generation,
            content_type: parsed
                .content_type
                .or_else(|| content_type.map(str::to_string)),
            attributes: HashMap::new(),
        })
    }

    fn gcs_generic_error(message: String) -> StorageError {
        StorageError::ObjectStore(object_store::Error::Generic {
            store: "GCS",
            source: message.into(),
        })
    }
}
