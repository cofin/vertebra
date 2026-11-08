//! Multi-backend `ObjectStoreEngine` wrapping Apache `object_store`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex};

use bytes::Bytes;
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::client::{HttpClient, HttpConnector, ReqwestConnector};
use object_store::gcp::{GoogleCloudStorage, GoogleCloudStorageBuilder, GoogleConfigKey};
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{
    Attribute, AttributeValue, Attributes, GetOptions, MultipartUpload, ObjectStore,
    ObjectStoreExt, PutMode, PutOptions, PutPayload, PutPayloadMut, PutResult, UpdateVersion,
};
use thiserror::Error;
use url::Url;
use vertebra_core::gcp_auth::GcpTokenCache;

use crate::conditional::{GcsStageConfig, PublicationBackend, PublishCondition};

use crate::oci::{
    OCI_UNIFORM_PART_SIZE, OciCavageCredentials, OciObjectStore, UniformPartAccumulator,
};

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

pub(crate) fn validate_object_path(path: &str) -> Result<StorePath, StorageError> {
    reject_traversal_segments(path)?;
    if path.is_empty()
        || path.trim() != path
        || path.starts_with('/')
        || path.ends_with('/')
        || path.contains('\\')
    {
        return Err(StorageError::InvalidInput(format!(
            "invalid relative object path '{path}'"
        )));
    }
    StorePath::parse(path).map_err(|error| StorageError::InvalidInput(error.to_string()))
}

fn validate_optional_prefix(prefix: Option<&str>) -> Result<Option<StorePath>, StorageError> {
    match prefix {
        None | Some("") | Some("/") => Ok(None),
        Some(prefix) => {
            reject_traversal_segments(prefix)?;
            if prefix.trim() != prefix
                || prefix.starts_with('/')
                || prefix.contains("//")
                || prefix.contains('\\')
            {
                return Err(StorageError::InvalidInput(format!(
                    "invalid relative object prefix '{prefix}'"
                )));
            }
            StorePath::parse(prefix.strip_suffix('/').unwrap_or(prefix))
                .map(Some)
                .map_err(|error| StorageError::InvalidInput(error.to_string()))
        }
    }
}

pub(crate) fn build_store_attributes(
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

#[derive(Debug)]
struct CapturingConnector(Arc<Mutex<Option<object_store::ClientOptions>>>);

impl HttpConnector for CapturingConnector {
    fn connect(&self, options: &object_store::ClientOptions) -> object_store::Result<HttpClient> {
        *self.0.lock().map_err(|_| object_store::Error::Generic {
            store: "GCS",
            source: "GCS client configuration capture unavailable".into(),
        })? = Some(options.clone());
        ReqwestConnector::default().connect(options)
    }
}

pub(crate) fn build_gcs_store(
    builder: GoogleCloudStorageBuilder,
) -> Result<(Arc<GoogleCloudStorage>, GcsStageConfig), StorageError> {
    #[derive(serde::Deserialize)]
    struct Endpoint {
        gcs_base_url: Option<String>,
    }
    let base = builder.get_config_value(&GoogleConfigKey::BaseUrl);
    let bucket = builder.get_config_value(&GoogleConfigKey::Bucket);
    let skip_signature = builder.get_config_value(&GoogleConfigKey::SkipSignature);
    let path = builder.get_config_value(&GoogleConfigKey::ServiceAccount);
    let key = builder.get_config_value(&GoogleConfigKey::ServiceAccountKey);
    let endpoint = (|| -> Result<Option<Endpoint>, StorageError> {
        let data = match (path, key) {
            (Some(path), None) if base.is_none() => Some(std::fs::read(path).map_err(|_| {
                StorageError::InvalidInput("GCS endpoint metadata unavailable".into())
            })?),
            (None, Some(key)) if base.is_none() => Some(key.into_bytes()),
            _ => None,
        };
        data.map(|data| {
            serde_json::from_slice(&data)
                .map_err(|_| StorageError::InvalidInput("invalid GCS endpoint metadata".into()))
        })
        .transpose()
    })();
    let captured = Arc::new(Mutex::new(None));
    let store = Arc::new(
        builder
            .with_http_connector(CapturingConnector(captured.clone()))
            .build()?,
    );
    let endpoint = endpoint?;
    let base = base
        .or_else(|| endpoint.and_then(|value| value.gcs_base_url))
        .unwrap_or_else(|| "https://storage.googleapis.com".into());
    let base =
        Url::parse(&base).map_err(|_| StorageError::InvalidInput("invalid GCS endpoint".into()))?;
    let client_options = captured
        .lock()
        .map_err(|_| {
            StorageError::InvalidInput("GCS client configuration capture unavailable".into())
        })?
        .take()
        .ok_or_else(|| StorageError::InvalidInput("missing GCS client configuration".into()))?;
    let skip_signature = skip_signature.is_some_and(|value| {
        matches!(
            value.to_ascii_lowercase().as_str(),
            "1" | "true" | "on" | "yes" | "y"
        )
    });
    Ok((
        store,
        GcsStageConfig {
            base,
            bucket: bucket
                .ok_or_else(|| StorageError::InvalidInput("missing GCS bucket".into()))?,
            client_options,
            skip_signature,
        },
    ))
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
/// S3 chunks use bounded 8 MiB segmented frames. When targeting OCI, chunks are
/// normalized through a rolling 8 MiB `UniformPartAccumulator` so every non-trailing part has
/// identical byte length without buffering the entire payload in memory.
pub struct MultipartUploadSession {
    path: String,
    upload: Option<Box<dyn MultipartUpload>>,
    accumulator: Option<UniformPartAccumulator>,
    s3_pending: Option<PutPayloadMut>,
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

    pub async fn write_chunk(&mut self, mut chunk: Bytes) -> Result<(), ObjectStoreError> {
        let upload = self.upload.as_mut().ok_or_else(|| {
            StorageError::InvalidInput("multipart upload session is already closed".to_string())
        })?;
        if chunk.is_empty() {
            return Ok(());
        }
        self.total_bytes += chunk.len() as u64;
        if let Some(ref mut pending) = self.s3_pending {
            while !chunk.is_empty() {
                let length = (8 * 1024 * 1024 - pending.content_length()).min(chunk.len());
                pending.push(chunk.split_to(length));
                if pending.content_length() == 8 * 1024 * 1024 {
                    if self.flushed_part_sizes.len() >= 10_000 {
                        self.had_error = true;
                        return Err(StorageError::InvalidInput(
                            "S3 multipart exceeds 10000 parts".into(),
                        ));
                    }
                    let payload = std::mem::take(pending).freeze();
                    if let Err(err) = upload.put_part(payload).await {
                        self.had_error = true;
                        return Err(err.into());
                    }
                    self.flushed_part_sizes.push(8 * 1024 * 1024);
                }
            }
        } else if let Some(ref mut acc) = self.accumulator {
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
        let upload = self.upload.as_mut().ok_or_else(|| {
            StorageError::InvalidInput("multipart upload session is already closed".to_string())
        })?;
        if let Some(ref mut pending) = self.s3_pending
            && !pending.is_empty()
        {
            if self.flushed_part_sizes.len() >= 10_000 {
                self.had_error = true;
                return Err(StorageError::InvalidInput(
                    "S3 multipart exceeds 10000 parts".into(),
                ));
            }
            let payload = std::mem::take(pending).freeze();
            let length = payload.content_length();
            if let Err(err) = upload.put_part(payload).await {
                self.had_error = true;
                return Err(err.into());
            }
            self.flushed_part_sizes.push(length);
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
        if let Some(ref mut pending) = self.s3_pending {
            *pending = PutPayloadMut::new();
        }
        if let Some(ref mut acc) = self.accumulator {
            acc.clear();
        }
        let Some(upload) = self.upload.as_mut() else {
            return Ok(());
        };
        match upload.abort().await {
            Ok(()) | Err(object_store::Error::NotFound { .. }) => {
                self.upload = None;
                Ok(())
            }
            Err(_) if self.had_error => Ok(()),
            Err(err) => Err(err.into()),
        }
    }
}

impl Drop for MultipartUploadSession {
    fn drop(&mut self) {
        if let Some(mut upload) = self.upload.take() {
            STORAGE_RUNTIME.spawn(async move {
                let _ =
                    tokio::time::timeout(std::time::Duration::from_secs(30), upload.abort()).await;
            });
        }
    }
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
        credential_cache: Option<Arc<GcpTokenCache>>,
        endpoint: Option<String>,
        allow_http: bool,
        skip_signature: bool,
    },
    Azure {
        container: String,
        account: Option<String>,
        access_key: Option<String>,
        endpoint: Option<String>,
    },
    Oci {
        bucket: String,
        namespace: String,
        region: String,
        endpoint: Option<String>,
        credentials: Option<OciCavageCredentials>,
    },
    Http {
        url: String,
    },
}

#[derive(Clone)]
pub struct ObjectStoreEngine {
    inner: Arc<dyn ObjectStore>,
    publication: PublicationBackend,
    scheme: String,
}

impl ObjectStoreEngine {
    pub fn new(config: BackendConfig) -> Result<Self, StorageError> {
        match config {
            BackendConfig::Memory => {
                let store = Arc::new(InMemory::new());
                Ok(Self {
                    inner: store.clone(),
                    publication: PublicationBackend::Memory(store),
                    scheme: "memory".to_string(),
                })
            }
            BackendConfig::Local { root } => {
                std::fs::create_dir_all(&root).map_err(|e| {
                    StorageError::InvalidUrl(
                        root.clone(),
                        format!("failed creating directory: {e}"),
                    )
                })?;
                let store = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
                Ok(Self {
                    inner: store.clone(),
                    publication: PublicationBackend::Local(store),
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
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(&bucket)
                    .with_allow_http(allow_http);
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
                let store = Arc::new(builder.build()?);
                Ok(Self {
                    inner: store.clone(),
                    publication: PublicationBackend::S3(store),
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
                endpoint,
                allow_http,
                skip_signature,
            } => {
                let mut builder = GoogleCloudStorageBuilder::from_env().with_bucket_name(&bucket);
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
                        let creds = Arc::new(object_store::StaticCredentialProvider::new(
                            object_store::gcp::GcpCredential { bearer: token },
                        ));
                        builder = builder.with_credentials(creds);
                    }
                }
                if let Some(ref ep) = endpoint {
                    builder = builder.with_base_url(ep);
                }
                if allow_http {
                    builder = builder.with_client_options(
                        object_store::ClientOptions::new()
                            .with_allow_http(true)
                            .with_proxy_excludes(
                                "127.0.0.1,localhost,::1,metadata.google.internal",
                            ),
                    );
                }
                if skip_signature {
                    builder = builder.with_skip_signature(true);
                }
                let (store, config) = build_gcs_store(builder)?;
                Ok(Self {
                    inner: store.clone(),
                    publication: PublicationBackend::Gcs {
                        store,
                        config: Arc::new(config),
                    },
                    scheme: "gs".to_string(),
                })
            }
            BackendConfig::Azure {
                container,
                account,
                access_key,
                endpoint,
            } => {
                let mut builder = MicrosoftAzureBuilder::new().with_container_name(&container);
                if let Some(acc) = account {
                    builder = builder.with_account(acc);
                }
                if let Some(key) = access_key {
                    builder = builder.with_access_key(key);
                }
                if let Some(ep) = endpoint {
                    builder = builder.with_endpoint(ep);
                }
                let store = Arc::new(builder.build()?);
                Ok(Self {
                    inner: store.clone(),
                    publication: PublicationBackend::Azure(store),
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
                let store = Arc::new(OciObjectStore::new(
                    namespace,
                    bucket,
                    region,
                    endpoint,
                    credentials,
                )?);
                Ok(Self {
                    inner: store.clone(),
                    publication: PublicationBackend::Oci(store),
                    scheme: "oci".to_string(),
                })
            }
            BackendConfig::Http { url } => {
                let store = Arc::new(HttpBuilder::new().with_url(url).build()?);
                Ok(Self {
                    inner: store.clone(),
                    publication: PublicationBackend::Custom(store),
                    scheme: "http".to_string(),
                })
            }
        }
    }

    pub fn from_url(
        url_str: &str,
        options: Option<&HashMap<String, String>>,
    ) -> Result<Self, StorageError> {
        if url_str == "memory://" || url_str == "memory:" {
            return Self::new(BackendConfig::Memory);
        }
        let parsed = Url::parse(url_str)
            .map_err(|e| StorageError::InvalidUrl(url_str.to_string(), e.to_string()))?;
        let opts = options.cloned().unwrap_or_default();

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
                let credential_cache =
                    GcpTokenCache::from_options(&opts, crate::gcp_auth::GCS_OAUTH_SCOPES);
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
                    endpoint: opts
                        .get("endpoint")
                        .or_else(|| opts.get("base_url"))
                        .cloned(),
                    allow_http: opts.get("allow_http").is_some_and(|v| v == "true"),
                    skip_signature: opts.get("skip_signature").is_some_and(|v| v == "true"),
                })
            }
            "az" | "azure" | "abfs" => {
                let container = parsed.host_str().unwrap_or_default().to_string();
                Self::new(BackendConfig::Azure {
                    container,
                    account: opts.get("account").cloned(),
                    access_key: opts.get("access_key").cloned(),
                    endpoint: opts.get("endpoint").cloned(),
                })
            }
            "http" | "https" => Self::new(BackendConfig::Http {
                url: url_str.to_string(),
            }),
            other => Err(StorageError::InvalidUrl(
                url_str.to_string(),
                format!("unsupported storage URL scheme '{other}'"),
            )),
        }
    }

    pub fn with_store(inner: Arc<dyn ObjectStore>, scheme: impl Into<String>) -> Self {
        Self {
            publication: PublicationBackend::Custom(inner.clone()),
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
        let mode = match PublishCondition::normalize(&opts)? {
            PublishCondition::Create => PutMode::Create,
            PublishCondition::Match(etag) => PutMode::Update(UpdateVersion {
                e_tag: Some(etag),
                version: None,
            }),
            PublishCondition::NotMatch(none_etag) => match self.inner.head(&location).await {
                Ok(existing) if existing.e_tag.as_deref() == Some(none_etag.as_str()) => {
                    return Err(StorageError::Precondition {
                        path: location.to_string(),
                        message: format!("If-None-Match matched existing ETag {none_etag}"),
                    });
                }
                Ok(_) | Err(object_store::Error::NotFound { .. }) => PutMode::Overwrite,
                Err(err) => return Err(err.into()),
            },
            PublishCondition::Overwrite => PutMode::Overwrite,
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
        let upload = self.publication.begin(&location, &opts).await?;

        let accumulator = if self.scheme == "oci" {
            Some(UniformPartAccumulator::new(OCI_UNIFORM_PART_SIZE))
        } else {
            None
        };

        Ok(MultipartUploadSession {
            path: location.to_string(),
            upload: Some(upload),
            accumulator,
            s3_pending: (self.scheme == "s3").then(PutPayloadMut::new),
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[tokio::test]
    async fn disallowed_frame_and_tail_keep_real_upload_abortable() {
        for length in [8 * 1024 * 1024, 1] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let mut parts = 0;
                loop {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    let mut bytes = Vec::new();
                    let boundary = loop {
                        if let Some(index) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
                            break index + 4;
                        }
                        let mut buffer = [0; 4096];
                        let size = socket.read(&mut buffer).await.unwrap();
                        assert_ne!(size, 0);
                        bytes.extend_from_slice(&buffer[..size]);
                    };
                    let headers = String::from_utf8(bytes[..boundary].to_vec()).unwrap();
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            line.split_once(':')
                                .filter(|(key, _)| key.eq_ignore_ascii_case("content-length"))
                                .map(|(_, value)| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    while bytes.len() - boundary < length {
                        let mut buffer = [0; 65536];
                        let size = socket.read(&mut buffer).await.unwrap();
                        assert_ne!(size, 0);
                        bytes.extend_from_slice(&buffer[..size]);
                    }
                    let line = headers.lines().next().unwrap();
                    let done = line.starts_with("DELETE ");
                    let (status, body) = if done {
                        (204, "")
                    } else if line.starts_with("POST ") {
                        (
                            200,
                            "<InitiateMultipartUploadResult><UploadId>count-limit</UploadId></InitiateMultipartUploadResult>",
                        )
                    } else {
                        parts += 1;
                        (400, "<Error><Code>TooManyParts</Code></Error>")
                    };
                    socket.write_all(format!("HTTP/1.1 {status} response\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
                    if done {
                        return parts;
                    }
                }
            });
            let engine = ObjectStoreEngine::new(BackendConfig::S3 {
                bucket: "unit".into(),
                region: Some("us-east-1".into()),
                access_key_id: Some("test".into()),
                secret_access_key: Some("test".into()),
                endpoint: Some(format!("http://{address}")),
                allow_http: true,
            })
            .unwrap();
            let mut session = engine
                .create_multipart("limit", Default::default())
                .await
                .unwrap();
            session.flushed_part_sizes = vec![8 * 1024 * 1024; 10_000];
            let error = if length == 1 {
                session.write_chunk(Bytes::from_static(b"x")).await.unwrap();
                session.complete().await.unwrap_err()
            } else {
                session
                    .write_chunk(Bytes::from(vec![0; length]))
                    .await
                    .unwrap_err()
            };
            session.abort().await.unwrap();
            assert!(
                matches!(error, StorageError::InvalidInput(ref message) if message.contains("10000"))
            );
            assert_eq!(
                tokio::time::timeout(std::time::Duration::from_secs(5), server)
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
        }
    }
}

#[cfg(test)]
mod gcs_config_tests {
    use super::*;
    use object_store::{ClientConfigKey, ClientOptions};
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;

    struct Responder {
        base: String,
        count: Arc<AtomicUsize>,
        requests: Arc<std::sync::Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    impl Responder {
        fn new() -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let count = Arc::new(AtomicUsize::new(0));
            let requests = Arc::new(std::sync::Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let (seen, captured, done) = (count.clone(), requests.clone(), stop.clone());
            let thread = std::thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    let Ok((mut socket, _)) = listener.accept() else {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    };
                    socket
                        .set_read_timeout(Some(Duration::from_secs(3)))
                        .unwrap();
                    let mut bytes = Vec::new();
                    while !bytes.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        if socket.read(&mut byte).unwrap_or(0) == 0 {
                            break;
                        }
                        bytes.push(byte[0]);
                    }
                    captured
                        .lock()
                        .unwrap()
                        .push(String::from_utf8(bytes).unwrap());
                    seen.fetch_add(1, Ordering::Release);
                    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 3\r\nETag: \"etag\"\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\nx-goog-generation: 7\r\nConnection: close\r\n\r\nsdk").unwrap();
                }
            });
            Self {
                base,
                count,
                requests,
                stop,
                thread: Some(thread),
            }
        }
    }

    impl Drop for Responder {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            self.thread.take().unwrap().join().unwrap();
        }
    }

    fn account(base: &str) -> String {
        serde_json::json!({"private_key":"unused", "private_key_id":"unused", "client_email":"fixture@example.invalid", "disable_oauth":true, "gcs_base_url":base}).to_string()
    }

    #[tokio::test]
    async fn gcs_singleton_sources_and_explicit_override_preserve_sdk_reads() {
        for path_source in [false, true] {
            for explicit in [false, true] {
                let fallback = Responder::new();
                let override_server = Responder::new();
                let file = tempfile::NamedTempFile::new().unwrap();
                std::fs::write(file.path(), account(&fallback.base)).unwrap();
                let mut builder = GoogleCloudStorageBuilder::new()
                    .with_bucket_name("bucket")
                    .with_client_options(ClientOptions::new().with_allow_http(true));
                builder = if path_source {
                    builder.with_service_account_path(file.path().to_str().unwrap())
                } else {
                    builder.with_service_account_key(account(&fallback.base))
                };
                if explicit {
                    builder = builder.with_base_url(&override_server.base);
                }
                let (store, config) = build_gcs_store(builder).unwrap();
                assert_eq!(config.bucket, "bucket");
                assert!(!config.skip_signature);
                assert_eq!(
                    config.base.as_str().trim_end_matches('/'),
                    if explicit {
                        &override_server.base
                    } else {
                        &fallback.base
                    }
                );
                assert_eq!(fallback.count.load(Ordering::Acquire), 0);
                assert_eq!(override_server.count.load(Ordering::Acquire), 0);
                assert!(
                    store
                        .credentials()
                        .get_credential()
                        .await
                        .unwrap()
                        .bearer
                        .is_empty()
                );
                assert_eq!(
                    store
                        .get(&StorePath::from("key"))
                        .await
                        .unwrap()
                        .bytes()
                        .await
                        .unwrap(),
                    Bytes::from_static(b"sdk")
                );
                let target = if explicit {
                    &override_server
                } else {
                    &fallback
                };
                let unused = if explicit {
                    &fallback
                } else {
                    &override_server
                };
                assert_eq!(target.count.load(Ordering::Acquire), 1);
                assert_eq!(unused.count.load(Ordering::Acquire), 0);
                let request = target.requests.lock().unwrap()[0].clone();
                assert!(
                    request.starts_with("GET /bucket/key HTTP/1.1\r\n"),
                    "{request}"
                );
                assert!(!request.to_ascii_lowercase().contains("authorization:"));
            }
        }
    }

    #[test]
    fn gcs_dual_service_account_sources_keep_sdk_rejection() {
        let responder = Responder::new();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), account(&responder.base)).unwrap();
        let result = build_gcs_store(
            GoogleCloudStorageBuilder::new()
                .with_bucket_name("bucket")
                .with_service_account_path(file.path().to_str().unwrap())
                .with_service_account_key(account(&responder.base)),
        );
        let error = result.err().unwrap();
        assert!(matches!(
            error,
            StorageError::ObjectStore(object_store::Error::Generic { store: "GCS", .. })
        ));
        assert!(
            error
                .to_string()
                .contains("One of service account path or service account key may be provided."),
            "{error}"
        );
        assert_eq!(responder.count.load(Ordering::Acquire), 0);
    }

    #[tokio::test]
    async fn gcs_final_transport_options_preserve_real_sdk_request() {
        let responder = Responder::new();
        let mut headers = http::HeaderMap::new();
        headers.insert(
            "x-capture-fixture",
            http::HeaderValue::from_static("retained"),
        );
        let options = ClientOptions::new()
            .with_allow_http(true)
            .with_allow_invalid_certificates(true)
            .with_timeout(Duration::from_secs(3))
            .with_connect_timeout(Duration::from_secs(2))
            .with_proxy_url("http://127.0.0.1:1")
            .with_proxy_excludes("127.0.0.1")
            .with_default_headers(headers);
        let credentials: object_store::gcp::GcpCredentialProvider = Arc::new(
            object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
                bearer: "fixture-token".into(),
            }),
        );
        let builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name("bucket")
            .with_base_url(&responder.base)
            .with_skip_signature(true)
            .with_credentials(credentials.clone())
            .with_client_options(options.clone());
        let (store, config) = build_gcs_store(builder).unwrap();
        assert!(Arc::ptr_eq(store.credentials(), &credentials));
        assert_eq!(config.base.as_str().trim_end_matches('/'), responder.base);
        assert_eq!(config.bucket, "bucket");
        assert!(config.skip_signature);
        for key in [
            ClientConfigKey::AllowHttp,
            ClientConfigKey::AllowInvalidCertificates,
            ClientConfigKey::Timeout,
            ClientConfigKey::ConnectTimeout,
            ClientConfigKey::ProxyUrl,
            ClientConfigKey::ProxyExcludes,
        ] {
            assert_eq!(
                config.client_options.get_config_value(&key),
                options.get_config_value(&key),
                "{key:?}"
            );
        }
        assert_eq!(
            config.client_options.get_default_headers(),
            options.get_default_headers()
        );
        assert_eq!(responder.count.load(Ordering::Acquire), 0);
        assert_eq!(
            store
                .get(&StorePath::from("key"))
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap(),
            Bytes::from_static(b"sdk")
        );
        assert!(
            responder.requests.lock().unwrap()[0]
                .to_ascii_lowercase()
                .contains("x-capture-fixture: retained\r\n")
        );
        assert_eq!(
            options
                .get_config_value(&ClientConfigKey::AllowHttp)
                .as_deref(),
            Some("true")
        );
    }
    #[test]
    fn gcs_engine_environment_override_shares_sdk_identity() {
        if std::env::var_os("VERTEBRA_GCS_CAPTURE_CHILD").is_none() {
            let fallback = Responder::new();
            let target = Responder::new();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--exact",
                    "store::gcs_config_tests::gcs_engine_environment_override_shares_sdk_identity",
                    "--nocapture",
                ])
                .env("VERTEBRA_GCS_CAPTURE_CHILD", "1")
                .env("GOOGLE_BASE_URL", &target.base)
                .env("GOOGLE_SERVICE_ACCOUNT_KEY", account(&fallback.base))
                .env_remove("GOOGLE_SERVICE_ACCOUNT")
                .env_remove("GOOGLE_SERVICE_ACCOUNT_PATH")
                .status()
                .unwrap();
            assert!(status.success());
            assert_eq!(target.count.load(Ordering::Acquire), 1);
            assert_eq!(fallback.count.load(Ordering::Acquire), 0);
            return;
        }
        let engine = ObjectStoreEngine::new(BackendConfig::Gcs {
            bucket: "bucket".into(),
            service_account_path: None,
            service_account_key: None,
            application_credentials_path: None,
            access_token: None,
            credential_cache: None,
            endpoint: None,
            allow_http: true,
            skip_signature: false,
        })
        .unwrap();
        let cloned = engine.clone();
        let PublicationBackend::Gcs { store, config } = &engine.publication else {
            panic!("GCS backend")
        };
        let PublicationBackend::Gcs { store: other, .. } = &cloned.publication else {
            panic!("GCS clone")
        };
        assert!(Arc::ptr_eq(store, other));
        let ordinary: Arc<dyn ObjectStore> = store.clone();
        assert!(Arc::ptr_eq(&engine.inner, &ordinary));
        assert!(Arc::ptr_eq(store.credentials(), other.credentials()));
        assert_eq!(
            config.base.as_str().trim_end_matches('/'),
            std::env::var("GOOGLE_BASE_URL").unwrap()
        );
        assert_eq!(config.bucket, "bucket");
        assert!(!config.skip_signature);
        assert_eq!(
            block_on_storage(engine.get("key")).unwrap(),
            Bytes::from_static(b"sdk")
        );
    }
    #[test]
    fn gcs_sdk_defaults_and_configuration_errors_are_preserved() {
        let credentials: object_store::gcp::GcpCredentialProvider = Arc::new(
            object_store::StaticCredentialProvider::new(object_store::gcp::GcpCredential {
                bearer: "fixture-token".into(),
            }),
        );
        let builder = GoogleCloudStorageBuilder::new()
            .with_bucket_name("bucket")
            .with_credentials(credentials)
            .with_config(GoogleConfigKey::SkipSignature, "ON");
        let (_, config) = build_gcs_store(builder.clone()).unwrap();
        assert_eq!(config.base.as_str(), "https://storage.googleapis.com/");
        assert!(config.skip_signature);
        let error = build_gcs_store(builder.with_config(GoogleConfigKey::SkipSignature, "invalid"))
            .err()
            .unwrap();
        assert!(matches!(
            error,
            StorageError::ObjectStore(object_store::Error::Generic {
                store: "Config",
                ..
            })
        ));
        let responder = Responder::new();
        let error = build_gcs_store(
            GoogleCloudStorageBuilder::new()
                .with_bucket_name("bucket")
                .with_base_url(&responder.base)
                .with_service_account_key(
                    r#"{"private_key":"fixture-private-material","client_email": false}"#,
                ),
        )
        .err()
        .unwrap();
        assert!(matches!(
            error,
            StorageError::ObjectStore(object_store::Error::Generic { store: "GCS", .. })
        ));
        assert!(!error.to_string().contains("fixture-private-material"));
        assert_eq!(responder.count.load(Ordering::Acquire), 0);
    }
}
