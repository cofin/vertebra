//! Multi-backend `ObjectStoreEngine` wrapping Apache `object_store`.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

use bytes::Bytes;
use futures::TryStreamExt;
use object_store::aws::AmazonS3Builder;
use object_store::azure::MicrosoftAzureBuilder;
use object_store::gcp::GoogleCloudStorageBuilder;
use object_store::http::HttpBuilder;
use object_store::local::LocalFileSystem;
use object_store::memory::InMemory;
use object_store::path::Path as StorePath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use thiserror::Error;
use url::Url;

use crate::oci::{OciCavageCredentials, OciObjectStore};
use crate::r2::{R2_UNIFORM_PART_SIZE, chunk_uniform_parts};

static STORAGE_RUNTIME: LazyLock<tokio::runtime::Runtime> = LazyLock::new(|| {
    let worker_threads = std::env::var("VERTEBRA_STORAGE_IO_THREADS")
        .ok()
        .and_then(|s| s.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(2);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .expect("failed to initialize vertebra-storage Tokio runtime")
});

pub fn runtime_handle() -> tokio::runtime::Handle {
    tokio::runtime::Handle::try_current().unwrap_or_else(|_| STORAGE_RUNTIME.handle().clone())
}

pub fn block_on_storage<F: std::future::Future>(future: F) -> F::Output {
    if let Ok(handle) = tokio::runtime::Handle::try_current()
        && handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread
    {
        return tokio::task::block_in_place(|| handle.block_on(future));
    }
    STORAGE_RUNTIME.block_on(future)
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("object_store error: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid storage URL '{0}': {1}")]
    InvalidUrl(String, String),
    #[error("signing error: {0}")]
    Signing(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredObjectMeta {
    pub path: String,
    pub size: u64,
    pub last_modified: String,
    pub etag: Option<String>,
    pub version: Option<String>,
}

impl From<object_store::ObjectMeta> for StoredObjectMeta {
    fn from(value: object_store::ObjectMeta) -> Self {
        Self {
            path: value.location.to_string(),
            size: value.size,
            last_modified: value.last_modified.to_rfc3339(),
            etag: value.e_tag,
            version: value.version,
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
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    scheme: "s3".to_string(),
                })
            }
            BackendConfig::Gcs {
                bucket,
                service_account_path,
                service_account_key,
            } => {
                let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);
                if let Some(sa_path) = service_account_path {
                    builder = builder.with_service_account_path(sa_path);
                }
                if let Some(sa_key) = service_account_key {
                    builder = builder.with_service_account_key(sa_key);
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
                let mut builder = AmazonS3Builder::new()
                    .with_bucket_name(&bucket)
                    .with_region("auto")
                    .with_endpoint(resolved_endpoint)
                    .with_allow_http(allow_http);
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
            BackendConfig::Http { url } => {
                let store = HttpBuilder::new().with_url(url).build()?;
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
                Self::new(BackendConfig::Gcs {
                    bucket,
                    service_account_path: opts.get("service_account_path").cloned(),
                    service_account_key: opts.get("service_account_key").cloned(),
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

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub async fn put(&self, path: &str, data: Bytes) -> Result<StoredObjectMeta, StorageError> {
        let location = StorePath::from(path);
        let size = data.len() as u64;
        let result = self.inner.put(&location, PutPayload::from(data)).await?;
        Ok(StoredObjectMeta {
            path: location.to_string(),
            size,
            last_modified: chrono::Utc::now().to_rfc3339(),
            etag: result.e_tag,
            version: result.version,
        })
    }

    pub async fn put_file(
        &self,
        path: &str,
        file_path: &str,
    ) -> Result<StoredObjectMeta, StorageError> {
        let raw = tokio::fs::read(file_path).await?;
        self.put(path, Bytes::from(raw)).await
    }

    pub async fn put_multipart(
        &self,
        path: &str,
        parts: Vec<Bytes>,
    ) -> Result<StoredObjectMeta, StorageError> {
        let location = StorePath::from(path);
        let normalized_parts = if self.scheme == "r2" && parts.len() > 1 {
            let mut combined = Vec::new();
            for p in &parts {
                combined.extend_from_slice(p);
            }
            chunk_uniform_parts(Bytes::from(combined), R2_UNIFORM_PART_SIZE)
        } else {
            parts
        };
        let total_size: u64 = normalized_parts.iter().map(|b| b.len() as u64).sum();
        let mut upload = self.inner.put_multipart(&location).await?;
        for chunk in normalized_parts {
            upload.put_part(PutPayload::from(chunk)).await?;
        }
        let result = upload.complete().await?;
        Ok(StoredObjectMeta {
            path: location.to_string(),
            size: total_size,
            last_modified: chrono::Utc::now().to_rfc3339(),
            etag: result.e_tag,
            version: result.version,
        })
    }

    pub async fn get(&self, path: &str) -> Result<Bytes, StorageError> {
        let location = StorePath::from(path);
        let result = self.inner.get(&location).await?;
        Ok(result.bytes().await?)
    }

    pub async fn get_range(
        &self,
        path: &str,
        offset: u64,
        length: u64,
    ) -> Result<Bytes, StorageError> {
        let location = StorePath::from(path);
        let start = offset;
        let end = offset + length;
        Ok(self.inner.get_range(&location, start..end).await?)
    }

    pub async fn head(&self, path: &str) -> Result<StoredObjectMeta, StorageError> {
        let location = StorePath::from(path);
        let meta = self.inner.head(&location).await?;
        Ok(StoredObjectMeta::from(meta))
    }

    pub async fn exists(&self, path: &str) -> Result<bool, StorageError> {
        let location = StorePath::from(path);
        match self.inner.head(&location).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { .. }) => Ok(false),
            Err(err) => Err(StorageError::ObjectStore(err)),
        }
    }

    pub async fn delete(&self, path: &str) -> Result<(), StorageError> {
        let location = StorePath::from(path);
        self.inner.delete(&location).await?;
        Ok(())
    }

    pub async fn copy(&self, src: &str, dst: &str) -> Result<(), StorageError> {
        let from = StorePath::from(src);
        let to = StorePath::from(dst);
        self.inner.copy(&from, &to).await?;
        Ok(())
    }

    pub async fn list(&self, prefix: Option<&str>) -> Result<Vec<StoredObjectMeta>, StorageError> {
        let store_prefix = prefix.filter(|p| !p.is_empty()).map(StorePath::from);
        let stream = self.inner.list(store_prefix.as_ref());
        let metas: Vec<object_store::ObjectMeta> = stream.try_collect().await?;
        let mut items: Vec<StoredObjectMeta> =
            metas.into_iter().map(StoredObjectMeta::from).collect();
        items.sort_by(|a, b| a.path.cmp(&b.path));
        Ok(items)
    }
}
