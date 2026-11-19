//! Multi-backend `ObjectStoreEngine` wrapping Apache `object_store`.

use std::collections::HashMap;
use std::path::{Path as FsPath, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, RwLock};
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use futures::stream::BoxStream;
use futures::{StreamExt, TryStreamExt};
use md5::Md5;
use object_store::aws::{AmazonS3Builder, S3CopyIfNotExists};
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
use sha2::{Digest, Sha256};
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
    #[error("integrity check failed for '{path}' ({algorithm}): expected {expected}, got {actual}")]
    Integrity {
        path: String,
        algorithm: &'static str,
        expected: String,
        actual: String,
    },
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

    pub fn is_integrity(&self) -> bool {
        matches!(self, Self::Integrity { .. })
    }
}

const CRC32C_TABLES: [[u32; 256]; 8] = {
    let mut tables = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut bit = 0;
        while bit < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
            bit += 1;
        }
        tables[0][i] = crc;
        i += 1;
    }
    let mut slice = 1;
    while slice < 8 {
        let mut j = 0;
        while j < 256 {
            let prev = tables[slice - 1][j];
            tables[slice][j] = (prev >> 8) ^ tables[0][(prev & 0xFF) as usize];
            j += 1;
        }
        slice += 1;
    }
    tables
};

#[derive(Clone, Copy, Debug)]
struct Crc32cHasher {
    state: u32,
}

impl Crc32cHasher {
    const fn new() -> Self {
        Self { state: !0u32 }
    }

    fn update(&mut self, mut bytes: &[u8]) {
        let mut crc = self.state;
        while bytes.len() >= 8 {
            let lo = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) ^ crc;
            let hi = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
            crc = CRC32C_TABLES[7][(lo & 0xFF) as usize]
                ^ CRC32C_TABLES[6][((lo >> 8) & 0xFF) as usize]
                ^ CRC32C_TABLES[5][((lo >> 16) & 0xFF) as usize]
                ^ CRC32C_TABLES[4][(lo >> 24) as usize]
                ^ CRC32C_TABLES[3][(hi & 0xFF) as usize]
                ^ CRC32C_TABLES[2][((hi >> 8) & 0xFF) as usize]
                ^ CRC32C_TABLES[1][((hi >> 16) & 0xFF) as usize]
                ^ CRC32C_TABLES[0][(hi >> 24) as usize];
            bytes = &bytes[8..];
        }
        for &b in bytes {
            crc = (crc >> 8) ^ CRC32C_TABLES[0][((crc ^ u32::from(b)) & 0xFF) as usize];
        }
        self.state = crc;
    }

    const fn finalize(self) -> u32 {
        !self.state
    }

    fn finalize_hex(self) -> String {
        format!("{:08x}", self.finalize())
    }
}

pub(crate) fn validate_expected_hex(
    field: &'static str,
    raw: &str,
    expected_len: usize,
) -> Result<(), StorageError> {
    if raw.len() != expected_len || !raw.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return Err(StorageError::InvalidInput(format!(
            "{field} must be {expected_len} lowercase hexadecimal characters, got '{raw}'"
        )));
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct DigestReceipts {
    sha256: Option<String>,
    crc32c: Option<String>,
    md5: Option<String>,
}

#[derive(Clone, Default)]
struct DigestAccumulator {
    sha256: Option<Sha256>,
    crc32c: Option<Crc32cHasher>,
    md5: Option<Md5>,
    expected_sha256: Option<String>,
    expected_crc32c: Option<String>,
    expected_md5: Option<String>,
}

impl DigestAccumulator {
    fn from_options(opts: &PutConditionalOptions) -> Result<Self, StorageError> {
        opts.validate_expected_digests()?;
        let want_sha256 = opts.checksum || opts.expected_sha256.is_some();
        let want_crc32c = opts.checksum || opts.expected_crc32c.is_some();
        let want_md5 = opts.checksum || opts.expected_md5.is_some();
        Ok(Self {
            sha256: want_sha256.then(Sha256::new),
            crc32c: want_crc32c.then(Crc32cHasher::new),
            md5: want_md5.then(Md5::new),
            expected_sha256: opts.expected_sha256.clone(),
            expected_crc32c: opts.expected_crc32c.clone(),
            expected_md5: opts.expected_md5.clone(),
        })
    }

    fn update(&mut self, bytes: &[u8]) {
        if let Some(ref mut hasher) = self.sha256 {
            hasher.update(bytes);
        }
        if let Some(ref mut hasher) = self.crc32c {
            hasher.update(bytes);
        }
        if let Some(ref mut hasher) = self.md5 {
            hasher.update(bytes);
        }
    }

    fn finalize_and_verify(self, path: &str) -> Result<DigestReceipts, StorageError> {
        let sha256 = self.sha256.map(|h| hex::encode(h.finalize()));
        let crc32c = self.crc32c.map(Crc32cHasher::finalize_hex);
        let md5 = self.md5.map(|h| hex::encode(h.finalize()));

        if let (Some(expected), Some(actual)) = (&self.expected_sha256, &sha256)
            && actual != expected
        {
            return Err(StorageError::Integrity {
                path: path.to_string(),
                algorithm: "sha256",
                expected: expected.clone(),
                actual: actual.clone(),
            });
        }
        if let (Some(expected), Some(actual)) = (&self.expected_crc32c, &crc32c)
            && actual != expected
        {
            return Err(StorageError::Integrity {
                path: path.to_string(),
                algorithm: "crc32c",
                expected: expected.clone(),
                actual: actual.clone(),
            });
        }
        if let (Some(expected), Some(actual)) = (&self.expected_md5, &md5)
            && actual != expected
        {
            return Err(StorageError::Integrity {
                path: path.to_string(),
                algorithm: "md5",
                expected: expected.clone(),
                actual: actual.clone(),
            });
        }
        Ok(DigestReceipts {
            sha256,
            crc32c,
            md5,
        })
    }
}

struct VerifyingStream {
    inner: BoxStream<'static, object_store::Result<Bytes>>,
    path: String,
    expected_sha256: String,
    hasher: Option<Sha256>,
}

impl futures::Stream for VerifyingStream {
    type Item = Result<Bytes, StorageError>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        let Some(ref mut hasher) = this.hasher else {
            return std::task::Poll::Ready(None);
        };
        match this.inner.as_mut().poll_next(cx) {
            std::task::Poll::Pending => std::task::Poll::Pending,
            std::task::Poll::Ready(Some(Ok(chunk))) => {
                hasher.update(&chunk);
                std::task::Poll::Ready(Some(Ok(chunk)))
            }
            std::task::Poll::Ready(Some(Err(err))) => {
                this.hasher = None;
                std::task::Poll::Ready(Some(Err(StorageError::from(err))))
            }
            std::task::Poll::Ready(None) => {
                let finalized = this
                    .hasher
                    .take()
                    .expect("hasher checked Some above")
                    .finalize();
                let actual = hex::encode(finalized);
                if actual == this.expected_sha256 {
                    std::task::Poll::Ready(None)
                } else {
                    std::task::Poll::Ready(Some(Err(StorageError::Integrity {
                        path: this.path.clone(),
                        algorithm: "sha256",
                        expected: this.expected_sha256.clone(),
                        actual,
                    })))
                }
            }
        }
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
    pub sha256: Option<String>,
    pub crc32c: Option<String>,
    pub md5: Option<String>,
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
            sha256: None,
            crc32c: None,
            md5: None,
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
            sha256: None,
            crc32c: None,
            md5: None,
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
    pub checksum: bool,
    pub expected_sha256: Option<String>,
    pub expected_crc32c: Option<String>,
    pub expected_md5: Option<String>,
}

impl PutConditionalOptions {
    pub fn validate_expected_digests(&self) -> Result<(), StorageError> {
        if let Some(ref sha) = self.expected_sha256 {
            validate_expected_hex("expected_sha256", sha, 64)?;
        }
        if let Some(ref crc) = self.expected_crc32c {
            validate_expected_hex("expected_crc32c", crc, 8)?;
        }
        if let Some(ref md5) = self.expected_md5 {
            validate_expected_hex("expected_md5", md5, 32)?;
        }
        Ok(())
    }
}

/// Stateful incremental multipart upload session.
///
/// S3, GCS, Cloudflare R2, and OCI chunks pass through a rolling 8 MiB
/// `UniformPartAccumulator`. This satisfies the S3 and GCS XML non-trailing 5 MiB minimum and
/// R2/OCI part uniformity without buffering the entire payload in memory. B2 URLs normalize to
/// the S3 backend. Azure, memory, and local-file chunks are uploaded as written.
pub struct MultipartUploadSession {
    path: String,
    upload: Option<Box<dyn MultipartUpload>>,
    accumulator: Option<UniformPartAccumulator>,
    digests: DigestAccumulator,
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
        self.digests.update(&chunk);
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

    async fn complete_inner(&mut self) -> Result<(PutResult, DigestReceipts), ObjectStoreError> {
        if self.upload.is_none() {
            return Err(StorageError::InvalidInput(
                "multipart upload session is already closed".to_string(),
            ));
        }
        if self.had_error {
            return Err(StorageError::InvalidInput(format!(
                "multipart upload for '{}' had a failed part and cannot be completed; abort it",
                self.path
            )));
        }
        let receipts = match std::mem::take(&mut self.digests).finalize_and_verify(&self.path) {
            Ok(receipts) => receipts,
            Err(err) => {
                self.had_error = true;
                let _ = self.abort().await;
                return Err(err);
            }
        };
        let upload = self.upload.as_mut().expect("upload verified present above");
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
                Ok((result, receipts))
            }
            Err(err) => {
                self.had_error = true;
                Err(err.into())
            }
        }
    }

    /// Flush any buffered tail and commit the upload.
    ///
    /// A session whose earlier part upload failed never commits: it returns an error and keeps
    /// the upload so `abort` can release it. Expected-digest mismatches abort the upload before
    /// publishing so prior objects remain intact.
    pub async fn complete(&mut self) -> Result<PutResult, ObjectStoreError> {
        let (result, _) = self.complete_inner().await?;
        Ok(result)
    }

    pub async fn complete_with_meta(&mut self) -> Result<StoredObjectMeta, ObjectStoreError> {
        let (result, receipts) = self.complete_inner().await?;
        Ok(StoredObjectMeta {
            path: self.path.clone(),
            size: self.total_bytes,
            last_modified: chrono::Utc::now().to_rfc3339(),
            etag: result.e_tag,
            version: result.version,
            content_type: self.content_type.clone(),
            attributes: self.attributes.clone(),
            sha256: receipts.sha256,
            crc32c: receipts.crc32c,
            md5: receipts.md5,
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

fn local_staged_upload_path(dest: &FsPath, pid: u32, seq: u64) -> PathBuf {
    let mut staging = dest.as_os_str().to_os_string();
    staging.push(format!("#{pid}{seq:012}"));
    PathBuf::from(staging)
}

fn local_file_etag(metadata: &std::fs::Metadata) -> String {
    #[cfg(unix)]
    let inode = std::os::unix::fs::MetadataExt::ino(metadata);
    #[cfg(not(unix))]
    let inode = 0u64;
    let size = metadata.len();
    let mtime = metadata
        .modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .unwrap_or_default()
        .as_micros();
    format!("{inode:x}-{mtime:x}-{size:x}")
}

#[derive(Debug)]
struct LocalCreateOnlyUpload {
    location: StorePath,
    dest: PathBuf,
    state: Arc<std::sync::Mutex<Option<std::fs::File>>>,
    staging: Option<PathBuf>,
    offset: u64,
}

impl LocalCreateOnlyUpload {
    async fn open(location: StorePath, dest: PathBuf) -> Result<Self, StorageError> {
        let dest_for_open = dest.clone();
        let (file, staging) = tokio::task::spawn_blocking(move || {
            let mut created_dirs = false;
            loop {
                let seq = PART_FILE_SEQ.fetch_add(1, Ordering::Relaxed);
                let staging = local_staged_upload_path(&dest_for_open, std::process::id(), seq);
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&staging)
                {
                    Ok(file) => return Ok((file, staging)),
                    Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => continue,
                    Err(err) if err.kind() == std::io::ErrorKind::NotFound && !created_dirs => {
                        if let Some(parent) = dest_for_open.parent() {
                            std::fs::create_dir_all(parent)?;
                            created_dirs = true;
                            continue;
                        }
                        return Err(StorageError::Io(err));
                    }
                    Err(err) => return Err(StorageError::Io(err)),
                }
            }
        })
        .await
        .map_err(|err| StorageError::Io(std::io::Error::other(err)))??;

        Ok(Self {
            location,
            dest,
            state: Arc::new(std::sync::Mutex::new(Some(file))),
            staging: Some(staging),
            offset: 0,
        })
    }
}

#[async_trait::async_trait]
impl MultipartUpload for LocalCreateOnlyUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        let offset = self.offset;
        self.offset += data.content_length() as u64;
        let state = Arc::clone(&self.state);
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                use std::io::{Seek, SeekFrom, Write};

                let mut guard = state.lock().unwrap_or_else(|e| e.into_inner());
                let Some(file) = guard.as_mut() else {
                    return Err(object_store::Error::Generic {
                        store: "LocalFileSystem",
                        source: "local create-only upload already closed".into(),
                    });
                };
                file.seek(SeekFrom::Start(offset))
                    .map_err(|err| object_store::Error::Generic {
                        store: "LocalFileSystem",
                        source: Box::new(err),
                    })?;
                for chunk in data.iter() {
                    file.write_all(chunk)
                        .map_err(|err| object_store::Error::Generic {
                            store: "LocalFileSystem",
                            source: Box::new(err),
                        })?;
                }
                Ok(())
            })
            .await
            .map_err(|source| object_store::Error::JoinError { source })?
        })
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        let Some(staging) = self.staging.take() else {
            return Err(object_store::Error::Generic {
                store: "LocalFileSystem",
                source: "local create-only upload already completed or aborted".into(),
            });
        };
        let dest = self.dest.clone();
        let location = self.location.to_string();
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            use std::io::Write;

            let file = state.lock().unwrap_or_else(|e| e.into_inner()).take();
            let Some(mut file) = file else {
                let _ = std::fs::remove_file(&staging);
                return Err(object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: "local create-only upload file handle already closed".into(),
                });
            };
            if let Err(err) = file.flush() {
                drop(file);
                let _ = std::fs::remove_file(&staging);
                return Err(object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(err),
                });
            }
            let staged_meta = file.metadata().ok();
            drop(file);
            match std::fs::hard_link(&staging, &dest) {
                Ok(()) => {
                    let _ = std::fs::remove_file(&staging);
                    let meta = std::fs::metadata(&dest).ok().or(staged_meta);
                    let e_tag = meta.map(|m| local_file_etag(&m));
                    Ok(PutResult {
                        e_tag,
                        version: None,
                    })
                }
                Err(source) if source.kind() == std::io::ErrorKind::AlreadyExists => {
                    let _ = std::fs::remove_file(&staging);
                    Err(object_store::Error::AlreadyExists {
                        path: location,
                        source: Box::new(source),
                    })
                }
                Err(source) => {
                    let _ = std::fs::remove_file(&staging);
                    Err(object_store::Error::Generic {
                        store: "LocalFileSystem",
                        source: Box::new(source),
                    })
                }
            }
        })
        .await
        .map_err(|source| object_store::Error::JoinError { source })?
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        let Some(staging) = self.staging.take() else {
            return Ok(());
        };
        let state = Arc::clone(&self.state);
        tokio::task::spawn_blocking(move || {
            let file = state.lock().unwrap_or_else(|e| e.into_inner()).take();
            drop(file);
            match std::fs::remove_file(&staging) {
                Ok(()) => Ok(()),
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(err) => Err(object_store::Error::Generic {
                    store: "LocalFileSystem",
                    source: Box::new(err),
                }),
            }
        })
        .await
        .map_err(|source| object_store::Error::JoinError { source })?
    }
}

impl Drop for LocalCreateOnlyUpload {
    fn drop(&mut self) {
        if let Some(staging) = self.staging.take() {
            let file = self.state.lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn_blocking(move || {
                    drop(file);
                    let _ = std::fs::remove_file(staging);
                });
            } else {
                drop(file);
                let _ = std::fs::remove_file(staging);
            }
        }
    }
}

#[derive(Debug)]
struct MemoryCreateOnlyUpload {
    inner: Arc<dyn ObjectStore>,
    location: StorePath,
    attributes: Attributes,
    parts: Vec<PutPayload>,
    closed: bool,
}

impl MemoryCreateOnlyUpload {
    fn new(inner: Arc<dyn ObjectStore>, location: StorePath, attributes: Attributes) -> Self {
        Self {
            inner,
            location,
            attributes,
            parts: Vec::new(),
            closed: false,
        }
    }
}

#[async_trait::async_trait]
impl MultipartUpload for MemoryCreateOnlyUpload {
    fn put_part(&mut self, data: PutPayload) -> object_store::UploadPart {
        if self.closed {
            return Box::pin(futures::future::ready(Err(object_store::Error::Generic {
                store: "InMemory",
                source: format!("multipart upload for '{}' is already closed", self.location)
                    .into(),
            })));
        }
        self.parts.push(data);
        Box::pin(futures::future::ready(Ok(())))
    }

    async fn complete(&mut self) -> object_store::Result<PutResult> {
        if self.closed {
            return Err(object_store::Error::Generic {
                store: "InMemory",
                source: format!(
                    "multipart upload for '{}' is already completed or aborted",
                    self.location
                )
                .into(),
            });
        }
        self.closed = true;
        let payload: PutPayload = std::mem::take(&mut self.parts)
            .into_iter()
            .flatten()
            .collect();
        self.inner
            .put_opts(
                &self.location,
                payload,
                PutOptions {
                    mode: PutMode::Create,
                    attributes: std::mem::take(&mut self.attributes),
                    ..Default::default()
                },
            )
            .await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.closed = true;
        self.parts.clear();
        Ok(())
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
        sas_token: Option<String>,
        tenant_id: Option<String>,
        client_id: Option<String>,
        client_secret: Option<String>,
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

impl BackendConfig {
    /// Return `true` when the backend executes `rename` atomically without a `copy + delete` fallback.
    pub const fn supports_atomic_rename(&self) -> bool {
        matches!(self, Self::Memory | Self::Local { .. } | Self::Oci { .. })
    }
}

/// Collect outbound HTTP headers from the JSON `headers` option and `header.<name>` /
/// `http_header.<name>` keys; every other option key is backend configuration, not a header.
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
        let Some(header_name) = k
            .strip_prefix("header.")
            .or_else(|| k.strip_prefix("http_header."))
        else {
            continue;
        };
        let header_name = header_name.trim();
        if !header_name.is_empty() {
            headers.insert(header_name.to_string(), v.clone());
        }
    }
    Ok(headers)
}

/// Parse an Azure Shared Access Signature query string into decoded `(key, value)` pairs.
fn parse_azure_sas_query_pairs(raw_sas: &str) -> Result<Vec<(String, String)>, StorageError> {
    let without_prefix = raw_sas.trim().trim_start_matches('?').trim();
    if without_prefix.is_empty() || without_prefix.chars().any(char::is_whitespace) {
        return Err(StorageError::InvalidInput(
            "invalid Azure sas_token: expected non-empty query string".to_string(),
        ));
    }
    for segment in without_prefix.split('&') {
        let Some((raw_k, raw_v)) = segment.split_once('=') else {
            return Err(StorageError::InvalidInput(format!(
                "invalid Azure sas_token segment '{segment}': expected key=value query pairs"
            )));
        };
        if raw_k.is_empty() || raw_v.is_empty() {
            return Err(StorageError::InvalidInput(format!(
                "invalid Azure sas_token segment '{segment}': key and value must be non-empty"
            )));
        }
    }
    let pairs: Vec<(String, String)> = url::form_urlencoded::parse(without_prefix.as_bytes())
        .into_owned()
        .collect();
    if pairs.is_empty()
        || pairs.iter().any(|(k, v)| {
            k.is_empty() || v.is_empty() || k.contains('\u{FFFD}') || v.contains('\u{FFFD}')
        })
    {
        return Err(StorageError::InvalidInput(
            "invalid Azure sas_token: query pairs must be non-empty valid UTF-8".to_string(),
        ));
    }
    Ok(pairs)
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
    local_fs: Option<Arc<LocalFileSystem>>,
    scheme: String,
    supports_atomic_rename: bool,
    local_metadata: Option<LocalMetadataRegistry>,
    gcs_compose: Option<Arc<GcsComposeContext>>,
}

impl ObjectStoreEngine {
    pub fn new(config: BackendConfig) -> Result<Self, StorageError> {
        let supports_atomic_rename = config.supports_atomic_rename();
        match config {
            BackendConfig::Memory => Ok(Self {
                inner: Arc::new(InMemory::new()),
                local_fs: None,
                scheme: "memory".to_string(),
                supports_atomic_rename,
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
                let local = Arc::new(LocalFileSystem::new_with_prefix(&root)?);
                Ok(Self {
                    inner: Arc::clone(&local) as Arc<dyn ObjectStore>,
                    local_fs: Some(local),
                    scheme: "file".to_string(),
                    supports_atomic_rename,
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
                    .with_client_options(client_opts)
                    .with_copy_if_not_exists(S3CopyIfNotExists::Multipart);
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
                    local_fs: None,
                    scheme: "s3".to_string(),
                    supports_atomic_rename,
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
                    local_fs: None,
                    scheme: "gs".to_string(),
                    supports_atomic_rename,
                    local_metadata: None,
                    gcs_compose: Some(Arc::new(compose)),
                })
            }
            BackendConfig::Azure {
                container,
                account,
                access_key,
                sas_token,
                tenant_id,
                client_id,
                client_secret,
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
                match (tenant_id, client_id, client_secret) {
                    (Some(tid), Some(cid), Some(csec)) => {
                        if tid.trim().is_empty() || cid.trim().is_empty() || csec.trim().is_empty()
                        {
                            return Err(StorageError::InvalidInput(
                                "Azure Entra ID credentials (tenant_id, client_id, client_secret) must not be empty"
                                    .to_string(),
                            ));
                        }
                        builder = builder
                            .with_tenant_id(tid)
                            .with_client_id(cid)
                            .with_client_secret(csec);
                    }
                    (None, None, None) => {}
                    _ => {
                        return Err(StorageError::InvalidInput(
                            "incomplete Azure Entra ID credentials: tenant_id, client_id, and client_secret must all be provided together"
                                .to_string(),
                        ));
                    }
                }
                if let Some(raw_sas) = sas_token {
                    let sas_pairs = parse_azure_sas_query_pairs(&raw_sas)?;
                    builder = builder.with_sas_authorization(sas_pairs);
                }
                if let Some(ep) = endpoint {
                    builder = builder.with_endpoint(ep);
                }
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    local_fs: None,
                    scheme: "az".to_string(),
                    supports_atomic_rename,
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
                    local_fs: None,
                    scheme: "oci".to_string(),
                    supports_atomic_rename,
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
                    .with_client_options(client_opts)
                    .with_copy_if_not_exists(S3CopyIfNotExists::HeaderWithStatus(
                        "cf-copy-destination-if-none-match".to_string(),
                        "*".to_string(),
                        reqwest::StatusCode::PRECONDITION_FAILED,
                    ));
                if let Some(key_id) = access_key_id {
                    builder = builder.with_access_key_id(key_id);
                }
                if let Some(secret) = secret_access_key {
                    builder = builder.with_secret_access_key(secret);
                }
                Ok(Self {
                    inner: Arc::new(builder.build()?),
                    local_fs: None,
                    scheme: "r2".to_string(),
                    supports_atomic_rename,
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
                    local_fs: None,
                    scheme: "http".to_string(),
                    supports_atomic_rename,
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
                    (Some(t), Some(u), Some(fp), Some(pem)) => {
                        if t.trim().is_empty()
                            || u.trim().is_empty()
                            || fp.trim().is_empty()
                            || pem.trim().is_empty()
                        {
                            return Err(StorageError::InvalidInput(
                                "OCI Cavage credentials (tenancy_ocid, user_ocid, fingerprint, private_key_pem) must not be empty"
                                    .to_string(),
                            ));
                        }
                        Some(OciCavageCredentials {
                            tenancy_ocid: t.clone(),
                            user_ocid: u.clone(),
                            fingerprint: fp.clone(),
                            private_key_pem: pem.clone(),
                        })
                    }
                    (None, None, None, None) => None,
                    _ => {
                        return Err(StorageError::InvalidInput(
                            "incomplete OCI Cavage credentials: tenancy_ocid, user_ocid, fingerprint, and private_key_pem must all be provided together"
                                .to_string(),
                        ));
                    }
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
                    sas_token: opts.get("sas_token").cloned(),
                    tenant_id: opts.get("tenant_id").cloned(),
                    client_id: opts.get("client_id").cloned(),
                    client_secret: opts.get("client_secret").cloned(),
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
        let supports_atomic_rename = matches!(scheme.as_str(), "file" | "local" | "memory" | "oci");
        let local_metadata = if scheme == "file" {
            Some(Arc::new(RwLock::new(HashMap::new())))
        } else {
            None
        };
        Self {
            inner,
            local_fs: None,
            scheme,
            supports_atomic_rename,
            local_metadata,
            gcs_compose: None,
        }
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    /// Return `true` when the active backend executes `rename` atomically.
    pub const fn supports_atomic_rename(&self) -> bool {
        self.supports_atomic_rename
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
        let mut digests = DigestAccumulator::from_options(&opts)?;
        let create_only = opts.if_not_exists || opts.if_none_match.as_deref() == Some("*");
        if create_only && opts.if_match.is_some() {
            return Err(StorageError::InvalidInput(
                "cannot combine if_not_exists or if_none_match='*' with if_match".to_string(),
            ));
        }
        digests.update(&data);
        let receipts = digests.finalize_and_verify(location.as_ref())?;

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
            sha256: receipts.sha256,
            crc32c: receipts.crc32c,
            md5: receipts.md5,
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
        self.put_file_opts(path, file_path, PutConditionalOptions::default())
            .await
    }

    pub async fn put_file_opts(
        &self,
        path: &str,
        file_path: &str,
        opts: PutConditionalOptions,
    ) -> Result<StoredObjectMeta, StorageError> {
        let _ = validate_object_path(path)?;
        opts.validate_expected_digests()?;
        let size = tokio::fs::metadata(file_path).await?.len();
        if size <= FILE_TRANSFER_CHUNK_SIZE as u64 {
            let raw = tokio::fs::read(file_path).await?;
            return self.put_opts(path, Bytes::from(raw), opts).await;
        }
        let mut file = tokio::fs::File::open(file_path).await?;
        let mut session = self.create_multipart(path, opts).await?;
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
    /// Chunks stream into a sibling `.{name}.{pid}.{seq}.part` file that is flushed, synced via
    /// `sync_all`, and renamed over `file_path` only after the full payload arrives, so readers
    /// never observe a partial file. On Unix, the destination parent directory is synced after the
    /// rename. A drop guard unlinks the `.part` file on any error and when the future is cancelled
    /// mid-transfer. Parent directories are created.
    pub async fn get_file(
        &self,
        path: &str,
        file_path: &str,
    ) -> Result<StoredObjectMeta, StorageError> {
        self.get_file_inner(path, file_path, None).await
    }

    pub async fn get_file_verified(
        &self,
        path: &str,
        dest: &str,
        expected_sha256: &str,
    ) -> Result<StoredObjectMeta, StorageError> {
        self.get_file_inner(path, dest, Some(expected_sha256)).await
    }

    async fn get_file_inner(
        &self,
        path: &str,
        file_path: &str,
        expected_sha256: Option<&str>,
    ) -> Result<StoredObjectMeta, StorageError> {
        let location = validate_object_path(path)?;
        if let Some(expected) = expected_sha256 {
            validate_expected_hex("expected_sha256", expected, 64)?;
        }
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
        let computed_sha256 =
            Self::write_stream_to_file(result.into_stream(), &part.path, expected_sha256.is_some())
                .await?;
        if let (Some(expected), Some(actual)) = (expected_sha256, computed_sha256) {
            if actual != expected {
                let _ = tokio::fs::remove_file(&part.path).await;
                part.armed = false;
                return Err(StorageError::Integrity {
                    path: location.to_string(),
                    algorithm: "sha256",
                    expected: expected.to_string(),
                    actual,
                });
            }
            meta.sha256 = Some(actual);
        }
        tokio::fs::rename(&part.path, target).await?;
        part.armed = false;
        #[cfg(unix)]
        {
            let dir = tokio::fs::File::open(&parent).await?;
            dir.sync_all().await?;
        }
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
        let digests = DigestAccumulator::from_options(&opts)?;
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

        let upload: Box<dyn MultipartUpload> = if create_only
            && let Some(ref local_fs) = self.local_fs
        {
            let dest = local_fs.path_to_filesystem(&location)?;
            Box::new(LocalCreateOnlyUpload::open(location.clone(), dest).await?)
        } else if create_only && self.scheme == "memory" {
            let store_attributes =
                build_store_attributes(opts.content_type.as_deref(), &opts.attributes);
            Box::new(MemoryCreateOnlyUpload::new(
                Arc::clone(&self.inner),
                location.clone(),
                store_attributes,
            ))
        } else {
            let mp_opts = PutMultipartOptions {
                attributes: build_store_attributes(opts.content_type.as_deref(), &opts.attributes),
                ..Default::default()
            };
            match self.inner.put_multipart_opts(&location, mp_opts).await {
                Ok(up) => up,
                Err(object_store::Error::NotImplemented { .. }) if self.scheme == "file" => {
                    self.inner.put_multipart(&location).await?
                }
                Err(err) => return Err(err.into()),
            }
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
            digests,
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

    pub async fn get_verified(
        &self,
        path: &str,
        expected_sha256: &str,
    ) -> Result<Bytes, StorageError> {
        let location = validate_object_path(path)?;
        validate_expected_hex("expected_sha256", expected_sha256, 64)?;
        let result = self.inner.get(&location).await?;
        let bytes = result.bytes().await?;
        let actual = hex::encode(Sha256::digest(&bytes));
        if actual != expected_sha256 {
            return Err(StorageError::Integrity {
                path: location.to_string(),
                algorithm: "sha256",
                expected: expected_sha256.to_string(),
                actual,
            });
        }
        Ok(bytes)
    }

    pub async fn get_stream(
        &self,
        path: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, ObjectStoreError>>, ObjectStoreError> {
        let location = validate_object_path(path)?;
        let result = self.inner.get(&location).await?;
        Ok(result.into_stream().map_err(StorageError::from).boxed())
    }

    pub async fn get_stream_verified(
        &self,
        path: &str,
        expected_sha256: &str,
    ) -> Result<BoxStream<'static, Result<Bytes, StorageError>>, StorageError> {
        let location = validate_object_path(path)?;
        validate_expected_hex("expected_sha256", expected_sha256, 64)?;
        let result = self.inner.get(&location).await?;
        Ok(VerifyingStream {
            inner: result.into_stream(),
            path: location.to_string(),
            expected_sha256: expected_sha256.to_string(),
            hasher: Some(Sha256::new()),
        }
        .boxed())
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
    /// S3/B2 use conditional multipart completion; R2 uses its conditional copy header.
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
                Err(
                    err @ (object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. }),
                ) => {
                    return Err(StorageError::AlreadyExists {
                        path: dst.to_string(),
                        message: err.to_string(),
                    });
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
        self.rename_with_options(src, dst, if_not_exists, false)
            .await
    }

    pub async fn rename_with_options(
        &self,
        from: &str,
        to: &str,
        if_not_exists: bool,
        atomic_only: bool,
    ) -> Result<(), StorageError> {
        if atomic_only && !self.supports_atomic_rename() {
            return Err(StorageError::InvalidInput(format!(
                "atomic rename is not supported for '{}' backend (pass atomic_only=false to allow copy+delete fallback)",
                self.scheme
            )));
        }
        let from_path = validate_object_path(from)?;
        let to_path = validate_object_path(to)?;
        if if_not_exists {
            match self.inner.rename_if_not_exists(&from_path, &to_path).await {
                Ok(()) => {}
                Err(
                    err @ (object_store::Error::AlreadyExists { .. }
                    | object_store::Error::Precondition { .. }),
                ) => {
                    return Err(StorageError::AlreadyExists {
                        path: to.to_string(),
                        message: err.to_string(),
                    });
                }
                Err(err) => return Err(err.into()),
            }
        } else {
            self.inner.rename(&from_path, &to_path).await?;
        }
        if let Some(ref registry) = self.local_metadata {
            let mut guard = registry.write().unwrap_or_else(|e| e.into_inner());
            if let Some(entry) = guard.remove(from_path.as_ref()) {
                guard.insert(to_path.to_string(), entry);
            } else if from_path != to_path {
                guard.remove(to_path.as_ref());
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
        compute_sha256: bool,
    ) -> Result<Option<String>, StorageError> {
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(part_path)
            .await?;
        let mut writer = tokio::io::BufWriter::with_capacity(1024 * 1024, file);
        let mut hasher = compute_sha256.then(Sha256::new);
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            if let Some(ref mut h) = hasher {
                h.update(&chunk);
            }
            writer.write_all(&chunk).await?;
        }
        writer.flush().await?;
        writer.get_ref().sync_all().await?;
        Ok(hasher.map(|h| hex::encode(h.finalize())))
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
            sha256: None,
            crc32c: None,
            md5: None,
        })
    }

    fn gcs_generic_error(message: String) -> StorageError {
        StorageError::ObjectStore(object_store::Error::Generic {
            store: "GCS",
            source: message.into(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::Barrier;

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_create_only_concurrent_barrier_race() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(
            ObjectStoreEngine::new(BackendConfig::Local {
                root: dir.path().to_string_lossy().into_owned(),
            })
            .expect("local engine"),
        );
        let racers = 8usize;
        let barrier = Arc::new(Barrier::new(racers));
        let mut handles = Vec::with_capacity(racers);

        for idx in 0..racers {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let mut attributes = HashMap::new();
                attributes.insert("owner".to_string(), format!("racer-{idx}"));
                let opts = PutConditionalOptions {
                    if_not_exists: true,
                    content_type: Some(format!("application/x-racer-{idx}")),
                    attributes: attributes.clone(),
                    ..Default::default()
                };
                let mut session = eng.create_multipart("nested/race.bin", opts).await?;
                let payload =
                    Bytes::from(format!("payload-from-racer-{idx:02}-{}", "x".repeat(64)));
                session.write_chunk(payload.clone()).await?;
                bar.wait().await;
                match session.complete_with_meta().await {
                    Ok(meta) => Ok((idx, payload, attributes, meta)),
                    Err(err) => {
                        let _ = session.abort().await;
                        Err(err)
                    }
                }
            }));
        }

        let mut winners = Vec::new();
        let mut already_exists_count = 0usize;
        for handle in handles {
            match handle.await.expect("racer task panicked") {
                Ok(win) => winners.push(win),
                Err(err) => {
                    assert!(
                        err.is_already_exists(),
                        "expected AlreadyExists error, got {err:?}"
                    );
                    already_exists_count += 1;
                }
            }
        }

        assert_eq!(winners.len(), 1, "expected exactly one winner");
        assert_eq!(already_exists_count, racers - 1);

        let (winner_idx, winner_payload, winner_attrs, winner_meta) = winners.pop().unwrap();
        let stored_bytes = engine.get("nested/race.bin").await.expect("get winner");
        assert_eq!(stored_bytes, winner_payload);

        let head = engine.head("nested/race.bin").await.expect("head winner");
        assert_eq!(head.size, winner_payload.len() as u64);
        assert_eq!(
            head.content_type.as_deref(),
            Some(format!("application/x-racer-{winner_idx}").as_str())
        );
        assert_eq!(head.attributes, winner_attrs);
        assert!(winner_meta.etag.is_some());
        assert_eq!(head.etag, winner_meta.etag);

        let entries: Vec<String> = std::fs::read_dir(dir.path().join("nested"))
            .expect("read_dir nested")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(entries, vec!["race.bin".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_create_only_if_none_match_star_race() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = Arc::new(
            ObjectStoreEngine::new(BackendConfig::Local {
                root: dir.path().to_string_lossy().into_owned(),
            })
            .expect("local engine"),
        );
        let racers = 6usize;
        let barrier = Arc::new(Barrier::new(racers));
        let mut handles = Vec::with_capacity(racers);

        for idx in 0..racers {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let opts = PutConditionalOptions {
                    if_not_exists: false,
                    if_none_match: Some("*".to_string()),
                    ..Default::default()
                };
                let mut session = eng.create_multipart("star-race.bin", opts).await?;
                let payload = Bytes::from(format!("star-racer-{idx:02}-{}", "y".repeat(32)));
                session.write_chunk(payload.clone()).await?;
                bar.wait().await;
                match session.complete_with_meta().await {
                    Ok(meta) => Ok((payload, meta)),
                    Err(err) => {
                        let _ = session.abort().await;
                        Err(err)
                    }
                }
            }));
        }

        let mut winners = Vec::new();
        let mut already_exists_count = 0usize;
        for handle in handles {
            match handle.await.expect("racer task panicked") {
                Ok(win) => winners.push(win),
                Err(err) => {
                    assert!(
                        err.is_already_exists(),
                        "expected AlreadyExists error, got {err:?}"
                    );
                    already_exists_count += 1;
                }
            }
        }

        assert_eq!(winners.len(), 1);
        assert_eq!(already_exists_count, racers - 1);
        let (winner_payload, _winner_meta) = winners.pop().unwrap();
        let stored = engine.get("star-race.bin").await.expect("get winner");
        assert_eq!(stored, winner_payload);

        let entries: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir root")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(entries, vec!["star-race.bin".to_string()]);
    }

    #[tokio::test]
    async fn local_create_only_fast_fails_on_existing_key() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = ObjectStoreEngine::new(BackendConfig::Local {
            root: dir.path().to_string_lossy().into_owned(),
        })
        .expect("local engine");

        engine
            .put("taken.bin", Bytes::from_static(b"existing-content"))
            .await
            .expect("seed taken.bin");

        let err = engine
            .create_multipart(
                "taken.bin",
                PutConditionalOptions {
                    if_not_exists: true,
                    ..Default::default()
                },
            )
            .await
            .err()
            .expect("expected fast-fail AlreadyExists");
        assert!(
            err.is_already_exists(),
            "expected AlreadyExists, got {err:?}"
        );

        let entries: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir root")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(entries, vec!["taken.bin".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn local_create_only_hides_staging_in_list_and_cleans_on_abort_and_drop() {
        let dir = tempfile::tempdir().expect("tempdir");
        let engine = ObjectStoreEngine::new(BackendConfig::Local {
            root: dir.path().to_string_lossy().into_owned(),
        })
        .expect("local engine");

        let mut aborted = engine
            .create_multipart(
                "aborted.bin",
                PutConditionalOptions {
                    if_not_exists: true,
                    ..Default::default()
                },
            )
            .await
            .expect("open aborted session");
        aborted
            .write_chunk(Bytes::from_static(b"aborted-chunk"))
            .await
            .expect("write aborted chunk");

        let drop_location = validate_object_path("dropped.bin").expect("valid path");
        let drop_dest = engine
            .local_fs
            .as_ref()
            .expect("local_fs")
            .path_to_filesystem(&drop_location)
            .expect("dest path");
        let mut dropped = LocalCreateOnlyUpload::open(drop_location, drop_dest)
            .await
            .expect("open direct upload");
        dropped
            .put_part(PutPayload::from(Bytes::from_static(b"dropped-chunk")))
            .await
            .expect("put_part dropped");

        let active_entries: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir active")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(active_entries.len(), 2, "expected 2 staging files on disk");
        assert!(active_entries.iter().all(|name| name.contains('#')));

        let listed = engine.list(None).await.expect("list while staging active");
        assert!(
            listed.is_empty(),
            "expected list(None) to hide active staging files, got {listed:?}"
        );

        aborted.abort().await.expect("abort session");
        drop(dropped);

        for _ in 0..50 {
            let remaining = std::fs::read_dir(dir.path())
                .expect("read_dir cleanup")
                .count();
            if remaining == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }

        let remaining: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir final")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert!(
            remaining.is_empty(),
            "expected zero destination or staging files after abort/drop, got {remaining:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn local_create_only_put_file_opts_large_race() {
        let dir = tempfile::tempdir().expect("tempdir");
        let src_dir = tempfile::tempdir().expect("src tempdir");
        let payload_len = 9 * 1024 * 1024;
        let payload_a = vec![b'A'; payload_len];
        let payload_b = vec![b'B'; payload_len];
        let src_a = src_dir.path().join("src_a.bin");
        let src_b = src_dir.path().join("src_b.bin");
        std::fs::write(&src_a, &payload_a).expect("write src_a");
        std::fs::write(&src_b, &payload_b).expect("write src_b");

        let engine = Arc::new(
            ObjectStoreEngine::new(BackendConfig::Local {
                root: dir.path().to_string_lossy().into_owned(),
            })
            .expect("local engine"),
        );
        let barrier = Arc::new(Barrier::new(2));

        let mut handles = Vec::new();
        for (marker, src_path) in [(b'A', src_a), (b'B', src_b)] {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            let src_str = src_path.to_string_lossy().into_owned();
            handles.push(tokio::spawn(async move {
                bar.wait().await;
                let opts = PutConditionalOptions {
                    if_not_exists: true,
                    ..Default::default()
                };
                eng.put_file_opts("large.bin", &src_str, opts)
                    .await
                    .map(|meta| (marker, meta))
            }));
        }

        let mut winners = Vec::new();
        let mut already_exists_count = 0usize;
        for handle in handles {
            match handle.await.expect("put_file_opts task panicked") {
                Ok(win) => winners.push(win),
                Err(err) => {
                    assert!(
                        err.is_already_exists(),
                        "expected AlreadyExists error, got {err:?}"
                    );
                    already_exists_count += 1;
                }
            }
        }

        assert_eq!(winners.len(), 1);
        assert_eq!(already_exists_count, 1);
        let (winner_marker, winner_meta) = winners.pop().unwrap();
        assert_eq!(winner_meta.size, payload_len as u64);

        let stored = engine.get("large.bin").await.expect("get large.bin");
        assert_eq!(stored.len(), payload_len);
        assert!(stored.iter().all(|&b| b == winner_marker));

        let entries: Vec<String> = std::fs::read_dir(dir.path())
            .expect("read_dir root")
            .map(|e| {
                e.expect("dir entry")
                    .file_name()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(entries, vec!["large.bin".to_string()]);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn memory_create_only_concurrent_barrier_race() {
        let engine =
            Arc::new(ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine"));
        let racers = 8usize;
        let barrier = Arc::new(Barrier::new(racers));
        let mut handles = Vec::with_capacity(racers);

        for idx in 0..racers {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let mut attributes = HashMap::new();
                attributes.insert("owner".to_string(), format!("mem-racer-{idx}"));
                let opts = PutConditionalOptions {
                    if_not_exists: true,
                    content_type: Some(format!("application/x-mem-racer-{idx}")),
                    attributes: attributes.clone(),
                    ..Default::default()
                };
                let mut session = eng.create_multipart("mem/race.bin", opts).await?;
                let part1 = Bytes::from(format!("part1-racer-{idx:02}-"));
                let part2 = Bytes::from(format!("part2-racer-{idx:02}-{}", "z".repeat(48)));
                let mut expected = Vec::with_capacity(part1.len() + part2.len());
                expected.extend_from_slice(&part1);
                expected.extend_from_slice(&part2);
                session.write_chunk(part1).await?;
                session.write_chunk(part2).await?;
                bar.wait().await;
                match session.complete_with_meta().await {
                    Ok(meta) => Ok((idx, Bytes::from(expected), attributes, meta)),
                    Err(err) => {
                        let _ = session.abort().await;
                        Err(err)
                    }
                }
            }));
        }

        let mut winners = Vec::new();
        let mut already_exists_count = 0usize;
        for handle in handles {
            match handle.await.expect("racer task panicked") {
                Ok(win) => winners.push(win),
                Err(err) => {
                    assert!(
                        err.is_already_exists(),
                        "expected AlreadyExists error, got {err:?}"
                    );
                    already_exists_count += 1;
                }
            }
        }

        assert_eq!(winners.len(), 1, "expected exactly one winner");
        assert_eq!(already_exists_count, racers - 1);

        let (winner_idx, winner_payload, winner_attrs, winner_meta) = winners.pop().unwrap();
        assert!(
            winner_meta
                .etag
                .as_deref()
                .is_some_and(|etag| !etag.is_empty()),
            "expected non-empty etag on winner meta"
        );

        let stored_bytes = engine.get("mem/race.bin").await.expect("get winner");
        assert_eq!(stored_bytes, winner_payload);

        let head = engine.head("mem/race.bin").await.expect("head winner");
        assert_eq!(head.size, winner_payload.len() as u64);
        assert_eq!(
            head.content_type.as_deref(),
            Some(format!("application/x-mem-racer-{winner_idx}").as_str())
        );
        assert_eq!(head.attributes, winner_attrs);
        assert_eq!(head.etag, winner_meta.etag);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn memory_create_only_if_none_match_star_race() {
        let engine =
            Arc::new(ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine"));
        let racers = 6usize;
        let barrier = Arc::new(Barrier::new(racers));
        let mut handles = Vec::with_capacity(racers);

        for idx in 0..racers {
            let eng = Arc::clone(&engine);
            let bar = Arc::clone(&barrier);
            handles.push(tokio::spawn(async move {
                let opts = PutConditionalOptions {
                    if_not_exists: false,
                    if_none_match: Some("*".to_string()),
                    ..Default::default()
                };
                let mut session = eng.create_multipart("mem-star-race.bin", opts).await?;
                let payload = Bytes::from(format!("mem-star-racer-{idx:02}-{}", "y".repeat(32)));
                session.write_chunk(payload.clone()).await?;
                bar.wait().await;
                match session.complete_with_meta().await {
                    Ok(meta) => Ok((payload, meta)),
                    Err(err) => {
                        let _ = session.abort().await;
                        Err(err)
                    }
                }
            }));
        }

        let mut winners = Vec::new();
        let mut already_exists_count = 0usize;
        for handle in handles {
            match handle.await.expect("racer task panicked") {
                Ok(win) => winners.push(win),
                Err(err) => {
                    assert!(
                        err.is_already_exists(),
                        "expected AlreadyExists error, got {err:?}"
                    );
                    already_exists_count += 1;
                }
            }
        }

        assert_eq!(winners.len(), 1);
        assert_eq!(already_exists_count, racers - 1);
        let (winner_payload, _winner_meta) = winners.pop().unwrap();
        let stored = engine.get("mem-star-race.bin").await.expect("get winner");
        assert_eq!(stored, winner_payload);
    }

    #[tokio::test]
    async fn memory_create_only_fast_fails_on_existing_key() {
        let engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
        let original = Bytes::from_static(b"existing-memory-content");
        engine
            .put("taken.bin", original.clone())
            .await
            .expect("seed taken.bin");

        let err = engine
            .create_multipart(
                "taken.bin",
                PutConditionalOptions {
                    if_not_exists: true,
                    ..Default::default()
                },
            )
            .await
            .err()
            .expect("expected fast-fail AlreadyExists");
        assert!(
            err.is_already_exists(),
            "expected AlreadyExists, got {err:?}"
        );

        let stored = engine.get("taken.bin").await.expect("get taken.bin");
        assert_eq!(stored, original);
    }

    #[tokio::test]
    async fn memory_create_only_abort_discards_without_creating() {
        let engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
        let mut session = engine
            .create_multipart(
                "aborted.bin",
                PutConditionalOptions {
                    if_not_exists: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create_multipart");
        session
            .write_chunk(Bytes::from_static(b"first-chunk-"))
            .await
            .expect("write chunk 1");
        session
            .write_chunk(Bytes::from_static(b"second-chunk"))
            .await
            .expect("write chunk 2");

        session.abort().await.expect("first abort");
        session.abort().await.expect("second abort");

        assert!(
            !engine.exists("aborted.bin").await.expect("exists check"),
            "aborted key should not exist"
        );
    }
}
