//! Native Oracle Cloud Infrastructure (OCI) Object Storage v2 REST backend, PAR URL builder, and Cavage RSA-SHA256 signing.

use std::collections::{BTreeSet, HashMap};
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::TryStreamExt;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as StorePath;
use object_store::{
    Attribute, AttributeValue, Attributes, CopyMode, CopyOptions, GetOptions, GetRange, GetResult,
    GetResultPayload, ListResult, MultipartUpload, ObjectMeta, ObjectStore, PutMode,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as StoreResult, UploadPart,
};
use rsa::RsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::signing::{
    SigningError, parse_rsa_private_key, rfc3986_encode, validate_expires_in_secs,
};
use crate::store::StorageError;

pub use crate::r2::{R2_UNIFORM_PART_SIZE, chunk_uniform_parts};

/// Base64 SHA-256 of an empty 0-byte body (`""`), required by OCI on 0-byte `POST`/`PUT` requests.
pub const EMPTY_BODY_SHA256_BASE64: &str = "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=";

/// Compute the Base64-encoded SHA-256 digest of `body`.
pub fn sha256_base64(body: &[u8]) -> String {
    if body.is_empty() {
        return EMPTY_BODY_SHA256_BASE64.to_string();
    }
    let mut hasher = Sha256::new();
    hasher.update(body);
    BASE64_STANDARD.encode(hasher.finalize())
}

/// Configuration for building an OCI Object Storage Pre-Authenticated Request (PAR) URL.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OciParConfig {
    pub region: String,
    pub namespace: String,
    pub bucket: String,
    pub par_token: String,
    pub endpoint: Option<String>,
}

fn normalize_oci_par_token(par_token: &str) -> Result<String, SigningError> {
    let trimmed = par_token.trim().trim_matches('/');
    let raw = trimmed
        .strip_prefix("p/")
        .unwrap_or(trimmed)
        .trim_matches('/');
    if raw.is_empty() {
        return Err(SigningError::InvalidKey(
            "OCI PAR token must not be empty".to_string(),
        ));
    }
    Ok(rfc3986_encode(raw, true))
}

/// Construct a deterministic OCI Object Storage Pre-Authenticated Request (PAR) URL with RFC 3986 key escaping and expiry bounds check.
pub fn build_oci_par_url(
    config: &OciParConfig,
    object_key: &str,
    expires_in_secs: u64,
) -> Result<String, SigningError> {
    validate_expires_in_secs(expires_in_secs)?;
    let encoded_token = normalize_oci_par_token(&config.par_token)?;
    let namespace = config.namespace.trim();
    if namespace.is_empty() {
        return Err(SigningError::InvalidInput(
            "empty OCI namespace".to_string(),
        ));
    }
    let bucket = config.bucket.trim();
    if bucket.is_empty() {
        return Err(SigningError::InvalidInput("empty OCI bucket".to_string()));
    }
    let trimmed_key = object_key.trim_start_matches('/');
    if trimmed_key.is_empty() {
        return Err(SigningError::InvalidInput(
            "empty OCI object_key".to_string(),
        ));
    }

    let base_endpoint = if let Some(ep) = config
        .endpoint
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        Url::parse(ep)
            .map_err(|e| SigningError::InvalidInput(format!("invalid OCI endpoint `{ep}`: {e}")))?;
        ep.trim_end_matches('/').to_string()
    } else {
        let region = config.region.trim();
        if region.is_empty() {
            return Err(SigningError::InvalidInput("empty OCI region".to_string()));
        }
        format!("https://objectstorage.{region}.oraclecloud.com")
    };

    let encoded_ns = rfc3986_encode(namespace, true);
    let encoded_bkt = rfc3986_encode(bucket, true);
    let encoded_key = rfc3986_encode(trimmed_key, true);

    Ok(format!(
        "{base_endpoint}/p/{encoded_token}/n/{encoded_ns}/b/{encoded_bkt}/o/{encoded_key}"
    ))
}

/// OCI `draft-cavage-http-signatures-08` credentials.
#[derive(Clone, Debug)]
pub struct OciCavageCredentials {
    pub tenancy_ocid: String,
    pub user_ocid: String,
    pub fingerprint: String,
    pub private_key_pem: String,
}

impl OciCavageCredentials {
    /// Return the OCI `keyId` string (`{tenancy_ocid}/{user_ocid}/{fingerprint}`).
    pub fn key_id(&self) -> String {
        if self.tenancy_ocid.starts_with("ST$") {
            return format!("{}/{}", self.tenancy_ocid, self.fingerprint);
        }
        format!(
            "{}/{}/{}",
            self.tenancy_ocid, self.user_ocid, self.fingerprint
        )
    }

    fn parse_private_key(&self) -> Result<RsaPrivateKey, SigningError> {
        parse_rsa_private_key(&self.private_key_pem)
    }
}

/// Signed HTTP headers produced by `sign_oci_request_headers` and `sign_oci_cavage_request`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OciSignedHeaders {
    pub authorization: String,
    pub date: String,
    pub host: String,
    pub x_content_sha256: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<String>,
}

/// Sign an OCI REST v2 HTTP request target and headers using `draft-cavage-http-signatures-08` (`rsa-sha256`).
pub fn sign_oci_request_headers(
    creds: &OciCavageCredentials,
    method: &str,
    url_str: &str,
    body: Option<&[u8]>,
    content_type: Option<&str>,
    now_unix: i64,
) -> Result<OciSignedHeaders, SigningError> {
    if creds.tenancy_ocid.trim().is_empty() || creds.fingerprint.trim().is_empty() {
        return Err(SigningError::InvalidKey(
            "missing OCI tenancy_ocid or fingerprint".to_string(),
        ));
    }
    if !creds.tenancy_ocid.starts_with("ST$") && creds.user_ocid.trim().is_empty() {
        return Err(SigningError::InvalidKey(
            "missing OCI user_ocid".to_string(),
        ));
    }
    let parsed = Url::parse(url_str)
        .map_err(|e| SigningError::InvalidInput(format!("invalid OCI URL `{url_str}`: {e}")))?;
    let host_only = parsed
        .host_str()
        .filter(|h| !h.is_empty())
        .ok_or_else(|| SigningError::InvalidInput("missing OCI host".to_string()))?;
    let host = match parsed.port() {
        Some(port)
            if (parsed.scheme() == "http" && port != 80)
                || (parsed.scheme() == "https" && port != 443) =>
        {
            format!("{host_only}:{port}")
        }
        _ => host_only.to_string(),
    };
    let timestamp: DateTime<Utc> = Utc
        .timestamp_opt(now_unix, 0)
        .single()
        .ok_or_else(|| SigningError::InvalidExpiry("invalid unix timestamp".to_string()))?;
    let date_hdr = timestamp.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    let method_lower = method.trim().to_ascii_lowercase();
    let path_and_query = match parsed.query() {
        Some(q) if !q.is_empty() => format!("{}?{}", parsed.path(), q),
        _ => parsed.path().to_string(),
    };

    let is_write = matches!(method.trim().to_ascii_uppercase().as_str(), "PUT" | "POST");
    let (headers_list, signing_string, x_sha, c_type, c_len) = if is_write {
        let payload = body.unwrap_or(&[]);
        let body_sha = sha256_base64(payload);
        let ct = content_type
            .unwrap_or("application/octet-stream")
            .to_string();
        let cl = payload.len().to_string();
        let signing_str = format!(
            "(request-target): {method_lower} {path_and_query}\ndate: {date_hdr}\nhost: {host}\nx-content-sha256: {body_sha}\ncontent-type: {ct}\ncontent-length: {cl}"
        );
        (
            "(request-target) date host x-content-sha256 content-type content-length",
            signing_str,
            Some(body_sha),
            Some(ct),
            Some(cl),
        )
    } else {
        let signing_str = format!(
            "(request-target): {method_lower} {path_and_query}\ndate: {date_hdr}\nhost: {host}"
        );
        ("(request-target) date host", signing_str, None, None, None)
    };

    let rsa_key = creds.parse_private_key()?;
    let signing_key = SigningKey::<Sha256>::new(rsa_key);
    let sig_bytes = signing_key.sign(signing_string.as_bytes()).to_vec();
    let sig_b64 = BASE64_STANDARD.encode(sig_bytes);

    let authorization = format!(
        "Signature version=\"1\",keyId=\"{}\",algorithm=\"rsa-sha256\",headers=\"{}\",signature=\"{}\"",
        creds.key_id(),
        headers_list,
        sig_b64
    );

    Ok(OciSignedHeaders {
        authorization,
        date: date_hdr,
        host,
        x_content_sha256: x_sha,
        content_type: c_type,
        content_length: c_len,
    })
}

/// Sign an OCI REST v2 HTTP request using `draft-cavage-http-signatures-08` (`rsa-sha256`).
pub fn sign_oci_cavage_request(
    creds: &OciCavageCredentials,
    method: &str,
    url_str: &str,
    body: Option<&[u8]>,
    content_type: Option<&str>,
    now_unix: i64,
) -> Result<OciSignedHeaders, StorageError> {
    sign_oci_request_headers(creds, method, url_str, body, content_type, now_unix)
        .map_err(StorageError::from)
}

#[derive(Serialize)]
struct OciCreateMultipartRequest<'a> {
    object: &'a str,
}

#[derive(Deserialize)]
struct OciCreateMultipartResponse {
    #[serde(rename = "uploadId")]
    upload_id: String,
}

#[derive(Serialize)]
struct OciCommitPartDetail<'a> {
    #[serde(rename = "partNum")]
    part_num: usize,
    etag: &'a str,
}

#[derive(Serialize)]
struct OciCommitMultipartRequest<'a> {
    #[serde(rename = "partsToCommit")]
    parts_to_commit: Vec<OciCommitPartDetail<'a>>,
}

#[derive(Serialize)]
struct OciCopyObjectRequest<'a> {
    #[serde(rename = "sourceObjectName")]
    source_object_name: &'a str,
    #[serde(rename = "destinationRegion")]
    destination_region: &'a str,
    #[serde(rename = "destinationNamespace")]
    destination_namespace: &'a str,
    #[serde(rename = "destinationBucket")]
    destination_bucket: &'a str,
    #[serde(rename = "destinationObjectName")]
    destination_object_name: &'a str,
    #[serde(
        rename = "destinationObjectIfNoneMatchETag",
        skip_serializing_if = "Option::is_none"
    )]
    destination_if_none_match_etag: Option<&'a str>,
}

#[derive(Deserialize)]
struct OciListObjectsResponse {
    #[serde(default)]
    objects: Vec<OciObjectSummary>,
    #[serde(default)]
    prefixes: Vec<String>,
    #[serde(rename = "nextStartWith", default)]
    next_start_with: Option<String>,
}

#[derive(Deserialize)]
struct OciObjectSummary {
    name: String,
    size: Option<u64>,
    etag: Option<String>,
    #[serde(rename = "timeCreated", default)]
    time_created: Option<String>,
    #[serde(rename = "timeModified", default)]
    time_modified: Option<String>,
}

static OCI_LOOPBACK_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .no_proxy()
        .pool_max_idle_per_host(64)
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(300))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
});

static OCI_HTTP_CLIENT: LazyLock<reqwest::Client> = LazyLock::new(|| {
    reqwest::Client::builder()
        .pool_max_idle_per_host(64)
        .pool_idle_timeout(Duration::from_secs(90))
        .connect_timeout(Duration::from_secs(10))
        .timeout(Duration::from_secs(300))
        .http2_keep_alive_interval(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new())
});

fn is_oci_loopback_endpoint(endpoint: &str) -> bool {
    let Ok(parsed) = Url::parse(endpoint) else {
        return false;
    };
    let Some(host) = parsed.host_str() else {
        return false;
    };
    matches!(host, "127.0.0.1" | "localhost" | "::1" | "[::1]") || host.ends_with(".localhost")
}

fn parse_http_timestamp(raw: Option<&str>) -> DateTime<Utc> {
    let Some(val) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Utc::now();
    };
    DateTime::parse_from_rfc2822(val)
        .or_else(|_| DateTime::parse_from_rfc3339(val))
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_else(|_| Utc::now())
}

fn parse_content_range_header(raw: &str) -> Option<(Range<u64>, u64)> {
    let rest = raw.trim().strip_prefix("bytes ")?;
    let (span, total_str) = rest.split_once('/')?;
    let (start_str, end_str) = span.split_once('-')?;
    let start = start_str.trim().parse::<u64>().ok()?;
    let end_inclusive = end_str.trim().parse::<u64>().ok()?;
    let total = total_str.trim().parse::<u64>().ok()?;
    let end_exclusive = end_inclusive.checked_add(1)?;
    (start <= end_exclusive).then_some((start..end_exclusive, total))
}

fn extract_oci_response_attributes(headers: &reqwest::header::HeaderMap) -> Attributes {
    let mut attrs = Attributes::new();
    if let Some(ct) = headers.get("content-type").and_then(|v| v.to_str().ok()) {
        attrs.insert(Attribute::ContentType, AttributeValue::from(ct.to_string()));
    }
    for (name, val) in headers {
        if let Some(meta_key) = name.as_str().strip_prefix("opc-meta-")
            && !meta_key.is_empty()
            && let Ok(val_str) = val.to_str()
        {
            attrs.insert(
                Attribute::Metadata(meta_key.to_string().into()),
                AttributeValue::from(val_str.to_string()),
            );
        }
    }
    attrs
}

fn apply_put_attributes_to_request(
    mut req: reqwest::RequestBuilder,
    attrs: &Attributes,
) -> reqwest::RequestBuilder {
    for (key, val) in attrs.iter() {
        if let Attribute::Metadata(meta_key) = key {
            let hdr_name = format!("opc-meta-{}", meta_key.as_ref());
            req = req.header(hdr_name, val.as_ref());
        }
    }
    req
}

fn parse_oci_store_path(raw: &str) -> StoreResult<StorePath> {
    StorePath::parse(raw).map_err(|e| object_store::Error::Generic {
        store: "OCI",
        source: Box::new(e),
    })
}

/// Native OCI Object Storage REST API v2 implementation (`/n/{ns}/b/{bucket}/o/{object}`).
#[derive(Clone, Debug)]
pub struct OciObjectStore {
    namespace: String,
    bucket: String,
    region: String,
    endpoint: String,
    credentials: Option<OciCavageCredentials>,
    client: reqwest::Client,
}

impl fmt::Display for OciObjectStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OciObjectStore(namespace={}, bucket={}, endpoint={})",
            self.namespace, self.bucket, self.endpoint
        )
    }
}

impl OciObjectStore {
    pub fn new(
        namespace: String,
        bucket: String,
        region: String,
        endpoint: Option<String>,
        credentials: Option<OciCavageCredentials>,
    ) -> Result<Self, StorageError> {
        let resolved_endpoint = endpoint
            .unwrap_or_else(|| format!("https://objectstorage.{region}.oraclecloud.com"))
            .trim_end_matches('/')
            .to_string();
        let client = if is_oci_loopback_endpoint(&resolved_endpoint) {
            OCI_LOOPBACK_CLIENT.clone()
        } else {
            OCI_HTTP_CLIENT.clone()
        };
        Ok(Self {
            namespace,
            bucket,
            region,
            endpoint: resolved_endpoint,
            credentials,
            client,
        })
    }

    pub fn object_url(&self, location: &StorePath) -> String {
        let encoded_name = rfc3986_encode(location.as_ref(), true);
        format!(
            "{}/n/{}/b/{}/o/{encoded_name}",
            self.endpoint, self.namespace, self.bucket
        )
    }

    pub fn par_url(&self, token_id: &str, location: &StorePath) -> String {
        let trimmed = token_id.trim().trim_matches('/');
        let raw_tok = trimmed
            .strip_prefix("p/")
            .unwrap_or(trimmed)
            .trim_matches('/');
        let encoded_tok = rfc3986_encode(raw_tok, true);
        let encoded_name = rfc3986_encode(location.as_ref(), true);
        format!(
            "{}/p/{encoded_tok}/n/{}/b/{}/o/{encoded_name}",
            self.endpoint, self.namespace, self.bucket
        )
    }

    fn apply_signing(
        &self,
        mut builder: reqwest::RequestBuilder,
        method: &str,
        url_str: &str,
        body: Option<&[u8]>,
        content_type: Option<&str>,
    ) -> StoreResult<reqwest::RequestBuilder> {
        if let Some(ref creds) = self.credentials {
            let signed = sign_oci_cavage_request(
                creds,
                method,
                url_str,
                body,
                content_type,
                Utc::now().timestamp(),
            )
            .map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
            builder = builder
                .header("Authorization", signed.authorization)
                .header("Date", signed.date)
                .header("Host", signed.host);
            if let Some(sha) = signed.x_content_sha256 {
                builder = builder.header("x-content-sha256", sha);
            }
            if let Some(ct) = signed.content_type {
                builder = builder.header("content-type", ct);
            }
            if let Some(cl) = signed.content_length {
                builder = builder.header("content-length", cl);
            }
        }
        Ok(builder)
    }
}

#[async_trait]
impl ObjectStore for OciObjectStore {
    async fn put_opts(
        &self,
        location: &StorePath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let url_str = self.object_url(location);
        let bytes: Bytes = payload.into();
        let content_type = opts
            .attributes
            .get(&Attribute::ContentType)
            .map(AsRef::as_ref)
            .unwrap_or("application/octet-stream");
        let mut req = self.apply_signing(
            self.client.put(&url_str),
            "PUT",
            &url_str,
            Some(&bytes),
            Some(content_type),
        )?;
        if self.credentials.is_none() {
            req = req.header("content-type", content_type);
        }
        req = apply_put_attributes_to_request(req, &opts.attributes);

        let is_create_only = matches!(opts.mode, PutMode::Create);
        match opts.mode {
            PutMode::Overwrite => {}
            PutMode::Create => {
                req = req.header("if-none-match", "*");
            }
            PutMode::Update(ref uv) => {
                let Some(match_token) = uv
                    .e_tag
                    .as_deref()
                    .or(uv.version.as_deref())
                    .filter(|s| !s.is_empty())
                else {
                    return Err(object_store::Error::Precondition {
                        path: location.to_string(),
                        source: "OCI PutMode::Update requires non-empty e_tag or version".into(),
                    });
                };
                req = req.header("if-match", match_token);
            }
        }

        let resp = req
            .body(bytes)
            .send()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
        let status = resp.status();
        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            return if is_create_only {
                Err(object_store::Error::AlreadyExists {
                    path: location.to_string(),
                    source: "OCI 412 object already exists".into(),
                })
            } else {
                Err(object_store::Error::Precondition {
                    path: location.to_string(),
                    source: "OCI 412 precondition failed".into(),
                })
            };
        }
        if !status.is_success() {
            return Err(object_store::Error::Generic {
                store: "OCI",
                source: format!("OCI PUT failed with HTTP {status}").into(),
            });
        }
        let e_tag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        let version = resp
            .headers()
            .get("opc-version-id")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        Ok(PutResult { e_tag, version })
    }

    async fn put_multipart_opts(
        &self,
        location: &StorePath,
        opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        let create_url = format!("{}/n/{}/b/{}/u", self.endpoint, self.namespace, self.bucket);
        let body_bytes = serde_json::to_vec(&OciCreateMultipartRequest {
            object: location.as_ref(),
        })
        .map_err(|e| object_store::Error::Generic {
            store: "OCI",
            source: Box::new(e),
        })?;
        let mut req = self.apply_signing(
            self.client.post(&create_url),
            "POST",
            &create_url,
            Some(&body_bytes),
            Some("application/json"),
        )?;
        req = apply_put_attributes_to_request(req, &opts.attributes);
        let resp = req
            .body(body_bytes)
            .send()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
        if !resp.status().is_success() {
            return Err(object_store::Error::Generic {
                store: "OCI",
                source: format!("OCI multipart create failed with HTTP {}", resp.status()).into(),
            });
        }
        let parsed: OciCreateMultipartResponse =
            resp.json()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "OCI",
                    source: Box::new(e),
                })?;
        Ok(Box::new(OciMultipartUpload {
            store: self.clone(),
            location: location.clone(),
            upload_id: parsed.upload_id,
            next_part: 1,
            parts: Arc::new(Mutex::new(HashMap::new())),
        }))
    }

    async fn get_opts(&self, location: &StorePath, options: GetOptions) -> StoreResult<GetResult> {
        let url_str = self.object_url(location);
        let (method, builder) = if options.head {
            ("HEAD", self.client.head(&url_str))
        } else {
            ("GET", self.client.get(&url_str))
        };
        let mut req = self.apply_signing(builder, method, &url_str, None, None)?;

        let requested_bounded = match options.range {
            Some(GetRange::Bounded(ref range)) => {
                if range.start >= range.end {
                    return Ok(GetResult {
                        payload: GetResultPayload::Stream(
                            futures::stream::empty::<StoreResult<Bytes>>().boxed(),
                        ),
                        meta: ObjectMeta {
                            location: location.clone(),
                            last_modified: Utc::now(),
                            size: range.start,
                            e_tag: None,
                            version: None,
                        },
                        range: range.start..range.start,
                        attributes: Attributes::default(),
                    });
                }
                req = req.header("Range", format!("bytes={}-{}", range.start, range.end - 1));
                Some(range.clone())
            }
            Some(GetRange::Offset(offset)) => {
                req = req.header("Range", format!("bytes={offset}-"));
                None
            }
            Some(GetRange::Suffix(suffix)) => {
                req = req.header("Range", format!("bytes=-{suffix}"));
                None
            }
            None => None,
        };

        if let Some(ref etag) = options.if_match {
            req = req.header("if-match", etag);
        }
        if let Some(ref etag) = options.if_none_match {
            req = req.header("if-none-match", etag);
        }
        if let Some(dt) = options.if_modified_since {
            req = req.header("if-modified-since", dt.to_rfc2822());
        }
        if let Some(dt) = options.if_unmodified_since {
            req = req.header("if-unmodified-since", dt.to_rfc2822());
        }

        let resp = req.send().await.map_err(|e| object_store::Error::Generic {
            store: "OCI",
            source: Box::new(e),
        })?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(object_store::Error::NotFound {
                path: location.to_string(),
                source: format!("OCI {method} 404 Not Found").into(),
            });
        }
        if status == reqwest::StatusCode::NOT_MODIFIED {
            return Err(object_store::Error::NotModified {
                path: location.to_string(),
                source: format!("OCI {method} 304 Not Modified").into(),
            });
        }
        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            return Err(object_store::Error::Precondition {
                path: location.to_string(),
                source: format!("OCI {method} 412 Precondition Failed").into(),
            });
        }
        if !status.is_success() {
            return Err(object_store::Error::Generic {
                store: "OCI",
                source: format!("OCI {method} failed with HTTP {status}").into(),
            });
        }

        let headers = resp.headers();
        let content_len = headers
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok());
        let parsed_cr = headers
            .get("content-range")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_content_range_header);
        let e_tag = headers
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        let version = headers
            .get("opc-version-id")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        let last_modified =
            parse_http_timestamp(headers.get("last-modified").and_then(|v| v.to_str().ok()));
        let attributes = extract_oci_response_attributes(headers);

        let (range, total_size) = if let Some((cr_range, cr_total)) = parsed_cr {
            (cr_range, cr_total)
        } else if let Some(bounded) = requested_bounded {
            let len = content_len.unwrap_or_else(|| bounded.end.saturating_sub(bounded.start));
            let end = bounded.start.saturating_add(len);
            (bounded.start..end, end)
        } else {
            let sz = content_len.unwrap_or(0);
            (0..sz, sz)
        };

        let meta = ObjectMeta {
            location: location.clone(),
            last_modified,
            size: total_size,
            e_tag,
            version,
        };

        if options.head {
            let empty_stream = futures::stream::empty::<StoreResult<Bytes>>().boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(empty_stream),
                meta,
                range,
                attributes,
            });
        }

        let stream = resp
            .bytes_stream()
            .map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })
            .boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range,
            attributes,
        })
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<u64>],
    ) -> StoreResult<Vec<Bytes>> {
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges {
            let res = self
                .get_opts(
                    location,
                    GetOptions {
                        range: Some(GetRange::Bounded(range.clone())),
                        ..Default::default()
                    },
                )
                .await?;
            results.push(res.bytes().await?);
        }
        Ok(results)
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, StoreResult<StorePath>>,
    ) -> BoxStream<'static, StoreResult<StorePath>> {
        let store = self.clone();
        locations
            .and_then(move |location| {
                let s = store.clone();
                async move {
                    let url_str = s.object_url(&location);
                    let req =
                        s.apply_signing(s.client.delete(&url_str), "DELETE", &url_str, None, None)?;
                    let resp = req.send().await.map_err(|e| object_store::Error::Generic {
                        store: "OCI",
                        source: Box::new(e),
                    })?;
                    if resp.status() == reqwest::StatusCode::NOT_FOUND {
                        return Err(object_store::Error::NotFound {
                            path: location.to_string(),
                            source: "OCI 404 Not Found".into(),
                        });
                    }
                    if !resp.status().is_success() {
                        return Err(object_store::Error::Generic {
                            store: "OCI",
                            source: format!("OCI DELETE failed with HTTP {}", resp.status()).into(),
                        });
                    }
                    Ok(location)
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&StorePath>) -> BoxStream<'static, StoreResult<ObjectMeta>> {
        let store = self.clone();
        let prefix_str = prefix.map(|p| {
            let raw = p.as_ref();
            if raw.is_empty() {
                String::new()
            } else {
                format!("{}/", raw.trim_end_matches('/'))
            }
        });

        struct ListPageState {
            store: OciObjectStore,
            prefix: Option<String>,
            next_start_with: Option<String>,
            done: bool,
        }

        let initial = ListPageState {
            store,
            prefix: prefix_str.filter(|s| !s.is_empty()),
            next_start_with: None,
            done: false,
        };

        futures::stream::try_unfold(initial, |mut state| async move {
            if state.done {
                return Ok::<_, object_store::Error>(None);
            }
            let mut query_parts =
                vec!["fields=name,size,etag,timeCreated,timeModified".to_string()];
            if let Some(ref p) = state.prefix {
                let enc: String = url::form_urlencoded::byte_serialize(p.as_bytes()).collect();
                query_parts.push(format!("prefix={enc}"));
            }
            if let Some(ref start) = state.next_start_with {
                let enc: String = url::form_urlencoded::byte_serialize(start.as_bytes()).collect();
                query_parts.push(format!("start={enc}"));
            }
            let url_str = format!(
                "{}/n/{}/b/{}/o?{}",
                state.store.endpoint,
                state.store.namespace,
                state.store.bucket,
                query_parts.join("&")
            );
            let req = state.store.apply_signing(
                state.store.client.get(&url_str),
                "GET",
                &url_str,
                None,
                None,
            )?;
            let resp = req.send().await.map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
            if !resp.status().is_success() {
                return Err(object_store::Error::Generic {
                    store: "OCI",
                    source: format!("OCI LIST failed with HTTP {}", resp.status()).into(),
                });
            }
            let parsed: OciListObjectsResponse =
                resp.json()
                    .await
                    .map_err(|e| object_store::Error::Generic {
                        store: "OCI",
                        source: Box::new(e),
                    })?;
            match parsed.next_start_with.filter(|s| !s.is_empty()) {
                Some(next) => state.next_start_with = Some(next),
                None => {
                    state.next_start_with = None;
                    state.done = true;
                }
            }
            let mut page_items = Vec::with_capacity(parsed.objects.len());
            for obj in parsed.objects {
                let loc = parse_oci_store_path(&obj.name)?;
                let ts = parse_http_timestamp(
                    obj.time_modified.as_deref().or(obj.time_created.as_deref()),
                );
                page_items.push(Ok(ObjectMeta {
                    location: loc,
                    last_modified: ts,
                    size: obj.size.unwrap_or(0),
                    e_tag: obj.etag,
                    version: None,
                }));
            }
            Ok(Some((futures::stream::iter(page_items), state)))
        })
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        let normalized_prefix = prefix.and_then(|p| {
            let raw = p.as_ref().trim_matches('/');
            (!raw.is_empty()).then(|| format!("{raw}/"))
        });
        let mut objects = Vec::new();
        let mut common_prefixes_set = BTreeSet::new();
        let mut next_start_with: Option<String> = None;

        loop {
            let mut query_parts = vec![
                "delimiter=%2F".to_string(),
                "fields=name,size,etag,timeCreated,timeModified".to_string(),
            ];
            if let Some(ref p) = normalized_prefix {
                let enc: String = url::form_urlencoded::byte_serialize(p.as_bytes()).collect();
                query_parts.push(format!("prefix={enc}"));
            }
            if let Some(ref start) = next_start_with {
                let enc: String = url::form_urlencoded::byte_serialize(start.as_bytes()).collect();
                query_parts.push(format!("start={enc}"));
            }
            let url_str = format!(
                "{}/n/{}/b/{}/o?{}",
                self.endpoint,
                self.namespace,
                self.bucket,
                query_parts.join("&")
            );
            let req = self.apply_signing(self.client.get(&url_str), "GET", &url_str, None, None)?;
            let resp = req.send().await.map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
            if !resp.status().is_success() {
                return Err(object_store::Error::Generic {
                    store: "OCI",
                    source: format!("OCI LIST with delimiter failed with HTTP {}", resp.status())
                        .into(),
                });
            }
            let parsed: OciListObjectsResponse =
                resp.json()
                    .await
                    .map_err(|e| object_store::Error::Generic {
                        store: "OCI",
                        source: Box::new(e),
                    })?;
            for obj in parsed.objects {
                let loc = parse_oci_store_path(&obj.name)?;
                let ts = parse_http_timestamp(
                    obj.time_modified.as_deref().or(obj.time_created.as_deref()),
                );
                objects.push(ObjectMeta {
                    location: loc,
                    last_modified: ts,
                    size: obj.size.unwrap_or(0),
                    e_tag: obj.etag,
                    version: None,
                });
            }
            for pref in parsed.prefixes {
                let cleaned = pref.trim_matches('/');
                if !cleaned.is_empty() {
                    common_prefixes_set.insert(parse_oci_store_path(cleaned)?);
                }
            }
            match parsed.next_start_with.filter(|s| !s.is_empty()) {
                Some(next) => next_start_with = Some(next),
                None => break,
            }
        }

        Ok(ListResult {
            common_prefixes: common_prefixes_set.into_iter().collect(),
            objects,
        })
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        options: CopyOptions,
    ) -> StoreResult<()> {
        let copy_url = format!(
            "{}/n/{}/b/{}/actions/copyObject",
            self.endpoint, self.namespace, self.bucket
        );
        let is_create_only = matches!(options.mode, CopyMode::Create);
        let body_bytes = serde_json::to_vec(&OciCopyObjectRequest {
            source_object_name: from.as_ref(),
            destination_region: &self.region,
            destination_namespace: &self.namespace,
            destination_bucket: &self.bucket,
            destination_object_name: to.as_ref(),
            destination_if_none_match_etag: is_create_only.then_some("*"),
        })
        .map_err(|e| object_store::Error::Generic {
            store: "OCI",
            source: Box::new(e),
        })?;
        let mut req = self.apply_signing(
            self.client.post(&copy_url),
            "POST",
            &copy_url,
            Some(&body_bytes),
            Some("application/json"),
        )?;
        if self.credentials.is_none() {
            req = req.header("content-type", "application/json");
        }
        let resp = req
            .body(body_bytes)
            .send()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
        let status = resp.status();
        if status == reqwest::StatusCode::NOT_FOUND {
            return Err(object_store::Error::NotFound {
                path: from.to_string(),
                source: "OCI copyObject 404 Not Found".into(),
            });
        }
        if status == reqwest::StatusCode::PRECONDITION_FAILED {
            return if is_create_only {
                Err(object_store::Error::AlreadyExists {
                    path: to.to_string(),
                    source: "OCI copyObject 412 destination already exists".into(),
                })
            } else {
                Err(object_store::Error::Precondition {
                    path: to.to_string(),
                    source: "OCI copyObject 412 precondition failed".into(),
                })
            };
        }
        if !status.is_success() {
            return Err(object_store::Error::Generic {
                store: "OCI",
                source: format!("OCI copyObject failed with HTTP {status}").into(),
            });
        }
        Ok(())
    }
}

#[derive(Debug)]
struct OciMultipartUpload {
    store: OciObjectStore,
    location: StorePath,
    upload_id: String,
    next_part: usize,
    parts: Arc<Mutex<HashMap<usize, String>>>,
}

#[async_trait]
impl MultipartUpload for OciMultipartUpload {
    fn put_part(&mut self, data: PutPayload) -> UploadPart {
        let part_num = self.next_part;
        self.next_part += 1;
        let store = self.store.clone();
        let location = self.location.clone();
        let upload_id = self.upload_id.clone();
        let parts_map = Arc::clone(&self.parts);
        let bytes: Bytes = data.into();

        Box::pin(async move {
            let encoded_name = rfc3986_encode(location.as_ref(), true);
            let part_url = format!(
                "{}/n/{}/b/{}/u/{encoded_name}?uploadId={upload_id}&uploadPartNum={part_num}",
                store.endpoint, store.namespace, store.bucket
            );
            let req = store
                .apply_signing(
                    store.client.put(&part_url),
                    "PUT",
                    &part_url,
                    Some(&bytes),
                    Some("application/octet-stream"),
                )?
                .body(bytes);
            let resp = req.send().await.map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
            if !resp.status().is_success() {
                return Err(object_store::Error::Generic {
                    store: "OCI",
                    source: format!("OCI put_part failed with HTTP {}", resp.status()).into(),
                });
            }
            let etag = resp
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("part-etag")
                .to_string();
            parts_map
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(part_num, etag);
            Ok(())
        })
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        let encoded_name = rfc3986_encode(self.location.as_ref(), true);
        let commit_url = format!(
            "{}/n/{}/b/{}/u/{encoded_name}?uploadId={}",
            self.store.endpoint, self.store.namespace, self.store.bucket, self.upload_id
        );
        let snapshot: Vec<(usize, String)> = {
            let guard = self.parts.lock().unwrap_or_else(|e| e.into_inner());
            let mut entries: Vec<(usize, String)> =
                guard.iter().map(|(k, v)| (*k, v.clone())).collect();
            entries.sort_by_key(|(k, _)| *k);
            entries
        };
        let commit_req = OciCommitMultipartRequest {
            parts_to_commit: snapshot
                .iter()
                .map(|(num, etag)| OciCommitPartDetail {
                    part_num: *num,
                    etag: etag.as_str(),
                })
                .collect(),
        };
        let body_bytes =
            serde_json::to_vec(&commit_req).map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
        let req = self
            .store
            .apply_signing(
                self.store.client.post(&commit_url),
                "POST",
                &commit_url,
                Some(&body_bytes),
                Some("application/json"),
            )?
            .body(body_bytes);
        let resp = req.send().await.map_err(|e| object_store::Error::Generic {
            store: "OCI",
            source: Box::new(e),
        })?;
        if !resp.status().is_success() {
            return Err(object_store::Error::Generic {
                store: "OCI",
                source: format!("OCI commit multipart failed with HTTP {}", resp.status()).into(),
            });
        }
        let e_tag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        Ok(PutResult {
            e_tag,
            version: None,
        })
    }

    async fn abort(&mut self) -> StoreResult<()> {
        let encoded_name = rfc3986_encode(self.location.as_ref(), true);
        let abort_url = format!(
            "{}/n/{}/b/{}/u/{encoded_name}?uploadId={}",
            self.store.endpoint, self.store.namespace, self.store.bucket, self.upload_id
        );
        let req = self.store.apply_signing(
            self.store.client.delete(&abort_url),
            "DELETE",
            &abort_url,
            None,
            None,
        )?;
        let _ = req.send().await;
        Ok(())
    }
}
