//! Native Oracle Cloud Infrastructure (OCI) Object Storage v2 REST backend and Cavage RSA-SHA256 signing.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::TryStreamExt;
use futures::stream::{BoxStream, StreamExt};
use object_store::path::Path as StorePath;
use object_store::{
    Attributes, CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    Result as StoreResult, UploadPart,
};
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs1v15::SigningKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::signature::{SignatureEncoding, Signer};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

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

    fn parse_private_key(&self) -> Result<RsaPrivateKey, StorageError> {
        RsaPrivateKey::from_pkcs8_pem(&self.private_key_pem)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(&self.private_key_pem))
            .map_err(|e| StorageError::Signing(format!("invalid OCI RSA private key PEM: {e}")))
    }
}

/// Signed HTTP headers produced by `sign_oci_cavage_request`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OciSignedHeaders {
    pub authorization: String,
    pub date: String,
    pub host: String,
    pub x_content_sha256: Option<String>,
    pub content_type: Option<String>,
    pub content_length: Option<String>,
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
    let parsed = Url::parse(url_str)
        .map_err(|e| StorageError::InvalidUrl(url_str.to_string(), e.to_string()))?;
    let host = match parsed.port() {
        Some(port) => format!("{}:{}", parsed.host_str().unwrap_or_default(), port),
        None => parsed.host_str().unwrap_or_default().to_string(),
    };
    let timestamp: DateTime<Utc> = Utc
        .timestamp_opt(now_unix, 0)
        .single()
        .ok_or_else(|| StorageError::Signing("invalid unix timestamp".to_string()))?;
    let date_hdr = timestamp.format("%a, %d %b %Y %H:%M:%S GMT").to_string();

    let method_lower = method.to_ascii_lowercase();
    let path_and_query = match parsed.query() {
        Some(q) if !q.is_empty() => format!("{}?{}", parsed.path(), q),
        _ => parsed.path().to_string(),
    };

    let is_write = matches!(method.to_ascii_uppercase().as_str(), "PUT" | "POST");
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

#[derive(Deserialize)]
struct OciListObjectsResponse {
    objects: Vec<OciObjectSummary>,
}

#[derive(Deserialize)]
struct OciObjectSummary {
    name: String,
    size: Option<u64>,
    etag: Option<String>,
}

/// Native OCI Object Storage REST API v2 implementation (`/n/{ns}/b/{bucket}/o/{object}`).
#[derive(Clone, Debug)]
pub struct OciObjectStore {
    namespace: String,
    bucket: String,
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
        let resolved_endpoint =
            endpoint.unwrap_or_else(|| format!("https://objectstorage.{region}.oraclecloud.com"));
        let client = reqwest::Client::builder()
            .no_proxy()
            .build()
            .map_err(|e| StorageError::Signing(format!("failed building OCI HTTP client: {e}")))?;
        Ok(Self {
            namespace,
            bucket,
            endpoint: resolved_endpoint.trim_end_matches('/').to_string(),
            credentials,
            client,
        })
    }

    pub fn object_url(&self, location: &StorePath) -> String {
        let encoded_name: String =
            url::form_urlencoded::byte_serialize(location.as_ref().as_bytes()).collect();
        format!(
            "{}/n/{}/b/{}/o/{}",
            self.endpoint, self.namespace, self.bucket, encoded_name
        )
    }

    pub fn par_url(&self, token_id: &str, location: &StorePath) -> String {
        let encoded_name: String =
            url::form_urlencoded::byte_serialize(location.as_ref().as_bytes()).collect();
        format!(
            "{}/p/{}/n/{}/b/{}/o/{}",
            self.endpoint, token_id, self.namespace, self.bucket, encoded_name
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
        _opts: PutOptions,
    ) -> StoreResult<PutResult> {
        let url_str = self.object_url(location);
        let bytes: Bytes = payload.into();
        let req = self
            .apply_signing(
                self.client.put(&url_str),
                "PUT",
                &url_str,
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
                source: format!("OCI PUT failed with HTTP {}", resp.status()).into(),
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
        _opts: PutMultipartOptions,
    ) -> StoreResult<Box<dyn MultipartUpload>> {
        let create_url = format!("{}/n/{}/b/{}/u", self.endpoint, self.namespace, self.bucket);
        let body_bytes = serde_json::to_vec(&OciCreateMultipartRequest {
            object: location.as_ref(),
        })
        .map_err(|e| object_store::Error::Generic {
            store: "OCI",
            source: Box::new(e),
        })?;
        let req = self
            .apply_signing(
                self.client.post(&create_url),
                "POST",
                &create_url,
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
        if options.head {
            let url_str = self.object_url(location);
            let req =
                self.apply_signing(self.client.head(&url_str), "HEAD", &url_str, None, None)?;
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
                    source: format!("OCI HEAD failed with HTTP {}", resp.status()).into(),
                });
            }
            let size = resp
                .headers()
                .get("content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let e_tag = resp
                .headers()
                .get("etag")
                .and_then(|v| v.to_str().ok())
                .map(ToString::to_string);
            let meta = ObjectMeta {
                location: location.clone(),
                last_modified: Utc::now(),
                size,
                e_tag,
                version: None,
            };
            let empty_stream = futures::stream::empty::<StoreResult<Bytes>>().boxed();
            return Ok(GetResult {
                payload: GetResultPayload::Stream(empty_stream),
                meta,
                range: 0..size,
                attributes: Attributes::default(),
            });
        }

        let url_str = self.object_url(location);
        let mut req = self.apply_signing(self.client.get(&url_str), "GET", &url_str, None, None)?;
        if let Some(object_store::GetRange::Bounded(ref range)) = options.range {
            req = req.header(
                "Range",
                format!("bytes={}-{}", range.start, range.end.saturating_sub(1)),
            );
        }
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
                source: format!("OCI GET failed with HTTP {}", resp.status()).into(),
            });
        }
        let e_tag = resp
            .headers()
            .get("etag")
            .and_then(|v| v.to_str().ok())
            .map(ToString::to_string);
        let body = resp
            .bytes()
            .await
            .map_err(|e| object_store::Error::Generic {
                store: "OCI",
                source: Box::new(e),
            })?;
        let size = body.len() as u64;
        let meta = ObjectMeta {
            location: location.clone(),
            last_modified: Utc::now(),
            size,
            e_tag,
            version: None,
        };
        let stream = futures::stream::once(async move { Ok(body) }).boxed();
        Ok(GetResult {
            payload: GetResultPayload::Stream(stream),
            meta,
            range: 0..size,
            attributes: Attributes::default(),
        })
    }

    async fn get_ranges(
        &self,
        location: &StorePath,
        ranges: &[Range<u64>],
    ) -> StoreResult<Vec<Bytes>> {
        let mut results = Vec::with_capacity(ranges.len());
        for range in ranges {
            let url_str = self.object_url(location);
            let req = self
                .apply_signing(self.client.get(&url_str), "GET", &url_str, None, None)?
                .header(
                    "Range",
                    format!("bytes={}-{}", range.start, range.end.saturating_sub(1)),
                );
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
                    source: format!("OCI GET range failed with HTTP {}", resp.status()).into(),
                });
            }
            let chunk = resp
                .bytes()
                .await
                .map_err(|e| object_store::Error::Generic {
                    store: "OCI",
                    source: Box::new(e),
                })?;
            results.push(chunk);
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
        let prefix_str = prefix.map(ToString::to_string);
        let fut = async move {
            let mut url_str = format!(
                "{}/n/{}/b/{}/o",
                store.endpoint, store.namespace, store.bucket
            );
            if let Some(ref p) = prefix_str {
                let encoded: String = url::form_urlencoded::byte_serialize(p.as_bytes()).collect();
                url_str.push_str(&format!("?prefix={encoded}&fields=name,size,etag"));
            } else {
                url_str.push_str("?fields=name,size,etag");
            }
            let req =
                store.apply_signing(store.client.get(&url_str), "GET", &url_str, None, None)?;
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
            let items: Vec<StoreResult<ObjectMeta>> = parsed
                .objects
                .into_iter()
                .map(|obj| {
                    Ok(ObjectMeta {
                        location: StorePath::from(obj.name),
                        last_modified: Utc::now(),
                        size: obj.size.unwrap_or(0),
                        e_tag: obj.etag,
                        version: None,
                    })
                })
                .collect();
            Ok::<_, object_store::Error>(futures::stream::iter(items))
        };
        futures::stream::once(fut).try_flatten().boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&StorePath>) -> StoreResult<ListResult> {
        let objects: Vec<ObjectMeta> = self.list(prefix).try_collect().await?;
        Ok(ListResult {
            common_prefixes: Vec::new(),
            objects,
        })
    }

    async fn copy_opts(
        &self,
        from: &StorePath,
        to: &StorePath,
        _options: CopyOptions,
    ) -> StoreResult<()> {
        let result = self.get_opts(from, GetOptions::default()).await?;
        let data = result.bytes().await?;
        self.put_opts(to, PutPayload::from(data), PutOptions::default())
            .await?;
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
            let encoded_name: String =
                url::form_urlencoded::byte_serialize(location.as_ref().as_bytes()).collect();
            let part_url = format!(
                "{}/n/{}/b/{}/u/{}?uploadId={}&uploadPartNum={}",
                store.endpoint, store.namespace, store.bucket, encoded_name, upload_id, part_num
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
                .expect("lock OCI multipart parts")
                .insert(part_num, etag);
            Ok(())
        })
    }

    async fn complete(&mut self) -> StoreResult<PutResult> {
        let encoded_name: String =
            url::form_urlencoded::byte_serialize(self.location.as_ref().as_bytes()).collect();
        let commit_url = format!(
            "{}/n/{}/b/{}/u/{}?uploadId={}",
            self.store.endpoint,
            self.store.namespace,
            self.store.bucket,
            encoded_name,
            self.upload_id
        );
        let snapshot: Vec<(usize, String)> = {
            let guard = self.parts.lock().expect("lock OCI multipart parts");
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
        let encoded_name: String =
            url::form_urlencoded::byte_serialize(self.location.as_ref().as_bytes()).collect();
        let abort_url = format!(
            "{}/n/{}/b/{}/u/{}?uploadId={}",
            self.store.endpoint,
            self.store.namespace,
            self.store.bucket,
            encoded_name,
            self.upload_id
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
