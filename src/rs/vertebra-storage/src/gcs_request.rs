//! Paired data and metadata fences for opaque GCS XML ETags.

use std::sync::Arc;

use http::header::AUTHORIZATION;
use http::{HeaderMap, HeaderValue, Method};
use object_store::client::{HttpRequest, HttpRequestBody, HttpResponse};
use object_store::gcp::GoogleCloudStorage;
use object_store::path::Path as StorePath;
use object_store::{Attribute, Error, PutOptions, PutPayload, PutResult};

use crate::cloud_request::CloudRequestClient;
use crate::conditional::GcsStageConfig;
use crate::store::StorageError;

#[derive(Clone, Debug)]
pub(crate) struct GcsObjectVersion {
    pub(crate) generation: String,
    pub(crate) metageneration: String,
}

#[derive(Clone, Debug)]
pub(crate) struct GcsObjectSnapshot {
    pub(crate) xml_etag: String,
    pub(crate) version: GcsObjectVersion,
}

pub(crate) struct GcsRequestClient {
    store: Arc<GoogleCloudStorage>,
    pub(crate) config: Arc<GcsStageConfig>,
    pub(crate) transport: Arc<CloudRequestClient>,
}

impl std::fmt::Debug for GcsRequestClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("GcsRequestClient")
    }
}

impl GcsRequestClient {
    pub(crate) fn new(
        store: Arc<GoogleCloudStorage>,
        config: Arc<GcsStageConfig>,
    ) -> object_store::Result<Self> {
        let transport =
            Arc::new(CloudRequestClient::new(&config.client_options).map_err(cloud_error)?);
        Ok(Self {
            store,
            config,
            transport,
        })
    }

    pub(crate) async fn execute(
        &self,
        mut request: HttpRequest,
        authenticated: bool,
    ) -> object_store::Result<HttpResponse> {
        if authenticated && !self.config.skip_signature {
            let credential = self
                .store
                .credentials()
                .get_credential()
                .await
                .map_err(|error| {
                    let source = "GCS credentials unavailable".into();
                    match error {
                        Error::PermissionDenied { .. } => Error::PermissionDenied {
                            path: String::new(),
                            source,
                        },
                        Error::Unauthenticated { .. } => Error::Unauthenticated {
                            path: String::new(),
                            source,
                        },
                        _ => failure("credentials unavailable"),
                    }
                })?;
            if !credential.bearer.is_empty() {
                let mut value = HeaderValue::from_str(&format!("Bearer {}", credential.bearer))
                    .map_err(|_| failure("invalid credential header"))?;
                value.set_sensitive(true);
                request.headers_mut().insert(AUTHORIZATION, value);
            }
        }
        self.transport
            .execute(request, "GCS request")
            .await
            .map_err(cloud_error)
    }

    fn request(
        &self,
        method: Method,
        path: &StorePath,
        expected: Option<&GcsObjectVersion>,
    ) -> object_store::Result<http::request::Builder> {
        let mut url = self.config.base.clone();
        url.path_segments_mut()
            .map_err(|_| failure("invalid endpoint"))?
            .pop_if_empty()
            .push(&self.config.bucket)
            .push(path.as_ref());
        let mut builder = http::Request::builder().method(method.clone());
        if let Some(version) = expected {
            positive(&version.generation)?;
            positive(&version.metageneration)?;
            if method == Method::HEAD {
                url.query_pairs_mut()
                    .append_pair("generation", &version.generation);
            }
            builder = builder
                .header("x-goog-if-generation-match", &version.generation)
                .header("x-goog-if-metageneration-match", &version.metageneration);
        }
        Ok(builder.uri(url.as_str()))
    }

    pub(crate) async fn head_snapshot(
        &self,
        path: &StorePath,
        expected: Option<&GcsObjectVersion>,
    ) -> object_store::Result<Option<GcsObjectSnapshot>> {
        let request = self
            .request(Method::HEAD, path, expected)?
            .body(HttpRequestBody::empty())
            .map_err(|_| failure("invalid HEAD request"))?;
        let response = self.execute(request, true).await?;
        if response.status() == http::StatusCode::NOT_FOUND && expected.is_none() {
            return Ok(None);
        }
        check_status(response.status())?;
        let headers = response.headers();
        Ok(Some(GcsObjectSnapshot {
            xml_etag: text(headers, "etag")?,
            version: GcsObjectVersion {
                generation: positive(&text(headers, "x-goog-generation")?)?,
                metageneration: positive(&text(headers, "x-goog-metageneration")?)?,
            },
        }))
    }

    pub(crate) async fn put_snapshot(
        &self,
        path: &StorePath,
        payload: PutPayload,
        options: PutOptions,
        expected: Option<&GcsObjectSnapshot>,
    ) -> object_store::Result<PutResult> {
        let mut builder = self
            .request(Method::PUT, path, expected.map(|value| &value.version))?
            .header("content-length", payload.content_length());
        if expected.is_none() {
            builder = builder.header("x-goog-if-generation-match", "0");
        }
        let mut content_type = false;
        for (attribute, value) in &options.attributes {
            let name = match attribute {
                Attribute::CacheControl => "cache-control".into(),
                Attribute::ContentDisposition => "content-disposition".into(),
                Attribute::ContentEncoding => "content-encoding".into(),
                Attribute::ContentLanguage => "content-language".into(),
                Attribute::ContentType => {
                    content_type = true;
                    "content-type".into()
                }
                Attribute::StorageClass => "x-goog-storage-class".into(),
                Attribute::Metadata(name) => format!("x-goog-meta-{name}"),
                _ => return Err(failure("unsupported attribute")),
            };
            builder = builder.header(name, value.as_ref());
        }
        if !content_type {
            builder = builder.header(
                "content-type",
                self.config
                    .client_options
                    .get_content_type(path)
                    .unwrap_or("application/octet-stream"),
            );
        }
        let mut request = builder
            .body(HttpRequestBody::from(payload))
            .map_err(|_| failure("invalid PUT request"))?;
        *request.extensions_mut() = options.extensions;
        let response = self.execute(request, true).await?;
        check_status(response.status())?;
        Ok(PutResult {
            e_tag: Some(text(response.headers(), "etag")?),
            version: response
                .headers()
                .contains_key("x-goog-generation")
                .then(|| text(response.headers(), "x-goog-generation").and_then(|v| positive(&v)))
                .transpose()?,
        })
    }
}

fn failure(message: &'static str) -> Error {
    Error::Generic {
        store: "GCS requests",
        source: message.into(),
    }
}

fn cloud_error(error: StorageError) -> Error {
    match error {
        StorageError::ObjectStore(error) => error,
        _ => failure("transport configuration unavailable"),
    }
}

fn text(headers: &HeaderMap, name: &'static str) -> object_store::Result<String> {
    headers
        .get(name)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .ok_or_else(|| failure("missing or invalid response metadata"))
}

fn positive(value: &str) -> object_store::Result<String> {
    if value.bytes().all(|v| v.is_ascii_digit()) && value.parse::<u64>().is_ok_and(|v| v > 0) {
        Ok(value.to_owned())
    } else {
        Err(failure("invalid object version"))
    }
}

fn check_status(status: http::StatusCode) -> object_store::Result<()> {
    if status.is_success() {
        return Ok(());
    }
    let source = format!("GCS request HTTP {}", status.as_u16()).into();
    Err(match status.as_u16() {
        401 => Error::Unauthenticated {
            path: String::new(),
            source,
        },
        403 => Error::PermissionDenied {
            path: String::new(),
            source,
        },
        412 => Error::Precondition {
            path: String::new(),
            source,
        },
        _ => Error::Generic {
            store: "GCS requests",
            source,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::JoinHandle;
    use std::time::Duration;

    use async_trait::async_trait;
    use bytes::Bytes;
    use object_store::client::CredentialProvider;
    use object_store::gcp::{GcpCredential, GoogleCloudStorageBuilder};
    use object_store::path::Path as StorePath;
    use object_store::{ClientOptions, ObjectStoreExt};

    use crate::store::build_gcs_store;

    struct Responder {
        base: String,
        requests: Arc<Mutex<Vec<String>>>,
        stop: Arc<AtomicBool>,
        worker: Option<JoinHandle<()>>,
    }

    impl Responder {
        fn new(handler: impl Fn(&str) -> Option<String> + Send + 'static) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let requests = Arc::new(Mutex::new(Vec::new()));
            let stop = Arc::new(AtomicBool::new(false));
            let (captured, done) = (requests.clone(), stop.clone());
            let worker = std::thread::spawn(move || {
                while !done.load(Ordering::Acquire) {
                    let Ok((mut socket, _)) = listener.accept() else {
                        std::thread::sleep(Duration::from_millis(1));
                        continue;
                    };
                    socket
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    socket
                        .set_write_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut data = Vec::new();
                    loop {
                        let mut chunk = [0; 1024];
                        let n = socket.read(&mut chunk).unwrap();
                        assert!(n > 0, "incomplete HTTP request");
                        data.extend_from_slice(&chunk[..n]);
                        assert!(data.len() < 65536);
                        if let Some(end) = data.windows(4).position(|v| v == b"\r\n\r\n") {
                            let length = header(
                                std::str::from_utf8(&data[..end]).unwrap(),
                                "content-length",
                            )
                            .map(|v| v.parse::<usize>().unwrap())
                            .unwrap_or(0);
                            if data.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    let request = String::from_utf8(data).unwrap();
                    captured.lock().unwrap().push(request.clone());
                    if let Some(response) = handler(&request) {
                        socket.write_all(response.as_bytes()).unwrap();
                    }
                }
            });
            Self {
                base,
                requests,
                stop,
                worker: Some(worker),
            }
        }

        fn captured(&self) -> Vec<String> {
            self.requests.lock().unwrap().clone()
        }
    }

    impl Drop for Responder {
        fn drop(&mut self) {
            self.stop.store(true, Ordering::Release);
            self.worker.take().unwrap().join().unwrap();
        }
    }

    fn header<'a>(request: &'a str, key: &str) -> Option<&'a str> {
        request
            .split("\r\n\r\n")
            .next()
            .unwrap()
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case(key).then(|| value.trim())
            })
    }

    fn reply(status: u16, headers: &str, body: &str) -> String {
        format!(
            "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n{headers}\r\n{body}",
            body.len()
        )
    }

    fn snapshot_headers(tag: &str, generation: &str, meta: &str) -> String {
        format!(
            "ETag: {tag}\r\nx-goog-generation: {generation}\r\nx-goog-metageneration: {meta}\r\nLast-Modified: Wed, 01 Jan 2025 00:00:00 GMT\r\n"
        )
    }

    #[derive(Debug, Default)]
    struct Credentials {
        calls: AtomicUsize,
        fail: bool,
        empty: bool,
    }

    #[async_trait]
    impl CredentialProvider for Credentials {
        type Credential = GcpCredential;
        async fn get_credential(&self) -> object_store::Result<Arc<GcpCredential>> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(object_store::Error::Unauthenticated {
                    path: "secret-target?token=secret-query".into(),
                    source: "secret-credential-source".into(),
                });
            }
            Ok(Arc::new(GcpCredential {
                bearer: if self.empty {
                    String::new()
                } else {
                    format!("token-{attempt}")
                },
            }))
        }
    }

    fn sdk(
        responder: &Responder,
        credentials: Arc<Credentials>,
        skip: bool,
    ) -> (
        Arc<object_store::gcp::GoogleCloudStorage>,
        crate::conditional::GcsStageConfig,
    ) {
        let mut headers = http::HeaderMap::new();
        headers.insert("x-configured", "retained".parse().unwrap());
        build_gcs_store(
            GoogleCloudStorageBuilder::new()
                .with_bucket_name("bucket")
                .with_base_url(&responder.base)
                .with_credentials(credentials)
                .with_skip_signature(skip)
                .with_client_options(
                    ClientOptions::new()
                        .with_allow_http(true)
                        .with_content_type_for_suffix("txt", "text/custom")
                        .with_timeout(Duration::from_secs(2))
                        .with_proxy_url("http://127.0.0.1:1")
                        .with_proxy_excludes("127.0.0.1")
                        .with_default_headers(headers),
                ),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn actual_configured_sdk_http_baseline() {
        let responder = Responder::new(|_| {
            Some(reply(
                200,
                &snapshot_headers("\"opaque-A\"", "1729", "4"),
                "sdk",
            ))
        });
        let credentials = Arc::new(Credentials::default());
        let (store, _) = sdk(&responder, credentials.clone(), false);
        let result = store
            .get(&StorePath::parse("literal%2F/key").unwrap())
            .await
            .unwrap();
        assert_eq!(result.meta.e_tag.as_deref(), Some("\"opaque-A\""));
        assert_eq!(result.bytes().await.unwrap(), Bytes::from_static(b"sdk"));
        let captured = responder.captured();
        assert!(
            captured[0].starts_with("GET /bucket/literal%252F%2Fkey HTTP/1.1\r\n"),
            "{}",
            captured[0]
        );
        assert_eq!(header(&captured[0], "x-configured"), Some("retained"));
        assert_eq!(
            header(&captured[0], "authorization"),
            Some("Bearer token-0")
        );
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 1);
    }
    fn request_client(
        responder: &Responder,
        credentials: Arc<Credentials>,
        skip: bool,
    ) -> GcsRequestClient {
        let (store, config) = sdk(responder, credentials, skip);
        GcsRequestClient::new(store, Arc::new(config)).unwrap()
    }

    #[tokio::test]
    async fn opaque_keys_roundtrip_snapshot_put_and_exact_receipt() {
        for (key, encoded) in [
            ("literal%2F", "literal%252F"),
            ("literal%252F", "literal%25252F"),
            ("目录/雪?.txt", "%E7%9B%AE%E5%BD%95%2F%E9%9B%AA%3F.txt"),
            ("reserved ?#&+=.bin", "reserved%20%3F%23&+=.bin"),
        ] {
            let responder = Responder::new(|request| {
                let published = request.starts_with("PUT ") || request.contains("?generation=900 ");
                Some(reply(
                    200,
                    &snapshot_headers(
                        if published {
                            "\"opaque-B\""
                        } else {
                            "\"opaque-A\""
                        },
                        if published { "900" } else { "1729" },
                        if published { "1" } else { "4" },
                    ),
                    "",
                ))
            });
            let credentials = Arc::new(Credentials::default());
            let client = request_client(&responder, credentials.clone(), false);
            assert!(Arc::ptr_eq(
                client.store.credentials(),
                &(credentials.clone() as object_store::gcp::GcpCredentialProvider)
            ));
            let path = StorePath::parse(key).unwrap();
            let snapshot = client.head_snapshot(&path, None).await.unwrap().unwrap();
            assert_eq!(snapshot.xml_etag, "\"opaque-A\"");
            assert_eq!(snapshot.version.generation, "1729");
            assert_eq!(snapshot.version.metageneration, "4");
            let result = client
                .put_snapshot(
                    &path,
                    PutPayload::from("payload"),
                    PutOptions::default(),
                    Some(&snapshot),
                )
                .await
                .unwrap();
            assert_eq!(result.e_tag.as_deref(), Some("\"opaque-B\""));
            assert_eq!(result.version.as_deref(), Some("900"));
            let version = GcsObjectVersion {
                generation: result.version.unwrap(),
                metageneration: "1".into(),
            };
            let receipt = client
                .head_snapshot(&path, Some(&version))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(receipt.xml_etag, "\"opaque-B\"");
            assert_eq!(receipt.version.generation, "900");
            assert_eq!(receipt.version.metageneration, "1");
            let requests = responder.captured();
            assert_eq!(requests.len(), 3);
            for (index, method) in ["HEAD", "PUT", "HEAD"].iter().enumerate() {
                let query = if index == 2 { "?generation=900" } else { "" };
                assert!(
                    requests[index]
                        .starts_with(&format!("{method} /bucket/{encoded}{query} HTTP/1.1\r\n")),
                    "{}",
                    requests[index]
                );
                assert_eq!(header(&requests[index], "x-configured"), Some("retained"));
                assert_eq!(
                    header(&requests[index], "authorization"),
                    Some(format!("Bearer token-{index}").as_str())
                );
            }
            assert_eq!(header(&requests[0], "x-goog-if-generation-match"), None);
            for (index, generation, meta) in [(1, "1729", "4"), (2, "900", "1")] {
                assert_eq!(
                    header(&requests[index], "x-goog-if-generation-match"),
                    Some(generation)
                );
                assert_eq!(
                    header(&requests[index], "x-goog-if-metageneration-match"),
                    Some(meta)
                );
            }
            assert_eq!(header(&requests[1], "content-length"), Some("7"));
            assert_eq!(
                header(&requests[1], "content-type"),
                Some(if key.ends_with(".txt") {
                    "text/custom"
                } else {
                    "application/octet-stream"
                })
            );
            assert!(requests[1].ends_with("\r\n\r\npayload"));
            assert_eq!(credentials.calls.load(Ordering::SeqCst), 3);
        }
    }

    #[tokio::test]
    async fn metadata_only_competitor_survives_failed_put_and_receipt() {
        let current = Arc::new(Mutex::new((
            "\"opaque-A\"".to_owned(),
            "4".to_owned(),
            "original".to_owned(),
        )));
        let remote = current.clone();
        let responder = Responder::new(move |request| {
            let mut state = remote.lock().unwrap();
            if (request.starts_with("PUT ") || request.contains("?generation="))
                && (header(request, "x-goog-if-generation-match") != Some("1729")
                    || header(request, "x-goog-if-metageneration-match") != Some(state.1.as_str()))
            {
                return Some(reply(412, "", "secret-response-body"));
            }
            if request.starts_with("PUT ") {
                state.2 = "wrongly replaced".into();
            }
            Some(reply(
                200,
                &snapshot_headers(&state.0, "1729", &state.1),
                if request.starts_with("GET ") {
                    &state.2
                } else {
                    ""
                },
            ))
        });
        let client = request_client(&responder, Arc::new(Credentials::default()), false);
        let path = StorePath::parse("object").unwrap();
        let snapshot = client.head_snapshot(&path, None).await.unwrap().unwrap();
        *current.lock().unwrap() = (
            "\"opaque-F\"".into(),
            "5".into(),
            "competitor bytes and metadata".into(),
        );
        let error = client
            .put_snapshot(
                &path,
                PutPayload::from("candidate"),
                PutOptions::default(),
                Some(&snapshot),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, Error::Precondition { .. }));
        let receipt = client
            .head_snapshot(&path, Some(&snapshot.version))
            .await
            .unwrap_err();
        assert!(matches!(receipt, Error::Precondition { .. }));
        let actual = client.store.get(&path).await.unwrap();
        assert_eq!(actual.meta.e_tag.as_deref(), Some("\"opaque-F\""));
        assert_eq!(
            actual.bytes().await.unwrap(),
            "competitor bytes and metadata"
        );
        assert_eq!(
            *current.lock().unwrap(),
            (
                "\"opaque-F\"".into(),
                "5".into(),
                "competitor bytes and metadata".into()
            )
        );
        let requests = responder.captured();
        assert_eq!(requests.len(), 4);
        assert!(requests[2].starts_with("HEAD /bucket/object?generation=1729 HTTP/1.1\r\n"));
        for request in &requests[1..3] {
            assert_eq!(header(request, "x-goog-if-generation-match"), Some("1729"));
            assert_eq!(header(request, "x-goog-if-metageneration-match"), Some("4"));
        }
    }

    #[tokio::test]
    async fn create_preserves_attributes_segmented_bytes_and_conflict_identity() {
        let content = Arc::new(Mutex::new(None::<String>));
        let remote = content.clone();
        let responder = Responder::new(move |request| {
            let mut stored = remote.lock().unwrap();
            if request.starts_with("PUT ") {
                if stored.is_some() {
                    return Some(reply(412, "", "secret-response-body"));
                }
                assert_eq!(header(request, "x-goog-if-generation-match"), Some("0"));
                assert_eq!(header(request, "x-goog-if-metageneration-match"), None);
                *stored = Some(request.split_once("\r\n\r\n").unwrap().1.to_owned());
            } else if stored.is_none() {
                return Some(reply(404, "", ""));
            }
            Some(reply(200, &snapshot_headers("\"created\"", "900", "1"), ""))
        });
        let client = request_client(&responder, Arc::new(Credentials::default()), false);
        let path = StorePath::parse("object").unwrap();
        assert!(client.head_snapshot(&path, None).await.unwrap().is_none());
        let mut options = PutOptions::default();
        for (attribute, value) in [
            (Attribute::ContentType, "application/custom"),
            (Attribute::CacheControl, "no-cache"),
            (Attribute::ContentDisposition, "attachment"),
            (Attribute::ContentEncoding, "gzip"),
            (Attribute::ContentLanguage, "en"),
            (Attribute::StorageClass, "NEARLINE"),
            (Attribute::Metadata("raw".into()), "%2F%252F"),
        ] {
            options.attributes.insert(attribute, value.into());
        }
        let payload: PutPayload = [Bytes::from_static(b"first"), Bytes::from_static(b"second")]
            .into_iter()
            .collect();
        let result = client
            .put_snapshot(&path, payload, options, None)
            .await
            .unwrap();
        assert_eq!(result.e_tag.as_deref(), Some("\"created\""));
        assert_eq!(result.version.as_deref(), Some("900"));
        let version = GcsObjectVersion {
            generation: "900".into(),
            metageneration: "1".into(),
        };
        assert_eq!(
            client
                .head_snapshot(&path, Some(&version))
                .await
                .unwrap()
                .unwrap()
                .xml_etag,
            "\"created\""
        );
        assert!(matches!(
            client
                .put_snapshot(
                    &path,
                    PutPayload::from("racer"),
                    PutOptions::default(),
                    None
                )
                .await
                .unwrap_err(),
            Error::Precondition { .. }
        ));
        assert_eq!(content.lock().unwrap().as_deref(), Some("firstsecond"));
        let requests = responder.captured();
        for (name, value) in [
            ("content-length", "11"),
            ("content-type", "application/custom"),
            ("cache-control", "no-cache"),
            ("content-disposition", "attachment"),
            ("content-encoding", "gzip"),
            ("content-language", "en"),
            ("x-goog-storage-class", "NEARLINE"),
            ("x-goog-meta-raw", "%2F%252F"),
        ] {
            assert_eq!(header(&requests[1], name), Some(value));
        }
        assert!(requests[1].ends_with("\r\n\r\nfirstsecond"));
        assert_eq!(requests.len(), 4);
    }

    #[tokio::test]
    async fn absent_predicate_is_distinct_from_failed_receipts_and_provider_errors() {
        let statuses = Arc::new(Mutex::new(std::collections::VecDeque::from([
            404, 404, 412, 403, 401, 500,
        ])));
        let responder = Responder::new(move |_| {
            Some(reply(
                statuses.lock().unwrap().pop_front().unwrap(),
                "",
                "secret-response-body",
            ))
        });
        let client = request_client(&responder, Arc::new(Credentials::default()), false);
        let path = StorePath::parse("secret-target").unwrap();
        let version = GcsObjectVersion {
            generation: "900".into(),
            metageneration: "1".into(),
        };
        assert!(client.head_snapshot(&path, None).await.unwrap().is_none());
        let errors = [
            client
                .head_snapshot(&path, Some(&version))
                .await
                .unwrap_err(),
            client
                .head_snapshot(&path, Some(&version))
                .await
                .unwrap_err(),
            client
                .put_snapshot(&path, PutPayload::default(), PutOptions::default(), None)
                .await
                .unwrap_err(),
            client.head_snapshot(&path, None).await.unwrap_err(),
            client.head_snapshot(&path, None).await.unwrap_err(),
        ];
        assert!(matches!(errors[0], Error::Generic { .. }));
        assert!(matches!(errors[1], Error::Precondition { .. }));
        assert!(matches!(errors[2], Error::PermissionDenied { .. }));
        assert!(matches!(errors[3], Error::Unauthenticated { .. }));
        assert!(matches!(errors[4], Error::Generic { .. }));
        for error in errors {
            for output in [error.to_string(), format!("{error:?}")] {
                for secret in [
                    "secret-target",
                    "secret-response-body",
                    "token-",
                    responder.base.as_str(),
                ] {
                    assert!(!output.contains(secret), "{output}");
                }
            }
        }
        assert_eq!(format!("{client:?}"), "GcsRequestClient");
        assert_eq!(responder.captured().len(), 6);
    }

    #[tokio::test]
    async fn malformed_snapshot_versions_fail_without_substitute_metadata() {
        for (generation, meta) in [
            ("0", "4"),
            ("1", "0"),
            ("-1", "4"),
            ("1", "+4"),
            ("18446744073709551616", "4"),
            ("1", "4.0"),
        ] {
            let headers = snapshot_headers("\"quoted\"", generation, meta);
            let responder = Responder::new(move |_| Some(reply(200, &headers, "")));
            let client = request_client(&responder, Arc::new(Credentials::default()), false);
            assert!(matches!(
                client
                    .head_snapshot(&StorePath::parse("object").unwrap(), None)
                    .await
                    .unwrap_err(),
                Error::Generic { .. }
            ));
            assert_eq!(responder.captured().len(), 1);
        }
        for headers in [
            "ETag: \"a\"\r\nx-goog-generation: 1\r\n",
            "x-goog-generation: 1\r\nx-goog-metageneration: 1\r\n",
        ] {
            let responder = Responder::new(move |_| Some(reply(200, headers, "")));
            let client = request_client(&responder, Arc::new(Credentials::default()), false);
            assert!(
                client
                    .head_snapshot(&StorePath::parse("object").unwrap(), None)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn skip_empty_and_session_requests_do_not_inject_oauth() {
        for (skip, empty, expected_calls) in [(true, false, 0), (false, true, 1)] {
            let responder =
                Responder::new(|_| Some(reply(200, &snapshot_headers("\"a\"", "1", "1"), "")));
            let credentials = Arc::new(Credentials {
                fail: skip,
                empty,
                ..Default::default()
            });
            let client = request_client(&responder, credentials.clone(), skip);
            client
                .head_snapshot(&StorePath::parse("object").unwrap(), None)
                .await
                .unwrap();
            let request = http::Request::builder()
                .method("PUT")
                .uri(format!("{}/session?capability=private", responder.base))
                .body(HttpRequestBody::empty())
                .unwrap();
            client.execute(request, false).await.unwrap();
            assert_eq!(credentials.calls.load(Ordering::SeqCst), expected_calls);
            let captured = responder.captured();
            assert_eq!(captured.len(), 2);
            for request in captured {
                assert_eq!(header(&request, "authorization"), None);
            }
        }
    }

    #[tokio::test]
    async fn credential_and_transport_failures_are_redacted_and_not_preconditions() {
        let responder = Responder::new(|_| None);
        let credentials = Arc::new(Credentials {
            fail: true,
            ..Default::default()
        });
        let client = request_client(&responder, credentials.clone(), false);
        let path = StorePath::parse("secret-target").unwrap();
        let error = client.head_snapshot(&path, None).await.unwrap_err();
        assert!(matches!(error, Error::Unauthenticated { .. }));
        assert_eq!(credentials.calls.load(Ordering::SeqCst), 1);
        assert!(responder.captured().is_empty());
        for output in [error.to_string(), format!("{error:?}")] {
            for secret in ["secret-target", "secret-query", "secret-credential-source"] {
                assert!(!output.contains(secret));
            }
        }
        let healthy = Arc::new(Credentials::default());
        let client = request_client(&responder, healthy.clone(), false);
        let error = client.head_snapshot(&path, None).await.unwrap_err();
        assert!(matches!(error, Error::Generic { .. }));
        for output in [error.to_string(), format!("{error:?}")] {
            assert!(output.contains("transport"));
            for secret in [
                "secret-target",
                "token-0",
                responder.base.as_str(),
                "reqwest",
                "hyper",
            ] {
                assert!(!output.contains(secret));
            }
        }
        assert_eq!(healthy.calls.load(Ordering::SeqCst), 1);
        assert_eq!(responder.captured().len(), 1);
    }
}
