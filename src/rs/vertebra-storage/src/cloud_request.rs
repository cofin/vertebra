//! SDK transport and authorization for provider publication requests.

use object_store::ClientOptions;
use object_store::aws::{AwsAuthorizer, AwsCredentialProvider};
use object_store::client::{
    HttpClient, HttpConnector, HttpRequest, HttpResponse, ReqwestConnector,
};

use crate::store::ObjectStoreError;

pub(crate) struct CloudRequestClient {
    client: HttpClient,
}

impl CloudRequestClient {
    pub(crate) fn new(options: &ClientOptions) -> Result<Self, ObjectStoreError> {
        let client = ReqwestConnector::default()
            .connect(options)
            .map_err(|error| sdk_failure("configure client", "configuration", &error))?;
        Ok(Self { client })
    }

    pub(crate) async fn execute(
        &self,
        request: HttpRequest,
        operation: &'static str,
    ) -> Result<HttpResponse, ObjectStoreError> {
        self.client.execute(request).await.map_err(|error| {
            object_store::Error::Generic {
                store: "cloud publication",
                source: format!("{operation}: transport {:?}", error.kind()).into(),
            }
            .into()
        })
    }

    pub(crate) async fn execute_aws(
        &self,
        mut request: HttpRequest,
        credentials: &AwsCredentialProvider,
        region: &str,
        operation: &'static str,
    ) -> Result<HttpResponse, ObjectStoreError> {
        let credential = credentials
            .get_credential()
            .await
            .map_err(|error| sdk_failure(operation, "credentials", &error))?;
        AwsAuthorizer::new(credential.as_ref(), "s3", region).authorize(&mut request, None);
        self.execute(request, operation).await
    }
}

fn sdk_failure(
    operation: &'static str,
    context: &'static str,
    error: &object_store::Error,
) -> ObjectStoreError {
    let kind = match error {
        object_store::Error::Generic { .. } => "Generic",
        object_store::Error::NotFound { .. } => "NotFound",
        object_store::Error::InvalidPath { .. } => "InvalidPath",
        object_store::Error::JoinError { .. } => "JoinError",
        object_store::Error::NotSupported { .. } => "NotSupported",
        object_store::Error::AlreadyExists { .. } => "AlreadyExists",
        object_store::Error::Precondition { .. } => "Precondition",
        object_store::Error::NotModified { .. } => "NotModified",
        object_store::Error::NotImplemented { .. } => "NotImplemented",
        object_store::Error::PermissionDenied { .. } => "PermissionDenied",
        object_store::Error::Unauthenticated { .. } => "Unauthenticated",
        object_store::Error::UnknownConfigurationKey { .. } => "UnknownConfigurationKey",
        _ => "Unknown",
    };
    object_store::Error::Generic {
        store: "cloud publication",
        source: format!("{operation}: {context} {kind}").into(),
    }
    .into()
}

#[cfg(test)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::thread::{self, JoinHandle};
    use std::time::{Duration, Instant};

    use async_trait::async_trait;
    use object_store::ClientOptions;
    use object_store::aws::{AmazonS3, AmazonS3Builder, AwsCredential};
    use object_store::client::{CredentialProvider, HttpRequest, HttpRequestBody};
    use reqwest::header::HeaderMap;

    use super::CloudRequestClient;

    struct Responder {
        url: String,
        worker: JoinHandle<Vec<String>>,
    }

    impl Responder {
        fn new(statuses: Vec<Option<u16>>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").unwrap();
            listener.set_nonblocking(true).unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let worker = thread::spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(5);
                let mut captured = Vec::new();
                for status in statuses {
                    let mut stream = loop {
                        match listener.accept() {
                            Ok((stream, _)) => break stream,
                            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                                assert!(Instant::now() < deadline, "HTTP request did not arrive");
                                thread::sleep(Duration::from_millis(5));
                            }
                            Err(error) => panic!("HTTP accept failed: {error}"),
                        }
                    };
                    stream
                        .set_read_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    stream
                        .set_write_timeout(Some(Duration::from_secs(2)))
                        .unwrap();
                    let mut data = Vec::new();
                    loop {
                        let mut chunk = [0; 1024];
                        let n = stream.read(&mut chunk).unwrap();
                        assert!(n > 0, "request ended before its body");
                        data.extend_from_slice(&chunk[..n]);
                        assert!(data.len() <= 16384, "test request exceeds fixture bound");
                        if let Some(end) = data.windows(4).position(|w| w == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&data[..end]);
                            let length = headers
                                .lines()
                                .find_map(|line| {
                                    let (name, value) = line.split_once(':')?;
                                    name.eq_ignore_ascii_case("content-length")
                                        .then(|| value.trim().parse::<usize>().unwrap())
                                })
                                .unwrap_or(0);
                            if data.len() >= end + 4 + length {
                                break;
                            }
                        }
                    }
                    captured.push(String::from_utf8(data).unwrap());
                    if let Some(status) = status {
                        write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: 10\r\nConnection: close\r\n\r\nreply-body").unwrap();
                    }
                }
                captured
            });
            Self { url, worker }
        }

        fn finish(self) -> Vec<String> {
            self.worker.join().unwrap()
        }
    }

    fn request(url: &str, body: &str) -> HttpRequest {
        let mut request = HttpRequest::new(HttpRequestBody::from(body.to_owned()));
        *request.method_mut() = "PUT".parse().unwrap();
        *request.uri_mut() = url.parse().unwrap();
        request
    }

    fn options() -> ClientOptions {
        ClientOptions::new()
            .with_allow_http(true)
            .with_timeout(Duration::from_secs(2))
    }

    #[derive(Debug, Default)]
    struct RotatingCredentials {
        calls: AtomicUsize,
        fail: bool,
    }

    #[async_trait]
    impl CredentialProvider for RotatingCredentials {
        type Credential = AwsCredential;

        async fn get_credential(&self) -> object_store::Result<Arc<AwsCredential>> {
            let attempt = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(object_store::Error::Unauthenticated {
                    path: "private-target?token=private-token".into(),
                    source: "private-source-secret".into(),
                });
            }
            Ok(Arc::new(AwsCredential {
                key_id: format!("test-key-{attempt}"),
                secret_key: "test-secret".into(),
                token: Some(format!("test-token-{attempt}")),
            }))
        }
    }

    fn sdk(url: &str, provider: Arc<RotatingCredentials>) -> AmazonS3 {
        AmazonS3Builder::new()
            .with_bucket_name("unit")
            .with_region("us-east-1")
            .with_endpoint(url)
            .with_allow_http(true)
            .with_credentials(provider)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn configured_transport_preserves_headers_path_and_payload() {
        let responder = Responder::new(vec![Some(200)]);
        let mut headers = HeaderMap::new();
        headers.insert("x-configured", "configured-value".parse().unwrap());
        let configured = options()
            .with_default_headers(headers)
            .with_proxy_url("http://127.0.0.1:1")
            .with_proxy_excludes("127.0.0.1");
        let client = CloudRequestClient::new(&configured).unwrap();
        let response = client
            .execute(
                request(
                    &format!("{}/raw%252Fkey?upload=1", responder.url),
                    "payload",
                ),
                "configured",
            )
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let captured = responder.finish();
        assert!(captured[0].starts_with("PUT /raw%252Fkey?upload=1 HTTP/1.1\r\n"));
        assert!(
            captured[0]
                .to_lowercase()
                .contains("x-configured: configured-value\r\n")
        );
        assert!(captured[0].ends_with("\r\n\r\npayload"));
    }

    #[tokio::test]
    async fn explicit_attempts_refresh_sdk_credentials_without_hidden_replay() {
        let responder = Responder::new(vec![Some(503), Some(200)]);
        let provider = Arc::new(RotatingCredentials::default());
        let sdk = sdk(&responder.url, provider.clone());
        let client = CloudRequestClient::new(&options()).unwrap();
        for status in [503, 200] {
            let response = client
                .execute_aws(
                    request(&format!("{}/unit/key", responder.url), "part"),
                    sdk.credentials(),
                    "us-east-1",
                    "attempt",
                )
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
            assert_eq!(response.into_body().bytes().await.unwrap(), "reply-body");
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 2);
        for (attempt, captured) in responder.finish().iter().enumerate() {
            let headers = captured.to_lowercase();
            assert!(headers.contains(&format!(
                "authorization: aws4-hmac-sha256 credential=test-key-{attempt}/"
            )));
            assert!(headers.contains("/us-east-1/s3/aws4_request"));
            assert!(headers.contains(&format!("x-amz-security-token: test-token-{attempt}\r\n")));
            assert!(captured.ends_with("\r\n\r\npart"));
        }
    }

    #[tokio::test]
    async fn http_failures_and_bodies_belong_to_the_caller() {
        let responder = Responder::new(vec![Some(403), Some(412)]);
        let client = CloudRequestClient::new(&options()).unwrap();
        for status in [403, 412] {
            let response = client
                .execute(request(&responder.url, ""), "status")
                .await
                .unwrap();
            assert_eq!(response.status().as_u16(), status);
            assert_eq!(response.into_body().bytes().await.unwrap(), "reply-body");
        }
        assert_eq!(responder.finish().len(), 2);
    }

    #[tokio::test]
    async fn connection_failure_redacts_signed_target_and_source_chain() {
        let responder = Responder::new(vec![None]);
        let provider = Arc::new(RotatingCredentials::default());
        let sdk = sdk(&responder.url, provider.clone());
        let client = CloudRequestClient::new(&options()).unwrap();
        let target = format!("{}/private-target?token=private-token", responder.url);
        let error = client
            .execute_aws(
                request(&target, ""),
                sdk.credentials(),
                "us-east-1",
                "complete multipart",
            )
            .await
            .unwrap_err();
        let display = error.to_string();
        let debug = format!("{error:?}");
        assert!(display.contains("complete multipart"));
        assert!(display.contains("transport"));
        assert!(
            ["Request", "Interrupted", "Unknown"]
                .iter()
                .any(|kind| display.contains(kind))
        );
        for output in [display, debug] {
            for secret in [
                target.as_str(),
                "private-target",
                "private-token",
                "test-key",
                "test-token",
                "test-secret",
                "Authorization",
                "hyper",
                "reqwest",
            ] {
                assert!(!output.contains(secret), "leaked {secret}: {output}");
            }
        }
        let captured = responder.finish();
        assert!(
            captured[0]
                .to_lowercase()
                .contains("authorization: aws4-hmac-sha256")
        );
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn credential_failure_redacts_provider_error_without_sending() {
        let provider = Arc::new(RotatingCredentials {
            fail: true,
            ..Default::default()
        });
        let sdk = sdk("http://127.0.0.1:1", provider.clone());
        let client = CloudRequestClient::new(&options()).unwrap();
        let error = client
            .execute_aws(
                request("http://127.0.0.1:1/private-target", ""),
                sdk.credentials(),
                "us-east-1",
                "initiate multipart",
            )
            .await
            .unwrap_err();
        for output in [error.to_string(), format!("{error:?}")] {
            assert!(output.contains("initiate multipart"));
            assert!(output.contains("Unauthenticated"));
            for secret in ["private-target", "private-token", "private-source-secret"] {
                assert!(!output.contains(secret));
            }
        }
        assert_eq!(provider.calls.load(Ordering::SeqCst), 1);
    }
}
