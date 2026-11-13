//! Integration tests for `vertebra_core::gcp_auth`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_core::gcp_auth::{
    DEFAULT_GCP_SCOPES, DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache, ProxyPolicy,
};
use vertebra_core::gcp_identity::{
    DEFAULT_ID_TOKEN_REFRESH_SKEW_MS, DEFAULT_MAX_ID_TOKEN_AUDIENCES, GcpIdTokenCache,
    GcpIdTokenOptions, MetadataIdTokenFormat, validate_id_token_audience,
};

const TEST_RSA_PRIVATE_KEY_PEM: &str = concat!(
    "-----BEGIN PRIVATE KEY-----\n",
    "MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDJTl8KiBfisWRw\n",
    "nP4tfbLiOa0io1rTfTen2IIYQgek3ET1isNNTWqeWSaD8mzOrVlLZlAaDZopzq4Z\n",
    "jzLNq9uwIDWPuCzZIVMIAGDpcI+cAuofDs9FGRh3x5fO78Z0ch4tfwAh3SZpcu4P\n",
    "LqPpU1FwvhfHftK5UdM0LHW57xhPPzu+lfVuBH8fRULmrqQJvDpcwb0QZtHCTs/z\n",
    "VWbLGhl1XCLUEI/95CnOVctJHeckgJL5chUXxDoXpZGyFfv8qz832BgO3LN7Hnn1\n",
    "w9gLsidb94Bmq/kA49esiXDECgcBSRgZmpYSOZOwibU9RI3nFtNGb6IpGiXhiqI9\n",
    "ci5V3jtFAgMBAAECggEAAN/7ltGaSqh3xdB5tUlUQiEjhMQTGTHm+N2tuCA4FH8k\n",
    "zL4XILBbjhAAgB2uQNvrY290Frm94gA2s5s2CpRGiTdT4pWABoGRTz1HE5r6sJls\n",
    "v7SLgn96ekW+i2DCW9gMZOXOhDw3F/9/q3yXWwzjjKyhyFpLSQNVnBXqb5m1lwXy\n",
    "M9Eej90qFDr3Or+Y+Met0LEvt3E+tYuPjVlLTmQFjZOTCB/Kqudq3mkUG8G9cV6Y\n",
    "YNN+RXjsHj7xsL3Rp+V6vua3B3hWjSnsqpl1u0CD0fBDOrbie2yCmK4n7C/0Hi7H\n",
    "wwZoSGXR+y0hdMEXuZXX0iXjv3pFBvr7+RQwxWggAQKBgQDo8f2xFAUIUVH7t6Rm\n",
    "nSN2XbVqOl2vdR47yoYS5HOxjgQtmgMqHQ4eusVIVirSStyGMs49ZtVGiL4cYqxs\n",
    "vECXDHyiL3dPCcSS8MkzYcGv5QjXsuQLHvp4my2REWNB9RVCADslTIetMEFoFm0T\n",
    "GvQz+RMvyJoGZl26mchwFGldRQKBgQDdOsGlqtjSc+TQ5q5YtZ0+phIzPZ1usbN4\n",
    "qdjEYp5SCGNU2/AFayqKSPgD/ulMtdiXzA2qK4I9h5xmRE/A/FJVhW66S29+6CAP\n",
    "/EGtfAYdFozsaZpjxCpYszagGVBnvViv9Y4KWUeyhN1lwfkbTLZ0jIiICt4Vp3Ci\n",
    "5k6Vp2RGAQKBgA/6IQtXMy0p3rSkGNuM5ZTFwp6hmDFjmApDzsRW9PR1qCW0jYkC\n",
    "6ygCaWuXtWItDkkKDQk7F1+6G0f5IJhopQl3Ksk49jbulZP6feJPTyiQTfX1aBy3\n",
    "KDjDc/GiPquUcrvPJj9dq2Seja3k8+A+lgdGiyFY4e+Ub9NPnubJD5z9AoGBALeA\n",
    "2kmpykne2eixv+JTWwgxRp18OU8tVTn8YJKdXsear5IfERvdhZ7I4fimhCweFnfr\n",
    "SVZi2phhGUj21tS0NzM7B9q9YdF5h8barjvRyVxtbR1YhzjD6YBDv0bTWI+ETsE9\n",
    "xMEuxzr73I1gdVnAqwNH4qQ9p3N8vQBSWtLl01YBAoGBAND7MuyVNdG+9pGItdFt\n",
    "k9pz3Vc9t+8Fn31K9HldC9ejmnBapfkgwLdJemLQaqAs6tkbG2ajlCqv7Y4ZAARc\n",
    "qkcDJbfy9jP/h7kvb/UNP21SxDscrKTAczxEm7PiTk3HPNOwx3/N6tPc+5HhR8LR\n",
    "8OwWavCVE6HreFLFfrEAgSgF\n",
    "-----END PRIVATE KEY-----\n"
);

const TEST_RSA_PUBLIC_KEY_PEM: &str = concat!(
    "-----BEGIN PUBLIC KEY-----\n",
    "MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAyU5fCogX4rFkcJz+LX2y\n",
    "4jmtIqNa0303p9iCGEIHpNxE9YrDTU1qnlkmg/Jszq1ZS2ZQGg2aKc6uGY8yzavb\n",
    "sCA1j7gs2SFTCABg6XCPnALqHw7PRRkYd8eXzu/GdHIeLX8AId0maXLuDy6j6VNR\n",
    "cL4Xx37SuVHTNCx1ue8YTz87vpX1bgR/H0VC5q6kCbw6XMG9EGbRwk7P81VmyxoZ\n",
    "dVwi1BCP/eQpzlXLSR3nJICS+XIVF8Q6F6WRshX7/Ks/N9gYDtyzex559cPYC7In\n",
    "W/eAZqv5AOPXrIlwxAoHAUkYGZqWEjmTsIm1PUSN5xbTRm+iKRol4YqiPXIuVd47\n",
    "RQIDAQAB\n",
    "-----END PUBLIC KEY-----\n"
);

#[derive(Debug, Deserialize)]
struct DecodedJwtClaims {
    iss: String,
    sub: String,
    #[serde(default)]
    scope: Option<String>,
    aud: String,
    iat: u64,
    exp: u64,
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> String {
    let mut buf = Vec::with_capacity(8192);
    let mut chunk = [0u8; 4096];
    let mut expected_total: Option<usize> = None;
    loop {
        let n = stream.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if expected_total.is_none()
            && let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n")
        {
            let header_end = pos + 4;
            let headers_str = String::from_utf8_lossy(&buf[..header_end]);
            let content_len = headers_str
                .lines()
                .find_map(|line| {
                    let (k, v) = line.split_once(':')?;
                    if k.eq_ignore_ascii_case("content-length") {
                        v.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            expected_total = Some(header_end + content_len);
        }
        if let Some(total) = expected_total
            && buf.len() >= total
        {
            break;
        }
    }
    String::from_utf8_lossy(&buf).into_owned()
}

#[tokio::test]
async fn service_account_jwt_assertion_single_flight_and_invalidation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let token_uri = format!("http://127.0.0.1:{}/token", addr.port());
    let expected_aud = token_uri.clone();
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_server = Arc::clone(&call_count);

    tokio::spawn(async move {
        let decoding_key =
            DecodingKey::from_rsa_pem(TEST_RSA_PUBLIC_KEY_PEM.as_bytes()).expect("rsa pubkey");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[&expected_aud]);

        while let Ok((mut stream, _)) = listener.accept().await {
            let count = Arc::clone(&call_count_server);
            let decoding_key = decoding_key.clone();
            let validation = validation.clone();
            let expected_aud = expected_aud.clone();
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                assert!(req.starts_with("POST /token HTTP/1.1"));
                let (_, body) = req.split_once("\r\n\r\n").expect("http body");
                let form: HashMap<String, String> = url::form_urlencoded::parse(body.as_bytes())
                    .into_owned()
                    .collect();
                assert_eq!(
                    form.get("grant_type").map(String::as_str),
                    Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
                );
                let assertion = form.get("assertion").expect("jwt assertion");
                let token_data = decode::<DecodedJwtClaims>(assertion, &decoding_key, &validation)
                    .expect("valid RS256 JWT assertion");
                assert_eq!(
                    token_data.claims.iss,
                    "bq-runner@analytics-proj.iam.gserviceaccount.com"
                );
                assert_eq!(
                    token_data.claims.sub,
                    "bq-runner@analytics-proj.iam.gserviceaccount.com"
                );
                assert_eq!(token_data.claims.aud, expected_aud);
                assert_eq!(
                    token_data.claims.scope.as_deref(),
                    Some(
                        "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/spanner.data"
                    )
                );
                assert_eq!(token_data.claims.exp - token_data.claims.iat, 3600);

                let seq = count.fetch_add(1, Ordering::SeqCst) + 1;
                let resp_body = format!(
                    r#"{{"access_token":"test-access-token-{seq}","expires_in":3600,"token_type":"Bearer"}}"#
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let sa_json = serde_json::json!({
        "type": "service_account",
        "project_id": "analytics-proj",
        "private_key_id": "key-1",
        "private_key": TEST_RSA_PRIVATE_KEY_PEM,
        "client_email": "bq-runner@analytics-proj.iam.gserviceaccount.com",
        "token_uri": token_uri,
        "quota_project_id": "billing-proj"
    })
    .to_string();

    let mut opts = HashMap::new();
    opts.insert("service_account_key".to_string(), sa_json);
    opts.insert("proxy_policy".to_string(), "direct".to_string());
    opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/bigquery, https://www.googleapis.com/auth/spanner.data"
            .to_string(),
    );

    let cache = GcpTokenCache::from_options(&opts, DEFAULT_GCP_SCOPES)
        .expect("build service account cache");
    assert!(cache.service_account_key().is_some());

    let mut tasks = Vec::new();
    for _ in 0..16 {
        let c = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            c.get_or_refresh().await.expect("get_or_refresh")
        }));
    }
    for task in tasks {
        let snap = task.await.expect("join");
        assert_eq!(snap.access_token, "test-access-token-1");
        assert_eq!(snap.generation, 1);
        assert_eq!(snap.quota_project_id.as_deref(), Some("billing-proj"));
    }
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    cache.invalidate(1);
    let snap2 = cache
        .get_or_refresh()
        .await
        .expect("refresh after invalidate");
    assert_eq!(snap2.access_token, "test-access-token-2");
    assert_eq!(snap2.generation, 2);
    assert_eq!(call_count.load(Ordering::SeqCst), 2);

    cache.invalidate(1);
    let snap_still_2 = cache
        .get_or_refresh()
        .await
        .expect("stale invalidate is no-op");
    assert_eq!(snap_still_2.generation, 2);
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[test]
fn service_account_sign_jwt_for_audience_zero_network() {
    let sa_json = serde_json::json!({
        "type": "service_account",
        "project_id": "spanner-proj",
        "private_key": TEST_RSA_PRIVATE_KEY_PEM,
        "client_email": "spanner-sa@spanner-proj.iam.gserviceaccount.com",
        "token_uri": "http://127.0.0.1:9/unreachable"
    })
    .to_string();

    let mut opts = HashMap::new();
    opts.insert("service_account_key".to_string(), sa_json);
    opts.insert("proxy_policy".to_string(), "direct".to_string());
    let cache = GcpTokenCache::from_options(&opts, DEFAULT_GCP_SCOPES)
        .expect("build service account cache");

    let jwt = cache
        .sign_jwt_for_audience("https://spanner.googleapis.com/")
        .expect("sign audience jwt");

    let decoding_key =
        DecodingKey::from_rsa_pem(TEST_RSA_PUBLIC_KEY_PEM.as_bytes()).expect("rsa pubkey");
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_audience(&["https://spanner.googleapis.com/"]);
    let decoded = decode::<DecodedJwtClaims>(&jwt, &decoding_key, &validation).expect("decode jwt");
    assert_eq!(
        decoded.claims.iss,
        "spanner-sa@spanner-proj.iam.gserviceaccount.com"
    );
    assert_eq!(
        decoded.claims.sub,
        "spanner-sa@spanner-proj.iam.gserviceaccount.com"
    );
    assert_eq!(decoded.claims.aud, "https://spanner.googleapis.com/");
    assert!(decoded.claims.scope.is_none());
    assert_eq!(decoded.claims.exp - decoded.claims.iat, 3600);

    let auth_user_cache = GcpTokenCache::with_proxy_policy(
        GcpAuthSource::AuthorizedUser {
            client_id: "cid".to_string(),
            client_secret: "csec".to_string(),
            refresh_token: "rtok".to_string(),
            quota_project_id: None,
            token_uri: "http://127.0.0.1:9/token".to_string(),
        },
        DEFAULT_REFRESH_SKEW_MS,
        ProxyPolicy::Direct,
    );
    assert!(
        auth_user_cache
            .sign_jwt_for_audience("https://spanner.googleapis.com/")
            .is_err()
    );
}

#[tokio::test]
async fn metadata_server_and_impersonation_custom_scopes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                let body = if req.contains("/computeMetadata/v1/instance/service-accounts/") {
                    assert!(req.contains(
                        "GET /computeMetadata/v1/instance/service-accounts/default/token?scopes=https://www.googleapis.com/auth/bigquery,https://www.googleapis.com/auth/spanner.data HTTP/1.1"
                    ));
                    r#"{"access_token":"meta-base-token","expires_in":3600,"token_type":"Bearer"}"#
                        .to_string()
                } else {
                    assert!(req.contains(":generateAccessToken"));
                    assert!(req.contains("https://www.googleapis.com/auth/bigquery"));
                    assert!(req.contains("https://www.googleapis.com/auth/spanner.data"));
                    r#"{"accessToken":"impersonated-custom-scope-token","expireTime":"2099-01-01T00:00:00Z"}"#
                        .to_string()
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let mut opts = HashMap::new();
    opts.insert("use_metadata_server".to_string(), "true".to_string());
    opts.insert("metadata_host".to_string(), base_url.clone());
    opts.insert(
        "target_service_account".to_string(),
        "target@proj.iam.gserviceaccount.com".to_string(),
    );
    opts.insert("iam_endpoint".to_string(), base_url);
    opts.insert("proxy_policy".to_string(), "direct".to_string());
    opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/spanner.data"
            .to_string(),
    );

    let cache =
        GcpTokenCache::from_options(&opts, DEFAULT_GCP_SCOPES).expect("build impersonated cache");
    let snap = cache.get_or_refresh().await.expect("impersonated token");
    assert_eq!(snap.access_token, "impersonated-custom-scope-token");
}

#[tokio::test]
async fn impersonation_defaults_to_caller_scopes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let base_url = format!("http://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                let body = if req.contains("/computeMetadata/v1/instance/service-accounts/") {
                    assert!(req.contains(
                        "GET /computeMetadata/v1/instance/service-accounts/default/token HTTP/1.1"
                    ));
                    r#"{"access_token":"meta-base-token","expires_in":3600,"token_type":"Bearer"}"#
                        .to_string()
                } else {
                    assert!(req.contains(":generateAccessToken"));
                    assert!(req.contains("https://www.googleapis.com/auth/spanner.data"));
                    assert!(!req.contains("devstorage"));
                    r#"{"accessToken":"impersonated-spanner-scope-token","expireTime":"2099-01-01T00:00:00Z"}"#
                        .to_string()
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let mut opts = HashMap::new();
    opts.insert("use_metadata_server".to_string(), "true".to_string());
    opts.insert("metadata_host".to_string(), base_url.clone());
    opts.insert(
        "target_service_account".to_string(),
        "target@proj.iam.gserviceaccount.com".to_string(),
    );
    opts.insert("iam_endpoint".to_string(), base_url);
    opts.insert("proxy_policy".to_string(), "direct".to_string());

    let cache =
        GcpTokenCache::from_options(&opts, &["https://www.googleapis.com/auth/spanner.data"])
            .expect("build impersonated cache");
    let snap = cache.get_or_refresh().await.expect("impersonated token");
    assert_eq!(snap.access_token, "impersonated-spanner-scope-token");
}

#[cfg(unix)]
#[tokio::test(flavor = "current_thread")]
async fn gcloud_cli_token_fetch_is_async_and_times_out() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let script_path = dir.path().join("mock-gcloud");
    std::fs::write(&script_path, "#!/bin/sh\nsleep 60\n").expect("write script");
    std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod script");

    let cache = GcpTokenCache::new(
        GcpAuthSource::GcloudCli {
            binary: script_path,
            timeout: Duration::from_millis(200),
            quota_project_id: None,
            impersonate_service_account: None,
        },
        60_000,
    );

    let ticked = Arc::new(AtomicBool::new(false));
    let ticked_clone = Arc::clone(&ticked);
    let ticker = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        ticked_clone.store(true, Ordering::SeqCst);
    });

    let outcome = cache.get_or_refresh().await;
    let _ = ticker.await;
    assert!(outcome.is_err());
    let err = outcome.unwrap_err();
    assert!(
        err.contains("timed out after 200 ms"),
        "unexpected error: {err}"
    );
    assert!(
        ticked.load(Ordering::SeqCst),
        "tokio runtime was blocked during gcloud token fetch"
    );
}

#[tokio::test]
async fn explicit_proxy_routes_token_requests_and_metadata_bypasses_it() {
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
    let proxy_port = proxy_listener.local_addr().expect("proxy addr").port();
    let proxy_count = Arc::new(AtomicUsize::new(0));
    let proxy_count_server = Arc::clone(&proxy_count);
    let proxy_request_line = Arc::new(std::sync::Mutex::new(String::new()));
    let proxy_request_line_server = Arc::clone(&proxy_request_line);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = proxy_listener.accept().await {
            let count = Arc::clone(&proxy_count_server);
            let req_line = Arc::clone(&proxy_request_line_server);
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                let first_line = req.lines().next().unwrap_or_default().to_string();
                *req_line.lock().expect("lock") = first_line;
                count.fetch_add(1, Ordering::SeqCst);
                let body = r#"{"access_token":"proxied","expires_in":3600,"token_type":"Bearer"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let proxy = reqwest::Proxy::http(format!("http://127.0.0.1:{proxy_port}")).expect("proxy");
    let token_cache = GcpTokenCache::with_proxy_policy(
        GcpAuthSource::AuthorizedUser {
            client_id: "c".into(),
            client_secret: "s".into(),
            refresh_token: "r".into(),
            quota_project_id: None,
            token_uri: "http://oauth.invalid/token".into(),
        },
        60_000,
        ProxyPolicy::Explicit(proxy.clone()),
    );
    let snap = token_cache
        .get_or_refresh()
        .await
        .expect("proxied oauth token");
    assert_eq!(snap.access_token, "proxied");
    assert_eq!(proxy_count.load(Ordering::SeqCst), 1);
    assert!(
        proxy_request_line
            .lock()
            .expect("lock")
            .starts_with("POST http://oauth.invalid/token"),
        "unexpected proxy request line: {}",
        proxy_request_line.lock().expect("lock")
    );

    let meta_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind metadata");
    let meta_port = meta_listener.local_addr().expect("meta addr").port();
    let meta_count = Arc::new(AtomicUsize::new(0));
    let meta_count_server = Arc::clone(&meta_count);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = meta_listener.accept().await {
            let count = Arc::clone(&meta_count_server);
            tokio::spawn(async move {
                let _req = read_http_request(&mut stream).await;
                count.fetch_add(1, Ordering::SeqCst);
                let body =
                    r#"{"access_token":"direct-metadata","expires_in":3600,"token_type":"Bearer"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let meta_cache = GcpTokenCache::with_proxy_policy(
        GcpAuthSource::MetadataServer {
            service_account: "default".into(),
            quota_project_id: None,
            endpoint: format!("http://127.0.0.1:{meta_port}"),
            scopes: Vec::new(),
        },
        60_000,
        ProxyPolicy::Explicit(proxy),
    );
    let meta_snap = meta_cache
        .get_or_refresh()
        .await
        .expect("direct metadata token");
    assert_eq!(meta_snap.access_token, "direct-metadata");
    assert_eq!(meta_count.load(Ordering::SeqCst), 1);
    assert_eq!(proxy_count.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn from_scoped_options_enforces_scopes_for_authorized_user_and_rejects_static_tokens() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let token_uri = format!("http://127.0.0.1:{}/token", addr.port());

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                let (_, body) = req.split_once("\r\n\r\n").expect("http body");
                let form: HashMap<String, String> = url::form_urlencoded::parse(body.as_bytes())
                    .into_owned()
                    .collect();
                assert_eq!(
                    form.get("scope").map(String::as_str),
                    Some("https://www.googleapis.com/auth/sqlservice.login")
                );
                let resp_body = r#"{"access_token":"scoped-user-tok","expires_in":3600,"scope":"https://www.googleapis.com/auth/sqlservice.login"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let mut opts = HashMap::new();
    opts.insert("client_id".to_string(), "cid".to_string());
    opts.insert("client_secret".to_string(), "csec".to_string());
    opts.insert("refresh_token".to_string(), "rtok".to_string());
    opts.insert("token_uri".to_string(), token_uri);
    opts.insert("proxy_policy".to_string(), "direct".to_string());

    let default_scopes = vec!["https://www.googleapis.com/auth/sqlservice.login".to_string()];
    let cache = GcpTokenCache::from_scoped_options(Some(&opts), &default_scopes)
        .expect("build scoped authorized_user cache");
    let snap = cache.get_or_refresh().await.expect("scoped refresh");
    assert_eq!(snap.access_token, "scoped-user-tok");

    let mut static_opts = HashMap::new();
    static_opts.insert("access_token".to_string(), "static-tok".to_string());
    let static_err = GcpTokenCache::from_scoped_options(Some(&static_opts), &default_scopes)
        .expect_err("static token must reject explicit scopes");
    assert!(
        static_err.contains("static access token cannot satisfy explicit non-empty OAuth scopes")
    );
}

#[tokio::test]
async fn explicit_login_scopes_rejected_when_authorized_user_omits_scope() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let token_uri = format!("http://127.0.0.1:{}/token", addr.port());

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let _req = read_http_request(&mut stream).await;
                let resp_body = r#"{"access_token":"unscoped-tok","expires_in":3600,"scope":"https://www.googleapis.com/auth/cloud-platform"}"#;
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let mut opts = HashMap::new();
    opts.insert("client_id".to_string(), "cid".to_string());
    opts.insert("client_secret".to_string(), "csec".to_string());
    opts.insert("refresh_token".to_string(), "rtok".to_string());
    opts.insert("token_uri".to_string(), token_uri);
    opts.insert("proxy_policy".to_string(), "direct".to_string());

    let default_scopes = vec!["https://www.googleapis.com/auth/sqlservice.login".to_string()];
    let cache = GcpTokenCache::from_scoped_options(Some(&opts), &default_scopes).unwrap();
    let err = cache
        .get_or_refresh()
        .await
        .expect_err("must reject token missing requested scope");
    assert!(err.contains("omitted requested scope"));
}

#[cfg(unix)]
#[tokio::test]
async fn explicit_login_scopes_passed_to_gcloud_without_unscoped_fallback() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().expect("tempdir");
    let script_path = dir.path().join("mock-gcloud-scoped");
    std::fs::write(
        &script_path,
        "#!/bin/sh\nif [ \"$1\" = \"auth\" ] && [ \"$2\" = \"application-default\" ] && [ \"$3\" = \"print-access-token\" ] && [ \"$4\" = \"--quiet\" ] && [ \"$5\" = \"--scopes=https://www.googleapis.com/auth/alloydb.login\" ]; then\n  echo \"gcloud-scoped-tok\"\n  exit 0\nfi\nexit 1\n",
    )
    .expect("write script");
    std::fs::set_permissions(&script_path, std::fs::Permissions::from_mode(0o755))
        .expect("chmod script");

    let cache = GcpTokenCache::new(
        GcpAuthSource::GcloudCli {
            binary: script_path,
            timeout: Duration::from_secs(2),
            quota_project_id: None,
            impersonate_service_account: None,
        },
        60_000,
    )
    .with_requested_scopes(vec![
        "https://www.googleapis.com/auth/alloydb.login".to_string(),
    ]);

    let snap = cache.get_or_refresh().await.expect("scoped gcloud token");
    assert_eq!(snap.access_token, "gcloud-scoped-tok");

    let fail_script = dir.path().join("mock-gcloud-fail-adc");
    std::fs::write(
        &fail_script,
        "#!/bin/sh\nif [ \"$2\" = \"application-default\" ]; then\n  echo \"adc failed\" >&2\n  exit 1\nfi\necho \"unscoped-fallback-tok\"\nexit 0\n",
    )
    .expect("write fail script");
    std::fs::set_permissions(&fail_script, std::fs::Permissions::from_mode(0o755))
        .expect("chmod fail script");

    let fail_cache = GcpTokenCache::new(
        GcpAuthSource::GcloudCli {
            binary: fail_script,
            timeout: Duration::from_secs(2),
            quota_project_id: None,
            impersonate_service_account: None,
        },
        60_000,
    )
    .with_requested_scopes(vec![
        "https://www.googleapis.com/auth/alloydb.login".to_string(),
    ]);
    let err = fail_cache
        .get_or_refresh()
        .await
        .expect_err("must not fall back to unscoped gcloud auth print-access-token");
    assert!(err.contains("adc failed"));
}

static ENV_MUTATION_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

struct EnvVarGuard {
    saved: Vec<(&'static str, Option<String>)>,
}

impl EnvVarGuard {
    fn set(vars: &[(&'static str, Option<&str>)]) -> Self {
        let mut saved = Vec::with_capacity(vars.len());
        for (key, val) in vars {
            saved.push((*key, std::env::var(key).ok()));
            // SAFETY: Caller holds `ENV_MUTATION_LOCK` for the full lifetime of this guard.
            unsafe {
                match val {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
        Self { saved }
    }
}

impl Drop for EnvVarGuard {
    fn drop(&mut self) {
        for (key, prev) in self.saved.iter().rev() {
            // SAFETY: Caller holds `ENV_MUTATION_LOCK` across construction and drop.
            unsafe {
                match prev {
                    Some(v) => std::env::set_var(key, v),
                    None => std::env::remove_var(key),
                }
            }
        }
    }
}

#[derive(serde::Serialize)]
struct MockIdTokenJwtClaims<'a> {
    iss: &'a str,
    aud: serde_json::Value,
    iat: u64,
    exp: u64,
}

fn sign_test_id_token(aud: serde_json::Value, exp_offset_secs: i64) -> String {
    let now_secs = vertebra_core::gcp_auth::current_unix_ms() / 1_000;
    let exp = if exp_offset_secs >= 0 {
        now_secs.saturating_add(exp_offset_secs as u64)
    } else {
        now_secs.saturating_sub(exp_offset_secs.unsigned_abs())
    };
    let claims = MockIdTokenJwtClaims {
        iss: "https://accounts.google.com",
        aud,
        iat: now_secs,
        exp,
    };
    let encoding_key = jsonwebtoken::EncodingKey::from_rsa_pem(TEST_RSA_PRIVATE_KEY_PEM.as_bytes())
        .expect("rsa encoding key");
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(Algorithm::RS256),
        &claims,
        &encoding_key,
    )
    .expect("encode mock id_token")
}

#[derive(Debug, Deserialize)]
struct DecodedIdAssertionClaims {
    iss: String,
    sub: String,
    #[serde(default)]
    scope: Option<String>,
    aud: String,
    target_audience: String,
    iat: u64,
    exp: u64,
}

#[tokio::test]
async fn id_token_service_account_single_flight_skew_invalidation_and_redaction() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let token_uri = format!("http://127.0.0.1:{}/token", addr.port());
    let expected_token_uri = token_uri.clone();
    let call_count = Arc::new(AtomicUsize::new(0));
    let call_count_server = Arc::clone(&call_count);

    tokio::spawn(async move {
        let decoding_key =
            DecodingKey::from_rsa_pem(TEST_RSA_PUBLIC_KEY_PEM.as_bytes()).expect("rsa pubkey");
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_audience(&[&expected_token_uri]);

        while let Ok((mut stream, _)) = listener.accept().await {
            let count = Arc::clone(&call_count_server);
            let decoding_key = decoding_key.clone();
            let validation = validation.clone();
            let expected_token_uri = expected_token_uri.clone();
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                let (_, body) = req.split_once("\r\n\r\n").expect("http body");
                let form: HashMap<String, String> = url::form_urlencoded::parse(body.as_bytes())
                    .into_owned()
                    .collect();
                assert_eq!(
                    form.get("grant_type").map(String::as_str),
                    Some("urn:ietf:params:oauth:grant-type:jwt-bearer")
                );
                let assertion = form.get("assertion").expect("jwt assertion");
                let token_data =
                    decode::<DecodedIdAssertionClaims>(assertion, &decoding_key, &validation)
                        .expect("valid RS256 ID assertion");
                assert_eq!(token_data.claims.iss, "id-sa@proj.iam.gserviceaccount.com");
                assert_eq!(token_data.claims.sub, "id-sa@proj.iam.gserviceaccount.com");
                assert_eq!(token_data.claims.aud, expected_token_uri);
                assert_eq!(token_data.claims.target_audience, "https://svc-a.run.app");
                assert!(token_data.claims.scope.is_none());
                assert_eq!(token_data.claims.exp - token_data.claims.iat, 3600);

                let seq = count.fetch_add(1, Ordering::SeqCst) + 1;
                let exp_offset = if seq == 1 { 3600 } else { 3700 };
                let id_tok =
                    sign_test_id_token(serde_json::json!("https://svc-a.run.app"), exp_offset);
                let resp_body = format!(r#"{{"id_token":"{id_tok}"}}"#);
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let sa_json = serde_json::json!({
        "type": "service_account",
        "project_id": "proj",
        "private_key": TEST_RSA_PRIVATE_KEY_PEM,
        "client_email": "id-sa@proj.iam.gserviceaccount.com",
        "token_uri": token_uri
    })
    .to_string();

    let mut opts = HashMap::new();
    opts.insert("service_account_key".to_string(), sa_json);
    opts.insert("proxy_policy".to_string(), "direct".to_string());
    let cache = GcpIdTokenCache::from_options(&opts).expect("id token cache");
    assert_eq!(cache.refresh_skew_ms(), DEFAULT_ID_TOKEN_REFRESH_SKEW_MS);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&cache);
        tasks.push(tokio::spawn(async move {
            c.get_or_refresh("https://svc-a.run.app", false)
                .await
                .expect("id token")
        }));
    }
    let mut first_tok = String::new();
    for task in tasks {
        let snap = task.await.expect("join");
        assert_eq!(snap.audience, "https://svc-a.run.app");
        assert_eq!(snap.generation, 1);
        let debug_str = format!("{snap:?}");
        assert!(debug_str.contains("[REDACTED]"));
        assert!(!debug_str.contains(&snap.id_token));
        first_tok = snap.id_token.clone();
    }
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    let cached_again = cache
        .get_or_refresh("https://svc-a.run.app", false)
        .await
        .expect("cached hit");
    assert_eq!(cached_again.id_token, first_tok);
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    assert!(!cache.needs_refresh(
        "https://svc-a.run.app",
        cached_again.expires_at_unix_ms - 400_000
    ));
    assert!(cache.needs_refresh(
        "https://svc-a.run.app",
        cached_again.expires_at_unix_ms - 100_000
    ));

    cache.invalidate("https://svc-a.run.app", 999);
    let still_cached = cache
        .get_or_refresh("https://svc-a.run.app", false)
        .await
        .expect("stale invalidate is no-op");
    assert_eq!(still_cached.generation, 1);
    assert_eq!(call_count.load(Ordering::SeqCst), 1);

    cache.invalidate("https://svc-a.run.app", 1);
    let refreshed = cache
        .get_or_refresh("https://svc-a.run.app", false)
        .await
        .expect("refreshed after matching generation invalidate");
    assert_eq!(refreshed.generation, 2);
    assert_eq!(call_count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn id_token_metadata_formats_claims_validation_and_eviction() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let endpoint = format!("http://127.0.0.1:{}", addr.port());
    let request_urls = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let request_urls_srv = Arc::clone(&request_urls);
    let mode = Arc::new(std::sync::Mutex::new(String::from("normal")));
    let mode_srv = Arc::clone(&mode);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let urls = Arc::clone(&request_urls_srv);
            let mode_ref = Arc::clone(&mode_srv);
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                let first_line = req.lines().next().unwrap_or_default().to_string();
                assert!(req.to_ascii_lowercase().contains("metadata-flavor: google"));
                urls.lock().expect("lock").push(first_line.clone());

                let path_query = first_line.split_whitespace().nth(1).unwrap_or_default();
                let parsed =
                    url::Url::parse(&format!("http://localhost{path_query}")).expect("url");
                let aud = parsed
                    .query_pairs()
                    .find(|(k, _)| k == "audience")
                    .map(|(_, v)| v.into_owned())
                    .unwrap_or_default();

                let cur_mode = mode_ref.lock().expect("lock").clone();
                let body = match cur_mode.as_str() {
                    "malformed" => "not-a-jwt".to_string(),
                    "expired" => sign_test_id_token(serde_json::json!(aud), -10),
                    "wrong_aud" => {
                        sign_test_id_token(serde_json::json!("https://other.run.app"), 3600)
                    }
                    _ => {
                        let exp_offset = if aud.ends_with("aud-1") {
                            600
                        } else if aud.ends_with("aud-2") {
                            1200
                        } else {
                            1800
                        };
                        sign_test_id_token(serde_json::json!([aud]), exp_offset)
                    }
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    assert!(validate_id_token_audience("").is_err());
    assert!(validate_id_token_audience("   ").is_err());
    assert!(validate_id_token_audience("https://bad\n.run.app").is_err());
    assert!(validate_id_token_audience(&"a".repeat(2049)).is_err());

    let mut bad_skew = HashMap::new();
    bad_skew.insert("use_metadata_server".to_string(), "true".to_string());
    bad_skew.insert("id_token_refresh_skew_ms".to_string(), "nope".to_string());
    assert!(
        GcpIdTokenCache::from_options(&bad_skew)
            .unwrap_err()
            .contains("id_token_refresh_skew_ms")
    );

    let mut bad_fmt = HashMap::new();
    bad_fmt.insert("use_metadata_server".to_string(), "true".to_string());
    bad_fmt.insert(
        "metadata_id_token_format".to_string(),
        "compact".to_string(),
    );
    assert!(
        GcpIdTokenCache::from_options(&bad_fmt)
            .unwrap_err()
            .contains("metadata_id_token_format")
    );

    let mut bad_max = HashMap::new();
    bad_max.insert("use_metadata_server".to_string(), "true".to_string());
    bad_max.insert("max_id_token_audiences".to_string(), "0".to_string());
    assert!(
        GcpIdTokenCache::from_options(&bad_max)
            .unwrap_err()
            .contains("max_id_token_audiences")
    );

    let mut std_opts = HashMap::new();
    std_opts.insert("use_metadata_server".to_string(), "true".to_string());
    std_opts.insert("metadata_host".to_string(), endpoint.clone());
    std_opts.insert(
        "metadata_id_token_format".to_string(),
        "standard".to_string(),
    );
    std_opts.insert("max_id_token_audiences".to_string(), "2".to_string());
    let std_cache = GcpIdTokenCache::from_options(&std_opts).expect("standard metadata cache");

    assert!(
        std_cache
            .get_or_refresh("https://bad\x00.run.app", false)
            .await
            .is_err()
    );
    assert!(request_urls.lock().expect("lock").is_empty());

    let _s1 = std_cache
        .get_or_refresh("https://aud-1", false)
        .await
        .expect("aud-1");
    let _s2 = std_cache
        .get_or_refresh("https://aud-2", false)
        .await
        .expect("aud-2");
    let _s3 = std_cache
        .get_or_refresh("https://aud-3", false)
        .await
        .expect("aud-3 evicts earliest aud-1");

    let urls = request_urls.lock().expect("lock").clone();
    assert!(urls[0].contains("/computeMetadata/v1/instance/service-accounts/default/identity?"));
    assert!(urls[0].contains("format=standard"));
    assert!(urls[0].contains("audience=https%3A%2F%2Faud-1"));

    let now_ms = vertebra_core::gcp_auth::current_unix_ms();
    assert!(std_cache.get_cached("https://aud-1", now_ms).is_none());
    assert!(std_cache.get_cached("https://aud-2", now_ms).is_some());
    assert!(std_cache.get_cached("https://aud-3", now_ms).is_some());

    let mut full_opts = HashMap::new();
    full_opts.insert("use_metadata_server".to_string(), "true".to_string());
    full_opts.insert("metadata_host".to_string(), endpoint);
    let full_cache = GcpIdTokenCache::from_options(&full_opts).expect("full metadata cache");
    assert_eq!(DEFAULT_MAX_ID_TOKEN_AUDIENCES, 64);
    assert!(
        full_cache
            .get_or_refresh("https://aud-full", false)
            .await
            .is_ok()
    );
    assert!(
        request_urls
            .lock()
            .expect("lock")
            .last()
            .expect("last")
            .contains("format=full")
    );

    *mode.lock().expect("lock") = "malformed".to_string();
    assert!(
        full_cache
            .get_or_refresh("https://aud-bad-1", false)
            .await
            .is_err()
    );
    *mode.lock().expect("lock") = "expired".to_string();
    assert!(
        full_cache
            .get_or_refresh("https://aud-bad-2", false)
            .await
            .unwrap_err()
            .contains("expired")
    );
    *mode.lock().expect("lock") = "wrong_aud".to_string();
    assert!(
        full_cache
            .get_or_refresh("https://aud-bad-3", false)
            .await
            .unwrap_err()
            .contains("mismatch")
    );

    let full_default = GcpIdTokenOptions::default();
    assert_eq!(full_default.metadata_format, MetadataIdTokenFormat::Full);
}

#[tokio::test]
async fn id_token_impersonation_user_adc_cli_and_static_rejection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    let base_url = format!("http://127.0.0.1:{}", addr.port());
    let token_calls = Arc::new(AtomicUsize::new(0));
    let token_calls_srv = Arc::clone(&token_calls);
    let omit_id_token = Arc::new(AtomicBool::new(false));
    let omit_id_token_srv = Arc::clone(&omit_id_token);
    let saw_include_email = Arc::new(AtomicBool::new(false));
    let saw_include_email_srv = Arc::clone(&saw_include_email);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let tc = Arc::clone(&token_calls_srv);
            let omit = Arc::clone(&omit_id_token_srv);
            let inc = Arc::clone(&saw_include_email_srv);
            tokio::spawn(async move {
                let req = read_http_request(&mut stream).await;
                let (_, body) = req.split_once("\r\n\r\n").expect("body");
                let resp_body = if req.starts_with("POST /token ") {
                    tc.fetch_add(1, Ordering::SeqCst);
                    let form: HashMap<String, String> =
                        url::form_urlencoded::parse(body.as_bytes())
                            .into_owned()
                            .collect();
                    assert_eq!(
                        form.get("grant_type").map(String::as_str),
                        Some("refresh_token")
                    );
                    assert!(!form.contains_key("scope"));
                    if omit.load(Ordering::SeqCst) {
                        r#"{"access_token":"mock-oauth-token-only","expires_in":3600,"token_type":"Bearer"}"#.to_string()
                    } else {
                        let cid = form.get("client_id").map(String::as_str).unwrap_or("cid");
                        let id_tok = sign_test_id_token(serde_json::json!(cid), 3600);
                        format!(
                            r#"{{"access_token":"mock-oauth-token","expires_in":3600,"id_token":"{id_tok}"}}"#
                        )
                    }
                } else {
                    assert!(req.contains(":generateIdToken"));
                    assert!(req.contains("Bearer mock-oauth-token"));
                    let parsed: serde_json::Value = serde_json::from_str(body).expect("json body");
                    let aud = parsed["audience"].as_str().expect("audience");
                    if parsed["includeEmail"].as_bool() == Some(true) {
                        inc.store(true, Ordering::SeqCst);
                    }
                    let id_tok = sign_test_id_token(serde_json::json!(aud), 3600);
                    format!(r#"{{"token":"{id_tok}"}}"#)
                };
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    resp_body.len(),
                    resp_body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let mut static_opts = HashMap::new();
    static_opts.insert("access_token".to_string(), "mock-oauth-token".to_string());
    static_opts.insert("allow_ambient_adc".to_string(), "false".to_string());
    let static_err = GcpIdTokenCache::from_options(&static_opts)
        .expect_err("static access tokens must fail at construction");
    assert!(static_err.contains("static access tokens cannot mint ID tokens"));

    let mut user_opts = HashMap::new();
    user_opts.insert(
        "client_id".to_string(),
        "my-adc-client-id.apps.googleusercontent.com".to_string(),
    );
    user_opts.insert("client_secret".to_string(), "my-secret".to_string());
    user_opts.insert("refresh_token".to_string(), "my-refresh".to_string());
    user_opts.insert("token_uri".to_string(), format!("{base_url}/token"));
    user_opts.insert("proxy_policy".to_string(), "direct".to_string());

    let user_cache = GcpIdTokenCache::from_options(&user_opts).expect("authorized_user id cache");
    let snap = user_cache
        .get_or_refresh("my-adc-client-id.apps.googleusercontent.com", false)
        .await
        .expect("authorized_user client_id audience");
    assert_eq!(snap.audience, "my-adc-client-id.apps.googleusercontent.com");
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);

    let mismatch_err = user_cache
        .get_or_refresh("https://cloudrun-svc-abc.a.run.app", false)
        .await
        .expect_err("non-client_id audience must fail pre-network");
    assert!(mismatch_err.contains("target_service_account"));
    assert!(mismatch_err.contains("programmaticClients"));
    assert!(mismatch_err.contains("my-adc-client-id.apps.googleusercontent.com"));
    assert_eq!(token_calls.load(Ordering::SeqCst), 1);

    omit_id_token.store(true, Ordering::SeqCst);
    let missing_tok_err = user_cache
        .get_or_refresh("my-adc-client-id.apps.googleusercontent.com", true)
        .await
        .expect_err("missing id_token must fail loud without CLI fallback");
    assert!(missing_tok_err.contains("contained no id_token"));
    assert!(missing_tok_err.contains("gcloud auth application-default login"));
    omit_id_token.store(false, Ordering::SeqCst);

    let mut imp_opts = user_opts.clone();
    imp_opts.insert(
        "target_service_account".to_string(),
        "runner@proj.iam.gserviceaccount.com".to_string(),
    );
    imp_opts.insert("iam_endpoint".to_string(), base_url.clone());
    imp_opts.insert("include_email".to_string(), "true".to_string());
    let imp_cache = GcpIdTokenCache::from_options(&imp_opts).expect("impersonated user adc cache");
    let imp_a = imp_cache
        .get_or_refresh("https://cloudrun-a.run.app", false)
        .await
        .expect("impersonated aud A");
    let imp_b = imp_cache
        .get_or_refresh("https://cloudrun-b.run.app", false)
        .await
        .expect("impersonated aud B");
    assert_eq!(imp_a.audience, "https://cloudrun-a.run.app");
    assert_eq!(imp_b.audience, "https://cloudrun-b.run.app");
    assert!(saw_include_email.load(Ordering::SeqCst));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().expect("tempdir");
        let ok_cli = dir.path().join("mock-gcloud-id");
        let cli_tok = sign_test_id_token(serde_json::json!("https://cli-aud.run.app"), 3600);
        std::fs::write(
            &ok_cli,
            format!(
                "#!/bin/sh\nif [ \"$1\" = \"auth\" ] && [ \"$2\" = \"print-identity-token\" ] && [ \"$3\" = \"--audiences=https://cli-aud.run.app\" ] && [ \"$4\" = \"--quiet\" ]; then\n  echo \"{cli_tok}\"\n  exit 0\nfi\nexit 1\n"
            ),
        )
        .expect("write cli");
        std::fs::set_permissions(&ok_cli, std::fs::Permissions::from_mode(0o755))
            .expect("chmod cli");
        let cli_cache = GcpIdTokenCache::new(
            Arc::new(GcpTokenCache::new(
                GcpAuthSource::GcloudCli {
                    binary: ok_cli,
                    timeout: Duration::from_secs(2),
                    quota_project_id: None,
                    impersonate_service_account: None,
                },
                DEFAULT_REFRESH_SKEW_MS,
            )),
            GcpIdTokenOptions::default(),
        )
        .expect("cli id cache");
        let cli_snap = cli_cache
            .get_or_refresh("https://cli-aud.run.app", false)
            .await
            .expect("cli id token");
        assert_eq!(cli_snap.id_token, cli_tok);

        let timeout_cli = dir.path().join("mock-gcloud-timeout");
        std::fs::write(&timeout_cli, "#!/bin/sh\nsleep 60\n").expect("write sleep");
        std::fs::set_permissions(&timeout_cli, std::fs::Permissions::from_mode(0o755))
            .expect("chmod sleep");
        let timeout_cache = GcpIdTokenCache::new(
            Arc::new(GcpTokenCache::new(
                GcpAuthSource::GcloudCli {
                    binary: timeout_cli,
                    timeout: Duration::from_millis(150),
                    quota_project_id: None,
                    impersonate_service_account: None,
                },
                DEFAULT_REFRESH_SKEW_MS,
            )),
            GcpIdTokenOptions::default(),
        )
        .expect("timeout cli cache");
        let timeout_err = timeout_cache
            .get_or_refresh("https://cli-aud.run.app", false)
            .await
            .expect_err("cli timeout must err");
        assert!(timeout_err.contains("timed out after 150 ms"));
    }
}

#[test]
fn canonical_adc_explicit_error_isolation_and_metadata_fallback() {
    let _lock = ENV_MUTATION_LOCK.lock().expect("env lock");
    let dir = tempfile::tempdir().expect("tempdir");
    let valid_sa_path = dir.path().join("ambient_sa.json");
    std::fs::write(
        &valid_sa_path,
        serde_json::json!({
            "type": "service_account",
            "project_id": "ambient-proj",
            "private_key": TEST_RSA_PRIVATE_KEY_PEM,
            "client_email": "ambient@ambient-proj.iam.gserviceaccount.com",
            "token_uri": "http://127.0.0.1:9/token"
        })
        .to_string(),
    )
    .expect("write ambient sa");

    let empty_home = dir.path().join("empty_home");
    std::fs::create_dir_all(&empty_home).expect("mkdir");
    let valid_sa_str = valid_sa_path.to_string_lossy().into_owned();
    let empty_home_str = empty_home.to_string_lossy().into_owned();

    let _guard = EnvVarGuard::set(&[
        (
            "GOOGLE_APPLICATION_CREDENTIALS",
            Some(valid_sa_str.as_str()),
        ),
        ("CLOUDSDK_CONFIG", Some(empty_home_str.as_str())),
        ("HOME", Some(empty_home_str.as_str())),
        ("GOOGLE_OAUTH_ACCESS_TOKEN", None),
        ("CLOUDSDK_AUTH_ACCESS_TOKEN", None),
    ]);

    let mut bad_opts = HashMap::new();
    bad_opts.insert(
        "service_account_path".to_string(),
        dir.path()
            .join("does-not-exist.json")
            .to_string_lossy()
            .into_owned(),
    );
    assert!(
        GcpTokenCache::from_options(&bad_opts, DEFAULT_GCP_SCOPES).is_none(),
        "explicit missing service_account_path must not fall through to ambient ADC"
    );

    let mut bad_json_opts = HashMap::new();
    bad_json_opts.insert("service_account_key".to_string(), "{not-json".to_string());
    assert!(
        GcpTokenCache::from_options(&bad_json_opts, DEFAULT_GCP_SCOPES).is_none(),
        "explicit malformed service_account_key must not fall through to ambient ADC"
    );

    drop(_guard);
    let _no_adc_guard = EnvVarGuard::set(&[
        ("GOOGLE_APPLICATION_CREDENTIALS", None),
        ("CLOUDSDK_CONFIG", Some(empty_home_str.as_str())),
        ("HOME", Some(empty_home_str.as_str())),
        ("GOOGLE_OAUTH_ACCESS_TOKEN", None),
        ("CLOUDSDK_AUTH_ACCESS_TOKEN", None),
    ]);

    let fallback_cache = GcpTokenCache::from_options(&HashMap::new(), DEFAULT_GCP_SCOPES)
        .expect("ambient ADC discovery must end with MetadataServer");
    assert!(matches!(
        fallback_cache.auth_source(),
        GcpAuthSource::MetadataServer { .. }
    ));

    let mut no_ambient_opts = HashMap::new();
    no_ambient_opts.insert("allow_ambient_adc".to_string(), "false".to_string());
    assert!(
        GcpTokenCache::from_options(&no_ambient_opts, DEFAULT_GCP_SCOPES).is_none(),
        "allow_ambient_adc=false must disable MetadataServer fallback"
    );
}
