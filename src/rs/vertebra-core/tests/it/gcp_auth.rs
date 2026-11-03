//! Integration tests for `vertebra_core::gcp_auth`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_core::gcp_auth::{DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache};

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
    opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/bigquery, https://www.googleapis.com/auth/spanner.data"
            .to_string(),
    );

    let cache = GcpTokenCache::from_options(&opts).expect("build service account cache");
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
    let cache = GcpTokenCache::from_options(&opts).expect("build service account cache");

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

    let auth_user_cache = GcpTokenCache::new(
        GcpAuthSource::AuthorizedUser {
            client_id: "cid".to_string(),
            client_secret: "csec".to_string(),
            refresh_token: "rtok".to_string(),
            quota_project_id: None,
            token_uri: "http://127.0.0.1:9/token".to_string(),
        },
        DEFAULT_REFRESH_SKEW_MS,
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
    opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/bigquery https://www.googleapis.com/auth/spanner.data"
            .to_string(),
    );

    let cache = GcpTokenCache::from_options(&opts).expect("build impersonated cache");
    let snap = cache.get_or_refresh().await.expect("impersonated token");
    assert_eq!(snap.access_token, "impersonated-custom-scope-token");
}
