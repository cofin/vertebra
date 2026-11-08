use _storage::oci::{
    EMPTY_BODY_SHA256_BASE64, OciCavageCredentials, R2_UNIFORM_PART_SIZE, chunk_uniform_parts,
    sha256_base64, sign_oci_cavage_request,
};
use _storage::signing::{
    AzureSasConfig, GcsSignConfig, HmacSigner, S3SignConfig, sign_azure_sas_url,
    sign_gcs_v4_with_iam_endpoint, sign_gcs_v4_with_options_and_cache, sign_s3_v4_url,
    verify_s3_v4_url,
};
use _storage::store::{BackendConfig, ObjectStoreEngine};
use bytes::Bytes;
use std::collections::HashMap;
use tempfile::TempDir;

#[tokio::test]
async fn test_memory_backend_crud_range_and_multipart() {
    let engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");

    let put_meta = engine
        .put(
            "datasets/events.bin",
            Bytes::from_static(b"0123456789abcdef"),
        )
        .await
        .expect("put bytes");
    assert_eq!(put_meta.path, "datasets/events.bin");
    assert_eq!(put_meta.size, 16);

    let full = engine.get("datasets/events.bin").await.expect("get full");
    assert_eq!(&full[..], b"0123456789abcdef");

    let slice = engine
        .get_range("datasets/events.bin", 4, 8)
        .await
        .expect("get range");
    assert_eq!(&slice[..], b"456789ab");

    let head = engine.head("datasets/events.bin").await.expect("head");
    assert_eq!(head.size, 16);

    engine
        .copy("datasets/events.bin", "datasets/events_copy.bin")
        .await
        .expect("copy");

    let listed = engine.list(Some("datasets")).await.expect("list");
    assert_eq!(listed.len(), 2);

    let mp_meta = engine
        .put_multipart(
            "datasets/multipart.bin",
            vec![
                Bytes::from_static(b"part-one-"),
                Bytes::from_static(b"part-two"),
            ],
        )
        .await
        .expect("multipart upload");
    assert_eq!(mp_meta.size, 17);

    engine
        .delete("datasets/events_copy.bin")
        .await
        .expect("delete");
    let remaining = engine
        .list(Some("datasets"))
        .await
        .expect("list after delete");
    assert_eq!(remaining.len(), 2);
}

#[tokio::test]
async fn test_local_filesystem_backend_roundtrip() {
    let tmp = TempDir::new().expect("tempdir");
    let url = format!("file://{}", tmp.path().display());
    let engine = ObjectStoreEngine::from_url(&url, None).expect("local engine");

    engine
        .put(
            "nested/report.txt",
            Bytes::from_static(b"vertebra-storage-local"),
        )
        .await
        .expect("put local");
    let data = engine.get("nested/report.txt").await.expect("get local");
    assert_eq!(&data[..], b"vertebra-storage-local");
}

#[test]
fn test_hmac_and_s3_v4_signing_and_verification() {
    let signer = HmacSigner::new(b"super-secret-signing-key-32bytes");
    let signed = signer
        .sign_url(
            "https://cdn.example.com/assets/logo.png",
            "GET",
            3600,
            1_700_000_000,
        )
        .expect("sign hmac");
    assert!(
        signer
            .verify_url(&signed, "GET", 1_700_000_100)
            .expect("verify valid hmac")
    );
    assert!(
        !signer
            .verify_url(&signed, "GET", 1_700_004_000)
            .expect("verify expired hmac")
    );

    let s3_cfg = S3SignConfig {
        bucket: "artifacts".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        endpoint: Some("https://s3.us-east-1.amazonaws.com".to_string()),
    };
    let presigned = sign_s3_v4_url(
        &s3_cfg,
        "GET",
        "models/v1/weights.safetensors",
        900,
        1_700_000_000,
    )
    .expect("s3 v4 presign");
    assert!(presigned.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
    assert!(verify_s3_v4_url(&s3_cfg, "GET", &presigned, 1_700_000_100).expect("s3 v4 verify"));
}

#[tokio::test]
async fn test_gcs_v4_keyless_iam_signblob_fallback() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock iam");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        if let Ok((mut socket, _)) = listener.accept().await {
            let mut buf = vec![0u8; 4096];
            let _ = socket.read(&mut buf).await;
            let body = r#"{"keyId":"key-1","signedBlob":"ZGVhZGJlZWZjYWZl"}"#;
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = socket.write_all(resp.as_bytes()).await;
        }
    });

    let gcs_cfg = GcsSignConfig {
        bucket: "ml-bundles".to_string(),
        service_account_email: "runner@project.iam.gserviceaccount.com".to_string(),
        private_key_pem: None,
        access_token: Some("mock-oauth-token".to_string()),
    };
    let iam_base = format!("http://{addr}");
    let url = sign_gcs_v4_with_iam_endpoint(
        &gcs_cfg,
        "GET",
        "checkpoints/step-100.pt",
        600,
        1_700_000_000,
        &iam_base,
    )
    .await
    .expect("gcs keyless signBlob");
    assert!(url.contains("X-Goog-Algorithm=GOOG4-RSA-SHA256"));
    assert!(url.contains("X-Goog-Signature=646561646265656663616665"));
    server.await.expect("mock server task");
}

#[test]
fn test_r2_uniform_part_chunking_and_azure_sas_signing() {
    let payload = Bytes::from(vec![b'a'; R2_UNIFORM_PART_SIZE * 2 + 1234]);
    let parts = chunk_uniform_parts(payload, R2_UNIFORM_PART_SIZE);
    assert_eq!(parts.len(), 3);
    assert_eq!(parts[0].len(), R2_UNIFORM_PART_SIZE);
    assert_eq!(parts[1].len(), R2_UNIFORM_PART_SIZE);
    assert_eq!(parts[2].len(), 1234);

    let sas_cfg = AzureSasConfig {
        account: "devstoreaccount1".to_string(),
        container: "datasets".to_string(),
        access_key_b64:
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                .to_string(),
        endpoint: Some("http://127.0.0.1:4577/devstoreaccount1".to_string()),
    };
    let sas_url = sign_azure_sas_url(
        &sas_cfg,
        "GET",
        "parquet/part-0.parquet",
        3600,
        1_700_000_000,
    )
    .expect("azure sas url");
    assert!(sas_url.contains("sv=2020-12-06"));
    assert!(sas_url.contains("sp=r"));
    assert!(sas_url.contains("sig="));
}

#[tokio::test]
async fn test_oci_cavage_signing_and_native_rest_v2_crud() {
    let pem = concat!(
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
    )
    .to_string();

    let creds = OciCavageCredentials {
        tenancy_ocid: "ocid1.tenancy.oc1..aaaa".to_string(),
        user_ocid: "ocid1.user.oc1..bbbb".to_string(),
        fingerprint: "12:34:56:78:90:ab:cd:ef".to_string(),
        private_key_pem: pem.clone(),
    };

    assert_eq!(sha256_base64(&[]), EMPTY_BODY_SHA256_BASE64);
    let get_headers = sign_oci_cavage_request(
        &creds,
        "GET",
        "https://objectstorage.us-ashburn-1.oraclecloud.com/n/myns/b/mybucket/o/file.parquet",
        None,
        None,
        1_700_000_000,
    )
    .expect("sign OCI GET");
    assert!(
        get_headers
            .authorization
            .contains("headers=\"(request-target) date host\"")
    );
    assert!(get_headers.x_content_sha256.is_none());

    let empty_post_headers = sign_oci_cavage_request(
        &creds,
        "POST",
        "https://objectstorage.us-ashburn-1.oraclecloud.com/n/myns/b/mybucket/u",
        Some(&[]),
        Some("application/json"),
        1_700_000_000,
    )
    .expect("sign OCI 0-byte POST");
    assert_eq!(
        empty_post_headers.x_content_sha256.as_deref(),
        Some(EMPTY_BODY_SHA256_BASE64)
    );
    assert!(empty_post_headers.authorization.contains(
        "headers=\"(request-target) date host x-content-sha256 content-type content-length\""
    ));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock oci server");
    let addr = listener.local_addr().expect("local addr");

    let server = tokio::spawn(async move {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        for step in 0..3 {
            if let Ok((mut socket, _)) = listener.accept().await {
                let mut buf = vec![0u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                assert!(
                    req_str.contains("authorization: Signature version=\"1\"")
                        || req_str.contains("Authorization: Signature version=\"1\"")
                );
                let resp = match step {
                    0 => {
                        assert!(req_str.starts_with("PUT /n/myns/b/mybucket/o/"));
                        "HTTP/1.1 200 OK\r\netag: \"oci-etag-1\"\r\nopc-version-id: v1\r\nContent-Length: 0\r\n\r\n".to_string()
                    }
                    1 => {
                        assert!(req_str.starts_with("GET /n/myns/b/mybucket/o/"));
                        let body = "oci-native-rest-v2-payload";
                        format!(
                            "HTTP/1.1 200 OK\r\netag: \"oci-etag-1\"\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                    _ => {
                        assert!(req_str.starts_with("GET /n/myns/b/mybucket/o?"));
                        let body = r#"{"objects":[{"name":"datasets/test.bin","size":26,"etag":"oci-etag-1"}]}"#;
                        format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                            body.len(),
                            body
                        )
                    }
                };
                let _ = socket.write_all(resp.as_bytes()).await;
            }
        }
    });

    let mut opts = HashMap::new();
    opts.insert("endpoint".to_string(), format!("http://{addr}"));
    opts.insert("tenancy_ocid".to_string(), creds.tenancy_ocid);
    opts.insert("user_ocid".to_string(), creds.user_ocid);
    opts.insert("fingerprint".to_string(), creds.fingerprint);
    opts.insert("private_key_pem".to_string(), pem);

    let engine =
        ObjectStoreEngine::from_url("oci://mybucket@myns", Some(&opts)).expect("build OCI engine");
    let put_meta = engine
        .put(
            "datasets/test.bin",
            Bytes::from_static(b"oci-native-rest-v2-payload"),
        )
        .await
        .expect("OCI put");
    assert_eq!(put_meta.etag.as_deref(), Some("\"oci-etag-1\""));

    let fetched = engine.get("datasets/test.bin").await.expect("OCI get");
    assert_eq!(&fetched[..], b"oci-native-rest-v2-payload");

    let listed = engine.list(Some("datasets")).await.expect("OCI list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, "datasets/test.bin");

    server.await.expect("join mock oci server");
}

#[tokio::test]
async fn gcs_v4_presign_auto_local_rsa_from_adc_service_account_zero_rtt() {
    use rsa::RsaPrivateKey;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::signature::Verifier;
    use sha2::Sha256;
    use vertebra_core::util::sha256_hex;

    let pem = concat!(
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

    let tmp = TempDir::new().expect("tempdir");
    let sa_path = tmp.path().join("sa_adc.json");
    let sa_json = serde_json::json!({
        "type": "service_account",
        "project_id": "analytics-proj",
        "private_key": pem,
        "client_email": "sa-local@analytics.iam.gserviceaccount.com"
    });
    std::fs::write(&sa_path, sa_json.to_string()).expect("write sa json");

    let mut creds_opts = HashMap::new();
    creds_opts.insert(
        "application_credentials".to_string(),
        sa_path.to_string_lossy().into_owned(),
    );

    let gcs_cfg = GcsSignConfig {
        bucket: "analytics-bucket".to_string(),
        service_account_email: "sa-local@analytics.iam.gserviceaccount.com".to_string(),
        private_key_pem: None,
        access_token: None,
    };

    let signed_url = sign_gcs_v4_with_options_and_cache(
        &gcs_cfg,
        Some(&creds_opts),
        None,
        "GET",
        "exports/part-000.parquet",
        600,
        1_700_000_000,
        "http://127.0.0.1:9",
    )
    .await
    .expect("zero-RTT local RSA signing should succeed without hitting unreachable IAM");

    let (before_sig, sig_hex) = signed_url
        .split_once("&X-Goog-Signature=")
        .expect("signature query parameter");
    let (_, canonical_qs) = before_sig.split_once('?').expect("query string");
    let canonical_req = format!(
        "GET\n/analytics-bucket/exports/part-000.parquet\n{canonical_qs}\nhost:storage.googleapis.com\n\nhost\nUNSIGNED-PAYLOAD"
    );
    let string_to_sign = format!(
        "GOOG4-RSA-SHA256\n20231114T221320Z\n20231114/auto/storage/goog4_request\n{}",
        sha256_hex(canonical_req.as_bytes())
    );
    let sig_bytes = hex::decode(sig_hex).expect("hex signature");
    let signature = Signature::try_from(sig_bytes.as_slice()).expect("rsa signature");
    let priv_key = RsaPrivateKey::from_pkcs8_pem(pem).expect("parse rsa key");
    let verifying_key = VerifyingKey::<Sha256>::new(priv_key.to_public_key());
    verifying_key
        .verify(string_to_sign.as_bytes(), &signature)
        .expect("RSA signature must verify against public key");
}

#[tokio::test]
async fn gcs_v4_presign_iam_sign_blob_reuses_cached_token_and_retries_on_401() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback server");
    let addr = listener.local_addr().expect("local addr");
    let base_url = format!("http://{addr}");

    let oauth_calls = Arc::new(AtomicUsize::new(0));
    let iam_calls = Arc::new(AtomicUsize::new(0));
    let oauth_calls_srv = Arc::clone(&oauth_calls);
    let iam_calls_srv = Arc::clone(&iam_calls);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let oauth_c = Arc::clone(&oauth_calls_srv);
            let iam_c = Arc::clone(&iam_calls_srv);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req_str = String::from_utf8_lossy(&buf[..n]);
                if req_str.starts_with("POST /token ") {
                    let seq = oauth_c.fetch_add(1, Ordering::SeqCst) + 1;
                    let body = format!(
                        r#"{{"access_token":"test-access-token-{seq}","expires_in":3600,"token_type":"Bearer"}}"#
                    );
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = stream.write_all(resp.as_bytes()).await;
                } else if req_str.contains(":signBlob") {
                    let call_no = iam_c.fetch_add(1, Ordering::SeqCst) + 1;
                    if call_no == 2 {
                        let body = r#"{"error":{"code":401,"message":"token expired"}}"#;
                        let resp = format!(
                            "HTTP/1.1 401 Unauthorized\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                    } else {
                        let expected_tok = if call_no == 1 {
                            "Bearer test-access-token-1"
                        } else {
                            "Bearer test-access-token-2"
                        };
                        assert!(
                            req_str
                                .to_ascii_lowercase()
                                .contains(&expected_tok.to_ascii_lowercase())
                        );
                        let body = r#"{"keyId":"key-1","signedBlob":"ZGVhZGJlZWZjYWZl"}"#;
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                            body.len(),
                            body
                        );
                        let _ = stream.write_all(resp.as_bytes()).await;
                    }
                }
            });
        }
    });

    let tmp = TempDir::new().expect("tempdir");
    let adc_path = tmp.path().join("authorized_user.json");
    let adc_json = serde_json::json!({
        "type": "authorized_user",
        "client_id": "cid-1",
        "client_secret": "csec-1",
        "refresh_token": "rtok-1",
        "token_uri": format!("{base_url}/token")
    });
    std::fs::write(&adc_path, adc_json.to_string()).expect("write authorized_user json");

    let mut creds_opts = HashMap::new();
    creds_opts.insert(
        "application_credentials".to_string(),
        adc_path.to_string_lossy().into_owned(),
    );

    let gcs_cfg = GcsSignConfig {
        bucket: "ml-bundles".to_string(),
        service_account_email: "runner@project.iam.gserviceaccount.com".to_string(),
        private_key_pem: None,
        access_token: None,
    };

    for _ in 0..3 {
        let url = sign_gcs_v4_with_options_and_cache(
            &gcs_cfg,
            Some(&creds_opts),
            None,
            "GET",
            "checkpoints/step-100.pt",
            600,
            1_700_000_000,
            &base_url,
        )
        .await
        .expect("gcs signBlob with cached token and 401 retry");
        assert!(url.contains("X-Goog-Signature=646561646265656663616665"));
    }

    assert_eq!(oauth_calls.load(Ordering::SeqCst), 2);
    assert_eq!(iam_calls.load(Ordering::SeqCst), 4);
}

#[test]
fn block_on_storage_succeeds_inside_multi_thread_runtime_and_enter() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("runtime");
    let out = rt.block_on(async {
        _storage::store::block_on_storage(async {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            42_u64
        })
    });
    assert_eq!(out, 42);

    let _guard = rt.enter();
    let entered = _storage::store::block_on_storage(async { 7_u64 });
    assert_eq!(entered, 7);
}

#[tokio::test]
async fn test_refreshing_gcp_credential_provider_over_core_token_cache() {
    use _storage::gcp_auth::{GCS_OAUTH_SCOPES, RefreshingGcpCredentialProvider};
    use object_store::CredentialProvider;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use vertebra_core::gcp_auth::{
        DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache, ProxyPolicy,
    };

    assert!(GCS_OAUTH_SCOPES.contains(&"https://www.googleapis.com/auth/devstorage.full_control"));
    assert!(GCS_OAUTH_SCOPES.contains(&"https://www.googleapis.com/auth/cloud-platform"));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock oauth");
    let addr = listener.local_addr().expect("local addr");
    let calls = Arc::new(AtomicUsize::new(0));
    let calls_srv = Arc::clone(&calls);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let c = Arc::clone(&calls_srv);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf).await;
                let seq = c.fetch_add(1, Ordering::SeqCst) + 1;
                let body = format!(
                    r#"{{"access_token":"tok_{seq}","expires_in":3600,"token_type":"Bearer"}}"#
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let cache = Arc::new(GcpTokenCache::with_proxy_policy(
        GcpAuthSource::AuthorizedUser {
            client_id: "cid".to_string(),
            client_secret: "csec".to_string(),
            refresh_token: "rtok".to_string(),
            quota_project_id: Some("proj-storage".to_string()),
            token_uri: format!("http://127.0.0.1:{}/token", addr.port()),
        },
        DEFAULT_REFRESH_SKEW_MS,
        ProxyPolicy::Direct,
    ));
    let provider = RefreshingGcpCredentialProvider::new(Arc::clone(&cache));
    assert!(Arc::ptr_eq(provider.cache(), &cache));

    let cred1 = provider.get_credential().await.expect("get_credential 1");
    assert_eq!(cred1.bearer, "tok_1");
    let cred2 = provider.get_credential().await.expect("get_credential 2");
    assert_eq!(cred2.bearer, "tok_1");
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    cache.invalidate(1);
    let cred3 = provider
        .get_credential()
        .await
        .expect("get_credential after invalidate");
    assert_eq!(cred3.bearer, "tok_2");
    assert_eq!(calls.load(Ordering::SeqCst), 2);
}

#[test]
fn test_multicloud_presigning_parity_rfc3986_oci_par_and_expiry_bounds() {
    use _storage::oci::{
        OciCavageCredentials, OciParConfig, build_oci_par_url, sign_oci_request_headers,
    };
    use _storage::signing::{
        SigningError, assemble_gcs_iam_v4_url, build_gcs_v4_canonical_request, sign_gcs_v4_url,
    };
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use hmac::{Hmac, Mac};
    use rsa::RsaPrivateKey;
    use rsa::pkcs1v15::{SigningKey, VerifyingKey};
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::signature::{SignatureEncoding, Signer, Verifier};
    use sha2::Sha256;

    let pem = concat!(
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

    let fixed_ts = chrono::NaiveDate::from_ymd_opt(2026, 11, 7)
        .expect("valid date")
        .and_hms_opt(0, 0, 0)
        .expect("valid time")
        .and_utc()
        .timestamp();

    let gcs_cfg = GcsSignConfig {
        bucket: "analytics-bkt".to_string(),
        service_account_email: "signer@proj.iam.gserviceaccount.com".to_string(),
        private_key_pem: Some(pem.to_string()),
        access_token: None,
    };
    let local_gcs_url = sign_gcs_v4_url(&gcs_cfg, "GET", "dir/a b+c.txt", 900, fixed_ts)
        .expect("local GCS V4 RSA sign");
    assert!(
        local_gcs_url
            .starts_with("https://storage.googleapis.com/analytics-bkt/dir/a%20b%2Bc.txt?")
    );
    assert!(local_gcs_url.contains("X-Goog-Date=20261107T000000Z"));

    let canonical = build_gcs_v4_canonical_request(
        "analytics-bkt",
        "signer@proj.iam.gserviceaccount.com",
        "GET",
        "dir/a b+c.txt",
        900,
        fixed_ts,
    )
    .expect("build canonical request");
    let rsa_priv = RsaPrivateKey::from_pkcs8_pem(pem).expect("parse test rsa");
    let sig_bytes = SigningKey::<Sha256>::new(rsa_priv.clone())
        .sign(canonical.string_to_sign.as_bytes())
        .to_bytes();
    let iam_assembled_url = assemble_gcs_iam_v4_url(&canonical, &hex::encode_upper(&sig_bytes))
        .expect("assemble IAM URL");
    assert_eq!(local_gcs_url, iam_assembled_url);

    let utf8_gcs_url = sign_gcs_v4_url(
        &gcs_cfg,
        "PUT",
        "reports/2026 Q4/summary+final.parquet",
        900,
        fixed_ts,
    )
    .expect("GCS V4 complex path");
    assert!(utf8_gcs_url.starts_with(
        "https://storage.googleapis.com/analytics-bkt/reports/2026%20Q4/summary%2Bfinal.parquet?"
    ));

    let bad_gcs_cfg = GcsSignConfig {
        private_key_pem: Some("not-a-valid-pem".to_string()),
        ..gcs_cfg.clone()
    };
    assert!(matches!(
        sign_gcs_v4_url(&bad_gcs_cfg, "GET", "dir/a.txt", 900, fixed_ts),
        Err(SigningError::InvalidKey(_))
    ));
    assert!(matches!(
        assemble_gcs_iam_v4_url(&canonical, "zz-not-hex"),
        Err(SigningError::InvalidKey(_))
    ));

    let r2_cfg = S3SignConfig {
        bucket: "assets".to_string(),
        region: "auto".to_string(),
        access_key_id: "r2-access-key".to_string(),
        secret_access_key: "r2-secret-key".to_string(),
        endpoint: Some("https://acct.r2.cloudflarestorage.com".to_string()),
    };
    let r2_url =
        sign_s3_v4_url(&r2_cfg, "GET", "img/logo.png", 3600, fixed_ts).expect("R2 SigV4 sign");
    assert!(r2_url.starts_with("https://acct.r2.cloudflarestorage.com/assets/img/logo.png?"));
    assert!(r2_url.contains("X-Amz-Algorithm=AWS4-HMAC-SHA256"));
    assert!(
        r2_url.contains("X-Amz-Credential=r2-access-key%2F20261107%2Fauto%2Fs3%2Faws4_request")
    );
    let (_, r2_sig) = r2_url
        .split_once("&X-Amz-Signature=")
        .expect("X-Amz-Signature");
    assert_eq!(r2_sig.len(), 64);
    assert!(r2_sig.chars().all(|c| c.is_ascii_hexdigit()));
    let r2_url_repeat =
        sign_s3_v4_url(&r2_cfg, "GET", "img/logo.png", 3600, fixed_ts).expect("R2 SigV4 repeat");
    assert_eq!(r2_url, r2_url_repeat);
    assert!(verify_s3_v4_url(&r2_cfg, "GET", &r2_url, fixed_ts + 60).expect("verify R2 URL"));

    let r2_encoded = sign_s3_v4_url(
        &r2_cfg,
        "PUT",
        "reports/2026 Q4/summary+final.parquet",
        3600,
        fixed_ts,
    )
    .expect("R2 special key");
    assert!(r2_encoded.starts_with(
        "https://acct.r2.cloudflarestorage.com/assets/reports/2026%20Q4/summary%2Bfinal.parquet?"
    ));
    assert!(
        verify_s3_v4_url(&r2_cfg, "PUT", &r2_encoded, fixed_ts + 60)
            .expect("verify R2 special key")
    );

    let raw_az_key = b"0123456789abcdef0123456789abcdef";
    let az_cfg = AzureSasConfig {
        account: "acct1".to_string(),
        container: "container".to_string(),
        access_key_b64: BASE64_STANDARD.encode(raw_az_key),
        endpoint: None,
    };
    let az_url =
        sign_azure_sas_url(&az_cfg, "PUT", "blob/file.csv", 3600, fixed_ts).expect("Azure SAS PUT");
    assert!(az_url.starts_with("https://acct1.blob.core.windows.net/container/blob/file.csv?"));
    assert!(az_url.contains("sv=2020-12-06"));
    assert!(az_url.contains("sr=b"));
    assert!(az_url.contains("sp=cw"));
    assert!(az_url.contains("se=2026-11-07T01%3A00%3A00Z"));
    let parsed_az = url::Url::parse(&az_url).expect("parse Azure SAS URL");
    let az_sig = parsed_az
        .query_pairs()
        .find(|(k, _)| k == "sig")
        .map(|(_, v)| v.into_owned())
        .expect("sig param");
    let expected_sts = "cw\n2026-11-07T00:00:00Z\n2026-11-07T01:00:00Z\n/blob/acct1/container/blob/file.csv\n\n\nhttps\n2020-12-06\nb\n\n\n\n\n\n";
    let mut mac = Hmac::<Sha256>::new_from_slice(raw_az_key).expect("hmac key");
    mac.update(expected_sts.as_bytes());
    let expected_az_sig = BASE64_STANDARD.encode(mac.finalize().into_bytes());
    assert_eq!(az_sig, expected_az_sig);

    let bad_az_cfg = AzureSasConfig {
        access_key_b64: "%%%not-valid-base64%%%".to_string(),
        ..az_cfg.clone()
    };
    assert!(matches!(
        bad_az_cfg.sign_sas_url("PUT", "blob/file.csv", 3600, fixed_ts),
        Err(SigningError::InvalidKey(_))
    ));
    assert!(sign_azure_sas_url(&bad_az_cfg, "PUT", "blob/file.csv", 3600, fixed_ts).is_err());

    let oci_par_cfg = OciParConfig {
        region: "us-ashburn-1".to_string(),
        namespace: "ns1".to_string(),
        bucket: "bkt".to_string(),
        par_token: "p/tok/".to_string(),
        endpoint: None,
    };
    let oci_url = build_oci_par_url(&oci_par_cfg, "k/1.bin", 3600).expect("OCI PAR URL");
    assert_eq!(
        oci_url,
        "https://objectstorage.us-ashburn-1.oraclecloud.com/p/tok/n/ns1/b/bkt/o/k%2F1.bin"
    );

    let oci_creds = OciCavageCredentials {
        tenancy_ocid: "ocid1.tenancy.oc1..aaaa".to_string(),
        user_ocid: "ocid1.user.oc1..bbbb".to_string(),
        fingerprint: "aa:bb:cc".to_string(),
        private_key_pem: pem.to_string(),
    };
    let oci_hdrs = sign_oci_request_headers(
        &oci_creds,
        "GET",
        "https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns1/b/bkt/o/k%2F1.bin",
        None,
        None,
        fixed_ts,
    )
    .expect("sign OCI request headers");
    let sig_part = oci_hdrs
        .authorization
        .split("signature=\"")
        .nth(1)
        .and_then(|s| s.strip_suffix('"'))
        .expect("OCI signature field");
    let oci_sig_bytes = BASE64_STANDARD.decode(sig_part).expect("decode OCI sig");
    let oci_rsa_sig =
        rsa::pkcs1v15::Signature::try_from(oci_sig_bytes.as_slice()).expect("rsa sig");
    let expected_oci_signing_str = format!(
        "(request-target): get /n/ns1/b/bkt/o/k%2F1.bin\ndate: {}\nhost: objectstorage.us-ashburn-1.oraclecloud.com",
        oci_hdrs.date
    );
    VerifyingKey::<Sha256>::new(rsa_priv.to_public_key())
        .verify(expected_oci_signing_str.as_bytes(), &oci_rsa_sig)
        .expect("OCI RSA signature must verify");

    let bad_oci_creds = OciCavageCredentials {
        private_key_pem: "invalid-pem".to_string(),
        ..oci_creds
    };
    assert!(matches!(
        sign_oci_request_headers(
            &bad_oci_creds,
            "GET",
            "https://objectstorage.us-ashburn-1.oraclecloud.com/n/ns1/b/bkt/o/k%2F1.bin",
            None,
            None,
            fixed_ts,
        ),
        Err(SigningError::InvalidKey(_))
    ));

    for bad_exp in [0_u64, 604_801_u64] {
        assert!(matches!(
            sign_gcs_v4_url(&gcs_cfg, "GET", "dir/a.txt", bad_exp, fixed_ts),
            Err(SigningError::InvalidExpiry(_))
        ));
        assert!(matches!(
            build_gcs_v4_canonical_request(
                "analytics-bkt",
                "signer@proj.iam.gserviceaccount.com",
                "GET",
                "dir/a.txt",
                bad_exp,
                fixed_ts,
            ),
            Err(SigningError::InvalidExpiry(_))
        ));
        assert!(matches!(
            r2_cfg.sign_v4_url("GET", "img/logo.png", bad_exp, fixed_ts),
            Err(SigningError::InvalidExpiry(_))
        ));
        assert!(sign_s3_v4_url(&r2_cfg, "GET", "img/logo.png", bad_exp, fixed_ts).is_err());
        assert!(matches!(
            az_cfg.sign_sas_url("PUT", "blob/file.csv", bad_exp, fixed_ts),
            Err(SigningError::InvalidExpiry(_))
        ));
        assert!(sign_azure_sas_url(&az_cfg, "PUT", "blob/file.csv", bad_exp, fixed_ts).is_err());
        assert!(matches!(
            build_oci_par_url(&oci_par_cfg, "k/1.bin", bad_exp),
            Err(SigningError::InvalidExpiry(_))
        ));
    }

    assert!(sign_gcs_v4_url(&gcs_cfg, "GET", "dir/a.txt", 1, fixed_ts).is_ok());
    assert!(sign_gcs_v4_url(&gcs_cfg, "GET", "dir/a.txt", 604_800, fixed_ts).is_ok());
    assert!(sign_s3_v4_url(&r2_cfg, "GET", "img/logo.png", 604_800, fixed_ts).is_ok());
    assert!(sign_azure_sas_url(&az_cfg, "PUT", "blob/file.csv", 604_800, fixed_ts).is_ok());
    assert!(build_oci_par_url(&oci_par_cfg, "k/1.bin", 604_800).is_ok());
}

#[tokio::test]
async fn test_streaming_chunk_read_memory_and_local() {
    use futures::TryStreamExt;

    let expected: Vec<u8> = (0..(3 * 65536)).map(|i| (i % 251) as u8).collect();
    let payload = Bytes::from(expected.clone());

    let mem_engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
    mem_engine
        .put("blobs/stream.bin", payload.clone())
        .await
        .expect("put memory stream blob");
    let mem_chunks: Vec<Bytes> = mem_engine
        .get_stream("blobs/stream.bin")
        .await
        .expect("open memory stream")
        .try_collect()
        .await
        .expect("collect memory stream");
    assert!(!mem_chunks.is_empty());
    let mem_joined: Vec<u8> = mem_chunks.iter().flat_map(|b| b.iter().copied()).collect();
    assert_eq!(mem_joined.len(), 196_608);
    assert_eq!(mem_joined, expected);

    let tmp = TempDir::new().expect("tempdir");
    let url = format!("file://{}", tmp.path().display());
    let local_engine = ObjectStoreEngine::from_url(&url, None).expect("local engine");
    local_engine
        .put("blobs/stream.bin", payload)
        .await
        .expect("put local stream blob");
    let local_chunks: Vec<Bytes> = local_engine
        .get_stream("blobs/stream.bin")
        .await
        .expect("open local stream")
        .try_collect()
        .await
        .expect("collect local stream");
    assert!(!local_chunks.is_empty());
    let local_joined: Vec<u8> = local_chunks
        .iter()
        .flat_map(|b| b.iter().copied())
        .collect();
    assert_eq!(local_joined.len(), 196_608);
    assert_eq!(local_joined, expected);
}

#[tokio::test]
async fn test_incremental_r2_uniform_multipart_session() {
    assert_incremental_uniform_multipart_session("r2").await;
}

#[tokio::test]
async fn test_incremental_oci_uniform_multipart_session() {
    assert_incremental_uniform_multipart_session("oci").await;
}

#[tokio::test]
async fn test_incremental_s3_uniform_multipart_session() {
    assert_incremental_uniform_multipart_session("s3").await;
}

#[test]
fn test_s3_and_b2_url_aliases_normalize_to_s3_backend() {
    let options = HashMap::from([
        ("access_key_id".to_string(), "test".to_string()),
        ("secret_access_key".to_string(), "test".to_string()),
    ]);
    for scheme in ["s3", "b2"] {
        let engine = ObjectStoreEngine::from_url(&format!("{scheme}://fixture"), Some(&options))
            .expect("S3-family backend");
        assert_eq!(engine.scheme(), "s3");
    }
}

async fn assert_incremental_uniform_multipart_session(scheme: &str) {
    use _storage::store::PutConditionalOptions;
    use std::sync::Arc;

    let engine =
        ObjectStoreEngine::with_store(Arc::new(object_store::memory::InMemory::new()), scheme);
    let mut session = engine
        .create_multipart(
            "blobs/r2_incremental.bin",
            PutConditionalOptions {
                content_type: Some("application/octet-stream".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("create r2 multipart session");

    let chunk_sizes = [
        3 * 1024 * 1024,
        3 * 1024 * 1024,
        3 * 1024 * 1024,
        1024 * 1024,
        512 * 1024,
    ];
    let mut expected = Vec::with_capacity(chunk_sizes.iter().sum());
    for (part_idx, &len) in chunk_sizes.iter().enumerate() {
        let data: Vec<u8> = (0..len)
            .map(|offset| ((part_idx * 37 + offset) % 253) as u8)
            .collect();
        expected.extend_from_slice(&data);
        session
            .write_chunk(Bytes::from(data))
            .await
            .expect("write_chunk");
        if part_idx < 2 {
            assert!(
                session.part_sizes_flushed().is_empty(),
                "must buffer until 8 MiB threshold"
            );
        } else {
            assert_eq!(
                session.part_sizes_flushed(),
                &[R2_UNIFORM_PART_SIZE],
                "must flush exact 8 MiB part immediately upon crossing threshold"
            );
        }
    }

    let _put_res = session.complete().await.expect("complete r2 multipart");
    assert_eq!(
        session.part_sizes_flushed(),
        &[R2_UNIFORM_PART_SIZE, 2 * 1024 * 1024 + 512 * 1024]
    );

    let fetched = engine
        .get("blobs/r2_incremental.bin")
        .await
        .expect("get completed r2 object");
    assert_eq!(fetched.len(), 10 * 1024 * 1024 + 512 * 1024);
    assert_eq!(&fetched[..], &expected[..]);

    let mut empty_session = engine
        .create_multipart("blobs/r2_empty.bin", PutConditionalOptions::default())
        .await
        .expect("create empty multipart");
    empty_session
        .complete()
        .await
        .expect("complete 0-byte multipart");
    let empty_fetched = engine.get("blobs/r2_empty.bin").await.expect("get empty");
    assert!(empty_fetched.is_empty());
}

#[tokio::test]
async fn test_multipart_abort_cleanup_and_idempotency() {
    use _storage::store::PutConditionalOptions;

    let engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
    let mut session = engine
        .create_multipart("blobs/aborted.bin", PutConditionalOptions::default())
        .await
        .expect("create multipart");
    session
        .write_chunk(Bytes::from_static(b"part1"))
        .await
        .expect("write part1");
    session.abort().await.expect("first abort");
    session.abort().await.expect("second idempotent abort");

    assert!(
        !engine
            .exists("blobs/aborted.bin")
            .await
            .expect("exists check")
    );
}

#[tokio::test]
async fn test_conditional_put_if_not_exists_if_match_and_metadata() {
    use _storage::store::{ObjectStoreError, PutConditionalOptions};

    let engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
    let mut attrs = HashMap::new();
    attrs.insert("owner".to_string(), "worker-1".to_string());

    let meta1 = engine
        .put_opts(
            "locks/a",
            Bytes::from_static(b"v1"),
            PutConditionalOptions {
                if_not_exists: true,
                content_type: Some("text/plain".to_string()),
                attributes: attrs.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("initial conditional create");
    let etag1 = meta1.etag.clone().expect("etag1 must be present");
    assert_eq!(meta1.content_type.as_deref(), Some("text/plain"));
    assert_eq!(meta1.attributes, attrs);

    let head_meta = engine.head("locks/a").await.expect("head locks/a");
    assert_eq!(head_meta.content_type.as_deref(), Some("text/plain"));
    assert_eq!(head_meta.attributes, attrs);

    let dup_err = engine
        .put_opts(
            "locks/a",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_not_exists: true,
                ..Default::default()
            },
        )
        .await
        .expect_err("second if_not_exists must fail");
    assert!(
        matches!(dup_err, ObjectStoreError::AlreadyExists { .. }),
        "expected AlreadyExists, got {dup_err:?}"
    );

    let precond_err = engine
        .put_opts(
            "locks/a",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_match: Some("wrong-etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("wrong if_match must fail");
    assert!(
        matches!(precond_err, ObjectStoreError::Precondition { .. }),
        "expected Precondition, got {precond_err:?}"
    );

    let meta2 = engine
        .put_opts(
            "locks/a",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_match: Some(etag1),
                ..Default::default()
            },
        )
        .await
        .expect("matching if_match must update");
    assert_eq!(meta2.size, 2);
    let updated = engine.get("locks/a").await.expect("get updated locks/a");
    assert_eq!(&updated[..], b"v2");
}

#[tokio::test]
async fn test_cursor_pagination_and_delimiter_listing() {
    use _storage::store::ObjectStoreError;

    async fn verify_pagination_and_delimiter(engine: &ObjectStoreEngine) {
        engine
            .put("dir/sub/c.txt", Bytes::from_static(b"c"))
            .await
            .expect("put c");
        engine
            .put("dir/a.txt", Bytes::from_static(b"a"))
            .await
            .expect("put a");
        engine
            .put("dir/b.txt", Bytes::from_static(b"b"))
            .await
            .expect("put b");

        let zero_err = engine
            .list_page(Some("dir/"), None, 0)
            .await
            .expect_err("limit 0 must fail");
        assert!(matches!(zero_err, ObjectStoreError::InvalidInput(_)));

        let (page1, cursor1) = engine
            .list_page(Some("dir/"), None, 2)
            .await
            .expect("list_page 1");
        let page1_paths: Vec<&str> = page1.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(page1_paths, vec!["dir/a.txt", "dir/b.txt"]);
        assert_eq!(cursor1.as_deref(), Some("dir/b.txt"));

        let (page2, cursor2) = engine
            .list_page(Some("dir/"), cursor1.as_deref(), 2)
            .await
            .expect("list_page 2");
        let page2_paths: Vec<&str> = page2.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(page2_paths, vec!["dir/sub/c.txt"]);
        assert_eq!(cursor2, None);

        let (objs, prefixes) = engine
            .list_with_delimiter(Some("dir/"))
            .await
            .expect("list_with_delimiter");
        let obj_paths: Vec<&str> = objs.iter().map(|m| m.path.as_str()).collect();
        assert_eq!(obj_paths, vec!["dir/a.txt", "dir/b.txt"]);
        assert_eq!(prefixes, vec!["dir/sub".to_string()]);
    }

    let mem_engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
    verify_pagination_and_delimiter(&mem_engine).await;

    let tmp = TempDir::new().expect("tempdir");
    let local_url = format!("file://{}", tmp.path().display());
    let local_engine = ObjectStoreEngine::from_url(&local_url, None).expect("local engine");
    verify_pagination_and_delimiter(&local_engine).await;
}

#[tokio::test]
async fn test_atomic_rename_and_conflict() {
    use _storage::store::ObjectStoreError;

    async fn verify_rename(engine: &ObjectStoreEngine) {
        engine
            .put("dir/a.txt", Bytes::from_static(b"alpha"))
            .await
            .expect("put a");
        engine
            .put("dir/b.txt", Bytes::from_static(b"beta"))
            .await
            .expect("put b");

        engine
            .rename("dir/a.txt", "dir/moved.txt", true)
            .await
            .expect("rename a -> moved");
        assert!(!engine.exists("dir/a.txt").await.expect("exists a"));
        let moved = engine.get("dir/moved.txt").await.expect("get moved");
        assert_eq!(&moved[..], b"alpha");

        let conflict_err = engine
            .rename("dir/b.txt", "dir/moved.txt", true)
            .await
            .expect_err("rename onto existing dst must fail");
        assert!(
            matches!(conflict_err, ObjectStoreError::AlreadyExists { .. }),
            "expected AlreadyExists, got {conflict_err:?}"
        );
        let b_data = engine.get("dir/b.txt").await.expect("b must remain intact");
        assert_eq!(&b_data[..], b"beta");
    }

    let mem_engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
    verify_rename(&mem_engine).await;

    let tmp = TempDir::new().expect("tempdir");
    let local_url = format!("file://{}", tmp.path().display());
    let local_engine = ObjectStoreEngine::from_url(&local_url, None).expect("local engine");
    verify_rename(&local_engine).await;
}

#[tokio::test]
async fn test_local_conditional_put_if_none_match_and_traversal_rejection() {
    use _storage::store::{ObjectStoreError, PutConditionalOptions};

    let tmp = TempDir::new().expect("tempdir");
    let local_url = format!("file://{}", tmp.path().display());
    let local = ObjectStoreEngine::from_url(&local_url, None).expect("local engine");

    async fn assert_traversal_rejected(engine: &ObjectStoreEngine, bad_path: &str) {
        let err = engine
            .put(bad_path, Bytes::from_static(b"nope"))
            .await
            .expect_err("traversal must fail");
        assert!(matches!(err, ObjectStoreError::InvalidInput(_)));
    }

    assert_traversal_rejected(&local, "../escape.txt").await;
    assert_traversal_rejected(&local, "nested/../../escape.txt").await;
    assert_traversal_rejected(&local, "dir/%2e%2e/x").await;

    let meta1 = local
        .put_opts(
            "locks/local.txt",
            Bytes::from_static(b"v1"),
            PutConditionalOptions {
                if_not_exists: true,
                content_type: Some("text/plain".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("local conditional create");
    let etag1 = meta1.etag.expect("local etag");

    let dup = local
        .put_opts(
            "locks/local.txt",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_none_match: Some("*".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("if_none_match=* on existing local file must fail");
    assert!(dup.is_already_exists());

    let none_match_hit = local
        .put_opts(
            "locks/local.txt",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_none_match: Some(etag1.clone()),
                ..Default::default()
            },
        )
        .await
        .expect_err("if_none_match matching current etag must fail");
    assert!(none_match_hit.is_precondition());

    let wrong_match = local
        .put_opts(
            "locks/local.txt",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_match: Some("stale-etag".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("stale if_match on local file must fail");
    assert!(wrong_match.is_precondition());

    local
        .put_opts(
            "locks/local.txt",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_match: Some(etag1),
                ..Default::default()
            },
        )
        .await
        .expect("matching if_match on local file must update");
    assert_eq!(
        &local.get("locks/local.txt").await.expect("get local")[..],
        b"v2"
    );
}

#[test]
fn test_pyo3_sync_and_async_streaming_multipart_conditional_pagination_and_rename() {
    use pyo3::ffi::c_str;
    use pyo3::prelude::*;
    use pyo3::types::PyModule;

    Python::initialize();
    Python::attach(|py| {
        let storage_mod = PyModule::new(py, "_storage").expect("create _storage module");
        _storage::_storage(&storage_mod).expect("populate _storage module");
        let sys_modules = py
            .import("sys")
            .expect("import sys")
            .getattr("modules")
            .expect("sys.modules");
        sys_modules
            .set_item("vertebra._storage", &storage_mod)
            .expect("register vertebra._storage");

        py.run(
            c_str!(
                r#"
import asyncio
import vertebra._storage as st

sync_store = st.SyncObjectStore("memory://")
m1 = sync_store.put(
    "locks/item.txt",
    b"hello",
    content_type="text/plain",
    attributes={"env": "test"},
    if_not_exists=True,
)
assert m1.content_type == "text/plain"
assert m1.attributes == {"env": "test"}
assert sync_store.head("locks/item.txt").content_type == "text/plain"
assert sync_store.head("locks/item.txt").attributes == {"env": "test"}

try:
    sync_store.put("locks/item.txt", b"dup", if_not_exists=True)
    raise AssertionError("expected FileExistsError")
except FileExistsError:
    pass

try:
    sync_store.put("locks/item.txt", b"stale", if_match="bad-etag")
    raise AssertionError("expected PermissionError")
except PermissionError:
    pass

sync_store.put("locks/item.txt", b"updated", if_match=m1.etag)
assert sync_store.get("locks/item.txt") == b"updated"

mp = sync_store.create_multipart("blobs/mp.bin", content_type="application/octet-stream")
mp.write_part(b"alpha-")
mp.write_part(memoryview(b"beta"))
mp_meta = mp.complete()
assert mp_meta.size == 10

chunks = [ bytes(memoryview(c)) for c in sync_store.get_stream("blobs/mp.bin", chunk_size=4) ]
assert [len(c) for c in chunks] == [4, 4, 2]
assert b"".join(chunks) == b"alpha-beta"

try:
    sync_store.get_stream("blobs/mp.bin", chunk_size=0)
    raise AssertionError("expected ValueError for chunk_size=0")
except ValueError:
    pass

try:
    sync_store.list_page(limit=0)
    raise AssertionError("expected ValueError for limit=0")
except ValueError as exc:
    assert "limit must be >= 1" in str(exc)

async def run_async_checks():
    astore = st.AsyncObjectStore("memory://")
    am1 = await astore.put(
        "dir/a.txt",
        b"a-val",
        content_type="text/plain",
        attributes={"k": "v"},
        if_not_exists=True,
    )
    assert am1.content_type == "text/plain"
    assert am1.attributes == {"k": "v"}
    await astore.put("dir/b.txt", b"b-val")
    await astore.put("dir/sub/c.txt", b"c-val")

    p1, cur1 = await astore.list_page("dir/", None, 2)
    assert [x.path for x in p1] == ["dir/a.txt", "dir/b.txt"]
    assert cur1 == "dir/b.txt"
    p2, cur2 = await astore.list_page("dir/", cur1, 2)
    assert [x.path for x in p2] == ["dir/sub/c.txt"]
    assert cur2 is None

    objs, prefixes = await astore.list_with_delimiter("dir/")
    assert [x.path for x in objs] == ["dir/a.txt", "dir/b.txt"]
    assert prefixes == ["dir/sub"]

    amp = await astore.create_multipart("dir/streamed.bin")
    await amp.write_part(b"chunk-1-")
    await amp.write_part(b"chunk-2")
    amp_meta = await amp.complete()
    assert amp_meta.size == 15

    aborted = await astore.create_multipart("dir/aborted.bin")
    await aborted.write_part(b"discard")
    await aborted.abort()
    await aborted.abort()
    assert not await astore.exists("dir/aborted.bin")

    parts = []
    async for buf in astore.get_stream("dir/streamed.bin", chunk_size=6):
        parts.append(bytes(memoryview(buf)))
    assert [len(p) for p in parts] == [6, 6, 3]
    assert b"".join(parts) == b"chunk-1-chunk-2"

    await astore.rename("dir/a.txt", "dir/moved.txt", if_not_exists=True)
    assert not await astore.exists("dir/a.txt")
    try:
        await astore.rename("dir/b.txt", "dir/moved.txt", if_not_exists=True)
        raise AssertionError("expected FileExistsError on rename conflict")
    except FileExistsError:
        pass
    assert await astore.get("dir/b.txt") == b"b-val"

asyncio.run(run_async_checks())
"#
            ),
            None,
            None,
        )
        .expect("run Python sync + asyncio verification");
    });
}
