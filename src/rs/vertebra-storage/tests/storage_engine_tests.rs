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
