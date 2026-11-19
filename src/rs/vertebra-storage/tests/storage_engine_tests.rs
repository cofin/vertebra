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
        .copy("datasets/events.bin", "datasets/events_copy.bin", false)
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
        virtual_hosted_style: false,
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
        endpoint: None,
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

/// RSA-2048 PKCS#8 test key shared by OCI Cavage signing tests.
const TEST_OCI_RSA_PEM: &str = concat!(
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

#[tokio::test]
async fn test_oci_cavage_signing_and_native_rest_v2_crud() {
    let pem = TEST_OCI_RSA_PEM.to_string();

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
        endpoint: None,
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
        endpoint: None,
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
    let provider = RefreshingGcpCredentialProvider::from_arc(Arc::clone(&cache));
    assert_eq!(
        provider.cache().quota_project_id().as_deref(),
        Some("proj-storage")
    );

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
        endpoint: None,
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
        None,
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
        virtual_hosted_style: false,
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
                None,
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

#[tokio::test]
async fn test_incremental_gcs_uniform_multipart_session() {
    assert_incremental_uniform_multipart_session("gs").await;
}

async fn write_small_multipart_chunks(
    scheme: &str,
    chunk_count: usize,
    chunk_len: usize,
) -> (Vec<usize>, Vec<u8>, Bytes) {
    use _storage::store::PutConditionalOptions;
    use std::sync::Arc;

    let counting = Arc::new(CountingStore::new(std::time::Duration::ZERO));
    let engine = ObjectStoreEngine::with_store(counting.clone(), scheme);
    let mut session = engine
        .create_multipart("blobs/small-chunks.bin", PutConditionalOptions::default())
        .await
        .expect("create multipart session");
    let mut expected = Vec::with_capacity(chunk_count * chunk_len);
    for chunk_idx in 0..chunk_count {
        let data: Vec<u8> = (0..chunk_len)
            .map(|offset| ((chunk_idx * 31 + offset) % 251) as u8)
            .collect();
        expected.extend_from_slice(&data);
        session
            .write_chunk(Bytes::from(data))
            .await
            .expect("write_chunk");
    }
    session.complete().await.expect("complete multipart");
    let stored_parts = counting
        .uploads
        .part_sizes
        .lock()
        .expect("part sizes lock")
        .clone();
    assert_eq!(
        stored_parts,
        session.part_sizes_flushed(),
        "session part ledger must match parts received by the store"
    );
    let fetched = engine
        .get("blobs/small-chunks.bin")
        .await
        .expect("get completed object");
    (stored_parts, expected, fetched)
}

#[tokio::test]
async fn test_gcs_multipart_coalesces_small_chunks_into_uniform_parts() {
    const CHUNK_LEN: usize = 64 * 1024;
    const CHUNK_COUNT: usize = 300;

    for scheme in ["gs", "gcs"] {
        let (parts, expected, fetched) =
            write_small_multipart_chunks(scheme, CHUNK_COUNT, CHUNK_LEN).await;
        let total = CHUNK_COUNT * CHUNK_LEN;
        assert_eq!(
            parts.len(),
            total.div_ceil(R2_UNIFORM_PART_SIZE),
            "{scheme}: unexpected part count {parts:?}"
        );
        let (tail, non_final) = parts.split_last().expect("at least one part");
        assert!(
            non_final.iter().all(|&len| len == R2_UNIFORM_PART_SIZE),
            "{scheme}: every non-final part must be exactly 8 MiB, got {parts:?}"
        );
        assert_eq!(*tail, total - non_final.len() * R2_UNIFORM_PART_SIZE);
        assert_eq!(fetched.len(), total);
        assert_eq!(
            &fetched[..],
            &expected[..],
            "{scheme}: bytes must round-trip"
        );
    }
}

#[tokio::test]
async fn test_non_uniform_backends_upload_chunks_as_written() {
    const CHUNK_LEN: usize = 64 * 1024;
    const CHUNK_COUNT: usize = 20;

    for scheme in ["az", "azure", "memory", "file"] {
        let (parts, expected, fetched) =
            write_small_multipart_chunks(scheme, CHUNK_COUNT, CHUNK_LEN).await;
        assert_eq!(
            parts,
            vec![CHUNK_LEN; CHUNK_COUNT],
            "{scheme}: chunks must pass through without coalescing"
        );
        assert_eq!(
            &fetched[..],
            &expected[..],
            "{scheme}: bytes must round-trip"
        );
    }
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

#[tokio::test]
async fn test_gcs_native_plane_if_match_skip_signature_and_capsule_quota_project() {
    use _storage::store::{PutConditionalOptions, StorageError};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use vertebra_core::gcp_auth::{
        DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache, ProxyPolicy,
        SharedGcpTokenCacheHandle,
    };

    let gcs_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback gcs");
    let gcs_addr = gcs_listener.local_addr().expect("gcs addr");
    let seen_headers: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_srv = Arc::clone(&seen_headers);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = gcs_listener.accept().await {
            let log = Arc::clone(&seen_srv);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 16384];
                let mut total = 0;
                loop {
                    let n = stream.read(&mut buf[total..]).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    total += n;
                    if let Some(hdr_end) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
                        let hdr_str = String::from_utf8_lossy(&buf[..hdr_end]);
                        let content_len = hdr_str
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                if k.eq_ignore_ascii_case("content-length") {
                                    v.trim().parse::<usize>().ok()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0);
                        if total >= hdr_end + 4 + content_len {
                            break;
                        }
                    }
                }
                let req = String::from_utf8_lossy(&buf[..total]).into_owned();
                let first_line = req.lines().next().unwrap_or_default().to_string();
                log.lock().expect("lock").push(req.clone());

                let lower = req.to_ascii_lowercase();
                let resp = if first_line.starts_with("HEAD ")
                    && (first_line.contains("etag_obj") || first_line.contains("etag%5Fobj"))
                {
                    "HTTP/1.1 200 OK\r\nETag: \"etag-77\"\r\nx-goog-generation: 77\r\nLast-Modified: Sat, 07 Nov 2026 00:00:00 GMT\r\nContent-Length: 2\r\nConnection: close\r\n\r\n".to_string()
                } else if first_line.starts_with("HEAD ")
                    && (first_line.contains("missing_obj") || first_line.contains("missing%5Fobj"))
                {
                    "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                } else if first_line.starts_with("PUT ")
                    && lower.contains("x-goog-if-generation-match: 41")
                {
                    let body = "{\"error\":{\"code\":412}}";
                    format!(
                        "HTTP/1.1 412 Precondition Failed\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if first_line.starts_with("POST ") && first_line.contains("uploads") {
                    let body = "<InitiateMultipartUploadResult><Bucket>emu-bucket</Bucket><Key>mp.txt</Key><UploadId>up-1</UploadId></InitiateMultipartUploadResult>";
                    format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if first_line.starts_with("POST ") && first_line.contains("uploadId=up-1") {
                    let body = "<CompleteMultipartUploadResult><Bucket>emu-bucket</Bucket><Key>mp.txt</Key><ETag>\"etag-mp\"</ETag></CompleteMultipartUploadResult>";
                    format!(
                        "HTTP/1.1 200 OK\r\nx-goog-generation: 43\r\nContent-Type: application/xml\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if first_line.starts_with("PUT ") || first_line.starts_with("POST ") {
                    let body = r#"{"name":"obj.txt","bucket":"emu-bucket","generation":"42","etag":"etag-42","size":"2"}"#;
                    format!(
                        "HTTP/1.1 200 OK\r\nETag: \"etag-42\"\r\nx-goog-generation: 42\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    )
                } else if first_line.starts_with("DELETE ") {
                    "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                        .to_string()
                } else {
                    "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nv1"
                        .to_string()
                };
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    let emu_opts = HashMap::from([
        (
            "endpoint".to_string(),
            format!("http://127.0.0.1:{}", gcs_addr.port()),
        ),
        ("allow_http".to_string(), "true".to_string()),
        ("skip_signature".to_string(), "true".to_string()),
    ]);
    let emu_engine =
        ObjectStoreEngine::from_url("gs://emu-bucket", Some(&emu_opts)).expect("emu gcs engine");
    emu_engine
        .put("obj.txt", Bytes::from_static(b"v1"))
        .await
        .expect("emu put without metadata server");
    let mut mp = emu_engine
        .create_multipart("mp.txt", PutConditionalOptions::default())
        .await
        .expect("emu create_multipart");
    mp.write_chunk(Bytes::from_static(b"hello"))
        .await
        .expect("emu write_chunk");
    mp.complete().await.expect("emu complete multipart");
    emu_engine
        .delete("obj.txt")
        .await
        .expect("emu delete without metadata server");

    let gen_ok = emu_engine
        .put_opts(
            "obj.txt",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_match: Some("42".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("numeric generation if_match must succeed in 1 RTT");
    assert_eq!(gen_ok.size, 2);

    let gen_mismatch = emu_engine
        .put_opts(
            "obj.txt",
            Bytes::from_static(b"v3"),
            PutConditionalOptions {
                if_match: Some("41".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("412 generation mismatch must fail");
    assert!(gen_mismatch.is_precondition());

    let etag_ok = emu_engine
        .put_opts(
            "etag_obj",
            Bytes::from_static(b"v2"),
            PutConditionalOptions {
                if_match: Some("\"etag-77\"".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("matching non-numeric ETag on GCS must resolve generation via HEAD");
    assert_eq!(etag_ok.size, 2);

    let etag_bad = emu_engine
        .put_opts(
            "etag_obj",
            Bytes::from_static(b"v3"),
            PutConditionalOptions {
                if_match: Some("\"wrong-etag\"".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("mismatched ETag must return Precondition");
    assert!(matches!(etag_bad, StorageError::Precondition { .. }));

    let etag_missing = emu_engine
        .put_opts(
            "missing_obj",
            Bytes::from_static(b"v3"),
            PutConditionalOptions {
                if_match: Some("\"etag-77\"".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("missing object on ETag if_match must return Precondition");
    assert!(matches!(etag_missing, StorageError::Precondition { .. }));

    let oauth_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind oauth");
    let oauth_addr = oauth_listener.local_addr().expect("oauth addr");
    let oauth_calls = Arc::new(AtomicUsize::new(0));
    let oauth_calls_srv = Arc::clone(&oauth_calls);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = oauth_listener.accept().await {
            let c = Arc::clone(&oauth_calls_srv);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf).await;
                c.fetch_add(1, Ordering::SeqCst);
                let body =
                    r#"{"access_token":"capsule-token-1","expires_in":3600,"token_type":"Bearer"}"#;
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
            quota_project_id: Some("billing-proj-99".to_string()),
            token_uri: format!("http://127.0.0.1:{}/token", oauth_addr.port()),
        },
        DEFAULT_REFRESH_SKEW_MS,
        ProxyPolicy::Direct,
    ));
    let handle = SharedGcpTokenCacheHandle::from_arc(cache);
    let authed_opts = HashMap::from([
        (
            "endpoint".to_string(),
            format!("http://127.0.0.1:{}", gcs_addr.port()),
        ),
        ("allow_http".to_string(), "true".to_string()),
        ("allow_ambient_adc".to_string(), "false".to_string()),
    ]);
    seen_headers.lock().expect("lock").clear();
    let authed_engine =
        ObjectStoreEngine::from_url_with_cache("gs://emu-bucket", Some(authed_opts), Some(handle))
            .expect("from_url_with_cache");
    authed_engine
        .put("authed.txt", Bytes::from_static(b"ok"))
        .await
        .expect("authed put");
    assert_eq!(oauth_calls.load(Ordering::SeqCst), 1);
    let logged = seen_headers.lock().expect("lock");
    let last_req = logged
        .last()
        .expect("captured authed request")
        .to_ascii_lowercase();
    assert!(last_req.contains("authorization: bearer capsule-token-1"));
    assert!(last_req.contains("x-goog-user-project: billing-proj-99"));
}

#[tokio::test]
async fn test_gcs_v4_custom_endpoint_and_iam_signblob_quota_project() {
    use _storage::signing::{
        GcsSignConfig, build_gcs_v4_canonical_request, sign_gcs_v4_url,
        sign_gcs_v4_with_iam_endpoint_and_cache,
    };
    use rsa::RsaPrivateKey;
    use rsa::pkcs1v15::{Signature, VerifyingKey};
    use rsa::pkcs8::DecodePrivateKey;
    use rsa::signature::Verifier;
    use sha2::Sha256;
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use vertebra_core::gcp_auth::{
        DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache, ProxyPolicy,
        SharedGcpTokenCacheHandle,
    };

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

    let ep = "http://127.0.0.1:4443/storage/v1";
    let cfg = GcsSignConfig {
        bucket: "my-bkt".to_string(),
        service_account_email: "signer@proj.iam.gserviceaccount.com".to_string(),
        private_key_pem: Some(pem.to_string()),
        access_token: None,
        endpoint: Some(ep.to_string()),
    };
    let signed = sign_gcs_v4_url(&cfg, "GET", "dir/file.txt", 900, 1_700_000_000)
        .expect("custom endpoint sign");
    assert!(signed.starts_with("http://127.0.0.1:4443/storage/v1/my-bkt/dir/file.txt?"));
    let canonical = build_gcs_v4_canonical_request(
        "my-bkt",
        "signer@proj.iam.gserviceaccount.com",
        "GET",
        "dir/file.txt",
        900,
        1_700_000_000,
        Some(ep),
    )
    .expect("canonical custom ep");
    assert_eq!(canonical.canonical_uri, "/storage/v1/my-bkt/dir/file.txt");
    assert!(
        canonical
            .canonical_request
            .contains("\nhost:127.0.0.1:4443\n")
    );
    let (_, sig_hex) = signed.split_once("&X-Goog-Signature=").expect("sig");
    let sig_bytes = hex::decode(sig_hex).expect("hex");
    let signature = Signature::try_from(sig_bytes.as_slice()).expect("rsa sig");
    let priv_key = RsaPrivateKey::from_pkcs8_pem(pem).expect("rsa key");
    VerifyingKey::<Sha256>::new(priv_key.to_public_key())
        .verify(canonical.string_to_sign.as_bytes(), &signature)
        .expect("custom endpoint RSA signature must verify");

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind iam");
    let addr = listener.local_addr().expect("iam addr");
    let seen_iam_reqs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let seen_iam_srv = Arc::clone(&seen_iam_reqs);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let log = Arc::clone(&seen_iam_srv);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let n = stream.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                let body = if req.starts_with("POST /token ") {
                    r#"{"access_token":"iam-qp-tok","expires_in":3600,"token_type":"Bearer"}"#
                } else {
                    log.lock().expect("lock").push(req);
                    r#"{"keyId":"k1","signedBlob":"ZGVhZGJlZWZjYWZl"}"#
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

    let cache = Arc::new(GcpTokenCache::with_proxy_policy(
        GcpAuthSource::AuthorizedUser {
            client_id: "cid".to_string(),
            client_secret: "csec".to_string(),
            refresh_token: "rtok".to_string(),
            quota_project_id: Some("qp-sign".to_string()),
            token_uri: format!("http://127.0.0.1:{}/token", addr.port()),
        },
        DEFAULT_REFRESH_SKEW_MS,
        ProxyPolicy::Direct,
    ));
    let handle = SharedGcpTokenCacheHandle::from_arc(cache);
    let iam_cfg = GcsSignConfig {
        bucket: "my-bkt".to_string(),
        service_account_email: "signer@proj.iam.gserviceaccount.com".to_string(),
        private_key_pem: None,
        access_token: None,
        endpoint: Some(ep.to_string()),
    };
    let iam_signed = sign_gcs_v4_with_iam_endpoint_and_cache(
        &iam_cfg,
        Some(&handle),
        "GET",
        "dir/file.txt",
        900,
        1_700_000_000,
        &format!("http://127.0.0.1:{}", addr.port()),
    )
    .await
    .expect("iam sign with handle");
    assert!(iam_signed.starts_with("http://127.0.0.1:4443/storage/v1/my-bkt/dir/file.txt?"));
    let captured = seen_iam_reqs.lock().expect("lock");
    assert!(
        captured
            .last()
            .expect("iam request")
            .to_ascii_lowercase()
            .contains("x-goog-user-project: qp-sign")
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
        let sys_mod = py.import("sys").expect("import sys");
        let sys_path = sys_mod.getattr("path").expect("sys.path");
        let py_src_dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../py");
        let _ = sys_path.call_method1("insert", (0, py_src_dir));
        let sys_modules = sys_mod.getattr("modules").expect("sys.modules");
        sys_modules
            .set_item("vertebra._storage", &storage_mod)
            .expect("register vertebra._storage");

        py.run(
            c_str!(
                r#"
import asyncio
import sys
import vertebra._storage as st

_saved_msgspec = sys.modules.get("msgspec", ...)
sys.modules["msgspec"] = None
sys.modules.pop("vertebra.typing", None)
sys.modules.pop("vertebra._typing", None)
try:
    from vertebra.typing import PreconditionError
finally:
    if _saved_msgspec is ...:
        sys.modules.pop("msgspec", None)
    else:
        sys.modules["msgspec"] = _saved_msgspec
"#
            ),
            None,
            None,
        )
        .expect("import vertebra.typing without msgspec installed");

        py.run(
            c_str!(
                r#"
import asyncio
import vertebra._storage as st
from vertebra.typing import PreconditionError

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
    raise AssertionError("expected PreconditionError")
except PreconditionError:
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

#[tokio::test]
async fn test_checked_get_range_raw_keys_local_metadata_and_s3_http_options() {
    use _storage::store::{ObjectStoreError, PutConditionalOptions, StorageError};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
    engine
        .put("exists.bin", Bytes::from_static(b"0123456789"))
        .await
        .expect("put exists.bin");

    let overflow_err = engine
        .get_range("exists.bin", 5, u64::MAX)
        .await
        .expect_err("offset + length overflow must fail");
    assert!(matches!(overflow_err, StorageError::InvalidInput(_)));

    let zero_ok = engine
        .get_range("exists.bin", 5, 0)
        .await
        .expect("zero-length range on existing key must succeed");
    assert!(zero_ok.is_empty());

    let zero_missing = engine
        .get_range("missing.bin", 0, 0)
        .await
        .expect_err("zero-length range on missing key must return NotFound");
    assert!(matches!(
        zero_missing,
        ObjectStoreError::ObjectStore(object_store::Error::NotFound { .. })
    ));

    let raw_key = "a~b/c%25d.txt";
    engine
        .put(raw_key, Bytes::from_static(b"verbatim"))
        .await
        .expect("put raw key");
    let listed = engine.list(Some("a~b")).await.expect("list raw prefix");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].path, raw_key);
    assert_eq!(
        &engine.get(raw_key).await.expect("get raw key")[..],
        b"verbatim"
    );

    let tmp = TempDir::new().expect("tempdir");
    let local_url = format!("file://{}", tmp.path().display());
    let local = ObjectStoreEngine::from_url(&local_url, None).expect("local engine");
    let attrs = HashMap::from([("owner".to_string(), "worker-7".to_string())]);
    let local_meta = local
        .put_opts(
            "meta~dir/item%251.txt",
            Bytes::from_static(b"payload-1"),
            PutConditionalOptions {
                if_not_exists: true,
                content_type: Some("application/json".to_string()),
                attributes: attrs.clone(),
                ..Default::default()
            },
        )
        .await
        .expect("local conditional put with metadata");
    assert_eq!(local_meta.path, "meta~dir/item%251.txt");
    assert_eq!(local_meta.content_type.as_deref(), Some("application/json"));
    assert_eq!(local_meta.attributes, attrs);

    let local_head = local
        .head("meta~dir/item%251.txt")
        .await
        .expect("local head preserves metadata");
    assert_eq!(local_head.content_type.as_deref(), Some("application/json"));
    assert_eq!(local_head.attributes, attrs);

    local
        .copy("meta~dir/item%251.txt", "meta~dir/copied.txt", false)
        .await
        .expect("local copy");
    let copied_head = local
        .head("meta~dir/copied.txt")
        .await
        .expect("copied local head");
    assert_eq!(
        copied_head.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(copied_head.attributes, attrs);

    local
        .rename("meta~dir/copied.txt", "meta~dir/renamed.txt", true)
        .await
        .expect("local rename");
    let renamed_head = local
        .head("meta~dir/renamed.txt")
        .await
        .expect("renamed local head");
    assert_eq!(
        renamed_head.content_type.as_deref(),
        Some("application/json")
    );
    assert_eq!(renamed_head.attributes, attrs);

    let vhost_default = S3SignConfig {
        bucket: "assets".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        endpoint: None,
        virtual_hosted_style: true,
    };
    let signed_vhost = sign_s3_v4_url(
        &vhost_default,
        "GET",
        "dir/report 1.parquet",
        900,
        1_700_000_000,
    )
    .expect("sign default vhost");
    assert!(
        signed_vhost
            .starts_with("https://assets.s3.us-east-1.amazonaws.com/dir/report%201.parquet?"),
        "unexpected signed_vhost: {signed_vhost}"
    );
    assert!(
        verify_s3_v4_url(&vhost_default, "GET", &signed_vhost, 1_700_000_100)
            .expect("verify default vhost")
    );

    let vhost_custom = S3SignConfig {
        bucket: "assets".to_string(),
        region: "us-east-1".to_string(),
        access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
        secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
        endpoint: Some("https://s3.example.com:9000/v1".to_string()),
        virtual_hosted_style: true,
    };
    let signed_custom = sign_s3_v4_url(&vhost_custom, "PUT", "k/obj.bin", 900, 1_700_000_000)
        .expect("sign custom vhost");
    assert!(
        signed_custom.starts_with("https://assets.s3.example.com:9000/v1/k/obj.bin?"),
        "unexpected signed_custom: {signed_custom}"
    );
    assert!(
        verify_s3_v4_url(&vhost_custom, "PUT", &signed_custom, 1_700_000_100)
            .expect("verify custom vhost")
    );

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback server");
    let addr = listener.local_addr().expect("addr");
    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_srv = Arc::clone(&captured);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let log = Arc::clone(&captured_srv);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                log.lock().expect("lock").push(req);
                let resp = "HTTP/1.1 200 OK\r\nETag: \"etag-1\"\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
                let _ = socket.write_all(resp.as_bytes()).await;
            });
        }
    });

    let proxy_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback http proxy");
    let proxy_addr = proxy_listener.local_addr().expect("proxy addr");
    let captured_proxy = Arc::clone(&captured);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = proxy_listener.accept().await {
            let log = Arc::clone(&captured_proxy);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let n = socket.read(&mut buf).await.unwrap_or(0);
                let req = String::from_utf8_lossy(&buf[..n]).into_owned();
                log.lock().expect("lock").push(req);
                let resp = "HTTP/1.1 200 OK\r\nETag: \"etag-1\"\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok";
                let _ = socket.write_all(resp.as_bytes()).await;
            });
        }
    });

    let s3_opts = HashMap::from([
        ("region".to_string(), "us-east-1".to_string()),
        ("access_key_id".to_string(), "ak".to_string()),
        ("secret_access_key".to_string(), "sk".to_string()),
        ("session_token".to_string(), "sts-tok-123".to_string()),
        ("virtual_hosted_style".to_string(), "true".to_string()),
        (
            "endpoint".to_string(),
            "http://s3.example.test:9000".to_string(),
        ),
        (
            "proxy_url".to_string(),
            format!("http://127.0.0.1:{}", proxy_addr.port()),
        ),
        ("allow_http".to_string(), "true".to_string()),
    ]);
    let s3_engine =
        ObjectStoreEngine::from_url("s3://bkt", Some(&s3_opts)).expect("s3 engine with sts+vhost");
    let s3_bytes = s3_engine.get("item.txt").await.expect("s3 get");
    assert_eq!(&s3_bytes[..], b"ok");

    let http_opts = HashMap::from([
        (
            "headers".to_string(),
            r#"{"X-Api-Key":"secret-api-key"}"#.to_string(),
        ),
        ("header.X-Trace-Id".to_string(), "trace-42".to_string()),
        ("http_header.X-Tenant".to_string(), "tenant-7".to_string()),
        ("region".to_string(), "x".to_string()),
    ]);
    let http_engine = ObjectStoreEngine::from_url(
        &format!("http://127.0.0.1:{}", addr.port()),
        Some(&http_opts),
    )
    .expect("http engine with headers");
    let http_bytes = http_engine.get("doc.txt").await.expect("http get");
    assert_eq!(&http_bytes[..], b"ok");

    let logs = captured.lock().expect("lock");
    assert!(logs.len() >= 2);
    let s3_req = logs[0].to_ascii_lowercase();
    assert!(
        s3_req.starts_with("get http://bkt.s3.example.test:9000/item.txt http/1.1\r\n")
            || s3_req.starts_with("get /item.txt http/1.1\r\n"),
        "expected virtual-hosted S3 request line, got: {s3_req}"
    );
    assert!(
        s3_req.contains("host: bkt.s3.example.test:9000"),
        "expected virtual-hosted Host header, got: {s3_req}"
    );
    assert!(
        s3_req.contains("x-amz-security-token: sts-tok-123"),
        "expected x-amz-security-token header, got: {s3_req}"
    );
    let http_req = logs[1].to_ascii_lowercase();
    assert!(
        http_req.contains("x-api-key: secret-api-key"),
        "expected x-api-key header, got: {http_req}"
    );
    assert!(
        http_req.contains("x-trace-id: trace-42"),
        "expected x-trace-id header, got: {http_req}"
    );
    assert!(
        http_req.contains("x-tenant: tenant-7"),
        "expected x-tenant header, got: {http_req}"
    );
    assert!(
        !http_req.contains("\r\nregion:"),
        "unrelated option keys must not become headers, got: {http_req}"
    );
    assert!(
        !http_req.contains("\r\nheaders:"),
        "the JSON headers option must not be sent verbatim, got: {http_req}"
    );
}

#[tokio::test]
async fn test_oci_streaming_get_opts_conditional_put_copy_and_paginated_list() {
    use _storage::oci::OciObjectStore;
    use futures::TryStreamExt;
    use object_store::path::Path as StorePath;
    use object_store::{
        Attribute, AttributeValue, Attributes, CopyMode, CopyOptions, GetOptions, GetRange,
        ObjectStore, PutMode, PutOptions, PutPayload, RenameOptions, RenameTargetMode,
        UpdateVersion,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback oci");
    let addr = listener.local_addr().expect("oci addr");
    let captured_reqs: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_srv = Arc::clone(&captured_reqs);
    let wr_ok_polls = Arc::new(AtomicUsize::new(0));
    let wr_ok_polls_srv = Arc::clone(&wr_ok_polls);

    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let log = Arc::clone(&captured_srv);
            let wr_polls = Arc::clone(&wr_ok_polls_srv);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let mut total = 0usize;
                loop {
                    let n = socket.read(&mut buf[total..]).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    total += n;
                    if let Some(hdr_end) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
                        let hdr_str = String::from_utf8_lossy(&buf[..hdr_end]);
                        let expected_cl = hdr_str
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.trim()
                                    .eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().ok())?
                            })
                            .unwrap_or(0);
                        if total >= hdr_end + 4 + expected_cl {
                            break;
                        }
                    }
                }
                let req = String::from_utf8_lossy(&buf[..total]).into_owned();
                let req_lower = req.to_ascii_lowercase();
                let first_line = req.lines().next().unwrap_or("");
                log.lock().expect("lock").push(req.clone());

                if first_line.starts_with("PUT /n/ns1/b/bkt1/o/") {
                    if req_lower.contains("if-none-match: *") && first_line.contains("existing") {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await;
                        return;
                    }
                    if req_lower.contains("if-match: stale-etag") {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await;
                        return;
                    }
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\netag: \"oci-etag-1\"\r\nopc-version-id: v-101\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    return;
                }

                if first_line.starts_with("POST /n/ns1/b/bkt1/actions/copyObject ") {
                    let wr_id = if req.contains("\"destinationObjectIfNoneMatchETag\":\"*\"")
                        && req.contains("\"destinationObjectName\":\"dup.bin\"")
                    {
                        "wr-dup"
                    } else {
                        "wr-ok"
                    };
                    let resp = format!(
                        "HTTP/1.1 202 Accepted\r\nopc-work-request-id: {wr_id}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if first_line.starts_with("GET /workRequests/") {
                    let body = if first_line.starts_with("GET /workRequests/wr-dup/errors ") {
                        r#"[{"code":"PreconditionFailed","message":"The destination object already exists","timestamp":"2026-11-06T12:00:00Z"}]"#.to_string()
                    } else if first_line.starts_with("GET /workRequests/wr-dup ") {
                        r#"{"id":"wr-dup","operationType":"COPY_OBJECT","status":"FAILED"}"#
                            .to_string()
                    } else {
                        let status = if wr_polls.fetch_add(1, Ordering::SeqCst) == 0 {
                            "IN_PROGRESS"
                        } else {
                            "COMPLETED"
                        };
                        format!(
                            r#"{{"id":"wr-ok","operationType":"COPY_OBJECT","status":"{status}"}}"#
                        )
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                if first_line.starts_with("POST /n/ns1/b/bkt1/actions/renameObject ") {
                    let resp: &[u8] = if req.contains("\"sourceName\":\"missing.bin\"") {
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    } else if req.contains("\"newObjIfNoneMatchETag\":\"*\"")
                        && req.contains("\"newName\":\"taken.bin\"")
                    {
                        b"HTTP/1.1 409 Conflict\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    } else {
                        b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                    };
                    let _ = socket.write_all(resp).await;
                    return;
                }

                if first_line.starts_with("HEAD /n/ns1/b/bkt1/o/") {
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nContent-Length: 10\r\nContent-Type: application/json\r\nopc-meta-owner: team-a\r\netag: \"oci-etag-1\"\r\nopc-version-id: v-101\r\nLast-Modified: Wed, 06 Nov 2026 12:00:00 GMT\r\nConnection: close\r\n\r\n",
                        )
                        .await;
                    return;
                }

                if first_line.starts_with("GET /n/ns1/b/bkt1/o/stream.bin ") {
                    if req_lower.contains("if-none-match: \"oci-etag-1\"") {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 304 Not Modified\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await;
                        return;
                    }
                    if req_lower.contains("if-match: \"wrong\"") {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                            )
                            .await;
                        return;
                    }
                    if req_lower.contains("range: bytes=2-5") {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-5/10\r\nContent-Length: 4\r\netag: \"oci-etag-1\"\r\nConnection: close\r\n\r\n2345",
                            )
                            .await;
                        return;
                    }
                    if req_lower.contains("range: bytes=4-") {
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 206 Partial Content\r\nContent-Length: 6\r\netag: \"oci-etag-1\"\r\nConnection: close\r\n\r\n456789",
                            )
                            .await;
                        return;
                    }
                    let _ = socket
                        .write_all(
                            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nContent-Type: application/octet-stream\r\nopc-meta-stage: raw\r\netag: \"oci-etag-1\"\r\nConnection: close\r\n\r\n5\r\n01234\r\n5\r\n56789\r\n0\r\n\r\n",
                        )
                        .await;
                    return;
                }

                if first_line.starts_with("GET /n/ns1/b/bkt1/o?") {
                    let body = if first_line.contains("delimiter=%2F")
                        || first_line.contains("delimiter=/")
                    {
                        if first_line.contains("start=dir%2Fk2") {
                            r#"{"objects":[{"name":"dir/k2~raw%25.parquet","size":12,"etag":"e2","timeCreated":"2026-11-06T12:00:00Z"}],"prefixes":["dir/sub2/","dir/sub1/"]}"#
                        } else {
                            r#"{"objects":[{"name":"dir/k1~raw%25.parquet","size":8,"etag":"e1","timeCreated":"2026-11-06T12:00:00Z"}],"prefixes":["dir/sub1/"],"nextStartWith":"dir/k2"}"#
                        }
                    } else if first_line.contains("start=p2") {
                        r#"{"objects":[{"name":"p2/item~2%25.bin","size":20,"etag":"e2"}]}"#
                    } else {
                        r#"{"objects":[{"name":"p1/item~1%25.bin","size":10,"etag":"e1"}],"nextStartWith":"p2"}"#
                    };
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        body.len(),
                        body
                    );
                    let _ = socket.write_all(resp.as_bytes()).await;
                    return;
                }

                let _ = socket
                    .write_all(
                        b"HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            });
        }
    });

    let oci = OciObjectStore::new(
        "ns1".to_string(),
        "bkt1".to_string(),
        "us-ashburn-1".to_string(),
        Some(format!("http://127.0.0.1:{}", addr.port())),
        None,
    )
    .expect("build oci store");

    let mut put_attrs = Attributes::new();
    put_attrs.insert(
        Attribute::ContentType,
        AttributeValue::from("application/json"),
    );
    put_attrs.insert(
        Attribute::Metadata("owner".into()),
        AttributeValue::from("team-a"),
    );
    let create_res = oci
        .put_opts(
            &StorePath::parse("new~1%25.json").expect("parse"),
            PutPayload::from_static(b"{}"),
            PutOptions {
                mode: PutMode::Create,
                attributes: put_attrs,
                ..Default::default()
            },
        )
        .await
        .expect("oci PutMode::Create");
    assert_eq!(create_res.e_tag.as_deref(), Some("\"oci-etag-1\""));
    assert_eq!(create_res.version.as_deref(), Some("v-101"));

    let dup_err = oci
        .put_opts(
            &StorePath::parse("existing.json").expect("parse"),
            PutPayload::from_static(b"{}"),
            PutOptions {
                mode: PutMode::Create,
                ..Default::default()
            },
        )
        .await
        .expect_err("PutMode::Create 412 must map to AlreadyExists");
    assert!(matches!(dup_err, object_store::Error::AlreadyExists { .. }));

    let stale_err = oci
        .put_opts(
            &StorePath::parse("existing.json").expect("parse"),
            PutPayload::from_static(b"{}"),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: Some("stale-etag".to_string()),
                    version: None,
                }),
                ..Default::default()
            },
        )
        .await
        .expect_err("PutMode::Update 412 must map to Precondition");
    assert!(matches!(
        stale_err,
        object_store::Error::Precondition { .. }
    ));

    let empty_uv_err = oci
        .put_opts(
            &StorePath::parse("existing.json").expect("parse"),
            PutPayload::from_static(b"{}"),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: None,
                    version: None,
                }),
                ..Default::default()
            },
        )
        .await
        .expect_err("empty UpdateVersion must fail");
    assert!(matches!(
        empty_uv_err,
        object_store::Error::Precondition { .. }
    ));

    let version_only_err = oci
        .put_opts(
            &StorePath::parse("existing.json").expect("parse"),
            PutPayload::from_static(b"{}"),
            PutOptions {
                mode: PutMode::Update(UpdateVersion {
                    e_tag: None,
                    version: Some("v-101".to_string()),
                }),
                ..Default::default()
            },
        )
        .await
        .expect_err("PutMode::Update without e_tag must not fall back to version");
    assert!(matches!(
        version_only_err,
        object_store::Error::Precondition { .. }
    ));

    let stream_loc = StorePath::parse("stream.bin").expect("parse");
    let streamed = oci
        .get_opts(&stream_loc, GetOptions::default())
        .await
        .expect("streamed get_opts");
    assert_eq!(
        streamed
            .attributes
            .get(&Attribute::Metadata("stage".into())),
        Some(&AttributeValue::from("raw"))
    );
    let chunks: Vec<Bytes> = streamed
        .into_stream()
        .try_collect()
        .await
        .expect("collect chunks");
    let combined: Vec<u8> = chunks.iter().flat_map(|c| c.iter().copied()).collect();
    assert_eq!(combined, b"0123456789");

    let ranged = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                range: Some(GetRange::Bounded(2..6)),
                ..Default::default()
            },
        )
        .await
        .expect("ranged get_opts");
    assert_eq!(ranged.range, 2..6);
    assert_eq!(ranged.meta.size, 10);
    assert_eq!(&ranged.bytes().await.expect("ranged bytes")[..], b"2345");

    let ignored_range = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                range: Some(GetRange::Bounded(0..4)),
                ..Default::default()
            },
        )
        .await
        .expect_err("200 response to a Range request must be rejected");
    assert!(
        matches!(&ignored_range, object_store::Error::Generic { source, .. } if source.to_string().contains("expected 206")),
        "unexpected ignored range error: {ignored_range:?}"
    );

    let offset_no_cr = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                range: Some(GetRange::Offset(4)),
                ..Default::default()
            },
        )
        .await
        .expect_err("206 offset response without Content-Range must be rejected");
    assert!(
        matches!(&offset_no_cr, object_store::Error::Generic { source, .. } if source.to_string().contains("Content-Range")),
        "unexpected offset error: {offset_no_cr:?}"
    );

    let (rev_start, rev_end) = (5u64, 2u64);
    let reversed = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                range: Some(GetRange::Bounded(rev_start..rev_end)),
                ..Default::default()
            },
        )
        .await
        .expect_err("reversed bounded range must be rejected");
    assert!(matches!(reversed, object_store::Error::Generic { .. }));

    let empty_range = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                range: Some(GetRange::Bounded(3..3)),
                ..Default::default()
            },
        )
        .await
        .expect("empty bounded range verifies existence via HEAD");
    assert_eq!(empty_range.range, 3..3);
    assert_eq!(empty_range.meta.size, 10);
    assert_eq!(empty_range.meta.e_tag.as_deref(), Some("\"oci-etag-1\""));
    assert!(empty_range.bytes().await.expect("empty bytes").is_empty());

    let past_end = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                range: Some(GetRange::Bounded(11..11)),
                ..Default::default()
            },
        )
        .await
        .expect_err("empty bounded range past object size must be rejected");
    assert!(matches!(past_end, object_store::Error::Generic { .. }));

    let not_mod = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                if_none_match: Some("\"oci-etag-1\"".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("304 must map to NotModified");
    assert!(matches!(not_mod, object_store::Error::NotModified { .. }));

    let precond_get = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                if_match: Some("\"wrong\"".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect_err("412 on GET must map to Precondition");
    assert!(matches!(
        precond_get,
        object_store::Error::Precondition { .. }
    ));

    let head_res = oci
        .get_opts(
            &stream_loc,
            GetOptions {
                head: true,
                ..Default::default()
            },
        )
        .await
        .expect("head get_opts");
    assert_eq!(head_res.meta.size, 10);
    assert_eq!(
        head_res.attributes.get(&Attribute::ContentType),
        Some(&AttributeValue::from("application/json"))
    );
    assert_eq!(
        head_res
            .attributes
            .get(&Attribute::Metadata("owner".into())),
        Some(&AttributeValue::from("team-a"))
    );

    oci.copy_opts(
        &stream_loc,
        &StorePath::parse("copied.bin").expect("parse"),
        CopyOptions {
            mode: CopyMode::Create,
            ..Default::default()
        },
    )
    .await
    .expect("copy_if_not_exists ok");

    let copy_dup = oci
        .copy_opts(
            &stream_loc,
            &StorePath::parse("dup.bin").expect("parse"),
            CopyOptions {
                mode: CopyMode::Create,
                ..Default::default()
            },
        )
        .await
        .expect_err("copy Create work request FAILED must map to AlreadyExists");
    assert!(matches!(
        copy_dup,
        object_store::Error::AlreadyExists { .. }
    ));
    assert!(
        wr_ok_polls.load(Ordering::SeqCst) >= 2,
        "copyObject must poll the work request until COMPLETED"
    );

    oci.rename_opts(
        &stream_loc,
        &StorePath::parse("renamed.bin").expect("parse"),
        RenameOptions::new().with_target_mode(RenameTargetMode::Overwrite),
    )
    .await
    .expect("plain rename ok");

    let rename_taken = oci
        .rename_opts(
            &stream_loc,
            &StorePath::parse("taken.bin").expect("parse"),
            RenameOptions::new().with_target_mode(RenameTargetMode::Create),
        )
        .await
        .expect_err("rename Create conflict must map to AlreadyExists");
    assert!(matches!(
        rename_taken,
        object_store::Error::AlreadyExists { .. }
    ));

    let rename_missing = oci
        .rename_opts(
            &StorePath::parse("missing.bin").expect("parse"),
            &StorePath::parse("anywhere.bin").expect("parse"),
            RenameOptions::new(),
        )
        .await
        .expect_err("rename of missing source must map to NotFound");
    assert!(matches!(
        rename_missing,
        object_store::Error::NotFound { .. }
    ));

    let all_items: Vec<_> = oci.list(None).try_collect().await.expect("paginated list");
    let all_paths: Vec<String> = all_items.iter().map(|m| m.location.to_string()).collect();
    assert_eq!(all_paths, vec!["p1/item~1%25.bin", "p2/item~2%25.bin"]);

    let delim_res = oci
        .list_with_delimiter(Some(&StorePath::parse("dir").expect("parse")))
        .await
        .expect("paginated list_with_delimiter");
    let delim_paths: Vec<String> = delim_res
        .objects
        .iter()
        .map(|m| m.location.to_string())
        .collect();
    let delim_prefixes: Vec<String> = delim_res
        .common_prefixes
        .iter()
        .map(ToString::to_string)
        .collect();
    assert_eq!(
        delim_paths,
        vec!["dir/k1~raw%25.parquet", "dir/k2~raw%25.parquet"]
    );
    assert_eq!(delim_prefixes, vec!["dir/sub1", "dir/sub2"]);

    let reqs = captured_reqs.lock().expect("lock");
    let put_req = reqs[0].to_ascii_lowercase();
    assert!(put_req.contains("if-none-match: *"));
    assert!(put_req.contains("content-type: application/json"));
    assert!(put_req.contains("opc-meta-owner: team-a"));
    let rename_bodies: Vec<&String> = reqs
        .iter()
        .filter(|r| r.starts_with("POST /n/ns1/b/bkt1/actions/renameObject "))
        .collect();
    assert_eq!(rename_bodies.len(), 3);
    assert!(rename_bodies[0].contains(r#"{"sourceName":"stream.bin","newName":"renamed.bin"}"#));
    assert!(rename_bodies[1].contains(r#""newObjIfNoneMatchETag":"*""#));
    assert!(
        reqs.iter()
            .any(|r| r.starts_with("GET /workRequests/wr-dup/errors "))
    );
}

#[tokio::test]
async fn test_oci_cavage_conditional_put_signs_only_canonical_headers() {
    use _storage::oci::OciObjectStore;
    use object_store::path::Path as StorePath;
    use object_store::{
        Attribute, AttributeValue, Attributes, ObjectStore, PutMode, PutOptions, PutPayload,
    };
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback oci");
    let addr = listener.local_addr().expect("oci addr");
    let captured: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_srv = Arc::clone(&captured);
    tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let log = Arc::clone(&captured_srv);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 8192];
                let mut total = 0usize;
                loop {
                    let n = socket.read(&mut buf[total..]).await.unwrap_or(0);
                    if n == 0 {
                        break;
                    }
                    total += n;
                    if let Some(hdr_end) = buf[..total].windows(4).position(|w| w == b"\r\n\r\n") {
                        let hdr_str = String::from_utf8_lossy(&buf[..hdr_end]);
                        let expected_cl = hdr_str
                            .lines()
                            .find_map(|l| {
                                let (k, v) = l.split_once(':')?;
                                k.trim()
                                    .eq_ignore_ascii_case("content-length")
                                    .then(|| v.trim().parse::<usize>().ok())?
                            })
                            .unwrap_or(0);
                        if total >= hdr_end + 4 + expected_cl {
                            break;
                        }
                    }
                }
                log.lock()
                    .expect("lock")
                    .push(String::from_utf8_lossy(&buf[..total]).into_owned());
                let _ = socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\netag: \"signed-etag\"\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                    )
                    .await;
            });
        }
    });

    let creds = OciCavageCredentials {
        tenancy_ocid: "ocid1.tenancy.oc1..aaaa".to_string(),
        user_ocid: "ocid1.user.oc1..bbbb".to_string(),
        fingerprint: "12:34:56:78:90:ab:cd:ef".to_string(),
        private_key_pem: TEST_OCI_RSA_PEM.to_string(),
    };
    let oci = OciObjectStore::new(
        "ns1".to_string(),
        "bkt1".to_string(),
        "us-ashburn-1".to_string(),
        Some(format!("http://127.0.0.1:{}", addr.port())),
        Some(creds),
    )
    .expect("build signed oci store");

    let mut attrs = Attributes::new();
    attrs.insert(
        Attribute::ContentType,
        AttributeValue::from("application/json"),
    );
    attrs.insert(
        Attribute::Metadata("owner".into()),
        AttributeValue::from("team-a"),
    );
    let res = oci
        .put_opts(
            &StorePath::parse("signed.json").expect("parse"),
            PutPayload::from_static(b"{\"a\":1}"),
            PutOptions {
                mode: PutMode::Create,
                attributes: attrs,
                ..Default::default()
            },
        )
        .await
        .expect("signed PutMode::Create");
    assert_eq!(res.e_tag.as_deref(), Some("\"signed-etag\""));

    let reqs = captured.lock().expect("lock");
    assert_eq!(reqs.len(), 1);
    let req = &reqs[0];
    let req_lower = req.to_ascii_lowercase();
    let auth_line = req
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("authorization:"))
        .expect("Authorization header present");
    let signed_headers = auth_line
        .split_once("headers=\"")
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(list, _)| list)
        .expect("headers=\"...\" parameter");
    assert_eq!(
        signed_headers,
        "(request-target) date host x-content-sha256 content-type content-length"
    );
    assert!(auth_line.contains("algorithm=\"rsa-sha256\""));
    assert!(req_lower.contains("if-none-match: *"));
    assert!(req_lower.contains("opc-meta-owner: team-a"));
    assert!(req_lower.contains("content-type: application/json"));
    assert!(req_lower.contains("content-length: 7"));
    assert!(req_lower.contains(&format!(
        "x-content-sha256: {}",
        sha256_base64(b"{\"a\":1}").to_ascii_lowercase()
    )));
}

#[test]
fn test_s3_virtual_hosted_style_rejects_ip_literal_endpoints() {
    use _storage::store::StorageError;

    for endpoint in ["http://127.0.0.1:9000", "http://[::1]:9000"] {
        let vhost_ip = S3SignConfig {
            bucket: "assets".to_string(),
            region: "us-east-1".to_string(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            endpoint: Some(endpoint.to_string()),
            virtual_hosted_style: true,
        };
        let sign_err = sign_s3_v4_url(&vhost_ip, "GET", "k/obj.bin", 900, 1_700_000_000)
            .expect_err("virtual-hosted signing must reject IP-literal endpoints");
        assert!(
            matches!(sign_err, StorageError::InvalidInput(ref msg) if msg.contains("IP-literal")),
            "unexpected sign error for {endpoint}: {sign_err:?}"
        );

        let path_style = S3SignConfig {
            virtual_hosted_style: false,
            ..vhost_ip.clone()
        };
        let signed_path = sign_s3_v4_url(&path_style, "GET", "k/obj.bin", 900, 1_700_000_000)
            .expect("path-style signing accepts IP-literal endpoints");
        let verify_err = verify_s3_v4_url(&vhost_ip, "GET", &signed_path, 1_700_000_100)
            .expect_err("virtual-hosted verification must reject IP-literal endpoints");
        assert!(
            matches!(verify_err, StorageError::InvalidInput(ref msg) if msg.contains("IP-literal")),
            "unexpected verify error for {endpoint}: {verify_err:?}"
        );
        assert!(
            verify_s3_v4_url(&path_style, "GET", &signed_path, 1_700_000_100)
                .expect("path-style verify")
        );

        let opts = HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            ("access_key_id".to_string(), "ak".to_string()),
            ("secret_access_key".to_string(), "sk".to_string()),
            ("virtual_hosted_style".to_string(), "true".to_string()),
            ("endpoint".to_string(), endpoint.to_string()),
            ("allow_http".to_string(), "true".to_string()),
        ]);
        match ObjectStoreEngine::from_url("s3://bkt", Some(&opts)) {
            Err(StorageError::InvalidInput(msg)) => assert!(msg.contains("IP-literal")),
            Err(other) => panic!("unexpected engine error for {endpoint}: {other:?}"),
            Ok(_) => panic!("virtual-hosted engine must reject IP-literal endpoint {endpoint}"),
        }

        let path_opts = HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            ("access_key_id".to_string(), "ak".to_string()),
            ("secret_access_key".to_string(), "sk".to_string()),
            ("endpoint".to_string(), endpoint.to_string()),
            ("allow_http".to_string(), "true".to_string()),
        ]);
        assert!(ObjectStoreEngine::from_url("s3://bkt", Some(&path_opts)).is_ok());
    }
}

#[derive(Debug, Default)]
struct UploadProbes {
    parts: std::sync::atomic::AtomicUsize,
    completes: std::sync::atomic::AtomicUsize,
    aborts: std::sync::atomic::AtomicUsize,
    fail_parts: std::sync::atomic::AtomicBool,
    fail_complete: std::sync::atomic::AtomicBool,
    part_sizes: std::sync::Mutex<Vec<usize>>,
}

#[derive(Debug)]
struct CountingUpload {
    inner: Box<dyn object_store::MultipartUpload>,
    probes: std::sync::Arc<UploadProbes>,
}

fn injected_failure(what: &str) -> object_store::Error {
    object_store::Error::Generic {
        store: "CountingStore",
        source: format!("injected {what} failure").into(),
    }
}

#[async_trait::async_trait]
impl object_store::MultipartUpload for CountingUpload {
    fn put_part(
        &mut self,
        data: object_store::PutPayload,
    ) -> futures::future::BoxFuture<'static, object_store::Result<()>> {
        use std::sync::atomic::Ordering;
        self.probes.parts.fetch_add(1, Ordering::SeqCst);
        self.probes
            .part_sizes
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(data.content_length());
        if self.probes.fail_parts.load(Ordering::SeqCst) {
            return Box::pin(async { Err(injected_failure("part")) });
        }
        self.inner.put_part(data)
    }

    async fn complete(&mut self) -> object_store::Result<object_store::PutResult> {
        use std::sync::atomic::Ordering;
        self.probes.completes.fetch_add(1, Ordering::SeqCst);
        if self.probes.fail_complete.load(Ordering::SeqCst) {
            return Err(injected_failure("complete"));
        }
        self.inner.complete().await
    }

    async fn abort(&mut self) -> object_store::Result<()> {
        self.probes
            .aborts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.abort().await
    }
}

#[derive(Debug)]
struct CountingStore {
    inner: object_store::memory::InMemory,
    put_calls: std::sync::atomic::AtomicUsize,
    multipart_calls: std::sync::atomic::AtomicUsize,
    get_in_flight: std::sync::atomic::AtomicUsize,
    get_max_in_flight: std::sync::atomic::AtomicUsize,
    get_ok: std::sync::atomic::AtomicUsize,
    get_delay: std::time::Duration,
    stream_stall: std::time::Duration,
    uploads: std::sync::Arc<UploadProbes>,
}

impl CountingStore {
    fn new(get_delay: std::time::Duration) -> Self {
        Self {
            inner: object_store::memory::InMemory::new(),
            put_calls: std::sync::atomic::AtomicUsize::new(0),
            multipart_calls: std::sync::atomic::AtomicUsize::new(0),
            get_in_flight: std::sync::atomic::AtomicUsize::new(0),
            get_max_in_flight: std::sync::atomic::AtomicUsize::new(0),
            get_ok: std::sync::atomic::AtomicUsize::new(0),
            get_delay,
            stream_stall: std::time::Duration::ZERO,
            uploads: std::sync::Arc::new(UploadProbes::default()),
        }
    }

    /// Deliver each object's first chunk, then stall the body stream for `stall`.
    fn with_stream_stall(mut self, stall: std::time::Duration) -> Self {
        self.stream_stall = stall;
        self
    }
}

impl std::fmt::Display for CountingStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CountingStore")
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        location: &object_store::path::Path,
        payload: object_store::PutPayload,
        opts: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        self.put_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &object_store::path::Path,
        opts: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        self.multipart_calls
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let inner = self.inner.put_multipart_opts(location, opts).await?;
        Ok(Box::new(CountingUpload {
            inner,
            probes: std::sync::Arc::clone(&self.uploads),
        }))
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let now = self
            .get_in_flight
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        self.get_max_in_flight
            .fetch_max(now, std::sync::atomic::Ordering::SeqCst);
        if !self.get_delay.is_zero() {
            tokio::time::sleep(self.get_delay).await;
        }
        let result = self.inner.get_opts(location, options).await;
        self.get_in_flight
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        let mut result = result?;
        self.get_ok
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if !self.stream_stall.is_zero()
            && let object_store::GetResultPayload::Stream(body) = result.payload
        {
            use futures::StreamExt;
            let stall = self.stream_stall;
            let tail = futures::stream::once(async move {
                tokio::time::sleep(stall).await;
                Ok(Bytes::new())
            });
            result.payload = object_store::GetResultPayload::Stream(body.chain(tail).boxed());
        }
        Ok(result)
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &object_store::path::Path,
        to: &object_store::path::Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

fn patterned_payload(len: usize, seed: u8) -> Vec<u8> {
    (0..len)
        .map(|i| ((i as u64 * 31 + u64::from(seed)) % 251) as u8)
        .collect()
}

fn leftover_part_files(dir: &std::path::Path) -> Vec<String> {
    let mut found = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&current) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.ends_with(".part"))
            {
                found.push(path.display().to_string());
            }
        }
    }
    found
}

fn percent_decode_path(raw: &str) -> String {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && i + 3 <= bytes.len()
            && let Ok(v) = u8::from_str_radix(&raw[i + 1..i + 3], 16)
        {
            out.push(v);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

async fn read_http_request(stream: &mut tokio::net::TcpStream) -> (String, Vec<u8>) {
    use tokio::io::AsyncReadExt;
    let mut buf: Vec<u8> = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];
    loop {
        let n = stream.read(&mut chunk).await.unwrap_or(0);
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(hdr_end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..hdr_end]).into_owned();
            let content_len = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    if k.eq_ignore_ascii_case("content-length") {
                        v.trim().parse::<usize>().ok()
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            if buf.len() >= hdr_end + 4 + content_len {
                let body = buf[hdr_end + 4..hdr_end + 4 + content_len].to_vec();
                return (head, body);
            }
        }
    }
    (String::from_utf8_lossy(&buf).into_owned(), Vec::new())
}

fn http_json_response(status: &str, body: &str) -> String {
    format!(
        "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
}

type EmuObjects = std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>>;

struct GcsComposeEmulator {
    port: u16,
    objects: EmuObjects,
    requests: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    max_sources_per_call: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

/// Spawn a loopback GCS emulator serving JSON `compose`, XML `HEAD`, and XML `DELETE`.
///
/// `HEAD` reports an XML-API ETag (`"xml-{size}"`) distinct from the JSON `compose` ETag so
/// tests can tell which API produced returned metadata. When `stall_compose_to` names a
/// destination, compose calls targeting it hang for 30 seconds before responding.
async fn spawn_gcs_compose_emulator(
    reject_bearer: Option<&'static str>,
    stall_compose_to: Option<&'static str>,
) -> GcsComposeEmulator {
    use std::sync::atomic::Ordering;
    use std::sync::{Arc, Mutex};
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind compose emulator");
    let port = listener.local_addr().expect("emulator addr").port();
    let objects: EmuObjects = Arc::new(Mutex::new(std::collections::BTreeMap::new()));
    let requests = Arc::new(Mutex::new(Vec::new()));
    let max_sources = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let objects_srv = Arc::clone(&objects);
    let requests_srv = Arc::clone(&requests);
    let max_sources_srv = Arc::clone(&max_sources);

    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let objects = Arc::clone(&objects_srv);
            let requests = Arc::clone(&requests_srv);
            let max_sources = Arc::clone(&max_sources_srv);
            tokio::spawn(async move {
                let (head, body) = read_http_request(&mut stream).await;
                requests.lock().expect("lock").push(head.clone());
                let first_line = head.lines().next().unwrap_or_default().to_string();
                let mut parts = first_line.split_whitespace();
                let method = parts.next().unwrap_or_default().to_string();
                let target = parts.next().unwrap_or_default().to_string();
                let path_only = percent_decode_path(target.split('?').next().unwrap_or_default());
                let lower = head.to_ascii_lowercase();
                if method == "POST"
                    && let Some(stalled) = stall_compose_to
                    && path_only.ends_with(&format!("/o/{stalled}/compose"))
                {
                    tokio::time::sleep(std::time::Duration::from_secs(30)).await;
                }

                let resp = if let Some(rejected) = reject_bearer
                    && lower.contains(&format!("authorization: bearer {rejected}"))
                {
                    http_json_response("401 Unauthorized", r#"{"error":{"code":401}}"#)
                } else if method == "POST"
                    && let Some(rest) = path_only.strip_prefix("/storage/v1/b/emu-bucket/o/")
                    && let Some(dst) = rest.strip_suffix("/compose")
                {
                    let dst = dst.to_string();
                    let parsed: serde_json::Value =
                        serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
                    let names: Vec<String> = parsed["sourceObjects"]
                        .as_array()
                        .map(|arr| {
                            arr.iter()
                                .filter_map(|o| o["name"].as_str().map(str::to_string))
                                .collect()
                        })
                        .unwrap_or_default();
                    max_sources.fetch_max(names.len(), Ordering::SeqCst);
                    let content_type = parsed["destination"]["contentType"]
                        .as_str()
                        .unwrap_or("application/octet-stream")
                        .to_string();
                    let mut guard = objects.lock().expect("lock");
                    let mut combined = Vec::new();
                    let mut missing = None;
                    for name in &names {
                        match guard.get(name) {
                            Some(data) => combined.extend_from_slice(data),
                            None => {
                                missing = Some(name.clone());
                                break;
                            }
                        }
                    }
                    if names.is_empty() || names.len() > 32 {
                        http_json_response("400 Bad Request", r#"{"error":{"code":400}}"#)
                    } else if let Some(name) = missing {
                        http_json_response(
                            "404 Not Found",
                            &format!(
                                r#"{{"error":{{"code":404,"message":"No such object: {name}"}}}}"#
                            ),
                        )
                    } else {
                        let size = combined.len();
                        guard.insert(dst.clone(), combined);
                        let generation = guard.len();
                        http_json_response(
                            "200 OK",
                            &format!(
                                r#"{{"kind":"storage#object","name":"{dst}","bucket":"emu-bucket","generation":"{generation}","contentType":"{content_type}","size":"{size}","etag":"etag-{generation}","updated":"2026-11-09T00:00:00.000Z"}}"#
                            ),
                        )
                    }
                } else if method == "HEAD"
                    && let Some(key) = path_only.strip_prefix("/emu-bucket/")
                {
                    match objects.lock().expect("lock").get(key).map(Vec::len) {
                        Some(size) => format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {size}\r\nETag: \"xml-{size}\"\r\nLast-Modified: Mon, 09 Nov 2026 00:00:00 GMT\r\nx-goog-generation: {size}\r\nConnection: close\r\n\r\n"
                        ),
                        None => "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string(),
                    }
                } else if method == "DELETE"
                    && let Some(key) = path_only.strip_prefix("/emu-bucket/")
                {
                    if objects.lock().expect("lock").remove(key).is_some() {
                        "HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    } else {
                        "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
                            .to_string()
                    }
                } else {
                    http_json_response("400 Bad Request", r#"{"error":{"code":400}}"#)
                };
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    GcsComposeEmulator {
        port,
        objects,
        requests,
        max_sources_per_call: max_sources,
    }
}

#[tokio::test]
async fn test_put_file_streams_multipart_and_get_file_renames_atomically() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let tmp = TempDir::new().expect("tempdir");
    let large = patterned_payload(10 * 1024 * 1024, 7);
    let small = patterned_payload(4096, 3);
    let large_src = tmp.path().join("large.bin");
    let small_src = tmp.path().join("small.bin");
    std::fs::write(&large_src, &large).expect("write large source");
    std::fs::write(&small_src, &small).expect("write small source");

    let local_root = tmp.path().join("store-root");
    let local_url = format!("file://{}", local_root.display());
    let engines = [
        ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine"),
        ObjectStoreEngine::from_url(&local_url, None).expect("local engine"),
    ];

    for (idx, engine) in engines.iter().enumerate() {
        let meta = engine
            .put_file("files/large.bin", large_src.to_str().expect("utf8 path"))
            .await
            .expect("put_file large");
        assert_eq!(meta.size, large.len() as u64);
        assert_eq!(meta.path, "files/large.bin");
        let fetched = engine.get("files/large.bin").await.expect("get large");
        assert_eq!(fetched.len(), large.len());
        assert!(
            fetched[..] == large[..],
            "uploaded large payload must match"
        );

        let small_meta = engine
            .put_file("files/small.bin", small_src.to_str().expect("utf8 path"))
            .await
            .expect("put_file small");
        assert_eq!(small_meta.size, small.len() as u64);

        let out_dir = tmp.path().join(format!("out-{idx}"));
        let target = out_dir.join("nested").join("download.bin");
        let got = engine
            .get_file("files/large.bin", target.to_str().expect("utf8 path"))
            .await
            .expect("get_file large");
        assert_eq!(got.size, large.len() as u64);
        assert_eq!(got.path, "files/large.bin");
        let on_disk = std::fs::read(&target).expect("read downloaded file");
        assert!(on_disk == large, "downloaded large payload must match");
        assert!(leftover_part_files(&out_dir).is_empty());

        let missing_target = out_dir.join("missing.bin");
        let missing = engine
            .get_file(
                "files/absent.bin",
                missing_target.to_str().expect("utf8 path"),
            )
            .await
            .expect_err("missing key must fail");
        assert!(matches!(
            missing,
            _storage::store::StorageError::ObjectStore(object_store::Error::NotFound { .. })
        ));
        assert!(!missing_target.exists());
        assert!(leftover_part_files(&out_dir).is_empty());
    }

    let counting = Arc::new(CountingStore::new(std::time::Duration::ZERO));
    let counted = ObjectStoreEngine::with_store(counting.clone(), "memory");
    counted
        .put_file("c/large.bin", large_src.to_str().expect("utf8 path"))
        .await
        .expect("counted put_file large");
    assert_eq!(counting.multipart_calls.load(Ordering::SeqCst), 1);
    assert_eq!(counting.put_calls.load(Ordering::SeqCst), 0);
    counted
        .put_file("c/small.bin", small_src.to_str().expect("utf8 path"))
        .await
        .expect("counted put_file small");
    assert_eq!(counting.multipart_calls.load(Ordering::SeqCst), 1);
    assert_eq!(counting.put_calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        counted.get("c/large.bin").await.expect("get counted").len(),
        large.len()
    );
}

#[tokio::test]
async fn test_get_files_put_files_bounded_concurrency_order_and_cleanup() {
    use _storage::store::StorageError;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let tmp = TempDir::new().expect("tempdir");
    let counting = Arc::new(CountingStore::new(std::time::Duration::from_millis(25)));
    let engine = ObjectStoreEngine::with_store(counting.clone(), "memory");

    let mut upload_pairs = Vec::new();
    for i in 0..8u8 {
        let src = tmp.path().join(format!("src-{i}.bin"));
        std::fs::write(&src, patterned_payload(1000 + usize::from(i) * 17, i)).expect("write src");
        upload_pairs.push((format!("batch/obj-{i}"), src.display().to_string()));
    }
    let put_metas = engine
        .put_files(upload_pairs.clone(), 3)
        .await
        .expect("put_files");
    assert_eq!(put_metas.len(), 8);
    for (i, meta) in put_metas.iter().enumerate() {
        assert_eq!(meta.path, format!("batch/obj-{i}"));
        assert_eq!(meta.size, 1000 + i as u64 * 17);
        let stored = engine.get(&meta.path).await.expect("get uploaded");
        assert_eq!(&stored[..], &patterned_payload(1000 + i * 17, i as u8)[..]);
    }

    counting.get_max_in_flight.store(0, Ordering::SeqCst);
    let dl_dir = tmp.path().join("dl");
    let download_pairs: Vec<(String, String)> = (0..8)
        .map(|i| {
            (
                format!("batch/obj-{i}"),
                dl_dir.join(format!("obj-{i}.bin")).display().to_string(),
            )
        })
        .collect();
    let got = engine
        .get_files(download_pairs.clone(), 3)
        .await
        .expect("get_files");
    assert_eq!(got.len(), 8);
    for (i, meta) in got.iter().enumerate() {
        assert_eq!(meta.path, format!("batch/obj-{i}"));
        let on_disk = std::fs::read(&download_pairs[i].1).expect("read downloaded");
        assert_eq!(on_disk, patterned_payload(1000 + i * 17, i as u8));
    }
    let max_in_flight = counting.get_max_in_flight.load(Ordering::SeqCst);
    assert!(
        (2..=3).contains(&max_in_flight),
        "get_files must overlap requests but never exceed concurrency=3 (observed {max_in_flight})"
    );

    let fail_dir = tmp.path().join("dl-fail");
    std::fs::create_dir_all(&fail_dir).expect("create fail dir");
    let preexisting = fail_dir.join("obj-0.bin");
    std::fs::write(&preexisting, b"stale").expect("write pre-existing destination");
    counting.get_ok.store(0, Ordering::SeqCst);
    let failing_pairs: Vec<(String, String)> = (0..8)
        .map(|i| {
            let key = if i == 5 {
                "batch/missing".to_string()
            } else {
                format!("batch/obj-{i}")
            };
            (
                key,
                fail_dir.join(format!("obj-{i}.bin")).display().to_string(),
            )
        })
        .collect();
    let err = engine
        .get_files(failing_pairs, 3)
        .await
        .expect_err("missing item must fail the batch");
    assert!(matches!(
        err,
        StorageError::ObjectStore(object_store::Error::NotFound { .. })
    ));
    let completed_before_failure = counting.get_ok.load(Ordering::SeqCst);
    assert!(
        completed_before_failure >= 3,
        "item 5 only starts after earlier downloads free a slot (observed {completed_before_failure})"
    );
    let leftovers: Vec<_> = std::fs::read_dir(&fail_dir)
        .expect("fail dir survives cleanup")
        .flatten()
        .map(|e| e.path())
        .collect();
    assert_eq!(
        leftovers,
        vec![preexisting.clone()],
        "failed get_files must unlink only destinations it created"
    );
    assert_eq!(
        std::fs::read(&preexisting).expect("read replaced destination"),
        patterned_payload(1000, 0),
        "a pre-existing destination that was replaced stays replaced"
    );

    for zero in [
        engine.get_files(download_pairs.clone(), 0).await,
        engine.put_files(upload_pairs.clone(), 0).await,
    ] {
        match zero {
            Err(StorageError::InvalidInput(msg)) => {
                assert!(msg.contains("concurrency must be >= 1"))
            }
            other => panic!("concurrency=0 must be rejected, got {other:?}"),
        }
    }
}

async fn copy_if_not_exists_server() -> (String, tokio::task::JoinHandle<()>) {
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}", listener.local_addr().unwrap());
    let objects = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
    let uploads = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
    let server = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let objects = objects.clone();
            let uploads = uploads.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut chunk = [0; 4096];
                let header_end = loop {
                    let count = socket.read(&mut chunk).await.unwrap();
                    if count == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..count]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&request[..header_end]).to_string();
                let mut first = headers.lines().next().unwrap().split_whitespace();
                let method = first.next().unwrap();
                let target = first.next().unwrap();
                let parsed = url::Url::parse(&format!("http://localhost{target}")).unwrap();
                let key = parsed
                    .path()
                    .trim_start_matches('/')
                    .trim_start_matches("bucket/")
                    .to_string();
                let query: HashMap<_, _> = parsed.query_pairs().into_owned().collect();
                let fields: HashMap<_, _> = headers
                    .lines()
                    .skip(1)
                    .filter_map(|line| {
                        line.split_once(':')
                            .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().to_string()))
                    })
                    .collect();
                let length = fields
                    .get("content-length")
                    .and_then(|v| v.parse::<usize>().ok())
                    .unwrap_or(0);
                while request.len() < header_end + length {
                    let count = socket.read(&mut chunk).await.unwrap();
                    assert_ne!(count, 0);
                    request.extend_from_slice(&chunk[..count]);
                }
                let (status, body) = {
                    let mut objects = objects.lock().unwrap();
                    let mut uploads = uploads.lock().unwrap();
                    let mut status = "200 OK";
                    let body = if method == "POST" && query.contains_key("delete") {
                        let xml =
                            String::from_utf8_lossy(&request[header_end..header_end + length]);
                        let deleted = xml
                            .split_once("<Key>")
                            .unwrap()
                            .1
                            .split_once("</Key>")
                            .unwrap()
                            .0;
                        objects.remove(deleted);
                        format!(
                            "<DeleteResult><Deleted><Key>{deleted}</Key></Deleted></DeleteResult>"
                        )
                        .into_bytes()
                    } else if method == "POST" && query.contains_key("uploads") {
                        b"<InitiateMultipartUploadResult><UploadId>fixture</UploadId></InitiateMultipartUploadResult>".to_vec()
                    } else if method == "PUT" && query.contains_key("partNumber") {
                        let source = fields
                            .get("x-amz-copy-source")
                            .unwrap()
                            .trim_start_matches('/')
                            .trim_start_matches("bucket/");
                        uploads.insert(key.clone(), objects.get(source).unwrap().clone());
                        b"<CopyPartResult><ETag>\"fixture\"</ETag><LastModified>2026-11-12T00:00:00Z</LastModified></CopyPartResult>".to_vec()
                    } else if method == "POST" && query.contains_key("uploadId") {
                        assert_eq!(fields.get("if-none-match").map(String::as_str), Some("*"));
                        if let std::collections::hash_map::Entry::Vacant(entry) =
                            objects.entry(key.clone())
                        {
                            entry.insert(uploads.remove(&key).unwrap());
                            b"<CompleteMultipartUploadResult><ETag>\"fixture\"</ETag></CompleteMultipartUploadResult>".to_vec()
                        } else {
                            status = "412 Precondition Failed";
                            b"<Error><Code>PreconditionFailed</Code></Error>".to_vec()
                        }
                    } else if method == "DELETE" {
                        if query.contains_key("uploadId") {
                            uploads.remove(&key);
                        } else {
                            objects.remove(&key);
                        }
                        Vec::new()
                    } else if method == "PUT" {
                        if let Some(source) = fields.get("x-amz-copy-source") {
                            assert_eq!(
                                fields
                                    .get("cf-copy-destination-if-none-match")
                                    .map(String::as_str),
                                Some("*")
                            );
                            if key == "unsupported" {
                                status = "501 Not Implemented";
                                b"<Error><Code>NotImplemented</Code></Error>".to_vec()
                            } else if objects.contains_key(&key) {
                                status = "412 Precondition Failed";
                                b"<Error><Code>PreconditionFailed</Code></Error>".to_vec()
                            } else {
                                let data = objects
                                    .get(
                                        source
                                            .trim_start_matches('/')
                                            .trim_start_matches("bucket/"),
                                    )
                                    .unwrap()
                                    .clone();
                                objects.insert(key, data);
                                b"<CopyObjectResult><ETag>\"fixture\"</ETag></CopyObjectResult>"
                                    .to_vec()
                            }
                        } else {
                            objects.insert(key, request[header_end..header_end + length].to_vec());
                            Vec::new()
                        }
                    } else if let Some(data) = objects.get(&key) {
                        data.clone()
                    } else {
                        status = "404 Not Found";
                        b"<Error><Code>NoSuchKey</Code></Error>".to_vec()
                    };
                    (status, body)
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\nETag: \"fixture\"\r\nLast-Modified: Thu, 12 Nov 2026 00:00:00 GMT\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                socket.write_all(response.as_bytes()).await.unwrap();
                if method != "HEAD" {
                    socket.write_all(&body).await.unwrap();
                }
            });
        }
    });
    (endpoint, server)
}

#[tokio::test]
async fn test_copy_if_not_exists_s3_compatible() {
    use _storage::store::StorageError;

    let tmp = TempDir::new().unwrap();
    for scheme in ["memory", "file", "s3", "b2", "r2"] {
        let (endpoint, server) = copy_if_not_exists_server().await;
        let options = HashMap::from([
            ("endpoint".to_string(), endpoint),
            ("allow_http".to_string(), "true".to_string()),
            ("access_key_id".to_string(), "fixture".to_string()),
            ("secret_access_key".to_string(), "fixture".to_string()),
            ("account_id".to_string(), "fixture".to_string()),
            ("region".to_string(), "us-east-1".to_string()),
        ]);
        let url = if scheme == "file" {
            format!("file://{}", tmp.path().display())
        } else {
            format!("{scheme}://bucket")
        };
        let engine = ObjectStoreEngine::from_url(&url, Some(&options)).unwrap();
        engine.put("a", Bytes::from_static(b"alpha")).await.unwrap();
        engine.put("b", Bytes::from_static(b"bravo")).await.unwrap();
        for rename in [false, true] {
            let result = if rename {
                engine.rename("a", "b", true).await
            } else {
                engine.copy("a", "b", true).await
            };
            assert!(
                matches!(result, Err(StorageError::AlreadyExists { .. })),
                "{scheme}: {result:?}"
            );
            assert_eq!(engine.get("b").await.unwrap().as_ref(), b"bravo");
            assert_eq!(engine.get("a").await.unwrap().as_ref(), b"alpha");
        }
        engine.copy("a", "c", true).await.unwrap();
        assert_eq!(engine.get("c").await.unwrap().as_ref(), b"alpha");
        engine.rename("a", "d", true).await.unwrap();
        assert!(!engine.exists("a").await.unwrap());
        assert_eq!(engine.get("d").await.unwrap().as_ref(), b"alpha");
        if scheme == "r2" {
            let err = engine.copy("d", "unsupported", true).await.unwrap_err();
            assert!(!matches!(err, StorageError::AlreadyExists { .. }));
            assert_eq!(engine.get("d").await.unwrap().as_ref(), b"alpha");
        }
        server.abort();
    }
}

#[tokio::test]
async fn test_gcs_compose_tree_fold_cleans_temporaries_and_retries_401() {
    use _storage::store::StorageError;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use vertebra_core::gcp_auth::{
        DEFAULT_REFRESH_SKEW_MS, GcpAuthSource, GcpTokenCache, ProxyPolicy,
        SharedGcpTokenCacheHandle,
    };

    let emu = spawn_gcs_compose_emulator(None, None).await;
    let mut expected = Vec::new();
    let mut shard_names = Vec::new();
    {
        let mut guard = emu.objects.lock().expect("lock");
        for i in 0..70u8 {
            let name = format!("shards/part-{i:04}.bin");
            let data = patterned_payload(100 + usize::from(i), i);
            expected.extend_from_slice(&data);
            guard.insert(name.clone(), data);
            shard_names.push(name);
        }
    }
    let opts = HashMap::from([
        (
            "endpoint".to_string(),
            format!("http://127.0.0.1:{}", emu.port),
        ),
        ("allow_http".to_string(), "true".to_string()),
        ("skip_signature".to_string(), "true".to_string()),
    ]);
    let engine = ObjectStoreEngine::from_url("gs://emu-bucket", Some(&opts)).expect("gcs engine");

    let meta = engine
        .compose(
            shard_names.clone(),
            "out/combined.bin",
            Some("application/octet-stream".to_string()),
        )
        .await
        .expect("70-shard compose");
    assert_eq!(meta.path, "out/combined.bin");
    assert_eq!(meta.size, expected.len() as u64);
    assert_eq!(
        meta.content_type.as_deref(),
        Some("application/octet-stream")
    );
    let xml_etag = format!("\"xml-{}\"", expected.len());
    assert_eq!(
        meta.etag.as_deref(),
        Some(xml_etag.as_str()),
        "compose metadata must carry the XML-API ETag that head and put_opts use"
    );
    assert_eq!(
        meta.etag,
        engine.head("out/combined.bin").await.expect("head").etag
    );
    assert_eq!(meta.version, Some(expected.len().to_string()));
    assert!(emu.max_sources_per_call.load(Ordering::SeqCst) <= 32);
    {
        let guard = emu.objects.lock().expect("lock");
        assert!(guard.get("out/combined.bin").expect("combined object") == &expected);
        let temps: Vec<_> = guard.keys().filter(|k| k.contains(".compose-")).collect();
        assert!(temps.is_empty(), "temporaries must be deleted: {temps:?}");
    }
    let compose_calls = emu
        .requests
        .lock()
        .expect("lock")
        .iter()
        .filter(|r| r.contains("/compose"))
        .count();
    assert_eq!(compose_calls, 4);

    engine
        .compose(shard_names.clone(), "out/combined.bin", None)
        .await
        .expect("repeat 70-shard compose");
    let temp_names: std::collections::BTreeSet<String> = emu
        .requests
        .lock()
        .expect("lock")
        .iter()
        .filter(|r| r.starts_with("POST "))
        .map(|r| percent_decode_path(r))
        .filter_map(|r| {
            let start = r.find("out/combined.bin.compose-")?;
            let end = r[start..].find("/compose ")? + start;
            Some(r[start..end].to_string())
        })
        .collect();
    assert_eq!(
        temp_names.len(),
        6,
        "each compose call must fold through its own uniquely named temporaries: {temp_names:?}"
    );

    emu.objects
        .lock()
        .expect("lock")
        .remove("shards/part-0050.bin");
    let err = engine
        .compose(shard_names.clone(), "out/broken.bin", None)
        .await
        .expect_err("missing shard must fail compose");
    assert!(
        matches!(
            err,
            StorageError::ObjectStore(object_store::Error::NotFound { .. })
        ),
        "unexpected compose error: {err:?}"
    );
    {
        let guard = emu.objects.lock().expect("lock");
        assert!(!guard.contains_key("out/broken.bin"));
        let temps: Vec<_> = guard.keys().filter(|k| k.contains(".compose-")).collect();
        assert!(
            temps.is_empty(),
            "failed compose must delete temporaries: {temps:?}"
        );
    }
    let deleted_temp = emu.requests.lock().expect("lock").iter().any(|r| {
        let decoded = percent_decode_path(r);
        r.starts_with("DELETE ")
            && decoded.contains("broken.bin.compose-")
            && decoded.contains("-0000 ")
    });
    assert!(
        deleted_temp,
        "temporary from the successful first fold must be deleted"
    );

    let oauth_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind oauth");
    let oauth_port = oauth_listener.local_addr().expect("oauth addr").port();
    let oauth_calls = Arc::new(AtomicUsize::new(0));
    let oauth_calls_srv = Arc::clone(&oauth_calls);
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = oauth_listener.accept().await {
            let calls = Arc::clone(&oauth_calls_srv);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf).await;
                let n = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let body = format!(
                    r#"{{"access_token":"tok_{n}","expires_in":3600,"token_type":"Bearer"}}"#
                );
                let _ = stream
                    .write_all(http_json_response("200 OK", &body).as_bytes())
                    .await;
            });
        }
    });
    let authed_emu = spawn_gcs_compose_emulator(Some("tok_1"), None).await;
    authed_emu.objects.lock().expect("lock").extend([
        ("x/one".to_string(), b"one-".to_vec()),
        ("x/two".to_string(), b"two".to_vec()),
    ]);
    let cache = Arc::new(GcpTokenCache::with_proxy_policy(
        GcpAuthSource::AuthorizedUser {
            client_id: "cid".to_string(),
            client_secret: "csec".to_string(),
            refresh_token: "rtok".to_string(),
            quota_project_id: Some("billing-proj-99".to_string()),
            token_uri: format!("http://127.0.0.1:{oauth_port}/token"),
        },
        DEFAULT_REFRESH_SKEW_MS,
        ProxyPolicy::Direct,
    ));
    let authed_opts = HashMap::from([
        (
            "endpoint".to_string(),
            format!("http://127.0.0.1:{}", authed_emu.port),
        ),
        ("allow_http".to_string(), "true".to_string()),
        ("allow_ambient_adc".to_string(), "false".to_string()),
    ]);
    let authed = ObjectStoreEngine::from_url_with_cache(
        "gs://emu-bucket",
        Some(authed_opts),
        Some(SharedGcpTokenCacheHandle::from_arc(cache)),
    )
    .expect("authed gcs engine");
    let single = authed
        .compose(
            vec!["x/one".to_string(), "x/two".to_string()],
            "x/joined",
            None,
        )
        .await
        .expect("compose retries once after 401 with refreshed token");
    assert_eq!(single.size, 7);
    assert_eq!(oauth_calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        authed_emu
            .objects
            .lock()
            .expect("lock")
            .get("x/joined")
            .map(Vec::as_slice),
        Some(&b"one-two"[..])
    );
    let authed_requests = authed_emu.requests.lock().expect("lock");
    let last = authed_requests
        .iter()
        .rfind(|r| r.contains("/compose"))
        .expect("captured compose request")
        .to_ascii_lowercase();
    assert!(last.contains("authorization: bearer tok_2"));
    assert!(last.contains("x-goog-user-project: billing-proj-99"));
}

#[tokio::test]
async fn test_compose_multipart_fallback_and_empty_sources() {
    use _storage::store::StorageError;

    let tmp = TempDir::new().expect("tempdir");
    let local_url = format!("file://{}", tmp.path().display());
    let engines = [
        ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine"),
        ObjectStoreEngine::from_url(&local_url, None).expect("local engine"),
    ];
    for engine in &engines {
        engine
            .put("parts/a", Bytes::from_static(b"alpha-"))
            .await
            .expect("put a");
        engine
            .put("parts/b", Bytes::from_static(b"bravo-"))
            .await
            .expect("put b");
        engine
            .put("parts/c", Bytes::from_static(b"charlie"))
            .await
            .expect("put c");

        let meta = engine
            .compose(
                vec![
                    "parts/a".to_string(),
                    "parts/b".to_string(),
                    "parts/c".to_string(),
                ],
                "joined.txt",
                Some("text/plain".to_string()),
            )
            .await
            .expect("fallback compose");
        assert_eq!(meta.size, 19);
        assert_eq!(meta.content_type.as_deref(), Some("text/plain"));
        assert_eq!(
            &engine.get("joined.txt").await.expect("get joined")[..],
            b"alpha-bravo-charlie"
        );

        let single = engine
            .compose(vec!["parts/c".to_string()], "single.txt", None)
            .await
            .expect("single-source compose");
        assert_eq!(single.size, 7);

        match engine.compose(Vec::new(), "empty.txt", None).await {
            Err(StorageError::InvalidInput(_)) => {}
            other => panic!("empty sources must be InvalidInput, got {other:?}"),
        }
        match engine
            .compose(vec!["../escape".to_string()], "bad.txt", None)
            .await
        {
            Err(StorageError::InvalidInput(_)) => {}
            other => panic!("traversal source must be InvalidInput, got {other:?}"),
        }

        let missing = engine
            .compose(
                vec!["parts/a".to_string(), "parts/missing".to_string()],
                "partial.txt",
                None,
            )
            .await
            .expect_err("missing source must fail");
        assert!(matches!(
            missing,
            StorageError::ObjectStore(object_store::Error::NotFound { .. })
        ));
        assert!(!engine.exists("partial.txt").await.expect("exists partial"));
    }
}

#[tokio::test]
async fn test_get_file_cancelled_mid_stream_removes_part_file() {
    use std::sync::Arc;

    let tmp = TempDir::new().expect("tempdir");
    let counting = Arc::new(
        CountingStore::new(std::time::Duration::ZERO)
            .with_stream_stall(std::time::Duration::from_secs(30)),
    );
    let engine = ObjectStoreEngine::with_store(counting, "memory");
    engine
        .put(
            "slow/object.bin",
            Bytes::from(patterned_payload(64 * 1024, 9)),
        )
        .await
        .expect("seed object");

    let out_dir = tmp.path().join("cancel");
    let target = out_dir.join("object.bin");
    let task_engine = engine.clone();
    let task_target = target.display().to_string();
    let task =
        tokio::spawn(async move { task_engine.get_file("slow/object.bin", &task_target).await });

    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while leftover_part_files(&out_dir).is_empty() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "get_file never created its .part file"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    task.abort();
    let joined = task.await.expect_err("aborted download must not finish");
    assert!(joined.is_cancelled());

    assert!(
        leftover_part_files(&out_dir).is_empty(),
        "cancelled get_file must unlink its .part file: {:?}",
        leftover_part_files(&out_dir)
    );
    assert!(!target.exists());
}

#[tokio::test]
async fn test_multipart_compose_frames_parts_and_cancellation_aborts_upload() {
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let counting = Arc::new(CountingStore::new(std::time::Duration::ZERO));
    let engine = ObjectStoreEngine::with_store(counting.clone(), "memory");
    let chunk = 64 * 1024;
    let mut sources = Vec::new();
    let mut expected = Vec::new();
    for i in 0..300u32 {
        let data = patterned_payload(chunk, (i % 251) as u8);
        expected.extend_from_slice(&data);
        let key = format!("small/part-{i:04}");
        engine
            .put(&key, Bytes::from(data))
            .await
            .expect("seed part");
        sources.push(key);
    }
    let total = expected.len();
    let meta = engine
        .compose(sources.clone(), "framed/out.bin", None)
        .await
        .expect("multipart compose");
    assert_eq!(meta.size, total as u64);
    let parts = counting.uploads.parts.load(Ordering::SeqCst);
    let max_parts = total.div_ceil(8 * 1024 * 1024);
    assert!(
        parts <= max_parts,
        "{} source chunks must be framed into at most {max_parts} parts, got {parts}",
        sources.len()
    );
    assert!(engine.get("framed/out.bin").await.expect("get composed") == expected);

    let stalled = Arc::new(
        CountingStore::new(std::time::Duration::ZERO)
            .with_stream_stall(std::time::Duration::from_secs(30)),
    );
    let stalled_engine = ObjectStoreEngine::with_store(stalled.clone(), "memory");
    stalled_engine
        .put("slow/a", Bytes::from_static(b"alpha"))
        .await
        .expect("seed slow source");
    let task_engine = stalled_engine.clone();
    let task = tokio::spawn(async move {
        task_engine
            .compose(vec!["slow/a".to_string()], "slow/out", None)
            .await
    });
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while stalled.get_ok.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "compose never started reading its source"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(stalled.multipart_calls.load(Ordering::SeqCst), 1);
    task.abort();
    assert!(task.await.expect_err("aborted compose").is_cancelled());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while stalled.uploads.aborts.load(Ordering::SeqCst) == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "dropped compose must abort its multipart upload"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(stalled.uploads.completes.load(Ordering::SeqCst), 0);
    assert!(!stalled_engine.exists("slow/out").await.expect("exists"));
}

#[tokio::test]
async fn test_multipart_session_failed_part_or_commit_never_commits_and_stays_abortable() {
    use _storage::store::PutConditionalOptions;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let counting = Arc::new(CountingStore::new(std::time::Duration::ZERO));
    let engine = ObjectStoreEngine::with_store(counting.clone(), "memory");
    let probes = &counting.uploads;

    probes.fail_parts.store(true, Ordering::SeqCst);
    let mut session = engine
        .create_multipart("mp/failed-part", PutConditionalOptions::default())
        .await
        .expect("create session");
    session
        .write_chunk(Bytes::from_static(b"payload"))
        .await
        .expect_err("injected part failure");
    probes.fail_parts.store(false, Ordering::SeqCst);
    session
        .complete()
        .await
        .expect_err("a session with a failed part must not commit");
    assert_eq!(probes.completes.load(Ordering::SeqCst), 0);
    session.abort().await.expect("abort after failed part");
    assert_eq!(probes.aborts.load(Ordering::SeqCst), 1);
    assert!(!engine.exists("mp/failed-part").await.expect("exists"));

    probes.fail_complete.store(true, Ordering::SeqCst);
    let mut session = engine
        .create_multipart("mp/failed-commit", PutConditionalOptions::default())
        .await
        .expect("create session");
    session
        .write_chunk(Bytes::from_static(b"payload"))
        .await
        .expect("write chunk");
    session
        .complete()
        .await
        .expect_err("injected commit failure");
    assert_eq!(probes.completes.load(Ordering::SeqCst), 1);
    session
        .abort()
        .await
        .expect("abort reaches the upload whose commit failed");
    assert_eq!(probes.aborts.load(Ordering::SeqCst), 2);
    session.abort().await.expect("second abort is a no-op");
    assert_eq!(probes.aborts.load(Ordering::SeqCst), 2);
    assert!(!engine.exists("mp/failed-commit").await.expect("exists"));
}

#[tokio::test]
async fn test_gcs_compose_cancellation_deletes_temporaries() {
    let emu = spawn_gcs_compose_emulator(None, Some("out/cancelled.bin")).await;
    let mut shard_names = Vec::new();
    {
        let mut guard = emu.objects.lock().expect("lock");
        for i in 0..70u8 {
            let name = format!("shards/part-{i:04}.bin");
            guard.insert(name.clone(), patterned_payload(64, i));
            shard_names.push(name);
        }
    }
    let opts = HashMap::from([
        (
            "endpoint".to_string(),
            format!("http://127.0.0.1:{}", emu.port),
        ),
        ("allow_http".to_string(), "true".to_string()),
        ("skip_signature".to_string(), "true".to_string()),
    ]);
    let engine = ObjectStoreEngine::from_url("gs://emu-bucket", Some(&opts)).expect("gcs engine");
    let task_engine = engine.clone();
    let task = tokio::spawn(async move {
        task_engine
            .compose(shard_names, "out/cancelled.bin", None)
            .await
    });

    let temp_count = || {
        emu.objects
            .lock()
            .expect("lock")
            .keys()
            .filter(|k| k.contains(".compose-"))
            .count()
    };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while !emu.requests.lock().expect("lock").iter().any(|r| {
        r.starts_with("POST ") && percent_decode_path(r).contains("/o/out/cancelled.bin/compose ")
    }) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "final compose call never started"
        );
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    assert_eq!(
        temp_count(),
        3,
        "three folds must exist before the final compose"
    );

    task.abort();
    assert!(task.await.expect_err("aborted compose").is_cancelled());
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while temp_count() > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "cancelled compose must delete its temporaries"
        );
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    assert!(
        !emu.objects
            .lock()
            .expect("lock")
            .contains_key("out/cancelled.bin")
    );
}

fn ref_crc32c_hex(bytes: &[u8]) -> String {
    let mut crc = !0u32;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F6_3B78;
            } else {
                crc >>= 1;
            }
        }
    }
    format!("{:08x}", !crc)
}

fn ref_digests(bytes: &[u8]) -> (String, String, String) {
    use md5::Md5;
    use sha2::{Digest, Sha256};

    let sha256 = hex::encode(Sha256::digest(bytes));
    let crc32c = ref_crc32c_hex(bytes);
    let md5 = hex::encode(Md5::digest(bytes));
    (sha256, crc32c, md5)
}

struct MockS3MultipartServer {
    endpoint: String,
    objects: std::sync::Arc<std::sync::Mutex<HashMap<String, Vec<u8>>>>,
    aborted_uploads: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    completed_uploads: std::sync::Arc<std::sync::Mutex<Vec<(String, String)>>>,
    put_requests: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    server: tokio::task::JoinHandle<()>,
}

async fn spawn_mock_s3_multipart_server() -> MockS3MultipartServer {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    type InFlightUploads = HashMap<(String, String), BTreeMap<usize, Vec<u8>>>;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock s3");
    let endpoint = format!("http://{}", listener.local_addr().expect("mock s3 addr"));
    let objects = Arc::new(Mutex::new(HashMap::<String, Vec<u8>>::new()));
    let uploads: Arc<Mutex<InFlightUploads>> = Arc::new(Mutex::new(HashMap::new()));
    let aborted_uploads = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let completed_uploads = Arc::new(Mutex::new(Vec::<(String, String)>::new()));
    let put_requests = Arc::new(AtomicUsize::new(0));
    let next_upload_seq = Arc::new(AtomicUsize::new(1));

    let objects_srv = Arc::clone(&objects);
    let uploads_srv = Arc::clone(&uploads);
    let aborted_srv = Arc::clone(&aborted_uploads);
    let completed_srv = Arc::clone(&completed_uploads);
    let put_requests_srv = Arc::clone(&put_requests);
    let seq_srv = Arc::clone(&next_upload_seq);

    let server = tokio::spawn(async move {
        while let Ok((mut socket, _)) = listener.accept().await {
            let objects = Arc::clone(&objects_srv);
            let uploads = Arc::clone(&uploads_srv);
            let aborted = Arc::clone(&aborted_srv);
            let completed = Arc::clone(&completed_srv);
            let put_requests = Arc::clone(&put_requests_srv);
            let seq = Arc::clone(&seq_srv);
            tokio::spawn(async move {
                let mut request = Vec::with_capacity(8192);
                let mut chunk = vec![0u8; 65536];
                let header_end = loop {
                    let count = socket.read(&mut chunk).await.unwrap_or(0);
                    if count == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..count]);
                    if let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") {
                        break end + 4;
                    }
                };
                let headers = String::from_utf8_lossy(&request[..header_end]).into_owned();
                let mut first = headers
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .split_whitespace();
                let method = first.next().unwrap_or_default().to_string();
                let target = first.next().unwrap_or_default().to_string();
                let parsed =
                    url::Url::parse(&format!("http://localhost{target}")).expect("parse target");
                let key = percent_decode_path(parsed.path())
                    .trim_start_matches('/')
                    .trim_start_matches("bucket/")
                    .to_string();
                let query: HashMap<String, String> = parsed.query_pairs().into_owned().collect();
                let content_len = headers
                    .lines()
                    .skip(1)
                    .find_map(|line| {
                        let (k, v) = line.split_once(':')?;
                        k.trim()
                            .eq_ignore_ascii_case("content-length")
                            .then(|| v.trim().parse::<usize>().ok())?
                    })
                    .unwrap_or(0);
                if header_end + content_len > request.len() {
                    request.reserve(header_end + content_len - request.len());
                }
                while request.len() < header_end + content_len {
                    let count = socket.read(&mut chunk).await.unwrap_or(0);
                    if count == 0 {
                        return;
                    }
                    request.extend_from_slice(&chunk[..count]);
                }
                let body_slice = &request[header_end..header_end + content_len];

                let (status, extra_headers, resp_body) = if method == "POST"
                    && query.contains_key("uploads")
                {
                    let upload_id = format!("up-{}", seq.fetch_add(1, Ordering::SeqCst));
                    uploads
                        .lock()
                        .expect("uploads lock")
                        .insert((key.clone(), upload_id.clone()), BTreeMap::new());
                    let xml = format!(
                        "<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>{key}</Key><UploadId>{upload_id}</UploadId></InitiateMultipartUploadResult>"
                    );
                    ("200 OK", String::new(), xml.into_bytes())
                } else if method == "PUT"
                    && let (Some(part_raw), Some(upload_id)) =
                        (query.get("partNumber"), query.get("uploadId"))
                {
                    let part_no: usize = part_raw.parse().expect("partNumber");
                    uploads
                        .lock()
                        .expect("uploads lock")
                        .entry((key.clone(), upload_id.clone()))
                        .or_default()
                        .insert(part_no, body_slice.to_vec());
                    (
                        "200 OK",
                        format!("ETag: \"part-{part_no}\"\r\n"),
                        Vec::new(),
                    )
                } else if method == "POST"
                    && let Some(upload_id) = query.get("uploadId")
                {
                    let parts_map = uploads
                        .lock()
                        .expect("uploads lock")
                        .remove(&(key.clone(), upload_id.clone()))
                        .unwrap_or_default();
                    let mut combined = Vec::new();
                    for part_bytes in parts_map.into_values() {
                        combined.extend_from_slice(&part_bytes);
                    }
                    objects
                        .lock()
                        .expect("objects lock")
                        .insert(key.clone(), combined);
                    completed
                        .lock()
                        .expect("completed lock")
                        .push((key.clone(), upload_id.clone()));
                    let xml = format!(
                        "<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>{key}</Key><ETag>\"mp-complete\"</ETag></CompleteMultipartUploadResult>"
                    );
                    ("200 OK", String::new(), xml.into_bytes())
                } else if method == "DELETE"
                    && let Some(upload_id) = query.get("uploadId")
                {
                    uploads
                        .lock()
                        .expect("uploads lock")
                        .remove(&(key.clone(), upload_id.clone()));
                    aborted
                        .lock()
                        .expect("aborted lock")
                        .push((key, upload_id.clone()));
                    ("204 No Content", String::new(), Vec::new())
                } else if method == "PUT" {
                    put_requests.fetch_add(1, Ordering::SeqCst);
                    objects
                        .lock()
                        .expect("objects lock")
                        .insert(key, body_slice.to_vec());
                    (
                        "200 OK",
                        "ETag: \"single-etag\"\r\n".to_string(),
                        Vec::new(),
                    )
                } else if method == "GET" || method == "HEAD" {
                    match objects.lock().expect("objects lock").get(&key).cloned() {
                        Some(data) => (
                            "200 OK",
                            "ETag: \"stored-etag\"\r\nLast-Modified: Thu, 12 Nov 2026 00:00:00 GMT\r\n"
                                .to_string(),
                            data,
                        ),
                        None => (
                            "404 Not Found",
                            String::new(),
                            b"<Error><Code>NoSuchKey</Code></Error>".to_vec(),
                        ),
                    }
                } else if method == "DELETE" {
                    objects.lock().expect("objects lock").remove(&key);
                    ("204 No Content", String::new(), Vec::new())
                } else {
                    ("400 Bad Request", String::new(), Vec::new())
                };

                let head = format!(
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\n{extra_headers}Connection: close\r\n\r\n",
                    resp_body.len()
                );
                let _ = socket.write_all(head.as_bytes()).await;
                if method != "HEAD" && !resp_body.is_empty() {
                    let _ = socket.write_all(&resp_body).await;
                }
            });
        }
    });

    MockS3MultipartServer {
        endpoint,
        objects,
        aborted_uploads,
        completed_uploads,
        put_requests,
        server,
    }
}

fn s3_engine_for_mock(endpoint: &str) -> ObjectStoreEngine {
    let opts = HashMap::from([
        ("endpoint".to_string(), endpoint.to_string()),
        ("allow_http".to_string(), "true".to_string()),
        ("access_key_id".to_string(), "test-ak".to_string()),
        ("secret_access_key".to_string(), "test-sk".to_string()),
        ("region".to_string(), "us-east-1".to_string()),
    ]);
    ObjectStoreEngine::from_url("s3://bucket", Some(&opts)).expect("mock s3 engine")
}

#[tokio::test]
async fn test_write_receipts_single_shot_empty_file_and_s3_multipart() {
    use _storage::store::PutConditionalOptions;

    let (vec_sha, vec_crc, vec_md5) = ref_digests(b"123456789");
    assert_eq!(vec_crc, "e3069283");
    assert_eq!(
        vec_sha,
        "15e2b0d3c33891ebb0f1ef609ec419420c20e320ce94c65fbc8c3312448eb225"
    );
    assert_eq!(vec_md5, "25f9e794323b453885f5181f1b624d0b");

    let (empty_sha, empty_crc, empty_md5) = ref_digests(b"");
    assert_eq!(empty_crc, "00000000");
    assert_eq!(
        empty_sha,
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
    assert_eq!(empty_md5, "d41d8cd98f00b204e9800998ecf8427e");

    let tmp = TempDir::new().expect("tempdir");
    let local_url = format!("file://{}", tmp.path().join("local-store").display());
    let engines = [
        ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine"),
        ObjectStoreEngine::from_url(&local_url, None).expect("local engine"),
    ];

    for engine in &engines {
        let default_meta = engine
            .put_opts(
                "receipts/default.bin",
                Bytes::from_static(b"123456789"),
                PutConditionalOptions {
                    attributes: HashMap::from([
                        ("sha256".to_string(), "0".repeat(64)),
                        ("x-amz-checksum-crc32c".to_string(), "00000000".to_string()),
                        ("content-md5".to_string(), "0".repeat(32)),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .expect("default put");
        assert_eq!(default_meta.sha256, None);
        assert_eq!(default_meta.crc32c, None);
        assert_eq!(default_meta.md5, None);
        let head_default = engine.head("receipts/default.bin").await.expect("head");
        assert_eq!(head_default.sha256, None);
        assert_eq!(head_default.crc32c, None);
        assert_eq!(head_default.md5, None);
        assert_eq!(
            head_default.attributes.get("sha256").map(String::as_str),
            Some("0000000000000000000000000000000000000000000000000000000000000000")
        );

        let single_meta = engine
            .put_opts(
                "receipts/vector.bin",
                Bytes::from_static(b"123456789"),
                PutConditionalOptions {
                    checksum: true,
                    ..Default::default()
                },
            )
            .await
            .expect("checksum put");
        assert_eq!(single_meta.sha256.as_deref(), Some(vec_sha.as_str()));
        assert_eq!(single_meta.crc32c.as_deref(), Some("e3069283"));
        assert_eq!(single_meta.md5.as_deref(), Some(vec_md5.as_str()));

        let empty_meta = engine
            .put_opts(
                "receipts/empty.bin",
                Bytes::new(),
                PutConditionalOptions {
                    checksum: true,
                    ..Default::default()
                },
            )
            .await
            .expect("empty checksum put");
        assert_eq!(empty_meta.size, 0);
        assert_eq!(empty_meta.sha256.as_deref(), Some(empty_sha.as_str()));
        assert_eq!(empty_meta.crc32c.as_deref(), Some("00000000"));
        assert_eq!(empty_meta.md5.as_deref(), Some(empty_md5.as_str()));

        let selective = engine
            .put_opts(
                "receipts/selective.bin",
                Bytes::from_static(b"123456789"),
                PutConditionalOptions {
                    checksum: false,
                    expected_crc32c: Some("e3069283".to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("selective expected_crc32c put");
        assert_eq!(selective.sha256, None);
        assert_eq!(selective.crc32c.as_deref(), Some("e3069283"));
        assert_eq!(selective.md5, None);
    }

    let mock_s3 = spawn_mock_s3_multipart_server().await;
    let s3 = s3_engine_for_mock(&mock_s3.endpoint);

    let small_payload = patterned_payload(65_537, 11);
    let (small_sha, small_crc, small_md5) = ref_digests(&small_payload);
    let small_file = tmp.path().join("small-upload.bin");
    std::fs::write(&small_file, &small_payload).expect("write small file");
    let small_file_meta = s3
        .put_file_opts(
            "files/small.bin",
            small_file.to_str().expect("utf8"),
            PutConditionalOptions {
                checksum: true,
                expected_sha256: Some(small_sha.clone()),
                expected_crc32c: Some(small_crc.clone()),
                expected_md5: Some(small_md5.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("put_file_opts small");
    assert_eq!(small_file_meta.sha256.as_deref(), Some(small_sha.as_str()));
    assert_eq!(small_file_meta.crc32c.as_deref(), Some(small_crc.as_str()));
    assert_eq!(small_file_meta.md5.as_deref(), Some(small_md5.as_str()));

    let chunk_sizes = [
        3 * 1024 * 1024,
        3 * 1024 * 1024,
        3 * 1024 * 1024,
        512 * 1024 + 19,
    ];
    let mut multi_payload = Vec::with_capacity(chunk_sizes.iter().sum());
    let mut chunks = Vec::with_capacity(chunk_sizes.len());
    for (idx, &len) in chunk_sizes.iter().enumerate() {
        let part = patterned_payload(len, (idx as u8) * 13 + 5);
        multi_payload.extend_from_slice(&part);
        chunks.push(Bytes::from(part));
    }
    let (multi_sha, multi_crc, multi_md5) = ref_digests(&multi_payload);

    let mut session = s3
        .create_multipart(
            "mp/large-session.bin",
            PutConditionalOptions {
                checksum: true,
                expected_sha256: Some(multi_sha.clone()),
                expected_crc32c: Some(multi_crc.clone()),
                expected_md5: Some(multi_md5.clone()),
                ..Default::default()
            },
        )
        .await
        .expect("create s3 multipart");
    for chunk in chunks {
        session.write_chunk(chunk).await.expect("write_chunk");
    }
    let mp_meta = session
        .complete_with_meta()
        .await
        .expect("complete s3 multipart with receipts");
    assert_eq!(
        session.part_sizes_flushed(),
        &[
            R2_UNIFORM_PART_SIZE,
            multi_payload.len() - R2_UNIFORM_PART_SIZE
        ]
    );
    assert_eq!(mp_meta.size, multi_payload.len() as u64);
    assert_eq!(mp_meta.sha256.as_deref(), Some(multi_sha.as_str()));
    assert_eq!(mp_meta.crc32c.as_deref(), Some(multi_crc.as_str()));
    assert_eq!(mp_meta.md5.as_deref(), Some(multi_md5.as_str()));
    assert_eq!(
        &s3.get("mp/large-session.bin").await.expect("get mp")[..],
        &multi_payload[..]
    );

    let large_file = tmp.path().join("large-upload.bin");
    std::fs::write(&large_file, &multi_payload).expect("write large file");
    let large_file_meta = s3
        .put_file_opts(
            "files/large.bin",
            large_file.to_str().expect("utf8"),
            PutConditionalOptions {
                checksum: true,
                ..Default::default()
            },
        )
        .await
        .expect("put_file_opts >8MiB");
    assert_eq!(large_file_meta.size, multi_payload.len() as u64);
    assert_eq!(large_file_meta.sha256.as_deref(), Some(multi_sha.as_str()));
    assert_eq!(large_file_meta.crc32c.as_deref(), Some(multi_crc.as_str()));
    assert_eq!(large_file_meta.md5.as_deref(), Some(multi_md5.as_str()));
    assert_eq!(
        mock_s3.completed_uploads.lock().expect("lock").len(),
        2,
        "both session multipart and >8MiB put_file_opts must complete on mock S3"
    );

    let mut empty_mp = s3
        .create_multipart(
            "mp/empty.bin",
            PutConditionalOptions {
                checksum: true,
                ..Default::default()
            },
        )
        .await
        .expect("create empty multipart");
    let empty_mp_meta = empty_mp
        .complete_with_meta()
        .await
        .expect("complete empty multipart");
    assert_eq!(empty_mp_meta.size, 0);
    assert_eq!(empty_mp_meta.sha256.as_deref(), Some(empty_sha.as_str()));
    assert_eq!(empty_mp_meta.crc32c.as_deref(), Some("00000000"));
    assert_eq!(empty_mp_meta.md5.as_deref(), Some(empty_md5.as_str()));

    mock_s3.server.abort();
}

#[tokio::test]
async fn test_expected_digest_mismatch_aborts_before_publish_and_preserves_prior_object() {
    use _storage::store::{PutConditionalOptions, StorageError};
    use std::sync::atomic::Ordering;

    let mock_s3 = spawn_mock_s3_multipart_server().await;
    let s3 = s3_engine_for_mock(&mock_s3.endpoint);

    let prior = b"prior-intact-s3-object-bytes";
    s3.put("guarded/item.bin", Bytes::from_static(prior))
        .await
        .expect("seed prior object");
    let puts_after_seed = mock_s3.put_requests.load(Ordering::SeqCst);
    assert_eq!(puts_after_seed, 1);

    let bad_sha = "0".repeat(64);
    let bad_crc = "ffffffff".to_string();
    let bad_md5 = "0".repeat(32);

    let mismatch_cases = [
        (
            "sha256",
            PutConditionalOptions {
                expected_sha256: Some(bad_sha.clone()),
                ..Default::default()
            },
        ),
        (
            "crc32c",
            PutConditionalOptions {
                expected_crc32c: Some(bad_crc.clone()),
                ..Default::default()
            },
        ),
        (
            "md5",
            PutConditionalOptions {
                expected_md5: Some(bad_md5.clone()),
                ..Default::default()
            },
        ),
    ];

    for (expected_alg, opts) in mismatch_cases.clone() {
        let err = s3
            .put_opts(
                "guarded/item.bin",
                Bytes::from_static(b"replacement-attempt"),
                opts,
            )
            .await
            .expect_err("mismatched single-shot put_opts must fail");
        assert!(err.is_integrity());
        match err {
            StorageError::Integrity {
                path, algorithm, ..
            } => {
                assert_eq!(path, "guarded/item.bin");
                assert_eq!(algorithm, expected_alg);
            }
            other => panic!("expected Integrity error, got {other:?}"),
        }
        assert_eq!(
            mock_s3.put_requests.load(Ordering::SeqCst),
            puts_after_seed,
            "single-shot digest mismatch must reject before sending HTTP PUT"
        );
        assert_eq!(
            &s3.get("guarded/item.bin").await.expect("get prior")[..],
            prior
        );
    }

    let part_one = Bytes::from(patterned_payload(5 * 1024 * 1024, 21));
    let part_two = Bytes::from(patterned_payload(4 * 1024 * 1024, 42));
    for (idx, (expected_alg, opts)) in mismatch_cases.into_iter().enumerate() {
        let target_key = if idx == 0 {
            "guarded/item.bin"
        } else {
            "guarded/uncommitted.bin"
        };
        let mut session = s3
            .create_multipart(target_key, opts)
            .await
            .expect("create multipart");
        session
            .write_chunk(part_one.clone())
            .await
            .expect("write chunk 1");
        session
            .write_chunk(part_two.clone())
            .await
            .expect("write chunk 2 flushing 8 MiB part to mock S3");
        assert_eq!(session.part_sizes_flushed(), &[R2_UNIFORM_PART_SIZE]);

        let err = session
            .complete_with_meta()
            .await
            .expect_err("mismatched multipart must fail before complete");
        match err {
            StorageError::Integrity {
                path, algorithm, ..
            } => {
                assert_eq!(path, target_key);
                assert_eq!(algorithm, expected_alg);
            }
            other => panic!("expected Integrity error, got {other:?}"),
        }
        drop(session);
        tokio::task::yield_now().await;

        assert!(
            mock_s3.completed_uploads.lock().expect("lock").is_empty(),
            "mismatched multipart upload must never send CompleteMultipartUpload"
        );
        let aborted = mock_s3.aborted_uploads.lock().expect("lock").clone();
        assert_eq!(
            aborted.len(),
            idx + 1,
            "mismatch must issue exactly one DELETE ?uploadId without double-aborting on Drop"
        );
        assert_eq!(aborted[idx].0, target_key);
        assert!(aborted[idx].1.starts_with("up-"));
    }

    assert_eq!(
        &s3.get("guarded/item.bin").await.expect("prior intact")[..],
        prior
    );
    assert!(
        !s3.exists("guarded/uncommitted.bin")
            .await
            .expect("exists check")
    );

    let tmp = TempDir::new().expect("tempdir");
    let large_src = tmp.path().join("bad-large.bin");
    std::fs::write(&large_src, patterned_payload(9 * 1024 * 1024, 77)).expect("write >8MiB file");
    let file_err = s3
        .put_file_opts(
            "guarded/item.bin",
            large_src.to_str().expect("utf8"),
            PutConditionalOptions {
                expected_sha256: Some(bad_sha),
                ..Default::default()
            },
        )
        .await
        .expect_err("mismatched >8MiB put_file_opts must abort before publish");
    assert!(file_err.is_integrity());
    assert_eq!(mock_s3.aborted_uploads.lock().expect("lock").len(), 4);
    assert!(mock_s3.completed_uploads.lock().expect("lock").is_empty());
    assert_eq!(
        mock_s3
            .objects
            .lock()
            .expect("lock")
            .get("guarded/item.bin")
            .map(Vec::as_slice),
        Some(&prior[..])
    );
    assert_eq!(
        &s3.get("guarded/item.bin").await.expect("prior intact")[..],
        prior
    );

    mock_s3.server.abort();
}

#[tokio::test]
async fn test_malformed_expected_digest_hex_rejected_before_io() {
    use _storage::store::{PutConditionalOptions, StorageError};
    use std::sync::Arc;
    use std::sync::atomic::Ordering;

    let counting = Arc::new(CountingStore::new(std::time::Duration::ZERO));
    let engine = ObjectStoreEngine::with_store(counting.clone(), "memory");

    let bad_sha_values = [
        "",
        "e3b0c442",
        &"A".repeat(64),
        &format!("{}g", "0".repeat(63)),
        &"0".repeat(65),
    ];
    let bad_crc_values = ["", "e306928", "e30692830", "E3069283", "zzzzzzzz"];
    let bad_md5_values = [
        "",
        "d41d8cd9",
        &"D".repeat(32),
        &format!("{}z", "0".repeat(31)),
    ];

    for bad in bad_sha_values {
        let opts = PutConditionalOptions {
            expected_sha256: Some(bad.to_string()),
            ..Default::default()
        };
        assert!(matches!(
            engine
                .put_opts("k/obj", Bytes::from_static(b"x"), opts.clone())
                .await,
            Err(StorageError::InvalidInput(_))
        ));
        assert!(matches!(
            engine
                .put_file_opts("k/obj", "/nonexistent/path/never/opened.bin", opts.clone())
                .await,
            Err(StorageError::InvalidInput(_))
        ));
        assert!(matches!(
            engine.create_multipart("k/obj", opts).await,
            Err(StorageError::InvalidInput(_))
        ));
        assert!(matches!(
            engine.get_verified("k/obj", bad).await,
            Err(StorageError::InvalidInput(_))
        ));
        assert!(matches!(
            engine
                .get_file_verified("k/obj", "/tmp/never-written.bin", bad)
                .await,
            Err(StorageError::InvalidInput(_))
        ));
        assert!(matches!(
            engine.get_stream_verified("k/obj", bad).await,
            Err(StorageError::InvalidInput(_))
        ));
    }

    for bad in bad_crc_values {
        let opts = PutConditionalOptions {
            expected_crc32c: Some(bad.to_string()),
            ..Default::default()
        };
        assert!(matches!(
            engine
                .put_opts("k/obj", Bytes::from_static(b"x"), opts)
                .await,
            Err(StorageError::InvalidInput(_))
        ));
    }

    for bad in bad_md5_values {
        let opts = PutConditionalOptions {
            expected_md5: Some(bad.to_string()),
            ..Default::default()
        };
        assert!(matches!(
            engine
                .put_opts("k/obj", Bytes::from_static(b"x"), opts)
                .await,
            Err(StorageError::InvalidInput(_))
        ));
    }

    assert_eq!(counting.put_calls.load(Ordering::SeqCst), 0);
    assert_eq!(counting.multipart_calls.load(Ordering::SeqCst), 0);
    assert_eq!(counting.get_ok.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn test_verified_reads_get_get_file_and_verifying_stream() {
    use _storage::store::StorageError;
    use futures::StreamExt;

    let tmp = TempDir::new().expect("tempdir");
    let local_root = tmp.path().join("store");
    let local_url = format!("file://{}", local_root.display());
    let mem_engine = ObjectStoreEngine::new(BackendConfig::Memory).expect("memory engine");
    let local_engine = ObjectStoreEngine::from_url(&local_url, None).expect("local engine");

    let payload = patterned_payload(3 * 65536 + 37, 19);
    let (good_sha, _, _) = ref_digests(&payload);
    let wrong_sha = "f".repeat(64);

    for (idx, engine) in [&mem_engine, &local_engine].into_iter().enumerate() {
        engine
            .put("verified/data.bin", Bytes::from(payload.clone()))
            .await
            .expect("seed verified/data.bin");

        let fetched = engine
            .get_verified("verified/data.bin", &good_sha)
            .await
            .expect("get_verified pass");
        assert_eq!(&fetched[..], &payload[..]);

        let bad_get = engine
            .get_verified("verified/data.bin", &wrong_sha)
            .await
            .expect_err("get_verified mismatch");
        match bad_get {
            StorageError::Integrity {
                path,
                algorithm,
                expected,
                actual,
            } => {
                assert_eq!(path, "verified/data.bin");
                assert_eq!(algorithm, "sha256");
                assert_eq!(expected, wrong_sha);
                assert_eq!(actual, good_sha);
            }
            other => panic!("expected Integrity error, got {other:?}"),
        }

        let dl_dir = tmp.path().join(format!("verified-dl-{idx}"));
        let good_dest = dl_dir.join("good.bin");
        let file_meta = engine
            .get_file_verified(
                "verified/data.bin",
                good_dest.to_str().expect("utf8"),
                &good_sha,
            )
            .await
            .expect("get_file_verified pass");
        assert_eq!(file_meta.sha256.as_deref(), Some(good_sha.as_str()));
        assert_eq!(std::fs::read(&good_dest).expect("read good_dest"), payload);
        assert!(leftover_part_files(&dl_dir).is_empty());

        let preexisting = dl_dir.join("existing.bin");
        std::fs::write(&preexisting, b"pre-existing-destination-bytes")
            .expect("seed pre-existing target");
        let bad_file_existing = engine
            .get_file_verified(
                "verified/data.bin",
                preexisting.to_str().expect("utf8"),
                &wrong_sha,
            )
            .await
            .expect_err("get_file_verified mismatch over pre-existing file");
        assert!(bad_file_existing.is_integrity());
        assert_eq!(
            std::fs::read(&preexisting).expect("preexisting intact"),
            b"pre-existing-destination-bytes"
        );
        assert!(leftover_part_files(&dl_dir).is_empty());

        let fresh_bad_dest = dl_dir.join("fresh-bad.bin");
        let bad_file_fresh = engine
            .get_file_verified(
                "verified/data.bin",
                fresh_bad_dest.to_str().expect("utf8"),
                &wrong_sha,
            )
            .await
            .expect_err("get_file_verified mismatch on fresh target");
        assert!(bad_file_fresh.is_integrity());
        assert!(!fresh_bad_dest.exists());
        assert!(leftover_part_files(&dl_dir).is_empty());

        let mut good_stream = engine
            .get_stream_verified("verified/data.bin", &good_sha)
            .await
            .expect("open good get_stream_verified");
        let mut streamed = Vec::with_capacity(payload.len());
        while let Some(item) = good_stream.next().await {
            streamed.extend_from_slice(&item.expect("good stream chunk"));
        }
        assert_eq!(streamed, payload);

        let mut bad_stream = engine
            .get_stream_verified("verified/data.bin", &wrong_sha)
            .await
            .expect("open mismatched get_stream_verified");
        let mut bad_collected = Vec::new();
        let mut terminal_err = None;
        while let Some(item) = bad_stream.next().await {
            match item {
                Ok(chunk) => bad_collected.extend_from_slice(&chunk),
                Err(err) => {
                    assert!(terminal_err.is_none());
                    terminal_err = Some(err);
                }
            }
        }
        assert_eq!(bad_collected, payload);
        match terminal_err.expect("final stream poll must yield StorageError::Integrity") {
            StorageError::Integrity {
                path,
                algorithm,
                expected,
                actual,
            } => {
                assert_eq!(path, "verified/data.bin");
                assert_eq!(algorithm, "sha256");
                assert_eq!(expected, wrong_sha);
                assert_eq!(actual, good_sha);
            }
            other => panic!("expected Integrity error on stream EOF, got {other:?}"),
        }
        assert!(bad_stream.next().await.is_none());

        let mut early_drop_stream = engine
            .get_stream_verified("verified/data.bin", &wrong_sha)
            .await
            .expect("open early-drop get_stream_verified");
        let first_chunk = early_drop_stream
            .next()
            .await
            .expect("at least one chunk")
            .expect("first chunk before EOF must not fail");
        assert!(!first_chunk.is_empty());
        drop(early_drop_stream);
    }
}

#[tokio::test]
async fn test_atomic_rename_support_and_atomic_only_guard() {
    use _storage::store::{ObjectStoreEngine, StorageError};
    use std::sync::atomic::Ordering;

    let tmp = TempDir::new().expect("tempdir");
    let mem_cfg = BackendConfig::Memory;
    let local_cfg = BackendConfig::Local {
        root: tmp.path().to_string_lossy().into_owned(),
    };
    let oci_cfg = BackendConfig::Oci {
        bucket: "b".to_string(),
        namespace: "ns".to_string(),
        region: "us-ashburn-1".to_string(),
        endpoint: Some("http://127.0.0.1:9".to_string()),
        credentials: None,
    };
    let s3_cfg = BackendConfig::S3 {
        bucket: "b".to_string(),
        region: Some("us-east-1".to_string()),
        access_key_id: Some("ak".to_string()),
        secret_access_key: Some("sk".to_string()),
        session_token: None,
        virtual_hosted_style: false,
        endpoint: Some("http://127.0.0.1:9".to_string()),
        proxy_url: None,
        allow_http: true,
    };
    let r2_cfg = BackendConfig::R2 {
        bucket: "b".to_string(),
        account_id: "acct".to_string(),
        access_key_id: Some("ak".to_string()),
        secret_access_key: Some("sk".to_string()),
        jurisdiction: None,
        endpoint: Some("http://127.0.0.1:9".to_string()),
        allow_http: true,
    };
    let gcs_cfg = BackendConfig::Gcs {
        bucket: "b".to_string(),
        service_account_path: None,
        service_account_key: None,
        application_credentials_path: None,
        access_token: Some("tok".to_string()),
        credential_cache: None,
        quota_project_id: None,
        endpoint: Some("http://127.0.0.1:9".to_string()),
        allow_http: true,
        skip_signature: true,
        allow_ambient_adc: false,
    };
    let azure_cfg = BackendConfig::Azure {
        container: "c".to_string(),
        account: Some("acct".to_string()),
        access_key: Some("Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==".to_string()),
        sas_token: None,
        tenant_id: None,
        client_id: None,
        client_secret: None,
        endpoint: Some("http://127.0.0.1:9/acct".to_string()),
        allow_http: true,
    };
    let http_cfg = BackendConfig::Http {
        url: "http://127.0.0.1:9".to_string(),
        allow_http: true,
        headers: HashMap::new(),
    };

    assert!(mem_cfg.supports_atomic_rename());
    assert!(local_cfg.supports_atomic_rename());
    assert!(oci_cfg.supports_atomic_rename());
    assert!(!s3_cfg.supports_atomic_rename());
    assert!(!r2_cfg.supports_atomic_rename());
    assert!(!gcs_cfg.supports_atomic_rename());
    assert!(!azure_cfg.supports_atomic_rename());
    assert!(!http_cfg.supports_atomic_rename());

    let mem_engine = ObjectStoreEngine::new(mem_cfg).expect("mem engine");
    let local_engine = ObjectStoreEngine::new(local_cfg).expect("local engine");
    let oci_engine = ObjectStoreEngine::new(oci_cfg).expect("oci engine");
    assert!(mem_engine.supports_atomic_rename());
    assert!(local_engine.supports_atomic_rename());
    assert!(oci_engine.supports_atomic_rename());

    for cfg in [s3_cfg, r2_cfg, gcs_cfg, azure_cfg, http_cfg] {
        let eng = ObjectStoreEngine::new(cfg).expect("non-atomic engine");
        assert!(!eng.supports_atomic_rename());
        let err = eng
            .rename_with_options("a.txt", "b.txt", false, true)
            .await
            .expect_err("atomic_only=true must fail loudly on non-atomic backend");
        assert!(matches!(err, StorageError::InvalidInput(_)));
    }

    for scheme in ["s3", "gs", "az", "r2", "http"] {
        let inner = std::sync::Arc::new(CountingStore::new(std::time::Duration::ZERO));
        let engine = ObjectStoreEngine::with_store(inner.clone(), scheme);
        assert!(!engine.supports_atomic_rename());
        engine
            .put("src.txt", Bytes::from_static(b"payload"))
            .await
            .expect("seed src");
        let err = engine
            .rename_with_options("src.txt", "dst.txt", false, true)
            .await
            .expect_err("atomic_only must reject non-atomic store");
        assert!(matches!(err, StorageError::InvalidInput(_)));
        assert_eq!(inner.put_calls.load(Ordering::SeqCst), 1);
        assert!(engine.exists("src.txt").await.expect("src remains"));
        assert!(!engine.exists("dst.txt").await.expect("dst untouched"));
    }

    mem_engine
        .put("one.txt", Bytes::from_static(b"v1"))
        .await
        .expect("put one");
    mem_engine
        .rename_with_options("one.txt", "two.txt", true, true)
        .await
        .expect("atomic_only succeeds on memory");
    assert_eq!(
        &mem_engine.get("two.txt").await.expect("get two")[..],
        b"v1"
    );
}

#[tokio::test]
async fn test_get_file_sync_durability_and_cleanup_on_stream_error() {
    #[derive(Debug)]
    struct FailingGetStore {
        inner: object_store::memory::InMemory,
    }

    impl std::fmt::Display for FailingGetStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("FailingGetStore")
        }
    }

    #[async_trait::async_trait]
    impl object_store::ObjectStore for FailingGetStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            opts: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            opts: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            _options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            let stream: futures::stream::BoxStream<'static, object_store::Result<Bytes>> =
                Box::pin(futures::stream::iter(vec![
                    Ok(Bytes::from_static(b"partial-chunk")),
                    Err(object_store::Error::Generic {
                        store: "FailingGetStore",
                        source: "simulated mid-stream failure".into(),
                    }),
                ]));
            let meta = object_store::ObjectMeta {
                location: location.clone(),
                last_modified: chrono::Utc::now(),
                size: 25,
                e_tag: None,
                version: None,
            };
            Ok(object_store::GetResult {
                payload: object_store::GetResultPayload::Stream(stream),
                meta,
                range: 0..25,
                attributes: Default::default(),
            })
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    let tmp = TempDir::new().expect("tempdir");
    let local_root = tmp.path().join("store");
    let local_url = format!("file://{}", local_root.display());
    let local_engine = ObjectStoreEngine::from_url(&local_url, None).expect("local engine");
    local_engine
        .put("durable/item.bin", Bytes::from_static(b"fsynced-payload"))
        .await
        .expect("put durable");

    let dest_dir = tmp.path().join("downloads/nested");
    let dest_file = dest_dir.join("item.bin");
    local_engine
        .get_file("durable/item.bin", dest_file.to_str().expect("utf8"))
        .await
        .expect("get_file succeeds with sync_all");
    assert_eq!(
        std::fs::read(&dest_file).expect("read synced target"),
        b"fsynced-payload"
    );
    assert!(leftover_part_files(&dest_dir).is_empty());

    let failing_engine = ObjectStoreEngine::with_store(
        std::sync::Arc::new(FailingGetStore {
            inner: object_store::memory::InMemory::new(),
        }),
        "memory",
    );
    let fail_dir = tmp.path().join("fail-downloads");
    let fail_target = fail_dir.join("broken.bin");
    let err = failing_engine
        .get_file("broken.bin", fail_target.to_str().expect("utf8"))
        .await
        .expect_err("mid-stream error must propagate");
    assert!(matches!(err, _storage::store::StorageError::ObjectStore(_)));
    assert!(!fail_target.exists());
    assert!(leftover_part_files(&fail_dir).is_empty());
}

#[test]
fn test_verify_gcs_azure_and_oci_presigned_urls() {
    use _storage::oci::{OciParConfig, build_oci_par_url, verify_oci_par_url};
    use _storage::signing::{
        GcsVerifyConfig, SigningError, sign_gcs_v4_hmac_url, sign_gcs_v4_url, verify_azure_sas_url,
        verify_gcs_v4_url,
    };

    let now = 1_700_000_000_i64;
    let rsa_sign_cfg = GcsSignConfig {
        bucket: "ml-artifacts".to_string(),
        service_account_email: "signer@project.iam.gserviceaccount.com".to_string(),
        private_key_pem: Some(TEST_OCI_RSA_PEM.to_string()),
        access_token: None,
        endpoint: None,
    };
    let gcs_rsa_url = sign_gcs_v4_url(&rsa_sign_cfg, "GET", "models/weights.bin", 600, now)
        .expect("sign gcs rsa");
    let gcs_rsa_verify_cfg = GcsVerifyConfig {
        bucket: "ml-artifacts".to_string(),
        service_account_email: "signer@project.iam.gserviceaccount.com".to_string(),
        verify_key_pem: Some(TEST_OCI_RSA_PEM.to_string()),
        access_key_id: None,
        secret_access_key: None,
        endpoint: None,
    };
    assert!(
        verify_gcs_v4_url(&gcs_rsa_verify_cfg, "GET", &gcs_rsa_url, now + 60).expect("valid rsa")
    );
    assert!(
        !verify_gcs_v4_url(&gcs_rsa_verify_cfg, "PUT", &gcs_rsa_url, now + 60)
            .expect("wrong method")
    );
    assert!(
        !verify_gcs_v4_url(&gcs_rsa_verify_cfg, "GET", &gcs_rsa_url, now + 700).expect("expired")
    );
    assert!(
        !verify_gcs_v4_url(
            &gcs_rsa_verify_cfg,
            "GET",
            &gcs_rsa_url.replace(
                "X-Goog-SignedHeaders=host",
                "X-Goog-SignedHeaders=host%3Bx-evil"
            ),
            now + 60
        )
        .expect("tampered signed headers")
    );
    assert!(
        !verify_gcs_v4_url(
            &gcs_rsa_verify_cfg,
            "GET",
            &gcs_rsa_url.replace("storage.googleapis.com", "evil.example.com"),
            now + 60
        )
        .expect("altered host")
    );
    assert!(
        !verify_gcs_v4_url(&gcs_rsa_verify_cfg, "GET", "not a url", now + 60).expect("malformed")
    );

    let gcs_hmac_url = sign_gcs_v4_hmac_url(
        "ml-artifacts",
        "GOOG1EHMACKEY",
        "super-secret-gcs-hmac-key",
        "PUT",
        "datasets/part 0.parquet",
        600,
        now,
        None,
    )
    .expect("sign gcs hmac");
    let gcs_hmac_verify_cfg = GcsVerifyConfig {
        bucket: "ml-artifacts".to_string(),
        service_account_email: String::new(),
        verify_key_pem: None,
        access_key_id: Some("GOOG1EHMACKEY".to_string()),
        secret_access_key: Some("super-secret-gcs-hmac-key".to_string()),
        endpoint: None,
    };
    assert!(
        verify_gcs_v4_url(&gcs_hmac_verify_cfg, "PUT", &gcs_hmac_url, now + 10)
            .expect("valid hmac")
    );
    assert!(
        !verify_gcs_v4_url(&gcs_hmac_verify_cfg, "GET", &gcs_hmac_url, now + 10)
            .expect("wrong method hmac")
    );
    assert!(
        !verify_gcs_v4_url(&gcs_hmac_verify_cfg, "PUT", &gcs_hmac_url, now + 700)
            .expect("expired hmac")
    );

    let missing_gcs_secret = GcsVerifyConfig {
        bucket: "ml-artifacts".to_string(),
        service_account_email: "signer@project.iam.gserviceaccount.com".to_string(),
        verify_key_pem: None,
        access_key_id: None,
        secret_access_key: None,
        endpoint: None,
    };
    assert!(matches!(
        verify_gcs_v4_url(&missing_gcs_secret, "GET", &gcs_rsa_url, now),
        Err(SigningError::InvalidKey(_))
    ));

    let sas_cfg = AzureSasConfig {
        account: "acct1".to_string(),
        container: "lake".to_string(),
        access_key_b64:
            "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
                .to_string(),
        endpoint: None,
    };
    let sas_get =
        sign_azure_sas_url(&sas_cfg, "GET", "dir/file.parquet", 600, now).expect("sas get");
    assert!(verify_azure_sas_url(&sas_cfg, "GET", &sas_get, now + 30).expect("valid sas get"));
    assert!(!verify_azure_sas_url(&sas_cfg, "PUT", &sas_get, now + 30).expect("wrong sas perm"));
    assert!(!verify_azure_sas_url(&sas_cfg, "GET", &sas_get, now - 10).expect("before st"));
    assert!(!verify_azure_sas_url(&sas_cfg, "GET", &sas_get, now + 700).expect("after se"));
    assert!(
        !verify_azure_sas_url(&sas_cfg, "GET", &sas_get.replace("sp=r", "sp=rw"), now + 30)
            .expect("tampered sp")
    );
    let sas_put =
        sign_azure_sas_url(&sas_cfg, "PUT", "dir/file.parquet", 600, now).expect("sas put");
    assert!(verify_azure_sas_url(&sas_cfg, "PUT", &sas_put, now + 30).expect("valid sas put"));
    let sas_del =
        sign_azure_sas_url(&sas_cfg, "DELETE", "dir/file.parquet", 600, now).expect("sas del");
    assert!(verify_azure_sas_url(&sas_cfg, "DELETE", &sas_del, now + 30).expect("valid sas del"));

    let oci_cfg = OciParConfig {
        region: "us-ashburn-1".to_string(),
        namespace: "mytenancy".to_string(),
        bucket: "oci-warehouse".to_string(),
        par_token: "par-tok-xyz".to_string(),
        endpoint: None,
    };
    let oci_url = build_oci_par_url(&oci_cfg, "reports/q4.parquet", 600).expect("oci par url");
    assert!(
        verify_oci_par_url(
            &oci_url,
            &oci_cfg,
            "reports/q4.parquet",
            Some(now + 600),
            now + 10
        )
        .expect("valid oci")
    );
    assert!(
        !verify_oci_par_url(
            &oci_url,
            &oci_cfg,
            "reports/other.parquet",
            Some(now + 600),
            now + 10
        )
        .expect("wrong key")
    );
    assert!(
        !verify_oci_par_url(
            &oci_url,
            &oci_cfg,
            "reports/q4.parquet",
            Some(now + 600),
            now + 700
        )
        .expect("expired oci")
    );
    assert!(
        !verify_oci_par_url(
            &oci_url.replace("par-tok-xyz", "par-tok-bad"),
            &oci_cfg,
            "reports/q4.parquet",
            None,
            now
        )
        .expect("tampered token")
    );
    assert!(
        !verify_oci_par_url("not-a-url", &oci_cfg, "reports/q4.parquet", None, now)
            .expect("malformed oci")
    );
}

#[tokio::test]
async fn test_azure_sas_and_entra_credential_wiring_and_validation() {
    use _storage::store::StorageError;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind mock azure");
    let addr = listener.local_addr().expect("mock azure addr");
    let server = tokio::spawn(async move {
        let mut seen = Vec::new();
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.expect("accept");
            let mut buf = vec![0u8; 4096];
            let n = socket.read(&mut buf).await.expect("read");
            let req = String::from_utf8_lossy(&buf[..n]);
            let first_line = req.lines().next().unwrap_or_default().to_string();
            seen.push(first_line);
            let body = b"azure-sas-payload";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nx-ms-blob-type: BlockBlob\r\nETag: \"0x1\"\r\nLast-Modified: Wed, 21 Oct 2015 07:28:00 GMT\r\n\r\n",
                body.len()
            );
            socket.write_all(resp.as_bytes()).await.expect("write head");
            socket.write_all(body).await.expect("write body");
        }
        seen
    });

    for sas_input in [
        "?sv=2020-12-06&sp=r&sig=test%2Btoken%3D",
        "sv=2020-12-06&sp=r&sig=test%2Btoken%3D",
    ] {
        let mut opts = HashMap::new();
        opts.insert("account".to_string(), "devstoreaccount1".to_string());
        opts.insert(
            "endpoint".to_string(),
            format!("http://{addr}/devstoreaccount1"),
        );
        opts.insert("sas_token".to_string(), sas_input.to_string());
        let engine = ObjectStoreEngine::from_url("az://datasets", Some(&opts))
            .expect("valid azure sas store");
        let payload = engine.get("part.bin").await.expect("get via sas");
        assert_eq!(&payload[..], b"azure-sas-payload");
    }

    let request_lines = server.await.expect("mock azure server");
    for line in request_lines {
        assert!(line.contains("sv=2020-12-06"), "missing sv in {line}");
        assert!(line.contains("sp=r"), "missing sp in {line}");
        assert!(
            line.contains("sig=test%2Btoken%3D"),
            "missing encoded sig in {line}"
        );
    }

    for invalid_sas in [
        "",
        "?",
        "   ",
        "not-a-query-pair",
        "sv=",
        "=2020-12-06",
        "sv=2020-12-06&&sig=abc",
        "sv=2020-12-06&sig=has space",
    ] {
        let mut opts = HashMap::new();
        opts.insert("account".to_string(), "devstoreaccount1".to_string());
        opts.insert("sas_token".to_string(), invalid_sas.to_string());
        let err = match ObjectStoreEngine::from_url("az://datasets", Some(&opts)) {
            Ok(_) => panic!("invalid SAS '{invalid_sas}' must fail closed"),
            Err(err) => err,
        };
        assert!(
            matches!(err, StorageError::InvalidInput(_)),
            "expected InvalidInput for '{invalid_sas}', got {err:?}"
        );
    }

    let mut valid_entra = HashMap::new();
    valid_entra.insert("account".to_string(), "devstoreaccount1".to_string());
    valid_entra.insert("tenant_id".to_string(), "tenant-123".to_string());
    valid_entra.insert("client_id".to_string(), "client-456".to_string());
    valid_entra.insert(
        "client_secret".to_string(),
        "test-token-placeholder".to_string(),
    );
    ObjectStoreEngine::from_url("az://datasets", Some(&valid_entra))
        .expect("complete Azure Entra triple should construct store");

    let incomplete_entra_cases: Vec<Vec<(&str, &str)>> = vec![
        vec![("tenant_id", "tenant-123")],
        vec![("client_id", "client-456")],
        vec![("client_secret", "test-token-placeholder")],
        vec![("tenant_id", "tenant-123"), ("client_id", "client-456")],
        vec![
            ("tenant_id", "tenant-123"),
            ("client_secret", "test-token-placeholder"),
        ],
        vec![
            ("client_id", "client-456"),
            ("client_secret", "test-token-placeholder"),
        ],
        vec![
            ("tenant_id", "tenant-123"),
            ("client_id", "client-456"),
            ("client_secret", "   "),
        ],
    ];
    for pairs in incomplete_entra_cases {
        let mut opts = HashMap::new();
        opts.insert("account".to_string(), "devstoreaccount1".to_string());
        for (k, v) in &pairs {
            opts.insert((*k).to_string(), (*v).to_string());
        }
        let err = match ObjectStoreEngine::from_url("az://datasets", Some(&opts)) {
            Ok(_) => panic!("incomplete Entra options {pairs:?} must fail closed"),
            Err(err) => err,
        };
        assert!(
            matches!(err, StorageError::InvalidInput(_)),
            "expected InvalidInput for {pairs:?}, got {err:?}"
        );
    }
}

#[test]
fn test_oci_cavage_partial_credentials_fail_closed() {
    use _storage::store::StorageError;

    ObjectStoreEngine::from_url("oci://warehouse@mytenancy", None)
        .expect("unauthenticated OCI store with zero Cavage fields");

    let mut full_opts = HashMap::new();
    full_opts.insert(
        "tenancy_ocid".to_string(),
        "ocid1.tenancy.oc1..aaaa".to_string(),
    );
    full_opts.insert("user_ocid".to_string(), "ocid1.user.oc1..bbbb".to_string());
    full_opts.insert(
        "fingerprint".to_string(),
        "12:34:56:78:90:ab:cd:ef".to_string(),
    );
    full_opts.insert("private_key_pem".to_string(), TEST_OCI_RSA_PEM.to_string());
    ObjectStoreEngine::from_url("oci://warehouse@mytenancy", Some(&full_opts))
        .expect("complete 4-field OCI Cavage store");

    let partial_cases: Vec<Vec<(&str, &str)>> = vec![
        vec![("tenancy_ocid", "ocid1.tenancy.oc1..aaaa")],
        vec![
            ("tenancy_ocid", "ocid1.tenancy.oc1..aaaa"),
            ("user_ocid", "ocid1.user.oc1..bbbb"),
        ],
        vec![
            ("tenancy_ocid", "ocid1.tenancy.oc1..aaaa"),
            ("user_ocid", "ocid1.user.oc1..bbbb"),
            ("fingerprint", "12:34:56:78:90:ab:cd:ef"),
        ],
        vec![
            ("user_ocid", "ocid1.user.oc1..bbbb"),
            ("fingerprint", "12:34:56:78:90:ab:cd:ef"),
            ("private_key_pem", TEST_OCI_RSA_PEM),
        ],
        vec![
            ("tenancy_ocid", "ocid1.tenancy.oc1..aaaa"),
            ("user_ocid", "ocid1.user.oc1..bbbb"),
            ("fingerprint", "   "),
            ("private_key_pem", TEST_OCI_RSA_PEM),
        ],
    ];
    for pairs in partial_cases {
        let mut opts = HashMap::new();
        for (k, v) in &pairs {
            opts.insert((*k).to_string(), (*v).to_string());
        }
        let err = match ObjectStoreEngine::from_url("oci://warehouse@mytenancy", Some(&opts)) {
            Ok(_) => panic!("partial OCI Cavage options must fail closed"),
            Err(err) => err,
        };
        assert!(
            matches!(err, StorageError::InvalidInput(_)),
            "expected InvalidInput, got {err:?}"
        );
    }
}
