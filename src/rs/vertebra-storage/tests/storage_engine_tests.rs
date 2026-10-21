use _storage::signing::{
    GcsSignConfig, HmacSigner, S3SignConfig, sign_gcs_v4_with_iam_endpoint, sign_s3_v4_url,
    verify_s3_v4_url,
};
use _storage::store::{BackendConfig, ObjectStoreEngine};
use bytes::Bytes;
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
