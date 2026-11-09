use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use _postgres::cloud::keys::{generate_ephemeral_rsa_keypair, validate_issued_certificate_chain};
use _postgres::cloud::material::{
    Clock, CloudConnector, CloudConnectorConfig, CloudResourceTarget,
};
use _postgres::errors::PostgresError;
use rcgen::{
    BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, PublicKeyData, SignatureAlgorithm,
    date_time_ymd,
};
use rsa::RsaPublicKey;
use rsa::pkcs1::{DecodeRsaPrivateKey, DecodeRsaPublicKey, EncodeRsaPublicKey};
use rsa::traits::PublicKeyParts;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::Instant;
use vertebra_db::dialer::{AlloyDbClusterUri, AuthType, CloudSqlInstanceUri, IpType};

#[derive(Default)]
struct ManualClock {
    now_ms: AtomicU64,
}

impl ManualClock {
    fn new(initial_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(initial_ms),
        }
    }

    fn set(&self, ms: u64) {
        self.now_ms.store(ms, Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_unix_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

struct RsaPublicKeyAdapter(Vec<u8>);

impl PublicKeyData for RsaPublicKeyAdapter {
    fn der_bytes(&self) -> &[u8] {
        &self.0
    }

    fn algorithm(&self) -> &'static SignatureAlgorithm {
        &rcgen::PKCS_RSA_SHA256
    }
}

struct TestCa {
    ca_pem: String,
    ca_params: CertificateParams,
    ca_key: KeyPair,
}

impl TestCa {
    fn new() -> Self {
        let ca_key = KeyPair::generate().expect("generate CA key");
        let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("CA params");
        ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca_cert = ca_params.self_signed(&ca_key).expect("self-sign CA");
        Self {
            ca_pem: ca_cert.pem(),
            ca_params,
            ca_key,
        }
    }

    fn sign_rsa_public_key_pem(&self, public_key_pem: &str, not_after_unix_secs: i64) -> String {
        let rsa_pub =
            RsaPublicKey::from_pkcs1_pem(public_key_pem).expect("decode PKCS#1 RSA public key");
        let pkcs1_der = rsa_pub
            .to_pkcs1_der()
            .expect("encode PKCS#1 DER")
            .as_bytes()
            .to_vec();
        let adapter = RsaPublicKeyAdapter(pkcs1_der);
        let issuer = Issuer::new(
            self.ca_params.clone(),
            KeyPair::from_pem(&self.ca_key.serialize_pem()).expect("clone ca key"),
        );

        let mut leaf_params =
            CertificateParams::new(vec!["client.internal".to_string()]).expect("leaf params");
        leaf_params.not_before = date_time_ymd(2020, 1, 1);
        leaf_params.not_after = rcgen::date_time_ymd(1970, 1, 1)
            + std::time::Duration::from_secs(not_after_unix_secs as u64);
        let cert = leaf_params
            .signed_by(&adapter, &issuer)
            .expect("sign client RSA SPKI");
        cert.pem()
    }
}

async fn read_http_request(
    stream: &mut TcpStream,
) -> (String, String, HashMap<String, String>, Vec<u8>) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    let header_end = loop {
        let n = stream.read(&mut tmp).await.expect("read http request");
        if n == 0 {
            return (String::new(), String::new(), HashMap::new(), Vec::new());
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };

    let header_str = String::from_utf8_lossy(&buf[..header_end - 4]);
    let mut lines = header_str.lines();
    let req_line = lines.next().unwrap_or("");
    let mut parts = req_line.split_whitespace();
    let method = parts.next().unwrap_or("").to_string();
    let path = parts.next().unwrap_or("").to_string();

    let mut headers = HashMap::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.insert(k.trim().to_ascii_lowercase(), v.trim().to_string());
        }
    }

    let content_len: usize = headers
        .get("content-length")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_len {
        let n = stream.read(&mut tmp).await.expect("read http body");
        if n == 0 {
            break;
        }
        body.extend_from_slice(&tmp[..n]);
    }
    body.truncate(content_len);
    (method, path, headers, body)
}

async fn write_http_json(stream: &mut TcpStream, status: u16, json_body: &str) {
    let resp = format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{json_body}",
        json_body.len()
    );
    let _ = stream.write_all(resp.as_bytes()).await;
    let _ = stream.flush().await;
}

async fn spawn_http_mock<F, Fut>(handler: F) -> (SocketAddr, tokio::task::JoinHandle<()>)
where
    F: Fn(TcpStream) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let addr = listener.local_addr().expect("local_addr");
    let handler = Arc::new(handler);
    let handle = tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let h = Arc::clone(&handler);
            tokio::spawn(async move {
                h(stream).await;
            });
        }
    });
    (addr, handle)
}

#[tokio::test]
async fn issued_key_pkcs1_pem_roundtrip_and_redaction() {
    let keypair = generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
        .await
        .expect("generate RSA-2048");
    let pub_pem = keypair.public_key_pem();
    assert!(pub_pem.starts_with("-----BEGIN RSA PUBLIC KEY-----\n"));
    assert!(pub_pem.ends_with("-----END RSA PUBLIC KEY-----\n"));
    assert!(!pub_pem.contains('\r'));
    assert!(!pub_pem.contains("BEGIN CERTIFICATE"));

    let decoded_pub = RsaPublicKey::from_pkcs1_pem(pub_pem).expect("decode PKCS#1 public key");
    assert_eq!(decoded_pub.size() * 8, 2048);

    let priv_str = std::str::from_utf8(keypair.private_key_pem()).expect("utf8 priv pem");
    let decoded_priv =
        rsa::RsaPrivateKey::from_pkcs1_pem(priv_str).expect("decode PKCS#1 private key");
    assert_eq!(RsaPublicKey::from(&decoded_priv), decoded_pub);

    let dbg = format!("{keypair:?}");
    assert!(!dbg.contains("BEGIN RSA PRIVATE KEY"));
    assert!(dbg.contains("<redacted>"));
}

#[tokio::test]
async fn issued_key_valid_leaf_first_chain() {
    let ca = TestCa::new();
    let keypair = generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let leaf_pem = ca.sign_rsa_public_key_pem(keypair.public_key_pem(), 2_000_000_000);
    let chain = vec![leaf_pem.clone(), ca.ca_pem.clone()];

    let identity = validate_issued_certificate_chain(&chain, &keypair, 1_900_000_000_000)
        .expect("valid chain");
    assert_eq!(identity.signed_not_after_ms, 2_000_000_000_000);
    let chain_str = String::from_utf8(identity.cert_chain_pem.clone()).unwrap();
    assert!(chain_str.starts_with(leaf_pem.trim()));
    assert!(chain_str.trim_end().ends_with(ca.ca_pem.trim()));
    assert!(!format!("{identity:?}").contains("BEGIN RSA PRIVATE KEY"));
}

#[tokio::test]
async fn issued_key_mismatched_spki_or_reversed_chain() {
    let ca = TestCa::new();
    let kp1 = generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let kp2 = generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let leaf_kp1 = ca.sign_rsa_public_key_pem(kp1.public_key_pem(), 2_000_000_000);
    let leaf_kp2 = ca.sign_rsa_public_key_pem(kp2.public_key_pem(), 2_000_000_000);

    // (a) Leaf signed for a different RSA-2048 keypair
    assert!(matches!(
        validate_issued_certificate_chain(&[leaf_kp2], &kp1, 1_900_000_000_000),
        Err(PostgresError::Protocol(_))
    ));

    // (b) Reversed chain [intermediate_ca_pem, valid_leaf_pem]
    assert!(matches!(
        validate_issued_certificate_chain(&[ca.ca_pem.clone(), leaf_kp1], &kp1, 1_900_000_000_000),
        Err(PostgresError::Protocol(_))
    ));

    // (c) ECDSA P-256 leaf
    let ec_leaf = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
        .unwrap()
        .cert
        .pem();
    assert!(matches!(
        validate_issued_certificate_chain(&[ec_leaf], &kp1, 1_000),
        Err(PostgresError::Protocol(_))
    ));
}

#[tokio::test]
async fn issued_key_expired_and_not_after_dominates_hint() {
    let ca = TestCa::new();
    let kp = generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let leaf = ca.sign_rsa_public_key_pem(kp.public_key_pem(), 1_700_000_120);

    // At exact expiry second (1_700_000_120_000 ms), must fail terminally
    assert!(matches!(
        validate_issued_certificate_chain(std::slice::from_ref(&leaf), &kp, 1_700_000_120_000),
        Err(PostgresError::Transport(_))
    ));

    // 120s before NotAfter, signed_not_after_ms is exact NotAfter (1_700_000_120_000)
    let id = validate_issued_certificate_chain(&[leaf], &kp, 1_700_000_000_000).unwrap();
    assert_eq!(id.signed_not_after_ms, 1_700_000_120_000);
}

#[tokio::test]
async fn issued_key_malformed_pem_and_deadline_abort() {
    let kp = generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert!(validate_issued_certificate_chain(&[], &kp, 1_000).is_err());
    assert!(validate_issued_certificate_chain(&["not a pem".to_string()], &kp, 1_000).is_err());
    assert!(
        validate_issued_certificate_chain(
            &["-----BEGIN CERTIFICATE-----\n!!corrupt!!\n-----END CERTIFICATE-----".to_string()],
            &kp,
            1_000
        )
        .is_err()
    );

    let expired_deadline = Instant::now() - Duration::from_millis(1);
    assert!(matches!(
        generate_ephemeral_rsa_keypair(expired_deadline).await,
        Err(PostgresError::Transport(_))
    ));
}

#[tokio::test]
async fn material_singleflight_and_reuse() {
    let ca = Arc::new(TestCa::new());
    let settings_hits = Arc::new(AtomicUsize::new(0));
    let cert_hits = Arc::new(AtomicUsize::new(0));

    let ca_clone = Arc::clone(&ca);
    let s_hits = Arc::clone(&settings_hits);
    let c_hits = Arc::clone(&cert_hits);
    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        let s_hits = Arc::clone(&s_hits);
        let c_hits = Arc::clone(&c_hits);
        async move {
            let (method, path, _, body) = read_http_request(&mut stream).await;
            if method == "GET" && path.ends_with("/connectSettings") {
                s_hits.fetch_add(1, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(25)).await;
                let json = serde_json::json!({
                    "region": "us-central1",
                    "databaseVersion": "POSTGRES_16",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.10.0.5" }]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if method == "POST" && path.ends_with(":generateEphemeralCert") {
                c_hits.fetch_add(1, Ordering::SeqCst);
                let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = parsed["public_key"].as_str().unwrap();
                let cert_pem = ca.sign_rsa_public_key_pem(pub_key, 4_600);
                let json = serde_json::json!({
                    "ephemeralCert": { "cert": cert_pem }
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = Arc::new(CloudConnector::with_clock(clock.clone()));
    let uri = CloudSqlInstanceUri::parse("proj:us-central1:inst1").unwrap();
    let mut cloud_opts = HashMap::new();
    cloud_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Private,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(cloud_opts);

    let mut tasks = Vec::new();
    for _ in 0..8 {
        let c = Arc::clone(&connector);
        let cfg_clone = cfg.clone();
        tasks.push(tokio::spawn(async move {
            c.prepare(&cfg_clone, Instant::now() + Duration::from_secs(5))
                .await
                .expect("concurrent prepare")
        }));
    }
    let mut results = Vec::new();
    for t in tasks {
        results.push(t.await.unwrap());
    }
    for r in &results[1..] {
        assert_eq!(r, &results[0]);
    }
    assert_eq!(settings_hits.load(Ordering::SeqCst), 1);
    assert_eq!(cert_hits.load(Ordering::SeqCst), 1);

    // Follow-up at t = 4_359s (4_359_000 + 240_000 == 4_599_000 < 4_600_000) reuses cache
    clock.set(4_359_000);
    let reused = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(reused, results[0]);
    assert_eq!(settings_hits.load(Ordering::SeqCst), 1);
    server_task.abort();
}

#[tokio::test]
async fn material_refresh_lead_and_effective_expiry() {
    let ca = Arc::new(TestCa::new());
    let fetches = Arc::new(AtomicUsize::new(0));
    let ca_clone = Arc::clone(&ca);
    let f_clone = Arc::clone(&fetches);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        let f_clone = Arc::clone(&f_clone);
        async move {
            let (method, path, _, body) = read_http_request(&mut stream).await;
            if method == "GET" && path.ends_with("/connectSettings") {
                f_clone.fetch_add(1, Ordering::SeqCst);
                let json = serde_json::json!({
                    "region": "us-central1",
                    "databaseVersion": "POSTGRES_16",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.10.0.5" }]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if method == "POST" && path.ends_with(":generateEphemeralCert") {
                let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = parsed["public_key"].as_str().unwrap();
                let cert_pem = ca.sign_rsa_public_key_pem(pub_key, 4_600);
                let json = serde_json::json!({ "ephemeralCert": { "cert": cert_pem } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock.clone());
    let uri = CloudSqlInstanceUri::parse("proj:us-central1:inst1").unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let mut iam_opts = HashMap::new();
    iam_opts.insert("access_token".to_string(), "login-tok".to_string());
    iam_opts.insert("expires_at_unix_ms".to_string(), "2500000".to_string());
    iam_opts.insert("refresh_skew_ms".to_string(), "300000".to_string());
    iam_opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/sqlservice.login".to_string(),
    );

    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Private,
        AuthType::Iam,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts)
    .with_iam_auth_options(Some(iam_opts));

    let m1 = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(m1.signed_cert_not_after_ms, 4_600_000);
    assert_eq!(m1.effective_expiry_ms, 2_500_000);
    assert_eq!(fetches.load(Ordering::SeqCst), 1);

    // At t = 2_259s (2_259_000 + 240_000 = 2_499_000 < 2_500_000), cache hit
    clock.set(2_259_000);
    let _ = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(fetches.load(Ordering::SeqCst), 1);

    // At t = 2_199s (2_199_000 + 240_000 = 2_439_000, wait: 2_260_000 + 240_000 == 2_500_000, and for ensure_usable with skew 100_000)
    let mut cfg2 = cfg.clone();
    if let Some(ref mut iam) = cfg2.iam_auth_options {
        iam.insert("refresh_skew_ms".to_string(), "100000".to_string());
    }
    clock.set(1_000_000);
    let _ = connector
        .prepare(&cfg2, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let count_before = fetches.load(Ordering::SeqCst);
    clock.set(2_260_000);
    let _ = connector
        .prepare(&cfg2, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(fetches.load(Ordering::SeqCst), count_before + 1);
    server_task.abort();
}

#[tokio::test]
async fn material_failed_refresh_never_serves_stale() {
    let ca = Arc::new(TestCa::new());
    let call_num = Arc::new(AtomicUsize::new(0));
    let ca_clone = Arc::clone(&ca);
    let c_clone = Arc::clone(&call_num);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        let c_clone = Arc::clone(&c_clone);
        async move {
            let (method, path, _, body) = read_http_request(&mut stream).await;
            if method == "GET" && path.ends_with("/connectSettings") {
                let n = c_clone.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    let json = serde_json::json!({
                        "region": "us-central1",
                        "databaseVersion": "POSTGRES_16",
                        "serverCaCert": { "cert": ca.ca_pem },
                        "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.10.0.5" }]
                    });
                    write_http_json(&mut stream, 200, &json.to_string()).await;
                } else {
                    write_http_json(&mut stream, 503, r#"{"error":"unavailable"}"#).await;
                }
            } else if method == "POST" && path.ends_with(":generateEphemeralCert") {
                let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = parsed["public_key"].as_str().unwrap();
                let cert_pem = ca.sign_rsa_public_key_pem(pub_key, 2_000);
                let json = serde_json::json!({ "ephemeralCert": { "cert": cert_pem } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock.clone());
    let uri = CloudSqlInstanceUri::parse("proj:us-central1:inst1").unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Private,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts);

    let active_snapshot = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(active_snapshot.effective_expiry_ms, 2_000_000);

    // Advance inside refresh lead: 503 failure must return Err and never return stale snapshot
    clock.set(1_800_000);
    let err = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect_err("503 refresh must fail");
    assert!(matches!(err, PostgresError::Transport(_)));
    assert_eq!(active_snapshot.host, "10.10.0.5");
    server_task.abort();
}

#[tokio::test]
async fn material_cancellation_releases_lock() {
    let ca = Arc::new(TestCa::new());
    let attempt = Arc::new(AtomicUsize::new(0));
    let ca_clone = Arc::clone(&ca);
    let att_clone = Arc::clone(&attempt);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        let att_clone = Arc::clone(&att_clone);
        async move {
            let (method, path, _, body) = read_http_request(&mut stream).await;
            if method == "GET" && path.ends_with("/connectSettings") {
                let idx = att_clone.fetch_add(1, Ordering::SeqCst);
                if idx == 0 {
                    tokio::time::sleep(Duration::from_millis(300)).await;
                }
                let json = serde_json::json!({
                    "region": "us-central1",
                    "databaseVersion": "POSTGRES_16",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.10.0.5" }]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if method == "POST" && path.ends_with(":generateEphemeralCert") {
                let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = parsed["public_key"].as_str().unwrap();
                let cert_pem = ca.sign_rsa_public_key_pem(pub_key, 4_000);
                let json = serde_json::json!({ "ephemeralCert": { "cert": cert_pem } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri = CloudSqlInstanceUri::parse("proj:us-central1:inst1").unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Private,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts);

    let err = connector
        .prepare(&cfg, Instant::now() + Duration::from_millis(50))
        .await
        .expect_err("50ms deadline must time out");
    assert!(matches!(err, PostgresError::Transport(_)));

    // Subsequent call immediately acquires lock and succeeds
    let ok = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect("second call after timeout succeeds");
    assert_eq!(ok.host, "10.10.0.5");
    server_task.abort();
}

#[tokio::test]
async fn material_oversized_and_partial_response_rejected() {
    let mode = Arc::new(AtomicUsize::new(0));
    let m_clone = Arc::clone(&mode);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let m_clone = Arc::clone(&m_clone);
        async move {
            let _ = read_http_request(&mut stream).await;
            let cur = m_clone.fetch_add(1, Ordering::SeqCst);
            if cur == 0 {
                // 1 MiB + 1 byte body
                let big_len = 1_048_577usize;
                let hdr = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {big_len}\r\n\r\n"
                );
                let _ = stream.write_all(hdr.as_bytes()).await;
                let _ = stream.write_all(&vec![b' '; big_len]).await;
            } else {
                // Truncated JSON body
                let hdr = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"region\":\"us-";
                let _ = stream.write_all(hdr.as_bytes()).await;
            }
            let _ = stream.shutdown().await;
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri = CloudSqlInstanceUri::parse("proj:us-central1:inst1").unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Private,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts);

    let err1 = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect_err("oversized 1MiB+1B must fail");
    assert!(matches!(err1, PostgresError::Protocol(_)));

    let err2 = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect_err("truncated JSON must fail");
    assert!(matches!(
        err2,
        PostgresError::Protocol(_) | PostgresError::Transport(_)
    ));
    server_task.abort();
}

#[tokio::test]
async fn material_endpoint_https_and_redaction() {
    let uri = CloudSqlInstanceUri::parse("proj:us-central1:inst1").unwrap();
    let base = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Private,
        AuthType::Password,
    );

    assert!(matches!(
        base.clone().with_admin_endpoint("http://example.com"),
        Err(PostgresError::Config(_))
    ));
    assert!(matches!(
        base.clone().with_admin_endpoint("http://10.0.0.5:8080"),
        Err(PostgresError::Config(_))
    ));
    assert!(
        base.clone()
            .with_admin_endpoint("http://127.0.0.1:8080")
            .is_ok()
    );

    let mut secret_opts = HashMap::new();
    secret_opts.insert(
        "access_token".to_string(),
        "vtb-secret-admin-token".to_string(),
    );
    let cfg = base.with_cloud_auth_options(secret_opts);
    assert!(!format!("{cfg:?}").contains("vtb-secret-admin-token"));
}

#[tokio::test]
async fn cloudsql_admin_iam_discovery_and_cert() {
    let ca = Arc::new(TestCa::new());
    let ca_clone = Arc::clone(&ca);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        async move {
            let (method, path, headers, body) = read_http_request(&mut stream).await;
            assert_eq!(
                headers.get("authorization").map(String::as_str),
                Some("Bearer vtb-admin-tok")
            );
            if method == "GET" {
                assert_eq!(
                    path,
                    "/sql/v1beta4/projects/google.com%3Aproj/instances/pg1/connectSettings"
                );
                let json = serde_json::json!({
                    "region": "us-central1",
                    "databaseVersion": "POSTGRES_17",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "ipAddresses": [
                        { "type": "PRIMARY", "ipAddress": "34.1.2.3" },
                        { "type": "PRIVATE", "ipAddress": "10.20.30.40" }
                    ]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if method == "POST" {
                assert_eq!(
                    path,
                    "/sql/v1beta4/projects/google.com%3Aproj/instances/pg1:generateEphemeralCert"
                );
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["public_key"].as_str().unwrap();
                assert!(pub_key.starts_with("-----BEGIN RSA PUBLIC KEY-----\n"));
                assert_eq!(req["access_token"].as_str(), Some("vtb-login-tok"));
                let cert_pem = ca.sign_rsa_public_key_pem(pub_key, 4_600);
                let json = serde_json::json!({ "ephemeralCert": { "cert": cert_pem } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri = CloudSqlInstanceUri::parse("google.com:proj:us-central1:pg1").unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "vtb-admin-tok".to_string());
    let mut iam_opts = HashMap::new();
    iam_opts.insert("access_token".to_string(), "vtb-login-tok".to_string());
    iam_opts.insert("expires_at_unix_ms".to_string(), "1900000".to_string());
    iam_opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/sqlservice.login".to_string(),
    );

    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Private,
        AuthType::Iam,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts)
    .with_iam_auth_options(Some(iam_opts));

    let mat = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect("cloudsql iam discovery");
    assert_eq!(mat.host, "10.20.30.40");
    assert_eq!(mat.port, 3307);
    assert_eq!(mat.expected_cn.as_deref(), Some("google.com:proj:pg1"));
    assert_eq!(mat.effective_expiry_ms, 1_900_000);
    server_task.abort();
}

#[tokio::test]
async fn cloudsql_admin_password_omits_access_token() {
    let ca = Arc::new(TestCa::new());
    let ca_clone = Arc::clone(&ca);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        async move {
            let (method, _, _, body) = read_http_request(&mut stream).await;
            if method == "GET" {
                let json = serde_json::json!({
                    "region": "us-east1",
                    "databaseVersion": "POSTGRES_15",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "ipAddresses": [{ "type": "PRIMARY", "ipAddress": "35.10.20.30" }]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let obj = req.as_object().unwrap();
                assert_eq!(obj.len(), 1);
                assert!(obj.contains_key("public_key"));
                assert!(!obj.contains_key("access_token"));
                let pub_key = obj["public_key"].as_str().unwrap();
                let cert_pem = ca.sign_rsa_public_key_pem(pub_key, 3_600);
                let json = serde_json::json!({ "ephemeralCert": { "cert": cert_pem } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri = CloudSqlInstanceUri::parse("my-proj:us-east1:pg2").unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Public,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts);

    let mat = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect("cloudsql password discovery");
    assert_eq!(mat.host, "35.10.20.30");
    assert_eq!(mat.port, 3307);
    assert_eq!(mat.effective_expiry_ms, 3_600_000);
    assert!(mat.login_token.is_none());
    server_task.abort();
}

#[tokio::test]
async fn cloudsql_admin_psc_dns_selection_and_missing_ip() {
    let ca = Arc::new(TestCa::new());
    let psc_enabled = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let ca_clone = Arc::clone(&ca);
    let psc_clone = Arc::clone(&psc_enabled);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        let psc_clone = Arc::clone(&psc_clone);
        async move {
            let (method, _, _, body) = read_http_request(&mut stream).await;
            if method == "GET" {
                let psc = psc_clone.load(Ordering::SeqCst);
                let json = serde_json::json!({
                    "region": "us-central1",
                    "databaseVersion": "POSTGRES_16",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "pscEnabled": psc,
                    "dnsName": if psc { Some("abc.psc.sql.goog.") } else { None },
                    "ipAddresses": [{ "type": "PRIMARY", "ipAddress": "35.1.1.1" }]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["public_key"].as_str().unwrap();
                let cert_pem = ca.sign_rsa_public_key_pem(pub_key, 3_600);
                let json = serde_json::json!({ "ephemeralCert": { "cert": cert_pem } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri = CloudSqlInstanceUri::parse("proj:us-central1:pg1").unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());

    // (a) PSC enabled -> selects "abc.psc.sql.goog" on port 3307
    let cfg_psc = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri.clone()),
        IpType::Psc,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts.clone());

    let mat = connector
        .prepare(&cfg_psc, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(mat.host, "abc.psc.sql.goog");
    assert_eq!(mat.port, 3307);
    assert_eq!(mat.peer_dns_name.as_deref(), Some("abc.psc.sql.goog"));

    // (b) Requesting Private when only PRIMARY exists -> fails
    let cfg_priv = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri.clone()),
        IpType::Private,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts.clone());
    assert!(
        connector
            .prepare(&cfg_priv, Instant::now() + Duration::from_secs(5))
            .await
            .is_err()
    );

    // (c) PSC disabled -> fails
    psc_enabled.store(false, Ordering::SeqCst);
    let uri2 = CloudSqlInstanceUri::parse("proj:us-central1:pg-no-psc").unwrap();
    let cfg_no_psc = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri2),
        IpType::Psc,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts);
    assert!(
        connector
            .prepare(&cfg_no_psc, Instant::now() + Duration::from_secs(5))
            .await
            .is_err()
    );
    server_task.abort();
}

#[tokio::test]
async fn cloudsql_admin_region_and_engine_guards() {
    let ca = Arc::new(TestCa::new());
    let ca_clone = Arc::clone(&ca);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        async move {
            let (_, path, _, _) = read_http_request(&mut stream).await;
            if path.contains("wrong-region") {
                let json = serde_json::json!({
                    "region": "us-west1",
                    "databaseVersion": "POSTGRES_16",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.0.0.1" }]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let json = serde_json::json!({
                    "region": "us-central1",
                    "databaseVersion": "MYSQL_8_0",
                    "serverCaCert": { "cert": ca.ca_pem },
                    "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.0.0.1" }]
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());

    for inst in [
        "proj:us-central1:wrong-region",
        "proj:us-central1:mysql-inst",
    ] {
        let uri = CloudSqlInstanceUri::parse(inst).unwrap();
        let cfg = CloudConnectorConfig::new(
            CloudResourceTarget::CloudSql(uri),
            IpType::Private,
            AuthType::Password,
        )
        .with_admin_endpoint(format!("http://{addr}"))
        .unwrap()
        .with_cloud_auth_options(admin_opts.clone());

        assert!(matches!(
            connector
                .prepare(&cfg, Instant::now() + Duration::from_secs(5))
                .await,
            Err(PostgresError::Config(_))
        ));
    }
    server_task.abort();
}

#[tokio::test]
async fn cloudsql_admin_missing_ca_or_key_mismatch() {
    let ca = Arc::new(TestCa::new());
    let wrong_kp = Arc::new(
        generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap(),
    );
    let ca_clone = Arc::clone(&ca);
    let w_clone = Arc::clone(&wrong_kp);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        let w_clone = Arc::clone(&w_clone);
        async move {
            let (method, path, _, _) = read_http_request(&mut stream).await;
            if method == "GET" {
                if path.contains("missing-ca") {
                    let json = serde_json::json!({
                        "region": "us-central1",
                        "databaseVersion": "POSTGRES_16",
                        "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.0.0.1" }]
                    });
                    write_http_json(&mut stream, 200, &json.to_string()).await;
                } else {
                    let json = serde_json::json!({
                        "region": "us-central1",
                        "databaseVersion": "POSTGRES_16",
                        "serverCaCert": { "cert": ca.ca_pem },
                        "ipAddresses": [{ "type": "PRIVATE", "ipAddress": "10.0.0.1" }]
                    });
                    write_http_json(&mut stream, 200, &json.to_string()).await;
                }
            } else {
                // Sign wrong key
                let cert_pem = ca.sign_rsa_public_key_pem(w_clone.public_key_pem(), 3_600);
                let json = serde_json::json!({ "ephemeralCert": { "cert": cert_pem } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());

    for inst in ["proj:us-central1:missing-ca", "proj:us-central1:wrong-key"] {
        let uri = CloudSqlInstanceUri::parse(inst).unwrap();
        let cfg = CloudConnectorConfig::new(
            CloudResourceTarget::CloudSql(uri),
            IpType::Private,
            AuthType::Password,
        )
        .with_admin_endpoint(format!("http://{addr}"))
        .unwrap()
        .with_cloud_auth_options(admin_opts.clone());

        assert!(
            connector
                .prepare(&cfg, Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );
    }
    server_task.abort();
}

#[tokio::test]
async fn alloydb_admin_v1alpha_paths_and_json_contract() {
    let ca = Arc::new(TestCa::new());
    let ca_clone = Arc::clone(&ca);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        async move {
            let (method, path, headers, body) = read_http_request(&mut stream).await;
            assert_eq!(
                headers.get("authorization").map(String::as_str),
                Some("Bearer vtb-admin-tok")
            );
            if method == "GET" {
                assert_eq!(
                    path,
                    "/v1alpha/projects/p1/locations/us-central1/clusters/c1/instances/i1/connectionInfo"
                );
                let json = serde_json::json!({
                    "ipAddress": "10.50.0.9",
                    "instanceUid": "uid-123"
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if method == "POST" {
                assert_eq!(
                    path,
                    "/v1alpha/projects/p1/locations/us-central1/clusters/c1:generateClientCertificate"
                );
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let obj = req.as_object().unwrap();
                let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
                keys.sort_unstable();
                assert_eq!(keys, vec!["certDuration", "publicKey", "useMetadataExchange"]);
                assert_eq!(obj["certDuration"].as_str(), Some("3600s"));
                assert_eq!(obj["useMetadataExchange"].as_bool(), Some(true));
                let pub_key = obj["publicKey"].as_str().unwrap();
                let leaf_pem = ca.sign_rsa_public_key_pem(pub_key, 4_600);
                let json = serde_json::json!({
                    "pemCertificateChain": [leaf_pem, ca.ca_pem],
                    "caCert": ca.ca_pem
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri =
        AlloyDbClusterUri::parse("projects/p1/locations/us-central1/clusters/c1/instances/i1")
            .unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "vtb-admin-tok".to_string());
    let mut iam_opts = HashMap::new();
    iam_opts.insert("access_token".to_string(), "vtb-alloy-login".to_string());
    iam_opts.insert("expires_at_unix_ms".to_string(), "2200000".to_string());
    iam_opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/alloydb.login".to_string(),
    );

    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::AlloyDb(uri),
        IpType::Private,
        AuthType::Iam,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts)
    .with_iam_auth_options(Some(iam_opts));

    let mat = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect("alloydb v1alpha discovery");
    assert_eq!(mat.host, "10.50.0.9");
    assert_eq!(mat.port, 5433);
    assert_eq!(mat.auth_type, AuthType::Iam);
    assert_eq!(mat.effective_expiry_ms, 2_200_000);
    server_task.abort();
}

#[tokio::test]
async fn alloydb_admin_password_mode_still_fetches_login_token() {
    let ca = Arc::new(TestCa::new());
    let ca_clone = Arc::clone(&ca);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        async move {
            let (method, _, _, body) = read_http_request(&mut stream).await;
            if method == "GET" {
                let json = serde_json::json!({ "publicIpAddress": "34.120.1.9" });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["publicKey"].as_str().unwrap();
                let leaf_pem = ca.sign_rsa_public_key_pem(pub_key, 4_600);
                let json = serde_json::json!({
                    "pemCertificateChain": [leaf_pem],
                    "caCert": ca.ca_pem
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri =
        AlloyDbClusterUri::parse("projects/p1/locations/us-central1/clusters/c1/instances/i1")
            .unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let mut iam_opts = HashMap::new();
    iam_opts.insert("access_token".to_string(), "metadata-login-tok".to_string());
    iam_opts.insert("expires_at_unix_ms".to_string(), "2500000".to_string());
    iam_opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/alloydb.login".to_string(),
    );

    let cfg = CloudConnectorConfig::new(
        CloudResourceTarget::AlloyDb(uri),
        IpType::Public,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts)
    .with_iam_auth_options(Some(iam_opts));

    let mat = connector
        .prepare(&cfg, Instant::now() + Duration::from_secs(5))
        .await
        .expect("alloydb password mode fetches login token");
    assert_eq!(mat.host, "34.120.1.9");
    assert_eq!(mat.port, 5433);
    assert_eq!(mat.auth_type, AuthType::Password);
    assert_eq!(mat.effective_expiry_ms, 2_500_000);
    assert_eq!(
        mat.login_token.as_ref().map(|t| t.token.as_str()),
        Some("metadata-login-tok")
    );
    server_task.abort();
}

#[tokio::test]
async fn alloydb_admin_psc_dns_and_missing_network_target() {
    let ca = Arc::new(TestCa::new());
    let ca_clone = Arc::clone(&ca);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        async move {
            let (method, _, _, body) = read_http_request(&mut stream).await;
            if method == "GET" {
                let json = serde_json::json!({
                    "ipAddress": "10.0.0.8",
                    "pscDnsName": "inst.psc.alloydb.goog."
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["publicKey"].as_str().unwrap();
                let leaf_pem = ca.sign_rsa_public_key_pem(pub_key, 4_600);
                let json = serde_json::json!({
                    "pemCertificateChain": [leaf_pem],
                    "caCert": ca.ca_pem
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let uri =
        AlloyDbClusterUri::parse("projects/p1/locations/us-central1/clusters/c1/instances/i1")
            .unwrap();
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let mut iam_opts = HashMap::new();
    iam_opts.insert("access_token".to_string(), "alloy-tok".to_string());
    iam_opts.insert("expires_at_unix_ms".to_string(), "3000000".to_string());
    iam_opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/alloydb.login".to_string(),
    );

    // (a) PSC selects stripped DNS name
    let cfg_psc = CloudConnectorConfig::new(
        CloudResourceTarget::AlloyDb(uri.clone()),
        IpType::Psc,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts.clone())
    .with_iam_auth_options(Some(iam_opts.clone()));
    let mat = connector
        .prepare(&cfg_psc, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(mat.host, "inst.psc.alloydb.goog");
    assert_eq!(mat.port, 5433);
    assert_eq!(mat.peer_dns_name.as_deref(), Some("inst.psc.alloydb.goog"));

    // (b) Public fails when publicIpAddress is absent
    let cfg_pub = CloudConnectorConfig::new(
        CloudResourceTarget::AlloyDb(uri),
        IpType::Public,
        AuthType::Password,
    )
    .with_admin_endpoint(format!("http://{addr}"))
    .unwrap()
    .with_cloud_auth_options(admin_opts)
    .with_iam_auth_options(Some(iam_opts));
    assert!(
        connector
            .prepare(&cfg_pub, Instant::now() + Duration::from_secs(5))
            .await
            .is_err()
    );
    server_task.abort();
}

#[tokio::test]
async fn alloydb_admin_missing_fields_key_mismatch_and_expired_chain() {
    let ca = Arc::new(TestCa::new());
    let wrong_kp = Arc::new(
        generate_ephemeral_rsa_keypair(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap(),
    );
    let ca_clone = Arc::clone(&ca);
    let w_clone = Arc::clone(&wrong_kp);

    let (addr, server_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_clone);
        let w_clone = Arc::clone(&w_clone);
        async move {
            let (method, path, _, body) = read_http_request(&mut stream).await;
            if method == "GET" {
                let json = serde_json::json!({ "ipAddress": "10.0.0.8" });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if path.contains("empty-chain") {
                let json = serde_json::json!({ "pemCertificateChain": [], "caCert": ca.ca_pem });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if path.contains("empty-ca") {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["publicKey"].as_str().unwrap();
                let leaf = ca.sign_rsa_public_key_pem(pub_key, 4_600);
                let json = serde_json::json!({ "pemCertificateChain": [leaf], "caCert": "" });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else if path.contains("wrong-key") {
                let leaf = ca.sign_rsa_public_key_pem(w_clone.public_key_pem(), 4_600);
                let json =
                    serde_json::json!({ "pemCertificateChain": [leaf], "caCert": ca.ca_pem });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["publicKey"].as_str().unwrap();
                let leaf = ca.sign_rsa_public_key_pem(pub_key, 1_000);
                let json =
                    serde_json::json!({ "pemCertificateChain": [leaf], "caCert": ca.ca_pem });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock);
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "admin-tok".to_string());
    let mut iam_opts = HashMap::new();
    iam_opts.insert("access_token".to_string(), "alloy-tok".to_string());
    iam_opts.insert("expires_at_unix_ms".to_string(), "3000000".to_string());
    iam_opts.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/alloydb.login".to_string(),
    );

    for cluster in ["empty-chain", "empty-ca", "wrong-key", "expired-leaf"] {
        let uri = AlloyDbClusterUri::parse(&format!(
            "projects/p1/locations/us-central1/clusters/{cluster}/instances/i1"
        ))
        .unwrap();
        let cfg = CloudConnectorConfig::new(
            CloudResourceTarget::AlloyDb(uri),
            IpType::Private,
            AuthType::Password,
        )
        .with_admin_endpoint(format!("http://{addr}"))
        .unwrap()
        .with_cloud_auth_options(admin_opts.clone())
        .with_iam_auth_options(Some(iam_opts.clone()));
        assert!(
            connector
                .prepare(&cfg, Instant::now() + Duration::from_secs(5))
                .await
                .is_err()
        );
    }
    server_task.abort();
}

#[tokio::test]
async fn cloud_scoped_static_login_lifetime_and_scopes_contract() {
    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock.clone() as Arc<dyn Clock>);
    let uri = CloudSqlInstanceUri::parse("proj-a:us-central1:inst-1").unwrap();

    // Opaque static access_token in cloud_auth_options without iam_auth_options must fail for IAM
    let mut admin_opts = HashMap::new();
    admin_opts.insert("access_token".to_string(), "opaque-admin".to_string());
    let cfg_opaque = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri.clone()),
        IpType::Public,
        AuthType::Iam,
    )
    .with_admin_endpoint("http://127.0.0.1:9")
    .unwrap()
    .with_cloud_auth_options(admin_opts.clone());
    let err = connector
        .prepare(&cfg_opaque, Instant::now() + Duration::from_secs(2))
        .await
        .expect_err("opaque static token must fail IAM login");
    assert!(matches!(err, PostgresError::Auth(_)));

    // Missing or wrong scope in iam_auth_options must fail before Admin API call
    let mut bad_scope_iam = HashMap::new();
    bad_scope_iam.insert("access_token".to_string(), "login-tok".to_string());
    bad_scope_iam.insert("expires_at_unix_ms".to_string(), "2000000".to_string());
    bad_scope_iam.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/cloud-platform".to_string(),
    );
    let cfg_bad_scope = CloudConnectorConfig::new(
        CloudResourceTarget::CloudSql(uri),
        IpType::Public,
        AuthType::Iam,
    )
    .with_admin_endpoint("http://127.0.0.1:9")
    .unwrap()
    .with_cloud_auth_options(admin_opts)
    .with_iam_auth_options(Some(bad_scope_iam));
    let err_scope = connector
        .prepare(&cfg_bad_scope, Instant::now() + Duration::from_secs(2))
        .await
        .expect_err("wrong scope must fail");
    assert!(matches!(err_scope, PostgresError::Auth(_)));
}

#[tokio::test]
async fn cloudsql_mtls_and_alloydb_end_to_end_dial_keepalive_and_token_expiry_guard() {
    use _postgres::cloud::{dial_alloydb, dial_cloudsql};
    use _postgres::config::{PostgresConfig, RawPostgresOptions};
    use rcgen::{DnType, KeyUsagePurpose};
    use rustls::ServerConfig;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    use rustls::server::WebPkiClientVerifier;
    use tokio_rustls::TlsAcceptor;
    use vertebra_db::dialer::encode_alloydb_metadata_response;

    let ca = Arc::new(TestCa::new());
    let issuer = Issuer::new(
        ca.ca_params.clone(),
        KeyPair::from_pem(&ca.ca_key.serialize_pem()).unwrap(),
    );

    // Build Cloud SQL server cert with CN="proj-a:inst-1" and SAN="db.csql.internal"
    let mut csql_srv_params = CertificateParams::new(vec!["db.csql.internal".to_string()]).unwrap();
    csql_srv_params
        .distinguished_name
        .push(DnType::CommonName, "proj-a:inst-1");
    csql_srv_params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    let csql_srv_key = KeyPair::generate().unwrap();
    let csql_srv_cert = csql_srv_params.signed_by(&csql_srv_key, &issuer).unwrap();

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    let ca_certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(ca.ca_pem.as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    roots.add_parsable_certificates(ca_certs);
    let client_verifier =
        WebPkiClientVerifier::builder_with_provider(Arc::new(roots), Arc::clone(&provider))
            .build()
            .unwrap();
    let srv_certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut std::io::BufReader::new(csql_srv_cert.pem().as_bytes()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let srv_key: PrivateKeyDer<'static> = rustls_pemfile::private_key(
        &mut std::io::BufReader::new(csql_srv_key.serialize_pem().as_bytes()),
    )
    .unwrap()
    .unwrap();
    let srv_cfg = ServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_client_cert_verifier(client_verifier.clone())
        .with_single_cert(srv_certs, srv_key)
        .unwrap();
    let csql_acceptor = TlsAcceptor::from(Arc::new(srv_cfg));

    let csql_pg_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let csql_pg_addr = csql_pg_listener.local_addr().unwrap();
    let csql_pg_task = tokio::spawn(async move {
        let (sock, _) = csql_pg_listener.accept().await.unwrap();
        let mut tls = csql_acceptor.accept(sock).await.unwrap();
        tls.write_all(b"CSQL_OK").await.unwrap();
    });

    let ca_for_admin = Arc::clone(&ca);
    let (csql_admin_addr, csql_admin_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_for_admin);
        async move {
            let (method, _, _, body) = read_http_request(&mut stream).await;
            if method == "GET" {
                let json = serde_json::json!({
                    "region": "us-central1",
                    "databaseVersion": "POSTGRES_16",
                    "ipAddresses": [{"type": "PRIMARY", "ipAddress": "127.0.0.1"}],
                    "serverCaCert": {"cert": ca.ca_pem},
                    "dnsName": "db.csql.internal."
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["public_key"].as_str().unwrap();
                let leaf = ca.sign_rsa_public_key_pem(pub_key, 4_102_444_800);
                let json = serde_json::json!({ "ephemeralCert": { "cert": leaf } });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let clock = Arc::new(ManualClock::new(1_000_000));
    let connector = CloudConnector::with_clock(clock.clone() as Arc<dyn Clock>);
    let mut raw_csql = RawPostgresOptions::new("127.0.0.1", "iam-user", "appdb");
    raw_csql.cloud_sql_instance = Some("proj-a:us-central1:inst-1".to_string());
    raw_csql.cloud_sql_ip_type = "PUBLIC".to_string();
    raw_csql.enable_iam_auth = true;
    raw_csql.cloud_auth_options.insert(
        "admin_endpoint".to_string(),
        format!("http://{csql_admin_addr}"),
    );
    raw_csql
        .cloud_auth_options
        .insert("access_token".to_string(), "admin-tok".to_string());
    raw_csql
        .cloud_auth_options
        .insert("proxy_port".to_string(), csql_pg_addr.port().to_string());
    let mut csql_iam = HashMap::new();
    csql_iam.insert("access_token".to_string(), "csql-login-tok".to_string());
    csql_iam.insert("expires_at_unix_ms".to_string(), "2500000".to_string());
    csql_iam.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/sqlservice.login".to_string(),
    );
    raw_csql.iam_auth_options = csql_iam;
    let pg_cfg_csql = PostgresConfig::validate(raw_csql).unwrap();

    let mut dialed_csql = dial_cloudsql(
        &pg_cfg_csql,
        &connector,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("dial cloudsql");
    dialed_csql
        .stream
        .set_tcp_keepalive(pg_cfg_csql.tcp_keepalive())
        .expect("keepalive on cloudsql tls stream");
    let mut csql_buf = [0u8; 7];
    dialed_csql.stream.read_exact(&mut csql_buf).await.unwrap();
    assert_eq!(&csql_buf, b"CSQL_OK");
    csql_pg_task.await.unwrap();
    csql_admin_task.abort();

    // AlloyDB end-to-end dial + token expiry during TLS check
    let mut alloy_srv_params = CertificateParams::new(vec!["127.0.0.1".to_string()]).unwrap();
    alloy_srv_params
        .distinguished_name
        .push(DnType::CommonName, "alloydb-srv");
    let alloy_srv_key = KeyPair::generate().unwrap();
    let alloy_srv_cert = alloy_srv_params.signed_by(&alloy_srv_key, &issuer).unwrap();
    let alloy_srv_certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(
        &mut std::io::BufReader::new(alloy_srv_cert.pem().as_bytes()),
    )
    .collect::<Result<Vec<_>, _>>()
    .unwrap();
    let alloy_srv_pk: PrivateKeyDer<'static> = rustls_pemfile::private_key(
        &mut std::io::BufReader::new(alloy_srv_key.serialize_pem().as_bytes()),
    )
    .unwrap()
    .unwrap();
    let alloy_srv_cfg = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_client_cert_verifier(client_verifier)
        .with_single_cert(alloy_srv_certs, alloy_srv_pk)
        .unwrap();
    let alloy_acceptor = TlsAcceptor::from(Arc::new(alloy_srv_cfg));

    let alloy_pg_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let alloy_pg_addr = alloy_pg_listener.local_addr().unwrap();
    let clock_for_srv = Arc::clone(&clock);
    let alloy_pg_task = tokio::spawn(async move {
        // First connection: advance clock past token expiry during TLS handshake so metadata exchange is aborted!
        let (sock1, _) = alloy_pg_listener.accept().await.unwrap();
        clock_for_srv.set(2_600_000);
        let mut tls1 = alloy_acceptor.accept(sock1).await.unwrap();
        let mut unexpected = Vec::new();
        let n = tls1.read_to_end(&mut unexpected).await.unwrap_or(0);
        assert_eq!(
            n, 0,
            "expired token must abort before metadata exchange write"
        );

        // Second connection: valid token completes metadata exchange
        let (sock2, _) = alloy_pg_listener.accept().await.unwrap();
        let mut tls2 = alloy_acceptor.accept(sock2).await.unwrap();
        let mut hdr = [0u8; 4];
        tls2.read_exact(&mut hdr).await.unwrap();
        let len = u32::from_be_bytes(hdr) as usize;
        let mut body = vec![0u8; len];
        tls2.read_exact(&mut body).await.unwrap();
        let resp = encode_alloydb_metadata_response(1, "");
        tls2.write_all(&resp).await.unwrap();
        tls2.write_all(b"ALLOY_OK").await.unwrap();
    });

    let ca_for_alloy = Arc::clone(&ca);
    let (alloy_admin_addr, alloy_admin_task) = spawn_http_mock(move |mut stream| {
        let ca = Arc::clone(&ca_for_alloy);
        async move {
            let (method, _, _, body) = read_http_request(&mut stream).await;
            if method == "GET" {
                let json = serde_json::json!({
                    "ipAddress": "127.0.0.1",
                    "instanceUid": "uid-123"
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            } else {
                let req: serde_json::Value = serde_json::from_slice(&body).unwrap();
                let pub_key = req["publicKey"].as_str().unwrap();
                let leaf = ca.sign_rsa_public_key_pem(pub_key, 4_102_444_800);
                let json = serde_json::json!({
                    "pemCertificateChain": [leaf],
                    "caCert": ca.ca_pem
                });
                write_http_json(&mut stream, 200, &json.to_string()).await;
            }
        }
    })
    .await;

    let mut raw_alloy = RawPostgresOptions::new("127.0.0.1", "iam-user", "appdb");
    raw_alloy.alloydb_instance_uri =
        Some("projects/p1/locations/us-central1/clusters/c1/instances/i1".to_string());
    raw_alloy.alloydb_ip_type = "PRIVATE".to_string();
    raw_alloy.enable_iam_auth = true;
    raw_alloy.cloud_auth_options.insert(
        "admin_endpoint".to_string(),
        format!("http://{alloy_admin_addr}"),
    );
    raw_alloy
        .cloud_auth_options
        .insert("access_token".to_string(), "admin-tok".to_string());
    raw_alloy
        .cloud_auth_options
        .insert("proxy_port".to_string(), alloy_pg_addr.port().to_string());
    let mut alloy_iam = HashMap::new();
    alloy_iam.insert("access_token".to_string(), "alloy-login-tok".to_string());
    alloy_iam.insert("expires_at_unix_ms".to_string(), "2500000".to_string());
    alloy_iam.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/alloydb.login".to_string(),
    );
    raw_alloy.iam_auth_options = alloy_iam;
    let pg_cfg_alloy = PostgresConfig::validate(raw_alloy.clone()).unwrap();

    let err_exp = dial_alloydb(
        &pg_cfg_alloy,
        &connector,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect_err("token expiring during TLS handshake must abort before metadata exchange");
    assert!(matches!(err_exp, PostgresError::Auth(_)));

    // Reset clock and retry with valid token
    clock.set(1_000_000);
    let mut dialed_alloy = dial_alloydb(
        &pg_cfg_alloy,
        &connector,
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .expect("dial alloydb with valid token");
    dialed_alloy
        .stream
        .set_tcp_keepalive(pg_cfg_alloy.tcp_keepalive())
        .expect("keepalive on alloydb stream");
    let mut alloy_buf = [0u8; 8];
    dialed_alloy
        .stream
        .read_exact(&mut alloy_buf)
        .await
        .unwrap();
    assert_eq!(&alloy_buf, b"ALLOY_OK");
    alloy_pg_task.await.unwrap();
    alloy_admin_task.abort();
}
