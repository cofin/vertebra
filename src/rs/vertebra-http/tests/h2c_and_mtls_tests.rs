use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use vertebra_http::h2c::{
    CleartextProtocol, H2C_PREFACE, PrependStream, build_h2c_switching_protocols_response,
    detect_cleartext_protocol, parse_h2c_upgrade_headers,
};
use vertebra_http::tls::{
    ClientAuthMode, IpCidr, MtlsConfig, TrustedProxyConfig, extract_client_identity_from_der,
    normalize_trusted_proxy_headers,
};

#[tokio::test]
async fn test_detect_cleartext_protocol_prior_knowledge_and_http1() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let client_h2 = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(H2C_PREFACE).await.unwrap();
        stream
    });

    let (server_stream, _) = listener.accept().await.unwrap();
    let _client_stream = client_h2.await.unwrap();
    let proto = detect_cleartext_protocol(&server_stream).await.unwrap();
    assert_eq!(proto, CleartextProtocol::H2cPriorKnowledge);

    let client_h1 = tokio::spawn(async move {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        stream
    });

    let (server_stream_h1, _) = listener.accept().await.unwrap();
    let _client_stream_h1 = client_h1.await.unwrap();
    let proto_h1 = detect_cleartext_protocol(&server_stream_h1).await.unwrap();
    assert_eq!(proto_h1, CleartextProtocol::Http1);
}

#[test]
fn test_parse_h2c_upgrade_headers_and_switching_protocols_response() {
    let req = Request::builder()
        .method(Method::GET)
        .uri("/items")
        .header("connection", "Upgrade, HTTP2-Settings")
        .header("upgrade", "h2c")
        .header("http2-settings", "AAMAAABkAAQCAAAAAAIAAAAA")
        .body(())
        .unwrap();

    let params = parse_h2c_upgrade_headers(req.headers())
        .expect("valid h2c upgrade should parse")
        .expect("upgrade parameters should be present");
    assert_eq!(params.max_concurrent_streams, Some(100));
    assert_eq!(params.initial_window_size, Some(33_554_432));
    assert_eq!(params.enable_push, Some(false));

    let resp = build_h2c_switching_protocols_response();
    assert_eq!(resp.status(), StatusCode::SWITCHING_PROTOCOLS);
    assert_eq!(resp.headers().get("upgrade").unwrap(), "h2c");
    assert_eq!(resp.headers().get("connection").unwrap(), "Upgrade");
}

#[tokio::test]
async fn test_prepend_stream_reads_prefix_then_inner_bytes() {
    let (mut client, server) = tokio::io::duplex(256);
    client.write_all(b"WORLD").await.unwrap();
    drop(client);

    let mut prepended = PrependStream::new(Bytes::from_static(b"HELLO "), server);
    let mut out = Vec::new();
    prepended.read_to_end(&mut out).await.unwrap();
    assert_eq!(out, b"HELLO WORLD");
}

#[test]
fn test_x509_identity_extraction_from_rcgen_certificate() {
    let mtls_cfg = MtlsConfig {
        mode: ClientAuthMode::Required,
        client_ca_path: None,
    };
    assert_eq!(mtls_cfg.mode, ClientAuthMode::Required);

    let mut params = rcgen::CertificateParams::new(vec![
        "worker.internal.example".to_string(),
        "spiffe.internal.example".to_string(),
    ])
    .unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "vertebra-client-node");
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let der = cert.der().as_ref();

    let identity = extract_client_identity_from_der(der).expect("valid DER certificate");
    assert_eq!(identity.subject_cn.as_deref(), Some("vertebra-client-node"));
    assert!(
        identity
            .subject_alt_names
            .contains(&"worker.internal.example".to_string())
    );
    assert_eq!(identity.sha256_fingerprint.len(), 64);

    let mut ca_params =
        rcgen::CertificateParams::new(vec!["ca.internal.example".to_string()]).unwrap();
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "Vertebra Root CA");
    let ca_key = rcgen::KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = rcgen::Issuer::from_params(&ca_params, &ca_key);

    let mut leaf_params =
        rcgen::CertificateParams::new(vec!["leaf.internal.example".to_string()]).unwrap();
    leaf_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "spiffe-leaf-client");
    let leaf_key = rcgen::KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &issuer).unwrap();
    assert!(!ca_cert.der().as_ref().is_empty());

    let leaf_identity =
        extract_client_identity_from_der(leaf_cert.der().as_ref()).expect("valid leaf DER");
    assert_eq!(
        leaf_identity.subject_cn.as_deref(),
        Some("spiffe-leaf-client")
    );
}

#[test]
fn test_trusted_proxy_header_normalization_strips_untrusted_peers() {
    let proxy_cfg = TrustedProxyConfig {
        trusted_cidrs: vec![
            IpCidr::new(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 0)), 8),
            IpCidr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 32),
        ],
    };

    let mut headers = HeaderMap::new();
    headers.insert(
        "x-goog-iap-jwt-assertion",
        HeaderValue::from_static("eyJhbGciOiJFUzI1NiJ9.payload.sig"),
    );
    headers.insert(
        "x-forwarded-client-cert",
        HeaderValue::from_static(
            "By=spiffe://mesh/ns/default/sa/api;Hash=abc123def;Subject=\"CN=mesh-caller\";URI=spiffe://mesh/ns/default/sa/caller",
        ),
    );
    headers.insert(
        "authorization",
        HeaderValue::from_static("Bearer token-xyz-987"),
    );

    let trusted_peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 24, 1, 9)), 44321);
    let auth_from_trusted = normalize_trusted_proxy_headers(&mut headers, trusted_peer, &proxy_cfg);
    assert_eq!(
        auth_from_trusted.iap_jwt_assertion.as_deref(),
        Some("eyJhbGciOiJFUzI1NiJ9.payload.sig")
    );
    assert_eq!(
        auth_from_trusted.xfcc_subject.as_deref(),
        Some("CN=mesh-caller")
    );
    assert_eq!(
        auth_from_trusted.xfcc_uri.as_deref(),
        Some("spiffe://mesh/ns/default/sa/caller")
    );
    assert_eq!(
        auth_from_trusted.bearer_token.as_deref(),
        Some("token-xyz-987")
    );

    let untrusted_peer = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(203, 0, 113, 5)), 51234);
    let auth_from_untrusted =
        normalize_trusted_proxy_headers(&mut headers, untrusted_peer, &proxy_cfg);
    assert!(auth_from_untrusted.iap_jwt_assertion.is_none());
    assert!(auth_from_untrusted.xfcc_subject.is_none());
    assert!(!headers.contains_key("x-goog-iap-jwt-assertion"));
    assert!(!headers.contains_key("x-forwarded-client-cert"));
}
