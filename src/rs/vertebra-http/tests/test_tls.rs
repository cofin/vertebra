//! TLS integration tests using self-signed certificates.
//!
//! Requires the `tls` feature to compile.

#![cfg(feature = "tls")]

use std::io::Write;

use tempfile::NamedTempFile;

use vertebra_http::config::TlsConfig;
use vertebra_http::tls::TlsAcceptor;

fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn generate_self_signed() -> (NamedTempFile, NamedTempFile, rcgen::CertifiedKey) {
    install_crypto_provider();
    let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();

    let mut cert_file = NamedTempFile::new().unwrap();
    cert_file
        .write_all(certified_key.cert.pem().as_bytes())
        .unwrap();
    cert_file.flush().unwrap();

    let mut key_file = NamedTempFile::new().unwrap();
    key_file
        .write_all(certified_key.key_pair.serialize_pem().as_bytes())
        .unwrap();
    key_file.flush().unwrap();

    (cert_file, key_file, certified_key)
}

#[test]
fn tls_acceptor_from_valid_config() {
    let (cert_file, key_file, _) = generate_self_signed();

    let config = TlsConfig {
        cert_path: cert_file.path().to_path_buf(),
        key_path: key_file.path().to_path_buf(),
    };

    let acceptor = TlsAcceptor::from_config(&config);
    assert!(acceptor.is_ok(), "TlsAcceptor should build from valid PEM files");
}

#[test]
fn tls_acceptor_missing_cert_file() {
    let (_, key_file, _) = generate_self_signed();

    let config = TlsConfig {
        cert_path: "/nonexistent/cert.pem".into(),
        key_path: key_file.path().to_path_buf(),
    };

    assert!(TlsAcceptor::from_config(&config).is_err());
}

#[test]
fn tls_acceptor_invalid_pem() {
    let mut cert_file = NamedTempFile::new().unwrap();
    cert_file.write_all(b"not a PEM file").unwrap();
    cert_file.flush().unwrap();

    let (_, key_file, _) = generate_self_signed();

    let config = TlsConfig {
        cert_path: cert_file.path().to_path_buf(),
        key_path: key_file.path().to_path_buf(),
    };

    assert!(TlsAcceptor::from_config(&config).is_err());
}

#[tokio::test]
async fn tls_handshake_and_https_request() {
    let (cert_file, key_file, certified_key) = generate_self_signed();

    let config = TlsConfig {
        cert_path: cert_file.path().to_path_buf(),
        key_path: key_file.path().to_path_buf(),
    };

    let acceptor = TlsAcceptor::from_config(&config).unwrap();

    // Bind to random port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    // Spawn TLS server that responds with "hello"
    let acceptor_clone = acceptor.clone();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut tls_stream = acceptor_clone.accept(stream).await.unwrap();
        // Simple HTTP/1.1 response
        use tokio::io::AsyncWriteExt;
        let response = b"HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nhello";
        tls_stream.write_all(response).await.unwrap();
        tls_stream.shutdown().await.unwrap();
    });

    // Build a reqwest client that trusts our self-signed cert
    let ca_cert = reqwest::Certificate::from_pem(certified_key.cert.pem().as_bytes()).unwrap();
    let client = reqwest::Client::builder()
        .add_root_certificate(ca_cert)
        .build()
        .unwrap();

    let resp = client
        .get(format!("https://localhost:{}", addr.port()))
        .send()
        .await
        .unwrap();

    assert_eq!(resp.status(), 200);
    assert_eq!(resp.text().await.unwrap(), "hello");
}
