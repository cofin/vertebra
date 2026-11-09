mod support;

use std::io::BufReader;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use rcgen::{BasicConstraints, CertificateParams, CertifiedKey, IsCa, Issuer, KeyPair};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use tokio::io::AsyncWriteExt;
use tokio_rustls::TlsAcceptor;
use _postgres::config::RawPostgresOptions;
use _postgres::errors::PostgresError;
use _postgres::session::SessionHandle;

use support::{
    frame_auth_ok, frame_ready_for_query, read_ssl_request, read_startup_packet,
    spawn_scripted_server, write_frames,
};

struct CertBundle {
    ca_pem: Vec<u8>,
    server_cert_pem: Vec<u8>,
    server_key_pem: Vec<u8>,
}

fn generate_ca_and_leaf(san_names: &[&str]) -> CertBundle {
    let ca_key = KeyPair::generate().expect("ca key");
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca_cert = ca_params.self_signed(&ca_key).expect("self-signed ca");
    let ca_issuer = Issuer::new(ca_params, ca_key);

    let leaf_key = KeyPair::generate().expect("leaf key");
    let leaf_params = CertificateParams::new(
        san_names
            .iter()
            .map(|s| (*s).to_string())
            .collect::<Vec<_>>(),
    )
    .expect("leaf params");
    let leaf_cert = leaf_params
        .signed_by(&leaf_key, &ca_issuer)
        .expect("signed leaf");

    CertBundle {
        ca_pem: ca_cert.pem().into_bytes(),
        server_cert_pem: leaf_cert.pem().into_bytes(),
        server_key_pem: leaf_key.serialize_pem().into_bytes(),
    }
}

fn make_tls_acceptor(server_cert_pem: &[u8], server_key_pem: &[u8]) -> TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(server_cert_pem))
            .collect::<Result<Vec<_>, _>>()
            .expect("server certs");
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut BufReader::new(server_key_pem))
            .expect("server key read")
            .expect("server key present");
    let cfg = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("versions")
    .with_no_client_auth()
    .with_single_cert(certs, key)
    .expect("single cert");
    TlsAcceptor::from(Arc::new(cfg))
}

fn make_mtls_acceptor(
    server_cert_pem: &[u8],
    server_key_pem: &[u8],
    client_ca_pem: &[u8],
) -> TlsAcceptor {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    let ca_certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(client_ca_pem))
            .collect::<Result<Vec<_>, _>>()
            .expect("client ca");
    roots.add_parsable_certificates(ca_certs);
    let client_verifier = WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        Arc::new(rustls::crypto::ring::default_provider()),
    )
    .build()
    .expect("client verifier");

    let certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(server_cert_pem))
            .collect::<Result<Vec<_>, _>>()
            .expect("server certs");
    let key: PrivateKeyDer<'static> =
        rustls_pemfile::private_key(&mut BufReader::new(server_key_pem))
            .expect("server key read")
            .expect("server key present");
    let cfg = ServerConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("versions")
    .with_client_cert_verifier(client_verifier)
    .with_single_cert(certs, key)
    .expect("single cert");
    TlsAcceptor::from(Arc::new(cfg))
}

#[tokio::test]
async fn tls_sslmode_disable_skips_sslrequest() {
    let (addr, accepts, task) = spawn_scripted_server(|mut stream| async move {
        let params = read_startup_packet(&mut stream)
            .await
            .expect("StartupMessage directly without SSLRequest");
        assert_eq!(params.get("user").map(String::as_str), Some("alice"));
        write_frames(&mut stream, &[frame_auth_ok(), frame_ready_for_query(b'I')])
            .await
            .unwrap();
    })
    .await;

    let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
    raw.port = i64::from(addr.port());
    raw.sslmode = "disable".to_string();
    let mut handle = SessionHandle::new(raw).unwrap();
    let conn = handle.connect().await.expect("disable connect");
    assert!(!conn.is_tls());
    assert_eq!(accepts.load(Ordering::SeqCst), 1);
    task.await.unwrap();
}

#[tokio::test]
async fn tls_sslmode_prefer_falls_back_on_same_socket_when_server_replies_n() {
    let (addr, accepts, task) = spawn_scripted_server(|mut stream| async move {
        read_ssl_request(&mut stream).await.expect("SSLRequest");
        stream.write_all(b"N").await.unwrap();
        stream.flush().await.unwrap();

        let params = read_startup_packet(&mut stream)
            .await
            .expect("StartupMessage on same TCP socket after N");
        assert_eq!(params.get("user").map(String::as_str), Some("alice"));
        write_frames(&mut stream, &[frame_auth_ok(), frame_ready_for_query(b'I')])
            .await
            .unwrap();
    })
    .await;

    let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
    raw.port = i64::from(addr.port());
    raw.sslmode = "prefer".to_string();
    let mut handle = SessionHandle::new(raw).unwrap();
    let conn = handle.connect().await.expect("prefer N fallback");
    assert!(!conn.is_tls());
    assert_eq!(accepts.load(Ordering::SeqCst), 1);
    task.await.unwrap();
}

#[tokio::test]
async fn tls_sslmode_require_verify_ca_and_verify_full_reject_n_or_invalid_byte() {
    for mode in ["require", "verify-ca", "verify-full"] {
        let (addr, _, task) = spawn_scripted_server(|mut stream| async move {
            read_ssl_request(&mut stream).await.expect("SSLRequest");
            stream.write_all(b"N").await.unwrap();
            stream.flush().await.unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = mode.to_string();
        let mut handle = SessionHandle::new(raw).unwrap();
        let err = handle
            .connect()
            .await
            .expect_err("N must fail required sslmode");
        assert!(matches!(err, PostgresError::Transport(_)));
        assert!(handle.is_closed());
        task.await.unwrap();
    }

    // Invalid response byte 'X'
    let (addr_x, _, task_x) = spawn_scripted_server(|mut stream| async move {
        read_ssl_request(&mut stream).await.expect("SSLRequest");
        stream.write_all(b"X").await.unwrap();
        stream.flush().await.unwrap();
    })
    .await;
    let mut raw_x = RawPostgresOptions::new(addr_x.ip().to_string(), "alice", "appdb");
    raw_x.port = i64::from(addr_x.port());
    raw_x.sslmode = "prefer".to_string();
    let mut handle_x = SessionHandle::new(raw_x).unwrap();
    let err_x = handle_x
        .connect()
        .await
        .expect_err("invalid byte X must fail");
    assert!(matches!(err_x, PostgresError::Protocol(_)));
    task_x.await.unwrap();
}

#[tokio::test]
async fn tls_sslmode_prefer_and_require_accept_untrusted_self_signed_cert() {
    let CertifiedKey { cert, signing_key } =
        rcgen::generate_simple_self_signed(vec!["untrusted.internal".to_string()])
            .expect("self-signed");
    let cert_pem = cert.pem().into_bytes();
    let key_pem = signing_key.serialize_pem().into_bytes();

    for mode in ["prefer", "require"] {
        let acceptor = make_tls_acceptor(&cert_pem, &key_pem);
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.expect("SSLRequest");
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();

            let mut tls_stream = acceptor.accept(stream).await.expect("TLS accept");
            let params = read_startup_packet(&mut tls_stream)
                .await
                .expect("encrypted StartupMessage");
            assert_eq!(params.get("user").map(String::as_str), Some("alice"));
            write_frames(
                &mut tls_stream,
                &[frame_auth_ok(), frame_ready_for_query(b'I')],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = mode.to_string();
        let mut handle = SessionHandle::new(raw).unwrap();
        let conn = handle
            .connect()
            .await
            .expect("prefer/require self-signed TLS");
        assert!(conn.is_tls());
        task.await.unwrap();
    }
}

#[tokio::test]
async fn tls_verify_ca_and_verify_full_enforce_chain_and_san_and_mtls() {
    let bundle_a = generate_ca_and_leaf(&["db.internal.example", "127.0.0.1"]);
    let bundle_wrong_san = generate_ca_and_leaf(&["other.internal.example"]);
    let bundle_b = generate_ca_and_leaf(&["db.internal.example", "127.0.0.1"]);

    // 1. verify-ca accepts trusted CA even when SAN does not match 127.0.0.1
    {
        let acceptor = make_tls_acceptor(
            &bundle_wrong_san.server_cert_pem,
            &bundle_wrong_san.server_key_pem,
        );
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();
            let mut tls_stream = acceptor.accept(stream).await.unwrap();
            let _ = read_startup_packet(&mut tls_stream).await.unwrap();
            write_frames(
                &mut tls_stream,
                &[frame_auth_ok(), frame_ready_for_query(b'I')],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "verify-ca".to_string();
        raw.ca_pem = Some(bundle_wrong_san.ca_pem.clone());
        let mut handle = SessionHandle::new(raw).unwrap();
        let conn = handle.connect().await.expect("verify-ca ignores SAN");
        assert!(conn.is_tls());
        task.await.unwrap();
    }

    // 2. verify-ca rejects untrusted CA
    {
        let acceptor = make_tls_acceptor(&bundle_a.server_cert_pem, &bundle_a.server_key_pem);
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();
            let _ = acceptor.accept(stream).await;
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "verify-ca".to_string();
        raw.ca_pem = Some(bundle_b.ca_pem.clone());
        let mut handle = SessionHandle::new(raw).unwrap();
        let err = handle
            .connect()
            .await
            .expect_err("verify-ca must reject wrong CA");
        assert!(matches!(err, PostgresError::Transport(_)));
        task.await.unwrap();
    }

    // 3. verify-full rejects mismatched SAN, succeeds with matching server_name override + mTLS
    {
        let acceptor = make_tls_acceptor(
            &bundle_wrong_san.server_cert_pem,
            &bundle_wrong_san.server_key_pem,
        );
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();
            let _ = acceptor.accept(stream).await;
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "verify-full".to_string();
        raw.ca_pem = Some(bundle_wrong_san.ca_pem.clone());
        let mut handle = SessionHandle::new(raw).unwrap();
        let err = handle
            .connect()
            .await
            .expect_err("verify-full must reject SAN mismatch");
        assert!(matches!(err, PostgresError::Transport(_)));
        task.await.unwrap();
    }

    // 4. verify-full with matching server_name and mTLS client identity
    {
        let client_bundle = generate_ca_and_leaf(&["alice"]);
        let acceptor = make_mtls_acceptor(
            &bundle_wrong_san.server_cert_pem,
            &bundle_wrong_san.server_key_pem,
            &client_bundle.ca_pem,
        );
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();
            let mut tls_stream = acceptor.accept(stream).await.expect("mTLS accept");
            let _ = read_startup_packet(&mut tls_stream).await.unwrap();
            write_frames(
                &mut tls_stream,
                &[frame_auth_ok(), frame_ready_for_query(b'I')],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "verify-full".to_string();
        raw.ca_pem = Some(bundle_wrong_san.ca_pem.clone());
        raw.server_name = Some("other.internal.example".to_string());
        raw.client_cert_pem = Some(client_bundle.server_cert_pem);
        raw.client_key_pem = Some(client_bundle.server_key_pem);
        let mut handle = SessionHandle::new(raw).unwrap();
        let conn = handle
            .connect()
            .await
            .expect("verify-full with server_name and mTLS");
        assert!(conn.is_tls());
        task.await.unwrap();
    }
}
