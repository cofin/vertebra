mod support;

use std::io::BufReader;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use _postgres::config::RawPostgresOptions;
use _postgres::errors::PostgresError;
use _postgres::session::SessionHandle;
use rcgen::{BasicConstraints, CertificateParams, CertifiedKey, IsCa, Issuer, KeyPair};
use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::WebPkiClientVerifier;
use tokio::io::AsyncWriteExt;
use tokio_rustls::TlsAcceptor;

use support::{
    frame_auth_ok, frame_auth_sasl, frame_auth_sasl_continue, frame_auth_sasl_final,
    frame_ready_for_query, parse_sasl_initial, read_ssl_request, read_startup_packet,
    read_tagged_frontend_frame, scram_verify_and_sign, spawn_scripted_server, write_frames,
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
    let cfg =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
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
    let cfg =
        ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
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

fn encode_der_len(len: usize) -> Vec<u8> {
    if len < 0x80 {
        vec![len as u8]
    } else if len <= 0xff {
        vec![0x81, len as u8]
    } else {
        vec![0x82, (len >> 8) as u8, (len & 0xff) as u8]
    }
}

fn replace_outer_sig_alg(base_cert_der: &[u8], new_alg_id_der: &[u8]) -> Vec<u8> {
    let (_, cert) = x509_parser::parse_x509_certificate(base_cert_der).expect("valid base cert");
    let tbs_raw = cert.tbs_certificate.as_ref();
    let sig_val_bytes = &cert.signature_value.data;

    let mut bit_string_content = Vec::with_capacity(1 + sig_val_bytes.len());
    bit_string_content.push(0u8);
    bit_string_content.extend_from_slice(sig_val_bytes);
    let mut bit_string_der = vec![0x03];
    bit_string_der.extend_from_slice(&encode_der_len(bit_string_content.len()));
    bit_string_der.extend_from_slice(&bit_string_content);

    let inner_len = tbs_raw.len() + new_alg_id_der.len() + bit_string_der.len();
    let mut out = vec![0x30];
    out.extend_from_slice(&encode_der_len(inner_len));
    out.extend_from_slice(tbs_raw);
    out.extend_from_slice(new_alg_id_der);
    out.extend_from_slice(&bit_string_der);
    out
}

#[test]
fn certificate_binding_sha1_sha224_sha256_sha384_sha512_rsassa_pss_and_ed25519() {
    use _postgres::binding::tls_server_end_point;
    use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};

    // 1. Native rcgen P-256 SHA-256 leaf
    let p256_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P256_SHA256).unwrap();
    let p256_cert = CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&p256_key)
        .unwrap();
    let p256_der = p256_cert.der().as_ref();
    assert_eq!(
        tls_server_end_point(p256_der).unwrap(),
        Sha256::digest(p256_der).to_vec()
    );

    // 2. Native rcgen P-384 SHA-384 leaf
    let p384_key = KeyPair::generate_for(&rcgen::PKCS_ECDSA_P384_SHA384).unwrap();
    let p384_cert = CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&p384_key)
        .unwrap();
    let p384_der = p384_cert.der().as_ref();
    assert_eq!(
        tls_server_end_point(p384_der).unwrap(),
        Sha384::digest(p384_der).to_vec()
    );

    // 3. Patched SHA-512 (`1.2.840.113549.1.1.13`: 06 09 2a 86 48 86 f7 0d 01 01 0d 05 00)
    let sha512_alg = [
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0d, 0x05, 0x00,
    ];
    let sha512_der = replace_outer_sig_alg(p256_der, &sha512_alg);
    assert_eq!(
        tls_server_end_point(&sha512_der).unwrap(),
        Sha512::digest(&sha512_der).to_vec()
    );

    // 4. Patched SHA-224 (`1.2.840.113549.1.1.14`: 06 09 2a 86 48 86 f7 0d 01 01 0e 05 00)
    let sha224_alg = [
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0e, 0x05, 0x00,
    ];
    let sha224_der = replace_outer_sig_alg(p256_der, &sha224_alg);
    assert_eq!(
        tls_server_end_point(&sha224_der).unwrap(),
        Sha224::digest(&sha224_der).to_vec()
    );

    // 5. Patched MD5 (`1.2.840.113549.1.1.4`) and SHA-1 (`1.2.840.113549.1.1.5`) -> substituted to SHA-256
    for last_byte in [0x04u8, 0x05u8] {
        let legacy_alg = [
            0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, last_byte,
            0x05, 0x00,
        ];
        let legacy_der = replace_outer_sig_alg(p256_der, &legacy_alg);
        assert_eq!(
            tls_server_end_point(&legacy_der).unwrap(),
            Sha256::digest(&legacy_der).to_vec()
        );
    }

    // 6. RSASSA-PSS (`1.2.840.113549.1.1.10`) with default parameters (empty SEQUENCE 30 00) -> SHA-1 -> substituted to SHA-256
    let pss_default_alg = [
        0x30, 0x0d, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0a, 0x30, 0x00,
    ];
    let pss_default_der = replace_outer_sig_alg(p256_der, &pss_default_alg);
    assert_eq!(
        tls_server_end_point(&pss_default_der).unwrap(),
        Sha256::digest(&pss_default_der).to_vec()
    );

    // 7. RSASSA-PSS (`1.2.840.113549.1.1.10`) with explicit SHA-384 (`2.16.840.1.101.3.4.2.2`: 06 09 60 86 48 01 65 03 04 02 02)
    // RSASSA-PSS-params ::= SEQUENCE { [0] EXPLICIT AlgorithmIdentifier { OID 2.16.840.1.101.3.4.2.2, NULL } }
    let pss_sha384_alg = [
        0x30, 0x1c, 0x06, 0x09, 0x2a, 0x86, 0x48, 0x86, 0xf7, 0x0d, 0x01, 0x01, 0x0a, 0x30, 0x0f,
        0xa0, 0x0d, 0x30, 0x0b, 0x06, 0x09, 0x60, 0x86, 0x48, 0x01, 0x65, 0x03, 0x04, 0x02, 0x02,
    ];
    let pss_sha384_der = replace_outer_sig_alg(p256_der, &pss_sha384_alg);
    assert_eq!(
        tls_server_end_point(&pss_sha384_der).unwrap(),
        Sha384::digest(&pss_sha384_der).to_vec()
    );

    // 8. Ed25519 (`1.3.101.112`) and Ed448 (`1.3.101.113`) -> UnsupportedAuth
    let ed25519_key = KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    let ed25519_cert = CertificateParams::new(vec!["localhost".to_string()])
        .unwrap()
        .self_signed(&ed25519_key)
        .unwrap();
    assert!(matches!(
        tls_server_end_point(ed25519_cert.der().as_ref()),
        Err(PostgresError::UnsupportedAuth(_))
    ));
    let ed448_alg = [0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x71];
    let ed448_der = replace_outer_sig_alg(p256_der, &ed448_alg);
    assert!(matches!(
        tls_server_end_point(&ed448_der),
        Err(PostgresError::UnsupportedAuth(_))
    ));
}

#[tokio::test]
async fn scram_plus_over_tls_verify_full_and_ed25519_prefer_vs_require() {
    use _postgres::binding::tls_server_end_point;
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    let bundle = generate_ca_and_leaf(&["127.0.0.1"]);
    let leaf_certs: Vec<CertificateDer<'static>> =
        rustls_pemfile::certs(&mut BufReader::new(bundle.server_cert_pem.as_slice()))
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
    let expected_cbind = tls_server_end_point(leaf_certs[0].as_ref()).unwrap();
    let salt = b"0123456789abcdef";

    // 1. SCRAM-SHA-256-PLUS over TLS verify-full succeeds with `p=tls-server-end-point,,`
    {
        let acceptor = make_tls_acceptor(&bundle.server_cert_pem, &bundle.server_key_pem);
        let cbind_clone = expected_cbind.clone();
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();
            let mut tls_stream = acceptor.accept(stream).await.unwrap();
            let _ = read_startup_packet(&mut tls_stream).await.unwrap();

            write_frames(
                &mut tls_stream,
                &[frame_auth_sasl(&[b"SCRAM-SHA-256", b"SCRAM-SHA-256-PLUS"])],
            )
            .await
            .unwrap();

            let (_, payload1) = read_tagged_frontend_frame(&mut tls_stream).await.unwrap();
            let initial = parse_sasl_initial(&payload1);
            assert_eq!(initial.mechanism, "SCRAM-SHA-256-PLUS");
            assert_eq!(initial.gs2_header, "p=tls-server-end-point,,");

            let server_first = format!(
                "r={}srv,s={},i=32",
                initial.client_nonce,
                STANDARD.encode(salt)
            );
            write_frames(
                &mut tls_stream,
                &[frame_auth_sasl_continue(server_first.as_bytes())],
            )
            .await
            .unwrap();

            let (_, payload2) = read_tagged_frontend_frame(&mut tls_stream).await.unwrap();
            let client_final = String::from_utf8(payload2).unwrap();
            let server_final = scram_verify_and_sign(
                b"secret",
                &initial,
                &server_first,
                salt,
                32,
                &cbind_clone,
                &client_final,
            );
            write_frames(
                &mut tls_stream,
                &[
                    frame_auth_sasl_final(server_final.as_bytes()),
                    frame_auth_ok(),
                    frame_ready_for_query(b'I'),
                ],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "verify-full".to_string();
        raw.ca_pem = Some(bundle.ca_pem.clone());
        raw.channel_binding = "require".to_string();
        raw.password = Some("secret".to_string());
        let mut handle = SessionHandle::new(raw).unwrap();
        let conn = handle
            .connect()
            .await
            .expect("SCRAM-SHA-256-PLUS verify-full");
        assert_eq!(conn.tls_server_end_point(), Some(expected_cbind.as_slice()));
        task.await.unwrap();
    }

    // 2. When TLS leaf is Ed25519:
    //    - `prefer` falls back to SCRAM-SHA-256 with `n,,` GS2 header
    //    - `require` fails with `UnsupportedAuth` without emitting credentials
    let ed_key = KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    let ed_cert = CertificateParams::new(vec!["127.0.0.1".to_string()])
        .unwrap()
        .self_signed(&ed_key)
        .unwrap();
    let ed_cert_pem = ed_cert.pem().into_bytes();
    let ed_key_pem = ed_key.serialize_pem().into_bytes();

    {
        let acceptor = make_tls_acceptor(&ed_cert_pem, &ed_key_pem);
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();
            let mut tls_stream = acceptor.accept(stream).await.unwrap();
            let _ = read_startup_packet(&mut tls_stream).await.unwrap();

            write_frames(
                &mut tls_stream,
                &[frame_auth_sasl(&[b"SCRAM-SHA-256", b"SCRAM-SHA-256-PLUS"])],
            )
            .await
            .unwrap();

            let (_, payload1) = read_tagged_frontend_frame(&mut tls_stream).await.unwrap();
            let initial = parse_sasl_initial(&payload1);
            assert_eq!(initial.mechanism, "SCRAM-SHA-256");
            assert_eq!(initial.gs2_header, "n,,");

            let server_first = format!(
                "r={}srv,s={},i=16",
                initial.client_nonce,
                STANDARD.encode(salt)
            );
            write_frames(
                &mut tls_stream,
                &[frame_auth_sasl_continue(server_first.as_bytes())],
            )
            .await
            .unwrap();
            let (_, payload2) = read_tagged_frontend_frame(&mut tls_stream).await.unwrap();
            let client_final = String::from_utf8(payload2).unwrap();
            let server_final = scram_verify_and_sign(
                b"secret",
                &initial,
                &server_first,
                salt,
                16,
                &[],
                &client_final,
            );
            write_frames(
                &mut tls_stream,
                &[
                    frame_auth_sasl_final(server_final.as_bytes()),
                    frame_auth_ok(),
                    frame_ready_for_query(b'I'),
                ],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "require".to_string();
        raw.channel_binding = "prefer".to_string();
        raw.password = Some("secret".to_string());
        let mut handle = SessionHandle::new(raw).unwrap();
        let conn = handle
            .connect()
            .await
            .expect("Ed25519 prefer degrades to SCRAM-SHA-256");
        assert!(conn.tls_server_end_point().is_none());
        task.await.unwrap();
    }

    {
        let acceptor = make_tls_acceptor(&ed_cert_pem, &ed_key_pem);
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            read_ssl_request(&mut stream).await.unwrap();
            stream.write_all(b"S").await.unwrap();
            stream.flush().await.unwrap();
            let mut tls_stream = acceptor.accept(stream).await.unwrap();
            let _ = read_startup_packet(&mut tls_stream).await.unwrap();
            write_frames(
                &mut tls_stream,
                &[frame_auth_sasl(&[b"SCRAM-SHA-256", b"SCRAM-SHA-256-PLUS"])],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "alice", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "require".to_string();
        raw.channel_binding = "require".to_string();
        raw.password = Some("secret".to_string());
        let mut handle = SessionHandle::new(raw).unwrap();
        let err = handle
            .connect()
            .await
            .expect_err("Ed25519 with channel_binding=require must fail UnsupportedAuth");
        assert!(matches!(err, PostgresError::UnsupportedAuth(_)));
        task.await.unwrap();
    }
}
