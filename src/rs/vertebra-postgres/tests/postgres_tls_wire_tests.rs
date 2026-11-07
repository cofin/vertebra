use std::{sync::Arc, time::Duration};

use _postgres::{
    config::PostgresConfig,
    errors::PostgresError,
    session::{BeginConnect, PendingSession, SessionHandle},
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, date_time_ymd,
};
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};
use tokio_rustls::TlsAcceptor;
use vertebra_db::connection::{TlsMode, install_rustls_ring_provider};

mod support;

const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

struct Pki {
    ca_pem: Vec<u8>,
    server_cert: rustls::pki_types::CertificateDer<'static>,
    server_key: Vec<u8>,
    client_cert_pem: Vec<u8>,
    client_key_pem: Vec<u8>,
}

impl Pki {
    fn new(name: &str, expired: bool) -> Self {
        let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
        ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        ca.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        let key = KeyPair::generate().unwrap();
        let cert = ca.self_signed(&key).unwrap();
        let issuer = Issuer::new(ca, key);
        let mut server = CertificateParams::new(vec![name.into()]).unwrap();
        server.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
        if expired {
            server.not_before = date_time_ymd(2000, 1, 1);
            server.not_after = date_time_ymd(2001, 1, 1);
        }
        let server_key = KeyPair::generate().unwrap();
        let server_cert = server.signed_by(&server_key, &issuer).unwrap();
        let mut client = CertificateParams::new(Vec::<String>::new()).unwrap();
        client.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
        let client_key = KeyPair::generate().unwrap();
        let client_cert = client.signed_by(&client_key, &issuer).unwrap();
        Self {
            ca_pem: cert.pem().into_bytes(),
            server_cert: server_cert.der().clone(),
            server_key: server_key.serialize_der(),
            client_cert_pem: client_cert.pem().into_bytes(),
            client_key_pem: client_key.serialize_pem().into_bytes(),
        }
    }

    fn acceptor(&self, mtls: bool) -> TlsAcceptor {
        install_rustls_ring_provider();
        let builder = ServerConfig::builder();
        let builder = if mtls {
            let mut roots = RootCertStore::empty();
            roots.add_parsable_certificates(
                rustls_pemfile::certs(&mut self.ca_pem.as_slice()).map(Result::unwrap),
            );
            builder.with_client_cert_verifier(
                WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .unwrap(),
            )
        } else {
            builder.with_no_client_auth()
        };
        let config = builder
            .with_single_cert(
                vec![self.server_cert.clone()],
                rustls::pki_types::PrivatePkcs8KeyDer::from(self.server_key.clone()).into(),
            )
            .unwrap();
        TlsAcceptor::from(Arc::new(config))
    }
}

async fn run(handle: &SessionHandle) -> Result<PendingSession, PostgresError> {
    let BeginConnect::Attempt(attempt) = handle.begin_connect()? else {
        panic!("expected a fresh attempt")
    };
    timeout(Duration::from_secs(3), attempt.run())
        .await
        .unwrap()
}

async fn ssl_request(listener: &TcpListener) -> TcpStream {
    let (mut stream, _) = listener.accept().await.unwrap();
    let mut bytes = [0; 8];
    stream.read_exact(&mut bytes).await.unwrap();
    assert_eq!(bytes, SSL_REQUEST);
    stream
}

async fn tls_case(config: PostgresConfig, listener: TcpListener, pki: &Pki, mtls: bool, ok: bool) {
    let acceptor = pki.acceptor(mtls);
    let expected_name = config.server_name.clone();
    let peer = tokio::spawn(async move {
        let mut raw = ssl_request(&listener).await;
        raw.write_all(b"S").await.unwrap();
        let result = acceptor.accept(raw).await;
        if !ok {
            match result {
                Err(_) => {}
                Ok(mut tls) => {
                    let mut byte = [0];
                    assert!(matches!(tls.read(&mut byte).await, Ok(0) | Err(_)));
                }
            }
            assert!(
                timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
            return;
        }
        let mut tls = result.unwrap();
        assert_eq!(tls.get_ref().1.server_name(), expected_name.as_deref());
        if mtls {
            assert_eq!(tls.get_ref().1.peer_certificates().unwrap().len(), 1);
        }
        support::startup(&mut tls).await;
        support::send(&mut tls, &support::trust()).await;
        let mut terminate = [0; 5];
        tls.read_exact(&mut terminate).await.unwrap();
        assert_eq!(&terminate, b"X\0\0\0\x04");
        support::eof(&mut tls).await;
    });
    let handle = SessionHandle::new(config);
    let result = run(&handle).await;
    if ok {
        let pending = result.unwrap();
        assert_eq!(pending.connection().parameters()["client_encoding"], "UTF8");
        assert_eq!(pending.connection().tls_server_end_point(), None);
        pending.commit().unwrap();
        assert!(!handle.is_closed());
        handle.close().await.unwrap();
    } else {
        assert!(matches!(result, Err(PostgresError::Transport(_))));
        assert!(handle.is_closed());
        assert!(handle.parameters().is_empty());
        assert!(handle.begin_connect().is_err());
    }
    timeout(Duration::from_secs(3), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn disable_sends_startup_without_sslrequest() {
    let (listener, config) = support::listener().await;
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &support::trust()).await;
        let mut terminate = [0; 5];
        stream.read_exact(&mut terminate).await.unwrap();
        assert_eq!(&terminate, b"X\0\0\0\x04");
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    run(&handle).await.unwrap().commit().unwrap();
    handle.close().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn prefer_accepts_n_on_the_original_plain_socket() {
    let (listener, mut config) = support::listener().await;
    config.sslmode = TlsMode::Prefer;
    let peer = tokio::spawn(async move {
        let mut stream = ssl_request(&listener).await;
        stream.write_all(b"N").await.unwrap();
        support::startup(&mut stream).await;
        support::send(&mut stream, &support::trust()).await;
        support::eof(&mut stream).await;
        assert!(
            timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    });
    let handle = SessionHandle::new(config);
    let pending = run(&handle).await.unwrap();
    assert_eq!(pending.connection().tls_server_end_point(), None);
    pending.commit().unwrap();
    assert!(!handle.is_closed());
    drop(handle);
    peer.await.unwrap();
}

#[tokio::test]
async fn enabled_modes_upgrade_before_trust_startup() {
    let pki = Pki::new("localhost", false);
    for mode in [
        TlsMode::Prefer,
        TlsMode::Require,
        TlsMode::VerifyCa,
        TlsMode::VerifyFull,
    ] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = mode;
        config.ca_pem = Some(pki.ca_pem.clone());
        config.server_name = Some("localhost".into());
        tls_case(config, listener, &pki, false, true).await;
    }
}

#[tokio::test]
async fn refusal_unknown_response_and_eof_close_without_startup() {
    for (mode, response) in [
        (TlsMode::Require, Some(b'N')),
        (TlsMode::VerifyCa, Some(b'N')),
        (TlsMode::VerifyFull, Some(b'N')),
        (TlsMode::Prefer, Some(b'?')),
        (TlsMode::Prefer, Some(0)),
        (TlsMode::Prefer, None),
    ] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = mode;
        let peer = tokio::spawn(async move {
            let mut stream = ssl_request(&listener).await;
            if let Some(response) = response {
                stream.write_all(&[response]).await.unwrap();
                support::eof(&mut stream).await;
            } else {
                stream.shutdown().await.unwrap();
                support::eof(&mut stream).await;
            }
        });
        let handle = SessionHandle::new(config);
        let result = run(&handle).await;
        if response.is_some_and(|byte| byte != b'N') {
            assert!(matches!(result, Err(PostgresError::Protocol(_))));
        } else {
            assert!(matches!(result, Err(PostgresError::Transport(_))));
        }
        assert!(handle.is_closed());
        assert!(handle.begin_connect().is_err());
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn prefer_never_reconnects_after_s_or_certificate_failure() {
    let (listener, mut config) = support::listener().await;
    config.sslmode = TlsMode::Prefer;
    let peer = tokio::spawn(async move {
        let mut stream = ssl_request(&listener).await;
        stream.write_all(b"Snot a TLS record").await.unwrap();
        let mut hello = [0; 5];
        stream.read_exact(&mut hello).await.unwrap();
        assert_eq!(hello[0], 22);
        drop(stream);
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let handle = SessionHandle::new(config);
    assert!(matches!(
        run(&handle).await,
        Err(PostgresError::Transport(_))
    ));
    assert!(handle.is_closed());
    peer.await.unwrap();
    let pki = Pki::new("db.internal", false);
    let wrong = Pki::new("wrong", false);
    let (listener, mut config) = support::listener().await;
    config.sslmode = TlsMode::Prefer;
    config.ca_pem = Some(wrong.ca_pem);
    tls_case(config, listener, &pki, false, false).await;
}

#[tokio::test]
async fn ca_full_and_require_enforce_their_identity_policies() {
    let pki = Pki::new("db.internal", false);
    let wrong = Pki::new("wrong", false);
    for (mode, name, ca, ok) in [
        (TlsMode::VerifyCa, None, Some(pki.ca_pem.clone()), true),
        (TlsMode::VerifyFull, None, Some(pki.ca_pem.clone()), false),
        (
            TlsMode::VerifyFull,
            Some("db.internal"),
            Some(pki.ca_pem.clone()),
            true,
        ),
        (
            TlsMode::VerifyFull,
            Some("db.internal"),
            Some(wrong.ca_pem.clone()),
            false,
        ),
        (TlsMode::Require, None, None, true),
        (TlsMode::Require, None, Some(wrong.ca_pem.clone()), false),
    ] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = mode;
        config.ca_pem = ca;
        config.server_name = name.map(str::to_owned);
        tls_case(config, listener, &pki, false, ok).await;
    }
}

#[tokio::test]
async fn require_and_prefer_accept_self_signed_peers_without_ca() {
    let mut pki = Pki::new("localhost", false);
    let params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    let key = KeyPair::generate().unwrap();
    pki.server_cert = params.self_signed(&key).unwrap().der().clone();
    pki.server_key = key.serialize_der();
    for mode in [TlsMode::Prefer, TlsMode::Require] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = mode;
        tls_case(config, listener, &pki, false, true).await;
    }
}

#[tokio::test]
async fn expired_certificates_are_refused() {
    let pki = Pki::new("db.internal", true);
    for mode in [
        TlsMode::Prefer,
        TlsMode::Require,
        TlsMode::VerifyCa,
        TlsMode::VerifyFull,
    ] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = mode;
        config.ca_pem = Some(pki.ca_pem.clone());
        config.server_name = Some("db.internal".into());
        tls_case(config, listener, &pki, false, false).await;
    }
}

#[tokio::test]
async fn mtls_requires_the_valid_client_identity() {
    let pki = Pki::new("localhost", false);
    let wrong = Pki::new("wrong", false);
    for (identity, ok) in [(Some(&pki), true), (None, false), (Some(&wrong), false)] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = TlsMode::VerifyFull;
        config.server_name = Some("localhost".into());
        config.ca_pem = Some(pki.ca_pem.clone());
        if let Some(identity) = identity {
            config.client_cert_pem = Some(identity.client_cert_pem.clone());
            config.client_key_pem = Some(identity.client_key_pem.clone());
        }
        tls_case(config, listener, &pki, true, ok).await;
    }
}

#[tokio::test]
async fn sslresponse_and_tls_share_the_reserved_deadline() {
    for after_s in [false, true] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = TlsMode::Require;
        config.connect_timeout = Duration::from_millis(250);
        let handle = SessionHandle::new(config);
        let BeginConnect::Attempt(attempt) = handle.begin_connect().unwrap() else {
            panic!()
        };
        let peer = tokio::spawn(async move {
            let mut stream = ssl_request(&listener).await;
            if after_s {
                tokio::time::sleep(Duration::from_millis(100)).await;
                stream.write_all(b"S").await.unwrap();
                let mut hello = [0; 5];
                stream.read_exact(&mut hello).await.unwrap();
                assert_eq!(hello[0], 22);
                let mut rest = vec![0; u16::from_be_bytes([hello[3], hello[4]]) as usize];
                stream.read_exact(&mut rest).await.unwrap();
            }
            support::eof(&mut stream).await;
        });
        tokio::time::sleep(Duration::from_millis(75)).await;
        let start = Instant::now();
        assert!(matches!(
            attempt.run().await,
            Err(PostgresError::Transport(_))
        ));
        assert!(start.elapsed() < Duration::from_millis(225));
        assert!(handle.is_closed());
        peer.await.unwrap();
    }
}
