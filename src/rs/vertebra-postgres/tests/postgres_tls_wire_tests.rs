use std::{sync::Arc, time::Duration};

use _postgres::{
    binding::tls_server_end_point,
    config::{ChannelBindingPolicy, PostgresConfig},
    errors::PostgresError,
    session::{BeginConnect, PendingSession, SessionHandle},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose, date_time_ymd,
};
use rustls::{RootCertStore, ServerConfig, server::WebPkiClientVerifier};
use sha2::{Digest, Sha224, Sha256, Sha384, Sha512};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, timeout},
};
use tokio_rustls::TlsAcceptor;
use vertebra_db::connection::{TlsMode, install_rustls_ring_provider};
use x509_parser::asn1_rs::{Any, FromDer};

mod support;

const SSL_REQUEST: [u8; 8] = [0, 0, 0, 8, 4, 210, 22, 47];

fn plus_auth(code: i32, data: &[u8]) -> Vec<u8> {
    support::frame(b'R', &[code.to_be_bytes().as_slice(), data].concat())
}

async fn plus_password<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> Vec<u8> {
    assert_eq!(stream.read_u8().await.unwrap(), b'p');
    let len = stream.read_i32().await.unwrap();
    assert!((4..=16_384).contains(&len));
    let mut body = vec![0; len as usize - 4];
    stream.read_exact(&mut body).await.unwrap();
    body
}

async fn plus_eof<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) {
    let mut byte = [0];
    match timeout(Duration::from_secs(30), stream.read(&mut byte))
        .await
        .unwrap()
    {
        Ok(0) => {}
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::ConnectionReset
            ) => {}
        result => panic!("unexpected credential or retry bytes: {result:?} {byte:?}"),
    }
}

fn plus_hmac(key: &[u8], bytes: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(bytes);
    mac.finalize().into_bytes().into()
}

async fn plus_exchange<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
    stream: &mut S,
    mechanism: &str,
    gs2: &str,
    binding: &[u8],
    outcome: &str,
) {
    let initial = plus_password(stream).await;
    let end = initial.iter().position(|byte| *byte == 0).unwrap();
    assert_eq!(&initial[..end], mechanism.as_bytes());
    let length = i32::from_be_bytes(initial[end + 1..end + 5].try_into().unwrap());
    let first = std::str::from_utf8(&initial[end + 5..]).unwrap();
    assert_eq!(length as usize, first.len());
    assert!(first.starts_with(&format!("{gs2}n=,r=")));
    let first = &first[gs2.len()..];
    let nonce = first.strip_prefix("n=,r=").unwrap();
    assert!(!nonce.is_empty());
    let server = format!("r={nonce}server,s=dnRiLXRlc3Qtc2FsdA==,i=4096");
    support::send(stream, &plus_auth(11, server.as_bytes())).await;
    let response = plus_password(stream).await;
    let response = std::str::from_utf8(&response).unwrap();
    let (bare, proof) = response.rsplit_once(",p=").unwrap();
    let (encoded, final_nonce) = bare.strip_prefix("c=").unwrap().split_once(",r=").unwrap();
    let expected_binding = [gs2.as_bytes(), binding].concat();
    assert_eq!(STANDARD.decode(encoded).unwrap(), expected_binding);
    assert_eq!(final_nonce, format!("{nonce}server"));
    let salted =
        hex::decode("2482c2af90bd00962845709770664bb70a090bcbd937e9d1fa1f855044d513bb").unwrap();
    let client_key = plus_hmac(&salted, b"Client Key");
    let stored_key = Sha256::digest(client_key);
    let auth = format!("{first},{server},{bare}");
    let signature = plus_hmac(&stored_key, auth.as_bytes());
    let expected_proof: Vec<_> = client_key
        .iter()
        .zip(signature)
        .map(|(key, mac)| key ^ mac)
        .collect();
    assert_eq!(STANDARD.decode(proof).unwrap(), expected_proof);
    let final_message = match outcome {
        "downgrade" => "e=server-does-support-channel-binding".into(),
        "wrong-leaf" => {
            let other = Pki::new("localhost", false);
            let other_digest = Sha256::digest(other.server_cert.as_ref());
            assert_ne!(binding, &other_digest[..]);
            let wrong_c = STANDARD.encode([gs2.as_bytes(), &other_digest[..]].concat());
            let wrong_auth = format!("{first},{server},c={wrong_c},r={nonce}server");
            let wrong_signature = plus_hmac(&stored_key, wrong_auth.as_bytes());
            let wrong_proof: Vec<_> = client_key
                .iter()
                .zip(wrong_signature)
                .map(|(key, mac)| key ^ mac)
                .collect();
            assert_ne!(STANDARD.decode(proof).unwrap(), wrong_proof);
            "e=channel-bindings-dont-match".into()
        }
        "wrong-proof" => format!("v={}", STANDARD.encode([0; 32])),
        "success" => format!(
            "v={}",
            STANDARD.encode(plus_hmac(
                &plus_hmac(&salted, b"Server Key"),
                auth.as_bytes()
            ))
        ),
        _ => unreachable!(),
    };
    let mut frames = plus_auth(12, final_message.as_bytes());
    if outcome == "success" {
        frames.extend(support::trust());
    }
    support::send(stream, &frames).await;
}

async fn plus_tls_case(
    policy: ChannelBindingPolicy,
    ed25519: bool,
    offer: usize,
    outcome: &'static str,
) {
    let mut pki = Pki::new("localhost", false);
    if ed25519 {
        let params = CertificateParams::new(vec!["localhost".into()]).unwrap();
        let key = KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
        let cert = params.self_signed(&key).unwrap();
        pki.ca_pem = cert.pem().into_bytes();
        pki.server_cert = cert.der().clone();
        pki.server_key = key.serialize_der();
    }
    let (listener, mut config) = support::listener().await;
    config.sslmode = TlsMode::VerifyFull;
    config.server_name = Some("localhost".into());
    config.ca_pem = Some(pki.ca_pem.clone());
    config.channel_binding = policy;
    config.password = Some("secret".into());
    config.connect_timeout = Duration::from_secs(30);
    let digest = Sha256::digest(pki.server_cert.as_ref()).to_vec();
    let acceptor = pki.acceptor(false);
    let use_plus = !ed25519 && policy != ChannelBindingPolicy::Disable && offer <= 1;
    let unavailable = policy == ChannelBindingPolicy::Require && ed25519;
    let refused = unavailable
        || (policy == ChannelBindingPolicy::Require && offer >= 2)
        || (offer == 1 && !use_plus);
    let success = !refused && outcome == "success";
    let (done_tx, done_rx) = tokio::sync::oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut raw = ssl_request(&listener).await;
        raw.write_all(b"S").await.unwrap();
        let mut tls = acceptor.accept(raw).await.unwrap();
        if unavailable {
            plus_eof(&mut tls).await;
        } else {
            support::startup(&mut tls).await;
            let frame = match offer {
                0 => plus_auth(10, b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0"),
                1 => plus_auth(10, b"SCRAM-SHA-256-PLUS\0\0"),
                2 => plus_auth(10, b"SCRAM-SHA-256\0\0"),
                3 => support::trust(),
                4 => plus_auth(3, b""),
                5 => plus_auth(5, &[1, 2, 3, 4]),
                _ => unreachable!(),
            };
            support::send(&mut tls, &frame).await;
            if !refused {
                if offer <= 2 {
                    let (mechanism, gs2, binding) = if use_plus {
                        (
                            "SCRAM-SHA-256-PLUS",
                            "p=tls-server-end-point,,",
                            digest.as_slice(),
                        )
                    } else {
                        (
                            "SCRAM-SHA-256",
                            if !ed25519 && policy == ChannelBindingPolicy::Prefer {
                                "y,,"
                            } else {
                                "n,,"
                            },
                            &[][..],
                        )
                    };
                    plus_exchange(&mut tls, mechanism, gs2, binding, outcome).await;
                } else if offer >= 4 {
                    let password = plus_password(&mut tls).await;
                    let expected = if offer == 4 {
                        "secret".to_owned()
                    } else {
                        postgres_protocol::authentication::md5_hash(b"app", b"secret", [1, 2, 3, 4])
                    };
                    assert_eq!(password, [expected.as_bytes(), b"\0"].concat());
                    support::send(&mut tls, &support::trust()).await;
                }
            }
            plus_eof(&mut tls).await;
        }
        done_rx.await.unwrap();
        let mut connections = 1;
        let listener = listener.into_std().unwrap();
        while listener.accept().is_ok() {
            connections += 1;
        }
        assert_eq!(connections, 1, "authentication reconnected");
    });
    let handle = SessionHandle::new(config);
    let BeginConnect::Attempt(attempt) = handle.begin_connect().unwrap() else {
        panic!()
    };
    let result = timeout(Duration::from_secs(30), attempt.run())
        .await
        .unwrap();
    if success {
        let pending = result.unwrap();
        assert_eq!(
            pending.connection().tls_server_end_point().is_some(),
            !ed25519
        );
        pending.commit().unwrap();
        assert!(!handle.is_closed());
    } else {
        let error = match result {
            Err(error) => error,
            Ok(_) => panic!("unexpected authentication success"),
        };
        if refused {
            assert!(matches!(error, PostgresError::UnsupportedAuth(_)));
        } else {
            assert!(matches!(error, PostgresError::Protocol(_)));
        }
        assert!(handle.is_closed());
        assert!(handle.parameters().is_empty());
        assert!(handle.begin_connect().is_err());
    }
    drop(handle);
    done_tx.send(()).unwrap();
    timeout(Duration::from_secs(30), peer)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn scram_plus_policy_matrix_tls() {
    for policy in [
        ChannelBindingPolicy::Disable,
        ChannelBindingPolicy::Prefer,
        ChannelBindingPolicy::Require,
    ] {
        for ed25519 in [false, true] {
            for offer in 0..6 {
                plus_tls_case(policy, ed25519, offer, "success").await;
            }
        }
    }
}

#[tokio::test]
async fn scram_plus_tls_leaf_bound_success_and_mismatch() {
    for outcome in ["success", "wrong-leaf", "wrong-proof"] {
        plus_tls_case(ChannelBindingPolicy::Require, false, 0, outcome).await;
    }
}

#[tokio::test]
async fn scram_plus_plain_offer_gs2_y() {
    plus_tls_case(ChannelBindingPolicy::Prefer, false, 2, "success").await;
}

#[tokio::test]
async fn scram_plus_downgrade_rejected() {
    plus_tls_case(ChannelBindingPolicy::Prefer, false, 2, "downgrade").await;
}

struct Pki {
    ca_pem: Vec<u8>,
    server_cert: rustls::pki_types::CertificateDer<'static>,
    server_key: Vec<u8>,
    chain: Vec<rustls::pki_types::CertificateDer<'static>>,
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
            chain: Vec::new(),
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
                std::iter::once(self.server_cert.clone())
                    .chain(self.chain.clone())
                    .collect(),
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
    tls_case_with_binding(
        config,
        listener,
        pki,
        mtls,
        ok,
        Some(Sha256::digest(pki.server_cert.as_ref()).to_vec()),
    )
    .await;
}

async fn tls_case_with_binding(
    config: PostgresConfig,
    listener: TcpListener,
    pki: &Pki,
    mtls: bool,
    ok: bool,
    expected_binding: Option<Vec<u8>>,
) {
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
        assert_eq!(
            pending.connection().tls_server_end_point(),
            expected_binding.as_deref()
        );
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
    async fn peer_stage(
        stage: &str,
        mut reached: tokio::sync::oneshot::Receiver<()>,
        client: &tokio::task::JoinHandle<Result<PendingSession, PostgresError>>,
    ) {
        let watchdog = std::time::Instant::now();
        loop {
            match reached.try_recv() {
                Ok(()) => return,
                Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                    assert!(
                        watchdog.elapsed() < Duration::from_secs(30),
                        "peer stalled at {stage} for 30 seconds"
                    );
                    assert!(
                        !client.is_finished(),
                        "client stopped before {stage} peer barrier"
                    );
                    // Keep socket I/O runnable without auto-advancing the paused clock.
                    tokio::task::yield_now().await;
                }
                Err(error) => panic!("{stage} peer barrier failed: {error}"),
            }
        }
    }

    for after_s in [false, true] {
        let (listener, mut config) = support::listener().await;
        config.sslmode = TlsMode::Require;
        config.connect_timeout = Duration::from_millis(250);
        let handle = SessionHandle::new(config);
        tokio::time::pause();
        let reserved_at = Instant::now();
        let BeginConnect::Attempt(attempt) = handle.begin_connect().unwrap() else {
            panic!()
        };
        let (request_seen, request_received) = tokio::sync::oneshot::channel();
        let (release_response, response_allowed) = tokio::sync::oneshot::channel();
        let (stage_seen, stage_received) = tokio::sync::oneshot::channel();
        let (release_eof, eof_allowed) = tokio::sync::oneshot::channel();
        let peer = tokio::spawn(async move {
            let mut stream = ssl_request(&listener).await;
            request_seen.send(()).unwrap();
            response_allowed.await.unwrap();
            if after_s {
                stream.write_all(b"S").await.unwrap();
                let mut hello = [0; 5];
                stream.read_exact(&mut hello).await.unwrap();
                assert_eq!(hello[0], 22);
                let mut rest = vec![0; u16::from_be_bytes([hello[3], hello[4]]) as usize];
                stream.read_exact(&mut rest).await.unwrap();
            }
            stage_seen.send(()).unwrap();
            eof_allowed.await.unwrap();
            support::eof(&mut stream).await;
        });
        tokio::time::advance(Duration::from_millis(75)).await;
        let client = tokio::spawn(attempt.run());
        peer_stage("SSLRequest", request_received, &client).await;
        tokio::time::advance(Duration::from_millis(100)).await;
        release_response.send(()).unwrap();
        peer_stage(
            if after_s {
                "TLS ClientHello"
            } else {
                "SSLResponse stall"
            },
            stage_received,
            &client,
        )
        .await;
        tokio::time::advance(Duration::from_millis(74)).await;
        tokio::task::yield_now().await;
        assert!(!client.is_finished());
        // Tokio timers round deadlines to the next millisecond.
        tokio::time::advance(Duration::from_millis(2)).await;
        assert!(matches!(
            client.await.unwrap(),
            Err(PostgresError::Transport(_))
        ));
        assert_eq!(Instant::now(), reserved_at + Duration::from_millis(251));
        assert!(handle.is_closed());
        assert!(handle.parameters().is_empty());
        assert!(handle.begin_connect().is_err());
        tokio::time::resume();
        release_eof.send(()).unwrap();
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn certificate_binding_live_tls_digest() {
    let pki = Pki::new("localhost", false);
    let (listener, mut config) = support::listener().await;
    config.sslmode = TlsMode::Require;
    tls_case(config, listener, &pki, false, true).await;
}

// Signed OpenSSL 3.5.5 certificates, with fixed independently computed hashlib
// digests. These exercise certificate signature OIDs that rcgen/ring cannot sign.
const SIGNED_VECTORS: &[(&str, &str, &str)] = &[
    (
        "rsa_sha224",
        "30820313308201fba00302010202142debbad34a26f4dfe1af48f8d97ed2441ecbaa0a300d06092a864886f70d01010e050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff300d06092a864886f70d01010e05000382010100278ac4a1a64e946a85a45a57ab72ce95872d448bbf6e7255be5022b2375b083c5c7cf5ad85f7986d773a7bcd8152f70ac437c07c93c48f0e4bd41ccf4bfe18c4d4826672a9d38eb4b72f6e83cd8f8a88182053dd0b3cfd7604fc5b3aad5fdcadd0cad8e29eb97a1db37d8d04d72b8d817dfdf33cda40e04384b6149019d39ff6250ca5f9d4370a8f29b2f5af4a2bce26b2e41ff631c068ade8403884f138ada8da5259b029b03810c979cb864d88f5339e632c36732834b683840b45d0d228c471bb933ab082c2436a6fd8fc8add84429e389fe825db97822efe558d4498b80882ba816bbbe14af9b65a0af1ddce61dd1a31ae7ff997b74ee106756401599333",
        "49a2c21bf057fcf9ac09c800097457e3663823bb19541cafeff3996d",
    ),
    (
        "rsa_sha256",
        "30820313308201fba00302010202144d95b0d412f0f3273ee90013efef988f85726f2e300d06092a864886f70d01010b050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff300d06092a864886f70d01010b050003820101003e4acd2e3f4bb565fa47052c765540d40572b8ca6e4b49a46e08a8f4221ccc99b8ad53a68d6d2e0b35d75f620adf319c47ffebc373194686624c2fc9785c0eaff7aca1dc624ea63c0e7cbf3827927d581bc12ddf7380d19d1b6528a7fb6365c90cbc391b84ed94196292dc2dce3cb2e0cedf67bf4ffa7b2b6d3869bf0be021eeae4d0a6bd1a3611dd77a24b3d3986a672c20e1523d1e09d6a9d7e3b4410b8c8f309662e2acd4afecd964702f263f3edf642a49d1400965064d422bbf9ed85c63fe8ace0770c511d72c6e90863b4d8c0087243b45f17421637217eed07732e22f5a4fc2e57a56039ed1e16f2ae4ee7d30d029882ca15f1505a47f7c4c0d28108b",
        "eef63becbb6f0d490c2ae6be65edca443a16edbf5ba23a96e778f7afb1eb28b8",
    ),
    (
        "rsa_sha384",
        "30820313308201fba0030201020214086509ca9e1eb4edf661e41a481c57f9772a5ead300d06092a864886f70d01010c050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff300d06092a864886f70d01010c050003820101003ab171dee2bb704ea93f92371b18ef2184076b86071d1515bf560bec6edd817fd0cabc38f1311025355b00d76691588de9768b20e37da2e936afa466da383e80971371b65438652078db7afe7bbfc22c5d821bbc40f0887e7b6ffbcbaef16b9df0cb804f1cd63e62eb81858307e88edb5b3d599a41d95b4f13075e37dcc11ac4238891de6e1699d77b9c09153ccfb58dea1e7d58b6098e5f67a37ac7a9a864204ed1745d605a9905d649d4e7d387918ff3469e991811c4f3ee359a7f6c967adfec8a18d69edf82840fabdc1dbfdafb208f779bb84598f11f29ce4142acceb840efe8db1e36a893aa7bf9c4cc6f748f4cc4bbbbebf82b8327613db64270d06ff3",
        "5704382aa3dcf1b8a51e34d8499addccbf018c84001c774f946e35333fcd0279835413768e0fd7061c0408d0595c5e2d",
    ),
    (
        "rsa_sha512",
        "30820313308201fba003020102021411e70dcbbeadc7528282693101a53a292964b66c300d06092a864886f70d01010d050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff300d06092a864886f70d01010d050003820101004a0ab73fec2c60353c9de88181f5cc0530dfcc39b6b4b7ef4b1ac2cb7a43d17e88ac48c26c145c42404f6817275de04e4c4a0776315527f38d37fc875640ed3302c8e9e9afb20db132d38e326485145ac631c815ede547013d96f009b44e926687ac6b3c085be0f379ff36078d36cb84bad3075a986d84165a3e8635d7902bb57a90c9c9ee761431847585650f0f8a720810e43737e3b047588350bc825292425295d6ec0e23a0d59e033a132ca6ef943c147691de8660e6f7bd324bc6a3689cdb67f3886036ee942f07b50fe5ea8e42afa7df68753fdd917b575224c361b934ab9a00ba119de01f5f8e5ba0bd5b400bb5a2d319e02f1fcfa0fd14212e831eee",
        "b9b6190e9aaa114d502b4683ec918fe11316d4d095d0ff07e7cdba39ded90474de4dbc63385926d9a9420c7e7e14b32a1ac4564f806c10018adc7494d45a090d",
    ),
    (
        "rsa_sha1",
        "30820313308201fba0030201020214701ab34143738e0b1ed3e31fc940332f7434334e300d06092a864886f70d010105050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff300d06092a864886f70d01010505000382010100736030b47f826473c1b5071481c15d91f4099db729c073e1e9ce157f63e9dd8dcd9aac57c6f32eca58eca58023cd8e4b8de9950187a93ba97587a66d70e26e3b9d956b13821b4dda045257af538933acba38276c943ea34788d54b455eff4bc2b7f5850289e9dc06a8f709dc255762696c0057a656046bc5452b3aa240b9d65017195b0c1a93d616bc497297be85738345a8f8e1d72e63a358b947da5d17cbfecbca3b2c0c7b39495a0cd9a8d9be32a8b74a2431ac911c6d2608f7cbe9ffda7ec93f5c34b684f582cc5be7dbf12aadb1f676f7c1ca2ad56bb66d0d4d79173d11bd18773374001506f2b9206aecc161389d4d3ad0a1b5b4e60232777c7595be57",
        "44b387f4cbfcdaa579da96b08ab59540574a372e2bdcf33e0e5a4012b60cd805",
    ),
    (
        "rsa_md5",
        "30820313308201fba0030201020214407ef7f7e20023570dca1e67423bcb6ab3c60306300d06092a864886f70d010104050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff300d06092a864886f70d010104050003820101008e8c22598cc4c7dc4a9017abc67aedd957073891e0f886bb9c7fe863e7132318a34c053b0182be1d17acfd28879ac7e6ece1b3b801544858e7fc97454131d135cef3536a8771c12a14e1f411362b8f09536a3f4665634e28b1dda0556d74dc775812f130ce2cbe11ceda94021765402b7ba4f2e7b0b8589ae7d6eddd8a7985dc6ed34af1432256a937695c27ad4aab2b9f1331a65fd555e068349d161ef24d3176789e115aabb2a6f22880e2049971b0cda809ce8082fe2003c438c46a91c782214598ae7d704e56f3db15f24ba68d3fb4d25496ca3fd691fcf155dcaae5246585c5c081ed550c39cd650f9c229c63bba9a4335bc9d02eda520043aa5096d68a",
        "4f708f094aa3163b87b7b0d096d71f29d32934c17be96a63143cab51006cc290",
    ),
    (
        "ec_sha224",
        "308201c43082014aa00302010202144490817c06afee5b5b83bbdb737b350291aadae0300a06082a8648ce3d04030130193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f723076301006072a8648ce3d020106052b810400220362000454e407a4a24c3dea916dadf54ab19620f8818cceb243c75d325a20272fda77e9259b3fd4c8f1e0fc214c15dc723ea37191458e4137662e2976cd888f8ddf5fe860f4d30ef10a84c8e573e3fe8cedc401d9f97ee7dbed816029bffdea01245fe9a3533051301d0603551d0e04160414a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c301f0603551d23041830168014a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c300f0603551d130101ff040530030101ff300a06082a8648ce3d0403010368003065023100b7c887afda1583e116cac415693068b71eb3b0dd71895d2222861d5ce1921f651e6e6e38a4f509109c0a35105176291a02300e5362704c3ff250ba86a84b0747158ee54a898a5d2304e14ade7c0521a5c1f5d685c1c3ef294f4ea58f853cbf84b0d8",
        "3f341273cd86476f748e7a2d3eef92253881122d5787acf5b0cbc423",
    ),
    (
        "ec_sha256",
        "308201c43082014aa00302010202140dbe9573926e96306e99319d7e54690a814e24f4300a06082a8648ce3d04030230193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f723076301006072a8648ce3d020106052b810400220362000454e407a4a24c3dea916dadf54ab19620f8818cceb243c75d325a20272fda77e9259b3fd4c8f1e0fc214c15dc723ea37191458e4137662e2976cd888f8ddf5fe860f4d30ef10a84c8e573e3fe8cedc401d9f97ee7dbed816029bffdea01245fe9a3533051301d0603551d0e04160414a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c301f0603551d23041830168014a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c300f0603551d130101ff040530030101ff300a06082a8648ce3d04030203680030650230227dc3e05764309dad037d8bd46acf1399e1dac7172b2552459bad3b15608c6acc3295278ad86b0e69a549d707a64949023100e7d3819ec2c18c9550563ead8457a6a5e11006a6fdb99d5848d22d5a47612bb048c0bb2132fa001889adfae0ec88091c",
        "bb07ca99ce37ab0b94752e344beb5e3595074dea57c1cb9d5138795de6e41e8c",
    ),
    (
        "ec_sha384",
        "308201c53082014aa00302010202140e47bd9e8fac943e9b73c30ec7d4c2246c02f8a5300a06082a8648ce3d04030330193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f723076301006072a8648ce3d020106052b810400220362000454e407a4a24c3dea916dadf54ab19620f8818cceb243c75d325a20272fda77e9259b3fd4c8f1e0fc214c15dc723ea37191458e4137662e2976cd888f8ddf5fe860f4d30ef10a84c8e573e3fe8cedc401d9f97ee7dbed816029bffdea01245fe9a3533051301d0603551d0e04160414a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c301f0603551d23041830168014a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c300f0603551d130101ff040530030101ff300a06082a8648ce3d0403030369003066023100d92b7d282226a54473a71fb49571f006559002d5537895653539a0fa2ba5949c2a8d0bb54758aa50cc4ca97c28711aa3023100fd80b00b7e2dca029d1800ee111b9be648fbc984bf7c5303b5c5655574301da1432ce31c6084af333deec161018749ce",
        "f5161a3d3acdabe930af394602557f9f0638f2eecce6a89406bf913020bfc6ca3304e158852896de0c6b0583018ffbe7",
    ),
    (
        "ec_sha512",
        "308201c33082014aa00302010202140fdace69f4cffd98174ea0c3663b594318dbe866300a06082a8648ce3d04030430193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f723076301006072a8648ce3d020106052b810400220362000454e407a4a24c3dea916dadf54ab19620f8818cceb243c75d325a20272fda77e9259b3fd4c8f1e0fc214c15dc723ea37191458e4137662e2976cd888f8ddf5fe860f4d30ef10a84c8e573e3fe8cedc401d9f97ee7dbed816029bffdea01245fe9a3533051301d0603551d0e04160414a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c301f0603551d23041830168014a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c300f0603551d130101ff040530030101ff300a06082a8648ce3d040304036700306402302e7bf7d8c5d1bdce7eb44ace52c34d0c16242306362a8758ade599eff213f80557a33ddccac0c38c9df1eaa95c09b84702303a1c4c03f2a874b2a373e975e43595c419d75639e8f60fa0eb27c6c9e81e42ea465ab814f093be8ce6886be269f6b4c3",
        "e684e74377b1920a19efcfca076628f3a6af1b3ff421261573efbfddb5be5ec8e7d883de93aed40c40bc13cfe20c4901b63a2f10942db14b065950f86c71a0bb",
    ),
    (
        "ec_sha1",
        "308201c330820149a0030201020214279a06936fd09bbe05e3194674c152f871c8a9bc300906072a8648ce3d040130193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f723076301006072a8648ce3d020106052b810400220362000454e407a4a24c3dea916dadf54ab19620f8818cceb243c75d325a20272fda77e9259b3fd4c8f1e0fc214c15dc723ea37191458e4137662e2976cd888f8ddf5fe860f4d30ef10a84c8e573e3fe8cedc401d9f97ee7dbed816029bffdea01245fe9a3533051301d0603551d0e04160414a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c301f0603551d23041830168014a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c300f0603551d130101ff040530030101ff300906072a8648ce3d04010369003066023100aea600757351c25328a833aeee749324ce20deb3ee59c16aeb8730e8c7837b5c3f9c0f0551fa8824e4a03ac46e9940d00231009b684b16de6433cbad3482ceb5473741f994b19ce4cc04ebaf9b8cde3601ab654a4bcc1bf2814ce01dcb33bd47fd6a09",
        "ce0ec94d86cd0b4c29d50e6ad4e7c7078fdde413a23337c7e11e99c9ebcb5b33",
    ),
    (
        "dsa_sha224",
        "308204783082041da003020102021414fa0970b2ad6d89cca8c48c209e20235d6d527d300b060960864801650304030130193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f72308203463082023906072a8648ce3804013082022c02820101008d2afb72933075b8b88fb5aae5ceb81f59cbc6a8a6e19a8e56dc391f49615524d75d7c92cd5c71f43a7e484e4bfa2352da268787bf24302952938d9a3e1dfd09826c4c2daf63573fdc22107e65cd19c4bb0513f1833fd6510db574368a8f89336aff0ebe921d433312a085119e88200c7ab49625c677a6217326ae812cb4cd08b0b426865648f704559fe44f0042cde0207ba91c8952a2ce90fe2af39efe8cf7677e7d4cf923010c4115f14cdf16a2d2126d32ce5ee8eb56fbe24f725089fff5cf1e8ead18cd0c141ca707bf6c0eb84f29fa67319aae45901f97421918f159a2404d95152c0208540519108570d5d4c212e0ebabb15a7a38bfbecc1b2c3ee845022100d11e64abd3ad35f18bde2abc9d51743c9fdfc5f649a552df04b6c746c65f1c4d028201005b9f11db122348dae8d57845d20c52094724c9d0d1ec800242fa2193f14c42bc90228eac61aa88d20ace66ec1b798d9a6b39a3899240fb10ecd861403db835fa45f6db0973d098239cd75958f55701a3d1d552eb39788c8abcc0dd391cf6e53440323c79e444b17e8b6a5c214f69060d19acfcab20e7e36dfb161bf873ec9ca5f03ad205d53f3795ce70be5a8474e3682717d80bdc6f8fdbb8d3355a4e8a314a55753bf24ed68f0c90a3b8b8778ba7ff4bce4f280f744654235e08df8c4e2e24daaad609be0f4c8d427af5bee57552dfac31be76ee608dcfbf8970fdebeaaabb6b2bd0d44f854a5593a15cb04679e010f8e085cd2b39c4c5bb6f0e1bd06547680382010500028201000b2b0c9942d691cb3fd4b326308db5b310264b0e287f9e392180b7554b63a25a0b092d15f59f277aae8f96cd6b1bf78d4b97b0956cb1d2f8cfe3d2897f72daefd13f42b8d1fce1fdf16846e1ec9b24db274dc84dbc6f62de48dee734ca68e48ace81a19a0cac30c127191b3ca038076d7eb261d1ca3cd6ff01223f8d80dade53a8a99b134ebc19952e4bb18ad3c0f3234b869ad1050b32c0a7fa04d433dd5fbdd3a88902ec328691892ffaf044894f782c4089f5ac953fe9c5546caf7690665e2f4865d922aa5972bd1621e5f50ace57edaf5594860dcd29972cba5e8b19b275510e9481234feafcf53791f74f1f5dbc50c2978292414cbb7e6f94eb9e68c092a3533051301d0603551d0e0416041471bfce71da168ed9a36c306a4091c1c498abb54b301f0603551d2304183016801471bfce71da168ed9a36c306a4091c1c498abb54b300f0603551d130101ff040530030101ff300b06096086480165030403010348003045022100b2d677c32ad9b09d05360977e46a8ad72170464e14fa90b93f8b680c9224e93002203c5cdae397238857e4aedd9a028633407f4cf92479fde047953f181092158115",
        "98e78fbe607dd440fbea321d2117ecdae246e5081b081d63eb64cc2d",
    ),
    (
        "dsa_sha256",
        "308204793082041da00302010202145fc30e7be7449ae855368fe58ec669f7bde4f623300b060960864801650304030230193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f72308203463082023906072a8648ce3804013082022c02820101008d2afb72933075b8b88fb5aae5ceb81f59cbc6a8a6e19a8e56dc391f49615524d75d7c92cd5c71f43a7e484e4bfa2352da268787bf24302952938d9a3e1dfd09826c4c2daf63573fdc22107e65cd19c4bb0513f1833fd6510db574368a8f89336aff0ebe921d433312a085119e88200c7ab49625c677a6217326ae812cb4cd08b0b426865648f704559fe44f0042cde0207ba91c8952a2ce90fe2af39efe8cf7677e7d4cf923010c4115f14cdf16a2d2126d32ce5ee8eb56fbe24f725089fff5cf1e8ead18cd0c141ca707bf6c0eb84f29fa67319aae45901f97421918f159a2404d95152c0208540519108570d5d4c212e0ebabb15a7a38bfbecc1b2c3ee845022100d11e64abd3ad35f18bde2abc9d51743c9fdfc5f649a552df04b6c746c65f1c4d028201005b9f11db122348dae8d57845d20c52094724c9d0d1ec800242fa2193f14c42bc90228eac61aa88d20ace66ec1b798d9a6b39a3899240fb10ecd861403db835fa45f6db0973d098239cd75958f55701a3d1d552eb39788c8abcc0dd391cf6e53440323c79e444b17e8b6a5c214f69060d19acfcab20e7e36dfb161bf873ec9ca5f03ad205d53f3795ce70be5a8474e3682717d80bdc6f8fdbb8d3355a4e8a314a55753bf24ed68f0c90a3b8b8778ba7ff4bce4f280f744654235e08df8c4e2e24daaad609be0f4c8d427af5bee57552dfac31be76ee608dcfbf8970fdebeaaabb6b2bd0d44f854a5593a15cb04679e010f8e085cd2b39c4c5bb6f0e1bd06547680382010500028201000b2b0c9942d691cb3fd4b326308db5b310264b0e287f9e392180b7554b63a25a0b092d15f59f277aae8f96cd6b1bf78d4b97b0956cb1d2f8cfe3d2897f72daefd13f42b8d1fce1fdf16846e1ec9b24db274dc84dbc6f62de48dee734ca68e48ace81a19a0cac30c127191b3ca038076d7eb261d1ca3cd6ff01223f8d80dade53a8a99b134ebc19952e4bb18ad3c0f3234b869ad1050b32c0a7fa04d433dd5fbdd3a88902ec328691892ffaf044894f782c4089f5ac953fe9c5546caf7690665e2f4865d922aa5972bd1621e5f50ace57edaf5594860dcd29972cba5e8b19b275510e9481234feafcf53791f74f1f5dbc50c2978292414cbb7e6f94eb9e68c092a3533051301d0603551d0e0416041471bfce71da168ed9a36c306a4091c1c498abb54b301f0603551d2304183016801471bfce71da168ed9a36c306a4091c1c498abb54b300f0603551d130101ff040530030101ff300b06096086480165030403020349003046022100bcf9a72ffab05a3ac0e67bda426b4f29a711d27d6c27f7a0866137692a61223a022100baff580372feb174ea12a95e80aa2dc14b6526da51c8dd87f090989936953e08",
        "b6f2b87b1d1c1e8a47b282572d45619ad09947025040ab3156ba442e690917e0",
    ),
    (
        "dsa_sha1",
        "308204743082041ba00302010202145cfd7c0ddffdde22c53ab31b64fd16cba462a8fd300906072a8648ce38040330193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f72308203463082023906072a8648ce3804013082022c02820101008d2afb72933075b8b88fb5aae5ceb81f59cbc6a8a6e19a8e56dc391f49615524d75d7c92cd5c71f43a7e484e4bfa2352da268787bf24302952938d9a3e1dfd09826c4c2daf63573fdc22107e65cd19c4bb0513f1833fd6510db574368a8f89336aff0ebe921d433312a085119e88200c7ab49625c677a6217326ae812cb4cd08b0b426865648f704559fe44f0042cde0207ba91c8952a2ce90fe2af39efe8cf7677e7d4cf923010c4115f14cdf16a2d2126d32ce5ee8eb56fbe24f725089fff5cf1e8ead18cd0c141ca707bf6c0eb84f29fa67319aae45901f97421918f159a2404d95152c0208540519108570d5d4c212e0ebabb15a7a38bfbecc1b2c3ee845022100d11e64abd3ad35f18bde2abc9d51743c9fdfc5f649a552df04b6c746c65f1c4d028201005b9f11db122348dae8d57845d20c52094724c9d0d1ec800242fa2193f14c42bc90228eac61aa88d20ace66ec1b798d9a6b39a3899240fb10ecd861403db835fa45f6db0973d098239cd75958f55701a3d1d552eb39788c8abcc0dd391cf6e53440323c79e444b17e8b6a5c214f69060d19acfcab20e7e36dfb161bf873ec9ca5f03ad205d53f3795ce70be5a8474e3682717d80bdc6f8fdbb8d3355a4e8a314a55753bf24ed68f0c90a3b8b8778ba7ff4bce4f280f744654235e08df8c4e2e24daaad609be0f4c8d427af5bee57552dfac31be76ee608dcfbf8970fdebeaaabb6b2bd0d44f854a5593a15cb04679e010f8e085cd2b39c4c5bb6f0e1bd06547680382010500028201000b2b0c9942d691cb3fd4b326308db5b310264b0e287f9e392180b7554b63a25a0b092d15f59f277aae8f96cd6b1bf78d4b97b0956cb1d2f8cfe3d2897f72daefd13f42b8d1fce1fdf16846e1ec9b24db274dc84dbc6f62de48dee734ca68e48ace81a19a0cac30c127191b3ca038076d7eb261d1ca3cd6ff01223f8d80dade53a8a99b134ebc19952e4bb18ad3c0f3234b869ad1050b32c0a7fa04d433dd5fbdd3a88902ec328691892ffaf044894f782c4089f5ac953fe9c5546caf7690665e2f4865d922aa5972bd1621e5f50ace57edaf5594860dcd29972cba5e8b19b275510e9481234feafcf53791f74f1f5dbc50c2978292414cbb7e6f94eb9e68c092a3533051301d0603551d0e0416041471bfce71da168ed9a36c306a4091c1c498abb54b301f0603551d2304183016801471bfce71da168ed9a36c306a4091c1c498abb54b300f0603551d130101ff040530030101ff300906072a8648ce38040303480030450220666edad367f9488c4d290a893e9aa0f334be94fe3520b8ffeefd19a3d9edc4c4022100822d6fe5f3acd9d9f4f324e1c6c9e29d853723fdd119e69e895b4be5beec03a6",
        "05b7af491c3f93bb613745a14d26fe45bba12aaf954c34345a5e1f74f156b76c",
    ),
    (
        "pss_sha1",
        "30820313308201fba003020102021456877fe2c1a7d38be8d90a8a8694d1af40ed3323300d06092a864886f70d01010a300030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff300d06092a864886f70d01010a300003820101005d20262b1867dfa64ac1f2e56276d35316aaf719aac19ec1a26ee825106bb9039df59bf1d268a67704f93f75811275175165fe7750a86a8e3aa537cb62bed2159693928cf6a38f0ea1d5525cec9e63207fec4482c5129090be86c56abec59d6b30757909a1c4cf53459d9aed27591b9e44c022d9829c676f982df3240595665346f5276ea92ab01a574fd8f3f43efa58edac4cb23b5b696b10a4f0619455dd0c97fef560a2020a6b5c1650bce51203e42eab036a7fa04964020aa5015212724a172dc48bce15845e2d396f2602fb6cba7b58acf12737919fe6454605c8dacf415236b1d3d829fcaba52922e3c37aec54935f20a025b60abfa03181d080505fdf",
        "bc8b87cb9720077b74b8208866bed8b8d6118073126dcd2926e450433b180c1d",
    ),
    (
        "pss_sha224",
        "308203713082022aa003020102021417afcaef840bd3a5fb831b6b156d05e59d1c83c0303c06092a864886f70d01010a302fa00f300d06096086480165030402040500a11c301a06092a864886f70d010108300d0609608648016503040204050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff303c06092a864886f70d01010a302fa00f300d06096086480165030402040500a11c301a06092a864886f70d010108300d06096086480165030402040500038201010024df01cacb633c9cd86a2ded8fe1e661a1d06a4db5bb8a9308c9c7be91352b36cd009b79104996cd2fd5bf7ff386deba173fa29c13ea7d8f241fb040f703121734dc9c117b3f25e312fd2d0b7eeb4e7d58f471a8a903501277001d4d2225074db7d6f198e84c231c2e970544849bc7325afe34f514bdd63d33b974051e6ba9c4f215fe635be0d80b459d40174839c6d270179ce9d586bdcb79cc58711e48f5d80891f641445c9eb99579018336921d303acfc1d638658800a30dcbf29fe97ec488d9f316e95b49d09694740c4239133ab956948d4cfff2f1c440fbba16de0a83adf8dba6bfe3c7ad87835e1f57739f750fe17769a2f8245ecbc38a36ee7ad8e5",
        "f3479eba228597971f5d131f233657a862a509a850e06b9ef0d2a3c8",
    ),
    (
        "pss_sha256",
        "308203713082022aa003020102021408af89fa5c15f0ae81c82ffe47c14892bb41a529303c06092a864886f70d01010a302fa00f300d06096086480165030402010500a11c301a06092a864886f70d010108300d0609608648016503040201050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff303c06092a864886f70d01010a302fa00f300d06096086480165030402010500a11c301a06092a864886f70d010108300d06096086480165030402010500038201010021a257ce202bd032ae001ccaa3ec231723df570297d1d5cdc80118928e6821146238b8725bdca1d7d0717ca07d1b5cbd2d9976e04308a5f700c48698a29a575d458ee81fb594d3f5d4790d312617a476b9db806f7ed8a007de19ce6335c48383133e95a670a1331b72642cccb290a5c455a4c6366e6161384196c65e75efae4b23f37bb4047492ad897483953223380cd476f78b5b3a899252e146552975769d8f208714ae4a51d0803b8404cc30867e64820c652f27118ecb5d8c64ae1ad4c149544be4e3605f2a39cc267732cf5cf9397e9c329a133b44de1b8f4d770cba5cfe944e8781814fd054244751f3838b57241d87ff9ccfd8f63b9ef2ecb3484df0",
        "069cdbdf3d7d3189aa8e49fae1534f8316fa4c40296d9ea39e56d4a8c089dd90",
    ),
    (
        "pss_sha384",
        "308203713082022aa00302010202147a949213a56a203f481f2413f9efb3868409876d303c06092a864886f70d01010a302fa00f300d06096086480165030402020500a11c301a06092a864886f70d010108300d0609608648016503040202050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff303c06092a864886f70d01010a302fa00f300d06096086480165030402020500a11c301a06092a864886f70d010108300d0609608648016503040202050003820101001a56fdef4fca642ae4b26bffed4f9d2c838c4d87131377fc4902e2568b0c446605b1bdb3f260ac02abdcabda5018f469807cee2e61478b368a6c18a125d24e922d49c6b18aff1fd743856d9450a86d00b7114d06a61320dffd0b9db8538941087e3270be3f6054ed3ad6980b8d2d5d89a9b603918e5d3bee0352de6f753adf9134459f169e352c4f575bf33a08049558ad889bffc1ae360628caaac51c0317bf8749d650295cc115ddfb8d9dc02888a7c1af95bc98e8ba9c76e1869b23892fed9c4926b32e28e0210421cd8452eeaca95a0404aaa2bc07160a5c1e42a6d9be8dd0b6cb11807ee16320401cafa8e93f704927d18c024d3538510e0bc990bbb30a",
        "573a5ed2c179e5ecf25fa477d3882c14b3395e09e8a001059d5bfa9f0db8335a2b14739378e6505b318887065d5464d6",
    ),
    (
        "pss_sha512",
        "308203713082022aa00302010202145657ba738966784d49b84c206ced67892242cf1b303c06092a864886f70d01010a302fa00f300d06096086480165030402030500a11c301a06092a864886f70d010108300d0609608648016503040203050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff303c06092a864886f70d01010a302fa00f300d06096086480165030402030500a11c301a06092a864886f70d010108300d06096086480165030402030500038201010067f712b1b29284382515daa4eb06eb97026993a04da9e404106998e4f0b2994f9b68d3314b2155716cf5c32d0a657651c1d7e4527a77c7b92cd6fc08857fdc8faf60850836bdd0e7d00a62353ad5247047736978ece745c8aa72cdae9ec66cd86d18f987200431446a213a8f68b464f23a7636fb10f585245c4e12b3bdaf5b35c8b889f817472f9bf6e273da774ccea40f1bc025f50d7c6330345b54ba9a5d2172e7a9da62431a90b973fc30f739f674890c262893da6bdf83954dad80c9e71bc084d385041064403d58161556b7c1a8251fbcdfd876cae7642a864f7ae9849eecbd0f54e01e1764ef20fd03ef8f4a502cd41d7aaf80e998f403ce5a7fad7207",
        "7733505877a7fcc2d1c373f3922f232e7059037f20c9b906be9a1488e3bbcefea24892e6c38090ce479999fbf6637d6242fdfbca879aaee1c918b5006a93d9c9",
    ),
    (
        "pss_divergent",
        "308203713082022aa003020102021441fe5dc5bff0b87d51f807762b25d53ac6ea6a85303c06092a864886f70d01010a302fa00f300d06096086480165030402020500a11c301a06092a864886f70d010108300d0609608648016503040201050030193117301506035504030c0e62696e64696e672d766563746f72301e170d3236313130383030323531395a170d3336313130353030323531395a30193117301506035504030c0e62696e64696e672d766563746f7230820122300d06092a864886f70d01010105000382010f003082010a0282010100a0bc861cb2d81a2333ce574a0804f7b282652b2a162be2c04156934aa774691ed50d7d5f09f4170f24eac8e9626cbbdcdaaea67c5b46d2eea676649457d6cb63be58c589a8a3dbce7889ed5226b8cbd0b7a61b6902afe3dd11be2e201c5768d034ff4846b8379d5a0411a755e9f9d80e4691830a5582936a4db00275e255d8531eee022484e713525a558735e55ad160e50c65afcb72cf7229bd0fb05fce62975a0d7a5086451f8f8ac750a7cb9f20ceecc53b8087687e76d32e53b893cbd5597470d1cc0a4de88207b3724596e1f3ce2d711da5cfd77627544f0f20af007ef16fcb6109885d19a16cb4ed542b9b58391f96448ea47202cceb69959ac3d861330203010001a3533051301d0603551d0e04160414bb13251266192fcb21879053bd3b060f55924ddd301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300f0603551d130101ff040530030101ff303c06092a864886f70d01010a302fa00f300d06096086480165030402020500a11c301a06092a864886f70d010108300d060960864801650304020105000382010100928fb68bcaae65dbfb4c212090634d146a387079bf9cf4cc2643f672c4dc246333f095d1d74077db6c078516fee6fba0d1404192ce64fbd9c7b25a689d9f2cd350ac4c842f576e4a5677a162866145c964207be48f66c3a9d8009171ed6c16949eb144c93d5b72d8a9396c6de2fcc5d640d2f20f09ca149f49e9849013a0de3962168b3b7440fe49ebb8fbcaca58ccd4c787306192983a609e385829f2d64b1bf6c34c89b1790da0617af2fd57b52b1957a1da937788ad051c78ed46f7364753e6695e77f21a9ce1a6e65c7f0714c8eddd6cf832deab9a5ec6bee3f52314a32d3bb8ba6d3e5a022bb71dea2b4dbc41d10fccbf3a4b96f1da732e3a7e415d8b18",
        "7ec7249adc15c659d3e7f56f595c1c4336bf9f799b84a41231f9e19dd6c83884f3f060b3b18c81fadb29546f9d085c78",
    ),
];
const LIVE_LEAF: &str = "308201d230820158a003020102020103300a06082a8648ce3d04030330173115301306035504030c0c696e7465726d656469617465301e170d3236313130383030323531395a170d3336313130353030323531395a30143112301006035504030c096c6f63616c686f73743076301006072a8648ce3d020106052b8104002203620004878b698471ad305205885efc0b5982a8a05c94bb964fe991964e29049568a4eb024fb68996fe520d874b8ac5dd4e19967f8929671a2367441ca7ac1a21f4ccb1ffe578bf340438b6b6d1f2a8b6dd78ad418e1124af9a2f6b3d27b2853f4e019ea37b3079300c0603551d130101ff0402300030140603551d11040d300b82096c6f63616c686f737430130603551d25040c300a06082b06010505070301301d0603551d0e04160414e42a938bdae3e80d3e44280862014c336e82ed65301f0603551d23041830168014a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c300a06082a8648ce3d0403030368003065023100c3fe2a1ccf2d422646c255e156c528be511185cb9d73f3a243dedc03485c92f798cb7a276df32615f68886e3aa2a136c02300d3adbc1f288b16f3e7c358841ac095bbb539a77bb09bdb8e521546956a889a3dcc7b540c9c71637384376cca3c530f9";
const LIVE_INTERMEDIATE: &str = "308202563082013ea003020102020102300d06092a864886f70d01010b0500300f310d300b06035504030c04726f6f74301e170d3236313130383030323531395a170d3336313130353030323531395a30173115301306035504030c0c696e7465726d6564696174653076301006072a8648ce3d020106052b810400220362000454e407a4a24c3dea916dadf54ab19620f8818cceb243c75d325a20272fda77e9259b3fd4c8f1e0fc214c15dc723ea37191458e4137662e2976cd888f8ddf5fe860f4d30ef10a84c8e573e3fe8cedc401d9f97ee7dbed816029bffdea01245fe9a3633061300f0603551d130101ff040530030101ff300e0603551d0f0101ff040403020106301d0603551d0e04160414a0d55fcd1e350b4ea089d94fff7cafa1e63bc88c301f0603551d23041830168014bb13251266192fcb21879053bd3b060f55924ddd300d06092a864886f70d01010b050003820101001ba7ca8cba854d579d86e05e22d18c551b6339362c0fc3ad2ce2dfc56c2fa0e594627815032a9fae0d488b2d4fa83dcf2bd9627bca6a72319691a52dd75ace88697e84fd0e0234d4be40bf80f9b80bee1b32cbc6ac0348f5527f741ffbfc080e90275daffd3af4cead42ac70330d33028ded56c0362576283e03e7e6e4ff413a2d04aa8279d38223d981388f007d6a2cc3f683d336adc8e76dd8c28cce69cab37dd6e79a5dbebc4737432230863b852eaf704b2d1e75d2e5e6a36aa737a1d8fedb636ff430d69d775b7ba6643e4fabc8ab32bb3bb44dce28604c4ea7fe889b3a26a95c6f6217883c7a509784096ecd717027c6fd91645f8afd3265a73e92867a";
const LIVE_KEY: &str = "3081b6020100301006072a8648ce3d020106052b8104002204819e30819b02010104304337a139fa345a3609faa4273126a32f4b3f913fe95c7d81c865baf85a7d42552a4c60d64e4ac1914c926d2b938b0658a16403620004878b698471ad305205885efc0b5982a8a05c94bb964fe991964e29049568a4eb024fb68996fe520d874b8ac5dd4e19967f8929671a2367441ca7ac1a21f4ccb1ffe578bf340438b6b6d1f2a8b6dd78ad418e1124af9a2f6b3d27b2853f4e019e";
const LIVE_DIGEST: &str = "3ba4e3efe0683ae90046385f2d8d48e951039f53c68ab6c6f3f3d8291cf71b63ef84c969bada2a9039cd722af8260403";

fn check_vectors(select: impl Fn(&str) -> bool) {
    for &(name, encoded, expected) in SIGNED_VECTORS.iter().filter(|v| select(v.0)) {
        let der = hex::decode(encoded).unwrap();
        let expected = hex::decode(expected).unwrap();
        assert_eq!(tls_server_end_point(&der).unwrap(), expected, "{name}");
        let (_, cert) = x509_parser::parse_x509_certificate(&der).unwrap();
        let spki = cert.tbs_certificate.subject_pki.raw;
        let spki_digest = match expected.len() {
            28 => Sha224::digest(spki).to_vec(),
            32 => Sha256::digest(spki).to_vec(),
            48 => Sha384::digest(spki).to_vec(),
            64 => Sha512::digest(spki).to_vec(),
            _ => panic!("unexpected digest size"),
        };
        assert_ne!(
            expected, spki_digest,
            "{name} must hash the entire certificate"
        );
    }
}

#[test]
fn certificate_binding_rsa_ecdsa_dsa_hashes() {
    check_vectors(|name| {
        !name.starts_with("pss") && !name.ends_with("sha1") && !name.ends_with("md5")
    });
}

#[test]
fn certificate_binding_sha1_md5_substitution() {
    check_vectors(|name| {
        !name.starts_with("pss") && (name.ends_with("sha1") || name.ends_with("md5"))
    });
}

#[test]
fn certificate_binding_rsa_pss_explicit_and_default() {
    check_vectors(|name| name.starts_with("pss"));
    let der = hex::decode(SIGNED_VECTORS.iter().find(|v| v.0 == "pss_sha1").unwrap().1).unwrap();
    let (_, cert) = x509_parser::parse_x509_certificate(&der).unwrap();
    assert!(cert.signature_algorithm.parameters.unwrap().data.is_empty());
}

fn der_value(tag: u8, content: &[u8]) -> Vec<u8> {
    let mut encoded = vec![tag];
    if content.len() < 128 {
        encoded.push(content.len() as u8);
    } else {
        let bytes = content.len().to_be_bytes();
        let bytes = &bytes[bytes.iter().position(|b| *b != 0).unwrap()..];
        encoded.push(0x80 | bytes.len() as u8);
        encoded.extend(bytes);
    }
    encoded.extend(content);
    encoded
}

// Replacing the outer signature algorithm deliberately invalidates the signature.
// These synthetic certificates test refusal only and never enter TLS handshakes.
fn refusal_certificate(algorithm: &[u8]) -> Vec<u8> {
    let der = hex::decode(SIGNED_VECTORS[0].1).unwrap();
    let (_, outer) = Any::from_der(&der).unwrap();
    let (after_tbs, _) = Any::from_der(outer.data).unwrap();
    let tbs_len = outer.data.len() - after_tbs.len();
    let (signature, _) = Any::from_der(after_tbs).unwrap();
    let content = [&outer.data[..tbs_len], algorithm, signature].concat();
    der_value(0x30, &content)
}

fn pss_algorithm(parameters: &[u8]) -> Vec<u8> {
    der_value(
        0x30,
        &[
            hex::decode("06092a864886f70d01010a").unwrap(),
            parameters.to_vec(),
        ]
        .concat(),
    )
}

#[test]
fn certificate_binding_eddsa_and_unknown_unavailable() {
    for oid in [
        "06092a864886f70d010102",
        "06032b6570",
        "06032b6571",
        "06042a030405",
    ] {
        let der = refusal_certificate(&der_value(0x30, &hex::decode(oid).unwrap()));
        assert!(
            matches!(
                tls_server_end_point(&der),
                Err(PostgresError::UnsupportedAuth(_))
            ),
            "{oid}"
        );
    }
    for parameters in [
        "",                                           // Whole parameters field absent.
        "0500",                                       // Not a SEQUENCE.
        "30020500",                                   // Unconsumed field.
        "3005a003300100",                             // Malformed nested AlgorithmIdentifier.
        "300aa008300606042a030405",                   // Unknown signature hash.
        "3013a011300f060960864801650304020105000500", // Trailing inner algorithm value.
        "3013a011300d060960864801650304020105000500", // Trailing explicit wrapper value.
        "301fa11d301b06092a864886f70d010108300e0609608648016503040201050000", // Nested MGF trailing byte.
        "3007a2050201140500",       // Trailing salt value.
        "300aa203020114a203020114", // Duplicate field.
        "300aa303020101a203020114", // Out-of-order fields.
    ] {
        let der = refusal_certificate(&pss_algorithm(&hex::decode(parameters).unwrap()));
        assert!(
            matches!(
                tls_server_end_point(&der),
                Err(PostgresError::UnsupportedAuth(_))
            ),
            "PSS {parameters}"
        );
    }
}

#[test]
fn certificate_binding_malformed_and_trailing_der() {
    let valid = hex::decode(SIGNED_VECTORS[0].1).unwrap();
    for der in [
        vec![],
        valid[..valid.len() - 1].to_vec(),
        [valid.as_slice(), &[0]].concat(),
    ] {
        assert!(matches!(
            tls_server_end_point(&der),
            Err(PostgresError::Protocol(_))
        ));
    }
}

#[tokio::test]
async fn certificate_binding_live_tls_leaf_vs_chain() {
    let mut pki = Pki::new("localhost", false);
    pki.server_cert = hex::decode(LIVE_LEAF).unwrap().into();
    pki.server_key = hex::decode(LIVE_KEY).unwrap();
    pki.chain = vec![hex::decode(LIVE_INTERMEDIATE).unwrap().into()];
    let (_, leaf) = x509_parser::parse_x509_certificate(pki.server_cert.as_ref()).unwrap();
    assert_ne!(
        hex::decode(LIVE_DIGEST).unwrap(),
        Sha384::digest(leaf.tbs_certificate.subject_pki.raw).to_vec()
    );
    assert_ne!(
        hex::decode(LIVE_DIGEST).unwrap(),
        Sha384::digest([pki.server_cert.as_ref(), pki.chain[0].as_ref()].concat()).to_vec()
    );
    let (listener, mut config) = support::listener().await;
    config.sslmode = TlsMode::Require;
    tls_case_with_binding(
        config,
        listener,
        &pki,
        false,
        true,
        Some(hex::decode(LIVE_DIGEST).unwrap()),
    )
    .await;

    let params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    let key = KeyPair::generate_for(&rcgen::PKCS_ED25519).unwrap();
    pki.server_cert = params.self_signed(&key).unwrap().der().clone();
    pki.server_key = key.serialize_der();
    pki.chain.clear();
    let (listener, mut config) = support::listener().await;
    config.sslmode = TlsMode::Require;
    tls_case_with_binding(config, listener, &pki, false, true, None).await;
}
