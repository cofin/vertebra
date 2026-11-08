mod support;

use _postgres::{
    config::ChannelBindingPolicy,
    errors::PostgresError,
    session::{BeginConnect, SessionHandle},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::TcpStream, sync::oneshot};

const MD5_RESPONSE: &[u8] = b"p\0\0\0\x28md5c73cff48cd454994b0c263a04cdfc859\0";

fn challenge(code: i32) -> Vec<u8> {
    let mut body = code.to_be_bytes().to_vec();
    if code == 5 {
        body.extend_from_slice(&[1, 2, 3, 4]);
    }
    support::frame(b'R', &body)
}

#[tokio::test]
async fn scram_plus_policy_matrix_plaintext() {
    for policy in [
        ChannelBindingPolicy::Disable,
        ChannelBindingPolicy::Prefer,
        ChannelBindingPolicy::Require,
    ] {
        for offer in 0..6 {
            let (listener, mut config) = support::listener().await;
            config.channel_binding = policy;
            config.password = Some("secret".into());
            config.connect_timeout = Duration::from_secs(30);
            let rejected_policy = policy == ChannelBindingPolicy::Require || offer == 1;
            let (done_tx, done_rx) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                if policy == ChannelBindingPolicy::Require {
                    let mut byte = [0];
                    assert_eq!(
                        tokio::time::timeout(Duration::from_secs(30), stream.read(&mut byte))
                            .await
                            .unwrap()
                            .unwrap(),
                        0,
                        "credential bytes on closed session"
                    );
                } else {
                    support::startup(&mut stream).await;
                    let frame = match offer {
                        0 => scram_offer(true),
                        1 => sasl(10, b"SCRAM-SHA-256-PLUS\0\0"),
                        2 => scram_offer(false),
                        3 => support::trust(),
                        4 => challenge(3),
                        5 => challenge(5),
                        _ => unreachable!(),
                    };
                    support::send(&mut stream, &frame).await;
                    match offer {
                        0 | 2 => {
                            let first = scram_initial(&mut stream).await;
                            let server = server_first(&first, 4096);
                            support::send(&mut stream, &sasl(11, server.as_bytes())).await;
                            let proof = scram_response(&mut stream, &first, &server, 4096).await;
                            support::send(
                                &mut stream,
                                &[sasl(12, proof.as_bytes()), support::trust()].concat(),
                            )
                            .await;
                        }
                        4 | 5 => {
                            let expected = if offer == 4 {
                                b"secret".to_vec()
                            } else {
                                postgres_protocol::authentication::md5_hash(
                                    b"app",
                                    b"secret",
                                    [1, 2, 3, 4],
                                )
                                .into_bytes()
                            };
                            let response = password_frame(&mut stream).await;
                            assert_eq!(&response[5..response.len() - 1], expected);
                            support::send(&mut stream, &support::trust()).await;
                        }
                        _ => {}
                    }
                    let mut byte = [0];
                    assert_eq!(
                        tokio::time::timeout(Duration::from_secs(30), stream.read(&mut byte))
                            .await
                            .unwrap()
                            .unwrap(),
                        0,
                        "credential bytes on closed session"
                    );
                }
                done_rx.await.unwrap();
                assert!(
                    matches!(listener.into_std().unwrap().accept(), Err(error) if error.kind() == std::io::ErrorKind::WouldBlock)
                );
            });
            let handle = SessionHandle::new(config);
            let result = tokio::time::timeout(Duration::from_secs(30), connect(&handle))
                .await
                .unwrap();
            if rejected_policy {
                assert!(matches!(
                    rejected(result, &handle),
                    PostgresError::UnsupportedAuth(_)
                ));
            } else {
                result.unwrap().commit().unwrap();
                assert!(!handle.is_closed());
            }
            drop(handle);
            done_tx.send(()).unwrap();
            tokio::time::timeout(Duration::from_secs(30), peer)
                .await
                .unwrap()
                .unwrap();
        }
    }
}

async fn password_frame(stream: &mut TcpStream) -> Vec<u8> {
    assert_eq!(stream.read_u8().await.unwrap(), b'p');
    let len = stream.read_i32().await.unwrap();
    assert!((5..=16_384).contains(&len));
    let mut bytes = vec![b'p'];
    bytes.extend_from_slice(&len.to_be_bytes());
    bytes.resize(len as usize + 1, 0);
    stream.read_exact(&mut bytes[5..]).await.unwrap();
    bytes
}

async fn connect(
    handle: &SessionHandle,
) -> Result<_postgres::session::PendingSession, PostgresError> {
    let BeginConnect::Attempt(attempt) = handle.begin_connect().unwrap() else {
        panic!("expected fresh attempt")
    };
    attempt.run().await
}

fn rejected(
    result: Result<_postgres::session::PendingSession, PostgresError>,
    handle: &SessionHandle,
) -> PostgresError {
    let error = match result {
        Err(error) => error,
        Ok(_) => panic!("authentication unexpectedly succeeded"),
    };
    assert!(handle.is_closed());
    assert!(handle.parameters().is_empty());
    assert!(handle.begin_connect().is_err());
    error
}

fn assert_redacted(error: &PostgresError) {
    for secret in ["ultra-secret-pw", "md5c73cff48cd454994b0c263a04cdfc859"] {
        assert!(!error.to_string().contains(secret));
        assert!(!format!("{error:?}").contains(secret));
    }
}

fn assert_response_redacted(error: &PostgresError, response: &[u8]) {
    let credential = std::str::from_utf8(&response[5..response.len() - 1]).unwrap();
    assert!(!error.to_string().contains(credential));
    assert!(!format!("{error:?}").contains(credential));
}

#[tokio::test]
async fn password_cleartext_success() {
    let (listener, mut config) = support::listener().await;
    config.password = Some("s3cr3t!".into());
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        let notice = support::frame(b'N', b"SNOTICE\0Mstartup notice\0\0");
        support::send(&mut stream, &[notice.clone(), challenge(3)].concat()).await;
        assert_eq!(password_frame(&mut stream).await, b"p\0\0\0\x0cs3cr3t!\0");
        support::send(
            &mut stream,
            &[
                notice.clone(),
                challenge(0),
                notice,
                support::parameter("client_encoding", "UTF8"),
                support::frame(b'Z', b"I"),
            ]
            .concat(),
        )
        .await;
        let mut terminate = [0; 5];
        stream.read_exact(&mut terminate).await.unwrap();
        assert_eq!(&terminate, b"X\0\0\0\x04");
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    connect(&handle).await.unwrap().commit().unwrap();
    assert!(!handle.is_closed());
    assert_eq!(handle.parameters()["client_encoding"], "UTF8");
    handle.close().await.unwrap();
    peer.await.unwrap();
}

#[tokio::test]
async fn password_md5_success() {
    let (listener, mut config) = support::listener().await;
    config.user = "postgres".into();
    config.password = Some("hunter2".into());
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let len = stream.read_i32().await.unwrap();
        assert!((8..1024).contains(&len));
        let mut startup = vec![0; len as usize - 4];
        stream.read_exact(&mut startup).await.unwrap();
        assert_eq!(&startup, b"\0\x03\0\0user\0postgres\0database\0prod\0application_name\0vertebra\0client_encoding\0UTF8\0\0");
        let notice = support::frame(b'N', b"SNOTICE\0Mstartup notice\0\0");
        support::send(&mut stream, &[notice.clone(), challenge(5)].concat()).await;
        assert_eq!(password_frame(&mut stream).await, MD5_RESPONSE);
        support::send(&mut stream, &[notice, support::trust()].concat()).await;
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    connect(&handle).await.unwrap().commit().unwrap();
    assert!(!handle.is_closed());
    drop(handle);
    peer.await.unwrap();
}

#[tokio::test]
async fn password_missing_rejected() {
    for code in [3, 5] {
        let (listener, config) = support::listener().await;
        let peer = tokio::spawn(async move {
            let mut stream = support::accept(listener).await;
            support::send(&mut stream, &challenge(code)).await;
            support::eof(&mut stream).await;
        });
        let handle = SessionHandle::new(config);
        let error = rejected(connect(&handle).await, &handle);
        assert!(matches!(error, PostgresError::Config(_)));
        assert_redacted(&error);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn password_wrong_28p01_and_redaction() {
    for code in [3, 5] {
        let (listener, mut config) = support::listener().await;
        config.password = Some("ultra-secret-pw".into());
        let peer = tokio::spawn(async move {
            let mut stream = support::accept(listener).await;
            support::send(&mut stream, &challenge(code)).await;
            let response = password_frame(&mut stream).await;
            support::send(
                &mut stream,
                &support::frame(
                    b'E',
                    b"SFATAL\0C28P01\0Mpassword authentication failed for user \"app\"\0\0",
                ),
            )
            .await;
            support::eof(&mut stream).await;
            response
        });
        let handle = SessionHandle::new(config);
        let error = rejected(connect(&handle).await, &handle);
        assert!(matches!(&error, PostgresError::Server(server) if server.sqlstate == "28P01"));
        assert_redacted(&error);
        assert_response_redacted(&error, &peer.await.unwrap());
    }
}

async fn server_echo(code: i32) {
    let (listener, mut config) = support::listener().await;
    config.password = Some("ultra-secret-pw".into());
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &challenge(code)).await;
        let response = password_frame(&mut stream).await;
        let credential = std::str::from_utf8(&response[5..response.len() - 1]).unwrap();
        let fields = format!(
            "SFATAL\0VFATAL\0C28P01\0Mrejected ultra-secret-pw and {credential}\0Ddetail {credential} / ultra-secret-pw\0Hsafe retry advice\0Wcontext ultra-secret-pw / {credential}\0P17\0\0"
        );
        support::send(&mut stream, &support::frame(b'E', fields.as_bytes())).await;
        support::eof(&mut stream).await;
        response
    });
    let handle = SessionHandle::new(config);
    let error = rejected(connect(&handle).await, &handle);
    let response = peer.await.unwrap();
    assert_response_redacted(&error, &response);
    assert_redacted(&error);
    let PostgresError::Server(server) = error else {
        panic!("expected typed server rejection")
    };
    assert_eq!(server.sqlstate, "28P01");
    assert_eq!(server.severity, "FATAL");
    assert_eq!(server.message, "rejected <redacted> and <redacted>");
    assert!(
        server
            .diagnostics
            .contains(&(b'H', "safe retry advice".into()))
    );
    assert!(server.diagnostics.contains(&(b'P', "17".into())));
    assert!(
        server
            .diagnostics
            .contains(&(b'D', "detail <redacted> / <redacted>".into()))
    );
    assert!(
        server
            .diagnostics
            .contains(&(b'W', "context <redacted> / <redacted>".into()))
    );
}

#[tokio::test]
async fn password_server_echo_cleartext_is_redacted() {
    server_echo(3).await;
}

#[tokio::test]
async fn password_server_echo_md5_is_redacted() {
    server_echo(5).await;
}

#[tokio::test]
async fn password_server_echo_before_challenge_is_redacted() {
    let (listener, mut config) = support::listener().await;
    config.password = Some("ultra-secret-pw".into());
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(
            &mut stream,
            &support::frame(b'E', b"SFATAL\0C28P01\0Mrejected ultra-secret-pw\0\0"),
        )
        .await;
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    let error = rejected(connect(&handle).await, &handle);
    peer.await.unwrap();
    assert_eq!(error.sqlstate(), Some("28P01"));
    assert_redacted(&error);
}

#[tokio::test]
async fn password_redaction_preserves_protocol_fields() {
    for password in ["17", "FATAL"] {
        let (listener, mut config) = support::listener().await;
        config.password = Some(password.into());
        let peer = tokio::spawn(async move {
            let mut stream = support::accept(listener).await;
            support::send(&mut stream, &challenge(3)).await;
            let response = password_frame(&mut stream).await;
            assert_eq!(&response[5..response.len() - 1], password.as_bytes());
            let fields = format!(
                "SFATAL\0VFATAL\0C28P01\0Mrejected {password}\0Ddetail {password}\0P17\0p17\0L17\0\0"
            );
            support::send(&mut stream, &support::frame(b'E', fields.as_bytes())).await;
            support::eof(&mut stream).await;
        });
        let handle = SessionHandle::new(config);
        let error = rejected(connect(&handle).await, &handle);
        peer.await.unwrap();
        let PostgresError::Server(server) = error else {
            panic!("expected typed server rejection")
        };
        assert_eq!(server.sqlstate, "28P01");
        assert_eq!(server.severity, "FATAL");
        assert_eq!(server.message, "rejected <redacted>");
        for (tag, value) in [
            (b'S', "FATAL"),
            (b'V', "FATAL"),
            (b'C', "28P01"),
            (b'P', "17"),
            (b'p', "17"),
            (b'L', "17"),
            (b'D', "detail <redacted>"),
        ] {
            assert!(server.diagnostics.contains(&(tag, value.into())));
        }
    }
}

#[tokio::test]
async fn password_empty_secret_preserves_server_text() {
    let (listener, mut config) = support::listener().await;
    config.password = Some(String::new());
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &challenge(3)).await;
        assert_eq!(password_frame(&mut stream).await, b"p\0\0\0\x05\0");
        support::send(
            &mut stream,
            &support::frame(b'E', b"SFATAL\0C28P01\0Mempty credentials refused\0\0"),
        )
        .await;
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    let error = rejected(connect(&handle).await, &handle);
    peer.await.unwrap();
    assert!(
        matches!(error, PostgresError::Server(server) if server.message == "empty credentials refused")
    );
}

async fn after_password(frames: Vec<u8>, code: i32) -> PostgresError {
    let (listener, mut config) = support::listener().await;
    config.password = Some("ultra-secret-pw".into());
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &challenge(code)).await;
        let response = password_frame(&mut stream).await;
        support::send(&mut stream, &frames).await;
        support::eof(&mut stream).await;
        response
    });
    let handle = SessionHandle::new(config);
    let error = rejected(connect(&handle).await, &handle);
    assert_redacted(&error);
    assert_response_redacted(&error, &peer.await.unwrap());
    error
}

#[tokio::test]
async fn password_duplicate_challenge_or_ok() {
    for first in [3, 5] {
        for frames in [
            challenge(3),
            challenge(5),
            [challenge(0), challenge(0)].concat(),
            [challenge(0), challenge(3)].concat(),
            [challenge(0), challenge(5)].concat(),
        ] {
            assert!(matches!(
                after_password(frames, first).await,
                PostgresError::Protocol(_)
            ));
        }
    }
}

#[tokio::test]
async fn password_metadata_requires_authentication_ok() {
    for frame in [
        support::parameter("client_encoding", "UTF8"),
        support::frame(b'K', &[0; 8]),
        support::frame(b'Z', b"I"),
    ] {
        assert!(matches!(
            after_password(frame, 3).await,
            PostgresError::Protocol(_)
        ));
    }
}

#[tokio::test]
async fn password_unsupported_mechanisms() {
    for (code, payload) in [
        (2, b"".as_slice()),
        (6, b""),
        (7, b""),
        (8, b"token"),
        (9, b""),
    ] {
        let (listener, mut config) = support::listener().await;
        config.password = Some("ultra-secret-pw".into());
        let peer = tokio::spawn(async move {
            let mut stream = support::accept(listener).await;
            let mut body = i32::to_be_bytes(code).to_vec();
            body.extend_from_slice(payload);
            support::send(&mut stream, &support::frame(b'R', &body)).await;
            support::eof(&mut stream).await;
        });
        let handle = SessionHandle::new(config);
        let error = rejected(connect(&handle).await, &handle);
        assert!(matches!(error, PostgresError::UnsupportedAuth(_)));
        assert_redacted(&error);
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn password_unexpected_sasl_continuation() {
    for code in [11_i32, 12] {
        for prefix in [vec![], challenge(0)] {
            let (listener, config) = support::listener().await;
            let peer = tokio::spawn(async move {
                let mut stream = support::accept(listener).await;
                support::send(
                    &mut stream,
                    &[prefix, support::frame(b'R', &code.to_be_bytes())].concat(),
                )
                .await;
                support::eof(&mut stream).await;
            });
            let handle = SessionHandle::new(config);
            assert!(matches!(
                rejected(connect(&handle).await, &handle),
                PostgresError::Protocol(_)
            ));
            peer.await.unwrap();
        }
        assert!(matches!(
            after_password(support::frame(b'R', &code.to_be_bytes()), 3).await,
            PostgresError::Protocol(_)
        ));
    }
}

fn sasl(code: i32, data: &[u8]) -> Vec<u8> {
    support::frame(b'R', &[code.to_be_bytes().as_slice(), data].concat())
}

fn scram_offer(both: bool) -> Vec<u8> {
    sasl(
        10,
        if both {
            b"SCRAM-SHA-256-PLUS\0SCRAM-SHA-256\0\0"
        } else {
            b"SCRAM-SHA-256\0\0"
        },
    )
}

async fn scram_initial(stream: &mut TcpStream) -> String {
    let frame = password_frame(stream).await;
    let body = &frame[5..];
    let end = body.iter().position(|byte| *byte == 0).unwrap();
    assert_eq!(&body[..end], b"SCRAM-SHA-256");
    let len = i32::from_be_bytes(body[end + 1..end + 5].try_into().unwrap());
    assert_eq!(len as usize, body.len() - end - 5);
    let first = std::str::from_utf8(&body[end + 5..]).unwrap();
    assert!(first.starts_with("n,,n=,r="));
    assert!(!first[8..].is_empty());
    first[3..].to_owned()
}

fn server_first(first: &str, iterations: u32) -> String {
    format!(
        "r={}server,s=dnRiLXRlc3Qtc2FsdA==,i={iterations}",
        first.strip_prefix("n=,r=").unwrap()
    )
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().into()
}

fn salted_password(iterations: u32) -> [u8; 32] {
    let mut previous = hmac(b"secret", b"vtb-test-salt\0\0\0\x01");
    let mut result = previous;
    for _ in 1..iterations {
        previous = hmac(b"secret", &previous);
        for (output, byte) in result.iter_mut().zip(previous) {
            *output ^= byte;
        }
    }
    if iterations == 4096 {
        assert_eq!(
            hex::encode(result),
            "2482c2af90bd00962845709770664bb70a090bcbd937e9d1fa1f855044d513bb"
        );
    }
    result
}

async fn scram_response(
    stream: &mut TcpStream,
    first: &str,
    server: &str,
    iterations: u32,
) -> String {
    let frame = password_frame(stream).await;
    let response = std::str::from_utf8(&frame[5..]).unwrap();
    let (bare, proof) = response.rsplit_once(",p=").unwrap();
    assert_eq!(
        bare,
        format!("c=biws,r={}server", first.strip_prefix("n=,r=").unwrap())
    );
    let auth = format!("{first},{server},{bare}");
    let salted = salted_password(iterations);
    let client_key = hmac(&salted, b"Client Key");
    let stored_key = Sha256::digest(client_key);
    let signature = hmac(&stored_key, auth.as_bytes());
    let expected: Vec<_> = client_key
        .into_iter()
        .zip(signature)
        .map(|(key, mac)| key ^ mac)
        .collect();
    assert_eq!(STANDARD.decode(proof).unwrap(), expected);
    format!(
        "v={}",
        STANDARD.encode(hmac(&hmac(&salted, b"Server Key"), auth.as_bytes()))
    )
}

async fn scram_success(iterations: u32, both: bool, oracle: bool, notices: bool) {
    let (listener, mut config) = support::listener().await;
    config.password = Some("secret".into());
    config.connect_timeout = Duration::from_secs(30);
    let port = config.port;
    let peer = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let len = stream.read_i32().await.unwrap();
        assert!((8..1024).contains(&len));
        let mut startup = vec![0; len as usize - 4];
        stream.read_exact(&mut startup).await.unwrap();
        assert_eq!(&startup[..4], &[0, 3, 0, 0]);
        let fields: Vec<_> = startup[4..].split(|byte| *byte == 0).collect();
        let fields: std::collections::HashMap<_, _> = fields
            .as_chunks::<2>()
            .0
            .iter()
            .take_while(|pair| !pair[0].is_empty())
            .map(|pair| (pair[0], pair[1]))
            .collect();
        for (key, value) in [
            (b"user".as_slice(), b"app".as_slice()),
            (b"database", b"prod"),
            (b"client_encoding", b"UTF8"),
            (b"application_name", b"vertebra"),
        ] {
            assert_eq!(fields[key], value);
        }
        let notice = if notices {
            support::frame(b'N', b"SNOTICE\0Mauthentication progress\0\0")
        } else {
            Vec::new()
        };
        support::send(&mut stream, &[notice.clone(), scram_offer(both)].concat()).await;
        let first = scram_initial(&mut stream).await;
        let server = server_first(&first, iterations);
        support::send(
            &mut stream,
            &[notice.clone(), sasl(11, server.as_bytes())].concat(),
        )
        .await;
        let final_message = scram_response(&mut stream, &first, &server, iterations).await;
        support::send(
            &mut stream,
            &[
                notice.clone(),
                sasl(12, final_message.as_bytes()),
                notice,
                support::trust(),
            ]
            .concat(),
        )
        .await;
        support::eof(&mut stream).await;
    });
    if oracle {
        let mut reference = tokio_postgres::Config::new();
        reference
            .host("127.0.0.1")
            .port(port)
            .user("app")
            .dbname("prod")
            .application_name("vertebra")
            .password("secret")
            .ssl_mode(tokio_postgres::config::SslMode::Disable);
        let (client, connection) = reference.connect(tokio_postgres::NoTls).await.unwrap();
        drop((client, connection));
    } else {
        let handle = SessionHandle::new(config);
        connect(&handle).await.unwrap().commit().unwrap();
        assert_eq!(handle.parameters()["client_encoding"], "UTF8");
        assert!(!handle.is_closed());
        drop(handle);
    }
    peer.await.unwrap();
}

#[tokio::test]
async fn scram_sha256_golden_and_oracle() {
    for both in [false, true] {
        for oracle in [true, false] {
            scram_success(4096, both, oracle, false).await;
        }
    }
}

#[tokio::test]
async fn scram_accepts_notices_in_all_phases() {
    scram_success(4096, false, false, true).await;
}

#[tokio::test]
async fn scram_iteration_boundaries() {
    for iterations in [1, 100_000] {
        scram_success(iterations, false, false, false).await;
    }
}

#[tokio::test]
async fn scram_mechanism_list_rejections() {
    for (offer, protocol) in [
        (b"\0".as_slice(), true),
        (b"\xff\0\0", true),
        (b"SCRAM-SHA-256\0", true),
        (b"SCRAM-SHA-256\0\xff\0\0", true),
        (b"SCRAM-SHA-256\0\0garbage", true),
        (b"SCRAM-SHA-256-PLUS\0\0", false),
        (b"GSSAPI\0\0", false),
    ] {
        let (listener, mut config) = support::listener().await;
        config.password = Some("secret".into());
        let peer = tokio::spawn(async move {
            let mut stream = support::accept(listener).await;
            support::send(&mut stream, &sasl(10, offer)).await;
            support::eof(&mut stream).await;
        });
        let handle = SessionHandle::new(config);
        let error = rejected(connect(&handle).await, &handle);
        if protocol {
            assert!(matches!(error, PostgresError::Protocol(_)));
        } else {
            assert!(matches!(error, PostgresError::UnsupportedAuth(_)));
        }
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn scram_missing_password_is_local() {
    let (listener, config) = support::listener().await;
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &scram_offer(false)).await;
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    assert!(matches!(
        rejected(connect(&handle).await, &handle),
        PostgresError::Config(_)
    ));
    peer.await.unwrap();
}

async fn invalid_server_first(iterations: &str, nonce: &str) {
    let (listener, mut config) = support::listener().await;
    config.password = Some("secret".into());
    let iterations = iterations.to_owned();
    let nonce = nonce.to_owned();
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &scram_offer(false)).await;
        let first = scram_initial(&mut stream).await;
        let client_nonce = first.strip_prefix("n=,r=").unwrap();
        let server_nonce = match nonce.as_str() {
            "equal" => client_nonce.to_owned(),
            "wrong" => format!("wrong{client_nonce}server"),
            _ => format!("{client_nonce}server"),
        };
        let server = format!("r={server_nonce},s=dnRiLXRlc3Qtc2FsdA==,i={iterations}");
        support::send(&mut stream, &sasl(11, server.as_bytes())).await;
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    let error = rejected(connect(&handle).await, &handle);
    assert!(matches!(error, PostgresError::Protocol(_)));
    assert!(!format!("{error:?}").contains("secret"));
    peer.await.unwrap();
}

#[tokio::test]
async fn scram_iteration_bounds() {
    for value in [
        "0",
        "100001",
        "4096abc",
        "+4096",
        "-4096",
        "42949672960",
        "4096,i=4096",
        "",
    ] {
        invalid_server_first(value, "valid").await;
    }
}

#[tokio::test]
async fn scram_nonce_strict_extension() {
    for nonce in ["equal", "wrong"] {
        invalid_server_first("4096", nonce).await;
    }
}

async fn scram_after_initial(stage: &str) {
    let (listener, mut config) = support::listener().await;
    config.password = Some("secret".into());
    let stage = stage.to_owned();
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &scram_offer(false)).await;
        let first = scram_initial(&mut stream).await;
        let server = server_first(&first, 4096);
        let bad = match stage.as_str() {
            "initial-ok" => challenge(0),
            "initial-final" => sasl(12, b"v=ZmFrZQ=="),
            "initial-challenge" => scram_offer(false),
            "initial-password" => challenge(3),
            "initial-metadata" => support::parameter("client_encoding", "UTF8"),
            _ => {
                support::send(&mut stream, &sasl(11, server.as_bytes())).await;
                let final_message = scram_response(&mut stream, &first, &server, 4096).await;
                match stage.as_str() {
                    "response-ok" => challenge(0),
                    "duplicate-continue" => sasl(11, server.as_bytes()),
                    "response-metadata" => support::frame(b'Z', b"I"),
                    "wrong-verifier" => sasl(12, b"v=ZmFrZQ=="),
                    "malformed-verifier" => sasl(12, b"v=!"),
                    "server-error" => sasl(12, b"e=invalid-proof"),
                    "duplicate-final" => [
                        sasl(12, final_message.as_bytes()),
                        sasl(12, final_message.as_bytes()),
                    ]
                    .concat(),
                    "verified-metadata" => [
                        sasl(12, final_message.as_bytes()),
                        support::parameter("client_encoding", "UTF8"),
                    ]
                    .concat(),
                    "duplicate-ok" => [
                        sasl(12, final_message.as_bytes()),
                        challenge(0),
                        challenge(0),
                    ]
                    .concat(),
                    _ => panic!("unknown rejection stage"),
                }
            }
        };
        support::send(&mut stream, &bad).await;
        support::eof(&mut stream).await;
    });
    let handle = SessionHandle::new(config);
    let error = rejected(connect(&handle).await, &handle);
    assert!(matches!(error, PostgresError::Protocol(_)));
    assert!(!format!("{error:?}").contains("secret"));
    peer.await.unwrap();
}

#[tokio::test]
async fn scram_phase_order_violations() {
    for stage in [
        "initial-ok",
        "initial-final",
        "initial-challenge",
        "initial-password",
        "initial-metadata",
        "response-ok",
        "duplicate-continue",
        "response-metadata",
        "duplicate-final",
        "verified-metadata",
        "duplicate-ok",
    ] {
        scram_after_initial(stage).await;
    }
}

#[tokio::test]
async fn scram_wrong_server_verifier() {
    for stage in ["wrong-verifier", "malformed-verifier", "server-error"] {
        scram_after_initial(stage).await;
    }
}

#[derive(Default)]
struct ScramWake(AtomicUsize);
impl Wake for ScramWake {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn poll_scram<F: Future>(future: Pin<&mut F>, wake: &Arc<ScramWake>) -> Poll<F::Output> {
    let waker = Waker::from(wake.clone());
    future.poll(&mut Context::from_waker(&waker))
}

fn scram_watchdog(start: std::time::Instant) {
    assert!(
        start.elapsed() < Duration::from_secs(30),
        "SCRAM peer/readiness stage stalled"
    );
}

#[tokio::test(start_paused = true)]
async fn scram_deadline_expired_before_ready_continue() {
    let (listener, mut config) = support::listener().await;
    config.password = Some("secret".into());
    config.connect_timeout = Duration::from_secs(1);
    let handle = SessionHandle::new(config);
    let reserved = tokio::time::Instant::now();
    let BeginConnect::Attempt(attempt) = handle.begin_connect().unwrap() else {
        panic!()
    };
    let (initial_tx, mut initial_rx) = oneshot::channel();
    let (continue_tx, continue_rx) = oneshot::channel();
    let (written_tx, mut written_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let mut stream = support::accept(listener).await;
        support::send(&mut stream, &scram_offer(false)).await;
        let first = scram_initial(&mut stream).await;
        initial_tx.send(()).unwrap();
        continue_rx.await.unwrap();
        support::send(
            &mut stream,
            &sasl(11, server_first(&first, 4096).as_bytes()),
        )
        .await;
        written_tx.send(()).unwrap();
        let mut outbound = Vec::new();
        stream.read_to_end(&mut outbound).await.unwrap();
        outbound
    });
    let mut client = Box::pin(attempt.run());
    let drive = Arc::new(ScramWake::default());
    let start = std::time::Instant::now();
    while initial_rx.try_recv().is_err() {
        assert!(poll_scram(client.as_mut(), &drive).is_pending());
        scram_watchdog(start);
        tokio::task::yield_now().await;
    }
    let read = Arc::new(ScramWake::default());
    assert!(poll_scram(client.as_mut(), &read).is_pending());
    assert_eq!(read.0.load(Ordering::SeqCst), 0);
    assert_eq!(tokio::time::Instant::now(), reserved);
    continue_tx.send(()).unwrap();
    while written_rx.try_recv().is_err() {
        scram_watchdog(start);
        tokio::task::yield_now().await;
    }
    while read.0.load(Ordering::SeqCst) == 0 {
        scram_watchdog(start);
        tokio::task::yield_now().await;
    }
    assert_eq!(tokio::time::Instant::now(), reserved);
    tokio::time::advance(Duration::from_millis(1001)).await;
    assert!(tokio::time::Instant::now() > reserved + Duration::from_secs(1));
    assert!(matches!(
        rejected(client.await, &handle),
        PostgresError::Transport(_)
    ));
    tokio::time::resume();
    let outbound = tokio::time::timeout(Duration::from_secs(2), peer)
        .await
        .unwrap()
        .unwrap();
    assert!(
        outbound.is_empty(),
        "expired client transmitted SASLResponse"
    );
}
