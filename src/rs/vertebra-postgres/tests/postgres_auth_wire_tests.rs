mod support;

use _postgres::{
    errors::PostgresError,
    session::{BeginConnect, SessionHandle},
};
use tokio::{io::AsyncReadExt, net::TcpStream};

const MD5_RESPONSE: &[u8] = b"p\0\0\0\x28md5c73cff48cd454994b0c263a04cdfc859\0";

fn challenge(code: i32) -> Vec<u8> {
    let mut body = code.to_be_bytes().to_vec();
    if code == 5 {
        body.extend_from_slice(&[1, 2, 3, 4]);
    }
    support::frame(b'R', &body)
}

async fn password_frame(stream: &mut TcpStream) -> Vec<u8> {
    assert_eq!(stream.read_u8().await.unwrap(), b'p');
    let len = stream.read_i32().await.unwrap();
    assert!((5..100).contains(&len));
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
        support::send(&mut stream, &challenge(5)).await;
        assert_eq!(password_frame(&mut stream).await, MD5_RESPONSE);
        support::send(&mut stream, &support::trust()).await;
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
        (10, b"SCRAM-SHA-256\0\0"),
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
