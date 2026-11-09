mod support;

use _postgres::config::RawPostgresOptions;
use _postgres::errors::PostgresError;
use _postgres::session::SessionHandle;
use tokio::io::AsyncReadExt;

use support::{
    frame_auth_cleartext, frame_auth_gss, frame_auth_kerberos, frame_auth_md5, frame_auth_ok,
    frame_auth_sspi, frame_error_response, frame_ready_for_query, read_startup_packet,
    read_tagged_frontend_frame, spawn_scripted_server, write_frames,
};

#[tokio::test]
async fn password_cleartext_and_md5_golden_vector_succeed() {
    let (addr1, _, task1) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_cleartext()])
            .await
            .unwrap();
        let (tag, payload) = read_tagged_frontend_frame(&mut stream)
            .await
            .expect("read PasswordMessage");
        assert_eq!(tag, b'p');
        assert_eq!(payload, b"s3cr3t!\0");
        write_frames(&mut stream, &[frame_auth_ok(), frame_ready_for_query(b'I')])
            .await
            .unwrap();
    })
    .await;

    let mut raw1 = RawPostgresOptions::new(addr1.ip().to_string(), "alice", "appdb");
    raw1.port = i64::from(addr1.port());
    raw1.sslmode = "disable".to_string();
    raw1.password = Some("s3cr3t!".to_string());
    let mut h1 = SessionHandle::new(raw1).unwrap();
    h1.connect().await.expect("cleartext auth");
    assert!(h1.is_ready());
    task1.await.unwrap();

    let (addr2, _, task2) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_md5([0x01, 0x02, 0x03, 0x04])])
            .await
            .unwrap();
        let (tag, payload) = read_tagged_frontend_frame(&mut stream)
            .await
            .expect("read MD5 PasswordMessage");
        assert_eq!(tag, b'p');
        assert_eq!(payload, b"md5c73cff48cd454994b0c263a04cdfc859\0");
        write_frames(&mut stream, &[frame_auth_ok(), frame_ready_for_query(b'I')])
            .await
            .unwrap();
    })
    .await;

    let mut raw2 = RawPostgresOptions::new(addr2.ip().to_string(), "postgres", "appdb");
    raw2.port = i64::from(addr2.port());
    raw2.sslmode = "disable".to_string();
    raw2.password = Some("hunter2".to_string());
    let mut h2 = SessionHandle::new(raw2).unwrap();
    h2.connect().await.expect("MD5 golden vector auth");
    assert!(h2.is_ready());
    task2.await.unwrap();
}

#[tokio::test]
async fn password_missing_rejected_before_emitting_password_message() {
    for challenge in [frame_auth_cleartext(), frame_auth_md5([9, 8, 7, 6])] {
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            let _ = read_startup_packet(&mut stream).await.unwrap();
            write_frames(&mut stream, &[challenge]).await.unwrap();
            let mut trailing = Vec::new();
            let n = stream.read_to_end(&mut trailing).await.unwrap();
            assert_eq!(n, 0, "must not write PasswordMessage when password is None");
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "postgres", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "disable".to_string();
        raw.password = None;
        let mut handle = SessionHandle::new(raw).unwrap();
        let err = handle
            .connect()
            .await
            .expect_err("missing password must fail");
        assert!(matches!(err, PostgresError::Config(_)));
        assert!(handle.is_closed());
        task.await.unwrap();
    }
}

#[tokio::test]
async fn password_wrong_28p01_duplicate_challenge_and_unsupported_mechanisms() {
    let (addr1, accepts1, task1) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_md5([1, 1, 1, 1])])
            .await
            .unwrap();
        let (tag, _) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        assert_eq!(tag, b'p');
        write_frames(
            &mut stream,
            &[frame_error_response(
                "28P01",
                "password authentication failed for user \"postgres\"",
                None,
            )],
        )
        .await
        .unwrap();
    })
    .await;

    let mut raw1 = RawPostgresOptions::new(addr1.ip().to_string(), "postgres", "appdb");
    raw1.port = i64::from(addr1.port());
    raw1.sslmode = "disable".to_string();
    raw1.password = Some("ultra-secret-pw".to_string());
    let mut h1 = SessionHandle::new(raw1).unwrap();
    let err1 = h1.connect().await.expect_err("28P01 must fail");
    assert_eq!(err1.sqlstate(), Some("28P01"));
    assert!(!err1.to_string().contains("ultra-secret-pw"));
    assert_eq!(accepts1.load(std::sync::atomic::Ordering::SeqCst), 1);
    task1.await.unwrap();

    let (addr2, _, task2) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_cleartext()])
            .await
            .unwrap();
        let _ = read_tagged_frontend_frame(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_cleartext()])
            .await
            .unwrap();
    })
    .await;
    let mut raw2 = RawPostgresOptions::new(addr2.ip().to_string(), "postgres", "appdb");
    raw2.port = i64::from(addr2.port());
    raw2.sslmode = "disable".to_string();
    raw2.password = Some("secret".to_string());
    let mut h2 = SessionHandle::new(raw2).unwrap();
    let err2 = h2
        .connect()
        .await
        .expect_err("second password challenge must fail");
    assert!(matches!(err2, PostgresError::Protocol(_)));
    task2.await.unwrap();

    for unsupported in [frame_auth_gss(), frame_auth_sspi(), frame_auth_kerberos()] {
        let (addr3, _, task3) = spawn_scripted_server(move |mut stream| async move {
            let _ = read_startup_packet(&mut stream).await.unwrap();
            write_frames(&mut stream, &[unsupported]).await.unwrap();
        })
        .await;
        let mut raw3 = RawPostgresOptions::new(addr3.ip().to_string(), "postgres", "appdb");
        raw3.port = i64::from(addr3.port());
        raw3.sslmode = "disable".to_string();
        let mut h3 = SessionHandle::new(raw3).unwrap();
        let err3 = h3
            .connect()
            .await
            .expect_err("GSS/SSPI/Kerberos must fail UnsupportedAuth");
        assert!(matches!(err3, PostgresError::UnsupportedAuth(_)));
        task3.await.unwrap();
    }
}
