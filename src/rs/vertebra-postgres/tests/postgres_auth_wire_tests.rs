mod support;

use _postgres::config::RawPostgresOptions;
use _postgres::errors::PostgresError;
use _postgres::session::SessionHandle;
use tokio::io::AsyncReadExt;

use support::{
    frame_auth_cleartext, frame_auth_gss, frame_auth_kerberos, frame_auth_md5, frame_auth_ok,
    frame_auth_sasl, frame_auth_sasl_continue, frame_auth_sasl_final, frame_auth_sspi,
    frame_error_response, frame_ready_for_query, parse_sasl_initial, read_startup_packet,
    read_tagged_frontend_frame, scram_verify_and_sign, spawn_scripted_server, write_frames,
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

#[tokio::test]
async fn scram_sha256_rfc7677_exchange_and_saslprep_normalization_succeed() {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    // 1. Standard SCRAM-SHA-256 over plain TCP (`n,,` GS2 header)
    let salt = b"\x72\x66\x63\x37\x36\x37\x37\x73\x61\x6c\x74\x21\x31\x32\x33\x34";
    let (addr1, _, task1) = spawn_scripted_server(move |mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_sasl(&[b"SCRAM-SHA-256"])])
            .await
            .unwrap();
        let (tag1, payload1) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        assert_eq!(tag1, b'p');
        let initial = parse_sasl_initial(&payload1);
        assert_eq!(initial.mechanism, "SCRAM-SHA-256");
        assert_eq!(initial.gs2_header, "n,,");
        assert!(initial.client_nonce.len() >= 18);

        let server_first = format!(
            "r={}s3rv3rN0nc3,s={},i=4096",
            initial.client_nonce,
            STANDARD.encode(salt)
        );
        write_frames(
            &mut stream,
            &[frame_auth_sasl_continue(server_first.as_bytes())],
        )
        .await
        .unwrap();

        let (tag2, payload2) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        assert_eq!(tag2, b'p');
        let client_final = String::from_utf8(payload2).unwrap();
        let server_final = scram_verify_and_sign(
            b"pencil",
            &initial,
            &server_first,
            salt,
            4096,
            &[],
            &client_final,
        );
        write_frames(
            &mut stream,
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

    let mut raw1 = RawPostgresOptions::new(addr1.ip().to_string(), "user", "appdb");
    raw1.port = i64::from(addr1.port());
    raw1.sslmode = "disable".to_string();
    raw1.password = Some("pencil".to_string());
    let mut h1 = SessionHandle::new(raw1).unwrap();
    h1.connect().await.expect("SCRAM-SHA-256 pencil");
    assert!(h1.is_ready());
    task1.await.unwrap();

    // 2. SASLprep NFKC normalization ("I\u{00AD}X" -> "IX")
    let (addr2, _, task2) = spawn_scripted_server(move |mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_sasl(&[b"SCRAM-SHA-256"])])
            .await
            .unwrap();
        let (_, payload1) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        let initial = parse_sasl_initial(&payload1);
        let server_first = format!(
            "r={}srv,s={},i=16",
            initial.client_nonce,
            STANDARD.encode(salt)
        );
        write_frames(
            &mut stream,
            &[frame_auth_sasl_continue(server_first.as_bytes())],
        )
        .await
        .unwrap();
        let (_, payload2) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        let client_final = String::from_utf8(payload2).unwrap();
        let server_final =
            scram_verify_and_sign(b"IX", &initial, &server_first, salt, 16, &[], &client_final);
        write_frames(
            &mut stream,
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

    let mut raw2 = RawPostgresOptions::new(addr2.ip().to_string(), "user", "appdb");
    raw2.port = i64::from(addr2.port());
    raw2.sslmode = "disable".to_string();
    raw2.password = Some("I\u{00AD}X".to_string());
    let mut h2 = SessionHandle::new(raw2).unwrap();
    h2.connect().await.expect("SASLprep normalized password");
    task2.await.unwrap();
}

#[tokio::test]
async fn scram_sha256_rejects_nonce_truncation_iteration_bounds_and_tampered_server_signature() {
    use base64::Engine as _;
    use base64::engine::general_purpose::STANDARD;

    let salt_b64 = STANDARD.encode(b"0123456789abcdef");

    // 1. Server nonce does not extend client nonce (exact equal without suffix, or wrong prefix)
    for suffix_mode in ["empty_suffix", "wrong_prefix"] {
        let salt_clone = salt_b64.clone();
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            let _ = read_startup_packet(&mut stream).await.unwrap();
            write_frames(&mut stream, &[frame_auth_sasl(&[b"SCRAM-SHA-256"])])
                .await
                .unwrap();
            let (_, payload1) = read_tagged_frontend_frame(&mut stream).await.unwrap();
            let initial = parse_sasl_initial(&payload1);
            let bad_nonce = if suffix_mode == "empty_suffix" {
                initial.client_nonce
            } else {
                "forged-prefix-nonce".to_string()
            };
            let server_first = format!("r={bad_nonce},s={salt_clone},i=4096");
            write_frames(
                &mut stream,
                &[frame_auth_sasl_continue(server_first.as_bytes())],
            )
            .await
            .unwrap();
            let mut trailing = Vec::new();
            let n = stream.read_to_end(&mut trailing).await.unwrap();
            assert_eq!(n, 0, "must not send SASLResponse on invalid server nonce");
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "user", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "disable".to_string();
        raw.password = Some("pencil".to_string());
        let mut h = SessionHandle::new(raw).unwrap();
        let err = h.connect().await.expect_err("bad server nonce must fail");
        assert!(matches!(err, PostgresError::Protocol(_)));
        task.await.unwrap();
    }

    // 2. Iteration count bounds: i=0, i=-1, i=100001, duplicate i=, and missing s=/r=
    for bad_continue in [
        format!("s={salt_b64},i=0"),
        format!("s={salt_b64},i=-1"),
        format!("s={salt_b64},i=100001"),
        format!("s={salt_b64},i=4096,i=4096"),
        "i=4096".to_string(),
    ] {
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            let _ = read_startup_packet(&mut stream).await.unwrap();
            write_frames(&mut stream, &[frame_auth_sasl(&[b"SCRAM-SHA-256"])])
                .await
                .unwrap();
            let (_, payload1) = read_tagged_frontend_frame(&mut stream).await.unwrap();
            let initial = parse_sasl_initial(&payload1);
            let server_first = format!("r={}srv,{bad_continue}", initial.client_nonce);
            write_frames(
                &mut stream,
                &[frame_auth_sasl_continue(server_first.as_bytes())],
            )
            .await
            .unwrap();
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "user", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "disable".to_string();
        raw.password = Some("pencil".to_string());
        let mut h = SessionHandle::new(raw).unwrap();
        let err = h
            .connect()
            .await
            .expect_err("invalid SCRAM server-first must fail");
        assert!(matches!(err, PostgresError::Protocol(_)));
        task.await.unwrap();
    }

    // 3. Tampered server signature `v=` in AuthenticationSASLFinal
    let (addr_v, _, task_v) = spawn_scripted_server(move |mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_sasl(&[b"SCRAM-SHA-256"])])
            .await
            .unwrap();
        let (_, payload1) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        let initial = parse_sasl_initial(&payload1);
        let server_first = format!("r={}srv,s={salt_b64},i=16", initial.client_nonce);
        write_frames(
            &mut stream,
            &[frame_auth_sasl_continue(server_first.as_bytes())],
        )
        .await
        .unwrap();
        let _ = read_tagged_frontend_frame(&mut stream).await.unwrap();
        let forged_final = format!("v={}", STANDARD.encode([0x55u8; 32]));
        write_frames(
            &mut stream,
            &[
                frame_auth_sasl_final(forged_final.as_bytes()),
                frame_auth_ok(),
                frame_ready_for_query(b'I'),
            ],
        )
        .await
        .unwrap();
    })
    .await;

    let mut raw_v = RawPostgresOptions::new(addr_v.ip().to_string(), "user", "appdb");
    raw_v.port = i64::from(addr_v.port());
    raw_v.sslmode = "disable".to_string();
    raw_v.password = Some("pencil".to_string());
    let mut h_v = SessionHandle::new(raw_v).unwrap();
    let err_v = h_v
        .connect()
        .await
        .expect_err("tampered ServerSignature must fail");
    assert!(matches!(err_v, PostgresError::Protocol(_)));
    task_v.await.unwrap();
}

#[tokio::test]
async fn scram_sha256_rejects_premature_auth_ok_out_of_order_sasl_and_missing_password() {
    // 1. Missing password with SCRAM challenge -> Config error before emitting SASLInitialResponse
    let (addr1, _, task1) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_sasl(&[b"SCRAM-SHA-256"])])
            .await
            .unwrap();
        let mut trailing = Vec::new();
        let n = stream.read_to_end(&mut trailing).await.unwrap();
        assert_eq!(
            n, 0,
            "must not emit SASLInitialResponse when password is None"
        );
    })
    .await;
    let mut raw1 = RawPostgresOptions::new(addr1.ip().to_string(), "user", "appdb");
    raw1.port = i64::from(addr1.port());
    raw1.sslmode = "disable".to_string();
    raw1.password = None;
    let mut h1 = SessionHandle::new(raw1).unwrap();
    assert!(matches!(
        h1.connect().await.expect_err("missing password"),
        PostgresError::Config(_)
    ));
    task1.await.unwrap();

    // 2. Premature AuthenticationOk during ScramStarted (skipping SASLFinal)
    let (addr2, _, task2) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_sasl(&[b"SCRAM-SHA-256"])])
            .await
            .unwrap();
        let _ = read_tagged_frontend_frame(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_ok(), frame_ready_for_query(b'I')])
            .await
            .unwrap();
    })
    .await;
    let mut raw2 = RawPostgresOptions::new(addr2.ip().to_string(), "user", "appdb");
    raw2.port = i64::from(addr2.port());
    raw2.sslmode = "disable".to_string();
    raw2.password = Some("pencil".to_string());
    let mut h2 = SessionHandle::new(raw2).unwrap();
    assert!(matches!(
        h2.connect().await.expect_err("premature AuthOk must fail"),
        PostgresError::Protocol(_)
    ));
    task2.await.unwrap();

    // 3. Out-of-order AuthenticationSASLContinue before AuthenticationSASL
    let (addr3, _, task3) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(
            &mut stream,
            &[frame_auth_sasl_continue(b"r=abc,s=YWJj,i=4096")],
        )
        .await
        .unwrap();
    })
    .await;
    let mut raw3 = RawPostgresOptions::new(addr3.ip().to_string(), "user", "appdb");
    raw3.port = i64::from(addr3.port());
    raw3.sslmode = "disable".to_string();
    raw3.password = Some("pencil".to_string());
    let mut h3 = SessionHandle::new(raw3).unwrap();
    assert!(matches!(
        h3.connect().await.expect_err("out of order SASLContinue"),
        PostgresError::Protocol(_)
    ));
    task3.await.unwrap();
}

#[tokio::test]
async fn scram_plus_channel_binding_matrix_and_require_policy_guards() {
    use tokio::io::AsyncWriteExt;

    // When channel_binding="require" on non-TLS (sslmode="prefer" downgraded by 'N'), reject Trust, Cleartext, MD5, and SASL before sending credentials
    for challenge in [
        frame_auth_ok(),
        frame_auth_cleartext(),
        frame_auth_md5([1, 2, 3, 4]),
        frame_auth_sasl(&[b"SCRAM-SHA-256", b"SCRAM-SHA-256-PLUS"]),
    ] {
        let (addr, _, task) = spawn_scripted_server(move |mut stream| async move {
            let mut ssl_req = [0u8; 8];
            stream.read_exact(&mut ssl_req).await.unwrap();
            stream.write_all(b"N").await.unwrap();
            let _ = read_startup_packet(&mut stream).await.unwrap();
            write_frames(&mut stream, &[challenge]).await.unwrap();
            let mut trailing = Vec::new();
            let n = stream.read_to_end(&mut trailing).await.unwrap();
            assert_eq!(
                n, 0,
                "must not emit credentials when channel_binding='require' is unsatisfied"
            );
        })
        .await;

        let mut raw = RawPostgresOptions::new(addr.ip().to_string(), "user", "appdb");
        raw.port = i64::from(addr.port());
        raw.sslmode = "prefer".to_string();
        raw.channel_binding = "require".to_string();
        raw.password = Some("pencil".to_string());
        let mut h = SessionHandle::new(raw).unwrap();
        let err = h
            .connect()
            .await
            .expect_err("require on non-TLS must fail UnsupportedAuth");
        assert!(matches!(err, PostgresError::UnsupportedAuth(_)));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn direct_iam_refresh_invalidation_and_redaction_and_tls_expiry_guard() {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;
    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;

    let token_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let token_addr = token_listener.local_addr().unwrap();
    let token_calls = Arc::new(AtomicUsize::new(0));
    let token_calls_srv = Arc::clone(&token_calls);

    let token_task = tokio::spawn(async move {
        while let Ok((mut stream, _)) = token_listener.accept().await {
            let calls = Arc::clone(&token_calls_srv);
            tokio::spawn(async move {
                let mut buf = [0u8; 2048];
                let _ = stream.read(&mut buf).await;
                let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                let body = format!(
                    r#"{{"access_token":"secret-iam-tok-{n}","expires_in":3600,"scope":"https://www.googleapis.com/auth/sqlservice.login"}}"#
                );
                let resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    body.len(),
                    body
                );
                let _ = stream.write_all(resp.as_bytes()).await;
            });
        }
    });

    // First PG attempt returns 28P01 ErrorResponse -> must invalidate token and redact secret token
    let (addr1, _, pg_task1) = spawn_scripted_server(|mut stream| async move {
        let _ = read_startup_packet(&mut stream).await.unwrap();
        write_frames(&mut stream, &[frame_auth_cleartext()])
            .await
            .unwrap();
        let (tag, payload) = read_tagged_frontend_frame(&mut stream).await.unwrap();
        assert_eq!(tag, b'p');
        assert_eq!(payload, b"secret-iam-tok-1\0");
        write_frames(
            &mut stream,
            &[frame_error_response(
                "28P01",
                "IAM token secret-iam-tok-1 rejected",
                Some("detail secret-iam-tok-1"),
            )],
        )
        .await
        .unwrap();
    })
    .await;

    let mut iam_opts = HashMap::new();
    iam_opts.insert("client_id".to_string(), "cid".to_string());
    iam_opts.insert("client_secret".to_string(), "csec".to_string());
    iam_opts.insert("refresh_token".to_string(), "rtok".to_string());
    iam_opts.insert(
        "token_uri".to_string(),
        format!("http://{token_addr}/token"),
    );
    iam_opts.insert("proxy_policy".to_string(), "direct".to_string());

    let mut raw1 = RawPostgresOptions::new(addr1.ip().to_string(), "iam-user", "appdb");
    raw1.port = i64::from(addr1.port());
    raw1.sslmode = "disable".to_string();
    raw1.enable_iam_auth = true;
    raw1.iam_auth_options = iam_opts.clone();

    let mut h1 = SessionHandle::new(raw1).unwrap();
    let err1 = h1.connect().await.expect_err("28P01 must fail");
    assert_eq!(err1.sqlstate(), Some("28P01"));
    assert!(
        !err1.to_string().contains("secret-iam-tok-1"),
        "IAM token must be redacted from error message"
    );
    pg_task1.await.unwrap();

    // Direct IAM with already-expired static token must fail AuthError before sending credentials
    let (addr2, accepts2, _pg_task2) = spawn_scripted_server(|_stream| async move {}).await;
    let mut expired_iam = HashMap::new();
    expired_iam.insert("access_token".to_string(), "expired-secret-tok".to_string());
    expired_iam.insert("expires_at_unix_ms".to_string(), "1000".to_string());
    expired_iam.insert(
        "scopes".to_string(),
        "https://www.googleapis.com/auth/sqlservice.login".to_string(),
    );
    let mut raw2 = RawPostgresOptions::new(addr2.ip().to_string(), "iam-user", "appdb");
    raw2.port = i64::from(addr2.port());
    raw2.sslmode = "disable".to_string();
    raw2.enable_iam_auth = true;
    raw2.iam_auth_options = expired_iam;
    let mut h2 = SessionHandle::new(raw2).unwrap();
    let err2 = h2
        .connect()
        .await
        .expect_err("expired static token must fail before TCP dial");
    assert!(matches!(err2, PostgresError::Auth(_)));
    assert_eq!(accepts2.load(std::sync::atomic::Ordering::SeqCst), 0);
    token_task.abort();
}
