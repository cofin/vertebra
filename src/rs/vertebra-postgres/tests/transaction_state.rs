//! Integration tests for transaction state machine, owner affinity, savepoint scopes,
//! and rollback recovery (`ch13:t01`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::TransactionStatus;
use vertebra_postgres::session::SessionHandle;
use vertebra_postgres::transaction::{TransactionDisposition, TransactionOwner};

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_ok_and_ready() -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'R', &0i32.to_be_bytes()));
    out.extend_from_slice(&frame(b'S', b"client_encoding\0UTF8\0"));
    out.extend_from_slice(&frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&frame(b'S', b"TimeZone\0UTC\0"));
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn cmd_complete(tag: &str, status: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let mut body = BytesMut::new();
    body.put_slice(tag.as_bytes());
    body.put_u8(0);
    out.extend_from_slice(&frame(b'C', &body));
    out.extend_from_slice(&frame(b'Z', &[status]));
    out
}

fn err_response(code: &str, msg: &str, status: u8) -> Vec<u8> {
    let mut out = Vec::new();
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(code.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(msg.as_bytes());
    body.push(0);
    body.push(0);
    out.extend_from_slice(&frame(b'E', &body));
    out.extend_from_slice(&frame(b'Z', &[status]));
    out
}

async fn read_startup(sock: &mut tokio::net::TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
}

async fn read_simple_query(sock: &mut tokio::net::TcpStream) -> String {
    let mut hdr = [0u8; 5];
    sock.read_exact(&mut hdr).await.unwrap();
    assert_eq!(hdr[0], b'Q');
    let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
    assert_eq!(body.pop(), Some(0));
    String::from_utf8(body).unwrap()
}

fn loopback_handle(port: u16) -> SessionHandle {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    SessionHandle::new(raw).unwrap()
}

#[tokio::test]
async fn begin_nested_savepoints_failed_branch_recovery_and_owner_affinity() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        // 1. BEGIN
        assert_eq!(read_simple_query(&mut sock).await, "BEGIN");
        sock.write_all(&cmd_complete("BEGIN", b'T')).await.unwrap();

        // 2. SAVEPOINT vtb_sp_1
        assert_eq!(read_simple_query(&mut sock).await, "SAVEPOINT vtb_sp_1");
        sock.write_all(&cmd_complete("SAVEPOINT", b'T'))
            .await
            .unwrap();

        // 3. SAVEPOINT vtb_sp_2
        assert_eq!(read_simple_query(&mut sock).await, "SAVEPOINT vtb_sp_2");
        sock.write_all(&cmd_complete("SAVEPOINT", b'T'))
            .await
            .unwrap();

        // 4. Failing query transitions to 'E'
        assert_eq!(read_simple_query(&mut sock).await, "SELECT 1/0");
        sock.write_all(&err_response("22012", "division by zero", b'E'))
            .await
            .unwrap();

        // 5. ROLLBACK TO SAVEPOINT vtb_sp_2 recovers to 'T', then RELEASE SAVEPOINT vtb_sp_2
        assert_eq!(
            read_simple_query(&mut sock).await,
            "ROLLBACK TO SAVEPOINT vtb_sp_2"
        );
        sock.write_all(&cmd_complete("ROLLBACK", b'T'))
            .await
            .unwrap();

        assert_eq!(
            read_simple_query(&mut sock).await,
            "RELEASE SAVEPOINT vtb_sp_2"
        );
        sock.write_all(&cmd_complete("RELEASE", b'T'))
            .await
            .unwrap();

        // 6. RELEASE SAVEPOINT vtb_sp_1
        assert_eq!(
            read_simple_query(&mut sock).await,
            "RELEASE SAVEPOINT vtb_sp_1"
        );
        sock.write_all(&cmd_complete("RELEASE", b'T'))
            .await
            .unwrap();

        // 7. COMMIT
        assert_eq!(read_simple_query(&mut sock).await, "COMMIT");
        sock.write_all(&cmd_complete("COMMIT", b'I')).await.unwrap();

        // 8. Second BEGIN -> error -> COMMIT returns ROLLBACK tag -> OperationalError!
        assert_eq!(read_simple_query(&mut sock).await, "BEGIN");
        sock.write_all(&cmd_complete("BEGIN", b'T')).await.unwrap();

        assert_eq!(read_simple_query(&mut sock).await, "SELECT 1/0");
        sock.write_all(&err_response("22012", "division by zero", b'E'))
            .await
            .unwrap();

        assert_eq!(read_simple_query(&mut sock).await, "COMMIT");
        sock.write_all(&cmd_complete("ROLLBACK", b'I'))
            .await
            .unwrap();
    });

    let mut handle = loopback_handle(port);
    handle.connect().await.unwrap();
    let generation = handle.generation();
    let owner_a = TransactionOwner::new(101, generation);
    let owner_b = TransactionOwner::new(202, generation);

    // Idle commit/rollback/savepoint rejected locally without network I/O
    assert!(matches!(
        handle.commit_as(owner_a).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        handle.rollback_as(owner_a).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        handle.savepoint_as(owner_a, 1).await,
        Err(PostgresError::Interface(_))
    ));

    let disp = handle.begin_as(owner_a).await.unwrap();
    assert_eq!(disp, TransactionDisposition::Begun);
    assert_eq!(
        handle.transaction_status(),
        TransactionStatus::InTransaction
    );

    // Cross-owner rejection while transaction is pinned to owner_a
    let cross_err = handle.commit_as(owner_b).await.unwrap_err();
    assert!(matches!(cross_err, PostgresError::Interface(_)));

    // Nested savepoints
    assert_eq!(
        handle.savepoint_as(owner_a, 1).await.unwrap(),
        TransactionDisposition::SavepointCreated(1)
    );
    assert_eq!(
        handle.savepoint_as(owner_a, 2).await.unwrap(),
        TransactionDisposition::SavepointCreated(2)
    );

    // Unowned simple_query_shared fails with InterfaceError while session is pinned to owner_a
    let q_err = handle.simple_query_shared("SELECT 1/0").await;
    assert!(matches!(q_err, Err(PostgresError::Interface(_))));

    let q_err2 = handle
        .connection_mut()
        .unwrap()
        .simple_query("SELECT 1/0")
        .await
        .unwrap_err();
    assert!(matches!(q_err2, PostgresError::Server(_)));
    assert_eq!(handle.transaction_status(), TransactionStatus::Failed);

    // Savepoint creation or release in Failed state is rejected locally
    assert!(matches!(
        handle.savepoint_as(owner_a, 3).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        handle.release_savepoint_as(owner_a, 2).await,
        Err(PostgresError::Interface(_))
    ));

    // Rollback to savepoint 2 recovers transaction status to InTransaction, then release savepoint 2
    assert_eq!(
        handle.rollback_to_savepoint_as(owner_a, 2).await.unwrap(),
        TransactionDisposition::RolledBackToSavepoint(2)
    );
    assert_eq!(
        handle.transaction_status(),
        TransactionStatus::InTransaction
    );
    assert_eq!(
        handle.release_savepoint_as(owner_a, 2).await.unwrap(),
        TransactionDisposition::SavepointReleased(2)
    );

    // Release savepoint 1 and commit outer transaction
    assert_eq!(
        handle.release_savepoint_as(owner_a, 1).await.unwrap(),
        TransactionDisposition::SavepointReleased(1)
    );
    let final_disp = handle.commit_as(owner_a).await.unwrap();
    assert_eq!(final_disp, TransactionDisposition::Committed);
    assert_eq!(handle.transaction_status(), TransactionStatus::Idle);

    // Failed outer transaction COMMIT returning ROLLBACK is surfaced as OperationalError, never silent Committed!
    handle.begin_as(owner_a).await.unwrap();
    let _ = handle
        .connection_mut()
        .unwrap()
        .simple_query("SELECT 1/0")
        .await;
    assert_eq!(handle.transaction_status(), TransactionStatus::Failed);
    let failed_commit = handle.commit_as(owner_a).await.unwrap_err();
    assert!(matches!(failed_commit, PostgresError::Operational(_)));
    assert_eq!(handle.transaction_status(), TransactionStatus::Idle);

    server.await.unwrap();
}

#[tokio::test]
async fn cockroachdb_40001_retryable_serialization_and_cockroach_restart_savepoint_flow() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;

        let mut greeting = Vec::new();
        greeting.extend_from_slice(&frame(b'R', &0i32.to_be_bytes()));
        greeting.extend_from_slice(&frame(b'S', b"client_encoding\0UTF8\0"));
        greeting.extend_from_slice(&frame(b'S', b"crdb_version\0CCL v24.1.0\0"));
        greeting.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
        greeting.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&greeting).await.unwrap();

        assert_eq!(read_simple_query(&mut sock).await, "BEGIN");
        sock.write_all(&cmd_complete("BEGIN", b'T')).await.unwrap();

        assert_eq!(
            read_simple_query(&mut sock).await,
            "SAVEPOINT cockroach_restart"
        );
        sock.write_all(&cmd_complete("SAVEPOINT", b'T'))
            .await
            .unwrap();

        assert_eq!(
            read_simple_query(&mut sock).await,
            "UPDATE accounts SET balance = balance - 10 WHERE id = 1"
        );
        sock.write_all(&err_response(
            "40001",
            "restart transaction: TransactionRetryWithProtoRefreshError: WriteTooOldError",
            b'E',
        ))
        .await
        .unwrap();

        assert_eq!(
            read_simple_query(&mut sock).await,
            "ROLLBACK TO SAVEPOINT cockroach_restart"
        );
        sock.write_all(&cmd_complete("ROLLBACK", b'T'))
            .await
            .unwrap();

        assert_eq!(
            read_simple_query(&mut sock).await,
            "UPDATE accounts SET balance = balance - 10 WHERE id = 1"
        );
        sock.write_all(&cmd_complete("UPDATE 1", b'T'))
            .await
            .unwrap();

        assert_eq!(
            read_simple_query(&mut sock).await,
            "RELEASE SAVEPOINT cockroach_restart"
        );
        sock.write_all(&cmd_complete("RELEASE", b'T'))
            .await
            .unwrap();

        assert_eq!(read_simple_query(&mut sock).await, "COMMIT");
        sock.write_all(&cmd_complete("COMMIT", b'I')).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    handle.connect().await.unwrap();
    assert!(handle.is_cockroachdb());
    let owner = TransactionOwner::new(7, handle.generation());

    assert!(matches!(
        handle.savepoint_cockroach_restart_as(owner).await,
        Err(PostgresError::Interface(_))
    ));

    handle.begin_as(owner).await.unwrap();
    assert!(matches!(
        handle.release_cockroach_restart_as(owner).await,
        Err(PostgresError::Interface(_))
    ));
    handle.savepoint_cockroach_restart_as(owner).await.unwrap();

    let retry_err = handle
        .connection_mut()
        .unwrap()
        .simple_query("UPDATE accounts SET balance = balance - 10 WHERE id = 1")
        .await
        .unwrap_err();
    assert!(retry_err.is_retryable_serialization());
    assert!(retry_err.is_transient());
    assert_eq!(retry_err.sqlstate(), Some("40001"));
    assert_eq!(handle.transaction_status(), TransactionStatus::Failed);

    let non_retry = PostgresError::server("ERROR", "22012", "division by zero", vec![]).unwrap();
    assert!(!non_retry.is_retryable_serialization());

    handle
        .rollback_to_cockroach_restart_as(owner)
        .await
        .unwrap();
    assert_eq!(
        handle.transaction_status(),
        TransactionStatus::InTransaction
    );

    handle
        .connection_mut()
        .unwrap()
        .simple_query("UPDATE accounts SET balance = balance - 10 WHERE id = 1")
        .await
        .unwrap();
    handle.release_cockroach_restart_as(owner).await.unwrap();
    assert_eq!(
        handle.commit_as(owner).await.unwrap(),
        TransactionDisposition::Committed
    );
    assert_eq!(handle.transaction_status(), TransactionStatus::Idle);

    server.await.unwrap();
}
