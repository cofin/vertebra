//! Integration tests for PostgreSQL simple `Query` protocol, multi-result correlation, and `ErrorResponse` draining (`ch09:t02`).

extern crate _postgres as vertebra_postgres;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::result::{Format, PgValue, TransactionStatus};
use vertebra_postgres::session::SessionHandle;

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
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn row_desc(cols: &[(&str, u32, i16)]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cols.len() as i16);
    for &(name, oid, fmt) in cols {
        body.put_slice(name.as_bytes());
        body.put_u8(0);
        body.put_u32(0);
        body.put_i16(0);
        body.put_u32(oid);
        body.put_i16(-1);
        body.put_i32(-1);
        body.put_i16(fmt);
    }
    frame(b'T', &body)
}

fn data_row(cells: &[Option<&[u8]>]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(cells.len() as i16);
    for cell in cells {
        match cell {
            None => body.put_i32(-1),
            Some(bytes) => {
                body.put_i32(bytes.len() as i32);
                body.put_slice(bytes);
            }
        }
    }
    frame(b'D', &body)
}

fn cmd_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

fn err_response(code: &str, msg: &str) -> Vec<u8> {
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
    frame(b'E', &body)
}

async fn read_startup(sock: &mut tokio::net::TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
}

async fn read_query_frame(sock: &mut tokio::net::TcpStream) -> String {
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
    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    SessionHandle::new(raw).unwrap()
}

#[tokio::test]
async fn simple_query_multi_result_empty_query_and_interleaved_async_frames() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let sql1 = read_query_frame(&mut sock).await;
        assert!(sql1.contains("CREATE TEMP TABLE"));
        let mut resp1 = Vec::new();
        resp1.extend_from_slice(&cmd_complete("CREATE TABLE"));
        resp1.extend_from_slice(&cmd_complete("INSERT 0 2"));
        resp1.extend_from_slice(&cmd_complete("UPDATE 2"));
        resp1.extend_from_slice(&cmd_complete("MERGE 1"));
        resp1.extend_from_slice(&row_desc(&[
            ("a", 23, Format::Text.code()),
            ("label", 25, Format::Text.code()),
        ]));
        resp1.extend_from_slice(&frame(b'N', b"SNOTICE\0C00000\0Mhello\0\0"));
        resp1.extend_from_slice(&frame(b'S', b"TimeZone\0UTC\0"));
        let mut notif = Vec::new();
        notif.extend_from_slice(&42i32.to_be_bytes());
        notif.extend_from_slice(b"events\0payload-1\0");
        resp1.extend_from_slice(&frame(b'A', &notif));
        resp1.extend_from_slice(&data_row(&[Some(b"3"), Some(b"ok")]));
        resp1.extend_from_slice(&data_row(&[None, Some(b"done")]));
        resp1.extend_from_slice(&cmd_complete("SELECT 2"));
        resp1.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp1).await.unwrap();

        let sql2 = read_query_frame(&mut sock).await;
        assert_eq!(sql2, "");
        let mut resp2 = Vec::new();
        resp2.extend_from_slice(&frame(b'I', b""));
        resp2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp2).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();
    conn.set_unnamed_statement_valid(true);

    let results = conn
        .simple_query("CREATE TEMP TABLE t(a int); INSERT INTO t VALUES (1),(2); UPDATE t SET a=3; MERGE INTO t; SELECT a, 'ok' FROM t")
        .await
        .unwrap();
    assert!(!conn.is_unnamed_statement_valid());
    assert_eq!(results.len(), 5);
    assert_eq!(results[0].affected_rows, 0);
    assert_eq!(results[1].affected_rows, 2);
    assert_eq!(results[2].affected_rows, 2);
    assert_eq!(results[3].affected_rows, 1);
    assert_eq!(results[4].affected_rows, 2);
    assert_eq!(
        results[4].rows,
        vec![
            vec![PgValue::Int4(3), PgValue::Text("ok".to_string())],
            vec![PgValue::Null, PgValue::Text("done".to_string())],
        ]
    );
    assert_eq!(conn.parameter("TimeZone"), Some("UTC"));
    let notes = conn.drain_notifications();
    assert_eq!(notes.len(), 1);
    assert_eq!(notes[0].process_id, 42);
    assert_eq!(notes[0].channel, "events");
    assert_eq!(notes[0].payload, "payload-1");

    let empty_res = conn.simple_query("").await.unwrap();
    assert_eq!(empty_res.len(), 1);
    assert_eq!(empty_res[0].command_tag, "");
    assert_eq!(empty_res[0].affected_rows, 0);
    srv.await.unwrap();
}

#[tokio::test]
async fn simple_query_drains_mid_batch_error_and_retires_on_protocol_violation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let srv = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        let _ = read_query_frame(&mut sock).await;
        let mut resp1 = Vec::new();
        resp1.extend_from_slice(&row_desc(&[("x", 23, Format::Text.code())]));
        resp1.extend_from_slice(&data_row(&[Some(b"1")]));
        resp1.extend_from_slice(&cmd_complete("SELECT 1"));
        resp1.extend_from_slice(&err_response("22012", "division by zero"));
        resp1.extend_from_slice(&frame(b'Z', b"E"));
        sock.write_all(&resp1).await.unwrap();

        let _ = read_query_frame(&mut sock).await;
        let mut resp2 = Vec::new();
        resp2.extend_from_slice(&row_desc(&[("v", 23, Format::Text.code())]));
        resp2.extend_from_slice(&data_row(&[Some(b"9")]));
        resp2.extend_from_slice(&cmd_complete("SELECT 1"));
        resp2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&resp2).await.unwrap();

        let _ = read_query_frame(&mut sock).await;
        let mut bad_resp = Vec::new();
        bad_resp.extend_from_slice(&row_desc(&[("a", 23, Format::Text.code())]));
        bad_resp.extend_from_slice(&data_row(&[Some(b"1"), Some(b"2")]));
        sock.write_all(&bad_resp).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    let conn = handle.connect().await.unwrap();

    assert!(matches!(
        conn.simple_query("SELECT\0bad").await,
        Err(PostgresError::Data(_))
    ));

    let err = conn
        .simple_query("SELECT 1; SELECT 1/0; SELECT 2")
        .await
        .unwrap_err();
    assert_eq!(err.sqlstate(), Some("22012"));
    assert_eq!(conn.transaction_status(), TransactionStatus::Failed);
    assert!(!conn.is_closed());

    let res2 = conn.simple_query("SELECT 9").await.unwrap();
    assert_eq!(res2[0].rows, vec![vec![PgValue::Int4(9)]]);
    assert_eq!(conn.transaction_status(), TransactionStatus::Idle);

    assert!(matches!(
        conn.simple_query("SELECT 1").await,
        Err(PostgresError::Protocol(_))
    ));
    assert!(conn.is_closed());
    assert!(matches!(
        conn.simple_query("SELECT 1").await,
        Err(PostgresError::Interface(_))
    ));
    srv.await.unwrap();
}
