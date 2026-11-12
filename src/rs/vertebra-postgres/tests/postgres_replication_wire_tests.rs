//! Wire-protocol integration tests for dedicated PostgreSQL `CopyBoth` logical replication sessions
//! (`_postgres::protocol::{read_frame, read_replication_message}` and
//! `_postgres::replication::PostgresReplicationConnection`).

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;

use _postgres::PoolerMode;
use _postgres::config::{PostgresConfig, RawPostgresOptions};
use _postgres::errors::PostgresError;
use _postgres::pool::{PostgresPool, PostgresPoolOptions};
use _postgres::protocol::{
    MAX_BACKEND_FRAME_LENGTH, ReplicationBackendMessage, read_frame, read_replication_message,
};
use _postgres::replication::{
    MAX_REPLICATION_IDENTIFIER_BYTES, PostgresReplicationConnection, VerifiedReplicationIdentity,
    format_pg_lsn, parse_pg_lsn,
};
use bytes::BytesMut;
use postgres_protocol::message::backend::Message;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::time::Instant;

const BOOL_OID: i32 = 16;
const NAME_OID: i32 = 19;
const INT2_OID: i32 = 21;
const INT4_OID: i32 = 23;
const TEXT_OID: i32 = 25;
const OID_OID: i32 = 26;
const NAME_ARRAY_OID: i32 = 1003;

fn build_backend_frame(tag: u8, payload: &[u8]) -> Vec<u8> {
    let len = (4 + payload.len()) as u32;
    let mut out = Vec::with_capacity(1 + 4 + payload.len());
    out.push(tag);
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(payload);
    out
}

fn auth_ok_frame() -> Vec<u8> {
    build_backend_frame(b'R', &0_i32.to_be_bytes())
}

fn parameter_status_frame(name: &str, value: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(name.len() + value.len() + 2);
    body.extend_from_slice(name.as_bytes());
    body.push(0);
    body.extend_from_slice(value.as_bytes());
    body.push(0);
    build_backend_frame(b'S', &body)
}

fn backend_key_frame(pid: i32, secret: i32) -> Vec<u8> {
    let mut body = Vec::with_capacity(8);
    body.extend_from_slice(&pid.to_be_bytes());
    body.extend_from_slice(&secret.to_be_bytes());
    build_backend_frame(b'K', &body)
}

fn ready_for_query_frame(status: u8) -> Vec<u8> {
    build_backend_frame(b'Z', &[status])
}

fn command_complete_frame(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    build_backend_frame(b'C', &body)
}

fn notice_frame(message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(b"SNOTICE\0C00000\0M");
    body.extend_from_slice(message.as_bytes());
    body.extend_from_slice(b"\0\0");
    build_backend_frame(b'N', &body)
}

fn error_response_frame(severity: &str, sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'V');
    body.extend_from_slice(severity.as_bytes());
    body.push(0);
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    build_backend_frame(b'E', &body)
}

fn copy_both_response_frame(format: u8, col_formats: &[u16]) -> Vec<u8> {
    let mut body = Vec::with_capacity(3 + 2 * col_formats.len());
    body.push(format);
    body.extend_from_slice(&(col_formats.len() as u16).to_be_bytes());
    for &cf in col_formats {
        body.extend_from_slice(&cf.to_be_bytes());
    }
    build_backend_frame(b'W', &body)
}

fn copy_data_frame(payload: &[u8]) -> Vec<u8> {
    build_backend_frame(b'd', payload)
}

fn copy_done_frame() -> Vec<u8> {
    build_backend_frame(b'c', &[])
}

fn row_description_frame(cols: &[(&str, i32)]) -> Vec<u8> {
    let mut rd = Vec::new();
    rd.extend_from_slice(&(cols.len() as i16).to_be_bytes());
    for &(name, oid) in cols {
        rd.extend_from_slice(name.as_bytes());
        rd.push(0);
        rd.extend_from_slice(&0_i32.to_be_bytes());
        rd.extend_from_slice(&0_i16.to_be_bytes());
        rd.extend_from_slice(&oid.to_be_bytes());
        rd.extend_from_slice(&(-1_i16).to_be_bytes());
        rd.extend_from_slice(&(-1_i32).to_be_bytes());
        rd.extend_from_slice(&0_i16.to_be_bytes());
    }
    build_backend_frame(b'T', &rd)
}

fn data_row_frame(cells: &[Option<&str>]) -> Vec<u8> {
    let mut dr = Vec::new();
    dr.extend_from_slice(&(cells.len() as i16).to_be_bytes());
    for cell in cells {
        match cell {
            Some(s) => {
                dr.extend_from_slice(&(s.len() as i32).to_be_bytes());
                dr.extend_from_slice(s.as_bytes());
            }
            None => dr.extend_from_slice(&(-1_i32).to_be_bytes()),
        }
    }
    build_backend_frame(b'D', &dr)
}

fn extended_select_response(
    param_oids: &[u32],
    cols: &[(&str, i32)],
    rows: &[Vec<Option<&str>>],
    include_close_complete: bool,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&build_backend_frame(b'1', &[]));
    let mut pd = Vec::with_capacity(2 + 4 * param_oids.len());
    pd.extend_from_slice(&(param_oids.len() as i16).to_be_bytes());
    for &oid in param_oids {
        pd.extend_from_slice(&oid.to_be_bytes());
    }
    out.extend_from_slice(&build_backend_frame(b't', &pd));
    out.extend_from_slice(&row_description_frame(cols));
    out.extend_from_slice(&build_backend_frame(b'2', &[]));
    out.extend_from_slice(&row_description_frame(cols));
    for row in rows {
        out.extend_from_slice(&data_row_frame(row));
    }
    out.extend_from_slice(&command_complete_frame(&format!("SELECT {}", rows.len())));
    if include_close_complete {
        out.extend_from_slice(&build_backend_frame(b'3', &[]));
    }
    out.extend_from_slice(&ready_for_query_frame(b'I'));
    out
}

fn identify_system_response(
    systemid: &str,
    timeline: &str,
    xlogpos: &str,
    dbname: Option<&str>,
) -> Vec<u8> {
    let cols = [
        ("systemid", TEXT_OID),
        ("timeline", INT4_OID),
        ("xlogpos", TEXT_OID),
        ("dbname", TEXT_OID),
    ];
    let mut out = Vec::new();
    out.extend_from_slice(&row_description_frame(&cols));
    out.extend_from_slice(&data_row_frame(&[
        Some(systemid),
        Some(timeline),
        Some(xlogpos),
        dbname,
    ]));
    out.extend_from_slice(&command_complete_frame("IDENTIFY_SYSTEM"));
    out.extend_from_slice(&ready_for_query_frame(b'I'));
    out
}

fn slot_columns() -> Vec<(&'static str, i32)> {
    vec![
        ("current_database", NAME_OID),
        ("database", NAME_OID),
        ("slot_type", TEXT_OID),
        ("plugin", NAME_OID),
        ("active", BOOL_OID),
        ("restart_lsn", TEXT_OID),
        ("confirmed_flush_lsn", TEXT_OID),
        ("wal_status", TEXT_OID),
    ]
}

fn pub_columns_pg15() -> Vec<(&'static str, i32)> {
    vec![
        ("pubname", NAME_OID),
        ("puballtables", BOOL_OID),
        ("pubinsert", BOOL_OID),
        ("pubupdate", BOOL_OID),
        ("pubdelete", BOOL_OID),
        ("pubtruncate", BOOL_OID),
        ("pubviaroot", BOOL_OID),
        ("schemaname", NAME_OID),
        ("tablename", NAME_OID),
        ("attnames", NAME_ARRAY_OID),
        ("rowfilter", TEXT_OID),
    ]
}

fn pub_columns_pg14() -> Vec<(&'static str, i32)> {
    vec![
        ("pubname", NAME_OID),
        ("puballtables", BOOL_OID),
        ("pubinsert", BOOL_OID),
        ("pubupdate", BOOL_OID),
        ("pubdelete", BOOL_OID),
        ("pubtruncate", BOOL_OID),
        ("pubviaroot", BOOL_OID),
        ("schemaname", NAME_OID),
        ("tablename", NAME_OID),
    ]
}

async fn read_startup_params(socket: &mut TcpStream) -> BTreeMap<String, String> {
    let mut first8 = [0_u8; 8];
    socket
        .read_exact(&mut first8)
        .await
        .expect("read startup header");
    let mut len = u32::from_be_bytes([first8[0], first8[1], first8[2], first8[3]]) as usize;
    let mut code = u32::from_be_bytes([first8[4], first8[5], first8[6], first8[7]]);
    if len == 8 && code == 80_877_103 {
        socket.write_all(b"N").await.expect("reject SSLRequest");
        socket.flush().await.expect("flush SSL N");
        socket
            .read_exact(&mut first8)
            .await
            .expect("read post-SSL startup header");
        len = u32::from_be_bytes([first8[0], first8[1], first8[2], first8[3]]) as usize;
        code = u32::from_be_bytes([first8[4], first8[5], first8[6], first8[7]]);
    }
    assert_eq!(code, 0x0003_0000, "expected PostgreSQL Protocol 3.0");
    let mut body = vec![0_u8; len - 8];
    socket
        .read_exact(&mut body)
        .await
        .expect("read startup body");
    assert_eq!(body.last().copied(), Some(0));
    let mut parts = body[..body.len() - 1].split(|&b| b == 0);
    let mut map = BTreeMap::new();
    while let Some(k) = parts.next() {
        if k.is_empty() {
            break;
        }
        let v = parts.next().expect("startup parameter value");
        map.insert(
            String::from_utf8(k.to_vec()).expect("utf8 startup key"),
            String::from_utf8(v.to_vec()).expect("utf8 startup val"),
        );
    }
    map
}

async fn complete_startup_ready_version(
    socket: &mut TcpStream,
    pid: i32,
    server_version: &str,
) -> BTreeMap<String, String> {
    let params = read_startup_params(socket).await;
    let mut banner = Vec::new();
    banner.extend_from_slice(&auth_ok_frame());
    banner.extend_from_slice(&parameter_status_frame("server_version", server_version));
    banner.extend_from_slice(&parameter_status_frame("client_encoding", "UTF8"));
    if let Some(app) = params.get("application_name") {
        banner.extend_from_slice(&parameter_status_frame("application_name", app));
    }
    banner.extend_from_slice(&backend_key_frame(pid, 0x2233_4455));
    banner.extend_from_slice(&ready_for_query_frame(b'I'));
    socket
        .write_all(&banner)
        .await
        .expect("write startup banner");
    socket.flush().await.expect("flush startup banner");
    params
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CapturedExtendedQuery {
    sql: String,
    param_oids: Vec<u32>,
    params: Vec<Option<String>>,
    saw_close_portal: bool,
}

#[derive(Debug, PartialEq, Eq)]
enum FrontendExchange {
    SimpleQuery(String),
    Extended(CapturedExtendedQuery),
    CopyDone,
    Terminate,
    Other(u8, Vec<u8>),
    Eof,
}

fn read_cstring(buf: &[u8], pos: &mut usize) -> String {
    let start = *pos;
    while *pos < buf.len() && buf[*pos] != 0 {
        *pos += 1;
    }
    let s = String::from_utf8(buf[start..*pos].to_vec()).expect("utf8 cstring");
    assert!(*pos < buf.len(), "unterminated cstring");
    *pos += 1;
    s
}

async fn read_frontend_exchange(socket: &mut TcpStream) -> FrontendExchange {
    let mut hdr = [0_u8; 5];
    if socket.read_exact(&mut hdr).await.is_err() {
        return FrontendExchange::Eof;
    }
    let first_tag = hdr[0];
    let first_len = u32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
    assert!(
        first_len >= 4,
        "invalid frontend frame length {first_len} for tag {}",
        first_tag as char
    );
    let mut first_body = vec![0_u8; first_len - 4];
    if !first_body.is_empty() && socket.read_exact(&mut first_body).await.is_err() {
        return FrontendExchange::Eof;
    }

    match first_tag {
        b'Q' => {
            assert_eq!(first_body.last().copied(), Some(0));
            let sql = String::from_utf8(first_body[..first_body.len() - 1].to_vec())
                .expect("utf8 simple query");
            FrontendExchange::SimpleQuery(sql)
        }
        b'c' => FrontendExchange::CopyDone,
        b'X' => FrontendExchange::Terminate,
        b'P' | b'C' => {
            let mut sql = String::new();
            let mut param_oids = Vec::new();
            let mut params = Vec::new();
            let mut saw_close_portal = false;

            let mut cur_tag = first_tag;
            let mut cur_body = first_body;
            loop {
                match cur_tag {
                    b'P' => {
                        let mut pos = 0;
                        let _stmt = read_cstring(&cur_body, &mut pos);
                        sql = read_cstring(&cur_body, &mut pos);
                        let n = i16::from_be_bytes([cur_body[pos], cur_body[pos + 1]]) as usize;
                        pos += 2;
                        param_oids.clear();
                        for _ in 0..n {
                            let oid = u32::from_be_bytes([
                                cur_body[pos],
                                cur_body[pos + 1],
                                cur_body[pos + 2],
                                cur_body[pos + 3],
                            ]);
                            pos += 4;
                            param_oids.push(oid);
                        }
                    }
                    b'B' => {
                        let mut pos = 0;
                        let _portal = read_cstring(&cur_body, &mut pos);
                        let _stmt = read_cstring(&cur_body, &mut pos);
                        let fmt_n = i16::from_be_bytes([cur_body[pos], cur_body[pos + 1]]) as usize;
                        pos += 2 + 2 * fmt_n;
                        let val_n = i16::from_be_bytes([cur_body[pos], cur_body[pos + 1]]) as usize;
                        pos += 2;
                        params.clear();
                        for _ in 0..val_n {
                            let vlen = i32::from_be_bytes([
                                cur_body[pos],
                                cur_body[pos + 1],
                                cur_body[pos + 2],
                                cur_body[pos + 3],
                            ]);
                            pos += 4;
                            if vlen < 0 {
                                params.push(None);
                            } else {
                                let vlen = vlen as usize;
                                let s = String::from_utf8(cur_body[pos..pos + vlen].to_vec())
                                    .expect("utf8 bind param");
                                pos += vlen;
                                params.push(Some(s));
                            }
                        }
                    }
                    b'C' => {
                        if cur_body.first().copied() == Some(b'P') {
                            saw_close_portal = true;
                        }
                    }
                    b'D' | b'E' => {}
                    b'S' => {
                        return FrontendExchange::Extended(CapturedExtendedQuery {
                            sql,
                            param_oids,
                            params,
                            saw_close_portal,
                        });
                    }
                    other => panic!("unexpected frame inside extended batch: {}", other as char),
                }

                let mut next_hdr = [0_u8; 5];
                socket
                    .read_exact(&mut next_hdr)
                    .await
                    .expect("read next extended frame header");
                cur_tag = next_hdr[0];
                let next_len =
                    u32::from_be_bytes([next_hdr[1], next_hdr[2], next_hdr[3], next_hdr[4]])
                        as usize;
                cur_body = vec![0_u8; next_len - 4];
                if !cur_body.is_empty() {
                    socket
                        .read_exact(&mut cur_body)
                        .await
                        .expect("read next extended frame body");
                }
            }
        }
        other => FrontendExchange::Other(other, first_body),
    }
}

async fn assert_clean_terminate_to_eof(socket: &mut TcpStream) {
    let msg = tokio::time::timeout(Duration::from_secs(2), read_frontend_exchange(socket))
        .await
        .expect("timed out awaiting Terminate/EOF");
    assert_eq!(msg, FrontendExchange::Terminate);
    let eof = tokio::time::timeout(Duration::from_secs(2), read_frontend_exchange(socket))
        .await
        .expect("timed out awaiting EOF after Terminate");
    assert_eq!(eof, FrontendExchange::Eof);
}

fn config_with_mode(port: u16, mode: PoolerMode, timeout: Duration) -> PostgresConfig {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "alice", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.connect_timeout = timeout.as_secs_f64();
    raw.pooler_mode = mode.as_str().to_string();
    raw.application_name = "vtb-repl-test".to_string();
    raw.session_gucs
        .insert("TimeZone".to_string(), "UTC".to_string());
    PostgresConfig::validate(raw).expect("valid config")
}

fn direct_config(port: u16) -> PostgresConfig {
    config_with_mode(port, PoolerMode::Direct, Duration::from_secs(3))
}

struct ChunkedReader {
    chunks: Vec<Vec<u8>>,
    idx: usize,
    offset: usize,
}

impl ChunkedReader {
    fn new(chunks: Vec<Vec<u8>>) -> Self {
        Self {
            chunks,
            idx: 0,
            offset: 0,
        }
    }
}

impl AsyncRead for ChunkedReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        while self.idx < self.chunks.len() {
            let chunk_len = self.chunks[self.idx].len();
            if self.offset < chunk_len {
                let n = (chunk_len - self.offset).min(buf.remaining());
                let start = self.offset;
                let slice = self.chunks[self.idx][start..start + n].to_vec();
                buf.put_slice(&slice);
                self.offset += n;
                if self.offset == chunk_len {
                    self.idx += 1;
                    self.offset = 0;
                }
                return Poll::Ready(Ok(()));
            }
            self.idx += 1;
            self.offset = 0;
        }
        Poll::Ready(Ok(()))
    }
}

struct PendingReader;

impl AsyncRead for PendingReader {
    fn poll_read(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        _buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Poll::Pending
    }
}

fn assert_no_correlated_select_subquery(sql: &str) {
    let upper = sql.to_ascii_uppercase();
    let after_first = upper.strip_prefix("SELECT ").expect("SELECT query");
    assert!(
        !after_first.contains("SELECT "),
        "SQL query must not contain correlated/nested SELECT subqueries: {sql}"
    );
}

#[test]
fn copyboth_lsn_format_and_parse_roundtrip_and_strict_rejection() {
    let cases = [
        (0_u64, "0/0"),
        (1_u64, "0/1"),
        (0x0000_0001_0000_01A0_u64, "1/1A0"),
        (0xABCD_EF01_2345_6789_u64, "ABCDEF01/23456789"),
        (u64::MAX, "FFFFFFFF/FFFFFFFF"),
    ];
    for (lsn, expected) in cases {
        let formatted = format_pg_lsn(lsn);
        assert_eq!(formatted, expected);
        assert_eq!(parse_pg_lsn(&formatted).expect("parse formatted"), lsn);
        let lower = formatted.to_ascii_lowercase();
        assert_eq!(parse_pg_lsn(&lower).expect("parse lowercase"), lsn);
    }
    assert_eq!(
        parse_pg_lsn("00000001/000001a0").expect("zero-padded LSN"),
        0x0000_0001_0000_01A0_u64
    );

    let invalid = [
        "",
        "/",
        "1/",
        "/1",
        "1/2/3",
        "100000000/1",
        "1/100000000",
        " 1/1A0",
        "1/1A0 ",
        "1 /1A0",
        "1/ 1A0",
        "+1/1A0",
        "1/+1A0",
        "-1/1A0",
        "0x1/1A0",
        "1/0x1A0",
        "G/0",
        "0/Z",
        "1_0/1",
    ];
    for bad in invalid {
        assert!(
            parse_pg_lsn(bad).is_err(),
            "expected parse_pg_lsn({bad:?}) to fail"
        );
    }
}

#[tokio::test]
async fn copyboth_read_frame_and_read_replication_message_validate_bounds_and_deadlines() {
    let mut stream_bytes = Vec::new();
    stream_bytes.extend_from_slice(&notice_frame("warmup"));
    stream_bytes.extend_from_slice(&copy_both_response_frame(0, &[]));
    stream_bytes.extend_from_slice(&copy_data_frame(b"k\x00\x00\x00\x00\x00\x00\x01\x00"));
    stream_bytes.extend_from_slice(&ready_for_query_frame(b'I'));

    let byte_chunks: Vec<Vec<u8>> = stream_bytes.iter().map(|b| vec![*b]).collect();
    let mut reader = ChunkedReader::new(byte_chunks);
    let mut scratch = BytesMut::new();

    let m1 = read_replication_message(&mut reader, &mut scratch, MAX_BACKEND_FRAME_LENGTH, None)
        .await
        .expect("NoticeResponse");
    assert!(matches!(
        m1,
        ReplicationBackendMessage::Standard(Message::NoticeResponse(_))
    ));

    let m2 = read_replication_message(&mut reader, &mut scratch, MAX_BACKEND_FRAME_LENGTH, None)
        .await
        .expect("CopyBothResponse");
    match m2 {
        ReplicationBackendMessage::CopyBothResponse {
            format,
            column_formats,
        } => {
            assert_eq!(format, 0);
            assert!(column_formats.is_empty());
        }
        other => panic!("expected CopyBothResponse, got {other:?}"),
    }

    let (tag, payload) = read_frame(&mut reader, &mut scratch, MAX_BACKEND_FRAME_LENGTH, None)
        .await
        .expect("CopyData via read_frame");
    assert_eq!(tag, b'd');
    assert_eq!(&payload[..], b"k\x00\x00\x00\x00\x00\x00\x01\x00");

    let m4 = read_replication_message(&mut reader, &mut scratch, MAX_BACKEND_FRAME_LENGTH, None)
        .await
        .expect("ReadyForQuery");
    match m4 {
        ReplicationBackendMessage::Standard(Message::ReadyForQuery(body)) => {
            assert_eq!(body.status(), b'I');
        }
        other => panic!("expected ReadyForQuery, got {other:?}"),
    }

    let oversized = {
        let mut v = vec![b'W'];
        v.extend_from_slice(&((1024_u32) + 1).to_be_bytes());
        v
    };
    let mut over_reader = ChunkedReader::new(vec![oversized]);
    let mut over_buf = BytesMut::with_capacity(64);
    let err = read_frame(&mut over_reader, &mut over_buf, 1024, None)
        .await
        .expect_err("oversized header rejected before allocation");
    assert!(matches!(err, PostgresError::Protocol(_)));
    assert!(over_buf.capacity() <= 8192);

    let undersized = vec![b'W', 0, 0, 0, 3];
    let mut under_reader = ChunkedReader::new(vec![undersized]);
    let mut under_buf = BytesMut::new();
    assert!(matches!(
        read_frame(&mut under_reader, &mut under_buf, 1024, None).await,
        Err(PostgresError::Protocol(_))
    ));

    let bad_w_frames: Vec<Vec<u8>> = vec![
        build_backend_frame(b'W', &[]),
        build_backend_frame(b'W', &[0, 0]),
        copy_both_response_frame(1, &[]),
        copy_both_response_frame(0, &[0, 0]),
        build_backend_frame(b'W', &[0, 0xFF, 0xFE]),
        build_backend_frame(b'W', &[0, 0, 2, 0, 0]),
        build_backend_frame(b'W', &[0, 0, 0, 0xAA]),
    ];
    for bad in bad_w_frames {
        let mut r = ChunkedReader::new(vec![bad.clone()]);
        let mut b = BytesMut::new();
        let err = read_replication_message(&mut r, &mut b, MAX_BACKEND_FRAME_LENGTH, None)
            .await
            .expect_err("malformed CopyBothResponse must fail");
        assert!(
            matches!(err, PostgresError::Protocol(_)),
            "expected Protocol error for {bad:?}, got {err:?}"
        );
    }

    let mut pending = PendingReader;
    let mut tbuf = BytesMut::new();
    let expired = Instant::now()
        .checked_sub(Duration::from_millis(5))
        .unwrap_or_else(Instant::now);
    let timeout_err = read_replication_message(
        &mut pending,
        &mut tbuf,
        MAX_BACKEND_FRAME_LENGTH,
        Some(expired),
    )
    .await
    .expect_err("expired deadline must fail immediately");
    assert!(
        matches!(
            timeout_err,
            PostgresError::Transport(_) | PostgresError::Operational(_)
        ),
        "unexpected timeout error: {timeout_err:?}"
    );
    assert!(tbuf.capacity() <= 8192);
}

#[tokio::test]
async fn copyboth_and_replication_identity_reject_non_session_pooler_modes_and_bad_identifiers_pre_dial()
 {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind listener");
    let port = listener.local_addr().expect("local addr").port();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_bg = Arc::clone(&accepts);
    let bg = tokio::spawn(async move {
        while let Ok((_sock, _)) = listener.accept().await {
            accepts_bg.fetch_add(1, Ordering::SeqCst);
        }
    });

    for bad_mode in [
        PoolerMode::Transaction,
        PoolerMode::Statement,
        PoolerMode::Auto,
    ] {
        let cfg = config_with_mode(port, bad_mode, Duration::from_secs(1));
        let probe_err =
            PostgresReplicationConnection::probe_identity(&cfg, "vertebra_slot", "vertebra_pub")
                .await
                .expect_err("probe_identity must reject non-session PoolerMode");
        assert!(
            matches!(probe_err, PostgresError::Interface(_)),
            "expected Interface for {bad_mode:?}, got {probe_err:?}"
        );

        let conn_err = PostgresReplicationConnection::connect_logical(
            &cfg,
            "vertebra_slot",
            "vertebra_pub",
            None,
        )
        .await
        .expect_err("connect_logical must reject non-session PoolerMode");
        assert!(
            matches!(conn_err, PostgresError::Interface(_)),
            "expected Interface for {bad_mode:?}, got {conn_err:?}"
        );
    }

    let valid_cfg = direct_config(port);
    let overlong = "s".repeat(MAX_REPLICATION_IDENTIFIER_BYTES + 1);
    let bad_names = [
        "",
        "bad\0name",
        "bad\nname",
        "bad\rname",
        "bad\x07name",
        overlong.as_str(),
    ];
    for bad in bad_names {
        assert!(
            PostgresReplicationConnection::probe_identity(&valid_cfg, bad, "vertebra_pub")
                .await
                .is_err(),
            "probe_identity must reject invalid slot {bad:?}"
        );
        assert!(
            PostgresReplicationConnection::probe_identity(&valid_cfg, "vertebra_slot", bad)
                .await
                .is_err(),
            "probe_identity must reject invalid publication {bad:?}"
        );
        assert!(
            PostgresReplicationConnection::connect_logical(&valid_cfg, bad, "vertebra_pub", None)
                .await
                .is_err(),
            "connect_logical must reject invalid slot {bad:?}"
        );
        assert!(
            PostgresReplicationConnection::connect_logical(&valid_cfg, "vertebra_slot", bad, None)
                .await
                .is_err(),
            "connect_logical must reject invalid publication {bad:?}"
        );
    }

    tokio::task::yield_now().await;
    assert_eq!(
        accepts.load(Ordering::SeqCst),
        0,
        "zero TCP sockets may be opened when PoolerMode or identifiers fail validation"
    );
    bg.abort();
}

#[tokio::test]
async fn copyboth_connect_logical_pg15_startup_left_join_pool_isolation_and_clean_shutdown() {
    let pool_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind pool listener");
    let pool_port = pool_listener.local_addr().expect("addr").port();
    let pool_bg = tokio::spawn(async move {
        let (mut sock, _) = pool_listener.accept().await.expect("accept pool conn");
        let _ = complete_startup_ready_version(&mut sock, 101, "16.4").await;
        let _ = read_frontend_exchange(&mut sock).await;
    });
    let pool_cfg = direct_config(pool_port);
    let pool_opts = PostgresPoolOptions {
        min_size: 0,
        max_size: 1,
        acquire_timeout: Duration::from_millis(50),
        ..PostgresPoolOptions::default()
    };
    let pool = PostgresPool::new(pool_cfg, pool_opts).expect("pool");
    let held_lease = pool.acquire().await.expect("acquire sole pool lease");

    let repl_listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind repl listener");
    let repl_port = repl_listener.local_addr().expect("addr").port();
    let meta_closed_before_repl = Arc::new(AtomicBool::new(false));
    let meta_closed_flag = Arc::clone(&meta_closed_before_repl);
    let captured_queries: Arc<Mutex<Vec<CapturedExtendedQuery>>> = Arc::new(Mutex::new(Vec::new()));
    let captured_queries_bg = Arc::clone(&captured_queries);

    let coalesced_keepalive =
        b"k\x00\x00\x00\x01\x00\x00\x02\x00\x00\x00\x00\x00\x00\x00\x00\x00\x01";

    let server_task = tokio::spawn(async move {
        let (mut meta_sock, _) = repl_listener.accept().await.expect("accept metadata sock");
        let meta_startup =
            complete_startup_ready_version(&mut meta_sock, 201, "16.4 (Debian 16.4-1)").await;
        assert_eq!(
            meta_startup.get("database").map(String::as_str),
            Some("appdb")
        );
        assert!(
            !meta_startup.contains_key("replication"),
            "preflight catalog socket must NOT use replication=database"
        );
        assert_eq!(
            meta_startup.get("TimeZone").map(String::as_str),
            Some("UTC")
        );

        let FrontendExchange::Extended(q1) = read_frontend_exchange(&mut meta_sock).await else {
            panic!("expected parameterized pg_replication_slots extended query");
        };
        assert!(q1.sql.contains("pg_replication_slots"));
        assert_eq!(q1.param_oids, vec![TEXT_OID as u32]);
        assert_eq!(q1.params, vec![Some("vertebra_slot".to_string())]);
        let slot_cols = slot_columns();
        let slot_rows = vec![vec![
            Some("appdb"),
            Some("appdb"),
            Some("logical"),
            Some("pgoutput"),
            Some("f"),
            Some("1/100"),
            Some("1/180"),
            Some("reserved"),
        ]];
        meta_sock
            .write_all(&extended_select_response(
                &q1.param_oids,
                &slot_cols,
                &slot_rows,
                q1.saw_close_portal,
            ))
            .await
            .expect("write slot row");
        meta_sock.flush().await.expect("flush slot row");

        let FrontendExchange::Extended(q2) = read_frontend_exchange(&mut meta_sock).await else {
            panic!("expected parameterized pg_publication LEFT JOIN extended query");
        };
        assert!(q2.sql.contains("LEFT JOIN"));
        assert!(q2.sql.contains("pg_publication_tables"));
        assert!(
            q2.sql.contains("pt.attnames") && q2.sql.contains("pt.rowfilter"),
            "PG16 must include attnames and rowfilter: {}",
            q2.sql
        );
        assert_no_correlated_select_subquery(&q2.sql);
        assert_eq!(q2.param_oids, vec![TEXT_OID as u32]);
        assert_eq!(q2.params, vec![Some("vertebra_pub".to_string())]);
        let pub_cols = pub_columns_pg15();
        let pub_rows = vec![
            vec![
                Some("vertebra_pub"),
                Some("f"),
                Some("t"),
                Some("t"),
                Some("t"),
                Some("f"),
                Some("f"),
                Some("public"),
                Some("accounts"),
                Some("{id,email}"),
                Some("(active = true)"),
            ],
            vec![
                Some("vertebra_pub"),
                Some("f"),
                Some("t"),
                Some("t"),
                Some("t"),
                Some("f"),
                Some("f"),
                Some("public"),
                Some("ledger"),
                None,
                None,
            ],
        ];
        meta_sock
            .write_all(&extended_select_response(
                &q2.param_oids,
                &pub_cols,
                &pub_rows,
                q2.saw_close_portal,
            ))
            .await
            .expect("write pub rows");
        meta_sock.flush().await.expect("flush pub rows");

        let FrontendExchange::Extended(q3) = read_frontend_exchange(&mut meta_sock).await else {
            panic!("expected parameterized custom-type preload ExtendedQuery");
        };
        assert_no_correlated_select_subquery(&q3.sql);
        assert_eq!(q3.params, vec![Some("vertebra_pub".to_string())]);
        let preload_cols = [
            ("oid", OID_OID),
            ("nspname", NAME_OID),
            ("typname", NAME_OID),
            ("typtype", TEXT_OID),
            ("typcategory", TEXT_OID),
            ("typdelim", TEXT_OID),
            ("typelem", OID_OID),
            ("typbasetype", OID_OID),
            ("rngsubtype", OID_OID),
            ("mrngtypid", OID_OID),
            ("rngsubtype", OID_OID),
            ("extname", NAME_OID),
            ("attname", NAME_OID),
            ("atttypid", OID_OID),
            ("atttypmod", INT4_OID),
            ("attnum", INT2_OID),
        ];
        meta_sock
            .write_all(&extended_select_response(
                &q3.param_oids,
                &preload_cols,
                &[],
                q3.saw_close_portal,
            ))
            .await
            .expect("write empty custom-type preload rows");
        meta_sock.flush().await.expect("flush preload rows");

        captured_queries_bg.lock().await.extend([q1, q2, q3]);
        assert_clean_terminate_to_eof(&mut meta_sock).await;
        meta_closed_flag.store(true, Ordering::SeqCst);

        let (mut repl_sock, _) = repl_listener.accept().await.expect("accept repl sock");
        assert!(
            meta_closed_flag.load(Ordering::SeqCst),
            "metadata socket must close before dialing replication socket"
        );
        let repl_startup = complete_startup_ready_version(&mut repl_sock, 202, "16.4").await;
        assert_eq!(
            repl_startup.get("replication").map(String::as_str),
            Some("database")
        );
        assert_eq!(
            repl_startup.get("database").map(String::as_str),
            Some("appdb")
        );
        assert!(
            !repl_startup.contains_key("TimeZone"),
            "replication=database startup must omit arbitrary session GUCs"
        );

        let cmd1 = read_frontend_exchange(&mut repl_sock).await;
        assert_eq!(
            cmd1,
            FrontendExchange::SimpleQuery("IDENTIFY_SYSTEM".to_string())
        );
        repl_sock
            .write_all(&identify_system_response(
                "7345678901234567890",
                "3",
                "1/500",
                Some("appdb"),
            ))
            .await
            .expect("write IDENTIFY_SYSTEM");
        repl_sock.flush().await.expect("flush IDENTIFY_SYSTEM");

        let cmd2 = read_frontend_exchange(&mut repl_sock).await;
        assert_eq!(
            cmd2,
            FrontendExchange::SimpleQuery(
                "START_REPLICATION SLOT \"vertebra_slot\" LOGICAL 1/1A0 (proto_version '1', publication_names 'vertebra_pub', binary 'false', streaming 'false', messages 'true')".to_string()
            )
        );

        let mut start_reply = Vec::new();
        start_reply.extend_from_slice(&notice_frame("starting logical decoding"));
        start_reply.extend_from_slice(&parameter_status_frame("wal_sender_timeout", "60s"));
        start_reply.extend_from_slice(&copy_both_response_frame(0, &[]));
        start_reply.extend_from_slice(&copy_data_frame(coalesced_keepalive));
        repl_sock
            .write_all(&start_reply)
            .await
            .expect("write coalesced CopyBothResponse + CopyData");
        repl_sock.flush().await.expect("flush start_reply");

        let end_cmd = read_frontend_exchange(&mut repl_sock).await;
        assert_eq!(end_cmd, FrontendExchange::CopyDone);

        let mut drain_reply = Vec::new();
        drain_reply.extend_from_slice(&copy_data_frame(b"w-inflight-wal-tail"));
        drain_reply.extend_from_slice(&copy_done_frame());
        drain_reply.extend_from_slice(&command_complete_frame("COPY 0"));
        drain_reply.extend_from_slice(&ready_for_query_frame(b'I'));
        repl_sock
            .write_all(&drain_reply)
            .await
            .expect("write shutdown CopyDone sequence");
        repl_sock.flush().await.expect("flush shutdown");

        assert_clean_terminate_to_eof(&mut repl_sock).await;
    });

    let cfg = config_with_mode(repl_port, PoolerMode::Session, Duration::from_secs(3));
    let mut repl = PostgresReplicationConnection::connect_logical(
        &cfg,
        "vertebra_slot",
        "vertebra_pub",
        Some(0x0000_0001_0000_01A0),
    )
    .await
    .expect("connect_logical succeeds while pool is exhausted");

    assert!(meta_closed_before_repl.load(Ordering::SeqCst));
    assert!(!repl.is_closed());
    assert_eq!(repl.parameter("wal_sender_timeout"), Some("60s"));
    assert_eq!(
        repl.buffered_read_len(),
        5 + coalesced_keepalive.len(),
        "coalesced post-'W' CopyData frame must remain buffered in read_buf"
    );

    let id = repl.identity().clone();
    assert_eq!(id.system_identifier, "7345678901234567890");
    assert_eq!(id.timeline, 3);
    assert_eq!(id.database, "appdb");
    assert_eq!(id.slot, "vertebra_slot");
    assert_eq!(id.restart_lsn, 0x0000_0001_0000_0100);
    assert_eq!(id.confirmed_flush_lsn, 0x0000_0001_0000_0180);
    assert_eq!(id.publication_fingerprint.len(), 64);
    assert!(
        id.publication_fingerprint
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    );

    repl.shutdown(Instant::now() + Duration::from_secs(3))
        .await
        .expect("clean CopyDone shutdown");
    assert!(repl.is_closed());
    repl.shutdown(Instant::now() + Duration::from_secs(1))
        .await
        .expect("idempotent second shutdown");

    server_task.await.expect("server assertions passed");
    drop(held_lease);
    let _ = pool.close(Duration::from_secs(1)).await;
    pool_bg.abort();
}

#[tokio::test]
async fn copyboth_pg14_default_zero_lsn_empty_publication_and_option_quote_escaping() {
    async fn run_once(
        server_version: &'static str,
        slot_name: &'static str,
        pub_name: &'static str,
        expected_start_sql: &'static str,
        pub_rows: Vec<Vec<Option<&'static str>>>,
    ) -> VerifiedReplicationIdentity {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();

        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("accept meta");
            let _ = complete_startup_ready_version(&mut meta, 301, server_version).await;

            let FrontendExchange::Extended(q1) = read_frontend_exchange(&mut meta).await else {
                panic!("expected slot query");
            };
            assert_eq!(q1.params, vec![Some(slot_name.to_string())]);
            meta.write_all(&extended_select_response(
                &q1.param_oids,
                &slot_columns(),
                &[vec![
                    Some("appdb"),
                    Some("appdb"),
                    Some("logical"),
                    Some("pgoutput"),
                    Some("f"),
                    Some("2/10"),
                    Some("2/20"),
                    Some("extended"),
                ]],
                q1.saw_close_portal,
            ))
            .await
            .expect("write slot");
            meta.flush().await.expect("flush");

            let FrontendExchange::Extended(q2) = read_frontend_exchange(&mut meta).await else {
                panic!("expected pub query");
            };
            assert_no_correlated_select_subquery(&q2.sql);
            if server_version.starts_with("14.") {
                assert!(
                    !q2.sql.contains("attnames") && !q2.sql.contains("rowfilter"),
                    "PG14 must omit PG15+ attnames/rowfilter columns: {}",
                    q2.sql
                );
            }
            let cols = if server_version.starts_with("14.") {
                pub_columns_pg14()
            } else {
                pub_columns_pg15()
            };
            meta.write_all(&extended_select_response(
                &q2.param_oids,
                &cols,
                &pub_rows,
                q2.saw_close_portal,
            ))
            .await
            .expect("write pub");
            meta.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut meta).await;

            let (mut repl, _) = listener.accept().await.expect("accept repl");
            let _ = complete_startup_ready_version(&mut repl, 302, server_version).await;
            assert_eq!(
                read_frontend_exchange(&mut repl).await,
                FrontendExchange::SimpleQuery("IDENTIFY_SYSTEM".to_string())
            );
            repl.write_all(&identify_system_response(
                "9990001112223334445",
                "1",
                "2/900",
                Some("appdb"),
            ))
            .await
            .expect("write identify");
            repl.flush().await.expect("flush");

            let start_cmd = read_frontend_exchange(&mut repl).await;
            assert_eq!(
                start_cmd,
                FrontendExchange::SimpleQuery(expected_start_sql.to_string())
            );
            assert!(
                !expected_start_sql.contains("two_phase"),
                "must never emit two_phase"
            );
            repl.write_all(&copy_both_response_frame(0, &[]))
                .await
                .expect("write W");
            repl.flush().await.expect("flush W");

            assert_eq!(
                read_frontend_exchange(&mut repl).await,
                FrontendExchange::CopyDone
            );
            let mut end = Vec::new();
            end.extend_from_slice(&copy_done_frame());
            end.extend_from_slice(&command_complete_frame("COPY 0"));
            end.extend_from_slice(&ready_for_query_frame(b'I'));
            repl.write_all(&end).await.expect("write end");
            repl.flush().await.expect("flush end");
            assert_clean_terminate_to_eof(&mut repl).await;
        });

        let cfg = direct_config(port);
        let mut conn =
            PostgresReplicationConnection::connect_logical(&cfg, slot_name, pub_name, None)
                .await
                .expect("connect_logical");
        let id = conn.identity().clone();
        conn.shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .expect("shutdown");
        srv.await.expect("server ok");
        id
    }

    let empty_pg14_a = run_once(
        "14.12",
        "vertebra_slot",
        "vertebra_pub",
        "START_REPLICATION SLOT \"vertebra_slot\" LOGICAL 0/0 (proto_version '1', publication_names 'vertebra_pub', binary 'false', streaming 'false', messages 'true')",
        vec![vec![
            Some("vertebra_pub"),
            Some("f"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("f"),
            Some("f"),
            None,
            None,
        ]],
    )
    .await;

    let empty_pg14_b = run_once(
        "14.12",
        "vertebra_slot",
        "vertebra_pub",
        "START_REPLICATION SLOT \"vertebra_slot\" LOGICAL 0/0 (proto_version '1', publication_names 'vertebra_pub', binary 'false', streaming 'false', messages 'true')",
        vec![vec![
            Some("vertebra_pub"),
            Some("f"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("f"),
            Some("f"),
            None,
            None,
        ]],
    )
    .await;
    assert_eq!(
        empty_pg14_a.publication_fingerprint, empty_pg14_b.publication_fingerprint,
        "empty existing publication fingerprint must be deterministic"
    );

    let populated_pg14 = run_once(
        "14.12",
        "vertebra_slot",
        "vertebra_pub",
        "START_REPLICATION SLOT \"vertebra_slot\" LOGICAL 0/0 (proto_version '1', publication_names 'vertebra_pub', binary 'false', streaming 'false', messages 'true')",
        vec![vec![
            Some("vertebra_pub"),
            Some("f"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("f"),
            Some("f"),
            Some(""),
            Some(""),
        ]],
    )
    .await;
    assert_ne!(
        empty_pg14_a.publication_fingerprint, populated_pg14.publication_fingerprint,
        "NULL table columns must never collide with empty-string columns in SHA-256 framing"
    );

    let tricky_slot = "slot\"a";
    let tricky_pub = "Pub'a,b\"c\\";
    let _escaped = run_once(
        "17.2",
        tricky_slot,
        tricky_pub,
        "START_REPLICATION SLOT \"slot\"\"a\" LOGICAL 0/0 (proto_version '1', publication_names '\"Pub''a,b\"\"c\\\"', binary 'false', streaming 'false', messages 'true')",
        vec![vec![
            Some(tricky_pub),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("f"),
            None,
            None,
            None,
            None,
        ]],
    )
    .await;

    let breakout_pub = "pub', binary 'true";
    let _breakout = run_once(
        "17.2",
        "slot_ok",
        breakout_pub,
        "START_REPLICATION SLOT \"slot_ok\" LOGICAL 0/0 (proto_version '1', publication_names '\"pub'', binary ''true\"', binary 'false', streaming 'false', messages 'true')",
        vec![vec![
            Some(breakout_pub),
            Some("f"),
            Some("t"),
            Some("t"),
            Some("t"),
            Some("f"),
            Some("f"),
            None,
            None,
            None,
            None,
        ]],
    )
    .await;
}

#[tokio::test]
async fn replication_identity_probe_validates_catalog_and_identify_system_without_starting_copyboth()
 {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let port = listener.local_addr().expect("addr").port();

    let srv = tokio::spawn(async move {
        let (mut meta, _) = listener.accept().await.expect("accept meta");
        let meta_params = complete_startup_ready_version(&mut meta, 401, "18beta1").await;
        assert!(!meta_params.contains_key("replication"));

        let FrontendExchange::Extended(q1) = read_frontend_exchange(&mut meta).await else {
            panic!("expected slot query");
        };
        meta.write_all(&extended_select_response(
            &q1.param_oids,
            &slot_columns(),
            &[vec![
                Some("appdb"),
                Some("appdb"),
                Some("logical"),
                Some("pgoutput"),
                Some("f"),
                Some("3/A0"),
                Some("3/F8"),
                Some("reserved"),
            ]],
            q1.saw_close_portal,
        ))
        .await
        .expect("write slot");
        meta.flush().await.expect("flush");

        let FrontendExchange::Extended(q2) = read_frontend_exchange(&mut meta).await else {
            panic!("expected pub query");
        };
        assert!(q2.sql.contains("pt.attnames") && q2.sql.contains("pt.rowfilter"));
        meta.write_all(&extended_select_response(
            &q2.param_oids,
            &pub_columns_pg15(),
            &[vec![
                Some("vertebra_pub"),
                Some("f"),
                Some("t"),
                Some("t"),
                Some("t"),
                Some("f"),
                Some("f"),
                None,
                None,
                None,
                None,
            ]],
            q2.saw_close_portal,
        ))
        .await
        .expect("write pub");
        meta.flush().await.expect("flush");
        assert_clean_terminate_to_eof(&mut meta).await;

        let (mut repl, _) = listener.accept().await.expect("accept repl");
        let repl_params = complete_startup_ready_version(&mut repl, 402, "18beta1").await;
        assert_eq!(
            repl_params.get("replication").map(String::as_str),
            Some("database")
        );
        assert_eq!(
            read_frontend_exchange(&mut repl).await,
            FrontendExchange::SimpleQuery("IDENTIFY_SYSTEM".to_string())
        );
        repl.write_all(&identify_system_response(
            "7000000000000000042",
            "7",
            "3/FF0",
            Some("appdb"),
        ))
        .await
        .expect("write identify");
        repl.flush().await.expect("flush");

        assert_clean_terminate_to_eof(&mut repl).await;
    });

    let cfg = direct_config(port);
    let identity =
        PostgresReplicationConnection::probe_identity(&cfg, "vertebra_slot", "vertebra_pub")
            .await
            .expect("probe_identity succeeds");

    assert_eq!(
        identity,
        VerifiedReplicationIdentity {
            system_identifier: "7000000000000000042".to_string(),
            timeline: 7,
            database: "appdb".to_string(),
            slot: "vertebra_slot".to_string(),
            publication_fingerprint: identity.publication_fingerprint.clone(),
            confirmed_flush_lsn: 0x0000_0003_0000_00F8,
            restart_lsn: 0x0000_0003_0000_00A0,
        }
    );
    assert_eq!(identity.publication_fingerprint.len(), 64);
    srv.await
        .expect("server completed cleanly without START_REPLICATION");
}

#[tokio::test]
async fn replication_identity_and_copyboth_reject_invalid_slots_missing_publications_and_lsn_rewind()
 {
    #[derive(Clone, Copy)]
    enum PreflightFailure {
        MissingSlotZeroRows,
        SlotServer42704,
        PhysicalSlot,
        WrongDatabase,
        WrongPlugin,
        ActiveSlotPreflight,
        LostWalStatus,
        NullRestartLsn,
        MissingPublicationZeroRows,
        PublicationServer42704,
        RewindBehindRestartLsn,
        RewindBehindConfirmedFlushLsn,
    }

    let cases = [
        PreflightFailure::MissingSlotZeroRows,
        PreflightFailure::SlotServer42704,
        PreflightFailure::PhysicalSlot,
        PreflightFailure::WrongDatabase,
        PreflightFailure::WrongPlugin,
        PreflightFailure::ActiveSlotPreflight,
        PreflightFailure::LostWalStatus,
        PreflightFailure::NullRestartLsn,
        PreflightFailure::MissingPublicationZeroRows,
        PreflightFailure::PublicationServer42704,
        PreflightFailure::RewindBehindRestartLsn,
        PreflightFailure::RewindBehindConfirmedFlushLsn,
    ];

    for case in cases {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let repl_dials = Arc::new(AtomicUsize::new(0));
        let repl_dials_bg = Arc::clone(&repl_dials);

        let req_lsn = match case {
            PreflightFailure::RewindBehindRestartLsn => Some(0x0000_0001_0000_0150),
            PreflightFailure::RewindBehindConfirmedFlushLsn => Some(0x0000_0001_0000_0250),
            _ => Some(0x0000_0001_0000_0300),
        };

        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("accept meta");
            let _ = complete_startup_ready_version(&mut meta, 501, "16.4").await;

            let FrontendExchange::Extended(q1) = read_frontend_exchange(&mut meta).await else {
                panic!("expected slot query");
            };
            let need_pub_query = match case {
                PreflightFailure::MissingSlotZeroRows => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write 0 slot rows");
                    false
                }
                PreflightFailure::SlotServer42704 => {
                    let mut out = error_response_frame("ERROR", "42704", "slot does not exist");
                    out.extend_from_slice(&ready_for_query_frame(b'I'));
                    meta.write_all(&out).await.expect("write 42704");
                    false
                }
                PreflightFailure::PhysicalSlot => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[vec![
                            Some("appdb"),
                            None,
                            Some("physical"),
                            None,
                            Some("f"),
                            Some("1/200"),
                            None,
                            Some("reserved"),
                        ]],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write physical slot");
                    false
                }
                PreflightFailure::WrongDatabase => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[vec![
                            Some("appdb"),
                            Some("otherdb"),
                            Some("logical"),
                            Some("pgoutput"),
                            Some("f"),
                            Some("1/200"),
                            Some("1/300"),
                            Some("reserved"),
                        ]],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write wrong db");
                    false
                }
                PreflightFailure::WrongPlugin => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[vec![
                            Some("appdb"),
                            Some("appdb"),
                            Some("logical"),
                            Some("test_decoding"),
                            Some("f"),
                            Some("1/200"),
                            Some("1/300"),
                            Some("reserved"),
                        ]],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write test_decoding");
                    false
                }
                PreflightFailure::ActiveSlotPreflight => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[vec![
                            Some("appdb"),
                            Some("appdb"),
                            Some("logical"),
                            Some("pgoutput"),
                            Some("t"),
                            Some("1/200"),
                            Some("1/300"),
                            Some("reserved"),
                        ]],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write active slot");
                    false
                }
                PreflightFailure::LostWalStatus => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[vec![
                            Some("appdb"),
                            Some("appdb"),
                            Some("logical"),
                            Some("pgoutput"),
                            Some("f"),
                            Some("1/200"),
                            Some("1/300"),
                            Some("lost"),
                        ]],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write lost wal");
                    false
                }
                PreflightFailure::NullRestartLsn => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[vec![
                            Some("appdb"),
                            Some("appdb"),
                            Some("logical"),
                            Some("pgoutput"),
                            Some("f"),
                            None,
                            Some("1/300"),
                            Some("unreserved"),
                        ]],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write null restart_lsn");
                    false
                }
                PreflightFailure::MissingPublicationZeroRows
                | PreflightFailure::PublicationServer42704
                | PreflightFailure::RewindBehindRestartLsn
                | PreflightFailure::RewindBehindConfirmedFlushLsn => {
                    meta.write_all(&extended_select_response(
                        &q1.param_oids,
                        &slot_columns(),
                        &[vec![
                            Some("appdb"),
                            Some("appdb"),
                            Some("logical"),
                            Some("pgoutput"),
                            Some("f"),
                            Some("1/200"),
                            Some("1/300"),
                            Some("reserved"),
                        ]],
                        q1.saw_close_portal,
                    ))
                    .await
                    .expect("write valid slot");
                    !matches!(
                        case,
                        PreflightFailure::RewindBehindRestartLsn
                            | PreflightFailure::RewindBehindConfirmedFlushLsn
                    )
                }
            };
            meta.flush().await.expect("flush q1");

            if need_pub_query {
                let FrontendExchange::Extended(q2) = read_frontend_exchange(&mut meta).await else {
                    panic!("expected pub query");
                };
                match case {
                    PreflightFailure::MissingPublicationZeroRows => {
                        meta.write_all(&extended_select_response(
                            &q2.param_oids,
                            &pub_columns_pg15(),
                            &[],
                            q2.saw_close_portal,
                        ))
                        .await
                        .expect("write 0 pub rows");
                    }
                    PreflightFailure::PublicationServer42704 => {
                        let mut out =
                            error_response_frame("ERROR", "42704", "publication does not exist");
                        out.extend_from_slice(&ready_for_query_frame(b'I'));
                        meta.write_all(&out).await.expect("write pub 42704");
                    }
                    _ => {}
                }
                meta.flush().await.expect("flush q2");
            } else {
                let next =
                    tokio::time::timeout(Duration::from_secs(2), read_frontend_exchange(&mut meta))
                        .await
                        .expect("await Terminate or q2");
                if let FrontendExchange::Extended(q2) = next {
                    meta.write_all(&extended_select_response(
                        &q2.param_oids,
                        &pub_columns_pg15(),
                        &[vec![
                            Some("vertebra_pub"),
                            Some("f"),
                            Some("t"),
                            Some("t"),
                            Some("t"),
                            Some("f"),
                            Some("f"),
                            None,
                            None,
                            None,
                            None,
                        ]],
                        q2.saw_close_portal,
                    ))
                    .await
                    .expect("write valid pub before rewind check");
                    meta.flush().await.expect("flush q2");
                    assert_clean_terminate_to_eof(&mut meta).await;
                } else {
                    assert_eq!(next, FrontendExchange::Terminate);
                    assert_eq!(
                        read_frontend_exchange(&mut meta).await,
                        FrontendExchange::Eof
                    );
                }
                if let Ok(Ok(_)) =
                    tokio::time::timeout(Duration::from_millis(20), listener.accept()).await
                {
                    repl_dials_bg.fetch_add(1, Ordering::SeqCst);
                }
                return;
            }

            assert_clean_terminate_to_eof(&mut meta).await;
            if let Ok(Ok(_)) =
                tokio::time::timeout(Duration::from_millis(20), listener.accept()).await
            {
                repl_dials_bg.fetch_add(1, Ordering::SeqCst);
            }
        });

        let cfg = direct_config(port);
        let err = PostgresReplicationConnection::connect_logical(
            &cfg,
            "vertebra_slot",
            "vertebra_pub",
            req_lsn,
        )
        .await
        .expect_err("preflight failure must reject connect_logical");

        match case {
            PreflightFailure::SlotServer42704 | PreflightFailure::PublicationServer42704 => {
                assert_eq!(err.sqlstate(), Some("42704"));
            }
            PreflightFailure::RewindBehindRestartLsn
            | PreflightFailure::RewindBehindConfirmedFlushLsn
            | PreflightFailure::LostWalStatus
            | PreflightFailure::NullRestartLsn => {
                assert!(
                    matches!(&err, PostgresError::Operational(msg) if msg.contains("wal_unavailable") || msg.contains("rewind")),
                    "expected OperationalError naming unusable WAL / rewind, got {err:?}"
                );
            }
            _ => {
                assert!(
                    matches!(err, PostgresError::Operational(_)),
                    "expected OperationalError, got {err:?}"
                );
            }
        }

        srv.await.expect("server cleaned up");
        assert_eq!(
            repl_dials.load(Ordering::SeqCst),
            0,
            "must never open replication=database socket or send START_REPLICATION when preflight fails"
        );
    }
}

#[tokio::test]
async fn copyboth_preserves_sqlstate_55006_and_rejects_beyond_wal_malformed_w_and_stalled_deadlines()
 {
    async fn run_preflight(meta: &mut TcpStream) {
        let _ = complete_startup_ready_version(meta, 601, "16.4").await;
        let FrontendExchange::Extended(q1) = read_frontend_exchange(meta).await else {
            panic!("slot query");
        };
        meta.write_all(&extended_select_response(
            &q1.param_oids,
            &slot_columns(),
            &[vec![
                Some("appdb"),
                Some("appdb"),
                Some("logical"),
                Some("pgoutput"),
                Some("f"),
                Some("1/100"),
                Some("1/200"),
                Some("reserved"),
            ]],
            q1.saw_close_portal,
        ))
        .await
        .expect("slot");
        meta.flush().await.expect("flush");
        let FrontendExchange::Extended(q2) = read_frontend_exchange(meta).await else {
            panic!("pub query");
        };
        meta.write_all(&extended_select_response(
            &q2.param_oids,
            &pub_columns_pg15(),
            &[vec![
                Some("vertebra_pub"),
                Some("f"),
                Some("t"),
                Some("t"),
                Some("t"),
                Some("f"),
                Some("f"),
                None,
                None,
                None,
                None,
            ]],
            q2.saw_close_portal,
        ))
        .await
        .expect("pub");
        meta.flush().await.expect("flush");
        assert_clean_terminate_to_eof(meta).await;
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 602, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/500",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            let _ = read_frontend_exchange(&mut repl).await;
            let mut err_frames = error_response_frame(
                "ERROR",
                "55006",
                "replication slot \"vertebra_slot\" is active for PID 999",
            );
            err_frames.extend_from_slice(&ready_for_query_frame(b'I'));
            repl.write_all(&err_frames).await.expect("write 55006");
            repl.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut repl).await;
        });

        let err = PostgresReplicationConnection::connect_logical(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
            Some(0x0000_0001_0000_0200),
        )
        .await
        .expect_err("55006 race on START_REPLICATION");
        assert_eq!(err.sqlstate(), Some("55006"));
        srv.await.expect("55006 server ok");
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 603, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/400",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut repl).await;
        });

        let err = PostgresReplicationConnection::connect_logical(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
            Some(0x0000_0001_0000_0900),
        )
        .await
        .expect_err("start_lsn beyond server xlogpos must fail closed");
        assert!(matches!(err, PostgresError::Operational(_)), "got {err:?}");
        srv.await.expect("future LSN server closed cleanly");
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 604, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/500",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&copy_both_response_frame(1, &[0, 0]))
                .await
                .expect("write malformed W");
            repl.flush().await.expect("flush");
            let tail =
                tokio::time::timeout(Duration::from_secs(2), read_frontend_exchange(&mut repl))
                    .await
                    .expect("socket must drop on malformed W");
            assert!(matches!(
                tail,
                FrontendExchange::Eof | FrontendExchange::Terminate
            ));
        });

        let err = PostgresReplicationConnection::connect_logical(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
            None,
        )
        .await
        .expect_err("malformed W must fail");
        assert!(matches!(err, PostgresError::Protocol(_)), "got {err:?}");
        srv.await.expect("malformed W server saw socket close");
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 605, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/500",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&copy_both_response_frame(0, &[]))
                .await
                .expect("write valid W");
            repl.flush().await.expect("flush");

            assert_eq!(
                read_frontend_exchange(&mut repl).await,
                FrontendExchange::CopyDone
            );
            let eof =
                tokio::time::timeout(Duration::from_secs(2), read_frontend_exchange(&mut repl))
                    .await
                    .expect("stalled shutdown must close socket on deadline");
            assert_eq!(eof, FrontendExchange::Eof);
        });

        let mut repl = PostgresReplicationConnection::connect_logical(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
            None,
        )
        .await
        .expect("connect");
        let shutdown_err = repl
            .shutdown(Instant::now() + Duration::from_millis(60))
            .await
            .expect_err("stalled peer must time out shutdown");
        assert!(
            matches!(shutdown_err, PostgresError::Operational(_)),
            "expected timeout OperationalError on shutdown, got {shutdown_err:?}"
        );
        assert!(repl.is_closed());
        drop(repl);
        srv.await.expect("stalled shutdown socket closed");
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 606, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/500",
                Some("wrongdb"),
            ))
            .await
            .expect("identify wrong db");
            repl.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut repl).await;
        });

        let err = PostgresReplicationConnection::probe_identity(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
        )
        .await
        .expect_err("IDENTIFY_SYSTEM database mismatch must fail closed");
        assert!(matches!(err, PostgresError::Operational(_)), "got {err:?}");
        srv.await.expect("wrongdb socket closed cleanly");
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 607, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/500",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            let _ = read_frontend_exchange(&mut repl).await;
            let eof =
                tokio::time::timeout(Duration::from_secs(2), read_frontend_exchange(&mut repl))
                    .await
                    .expect("stalled START_REPLICATION must close socket on connect_timeout");
            assert_eq!(eof, FrontendExchange::Eof);
        });

        let fast_timeout_cfg =
            config_with_mode(port, PoolerMode::Direct, Duration::from_millis(80));
        let err = PostgresReplicationConnection::connect_logical(
            &fast_timeout_cfg,
            "vertebra_slot",
            "vertebra_pub",
            None,
        )
        .await
        .expect_err("stalled START_REPLICATION must time out");
        assert!(
            matches!(
                err,
                PostgresError::Transport(_) | PostgresError::Operational(_)
            ),
            "got {err:?}"
        );
        srv.await.expect("stalled START_REPLICATION socket closed");
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 608, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/500",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&copy_both_response_frame(0, &[]))
                .await
                .expect("write W");
            repl.flush().await.expect("flush");
            assert_eq!(
                read_frontend_exchange(&mut repl).await,
                FrontendExchange::CopyDone
            );
            let mut err_drain = Vec::new();
            err_drain.extend_from_slice(&error_response_frame(
                "ERROR",
                "58P01",
                "requested WAL segment was removed during shutdown",
            ));
            err_drain.extend_from_slice(&ready_for_query_frame(b'I'));
            repl.write_all(&err_drain)
                .await
                .expect("write shutdown err");
            repl.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut repl).await;
        });

        let mut repl = PostgresReplicationConnection::connect_logical(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
            None,
        )
        .await
        .expect("connect");
        let err = repl
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .expect_err("shutdown must surface backend ErrorResponse");
        assert_eq!(err.sqlstate(), Some("58P01"));
        assert!(repl.is_closed());
        srv.await
            .expect("shutdown error drained and terminated cleanly");
    }

    {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            run_preflight(&mut meta).await;
            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 609, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "12345",
                "1",
                "1/500",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&copy_both_response_frame(0, &[]))
                .await
                .expect("write W");
            repl.flush().await.expect("flush");
            assert_eq!(
                read_frontend_exchange(&mut repl).await,
                FrontendExchange::CopyDone
            );
            repl.write_all(&ready_for_query_frame(b'I'))
                .await
                .expect("premature ReadyForQuery without CopyDone");
            repl.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut repl).await;
        });

        let mut repl = PostgresReplicationConnection::connect_logical(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
            None,
        )
        .await
        .expect("connect");
        let err = repl
            .shutdown(Instant::now() + Duration::from_secs(2))
            .await
            .expect_err("ReadyForQuery without CopyDone/CommandComplete must fail");
        assert!(matches!(err, PostgresError::Protocol(_)), "got {err:?}");
        assert!(repl.is_closed());
        srv.await.expect("premature ReadyForQuery closed cleanly");
    }
}

#[tokio::test]
async fn replication_identity_publication_fingerprint_tracks_flags_tables_attnames_and_rowfilters()
{
    async fn fingerprint_for(rows: Vec<Vec<Option<&'static str>>>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let port = listener.local_addr().expect("addr").port();
        let srv = tokio::spawn(async move {
            let (mut meta, _) = listener.accept().await.expect("meta");
            let _ = complete_startup_ready_version(&mut meta, 701, "16.4").await;
            let FrontendExchange::Extended(q1) = read_frontend_exchange(&mut meta).await else {
                panic!("slot query");
            };
            meta.write_all(&extended_select_response(
                &q1.param_oids,
                &slot_columns(),
                &[vec![
                    Some("appdb"),
                    Some("appdb"),
                    Some("logical"),
                    Some("pgoutput"),
                    Some("f"),
                    Some("1/10"),
                    Some("1/20"),
                    Some("reserved"),
                ]],
                q1.saw_close_portal,
            ))
            .await
            .expect("slot");
            meta.flush().await.expect("flush");

            let FrontendExchange::Extended(q2) = read_frontend_exchange(&mut meta).await else {
                panic!("pub query");
            };
            meta.write_all(&extended_select_response(
                &q2.param_oids,
                &pub_columns_pg15(),
                &rows,
                q2.saw_close_portal,
            ))
            .await
            .expect("pub");
            meta.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut meta).await;

            let (mut repl, _) = listener.accept().await.expect("repl");
            let _ = complete_startup_ready_version(&mut repl, 702, "16.4").await;
            let _ = read_frontend_exchange(&mut repl).await;
            repl.write_all(&identify_system_response(
                "55555",
                "1",
                "1/100",
                Some("appdb"),
            ))
            .await
            .expect("identify");
            repl.flush().await.expect("flush");
            assert_clean_terminate_to_eof(&mut repl).await;
        });

        let id = PostgresReplicationConnection::probe_identity(
            &direct_config(port),
            "vertebra_slot",
            "vertebra_pub",
        )
        .await
        .expect("probe_identity");
        srv.await.expect("server ok");
        id.publication_fingerprint
    }

    let baseline = fingerprint_for(vec![vec![
        Some("vertebra_pub"),
        Some("f"),
        Some("t"),
        Some("t"),
        Some("t"),
        Some("f"),
        Some("f"),
        Some("public"),
        Some("orders"),
        Some("{id,total}"),
        Some("(total > 0)"),
    ]])
    .await;

    let baseline_repeat = fingerprint_for(vec![vec![
        Some("vertebra_pub"),
        Some("f"),
        Some("t"),
        Some("t"),
        Some("t"),
        Some("f"),
        Some("f"),
        Some("public"),
        Some("orders"),
        Some("{id,total}"),
        Some("(total > 0)"),
    ]])
    .await;
    assert_eq!(baseline, baseline_repeat);

    let changed_viaroot = fingerprint_for(vec![vec![
        Some("vertebra_pub"),
        Some("f"),
        Some("t"),
        Some("t"),
        Some("t"),
        Some("f"),
        Some("t"),
        Some("public"),
        Some("orders"),
        Some("{id,total}"),
        Some("(total > 0)"),
    ]])
    .await;

    let changed_attnames = fingerprint_for(vec![vec![
        Some("vertebra_pub"),
        Some("f"),
        Some("t"),
        Some("t"),
        Some("t"),
        Some("f"),
        Some("f"),
        Some("public"),
        Some("orders"),
        Some("{id,total,currency}"),
        Some("(total > 0)"),
    ]])
    .await;

    let changed_rowfilter = fingerprint_for(vec![vec![
        Some("vertebra_pub"),
        Some("f"),
        Some("t"),
        Some("t"),
        Some("t"),
        Some("f"),
        Some("f"),
        Some("public"),
        Some("orders"),
        Some("{id,total}"),
        Some("(total >= 10)"),
    ]])
    .await;

    let boundary_split = fingerprint_for(vec![vec![
        Some("vertebra_pub"),
        Some("f"),
        Some("t"),
        Some("t"),
        Some("t"),
        Some("f"),
        Some("f"),
        Some("publi"),
        Some("corders"),
        Some("{id,total}"),
        Some("(total > 0)"),
    ]])
    .await;

    for other in [
        &changed_viaroot,
        &changed_attnames,
        &changed_rowfilter,
        &boundary_split,
    ] {
        assert_ne!(&baseline, other);
    }
}

#[test]
fn feedback_wire_xlogdata_preserves_fields_and_borrowed_payload() {
    use _postgres::replication::feedback::{ReplicationCopyData, decode_replication_copy_data};
    let empty = [
        b'w', 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0x90, 0, 255, 255, 255, 255, 255, 255, 255,
        249,
    ];
    let ReplicationCopyData::XLogData {
        wal_start,
        wal_end,
        server_time_pg_us,
        payload,
    } = decode_replication_copy_data(&empty).unwrap()
    else {
        panic!("expected XLogData")
    };
    assert_eq!(
        (wal_start, wal_end, server_time_pg_us),
        (0x1_0000_0100, 0x2_0000_9000, -7)
    );
    assert!(payload.is_empty());
    let mut nonempty = empty.to_vec();
    nonempty.extend_from_slice(&[0, 255, 66]);
    let ReplicationCopyData::XLogData {
        wal_start,
        wal_end,
        server_time_pg_us,
        payload,
    } = decode_replication_copy_data(&nonempty).unwrap()
    else {
        panic!("expected XLogData")
    };
    assert_eq!(
        (wal_start, wal_end, server_time_pg_us),
        (0x1_0000_0100, 0x2_0000_9000, -7)
    );
    assert_eq!(payload, [0, 255, 66]);
    assert_eq!(payload.as_ptr(), nonempty[25..].as_ptr());
}

#[test]
fn feedback_wire_keepalive_and_status_match_golden_bytes() {
    use _postgres::replication::feedback::{
        ReplicationCopyData, decode_replication_copy_data, encode_standby_status_update,
    };
    let frame = [
        b'k', 0, 0, 0, 2, 0, 0, 0x10, 0, 255, 255, 255, 255, 255, 255, 255, 255, 1,
    ];
    let ReplicationCopyData::PrimaryKeepalive {
        wal_end,
        server_time_pg_us,
        reply_requested,
    } = decode_replication_copy_data(&frame).unwrap()
    else {
        panic!("expected keepalive")
    };
    assert_eq!(
        (wal_end, server_time_pg_us, reply_requested),
        (0x2_0000_1000, -1, true)
    );
    let mut no_reply = frame;
    no_reply[17] = 0;
    assert!(matches!(
        decode_replication_copy_data(&no_reply).unwrap(),
        ReplicationCopyData::PrimaryKeepalive {
            reply_requested: false,
            ..
        }
    ));
    let expected = [
        b'r', 0, 0, 0, 2, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 128, 0, 0, 0, 2, 0, 0, 0, 128, 255, 255,
        255, 255, 255, 255, 255, 255, 0,
    ];
    assert_eq!(
        encode_standby_status_update(0x2_0000_0100, 0x2_0000_0080, 0x2_0000_0080, -1, false),
        expected
    );
    let mut reply = expected;
    reply[33] = 1;
    assert_eq!(
        encode_standby_status_update(0x2_0000_0100, 0x2_0000_0080, 0x2_0000_0080, -1, true),
        reply
    );
}

#[test]
fn feedback_wire_rejects_invalid_payloads() {
    use _postgres::replication::feedback::decode_replication_copy_data;
    for frame in [
        vec![],
        vec![b'x'],
        vec![b'w'; 24],
        vec![b'k'; 17],
        vec![b'k'; 19],
        {
            let mut frame = vec![0; 18];
            frame[0] = b'k';
            frame[17] = 2;
            frame
        },
    ] {
        assert!(matches!(
            decode_replication_copy_data(&frame),
            Err(PostgresError::Protocol(_))
        ));
    }
}

#[test]
fn feedback_wire_status_intervals_respect_timeout_and_clamps() {
    use _postgres::replication::feedback::{
        DEFAULT_STATUS_INTERVAL, MAX_UNACKED_COMMITS, compute_status_interval,
    };
    assert_eq!(DEFAULT_STATUS_INTERVAL, Duration::from_secs(10));
    assert_eq!(MAX_UNACKED_COMMITS, 1024);
    assert_eq!(compute_status_interval(None), Duration::from_secs(10));
    assert_eq!(
        compute_status_interval(Some(Duration::ZERO)),
        Duration::from_secs(10)
    );
    assert_eq!(
        compute_status_interval(Some(Duration::from_millis(150))),
        Duration::from_millis(100)
    );
    assert_eq!(
        compute_status_interval(Some(Duration::from_secs(9))),
        Duration::from_secs(3)
    );
    assert_eq!(
        compute_status_interval(Some(Duration::from_secs(30))),
        Duration::from_secs(10)
    );
    assert_eq!(
        compute_status_interval(Some(Duration::from_secs(90))),
        Duration::from_secs(10)
    );
}

#[test]
fn feedback_state_initial_and_unpublished_commit() {
    use _postgres::replication::feedback::ReplicationFeedback;
    let mut state = ReplicationFeedback::new(0x2_0000_0100);
    assert_eq!(
        state.status_positions(),
        (0x2_0000_0100, 0x2_0000_0100, 0x2_0000_0100)
    );
    assert_eq!(state.unacked_count(), 0);
    assert_eq!(state.acknowledge(0x2_0000_0100).unwrap(), 0x2_0000_0100);
    state.record_received_commit(0x2_0000_0200).unwrap();
    assert!(matches!(
        state.acknowledge(0x2_0000_0200),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(
        state.status_positions(),
        (0x2_0000_0200, 0x2_0000_0100, 0x2_0000_0100)
    );
}

#[test]
fn feedback_state_received_progress_is_monotonic() {
    use _postgres::replication::feedback::ReplicationFeedback;
    let mut state = ReplicationFeedback::new(100);
    state.record_received_commit(200).unwrap();
    state.record_received_commit(200).unwrap();
    assert!(matches!(
        state.record_received_commit(150),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(state.status_positions(), (200, 100, 100));
    assert_eq!(state.unacked_count(), 0);
}

#[test]
fn feedback_state_publication_refusals_preserve_boundaries() {
    use _postgres::replication::feedback::ReplicationFeedback;
    let mut state = ReplicationFeedback::new(100);
    state.record_received_commit(400).unwrap();
    state.record_published_commit(200).unwrap();
    for invalid in [200, 150, 500, 100, 99] {
        assert!(matches!(
            state.record_published_commit(invalid),
            Err(PostgresError::Interface(_))
        ));
        assert_eq!(state.status_positions(), (400, 100, 100));
        assert_eq!(state.unacked_count(), 1);
    }
    assert_eq!(state.acknowledge(200).unwrap(), 200);
    assert_eq!(state.status_positions(), (400, 200, 200));
    assert_eq!(state.unacked_count(), 0);
}

#[test]
fn feedback_state_acknowledgement_is_cumulative_and_exact() {
    use _postgres::replication::feedback::ReplicationFeedback;
    let mut state = ReplicationFeedback::new(100);
    state.record_received_commit(350).unwrap();
    state.record_published_commit(200).unwrap();
    state.record_published_commit(350).unwrap();
    for invalid in [250, 999] {
        assert!(matches!(
            state.acknowledge(invalid),
            Err(PostgresError::Interface(_))
        ));
        assert_eq!(state.status_positions(), (350, 100, 100));
        assert_eq!(state.unacked_count(), 2);
    }
    assert_eq!(state.acknowledge(350).unwrap(), 350);
    assert_eq!(state.acknowledge(350).unwrap(), 350);
    assert!(matches!(
        state.acknowledge(200),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(state.status_positions(), (350, 350, 350));
    assert_eq!(state.unacked_count(), 0);
}

#[test]
fn feedback_state_capacity_is_reclaimed_by_acknowledgement() {
    use _postgres::replication::feedback::{MAX_UNACKED_COMMITS, ReplicationFeedback};
    let mut state = ReplicationFeedback::new(0);
    state.record_received_commit(2048).unwrap();
    for lsn in 1..=1024 {
        state.record_published_commit(lsn).unwrap();
    }
    assert_eq!(MAX_UNACKED_COMMITS, 1024);
    assert!(matches!(
        state.record_published_commit(1025),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(state.status_positions(), (2048, 0, 0));
    assert_eq!(state.unacked_count(), 1024);
    assert_eq!(state.acknowledge(1024).unwrap(), 1024);
    assert_eq!(state.unacked_count(), 0);
    state.record_published_commit(1025).unwrap();
    assert_eq!(state.acknowledge(1025).unwrap(), 1025);
}
