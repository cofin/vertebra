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
    MAX_RAW_RECONNECT_ATTEMPTS, MAX_REPLICATION_IDENTIFIER_BYTES, PostgresReplicationConnection,
    PostgresReplicationResume, ReplicationStreamEvent, ReplicationSupervisor,
    VerifiedReplicationIdentity, format_pg_lsn, parse_pg_lsn, raw_reconnect_delay,
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
fn feedback_wire_rejects_empty_payload() {
    use _postgres::replication::feedback::decode_replication_copy_data;
    assert!(matches!(
        decode_replication_copy_data(&[]),
        Err(PostgresError::Protocol(_))
    ));
}

#[test]
fn feedback_wire_rejects_unknown_tag() {
    use _postgres::replication::feedback::decode_replication_copy_data;
    assert!(matches!(
        decode_replication_copy_data(b"x"),
        Err(PostgresError::Protocol(_))
    ));
}

#[test]
fn feedback_wire_rejects_short_xlog_data() {
    use _postgres::replication::feedback::decode_replication_copy_data;
    assert!(matches!(
        decode_replication_copy_data(&[b'w'; 24]),
        Err(PostgresError::Protocol(_))
    ));
}

#[test]
fn feedback_wire_rejects_short_keepalive() {
    use _postgres::replication::feedback::decode_replication_copy_data;
    assert!(matches!(
        decode_replication_copy_data(&[b'k'; 17]),
        Err(PostgresError::Protocol(_))
    ));
}

#[test]
fn feedback_wire_rejects_long_keepalive() {
    use _postgres::replication::feedback::decode_replication_copy_data;
    assert!(matches!(
        decode_replication_copy_data(&[b'k'; 19]),
        Err(PostgresError::Protocol(_))
    ));
}

#[test]
fn feedback_wire_rejects_invalid_reply_flag() {
    use _postgres::replication::feedback::decode_replication_copy_data;
    assert!(matches!(
        decode_replication_copy_data(&[b'k', 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2]),
        Err(PostgresError::Protocol(_))
    ));
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

async fn session_writer_connection(
    start: Option<u64>,
) -> (PostgresReplicationConnection, TcpStream) {
    session_connection_with_payload(start, Vec::new()).await
}

async fn session_connection_with_payload(
    start: Option<u64>,
    payload: Vec<u8>,
) -> (PostgresReplicationConnection, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = direct_config(listener.local_addr().unwrap().port());
    let peer = tokio::spawn(async move {
        let (mut meta, _) = listener.accept().await.unwrap();
        complete_startup_ready_version(&mut meta, 301, "14.12").await;
        let FrontendExchange::Extended(slot) = read_frontend_exchange(&mut meta).await else {
            panic!("slot query")
        };
        meta.write_all(&extended_select_response(
            &slot.param_oids,
            &slot_columns(),
            &[vec![
                Some("appdb"),
                Some("appdb"),
                Some("logical"),
                Some("pgoutput"),
                Some("f"),
                Some("2/10"),
                Some("2/80"),
                Some("extended"),
            ]],
            slot.saw_close_portal,
        ))
        .await
        .unwrap();
        let FrontendExchange::Extended(publication) = read_frontend_exchange(&mut meta).await
        else {
            panic!("publication query")
        };
        meta.write_all(&extended_select_response(
            &publication.param_oids,
            &pub_columns_pg14(),
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
            ]],
            publication.saw_close_portal,
        ))
        .await
        .unwrap();
        assert_clean_terminate_to_eof(&mut meta).await;
        let (mut socket, _) = listener.accept().await.unwrap();
        complete_startup_ready_version(&mut socket, 302, "14.12").await;
        assert_eq!(
            read_frontend_exchange(&mut socket).await,
            FrontendExchange::SimpleQuery("IDENTIFY_SYSTEM".into())
        );
        socket
            .write_all(&identify_system_response(
                "9990001112223334445",
                "1",
                "2/900",
                Some("appdb"),
            ))
            .await
            .unwrap();
        let FrontendExchange::SimpleQuery(sql) = read_frontend_exchange(&mut socket).await else {
            panic!("START")
        };
        assert!(sql.contains(&format!(
            "LOGICAL {} ",
            start.map_or("0/0".into(), format_pg_lsn)
        )));
        let mut startup = copy_both_response_frame(0, &[]);
        startup.extend(copy_data_frame(b"k\0\0\0\x02\0\0\x09\0\0\0\0\0\0\0\0\0\0"));
        startup.extend(payload);
        socket.write_all(&startup).await.unwrap();
        socket
    });
    let connection = PostgresReplicationConnection::connect_logical(
        &cfg,
        "vertebra_slot",
        "vertebra_pub",
        start,
    )
    .await
    .unwrap();
    (connection, peer.await.unwrap())
}

#[tokio::test(start_paused = true)]
async fn session_writer_preserves_fence_ticks_and_closes_once() {
    let initial = 0x200000100;
    let (connection, mut peer) = session_writer_connection(Some(initial)).await;
    let identity = connection.identity().clone();
    assert!(connection.buffered_read_len() > 0);
    let session = connection
        .into_stream(1, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    assert_eq!(session.identity(), &identity);
    assert_eq!(session.status_positions(), (initial, initial, initial));
    assert!(matches!(
        session.acknowledge(initial + 1),
        Err(PostgresError::Interface(_))
    ));
    tokio::task::yield_now().await;
    let mut byte = [0; 1];
    assert_eq!(
        peer.try_read(&mut byte).unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(10)).await;
        let FrontendExchange::Other(b'd', payload) = read_frontend_exchange(&mut peer).await else {
            panic!("status")
        };
        assert_eq!(payload.len(), 34);
        assert_eq!(payload[0], b'r');
        for offset in [1, 9, 17] {
            assert_eq!(
                u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap()),
                initial
            );
        }
    }
    let closing_peer = tokio::spawn(async move {
        assert!(matches!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Other(b'd', _)
        ));
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::CopyDone
        );
        peer.write_all(&session_shutdown_response()).await.unwrap();
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Eof
        );
    });
    session.shutdown().await.unwrap();
    session.shutdown().await.unwrap();
    assert!(session.is_closed());
    session.wait_closed().await.unwrap();
    closing_peer.await.unwrap();
}

#[tokio::test]
async fn session_writer_defaults_to_confirmed_fence_and_validates_options() {
    let (connection, _peer) = session_writer_connection(None).await;
    let session = connection
        .into_stream(1, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    assert_eq!(
        session.status_positions(),
        (0x200000080, 0x200000080, 0x200000080)
    );
    session.cancel();
    assert!(session.is_closed());
    for (capacity, interval, timeout) in [(0, 10, 2), (1025, 10, 2), (1, 0, 2), (1, 10, 0)] {
        let (connection, mut peer) = session_writer_connection(None).await;
        assert!(matches!(
            connection.into_stream(
                capacity,
                Duration::from_secs(interval),
                Duration::from_secs(timeout)
            ),
            Err(PostgresError::Interface(_))
        ));
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Eof
        );
    }
}

#[test]
fn session_writer_drop_without_runtime_closes_socket() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (session, peer) = runtime.block_on(async {
        let (connection, peer) = session_writer_connection(None).await;
        let session = connection
            .into_stream(1, Duration::from_secs(10), Duration::from_secs(2))
            .unwrap();
        (session, peer.into_std().unwrap())
    });
    drop(runtime);
    drop(session);
    peer.set_nonblocking(false).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(std::io::Read::read(&mut &peer, &mut [0; 1]).unwrap(), 0);
}

fn session_transaction_payload() -> Vec<u8> {
    let mut payload = vec![b'B'];
    payload.extend_from_slice(&0x200000200_u64.to_be_bytes());
    payload.extend_from_slice(&0_i64.to_be_bytes());
    payload.extend_from_slice(&7_u32.to_be_bytes());
    payload.push(b'R');
    payload.extend_from_slice(&42_u32.to_be_bytes());
    payload.extend_from_slice(b"public\0items\0d");
    payload.extend_from_slice(&1_u16.to_be_bytes());
    payload.extend_from_slice(b"\x01id\0");
    payload.extend_from_slice(&23_u32.to_be_bytes());
    payload.extend_from_slice(&(-1_i32).to_be_bytes());
    payload.push(b'I');
    payload.extend_from_slice(&42_u32.to_be_bytes());
    payload.push(b'N');
    payload.extend_from_slice(&1_u16.to_be_bytes());
    payload.push(b't');
    payload.extend_from_slice(&1_u32.to_be_bytes());
    payload.push(b'7');
    payload.extend_from_slice(b"C\0");
    payload.extend_from_slice(&0x200000200_u64.to_be_bytes());
    payload.extend_from_slice(&0x200000300_u64.to_be_bytes());
    payload.extend_from_slice(&0_i64.to_be_bytes());
    payload
}

fn session_wal_frame(payload: &[u8]) -> Vec<u8> {
    let mut body = vec![b'w'];
    body.extend_from_slice(&0x200000100_u64.to_be_bytes());
    body.extend_from_slice(&0x2ffffffff_u64.to_be_bytes());
    body.extend_from_slice(&0_i64.to_be_bytes());
    body.extend_from_slice(payload);
    copy_data_frame(&body)
}

#[tokio::test]
async fn session_reader_coalesced_startup_publishes_in_order() {
    use _postgres::replication::pgoutput::ReplicationRecord;
    let mut payload = copy_data_frame(b"k\0\0\0\x02\0\0\x09\0\0\0\0\0\0\0\0\0\x01");
    payload.extend(session_wal_frame(&session_transaction_payload()));
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    let FrontendExchange::Other(b'd', status) = read_frontend_exchange(&mut peer).await else {
        panic!("keepalive status")
    };
    assert_eq!(status[0], b'r');
    assert_eq!(
        u64::from_be_bytes(status[9..17].try_into().unwrap()),
        0x200000100
    );
    let begin = session.next().await.unwrap().unwrap().publish().unwrap();
    assert!(matches!(begin, ReplicationRecord::Begin { xid: 7, .. }));
    let insert = session.next().await.unwrap().unwrap().publish().unwrap();
    assert!(matches!(insert, ReplicationRecord::Insert { xid: 7, .. }));
    let commit = session.next().await.unwrap().unwrap();
    assert_eq!(commit.record().commit_end_lsn(), Some(0x200000300));
    assert!(matches!(
        session.acknowledge(0x200000300),
        Err(PostgresError::Interface(_))
    ));
    commit.publish().unwrap();
    assert_eq!(session.acknowledge(0x200000300).unwrap(), 0x200000300);
    session.cancel();
}

#[tokio::test]
async fn session_reader_malformed_keepalive_is_terminal() {
    let (connection, _peer) =
        session_connection_with_payload(Some(0x200000100), copy_data_frame(&[b'k'; 12])).await;
    let session = connection
        .into_stream(1, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Protocol(_))
    ));
    assert!(matches!(
        session.wait_closed().await,
        Err(PostgresError::Protocol(_))
    ));
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(
        session.status_positions(),
        (0x200000100, 0x200000100, 0x200000100)
    );
}

#[tokio::test]
async fn session_reader_unknown_copydata_is_terminal() {
    let (connection, _peer) =
        session_connection_with_payload(Some(0x200000100), copy_data_frame(b"x")).await;
    let session = connection
        .into_stream(1, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Protocol(_))
    ));
    assert!(session.is_closed());
}

#[tokio::test]
async fn session_reader_frame_boundary_eof_during_transaction_is_transport() {
    let begin = session_transaction_payload()[..21].to_vec();
    let (connection, mut peer) =
        session_connection_with_payload(Some(0x200000100), session_wal_frame(&begin)).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    peer.shutdown().await.unwrap();
    assert!(matches!(
        session.wait_closed().await,
        Err(PostgresError::Transport(_))
    ));
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Transport(_))
    ));
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(
        session.status_positions(),
        (0x200000100, 0x200000100, 0x200000100)
    );
}

#[tokio::test]
async fn session_reader_truncated_backend_frame_eof_is_protocol() {
    let (connection, mut peer) =
        session_connection_with_payload(Some(0x200000100), b"d\0\0\0\x20w".to_vec()).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    peer.shutdown().await.unwrap();
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Protocol(_))
    ));
}

#[tokio::test]
async fn session_reader_copydone_rejects_unfinished_transaction() {
    let mut payload = session_wal_frame(&session_transaction_payload()[..21]);
    payload.extend(copy_done_frame());
    let (connection, _peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    assert!(matches!(
        session.wait_closed().await,
        Err(PostgresError::Protocol(_))
    ));
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Protocol(_))
    ));
}

#[tokio::test]
async fn session_reader_copydone_drains_queued_records_before_none() {
    let mut payload = session_wal_frame(&session_transaction_payload());
    payload.extend(copy_done_frame());
    let (connection, _peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    for _ in 0..3 {
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(session.acknowledge(0x200000300).unwrap(), 0x200000300);
    session.cancel();
}

#[tokio::test(start_paused = true)]
async fn session_reader_full_fifo_does_not_starve_periodic_writer() {
    let payload = session_wal_frame(&session_transaction_payload());
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(1, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    let begin = session.next().await.unwrap().unwrap();
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(10)).await;
        let FrontendExchange::Other(b'd', status) = read_frontend_exchange(&mut peer).await else {
            panic!("periodic status")
        };
        assert_eq!(
            u64::from_be_bytes(status[9..17].try_into().unwrap()),
            0x200000100
        );
        assert_eq!(
            u64::from_be_bytes(status[17..25].try_into().unwrap()),
            0x200000100
        );
    }
    begin.publish().unwrap();
    session.next().await.unwrap().unwrap().publish().unwrap();
    session.next().await.unwrap().unwrap().publish().unwrap();
    assert_eq!(session.acknowledge(0x200000300).unwrap(), 0x200000300);
    session.cancel();
}

fn session_message_payload(size: usize, byte: u8) -> Vec<u8> {
    let mut payload = b"M\0".to_vec();
    payload.extend_from_slice(&0x200000200_u64.to_be_bytes());
    payload.extend_from_slice(b"bulk\0");
    payload.extend_from_slice(&(size as u32).to_be_bytes());
    payload.resize(payload.len() + size, byte);
    payload
}

#[tokio::test]
async fn session_reader_individually_valid_nine_mib_record_exceeds_aggregate_budget() {
    use _postgres::replication::pgoutput::ReplicationRecord;
    let (connection, mut peer) = session_writer_connection(Some(0x200000100)).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    let sender = tokio::spawn(async move {
        peer.write_all(&session_wal_frame(&session_message_payload(
            9 * 1024 * 1024,
            b'a',
        )))
        .await
        .unwrap();
        peer.write_all(&session_wal_frame(&session_message_payload(
            9 * 1024 * 1024,
            b'b',
        )))
        .await
        .unwrap();
        peer
    });
    for byte in *b"ab" {
        let record = session.next().await.unwrap().unwrap().publish().unwrap();
        let ReplicationRecord::Message { content, .. } = record else {
            panic!("logical message")
        };
        assert_eq!(content.len(), 9 * 1024 * 1024);
        assert!(content.iter().all(|value| *value == byte));
    }
    let _peer = sender.await.unwrap();
    assert_eq!(
        session.status_positions(),
        (0x200000100, 0x200000100, 0x200000100)
    );
    session.cancel();
}

#[tokio::test]
async fn session_delivery_dropped_commit_restores_fifo_and_blocks_overlap() {
    use _postgres::replication::pgoutput::ReplicationRecord;
    let payload = session_wal_frame(&session_transaction_payload());
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    for _ in 0..2 {
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    let commit = session.next().await.unwrap().unwrap();
    assert_eq!(commit.record().commit_end_lsn(), Some(0x200000300));
    let mut overlap = Box::pin(session.next());
    std::future::poll_fn(|cx| {
        assert!(matches!(
            std::future::Future::poll(overlap.as_mut(), cx),
            Poll::Ready(Err(PostgresError::Interface(_)))
        ));
        Poll::Ready(())
    })
    .await;
    drop(overlap);
    let mut begin = session_transaction_payload()[..21].to_vec();
    begin[1..9].copy_from_slice(&0x200000400_u64.to_be_bytes());
    begin[17..21].copy_from_slice(&9_u32.to_be_bytes());
    peer.write_all(&session_wal_frame(&begin)).await.unwrap();
    drop(commit);
    assert!(matches!(
        session.acknowledge(0x200000300),
        Err(PostgresError::Interface(_))
    ));
    let restored = session.next().await.unwrap().unwrap();
    assert_eq!(restored.record().commit_end_lsn(), Some(0x200000300));
    restored.publish().unwrap();
    assert_eq!(session.acknowledge(0x200000300).unwrap(), 0x200000300);
    assert!(matches!(
        session.next().await.unwrap().unwrap().publish().unwrap(),
        ReplicationRecord::Begin { xid: 9, .. }
    ));
    session.cancel();
}

#[tokio::test]
async fn session_delivery_cancelled_waiter_releases_reservation() {
    use _postgres::replication::pgoutput::ReplicationRecord;
    use std::future::Future;
    let (connection, mut peer) = session_writer_connection(Some(0x200000100)).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    let mut pending = Box::pin(session.next());
    std::future::poll_fn(|cx| {
        assert!(pending.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let mut overlap = Box::pin(session.next());
    std::future::poll_fn(|cx| {
        assert!(matches!(
            std::future::Future::poll(overlap.as_mut(), cx),
            Poll::Ready(Err(PostgresError::Interface(_)))
        ));
        Poll::Ready(())
    })
    .await;
    drop(overlap);
    drop(pending);
    peer.write_all(&session_wal_frame(&session_transaction_payload()))
        .await
        .unwrap();
    assert!(matches!(
        session.next().await.unwrap().unwrap().publish().unwrap(),
        ReplicationRecord::Begin { xid: 7, .. }
    ));
    session.cancel();
}

#[tokio::test]
async fn session_delivery_closed_lease_preserves_borrow_but_refuses_publication() {
    let payload = session_wal_frame(&session_transaction_payload());
    let (connection, _peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    for _ in 0..2 {
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    let commit = session.next().await.unwrap().unwrap();
    let borrowed = commit.record();
    session.cancel();
    assert_eq!(borrowed.commit_end_lsn(), Some(0x200000300));
    assert!(matches!(commit.publish(), Err(PostgresError::Interface(_))));
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(session.status_positions().1, 0x200000100);
}

fn session_empty_transaction_payload(end_lsn: u64, xid: u32) -> Vec<u8> {
    let mut payload = vec![b'B'];
    payload.extend_from_slice(&(end_lsn - 1).to_be_bytes());
    payload.extend_from_slice(&0_i64.to_be_bytes());
    payload.extend_from_slice(&xid.to_be_bytes());
    payload.extend_from_slice(b"C\0");
    payload.extend_from_slice(&(end_lsn - 1).to_be_bytes());
    payload.extend_from_slice(&end_lsn.to_be_bytes());
    payload.extend_from_slice(&0_i64.to_be_bytes());
    payload
}

#[tokio::test]
async fn session_delivery_unacknowledged_cap_resumes_after_ack_and_updates_writer() {
    use _postgres::replication::pgoutput::ReplicationRecord;
    use std::future::Future;
    let initial = 0x200000100;
    let (connection, mut peer) = session_writer_connection(Some(initial)).await;
    let session = connection
        .into_stream(8, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    let mut payload = Vec::new();
    for xid in 1..=1023 {
        payload.extend(session_empty_transaction_payload(
            initial + u64::from(xid),
            xid,
        ));
    }
    peer.write_all(&session_wal_frame(&payload)).await.unwrap();
    for _ in 0..1023 {
        assert!(matches!(
            session.next().await.unwrap().unwrap().publish().unwrap(),
            ReplicationRecord::Begin { .. }
        ));
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    let last_published = initial + 1024;
    let next_lsn = initial + 1025;
    let mut payload = session_empty_transaction_payload(last_published, 1024);
    payload.extend(session_empty_transaction_payload(next_lsn, 1025));
    let mut frames = session_wal_frame(&payload);
    frames.extend(copy_data_frame(
        b"k\0\0\0\x02\0\0\x09\0\0\0\0\0\0\0\0\0\x01",
    ));
    peer.write_all(&frames).await.unwrap();
    session.next().await.unwrap().unwrap().publish().unwrap();
    let final_lease = session.next().await.unwrap().unwrap();
    let FrontendExchange::Other(b'd', status) = read_frontend_exchange(&mut peer).await else {
        panic!("keepalive barrier")
    };
    assert_eq!(
        u64::from_be_bytes(status[1..9].try_into().unwrap()),
        next_lsn
    );
    assert_eq!(
        u64::from_be_bytes(status[9..17].try_into().unwrap()),
        initial
    );
    final_lease.publish().unwrap();
    session.next().await.unwrap().unwrap().publish().unwrap();
    let mut pending_commit = Box::pin(session.next());
    std::future::poll_fn(|cx| {
        assert!(pending_commit.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        session.acknowledge(next_lsn),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(session.acknowledge(last_published).unwrap(), last_published);
    let commit = pending_commit.await.unwrap().unwrap();
    assert_eq!(commit.record().commit_end_lsn(), Some(next_lsn));
    let FrontendExchange::Other(b'd', status) = read_frontend_exchange(&mut peer).await else {
        panic!("acknowledged status")
    };
    for offset in [9, 17] {
        assert_eq!(
            u64::from_be_bytes(status[offset..offset + 8].try_into().unwrap()),
            last_published
        );
    }
    commit.publish().unwrap();
    assert_eq!(session.acknowledge(next_lsn).unwrap(), next_lsn);
    session.cancel();
}

#[tokio::test]
async fn session_shutdown_waits_for_backend_drain_without_acknowledging_publication() {
    use std::future::Future;
    let payload = session_wal_frame(&session_transaction_payload());
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    for _ in 0..3 {
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    let mut closing = Box::pin(session.shutdown());
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    let FrontendExchange::Other(b'd', status) = read_frontend_exchange(&mut peer).await else {
        panic!("final feedback")
    };
    for offset in [9, 17] {
        assert_eq!(
            u64::from_be_bytes(status[offset..offset + 8].try_into().unwrap()),
            0x200000100
        );
    }
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::CopyDone
    );
    std::future::poll_fn(|cx| {
        assert!(
            closing.as_mut().poll(cx).is_pending(),
            "shutdown must await the backend drain"
        );
        Poll::Ready(())
    })
    .await;
    let mut response = copy_done_frame();
    response.extend(command_complete_frame("COPY 0"));
    response.extend(ready_for_query_frame(b'I'));
    peer.write_all(&response).await.unwrap();
    closing.await.unwrap();
    session.shutdown().await.unwrap();
    assert!(session.is_closed());
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Eof
    );
}

fn session_shutdown_response() -> Vec<u8> {
    let mut response = copy_done_frame();
    response.extend(command_complete_frame("COPY 0"));
    response.extend(ready_for_query_frame(b'I'));
    response
}

#[tokio::test]
async fn session_shutdown_accepts_post_copydone_keepalive_and_multiple_command_complete() {
    let (connection, mut peer) = session_writer_connection(None).await;
    let session = connection
        .into_stream(1, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    let peer_close = async {
        assert!(matches!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Other(b'd', _)
        ));
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::CopyDone
        );
        let mut response = copy_done_frame();
        let keepalive = [b'k', 0, 0, 0, 2, 0, 0, 0, 16, 0, 0, 0, 0, 0, 0, 0, 0, 0];
        response.extend(copy_data_frame(&keepalive));
        response.extend(command_complete_frame("COPY 0"));
        response.extend(command_complete_frame("START_REPLICATION"));
        response.extend(ready_for_query_frame(b'I'));
        peer.write_all(&response).await.unwrap();
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Eof
        );
    };
    let (res, ()) = tokio::join!(session.shutdown(), peer_close);
    res.unwrap();
    assert!(session.is_closed());
}

#[tokio::test(start_paused = true)]
async fn session_shutdown_unresponsive_peer_expires_once_and_closes_socket() {
    use std::future::Future;
    let (connection, mut peer) = session_writer_connection(Some(0x200000100)).await;
    let session = connection
        .into_stream(1, Duration::from_secs(3600), Duration::from_millis(100))
        .unwrap();
    let mut closing = Box::pin(session.shutdown());
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Other(b'd', _)
    ));
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::CopyDone
    );
    tokio::time::advance(Duration::from_millis(100)).await;
    assert!(matches!(closing.await, Err(PostgresError::Operational(_))));
    let (first, second) = tokio::join!(session.wait_closed(), session.wait_closed());
    assert!(matches!(first, Err(PostgresError::Operational(_))));
    assert!(matches!(second, Err(PostgresError::Operational(_))));
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Operational(_))
    ));
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Eof
    );
}

#[tokio::test]
async fn session_shutdown_concurrent_callers_send_only_one_copydone() {
    let (connection, mut peer) = session_writer_connection(None).await;
    let session = connection
        .into_stream(1, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    let peer_close = async {
        assert!(matches!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Other(b'd', _)
        ));
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::CopyDone
        );
        peer.write_all(&session_shutdown_response()).await.unwrap();
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Eof
        );
    };
    let (first, second, ()) = tokio::join!(session.shutdown(), session.shutdown(), peer_close);
    first.unwrap();
    second.unwrap();
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_shutdown_cancelled_close_waiter_keeps_transport_owned() {
    use std::future::Future;
    let (connection, mut peer) = session_writer_connection(None).await;
    let session = connection
        .into_stream(1, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    let mut closing = Box::pin(session.shutdown());
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert!(matches!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Other(b'd', _)
    ));
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::CopyDone
    );
    drop(closing);
    peer.write_all(&session_shutdown_response()).await.unwrap();
    let (first, second) = tokio::join!(session.wait_closed(), session.wait_closed());
    first.unwrap();
    second.unwrap();
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Eof
    );
}

#[tokio::test(start_paused = true)]
async fn session_shutdown_cancel_fences_backpressured_lease_and_wakes_all_waiters() {
    let payload = session_wal_frame(&session_transaction_payload());
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(1, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    let delivery = session.next().await.unwrap().unwrap();
    tokio::time::advance(Duration::from_secs(10)).await;
    assert!(matches!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Other(b'd', _)
    ));
    let record = delivery.record();
    session.cancel();
    session.cancel();
    assert!(matches!(
        record,
        _postgres::replication::pgoutput::ReplicationRecord::Begin { xid: 7, .. }
    ));
    assert!(matches!(
        delivery.publish(),
        Err(PostgresError::Interface(_))
    ));
    let (first, second) = tokio::join!(session.wait_closed(), session.wait_closed());
    first.unwrap();
    second.unwrap();
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(session.status_positions().1, 0x200000100);
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Eof
    );
}

#[tokio::test]
async fn session_shutdown_backend_error_preserves_sqlstate_and_discards_records() {
    let (connection, mut peer) = session_writer_connection(None).await;
    let session = connection
        .into_stream(1, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    let peer_close = async {
        assert!(matches!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Other(b'd', _)
        ));
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::CopyDone
        );
        peer.write_all(&error_response_frame("ERROR", "42501", "permission denied"))
            .await
            .unwrap();
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Eof
        );
    };
    let (result, ()) = tokio::join!(session.shutdown(), peer_close);
    assert_eq!(result.unwrap_err().sqlstate(), Some("42501"));
    assert_eq!(
        session.wait_closed().await.unwrap_err().sqlstate(),
        Some("42501")
    );
    assert_eq!(
        session.next().await.err().unwrap().sqlstate(),
        Some("42501")
    );
    assert!(session.next().await.unwrap().is_none());
    assert!(session.is_closed());
}

#[tokio::test]
async fn session_shutdown_clean_backend_eof_closes_transport_but_retains_fifo() {
    let mut payload = session_wal_frame(&session_transaction_payload());
    payload.extend(session_shutdown_response());
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    session.wait_closed().await.unwrap();
    assert!(session.is_closed());
    assert!(matches!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Other(b'd', _)
    ));
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::CopyDone
    );
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Eof
    );
    for _ in 0..3 {
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    assert!(session.next().await.unwrap().is_none());
    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_shutdown_held_commit_stays_borrowable_but_cannot_publish() {
    use std::future::Future;
    let payload = session_wal_frame(&session_transaction_payload());
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(1, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    for _ in 0..2 {
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    let delivery = session.next().await.unwrap().unwrap();
    let borrowed = delivery.record();
    peer.write_all(&session_wal_frame(&session_empty_transaction_payload(
        0x200000500,
        9,
    )))
    .await
    .unwrap();
    let mut closing = Box::pin(session.shutdown());
    std::future::poll_fn(|cx| {
        assert!(closing.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    assert_eq!(borrowed.commit_end_lsn(), Some(0x200000300));
    assert!(matches!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Other(b'd', _)
    ));
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::CopyDone
    );
    peer.write_all(&session_shutdown_response()).await.unwrap();
    closing.await.unwrap();
    assert!(matches!(
        delivery.publish(),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(session.status_positions().1, 0x200000100);
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Eof
    );
}

#[tokio::test]
async fn session_shutdown_transport_error_fences_held_commit_before_publication() {
    let payload = session_wal_frame(&session_transaction_payload());
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    for _ in 0..2 {
        session.next().await.unwrap().unwrap().publish().unwrap();
    }
    let delivery = session.next().await.unwrap().unwrap();
    peer.shutdown().await.unwrap();
    assert!(matches!(
        session.wait_closed().await,
        Err(PostgresError::Transport(_))
    ));
    assert_eq!(delivery.record().commit_end_lsn(), Some(0x200000300));
    assert!(matches!(
        delivery.publish(),
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Transport(_))
    ));
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(session.status_positions().1, 0x200000100);
    assert_eq!(
        read_frontend_exchange(&mut peer).await,
        FrontendExchange::Eof
    );
}

#[tokio::test]
async fn session_shutdown_truncated_command_frame_is_terminal_protocol_error() {
    let mut payload = copy_done_frame();
    payload.extend_from_slice(b"C\0\0\0\x0bCOP");
    let (connection, mut peer) = session_connection_with_payload(Some(0x200000100), payload).await;
    let session = connection
        .into_stream(4, Duration::from_secs(3600), Duration::from_secs(2))
        .unwrap();
    peer.shutdown().await.unwrap();
    assert!(matches!(
        session.wait_closed().await,
        Err(PostgresError::Protocol(_))
    ));
    assert!(matches!(
        session.next().await,
        Err(PostgresError::Protocol(_))
    ));
    assert!(session.next().await.unwrap().is_none());
    assert_eq!(session.status_positions().1, 0x200000100);
}

#[derive(Clone)]
struct ScriptedLogicalPeerSpec {
    db: &'static str,
    active: &'static str,
    restart_lsn: &'static str,
    confirmed_flush_lsn: &'static str,
    pub_delete_flag: &'static str,
    system_id: &'static str,
    timeline: &'static str,
    xlogpos: &'static str,
}

impl Default for ScriptedLogicalPeerSpec {
    fn default() -> Self {
        Self {
            db: "appdb",
            active: "f",
            restart_lsn: "2/10",
            confirmed_flush_lsn: "2/80",
            pub_delete_flag: "t",
            system_id: "9990001112223334445",
            timeline: "1",
            xlogpos: "2/900",
        }
    }
}

async fn accept_preflight_peer(
    listener: &TcpListener,
    spec: &ScriptedLogicalPeerSpec,
    slot_error: Option<(&str, &str)>,
) -> bool {
    let (mut meta, _) = listener.accept().await.unwrap();
    complete_startup_ready_version(&mut meta, 701, "14.12").await;
    let FrontendExchange::Extended(slot_q) = read_frontend_exchange(&mut meta).await else {
        panic!("expected slot preflight query");
    };
    if let Some((code, msg)) = slot_error {
        let mut err = error_response_frame("ERROR", code, msg);
        err.extend(ready_for_query_frame(b'I'));
        meta.write_all(&err).await.unwrap();
        assert_clean_terminate_to_eof(&mut meta).await;
        return false;
    }
    meta.write_all(&extended_select_response(
        &slot_q.param_oids,
        &slot_columns(),
        &[vec![
            Some(spec.db),
            Some(spec.db),
            Some("logical"),
            Some("pgoutput"),
            Some(spec.active),
            Some(spec.restart_lsn),
            Some(spec.confirmed_flush_lsn),
            Some("extended"),
        ]],
        slot_q.saw_close_portal,
    ))
    .await
    .unwrap();

    if spec.active == "t" || spec.db != "appdb" {
        assert_clean_terminate_to_eof(&mut meta).await;
        return false;
    }

    let FrontendExchange::Extended(pub_q) = read_frontend_exchange(&mut meta).await else {
        panic!("expected publication preflight query");
    };
    meta.write_all(&extended_select_response(
        &pub_q.param_oids,
        &pub_columns_pg14(),
        &[vec![
            Some("vertebra_pub"),
            Some("f"),
            Some("t"),
            Some("t"),
            Some(spec.pub_delete_flag),
            Some("f"),
            Some("f"),
            None,
            None,
        ]],
        pub_q.saw_close_portal,
    ))
    .await
    .unwrap();
    assert_clean_terminate_to_eof(&mut meta).await;
    true
}

async fn accept_copyboth_peer(
    listener: &TcpListener,
    spec: &ScriptedLogicalPeerSpec,
    expected_start_lsn: Option<&str>,
    start_error: Option<(&str, &str)>,
    initial_payload: &[u8],
) -> Option<TcpStream> {
    let (mut socket, _) = listener.accept().await.unwrap();
    complete_startup_ready_version(&mut socket, 702, "14.12").await;
    assert_eq!(
        read_frontend_exchange(&mut socket).await,
        FrontendExchange::SimpleQuery("IDENTIFY_SYSTEM".into())
    );
    socket
        .write_all(&identify_system_response(
            spec.system_id,
            spec.timeline,
            spec.xlogpos,
            Some(spec.db),
        ))
        .await
        .unwrap();

    let next = read_frontend_exchange(&mut socket).await;
    let FrontendExchange::SimpleQuery(sql) = next else {
        assert_eq!(next, FrontendExchange::Terminate);
        assert_eq!(
            read_frontend_exchange(&mut socket).await,
            FrontendExchange::Eof
        );
        return None;
    };
    if let Some(expected_lsn) = expected_start_lsn {
        assert!(
            sql.contains(&format!("LOGICAL {expected_lsn} ")),
            "expected LOGICAL {expected_lsn} in {sql}"
        );
    }
    if let Some((code, msg)) = start_error {
        let mut err = error_response_frame("ERROR", code, msg);
        err.extend(ready_for_query_frame(b'I'));
        socket.write_all(&err).await.unwrap();
        assert_clean_terminate_to_eof(&mut socket).await;
        return None;
    }
    let mut startup = parameter_status_frame("wal_sender_timeout", "30s");
    startup.extend(copy_both_response_frame(0, &[]));
    startup.extend_from_slice(initial_payload);
    socket.write_all(&startup).await.unwrap();
    Some(socket)
}

#[test]
fn supervisor_reconnect_delay_bounds_monotonic_progression_and_cap_jitter() {
    assert_eq!(MAX_RAW_RECONNECT_ATTEMPTS, 8);
    let min_delay = Duration::from_millis(100);
    let max_delay = Duration::from_secs(5);
    let seeds = [0_u64, 1, 7, 42, 0xDEAD_BEEF_CAFE_BABE, u64::MAX];
    let attempts = [0_u32, 1, 2, 3, 4, 5, 6, 7, 8, 16, u32::MAX];

    for &seed in &seeds {
        for &attempt in &attempts {
            let delay = raw_reconnect_delay(attempt, seed);
            assert!(
                (min_delay..=max_delay).contains(&delay),
                "delay {delay:?} out of 100ms..=5s for attempt={attempt} seed={seed}"
            );
            assert_eq!(delay, raw_reconnect_delay(attempt, seed));
        }
        for step in 0..6 {
            let earlier = raw_reconnect_delay(step, seed);
            let later = raw_reconnect_delay(step + 1, seed);
            assert!(
                earlier < later,
                "expected exponential progression {earlier:?} < {later:?} at step {step}"
            );
        }
    }

    let first_distinct: std::collections::BTreeSet<Duration> = (0_u64..32)
        .map(|seed| raw_reconnect_delay(0, seed))
        .collect();
    assert!(
        first_distinct.len() > 1,
        "expected jitter spread at attempt 0"
    );

    let cap_distinct: std::collections::BTreeSet<Duration> = (0_u64..32)
        .map(|seed| raw_reconnect_delay(7, seed))
        .collect();
    assert!(
        cap_distinct.len() > 1,
        "expected jitter spread even at the 5s cap"
    );
}

#[tokio::test]
async fn supervisor_open_validates_pooler_mode_capacity_and_identifiers_pre_dial() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_bg = Arc::clone(&accepts);
    let bg = tokio::spawn(async move {
        while listener.accept().await.is_ok() {
            accepts_bg.fetch_add(1, Ordering::SeqCst);
        }
    });

    for bad_mode in [
        PoolerMode::Transaction,
        PoolerMode::Statement,
        PoolerMode::Auto,
    ] {
        let cfg = config_with_mode(port, bad_mode, Duration::from_secs(1));
        let err = ReplicationSupervisor::open(
            cfg,
            "vertebra_slot".into(),
            "vertebra_pub".into(),
            None,
            4,
            true,
        )
        .await
        .expect_err("non-session pooler mode must fail pre-dial");
        assert!(matches!(err, PostgresError::Interface(_)));
    }

    for bad_capacity in [0_usize, 1025] {
        let err = ReplicationSupervisor::open(
            direct_config(port),
            "vertebra_slot".into(),
            "vertebra_pub".into(),
            None,
            bad_capacity,
            false,
        )
        .await
        .expect_err("out-of-range queue_capacity must fail pre-dial");
        assert!(matches!(err, PostgresError::Interface(_)));
    }

    let overlong = "x".repeat(MAX_REPLICATION_IDENTIFIER_BYTES + 1);
    for bad_ident in ["", "bad\0id", "bad\nid", overlong.as_str()] {
        let slot_err = ReplicationSupervisor::open(
            direct_config(port),
            bad_ident.into(),
            "vertebra_pub".into(),
            None,
            4,
            false,
        )
        .await
        .expect_err("invalid slot must fail pre-dial");
        assert!(matches!(slot_err, PostgresError::Data(_)));

        let pub_err = ReplicationSupervisor::open(
            direct_config(port),
            "vertebra_slot".into(),
            bad_ident.into(),
            None,
            4,
            false,
        )
        .await
        .expect_err("invalid publication must fail pre-dial");
        assert!(matches!(pub_err, PostgresError::Data(_)));
    }

    tokio::task::yield_now().await;
    assert_eq!(accepts.load(Ordering::SeqCst), 0);
    bg.abort();
}

#[tokio::test]
async fn supervisor_single_generation_leases_dropped_fifo_restoration_fenced_ack_and_clean_shutdown()
 {
    use _postgres::replication::pgoutput::ReplicationRecord;
    use std::future::Future;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = direct_config(listener.local_addr().unwrap().port());
    let spec = ScriptedLogicalPeerSpec::default();
    let tx1 = session_wal_frame(&session_transaction_payload());

    let peer_task = tokio::spawn(async move {
        assert!(accept_preflight_peer(&listener, &spec, None).await);
        accept_copyboth_peer(&listener, &spec, Some("2/100"), None, &tx1)
            .await
            .expect("initial CopyBoth socket")
    });

    let supervisor = ReplicationSupervisor::open(
        cfg,
        "vertebra_slot".into(),
        "vertebra_pub".into(),
        Some(0x2_0000_0100),
        4,
        false,
    )
    .await
    .unwrap();
    let mut peer = peer_task.await.unwrap();

    assert!(!supervisor.is_closed());
    assert_eq!(
        supervisor.resume(),
        PostgresReplicationResume {
            identity: supervisor.resume().identity,
            acknowledged_lsn: 0x2_0000_0100,
            generation: 1,
        }
    );

    let control_lease = supervisor.next_event().await.unwrap().unwrap();
    assert!(matches!(
        control_lease.event(),
        ReplicationStreamEvent::Control {
            acknowledged_lsn: 0x2_0000_0100,
            generation: 1,
            ..
        }
    ));

    let mut overlap = Box::pin(supervisor.next_event());
    std::future::poll_fn(|cx| {
        assert!(matches!(
            overlap.as_mut().poll(cx),
            Poll::Ready(Err(PostgresError::Interface(_)))
        ));
        Poll::Ready(())
    })
    .await;
    drop(overlap);

    drop(control_lease);
    let restored_control = supervisor
        .next_event()
        .await
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    assert!(matches!(
        restored_control,
        ReplicationStreamEvent::Control {
            acknowledged_lsn: 0x2_0000_0100,
            generation: 1,
            ..
        }
    ));

    let begin_ev = supervisor
        .next_event()
        .await
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    assert!(matches!(
        begin_ev,
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Begin { xid: 7, .. },
            generation: 1,
        }
    ));

    let insert_ev = supervisor
        .next_event()
        .await
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    assert!(matches!(
        insert_ev,
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Insert { xid: 7, .. },
            generation: 1,
        }
    ));

    let commit_lease = supervisor.next_event().await.unwrap().unwrap();
    assert!(matches!(
        commit_lease.event(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Commit {
                commit_end_lsn: 0x2_0000_0300,
                ..
            },
            generation: 1,
        }
    ));
    assert!(matches!(
        supervisor.acknowledge(1, 0x2_0000_0300),
        Err(PostgresError::Interface(_))
    ));

    let mut next_begin = session_transaction_payload()[..21].to_vec();
    next_begin[1..9].copy_from_slice(&0x2_0000_0400_u64.to_be_bytes());
    next_begin[17..21].copy_from_slice(&9_u32.to_be_bytes());
    peer.write_all(&session_wal_frame(&next_begin))
        .await
        .unwrap();

    drop(commit_lease);
    assert!(matches!(
        supervisor.acknowledge(1, 0x2_0000_0300),
        Err(PostgresError::Interface(_))
    ));

    let restored_commit = supervisor.next_event().await.unwrap().unwrap();
    assert!(matches!(
        restored_commit.publish().unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Commit {
                commit_end_lsn: 0x2_0000_0300,
                ..
            },
            generation: 1,
        }
    ));
    assert!(matches!(
        supervisor.acknowledge(0, 0x2_0000_0300),
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        supervisor.acknowledge(2, 0x2_0000_0300),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(
        supervisor.acknowledge(1, 0x2_0000_0300).unwrap(),
        0x2_0000_0300
    );
    assert_eq!(
        supervisor.acknowledge(1, 0x2_0000_0300).unwrap(),
        0x2_0000_0300
    );
    assert!(matches!(
        supervisor.acknowledge(1, 0x2_0000_0100),
        Err(PostgresError::Interface(_))
    ));
    assert_eq!(supervisor.resume().acknowledged_lsn, 0x2_0000_0300);

    let FrontendExchange::Other(b'd', status) = read_frontend_exchange(&mut peer).await else {
        panic!("expected StandbyStatusUpdate frame");
    };
    assert_eq!(
        u64::from_be_bytes(status[9..17].try_into().unwrap()),
        0x2_0000_0300
    );
    assert_eq!(
        u64::from_be_bytes(status[17..25].try_into().unwrap()),
        0x2_0000_0300
    );

    let held_begin = supervisor.next_event().await.unwrap().unwrap();
    assert!(matches!(
        held_begin.event(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Begin { xid: 9, .. },
            generation: 1,
        }
    ));

    let peer_close = tokio::spawn(async move {
        let FrontendExchange::Other(b'd', final_status) = read_frontend_exchange(&mut peer).await
        else {
            panic!("expected final status before CopyDone");
        };
        assert_eq!(
            u64::from_be_bytes(final_status[9..17].try_into().unwrap()),
            0x2_0000_0300
        );
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::CopyDone
        );
        peer.write_all(&session_shutdown_response()).await.unwrap();
        assert_eq!(
            read_frontend_exchange(&mut peer).await,
            FrontendExchange::Eof
        );
    });

    supervisor.shutdown().await.unwrap();
    supervisor.shutdown().await.unwrap();
    assert!(supervisor.is_closed());
    assert!(matches!(
        held_begin.publish(),
        Err(PostgresError::Interface(_))
    ));
    assert!(supervisor.next_event().await.unwrap().is_none());
    peer_close.await.unwrap();
}

#[test]
fn supervisor_drop_without_ambient_runtime_aborts_tasks_and_fences_held_lease() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (supervisor, held_lease, peer) = runtime.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = direct_config(listener.local_addr().unwrap().port());
        let spec = ScriptedLogicalPeerSpec::default();
        let peer_task = tokio::spawn(async move {
            assert!(accept_preflight_peer(&listener, &spec, None).await);
            accept_copyboth_peer(&listener, &spec, Some("0/0"), None, &[])
                .await
                .unwrap()
        });
        let supervisor = ReplicationSupervisor::open(
            cfg,
            "vertebra_slot".into(),
            "vertebra_pub".into(),
            None,
            2,
            true,
        )
        .await
        .unwrap();
        let peer = peer_task.await.unwrap().into_std().unwrap();
        let held_lease = supervisor.next_event().await.unwrap().unwrap();
        (supervisor, held_lease, peer)
    });

    drop(runtime);
    assert!(matches!(
        held_lease.event(),
        ReplicationStreamEvent::Control { generation: 1, .. }
    ));
    drop(supervisor);

    peer.set_nonblocking(false).unwrap();
    peer.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    assert_eq!(std::io::Read::read(&mut &peer, &mut [0; 1]).unwrap(), 0);
    assert!(matches!(
        held_lease.publish(),
        Err(PostgresError::Interface(_))
    ));
}

#[tokio::test]
async fn connect_logical_verified_enforces_identity_timeline_wal_bounds_and_confirmed_flush_floor()
{
    let base_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base_cfg = direct_config(base_listener.local_addr().unwrap().port());
    let base_spec = ScriptedLogicalPeerSpec::default();
    let base_peer = tokio::spawn(async move {
        assert!(accept_preflight_peer(&base_listener, &base_spec, None).await);
        let mut repl = accept_copyboth_peer(&base_listener, &base_spec, Some("2/100"), None, &[])
            .await
            .unwrap();
        assert_eq!(
            read_frontend_exchange(&mut repl).await,
            FrontendExchange::CopyDone
        );
        repl.write_all(&session_shutdown_response()).await.unwrap();
        assert_clean_terminate_to_eof(&mut repl).await;
    });
    let mut initial_conn = PostgresReplicationConnection::connect_logical(
        &base_cfg,
        "vertebra_slot",
        "vertebra_pub",
        Some(0x2_0000_0100),
    )
    .await
    .unwrap();
    let expected_identity = initial_conn.identity().clone();
    initial_conn
        .shutdown(Instant::now() + Duration::from_secs(2))
        .await
        .unwrap();
    base_peer.await.unwrap();

    let promoted_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let promoted_cfg = direct_config(promoted_listener.local_addr().unwrap().port());
    let promoted_spec = ScriptedLogicalPeerSpec {
        confirmed_flush_lsn: "2/380",
        timeline: "2",
        ..ScriptedLogicalPeerSpec::default()
    };
    let promoted_peer = tokio::spawn(async move {
        assert!(accept_preflight_peer(&promoted_listener, &promoted_spec, None).await);
        accept_copyboth_peer(&promoted_listener, &promoted_spec, Some("2/380"), None, &[])
            .await
            .unwrap()
    });
    let verified_conn = PostgresReplicationConnection::connect_logical_verified(
        &promoted_cfg,
        "vertebra_slot",
        "vertebra_pub",
        0x2_0000_0100,
        &expected_identity,
    )
    .await
    .unwrap();
    assert_eq!(verified_conn.identity().timeline, 2);
    let session = verified_conn
        .into_stream(2, Duration::from_secs(10), Duration::from_secs(2))
        .unwrap();
    assert_eq!(
        session.status_positions(),
        (0x2_0000_0380, 0x2_0000_0380, 0x2_0000_0380)
    );
    session.cancel();
    let _peer = promoted_peer.await.unwrap();

    #[derive(Clone, Copy, Debug)]
    enum MismatchCase {
        SlotPreDial,
        Database,
        PublicationFingerprint,
        SystemIdentifier,
        TimelineRegression,
        RestartLsnGap,
        XlogPosGap,
    }

    for case in [
        MismatchCase::SlotPreDial,
        MismatchCase::Database,
        MismatchCase::PublicationFingerprint,
        MismatchCase::SystemIdentifier,
        MismatchCase::TimelineRegression,
        MismatchCase::RestartLsnGap,
        MismatchCase::XlogPosGap,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = direct_config(listener.local_addr().unwrap().port());
        let dials = Arc::new(AtomicUsize::new(0));
        let dials_bg = Arc::clone(&dials);

        let mut expected = expected_identity.clone();
        if matches!(case, MismatchCase::TimelineRegression) {
            expected.timeline = 3;
        }
        let slot_arg = if matches!(case, MismatchCase::SlotPreDial) {
            "different_slot"
        } else {
            "vertebra_slot"
        };
        let req_lsn = if matches!(case, MismatchCase::XlogPosGap) {
            0x2_0000_0950
        } else {
            0x2_0000_0100
        };

        let srv = tokio::spawn(async move {
            match case {
                MismatchCase::SlotPreDial => {
                    if listener.accept().await.is_ok() {
                        dials_bg.fetch_add(1, Ordering::SeqCst);
                    }
                }
                MismatchCase::Database => {
                    dials_bg.fetch_add(1, Ordering::SeqCst);
                    let spec = ScriptedLogicalPeerSpec {
                        db: "wrongdb",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(!accept_preflight_peer(&listener, &spec, None).await);
                }
                MismatchCase::PublicationFingerprint => {
                    dials_bg.fetch_add(1, Ordering::SeqCst);
                    let spec = ScriptedLogicalPeerSpec {
                        pub_delete_flag: "f",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(accept_preflight_peer(&listener, &spec, None).await);
                }
                MismatchCase::RestartLsnGap => {
                    dials_bg.fetch_add(1, Ordering::SeqCst);
                    let spec = ScriptedLogicalPeerSpec {
                        restart_lsn: "2/200",
                        confirmed_flush_lsn: "2/180",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(accept_preflight_peer(&listener, &spec, None).await);
                }
                MismatchCase::SystemIdentifier => {
                    dials_bg.fetch_add(1, Ordering::SeqCst);
                    let spec = ScriptedLogicalPeerSpec {
                        system_id: "1112223334445556667",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(accept_preflight_peer(&listener, &spec, None).await);
                    assert!(
                        accept_copyboth_peer(&listener, &spec, None, None, &[])
                            .await
                            .is_none()
                    );
                }
                MismatchCase::TimelineRegression => {
                    dials_bg.fetch_add(1, Ordering::SeqCst);
                    let spec = ScriptedLogicalPeerSpec {
                        timeline: "2",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(accept_preflight_peer(&listener, &spec, None).await);
                    assert!(
                        accept_copyboth_peer(&listener, &spec, None, None, &[])
                            .await
                            .is_none()
                    );
                }
                MismatchCase::XlogPosGap => {
                    dials_bg.fetch_add(1, Ordering::SeqCst);
                    let spec = ScriptedLogicalPeerSpec {
                        xlogpos: "2/900",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(accept_preflight_peer(&listener, &spec, None).await);
                    assert!(
                        accept_copyboth_peer(&listener, &spec, None, None, &[])
                            .await
                            .is_none()
                    );
                }
            }
        });

        let err = PostgresReplicationConnection::connect_logical_verified(
            &cfg,
            slot_arg,
            "vertebra_pub",
            req_lsn,
            &expected,
        )
        .await
        .expect_err("verified connect must fail closed");
        assert!(
            matches!(err, PostgresError::Operational(_)),
            "expected Operational for {case:?}, got {err:?}"
        );
        if matches!(case, MismatchCase::SlotPreDial) {
            tokio::task::yield_now().await;
            assert_eq!(dials.load(Ordering::SeqCst), 0);
            srv.abort();
        } else {
            srv.await.unwrap();
        }
    }
}

#[tokio::test(start_paused = true)]
async fn supervisor_reconnect_fences_stale_generation_retries_55006_and_active_slot_and_advances_confirmed_floor()
 {
    use _postgres::replication::pgoutput::ReplicationRecord;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = direct_config(listener.local_addr().unwrap().port());

    let mut gen1_payload = session_wal_frame(&session_empty_transaction_payload(0x2_0000_0300, 7));
    gen1_payload.extend(session_wal_frame(&session_empty_transaction_payload(
        0x2_0000_0450,
        8,
    )));
    let gen2_payload = session_wal_frame(&session_empty_transaction_payload(0x2_0000_0600, 11));

    let server = tokio::spawn(async move {
        let gen1_spec = ScriptedLogicalPeerSpec::default();
        assert!(accept_preflight_peer(&listener, &gen1_spec, None).await);
        let mut gen1_sock =
            accept_copyboth_peer(&listener, &gen1_spec, Some("2/100"), None, &gen1_payload)
                .await
                .unwrap();

        let FrontendExchange::Other(b'd', ack_status) =
            read_frontend_exchange(&mut gen1_sock).await
        else {
            panic!("expected gen 1 StandbyStatusUpdate");
        };
        assert_eq!(
            u64::from_be_bytes(ack_status[9..17].try_into().unwrap()),
            0x2_0000_0300
        );
        gen1_sock.shutdown().await.unwrap();
        drop(gen1_sock);

        let active_spec = ScriptedLogicalPeerSpec {
            active: "t",
            confirmed_flush_lsn: "2/300",
            ..ScriptedLogicalPeerSpec::default()
        };
        assert!(!accept_preflight_peer(&listener, &active_spec, None).await);

        let race_55006_spec = ScriptedLogicalPeerSpec {
            confirmed_flush_lsn: "2/380",
            ..ScriptedLogicalPeerSpec::default()
        };
        assert!(accept_preflight_peer(&listener, &race_55006_spec, None).await);
        assert!(
            accept_copyboth_peer(
                &listener,
                &race_55006_spec,
                Some("2/380"),
                Some((
                    "55006",
                    "replication slot \"vertebra_slot\" is active for PID 901"
                )),
                &[],
            )
            .await
            .is_none()
        );

        let gen2_spec = ScriptedLogicalPeerSpec {
            confirmed_flush_lsn: "2/380",
            timeline: "2",
            ..ScriptedLogicalPeerSpec::default()
        };
        assert!(accept_preflight_peer(&listener, &gen2_spec, None).await);
        let mut gen2_sock =
            accept_copyboth_peer(&listener, &gen2_spec, Some("2/380"), None, &gen2_payload)
                .await
                .unwrap();

        let FrontendExchange::Other(b'd', gen2_ack) = read_frontend_exchange(&mut gen2_sock).await
        else {
            panic!("expected gen 2 StandbyStatusUpdate");
        };
        assert_eq!(
            u64::from_be_bytes(gen2_ack[9..17].try_into().unwrap()),
            0x2_0000_0600
        );
        gen2_sock
    });

    let supervisor = ReplicationSupervisor::open(
        cfg,
        "vertebra_slot".into(),
        "vertebra_pub".into(),
        Some(0x2_0000_0100),
        8,
        true,
    )
    .await
    .unwrap();

    let gen1_ctrl = supervisor
        .next_event()
        .await
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    assert!(matches!(
        gen1_ctrl,
        ReplicationStreamEvent::Control {
            acknowledged_lsn: 0x2_0000_0100,
            generation: 1,
            ..
        }
    ));

    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Begin { xid: 7, .. },
            generation: 1,
        }
    ));
    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Commit {
                commit_end_lsn: 0x2_0000_0300,
                ..
            },
            generation: 1,
        }
    ));

    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Begin { xid: 8, .. },
            generation: 1,
        }
    ));
    let held_unacked_gen1_commit = supervisor.next_event().await.unwrap().unwrap();
    assert!(matches!(
        held_unacked_gen1_commit.event(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Commit {
                commit_end_lsn: 0x2_0000_0450,
                ..
            },
            generation: 1,
        }
    ));

    assert_eq!(
        supervisor.acknowledge(1, 0x2_0000_0300).unwrap(),
        0x2_0000_0300
    );

    tokio::time::sleep(Duration::from_secs(2)).await;

    assert!(matches!(
        held_unacked_gen1_commit.publish(),
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        supervisor.acknowledge(1, 0x2_0000_0300),
        Err(PostgresError::Interface(_))
    ));
    assert!(!supervisor.is_closed());

    let gen2_ctrl = supervisor
        .next_event()
        .await
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    match gen2_ctrl {
        ReplicationStreamEvent::Control {
            identity,
            acknowledged_lsn,
            generation,
        } => {
            assert_eq!(generation, 2);
            assert_eq!(acknowledged_lsn, 0x2_0000_0380);
            assert_eq!(identity.timeline, 2);
        }
        other => panic!("expected gen 2 Control event, got {other:?}"),
    }

    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Begin { xid: 11, .. },
            generation: 2,
        }
    ));
    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Commit {
                commit_end_lsn: 0x2_0000_0600,
                ..
            },
            generation: 2,
        }
    ));
    assert_eq!(
        supervisor.acknowledge(2, 0x2_0000_0600).unwrap(),
        0x2_0000_0600
    );
    assert_eq!(supervisor.resume().generation, 2);
    assert_eq!(supervisor.resume().acknowledged_lsn, 0x2_0000_0600);

    let _gen2_peer = server.await.unwrap();
    supervisor.cancel();
}

#[tokio::test(start_paused = true)]
async fn supervisor_permanent_errors_and_disabled_reconnect_fail_immediately_and_exhaustion_caps_at_eight()
 {
    #[derive(Clone, Copy, Debug)]
    enum TerminalScenario {
        ReconnectDisabledFrameEof,
        TruncatedMidFrameEof,
        MissingSlot42704OnReconnect,
        AuthFailure28P01OnReconnect,
        SystemIdMismatchOnReconnect,
        PublicationMismatchOnReconnect,
    }

    for scenario in [
        TerminalScenario::ReconnectDisabledFrameEof,
        TerminalScenario::TruncatedMidFrameEof,
        TerminalScenario::MissingSlot42704OnReconnect,
        TerminalScenario::AuthFailure28P01OnReconnect,
        TerminalScenario::SystemIdMismatchOnReconnect,
        TerminalScenario::PublicationMismatchOnReconnect,
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = direct_config(listener.local_addr().unwrap().port());
        let reconnect_attempts = Arc::new(AtomicUsize::new(0));
        let reconnect_attempts_bg = Arc::clone(&reconnect_attempts);
        let reconnect_flag = !matches!(scenario, TerminalScenario::ReconnectDisabledFrameEof);

        let srv = tokio::spawn(async move {
            let spec = ScriptedLogicalPeerSpec::default();
            assert!(accept_preflight_peer(&listener, &spec, None).await);
            let initial_bytes: &[u8] = if matches!(scenario, TerminalScenario::TruncatedMidFrameEof)
            {
                b"d\0\0\0\x20w"
            } else {
                &[]
            };
            let mut gen1 = accept_copyboth_peer(&listener, &spec, Some("0/0"), None, initial_bytes)
                .await
                .unwrap();
            gen1.shutdown().await.unwrap();
            drop(gen1);

            match scenario {
                TerminalScenario::ReconnectDisabledFrameEof
                | TerminalScenario::TruncatedMidFrameEof => {
                    if listener.accept().await.is_ok() {
                        reconnect_attempts_bg.fetch_add(1, Ordering::SeqCst);
                    }
                }
                TerminalScenario::MissingSlot42704OnReconnect => {
                    reconnect_attempts_bg.fetch_add(1, Ordering::SeqCst);
                    assert!(
                        !accept_preflight_peer(
                            &listener,
                            &spec,
                            Some(("42704", "replication slot does not exist")),
                        )
                        .await
                    );
                }
                TerminalScenario::AuthFailure28P01OnReconnect => {
                    reconnect_attempts_bg.fetch_add(1, Ordering::SeqCst);
                    let (mut meta, _) = listener.accept().await.unwrap();
                    let _ = read_startup_params(&mut meta).await;
                    meta.write_all(&error_response_frame(
                        "FATAL",
                        "28P01",
                        "password authentication failed",
                    ))
                    .await
                    .unwrap();
                    meta.shutdown().await.unwrap();
                }
                TerminalScenario::SystemIdMismatchOnReconnect => {
                    reconnect_attempts_bg.fetch_add(1, Ordering::SeqCst);
                    let drifted = ScriptedLogicalPeerSpec {
                        system_id: "5555555555555555555",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(accept_preflight_peer(&listener, &drifted, None).await);
                    assert!(
                        accept_copyboth_peer(&listener, &drifted, None, None, &[])
                            .await
                            .is_none()
                    );
                }
                TerminalScenario::PublicationMismatchOnReconnect => {
                    reconnect_attempts_bg.fetch_add(1, Ordering::SeqCst);
                    let drifted = ScriptedLogicalPeerSpec {
                        pub_delete_flag: "f",
                        ..ScriptedLogicalPeerSpec::default()
                    };
                    assert!(accept_preflight_peer(&listener, &drifted, None).await);
                }
            }
        });

        let supervisor = ReplicationSupervisor::open(
            cfg,
            "vertebra_slot".into(),
            "vertebra_pub".into(),
            None,
            4,
            reconnect_flag,
        )
        .await
        .unwrap();

        assert!(matches!(
            supervisor
                .next_event()
                .await
                .unwrap()
                .unwrap()
                .publish()
                .unwrap(),
            ReplicationStreamEvent::Control { generation: 1, .. }
        ));

        let err = supervisor
            .next_event()
            .await
            .expect_err("terminal error must surface");
        match scenario {
            TerminalScenario::ReconnectDisabledFrameEof => {
                assert!(matches!(err, PostgresError::Transport(_)));
                assert_eq!(reconnect_attempts.load(Ordering::SeqCst), 0);
                srv.abort();
            }
            TerminalScenario::TruncatedMidFrameEof => {
                assert!(matches!(err, PostgresError::Protocol(_)));
                assert_eq!(reconnect_attempts.load(Ordering::SeqCst), 0);
                srv.abort();
            }
            TerminalScenario::MissingSlot42704OnReconnect => {
                assert_eq!(err.sqlstate(), Some("42704"));
                assert_eq!(reconnect_attempts.load(Ordering::SeqCst), 1);
                srv.await.unwrap();
            }
            TerminalScenario::AuthFailure28P01OnReconnect => {
                assert_eq!(err.sqlstate(), Some("28P01"));
                assert_eq!(reconnect_attempts.load(Ordering::SeqCst), 1);
                srv.await.unwrap();
            }
            TerminalScenario::SystemIdMismatchOnReconnect
            | TerminalScenario::PublicationMismatchOnReconnect => {
                assert!(matches!(err, PostgresError::Operational(_)));
                assert_eq!(reconnect_attempts.load(Ordering::SeqCst), 1);
                srv.await.unwrap();
            }
        }
        assert!(supervisor.is_closed());
        assert!(supervisor.next_event().await.unwrap().is_none());
    }

    let cap_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cap_cfg = direct_config(cap_listener.local_addr().unwrap().port());
    let attempts = Arc::new(AtomicUsize::new(0));
    let attempts_bg = Arc::clone(&attempts);
    let cap_srv = tokio::spawn(async move {
        let spec = ScriptedLogicalPeerSpec::default();
        assert!(accept_preflight_peer(&cap_listener, &spec, None).await);
        let mut gen1 = accept_copyboth_peer(&cap_listener, &spec, Some("0/0"), None, &[])
            .await
            .unwrap();
        gen1.shutdown().await.unwrap();
        drop(gen1);

        for _ in 0..MAX_RAW_RECONNECT_ATTEMPTS {
            attempts_bg.fetch_add(1, Ordering::SeqCst);
            assert!(accept_preflight_peer(&cap_listener, &spec, None).await);
            assert!(
                accept_copyboth_peer(
                    &cap_listener,
                    &spec,
                    Some("2/80"),
                    Some(("55006", "replication slot still active")),
                    &[],
                )
                .await
                .is_none()
            );
        }
    });

    let cap_sup = ReplicationSupervisor::open(
        cap_cfg,
        "vertebra_slot".into(),
        "vertebra_pub".into(),
        None,
        2,
        true,
    )
    .await
    .unwrap();
    cap_sup
        .next_event()
        .await
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    let exhausted_err = cap_sup
        .next_event()
        .await
        .expect_err("exhausted retries must surface error");
    assert_eq!(exhausted_err.sqlstate(), Some("55006"));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        MAX_RAW_RECONNECT_ATTEMPTS as usize
    );
    assert!(cap_sup.is_closed());
    assert!(cap_sup.next_event().await.unwrap().is_none());
    cap_srv.await.unwrap();

    let cancel_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cancel_cfg = direct_config(cancel_listener.local_addr().unwrap().port());
    let backoff_dials = Arc::new(AtomicUsize::new(0));
    let backoff_dials_bg = Arc::clone(&backoff_dials);
    let cancel_srv = tokio::spawn(async move {
        let spec = ScriptedLogicalPeerSpec::default();
        assert!(accept_preflight_peer(&cancel_listener, &spec, None).await);
        let mut gen1 = accept_copyboth_peer(&cancel_listener, &spec, Some("0/0"), None, &[])
            .await
            .unwrap();
        gen1.shutdown().await.unwrap();
        drop(gen1);
        if cancel_listener.accept().await.is_ok() {
            backoff_dials_bg.fetch_add(1, Ordering::SeqCst);
        }
    });

    let cancel_sup = ReplicationSupervisor::open(
        cancel_cfg,
        "vertebra_slot".into(),
        "vertebra_pub".into(),
        None,
        2,
        true,
    )
    .await
    .unwrap();
    cancel_sup
        .next_event()
        .await
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel_sup.shutdown().await.unwrap();
    assert!(cancel_sup.is_closed());
    assert_eq!(backoff_dials.load(Ordering::SeqCst), 0);
    cancel_srv.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn supervisor_acknowledge_race_with_disconnect_and_current_thread_error_yield_reconnects_cleanly()
 {
    use _postgres::replication::pgoutput::ReplicationRecord;

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let cfg = direct_config(listener.local_addr().unwrap().port());
    let (drop_gen1_tx, drop_gen1_rx) = tokio::sync::oneshot::channel::<()>();
    let (gen1_dropped_tx, gen1_dropped_rx) = tokio::sync::oneshot::channel::<()>();

    let gen1_payload = session_wal_frame(&session_empty_transaction_payload(0x2_0000_0240, 77));
    let gen2_payload = session_wal_frame(&session_empty_transaction_payload(0x2_0000_0280, 78));

    let srv = tokio::spawn(async move {
        let spec = ScriptedLogicalPeerSpec::default();
        assert!(accept_preflight_peer(&listener, &spec, None).await);
        let mut gen1 = accept_copyboth_peer(&listener, &spec, Some("0/0"), None, &gen1_payload)
            .await
            .unwrap();
        drop_gen1_rx.await.unwrap();
        gen1.shutdown().await.unwrap();
        drop(gen1);
        let _ = gen1_dropped_tx.send(());

        assert!(accept_preflight_peer(&listener, &spec, None).await);
        let mut gen2 = accept_copyboth_peer(&listener, &spec, Some("2/80"), None, &gen2_payload)
            .await
            .unwrap();
        let FrontendExchange::Other(b'd', gen2_ack) = read_frontend_exchange(&mut gen2).await
        else {
            panic!("expected gen 2 StandbyStatusUpdate");
        };
        assert_eq!(
            u64::from_be_bytes(gen2_ack[9..17].try_into().unwrap()),
            0x2_0000_0280
        );
        gen2
    });

    let supervisor = ReplicationSupervisor::open(
        cfg,
        "vertebra_slot".into(),
        "vertebra_pub".into(),
        None,
        8,
        true,
    )
    .await
    .unwrap();

    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Control { generation: 1, .. }
    ));
    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Begin { xid: 77, .. },
            generation: 1
        }
    ));
    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Commit {
                commit_end_lsn: 0x2_0000_0240,
                ..
            },
            generation: 1
        }
    ));

    drop_gen1_tx.send(()).unwrap();
    gen1_dropped_rx.await.unwrap();
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }

    assert!(
        supervisor.acknowledge(1, 0x2_0000_0240).is_err(),
        "acknowledging after peer disconnect must fail without mutating resume_lsn or losing worker"
    );
    assert_eq!(supervisor.resume().acknowledged_lsn, 0x2_0000_0080);

    let next_ev = tokio::time::timeout(Duration::from_secs(3), supervisor.next_event())
        .await
        .expect("current_thread next_event must yield to reconnect worker instead of hanging")
        .unwrap()
        .unwrap()
        .publish()
        .unwrap();
    assert!(matches!(
        next_ev,
        ReplicationStreamEvent::Control {
            generation: 2,
            acknowledged_lsn: 0x2_0000_0080,
            ..
        }
    ));
    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Begin { xid: 78, .. },
            generation: 2
        }
    ));
    assert!(matches!(
        supervisor
            .next_event()
            .await
            .unwrap()
            .unwrap()
            .publish()
            .unwrap(),
        ReplicationStreamEvent::Record {
            record: ReplicationRecord::Commit {
                commit_end_lsn: 0x2_0000_0280,
                ..
            },
            generation: 2
        }
    ));
    assert_eq!(
        supervisor.acknowledge(2, 0x2_0000_0280).unwrap(),
        0x2_0000_0280
    );
    let _gen2_sock = srv.await.unwrap();
    supervisor.cancel();
}
