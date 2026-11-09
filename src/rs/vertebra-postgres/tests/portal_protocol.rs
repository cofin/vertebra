//! Integration tests for named server portals, incremental `Execute(..., max_rows)`
//! suspension (`PortalSuspended` `'s'`), `PortalEvent` streaming, and pin enforcement (`ch13:t02`).

extern crate _postgres as vertebra_postgres;

use std::sync::Arc;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::codecs::INT4_OID;
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::portal::PortalEvent;
use vertebra_postgres::result::PgValue;
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
    out.extend_from_slice(&frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&frame(b'S', b"TimeZone\0UTC\0"));
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
        body.put_i16(4);
        body.put_i32(-1);
        body.put_i16(fmt);
    }
    frame(b'T', &body)
}

fn data_row_text(val: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(val.len() as i32);
    body.put_slice(val.as_bytes());
    frame(b'D', &body)
}

fn cmd_complete(tag: &str) -> Vec<u8> {
    let mut body = Vec::with_capacity(tag.len() + 1);
    body.extend_from_slice(tag.as_bytes());
    body.push(0);
    frame(b'C', &body)
}

async fn read_startup(sock: &mut tokio::net::TcpStream) {
    let mut len_buf = [0u8; 4];
    sock.read_exact(&mut len_buf).await.unwrap();
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len - 4];
    sock.read_exact(&mut body).await.unwrap();
}

async fn read_until_sync_or_query(sock: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        frames.push((tag, body));
        if tag == b'S' || tag == b'Q' {
            break;
        }
    }
    frames
}

fn loopback_handle(port: u16) -> SessionHandle {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    SessionHandle::new(raw).unwrap()
}

#[tokio::test]
async fn portal_suspended_chunks_pin_exclusion_and_completion_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        // 1. open_portal from Idle issues BEGIN ('Q')
        let begin_frames = read_until_sync_or_query(&mut sock).await;
        assert_eq!(begin_frames[0], (b'Q', b"BEGIN\0".to_vec()));
        let mut resp = Vec::new();
        resp.extend_from_slice(&cmd_complete("BEGIN"));
        resp.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&resp).await.unwrap();

        // 2. open_portal setup -> Parse("") + Describe(S) + Bind(portal) + Describe(P) + Sync
        let setup_frames = read_until_sync_or_query(&mut sock).await;
        let setup_tags: Vec<u8> = setup_frames.iter().map(|f| f.0).collect();
        assert_eq!(setup_tags, vec![b'P', b'D', b'B', b'D', b'S']);
        let mut r_setup = Vec::new();
        r_setup.extend_from_slice(&frame(b'1', &[]));
        r_setup.extend_from_slice(&frame(b't', &0i16.to_be_bytes()));
        r_setup.extend_from_slice(&row_desc(&[("n", INT4_OID, 0)]));
        r_setup.extend_from_slice(&frame(b'2', &[]));
        r_setup.extend_from_slice(&row_desc(&[("n", INT4_OID, 0)]));
        r_setup.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&r_setup).await.unwrap();

        // 3. First start_fetch(2) -> Execute(portal, 2) + Sync -> 2 DataRows + PortalSuspended ('s') + ReadyForQuery('T')
        let f1 = read_until_sync_or_query(&mut sock).await;
        let tags_f1: Vec<u8> = f1.iter().map(|f| f.0).collect();
        assert_eq!(tags_f1, vec![b'E', b'S']);
        let mut rf1 = Vec::new();
        rf1.extend_from_slice(&data_row_text("10"));
        rf1.extend_from_slice(&data_row_text("20"));
        rf1.extend_from_slice(&frame(b's', &[]));
        rf1.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&rf1).await.unwrap();

        // 4. Second fetch(2) -> Execute(portal, 2) + Sync -> 1 DataRow + CommandComplete("SELECT 1") + ReadyForQuery('T')
        let f2 = read_until_sync_or_query(&mut sock).await;
        let tags_f2: Vec<u8> = f2.iter().map(|f| f.0).collect();
        assert_eq!(tags_f2, vec![b'E', b'S']);
        let mut rf2 = Vec::new();
        rf2.extend_from_slice(&data_row_text("30"));
        rf2.extend_from_slice(&cmd_complete("SELECT 1"));
        rf2.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&rf2).await.unwrap();

        // 5. Exhausted portal sends Close(P) + Sync, then COMMIT ('Q') for its owned ExplicitTransaction!
        let close_frames = read_until_sync_or_query(&mut sock).await;
        let close_tags: Vec<u8> = close_frames.iter().map(|f| f.0).collect();
        assert_eq!(close_tags, vec![b'C', b'S']);
        let mut rc = Vec::new();
        rc.extend_from_slice(&frame(b'3', &[]));
        rc.extend_from_slice(&frame(b'Z', b"T"));
        sock.write_all(&rc).await.unwrap();

        let commit_frames = read_until_sync_or_query(&mut sock).await;
        assert_eq!(commit_frames[0], (b'Q', b"COMMIT\0".to_vec()));
        let mut rcm = Vec::new();
        rcm.extend_from_slice(&cmd_complete("COMMIT"));
        rcm.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&rcm).await.unwrap();

        // 6. Now that portal is unpinned, regular simple_query works again!
        let sq = read_until_sync_or_query(&mut sock).await;
        assert_eq!(sq[0], (b'Q', b"SELECT 1\0".to_vec()));
        let mut rsq = Vec::new();
        rsq.extend_from_slice(&cmd_complete("SELECT 1"));
        rsq.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&rsq).await.unwrap();
    });

    let mut handle = loopback_handle(port);
    handle.connect().await.unwrap();
    let session = Arc::new(handle);

    // Zero or negative batch_size rejected before wire I/O
    assert!(matches!(
        session.open_portal("SELECT 1", &[], None, 0).await,
        Err(PostgresError::Interface(_))
    ));

    let mut portal = session
        .open_portal("SELECT n FROM generate_series(1, 3) AS n", &[], None, 2)
        .await
        .unwrap();
    assert_eq!(portal.columns().len(), 1);
    assert_eq!(portal.columns()[0].name, "n");
    assert!(!portal.is_exhausted());
    assert!(!portal.is_closed());

    // Sibling queries while portal is open fail with InterfaceError
    assert!(matches!(
        session.simple_query_shared("SELECT 1").await,
        Err(PostgresError::Interface(_))
    ));

    // Zero or negative max_rows rejected
    assert!(matches!(
        portal.fetch(0).await,
        Err(PostgresError::Interface(_))
    ));
    assert!(matches!(
        portal.fetch(-5).await,
        Err(PostgresError::Interface(_))
    ));

    // First chunk via PortalEvent state machine
    portal.start_fetch(2).await.unwrap();
    let ev1 = portal.next_event().await.unwrap();
    assert_eq!(ev1, PortalEvent::Row(vec![PgValue::Int4(10)]));
    let ev2 = portal.next_event().await.unwrap();
    assert_eq!(ev2, PortalEvent::Row(vec![PgValue::Int4(20)]));
    let ev3 = portal.next_event().await.unwrap();
    assert_eq!(ev3, PortalEvent::Suspended);
    assert!(!portal.is_exhausted());

    // Second chunk via high-level fetch(2)
    let chunk2 = portal.fetch(2).await.unwrap();
    assert_eq!(chunk2.rows.len(), 1);
    assert_eq!(chunk2.rows[0], vec![PgValue::Int4(30)]);
    assert_eq!(chunk2.affected_rows, 1);
    assert_eq!(chunk2.command_tag, "SELECT 1");
    assert!(portal.is_exhausted());
    assert!(portal.is_closed());

    // Subsequent fetch on closed/exhausted portal fails locally with InterfaceError and 0 wire traffic
    assert!(matches!(
        portal.fetch(2).await,
        Err(PostgresError::Interface(_))
    ));

    // Session is unpinned and Idle
    session.simple_query_shared("SELECT 1").await.unwrap();

    server.await.unwrap();
}
