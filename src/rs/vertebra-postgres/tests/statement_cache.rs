//! Integration tests for bounded LRU prepared-statement cache, lazy `Close('S')`
//! piggybacking, `0A000`/`XX000` invalidation, and transaction/statement pooler auto-detection (`ch13:t03`).

extern crate _postgres as vertebra_postgres;

use std::collections::BTreeMap;

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::codecs::INT4_OID;
use vertebra_postgres::config::RawPostgresOptions;
use vertebra_postgres::extended::{Parameter, PreparedDescription};
use vertebra_postgres::result::PgValue;
use vertebra_postgres::session::{
    SessionHandle,
    statements::{
        PoolerCapabilities, PoolerMode, StatementCache, StatementKey, is_pooler_banner_parameter,
        is_statement_invalidation_sqlstate,
    },
};

fn frame(tag: u8, body: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + body.len());
    out.push(tag);
    out.extend_from_slice(&((body.len() + 4) as i32).to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn auth_ok_and_ready(extra_params: &[(&str, &str)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&frame(b'R', &0i32.to_be_bytes()));
    out.extend_from_slice(&frame(b'S', b"client_encoding\0UTF8\0"));
    out.extend_from_slice(&frame(b'S', b"DateStyle\0ISO, MDY\0"));
    out.extend_from_slice(&frame(b'S', b"TimeZone\0UTC\0"));
    for (k, v) in extra_params {
        let mut p = Vec::new();
        p.extend_from_slice(k.as_bytes());
        p.push(0);
        p.extend_from_slice(v.as_bytes());
        p.push(0);
        out.extend_from_slice(&frame(b'S', &p));
    }
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn param_desc(oids: &[u32]) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(oids.len() as i16);
    for &oid in oids {
        body.put_u32(oid);
    }
    frame(b't', &body)
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

fn data_row_int4_bin(val: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(1);
    body.put_i32(4);
    body.put_i32(val);
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

async fn read_until_sync(sock: &mut tokio::net::TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut frames = Vec::new();
    loop {
        let mut hdr = [0u8; 5];
        sock.read_exact(&mut hdr).await.unwrap();
        let tag = hdr[0];
        let len = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        frames.push((tag, body));
        if tag == b'S' {
            break;
        }
    }
    frames
}

fn loopback_handle(port: u16, capacity: usize, pooler_mode: &str) -> SessionHandle {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.statement_cache_size = capacity as i64;
    raw.pooler_mode = pooler_mode.to_string();
    SessionHandle::new(raw).unwrap()
}

#[test]
fn lru_eviction_pooler_detection_and_sqlstate_invalidation_unit() {
    assert!(is_pooler_banner_parameter("pgbouncer.version"));
    assert!(is_pooler_banner_parameter("pgbouncer.pool_mode"));
    assert!(is_pooler_banner_parameter(
        "pgbouncer.max_prepared_statements"
    ));
    assert!(!is_pooler_banner_parameter("server_version"));

    assert!(is_statement_invalidation_sqlstate("0A000"));
    assert!(is_statement_invalidation_sqlstate("26000"));
    assert!(is_statement_invalidation_sqlstate("XX000"));
    assert!(!is_statement_invalidation_sqlstate("23505"));

    let empty_params = BTreeMap::new();
    let caps_tx = PoolerCapabilities::resolve(PoolerMode::Transaction, 128, false, &empty_params);
    assert_eq!(caps_tx.effective_capacity, 0);
    assert!(caps_tx.supports_scoped_state());

    let caps_stmt = PoolerCapabilities::resolve(PoolerMode::Statement, 128, true, &empty_params);
    assert_eq!(caps_stmt.effective_capacity, 0);
    assert!(!caps_stmt.supports_scoped_state());

    let mut bouncer_params = BTreeMap::new();
    bouncer_params.insert("pgbouncer.pool_mode".to_string(), "transaction".to_string());
    let caps_auto = PoolerCapabilities::resolve(PoolerMode::Auto, 128, false, &bouncer_params);
    assert_eq!(caps_auto.effective_mode, PoolerMode::Transaction);
    assert_eq!(caps_auto.effective_capacity, 0);

    let cache = StatementCache::new(2);
    assert_eq!(cache.capacity(), 2);
    let k1 = StatementKey::new("SELECT $1", vec![INT4_OID], 0);
    let k2 = StatementKey::new("SELECT $1", vec![25], 0);
    assert_ne!(k1, k2);
}

#[tokio::test]
async fn named_statement_cache_hit_skips_parse_and_evicts_lru_with_close_frame() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready(&[])).await.unwrap();

        // 1. Cold query 1 ("SELECT $1::int4") -> Parse("vtb_s_1") + Describe(S) + Bind + Describe(P) + Execute + Close(P) + Sync
        let f1 = read_until_sync(&mut sock).await;
        let tags1: Vec<u8> = f1.iter().map(|f| f.0).collect();
        assert_eq!(tags1, vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']);
        assert!(f1[0].1.starts_with(b"vtb_s_1\0"));
        let mut r1 = Vec::new();
        r1.extend_from_slice(&frame(b'1', &[]));
        r1.extend_from_slice(&param_desc(&[INT4_OID]));
        r1.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r1.extend_from_slice(&frame(b'2', &[]));
        r1.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r1.extend_from_slice(&data_row_text("7"));
        r1.extend_from_slice(&cmd_complete("SELECT 1"));
        r1.extend_from_slice(&frame(b'3', &[]));
        r1.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r1).await.unwrap();

        // 2. Warm query 1 -> Bind("vtb_s_1") + Execute + Sync (NO Parse, Describe, or Close!)
        let f2 = read_until_sync(&mut sock).await;
        let tags2: Vec<u8> = f2.iter().map(|f| f.0).collect();
        assert_eq!(tags2, vec![b'B', b'E', b'S']);
        let bind_body = &f2[0].1;
        let stmt_end = 1 + bind_body[1..].iter().position(|&b| b == 0).unwrap() + 1;
        let param_fmt_count = i16::from_be_bytes([bind_body[stmt_end], bind_body[stmt_end + 1]]);
        assert_eq!(param_fmt_count, 1);
        let first_param_fmt =
            i16::from_be_bytes([bind_body[stmt_end + 2], bind_body[stmt_end + 3]]);
        assert_eq!(first_param_fmt, 1);
        let param_val_count_pos = stmt_end + 4;
        let param_val_count = i16::from_be_bytes([
            bind_body[param_val_count_pos],
            bind_body[param_val_count_pos + 1],
        ]);
        assert_eq!(param_val_count, 1);
        let param_len = i32::from_be_bytes([
            bind_body[param_val_count_pos + 2],
            bind_body[param_val_count_pos + 3],
            bind_body[param_val_count_pos + 4],
            bind_body[param_val_count_pos + 5],
        ]) as usize;
        let res_fmt_pos = param_val_count_pos + 6 + param_len;
        let res_fmt_count =
            i16::from_be_bytes([bind_body[res_fmt_pos], bind_body[res_fmt_pos + 1]]);
        assert_eq!(res_fmt_count, 1);
        let first_res_fmt =
            i16::from_be_bytes([bind_body[res_fmt_pos + 2], bind_body[res_fmt_pos + 3]]);
        assert_eq!(first_res_fmt, 1);
        let mut r2 = Vec::new();
        r2.extend_from_slice(&frame(b'2', &[]));
        r2.extend_from_slice(&data_row_int4_bin(42));
        r2.extend_from_slice(&cmd_complete("SELECT 1"));
        r2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r2).await.unwrap();

        // 3. Cold query 2 ("SELECT $1::int4 + 1") with capacity=1:
        // Parse("vtb_s_2") inserts and evicts "vtb_s_1" for the FOLLOWING query!
        let f3 = read_until_sync(&mut sock).await;
        let tags3: Vec<u8> = f3.iter().map(|f| f.0).collect();
        assert_eq!(tags3, vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']);
        assert!(f3[0].1.starts_with(b"vtb_s_2\0"));
        let mut r3 = Vec::new();
        r3.extend_from_slice(&frame(b'1', &[]));
        r3.extend_from_slice(&param_desc(&[INT4_OID]));
        r3.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r3.extend_from_slice(&frame(b'2', &[]));
        r3.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r3.extend_from_slice(&data_row_text("99"));
        r3.extend_from_slice(&cmd_complete("SELECT 1"));
        r3.extend_from_slice(&frame(b'3', &[]));
        r3.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r3).await.unwrap();

        // 4. Warm query 2 piggybacks Close('S', "vtb_s_1") before Bind("vtb_s_2")!
        let f4 = read_until_sync(&mut sock).await;
        let tags4: Vec<u8> = f4.iter().map(|f| f.0).collect();
        assert_eq!(tags4, vec![b'C', b'B', b'E', b'S']);
        assert_eq!(&f4[0].1, b"Svtb_s_1\0");
        let mut r4 = Vec::new();
        r4.extend_from_slice(&frame(b'3', &[]));
        r4.extend_from_slice(&frame(b'2', &[]));
        r4.extend_from_slice(&data_row_int4_bin(100));
        r4.extend_from_slice(&cmd_complete("SELECT 1"));
        r4.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r4).await.unwrap();
    });

    let mut handle = loopback_handle(port, 1, "session");
    handle.connect().await.unwrap();

    let res1 = handle
        .query_shared("SELECT $1::int4", &[Parameter::new(PgValue::Int4(7))], None)
        .await
        .unwrap();
    assert_eq!(res1.rows[0][0], PgValue::Int4(7));

    let res2 = handle
        .query_shared(
            "SELECT $1::int4",
            &[Parameter::new(PgValue::Int4(42))],
            None,
        )
        .await
        .unwrap();
    assert_eq!(res2.rows[0][0], PgValue::Int4(42));

    let res3 = handle
        .query_shared(
            "SELECT $1::int4 + 1",
            &[Parameter::new(PgValue::Int4(98))],
            None,
        )
        .await
        .unwrap();
    assert_eq!(res3.rows[0][0], PgValue::Int4(99));

    let res4 = handle
        .query_shared(
            "SELECT $1::int4 + 1",
            &[Parameter::new(PgValue::Int4(99))],
            None,
        )
        .await
        .unwrap();
    assert_eq!(res4.rows[0][0], PgValue::Int4(100));

    server.await.unwrap();
}

fn empty_description(param_oids: &[u32]) -> PreparedDescription {
    PreparedDescription {
        parameter_oids: param_oids.to_vec(),
        result_columns: Vec::new(),
    }
}

#[test]
fn statement_cache_disabled_zero_capacity_behaviour() {
    let mut cache = StatementCache::new(0);
    assert_eq!(cache.capacity(), 0);
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
    assert!(!cache.contains("SELECT 1", &[]));
    assert!(cache.get("SELECT 1", &[]).is_none());
    assert!(
        cache
            .insert("SELECT 1".to_string(), vec![], empty_description(&[]))
            .is_none()
    );
    assert!(cache.take_evicted_names().is_empty());
    assert_eq!(cache.len(), 0);
}

#[test]
fn statement_cache_o1_lru_eviction_promotion_and_slot_recycling() {
    let mut cache = StatementCache::new(3);
    let s1 = cache
        .insert("SELECT 1".to_string(), vec![], empty_description(&[]))
        .unwrap();
    let s2 = cache
        .insert(
            "SELECT $1::int4".to_string(),
            vec![INT4_OID],
            empty_description(&[INT4_OID]),
        )
        .unwrap();
    let s3 = cache
        .insert("SELECT 3".to_string(), vec![], empty_description(&[]))
        .unwrap();
    assert_eq!(cache.len(), 3);
    assert!(cache.contains("SELECT 1", &[]));

    // Promote s1 (head) to MRU tail, making s2 the LRU head.
    assert_eq!(cache.get("SELECT 1", &[]).unwrap().name, s1.name);
    // Re-inserting s2 updates it and promotes it to MRU tail, making s3 the LRU head.
    let s2_updated = cache
        .insert_named(
            "vtb_s_custom".to_string(),
            "SELECT $1::int4".to_string(),
            vec![INT4_OID],
            empty_description(&[INT4_OID]),
        )
        .unwrap();
    assert_eq!(s2_updated.name, "vtb_s_custom");
    assert_eq!(cache.take_evicted_names(), vec![s2.name.clone()]);
    assert_eq!(cache.len(), 3);

    // Inserting s4 must evict s3 (now LRU head).
    let s4 = cache
        .insert("SELECT 4".to_string(), vec![], empty_description(&[]))
        .unwrap();
    assert_eq!(cache.take_evicted_names(), vec![s3.name]);
    assert!(!cache.contains("SELECT 3", &[]));
    assert!(cache.contains("SELECT 1", &[]));
    assert!(cache.contains("SELECT $1::int4", &[INT4_OID]));
    assert!(cache.contains("SELECT 4", &[]));
    assert_eq!(cache.get("SELECT 4", &[]).unwrap().name, s4.name);
}

#[test]
fn statement_cache_unlink_head_middle_tail_and_clear() {
    let mut cache = StatementCache::new(4);
    let generation = cache.schema_generation();
    let s1 = cache
        .insert("Q1".to_string(), vec![], empty_description(&[]))
        .unwrap();
    let s2 = cache
        .insert("Q2".to_string(), vec![], empty_description(&[]))
        .unwrap();
    let s3 = cache
        .insert("Q3".to_string(), vec![], empty_description(&[]))
        .unwrap();
    let s4 = cache
        .insert("Q4".to_string(), vec![], empty_description(&[]))
        .unwrap();
    assert_eq!(cache.len(), 4);

    // Unlink middle node (Q2).
    let k2 = StatementKey::new("Q2", vec![], generation);
    assert_eq!(cache.invalidate_key(&k2).unwrap().name, s2.name);
    assert!(cache.invalidate_key(&k2).is_none());
    assert_eq!(cache.len(), 3);

    // Unlink head node (Q1).
    assert_eq!(cache.remove("Q1", &[]).unwrap().name, s1.name);
    assert_eq!(cache.len(), 2);

    // Unlink tail node (Q4).
    let k4 = StatementKey::new("Q4", vec![], generation);
    assert_eq!(cache.invalidate_key(&k4).unwrap().name, s4.name);
    assert_eq!(cache.len(), 1);
    assert!(cache.contains("Q3", &[]));

    // Re-insert into recycled slots and verify eviction order still starts at Q3.
    let s5 = cache
        .insert("Q5".to_string(), vec![], empty_description(&[]))
        .unwrap();
    let s6 = cache
        .insert("Q6".to_string(), vec![], empty_description(&[]))
        .unwrap();
    let _s7 = cache
        .insert("Q7".to_string(), vec![], empty_description(&[]))
        .unwrap();
    assert_eq!(cache.len(), 4);
    let _s8 = cache
        .insert("Q8".to_string(), vec![], empty_description(&[]))
        .unwrap();
    assert_eq!(cache.take_evicted_names(), vec![s3.name]);

    // Evict by name from interior.
    cache.evict_by_name(&s6.name, true);
    assert_eq!(cache.take_evicted_names(), vec![s6.name]);
    assert!(!cache.contains("Q6", &[]));
    assert!(cache.contains("Q5", &[]));

    // Clear resets cache and advances generation.
    cache.clear();
    assert_eq!(cache.len(), 0);
    assert!(cache.is_empty());
    assert_ne!(cache.schema_generation(), generation);
    assert!(!cache.contains("Q5", &[]));
    assert!(
        cache
            .get_by_key(&StatementKey::new("Q5", vec![], generation))
            .is_none()
    );
    assert!(cache.get("Q5", &[]).is_none());
    let _ = s5;
}

fn error_response(sqlstate: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    body.push(b'S');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'V');
    body.extend_from_slice(b"ERROR\0");
    body.push(b'C');
    body.extend_from_slice(sqlstate.as_bytes());
    body.push(0);
    body.push(b'M');
    body.extend_from_slice(message.as_bytes());
    body.push(0);
    body.push(0);
    frame(b'E', &body)
}

#[tokio::test]
async fn cached_statement_without_result_set_sends_bes_and_has_no_columns() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready(&[])).await.unwrap();

        let f1 = read_until_sync(&mut sock).await;
        let tags1: Vec<u8> = f1.iter().map(|f| f.0).collect();
        assert_eq!(tags1, vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']);
        let mut r1 = Vec::new();
        r1.extend_from_slice(&frame(b'1', &[]));
        r1.extend_from_slice(&param_desc(&[INT4_OID]));
        r1.extend_from_slice(&frame(b'n', &[]));
        r1.extend_from_slice(&frame(b'2', &[]));
        r1.extend_from_slice(&frame(b'n', &[]));
        r1.extend_from_slice(&cmd_complete("INSERT 0 1"));
        r1.extend_from_slice(&frame(b'3', &[]));
        r1.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r1).await.unwrap();

        let f2 = read_until_sync(&mut sock).await;
        let tags2: Vec<u8> = f2.iter().map(|f| f.0).collect();
        assert_eq!(tags2, vec![b'B', b'E', b'S']);
        let mut r2 = Vec::new();
        r2.extend_from_slice(&frame(b'2', &[]));
        r2.extend_from_slice(&cmd_complete("INSERT 0 1"));
        r2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r2).await.unwrap();
    });

    let mut handle = loopback_handle(port, 4, "session");
    handle.connect().await.unwrap();

    let res1 = handle
        .query_shared(
            "INSERT INTO t(x) VALUES ($1)",
            &[Parameter::new(PgValue::Int4(1))],
            None,
        )
        .await
        .unwrap();
    assert_eq!(res1.affected_rows, 1);
    assert!(res1.columns.is_empty());

    let res2 = handle
        .query_shared(
            "INSERT INTO t(x) VALUES ($1)",
            &[Parameter::new(PgValue::Int4(2))],
            None,
        )
        .await
        .unwrap();
    assert_eq!(res2.affected_rows, 1);
    assert!(res2.columns.is_empty());

    server.await.unwrap();
}

#[tokio::test]
async fn cached_statement_rejects_unrequested_description_no_data_and_uncounted_close_complete() {
    for (case_name, unexpected_frame) in [
        ("RowDescription", row_desc(&[("v", INT4_OID, 1)])),
        ("NoData", frame(b'n', &[])),
        ("CloseComplete", frame(b'3', &[])),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            read_startup(&mut sock).await;
            sock.write_all(&auth_ok_and_ready(&[])).await.unwrap();

            let f1 = read_until_sync(&mut sock).await;
            let tags1: Vec<u8> = f1.iter().map(|f| f.0).collect();
            assert_eq!(tags1, vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']);
            let mut r1 = Vec::new();
            r1.extend_from_slice(&frame(b'1', &[]));
            r1.extend_from_slice(&param_desc(&[INT4_OID]));
            r1.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
            r1.extend_from_slice(&frame(b'2', &[]));
            r1.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
            r1.extend_from_slice(&data_row_text("7"));
            r1.extend_from_slice(&cmd_complete("SELECT 1"));
            r1.extend_from_slice(&frame(b'3', &[]));
            r1.extend_from_slice(&frame(b'Z', b"I"));
            sock.write_all(&r1).await.unwrap();

            let f2 = read_until_sync(&mut sock).await;
            let tags2: Vec<u8> = f2.iter().map(|f| f.0).collect();
            assert_eq!(tags2, vec![b'B', b'E', b'S']);
            let mut r2 = Vec::new();
            r2.extend_from_slice(&frame(b'2', &[]));
            r2.extend_from_slice(&unexpected_frame);
            r2.extend_from_slice(&data_row_int4_bin(42));
            r2.extend_from_slice(&cmd_complete("SELECT 1"));
            r2.extend_from_slice(&frame(b'Z', b"I"));
            let _ = sock.write_all(&r2).await;
        });

        let mut handle = loopback_handle(port, 4, "session");
        handle.connect().await.unwrap();

        handle
            .query_shared("SELECT $1::int4", &[Parameter::new(PgValue::Int4(7))], None)
            .await
            .unwrap();

        let err = handle
            .query_shared(
                "SELECT $1::int4",
                &[Parameter::new(PgValue::Int4(42))],
                None,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(err, vertebra_postgres::errors::PostgresError::Protocol(_)),
            "expected Protocol error for {case_name}, got {err:?}"
        );

        let closed_err = handle
            .query_shared("SELECT 1", &[], None)
            .await
            .unwrap_err();
        assert!(
            matches!(
                closed_err,
                vertebra_postgres::errors::PostgresError::Interface(_)
            ),
            "connection must be closed after {case_name}, got {closed_err:?}"
        );

        server.await.unwrap();
    }
}

#[tokio::test]
async fn cached_statement_stale_description_0a000_invalidates_and_reparses() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        read_startup(&mut sock).await;
        sock.write_all(&auth_ok_and_ready(&[])).await.unwrap();

        let f1 = read_until_sync(&mut sock).await;
        assert_eq!(
            f1.iter().map(|f| f.0).collect::<Vec<u8>>(),
            vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']
        );
        let mut r1 = Vec::new();
        r1.extend_from_slice(&frame(b'1', &[]));
        r1.extend_from_slice(&param_desc(&[]));
        r1.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r1.extend_from_slice(&frame(b'2', &[]));
        r1.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r1.extend_from_slice(&data_row_text("1"));
        r1.extend_from_slice(&cmd_complete("SELECT 1"));
        r1.extend_from_slice(&frame(b'3', &[]));
        r1.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r1).await.unwrap();

        let f2 = read_until_sync(&mut sock).await;
        assert_eq!(
            f2.iter().map(|f| f.0).collect::<Vec<u8>>(),
            vec![b'B', b'E', b'S']
        );
        let mut r2 = Vec::new();
        r2.extend_from_slice(&error_response(
            "0A000",
            "cached plan must not change result type",
        ));
        r2.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r2).await.unwrap();

        let f3 = read_until_sync(&mut sock).await;
        let tags3: Vec<u8> = f3.iter().map(|f| f.0).collect();
        assert_eq!(tags3, vec![b'P', b'D', b'B', b'D', b'E', b'C', b'S']);
        let mut r3 = Vec::new();
        r3.extend_from_slice(&frame(b'1', &[]));
        r3.extend_from_slice(&param_desc(&[]));
        r3.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r3.extend_from_slice(&frame(b'2', &[]));
        r3.extend_from_slice(&row_desc(&[("v", INT4_OID, 0)]));
        r3.extend_from_slice(&data_row_text("2"));
        r3.extend_from_slice(&cmd_complete("SELECT 1"));
        r3.extend_from_slice(&frame(b'3', &[]));
        r3.extend_from_slice(&frame(b'Z', b"I"));
        sock.write_all(&r3).await.unwrap();
    });

    let mut handle = loopback_handle(port, 4, "session");
    handle.connect().await.unwrap();

    let res1 = handle
        .query_shared("SELECT v FROM t", &[], None)
        .await
        .unwrap();
    assert_eq!(res1.rows[0][0], PgValue::Int4(1));

    let err = handle
        .query_shared("SELECT v FROM t", &[], None)
        .await
        .unwrap_err();
    assert_eq!(err.sqlstate(), Some("0A000"));

    let res3 = handle
        .query_shared("SELECT v FROM t", &[], None)
        .await
        .unwrap();
    assert_eq!(res3.rows[0][0], PgValue::Int4(2));

    server.await.unwrap();
}
