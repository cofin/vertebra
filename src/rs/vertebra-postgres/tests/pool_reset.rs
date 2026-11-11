//! Integration tests for pooled connection reset (`ROLLBACK` + `DISCARD ALL` + startup GUC restore),
//! synchronous `Drop` background retirement/reset, and pool `close()` drain (`ch14:t02`).

extern crate _postgres as vertebra_postgres;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::pool::{PostgresPool, PostgresPoolOptions};
use vertebra_postgres::session::SessionHandle;
use vertebra_postgres::session::statements::PoolerMode;
use vertebra_postgres::transaction::TransactionOwner;

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
    out.extend_from_slice(&frame(b'S', b"application_name\0mutated_app\0"));
    out.extend_from_slice(&frame(b'K', &[0, 0, 0, 1, 0, 0, 0, 2]));
    out.extend_from_slice(&frame(b'Z', b"I"));
    out
}

fn client_config(port: u16) -> PostgresConfig {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    raw.application_name = "pool_app".to_string();
    raw.session_gucs
        .insert("search_path".to_string(), "tenant_a".to_string());
    PostgresConfig::validate(raw).unwrap()
}

#[tokio::test]
async fn dirty_transaction_rolls_back_discards_all_and_restores_startup_gucs_on_return() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let recorded_queries: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let rq_bg = Arc::clone(&recorded_queries);

    let server = tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        let mut len_buf = [0u8; 4];
        sock.read_exact(&mut len_buf).await.unwrap();
        let len = i32::from_be_bytes(len_buf) as usize;
        let mut body = vec![0u8; len - 4];
        sock.read_exact(&mut body).await.unwrap();
        sock.write_all(&auth_ok_and_ready()).await.unwrap();

        loop {
            let mut hdr = [0u8; 5];
            if sock.read_exact(&mut hdr).await.is_err() {
                break;
            }
            let tag = hdr[0];
            let flen = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
            let mut fbody = vec![0u8; flen - 4];
            sock.read_exact(&mut fbody).await.unwrap();
            if tag == b'X' {
                break;
            }
            assert_eq!(tag, b'Q');
            fbody.pop();
            let q = String::from_utf8(fbody).unwrap();
            rq_bg.lock().unwrap().push(q.clone());

            let (cmd_tag, status) = if q == "BEGIN" {
                ("BEGIN\0".as_bytes(), b'T')
            } else if q == "ROLLBACK" {
                ("ROLLBACK\0".as_bytes(), b'I')
            } else if q == "DISCARD ALL" {
                ("DISCARD ALL\0".as_bytes(), b'I')
            } else {
                ("SET\0".as_bytes(), b'I')
            };
            let mut resp = Vec::new();
            resp.extend_from_slice(&frame(b'C', cmd_tag));
            resp.extend_from_slice(&frame(b'Z', &[status]));
            sock.write_all(&resp).await.unwrap();
        }
    });

    let cfg = client_config(port);
    let opts = PostgresPoolOptions {
        pooler_mode: PoolerMode::Direct,
        protocol_level_pooler_cache: false,
        statement_cache_size: 32,
        min_size: 1,
        max_size: 1,
        acquire_timeout: Duration::from_secs(2),
        reset_timeout: Duration::from_secs(2),
        close_timeout: Duration::from_secs(2),
        max_idle_lifetime: None,
        max_lifetime: None,
    };

    let pool = PostgresPool::new(cfg, opts).unwrap();
    let lease = pool.acquire().await.unwrap();
    let mut session = SessionHandle::from_pooled(lease);

    let owner = TransactionOwner::new(1, session.generation());
    session.begin_as(owner).await.unwrap();

    session.close().await.unwrap();
    assert_eq!(pool.stats().idle, 1);

    let qs = recorded_queries.lock().unwrap().clone();
    assert_eq!(qs[0], "BEGIN");
    assert_eq!(qs[1], "ROLLBACK");
    assert_eq!(qs[2], "DISCARD ALL");
    assert!(qs[3].contains("SET \"application_name\" = 'pool_app'"));
    assert!(qs[3].contains("SET \"search_path\" = 'tenant_a'"));

    pool.close(Duration::from_secs(2)).await.unwrap();
    server.await.unwrap();
}
