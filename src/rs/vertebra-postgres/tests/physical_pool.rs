//! Integration tests for `PostgresPool` sizing, FIFO waiter handoff, cancellation baton-pass,
//! and idle/lifetime eviction (`ch14:t01`).

extern crate _postgres as vertebra_postgres;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use vertebra_postgres::config::{PostgresConfig, RawPostgresOptions};
use vertebra_postgres::errors::PostgresError;
use vertebra_postgres::pool::{PostgresPool, PostgresPoolOptions};
use vertebra_postgres::session::statements::PoolerMode;

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

async fn serve_reset_capable_backend(listener: TcpListener, accepts: Arc<AtomicUsize>) {
    while let Ok((mut sock, _)) = listener.accept().await {
        accepts.fetch_add(1, Ordering::SeqCst);
        tokio::spawn(async move {
            let mut len_buf = [0u8; 4];
            if sock.read_exact(&mut len_buf).await.is_err() {
                return;
            }
            let len = i32::from_be_bytes(len_buf) as usize;
            let mut body = vec![0u8; len - 4];
            if sock.read_exact(&mut body).await.is_err() {
                return;
            }
            if sock.write_all(&auth_ok_and_ready()).await.is_err() {
                return;
            }
            loop {
                let mut hdr = [0u8; 5];
                if sock.read_exact(&mut hdr).await.is_err() {
                    break;
                }
                let tag = hdr[0];
                let flen = i32::from_be_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
                let mut fbody = vec![0u8; flen - 4];
                if sock.read_exact(&mut fbody).await.is_err() {
                    break;
                }
                if tag == b'X' {
                    break;
                }
                if tag == b'Q' {
                    let mut resp = Vec::new();
                    resp.extend_from_slice(&frame(b'C', b"DISCARD ALL\0"));
                    resp.extend_from_slice(&frame(b'Z', b"I"));
                    let _ = sock.write_all(&resp).await;
                }
            }
        });
    }
}

fn client_config(port: u16) -> PostgresConfig {
    let mut raw = RawPostgresOptions::new("127.0.0.1", "postgres", "appdb");
    raw.port = i64::from(port);
    raw.sslmode = "disable".to_string();
    PostgresConfig::validate(raw).unwrap()
}

#[test]
fn pool_options_validation_rejects_invalid_bounds() {
    let mut bad = PostgresPoolOptions {
        max_size: 0,
        ..Default::default()
    };
    assert!(matches!(bad.validate(), Err(PostgresError::Config(_))));

    bad.max_size = 2;
    bad.min_size = 5;
    assert!(matches!(bad.validate(), Err(PostgresError::Config(_))));

    bad.min_size = 1;
    bad.acquire_timeout = Duration::ZERO;
    assert!(matches!(bad.validate(), Err(PostgresError::Config(_))));
}

#[tokio::test]
async fn pool_zero_network_construction_max_size_fifo_waiters_and_cancel_baton_pass() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let accepts = Arc::new(AtomicUsize::new(0));
    let accepts_bg = Arc::clone(&accepts);
    tokio::spawn(async move {
        serve_reset_capable_backend(listener, accepts_bg).await;
    });

    let cfg = client_config(port);
    let opts = PostgresPoolOptions {
        pooler_mode: PoolerMode::Direct,
        protocol_level_pooler_cache: false,
        statement_cache_size: 64,
        min_size: 1,
        max_size: 1,
        acquire_timeout: Duration::from_millis(500),
        reset_timeout: Duration::from_secs(2),
        close_timeout: Duration::from_secs(2),
        max_idle_lifetime: None,
        max_lifetime: None,
    };

    let pool = PostgresPool::new(cfg, opts).unwrap();
    assert_eq!(accepts.load(Ordering::SeqCst), 0);
    let st0 = pool.stats();
    assert_eq!(st0.connecting + st0.idle + st0.borrowed + st0.resetting, 0);

    let lease1 = pool.acquire().await.unwrap();
    assert_eq!(accepts.load(Ordering::SeqCst), 1);
    assert_eq!(pool.stats().borrowed, 1);

    let pool_w1 = Arc::clone(&pool);
    let w1 = tokio::spawn(async move { pool_w1.acquire().await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    let pool_w2 = Arc::clone(&pool);
    let w2 = tokio::spawn(async move { pool_w2.acquire().await });
    tokio::time::sleep(Duration::from_millis(20)).await;

    w1.abort();
    let _ = w1.await;
    tokio::time::sleep(Duration::from_millis(10)).await;

    lease1.release().await.unwrap();
    let lease2 = w2.await.unwrap().unwrap();
    assert_eq!(accepts.load(Ordering::SeqCst), 1);
    assert_eq!(pool.stats().borrowed, 1);

    lease2.release().await.unwrap();
    assert_eq!(pool.stats().idle, 1);

    pool.close(Duration::from_secs(2)).await.unwrap();
    let st_end = pool.stats();
    assert_eq!(st_end.idle + st_end.borrowed, 0);
    assert!(matches!(
        pool.acquire().await,
        Err(PostgresError::Interface(_))
    ));
}
