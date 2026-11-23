//! Integration tests for `vertebra-redis` configuration, error mapping, caching, and topologies.

extern crate _redis as vertebra_redis;

use std::sync::Arc;

use bytes::Bytes;
use vertebra_redis::client::RespEngine;
use vertebra_redis::cluster::ClusterTopology;
use vertebra_redis::codec::RespValue;
use vertebra_redis::config::{RedisTopologyMode, RespConfig, normalize_redis_url};
use vertebra_redis::error::{RespErrorKind, classify_server_error};
use vertebra_redis::sentinel::parse_get_master_addr_reply;
use vertebra_redis::tracking::TrackingCache;

fn build_cfg(
    url: &str,
    topology: RedisTopologyMode,
) -> Result<RespConfig, vertebra_redis::error::RespError> {
    RespConfig::build_redis(
        url,
        None,
        None,
        "vertebra".to_string(),
        3,
        topology,
        None,
        Vec::new(),
        Vec::new(),
        5,
        false,
        Vec::new(),
        4096,
        None,
        false,
        None,
        32,
    )
}

#[test]
fn test_url_normalization() {
    assert_eq!(
        normalize_redis_url("redis://127.0.0.1:6379/0").unwrap(),
        "redis://127.0.0.1:6379/0"
    );
    assert_eq!(
        normalize_redis_url("rediss://secure.redis.cache.windows.net:6380/1").unwrap(),
        "rediss://secure.redis.cache.windows.net:6380/1"
    );
    assert!(
        normalize_redis_url("unix:///var/run/redis.sock")
            .unwrap()
            .starts_with("redis+unix:")
    );

    let err_valkey = normalize_redis_url("valkey://127.0.0.1:6379").unwrap_err();
    assert!(err_valkey.message.contains("vertebra._valkey"));

    let err_valkeys = normalize_redis_url("valkeys://127.0.0.1:6379").unwrap_err();
    assert!(err_valkeys.message.contains("vertebra._valkey"));

    let err_mem = normalize_redis_url("memory://").unwrap_err();
    assert!(err_mem.message.contains("In-memory scheme"));

    let err_http = normalize_redis_url("http://127.0.0.1:6379").unwrap_err();
    assert!(err_http.message.contains("Unsupported Redis URL scheme"));
}

#[test]
fn test_topology_validation() {
    assert!(build_cfg("unix:///var/run/redis.sock", RedisTopologyMode::Standalone).is_ok());

    let err = build_cfg("unix:///var/run/redis.sock", RedisTopologyMode::Cluster).unwrap_err();
    assert!(
        err.message
            .contains("Unix domain socket URLs cannot be used with cluster")
    );

    let err2 = build_cfg("unix:///var/run/redis.sock", RedisTopologyMode::Sentinel).unwrap_err();
    assert!(
        err2.message
            .contains("Unix domain socket URLs cannot be used with cluster or sentinel")
    );
}

#[test]
fn test_error_classification() {
    let cases = [
        ("NOAUTH Authentication required.", true, false),
        ("WRONGPASS invalid username-password pair", true, false),
        (
            "CROSSSLOT Keys in request don't hash to the same slot",
            false,
            false,
        ),
        ("LOADING Redis is loading the dataset in memory", true, true),
        ("CLUSTERDOWN The cluster is down", true, true),
        (
            "NOSCRIPT No matching script. Please use EVAL.",
            false,
            false,
        ),
    ];
    for (msg, expect_operational, expect_transient) in cases {
        let kind = classify_server_error(msg);
        assert_eq!(kind.is_transient(), expect_transient, "msg={msg}");
        if expect_operational {
            assert!(
                matches!(kind, RespErrorKind::Operational { .. }),
                "msg={msg} got {kind:?}"
            );
        } else {
            assert!(
                matches!(kind, RespErrorKind::Programming),
                "msg={msg} got {kind:?}"
            );
        }
    }
}

#[test]
fn test_client_lru_caching() {
    let cache = TrackingCache::new(2, None);
    cache.insert(Bytes::from_static(b"k1"), Bytes::from_static(b"val1"));
    cache.insert(Bytes::from_static(b"k2"), Bytes::from_static(b"val2"));

    assert_eq!(cache.get(b"k1"), Some(Bytes::from_static(b"val1")));
    assert_eq!(cache.get(b"k2"), Some(Bytes::from_static(b"val2")));

    cache.insert(Bytes::from_static(b"k3"), Bytes::from_static(b"val3"));
    assert_eq!(cache.get(b"k3"), Some(Bytes::from_static(b"val3")));

    cache.invalidate_key(Some(b"k2"));
    assert_eq!(cache.get(b"k2"), None);
    assert_eq!(cache.drain_invalidations(), vec!["k2".to_string()]);

    cache.insert(Bytes::from_static(b"k4"), Bytes::from_static(b"val4"));
    cache.invalidate_key(None);
    assert_eq!(cache.get(b"k4"), None);
    assert!(!cache.drain_invalidations().is_empty());
}

#[test]
fn test_cluster_redirect_parsing() {
    let moved = vertebra_resp::parse_cluster_redirect("MOVED 1234 10.0.0.1:6379");
    assert_eq!(
        moved,
        Some(("MOVED".to_string(), 1234, "10.0.0.1".to_string(), 6379))
    );

    let ask = vertebra_resp::parse_cluster_redirect("ASK 5678 10.0.0.2:6379");
    assert_eq!(
        ask,
        Some(("ASK".to_string(), 5678, "10.0.0.2".to_string(), 6379))
    );

    let mut topo = ClusterTopology::empty();
    topo.update_moved_slot(1234, "10.0.0.1", 6379);
    let slot_map = topo.slot_map();
    assert_eq!(slot_map.get(&1234), Some(&("10.0.0.1".to_string(), 6379)));
    assert_eq!(slot_map.get(&9999), None);
}

#[test]
fn test_sentinel_parsing() {
    let response = RespValue::Array(vec![
        RespValue::BulkString(Bytes::from_static(b"192.168.1.100")),
        RespValue::Integer(6379),
    ]);
    let (host, port) = parse_get_master_addr_reply(&response).unwrap();
    assert_eq!(host, "192.168.1.100");
    assert_eq!(port, 6379);
}

#[test]
fn test_lazy_client_creation_and_close() {
    let cfg = build_cfg("redis://127.0.0.1:6379/0", RedisTopologyMode::Standalone).unwrap();
    let engine = RespEngine::new(Arc::new(cfg));
    assert!(!engine.is_closed());
    engine.close_sync();
    assert!(engine.is_closed());
}

#[test]
fn test_client_lru_invalidations_capped_at_4096() {
    let cache = TrackingCache::new(16, None);
    let keys: Vec<Vec<u8>> = (0..5000).map(|i| format!("inv:{i}").into_bytes()).collect();
    cache.invalidate_keys(Some(&keys));
    let drained = cache.drain_invalidations();
    assert_eq!(drained.len(), 4096);
    assert_eq!(drained.first().map(String::as_str), Some("inv:904"));
    assert_eq!(drained.last().map(String::as_str), Some("inv:4999"));
}

#[tokio::test]
async fn test_client_tracking_handshake_sentinel_blocking_xreadgroup_and_close() {
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    let commands_seen: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));

    fn pop_resp_cmd(buf: &mut Vec<u8>) -> Option<Vec<String>> {
        if buf.is_empty() || buf[0] != b'*' {
            return None;
        }
        let line_end = buf.windows(2).position(|w| w == b"\r\n")?;
        let argc: usize = std::str::from_utf8(&buf[1..line_end]).ok()?.parse().ok()?;
        let mut pos = line_end + 2;
        let mut parts = Vec::with_capacity(argc);
        for _ in 0..argc {
            if pos >= buf.len() || buf[pos] != b'$' {
                return None;
            }
            let rel_end = buf[pos..].windows(2).position(|w| w == b"\r\n")?;
            let blen: usize = std::str::from_utf8(&buf[pos + 1..pos + rel_end])
                .ok()?
                .parse()
                .ok()?;
            let data_start = pos + rel_end + 2;
            let data_end = data_start + blen;
            if data_end + 2 > buf.len() {
                return None;
            }
            parts.push(String::from_utf8_lossy(&buf[data_start..data_end]).into_owned());
            pos = data_end + 2;
        }
        buf.drain(..pos);
        Some(parts)
    }

    let master_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let master_port = master_listener.local_addr().unwrap().port();
    let seen_clone = Arc::clone(&commands_seen);
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = master_listener.accept().await {
            let seen = Arc::clone(&seen_clone);
            tokio::spawn(async move {
                let mut acc = Vec::new();
                let mut tmp = [0u8; 4096];
                while let Ok(n) = sock.read(&mut tmp).await {
                    if n == 0 {
                        break;
                    }
                    acc.extend_from_slice(&tmp[..n]);
                    while let Some(parts) = pop_resp_cmd(&mut acc) {
                        seen.lock().unwrap().push(parts.join(" "));
                        let op = parts
                            .first()
                            .map(|s| s.to_ascii_uppercase())
                            .unwrap_or_default();
                        if op == "HELLO" {
                            let _ = sock
                                .write_all(b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n")
                                .await;
                        } else if op == "ROLE" {
                            let _ = sock.write_all(b"*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n").await;
                        } else if op == "XREADGROUP" {
                            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            let _ = sock.write_all(b"_\r\n").await;
                        } else if op == "GET" {
                            let _ = sock.write_all(b"$5\r\nhello\r\n").await;
                        } else {
                            let _ = sock.write_all(b"+OK\r\n").await;
                        }
                    }
                }
            });
        }
    });

    let sentinel_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let sentinel_port = sentinel_listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = sentinel_listener.accept().await {
            tokio::spawn(async move {
                let mut acc = Vec::new();
                let mut tmp = [0u8; 4096];
                while let Ok(n) = sock.read(&mut tmp).await {
                    if n == 0 {
                        break;
                    }
                    acc.extend_from_slice(&tmp[..n]);
                    while let Some(parts) = pop_resp_cmd(&mut acc) {
                        let op = parts
                            .first()
                            .map(|s| s.to_ascii_uppercase())
                            .unwrap_or_default();
                        if op == "HELLO" {
                            let _ = sock
                                .write_all(b"%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n")
                                .await;
                        } else if op == "CLIENT" {
                            let _ = sock.write_all(b"+OK\r\n").await;
                        } else if op == "SENTINEL" {
                            let p = master_port.to_string();
                            let reply = format!("*2\r\n$9\r\n127.0.0.1\r\n${}\r\n{p}\r\n", p.len());
                            let _ = sock.write_all(reply.as_bytes()).await;
                        } else {
                            let _ = sock
                                .write_all(b"-ERR unsupported in mock sentinel\r\n")
                                .await;
                        }
                    }
                }
            });
        }
    });

    let cfg = RespConfig::build_redis(
        &format!("redis://127.0.0.1:{sentinel_port}/0"),
        None,
        None,
        "vertebra".to_string(),
        3,
        RedisTopologyMode::Sentinel,
        Some("mymaster".to_string()),
        vec![("127.0.0.1".to_string(), sentinel_port)],
        Vec::new(),
        5,
        true,
        vec!["user:".to_string()],
        4096,
        None,
        false,
        None,
        32,
    )
    .unwrap();

    let engine = RespEngine::new(Arc::new(cfg));
    let val = engine.get_bytes(b"user:1".to_vec()).await.unwrap();
    assert_eq!(val.as_deref(), Some(&b"hello"[..]));
    assert!(engine.is_caching_active());
    assert!(engine.has_active_connection());

    {
        let recorded = commands_seen
            .lock()
            .unwrap()
            .join("\n")
            .to_ascii_uppercase();
        assert!(
            recorded.contains("TRACKING"),
            "expected CLIENT TRACKING ON: {recorded}"
        );
        assert!(recorded.contains("BCAST"), "expected BCAST: {recorded}");
    }

    let xres = engine
        .xreadgroup(
            "grp",
            "c1",
            &[(b"mystream".to_vec(), ">".to_string())],
            Some(1),
            Some(1700),
            false,
        )
        .await
        .unwrap();
    assert_eq!(xres, RespValue::Null);

    engine.close().await;
    assert!(engine.is_closed());
    assert!(!engine.has_active_connection());
}
