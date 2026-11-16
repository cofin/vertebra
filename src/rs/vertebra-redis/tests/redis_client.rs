//! Integration tests for vertebra-redis configuration, error mapping, codec, and topologies.

extern crate _redis as vertebra_redis;

use redis::{ServerErrorKind, Value};
use vertebra_redis::caching::ClientLruCache;
use vertebra_redis::client_core::RedisCoreClient;
use vertebra_redis::config::{RedisClientConfig, RedisTopologyMode, normalize_redis_url};
use vertebra_redis::errors::{RedisDriverError, classify_redis_error};
use vertebra_redis::topology::{
    ClusterRedirectAction, ClusterSlotMap, check_cluster_redirect, parse_sentinel_master_addr,
};

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

    // Rejection of valkey:// and valkeys://
    let err_valkey = normalize_redis_url("valkey://127.0.0.1:6379").unwrap_err();
    assert!(err_valkey.message().contains("vertebra._valkey"));

    let err_valkeys = normalize_redis_url("valkeys://127.0.0.1:6379").unwrap_err();
    assert!(err_valkeys.message().contains("vertebra._valkey"));

    // Rejection of memory://
    let err_mem = normalize_redis_url("memory://").unwrap_err();
    assert!(err_mem.message().contains("In-memory scheme"));

    // Rejection of invalid scheme
    let err_http = normalize_redis_url("http://127.0.0.1:6379").unwrap_err();
    assert!(err_http.message().contains("Unsupported Redis URL scheme"));
}

#[test]
fn test_topology_validation() {
    let mut cfg = RedisClientConfig::new("unix:///var/run/redis.sock").unwrap();
    assert!(cfg.validate().is_ok());

    cfg.topology = RedisTopologyMode::Cluster;
    let err = cfg.validate().unwrap_err();
    assert!(
        err.message()
            .contains("Unix domain socket URLs cannot be used with cluster")
    );

    cfg.topology = RedisTopologyMode::Sentinel;
    let err2 = cfg.validate().unwrap_err();
    assert!(
        err2.message()
            .contains("Unix domain socket URLs cannot be used with cluster or sentinel")
    );
}

#[test]
fn test_error_classification() {
    let noauth_err = redis::RedisError::from((
        redis::ErrorKind::Server(ServerErrorKind::ResponseError),
        "NOAUTH Authentication required.",
    ));
    let classified = classify_redis_error(&noauth_err);
    assert!(matches!(classified, RedisDriverError::Programming { .. }));
    assert!(!classified.is_transient());

    let wrongpass_err = redis::RedisError::from((
        redis::ErrorKind::Server(ServerErrorKind::ResponseError),
        "WRONGPASS invalid username-password pair",
    ));
    let classified_wp = classify_redis_error(&wrongpass_err);
    assert!(matches!(
        classified_wp,
        RedisDriverError::Programming { .. }
    ));
    assert!(!classified_wp.is_transient());

    let crossslot_err = redis::RedisError::from((
        redis::ErrorKind::Server(ServerErrorKind::ResponseError),
        "CROSSSLOT Keys in request don't hash to the same slot",
    ));
    let classified_cs = classify_redis_error(&crossslot_err);
    assert!(matches!(
        classified_cs,
        RedisDriverError::Programming { .. }
    ));
    assert!(!classified_cs.is_transient());

    let loading_err = redis::RedisError::from((
        redis::ErrorKind::Server(ServerErrorKind::ResponseError),
        "LOADING Redis is loading the dataset in memory",
    ));
    let classified_loading = classify_redis_error(&loading_err);
    assert!(matches!(
        classified_loading,
        RedisDriverError::Operational { .. }
    ));
    assert!(classified_loading.is_transient());

    let clusterdown_err = redis::RedisError::from((
        redis::ErrorKind::Server(ServerErrorKind::ResponseError),
        "CLUSTERDOWN The cluster is down",
    ));
    let classified_cd = classify_redis_error(&clusterdown_err);
    assert!(matches!(
        classified_cd,
        RedisDriverError::Operational { .. }
    ));
    assert!(classified_cd.is_transient());

    let noscript_err = redis::RedisError::from((
        redis::ErrorKind::Server(ServerErrorKind::ResponseError),
        "NOSCRIPT No matching script. Please use EVAL.",
    ));
    let classified_ns = classify_redis_error(&noscript_err);
    assert!(matches!(
        classified_ns,
        RedisDriverError::Programming { .. }
    ));
    assert!(!classified_ns.is_transient());
}

#[test]
fn test_client_lru_caching() {
    let cache = ClientLruCache::new(2);
    cache.put("k1".to_string(), b"val1".to_vec(), None);
    cache.put("k2".to_string(), b"val2".to_vec(), None);

    assert_eq!(cache.get("k1"), Some(b"val1".to_vec()));
    assert_eq!(cache.get("k2"), Some(b"val2".to_vec()));

    // Exceed capacity: k3 should evict oldest accessed
    cache.put("k3".to_string(), b"val3".to_vec(), None);
    assert_eq!(cache.get("k3"), Some(b"val3".to_vec()));

    // Invalidation
    cache.invalidate_keys(&["k2".to_string()]);
    assert_eq!(cache.get("k2"), None);
    assert_eq!(cache.drain_invalidations(), vec!["k2".to_string()]);

    // Flush all
    cache.put("k4".to_string(), b"val4".to_vec(), None);
    cache.invalidate_all();
    assert_eq!(cache.get("k4"), None);
    assert!(!cache.drain_invalidations().is_empty());
}

#[test]
fn test_cluster_redirect_parsing() {
    let moved = check_cluster_redirect("MOVED 1234 10.0.0.1:6379");
    assert_eq!(
        moved,
        Some(ClusterRedirectAction::Moved {
            slot: 1234,
            host: "10.0.0.1".to_string(),
            port: 6379,
        })
    );

    let ask = check_cluster_redirect("ASK 5678 10.0.0.2:6379");
    assert_eq!(
        ask,
        Some(ClusterRedirectAction::Ask {
            slot: 5678,
            host: "10.0.0.2".to_string(),
            port: 6379,
        })
    );

    let slot_map = ClusterSlotMap::new();
    slot_map.update_slot(1234, "10.0.0.1".to_string(), 6379);
    assert_eq!(
        slot_map.get_slot_node(1234),
        Some(("10.0.0.1".to_string(), 6379))
    );
    assert_eq!(slot_map.get_slot_node(9999), None);
}

#[test]
fn test_sentinel_parsing() {
    let response = Value::Array(vec![
        Value::BulkString(b"192.168.1.100".to_vec()),
        Value::Int(6379),
    ]);
    let (host, port) = parse_sentinel_master_addr(&response).unwrap();
    assert_eq!(host, "192.168.1.100");
    assert_eq!(port, 6379);
}

#[test]
fn test_lazy_client_creation_and_close() {
    let cfg = RedisClientConfig::new("redis://127.0.0.1:6379/0").unwrap();
    let client = RedisCoreClient::new(cfg).unwrap();
    assert!(!client.is_closed());
    client.close();
    assert!(client.is_closed());
}

#[test]
fn test_client_lru_invalidations_capped_at_4096() {
    let cache = ClientLruCache::new(16);
    let keys: Vec<String> = (0..5000).map(|i| format!("inv:{i}")).collect();
    cache.invalidate_keys(&keys);
    let drained = cache.drain_invalidations();
    assert_eq!(drained.len(), 4096);
    assert_eq!(drained.first().map(String::as_str), Some("inv:904"));
    assert_eq!(drained.last().map(String::as_str), Some("inv:4999"));
}

#[tokio::test]
async fn test_client_tracking_handshake_sentinel_blocking_xreadgroup_and_close() {
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use vertebra_redis::streams;

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

    // Mock Redis Master speaking minimal RESP3 (handling pipelined handshake frames).
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
                        } else if op == "XREADGROUP" {
                            // Sleep 1600ms (> old 1500ms ConnectionManager timeout) before replying!
                            tokio::time::sleep(std::time::Duration::from_millis(1600)).await;
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

    // Mock Sentinel returning `[127.0.0.1, master_port]` to `SENTINEL get-master-addr-by-name`.
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

    let mut cfg = RedisClientConfig::new(&format!("redis://127.0.0.1:{sentinel_port}/0")).unwrap();
    cfg.topology = RedisTopologyMode::Sentinel;
    cfg.sentinel_master_name = Some("mymaster".to_string());
    cfg.sentinel_nodes = vec![("127.0.0.1".to_string(), sentinel_port)];
    cfg.enable_client_side_caching = true;
    cfg.client_tracking_prefixes = vec!["user:".to_string()];

    let client = RedisCoreClient::new(cfg).unwrap();
    let val = client.get(b"user:1").await.unwrap();
    assert_eq!(val.as_deref(), Some(&b"hello"[..]));
    assert!(client.is_caching_active());
    assert!(client.has_active_connection());

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

    // Blocking XREADGROUP with block_ms=1700 must not prematurely time out at 1.5s.
    let xres = streams::xreadgroup(
        &client,
        "grp",
        "c1",
        &[("mystream".to_string(), ">".to_string())],
        Some(1),
        Some(1700),
    )
    .await
    .unwrap();
    assert_eq!(xres, Value::Nil);

    // close() must drop the stored connection immediately.
    client.close();
    assert!(client.is_closed());
    assert!(!client.has_active_connection());
}
