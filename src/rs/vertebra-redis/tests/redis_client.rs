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
