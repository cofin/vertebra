---
type: Gotcha
title: Redis Native Driver Limitations
description: Operational boundaries for native Redis driver topologies, dedicated pub/sub sockets, Cluster hash-tag CROSSSLOT rules, RESP3 CLIENT TRACKING push invalidations, and unix socket standalone constraints.
status: stable
tags: [redis, limitations, driver, cluster, streams]
generated:
  by: flow-implement
  at: "2026-11-16T01:40:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T01:28:00Z"
    commit: 5a833832
  - by: agent:executor
    at: "2026-11-16T01:40:00Z"
    commit: 473e1f2e
sources:
  - id: redis-config-py
    resource: src/py/vertebra/db/adapters/redis/config.py::RedisConfig
    title: Redis URL validation, memory rejection, Azure Entra scope, and cluster/sentinel topologies
  - id: redis-sync-py
    resource: src/py/vertebra/db/adapters/redis/_sync.py::SyncRedisDriver
    title: Sync/async Redis driver layout, slot_map, and Sentinel rediscovery
  - id: resp-py
    resource: src/py/vertebra/db/driver/_resp.py::ClusterRedirect
    title: Shared RESP3 cluster redirect, CRC16-XMODEM slot, and Resp3Error primitives
  - id: redis-client-core-rs
    resource: src/rs/vertebra-resp/src/client.rs::RespEngine
    title: Pure-Rust RESP3 client engine and EVALSHA-to-EVAL script fallback
  - id: redis-pubsub-rs
    resource: src/rs/vertebra-redis/src/pubsub.rs::AsyncRedisListener
    title: Dedicated socket pub/sub listener and multiplexed push streaming
  - id: redis-queue-py
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: Redis Streams consumer groups and sorted-set delayed task scheduling
---

# Redis Native Driver Limitations

<!-- truth: start -->
- Adapter layout and shared RESP3 primitives: `SyncRedisDriver` (`_sync.py`) and `AsyncRedisDriver` (`_async.py`) sit behind `driver.py` as a barrel and share `ClusterRedirect`, `PubSubMessage`, `NON_KEY_COMMANDS`, `crc16_xmodem`, `hash_slot`, `parse_cluster_redirect`, and `Resp3Error` (subclassing `DatabaseError`) from `vertebra.db.driver._resp` [^redis-sync-py] [^resp-py].
- Cluster `slot_map` snapshot semantics: `SyncRedisDriver.slot_map` and `AsyncRedisDriver.slot_map` delegate to `SyncRedisClient.slot_map()` / `AsyncRedisClient.slot_map()`, returning `dict[int, tuple[str, int]]` from `slot_map.snapshot()` (`{}` under standalone mode or before cluster slot topology discovery completes) [^redis-sync-py].
- Sentinel rediscovery settings preservation: `discover_sentinel_master` queries `sentinel_nodes` using `sentinel_cfg` (`rediss://` or `redis://`, `client_name`, `protocol`) and rebuilds the primary client preserving TLS (`uses_tls`), client tracking/caching, protocol, and `effective_database_token_provider` (`AzureEntraTokenProvider` normalized with `AZURE_REDIS_SCOPE` / `GcpTokenCache`), raising `ImproperConfigurationError` from the last exception when every Sentinel fails; Sentinel probe connections do not forward primary auth/token credentials when querying `SENTINEL get-master-addr-by-name`, and `SyncRedisDriver.discover_sentinel_master` constructs `new_client` from `client_config` before refreshing `_client_token` via `resolve_sync()` (whereas `AsyncRedisDriver` passes `resolved_password=` directly to `_create_client`) [^redis-sync-py] [^redis-config-py].
- Dedicated pub/sub listener sockets: Pub/Sub connections cannot interleave ordinary key-value data commands (`GET`, `SET`, `DEL`); `SyncRedisListener` and `AsyncRedisListener` allocate dedicated TCP or Unix socket connections isolated from command connection pools [^redis-pubsub-rs].
- Redis Cluster `{hash_tag}` and `CROSSSLOT` rules: Multi-key operations, multi-key scripts, and atomic pipelines (`SyncRedisPipeline(atomic=True)`) under Cluster topology require keys to hash to the same slot using `{...}` hash tags; un-tagged cross-slot multi-key commands raise `OperationalError` (`CROSSSLOT Keys in request don't hash to the same slot`) [^redis-client-core-rs].
- RESP3 `CLIENT TRACKING` requirements: Local L1 invalidation caching requires RESP3 protocol negotiation (`HELLO 3`); using legacy RESP2 disables server-assisted push invalidations and falls back to direct network reads [^redis-client-core-rs].
- `unix://` domain sockets: Unix domain socket paths (`unix:///path/to/redis.sock`) are supported exclusively for standalone single-instance Redis deployments; Cluster and Sentinel topologies require routable TCP/TLS endpoints (`redis://`, `rediss://`) [^redis-config-py].
- Google Cloud Memorystore IAM authentication: Cloud Memorystore instances require short-lived Google Cloud OAuth2 access tokens via `GcpTokenCacheCapsule` / `GcpTokenSnapshot`; instances without automatic token cache renewal disconnect when credentials expire [^redis-config-py].
- Rejection of `memory://` URLs: `RedisConfig` accepts `redis://`, `rediss://`, and `unix://` schemes and rejects `memory://`, `valkey://`, and `valkeys://` with `ImproperConfigurationError` during driver initialization [^redis-config-py].
- Redis Streams queue semantics: `RedisQueueBackend` relies on Redis 5.0+ consumer groups (`XREADGROUP`, `XACK`, `XAUTOCLAIM`); dead-letter promotion is bounded by `TaskRetryPolicy.max_retries` before relocation to the dead-letter stream [^redis-queue-py].
<!-- truth: end -->

## Operational Boundaries

- **Pub/Sub Channel Isolation**: [`SyncRedisListener`](../../../../../src/rs/vertebra-redis/src/pubsub.rs)[^redis-pubsub-rs] and `AsyncRedisListener` maintain a separate persistent stream connection dedicated to SUBSCRIBE, PSUBSCRIBE, and SSUBSCRIBE frames; applications should not share notification listeners with high-frequency request/response pools.
- **Cluster Shard Redirection**: [`RespEngine`](../../../../../src/rs/vertebra-resp/src/client.rs)[^redis-client-core-rs] automatically follows `MOVED` and `ASK` redirects up to the configured retry budget and updates the slot map surfaced through `slot_map()`; permanent node re-sharding requires updating the cluster slot map via topology refresh.
- **Script Caching Discipline**: Lua scripts execute via `EVALSHA` and fall back to `EVAL` upon encountering `NOSCRIPT`; ephemeral servers or flushed script caches incur a one-round-trip script upload penalty.

[^redis-config-py]: `src/py/vertebra/db/adapters/redis/config.py::RedisConfig` — Redis URL validation, memory rejection, Azure Entra scope, and cluster/sentinel topologies.
[^redis-sync-py]: `src/py/vertebra/db/adapters/redis/_sync.py::SyncRedisDriver` — Sync/async Redis driver layout, `slot_map`, and Sentinel rediscovery.
[^resp-py]: `src/py/vertebra/db/driver/_resp.py::ClusterRedirect` — Shared RESP3 cluster redirect, CRC16-XMODEM slot, and `Resp3Error` primitives.
[^redis-client-core-rs]: `src/rs/vertebra-resp/src/client.rs::RespEngine` — Pure-Rust RESP3 client engine and EVALSHA-to-EVAL script fallback.
[^redis-pubsub-rs]: `src/rs/vertebra-redis/src/pubsub.rs::AsyncRedisListener` — Dedicated socket pub/sub listener and multiplexed push streaming.
[^redis-queue-py]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis Streams consumer groups and sorted-set delayed task scheduling.
