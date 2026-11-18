---
type: Gotcha
title: Valkey Native Driver Limitations
description: Operational boundaries for native Valkey driver topologies, dedicated pub/sub sockets, Valkey 9.0 hash expiration constraints, Cluster hash-tag CROSSSLOT rules, and unix socket standalone constraints.
status: stable
tags: [valkey, limitations, driver, cluster, streams, hash-expiration]
generated:
  by: flow/0.30.1
  at: "2026-11-17T01:28:00Z"
verified:
  - by: agent:flow-executor
    at: "2026-11-17T01:28:00Z"
    commit: 5a833832
sources:
  - id: valkey-config-py
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: Valkey URL validation, memory rejection, Azure Entra scope, and cluster/sentinel topologies
  - id: valkey-sync-py
    resource: src/py/vertebra/db/adapters/valkey/_sync.py::SyncValkeyDriver
    title: Sync/async Valkey driver layout, slot_map, and Sentinel rediscovery
  - id: resp-py
    resource: src/py/vertebra/db/driver/_resp.py::ClusterRedirect
    title: Shared RESP3 cluster redirect, CRC16-XMODEM slot, and Resp3Error primitives
  - id: valkey-client-rs
    resource: src/rs/vertebra-valkey/src/client.rs::AsyncValkeyClient
    title: Pure-Rust RESP3 client engine, cluster slot_map, Valkey 9.0 hash expiration, and CLIENT CAPA
  - id: valkey-pubsub-rs
    resource: src/rs/vertebra-valkey/src/pubsub.rs::AsyncValkeyListener
    title: Dedicated socket pub/sub listener and multiplexed push streaming
  - id: valkey-queue-py
    resource: src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend
    title: Valkey Streams consumer groups and sorted-set delayed task scheduling
---

# Valkey Native Driver Limitations

<!-- truth: start -->
- Adapter layout and shared RESP3 primitives: `SyncValkeyDriver` (`_sync.py`) and `AsyncValkeyDriver` (`_async.py`) sit behind `driver.py` as a barrel and share `ClusterRedirect`, `PubSubMessage`, `NON_KEY_COMMANDS`, `crc16_xmodem`, `hash_slot`, `parse_cluster_redirect`, and `Resp3Error` (subclassing `DatabaseError`) from `vertebra.db.driver._resp` [^valkey-sync-py] [^resp-py].
- Cluster `slot_map` snapshot semantics: `SyncValkeyDriver.slot_map` and `AsyncValkeyDriver.slot_map` delegate to `SyncValkeyClient.slot_map()` / `AsyncValkeyClient.slot_map()`, returning `dict[int, tuple[str, int]]` from the Rust topology state (`{}` under standalone mode or before cluster slot topology discovery completes) [^valkey-sync-py] [^valkey-client-rs].
- Sentinel rediscovery settings preservation: `discover_sentinel_master` rebuilds the primary client through `_create_client(...)` preserving scheme/TLS (`uses_tls`), `client_name`, `protocol`, `enable_client_side_caching`, `enable_client_tracking`, and `effective_database_token_provider` (`AzureEntraTokenProvider` normalized with `AZURE_REDIS_SCOPE` / `GcpTokenCache`), raising `ImproperConfigurationError` from the last exception when every Sentinel fails; Sentinel probe connections themselves disable client-side caching/tracking and do not forward primary auth/token credentials when querying `SENTINEL get-master-addr-by-name` [^valkey-sync-py] [^valkey-config-py].
- Dedicated pub/sub listener sockets: Pub/Sub connections cannot interleave ordinary key-value data commands (`GET`, `SET`, `DEL`); `SyncValkeyListener` and `AsyncValkeyListener` allocate dedicated TCP or Unix socket connections isolated from command connection pools [^valkey-pubsub-rs].
- Valkey Cluster `{hash_tag}` and `CROSSSLOT` rules: Multi-key operations, multi-key scripts, and atomic pipelines (`SyncValkeyPipeline(transaction=True)`) under Cluster topology require keys to hash to the same slot using `{...}` hash tags; un-tagged cross-slot multi-key commands raise `OperationalError` (`CROSSSLOT Keys in request don't hash to the same slot`) [^valkey-client-rs].
- Valkey 9.0 hash-field expiration availability: Hash-field expiration commands (`HEXPIRE`, `HPEXPIRE`, `HEXPIREAT`, `HPEXPIREAT`, `HTTL`, `HPTTL`, `HPERSIST`) require Valkey 9.0+ server support; invoking them on older engines returns an `OperationalError` or `ProgrammingError` (`ERR unknown command`) [^valkey-client-rs].
- RESP3 `CLIENT TRACKING` requirements: Local L1 invalidation caching requires RESP3 protocol negotiation (`HELLO 3`); using legacy RESP2 disables server-assisted push invalidations and falls back to direct network reads [^valkey-client-rs].
- `unix://` domain sockets: Unix domain socket paths (`unix:///path/to/valkey.sock`) are supported exclusively for standalone single-instance Valkey deployments; Cluster and Sentinel topologies require routable TCP/TLS endpoints (`valkey://`, `valkeys://`) [^valkey-config-py].
- Rejection of `memory://` and `redis://` URLs: `ValkeyConfig` accepts `valkey://`, `valkeys://`, and `unix://` schemes and rejects `memory://`, `redis://`, and `rediss://` with `ImproperConfigurationError` during driver initialization [^valkey-config-py].
- Valkey Streams queue semantics: `ValkeyQueueBackend` relies on Valkey consumer groups (`XREADGROUP`, `XACK`, `XAUTOCLAIM`); dead-letter promotion is bounded by `TaskRetryPolicy.max_retries` before relocation to the dead-letter stream [^valkey-queue-py].
<!-- truth: end -->

## Operational Boundaries

- **Pub/Sub Channel Isolation**: [`SyncValkeyListener`](../../../../../src/rs/vertebra-valkey/src/pubsub.rs)[^valkey-pubsub-rs] and `AsyncValkeyListener` maintain a separate persistent stream connection dedicated to SUBSCRIBE, PSUBSCRIBE, and SSUBSCRIBE frames; applications should not share notification listeners with high-frequency request/response pools.
- **Cluster Shard Redirection**: [`AsyncValkeyClient`](../../../../../src/rs/vertebra-valkey/src/client.rs)[^valkey-client-rs] automatically follows `MOVED` and `ASK` redirects up to the configured retry budget, negotiates `CLIENT CAPA redirect` for cluster routing, and exposes the active slot table via `slot_map()`.
- **Script Caching Discipline**: Lua scripts execute via `EVALSHA` and fall back to `EVAL` upon encountering `NOSCRIPT`; ephemeral servers or flushed script caches incur a one-round-trip script upload penalty.

[^valkey-config-py]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — Valkey URL validation, memory rejection, Azure Entra scope, and cluster/sentinel topologies.
[^valkey-sync-py]: `src/py/vertebra/db/adapters/valkey/_sync.py::SyncValkeyDriver` — Sync/async Valkey driver layout, `slot_map`, and Sentinel rediscovery.
[^resp-py]: `src/py/vertebra/db/driver/_resp.py::ClusterRedirect` — Shared RESP3 cluster redirect, CRC16-XMODEM slot, and `Resp3Error` primitives.
[^valkey-client-rs]: `src/rs/vertebra-valkey/src/client.rs::AsyncValkeyClient` — Pure-Rust RESP3 client engine, cluster `slot_map`, Valkey 9.0 hash expiration, and CLIENT CAPA.
[^valkey-pubsub-rs]: `src/rs/vertebra-valkey/src/pubsub.rs::AsyncValkeyListener` — Dedicated socket pub/sub listener and multiplexed push streaming.
[^valkey-queue-py]: `src/py/vertebra/framework/queues/valkey.py::ValkeyQueueBackend` — Valkey Streams consumer groups and sorted-set delayed task scheduling.
