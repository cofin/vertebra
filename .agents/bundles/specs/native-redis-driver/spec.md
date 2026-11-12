---
type: Spec
flow_id: native-redis-driver
title: "Standalone Rust Redis and Valkey Wheel (vertebra-redis)"
description: "Standalone PyO3 crate src/rs/vertebra-redis (vertebra._redis for vertebra-core[redis] and vertebra-core[valkey]) built on the redis 1.8.1 crate (tokio-rustls-comp, cluster-async, cache-aio) with pipelining, streams, Sentinel, Cluster routing, CLIENT TRACKING invalidation, and SyncRedisNotificationSource / AsyncRedisNotificationSource pub/sub sources, removing the Python socket driver, the vertebra-db RESP3 codec bindings, and the redis-py clients in the Redis and Valkey queue backends."
status: draft
state: draft
has_chapters: false
parent_prd: native-data-platform
depends_on_flows:
  - db-layout-and-contract
  - db-engine-primitives
  - driver-fail-loud
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 3
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-12T01:30:00Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified: []
sources:
  - id: redis-py
    resource: src/py/vertebra/db/adapters/redis/driver.py
    title: Python SyncRedisDriver and AsyncRedisDriver executing over Python sockets
  - id: valkey-py
    resource: src/py/vertebra/db/adapters/valkey/driver.py
    title: Python SyncValkeyDriver and AsyncValkeyDriver
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs
    title: RESP3 frame codecs in vertebra-db bound to Python for the socket driver
  - id: resp3-bindings
    resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
    title: vertebra._vertebra RESP3 helper bindings including redis_hash_slot and parse_cluster_redirect
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: Redis and Valkey queue backends building redis.asyncio clients
  - id: notification-source
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource capability protocol split into SyncNotificationSource and AsyncNotificationSource by queues-tasks-and-channels
  - id: crate-research
    resource: .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
    title: Rust client crate selection including redis 1.8.1
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Completed fail-loud baseline removing Redis and Valkey memory:// execution
tags:
  - rust
  - redis
  - valkey
  - resp3
  - driver
  - wheel
---

# Flow Charter: Standalone Rust Redis and Valkey Wheel (`vertebra-redis`)

<!-- truth: start -->
- **Current State**: `SyncRedisDriver` / `AsyncRedisDriver` execute over Python `socket` / `asyncio.open_connection` with the `vertebra_db::resp3` bindings on `vertebra._vertebra`; `RedisQueueBackend` and `ValkeyQueueBackend` build `redis.asyncio` clients; `pyproject.toml` declares no `redis` or `valkey` extra [^redis-py] [^resp3-bindings] [^queue-redis].
- **Shared Redis & Valkey Rust Wheel (target state)**: `src/rs/vertebra-redis` (`vertebra._redis`, `vertebra-core[redis]` and `vertebra-core[valkey]`) builds on the `redis` 1.8.1 crate (`tokio-rustls-comp`, `cluster-async`, `cache-aio`), which owns RESP3 framing, connection management, and cluster routing; this flow writes no hand-built RESP3 client [^crate-research] [^valkey-py].
- **RESP3 Pipelining, Cluster & Pub/Sub (target state)**: `SyncRedisClient` and `AsyncRedisClient` provide `HELLO 3`, multiplexed pipelining, `SCAN`+`UNLINK`, Sentinel, Cluster `MOVED`/`ASK` routing, `CLIENT TRACKING` invalidations, and `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` implementing the `SyncNotificationSource` / `AsyncNotificationSource` protocols [^py-async-rs] [^notification-source].
- **Socket, Codec & redis-py Removal (target state)**: Python raw socket / RESP parsers leave the `redis` and `valkey` adapters, the `vertebra_db::resp3` bindings leave `vertebra._vertebra`, and the Redis and Valkey queue backends execute through `vertebra._redis` instead of `redis.asyncio` [^resp3-rs].
<!-- truth: end -->

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Redis & Valkey (`src/rs/vertebra-redis/` -> `vertebra._redis` + `vertebra-core[redis]` / `vertebra-core[valkey]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `redis` `1.8.1` (`default-features = false`, `tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_redis.pyi` verified by `mypy.stubtest`, new `[project.optional-dependencies].{redis,valkey}` extras both installing `vertebra-redis`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration, benchmarked against the Python socket driver it replaces. The `redis` crate owns RESP3 framing, so `vertebra-redis` adds no RESP3 codec to `vertebra-db` [^crate-research].
- **Fail-loud baseline.** The merged `driver-fail-loud` baseline (`af1b3d30`, [knowledge](../../knowledge/domains/data/database-drivers-and-dialers.md)) deletes `memory://` support from the Redis and Valkey drivers and `RedisConfig.url_schemes` while keeping the `vertebra_db::resp3` bindings for the Python socket driver; the `from_redis` and `from_valkey` store factories stay and raise `ImproperConfigurationError` for `memory://` [^fail-loud-baseline] [^resp3-rs].
- **Explicit `Sync*` / `Async*` clients & the notification source.** Expose `SyncRedisClient` (`SyncCompletionQueue`) and `AsyncRedisClient` (`AsyncCompletionQueue` + `Notifier` via `vertebra_db::py_async` with `"_vertebra_redis_completion_drainer"`). Pub/sub is exposed as `SyncRedisNotificationSource` (over `SyncRedisClient`, blocking `publish` / `subscribe` / `unsubscribe` and `listen() -> Iterator[NotificationEvent]` for Django, Flask, synchronous workers, and CLI listeners) and `AsyncRedisNotificationSource` (over `AsyncRedisClient`, `listen() -> AsyncIterator[NotificationEvent]` for `ChannelBus` and async channels), implementing `SyncNotificationSource` and `AsyncNotificationSource` from `vertebra/db/driver/capabilities.py`, matching `Sync<Engine>NotificationSource` / `Async<Engine>NotificationSource` for PostgreSQL and Oracle, and re-exported for Valkey.
- **Queue backends.** `framework/queues/redis.py::RedisQueueBackend` and `ValkeyQueueBackend` build their default client from `vertebra._redis.AsyncRedisClient` through `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` (`extra="valkey"` for Valkey), accepting `redis://`, `rediss://`, `valkey://`, `valkeys://`, and `unix://` URLs, and remove the `redis.asyncio.from_url` path. The injected `client=` collaborator (`RedisStreamClient`) remains supported [^queue-redis].

## Scope

1. **Crate scaffold, RESP3 connection & pipelining (`src/rs/vertebra-redis/`).**
   - `SyncRedisClient` and `AsyncRedisClient` in `vertebra._redis` wrapping `redis` 1.8.1 multiplexed connections over `tokio-rustls-comp` (`HELLO 3`, ACL `AUTH`, GCP Memorystore IAM token refresh via `SyncGcpTokenCache` / `AsyncGcpTokenCache` through the `GcpTokenCacheCapsule`), multiplexed pipelining (`execute_pipeline`), `MULTI`/`EXEC`/`WATCH` transactions, `SCAN` + `UNLINK`, millisecond `PX` expiries, and `DatabaseError` mapping (`vertebra._vertebra`).
2. **Pub/sub, streams, Sentinel, Cluster & client-side tracking.**
   - Pub/sub and sharded pub/sub (`SSUBSCRIBE`) in a new `vertebra/db/adapters/redis/notify.py` (`SyncRedisNotificationSource`, `AsyncRedisNotificationSource`), re-exported by a new `vertebra/db/adapters/valkey/notify.py`.
   - Streams (`XADD`, `XREADGROUP`, `XACK`), Sentinel master discovery (`discover_sentinel_master` / `async_discover_sentinel_master`), Cluster slot routing (`MOVED`/`ASK`) through `cluster-async`, and RESP3 `CLIENT TRACKING` invalidation push handling through `cache-aio`.
3. **Python `redis` and `valkey` cutover.**
   - Replace Python raw socket / `asyncio.open_connection` code in `vertebra/db/adapters/redis/driver.py` and the Python RESP parser in `vertebra/db/adapters/redis/core.py` with `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` (and `extra="valkey"` in `valkey/driver.py`).
   - Delete the `vertebra_db::resp3` module, its `vertebra-py/src/db.rs` bindings (including `redis_hash_slot` and `parse_cluster_redirect`), and their `_vertebra.pyi` stubs in the same commit, since no Python caller remains once the socket driver goes.
4. **Queue backend cutover.**
   - Move `RedisQueueBackend._build_client` and `ValkeyQueueBackend` onto `vertebra._redis`, delete the `redis.asyncio` import path and its install hint, and update the queue unit tests in their canonical module to cover every accepted URL scheme and the missing-wheel `MissingDependencyError`.
5. **Docs & OKF.** Update the Redis/Valkey driver and queue sections in `docs/`, `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md`, and `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md`, and record caveats in a Redis/Valkey limitations chapter under `.agents/bundles/knowledge/domains/data/`.

## Public Seams

`vertebra._redis` (`SyncRedisClient`, `AsyncRedisClient`), `SyncRedisDriver`, `AsyncRedisDriver`, `SyncValkeyDriver`, `AsyncValkeyDriver`, `SyncRedisNotificationSource`, `AsyncRedisNotificationSource`, `RedisQueueBackend`, `ValkeyQueueBackend`, and the `vertebra-core[redis]` / `vertebra-core[valkey]` extras.

## Dependencies

Flow dependencies (all merged): `db-layout-and-contract` (`21c99f95`), integration-remediation `db-engine-primitives` (`80ff8a65`), and `driver-fail-loud` (`af1b3d30`), which removes Redis and Valkey `memory://` execution. This flow starts from `main @ c8722c76`. Downstream, `queues-tasks-and-channels` consumes `AsyncRedisNotificationSource` for distributed channels and `SyncRedisNotificationSource` for synchronous listeners and `vertebra._redis` for `ValkeyStore`.

## Acceptance Boundary

- Integration tests pass natively against Redis 7/8 and Valkey 8 (standalone and cluster) via `pytest-databases`.
- RESP3 `HELLO 3`, pipelining, `SCAN`+`UNLINK`, pub/sub `SyncRedisNotificationSource` and `AsyncRedisNotificationSource`, `MOVED`/`ASK` slot routing, and `CLIENT TRACKING` invalidation unit/integration tests pass with zero Python socket fallback.
- `RedisQueueBackend` and `ValkeyQueueBackend` deliver tasks across processes on Redis and Valkey through `vertebra._redis`, and `git grep -n "redis.asyncio" -- src/py/vertebra` returns nothing.
- `vertebra._vertebra` exports no RESP3 codec functions and `mypy.stubtest vertebra._vertebra` passes.
- Wheel isolation check confirms `vertebra-redis` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._redis`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-redis` (`vertebra._redis`, `_redis.pyi`, `[redis]` + `[valkey]` extras + `uv.lock` + `release.yaml`/`ci.yml`) and `redis`-crate client wrapper (`HELLO 3`, TLS, ACL/IAM auth, pipelining, `MULTI`/`EXEC`, `SCAN`+`UNLINK`).
- t02 Redis/Valkey pub/sub `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` and streams (`XADD`/`XREADGROUP`/`XACK`); t02 starts after the `queues-tasks-and-channels` notification-source rename task lands `SyncNotificationSource` / `AsyncNotificationSource` in `vertebra/db/driver/capabilities.py` (task-level ordering; the flow-level dependency stays `queues-tasks-and-channels` -> `native-redis-driver`).
- t03 Sentinel discovery, `cluster-async` slot routing (`MOVED`/`ASK`), and RESP3 `CLIENT TRACKING` invalidation push handling.
- t04 Python `SyncRedisDriver`/`AsyncRedisDriver` & `SyncValkeyDriver`/`AsyncValkeyDriver` cutover, Python socket/RESP parser removal, `vertebra_db::resp3` binding and stub deletion, and integration conformance.
- t05 `RedisQueueBackend` / `ValkeyQueueBackend` cutover from `redis.asyncio` to `vertebra._redis`, and docs/OKF update.

[^redis-py]: `src/py/vertebra/db/adapters/redis/driver.py` — Python SyncRedisDriver and AsyncRedisDriver executing over Python sockets.
[^valkey-py]: `src/py/vertebra/db/adapters/valkey/driver.py` — Python SyncValkeyDriver and AsyncValkeyDriver.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — RESP3 frame codecs in vertebra-db bound to Python for the socket driver.
[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — `vertebra._vertebra` RESP3 helper bindings including `redis_hash_slot` and `parse_cluster_redirect`.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis and Valkey queue backends building `redis.asyncio` clients.
[^notification-source]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — Current unprefixed asynchronous notification protocol that `queues-tasks-and-channels` splits into `SyncNotificationSource` and `AsyncNotificationSource`.
[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Rust client crate selection including `redis` 1.8.1.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud baseline removing Redis and Valkey `memory://` execution.
