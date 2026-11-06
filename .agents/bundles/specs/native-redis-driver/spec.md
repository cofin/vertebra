---
type: Spec
flow_id: native-redis-driver
title: Standalone Rust Redis and Valkey Wheel (vertebra-redis)
description: Standalone PyO3 crate src/rs/vertebra-redis (vertebra._redis for vertebra-core[redis] and vertebra-core[valkey]) with Tokio + rustls RESP3 client, pipelining, streams, Sentinel, Cluster CRC16 routing, CLIENT TRACKING invalidation, and pub/sub NotificationSource, removing Python raw sockets and memory:// mode.
state: draft
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T04:05:00Z
updated_at: "2026-11-06T06:30:00Z"
tags: [rust, redis, valkey, resp3, driver, wheel]
---

# Flow Charter: Standalone Rust Redis and Valkey Wheel (`vertebra-redis`)

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Redis & Valkey (`src/rs/vertebra-redis/` -> `vertebra._redis` + `vertebra-core[redis]` / `vertebra-core[valkey]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `redis` `1.8.1` (`default-features = false`, `tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_redis.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].{redis,valkey}`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration, benchmarked against `vertebra_db::resp3` hot-path frame codecs.
- **Explicit `Sync*` / `Async*` clients & notification sources.** Expose `SyncRedisClient` (`SyncCompletionQueue`) and `AsyncRedisClient` (`AsyncCompletionQueue` + `Notifier` via `vertebra_db::py_async` with `"_vertebra_redis_completion_drainer"`), plus `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` (`SyncNotificationSource` / `AsyncNotificationSource`) re-exported for Valkey.

## Scope

1. **Crate scaffold, RESP3 connection & pipelining (`src/rs/vertebra-redis/`).**
   - `SyncRedisClient` and `AsyncRedisClient` in `vertebra._redis`; Tokio + `rustls` (`ring`) RESP3 client (`HELLO 3`, ACL `AUTH`, GCP Memorystore IAM token refresh via `SyncGcpTokenCache` / `AsyncGcpTokenCache`), multiplexed pipelining (`execute_pipeline`), `MULTI`/`EXEC`/`WATCH` transactions, `SCAN` + `UNLINK`, millisecond `PX` expiries, and `DatabaseError` mapping (`vertebra._vertebra`).
2. **Pub/sub, streams, Sentinel, Cluster & client-side tracking.**
   - Pub/sub and sharded pub/sub (`SSUBSCRIBE`) implementing `SyncNotificationSource` and `AsyncNotificationSource` in `vertebra/db/adapters/redis/notify.py` (re-exported by `vertebra/db/adapters/valkey/notify.py`).
   - Streams (`XADD`, `XREADGROUP`, `XACK`), Sentinel master discovery (`discover_sentinel_master` / `async_discover_sentinel_master`), Cluster CRC16 slot routing (`MOVED`/`ASK`), and RESP3 `CLIENT TRACKING` invalidation push handling.
3. **Python `redis` and `valkey` cutover.**
   - Replace Python raw socket / `asyncio.open_connection` code in `vertebra/db/adapters/redis/driver.py` and the Python RESP parser in `vertebra/db/adapters/redis/core.py` with `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` (and `extra="valkey"` in `valkey/driver.py`), and remove `memory://` fake mode (and `"memory"` from `RedisConfig.url_schemes` and `ValkeyConfig.url_schemes`).

## Public Seams

`vertebra._redis` (`SyncRedisClient`, `AsyncRedisClient`), `SyncRedisDriver`, `AsyncRedisDriver`, `SyncValkeyDriver`, `AsyncValkeyDriver`, `SyncRedisNotificationSource`, and `AsyncRedisNotificationSource`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`) and `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`).

## Acceptance Boundary

- Integration tests pass natively against Redis 7/8 and Valkey 8 (standalone and cluster) via `pytest-databases`.
- RESP3 `HELLO 3`, pipelining, `SCAN`+`UNLINK`, pub/sub `NotificationSource`, `MOVED`/`ASK` slot routing, and `CLIENT TRACKING` invalidation unit/integration tests pass with zero Python socket fallback or `memory://` mode.
- Wheel isolation check confirms `vertebra-redis` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._redis`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-redis` (`vertebra._redis`, `_redis.pyi`, `[redis]` + `[valkey]` extras + `uv.lock` + `release.yaml`/`ci.yml`) and RESP3 client (`HELLO 3`, TLS, ACL/IAM auth, pipelining, `MULTI`/`EXEC`, `SCAN`+`UNLINK`).
- t02 Redis/Valkey pub/sub `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` and streams (`XADD`/`XREADGROUP`/`XACK`).
- t03 Sentinel discovery, Cluster CRC16 slot routing (`MOVED`/`ASK`), and RESP3 `CLIENT TRACKING` invalidation push handling.
- t04 Python `SyncRedisDriver`/`AsyncRedisDriver` & `SyncValkeyDriver`/`AsyncValkeyDriver` cutover, Python socket/RESP parser + `memory://` removal, integration conformance, and docs/OKF update.
