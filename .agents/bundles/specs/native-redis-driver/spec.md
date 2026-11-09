---
type: Spec
flow_id: native-redis-driver
title: "Standalone Rust Redis and Valkey Wheel (vertebra-redis)"
description: "Standalone PyO3 crate src/rs/vertebra-redis (vertebra._redis for vertebra-core[redis] and vertebra-core[valkey]) built on the redis 1.8.1 crate (tokio-rustls-comp, cluster-async, cache-aio) with pipelining, streams, Sentinel, Cluster routing, CLIENT TRACKING invalidation, and pub/sub NotificationSource, removing the Python socket driver and the vertebra-db RESP3 codec bindings."
status: draft
state: draft
has_chapters: false
parent_prd: native-data-platform
research:
  - ../native-data-platform/research/rust-driver-crates_20261103.md
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 2
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T04:05:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: 9af810f5
sources:
  - id: redis-py
    resource: src/py/vertebra/db/adapters/redis/driver.py
    title: Python SyncRedisDriver and AsyncRedisDriver target cutover module
  - id: valkey-py
    resource: src/py/vertebra/db/adapters/valkey/driver.py
    title: Python SyncValkeyDriver and AsyncValkeyDriver re-exporting vertebra._redis
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs
    title: Shared AsyncCompletionQueue + Notifier PyO3 bridge
  - id: resp3-rs
    resource: src/rs/vertebra-db/src/resp3/mod.rs
    title: RESP3 frame codecs in vertebra-db bound to Python for the socket driver
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
- **Shared Redis & Valkey Rust Wheel**: `src/rs/vertebra-redis` (`vertebra._redis`, `vertebra-core[redis]` and `vertebra-core[valkey]`) builds on the `redis` 1.8.1 crate (`tokio-rustls-comp`, `cluster-async`, `cache-aio`), which owns RESP3 framing, connection management, and cluster routing; this flow writes no hand-built RESP3 client [^crate-research] [^redis-py] [^valkey-py].
- **RESP3 Pipelining, Cluster & Pub/Sub**: `SyncRedisClient` and `AsyncRedisClient` provide `HELLO 3`, multiplexed pipelining, `SCAN`+`UNLINK`, Sentinel, Cluster `MOVED`/`ASK` routing, `CLIENT TRACKING` invalidations, and `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` [^py-async-rs].
- **Socket & Codec Removal**: Python raw socket / RESP parsers are deleted from the `redis` and `valkey` adapters, and the `vertebra_db::resp3` codec bindings on `vertebra._vertebra` are deleted at the same cutover [^redis-py] [^resp3-rs].
- **Fail-Loud Baseline**: `driver-fail-loud` already removes `memory://` from `RedisConfig.url_schemes` and every Redis/Valkey memory execution branch; this flow starts from that baseline [^fail-loud-baseline].
<!-- truth: end -->

## Approach

See [crate research](../native-data-platform/research/rust-driver-crates_20261103.md).

- **Redis & Valkey (`src/rs/vertebra-redis/` -> `vertebra._redis` + `vertebra-core[redis]` / `vertebra-core[valkey]`).** Build a standalone workspace PyO3 `cdylib` + `rlib` crate on `redis` `1.8.1` (`default-features = false`, `tokio-rustls-comp`, `cluster-async`, `connection-manager`, `cache-aio`, `streams`, `script`, `acl`) with `#[pymodule(gil_used = false)]`, `src/py/vertebra/_redis.pyi` verified by `mypy.stubtest`, `[project.optional-dependencies].{redis,valkey}`, wheel isolation `exclude` globs, `uv.lock`, and `areas.toml` registration, benchmarked against the Python socket driver it replaces. The `redis` crate owns RESP3 framing, so `vertebra-redis` adds no RESP3 codec to `vertebra-db` [^crate-research].
- **Fail-loud baseline.** The completed [`driver-fail-loud` baseline](../../knowledge/domains/data/database-drivers-and-dialers.md) deletes `memory://` support from the Redis and Valkey drivers and `RedisConfig.url_schemes` while keeping the `vertebra_db::resp3` bindings for the Python socket driver; the `from_redis` and `from_valkey` store factories stay and raise `ImproperConfigurationError` for `memory://` [^fail-loud-baseline] [^resp3-rs].
- **Explicit `Sync*` / `Async*` clients & notification sources.** Expose `SyncRedisClient` (`SyncCompletionQueue`) and `AsyncRedisClient` (`AsyncCompletionQueue` + `Notifier` via `vertebra_db::py_async` with `"_vertebra_redis_completion_drainer"`), plus `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` (`SyncNotificationSource` / `AsyncNotificationSource`) re-exported for Valkey.

## Scope

1. **Crate scaffold, RESP3 connection & pipelining (`src/rs/vertebra-redis/`).**
   - `SyncRedisClient` and `AsyncRedisClient` in `vertebra._redis` wrapping `redis` 1.8.1 multiplexed connections over `tokio-rustls-comp` (`HELLO 3`, ACL `AUTH`, GCP Memorystore IAM token refresh via `SyncGcpTokenCache` / `AsyncGcpTokenCache`), multiplexed pipelining (`execute_pipeline`), `MULTI`/`EXEC`/`WATCH` transactions, `SCAN` + `UNLINK`, millisecond `PX` expiries, and `DatabaseError` mapping (`vertebra._vertebra`).
2. **Pub/sub, streams, Sentinel, Cluster & client-side tracking.**
   - Pub/sub and sharded pub/sub (`SSUBSCRIBE`) implementing `SyncNotificationSource` and `AsyncNotificationSource` in `vertebra/db/adapters/redis/notify.py` (re-exported by `vertebra/db/adapters/valkey/notify.py`).
   - Streams (`XADD`, `XREADGROUP`, `XACK`), Sentinel master discovery (`discover_sentinel_master` / `async_discover_sentinel_master`), Cluster slot routing (`MOVED`/`ASK`) through `cluster-async`, and RESP3 `CLIENT TRACKING` invalidation push handling through `cache-aio`.
3. **Python `redis` and `valkey` cutover.**
   - Replace Python raw socket / `asyncio.open_connection` code in `vertebra/db/adapters/redis/driver.py` and the Python RESP parser in `vertebra/db/adapters/redis/core.py` with `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")` (and `extra="valkey"` in `valkey/driver.py`).
   - Delete the `vertebra_db::resp3` module, its `vertebra-py/src/db.rs` bindings (including `redis_hash_slot` and `parse_cluster_redirect`), and their `_vertebra.pyi` stubs in the same commit, since no Python caller remains once the socket driver goes.

## Public Seams

`vertebra._redis` (`SyncRedisClient`, `AsyncRedisClient`), `SyncRedisDriver`, `AsyncRedisDriver`, `SyncValkeyDriver`, `AsyncValkeyDriver`, `SyncRedisNotificationSource`, and `AsyncRedisNotificationSource`.

## Dependencies

`db-layout-and-contract` (`main @ 21c99f95`), `integration-remediation/db-engine-primitives` (`main @ 80ff8a65`), and the completed [`driver-fail-loud` baseline](../../knowledge/domains/data/database-drivers-and-dialers.md), which removes Redis and Valkey `memory://` execution; this flow starts from that baseline.

## Acceptance Boundary

- Integration tests pass natively against Redis 7/8 and Valkey 8 (standalone and cluster) via `pytest-databases`.
- RESP3 `HELLO 3`, pipelining, `SCAN`+`UNLINK`, pub/sub `NotificationSource`, `MOVED`/`ASK` slot routing, and `CLIENT TRACKING` invalidation unit/integration tests pass with zero Python socket fallback.
- `vertebra._vertebra` exports no RESP3 codec functions and `mypy.stubtest vertebra._vertebra` passes.
- Wheel isolation check confirms `vertebra-redis` builds cleanly without leaking `vertebra/*.py` files.
- `mypy.stubtest vertebra._redis`, `make bench-check`, `make lint-all`, `make rs-test`, and `make py-test` pass.

## Draft Task Outline

- t01 Scaffold `src/rs/vertebra-redis` (`vertebra._redis`, `_redis.pyi`, `[redis]` + `[valkey]` extras + `uv.lock` + `release.yaml`/`ci.yml`) and `redis`-crate client wrapper (`HELLO 3`, TLS, ACL/IAM auth, pipelining, `MULTI`/`EXEC`, `SCAN`+`UNLINK`).
- t02 Redis/Valkey pub/sub `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` and streams (`XADD`/`XREADGROUP`/`XACK`).
- t03 Sentinel discovery, `cluster-async` slot routing (`MOVED`/`ASK`), and RESP3 `CLIENT TRACKING` invalidation push handling.
- t04 Python `SyncRedisDriver`/`AsyncRedisDriver` & `SyncValkeyDriver`/`AsyncValkeyDriver` cutover, Python socket/RESP parser removal, `vertebra_db::resp3` binding and stub deletion, integration conformance, and docs/OKF update.

[^redis-py]: `src/py/vertebra/db/adapters/redis/driver.py` — Python SyncRedisDriver and AsyncRedisDriver target cutover module.
[^valkey-py]: `src/py/vertebra/db/adapters/valkey/driver.py` — Python SyncValkeyDriver and AsyncValkeyDriver re-exporting vertebra._redis.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs` — Shared AsyncCompletionQueue + Notifier PyO3 bridge.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — RESP3 frame codecs in vertebra-db bound to Python for the socket driver.
[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Rust client crate selection including `redis` 1.8.1.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed fail-loud baseline removing Redis and Valkey `memory://` execution.
