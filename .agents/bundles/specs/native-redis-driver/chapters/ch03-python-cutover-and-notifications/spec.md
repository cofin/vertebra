---
type: Spec
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: 'Chapter 03: Python Redis Adapter Cutover, Valkey Module Decoupling, and Sync/AsyncNotificationSource Split'
description: Decouple Resp3FakeServer and Valkey modules from Redis modules, cut Python Redis adapter over to vertebra._redis with zero socket fallback, split NotificationSource into SyncNotificationSource and AsyncNotificationSource across capabilities/Postgres/Oracle, extract ValkeyQueueBackend to framework/queues/valkey.py, and implement Redis Pub/Sub notification sources.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
depends_on_chapters:
  - ch02-rust-clients-topologies-and-streams
plan_commit: 473e1f2e
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:16:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:56:00Z'
    commit: 246d7d0e
tags:
  - redis
  - valkey
  - driver
  - cutover
  - notifications
  - capabilities
  - postgres
  - oracle
sources:
  - id: unit-conftest
    resource: src/py/tests/unit/conftest.py::Resp3FakeServer
    title: Resp3FakeServer loopback test fixture coupled to Resp3Parser
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::Resp3Parser
    title: Pure-Python Resp3Parser and RESP command encoders
  - id: valkey-core
    resource: src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore
    title: ValkeyCore currently re-exporting Redis RESP3 helpers
  - id: valkey-cfg
    resource: src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig
    title: ValkeyConfig currently subclassing RedisConfig
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Python socket/asyncio SyncRedisDriver and AsyncRedisDriver
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: SyncValkeyDriver and AsyncValkeyDriver currently subclassing Redis drivers
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource capability protocol in capabilities.py
  - id: driver-init
    resource: src/py/vertebra/db/driver/__init__.py
    title: Lazy capability exports in vertebra.db.driver
  - id: db-init
    resource: src/py/vertebra/db/__init__.py
    title: Lazy top-level exports in vertebra.db
  - id: channel-bus
    resource: src/py/vertebra/server/_channels.py::ChannelBus
    title: ChannelBus notification pump
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification source
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource
    title: Oracle AQ/TxEventQ notification source
  - id: ora-drv
    resource: src/py/vertebra/db/adapters/oracle/_async.py::AsyncOracleDriver
    title: Oracle driver notification_source factory
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: RedisQueueBackend and ValkeyQueueBackend currently co-located in queues/redis.py
  - id: queue-init
    resource: src/py/vertebra/framework/queues/__init__.py
    title: Framework queues package exports
current_task: "native-redis-driver:ch03-python-cutover-and-notifications:t01"
---

# Chapter 03: Python Redis Adapter Cutover, Valkey Module Decoupling, and Sync/AsyncNotificationSource Split

<!-- truth: start -->
- `src/py/tests/unit/conftest.py::Resp3FakeServer` moves onto a self-contained test-only `_parse_wire_frame` helper and `adapters/valkey/{config,core,driver,__init__}.py` removes all imports from `adapters/redis/` before `Resp3Parser` is deleted from `redis/core.py` and `SyncRedisDriver`/`AsyncRedisDriver` (`vertebra-core[redis]`) cut over to `vertebra._redis` [^unit-conftest] [^redis-core] [^valkey-core] [^valkey-cfg] [^redis-drv] [^valkey-drv].
- `capabilities.py` splits `NotificationSource` into `@runtime_checkable SyncNotificationSource` (`listen() -> Iterator[NotificationEvent]`) and `@runtime_checkable AsyncNotificationSource` (`listen() -> AsyncIterator[NotificationEvent]`) with zero compatibility aliases (`PY-07`), updating `ChannelBus`, `PostgresNotificationSource` (`Sync*`/`Async*`), and `OracleNotificationSource` (`Sync*`/`Async*`) [^driver-caps] [^driver-init] [^db-init] [^channel-bus] [^pg-notify] [^ora-notify] [^ora-drv].
- The thin `ValkeyQueueBackend(RedisQueueBackend)` subclass is extracted from `src/py/vertebra/framework/queues/redis.py`, together with a new `ValkeyStreamClient` protocol (copy of `RedisStreamClient`), into self-contained `src/py/vertebra/framework/queues/valkey.py` (with zero imports from `queues/redis.py`) so `queues/redis.py` owns `RedisQueueBackend` exclusively ahead of `ch04:t01` and sibling `native-valkey-driver` [^queue-redis] [^queue-init].
- `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` (`redis/notify.py`) implement `SyncNotificationSource` and `AsyncNotificationSource` over `SyncRedisListener` and `AsyncRedisListener` (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) [^driver-caps].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/tests/unit/conftest.py` imports `Resp3Parser` at module top level [^unit-conftest], `adapters/valkey/{config,core,driver}.py` imports from `adapters/redis/` [^valkey-core] [^valkey-cfg] [^valkey-drv], and `ValkeyQueueBackend` lives inside `framework/queues/redis.py` [^queue-redis]. Meanwhile, `capabilities.py::NotificationSource` is async-only [^driver-caps]. Decoupling `Resp3FakeServer` and `adapters/valkey/` first, cutting `adapters/redis/` onto `vertebra._redis`, splitting `NotificationSource` symmetrically across PostgreSQL and Oracle, extracting `framework/queues/valkey.py`, and delivering `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` keeps all Valkey tests green while sibling [`native-valkey-driver`](../../../native-valkey-driver/spec.md) cuts Valkey over to `vertebra._valkey` and deletes `vertebra_db::resp3`.

**Non-Goals:** Redis Streams queue backend cutover (`ch04:t01`); Valkey `vertebra._valkey` driver/queue/notification cutover and `vertebra_db::resp3` deletion (`native-valkey-driver`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Fixture, Valkey decoupling & Redis driver | `tests/unit/conftest.py`, `adapters/redis/*`, `adapters/valkey/*` [^unit-conftest] [^redis-core] [^valkey-core] [^redis-drv] [^valkey-drv] | Self-contained `_parse_wire_frame` and `adapters/valkey/`; native `SyncRedisDriver`/`AsyncRedisDriver` over `vertebra._redis` |
| Notification split | `db/driver/{capabilities,__init__}.py`, `db/__init__.py`, `server/_channels.py`, `postgres/notify.py`, `oracle/{notify,driver}.py` [^driver-caps] [^channel-bus] [^pg-notify] [^ora-notify] | `SyncNotificationSource` & `AsyncNotificationSource` across Postgres and Oracle |
| Queue module split | `framework/queues/{redis,valkey,__init__}.py` [^queue-redis] [^queue-init] | Self-contained `queues/valkey.py` (`ValkeyQueueBackend`, `ValkeyStreamClient`) with zero `queues/redis.py` imports |
| Redis notify | `adapters/redis/{notify,__init__}.py` [^driver-caps] | `SyncRedisNotificationSource` & `AsyncRedisNotificationSource` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10 / R11 / R12**: Inline a test-only `_parse_wire_frame` helper in `src/py/tests/unit/conftest.py::Resp3FakeServer`, decouple `adapters/valkey/{config,core,driver,__init__}.py` from `adapters/redis/*` (subclassing `BaseDatabaseConfig` directly in `ValkeyConfig`), delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` from `redis/core.py` and `redis/__init__.py`, delegate `crc16_xmodem`/`hash_slot`/`parse_cluster_redirect` in `redis/core.py` to `vertebra._redis`, and rewrite `SyncRedisDriver`/`AsyncRedisDriver` (`extra="redis"`) over `vertebra._redis` + `spawn_rust_db_task_with_attr_and_cancel` (`t01`).
- **R13**: Replace `NotificationSource` with `@runtime_checkable SyncNotificationSource` and `@runtime_checkable AsyncNotificationSource` in `capabilities.py`, `driver/__init__.py`, `db/__init__.py`, and `server/_channels.py`, and split `PostgresNotificationSource` + `OracleNotificationSource` into symmetric `Sync*`/`Async*` pairs with zero aliases, adding native blocking `SyncPostgresListener` (`src/rs/vertebra-postgres/src/async_client.rs`, `_postgres.pyi`) and `SyncOracleListener` (`src/rs/vertebra-oracle/src/notify.rs`, `_oracle.pyi`) beside the existing async listeners, plus `SyncOracleDriver.notification_source()` (`t02`).
- **R14**: Extract `ValkeyQueueBackend` and define a new `ValkeyStreamClient` protocol in dedicated `src/py/vertebra/framework/queues/valkey.py` (updating `framework/queues/__init__.py` lazy exports, the `framework/queues/backends.py` re-export, and the `QueueService._build_backend` dotted path in `framework/queues/base.py`; client construction stays on `redis.asyncio` until `native-valkey-driver:ch03:t04`) with zero cross-module imports from `queues/redis.py` (`t03`).
- **R16**: Implement `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` (`redis/notify.py` and `redis/__init__.py`) over `SyncRedisListener` / `AsyncRedisListener` (`subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `publish`, `listen()`, `ChangeFeed` hydration) (`t04`).

```python
class SyncRedisDriver:
    def pipeline(self) -> SyncRedisPipeline: ...
    def close(self) -> None: ...

class AsyncRedisDriver:
    def pipeline(self) -> AsyncRedisPipeline: ...
    async def close(self) -> None: ...

@runtime_checkable
class SyncNotificationSource(Protocol):
    def publish(self, channel: str, payload: bytes | str) -> None: ...
    def subscribe(self, channels: Sequence[str]) -> None: ...
    def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> Iterator[NotificationEvent]: ...

@runtime_checkable
class AsyncNotificationSource(Protocol):
    async def publish(self, channel: str, payload: bytes | str) -> None: ...
    async def subscribe(self, channels: Sequence[str]) -> None: ...
    async def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> AsyncIterator[NotificationEvent]: ...

class SyncRedisNotificationSource(SyncNotificationSource):
    def __init__(self, driver_or_config: SyncRedisDriver | RedisConfig | None = None, *, change_feed: ChangeFeed | None = None) -> None: ...
    def psubscribe(self, patterns: Sequence[str]) -> None: ...
    def ssubscribe(self, shard_channels: Sequence[str]) -> None: ...
    def close(self) -> None: ...

class AsyncRedisNotificationSource(AsyncNotificationSource): ...
```

`SyncRedisDriver`/`AsyncRedisDriver` probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")`, preserving store/cache methods (`get`, `set(ex=, px=, nx=, xx=)`, `delete`, `unlink`, `scan`, `pttl`, `flushdb`, `eval_script`, `discover_sentinel_master`, `add_invalidation_listener`, `cached_keys`) and using completion drainer attribute `"_vertebra_redis_completion_drainer"`.

**Integration Flow:**
- `KeyValueStore`, Litestar `RedisStore`, Starlette `CacheStore`, and `SyncCacheBackend`/`AsyncCacheBackend` continue calling `SyncRedisDriver`/`AsyncRedisDriver` unchanged.
- `ChannelBus(notification_source=AsyncRedisNotificationSource(driver))` streams Redis pub/sub frames into SSE/WebSocket channels while synchronous workers consume `SyncNotificationSource.listen()`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Wheel missing | `vertebra._redis` absent | `MissingDependencyError` naming `vertebra-core[redis]` | Distinct extra hints |
| Cross-adapter import check | `git grep 'vertebra.db.adapters.redis' src/py/vertebra/db/adapters/valkey` | Zero matches | Self-contained Valkey adapter |
| Legacy unprefixed import | `from vertebra.db.driver import NotificationSource` | Raises `ImportError` (`PY-07` clean break, zero aliases) | Strict `Sync*`/`Async*` symmetry |
| Pooled PgBouncer tx mode | `SyncPostgresNotificationSource` or `AsyncPostgresNotificationSource` without `direct_dsn` | Raises `ImproperConfigurationError` | Direct/session `LISTEN` invariant |
| Broken `async for` loop | Consumer exits `async for event in source.listen():` via `break` | Dedicated `AsyncRedisListener` closes deterministically | Zero leaked pub/sub sockets |

## 3. Documentation & OKF Knowledge Plan

Driver cutover docs, `SyncNotificationSource`/`AsyncNotificationSource` updates in `docs/usage/queues-and-realtime.md`, `docs/changelog/index.md`, and `vertebra-redis` entries in `.agents/bundles/product/product.md` execute in `ch04:t03` and `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Decouple Valkey Adapter and Cut Python Redis Core and Drivers Over to vertebra._redis | `src/py/vertebra/db/adapters/{redis,valkey}/{config,core,driver,__init__}.py`, `src/py/tests/unit/conftest.py` | `src/py/tests/unit/db/test_redis_valkey_driver.py`, `src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| `t02` | Split NotificationSource into SyncNotificationSource and AsyncNotificationSource Across Capabilities, Postgres, and Oracle | `src/py/vertebra/db/{__init__,driver/capabilities,driver/__init__}.py`, `src/py/vertebra/server/_channels.py`, `src/py/vertebra/db/adapters/{postgres,oracle}/*` | `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_postgres_native_notifications.py`, `src/py/tests/unit/db/test_oracle_adapter.py` |
| `t03` | Extract Self-Contained ValkeyQueueBackend into src/py/vertebra/framework/queues/valkey.py | `src/py/vertebra/framework/queues/{redis,valkey,__init__}.py` | `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t04` | Implement Sync/Async Redis Pub/Sub Notification Sources | `src/py/vertebra/db/adapters/redis/{notify,__init__}.py` | `src/py/tests/unit/db/test_redis_valkey_resp3.py` |

## 5. Implementation Plan

- [ ] [t01-python-redis-and-valkey-driver-cutover.md](tasks/t01-python-redis-and-valkey-driver-cutover.md) — Relocate test loopback wire parser to test helpers, decouple `adapters/valkey/` from `adapters/redis/`, and rewrite `RedisCore`, `SyncRedisDriver`, and `AsyncRedisDriver` onto `vertebra._redis`.
- [ ] [t02-notification-source-protocol-split.md](tasks/t02-notification-source-protocol-split.md) — Define `@runtime_checkable SyncNotificationSource` and `AsyncNotificationSource` in `capabilities.py` and `ChannelBus`, and split Postgres and Oracle notification sources.
- [ ] [t03-remove-legacy-resp3-bindings.md](tasks/t03-remove-legacy-resp3-bindings.md) — Extract `ValkeyQueueBackend` and `ValkeyStreamClient` from `framework/queues/redis.py` into self-contained `framework/queues/valkey.py` with zero cross-module imports.
- [ ] [t04-redis-and-valkey-pubsub-notifications.md](tasks/t04-redis-and-valkey-pubsub-notifications.md) — Implement `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` over `vertebra._redis` Pub/Sub.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: Zero imports from `adapters/redis/` or `queues/redis.py` in `valkey` modules; zero unprefixed `NotificationSource` aliases remain.
- **Fast Chapter Gate (`< 60s`)**: `make dev && uv run --no-sync python -m mypy.stubtest vertebra._redis vertebra._postgres vertebra._oracle --concise && uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_postgres_native_notifications.py src/py/tests/unit/db/test_oracle_adapter.py src/py/tests/unit/framework/test_distributed_queues.py -q && uv run --no-sync mypy src/py/vertebra/db src/py/vertebra/framework/queues src/py/vertebra/server/_channels.py`

[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore currently re-exporting Redis RESP3 helpers.
[^valkey-cfg]: `src/py/vertebra/db/adapters/valkey/config.py::ValkeyConfig` — ValkeyConfig currently subclassing RedisConfig.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver currently subclassing Redis drivers.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^driver-init]: `src/py/vertebra/db/driver/__init__.py` — Lazy capability exports in vertebra.db.driver.
[^db-init]: `src/py/vertebra/db/__init__.py` — Lazy top-level exports in vertebra.db.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — ChannelBus notification pump.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/_async.py::AsyncOracleDriver` — Oracle driver notification_source factory.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend currently co-located in queues/redis.py.
[^queue-init]: `src/py/vertebra/framework/queues/__init__.py` — Framework queues package exports.
