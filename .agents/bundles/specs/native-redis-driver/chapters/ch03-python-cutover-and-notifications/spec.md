---
type: Spec
flow_id: native-redis-driver
chapter_id: ch03-python-cutover-and-notifications
title: 'Chapter 03: Python Redis/Valkey Adapter Cutover, Sync/AsyncNotificationSource Split, and RESP3 Cleanup'
description: Cut Python Redis and Valkey adapters over to vertebra._redis with zero socket fallback, split NotificationSource into SyncNotificationSource and AsyncNotificationSource across capabilities/Postgres/Oracle, remove legacy _vertebra RESP3 bindings, and implement Redis/Valkey Pub/Sub notification sources.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 104fd6fa
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
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
  title: ValkeyCore re-exporting Redis RESP3 helpers
- id: redis-drv
  resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
  title: Python socket/asyncio SyncRedisDriver and AsyncRedisDriver
- id: valkey-drv
  resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
  title: SyncValkeyDriver and AsyncValkeyDriver
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
  resource: src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver
  title: Oracle driver notification_source factory
- id: resp3-bindings
  resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
  title: Legacy RESP3 helper bindings on vertebra._vertebra
- id: vertebra-pyi
  resource: src/py/vertebra/_vertebra.pyi
  title: Core vertebra._vertebra stub declarations
- id: resp3-rs
  resource: src/rs/vertebra-db/src/resp3/mod.rs
  title: Legacy RESP3 codec module in vertebra-db
- id: db-lib
  resource: src/rs/vertebra-db/src/lib.rs
  title: vertebra-db crate root re-exporting resp3
---

# Chapter 03: Python Redis/Valkey Adapter Cutover, Sync/AsyncNotificationSource Split, and RESP3 Cleanup

<!-- truth: start -->
- `src/py/tests/unit/conftest.py::Resp3FakeServer` moves onto a self-contained test-only `_parse_wire_frame` helper BEFORE `Resp3Parser` is deleted so pytest collection across `src/py/tests/unit/` never breaks [^unit-conftest] [^redis-core].
- `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` are removed from `redis/core.py`, `valkey/core.py`, `redis/__init__.py`, and `valkey/__init__.py` together [^redis-core] [^valkey-core].
- `SyncRedisDriver`/`AsyncRedisDriver` (`vertebra-core[redis]`) and `SyncValkeyDriver`/`AsyncValkeyDriver` (`vertebra-core[valkey]`) wrap `vertebra._redis` (`SyncRedisClient`, `AsyncRedisClient`, `SyncRedisPipeline`, `AsyncRedisPipeline`) with zero Python `socket` / `asyncio.open_connection` transport code [^redis-drv] [^valkey-drv].
- `src/py/vertebra/db/driver/capabilities.py` splits `NotificationSource` into `@runtime_checkable SyncNotificationSource` (`listen() -> Iterator[NotificationEvent]`) and `@runtime_checkable AsyncNotificationSource` (`listen() -> AsyncIterator[NotificationEvent]`) with leading `Sync*`/`Async*` prefixes and zero compatibility aliases (`PY-07`) [^driver-caps] [^driver-init] [^db-init].
- `ChannelBus` (`src/py/vertebra/server/_channels.py`) types `notification_source` as `AsyncNotificationSource | None` [^channel-bus].
- `PostgresNotificationSource` splits into `SyncPostgresNotificationSource` (over `SyncPostgresListener` / dedicated direct-or-session sync `LISTEN` connection) and `AsyncPostgresNotificationSource` (over `AsyncPostgresListener`) [^pg-notify].
- `OracleNotificationSource` splits into `SyncOracleNotificationSource` (`SyncOracleListener` over dedicated `SyncOracleClient` AQ dequeue) and `AsyncOracleNotificationSource` (`AsyncOracleListener`), with `SyncOracleDriver.notification_source()` and `AsyncOracleDriver.notification_source()` returning their respective mode [^ora-notify] [^ora-drv].
- `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` are removed from `vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi` [^resp3-bindings] [^vertebra-pyi].
- `src/rs/vertebra-db/src/resp3/mod.rs` and its `vertebra-db/src/lib.rs` exports are deleted completely [^resp3-rs] [^db-lib].
- `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` (`redis/notify.py`) and `SyncValkeyNotificationSource` / `AsyncValkeyNotificationSource` (`valkey/notify.py`) implement `SyncNotificationSource` and `AsyncNotificationSource` over `SyncRedisListener` and `AsyncRedisListener` (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) [^driver-caps].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/tests/unit/conftest.py` imports `Resp3Parser` from `vertebra.db.adapters.redis.core` at module top level [^unit-conftest], and `valkey/core.py` re-exports `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` from `redis/core.py` [^valkey-core]. Decoupling `conftest.py` in `t01`, updating `redis/core.py` + `valkey/core.py` together in `t02`, and cutting both drivers over in `t03` eliminates 1,600+ lines of Python socket/parser code while keeping `KeyValueStore`, `MemoryStore`, `CacheStore`, and `SQLAlchemyCacheBackend` green.

**Non-Goals:** `NotificationSource` capability split (`ch03`); `vertebra-db::resp3` Rust removal and Redis/Valkey notification sources (`ch03`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Loopback fixture | `src/py/tests/unit/conftest.py::Resp3FakeServer` [^unit-conftest] | Self-contained test-only `_parse_wire_frame` |
| Core modules | `adapters/redis/{core,__init__}.py`, `adapters/valkey/core.py` [^redis-core] [^valkey-core] | `crc16_xmodem`, `hash_slot`, `parse_cluster_redirect` delegated to `vertebra._redis` |
| Drivers | `adapters/redis/driver.py`, `adapters/valkey/{driver,__init__}.py` [^redis-drv] [^valkey-drv] | Thin native wrappers preserving store/cache contracts |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

Hoisting the `NotificationSource` -> `SyncNotificationSource` / `AsyncNotificationSource` capability split into `native-redis-driver` (User Decision `A3`) keeps the cross-flow DAG strictly acyclic (`native-postgres-driver -> native-redis-driver -> queues-tasks-and-channels`) and ensures every database/cache notification transport ships symmetric sync (Django, Flask, CLI, synchronous workers) and async (`ChannelBus`, ASGI) classes before `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` land in `ch03` [^driver-caps] [^channel-bus].

**Non-Goals:** Redis and Valkey notification sources (`t04`); Distributed channel fan-out extensions (`queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Capabilities & bus | `db/driver/{capabilities,__init__}.py`, `server/_channels.py` [^driver-caps] [^channel-bus] | `SyncNotificationSource` & `AsyncNotificationSource` (`@runtime_checkable`) |
| PostgreSQL notify | `db/__init__.py`, `db/adapters/postgres/{notify,__init__}.py` [^db-init] [^pg-notify] | `SyncPostgresNotificationSource` & `AsyncPostgresNotificationSource` |
| Oracle notify | `db/adapters/oracle/{notify,driver,__init__}.py` [^ora-notify] [^ora-drv] | `SyncOracleListener`, `SyncOracleNotificationSource`, `AsyncOracleNotificationSource` |

**Knowledge Anchors:** [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [PostgreSQL Native Driver](../../../../knowledge/domains/data/postgres-native-driver.md).

Once `t01` cuts Redis and Valkey adapters onto `vertebra._redis` and `t02` lands `SyncNotificationSource` / `AsyncNotificationSource`, no caller remains for `vertebra_db::resp3` or its `_vertebra` bindings [^resp3-rs] [^resp3-bindings]. Deleting them and adding dedicated Redis and Valkey `Sync*`/`Async*` notification sources completes the pub/sub transport plane.

**Non-Goals:** Distributed queue stream backends (`ch04:t01`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| `_vertebra` cleanup | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi` [^resp3-bindings] [^vertebra-pyi] | RESP3 helper bindings removed |
| `vertebra-db` cleanup | `src/rs/vertebra-db/src/{resp3/mod.rs,lib.rs}` [^resp3-rs] [^db-lib] | 475-line `resp3` codec module deleted |
| Redis & Valkey notify | `adapters/redis/{notify,__init__}.py`, `adapters/valkey/{notify,__init__}.py` [^driver-caps] | `SyncRedisNotificationSource`, `AsyncRedisNotificationSource`, and Valkey subclasses |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10**: Inline a self-contained test-only `_parse_wire_frame` helper into `src/py/tests/unit/conftest.py::Resp3FakeServer` and migrate unit tests off `Resp3Parser`/`Resp3Error`/`socket_factory` (`t01`).
- **R11**: Delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` from `redis/core.py`, `valkey/core.py`, and `redis/__init__.py`, delegating `crc16_xmodem`, `hash_slot`, and `parse_cluster_redirect` to `vertebra._redis` (`t02`).
- **R12**: Rewrite `SyncRedisDriver`/`AsyncRedisDriver` (`extra="redis"`) and `SyncValkeyDriver`/`AsyncValkeyDriver` (`extra="valkey"`) over `vertebra._redis` + `spawn_rust_db_task_with_attr_and_cancel`, preserving all downstream store/cache methods (`t03`).

```python
class SyncRedisDriver:
    # Probes ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
    # Preserves get, set(ex=, px=, nx=, xx=), delete, unlink, scan, pttl, flushdb, pipeline(),
    # eval_script, discover_sentinel_master, add_invalidation_listener, cached_keys, close()

class AsyncRedisDriver:
    # Probes ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")
    # Uses AsyncRedisClient + spawn_rust_db_task_with_attr_and_cancel ("_vertebra_redis_completion_drainer")

class SyncValkeyDriver(SyncRedisDriver): ...  # extra="valkey" -> vertebra-core[valkey]
class AsyncValkeyDriver(AsyncRedisDriver): ...  # extra="valkey" -> vertebra-core[valkey]
```

**Integration Flow:**
- `KeyValueStore`, Litestar `MemoryStore`, Starlette `CacheStore`, and `SQLAlchemyCacheBackend` continue calling `SyncRedisDriver`/`AsyncRedisDriver` and `SyncValkeyDriver`/`AsyncValkeyDriver` unchanged.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Wheel missing | `vertebra._redis` absent | `MissingDependencyError` naming `vertebra-core[redis]` or `vertebra-core[valkey]` | Distinct extra hints |
| `memory://` DSN | `RedisConfig(url="memory://")` | Raises `ImproperConfigurationError` | No in-memory fake |

- **R13a**: Replace `NotificationSource` with `@runtime_checkable SyncNotificationSource` and `@runtime_checkable AsyncNotificationSource` in `capabilities.py`, `driver/__init__.py`, and `server/_channels.py` (`t01`).
- **R13b**: Update `vertebra/db/__init__.py` exports and split `PostgresNotificationSource` into `SyncPostgresNotificationSource` and `AsyncPostgresNotificationSource` with zero aliases (`t02`).
- **R13c**: Split `OracleNotificationSource` into `SyncOracleNotificationSource` (`SyncOracleListener`) and `AsyncOracleNotificationSource` (`AsyncOracleListener`) across `oracle/{notify,driver,__init__}.py` (`t03`).

```python
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
```

**Integration Flow:**
- `ChannelBus` consumes `AsyncNotificationSource`; synchronous callers/workers consume `SyncNotificationSource`.
- `t04` implements `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` and `SyncValkeyNotificationSource` / `AsyncValkeyNotificationSource` directly against these protocols.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Legacy unprefixed import | `from vertebra.db.driver import NotificationSource` | Raises `ImportError` (`PY-07` clean break, zero aliases) | Strict `Sync*`/`Async*` symmetry |
| Pooled PgBouncer tx mode | `SyncPostgresNotificationSource` or `AsyncPostgresNotificationSource` without `direct_dsn` | Raises `ImproperConfigurationError` | Direct/session `LISTEN` invariant |

- **R14**: Remove `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` from `vertebra-py/src/db.rs` and `_vertebra.pyi` (`t01`).
- **R15**: Delete `src/rs/vertebra-db/src/resp3/mod.rs` and remove `pub mod resp3;` from `src/rs/vertebra-db/src/lib.rs` (`t02`).
- **R16a**: Implement `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` in `adapters/redis/notify.py` supporting `subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `publish`, `listen()`, and `ChangeFeed` hydration (`t03`).
- **R16b**: Implement `SyncValkeyNotificationSource` and `AsyncValkeyNotificationSource` in `adapters/valkey/notify.py` defaulting `ValkeyConfig` and probing `vertebra-core[valkey]` (`t04`).

```python
class SyncRedisNotificationSource(SyncNotificationSource):
    def __init__(self, driver_or_config: SyncRedisDriver | RedisConfig | None = None, *, change_feed: ChangeFeed | None = None) -> None: ...
    def publish(self, channel: str, payload: bytes | str) -> None: ...
    def subscribe(self, channels: Sequence[str]) -> None: ...
    def psubscribe(self, patterns: Sequence[str]) -> None: ...
    def ssubscribe(self, shard_channels: Sequence[str]) -> None: ...
    def unsubscribe(self, channels: Sequence[str]) -> None: ...
    def listen(self) -> Iterator[NotificationEvent]: ...
    def close(self) -> None: ...

class AsyncRedisNotificationSource(AsyncNotificationSource):
    # Async mirror over AsyncRedisListener + AsyncRedisDriver, with aclose() and async context manager
    ...

class SyncValkeyNotificationSource(SyncRedisNotificationSource): ...
class AsyncValkeyNotificationSource(AsyncRedisNotificationSource): ...
```

**Integration Flow:**
- `ChannelBus(notification_source=AsyncRedisNotificationSource(driver))` streams live Redis/Valkey pub/sub frames into SSE/WebSocket channels.
- Synchronous CLI/Flask/Django workers consume `SyncRedisNotificationSource.listen()` directly.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Core `_vertebra` probe | `hasattr(vertebra._vertebra, "encode_resp_command")` | `False`; `mypy.stubtest vertebra._vertebra` passes | Clean core binary |
| Broken `async for` loop | Consumer exits `async for event in source.listen():` via `break` | Dedicated `AsyncRedisListener` closes deterministically | Zero leaked pub/sub sockets |

## 3. Documentation & OKF Knowledge Plan

Driver cutover docs execute in `ch04:t03` and `ch04:t04`.

Capability split is documented in `docs/usage/queues-and-realtime.md` (`ch04:t03`) and `changelog` (`ch04:t03`).

Pub/sub notification sources and `vertebra-db` table updates are documented in `ch04:t03`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Cut Python Redis and Valkey Core and Drivers Over to vertebra._redis | `src/py/vertebra/db/adapters/redis/core.py`, `src/py/vertebra/db/adapters/valkey/core.py`, `src/py/vertebra/db/adapters/redis/__init__.py` (+3 more) | `src/py/tests/unit/conftest.py`, `src/py/tests/unit/db/test_redis_valkey_driver.py` |
| `t02` | Split NotificationSource into SyncNotificationSource and AsyncNotificationSource Across Capabilities, Postgres, and Oracle | `src/py/vertebra/db/driver/capabilities.py`, `src/py/vertebra/db/driver/__init__.py`, `src/py/vertebra/server/_channels.py` (+6 more) | `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py` |
| `t03` | Remove Legacy RESP3 Bindings from vertebra-py and Delete vertebra-db/src/resp3.rs | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi`, `src/rs/vertebra-db/src/resp3/mod.rs` (+1 more) | `src/py/tests/unit/db/test_redis_valkey_resp3.py`, `src/rs/vertebra-redis/tests/redis_client.rs` |
| `t04` | Implement Sync/Async Redis and Valkey Pub/Sub Notification Sources | `src/py/vertebra/db/adapters/redis/notify.py`, `src/py/vertebra/db/adapters/redis/__init__.py`, `src/py/vertebra/db/adapters/valkey/notify.py` (+1 more) | `src/py/tests/unit/db/test_redis_valkey_resp3.py` |

## 5. Implementation Plan

- [ ] [t01-python-redis-and-valkey-driver-cutover.md](tasks/t01-python-redis-and-valkey-driver-cutover.md) — Relocate test loopback wire parser to test helpers and rewrite RedisCore, ValkeyCore, Sync/AsyncRedisDriver, and Sync/AsyncValkeyDriver onto vertebra._redis.
- [ ] [t02-notification-source-protocol-split.md](tasks/t02-notification-source-protocol-split.md) — Define @runtime_checkable SyncNotificationSource and AsyncNotificationSource in capabilities.py and ChannelBus, and split Postgres and Oracle notification sources.
- [ ] [t03-remove-legacy-resp3-bindings.md](tasks/t03-remove-legacy-resp3-bindings.md) — Delete resp3_*, redis_*, crc16_xmodem, and redis_hash_slot from vertebra-py/_vertebra.pyi and remove src/rs/vertebra-db/src/resp3.rs.
- [ ] [t04-redis-and-valkey-pubsub-notifications.md](tasks/t04-redis-and-valkey-pubsub-notifications.md) — Implement SyncRedisNotificationSource, AsyncRedisNotificationSource, SyncValkeyNotificationSource, and AsyncValkeyNotificationSource over vertebra._redis Pub/Sub.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `vertebra._vertebra` and `vertebra-db` carry zero Redis/RESP3 codec code after `t02`.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db -p vertebra-py && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_resp3.py`

[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^valkey-core]: `src/py/vertebra/db/adapters/valkey/core.py::ValkeyCore` — ValkeyCore re-exporting Redis RESP3 helpers.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^driver-init]: `src/py/vertebra/db/driver/__init__.py` — Lazy capability exports in vertebra.db.driver.
[^db-init]: `src/py/vertebra/db/__init__.py` — Lazy top-level exports in vertebra.db.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — ChannelBus notification pump.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py::AsyncOracleDriver` — Oracle driver notification_source factory.
[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy RESP3 helper bindings on vertebra._vertebra.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Legacy RESP3 codec module in vertebra-db.
[^db-lib]: `src/rs/vertebra-db/src/lib.rs` — vertebra-db crate root re-exporting resp3.
