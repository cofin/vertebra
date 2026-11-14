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
depends_on_chapters:
- ch02-rust-clients-topologies-and-streams
plan_commit: 1b191388
updated_at: '2026-11-14T23:16:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:16:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T23:16:00Z'
  commit: 1b191388
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
- `src/py/tests/unit/conftest.py::Resp3FakeServer` moves onto a self-contained test-only `_parse_wire_frame` helper before `Resp3Parser` is deleted so pytest collection across `src/py/tests/unit/` stays green [^unit-conftest] [^redis-core].
- `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake` are deleted across `redis/core.py`, `valkey/core.py`, `redis/__init__.py`, and `valkey/__init__.py`, and `SyncRedisDriver`/`AsyncRedisDriver` (`vertebra-core[redis]`) + `SyncValkeyDriver`/`AsyncValkeyDriver` (`vertebra-core[valkey]`) execute over `vertebra._redis` with zero Python `socket` or `asyncio.open_connection` transport code [^redis-core] [^valkey-core] [^redis-drv] [^valkey-drv].
- `capabilities.py` splits `NotificationSource` into `@runtime_checkable SyncNotificationSource` (`listen() -> Iterator[NotificationEvent]`) and `@runtime_checkable AsyncNotificationSource` (`listen() -> AsyncIterator[NotificationEvent]`) with zero compatibility aliases (`PY-07`), updating `ChannelBus`, `PostgresNotificationSource` (`Sync*`/`Async*`), and `OracleNotificationSource` (`Sync*`/`Async*`) [^driver-caps] [^driver-init] [^db-init] [^channel-bus] [^pg-notify] [^ora-notify] [^ora-drv].
- Legacy RESP3 bindings (`encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, `parse_cluster_redirect`) are removed from `vertebra-py/src/db.rs` and `_vertebra.pyi`, and `src/rs/vertebra-db/src/resp3/mod.rs` is deleted [^resp3-bindings] [^vertebra-pyi] [^resp3-rs] [^db-lib].
- `SyncRedisNotificationSource` / `AsyncRedisNotificationSource` (`redis/notify.py`) and `SyncValkeyNotificationSource` / `AsyncValkeyNotificationSource` (`valkey/notify.py`) implement `SyncNotificationSource` and `AsyncNotificationSource` over `SyncRedisListener` and `AsyncRedisListener` (`SUBSCRIBE`, `PSUBSCRIBE`, `SSUBSCRIBE`) [^driver-caps].
<!-- truth: end -->

## 1. Specification, Context & Provenance

`src/py/tests/unit/conftest.py` imports `Resp3Parser` at module top level [^unit-conftest], `valkey/core.py` re-exports `Resp3Parser` and `encode_*` from `redis/core.py` [^valkey-core], and pure-Python socket/asyncio Redis/Valkey drivers duplicate 1,600+ lines over `vertebra_db::resp3` `_vertebra` bindings [^redis-drv] [^resp3-bindings] [^resp3-rs]. Meanwhile, `capabilities.py::NotificationSource` is async-only [^driver-caps]. Decoupling `Resp3FakeServer` first, cutting Redis/Valkey adapters onto `vertebra._redis`, splitting `NotificationSource` symmetrically across PostgreSQL and Oracle, removing dead `resp3` Rust code, and delivering `Sync*`/`Async*` Redis and Valkey pub/sub notification sources keeps the cross-flow DAG strictly acyclic (`native-postgres-driver -> native-redis-driver -> queues-tasks-and-channels`).

**Non-Goals:** Streams queue backend cutover (`ch04:t01`); distributed channel fan-out / Litestar / Django integration (`queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Fixture & drivers | `tests/unit/conftest.py`, `adapters/redis/*`, `adapters/valkey/*` [^unit-conftest] [^redis-core] [^valkey-core] [^redis-drv] [^valkey-drv] | Self-contained `_parse_wire_frame`, thin native `Sync*`/`Async*` drivers preserving store/cache APIs |
| Notification split | `db/driver/{capabilities,__init__}.py`, `db/__init__.py`, `server/_channels.py`, `postgres/notify.py`, `oracle/{notify,driver}.py` [^driver-caps] [^channel-bus] [^pg-notify] [^ora-notify] | `SyncNotificationSource` & `AsyncNotificationSource` across Postgres and Oracle |
| Legacy RESP3 removal | `vertebra-py/src/db.rs`, `_vertebra.pyi`, `vertebra-db/src/{resp3/mod.rs,lib.rs}` [^resp3-bindings] [^resp3-rs] [^db-lib] | RESP3 codec and bindings deleted completely from core crate |
| Redis & Valkey notify | `adapters/redis/{notify,__init__}.py`, `adapters/valkey/{notify,__init__}.py` [^driver-caps] | `SyncRedisNotificationSource`, `AsyncRedisNotificationSource`, and Valkey subclasses |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [Query Pipeline, CDC, and Tenancy](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [Polyglot Test Matrix](../../../../knowledge/testing/polyglot-test-matrix.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R10 / R11 / R12**: Inline a test-only `_parse_wire_frame` helper in `src/py/tests/unit/conftest.py::Resp3FakeServer`, migrate tests off `socket_factory`/`Resp3Error`, delete `Resp3Parser`, `Resp3Error`, `encode_resp_command`, and `encode_hello3_handshake`, delegate `crc16_xmodem`/`hash_slot`/`parse_cluster_redirect` to `vertebra._redis`, and rewrite `SyncRedisDriver`/`AsyncRedisDriver` (`extra="redis"`) and `SyncValkeyDriver`/`AsyncValkeyDriver` (`extra="valkey"`) over `vertebra._redis` + `spawn_rust_db_task_with_attr_and_cancel` (`t01`).
- **R13**: Replace `NotificationSource` with `@runtime_checkable SyncNotificationSource` and `@runtime_checkable AsyncNotificationSource` in `capabilities.py`, `driver/__init__.py`, `db/__init__.py`, and `server/_channels.py`, and split `PostgresNotificationSource` + `OracleNotificationSource` (`SyncOracleListener`, `SyncOracleDriver.notification_source()`) into symmetric `Sync*`/`Async*` pairs with zero aliases (`t02`).
- **R14 / R15**: Delete `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` from `vertebra-py/src/db.rs` and `_vertebra.pyi`, and delete `src/rs/vertebra-db/src/resp3/mod.rs` and `pub mod resp3;` from `vertebra-db/src/lib.rs` (`t03`).
- **R16**: Implement `SyncRedisNotificationSource`, `AsyncRedisNotificationSource`, `SyncValkeyNotificationSource`, and `AsyncValkeyNotificationSource` over `SyncRedisListener` / `AsyncRedisListener` (`subscribe`, `psubscribe`, `ssubscribe`, `unsubscribe`, `publish`, `listen()`, `ChangeFeed` hydration) (`t04`).

```python
class SyncRedisDriver:
    def pipeline(self) -> SyncRedisPipeline: ...
    def close(self) -> None: ...

class AsyncRedisDriver:
    def pipeline(self) -> AsyncRedisPipeline: ...
    async def close(self) -> None: ...

class SyncValkeyDriver(SyncRedisDriver): ...
class AsyncValkeyDriver(AsyncRedisDriver): ...

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
class SyncValkeyNotificationSource(SyncRedisNotificationSource): ...
class AsyncValkeyNotificationSource(AsyncRedisNotificationSource): ...
```

`SyncRedisDriver`/`AsyncRedisDriver` probe `ensure_module("vertebra._redis", package="vertebra-core[redis]", extra="redis")`, while `SyncValkeyDriver`/`AsyncValkeyDriver` and `SyncValkeyNotificationSource`/`AsyncValkeyNotificationSource` probe `package="vertebra-core[valkey]", extra="valkey"`, preserving store/cache methods (`get`, `set(ex=, px=, nx=, xx=)`, `delete`, `unlink`, `scan`, `pttl`, `flushdb`, `eval_script`, `discover_sentinel_master`, `add_invalidation_listener`, `cached_keys`) and using completion drainer attribute `"_vertebra_redis_completion_drainer"`.

**Integration Flow:**
- `KeyValueStore`, Litestar `MemoryStore`, Starlette `CacheStore`, and `SQLAlchemyCacheBackend` continue calling `SyncRedisDriver`/`AsyncRedisDriver` and `SyncValkeyDriver`/`AsyncValkeyDriver` unchanged.
- `ChannelBus(notification_source=AsyncRedisNotificationSource(driver))` streams Redis/Valkey pub/sub frames into SSE/WebSocket channels while synchronous workers consume `SyncNotificationSource.listen()`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| Wheel missing | `vertebra._redis` absent | `MissingDependencyError` naming `vertebra-core[redis]` or `vertebra-core[valkey]` | Distinct extra hints |
| `memory://` DSN | `RedisConfig(url="memory://")` | Raises `ImproperConfigurationError` | No in-memory fake |
| Legacy unprefixed import | `from vertebra.db.driver import NotificationSource` | Raises `ImportError` (`PY-07` clean break, zero aliases) | Strict `Sync*`/`Async*` symmetry |
| Pooled PgBouncer tx mode | `SyncPostgresNotificationSource` or `AsyncPostgresNotificationSource` without `direct_dsn` | Raises `ImproperConfigurationError` | Direct/session `LISTEN` invariant |
| Core `_vertebra` probe | `hasattr(vertebra._vertebra, "encode_resp_command")` | `False`; `mypy.stubtest vertebra._vertebra` passes | Clean core binary |
| Broken `async for` loop | Consumer exits `async for event in source.listen():` via `break` | Dedicated `AsyncRedisListener` closes deterministically | Zero leaked pub/sub sockets |

## 3. Documentation & OKF Knowledge Plan

Driver cutover docs, `SyncNotificationSource`/`AsyncNotificationSource` updates in `docs/usage/queues-and-realtime.md`, `docs/changelog/index.md`, and `vertebra-db` table cleanup in `.agents/bundles/product/product.md` execute in `ch04:t03` and `ch04:t04`.

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Cut Python Redis and Valkey Core and Drivers Over to vertebra._redis | `src/py/vertebra/db/adapters/{redis,valkey}/{core,driver,__init__}.py`, `src/py/tests/unit/conftest.py` | `src/py/tests/unit/db/test_redis_valkey_driver.py`, `src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| `t02` | Split NotificationSource into SyncNotificationSource and AsyncNotificationSource Across Capabilities, Postgres, and Oracle | `src/py/vertebra/db/{__init__,driver/capabilities,driver/__init__}.py`, `src/py/vertebra/server/_channels.py`, `src/py/vertebra/db/adapters/{postgres,oracle}/*` | `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_postgres_native_notifications.py`, `src/py/tests/unit/db/test_oracle_adapter.py` |
| `t03` | Remove Legacy RESP3 Bindings from vertebra-py and Delete vertebra-db/src/resp3/mod.rs | `src/rs/vertebra-py/src/db.rs`, `src/py/vertebra/_vertebra.pyi`, `src/rs/vertebra-db/src/{resp3/mod.rs,lib.rs}` | `cargo test -p vertebra-db`, `mypy.stubtest vertebra._vertebra`, `src/py/tests/unit/db/test_redis_valkey_resp3.py` |
| `t04` | Implement Sync/Async Redis and Valkey Pub/Sub Notification Sources | `src/py/vertebra/db/adapters/{redis,valkey}/{notify,__init__}.py` | `src/py/tests/unit/db/test_redis_valkey_resp3.py` |

## 5. Implementation Plan

- [ ] [t01-python-redis-and-valkey-driver-cutover.md](tasks/t01-python-redis-and-valkey-driver-cutover.md) — Relocate test loopback wire parser to test helpers and rewrite `RedisCore`, `ValkeyCore`, `Sync`/`AsyncRedisDriver`, and `Sync`/`AsyncValkeyDriver` onto `vertebra._redis`.
- [ ] [t02-notification-source-protocol-split.md](tasks/t02-notification-source-protocol-split.md) — Define `@runtime_checkable SyncNotificationSource` and `AsyncNotificationSource` in `capabilities.py` and `ChannelBus`, and split Postgres and Oracle notification sources.
- [ ] [t03-remove-legacy-resp3-bindings.md](tasks/t03-remove-legacy-resp3-bindings.md) — Delete `encode_resp_command`, `encode_hello3_handshake`, `crc16_xmodem`, `redis_hash_slot`, and `parse_cluster_redirect` from `vertebra-py`/`_vertebra.pyi` and remove `src/rs/vertebra-db/src/resp3/mod.rs`.
- [ ] [t04-redis-and-valkey-pubsub-notifications.md](tasks/t04-redis-and-valkey-pubsub-notifications.md) — Implement `SyncRedisNotificationSource`, `AsyncRedisNotificationSource`, `SyncValkeyNotificationSource`, and `AsyncValkeyNotificationSource` over `vertebra._redis` Pub/Sub.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `state: planned`; `0/4` tasks completed.
- **Active Invariants**: `vertebra._vertebra` and `vertebra-db` carry zero Redis/RESP3 codec code after `t03`; zero unprefixed `NotificationSource` aliases remain.
- **Fast Chapter Gate (`< 60s`)**: `cargo test --manifest-path src/rs/Cargo.toml -p vertebra-db --features gcp && make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra vertebra._redis --concise && uv run --no-sync pytest src/py/tests/unit/db/test_redis_valkey_driver.py src/py/tests/unit/db/test_redis_valkey_resp3.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_postgres_native_notifications.py src/py/tests/unit/db/test_oracle_adapter.py -q && uv run --no-sync mypy src/py/vertebra/db src/py/vertebra/server/_channels.py`

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
