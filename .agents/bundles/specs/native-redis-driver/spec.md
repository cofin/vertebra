---
type: Spec
flow_id: native-redis-driver
title: Standalone Rust Redis Wheel (vertebra-redis)
description: Build standalone PyO3 extension src/rs/vertebra-redis (vertebra._redis for vertebra-core[redis], gil_used = false) over pure-Rust redis 1.8.1 + rustls for Redis 7.2+/8.0, decouple Valkey adapter/queue modules from Redis modules, hoist the SyncNotificationSource/AsyncNotificationSource split across PostgreSQL and Oracle, add Redis notification sources and native Streams queue backend, and wire Redis 8.0 CI conformance.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
  - ch01-wheel-scaffold-and-isolation
  - ch02-rust-clients-topologies-and-streams
  - ch03-python-cutover-and-notifications
  - ch04-queues-conformance-and-docs
current_chapter: ch01-wheel-scaffold-and-isolation
current_task: native-redis-driver:ch01:t01
parent_prd: native-data-platform
depends_on_flows:
  - native-sqlite-driver
plan_revision: 3
plan_commit: 473e1f2e
created_at: '2026-11-12T23:45:00Z'
updated_at: '2026-11-15T23:30:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T23:14:00Z'
verified:
  - by: agent:spec-sync
    at: '2026-11-15T23:30:00Z'
    commit: 473e1f2e
  - by: agent:plan-generator
    at: '2026-11-15T20:56:00Z'
    commit: 246d7d0e
tags:
  - redis
  - rust
  - resp3
  - pubsub
  - streams
sources:
  - id: cargo-ws
    resource: src/rs/Cargo.toml
    title: Rust workspace member manifest
  - id: root-pyproject
    resource: pyproject.toml
    title: Root vertebra-core extras, uv sources, maturin excludes, and bumpversion
  - id: gcp-capsule
    resource: src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule
    title: C-ABI Google Cloud IAM token cache capsule
  - id: py-async
    resource: src/rs/vertebra-db/src/py_async.rs
    title: DbCompletionDrainer, spawn_rust_db_task_with_attr_and_cancel, and construct_py_arrow_cursor PyO3 async bridge
  - id: unit-conftest
    resource: src/py/tests/unit/conftest.py::Resp3FakeServer
    title: Resp3FakeServer loopback test fixture coupled to Resp3Parser
  - id: redis-core
    resource: src/py/vertebra/db/adapters/redis/core.py::Resp3Parser
    title: Pure-Python Resp3Parser and RESP command encoders
  - id: redis-drv
    resource: src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver
    title: Python socket/asyncio SyncRedisDriver and AsyncRedisDriver
  - id: valkey-drv
    resource: src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver
    title: SyncValkeyDriver and AsyncValkeyDriver currently subclassing Redis drivers
  - id: driver-caps
    resource: src/py/vertebra/db/driver/capabilities.py::NotificationSource
    title: NotificationSource capability protocol in capabilities.py
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification source
  - id: ora-notify
    resource: src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource
    title: Oracle AQ/TxEventQ notification source
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: RedisQueueBackend and ValkeyQueueBackend using redis.asyncio
---

# Specification: Standalone Rust Redis Wheel (vertebra-redis)

<!-- truth: start -->
- **Baseline (`473e1f2e`)**: `SyncRedisDriver`/`AsyncRedisDriver` run over Python `socket` / `asyncio.open_connection` + `_vertebra` `resp3` bindings, `adapters/valkey/` subclasses `adapters/redis/`, `conftest.py::Resp3FakeServer` imports `Resp3Parser` at top level, `RedisQueueBackend` depends on `redis.asyncio` and co-locates `ValkeyQueueBackend` in `queues/redis.py`, and `capabilities.py` defines async-only `NotificationSource` [^redis-drv] [^valkey-drv] [^unit-conftest] [^queue-redis] [^driver-caps].
- **Target Architecture**: Standalone `src/rs/vertebra-redis` (`vertebra._redis`, `vertebra-core[redis]`, `#[pymodule(gil_used = false)]`) over pure-Rust `redis = "=1.8.1"` + `rustls` supporting Redis `7.2+`/`8.0` (`SyncRedisClient`/`AsyncRedisClient`, `HELLO 3` + `CLIENT TRACKING ON ... BCAST`, Sentinel, Cluster `MOVED`/`ASK`, `GcpTokenCacheCapsule` Memorystore for Redis IAM, `SyncRedisPipeline`/`AsyncRedisPipeline`, Streams `XREADGROUP`/`XACK`/`XAUTOCLAIM`, and `SyncRedisListener`/`AsyncRedisListener`) [^gcp-capsule] [^py-async].
- **Clean Redis/Valkey Boundary & Hoisted Notification Split**: `ch03-python-cutover-and-notifications` decouples `adapters/valkey/` and `framework/queues/valkey.py` from `adapters/redis/` and `queues/redis.py` (with standalone `vertebra-valkey` cutover in sibling [`native-valkey-driver`](../native-valkey-driver/spec.md)), splits `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` across `capabilities.py`, PostgreSQL, and Oracle BEFORE `ch03:t04` adds `SyncRedisNotificationSource`/`AsyncRedisNotificationSource`, while `ch04:t01` cuts `RedisQueueBackend` over to `vertebra._redis` [^pg-notify] [^ora-notify].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `473e1f2e`, Redis maintains pure-Python socket/asyncio I/O and RESP3 parsing in `adapters/redis/` while `adapters/valkey/` and `ValkeyQueueBackend` inherit from Redis modules [^redis-core] [^redis-drv] [^valkey-drv] [^queue-redis]. Because Redis (`7.2+`/`8.0`) and Valkey (`8.0+`/`9.0`) have diverging server capabilities, `native-redis-driver` builds the dedicated `vertebra-redis` (`vertebra._redis`, `vertebra-core[redis]`) wheel and cuts `adapters/redis/` and `framework/queues/redis.py` over to `vertebra._redis` while decoupling `adapters/valkey/` and extracting `framework/queues/valkey.py` for sibling [`native-valkey-driver`](../native-valkey-driver/spec.md). Hoisting the `SyncNotificationSource` / `AsyncNotificationSource` split into `ch03:t02` (`PostgreSQL` + `Oracle`) allows `ch03:t04` to ship `SyncRedisNotificationSource` and `AsyncRedisNotificationSource` without circular dependencies on `queues-tasks-and-channels`.

**Non-Goals:**
- Hand-rolled Rust RESP3 wire framing (`redis = "=1.8.1"` owns protocol framing, multiplexing, and cluster routing).
- Valkey native Rust wheel (`vertebra-valkey` / `vertebra._valkey`) and Valkey 9.0 `HEXPIRE` optimizations (owned by sibling [`native-valkey-driver`](../native-valkey-driver/spec.md)).
- High-level distributed channel fan-out / Litestar channels / Django tasks (owned by downstream `queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate & wheel | `src/rs/vertebra-redis/*`, `pyproject.toml`, `Makefile`, `release.yaml` [^cargo-ws] [^root-pyproject] | `vertebra._redis` (`gil_used = false`), `vertebra-core[redis]`, `make redis-test`, `make redis-wheel-check` |
| RESP3 & topologies | `vertebra-redis/src/{config,errors,value,client_core,caching,topology,pubsub,streams,py_client}.rs` [^gcp-capsule] [^py-async] | `HELLO 3`, `CLIENT TRACKING`, Sentinel, Cluster `MOVED`/`ASK`, Memorystore IAM, pipelines, Streams, listeners |
| Notification split | `db/driver/capabilities.py`, `adapters/{postgres,oracle,redis}/notify.py` [^driver-caps] [^pg-notify] [^ora-notify] | `SyncNotificationSource` & `AsyncNotificationSource` across Postgres, Oracle, and Redis |
| Adapter & queue cutover | `conftest.py`, `adapters/redis/driver.py`, `framework/queues/{redis,valkey}.py` [^unit-conftest] [^queue-redis] | Cut `adapters/redis/` and `RedisQueueBackend` to `vertebra._redis`; decouple `valkey` modules |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [Distributed Queues and Email](../../knowledge/domains/framework/distributed-queues-and-email.md), [Query Pipeline, CDC, and Tenancy](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [ADR-004 Optional Driver Wheels and SHM Ownership](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Project Workflow](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1–R5a**: Scaffold `src/rs/vertebra-redis` (`redis = "=1.8.1"` + `rustls`), `[redis]` extra (`vertebra-redis==0.3.0`), `_redis.pyi`, `Makefile` targets (`redis-test`, `redis-wheel-check`), `release.yaml`, sibling `.pyi` wheel exclusions across all sibling PyO3 crates, and review manifests/skills (`ch01`).
- **R5b–R9**: Implement `RedisClientConfig` (`redis://`, `rediss://`, `unix://`; rejects `memory://` and `valkey://`/`valkeys://` with `ImproperConfigurationError`), `GcpTokenCacheCapsule` IAM refresh, `DatabaseError` mapping, `HELLO 3`, `CLIENT TRACKING ON ... BCAST`, Sentinel master discovery, Cluster `MOVED`/`ASK` routing, `SyncRedisClient`/`AsyncRedisClient`, `SyncRedisPipeline`/`AsyncRedisPipeline`, `SyncRedisListener`/`AsyncRedisListener`, and Streams (`ch02`).
- **R10–R16**: Decouple `conftest.py::Resp3FakeServer` and `adapters/valkey/` from `adapters/redis/`, delete `Resp3Parser` + socket/asyncio transport from `adapters/redis/` and cut `SyncRedisDriver`/`AsyncRedisDriver` over to `vertebra._redis`; split `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` (zero aliases) across PostgreSQL and Oracle; extract `ValkeyQueueBackend` to `framework/queues/valkey.py`; implement `SyncRedisNotificationSource`/`AsyncRedisNotificationSource` (`ch03`).
- **R17–R19**: Cut `RedisQueueBackend` (`framework/queues/redis.py`) onto `vertebra._redis` Streams/ZSETs, add a `redis` entry to the `ci.yml` `test-db-conformance` matrix (existing `pytest-databases` `redis_service` fixture, `ConformanceGate.SUPPORTED_ENGINES`) plus `make redis-test`/`make redis-wheel-check`, and synchronize `docs/`, `.agents/bundles/product/product.md`, `AGENTS.md`, `review-pyo3-bridge`, `workflow.md`, and `redis-limitations.md` (`ch04`).

```python
class SyncRedisClient: ...
class AsyncRedisClient: ...
class SyncRedisPipeline: ...
class AsyncRedisPipeline: ...
class SyncRedisListener: ...
class AsyncRedisListener: ...

@runtime_checkable
class SyncNotificationSource(Protocol): ...

@runtime_checkable
class AsyncNotificationSource(Protocol): ...
```

`vertebra._redis` exposes `GIL_USED: Final[bool] = False`, while `SyncNotificationSource` and `AsyncNotificationSource` live in `vertebra.db.driver.capabilities` and are lazy-exported from `vertebra.db.driver` and `vertebra.db`.

**Integration Flow:**
- Flow DAG remains strictly acyclic: `native-sqlite-driver -> native-redis-driver -> native-valkey-driver -> sqlalchemy-and-django-integration -> sqlspec-adapter-parity -> queues-tasks-and-channels`.
- `KeyValueStore`, `RedisStore`, `SyncCacheBackend`/`AsyncCacheBackend`, `ChannelBus`, and `RedisQueueBackend` execute Redis workloads over `vertebra._redis`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `conftest.py` & `valkey` decoupling | `Resp3Parser` removed from `redis/core.py` after `Resp3FakeServer` and `valkey` decoupling | Pytest collection and Valkey tests stay green before `native-valkey-driver` | Safe staged cutover |
| Missing wheel | `vertebra-redis` absent | `MissingDependencyError` naming `vertebra-core[redis]` | Engine-specific install hints |
| `valkey://` scheme on Redis client | `SyncRedisClient("valkey://...")` | Raises `ImproperConfigurationError` directing to `vertebra._valkey` | Strict engine separation |
| Unprefixed `NotificationSource` | Legacy import attempted | Raises `ImportError` (`PY-07` zero aliases) | Clean `Sync*`/`Async*` split |

## 3. Documentation & OKF Knowledge Plan

- `[ ]` `docs/usage/databases.md` & `docs/usage/queues-and-realtime.md` — `vertebra-core[redis]` install tabs, `CLIENT TRACKING`, Sentinel/Cluster, Streams queues, and `Sync*`/`Async*` notification sources (`ch04:t03`).
- `[ ]` `docs/reference/native-stubs.md` — `vertebra._redis` stub API reference with `heading_level: 4` (`ch04:t03`).
- `[ ]` `docs/changelog/index.md`, `AGENTS.md`, & `.agents/bundles/product/product.md` — release notes, crate/command inventory, and `vertebra-redis` row (`ch04:t03`).
- `[ ]` `.agents/bundles/knowledge/domains/data/redis-limitations.md`, `database-drivers-and-dialers.md`, & `distributed-queues-and-email.md` — OKF chapters (`<= 200` lines each, `ch04:t04`).
- `[ ]` `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/bundles/knowledge/domains/index.md`, & `.agents/bundles/knowledge/workflow.md` — review skill and OKF index sync (`ch04:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Chapter | Title | Tasks | Verification |
| --- | --- | --- | --- |
| `ch01` | Chapter 01: Standalone vertebra-redis Crate, [redis] Extra, Wheel Isolation, and Review Manifests | `4` tasks | `make redis-wheel-check`, `pytest test_optional_driver_wheels.py` |
| `ch02` | Chapter 02: Rust Transport Config, RESP2/RESP3 Codec, Topologies, and PyO3 Clients/Streams | `4` tasks | `cargo test -p vertebra-redis`, `pytest test_freethreaded_gil.py` |
| `ch03` | Chapter 03: Python Redis Adapter Cutover, Valkey Module Decoupling, and Sync/AsyncNotificationSource Split | `4` tasks | `make dev`, `pytest test_redis_valkey_driver.py test_redis_valkey_resp3.py` |
| `ch04` | Chapter 04: Native Redis Queue Backend, Conformance CI, and Docs/OKF Synchronization | `4` tasks | `pytest test_distributed_queues.py`, `make review-check` |

## 5. Implementation Plan

- [ ] [ch01-wheel-scaffold-and-isolation](chapters/ch01-wheel-scaffold-and-isolation/spec.md) — Scaffold `src/rs/vertebra-redis` (`vertebra._redis`, `gil_used = false`) over pure-Rust `redis 1.8.1` + `rustls`, wire `[redis]` extra and `Makefile`/release matrix, enforce companion wheel isolation across sibling extensions, and register review manifests.
- [ ] [ch02-rust-clients-topologies-and-streams](chapters/ch02-rust-clients-topologies-and-streams/spec.md) — Implement Redis URL/TLS/Google Cloud Memorystore IAM config and error mapping, RESP2/RESP3 codec + pipelines + Lua script cache, `CLIENT TRACKING` + Cluster + Sentinel topologies, and PyO3 `Sync`/`AsyncRedisClient`, Pub/Sub listeners, and Streams.
- [ ] [ch03-python-cutover-and-notifications](chapters/ch03-python-cutover-and-notifications/spec.md) — Decouple `Resp3FakeServer` and `adapters/valkey/` from `adapters/redis/`, cut Python Redis adapter over to `vertebra._redis`, split `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` across capabilities/Postgres/Oracle, extract `framework/queues/valkey.py`, and implement Redis Pub/Sub notification sources.
- [ ] [ch04-queues-conformance-and-docs](chapters/ch04-queues-conformance-and-docs/spec.md) — Cut `RedisQueueBackend` (`framework/queues/redis.py`) over to `vertebra._redis` Streams/ZSETs, wire Redis 8.0 conformance in CI, and synchronize public docs, changelog, OKF chapters, and review skills.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`473e1f2e`)**: `0/4` chapters and `0/16` tasks completed; frontier at `ch01-wheel-scaffold-and-isolation` (`native-redis-driver:ch01:t01`).
- **Upstream Flow Dependencies**: `native-sqlite-driver` (shares `pyproject.toml`, `src/rs/Cargo.toml`, sibling wheel manifests, and review manifests, so the two flows run serially); `native-postgres-driver` is merged at `7116ba13`.
- **Active Invariants**: Zero OpenSSL linkage; `GIL_USED == False`; zero cross-adapter imports from `adapters/redis/` or `queues/redis.py` in `valkey` modules; strict `Sync*`/`Async*` notification symmetry with zero aliases.
- **Full `/flow:finish` Gate**: `make redis-test && make redis-wheel-check && make lint-all && make py-test && make docs && make review-check`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI Google Cloud IAM token cache capsule.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver currently subclassing Redis drivers.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend using redis.asyncio.
