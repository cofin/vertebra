---
type: Spec
flow_id: native-redis-driver
title: Standalone Rust Redis and Valkey Wheel (vertebra-redis)
description: Build standalone PyO3 extension src/rs/vertebra-redis (vertebra._redis for vertebra-core[redis] and vertebra-core[valkey], gil_used = false) over pure-Rust redis 1.8.1 + rustls for Redis 7.2+/8.0 and Valkey 8.0+/9.0, hoist the SyncNotificationSource/AsyncNotificationSource split across PostgreSQL and Oracle, add Redis/Valkey notification sources and native Streams queue backends, and delete Python socket drivers, redis.asyncio, and vertebra_db::resp3.
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
- native-postgres-driver
plan_revision: 1
plan_commit: 104fd6fa
created_at: '2026-11-12T23:45:00Z'
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
  title: C-ABI GCP IAM token cache capsule
- id: py-async
  resource: src/rs/vertebra-db/src/py_async.rs
  title: AsyncCompletionQueue and Notifier PyO3 bridge
- id: resp3-rs
  resource: src/rs/vertebra-db/src/resp3/mod.rs
  title: Legacy RESP3 codec module in vertebra-db
- id: resp3-bindings
  resource: src/rs/vertebra-py/src/db.rs::redis_hash_slot
  title: Legacy RESP3 helper bindings on vertebra._vertebra
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
  title: SyncValkeyDriver and AsyncValkeyDriver
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

# Specification: Standalone Rust Redis and Valkey Wheel (vertebra-redis)

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `SyncRedisDriver`/`AsyncRedisDriver` and `SyncValkeyDriver`/`AsyncValkeyDriver` run over Python `socket` / `asyncio.open_connection` + `_vertebra` `resp3` bindings, `conftest.py::Resp3FakeServer` imports `Resp3Parser` at top level, `RedisQueueBackend`/`ValkeyQueueBackend` depend on `redis.asyncio`, and `capabilities.py` defines async-only `NotificationSource` [^redis-drv] [^unit-conftest] [^queue-redis] [^driver-caps].
- **Target Architecture**: Standalone `src/rs/vertebra-redis` (`vertebra._redis`, `vertebra-core[redis]` & `vertebra-core[valkey]`, `#[pymodule(gil_used = false)]`) over pure-Rust `redis = "=1.8.1"` + `rustls` supporting Redis `7.2+`/`8.0` and Valkey `8.0+`/`9.0` (`SyncRedisClient`/`AsyncRedisClient`, `HELLO 3` + `CLIENT TRACKING ON ... BCAST`, Sentinel, Cluster `MOVED`/`ASK`, `GcpTokenCacheCapsule` Memorystore IAM, `SyncRedisPipeline`/`AsyncRedisPipeline`, Streams `XREADGROUP`/`XACK`/`XAUTOCLAIM`, and `SyncRedisListener`/`AsyncRedisListener`) [^gcp-capsule] [^py-async].
- **Hoisted Notification Split (`A3`) & Clean Cutover**: `ch03-python-cutover-and-notifications` (`t02`) splits `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` across `capabilities.py`, PostgreSQL, and Oracle BEFORE `ch03:t04` adds `SyncRedisNotificationSource`/`AsyncRedisNotificationSource` and Valkey sources — keeping `native-postgres-driver -> native-redis-driver -> queues-tasks-and-channels` strictly acyclic while deleting `Resp3Parser`, `vertebra_db::resp3`, and `redis.asyncio` [^pg-notify] [^ora-notify] [^resp3-rs].
<!-- truth: end -->

## 1. Specification, Context & Provenance

At `18baadcd`, Redis and Valkey adapters maintain 2,300+ lines of pure-Python socket/asyncio I/O and RESP3 parsing backed by `vertebra_db::resp3` helper bindings on `_vertebra` [^redis-core] [^redis-drv] [^resp3-bindings], `Resp3FakeServer` in `src/py/tests/unit/conftest.py` couples unit test collection to `Resp3Parser` [^unit-conftest], and `RedisQueueBackend` / `ValkeyQueueBackend` require third-party `redis.asyncio` while rejecting `valkey://`, `valkeys://`, and `unix://` [^queue-redis]. Furthermore, `capabilities.py::NotificationSource` is async-only [^driver-caps]. Hoisting the `SyncNotificationSource` / `AsyncNotificationSource` split into `ch03:t02` (`PostgreSQL` + `Oracle`) allows `native-redis-driver` (`ch03`) to ship symmetric Redis and Valkey pub/sub notification sources directly without circular dependencies on `queues-tasks-and-channels`.

**Non-Goals:**
- Hand-rolled Rust RESP3 wire framing (`redis = "=1.8.1"` owns protocol framing, multiplexing, and cluster routing).
- In-memory `memory://` Redis/Valkey execution (rejected with `ImproperConfigurationError` per fail-loud baseline).
- High-level distributed channel fan-out / Litestar channels / Django tasks (owned by downstream `queues-tasks-and-channels`).

| Area | Primary Anchors | Public Seam & Integration |
| --- | --- | --- |
| Rust crate & wheel | `src/rs/vertebra-redis/*`, `pyproject.toml`, `Makefile`, `release.yaml` [^cargo-ws] [^root-pyproject] | `vertebra._redis` (`gil_used = false`), `vertebra-core[redis]` & `[valkey]`, `make redis-test`, `make redis-wheel-check` |
| RESP3 & topologies | `vertebra-redis/src/{client_core,caching,topology,pubsub,streams,py_client}.rs` [^gcp-capsule] [^py-async] | `HELLO 3`, `CLIENT TRACKING`, Sentinel, Cluster `MOVED`/`ASK`, Memorystore IAM, pipelines, Streams, listeners |
| Notification split | `db/driver/capabilities.py`, `adapters/{postgres,oracle,redis,valkey}/notify.py` [^driver-caps] [^pg-notify] [^ora-notify] | `SyncNotificationSource` & `AsyncNotificationSource` across Postgres, Oracle, Redis, and Valkey |
| Adapter & queue cutover | `conftest.py`, `adapters/{redis,valkey}/driver.py`, `framework/queues/redis.py`, `vertebra-db/src/resp3/` [^unit-conftest] [^queue-redis] [^resp3-rs] | Delete `Resp3Parser`, `vertebra_db::resp3`, and `redis.asyncio`; wire CI `redis_service` + `valkey_service` |

**Knowledge Anchors:** [Database Drivers and Cloud Dialers](../../knowledge/domains/data/database-drivers-and-dialers.md), [Distributed Queues and Email](../../knowledge/domains/framework/distributed-queues-and-email.md), [Query Pipeline, CDC, and Tenancy](../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), [ADR-004 Optional Driver Wheels and SHM Ownership](../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md), [Project Workflow](../../knowledge/workflow.md).

## 2. Requirements, Concrete Contracts & Architecture

- **R1–R4**: Scaffold `src/rs/vertebra-redis` (`redis 1.8.1` + `rustls`), `[redis]` + `[valkey]` extras, `_redis.pyi`, `Makefile` targets, `release.yaml`, and review manifests/skills (`ch01`).
- **R5**: Enforce sibling `.pyi` wheel isolation and implement `redis://`/`rediss://`/`valkey://`/`valkeys://`/`unix://` normalization, `memory://` rejection, `GcpTokenCacheCapsule` IAM refresh, and `DatabaseError` mapping (`ch02`).
- **R6–R9**: Implement `HELLO 3`, `CLIENT TRACKING ON ... BCAST`, Sentinel master discovery, Cluster `MOVED`/`ASK` routing, `SyncRedisClient`/`AsyncRedisClient`, `SyncRedisPipeline`/`AsyncRedisPipeline`, `SyncRedisListener`/`AsyncRedisListener`, and Streams (`ch03`).
- **R10–R12**: Decouple `conftest.py::Resp3FakeServer` first, delete `Resp3Parser` + socket/asyncio transport, and cut `SyncRedisDriver`/`AsyncRedisDriver` & `SyncValkeyDriver`/`AsyncValkeyDriver` over to `vertebra._redis` (`ch04`).
- **R13**: Split `NotificationSource` into `SyncNotificationSource` and `AsyncNotificationSource` (zero aliases) and split `PostgresNotificationSource` and `OracleNotificationSource` into `Sync*`/`Async*` pairs (`ch03`).
- **R14–R16**: Delete `_vertebra` RESP3 bindings and `src/rs/vertebra-db/src/resp3/mod.rs`, and implement `SyncRedisNotificationSource`/`AsyncRedisNotificationSource` and `SyncValkeyNotificationSource`/`AsyncValkeyNotificationSource` (`ch03`).
- **R17–R19**: Cut `RedisQueueBackend` and `ValkeyQueueBackend` onto `vertebra._redis`, add `redis_service` + `valkey_service` to `ci.yml`, and synchronize `docs/`, `product.md`, `AGENTS.md`, `review-pyo3-bridge`, and `redis-limitations.md` (`ch04`).

```python
# vertebra._redis
GIL_USED: Final[bool]  # False
class SyncRedisClient: ...
class AsyncRedisClient: ...
class SyncRedisPipeline: ...
class AsyncRedisPipeline: ...
class SyncRedisListener: ...
class AsyncRedisListener: ...

# vertebra.db.driver.capabilities
@runtime_checkable
class SyncNotificationSource(Protocol): ...
@runtime_checkable
class AsyncNotificationSource(Protocol): ...
```

**Integration Flow:**
- Flow DAG remains strictly acyclic: `native-postgres-driver -> native-redis-driver -> queues-tasks-and-channels`.
- `KeyValueStore`, `MemoryStore`, `CacheStore`, `SQLAlchemyCacheBackend`, `ChannelBus`, `RedisQueueBackend`, and `ValkeyQueueBackend` all execute over `vertebra._redis`.

| Boundary | Trigger | Expected Contract | Invariant |
| --- | --- | --- | --- |
| `conftest.py` ordering | `Resp3Parser` deleted after `t01` fixture decoupling | Pytest collection stays unbroken across every chapter | Safe staged cutover |
| Missing wheel | `vertebra-redis` absent | `MissingDependencyError` naming `vertebra-core[redis]` or `vertebra-core[valkey]` | Engine-specific install hints |
| Unprefixed `NotificationSource` | Legacy import attempted | Raises `ImportError` (`PY-07` zero aliases) | Clean `Sync*`/`Async*` split |
| Cluster `CROSSSLOT` | Multi-key pipeline/Lua without matching `{tag}` | Raises `ProgrammingError` | Explicit cluster contract |

## 3. Documentation & OKF Knowledge Plan

- `[ ]` `docs/usage/databases.md` & `docs/usage/queues-and-realtime.md` — `vertebra-core[redis]`/`[valkey]` install tabs, `CLIENT TRACKING`, Sentinel/Cluster, Streams queues, and `Sync*`/`Async*` notification sources (`ch04:t03`).
- `[ ]` `docs/reference/native-stubs.md` — `vertebra._redis` stub API reference with `heading_level: 4` (`ch04:t03`).
- `[ ]` `docs/changelog/index.md`, `AGENTS.md`, & `.agents/bundles/product/product.md` — release notes, crate/command inventory, and `vertebra-db` table cleanup (`ch04:t03`).
- `[ ]` `.agents/bundles/knowledge/domains/data/redis-limitations.md`, `database-drivers-and-dialers.md`, & `distributed-queues-and-email.md` — OKF chapters (`<= 200` lines each, `ch04:t04`).
- `[ ]` `.agents/skills/review-pyo3-bridge/SKILL.md`, `.agents/bundles/knowledge/domains/index.md`, & `.agents/bundles/knowledge/workflow.md` — review skill and OKF index sync (`ch04:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Chapter | Title | Tasks | Verification |
| --- | --- | --- | --- |
| `ch01` | Chapter 01: Standalone vertebra-redis Crate, [redis]/[valkey] Extras, Wheel Isolation, and Review Manifests | `4` tasks | `pytest test_optional_driver_wheels.py` |
| `ch02` | Chapter 02: Rust Transport Config, RESP2/RESP3 Codec, Topologies, and PyO3 Clients/Streams | `4` tasks | `pytest redis_client.rs` |
| `ch03` | Chapter 03: Python Redis/Valkey Adapter Cutover, Sync/AsyncNotificationSource Split, and RESP3 Cleanup | `4` tasks | `pytest conftest.py` |
| `ch04` | Chapter 04: Native Redis/Valkey Queue Backends, Conformance CI, and Docs/OKF Synchronization | `4` tasks | `pytest test_distributed_queues.py` |

## 5. Implementation Plan

- [ ] [ch01-wheel-scaffold-and-isolation](chapters/ch01-wheel-scaffold-and-isolation/spec.md) — Scaffold src/rs/vertebra-redis (vertebra._redis, gil_used = false) over pure-Rust redis 1.8.1 + rustls, wire [redis]/[valkey] extras and Makefile/release matrix, enforce companion wheel isolation across sibling extensions, and register review manifests.
- [ ] [ch02-rust-clients-topologies-and-streams](chapters/ch02-rust-clients-topologies-and-streams/spec.md) — Implement URL/TLS/GCP Memorystore IAM config and error mapping, RESP2/RESP3 codec + pipelines + Lua script cache, CLIENT TRACKING + Cluster + Sentinel topologies, and PyO3 Sync/AsyncRedisClient, Pub/Sub listeners, and Streams.
- [ ] [ch03-python-cutover-and-notifications](chapters/ch03-python-cutover-and-notifications/spec.md) — Cut Python Redis and Valkey adapters over to vertebra._redis with zero socket fallback, split NotificationSource into SyncNotificationSource and AsyncNotificationSource across capabilities/Postgres/Oracle, remove legacy _vertebra RESP3 bindings, and implement Redis/Valkey Pub/Sub notification sources.
- [ ] [ch04-queues-conformance-and-docs](chapters/ch04-queues-conformance-and-docs/spec.md) — Cut RedisQueueBackend and ValkeyQueueBackend over to vertebra._redis Streams/ZSETs, wire Redis 8.0 and Valkey 9.0 conformance in CI, and synchronize public docs, changelog, OKF chapters, and review skills.

## 6. Current State & Remaining Plan Snapshot

- **Baseline (`18baadcd`)**: `0/8` chapters and `0/28` tasks completed; frontier at `ch01-wheel-scaffold-and-isolation` (`native-redis-driver:ch01:t01`).
- **Upstream Flow Dependencies**: `native-postgres-driver`.
- **Active Invariants**: Zero OpenSSL linkage; `GIL_USED == False`; zero `redis.asyncio` or Python socket Redis drivers; zero `vertebra_db::resp3` remnants; strict `Sync*`/`Async*` notification symmetry with zero aliases.
- **Full `/flow:finish` Gate**: `make redis-test && make redis-wheel-check && make lint-all && make py-test && make docs && make review-check`

[^cargo-ws]: `src/rs/Cargo.toml` — Rust workspace member manifest.
[^root-pyproject]: `pyproject.toml` — Root vertebra-core extras, uv sources, maturin excludes, and bumpversion.
[^gcp-capsule]: `src/rs/vertebra-core/src/gcp_auth.rs::GcpTokenCacheCapsule` — C-ABI GCP IAM token cache capsule.
[^py-async]: `src/rs/vertebra-db/src/py_async.rs` — AsyncCompletionQueue and Notifier PyO3 bridge.
[^resp3-rs]: `src/rs/vertebra-db/src/resp3/mod.rs` — Legacy RESP3 codec module in vertebra-db.
[^resp3-bindings]: `src/rs/vertebra-py/src/db.rs::redis_hash_slot` — Legacy RESP3 helper bindings on vertebra._vertebra.
[^unit-conftest]: `src/py/tests/unit/conftest.py::Resp3FakeServer` — Resp3FakeServer loopback test fixture coupled to Resp3Parser.
[^redis-core]: `src/py/vertebra/db/adapters/redis/core.py::Resp3Parser` — Pure-Python Resp3Parser and RESP command encoders.
[^redis-drv]: `src/py/vertebra/db/adapters/redis/driver.py::SyncRedisDriver` — Python socket/asyncio SyncRedisDriver and AsyncRedisDriver.
[^valkey-drv]: `src/py/vertebra/db/adapters/valkey/driver.py::SyncValkeyDriver` — SyncValkeyDriver and AsyncValkeyDriver.
[^driver-caps]: `src/py/vertebra/db/driver/capabilities.py::NotificationSource` — NotificationSource capability protocol in capabilities.py.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification source.
[^ora-notify]: `src/py/vertebra/db/adapters/oracle/notify.py::OracleNotificationSource` — Oracle AQ/TxEventQ notification source.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend using redis.asyncio.
