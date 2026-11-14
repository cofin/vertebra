---
type: Spec
flow_id: queues-tasks-and-channels
title: Native Queues, Tasks, Channels, and Stores
description: Split QueueStore into SyncQueueStore and AsyncQueueStore, implement SQL-backed PostgreSQL and Oracle queue stores plus native Redis/Valkey stream queues, wire litestar-queues>=0.14.0 and Django 6.0+ TaskBackend with Container DI, and connect SyncNotificationSource / AsyncNotificationSource across distributed channels and ValkeyStore.
status: stable
state: planned
planning_quality: standard
planning_degraded_reason: null
has_chapters: true
chapters:
- ch01-queue-stores-and-first-party-backends
- ch02-workers-litestar-queues-and-django-tasks
- ch03-distributed-channels-stores-and-docs
current_chapter: ch01-queue-stores-and-first-party-backends
current_task: queues-tasks-and-channels:ch01:t01
parent_prd: integration-remediation
depends_on_flows:
- native-postgres-driver
- native-redis-driver
plan_revision: 3
plan_commit: 104fd6fa
created_at: '2026-11-03T03:30:00Z'
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 104fd6fa
tags:
- queues
- tasks
- django
- litestar
- channels
- stores
sources:
- id: queue-service
  resource: src/py/vertebra/framework/queues/base.py::QueueService
  title: First-party QueueService backend construction and cron scheduling
- id: queue-store
  resource: src/py/vertebra/db/driver/capabilities.py::QueueStore
  title: Unprefixed QueueStore protocol split into SyncQueueStore and AsyncQueueStore
- id: queue-oracle
  resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
  title: OracleQueueBackend probing oracledb when executor is omitted
- id: queue-postgres
  resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
  title: PostgreSQLQueueBackend requiring manual executor injection
- id: queue-redis
  resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
  title: RedisQueueBackend and ValkeyQueueBackend
- id: litestar-queues
  resource: src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend
  title: litestar-queues extension backends
- id: litestar-channels
  resource: src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend
  title: Litestar channels backend monkeypatching slated for deletion
- id: channel-bus
  resource: src/py/vertebra/server/_channels.py::ChannelBus
  title: Engine-neutral ChannelBus
- id: fail-loud-baseline
  resource: .agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md
  title: Fail-loud queue baseline requiring explicit executor= collaborators
---

# Specification: Native Queues, Tasks, Channels, and Stores

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `QueueStore` in `db/driver/capabilities.py` is async-only with zero implementations; `PostgreSQLQueueBackend` and `OracleQueueBackend` fail without manual `executor=`, `OracleQueueBackend` probes `oracledb`, `litestar-queues` is pinned at `>=0.12.1` with a broken `litestar_queues.records` import, `django/queues.py` wraps a Flask deque, and `litestar/channels.py` monkeypatches via `swap_channels_backend` [^queue-store] [^queue-oracle] [^queue-postgres] [^litestar-queues] [^litestar-channels] [^fail-loud-baseline].
- **`SyncQueueStore` / `AsyncQueueStore` & Native Stores**: `capabilities.py` splits `QueueStore` into `@runtime_checkable SyncQueueStore` and `@runtime_checkable AsyncQueueStore` (zero aliases), backed by `SyncPostgresQueueStore`/`AsyncPostgresQueueStore`, `SyncOracleQueueStore`/`AsyncOracleQueueStore`, and `vertebra._redis` stream/sorted-set `RedisQueueBackend`/`ValkeyQueueBackend` [^queue-redis].
- **Framework Tasks & Scoped DI**: `QueueService` (`QueueBackendSettings`), `litestar-queues>=0.14.0` (`StoreQueueBackend`, `SHMQueueBackend`, `queue_store_class`/`events_store_class`), and Django `6.0+` `TaskBackend` (`enqueue` via `SyncQueueStore`, `aenqueue` via `AsyncQueueStore`) resolve task dependencies via `framework/di.py::Container.scope()` [^queue-service].
- **Distributed Channels & `ValkeyStore`**: `ChannelBus`, Vertebra `ChannelsBackend`, and Litestar `ChannelsPlugin` consume `SyncNotificationSource`/`AsyncNotificationSource` (split in `native-redis-driver:ch03`) across Memory, Redis, Valkey, PostgreSQL, and Oracle, and `ValkeyStore` consolidates onto `vertebra._redis` [^channel-bus].
<!-- truth: end -->

## 1. Specification, Context & Provenance

Upstream `native-postgres-driver` cuts PostgreSQL over to `vertebra._postgres`, and `native-redis-driver` delivers `vertebra._redis` alongside the `SyncNotificationSource` / `AsyncNotificationSource` split (`SyncPostgresNotificationSource`/`AsyncPostgresNotificationSource`, `SyncOracleNotificationSource`/`AsyncOracleNotificationSource`, `SyncRedisNotificationSource`/`AsyncRedisNotificationSource`).

Building on those two upstream flows, this specification replaces interim fail-loud queue stubs and single-node channel shims across Vertebra, Litestar, Starlette, FastAPI, Flask, and Django `6.0+` [^queue-service] [^fail-loud-baseline]:
1. Synchronous producers such as Django `6.0+` `BaseTaskBackend.enqueue()` and Flask routes require a blocking `SyncQueueStore` surface without `PortalProvider` thread hops, motivating the clean split of `QueueStore` into `SyncQueueStore` and `AsyncQueueStore`.
2. `PostgreSQLQueueBackend` and `OracleQueueBackend` automatically construct `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` and `SyncOracleQueueStore`/`AsyncOracleQueueStore` from configured drivers, while `OracleQueueBackend` replaces its `oracledb` check with `vertebra-core[oracle]` [^queue-oracle] [^queue-postgres].
3. `litestar-queues` bumps to `>=0.14.0` with slotted `SHMQueueBackend`, engine-neutral `StoreQueueBackend`, `queue_store_class`/`events_store_class` settings selection, and `ContainerTaskDependencyProvider` [^litestar-queues].
4. Distributed channels replace `swap_channels_backend` monkeypatching with native `SyncNotificationSource`/`AsyncNotificationSource` fan-out across Memory, Redis, Valkey, PostgreSQL, and Oracle, and `ValkeyStore` consolidates onto `vertebra._redis` [^litestar-channels] [^channel-bus].

**Non-Goals**: Cloud Spanner interactive `ReadWrite` transactions and `SyncSpannerQueueStore` / `AsyncSpannerQueueStore` (owned by downstream flow `spanner-queue-and-rw-transactions`).

## 2. Requirements, Concrete Contracts & Architecture

- **R1–R2 (`SyncQueueStore` / `AsyncQueueStore` & Native PG/Oracle Stores)**: Split `QueueStore` into `@runtime_checkable SyncQueueStore` and `@runtime_checkable AsyncQueueStore` in `src/py/vertebra/db/driver/capabilities.py` (zero aliases); implement `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` (`FOR UPDATE OF q SKIP LOCKED ... RETURNING`, heartbeats, DLQ, cron leases, `pg_notify` wakeups, zero correlated scalar subqueries) and `SyncOracleQueueStore`/`AsyncOracleQueueStore` (`FOR UPDATE SKIP LOCKED`, heartbeats, DLQ, cron leases, TxEventQ wakeups).
- **R3–R4 (First-Party `QueueService` & Auto-Wired Backends)**: Add `QueueBackendSettings` and `TaskResultRecord`; drop `gc=False` from `TaskEnvelope` and `DeadLetterEntry`; delete duplicate `base.py::_cron_matches*` in favor of `scheduler.py::CronExpression`; auto-construct native stores in `PostgreSQLQueueBackend` and `OracleQueueBackend` (`vertebra-core[oracle]` install hint); wire `RedisQueueBackend` and `ValkeyQueueBackend` over `vertebra._redis` streams (`XREADGROUP`/`XAUTOCLAIM`) and sorted sets.
- **R5–R6 (`WorkerRunner`, Scoped DI & `CronScheduler`)**: `WorkerRunner` enters `framework/di.py::Container.scope()` per task attempt with heartbeats, lane semaphores, and graceful drain; `CronScheduler` enforces slot-keyed idempotent leases; Starlette, FastAPI, and Flask queue extensions bind `QueueService`.
- **R7 (`litestar-queues>=0.14.0` Integration)**: Bump `litestar-queues>=0.14.0`; implement slotted Rust-ring `SHMQueueBackend` and engine-neutral `StoreQueueBackend(BaseQueueBackend)` over `AsyncQueueStore`; wire settings-driven `queue_store_class` and `events_store_class` plus `ContainerTaskDependencyProvider`.
- **R8–R9 (Django `6.0+` `TaskBackend`, Worker Command & Cache/Channels)**: Delete `extensions/django/queues.py`; implement `extensions/django/tasks.py::TaskBackend(BaseTaskBackend)` (`enqueue` via `SyncQueueStore`, `aenqueue` via `AsyncQueueStore`), `vertebra_worker` management command with `Container.scope()` DI, and native Valkey/Redis `CacheBackend` + `ChannelLayer`.
- **R10–R12 (Distributed Channels, `ValkeyStore` & Docs/OKF)**: Wire `SyncNotificationSource` / `AsyncNotificationSource` into `ChannelBus`, Vertebra `ChannelsBackend`, Litestar `ChannelsPlugin` (deleting `swap_channels_backend`), Starlette, and Flask across Memory, Redis, Valkey, PostgreSQL, and Oracle; consolidate `ValkeyStore` (`CLIENT TRACKING`, `SCAN`+`UNLINK`, zero `flushdb()`); synchronize docs and `<= 200`-line OKF chapters.

## 3. Documentation & OKF Knowledge Plan

- Public documentation (`ch03:t04`): `docs/usage/{queues-and-realtime,extensions,databases,analytics-and-cdc}.md` and `docs/reference/{queues,database-capabilities}.md`.
- OKF knowledge chapters (`ch03:t04`, each `<= 200` lines): [distributed-queues-and-email.md](../../knowledge/domains/framework/distributed-queues-and-email.md), [litestar.md](../../knowledge/domains/extensions/litestar.md), and [server-runtime-limitations.md](../../knowledge/domains/server/server-runtime-limitations.md).

## 4. Requirement-to-Task Traceability Matrix

| Chapter | Title | Tasks | Verification |
| --- | --- | --- | --- |
| `ch01` | Chapter 01: Sync/AsyncQueueStore Protocols, Native PG/Oracle Stores, and Auto-Wired Queue Backends | `4` tasks | `pytest test_native_db_drivers.py` |
| `ch02` | Chapter 02: WorkerRunner Scoped DI, CronScheduler Leases, litestar-queues>=0.14.0 & Django 6.0+ Tasks | `4` tasks | `pytest test_distributed_queues.py` |
| `ch03` | Chapter 03: Distributed Channels Across 5 Backends, Django Cache/Channels, ValkeyStore & Docs/OKF | `4` tasks | `pytest test_django_plugin.py` |

## 5. Implementation Plan

- [ ] [ch01-queue-stores-and-first-party-backends](chapters/ch01-queue-stores-and-first-party-backends/spec.md) — Split QueueStore into @runtime_checkable SyncQueueStore and AsyncQueueStore, implement Sync/Async PostgreSQL and Oracle queue stores, add QueueBackendSettings, and auto-wire PostgreSQL, Oracle, Redis, and Valkey queue backends.
- [ ] [ch02-workers-litestar-queues-and-django-tasks](chapters/ch02-workers-litestar-queues-and-django-tasks/spec.md) — Implement WorkerRunner per-task Container.scope() DI and CronScheduler leases, wire Starlette/FastAPI/Flask queue lifespans, bump litestar-queues>=0.14.0 with SHM/StoreQueueBackend, and deliver Django 6.0+ TaskBackend + vertebra_worker.
- [ ] [ch03-distributed-channels-stores-and-docs](chapters/ch03-distributed-channels-stores-and-docs/spec.md) — Wire Django native cache/channels, bridge Sync/AsyncNotificationSource into ChannelBus and Litestar/Starlette/Flask channels across 5 backends, consolidate ValkeyStore over vertebra._redis, and synchronize docs and OKF chapters.

## 6. Current State & Remaining Plan Snapshot

- **Current Frontier**: `state: planned` at `ch01-queue-stores-and-first-party-backends` (`queues-tasks-and-channels:ch01:t01`, `0/12` tasks completed across `3` chapters).
- **Upstream Flow Gates**: Requires `native-postgres-driver` (cut-over `vertebra._postgres` driver) and `native-redis-driver` (`vertebra._redis` wheel plus `SyncNotificationSource` / `AsyncNotificationSource` split in `native-redis-driver:ch03`). Unblocks `spanner-queue-and-rw-transactions`.
- **Full `/flow:finish` Gate**: `make lint-all && make py-test && make docs`.

[^queue-service]: `src/py/vertebra/framework/queues/base.py::QueueService` — First-party QueueService backend construction and cron scheduling.
[^queue-store]: `src/py/vertebra/db/driver/capabilities.py::QueueStore` — Unprefixed QueueStore protocol split into SyncQueueStore and AsyncQueueStore.
[^queue-oracle]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — OracleQueueBackend probing oracledb when executor is omitted.
[^queue-postgres]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQLQueueBackend requiring manual executor injection.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — RedisQueueBackend and ValkeyQueueBackend.
[^litestar-queues]: `src/py/vertebra/extensions/litestar/queues/backend.py::SHMQueueBackend` — litestar-queues extension backends.
[^litestar-channels]: `src/py/vertebra/extensions/litestar/channels.py::swap_channels_backend` — Litestar channels backend monkeypatching slated for deletion.
[^channel-bus]: `src/py/vertebra/server/_channels.py::ChannelBus` — Engine-neutral ChannelBus.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` — Fail-loud queue baseline requiring explicit executor= collaborators.
