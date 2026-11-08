---
type: Spec
flow_id: queues-tasks-and-channels
title: "Native Queues, Tasks, Channels, and Stores"
description: "Build native Vertebra QueueStore backends for PostgreSQL and Oracle on the standalone Rust driver wheels, plug them into first-party QueueService, litestar-queues, and Django tasks with settings-driven selection and DI, unify cron parsing, provide distributed Litestar/Starlette channel backends, and consolidate ValkeyStore."
status: draft
state: draft
has_chapters: false
parent_prd: integration-remediation
depends_on_flows:
  - driver-fail-loud
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 1
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: "2026-11-03T03:30:00Z"
updated_at: "2026-11-08T00:00:00Z"
generated:
  by: flow-setup
  at: "2026-11-07T04:15:00Z"
verified:
  - by: agent:flow-setup
    at: "2026-11-07T04:15:00Z"
    commit: cdb978d3
sources:
  - id: queue-service
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: First-party QueueService backend registry and cron scheduling
  - id: litestar-queues
    resource: src/py/vertebra/extensions/litestar/queues/__init__.py
    title: litestar-queues extension integration
  - id: litestar-channels
    resource: src/py/vertebra/extensions/litestar/channels.py
    title: Litestar pub/sub channels integration
  - id: fail-loud-baseline
    resource: .agents/bundles/specs/driver-fail-loud/spec.md
    title: Upstream flow that deletes production in-memory queue executors and clients and requires executor= or client=
tags:
  - queues
  - tasks
  - django
  - litestar
  - channels
  - stores
  - di
---

# Flow Charter: Native Queues, Tasks, Channels, and Stores

<!-- truth: start -->
- **Durable Native `QueueStore` Backends**: `PostgresQueueStore` (`vertebra-postgres`) and `OracleQueueStore` (`vertebra-oracle`) implement `FOR UPDATE SKIP LOCKED` leases, fenced completions, retries, DLQ transitions, and `NotificationSource` wakeups, and serve as the live executors that `PostgreSQLQueueBackend` and `OracleQueueBackend` require [^queue-service] [^fail-loud-baseline].
- **Framework Task & DI Integration**: `QueueService`, `litestar-queues` (`PostgresQueueBackend`, `OracleQueueBackend`), and Django `django.tasks` resolve handler dependencies through `framework/di.py::Container` [^litestar-queues].
- **Distributed Channels & `ValkeyStore`**: Litestar and Starlette `ChannelsBackend`s run on `NotificationSource` without `swap_channels_backend` monkeypatching, and `ValkeyStore` consolidates onto `vertebra._redis` [^litestar-channels].
<!-- truth: end -->

## Charter

Vertebra executes durable queuing and pub/sub on its own Rust-backed adapter primitives and plugs cleanly into framework contracts. This flow builds native `QueueStore` implementations for **PostgreSQL and Oracle**, wires them into first-party `QueueService`, `litestar-queues`, and Django `django.tasks` with DI-resolved task dependencies, provides distributed Litestar/Starlette channel backends over `NotificationSource`, and consolidates `ValkeyStore`.

## Baseline

[`driver-fail-loud`](../driver-fail-loud/spec.md) runs first: production in-memory queue executors and clients are deleted; `OracleQueueBackend`, `PostgreSQLQueueBackend`, and `SpannerQueueBackend` require an injected `executor=` and `RedisQueueBackend` / `ValkeyQueueBackend` a live `client=` (or a `redis://host` URL with `redis` installed), raising `MissingDependencyError` or `ImproperConfigurationError` otherwise; the executor and stream-client fakes live in the queue test modules [^fail-loud-baseline]. This flow owns the live `PostgresQueueStore` and `OracleQueueStore` executors.

## Scope

1. **Native queue stores.** `vertebra/db/adapters/postgres/queue.py::PostgresQueueStore` (`vertebra-postgres`) and `vertebra/db/adapters/oracle/queue.py::OracleQueueStore` (`vertebra-oracle`) implement `QueueStore` (`db-layout-and-contract`):
   - Single and bulk `enqueue`, `FOR UPDATE SKIP LOCKED` `claim` with visibility leases, fenced `complete`/`fail` on `(job_id, attempt)`, `heartbeat`, exponential retry backoff, dead-letter queue transition, `promote_scheduled`, idempotent DDL (`ensure_schema`), and `NotificationSource` wakeups (`PostgresNotificationSource` `LISTEN`/`NOTIFY` and Oracle TxEventQ / polling fallback).
2. **First-party `QueueService`.**
   - Convert `framework/queues/base.py::QueueService._build_backend` to a config-keyed backend registry driven by `QueueBackendSettings(kind="postgres" | "oracle" | "memory", ...)`.
   - Bind `PostgresQueueStore` and `OracleQueueStore` as the `executor=` that `PostgreSQLQueueBackend` and `OracleQueueBackend` require when settings select `kind="postgres"` or `kind="oracle"`, keeping `MemoryQueueBackend` strictly for explicit `kind="memory"` single-process/test use.
   - Keep one cron parser (`framework/queues/scheduler.py::CronExpression`) and delete `base.py::_cron_matches` / `_cron_matches_field`.
3. **`litestar-queues` integration.**
   - Add `PostgresQueueBackend` and `OracleQueueBackend` in `extensions/litestar/queues/backend.py` implementing `litestar_queues` `BaseQueueBackend` (claim fencing, heartbeats, maintenance leases, execution refs, and statistics) over `PostgresQueueStore` and `OracleQueueStore`, selected by settings alongside `SHMQueueBackend` for single-host deployments.
4. **Django `django.tasks` integration.**
   - Implement `extensions/django/tasks.py::TaskBackend` (`django.tasks.backends.base.BaseTaskBackend`) backed by `QueueService`, add a Django management command to run workers, and fold `extensions/django/queues.py` into `tasks.py`.
5. **Dependency injection.**
   - Resolve task handler parameters through `framework/di.py::Container` within a per-task lifecycle scope across `QueueService`, `litestar-queues`, and Django tasks.
6. **Distributed channels.**
   - Building on `ChannelBus` (decoupled from `DatabaseChannelBackend` in `db-engine-primitives:t03`), provide Litestar and Starlette `ChannelsBackend` implementations backed by `PostgresNotificationSource`, `OracleNotificationSource`, and `RedisNotificationSource`, and remove `extensions/litestar/channels.py::swap_channels_backend` monkeypatching.
7. **Key-value stores.**
   - Implement `ValkeyStore(litestar.stores.base.Store)` in `extensions/litestar/store.py` (consuming `vertebra.db.adapters.valkey` / `vertebra._redis` with RESP3 client-side tracking invalidation), replacing `framework/stores.py::KeyValueStore` duplication and aligning `extensions/starlette/store.py`.
8. **Docs & OKF.** Update `docs/usage/queues-and-realtime.md`, `docs/reference/queues.md`, and `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md` from the executor-or-raise and client-or-raise baseline to the live PostgreSQL and Oracle stores.

## Out of Scope

Spanner and Redis/Valkey native `QueueStore` backends (deferred to PRD backlog); the live Spanner queue executor is a follow-up opened with `/flow:plan spanner-queue-executor`.

## Public Seams

`PostgresQueueStore`, `OracleQueueStore`, `QueueService`, `QueueBackend`, `QueueBackendSettings`, `PostgresQueueBackend`, `OracleQueueBackend`, `TaskBackend`, distributed `ChannelsBackend`s, and `ValkeyStore`.

## Dependencies

[`driver-fail-loud`](../driver-fail-loud/spec.md); `native-data-platform` `native-postgres-driver`, `native-oracle-driver`, and `native-redis-driver` (`main @ 80ff8a65`); `integration-remediation` `db-engine-primitives` and `sqlspec-adapter-parity`.

## Acceptance Boundary

- Multi-process worker delivery, lease expiration recovery, and fenced completion integration tests pass against PostgreSQL and Oracle 23ai/26ai Free via `pytest-databases`.
- A `litestar-queues` application executes tasks end-to-end on `PostgresQueueBackend`, and Django `task.enqueue()` executes via the worker command with DI-injected dependencies.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Draft Task Outline

- t01 `PostgresQueueStore` (`FOR UPDATE SKIP LOCKED`, leases, wakeups).
- t02 `OracleQueueStore` (`FOR UPDATE SKIP LOCKED`, leases, TxEventQ wakeups).
- t03 `QueueService` registry, `QueueBackendSettings`, live store executor binding, and single `CronExpression` parser.
- t04 `litestar-queues` native PostgreSQL backend.
- t05 `litestar-queues` native Oracle backend.
- t06 Django `django.tasks` backend, worker management command, and per-task DI scope.
- t07 Distributed Litestar/Starlette `ChannelsBackend`s over `NotificationSource` + `swap_channels_backend` removal.
- t08 `ValkeyStore` consolidation + docs & OKF update.

[^queue-service]: `src/py/vertebra/framework/queues/base.py::QueueService` — First-party QueueService backend registry and cron scheduling.
[^litestar-queues]: `src/py/vertebra/extensions/litestar/queues/__init__.py` — litestar-queues extension integration.
[^litestar-channels]: `src/py/vertebra/extensions/litestar/channels.py` — Litestar pub/sub channels integration.
[^fail-loud-baseline]: `.agents/bundles/specs/driver-fail-loud/spec.md` — Upstream flow that deletes production in-memory queue executors and clients and requires `executor=` or `client=`.
