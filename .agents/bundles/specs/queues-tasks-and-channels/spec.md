---
type: Spec
flow_id: queues-tasks-and-channels
title: Native Queues, Tasks, Channels, and Stores
description: Build native Vertebra QueueStore backends for PostgreSQL and Oracle on the standalone Rust driver wheels, plug them into first-party QueueService, litestar-queues, and Django tasks with settings-driven selection and DI, unify cron parsing, provide distributed Litestar/Starlette channel backends, and consolidate ValkeyStore.
state: draft
parent_prd: integration-remediation
research:
  - ../integration-remediation/research/audit-findings_20261103.md
plan_revision: 0
plan_commit: null
state_revision: 0
current_task: null
last_verified_checkpoint: null
created_at: 2026-11-03T03:30:00Z
updated_at: 2026-11-04T01:19:00Z
tags: [queues, tasks, django, litestar, channels, stores, di]
---

# Flow Charter: Native Queues, Tasks, Channels, and Stores

## Charter

Vertebra executes durable queuing and pub/sub on its own Rust-backed adapter primitives and plugs cleanly into framework contracts. This flow builds native `QueueStore` implementations for **PostgreSQL and Oracle**, wires them into first-party `QueueService`, `litestar-queues`, and Django `django.tasks` with DI-resolved task dependencies, provides distributed Litestar/Starlette channel backends over `NotificationSource`, and consolidates `ValkeyStore`.

## Scope

1. **Native queue stores.** `vertebra/db/adapters/postgres/queue.py::PostgresQueueStore` (`vertebra-postgres`) and `vertebra/db/adapters/oracle/queue.py::OracleQueueStore` (`vertebra-oracle`) implement `QueueStore` (`db-layout-and-contract`):
   - Single and bulk `enqueue`, `FOR UPDATE SKIP LOCKED` `claim` with visibility leases, fenced `complete`/`fail` on `(job_id, attempt)`, `heartbeat`, exponential retry backoff, dead-letter queue transition, `promote_scheduled`, idempotent DDL (`ensure_schema`), and `NotificationSource` wakeups (`PostgresNotificationSource` `LISTEN`/`NOTIFY` and Oracle TxEventQ / polling fallback).
2. **First-party `QueueService`.**
   - Convert `framework/queues/base.py::QueueService._build_backend` to a config-keyed backend registry driven by `QueueBackendSettings(kind="postgres" | "oracle" | "memory", ...)`.
   - Delete the in-memory fake engine modules `framework/queues/{postgres,oracle,spanner,redis}.py` and `_InMemoryRedisStreamClient`, keeping `MemoryQueueBackend` strictly for explicit `kind="memory"` single-process/test use.
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
8. **Docs & OKF.** Update `docs/usage/queues-and-realtime.md`, `docs/reference/queues.md`, and `.agents/bundles/knowledge/domains/framework/distributed-queues-and-email.md`.

## Out of Scope

Spanner and Redis/Valkey native `QueueStore` backends (deferred to PRD backlog).

## Public Seams

`PostgresQueueStore`, `OracleQueueStore`, `QueueService`, `QueueBackend`, `QueueBackendSettings`, `PostgresQueueBackend`, `OracleQueueBackend`, `TaskBackend`, distributed `ChannelsBackend`s, and `ValkeyStore`.

## Dependencies

`native-data-platform` `native-postgres-driver`, `native-oracle-driver`, and `native-redis-driver` (`main @ 80ff8a65`); `integration-remediation` `db-engine-primitives` and `sqlspec-adapter-parity`.

## Acceptance Boundary

- Multi-process worker delivery, lease expiration recovery, and fenced completion integration tests pass against PostgreSQL and Oracle 23ai/26ai Free via `pytest-databases`.
- A `litestar-queues` application executes tasks end-to-end on `PostgresQueueBackend`, and Django `task.enqueue()` executes via the worker command with DI-injected dependencies.
- `make lint-all`, `make py-test`, and `make docs` pass.

## Draft Task Outline

- t01 `PostgresQueueStore` (`FOR UPDATE SKIP LOCKED`, leases, wakeups).
- t02 `OracleQueueStore` (`FOR UPDATE SKIP LOCKED`, leases, TxEventQ wakeups).
- t03 `QueueService` registry, `QueueBackendSettings`, fake queue module removal, and single `CronExpression` parser.
- t04 `litestar-queues` native PostgreSQL backend.
- t05 `litestar-queues` native Oracle backend.
- t06 Django `django.tasks` backend, worker management command, and per-task DI scope.
- t07 Distributed Litestar/Starlette `ChannelsBackend`s over `NotificationSource` + `swap_channels_backend` removal.
- t08 `ValkeyStore` consolidation + docs & OKF update.
