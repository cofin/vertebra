---
type: Spec
flow_id: queues-tasks-and-channels
title: Working Queues, Tasks, Channels, and Stores
description: Give first-party queues real per-engine backends, expose them to litestar-queues and Django tasks with settings-driven backend selection and DI, unify cron parsing, provide per-engine Litestar channel backends, and replace duplicated key-value stores.
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
updated_at: 2026-11-03T03:30:00Z
tags: [queues, tasks, django, litestar, channels, stores, di]
---

# Flow Charter: Working Queues, Tasks, Channels, and Stores

## Charter

The first-party queue system must deliver work durably across processes. Today the Postgres, Oracle, and Spanner backends silently run in memory. The same engine-local queue stores back the first-party `QueueService`, a litestar-queues backend, and a Django native tasks backend. All three resolve task dependencies through vertebra DI.

## Scope

1. **Queue stores.** `db/<engine>/queue.py` implements the `QueueStore` protocol from Chapter 3 on the real drivers:
   - Postgres: `FOR UPDATE SKIP LOCKED` claims, LISTEN/NOTIFY wakeup, visibility leases, DLQ.
   - Oracle: `SKIP LOCKED` claims and leases.
   - Spanner: lease columns in read-write transactions, `ROW DELETION POLICY` on terminal rows.
   - Redis/Valkey: streams with consumer groups and `XAUTOCLAIM`.
   - SQLite and DuckDB are single-process only, and the docs say so.
2. **First-party service.**
   - `framework/queues/base.py::QueueService._build_backend` becomes a backend registry keyed by config type.
   - Delete the engine modules `framework/queues/{postgres,oracle,spanner,redis}.py` and their in-memory executors, and drop the `_InMemoryRedisStreamClient` fallback.
   - Keep `MemoryQueueBackend` for tests and single-process use.
   - Bound `_receipt_index`.
   - Keep one cron parser (`framework/queues/scheduler.py::CronExpression`) and delete `base.py::_cron_matches`/`_cron_matches_field`.
3. **litestar-queues** (Open Question Q2).
   - Settings-driven backend selection: `QueueBackendSettings(kind="sqlspec" | "shm" | "memory")`.
   - `sqlspec` runs litestar-queues' SQLSpec backend with a vertebra sqlspec adapter config. It uses the Chapter 4 resolver shim until [litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186) ships.
   - `shm` is `extensions/litestar/queues/backend.py::SHMQueueBackend`.
4. **Django tasks.** `extensions/django/tasks.py` provides a `django.tasks.backends.base.BaseTaskBackend` implementation on `QueueService`, plus a management command that runs the worker. `extensions/django/queues.py` is folded into it.
5. **DI.** Workers in all three integrations resolve task parameters through `framework/di.py::Container` with a per-task scope. Covered by tests for each integration.
6. **Channels.**
   - Per-engine Litestar `ChannelsBackend` implementations on the Chapter 3 `NotificationSource` primitives (Postgres, Oracle, Redis/Valkey).
   - `framework/channels.py::ChannelBus` is rebuilt on a `NotificationSource`.
   - `extensions/litestar/channels.py::swap_channels_backend` monkeypatching is removed in favor of explicit configuration.
7. **Stores.**
   - `framework/stores.py::KeyValueStore` and the duplicated parts of `extensions/litestar/store.py::MemoryStore` are replaced by one `ValkeyStore(litestar.stores.base.Store)` under `db/valkey/` that uses RESP3 client-side cache invalidation. In-process caching uses Litestar's `MemoryStore`.
8. **Docs.** Correct `domains/framework/distributed-queues-and-email.md` and the queue usage docs.

## Out of Scope

- A native full `BaseQueueBackend` for litestar-queues (backlog).

## Public Seams

`QueueService`, the `QueueBackend` protocol, `QueueBackendSettings`, `vertebra.extensions.django.tasks.VertebraTaskBackend`, the per-engine channel backends, and `ValkeyStore`.

## Dependencies

Chapter 3 (protocols, real drivers) and Chapter 4 (adapter configs, resolver shim).

## Acceptance Boundary

- Cross-process delivery integration tests pass on Postgres, Oracle, Spanner (emulator gaps `xfail`), and Redis.
- A litestar-queues app runs tasks through the vertebra sqlspec config.
- Django `enqueue()` runs through the worker with DI-resolved dependencies.
- `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 Postgres queue store.
- t02 Oracle and Spanner queue stores.
- t03 Redis/Valkey queue store.
- t04 `QueueService` backend registry, engine-module deletion, one cron parser.
- t05 litestar-queues settings-driven backends.
- t06 Django tasks backend + worker command + DI.
- t07 Per-engine channel backends and `ChannelBus`.
- t08 `ValkeyStore` replacing `KeyValueStore`/`MemoryStore` duplication + docs.
