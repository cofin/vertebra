---
type: Spec
flow_id: queues-tasks-and-channels
title: Native Queues, Tasks, Channels, and Stores
description: Build native Vertebra queue backends for Postgres and Oracle on the Rust drivers, plug them into first-party QueueService, litestar-queues, and Django tasks with settings-driven selection and DI, unify cron parsing, provide per-engine Litestar channel backends, and replace duplicated key-value stores.
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
updated_at: 2026-11-03T04:05:00Z
tags: [queues, tasks, django, litestar, channels, stores, di]
---

# Flow Charter: Native Queues, Tasks, Channels, and Stores

## Charter

Vertebra does the work with its own components and plugs into the other frameworks *(user direction)*. This flow builds native queue backends for **Postgres and Oracle** on the Rust drivers. The same backends serve the first-party `QueueService`, litestar-queues, and Django's native tasks, and all three resolve dependencies through Vertebra DI. Other engines build on this later.

## Scope

1. **Native queue stores.** `db/adapters/postgres/queue.py::PostgresQueueStore` and `db/adapters/oracle/queue.py::OracleQueueStore` implement the `QueueStore` protocol from `db-layout-and-contract`:
   - enqueue and bulk enqueue
   - `FOR UPDATE SKIP LOCKED` claim with visibility leases and fenced completion on `status` + `attempt`
   - heartbeats, retry with backoff, DLQ, and scheduled promotion
   - wakeup: Postgres LISTEN/NOTIFY through the native `NotificationSource`; Oracle TxEventQ or polling
   - idempotent DDL
2. **First-party service.**
   - `framework/queues/base.py::QueueService._build_backend` becomes a backend registry keyed by config type.
   - Settings select the backend: `QueueBackendSettings(kind="postgres" | "oracle" | "memory", ...)`.
   - Delete the fake engine modules `framework/queues/{postgres,oracle,spanner,redis}.py`, their in-memory executors, and the `_InMemoryRedisStreamClient` fallback.
   - Keep `MemoryQueueBackend` for tests and single-process use.
   - Keep one cron parser (`framework/queues/scheduler.py::CronExpression`) and delete `base.py::_cron_matches`/`_cron_matches_field`.
3. **litestar-queues.**
   - `extensions/litestar/queues/backend.py` gains native `VertebraPostgresQueueBackend`/`VertebraOracleQueueBackend` implementing litestar-queues `BaseQueueBackend`. They cover claim fencing, heartbeats, maintenance leases, execution refs, and statistics, with the native queue stores underneath.
   - `SHMQueueBackend` stays for single-host use.
   - Settings select the backend.
   - SQLSpec-config delegation through litestar-queues' SQLSpec backend is optional once [litestar-queues#186](https://github.com/cofin/litestar-queues/issues/186) ships.
4. **Django tasks.**
   - `extensions/django/tasks.py::VertebraTaskBackend` implements `django.tasks.backends.base.BaseTaskBackend` on `QueueService`.
   - A management command runs the worker.
   - `extensions/django/queues.py` is folded in.
5. **DI.** Task parameters resolve through `framework/di.py::Container` with a per-task scope in all three integrations.
6. **Channels.**
   - Litestar `ChannelsBackend` implementations for Postgres (LISTEN/NOTIFY) and Oracle (TxEventQ) on the native `NotificationSource`s, plus Redis/Valkey once `native-embedded-and-cache-drivers` lands.
   - `framework/channels.py::ChannelBus` is rebuilt on `NotificationSource`.
   - The `extensions/litestar/channels.py::swap_channels_backend` monkeypatching is removed.
7. **Stores.**
   - One `ValkeyStore(litestar.stores.base.Store)` in `db/adapters/valkey/` with RESP3 client-cache invalidation.
   - It replaces `framework/stores.py::KeyValueStore` and the duplicated parts of `extensions/litestar/store.py::MemoryStore`.
8. **Docs.** Correct `domains/framework/distributed-queues-and-email.md` and the queue usage docs.

## Out of Scope

Spanner and Redis queue backends (backlog).

## Public Seams

`QueueService`, `QueueBackend`, `QueueBackendSettings`, `VertebraPostgresQueueBackend`, `VertebraOracleQueueBackend`, `VertebraTaskBackend`, the channel backends, and `ValkeyStore`.

## Dependencies

native-data-platform `native-postgres-driver` and `native-oracle-driver`. integration-remediation `sqlspec-adapter-parity`, for DI wiring parity.

## Acceptance Boundary

- Cross-process delivery integration tests pass on Postgres and Oracle 23ai.
- A litestar-queues app runs tasks on `VertebraPostgresQueueBackend`.
- Django `enqueue()` runs through the worker with DI-resolved dependencies.
- `make lint-all` and `make py-test` pass.

## Draft Task Outline

- t01 `PostgresQueueStore`.
- t02 `OracleQueueStore`.
- t03 `QueueService` registry + settings + fake module deletion + single cron parser.
- t04 litestar-queues native Postgres backend.
- t05 litestar-queues native Oracle backend.
- t06 Django tasks backend + worker command + DI.
- t07 Channel backends + `ChannelBus`.
- t08 `ValkeyStore` + docs.
