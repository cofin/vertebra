---
type: Spec
flow_id: distributed-queue-backends
title: "Chapter 1: Distributed Queue Backends, Cron Leader Election, and Multi-Lane Worker Runner"
state: planned
plan_revision: 2
plan_commit: ""
state_revision: 0
current_task: null
last_operation: null
operation_targets: []
last_verified_checkpoint: null
created_at: 2026-10-22T04:40:00Z
updated_at: 2026-10-22T04:40:00Z
description: "Implement the QueueBackend protocol, Valkey/Redis Streams backend, PostgreSQL SKIP LOCKED + LISTEN/NOTIFY backend, Oracle SKIP LOCKED backend, lease-elected CronScheduler, and MultiLaneWorkerRunner with LameDuck drain and OpenTelemetry spans."
tags: [feature, queues, worker, redis, postgres, oracle]
parent_prd: vertebra-queues-and-messaging
research: [../vertebra-design-quality/findings.md]
---

# Flow: Chapter 1: Distributed Queue Backends, Cron Leader Election, and Multi-Lane Worker Runner

**Flow ID:** `distributed-queue-backends`

## Specification

### Parent Context

This chapter is Chapter 1 of the [`vertebra-queues-and-messaging`](../vertebra-queues-and-messaging/spec.md) Master PRD and resolves the distributed queue follow-up recorded in `vertebra-design-quality` Task `5.7` (`FW-07`). It owns parent requirements **R1**, **R2**, **R3**, **R4**, **R5**, and the queue portion of **R9**. It has no prerequisite within `vertebra-queues-and-messaging` and provides `QueueService` and `QueueBackend` required by Chapter 2 Task `2.3` (`EmailService.enqueue`).

### Code Analysis Summary

Inspected at repository checkout:

- **`src/py/vertebra/framework/queues/__init__.py` (145 lines)**:
  - Defines `TaskLane = Literal["cpu", "io", "agent"]` (`:25`), `TaskRetryPolicy` (`:28-35`), `CronSchedule` (`:37-42`), `TaskDef` (`:44-52`), `TaskEnvelope` (`:54-62`), `QueueConfig` (`:65-71`), `QueueService` (`:73-125`), and `@task` (`:127-145`).
  - `QueueConfig` (`:65-71`) only carries `backend_url: str = "memory://"`, `default_lane: TaskLane = "io"`, and `max_concurrency: int = 32`. It lacks `visibility_timeout_seconds`, `consumer_group`, `consumer_name`, `stream_prefix`, `dlq_max_len`, `poll_interval_seconds`, and `lease_ttl_seconds`.
  - `TaskEnvelope` (`:54-62`) lacks `scheduled_at: float | None`, `enqueued_at: float`, `idempotency_key: str | None`, `traceparent: str | None`, and `receipt_handle: str | None` needed for visibility-timeout acknowledgment (`XACK` entry ID or DB row ID) and W3C trace propagation across processes.
- **`src/py/vertebra/server/lanes.py` & `src/py/vertebra/cli/workers.py` (73 lines)**:
  - `WorkerRunner` (`lanes.py`) manages per-lane `asyncio.Semaphore` admission and `stop()`, while `execute_worker_cli` (`cli/workers.py:16-42`) sleeps on `asyncio.Event().wait()` (`:36`) without pulling tasks from `QueueService`, running `CronScheduler`, or transitioning `LameDuckController` (`src/py/vertebra/server/platform.py:71-100`) on `SIGTERM`.
- **`src/py/vertebra/db/redis.py` (141 lines), `src/py/vertebra/db/valkey.py` (161 lines), `src/py/vertebra/db/postgres.py` (173 lines), `src/py/vertebra/db/oracle/driver.py` (183 lines)**:
  - `SyncRedisDriver.encode_command` (`db/redis.py:53-66`) and `ValkeyConfig` (`db/valkey.py:19-58`) provide RESP3 wire framing; `PostgresConfig.effective_direct_dsn` (`db/postgres.py:59-62`) separates the direct primary DSN (required for `LISTEN/NOTIFY`) from transaction-pooled connections.
- **`src/py/vertebra/telemetry/otel.py` (168 lines)**:
  - `TraceContext` (`:20-89`) formats and parses 55-character W3C `traceparent` strings; `SpanData` (`:97-107`) and `SpanExporter` (`:110-126`) record spans in a bounded ring buffer.

#### Findings carried

| Finding | Source | Resolution in this chapter |
| --- | --- | --- |
| `FW-07` | `vertebra-design-quality` (`framework.md`) | Replace in-memory-only `QueueService` with pluggable `QueueBackend` implementations (`memory://`, `valkey://`/`valkeys://`, `redis://`/`rediss://`, `postgresql://`, `oracle://`), exponential backoff retry, DLQ routing, distributed cron leader election, and `MultiLaneWorkerRunner`. |

### Relevant Patterns

- [Product Guidelines](../../product/product-guidelines.md) — `msgspec.Struct(frozen=True, gc=False)`, `array_like=True` on `TaskEnvelope` and `DeadLetterEntry`, `vertebra.utils.serializers` (`encode_msgpack`/`decode_msgpack` and `encode_json_bytes`/`decode_json`), zero inline `#` comments.
- [Python Module and Class Layout](../../knowledge/patterns/python/module-and-class-layout.md) — strict ordering of class members and top-level imports with `import_optional` guards for optional driver packages.
- [Hot-Path and Async Dispatch](../../knowledge/patterns/python/hot-path-and-async-dispatch.md) — non-blocking `asyncio` pull loops, zero-poll `LISTEN/NOTIFY` wakeup events, and bounded per-lane semaphores.

### Requirements

| ID | Requirement |
| --- | --- |
| C1-R1 | Define `QueueBackend` protocol, `MemoryQueueBackend`, `DeadLetterEntry`, `CronLease`, and expanded `TaskEnvelope`/`QueueConfig` in `src/py/vertebra/framework/queues/base.py`, wired through `src/py/vertebra/framework/queues/__init__.py`. |
| C1-R2 | Implement `RedisQueueBackend` and `ValkeyQueueBackend` in `src/py/vertebra/framework/queues/redis.py` using Streams (`XADD`, `XGROUP CREATE ... MKSTREAM`, `XREADGROUP`), `XACK`, `XAUTOCLAIM` visibility timeout recovery, atomic Lua ZSET-to-Stream delayed promotion (Decision D1), and `<prefix>:dlq` dead-letter streams. |
| C1-R3 | Implement `PostgresQueueBackend` in `src/py/vertebra/framework/queues/postgres.py` (`FOR UPDATE SKIP LOCKED`, `LISTEN/NOTIFY` over `effective_direct_dsn` per Decision D2, exponential backoff retry, and `vertebra_queue_dlq`) and `OracleQueueBackend` in `src/py/vertebra/framework/queues/oracle.py` (`FOR UPDATE SKIP LOCKED` dequeue, retry rescheduling, and `vertebra_queue_dlq`). |
| C1-R4 | Implement `CronScheduler` in `src/py/vertebra/framework/queues/scheduler.py` with 5-field cron expression evaluation (`minute`, `hour`, `day_of_month`, `month`, `day_of_week` supporting `*`, `*/step`, `a-b`, `a,b`), `zoneinfo.ZoneInfo` timezone evaluation, and distributed lease-based leader election via `QueueBackend.acquire_lease`/`renew_lease`. |
| C1-R5 | Implement `MultiLaneWorkerRunner` in `src/py/vertebra/server/lanes.py` and update `src/py/vertebra/cli/workers.py` to pull from `QueueService` across `cpu`, `io`, and `agent` lanes, coordinate graceful `SIGTERM` drain with `LameDuckController`, and record `messaging.enqueue`, `messaging.process`, and `messaging.settle` spans in `SpanExporter`. |
| C1-R6 | Validate all four queue backends, visibility timeout recovery, cron leader election, DLQ replay, LameDuck drain, and OpenTelemetry spans in `src/py/tests/unit/framework/test_distributed_queues.py`. |

### Target Layout

```text
src/py/vertebra/
  framework/
    queues/
      __init__.py          Public re-exports and QueueService.from_config / URL scheme routing
      base.py              TaskLane, TaskRetryPolicy, CronSchedule, TaskDef, TaskEnvelope, DeadLetterEntry, CronLease, QueueConfig, QueueBackend, MemoryQueueBackend, QueueService, task
      redis.py             RedisQueueBackend, ValkeyQueueBackend (Streams XREADGROUP/XACK/XAUTOCLAIM, Lua ZSET promotion, DLQ stream, SET NX PX lease)
      postgres.py          PostgresQueueBackend (SKIP LOCKED, LISTEN/NOTIFY wakeup, DLQ table, lease table)
      oracle.py            OracleQueueBackend (SKIP LOCKED cursor locking, retry/DLQ table, MERGE lease)
      scheduler.py         CronField, CronExpression, CronScheduler (lease-based leader election + tick dispatch)
  server/
    lanes.py               LaneConfig, LaneMetrics, MultiLaneWorkerRunner (QueueService pull loops, LameDuck drain, SpanExporter messaging.* instrumentation)
  cli/
    workers.py             execute_worker_cli and vtb workers run wired to MultiLaneWorkerRunner, CronScheduler, and LameDuckController
src/py/tests/unit/framework/
  test_distributed_queues.py   Comprehensive unit and contract test suite for all backends, scheduler, lanes, and telemetry
```

### Risks

| Risk | Mitigation |
| --- | --- |
| Crashed worker leaves claimed task invisible forever. | `RedisQueueBackend`/`ValkeyQueueBackend` runs `XAUTOCLAIM` with `min_idle_time = visibility_timeout_ms`; `PostgresQueueBackend` and `OracleQueueBackend` select rows where `status = 'pending' OR (status = 'running' AND locked_until <= NOW())`. |
| Multiple worker processes enqueue duplicate cron jobs on the same minute tick. | `CronScheduler` acquires a distributed lease (`acquire_lease("cron:leader", owner_id, ttl_seconds)`) and records `last_dispatched_slot` (`YYYY-MM-DDTHH:MM`) in the lease metadata so failovers within the same minute never double-enqueue. |
| Transaction-mode PgBouncer drops `LISTEN/NOTIFY` state. | `PostgresQueueBackend` opens its `LISTEN` subscriber on `PostgresConfig.effective_direct_dsn` (`direct_dsn or dsn`) and keeps a fallback jittered poll timer (`poll_interval_seconds`). |

## Implementation Plan

### Phase 1: Queue Backends (`Redis`/`Valkey`, `PostgreSQL`, `Oracle`)
- [ ] 1.1 `QueueBackend` protocol, `MemoryQueueBackend`, and `ValkeyQueueBackend` / `RedisQueueBackend` ([worksheet](tasks/1.1.md))
- [ ] 1.2 `PostgresQueueBackend` (`SKIP LOCKED` + `LISTEN/NOTIFY`) and `OracleQueueBackend` ([worksheet](tasks/1.2.md))
- [ ] Checkpoint: `uv run ruff check src/py/vertebra/framework/queues/ && uv run mypy src/py/vertebra/framework/queues/`

### Phase 2: Distributed Cron Scheduler, Multi-Lane Worker Runner & Verification
- [ ] 1.3 Distributed cron scheduler, `MultiLaneWorkerRunner`, `LameDuckController` drain, and OpenTelemetry `messaging.*` spans ([worksheet](tasks/1.3.md))
- [ ] 1.4 Concurrency, visibility-timeout recovery, cron leader-election, and DLQ tests across all queue backends ([worksheet](tasks/1.4.md))
- [ ] Chapter gate: `make lint-all && make py-test`

## Requirement-to-task trace

| Requirement | Tasks | Verification |
| --- | --- | --- |
| C1-R1 | 1.1 | `uv run pytest src/py/tests/unit/framework/test_framework_subdomains.py -k queues -q` |
| C1-R2 | 1.1, 1.4 | `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -k "redis or valkey" -q` |
| C1-R3 | 1.2, 1.4 | `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -k "postgres or oracle" -q` |
| C1-R4 | 1.3, 1.4 | `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -k "cron or lease" -q` |
| C1-R5 | 1.3, 1.4 | `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py src/py/tests/unit/extensions/test_worker_lanes.py -q` |
| C1-R6 | 1.4 | `uv run pytest src/py/tests/unit/framework/test_distributed_queues.py -q` |

## Finding-to-task trace

| Finding | Task |
| --- | --- |
| FW-07 | 1.1, 1.2, 1.3, 1.4 |

## Continuity Snapshot

- Active flow: `distributed-queue-backends`, state `planned`. Current task: none. Next step: `/flow:implement distributed-queue-backends` after Master PRD approval.
- Plan identity: plan_revision 2, plan_commit "". State identity: state_revision 0.
- Blockers: none. Decisions: inherited D1 (`EVALSHA` atomic ZSET-to-Stream promotion) and D2 (`LISTEN/NOTIFY` over `effective_direct_dsn`) from `vertebra-queues-and-messaging`.
- Knowledge paths: `.agents/bundles/product/product-guidelines.md`, `.agents/bundles/knowledge/patterns/python/module-and-class-layout.md`, `.agents/bundles/knowledge/patterns/python/hot-path-and-async-dispatch.md`.
