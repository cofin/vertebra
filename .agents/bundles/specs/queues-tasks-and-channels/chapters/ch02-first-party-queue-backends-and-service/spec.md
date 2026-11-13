---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch02-first-party-queue-backends-and-service
title: "Chapter 02: First-Party QueueService Registry & Auto-Wired Native Backends"
description: "Introduce QueueBackendSettings, remove TaskEnvelope gc=False and duplicate cron helpers, and wire PostgreSQL, Oracle, Redis, Valkey, and SHM queue backends onto native drivers."
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 18baadcd
updated_at: "2026-11-12T23:55:00Z"
generated:
  by: flow-plan
  at: "2026-11-12T23:55:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T23:55:00Z"
    commit: 18baadcd
tags:
  - queues
  - postgres
  - oracle
  - redis
  - valkey
sources:
  - id: queue-base
    resource: src/py/vertebra/framework/queues/base.py::QueueService
    title: "First-party QueueService"
  - id: queue-pg
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: "PostgreSQL queue backend"
  - id: queue-ora
    resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
    title: "Oracle queue backend"
  - id: queue-redis
    resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
    title: "Redis and Valkey queue backends"
---

# Chapter 02: First-Party QueueService Registry & Auto-Wired Native Backends

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `PostgreSQLQueueBackend` and `OracleQueueBackend` fail without manual `executor=` injection, `OracleQueueBackend` probes `oracledb`, `base.py` duplicates cron parsing, and `TaskEnvelope` sets `gc=False` on container fields [^queue-base] [^queue-pg] [^queue-ora].
- **Target Architecture**: `QueueService` selects backends via `QueueBackendSettings`; `PostgreSQLQueueBackend` and `OracleQueueBackend` auto-construct `Sync*QueueStore`/`Async*QueueStore` from configured drivers; `RedisQueueBackend`/`ValkeyQueueBackend` run on `vertebra._redis` streams/sorted-sets [^queue-redis].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter wires Vertebra's first-party `QueueService` and backend implementations (`postgres`, `oracle`, `redis`, `valkey`, `shm`, `memory`) directly onto native drivers and queue stores with zero third-party client probes [^queue-base].

## 2. Requirements, Concrete Contracts & Architecture

- **R3 (`QueueBackendSettings` & Clean Models)**: Config-keyed `QueueService._build_backend`, `TaskResultRecord` persistence, `gc=False` removed from `TaskEnvelope`/`DeadLetterEntry`, and duplicate `base.py::_cron_matches*` deleted.
- **R4 (Auto-Wired Native Backends)**: `PostgreSQLQueueBackend`, `OracleQueueBackend` (`vertebra-core[oracle]` hint), `RedisQueueBackend`, and `ValkeyQueueBackend` build native stores/clients automatically from configured drivers.

```python
class QueueBackendSettings(msgspec.Struct, frozen=True):
    kind: Literal["postgres", "oracle", "redis", "valkey", "spanner", "shm", "memory"] = "shm"
    dsn: str | None = None
    visibility_timeout_seconds: float = 30.0
    poll_interval_seconds: float = 1.0
    consumer_name: str = "vertebra-worker"

class TaskEnvelope(msgspec.Struct, frozen=True):
    task_id: str
    task_name: str
    args: tuple[Any, ...] = ()
    kwargs: dict[str, Any] = msgspec.field(default_factory=dict)
    lane: TaskLane = "io"
    attempt: int = 0
    max_retries: int = 3
```

## 3. Documentation & OKF Knowledge Plan

First-party queue settings and backend options are documented in `ch07-queues-channels-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R3` `QueueBackendSettings` & `SHMQueueBackend` | `t01` | `framework/queues/{base,backends}.py`, `extensions/flask/queues.py` | `pytest test_distributed_queues.py` |
| `R4` PostgreSQL & Oracle auto-wired backends | `t02` | `framework/queues/{postgres,oracle}.py` | `pytest test_distributed_queues.py` |
| `R4` Redis & Valkey streams/sorted-sets | `t03` | `framework/queues/{redis,__init__}.py` | `pytest test_distributed_queues.py` |

## 5. Implementation Plan

- [ ] [t01-queue-service-settings-and-shm.md](tasks/t01-queue-service-settings-and-shm.md) — Add `QueueBackendSettings`, remove `gc=False` on `TaskEnvelope`/`DeadLetterEntry`, and harden `SHMQueueBackend`.
- [ ] [t02-postgres-and-oracle-queue-backends.md](tasks/t02-postgres-and-oracle-queue-backends.md) — Auto-wire `PostgreSQLQueueBackend` & `OracleQueueBackend` onto native queue stores and fix `vertebra-core[oracle]` hint.
- [ ] [t03-redis-and-valkey-queue-backends.md](tasks/t03-redis-and-valkey-queue-backends.md) — Wire `RedisQueueBackend` and `ValkeyQueueBackend` over `vertebra._redis` streams, sorted sets, and wakeups.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch02-first-party-queue-backends-and-service` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py
```

[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — First-party QueueService.
[^queue-pg]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend.
[^queue-ora]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis and Valkey queue backends.
