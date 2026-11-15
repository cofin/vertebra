---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-stores-and-first-party-backends
title: 'Chapter 01: Sync/AsyncQueueStore Protocols, Native PG/Oracle Stores, and Auto-Wired Queue Backends'
description: Split QueueStore into @runtime_checkable SyncQueueStore and AsyncQueueStore, implement Sync/Async PostgreSQL and Oracle queue stores, add QueueBackendSettings, and auto-wire PostgreSQL, Oracle, Redis, and Valkey queue backends.
status: stable
state: planned
planning_status: complete
planning_quality: standard
planning_degraded_reason: null
plan_commit: 1b191388
updated_at: '2026-11-14T18:50:00Z'
generated:
  by: flow/0.30.1
  at: '2026-11-14T18:50:00Z'
verified:
- by: agent:plan-generator
  at: '2026-11-14T18:50:00Z'
  commit: 1b191388
tags:
- queues
- postgres
- oracle
- capabilities
- redis
- valkey
sources:
- id: queue-cap
  resource: src/py/vertebra/db/driver/capabilities.py::QueueStore
  title: Current async-only QueueStore protocol
- id: pg-drv
  resource: src/py/vertebra/db/adapters/postgres/driver.py
  title: Native PostgreSQL driver
- id: ora-drv
  resource: src/py/vertebra/db/adapters/oracle/driver.py
  title: Native Oracle driver
- id: queue-base
  resource: src/py/vertebra/framework/queues/base.py::QueueService
  title: First-party QueueService
- id: queue-pg
  resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
  title: PostgreSQL queue backend
- id: queue-ora
  resource: src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend
  title: Oracle queue backend
- id: queue-redis
  resource: src/py/vertebra/framework/queues/redis.py::RedisQueueBackend
  title: Redis and Valkey queue backends
---

# Chapter 01: Sync/AsyncQueueStore Protocols, Native PG/Oracle Stores, and Auto-Wired Queue Backends

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `QueueStore` in `db/driver/capabilities.py` is a single unprefixed async protocol with zero concrete implementations [^queue-cap].
- **Target Architecture**: `@runtime_checkable SyncQueueStore` and `AsyncQueueStore` (zero aliases) are implemented natively by `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` (`vertebra._postgres`) and `SyncOracleQueueStore`/`AsyncOracleQueueStore` (`vertebra._oracle`), consuming `Sync*NotificationSource`/`Async*NotificationSource` from upstream `native-redis-driver:ch03` [^pg-drv] [^ora-drv].
- **Baseline (`18baadcd`)**: `PostgreSQLQueueBackend` and `OracleQueueBackend` fail without manual `executor=` injection, `OracleQueueBackend` probes `oracledb`, `base.py` duplicates cron parsing, and `TaskEnvelope` sets `gc=False` on container fields [^queue-base] [^queue-pg] [^queue-ora].
- **Target Architecture**: `QueueService` selects backends via `QueueBackendSettings`; `PostgreSQLQueueBackend` and `OracleQueueBackend` auto-construct `Sync*QueueStore`/`Async*QueueStore` from configured drivers; `RedisQueueBackend`/`ValkeyQueueBackend` run on `vertebra._redis` streams/sorted-sets [^queue-redis].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter splits `QueueStore` into `SyncQueueStore` and `AsyncQueueStore` (per User Decision `A3`) and builds the SQL-backed PostgreSQL and Oracle queue stores inside `vertebra.db.adapters.{postgres,oracle}.queue` [^queue-cap].

This chapter wires Vertebra's first-party `QueueService` and backend implementations (`postgres`, `oracle`, `redis`, `valkey`, `shm`, `memory`) directly onto native drivers and queue stores with zero third-party client probes [^queue-base].

## 2. Requirements, Concrete Contracts & Architecture

- **R1 (`SyncQueueStore` / `AsyncQueueStore`)**: `@runtime_checkable` protocols in `db/driver/capabilities.py` with zero aliases.
- **R2 (`SyncPostgresQueueStore` / `AsyncPostgresQueueStore` & `SyncOracleQueueStore` / `AsyncOracleQueueStore`)**: `FOR UPDATE SKIP LOCKED` leases, `(job_id, attempt)` fencing, heartbeats, retries, DLQ, cron leases, and native notification wakeups.

```python
@runtime_checkable
class SyncQueueStore(Protocol):
    def ensure_schema(self) -> None: ...
    def enqueue(self, entry: QueueEntry) -> str: ...
    def enqueue_many(self, entries: Sequence[QueueEntry]) -> list[str]: ...
    def claim(self, queues: Sequence[str], *, limit: int = 1, worker_id: str, lease_seconds: float = 30.0) -> list[QueueEntry]: ...
    def ack(self, job_id: str, *, attempt: int) -> bool: ...
    def nack(self, job_id: str, *, attempt: int, error: str, retry_delay_s: float | None = None) -> bool: ...
    def heartbeat(self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float) -> int: ...
    def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool: ...
    def close(self) -> None: ...

@runtime_checkable
class AsyncQueueStore(Protocol):
    async def ensure_schema(self) -> None: ...
    async def enqueue(self, entry: QueueEntry) -> str: ...
    async def claim(self, queues: Sequence[str], *, limit: int = 1, worker_id: str, lease_seconds: float = 30.0) -> list[QueueEntry]: ...
    async def ack(self, job_id: str, *, attempt: int) -> bool: ...
    async def nack(self, job_id: str, *, attempt: int, error: str, retry_delay_s: float | None = None) -> bool: ...
    async def heartbeat(self, job_ids: Sequence[str], *, worker_id: str, lease_seconds: float) -> int: ...
    async def acquire_lease(self, lease_key: str, *, owner_id: str, ttl_seconds: float) -> bool: ...
    async def aclose(self) -> None: ...
```

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

Protocol and store capabilities are documented in `ch03-distributed-channels-stores-and-docs` (`t04`).

First-party queue settings and backend options are documented in `ch03-distributed-channels-stores-and-docs` (`t04`).

## 4. Requirement-to-Task Traceability Matrix

| Task | Title | Primary Files | Verification |
| --- | --- | --- | --- |
| `t01` | Split Sync/AsyncQueueStore Protocols and Add QueueBackendSettings & SHM Fixes | `src/py/vertebra/db/driver/capabilities.py`, `src/py/vertebra/db/driver/__init__.py`, `src/py/vertebra/db/__init__.py` (+3 more) | `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t02` | Implement Sync/Async PostgresQueueStore and OracleQueueStore | `src/py/vertebra/db/adapters/postgres/queue.py`, `src/py/vertebra/db/adapters/postgres/__init__.py`, `src/py/vertebra/db/adapters/oracle/queue.py` (+1 more) | `src/py/tests/unit/db/test_postgres_native_queries.py`, `src/py/tests/unit/db/test_oracle_adapter.py` |
| `t03` | Auto-Wire PostgreSQLQueueBackend and OracleQueueBackend Over Native Queue Stores | `src/py/vertebra/framework/queues/postgres.py`, `src/py/vertebra/framework/queues/oracle.py` | `src/py/tests/unit/framework/test_distributed_queues.py` |
| `t04` | Wire RedisQueueBackend and ValkeyQueueBackend Over vertebra._redis Streams & ZSETs | `src/py/vertebra/framework/queues/redis.py`, `src/py/vertebra/framework/queues/__init__.py` | `src/py/tests/unit/framework/test_distributed_queues.py` |

## 5. Implementation Plan

- [ ] [t01-queue-store-protocols-and-service-settings.md](tasks/t01-queue-store-protocols-and-service-settings.md) — Split QueueStore into SyncQueueStore and AsyncQueueStore in capabilities.py, add QueueBackendSettings/TaskResultRecord, drop gc=False on TaskEnvelope/DeadLetterEntry, and remove duplicate cron helpers.
- [ ] [t02-postgres-and-oracle-queue-stores.md](tasks/t02-postgres-and-oracle-queue-stores.md) — Build Sync/AsyncPostgresQueueStore (FOR UPDATE OF q SKIP LOCKED, pg_notify, zero correlated subqueries) and Sync/AsyncOracleQueueStore (FOR UPDATE SKIP LOCKED, TxEventQ).
- [ ] [t03-postgres-and-oracle-queue-backends.md](tasks/t03-postgres-and-oracle-queue-backends.md) — Auto-construct native queue stores in PostgreSQLQueueBackend and OracleQueueBackend (replacing oracledb probe with vertebra-core[oracle]).
- [ ] [t04-redis-and-valkey-queue-backends.md](tasks/t04-redis-and-valkey-queue-backends.md) — Wire RedisQueueBackend and ValkeyQueueBackend onto vertebra._redis XREADGROUP/XAUTOCLAIM consumer groups and delayed ZSET schedules.

## 6. Current State & Remaining Plan Snapshot

Chapter `ch01-queue-stores-and-first-party-backends` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync mypy src/py/vertebra/framework/queues src/py/vertebra/extensions/flask/queues.py && uv run --no-sync pytest src/py/tests/unit/framework/test_distributed_queues.py
```

[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py::QueueStore` — Current async-only QueueStore protocol.
[^pg-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL driver.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle driver.
[^queue-base]: `src/py/vertebra/framework/queues/base.py::QueueService` — First-party QueueService.
[^queue-pg]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — PostgreSQL queue backend.
[^queue-ora]: `src/py/vertebra/framework/queues/oracle.py::OracleQueueBackend` — Oracle queue backend.
[^queue-redis]: `src/py/vertebra/framework/queues/redis.py::RedisQueueBackend` — Redis and Valkey queue backends.
