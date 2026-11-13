---
type: Spec
flow_id: queues-tasks-and-channels
chapter_id: ch01-queue-store-capabilities-and-native-stores
title: "Chapter 01: Sync/Async QueueStore Protocols & Native PostgreSQL/Oracle Queue Stores"
description: "Split QueueStore into SyncQueueStore / AsyncQueueStore and implement SQL-backed SyncPostgresQueueStore / AsyncPostgresQueueStore and SyncOracleQueueStore / AsyncOracleQueueStore."
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
  - capabilities
sources:
  - id: queue-cap
    resource: src/py/vertebra/db/driver/capabilities.py::QueueStore
    title: "Current async-only QueueStore protocol"
  - id: pg-drv
    resource: src/py/vertebra/db/adapters/postgres/driver.py
    title: "Native PostgreSQL driver"
  - id: ora-drv
    resource: src/py/vertebra/db/adapters/oracle/driver.py
    title: "Native Oracle driver"
---

# Chapter 01: Sync/Async QueueStore Protocols & Native PostgreSQL/Oracle Queue Stores

<!-- truth: start -->
- **Baseline (`18baadcd`)**: `QueueStore` in `db/driver/capabilities.py` is a single unprefixed async protocol with zero concrete implementations [^queue-cap].
- **Target Architecture**: `@runtime_checkable SyncQueueStore` and `AsyncQueueStore` (zero aliases) are implemented natively by `SyncPostgresQueueStore`/`AsyncPostgresQueueStore` (`vertebra._postgres`) and `SyncOracleQueueStore`/`AsyncOracleQueueStore` (`vertebra._oracle`), consuming `Sync*NotificationSource`/`Async*NotificationSource` from upstream `native-redis-driver:ch05` [^pg-drv] [^ora-drv].
<!-- truth: end -->

## 1. Specification, Context & Provenance

This chapter splits `QueueStore` into `SyncQueueStore` and `AsyncQueueStore` (per User Decision `A3`) and builds the SQL-backed PostgreSQL and Oracle queue stores inside `vertebra.db.adapters.{postgres,oracle}.queue` [^queue-cap].

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

## 3. Documentation & OKF Knowledge Plan

Protocol and store capabilities are documented in `ch07-queues-channels-docs-and-okf`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Chapter / Task | Primary Files | Verification |
| --- | --- | --- | --- |
| `R1` `SyncQueueStore` / `AsyncQueueStore` | `t01` | `db/driver/{capabilities,__init__}.py`, `db/__init__.py` | `pytest test_native_db_drivers.py` |
| `R2` PostgreSQL queue stores | `t02` | `db/adapters/postgres/{queue,__init__}.py` | `pytest test_postgres_native_queries.py` |
| `R2` Oracle queue stores | `t03` | `db/adapters/oracle/{queue,__init__}.py` | `pytest test_oracle_adapter.py` |

## 5. Implementation Plan

- [ ] [t01-sync-async-queue-store-protocols.md](tasks/t01-sync-async-queue-store-protocols.md) — Split `QueueStore` into `@runtime_checkable SyncQueueStore` and `AsyncQueueStore` in `capabilities.py`.
- [ ] [t02-postgres-sync-async-queue-store.md](tasks/t02-postgres-sync-async-queue-store.md) — Implement `SyncPostgresQueueStore` and `AsyncPostgresQueueStore` (`FOR UPDATE SKIP LOCKED`, heartbeats, DLQ, cron leases).
- [ ] [t03-oracle-sync-async-queue-store.md](tasks/t03-oracle-sync-async-queue-store.md) — Implement `SyncOracleQueueStore` and `AsyncOracleQueueStore` (`FOR UPDATE SKIP LOCKED` + TxEventQ wakeups).

## 6. Current State & Remaining Plan Snapshot

Chapter `ch01-queue-store-capabilities-and-native-stores` is `planned` behind [`../../spec.md`](../../spec.md) prerequisites (`native-postgres-driver` and `native-redis-driver`).

Fast Chapter Gate (`< 60s` offline):

```bash
uv run --no-sync ruff check src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync mypy src/py/vertebra/db/driver src/py/vertebra/db/adapters/postgres src/py/vertebra/db/adapters/oracle && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_queries.py src/py/tests/unit/db/test_oracle_adapter.py
```

[^queue-cap]: `src/py/vertebra/db/driver/capabilities.py::QueueStore` — Current async-only QueueStore protocol.
[^pg-drv]: `src/py/vertebra/db/adapters/postgres/driver.py` — Native PostgreSQL driver.
[^ora-drv]: `src/py/vertebra/db/adapters/oracle/driver.py` — Native Oracle driver.
