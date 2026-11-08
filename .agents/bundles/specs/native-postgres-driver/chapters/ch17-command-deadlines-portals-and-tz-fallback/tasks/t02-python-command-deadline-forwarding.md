---
type: Task
id: "native-postgres-driver:ch17:t02"
chapter_id: ch17-command-deadlines-portals-and-tz-fallback
title: "Forward Python query and query_multi command deadlines and verify live SQLSTATE precedence"
description: "Add keyword-only timeout: float | None = None to SyncPostgresClient and AsyncPostgresClient query and query_multi, update _postgres.pyi, and verify against live PostgreSQL."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch17:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch17-command-deadlines-portals-and-tz-fallback/spec.md
  - src/rs/vertebra-postgres/src/session.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
files:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/integration/db/test_postgres_command_deadlines.py
estimated_loc: 215
plan_revision: 7
plan_commit: 1c75af65
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-07T05:00:00Z"
commit: null
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen Python query and query_multi timeout=None signatures, reservation start rule, and non-preemptive decoding contract
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared PyO3 Tokio completion bridge and cancellation hook
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical OperationalError and QueryCancelledError in vertebra._vertebra
tags:
  - postgres
  - deadlines
  - pyo3
  - integration
---

# Task t02: Forward Python `query` and `query_multi` Command Deadlines and Verify Live SQLSTATE Precedence

## Objective & Scope Boundary

Extend `query` and `query_multi` on `SyncPostgresClient` and `AsyncPostgresClient` (`sync_client.rs`, `async_client.rs`, `src/py/vertebra/_postgres.pyi`) with keyword-only `timeout: float | None = None`, starting one absolute `CommandBudget` at native operation reservation before async worker scheduling, and verify live PostgreSQL behavior through `postgres_service` [^pg-contracts] [^py-async-rs]. High-level `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` `timeout=` forwarding and `_publish_notification(..., timeout=10.0)` belong to Chapters 25 and 26; no `timeout` parameter is added to `pipeline`, `COPY`, or `open_portal`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 17 Python deadline forwarding and live `pg_sleep` vs `statement_timeout` precedence rules.
- [contracts.md](../../../contracts.md) — Exact `query` / `query_multi` `timeout=None` signatures, reservation-time `Instant` computation, and non-preemptive CPU decode/projection contract [^pg-contracts].
- `src/rs/vertebra-postgres/src/session.rs` — `crate::session::CommandBudget` and `SessionHandle::{query_with_deadline, simple_query_with_deadline}` from `ch17:t01`.
- `src/rs/vertebra-postgres/src/sync_client.rs` — `PySyncPostgresClient::{query, query_multi}`.
- `src/rs/vertebra-postgres/src/async_client.rs` — `PyAsyncPostgresClient::{query, query_multi}` and `spawn_rust_db_task_with_attr_and_cancel` [^py-async-rs].
- `src/py/vertebra/_postgres.pyi` — Public type stub for `vertebra._postgres` [^db-errors-rs].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs` and `src/rs/vertebra-postgres/src/async_client.rs`:
  - Update `#[pyo3(signature = (sql, parameters=None, *, parameter_oids=None, timeout=None))]` on `query` and `#[pyo3(signature = (sql, *, timeout=None))]` on `query_multi` for both `SyncPostgresClient` and `AsyncPostgresClient`.
  - Reject `bool` passed as `timeout` (`TypeError`) and validate `crate::session::CommandBudget::validate_optional_seconds(timeout)` (`InterfaceError` / `ValueError` on `<= 0.0`, `NaN`, `+-inf`, or `Duration` overflow) BEFORE `SessionHandle` operation reservation or network I/O, preserving pipeline scope exclusivity (`SessionAccess::Ordinary`).
  - Compute the absolute `tokio::time::Instant` (`CommandBudget::start_from_duration`) **exactly once** when native operation reservation succeeds — BEFORE `spawn_rust_db_task_with_attr_and_cancel` schedules the async worker onto Tokio — so worker-queue wait after reservation consumes the command budget while pool `acquire_timeout`, `connect_timeout`, and `reset_timeout` remain independent [^pg-contracts].
  - Delegate to `SessionHandle::query_with_deadline` and `SessionHandle::simple_query_with_deadline` inside `py.detach(|| block_on_db(...))` (sync) and the guarded Tokio task (async).
- `src/py/vertebra/_postgres.pyi`:
  - Update exact method stubs and docstrings on `SyncPostgresClient` and `AsyncPostgresClient`:
    - `def query(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] = (), *, parameter_oids: Sequence[int] | None = None, timeout: float | None = None) -> PostgresQueryResult: ...`
    - `def query_multi(self, sql: str, *, timeout: float | None = None) -> tuple[PostgresQueryResult, ...]: ...`
    (and `Awaitable[...]` counterparts on `AsyncPostgresClient`), explicitly documenting that `timeout` bounds native command I/O after operation reservation and does not preempt CPU decoding or Python object projection.

## Edge Cases & Error Semantics

- **Invalid `timeout` values**: `0`, `-0.5`, `float("nan")`, `float("inf")`, `float("-inf")`, and `True`/`False` fail synchronously before reserving the session or emitting wire bytes; inside an active pipeline scope, ordinary reservation still fails with `InterfaceError`.
- **Live client deadline vs server `statement_timeout` vs `asyncio` cancel**:
  - Client `timeout=0.1` expiring during `SELECT pg_sleep(5)` raises `vertebra._vertebra.OperationalError` with `sqlstate is None` and `vendor_code is None`, cancels the backend query via `CancelRequest`, retires the physical socket, and yields a fresh `pg_backend_pid()` on the next `max_size=1` pool acquisition [^db-errors-rs].
  - Server `SET statement_timeout = '50ms'` firing before `timeout=5.0` raises `vertebra._vertebra.QueryCancelledError` with `sqlstate == "57014"`.
  - Outer `asyncio.wait_for(..., timeout=0.05)` firing before `timeout=5.0` raises `TimeoutError` / `asyncio.CancelledError` while native cleanup retires the socket.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/integration/db/test_postgres_command_deadlines.py` parameterized across `SyncPostgresClient` / `SyncPostgresPool` and `AsyncPostgresClient` / `AsyncPostgresPool` using primitive `postgres_service` fields (`host`, `port`, `user`, `password`, `database`, `sslmode="disable"`, `max_size=1`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Invalid `timeout` rejected before I/O | `timeout` in `(0, -1.0, float("nan"), float("inf"), True)` on `query` and `query_multi` | Raises `TypeError` / `ValueError` / `InterfaceError` synchronously with session remaining clean and reusable. |
| Default `timeout=None` & positive budget success | `query("SELECT $1::int4", [7])` and `query("SELECT 1", timeout=5.0)` | Both return `PostgresQueryResult` (`rows == ((7,),)` / `((1,),)`). |
| Client deadline during `pg_sleep` + PID replacement | `max_size=1` pool: acquire `c1`, record `pid1 = query("SELECT pg_backend_pid()")`, call `c1.query("SELECT pg_sleep(5)", timeout=0.1)` in `try/finally: c1.close()`, then acquire `c2` and query `pid2` | `pg_sleep` raises `vertebra._vertebra.OperationalError` with `exc.sqlstate is None` and `exc.vendor_code is None`; `c2` succeeds with `pid2 != pid1` and zero lingering `pg_sleep` in `pg_stat_activity`. |
| Shared `query_multi` command budget | `c.query_multi("SELECT pg_sleep(0.08); SELECT pg_sleep(0.08)", timeout=0.10)` | Combined simple-query duration exceeds single `0.10s` budget and raises `OperationalError` (`sqlstate is None`). |
| Server `statement_timeout` beats larger client `timeout` | `c.query("SET statement_timeout = '50ms'")`, then `c.query("SELECT pg_sleep(5)", timeout=5.0)` | Raises `vertebra._vertebra.QueryCancelledError` with `exc.sqlstate == "57014"`. |

## Verification

- Initial Red baseline:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/integration/db/test_postgres_command_deadlines.py -q
  ```
- Final Green, stubtest, Ruff, Mypy, Clippy, and diff gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/integration/db/test_postgres_command_deadlines.py -q
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/integration/db/test_postgres_command_deadlines.py
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `SyncPostgresClient` and `AsyncPostgresClient` `query` and `query_multi` accept keyword-only `timeout: float | None = None`, validate inputs before reservation/I/O, and start one absolute `CommandBudget` at reservation (R6, R7, R12).
- [ ] Live `postgres_service` tests verify `OperationalError(sqlstate=None)` + fresh backend PID replacement on client deadline expiry and `QueryCancelledError(sqlstate="57014")` when `statement_timeout` fires first (R10, R18).
- [ ] `_postgres.pyi` matches the compiled extension (`mypy.stubtest vertebra._postgres`) and documents the I/O-only non-preemptive budget contract (R6, R7).
- [ ] `test_postgres_command_deadlines.py`, `mypy.stubtest vertebra._postgres`, Ruff, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen Python `timeout=None` command deadline signatures and precedence rules.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared PyO3 Tokio completion bridge.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `OperationalError` and `QueryCancelledError` classes.
