---
type: Task
id: "native-postgres-driver:ch28:t01"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Activate strict PostgreSQL 14-18 driver conformance and live active-query cancellation
description: Enable native PostgreSQL types DDL, pipeline conformance, and pending_flow_id removal in test_driver_conformance.py, wire PG14-18 sslmode=disable fixtures in conftest.py, and verify live backend query cancellation and retirement in test_postgres_native.py.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch27:t04"
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/vertebra/db/adapters/postgres/config.py
  - src/py/vertebra/db/adapters/postgres/driver.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/tests/integration/conftest.py
tests:
  - src/py/tests/integration/db/test_driver_conformance.py
  - src/py/tests/integration/db/test_postgres_native.py
estimated_loc: 230
plan_revision: 5
plan_commit: fd571e3a
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
updated_at: "2026-11-08T02:05:22Z"
commit: null
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Integration pytest-databases PostgreSQL configuration fixtures
  - id: conformance-mod
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine driver conformance specification table
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Cancellation caller outcomes, retirement, and conformance contracts
tags:
  - postgres
  - conformance
  - cancellation
  - integration
---

# Task t01: Activate Strict PostgreSQL 14–18 Driver Conformance and Live Active-Query Cancellation

## Objective & Scope Boundary

Activate strict live PostgreSQL 14 through 18 conformance across `src/py/tests/integration/conftest.py`, `src/py/tests/integration/db/test_driver_conformance.py`, and `src/py/tests/integration/db/test_postgres_native.py` [^int-conftest] [^conformance-mod] [^pg-contracts]:
1. Add `_POSTGRES_TYPES_DDL` (`NUMERIC`, `BOOLEAN`, `BYTEA`, `TIMESTAMPTZ`, `JSONB`), set `supports_pipeline=True`, and clear `pending_flow_id=None` on the `postgres` `EngineConformanceSpec` in `test_driver_conformance.py` without altering other engines.
2. Update `vtb_postgres_config` and add parameterized `postgres_14_service`..`postgres_18_service` native configs in `integration/conftest.py` with explicit `sslmode="disable"`, clientless `psql` readiness, and zero `psycopg`/`asyncpg` fixtures.
3. Verify versioned PG14–18 conformance (`server_version_num` `140000`–`189999`), single-`Sync` pipeline atomic rollback, and live active-query cancellation (`driver.cancel()` vs `asyncio.Task.cancel()`) in `test_postgres_native.py`.

Out of scope: Logical replication containers (`ch28:t02`), `pgvector`/`PgBouncer` fixtures (`ch28:t03`), and mutating non-PostgreSQL `ENGINE_SPECS`.

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `vtb_postgres_config`, `vtb_postgres_driver`, and `vtb_sqlspec_postgres_config` fixtures [^int-conftest].
- [src/py/tests/integration/db/test_driver_conformance.py](../../../../../../../src/py/tests/integration/db/test_driver_conformance.py) — `EngineConformanceSpec`, `_TYPES_DDL`, and `ENGINE_SPECS` [^conformance-mod].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` `sslmode` and DSN options.
- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Canonical `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` exports.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Active-operation `driver.cancel()` vs `Task.cancel()` and physical session retirement contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py`:
  - `vtb_postgres_config(postgres_service: PostgresService) -> PostgreSQLConfig`: builds DSN from `postgres_service` with explicit `sslmode="disable"` (scoped strictly to local non-TLS PostgreSQL Docker containers).
  - Add version-matrix fixture `vtb_postgres_versioned_config` parameterized across `postgres_14_service`, `postgres_15_service`, `postgres_16_service`, `postgres_17_service`, and `postgres_18_service` yielding `(major_version: int, PostgreSQLConfig)` with `sslmode="disable"` and isolated test-owned schema/table cleanup in `finally`.
- `src/py/tests/integration/db/test_driver_conformance.py`:
  - Define `_POSTGRES_TYPES_DDL` using `BIGINT PRIMARY KEY`, `BIGINT`, `DOUBLE PRECISION`, `NUMERIC(18, 6)`, `BOOLEAN`, `TEXT`, `BYTEA`, `TIMESTAMPTZ`, and `JSONB`.
  - Update `ENGINE_SPECS` `"postgres"` entry: `types_ddl=_POSTGRES_TYPES_DDL`, `supports_pipeline=True`, `pending_flow_id=None`.
- `src/py/tests/integration/db/test_postgres_native.py`:
  - Execute versioned conformance assertions across PG14–18 verifying `SHOW server_version_num`, typed `NULL`, `Decimal`, UTF-8, `BYTEA`, `TIMESTAMPTZ`, `JSONB`, affected counts, `ConformanceItemRow` hydration, transactions, typed SQLSTATE errors, `ArrowCursor`, and pipeline rollback.
  - Execute live cancellation suites for sync (worker thread running `SELECT pg_sleep(30)`) and async (`asyncio.create_task` running `SELECT pg_sleep(30)`), using a separate native observer client (`sslmode="disable"`) polling `pg_stat_activity` until the target `pid` and query are active before issuing cancel.

## Edge Cases & Error Semantics

- Explicit `driver.cancel()` on an active sync or async `pg_sleep` raises `QueryCancelledError` carrying server `sqlstate == "57014"`.
- `asyncio` `task.cancel()` on an active async `pg_sleep` raises `asyncio.CancelledError` at the caller; server `57014` is drained by independent native cleanup and never replaces `CancelledError`.
- In both cancellation modes, the physical connection that may have received `CancelRequest` is retired so a subsequent `SELECT pg_backend_pid()` on the driver/pool returns a fresh backend PID; an ambient cancelled transaction stays failed (`InterfaceError` / aborted state) until explicit `rollback()`; a concurrent second borrower synchronized via barrier on the same pool completes unaffected by delayed cancellation.
- Under `VERTEBRA_CONFORMANCE_DOCKER=1`, unavailable Docker or container startup failure fails the test run rather than skipping or xfailing.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/integration/db/test_postgres_native.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Strict PG14–18 version matrix | Run conformance suite across `postgres_14_service`..`postgres_18_service` with `VERTEBRA_CONFORMANCE_DOCKER=1` | All 5 major versions (`140000 <= server_version_num < 190000`) pass CRUD, `_POSTGRES_TYPES_DDL`, SQLSTATE errors, `ArrowCursor`, and `pipeline()` rollback with `0` skips/xfails. |
| Explicit `driver.cancel()` (sync & async) | Start `SELECT pg_sleep(30)`, observe active `pid_1` in `pg_stat_activity`, call `driver.cancel()`, then run `SELECT pg_backend_pid()` | Active query raises `QueryCancelledError` (`sqlstate == "57014"`); next query succeeds on `pid_2 != pid_1`. |
| `asyncio.Task.cancel()` + concurrent borrower immunity | Task A runs `SELECT pg_sleep(30)` (`pid_a`), Task B borrows sibling lease on same pool (`pid_b`), cancel Task A | Task A raises `asyncio.CancelledError`, `pid_a` terminates and retires on server, Task B completes cleanly on `pid_b`, and next borrow gets `pid_c != pid_a`. |
| Cancelled ambient transaction | Cancel query inside `with`/`async with driver.transaction():` then attempt `SELECT 1` before `rollback()` | Subsequent statement inside failed scope is rejected; after exit/rollback, pool capacity is restored cleanly. |

## Verification

- Initial Red baseline: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -q -k postgres -rs`
- Final Green + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_native.py -q -k postgres -rs && uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_native.py`

## Acceptance Criteria

- [ ] `ENGINE_SPECS` for `postgres` uses `_POSTGRES_TYPES_DDL`, `supports_pipeline=True`, and `pending_flow_id=None`, passing strict `VERTEBRA_CONFORMANCE_DOCKER=1` across PG14–18 with zero skips or xfails (R25).
- [ ] Live sync/async `driver.cancel()` (`QueryCancelledError` `57014`) and `asyncio.Task.cancel()` (`asyncio.CancelledError`) verify server termination, physical session retirement, failed-transaction fencing, and concurrent-borrower immunity (R18, R25).

## Notes & Discoveries

- [2026-11-07 16:45] Keep `VERTEBRA_CONFORMANCE_POSTGRES_DSN` as a separate opt-in lane alongside the `VERTEBRA_CONFORMANCE_DOCKER=1` container matrix.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Integration `pytest-databases` PostgreSQL configuration fixtures.
[^conformance-mod]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine driver conformance specification table.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cancellation caller outcomes, retirement, and conformance contracts.
