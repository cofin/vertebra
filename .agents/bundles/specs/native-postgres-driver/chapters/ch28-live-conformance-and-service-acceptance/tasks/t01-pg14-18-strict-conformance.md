---
type: Task
id: "native-postgres-driver:ch28:t01"
chapter_id: ch28-live-conformance-and-service-acceptance
title: Activate strict PostgreSQL 14-18 driver conformance and live active-query cancellation
description: Gate PostgreSQL live conformance on VERTEBRA_CONFORMANCE_DOCKER or VERTEBRA_CONFORMANCE_POSTGRES_DSN with strict zero-skip enforcement, enable _POSTGRES_TYPES_DDL and pipeline conformance across PG14-18, and verify live backend query cancellation and retirement.
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
estimated_loc: 245
plan_revision: 6
plan_commit: bf53faf3
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
updated_at: "2026-11-10T14:45:00Z"
commit: null
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: bf53faf3
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_postgres_config
    title: Integration pytest-databases PostgreSQL fixtures and session zero-skip enforcement
  - id: conformance-mod
    resource: src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS
    title: Cross-engine driver conformance specification table and _resolve_live_config
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

Activate strict, offline-safe live PostgreSQL 14 through 18 conformance across `src/py/tests/integration/conftest.py`, `src/py/tests/integration/db/test_driver_conformance.py`, and `src/py/tests/integration/db/test_postgres_native.py` [^int-conftest] [^conformance-mod] [^pg-contracts]:
1. Update `_resolve_live_config()` in `test_driver_conformance.py` so `postgres` runs `_require_native_module(spec)` first and skips cleanly when neither `VERTEBRA_CONFORMANCE_POSTGRES_DSN` nor `VERTEBRA_CONFORMANCE_DOCKER == "1"` is set (keeping offline `make py-test`, `make coverage`, and `.github/workflows/ci.yml` `test-python` lanes green), while setting `types_ddl=_POSTGRES_TYPES_DDL`, `supports_pipeline=True`, and `pending_flow_id=None`.
2. Gate all live Docker PostgreSQL fixtures (`vtb_postgres_config`, `vtb_postgres_versioned_config`) in `integration/conftest.py` lazily on `VERTEBRA_CONFORMANCE_DOCKER == "1"` (checking `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` first, resolving `postgres_service` / `postgres_14_service`..`postgres_18_service` via `request.getfixturevalue` only after the gate passes, and attaching `sslmode="disable"`), plus add a `pytest_runtest_logreport` / `pytest_sessionfinish` enforcement hook failing loud whenever `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS == "1"` records any skipped or xfailed PostgreSQL test (or `0` executed PostgreSQL tests).
3. Verify parameterized PG14–18 conformance (`server_version_num` `140000`–`189999`), single-`Sync` pipeline atomic rollback, and parameterized sync/async active-query cancellation (`driver.cancel()` vs `asyncio.Task.cancel()`) in `test_postgres_native.py` with zero test-body `for` loops over cases or backends.

Out of scope: Logical replication containers (`ch28:t02`), `pgvector`/`PgBouncer` fixtures (`ch28:t03`), and mutating non-PostgreSQL `ENGINE_SPECS`.

## Bounded Read Manifest

- [src/py/tests/integration/conftest.py](../../../../../../../src/py/tests/integration/conftest.py) — `vtb_postgres_config`, `vtb_postgres_driver`, and `vtb_sqlspec_postgres_config` fixtures [^int-conftest].
- [src/py/tests/integration/db/test_driver_conformance.py](../../../../../../../src/py/tests/integration/db/test_driver_conformance.py) — `EngineConformanceSpec`, `_TYPES_DDL`, `ENGINE_SPECS`, `_require_native_module`, and `_resolve_live_config` [^conformance-mod].
- [src/py/vertebra/db/adapters/postgres/config.py](../../../../../../../src/py/vertebra/db/adapters/postgres/config.py) — `PostgreSQLConfig` `sslmode` and DSN options.
- [src/py/vertebra/db/adapters/postgres/driver.py](../../../../../../../src/py/vertebra/db/adapters/postgres/driver.py) — Canonical `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` exports.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Active-operation `driver.cancel()` vs `Task.cancel()` and physical session retirement contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/tests/integration/conftest.py`:
  - Helper `require_live_postgres_docker() -> None`: verifies `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` (failing loud under `VERTEBRA_CONFORMANCE_DOCKER == "1"` or `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS == "1"` if absent, skipping otherwise), then calls `pytest.skip("Set VERTEBRA_CONFORMANCE_DOCKER=1 to run live PostgreSQL Docker tests")` when `os.environ.get("VERTEBRA_CONFORMANCE_DOCKER") != "1"`.
  - `vtb_postgres_config(request: pytest.FixtureRequest) -> PostgreSQLConfig`: calls `require_live_postgres_docker()` before lazily resolving `request.getfixturevalue("postgres_service")` and builds `PostgreSQLConfig(..., sslmode="disable")`.
  - `@pytest.fixture(params=(14, 15, 16, 17, 18), scope="session") def vtb_postgres_versioned_config(request: pytest.FixtureRequest) -> Iterator[tuple[int, PostgreSQLConfig]]`: calls `require_live_postgres_docker()`, lazily resolves `request.getfixturevalue(f"postgres_{major}_service")`, yields `(major, PostgreSQLConfig(..., sslmode="disable"))`, and drops test-owned schemas in `finally`.
  - `pytest_runtest_logreport` / `pytest_sessionfinish` / `pytest_terminal_summary` hook: when `os.environ.get("VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS") == "1"`, records every selected PostgreSQL integration item (`"postgres"` in module/nodeid); if `executed == 0` or any item reports `skipped` or `wasxfail`, sets `session.exitstatus = pytest.ExitCode.TESTS_FAILED` and writes the offending nodeids to the terminal summary.
- `src/py/tests/integration/db/test_driver_conformance.py`:
  - Define `_POSTGRES_TYPES_DDL` using `BIGINT PRIMARY KEY`, `BIGINT`, `DOUBLE PRECISION`, `NUMERIC(18, 6)`, `BOOLEAN`, `TEXT`, `BYTEA`, `TIMESTAMPTZ`, and `JSONB`, and set `ENGINE_SPECS["postgres"]` (`id=="postgres"`) to `types_ddl=_POSTGRES_TYPES_DDL`, `supports_pipeline=True`, `pending_flow_id=None`.
  - Update `_resolve_live_config(spec, request)` for `"postgres"`: run `_require_native_module(spec)` first; if `VERTEBRA_CONFORMANCE_POSTGRES_DSN` is set, return `PostgreSQLConfig(dsn=dsn)` directly; else if `os.environ.get("VERTEBRA_CONFORMANCE_DOCKER") == "1"`, return `request.getfixturevalue("vtb_postgres_config")`; otherwise call `pytest.skip("Set VERTEBRA_CONFORMANCE_POSTGRES_DSN or VERTEBRA_CONFORMANCE_DOCKER=1")`.
- `src/py/tests/integration/db/test_postgres_native.py`:
  - Parameterize tests exclusively with `@pytest.mark.parametrize` and `vtb_postgres_versioned_config` (zero `for` loops over versions, cases, or sync/async backends inside test bodies) covering PG14–18 `SHOW server_version_num`, `_POSTGRES_TYPES_DDL` round-trips, SQLSTATE errors, `ArrowCursor`, pipeline rollback, and live sync/async `SELECT pg_sleep(30)` cancellation observed via `pg_stat_activity`.

## Edge Cases & Error Semantics

- Running `uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k postgres -q` with neither `VERTEBRA_CONFORMANCE_DOCKER` nor `VERTEBRA_CONFORMANCE_POSTGRES_DSN` set skips cleanly in milliseconds without invoking Docker.
- Running the same command with `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` (and `VERTEBRA_CONFORMANCE_DOCKER` unset) exits non-zero from the zero-skip enforcement hook, proving skips can never pass unnoticed in `make postgres-test` or live CI.
- Explicit `driver.cancel()` raises `QueryCancelledError` (`sqlstate == "57014"`); `asyncio.Task.cancel()` raises `asyncio.CancelledError`; both retire the cancelled backend PID (`pid_after != pid_before`), fence any ambient transaction until `rollback()`, and leave concurrent pool borrowers (`pid_b`) untouched.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/integration/db/test_driver_conformance.py`, `src/py/tests/integration/db/test_postgres_native.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Offline skip vs `REQUIRE_ZERO_SKIPS=1` enforcement | Run `-k postgres` offline without env vars, then with `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` alone | First skips cleanly (`0` Docker calls, exit `0`); second exits non-zero naming the skipped PostgreSQL nodeids. |
| Strict PG14–18 version matrix | Run conformance + `test_postgres_native.py` with `VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` | All `5` parameterized major versions (`14`..`18`) pass CRUD, `_POSTGRES_TYPES_DDL`, SQLSTATE errors, `ArrowCursor`, and `pipeline()` rollback with `0` skips/xfails. |
| Parameterized `driver.cancel()` (sync & async) | Start `SELECT pg_sleep(30)`, poll `pg_stat_activity` for active `pid_1`, call `driver.cancel()`, then query `pg_backend_pid()` | Raises `QueryCancelledError` (`sqlstate == "57014"`); next query runs on `pid_2 != pid_1`. |
| `asyncio.Task.cancel()` + sibling borrower + ambient tx fence | Task A runs `pg_sleep(30)` (`pid_a`), Task B borrows `pid_b`, cancel Task A; separately cancel inside `transaction()` | Task A raises `asyncio.CancelledError`, `pid_a` retires, Task B succeeds on `pid_b`, cancelled transaction rejects `SELECT 1` until `rollback()`. |

## Verification

- Offline safety check: `make dev && env -u VERTEBRA_CONFORMANCE_DOCKER -u VERTEBRA_CONFORMANCE_POSTGRES_DSN -u VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_native.py -k postgres -q`
- Live strict Green + lint gate: `make dev && VERTEBRA_CONFORMANCE_DOCKER=1 VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1 uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k postgres src/py/tests/integration/db/test_postgres_native.py -q -rs && uv run --no-sync ruff check src/py/tests/integration/conftest.py src/py/tests/integration/db/test_driver_conformance.py src/py/tests/integration/db/test_postgres_native.py`

## Acceptance Criteria

- [ ] `_resolve_live_config()` and `conftest.py` skip PostgreSQL Docker fixtures cleanly offline while `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS=1` fails loud on any skipped/xfailed PostgreSQL test, and PG14–18 pass `_POSTGRES_TYPES_DDL` + `supports_pipeline=True` conformance with `0` skips (R12, R25).
- [ ] Parameterized sync/async `driver.cancel()` (`QueryCancelledError` `57014`) and `asyncio.Task.cancel()` (`asyncio.CancelledError`) verify backend termination, socket retirement, failed-transaction fencing, and sibling-borrower immunity (R18, R25).

## Notes & Discoveries

- Keep `VERTEBRA_CONFORMANCE_POSTGRES_DSN` active as a direct DSN override alongside the `VERTEBRA_CONFORMANCE_DOCKER=1` container matrix in `_resolve_live_config()`.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_postgres_config` — Integration `pytest-databases` PostgreSQL fixtures and session zero-skip enforcement.
[^conformance-mod]: `src/py/tests/integration/db/test_driver_conformance.py::ENGINE_SPECS` — Cross-engine driver conformance specification table and `_resolve_live_config`.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Cancellation caller outcomes, retirement, and conformance contracts.
