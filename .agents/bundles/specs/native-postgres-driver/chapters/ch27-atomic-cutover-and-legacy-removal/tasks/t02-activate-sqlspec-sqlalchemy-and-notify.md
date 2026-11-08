---
type: Task
id: "native-postgres-driver:ch27:t02"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Activate SQLSpec NUMERIC defaults, SQLAlchemy numeric_dollar dialects, and public event-hook imports
description: Set SQLSpec PostgreSQLSyncDriver and PostgreSQLAsyncDriver to ParameterStyle.NUMERIC with single-pass transport compilation, activate numeric_dollar on registered SQLAlchemy PostgreSQL dialects, and migrate extension and event-hook tests to canonical public driver imports.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch27:t01"
read_set:
  - src/py/vertebra/db/adapters/postgres/notify.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  - .agents/bundles/specs/native-postgres-driver/contracts.md
files:
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/py/vertebra/extensions/sqlalchemy/dialects.py
tests:
  - src/py/tests/unit/extensions/test_postgres_compiler_transport.py
  - src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  - src/py/tests/unit/db/test_postgres_event_hooks.py
estimated_loc: 210
plan_revision: 4
plan_commit: dd432f18
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
updated_at: "2026-11-07T18:00:00Z"
commit: null
sources:
  - id: pg-notify-mod
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification publisher and streaming source completed in ch21:t03
  - id: sqlspec-pg
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver
    title: SQLSpec PostgreSQL sync and async adapter parameter profiles
  - id: sqlalchemy-pg
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: SQLAlchemy +vertebra and +vtb PostgreSQL sync and async dialects
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen public contracts for consumer compilation and event publication
tags:
  - postgres
  - cutover
  - sqlspec
  - sqlalchemy
  - notifications
---

# Task t02: Activate SQLSpec `NUMERIC` Defaults, SQLAlchemy `numeric_dollar` Dialects, and Public Event-Hook Imports

## Objective & Scope Boundary

Complete the consumer-side native PostgreSQL cutover across `extensions/sqlspec/adapters.py` and `extensions/sqlalchemy/dialects.py` while migrating extension and event-hook test suites to canonical public imports (`ch21:t03` already rewrote `src/py/vertebra/db/adapters/postgres/notify.py` over `PostgresNotificationDriver`) [^pg-notify-mod] [^sqlspec-pg] [^sqlalchemy-pg]:
1. Set `PostgreSQLSyncDriver._execution_parameter_style = ParameterStyle.NUMERIC` alongside `PostgreSQLAsyncDriver` and activate the `ch26:t01` single-pass numeric transport helper across `execute`, `select`, `execute_many`, `select_to_arrow`, and `execute_stack` while preserving caller `StatementConfig` identity and `?` provenance.
2. Activate `paramstyle = "numeric_dollar"` on registered `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` (`postgresql+vertebra`, `postgresql+vtb`, `postgresql+vertebra_async`, `postgresql+vtb_async`).
3. Switch `test_postgres_compiler_transport.py`, `test_postgres_event_hooks.py` (verifying `PostgresNotificationSource(driver, config, *, change_feed=None, queue_capacity=1024, reconnect=False)` and `PostgresChangeSource` over canonical public `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver`), `test_sqlalchemy_and_sqlspec_vtb.py`, and `test_gcp_adapters_and_sqlalchemy.py` from staged private imports to canonical public driver/dialect imports [^pg-contracts].

Out of scope: Editing `src/py/vertebra/db/adapters/postgres/notify.py` (already completed in `ch21:t03`), modifying MySQL, SQLite, Oracle, MSSQL, BigQuery, or Spanner SQLSpec/SQLAlchemy profiles, or deleting `_Legacy*` classes from `driver.py` (`ch27:t03`, `ch27:t04`).

## Bounded Read Manifest

- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — Read-only verification of `ch21:t03`'s `PostgresNotificationSource(driver, config=None, *, change_feed=None, queue_capacity=1024, reconnect=False)` and `publish` contracts [^pg-notify-mod].
- [src/py/vertebra/extensions/sqlspec/adapters.py](../../../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) — `PostgreSQLSyncDriver`, `PostgreSQLAsyncDriver`, `BaseSyncDriver`, `BaseAsyncDriver`, and the `ch26:t01` numeric transport compilation helper [^sqlspec-pg].
- [src/py/vertebra/extensions/sqlalchemy/dialects.py](../../../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) — `SyncPostgreSQLDialect`, `AsyncPostgreSQLDialect`, `SyncDBAPIModule`, `AsyncDBAPIModule`, and cursor execution [^sqlalchemy-pg].
- [src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py](../../../../../../../src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py) — Existing PostgreSQL SQLSpec and SQLAlchemy bind, stack, Arrow, and tenant regressions.
- [src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py](../../../../../../../src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py) — Cloud SQL / AlloyDB + SQLAlchemy PostgreSQL integration unit tests.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Consumer compilation and `PostgresNotificationDriver` / `PostgresReplicationDriver` contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/adapters.py`:
  - `PostgreSQLSyncDriver._execution_parameter_style = ParameterStyle.NUMERIC` and `PostgreSQLAsyncDriver._execution_parameter_style = ParameterStyle.NUMERIC`.
  - Invoke the `ch26:t01` native PostgreSQL transport copy helper once per statement at PostgreSQL `dispatch_execute`, `dispatch_execute_many`, `select_to_arrow`, and `execute_stack` boundaries so explicit caller `StatementConfig` (e.g., `ParameterStyle.QMARK`) retains its original identity, `SQLResult.statement` / `StackResult` provenance, and single-application statement transformers while the wire driver receives `$1..$N` SQL and ordered tuple values.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py`:
  - `SyncPostgreSQLDialect.paramstyle = "numeric_dollar"` and `AsyncPostgreSQLDialect.paramstyle = "numeric_dollar"`, paired with `SyncDBAPIModule(paramstyle="numeric_dollar")` / `AsyncDBAPIModule(paramstyle="numeric_dollar")` on PostgreSQL dialect `dbapi()` classmethods; remove any remaining `hasattr(self._driver, "_sync")` legacy inspection on PostgreSQL paths while keeping SQLite `last_insert_rowid` behavior intact.
- `src/py/tests/unit/db/test_postgres_event_hooks.py`:
  - Import canonical `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` from `vertebra.db.adapters.postgres` (not `._sync`/`._async`) and verify `PostgresNotificationSource(driver, config=None, *, change_feed=None, queue_capacity=1024, reconnect=False)` (`publish` and `stream`) plus `PostgresChangeSource` over public drivers.

## Edge Cases & Error Semantics

- Non-idempotent SQLSpec `StatementConfig` statement transformers run once semantically when compiling caller `?` SQL to `$1..$N` transport copies; mutating or re-running the caller's original `StatementConfig` is forbidden.
- JSON `?`, `?|`, and `?&` PostgreSQL operators and dollar-quoted `$tag$...$tag$` bodies pass through SQLSpec and SQLAlchemy compilation untouched without false parameter expansion.
- `PostgreSQLSyncDriver.apply_tenant_predicate` and `PostgreSQLAsyncDriver.apply_tenant_predicate` propagate setter `DatabaseError` failures directly and return `$1` / `$1::<cast>` predicates.
- Calling `await source.publish("events", "msg")` while canonical `SyncPostgreSQLDriver` has an active sync `with driver.transaction():` raises `InterfaceError` before worker offload, whereas `AsyncPostgreSQLDriver` joins its active `async with driver.transaction():`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_postgres_compiler_transport.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| SQLSpec sync & async default `NUMERIC` | `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` executing `SQL("SELECT * FROM items WHERE id = :id AND meta ? 'k'", id=42)` | Native port receives `"SELECT * FROM items WHERE id = $1 AND meta ? 'k'"` with `(42,)` on both sync and async drivers. |
| Caller `QMARK` `StatementConfig` + single-pass transformer | Explicit `StatementConfig(parameter_config=ParameterStyleConfig(default_parameter_style=ParameterStyle.QMARK), ...)` with counter-incrementing transformer | Transformer call count `== 1`, `result.statement` retains caller config identity, and native port receives `"$1"` transport SQL. |
| Registered SQLAlchemy `+vtb` & `+vtb_async` dialects | Compile and execute `select(items).where(items.c.id.in_([10, 20]))` and `insert(items).returning(items.c.id)` via `postgresql+vtb://` and `postgresql+vtb_async://` | Emits `$1, $2` (`numeric_dollar`) parameters and returns exact rows/counts; MySQL (`%s`) and SQLite (`?`) dialects remain unchanged. |
| Public-driver `PostgresNotificationSource` parity | `PostgresNotificationSource(driver, queue_capacity=512, reconnect=True)` + `await source.publish("orders", b"created")` on canonical `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` | Both invoke `SELECT pg_notify($1, $2)` with `("orders", "created")` and `timeout=10.0`, forward `queue_capacity`/`reconnect` to `_open_notification_listener`, and reject active sync transaction publish with `InterfaceError`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q`
- Final Green + lint + typecheck gate: `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/db/test_postgres_event_hooks.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/tests/unit/extensions src/py/tests/unit/db/test_postgres_event_hooks.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions`

## Acceptance Criteria

- [ ] `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` default to `ParameterStyle.NUMERIC` and compile transport copies once without mutating caller `StatementConfig` or double-running transformers (R2, R24).
- [ ] Registered `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` use `numeric_dollar` compilation with zero private staging entry points remaining (R24).
- [ ] `test_postgres_event_hooks.py` and extension suites import canonical public `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` and verify `PostgresNotificationSource` (`queue_capacity`, `reconnect`, `publish`) and `PostgresChangeSource` end-to-end (R21, R24).

## Notes & Discoveries

- [2026-11-07 16:45] Keep MySQL (`POSITIONAL_PYFORMAT`) and SQLite (`QMARK` / `qmark`) SQLSpec and SQLAlchemy dialect branches untouched when activating PostgreSQL `NUMERIC` / `numeric_dollar`.

[^pg-notify-mod]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification publisher and streaming source completed in `ch21:t03`.
[^sqlspec-pg]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQL sync and async adapter parameter profiles.
[^sqlalchemy-pg]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — SQLAlchemy `+vertebra` and `+vtb` PostgreSQL sync and async dialects.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen public contracts for consumer compilation and event publication.
