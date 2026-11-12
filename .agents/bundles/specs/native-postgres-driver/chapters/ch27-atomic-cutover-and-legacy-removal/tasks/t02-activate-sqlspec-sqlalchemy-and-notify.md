---
type: Task
id: "native-postgres-driver:ch27:t02"
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: Activate SQLSpec NUMERIC routing and SQLAlchemy numeric_dollar dialect and DB-API paramstyle
description: Route every SQLSpec PostgreSQL execution, stack, Arrow, dispatch, and tenant-session method through _compile_postgres_numeric_transport, activate default_paramstyle and paramstyle numeric_dollar on SQLAlchemy +vtb dialects, and migrate extension, tenant, and event-hook tests to canonical public imports.
status: draft
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
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
  - src/py/tests/unit/db/test_postgres_event_hooks.py
estimated_loc: 235
plan_revision: 7
plan_commit: c8722c76
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
updated_at: "2026-11-12T01:30:00Z"
commit: null
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: pg-notify-mod
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification publisher and streaming source completed in ch21:t03
  - id: sqlspec-pg
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver
    title: SQLSpec PostgreSQL sync and async query, stack, Arrow, dispatch, and tenant methods
  - id: sqlalchemy-pg
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect
    title: SQLAlchemy +vertebra and +vtb PostgreSQL sync and async dialects
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen public contracts for consumer compilation and event publication
  - id: sa-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: DB-API module facades constructed with a per-dialect paramstyle argument
  - id: broker-flow
    resource: .agents/bundles/specs/broker-pool-sharing/spec.md
    title: Flow owning extraction of SyncDBAPI*/AsyncDBAPI* into vertebra.db.dbapi
tags:
  - postgres
  - cutover
  - sqlspec
  - sqlalchemy
  - notifications
---

# Task t02: Activate SQLSpec `NUMERIC` Routing and SQLAlchemy `numeric_dollar` Dialect and DB-API Paramstyle

## Objective & Scope Boundary

Complete the consumer-side native PostgreSQL cutover across `extensions/sqlspec/adapters.py` and `extensions/sqlalchemy/dialects.py` while migrating extension, tenant-isolation, and event-hook test suites to canonical public imports (`ch21:t03` already rewrote `src/py/vertebra/db/adapters/postgres/notify.py` over `PostgresNotificationDriver`) [^pg-notify-mod] [^sqlspec-pg] [^sqlalchemy-pg]:
1. Set `PostgreSQLSyncDriver._execution_parameter_style = ParameterStyle.NUMERIC` alongside `PostgreSQLAsyncDriver` and route every SQLSpec PostgreSQL execution path (`execute`, `select`, `execute_many`, `select_to_arrow`, `execute_stack` across pipeline, sequential, `execute_many`, and Arrow operations, `dispatch_execute`, `dispatch_execute_many`, and `_prepare_postgres_tenant_session`) through `_compile_postgres_numeric_transport` while preserving caller `StatementConfig` identity, `TypedParameter` unwrapping, single-execution `statement_transformers`, and `SQLResult.statement` provenance.
2. Set **both** `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` on registered `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` (`postgresql+vertebra`, `postgresql+vtb`, `postgresql+vertebra_async`, `postgresql+vtb_async`) and change their `dbapi` factories from `SyncDBAPIModule(paramstyle="pyformat")` / `AsyncDBAPIModule(paramstyle="pyformat")` to `paramstyle="numeric_dollar"` [^sa-dbapi]. This is the only DB-API change: module, connection, and cursor internals stay as they are, and extraction of `SyncDBAPI*` / `AsyncDBAPI*` into `vertebra.db.dbapi` plus `BrokerPool` belongs to `broker-pool-sharing` [^broker-flow]. If that extraction has landed when this task executes, the dialect factories construct the classes imported from `vertebra.db.dbapi` with the same argument and no re-export is added to `dialects.py`.
3. Switch `test_postgres_compiler_transport.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, `test_gcp_adapters_and_sqlalchemy.py`, `test_enterprise_olap_drivers.py` (`SELECT set_config($1, $2, $3)` over `postgres_ports.py`), and `test_postgres_event_hooks.py` from staged imports to canonical public driver/dialect imports [^pg-contracts].

Out of scope: Editing `src/py/vertebra/db/adapters/postgres/notify.py` (completed in `ch21:t03`), modifying MySQL, SQLite, Oracle, MSSQL, DuckDB, Redis, BigQuery, or Spanner profiles, or deleting `_Legacy*` classes (`ch27:t03`, `ch27:t04`).

## Bounded Read Manifest

- [src/py/vertebra/db/adapters/postgres/notify.py](../../../../../../../src/py/vertebra/db/adapters/postgres/notify.py) — Read-only verification of `PostgresNotificationSource(driver, config=None, *, change_feed=None, queue_capacity=1024, reconnect=False)` and `publish` contracts [^pg-notify-mod].
- [src/py/vertebra/extensions/sqlspec/adapters.py](../../../../../../../src/py/vertebra/extensions/sqlspec/adapters.py) — `PostgreSQLSyncDriver`, `PostgreSQLAsyncDriver`, `BaseSyncDriver`, `BaseAsyncDriver`, `_prepare_postgres_tenant_session`, and `_compile_postgres_numeric_transport` from `ch26:t01` [^sqlspec-pg].
- [src/py/vertebra/extensions/sqlalchemy/dialects.py](../../../../../../../src/py/vertebra/extensions/sqlalchemy/dialects.py) — `SyncPostgreSQLDialect`, `AsyncPostgreSQLDialect`, `SyncDBAPIModule`, `AsyncDBAPIModule`, and cursor execution [^sqlalchemy-pg].
- [src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py](../../../../../../../src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py) — Existing PostgreSQL SQLSpec and SQLAlchemy bind, stack, Arrow, and tenant regressions (plus PostgreSQL tenant tests in `src/py/tests/unit/db/test_enterprise_olap_drivers.py`).
- [src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py](../../../../../../../src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py) — Cloud SQL / AlloyDB + SQLAlchemy PostgreSQL integration unit tests.
- [.agents/bundles/specs/native-postgres-driver/contracts.md](../../../contracts.md) — Consumer compilation and `PostgresNotificationDriver` / `PostgresReplicationDriver` contracts [^pg-contracts].

## Symbol & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/adapters.py`:
  - `PostgreSQLSyncDriver._execution_parameter_style = ParameterStyle.NUMERIC` and `PostgreSQLAsyncDriver._execution_parameter_style = ParameterStyle.NUMERIC`.
  - Explicitly route `execute`, `select`, `execute_many`, `select_to_arrow`, `execute_stack` (pipeline fast path, sequential fallback, `execute_many` stack items, and Arrow stack items), `dispatch_execute`, `dispatch_execute_many`, and `_prepare_postgres_tenant_session` on both `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` through `_compile_postgres_numeric_transport` (`SELECT set_config($1, $2, $3)` for local/session tenant GUC setup) so explicit caller `StatementConfig` (`QMARK`, `NAMED_COLON`, `PYFORMAT`) preserves its unmutated identity, `TypedParameter` values unwrap cleanly, `sqlglot` AST `statement_transformers` run once, and `SQLResult.statement` / `StackResult` retain caller statement provenance while the wire driver always receives `$1..$N` SQL and ordered tuples.
- `src/py/vertebra/extensions/sqlalchemy/dialects.py`:
  - Set **both** `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` on `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect`, paired with `SyncDBAPIModule(paramstyle="numeric_dollar")` / `AsyncDBAPIModule(paramstyle="numeric_dollar")` in their `dbapi()` / `import_dbapi()` classmethods, removing any staging-only helper switches or legacy `hasattr(self._driver, "_sync")` checks while keeping MySQL (`pyformat`) and SQLite (`qmark` + `last_insert_rowid`) dialects untouched.
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` renames `test_postgresql_tenant_session_prepares_psycopg_binds` and `test_postgresql_tenant_session_prepares_asyncpg_binds` to `test_postgresql_tenant_session_prepares_native_binds` (parametrized over sync/async adapters) with native-port docstrings.
- `src/py/tests/unit/db/test_postgres_event_hooks.py` & `src/py/tests/unit/db/test_enterprise_olap_drivers.py`:
  - Import canonical `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver` from `vertebra.db.adapters.postgres` in `test_postgres_event_hooks.py` (verifying `PostgresNotificationSource`, `PostgresChangeSource`, and `EventSessionRegistry` teardown over public drivers), and update PostgreSQL tenant-session / RLS cases in `test_enterprise_olap_drivers.py` to assert `SELECT set_config($1, $2, $3)` and `$1` predicates over `postgres_ports.py`.

## Edge Cases & Error Semantics

- Non-idempotent `sqlglot` AST `statement_transformers` on `StatementConfig` run once across `execute`, `select`, `execute_many`, `select_to_arrow`, and every `execute_stack` mode without stripping transformers or re-applying `StatementFilter`s.
- JSON `?`, `?|`, `?&` operators and dollar-quoted `$tag$...$tag$` bodies pass through SQLSpec and SQLAlchemy offline + live `numeric_dollar` compilation untouched.
- `_prepare_postgres_tenant_session` and `apply_tenant_predicate` propagate `DatabaseError` from `SELECT set_config($1, $2, $3)` directly and emit `$1` / `$1::<cast>` predicates.
- Calling `await source.publish("events", "msg")` inside active `with sync_driver.transaction():` raises `InterfaceError` before worker offload, whereas `AsyncPostgreSQLDriver` joins its active `async with driver.transaction():`.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/py/tests/unit/extensions/test_postgres_compiler_transport.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| SQLSpec sync & async `execute`/`select`/`execute_many`/`select_to_arrow`/`execute_stack` | Run `?`, `:name`, `%s`, `TypedParameter`, `StatementFilter`, and stateful `sqlglot` AST transformer across all `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` methods (including pipeline + sequential + Arrow stack items) | Native port receives `$1..$N` SQL and unwrapped ordered tuples on every path; AST transformer count `== 1`; `SQLResult.statement is caller_stmt` with unmutated `StatementConfig`. |
| Tenant GUC & RLS predicate (`test_enterprise_olap_drivers.py`) | `_prepare_postgres_tenant_session` and `apply_tenant_predicate` on sync and async PostgreSQL adapters over `postgres_ports.py` | Executes `SELECT set_config($1, $2, $3)` with `(setting, tenant_id, is_local)` and emits `$1` / `$1::uuid` predicates, propagating setter `DatabaseError`. |
| DB-API module paramstyle | `SyncPostgreSQLDialect.import_dbapi().paramstyle`, `AsyncPostgreSQLDialect.import_dbapi().paramstyle`, `SyncMySQLDialect.import_dbapi().paramstyle` | `"numeric_dollar"`, `"numeric_dollar"`, `"format"` (other engines unchanged). |
| Registered SQLAlchemy offline + live `+vtb` & `+vtb_async` | `str(stmt.compile(dialect=SyncPostgreSQLDialect()))` plus live `select(...in_([10, 20]))`, `insert().returning()`, and `executemany` via `postgresql+vtb://` and `postgresql+vtb_async://` | Offline and live paths both emit `$1, $2` (`default_paramstyle == paramstyle == "numeric_dollar"`); MySQL (`%s`) and SQLite (`?`) remain unchanged. |
| Public-driver `PostgresNotificationSource` & `PostgresChangeSource` | `PostgresNotificationSource(driver, queue_capacity=512, reconnect=True)` + `PostgresChangeSource` on canonical `vertebra.db.adapters.postgres` drivers | Emits `SELECT pg_notify($1, $2)` (`timeout=10.0`), forwards `queue_capacity`/`reconnect`, rejects active sync transaction publish with `InterfaceError`, and closes cleanly via `await handle.aclose()` + `await client.close()`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/db/test_enterprise_olap_drivers.py -q`
- Final Green + lint + typecheck gate: `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_postgres_event_hooks.py -q && uv run --no-sync ruff check src/py/vertebra/extensions/sqlspec/adapters.py src/py/vertebra/extensions/sqlalchemy/dialects.py src/py/tests/unit/extensions src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_postgres_event_hooks.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions`

## Acceptance Criteria

- [ ] `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` route `execute`, `select`, `execute_many`, `select_to_arrow`, `execute_stack` (pipeline, sequential, `execute_many`, Arrow), `dispatch_execute*`, and `_prepare_postgres_tenant_session` through `_compile_postgres_numeric_transport` preserving `StatementConfig` identity, `TypedParameter` unwrapping, single-run `statement_transformers`, and `SQLResult.statement` provenance (R2, R24).
- [ ] Registered `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` set both `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` with `numeric_dollar` DBAPI modules and zero private staging switches remaining (R24).
- [ ] `test_postgres_compiler_transport.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, `test_gcp_adapters_and_sqlalchemy.py`, `test_enterprise_olap_drivers.py`, and `test_postgres_event_hooks.py` pass over canonical public APIs and `postgres_ports.py` (R21, R24).

## Notes & Discoveries

- Notification source naming: this worksheet executes before `queues-tasks-and-channels` t01 splits `NotificationSource` into `SyncNotificationSource` / `AsyncNotificationSource` and renames `PostgresNotificationSource` to `AsyncPostgresNotificationSource` alongside a new `SyncPostgresNotificationSource` (see [native-data-platform](../../../../native-data-platform/spec.md) Seam 2). It keeps the HEAD names, adds no new unprefixed notification protocol, class, or alias, and leaves the rename of every symbol, doc, and OKF reference it touches to that task.
- Keep MySQL (`POSITIONAL_PYFORMAT`), SQLite (`QMARK` / `qmark`), DuckDB, Oracle, MSSQL, BigQuery, and Spanner SQLSpec and SQLAlchemy dialect branches untouched when activating PostgreSQL `NUMERIC` / `numeric_dollar`.

[^pg-notify-mod]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification publisher and streaming source completed in `ch21:t03`.
[^sqlspec-pg]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQL sync and async query, stack, Arrow, dispatch, and tenant methods.
[^sqlalchemy-pg]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy `+vertebra` and `+vtb` PostgreSQL sync and async dialects.
[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen public contracts for consumer compilation and event publication.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — DB-API module facades constructed with a per-dialect `paramstyle` argument.
[^broker-flow]: `.agents/bundles/specs/broker-pool-sharing/spec.md` — Flow owning extraction of `SyncDBAPI*`/`AsyncDBAPI*` into `vertebra.db.dbapi`.
