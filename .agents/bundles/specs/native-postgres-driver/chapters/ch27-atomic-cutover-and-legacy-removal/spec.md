---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: "Chapter 27: Atomic Native Driver Cutover and Legacy Removal"
description: Activate the permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver classes across the postgres barrel, package exports, embeddings, SQLSpec, SQLAlchemy, and queue consumers, then delete unreachable legacy driver bodies.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch26-compiler-pipeline-arrow-and-event-hooks
replan_trigger: null
current_task: "native-postgres-driver:ch27:t01"
plan_revision: 6
plan_commit: bf53faf3
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-10T14:45:00Z"
generated:
  by: flow-revise
  at: "2026-11-10T14:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: bf53faf3
sources:
  - id: pg-driver-barrel
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Canonical PostgreSQL driver export module targeted for atomic cutover
  - id: pg-pkg-init
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: Lazy package export map for vertebra.db.adapters.postgres
  - id: pg-embeddings
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: AlloyDB SQL embedding pushdown and pgvector similarity search helper
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification publisher and streaming source rewritten in ch21:t03
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver
    title: SQLSpec PostgreSQL sync and async adapter parameter and tenant profiles
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: SQLAlchemy +vertebra and +vtb sync and async PostgreSQL dialects
  - id: pg-queues
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: Distributed PostgreSQL queue backend dependency guard
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Upstream flow baseline with zero PostgreSQL memory:// execution paths
tags:
  - postgres
  - cutover
  - sqlspec
  - sqlalchemy
  - driver
---

# Chapter 27: Atomic Native Driver Cutover and Legacy Removal

<!-- truth: start -->
- **Canonical Barrel & Package Cutover**: `src/py/vertebra/db/adapters/postgres/driver.py` re-exports permanent `SyncPostgreSQLDriver` (`._sync`) and `AsyncPostgreSQLDriver` (`._async`) together without runtime flags or `memory://` execution paths [^fail-loud-baseline], while `postgres/__init__.py` preserves `PoolerMode` (`config`), `PostgresNotifyConfig` (`notify`), `PG_UNCHANGED_TOAST`/`PgUnchangedToast`/`PostgresArrowResult`/`PostgresReplicationIdentity`/`PostgresReplicationRecord` (`types`), retained `core` helpers (`PGCOPY_BINARY_SIGNATURE`, `decode_pgcopy_rows`, `encode_pg_jsonb`, `encode_pgcopy_rows`, `strip_pg_jsonb_header`), `vertebra.db.arrow` re-exports (`ARROW_SHM_THRESHOLD_ROWS`, `ArrowCursor`, `read_arrow_shm`, `write_arrow_shm`), and CDC/checkpoint exports from `change_stream` (keeping `EventSessionRegistry` internal to `events.py`) [^pg-driver-barrel] [^pg-pkg-init].
- **Numeric Consumer Compilation & Public Event Verification**: `PostgresEmbeddingPushdown`, every SQLSpec `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` query/stack/Arrow/tenant seam (`_compile_postgres_numeric_transport`), and SQLAlchemy `+vtb` dialects (`default_paramstyle = paramstyle = "numeric_dollar"`) emit `$N` positional binds while `PostgresNotificationSource` is verified over canonical public drivers [^pg-embeddings] [^pg-notify] [^sqlspec-adapters] [^sa-dialects].
- **Clean Dead-Code & Queue Guard Cutover**: `_LegacyAsyncPostgreSQLDriver` (`t03`, keeping `import asyncio` for sync `_publish_notification`) and `_LegacySyncPostgreSQLDriver` + `import asyncio` (`t04`) are deleted cleanly alongside switching `PostgreSQLQueueBackend.__init__`'s `executor is None` dependency check from `psycopg` to `vertebra._postgres` (`vertebra-core[postgres]`) while keeping `PostgreSQLQueueBackend` strictly caller-`PostgreSQLQueueExecutor`-injected [^pg-queues].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapters 25 and 26 complete the permanent native `._sync.SyncPostgreSQLDriver` and `._async.AsyncPostgreSQLDriver` implementations, SQLSpec/SQLAlchemy numeric transport helpers, native pipeline/Arrow bindings, and `EventSessionRegistry` hooks while leaving the legacy `driver.py` classes selected on public paths so intermediate commits stay coherent [^pg-driver-barrel]. Legacy `AsyncPostgreSQLDriver.__init__` constructs legacy `SyncPostgreSQLDriver(executor=...)` and references static `strip_jsonb_header`/`encode_jsonb` helpers, so switching only one public class would break type-checking and construction. Meanwhile, `ch21:t03` already rewrote `src/py/vertebra/db/adapters/postgres/notify.py` around `PostgresNotificationDriver` (`queue_capacity: int = 1024, reconnect: bool = False`), leaving public driver import verification alongside SQLSpec, SQLAlchemy, embedding, and queue backend activation [^pg-notify] [^pg-queues].

This chapter executes the public cutover and cleanup across four bounded commits (`<= 3` production files per task):
1. Switch `driver.py`, `__init__.py` (preserving `PG_UNCHANGED_TOAST`, `PgUnchangedToast`, and exact `types`, `events`, and `change_stream` lazy targets with zero phantom Arrow codecs), and `embeddings.py` (`$1`/`$2` pushdown binds + `tools.docs` docstring scan) to select the permanent native pair, updating `test_postgres_driver.py`, `test_pg_dataplane_and_datafusion.py`, `test_google_embedders.py`, `test_native_db_drivers.py`, and staged native adapter tests (`ch27:t01`) [^pg-driver-barrel] [^pg-pkg-init] [^pg-embeddings].
2. Route every SQLSpec PostgreSQL execution, stack, Arrow, dispatch, and tenant-session entry point through `_compile_postgres_numeric_transport` in `extensions/sqlspec/adapters.py` and activate `default_paramstyle = paramstyle = "numeric_dollar"` on registered SQLAlchemy dialects in `extensions/sqlalchemy/dialects.py`, migrating extension, `test_enterprise_olap_drivers.py`, and `test_postgres_event_hooks.py` tests to canonical public imports (`ch27:t02`) [^pg-notify] [^sqlspec-adapters] [^sa-dialects].
3. Delete unreachable `_LegacyAsyncPostgreSQLDriver` from `driver.py` while retaining `import asyncio` until `t04` (`ch27:t03`).
4. Delete unreachable `_LegacySyncPostgreSQLDriver` and `import asyncio` from `driver.py`, and switch `PostgreSQLQueueBackend.__init__` in `src/py/vertebra/framework/queues/postgres.py` from `psycopg` to `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` (`ch27:t04`) [^pg-queues].

### 1.2 Non-Goals

- Live Docker PG14–18 conformance, `pgoutput` logical replication containers, `pgvector` 0.8.1 containers, and `PgBouncer` containers (owned by [ch28-live-conformance-and-service-acceptance](../ch28-live-conformance-and-service-acceptance/spec.md)).
- Rewriting `src/py/vertebra/db/adapters/postgres/notify.py` (completed in `ch21:t03`).
- Removing shared `PostgreSQLCore` CPU helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_rows`, `decode_pgcopy_rows`), driver static `parse_pgcopy_binary_rows`, `vertebra._vertebra` vector CPU views (`decode_pgvector_f32`), or standalone dialer helper shims under `src/py/vertebra/db/dialer/`.
- Removing `_in_memory_rows` or `vertebra_db::postgres::PostgresPool` execution fallbacks already eliminated by the upstream fail-loud baseline [^fail-loud-baseline].

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch26-compiler-pipeline-arrow-and-event-hooks](../ch26-compiler-pipeline-arrow-and-event-hooks/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md).

| Seam | Pre-Cutover State (`after ch26`) | Chapter 27 Target State |
| --- | --- | --- |
| `src/py/vertebra/db/adapters/postgres/driver.py` [^pg-driver-barrel] | Defines legacy `psycopg_pool` / `asyncpg` classes directly. | Pure public barrel importing `AsyncPostgreSQLDriver` from `._async` and `SyncPostgreSQLDriver` from `._sync` with zero legacy bodies after `t04`. |
| `src/py/vertebra/db/adapters/postgres/__init__.py` [^pg-pkg-init] | Lazily exports `driver`, `config`, `core`, `embeddings`, `notify`, `pipeline`, `types`, `events`, `change_stream`, and `vertebra.db.arrow` symbols. | Preserves `PoolerMode` (`config`), `PostgresNotifyConfig` (`notify`), retained `core` helpers, and `vertebra.db.arrow` re-exports while mapping `PG_UNCHANGED_TOAST`, `PgUnchangedToast`, `PostgresArrowResult`, `PostgresReplicationIdentity`, and `PostgresReplicationRecord` to `types`, protocols to `events` (omitting `EventSessionRegistry`), and CDC/checkpoint exports to `change_stream`. |
| `src/py/vertebra/db/adapters/postgres/embeddings.py` [^pg-embeddings] | Branches between `("$1", "$2")` and `("%s", "%s")` via `_uses_dollar_binds()`. | Emits `("$1", "$2")` unconditionally across both permanent native drivers, removes `_uses_dollar_binds()`, and passes `tools.docs` docstring scanning. |
| `src/py/vertebra/extensions/sqlspec/adapters.py` [^sqlspec-adapters] | `PostgreSQLSyncDriver._execution_parameter_style` uses `POSITIONAL_PYFORMAT`; tenant setter emits `%s`. | `execute`, `select`, `execute_many`, `select_to_arrow`, `execute_stack` (pipeline, sequential, `execute_many`, Arrow), `dispatch_execute`, `dispatch_execute_many`, and `_prepare_postgres_tenant_session` all route through `_compile_postgres_numeric_transport` (`$1..$N`). |
| `src/py/vertebra/extensions/sqlalchemy/dialects.py` [^sa-dialects] | Registered PostgreSQL dialects emit `%s` pyformat parameters on sync paths. | Both `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` set `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` with `numeric_dollar` DBAPI modules. |
| `src/py/vertebra/framework/queues/postgres.py` [^pg-queues] | `PostgreSQLQueueBackend.__init__` probes `ensure_module("psycopg", ...)` when `executor is None`. | Keeps `PostgreSQLQueueBackend` strictly caller-`PostgreSQLQueueExecutor`-injected (built-in native queue executor shipping belongs to `queues-tasks-and-channels`); when `executor is None`, probes `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` and raises `ImproperConfigurationError` instructing callers to pass `executor=` implementing `PostgreSQLQueueExecutor` (e.g., wrapping `AsyncPostgreSQLDriver` + `PostgresNotificationSource`). |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R24.1 (`ch27:t01`)**: Rename legacy classes in `driver.py` to `_LegacySyncPostgreSQLDriver` and `_LegacyAsyncPostgreSQLDriver` (updating `_LegacyAsyncPostgreSQLDriver.__init__` and `strip_jsonb_header`/`encode_jsonb` calls), re-export permanent `._sync.SyncPostgreSQLDriver` and `._async.AsyncPostgreSQLDriver`, preserve `PG_UNCHANGED_TOAST` and `PgUnchangedToast` alongside canonical `types`/`events`/`change_stream`/`pipeline` entries in `postgres/__init__.py`, switch `PostgresEmbeddingPushdown` to unconditional `$1`/`$2` binds passing `tools.docs.scan_public_docstrings`, and migrate `test_postgres_driver.py`, `test_pg_dataplane_and_datafusion.py` (deleting `_PsycopgConnectionPort` and `@requires_psycopg`), `test_google_embedders.py`, `test_native_db_drivers.py`, and staged native adapter suites to public imports.
- **R24.2 (`ch27:t02`)**: Route SQLSpec `execute`, `select`, `execute_many`, `select_to_arrow`, `execute_stack` (pipeline, sequential, `execute_many`, Arrow), `dispatch_execute`, `dispatch_execute_many`, and `_prepare_postgres_tenant_session` on `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` through `_compile_postgres_numeric_transport`, set both `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` (plus `numeric_dollar` DBAPI modules) on `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect`, and migrate `test_postgres_compiler_transport.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, `test_gcp_adapters_and_sqlalchemy.py`, `test_enterprise_olap_drivers.py` (`SELECT set_config($1, $2, $3)`), and `test_postgres_event_hooks.py` to canonical public APIs.
- **R24.3 (`ch27:t03`)**: Delete unreachable `_LegacyAsyncPostgreSQLDriver` and its exclusively owned imports (`anyio.CancelScope`, `inspect`, `AsyncIterator`, `Awaitable`, `AsyncDatabaseDriver`) from `driver.py` while keeping `import asyncio` for `_LegacySyncPostgreSQLDriver._publish_notification` until `t04`.
- **R24.4 (`ch27:t04`)**: Delete unreachable `_LegacySyncPostgreSQLDriver`, `import asyncio`, and all remaining legacy imports from `driver.py` (preserving `parse_pgcopy_binary_rows` on `_sync.SyncPostgreSQLDriver`), and update `PostgreSQLQueueBackend.__init__` (`src/py/vertebra/framework/queues/postgres.py` lines `147`–`159`) from `psycopg` to `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` with `src/py/tests/unit/framework/test_distributed_queues.py` coverage.

### 2.2 Public Contracts & Caller -> Seam -> Collaborator Map

```python
"""Synchronous and asynchronous native PostgreSQL drivers."""

from __future__ import annotations

from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver

__all__ = (
    "AsyncPostgreSQLDriver",
    "SyncPostgreSQLDriver",
)
```

- `DatabaseRegistry` / `PostgreSQLConfig` / direct callers -> `vertebra.db.adapters.postgres.{SyncPostgreSQLDriver, AsyncPostgreSQLDriver}` (`driver.py`) -> `._sync.SyncPostgreSQLDriver` / `._async.AsyncPostgreSQLDriver` -> `vertebra._postgres.{SyncPostgresPool, AsyncPostgresPool}`.
- `PostgresEmbeddingPushdown.{embed_documents, similarity_search}` -> `$1`, `$2` numeric SQL -> `SyncPostgreSQLDriver.select` / `AsyncPostgreSQLDriver.select` -> `VectorBatchView` / `VectorView`.
- `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` (`execute`, `select`, `execute_many`, `select_to_arrow`, `execute_stack`, `dispatch_execute*`, `_prepare_postgres_tenant_session`) -> `_compile_postgres_numeric_transport` (`ParameterStyle.NUMERIC`, `TypedParameter` unwrap, single-run `statement_transformers`) -> native drivers.
- `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` -> `default_paramstyle = paramstyle = "numeric_dollar"` + `SyncDBAPIModule`/`AsyncDBAPIModule(paramstyle="numeric_dollar")` -> native drivers.
- `PostgreSQLQueueBackend(executor=None)` -> `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` -> `ImproperConfigurationError("PostgreSQLQueueBackend requires a PostgreSQLQueueExecutor; pass executor= implementing PostgreSQLQueueExecutor (for example wrapping AsyncPostgreSQLDriver and PostgresNotificationSource)")`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| Intermediate state between `ch27:t01` and `ch27:t02` | `SyncPostgreSQLDriver._prepare_native_query` lowers `%s` pyformat binds from pre-`t02` SQLSpec/SQLAlchemy sync callers cleanly until `t02` switches consumers to direct `NUMERIC` / `numeric_dollar`. |
| Missing `vertebra._postgres` optional wheel at runtime (drivers & `PostgreSQLQueueBackend`) | `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` raises `MissingDependencyError` with `package == "vertebra-core[postgres]"` in `str(exc)`, never `asyncpg` or `psycopg`. |
| Caller `StatementConfig` with `?`/`:name`, `TypedParameter`, `sqlglot` AST transformer, and tenant `set_config` | Original `StatementConfig` and `SQLResult.statement` stay untouched; `_compile_postgres_numeric_transport` runs transformers once, unwraps `TypedParameter`, and emits `SELECT set_config($1, $2, $3)` for tenant sessions. |
| Offline `stmt.compile(dialect=SyncPostgreSQLDialect())` vs live `+vtb` execution | Setting both `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` ensures offline and connected compilation both emit `$1..$N`. |
| Legacy kwargs (`executor=...`) and `dsn="MEMORY://..."` | `executor=...` raises `TypeError`; `memory://` (any casing) raises `ImproperConfigurationError(match="memory://")` synchronously in `__init__` [^fail-loud-baseline]. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Cutover & legacy deletion sequencing | Activate barrel + helpers (`t01`), activate consumers (`t02`), then delete `_LegacyAsync*` (`t03`) and `_LegacySync*` + queue `psycopg` probe (`t04`) | Deleting 800+ legacy lines in the same commit as consumer/test migration — mixes behavioral cutover with dead-code removal and exceeds review budgets. |
| Runtime fallback policy | Hard cutover to `._sync` and `._async` with `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")` | Retaining a `psycopg`/`asyncpg` fallback when `_postgres` is uninstalled — violates R24 clean-break native ownership. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]` (`[0/33 files documented]` across the `33`-chapter / `118`-task flow synchronized in `ch30-user-and-workflow-docs`, `ch31-data-and-testing-okf-sync`, and `ch32-ffi-okf-and-review-routing-sync` ahead of `ch33-final-review-and-release-gate`; Google-style docstrings across `driver.py`, `__init__.py`, `embeddings.py`, `adapters.py`, `dialects.py`, and `queues/postgres.py` are verified via `tools.docs` within `ch27:t01`–`ch27:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R24 (Barrel, package exports, embeddings cutover) | [t01](tasks/t01-activate-driver-barrel-and-helpers.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/embeddings/test_google_embedders.py`, `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` |
| R24 (SQLSpec NUMERIC, SQLAlchemy numeric_dollar, tenant & event-hook imports) | [t02](tasks/t02-activate-sqlspec-sqlalchemy-and-notify.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_postgres_compiler_transport.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py` |
| R24 (Legacy async driver removal) | [t03](tasks/t03-remove-legacy-async-driver.md) | `characterization` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| R24 (Legacy sync driver removal & queue guard cutover) | [t04](tasks/t04-remove-legacy-sync-driver.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/framework/test_distributed_queues.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch27:t01 — Activate permanent native PostgreSQL driver barrel, package exports, and numeric embedding pushdown (tasks/t01-activate-driver-barrel-and-helpers.md)
- [ ] native-postgres-driver:ch27:t02 — Activate SQLSpec NUMERIC defaults, SQLAlchemy numeric_dollar dialects, and public event-hook imports (tasks/t02-activate-sqlspec-sqlalchemy-and-notify.md)
- [ ] native-postgres-driver:ch27:t03 — Remove unreachable _LegacyAsyncPostgreSQLDriver implementation from driver.py (tasks/t03-remove-legacy-async-driver.md)
- [ ] native-postgres-driver:ch27:t04 — Remove unreachable _LegacySyncPostgreSQLDriver and update PostgreSQLQueueBackend dependency guard (tasks/t04-remove-legacy-sync-driver.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres src/py/vertebra/framework/queues/postgres.py && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/framework/test_distributed_queues.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy src/py/vertebra/framework/queues/postgres.py && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions src/py/vertebra/framework/queues/postgres.py && uv lock --check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 6`, `plan_commit: bf53faf3`; `depends_on_chapters: [ch26-compiler-pipeline-arrow-and-event-hooks]`, `state: planned`, `planning_status: complete` (`33` chapters / `118` tasks in full flow, `ch01`–`ch19` / `71` tasks completed).
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Active Invariants**: Zero `psycopg`, `psycopg_pool`, `asyncpg`, `_executor`, or `_Legacy*` references survive in `src/py/vertebra/db/adapters/postgres/`, `src/py/vertebra/extensions/{sqlspec,sqlalchemy,alchemy}/`, and `src/py/vertebra/framework/queues/postgres.py` after `t04` (`src/py/vertebra/db/dialer/` helper shims remain separate); `core.py`, `parse_pgcopy_binary_rows`, and `vertebra._vertebra` CPU helpers remain intact.

[^pg-driver-barrel]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Canonical PostgreSQL driver export module targeted for atomic cutover.
[^pg-pkg-init]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package export map for `vertebra.db.adapters.postgres`.
[^pg-embeddings]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — AlloyDB SQL embedding pushdown and `pgvector` similarity search helper.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification publisher and streaming source rewritten in `ch21:t03`.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQL sync and async adapter parameter and tenant profiles.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — SQLAlchemy `+vertebra` and `+vtb` sync and async PostgreSQL dialects.
[^pg-queues]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — Distributed PostgreSQL queue backend dependency guard.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Upstream flow baseline with zero PostgreSQL `memory://` execution paths.
