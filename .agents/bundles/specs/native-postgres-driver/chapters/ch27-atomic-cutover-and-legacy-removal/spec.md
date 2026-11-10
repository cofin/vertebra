---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: "Chapter 27: Atomic Native Driver Cutover and Legacy Removal"
description: Activate the permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver classes across the postgres barrel, package exports, embeddings, SQLSpec, and SQLAlchemy consumers, then delete unreachable legacy driver bodies.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch26-compiler-pipeline-arrow-and-event-hooks
replan_trigger: null
current_task: "native-postgres-driver:ch27:t01"
plan_revision: 5
plan_commit: 73c8a6ff
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-08T02:05:22Z"
generated:
  by: flow-revise
  at: "2026-11-07T16:45:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T16:45:00Z"
    commit: 73c8a6ff
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
    title: SQLSpec PostgreSQL sync and async adapter parameter profiles
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py
    title: SQLAlchemy +vertebra and +vtb sync and async PostgreSQL dialects
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Upstream flow that deletes PostgreSQL memory:// paths and vertebra_db::postgres::PostgresPool
tags:
  - postgres
  - cutover
  - sqlspec
  - sqlalchemy
  - driver
---

# Chapter 27: Atomic Native Driver Cutover and Legacy Removal

<!-- truth: start -->
- **Canonical Barrel & Package Cutover**: `src/py/vertebra/db/adapters/postgres/driver.py` re-exports the permanent `SyncPostgreSQLDriver` (`._sync`) and `AsyncPostgreSQLDriver` (`._async`) classes together without runtime flags or `psycopg`/`asyncpg` imports (the `driver-fail-loud` baseline already carries no `memory://` execution paths [^fail-loud-baseline]), while `postgres/__init__.py` maps `PostgresArrowResult` and `PostgresReplicationIdentity` to `types` and CDC/checkpoint contracts to `change_stream` [^pg-driver-barrel] [^pg-pkg-init].
- **Numeric Consumer Compilation & Public Event Verification**: `PostgresEmbeddingPushdown`, `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` (`ParameterStyle.NUMERIC`), and `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` (`paramstyle = "numeric_dollar"`) emit `$N` positional binds while `PostgresNotificationSource` (rewritten in `ch21:t03` with `queue_capacity: int = 1024, reconnect: bool = False`) is verified over canonical public drivers [^pg-embeddings] [^pg-notify] [^sqlspec-adapters] [^sa-dialects].
- **Clean Dead-Code Deletion**: Unreachable `_LegacyAsyncPostgreSQLDriver` (`t03`) and `_LegacySyncPostgreSQLDriver` (`t04`) are deleted in dedicated characterization commits while preserving `core.py` and `vertebra._vertebra` CPU helpers.
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Chapters 25 and 26 complete the permanent native `._sync.SyncPostgreSQLDriver` and `._async.AsyncPostgreSQLDriver` implementations, SQLSpec/SQLAlchemy numeric transport helpers, native pipeline/Arrow bindings, and dedicated event hooks while leaving the legacy `driver.py` classes selected on public paths so intermediate commits stay coherent [^pg-driver-barrel]. Legacy `AsyncPostgreSQLDriver.__init__` constructs legacy `SyncPostgreSQLDriver(executor=...)` and references static `JSONB` helpers, so switching only one public class would break type-checking and construction. Meanwhile, `ch21:t03` already rewrote `src/py/vertebra/db/adapters/postgres/notify.py` around `PostgresNotificationDriver` (`queue_capacity: int = 1024, reconnect: bool = False`), leaving only public driver import verification for event hooks alongside SQLSpec and SQLAlchemy consumer activation [^pg-notify].

This chapter executes the public cutover and cleanup across four bounded commits (`<= 3` production files per task):
1. Switch `driver.py`, `__init__.py` (exact `types`, `events`, and `change_stream` lazy targets), and `embeddings.py` (`$1`/`$2` pushdown binds + `tools.docs` docstring verification) to select the permanent native pair (`ch27:t01`) [^pg-driver-barrel] [^pg-pkg-init] [^pg-embeddings].
2. Activate SQLSpec `ParameterStyle.NUMERIC` transport compilation in `extensions/sqlspec/adapters.py` and SQLAlchemy `numeric_dollar` dialect defaults in `extensions/sqlalchemy/dialects.py`, migrating extension and `test_postgres_event_hooks.py` tests to canonical public driver imports (`ch27:t02`) [^pg-notify] [^sqlspec-adapters] [^sa-dialects].
3. Delete unreachable `_LegacyAsyncPostgreSQLDriver` (`ch27:t03`) and `_LegacySyncPostgreSQLDriver` (`ch27:t04`) from `driver.py` under characterization gates.

### 1.2 Non-Goals

- Live Docker PG14–18 conformance, `pgoutput` logical replication containers, `pgvector` 0.8.1 containers, and `PgBouncer` containers (owned by [ch28-live-conformance-and-service-acceptance](../ch28-live-conformance-and-service-acceptance/spec.md)).
- Rewriting `src/py/vertebra/db/adapters/postgres/notify.py` (already completed in `ch21:t03` with `PostgresNotificationDriver`, `queue_capacity: int = 1024`, and `reconnect: bool = False`).
- Removing shared `PostgreSQLCore` CPU helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_rows`, `decode_pgcopy_rows`) or `vertebra._vertebra` vector CPU views (`decode_pgvector_f32`).
- Removing `_in_memory_rows`, PostgreSQL `memory://` branches, or `vertebra_db::postgres::PostgresPool`: `driver-fail-loud` deletes them, and this chapter starts from that baseline [^fail-loud-baseline].

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch26-compiler-pipeline-arrow-and-event-hooks](../ch26-compiler-pipeline-arrow-and-event-hooks/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md).

| Seam | Pre-Cutover State (`after ch26`) | Chapter 27 Target State |
| --- | --- | --- |
| `src/py/vertebra/db/adapters/postgres/driver.py` [^pg-driver-barrel] | Defines legacy `psycopg_pool` / `asyncpg` classes directly; `memory://` DSNs already raise `ImproperConfigurationError` from the `driver-fail-loud` baseline. | Pure public barrel importing `AsyncPostgreSQLDriver` from `._async` and `SyncPostgreSQLDriver` from `._sync` with Google-style docstrings and zero legacy bodies after `t04`. |
| `src/py/vertebra/db/adapters/postgres/__init__.py` [^pg-pkg-init] | Lazily exports `driver`, `config`, `core`, `embeddings`, `notify`, `arrow`, `types`, `events`, and `change_stream` symbols. | Preserves canonical lazy driver targets while mapping `PostgresArrowResult` and `PostgresReplicationIdentity` to `types` and `PostgresChange*` / `PostgresCheckpoint*` / `PostgresReplicationGapError` to `change_stream`. |
| `src/py/vertebra/db/adapters/postgres/embeddings.py` [^pg-embeddings] | Branches between `("$1", "$2")` and `("%s", "%s")` via `_uses_dollar_binds()`. | Emits `("$1", "$2")` unconditionally across both permanent native drivers, removes `_uses_dollar_binds()`, and passes `tools.docs` docstring scanning. |
| `src/py/vertebra/db/adapters/postgres/notify.py` [^pg-notify] | Rewritten in `ch21:t03` to accept `PostgresNotificationDriver` (`queue_capacity: int = 1024, reconnect: bool = False`) and await `_publish_notification`. | Untouched in `ch27` production files; `test_postgres_event_hooks.py` switches from private `_sync`/`_async` imports to canonical public drivers in `t02`. |
| `src/py/vertebra/extensions/sqlspec/adapters.py` [^sqlspec-adapters] | `PostgreSQLSyncDriver._execution_parameter_style` uses `POSITIONAL_PYFORMAT`. | Both `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` default to `ParameterStyle.NUMERIC` and invoke single-pass numeric transport compilation. |
| `src/py/vertebra/extensions/sqlalchemy/dialects.py` [^sa-dialects] | Registered PostgreSQL dialects emit `%s` pyformat parameters on sync paths. | Registered `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` wire the `numeric_dollar` DBAPI and compiler profile. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R24.1 (`ch27:t01`)**: Rename legacy class definitions in `driver.py` to private `_LegacySyncPostgreSQLDriver` and `_LegacyAsyncPostgreSQLDriver` (updating internal `_LegacyAsyncPostgreSQLDriver` references), import permanent `SyncPostgreSQLDriver` from `._sync` and `AsyncPostgreSQLDriver` from `._async` into `driver.py`, update `postgres/__init__.py` `_EXPORT_MAP` so `PostgresArrowResult` and `PostgresReplicationIdentity` resolve from `vertebra.db.adapters.postgres.types` and `PostgresChangeConfig`, `PostgresChangeSource`, `PostgresCheckpoint`, `PostgresCheckpointStore`, `MemoryPostgresCheckpointStore`, `PostgresCheckpointConflictError`, and `PostgresReplicationGapError` resolve from `vertebra.db.adapters.postgres.change_stream`, switch `PostgresEmbeddingPushdown` to unconditional `$1`/`$2` binds with Google-style docstrings (including PEP 257 field docstrings on `msgspec.Struct` attributes) verified via `tools.docs.scan_public_docstrings`, and migrate core unit tests to canonical public imports.
- **R24.2 (`ch27:t02`)**: Activate the `ch26:t01` single-pass numeric SQLSpec transport helper and `ParameterStyle.NUMERIC` default on `PostgreSQLSyncDriver` and `PostgreSQLAsyncDriver` in `extensions/sqlspec/adapters.py`, wire `numeric_dollar` on registered `SyncPostgreSQLDialect` and `AsyncPostgreSQLDialect` in `extensions/sqlalchemy/dialects.py`, and migrate `test_postgres_compiler_transport.py`, `test_sqlalchemy_and_sqlspec_vtb.py`, `test_gcp_adapters_and_sqlalchemy.py`, and `test_postgres_event_hooks.py` (verifying `ch21:t03`'s `PostgresNotificationSource` over public `SyncPostgreSQLDriver` and `AsyncPostgreSQLDriver`) from staged helper imports to canonical public APIs.
- **R24.3 (`ch27:t03`)**: Delete unreachable `_LegacyAsyncPostgreSQLDriver` and its newly unused imports (`anyio.CancelScope`, `inspect`, `Awaitable`, etc.) from `driver.py` with zero public class-identity or behavioral change.
- **R24.4 (`ch27:t04`)**: Delete unreachable `_LegacySyncPostgreSQLDriver` and all remaining legacy imports/state from `driver.py`, leaving only the module docstring, `from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver`, `from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver`, and `__all__ = ("AsyncPostgreSQLDriver", "SyncPostgreSQLDriver")`.

### 2.2 Public Contracts & Caller -> Seam -> Collaborator Map

```python
"""Synchronous and asynchronous native PostgreSQL drivers."""

from vertebra.db.adapters.postgres._async import AsyncPostgreSQLDriver
from vertebra.db.adapters.postgres._sync import SyncPostgreSQLDriver

__all__ = (
    "AsyncPostgreSQLDriver",
    "SyncPostgreSQLDriver",
)
```

- `DatabaseRegistry` / `PostgreSQLConfig` / direct callers -> `vertebra.db.adapters.postgres.{SyncPostgreSQLDriver, AsyncPostgreSQLDriver}` (`driver.py`) -> `._sync.SyncPostgreSQLDriver` / `._async.AsyncPostgreSQLDriver` -> `vertebra._postgres.{SyncPostgresPool, AsyncPostgresPool}`.
- `PostgresEmbeddingPushdown.{embed_documents, similarity_search}` -> `$1`, `$2` numeric SQL -> `SyncPostgreSQLDriver.select` (via `asyncio.to_thread`) or `AsyncPostgreSQLDriver.select` -> `VectorBatchView` / `VectorView`.
- `PostgresNotificationSource(driver, config, *, change_feed=None, queue_capacity=1024, reconnect=False)` (from `ch21:t03`) -> `await self._driver._publish_notification(channel, wire_payload)` (`timeout=10.0`) and `await self._driver._open_notification_listener(...)` on canonical public drivers.
- `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` (`extensions/sqlspec/adapters.py`) -> single-pass numeric transport copy (`ParameterStyle.NUMERIC`) preserving caller `StatementConfig` -> `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver`.
- `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` (`extensions/sqlalchemy/dialects.py`) -> `paramstyle = "numeric_dollar"` compiler & DBAPI cursor -> `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| Intermediate state between `ch27:t01` and `ch27:t02` | `SyncPostgreSQLDriver._prepare_native_query` lowers `%s` pyformat binds from pre-`t02` SQLSpec/SQLAlchemy sync callers cleanly until `t02` switches consumers to direct `NUMERIC` / `numeric_dollar`. |
| Missing `vertebra._postgres` optional wheel at runtime | `SyncPostgreSQLDriver` / `AsyncPostgreSQLDriver` pool initialization raises `MissingDependencyError` naming `vertebra-core[postgres]` (`extra="postgres"`), never `asyncpg` or `psycopg_pool`. |
| Explicit caller `StatementConfig` with `?` placeholders and non-idempotent statement transformer | Original `StatementConfig` and `?` SQL provenance remain untouched; the native transport copy compiles to `$1..$N` without invoking statement transformers a second time. |
| `PostgresNotificationSource.publish` inside active `SyncPostgreSQLDriver.transaction()` | `_publish_notification` raises `InterfaceError` before `asyncio.to_thread` or network I/O; inside `AsyncPostgreSQLDriver.transaction()`, `publish` joins the ambient transaction and suppresses delivery on rollback. |
| Legacy constructor kwargs (`executor=...`, `async_executor=...`) | Rejected with `TypeError`. |
| `PostgreSQLConfig(dsn="memory://")` on the permanent drivers | `ImproperConfigurationError`, preserving the `driver-fail-loud` guard across the cutover [^fail-loud-baseline]. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why Rejected |
| --- | --- | --- |
| Cutover & legacy deletion sequencing | Activate barrel + helpers (`t01`), activate consumers (`t02`), then delete `_LegacyAsync*` (`t03`) and `_LegacySync*` (`t04`) in small characterization commits | Deleting 800+ legacy lines in the same commit as consumer/test migration — exceeds bounded task review budgets and mixes behavioral cutover with dead-code removal. |
| Runtime fallback policy | Hard cutover to `._sync` and `._async` with `MissingDependencyError("vertebra._postgres", ...)` | Retaining a `psycopg`/`asyncpg` fallback when `_postgres` is uninstalled — violates R24 clean-break native ownership. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in this chapter]` (flow-wide user docs and OKF chapters `[0/11]` are synchronized in [ch30-user-and-domain-docs](../ch30-user-and-domain-docs/spec.md) tasks `ch30:t01`–`ch30:t04` once live conformance and benchmark gates complete; public Google-style docstrings and PEP 257 `msgspec.Struct` attribute docstrings in `driver.py`, `__init__.py`, `embeddings.py`, `adapters.py`, and `dialects.py` are verified via `tools.docs` within `ch27:t01`–`ch27:t04`).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R24 (Barrel, package exports, embeddings cutover) | [t01](tasks/t01-activate-driver-barrel-and-helpers.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` |
| R24 (SQLSpec NUMERIC, SQLAlchemy numeric_dollar, public event-hook imports) | [t02](tasks/t02-activate-sqlspec-sqlalchemy-and-notify.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_postgres_compiler_transport.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py` |
| R24 (Legacy async driver removal) | [t03](tasks/t03-remove-legacy-async-driver.md) | `characterization` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |
| R24 (Legacy sync driver removal) | [t04](tasks/t04-remove-legacy-sync-driver.md) | `characterization` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch27:t01 — Activate permanent native PostgreSQL driver barrel, package exports, and numeric embedding pushdown (tasks/t01-activate-driver-barrel-and-helpers.md)
- [ ] native-postgres-driver:ch27:t02 — Activate SQLSpec NUMERIC defaults, SQLAlchemy numeric_dollar dialects, and public event-hook imports (tasks/t02-activate-sqlspec-sqlalchemy-and-notify.md)
- [ ] native-postgres-driver:ch27:t03 — Remove unreachable _LegacyAsyncPostgreSQLDriver implementation from driver.py (tasks/t03-remove-legacy-async-driver.md)
- [ ] native-postgres-driver:ch27:t04 — Remove unreachable _LegacySyncPostgreSQLDriver implementation leaving clean public barrel (tasks/t04-remove-legacy-sync-driver.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions && uv lock --check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `73c8a6ff`; `depends_on_chapters: [ch26-compiler-pipeline-arrow-and-event-hooks]`, `state: planned`, `plan_revision: 4`; upstream `driver-fail-loud` removes PostgreSQL `memory://` paths before this chapter runs.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Active Invariants**: Zero `psycopg`, `psycopg_pool`, `asyncpg`, or `_executor` paths survive in `src/py/vertebra/db/adapters/postgres/driver.py` after `t04`; `core.py` and `vertebra._vertebra` CPU helpers remain intact; `_in_memory_rows` and `memory://` stay absent.

[^pg-driver-barrel]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Canonical PostgreSQL driver export module targeted for atomic cutover.
[^pg-pkg-init]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package export map for `vertebra.db.adapters.postgres`.
[^pg-embeddings]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — AlloyDB SQL embedding pushdown and `pgvector` similarity search helper.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification publisher and streaming source rewritten in `ch21:t03`.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQL sync and async adapter parameter profiles.
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py` — SQLAlchemy `+vertebra` and `+vtb` sync and async PostgreSQL dialects.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Completed upstream flow that deletes PostgreSQL `memory://` paths and `vertebra_db::postgres::PostgresPool`.
