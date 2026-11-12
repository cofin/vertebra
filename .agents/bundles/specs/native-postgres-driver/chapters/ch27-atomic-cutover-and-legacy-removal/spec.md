---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch27-atomic-cutover-and-legacy-removal
title: "Chapter 27: Atomic Native Driver Cutover and psycopg/asyncpg Removal"
description: Activate the permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver across the postgres barrel, package exports, embeddings, SQLSpec, and SQLAlchemy, replace the SQLite-backed asyncpg and psycopg test doubles, delete the legacy driver bodies and dialer psycopg/asyncpg helpers, switch the PostgreSQLQueueBackend missing-executor guard to vertebra-core[postgres], and remove the split-state caveats from the limitations entry.
status: draft
state: planned
planning_status: complete
depends_on_chapters:
  - ch26-compiler-pipeline-arrow-and-event-hooks
replan_trigger: null
current_task: "native-postgres-driver:ch27:t01"
plan_revision: 8
plan_commit: 5eb75b2a
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T04:16:55Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: pg-driver-barrel
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Public PostgreSQL driver module defining psycopg_pool and asyncio.to_thread classes on main
  - id: pg-pkg-init
    resource: src/py/vertebra/db/adapters/postgres/__init__.py
    title: Lazy package export map for vertebra.db.adapters.postgres
  - id: pg-embeddings
    resource: src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown
    title: AlloyDB SQL embedding pushdown with _uses_dollar_binds branching
  - id: pg-notify
    resource: src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource
    title: PostgreSQL notification publisher and streaming source rewritten in ch21:t03
  - id: sqlspec-adapters
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver
    title: SQLSpec PostgreSQL adapters (sync POSITIONAL_PYFORMAT, async NUMERIC)
  - id: sa-dialects
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect
    title: SQLAlchemy +vertebra/+vtb PostgreSQL dialects with pyformat paramstyle and DB-API module factories
  - id: sa-dbapi
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule
    title: DB-API module facades constructed with a per-dialect paramstyle argument
  - id: pg-queues
    resource: src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend
    title: Executor-injected PostgreSQL queue backend whose missing-executor guard probes psycopg
  - id: dialer-cloudsql
    resource: src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig
    title: Cloud SQL dialer carrying create_psycopg_connect_func and create_asyncpg_connect_func
  - id: dialer-alloydb
    resource: src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig
    title: AlloyDB dialer carrying create_psycopg_connect_func and create_asyncpg_connect_func
  - id: queues-flow
    resource: .agents/bundles/specs/queues-tasks-and-channels/spec.md
    title: Flow owning PostgresQueueStore, the settings-driven queue registry, and the litestar-queues backend
  - id: broker-flow
    resource: .agents/bundles/specs/broker-pool-sharing/spec.md
    title: Flow owning extraction of SyncDBAPI*/AsyncDBAPI* into vertebra.db.dbapi and BrokerPool
  - id: fail-loud-baseline
    resource: .agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md
    title: Upstream flow baseline with zero PostgreSQL memory:// execution paths
tags:
  - postgres
  - cutover
  - sqlspec
  - sqlalchemy
  - driver
  - queues
---

# Chapter 27: Atomic Native Driver Cutover and psycopg/asyncpg Removal

<!-- truth: start -->
- **Barrel & Package Cutover (`t01`)**: `driver.py` re-exports permanent `SyncPostgreSQLDriver` (`._sync`) and `AsyncPostgreSQLDriver` (`._async`) without runtime flags, `postgres/__init__.py` maps `pipeline`, `types`, `events`, and `change_stream` exports (keeping `EventSessionRegistry` internal), `PostgresEmbeddingPushdown` emits `$1`/`$2` unconditionally, and the SQLite-backed asyncpg pool double, psycopg pool mocks, and psycopg converter ports are replaced by native-shaped `postgres_ports.py` tests [^pg-driver-barrel] [^pg-pkg-init] [^pg-embeddings].
- **Consumer Compilation (`t02`)**: Every SQLSpec `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` path compiles through `_compile_postgres_numeric_transport` (`$1..$N`), and `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` set `default_paramstyle = paramstyle = "numeric_dollar"` and construct their DB-API module with `paramstyle="numeric_dollar"` [^sqlspec-adapters] [^sa-dialects] [^sa-dbapi].
- **Legacy & Third-Party Removal (`t03`, `t04`)**: `_LegacyAsyncPostgreSQLDriver`, `_LegacySyncPostgreSQLDriver`, and the dialer `create_psycopg_connect_func` / `create_asyncpg_connect_func` helpers are deleted; `PostgreSQLQueueBackend` stays caller-executor-injected and its missing-executor guard probes `vertebra._postgres` (`vertebra-core[postgres]`) instead of `psycopg`; `postgres-limitations.md` drops the split-state caveats [^dialer-cloudsql] [^dialer-alloydb] [^pg-queues].
- **Ownership Boundary**: The native `PostgresQueueStore` executor, settings-driven queue backend registry, and `litestar-queues` / Django task backends belong to `queues-tasks-and-channels`; extraction of `SyncDBAPI*` / `AsyncDBAPI*` into `vertebra.db.dbapi` and `BrokerPool` belong to `broker-pool-sharing` [^queues-flow] [^broker-flow].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

On `main` (`c8722c76`), `driver.py` defines `SyncPostgreSQLDriver` over `psycopg_pool.ConnectionPool` and `AsyncPostgreSQLDriver` as an `asyncio.to_thread` wrapper around it; `embeddings.py` branches between `$N` and `%s` binds via `_uses_dollar_binds()`; SQLSpec `PostgreSQLSyncDriver` executes with `ParameterStyle.POSITIONAL_PYFORMAT`; SQLAlchemy PostgreSQL dialects use `paramstyle = "pyformat"` and `SyncDBAPIModule(paramstyle="pyformat")` / `AsyncDBAPIModule(paramstyle="pyformat")`; `PostgreSQLQueueBackend.__init__` calls `ensure_module("psycopg", package="psycopg", install_package="psycopg[binary,pool]")` before raising for a missing `executor=`; both cloud dialers expose `create_psycopg_connect_func` and `create_asyncpg_connect_func`; and `test_postgres_driver.py` covers `AsyncPostgreSQLDriver` through `_SQLiteAsyncPGPool` [^pg-driver-barrel] [^pg-embeddings] [^sqlspec-adapters] [^sa-dialects] [^pg-queues] [^dialer-cloudsql] [^dialer-alloydb].

Chapters 25 and 26 build the permanent native `_sync.py` / `_async.py` drivers, the SQLSpec/SQLAlchemy numeric helpers, the driver wiring of `pipeline.py` and `arrow.py`, and `EventSessionRegistry` hooks without switching public paths. Legacy `AsyncPostgreSQLDriver.__init__` constructs the legacy sync driver, so switching one class alone breaks construction and typing. This chapter performs the public cutover and cleanup in four bounded commits:

1. Switch `driver.py`, `__init__.py`, and `embeddings.py` to the permanent pair and migrate driver, dataplane, embedding, registry, and staged adapter tests to public imports over `postgres_ports.py`, deleting the SQLite-backed asyncpg double and psycopg mocks (`t01`).
2. Route every SQLSpec PostgreSQL entry point through `_compile_postgres_numeric_transport` and activate `numeric_dollar` on the SQLAlchemy PostgreSQL dialects and their DB-API module construction (`t02`).
3. Delete `_LegacyAsyncPostgreSQLDriver` and the dialer `psycopg`/`asyncpg` connect helpers (`t03`).
4. Delete `_LegacySyncPostgreSQLDriver`, switch the queue guard to `vertebra._postgres`, and trim the split-state caveats from `postgres-limitations.md` (`t04`).

### 1.2 Ownership Reconciliation With Sibling Flows

| Concern | `ch27` rewires | Left to the owning flow |
| --- | --- | --- |
| `PostgreSQLQueueBackend` executor | `t04` replaces the `psycopg` probe with `ensure_module("vertebra._postgres", package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, rewrites the `Raises:` docstring and the `ImproperConfigurationError` message to name `PostgreSQLQueueExecutor`, and keeps `executor=` mandatory. The existing `PostgreSQLQueueExecutor` protocol and `$1..$N` queue SQL stay unchanged. | [`queues-tasks-and-channels`](../../../queues-tasks-and-channels/spec.md) builds `PostgresQueueStore` (the native executor), binds it through a settings-driven `QueueBackendSettings(kind="postgres")` registry, and adds the `litestar-queues` and Django task backends [^queues-flow]. |
| SQLAlchemy DB-API paramstyle | `t02` sets `default_paramstyle` and `paramstyle` to `"numeric_dollar"` on `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` and passes `paramstyle="numeric_dollar"` to the `SyncDBAPIModule` / `AsyncDBAPIModule` constructors in their `dbapi` / `import_dbapi` factories. DB-API module, connection, and cursor internals are not modified. | [`broker-pool-sharing`](../../../broker-pool-sharing/spec.md) extracts `SyncDBAPI*` / `AsyncDBAPI*` into `src/py/vertebra/db/dbapi/`, moves SQLAlchemy async adaptation to `extensions/sqlalchemy/async_adapter.py`, and adds `BrokerPool`; its extraction preserves the per-dialect `paramstyle` constructor argument. When `vertebra.db.dbapi` exists at execution time, `t02` imports the module classes from there instead of `dialects.py` [^broker-flow]. |

### 1.3 Non-Goals

- Live Docker PG14–18 conformance, logical replication, `pgvector`, and `PgBouncer` containers ([ch28](../ch28-live-conformance-and-service-acceptance/spec.md)).
- Rewriting `notify.py` (completed in `ch21:t03`) [^pg-notify].
- Removing `PostgreSQLCore` CPU helpers (`strip_jsonb_header`, `encode_jsonb`, `encode_pgcopy_rows`, `decode_pgcopy_rows`), driver static `parse_pgcopy_binary_rows`, `vertebra._vertebra` vector views, or the dialers' native ephemeral-certificate and metadata-exchange helpers.
- Adding `psycopg` or `asyncpg` anywhere except `[dependency-groups].postgres-bench` (`ch29:t02`).

### 1.4 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md); frozen contracts: [contracts.md](../../contracts.md); depends on [ch26-compiler-pipeline-arrow-and-event-hooks](../ch26-compiler-pipeline-arrow-and-event-hooks/spec.md). Skills: [python-framework](../../../../../skills/python-framework/SKILL.md) and [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md). Knowledge: [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md), [sqlspec-alchemy-and-dialects.md](../../../../knowledge/domains/extensions/sqlspec-alchemy-and-dialects.md), and [distributed-queues-and-email.md](../../../../knowledge/domains/framework/distributed-queues-and-email.md).

| Seam | State on `main` / after `ch26` | Target State |
| --- | --- | --- |
| `driver.py` [^pg-driver-barrel] | Legacy `psycopg_pool` / `asyncio.to_thread` classes. | Barrel importing `AsyncPostgreSQLDriver` from `._async` and `SyncPostgreSQLDriver` from `._sync`, no legacy bodies after `t04`. |
| `__init__.py` [^pg-pkg-init] | Exports `driver`, `config`, `core`, `embeddings`, `notify`, `types`, and `vertebra.db.arrow` symbols. | Adds `pipeline`, `events` protocols, and `change_stream` exports; keeps `EventSessionRegistry` internal. |
| `embeddings.py` [^pg-embeddings] | `_uses_dollar_binds()` chooses `$N` or `%s`. | `$1`/`$2` unconditionally; `_uses_dollar_binds()` removed. |
| `extensions/sqlspec/adapters.py` [^sqlspec-adapters] | Sync adapter `POSITIONAL_PYFORMAT`; tenant setter emits `%s`. | All PostgreSQL paths through `_compile_postgres_numeric_transport`; `SELECT set_config($1, $2, $3)`. |
| `extensions/sqlalchemy/dialects.py` [^sa-dialects] [^sa-dbapi] | `paramstyle = "pyformat"`, `SyncDBAPIModule(paramstyle="pyformat")`. | `default_paramstyle = paramstyle = "numeric_dollar"` and `paramstyle="numeric_dollar"` module construction. |
| `db/dialer/{cloudsql,alloydb}.py` [^dialer-cloudsql] [^dialer-alloydb] | `create_psycopg_connect_func`, `create_asyncpg_connect_func`. | Both methods deleted from both dialers. |
| `framework/queues/postgres.py` [^pg-queues] | `psycopg` probe when `executor is None`. | `vertebra._postgres` probe; executor still mandatory. |
| `test_postgres_driver.py` | `_SQLiteAsyncPGPool`, `MockConnectionPool`, `test_postgres_sync_and_async_guards_name_psycopg_pool`. | Native-shaped `postgres_ports.py` doubles with real lease and transaction accounting. |

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R24.1 (`ch27:t01`)**: Rename legacy classes to `_LegacySyncPostgreSQLDriver` / `_LegacyAsyncPostgreSQLDriver`, re-export the permanent pair, extend `__init__.py`, switch `PostgresEmbeddingPushdown` to `$1`/`$2`, and replace `test_asyncpg_pool_transaction_and_affected_rows`, `test_asyncpg_pool_rollback_and_cancel_release_connection`, `test_asyncpg_pool_failed_begin_releases_connection`, `test_sync_postgres_driver_pool_gucs_and_prepared_statements`, `test_postgres_sync_and_async_guards_name_psycopg_pool`, and their `_SQLiteAsyncPG*` / `Mock*` doubles with native-port equivalents; delete `_PsycopgConnectionPort` and `@requires_psycopg` from `test_pg_dataplane_and_datafusion.py`.
- **R24.2 (`ch27:t02`)**: SQLSpec numeric routing for `execute`, `select`, `execute_many`, `select_to_arrow`, `execute_stack`, `dispatch_execute`, `dispatch_execute_many`, and `_prepare_postgres_tenant_session`; SQLAlchemy dialect and DB-API module `numeric_dollar`; tenant tests renamed from psycopg/asyncpg wording to native wording.
- **R24.3 (`ch27:t03`)**: Delete `_LegacyAsyncPostgreSQLDriver` and its exclusively owned imports (`anyio.CancelScope`, `inspect`, `AsyncIterator`, `Awaitable`, `AsyncDatabaseDriver`) and delete `SyncCloudSQLDialer.create_psycopg_connect_func`, `AsyncCloudSQLDialer.create_asyncpg_connect_func` (`cloudsql.py`), `SyncAlloyDBDialer.create_psycopg_connect_func`, and `AsyncAlloyDBDialer.create_asyncpg_connect_func` (`alloydb.py`).
- **R24.4 & R27 (`ch27:t04`)**: Delete `_LegacySyncPostgreSQLDriver` and remaining legacy imports, switch the queue guard to `vertebra._postgres`, and remove the split-state bullets from `postgres-limitations.md`.

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

- `DatabaseRegistry` / direct callers -> `vertebra.db.adapters.postgres.{SyncPostgreSQLDriver, AsyncPostgreSQLDriver}` -> `._sync` / `._async` -> `vertebra._postgres.{SyncPostgresPool, AsyncPostgresPool}`.
- `PostgresEmbeddingPushdown.{embed_documents, similarity_search}` -> `$1`, `$2` SQL -> native driver `select`.
- SQLSpec `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` -> `_compile_postgres_numeric_transport` -> native drivers.
- `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` -> `SyncDBAPIModule(paramstyle="numeric_dollar")` / `AsyncDBAPIModule(paramstyle="numeric_dollar")` -> native drivers.
- `PostgreSQLQueueBackend(executor=None)` -> `ensure_module("vertebra._postgres", ...)` -> `ImproperConfigurationError("PostgreSQLQueueBackend requires a PostgreSQLQueueExecutor; pass executor= implementing PostgreSQLQueueExecutor (for example wrapping AsyncPostgreSQLDriver and PostgresNotificationSource)")`.

### 2.3 Edge-Case & Failure-Mode Matrix

| Boundary / Failure Mode | Deterministic Behavior |
| --- | --- |
| Between `t01` and `t02` | `SyncPostgreSQLDriver._prepare_native_query` lowers `%s` binds from pre-`t02` SQLSpec/SQLAlchemy callers until `t02` switches them to `$N`. |
| Missing `vertebra._postgres` (drivers and queue backend) | `MissingDependencyError` with `package == "vertebra-core[postgres]"` in `str(exc)`, never naming `psycopg` or `asyncpg`. |
| Caller `StatementConfig` with `?`/`:name`, `TypedParameter`, AST transformer, and tenant `set_config` | Caller config and `SQLResult.statement` untouched; transformers run once; `SELECT set_config($1, $2, $3)`. |
| Offline `stmt.compile(dialect=SyncPostgreSQLDialect())` vs live `+vtb` | Both emit `$1..$N`. |
| `broker-pool-sharing` extraction landed before `t02` | `t02` imports `SyncDBAPIModule` / `AsyncDBAPIModule` from `vertebra.db.dbapi` and applies the same `paramstyle` argument; no re-export shim is added in `dialects.py`. |
| Legacy kwargs (`executor=...`) and `dsn="MEMORY://..."` | `TypeError`; `ImproperConfigurationError(match="memory://")` [^fail-loud-baseline]. |
| Caller still uses `SyncCloudSQLDialer.create_psycopg_connect_func` after `t03` | `AttributeError`; docs (`ch30:t03`) describe native `PostgreSQLConfig` cloud options instead. |

### 2.4 Alternatives Comparison

| Decision | Chosen Seam | Rejected Alternative & Why |
| --- | --- | --- |
| Sequencing | Activate barrel/helpers (`t01`), consumers (`t02`), delete async legacy + dialer helpers (`t03`), delete sync legacy + queue probe + split-state caveats (`t04`) | One commit mixing behavioral cutover with 800+ deleted lines, exceeding review budgets. |
| Runtime fallback | Hard cutover with `ensure_module("vertebra._postgres", ...)` | Keeping `psycopg`/`asyncpg` when `_postgres` is missing, violating clean-break native ownership. |
| Dialer helpers | Delete `create_psycopg_connect_func` / `create_asyncpg_connect_func` | Keeping third-party connect factories that steer users to `psycopg`/`asyncpg` I/O while the native driver dials Cloud SQL and AlloyDB itself. |
| Queue executor | Guard swap only; native executor from `queues-tasks-and-channels` | Shipping an ad-hoc driver-backed executor here, duplicating `PostgresQueueStore`. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/1 files documented in this chapter]` — `ch27:t04` removes the split-state bullets from `.agents/bundles/knowledge/domains/data/postgres-limitations.md` (created in `ch22:t04`, completed in `ch31:t01`). Google-style docstrings across `driver.py`, `__init__.py`, `embeddings.py`, `adapters.py`, `dialects.py`, the dialers, and `queues/postgres.py` are verified by `tools.docs.scan_public_docstrings` within `t01`–`t04`; user docs follow in `ch30`.

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R24 (barrel, exports, embeddings, test doubles) | [t01](tasks/t01-activate-driver-barrel-and-helpers.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py`, `src/py/tests/unit/embeddings/test_google_embedders.py`, `src/py/tests/unit/db/test_native_db_drivers.py`, `src/py/tests/unit/db/test_postgres_native_adapters.py`, `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py`, `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` |
| R24 (SQLSpec NUMERIC, SQLAlchemy and DB-API numeric_dollar) | [t02](tasks/t02-activate-sqlspec-sqlalchemy-and-notify.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_postgres_compiler_transport.py`, `src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`, `src/py/tests/unit/db/test_enterprise_olap_drivers.py`, `src/py/tests/unit/db/test_postgres_event_hooks.py` |
| R24 (legacy async driver and dialer helper removal) | [t03](tasks/t03-remove-legacy-async-driver.md) | `characterization` | `src/py/tests/unit/db/test_postgres_driver.py`, `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py` |
| R24, R27 (legacy sync removal, queue guard, limitations trim) | [t04](tasks/t04-remove-legacy-sync-driver.md) | `behavior_tdd` | `src/py/tests/unit/framework/test_distributed_queues.py`, `src/py/tests/unit/db/test_postgres_driver.py`, `make review-check` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch27:t01 — Activate the permanent native driver barrel, package exports, and numeric embedding pushdown, and replace legacy test doubles (tasks/t01-activate-driver-barrel-and-helpers.md)
- [ ] native-postgres-driver:ch27:t02 — Activate SQLSpec NUMERIC routing and SQLAlchemy numeric_dollar dialect and DB-API paramstyle (tasks/t02-activate-sqlspec-sqlalchemy-and-notify.md)
- [ ] native-postgres-driver:ch27:t03 — Remove the legacy async driver and dialer psycopg/asyncpg connect helpers (tasks/t03-remove-legacy-async-driver.md)
- [ ] native-postgres-driver:ch27:t04 — Remove the legacy sync driver, switch the queue guard to vertebra-core[postgres], and trim split-state caveats (tasks/t04-remove-legacy-sync-driver.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync python -m mypy.stubtest vertebra._postgres && PYTHONPATH="src/py:." uv run --no-sync python -m tools.docs.scan_public_docstrings --check-leaks --check-overloads --check-typeddicts src/py/vertebra/db/adapters/postgres src/py/vertebra/db/dialer src/py/vertebra/framework/queues/postgres.py && uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py src/py/tests/unit/embeddings/test_google_embedders.py src/py/tests/unit/db/test_native_db_drivers.py src/py/tests/unit/db/test_enterprise_olap_drivers.py src/py/tests/unit/db/test_postgres_native_adapters.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/framework/test_distributed_queues.py -q && uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres src/py/vertebra/db/dialer src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy src/py/vertebra/framework/queues/postgres.py && uv run --no-sync mypy src/py/vertebra/db src/py/vertebra/extensions src/py/vertebra/framework/queues/postgres.py && test -z "$(git grep -n -E 'psycopg|asyncpg' -- src/py/vertebra)" && make review-check && uv lock --check` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 8`, `plan_commit: 5eb75b2a`; `state: planned`, `planning_status: complete` (`36` chapters / `129` tasks; `ch01`–`ch21` and `ch22:t01`–`t02` are on `main`).
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Active Invariants**: After `t04`, `git grep -n -E 'psycopg|asyncpg' -- src/py/vertebra` prints nothing; driver unit tests contain no SQLite-backed or psycopg/asyncpg doubles; `core.py`, `parse_pgcopy_binary_rows`, and `vertebra._vertebra` CPU helpers remain intact; the only remaining comparator references live under `[dependency-groups].postgres-bench` (`ch29:t02`) and the extras-literal fixture in `src/py/tests/unit/build/test_packaging_and_pgo.py`.

[^pg-driver-barrel]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Public PostgreSQL driver module defining `psycopg_pool` and `asyncio.to_thread` classes on `main`.
[^pg-pkg-init]: `src/py/vertebra/db/adapters/postgres/__init__.py` — Lazy package export map for `vertebra.db.adapters.postgres`.
[^pg-embeddings]: `src/py/vertebra/db/adapters/postgres/embeddings.py::PostgresEmbeddingPushdown` — AlloyDB SQL embedding pushdown with `_uses_dollar_binds` branching.
[^pg-notify]: `src/py/vertebra/db/adapters/postgres/notify.py::PostgresNotificationSource` — PostgreSQL notification publisher and streaming source rewritten in `ch21:t03`.
[^sqlspec-adapters]: `src/py/vertebra/extensions/sqlspec/adapters.py::PostgreSQLSyncDriver` — SQLSpec PostgreSQL adapters (sync `POSITIONAL_PYFORMAT`, async `NUMERIC`).
[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy `+vertebra`/`+vtb` PostgreSQL dialects with `pyformat` paramstyle and DB-API module factories.
[^sa-dbapi]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncDBAPIModule` — DB-API module facades constructed with a per-dialect `paramstyle` argument.
[^pg-queues]: `src/py/vertebra/framework/queues/postgres.py::PostgreSQLQueueBackend` — Executor-injected PostgreSQL queue backend whose missing-executor guard probes `psycopg`.
[^dialer-cloudsql]: `src/py/vertebra/db/dialer/cloudsql.py::CloudSQLConfig` — Cloud SQL dialer carrying `create_psycopg_connect_func` and `create_asyncpg_connect_func`.
[^dialer-alloydb]: `src/py/vertebra/db/dialer/alloydb.py::AlloyDBConfig` — AlloyDB dialer carrying `create_psycopg_connect_func` and `create_asyncpg_connect_func`.
[^queues-flow]: `.agents/bundles/specs/queues-tasks-and-channels/spec.md` — Flow owning `PostgresQueueStore`, the settings-driven queue registry, and the `litestar-queues` backend.
[^broker-flow]: `.agents/bundles/specs/broker-pool-sharing/spec.md` — Flow owning extraction of `SyncDBAPI*`/`AsyncDBAPI*` into `vertebra.db.dbapi` and `BrokerPool`.
[^fail-loud-baseline]: `.agents/bundles/knowledge/domains/data/database-drivers-and-dialers.md` — Upstream flow baseline with zero PostgreSQL `memory://` execution paths.
