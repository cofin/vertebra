---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: "Chapter 26: SQLSpec/SQLAlchemy Compiler Boundaries, Driver Wiring of pipeline.py and arrow.py, and Event Hooks"
description: Complete SQLSpec/SQLAlchemy numeric parameter compilation helpers, transaction-bound pipeline factories, Arrow/COPY forwarding, and dedicated event session hooks on the permanent PostgreSQL drivers.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch25-dsn-bridge-lexer-and-permanent-drivers
replan_trigger: null
current_task: "native-postgres-driver:ch26:t01"
plan_revision: 8
plan_commit: d90d4625
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-12T04:25:15Z"
generated:
  by: flow-revise
  at: "2026-11-12T01:30:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-12T01:30:00Z"
    commit: c8722c76
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: SQLSpec compiler boundary, single-lifecycle native pipelines, ArrowCursor Liskov signatures, and EventSessionRegistry hooks
  - id: sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver
    title: SQLSpec BaseSyncDriver, BaseAsyncDriver, and PostgreSQLSyncDriver/PostgreSQLAsyncDriver adapters
  - id: sqla-py
    resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect
    title: SQLAlchemy 2.1 +vtb/+vertebra PostgreSQL sync and async dialects
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Shared ArrowCursor wrapper over VTBARROW ShmCapsule and inline IPC buffers
  - id: portal-py
    resource: src/py/vertebra/utils/portal.py::PortalProvider
    title: PortalProvider background event-loop bridge for synchronous driver cleanup
  - id: pg-pipeline-py
    resource: src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline
    title: Adapter pipeline wrappers with no production caller on main
  - id: pg-arrow-py
    resource: src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync
    title: Adapter Arrow COPY import helpers with no production caller on main
tags:
  - postgres
  - sqlspec
  - sqlalchemy
  - pipeline
  - arrow
  - events
---

# Chapter 26: SQLSpec/SQLAlchemy Compiler Boundaries, Pipeline Scopes, Arrow Forwarding, and Event Hooks

<!-- truth: start -->
- **Pure Consumer Compiler Boundaries**: SQLSpec copies unmutated `SQL` instances to `ParameterStyle.NUMERIC` (`$1..$N`) while preserving `base_config.statement_transformers` (compiled once, never re-applying `StatementFilter`s) and unwrapping `TypedParameter` across single and `is_many=True` binds; SQLAlchemy stages `default_paramstyle = "numeric_dollar"` + `paramstyle = "numeric_dollar"` alongside `SyncDBAPIModule`/`AsyncDBAPIModule(paramstyle="numeric_dollar")` without altering legacy defaults until `ch27:t02` [^npd-contracts] [^sqlspec-py] [^sqla-py].
- **Single-Lifecycle Lazy Pipelines**: `src/py/vertebra/db/adapters/postgres/pipeline.py` is imported only by `test_postgres_native_pipeline.py` on `main`; synchronous `driver.pipeline()` factories wire its `SyncPostgresPipeline` / `AsyncPostgresPipeline` into the permanent drivers over a lazy driver-owned protocol adapter that acquires transient leases inside `__enter__`/`__aenter__`, delegates `BEGIN`/`SAVEPOINT`/`COMMIT`/`ROLLBACK` to one native `pipeline_begin`/`pipeline_results`/`pipeline_finish` lifecycle (zero duplicate Python `BEGIN`), registers active-operation `cancel()` guards, and rejects overlapping scopes [^npd-contracts] [^sqlspec-py].
- **Liskov `VTBARROW` & `EventSessionRegistry`**: Driver Arrow methods match `SyncDatabaseDriver`/`AsyncDatabaseDriver` signatures (`threshold_rows`, `max_result_bytes`, `batch_size=65_536`, `columns`, `table_name`, `data`, `schema_name`) delegating `load_from_arrow` to `postgres/arrow.py::{load_arrow_sync, load_arrow_async}` (imported only by `test_postgres_native_data.py` on `main`) over `copy_from_arrow`/`write_arrow_ipc`/`finish`/`close`/`aclose` [^arrow-py] [^pg-arrow-py], and `EventSessionRegistry` closes handles via `await handle.aclose()` and clients via `await client.close()` [^npd-contracts] [^portal-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

On `main` (`c8722c76`), `pipeline.py` and `arrow.py` are complete adapter modules with no production caller: only `src/py/tests/unit/db/test_postgres_native_pipeline.py` imports `pipeline.py` and only `src/py/tests/unit/db/test_postgres_native_data.py` imports `arrow.py` [^pg-pipeline-py] [^pg-arrow-py]. Before `ch27:t01`–`t02` atomically switches `driver.py` and consumer adapters to the native engine, the permanent `_sync.py` and `_async.py` driver classes must support every higher-level capability consumed by SQLSpec, SQLAlchemy, analytical `VTBARROW` pipelines, `PostgresNotificationSource`, and `PostgresChangeSource` without breaking the still-active legacy `driver.py` pair [^npd-contracts]. Chapter 26 implements pure SQLSpec `ParameterStyle.NUMERIC` (`sqlspec==0.71.1`) and SQLAlchemy `numeric_dollar` compilation profiles (`ch26:t01`), binds synchronous `pipeline()` factories lazily to a single native `pipeline_begin`/`pipeline_results`/`pipeline_finish` lifecycle (`ch26:t02`), completes Liskov-exact Arrow selection, streaming, and `postgres/arrow.py` COPY forwarding (`ch26:t03`), and wires `EventSessionRegistry` notification and replication hooks across both permanent drivers (`ch26:t04`) [^sqlspec-py] [^sqla-py] [^arrow-py] [^portal-py].

### 1.2 Non-Goals

- Changing public `driver.py` exports, `PostgreSQLSyncDriver._execution_parameter_style`, active `SyncPostgreSQLDialect` / `AsyncPostgreSQLDialect` defaults, `embeddings.py` SQL markers, or barrel `PostgresNotificationSource` re-exports (`notify.py` itself was already migrated in `ch21:t03`; remaining cutover swaps land in `ch27:t01`–`t02`).
- Removing `_LegacySyncPostgreSQLDriver` or `_LegacyAsyncPostgreSQLDriver` (`ch27:t03`–`t04`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Applicable skills and knowledge chapters: [python-framework](../../../../../skills/python-framework/SKILL.md), [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R2.7 & R24.4 (`ch26:t01`)**: Add permanent SQLSpec `ParameterStyle.NUMERIC` transport copy helpers in `src/py/vertebra/extensions/sqlspec/adapters.py` preserving `base_config.statement_transformers` on unmutated `statement.copy(statement_config=...)` (compiling once without re-applying lowered `StatementFilter`s), unwrapping `TypedParameter` across single and `is_many=True` binds, and preserving `StatementConfig` identity and `SQLResult.statement` provenance; stage the PostgreSQL `numeric_dollar` SQLAlchemy profile setting both `default_paramstyle = "numeric_dollar"` and `paramstyle = "numeric_dollar"` plus `SyncDBAPIModule`/`AsyncDBAPIModule(paramstyle="numeric_dollar")` without altering legacy dialect defaults.
- **R13, R17, R24.5 (`ch26:t02`)**: Keep `SyncPostgreSQLDriver.pipeline() -> SyncPipeline` and `AsyncPostgreSQLDriver.pipeline() -> AsyncPipeline` synchronous factories wrapping `SyncPostgresPipeline` / `AsyncPostgresPipeline` over a lazy driver-owned `SyncPipelineClientProtocol` / `AsyncPipelineClientProtocol` adapter and `_prepare_native_query`, lazily acquiring any transient pool lease in `__enter__`/`__aenter__`, delegating `BEGIN`/`SAVEPOINT`/`COMMIT`/`ROLLBACK` to a single native `pipeline_begin`/`pipeline_results`/`pipeline_finish` lifecycle, enforcing `(thread_id, task_id)` `ContextVar` ownership and overlapping-pipeline `InterfaceError`, registering active-operation `cancel()` guards, and releasing transient leases after `pipeline_finish`.
- **R20, R24.6 (`ch26:t03`)**: Enforce exact `SyncDatabaseDriver`/`AsyncDatabaseDriver` Liskov signatures on `select_to_arrow(..., *, threshold_rows=ARROW_SHM_THRESHOLD_ROWS, max_result_bytes=268_435_456, **kwargs)`, `stream_arrow_batches(..., *, batch_size=65_536, columns=None, **kwargs)` wrapping `vertebra._vertebra.ArrowCursor` in `vertebra.db.arrow.ArrowCursor`, and `load_from_arrow(self, table_name, data, *, columns=None, schema_name=None, **kwargs)` delegating to `postgres/arrow.py::{load_arrow_sync, load_arrow_async}` over `copy_from_arrow`/`write_arrow_ipc`/`finish`/`close`/`aclose`.
- **R21, R22, R23, R24.7 (`ch26:t04`)**: Implement adapter-internal `EventSessionRegistry` in `events.py` (omitted from `postgres/__init__.py`) closing handles with `await handle.aclose()` and direct `AsyncPostgresClient` instances with `await client.close()`, and wire `_open_notification_listener`, `_replication_identity`, `_open_replication_stream`, and `_publish_notification` (`ensure_module(..., package="vertebra-core[postgres]", install_package="vertebra-core[postgres]", extra="postgres")`, `timeout=10.0`, sync ambient-scope `InterfaceError` preflight, `PortalProvider.default().call` sync shutdown) on both permanent drivers.

### 2.2 Concrete Type & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/adapters.py` & `sqlalchemy/dialects.py`:
  - `def _build_postgres_numeric_statement_config(base_config: StatementConfig | None = None) -> StatementConfig` (preserving `base_config.statement_transformers`)
  - `def _compile_postgres_numeric_transport(statement: SQL, driver_adapter: Any = None, *, is_many: bool = False) -> tuple[str, Any]` (compiling once and unwrapping `TypedParameter` values for single and `is_many=True` parameter sets)
  - Staged SQLAlchemy profile pairing `default_paramstyle = "numeric_dollar"`, `paramstyle = "numeric_dollar"`, and `SyncDBAPIModule(paramstyle="numeric_dollar")` / `AsyncDBAPIModule(paramstyle="numeric_dollar")`.
- `src/py/vertebra/db/adapters/postgres/_sync.py` & `_async.py`:
  - `SyncPostgreSQLDriver.pipeline(self) -> SyncPipeline` and `AsyncPostgreSQLDriver.pipeline(self) -> AsyncPipeline` (synchronous factories wrapping `SyncPostgresPipeline` / `AsyncPostgresPipeline`)
  - `select_to_arrow(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, max_result_bytes: int = 268_435_456, **kwargs: Any) -> ArrowCursor` (and `async def` counterpart)
  - `stream_arrow_batches(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, batch_size: int = 65_536, columns: Sequence[str] | None = None, **kwargs: Any)` (`Iterator[ArrowCursor]` / `AsyncIterator[ArrowCursor]`)
  - `load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None, **kwargs: Any) -> int` (and `async def` counterpart delegating to `load_arrow_sync` / `load_arrow_async`)
  - `async def _publish_notification(self, channel: str, payload: str) -> None`, `async def _open_notification_listener(...) -> AsyncPostgresListener`, `async def _replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity`, `async def _open_replication_stream(...) -> AsyncPostgresReplication`
- `src/py/vertebra/db/adapters/postgres/events.py`:
  - `class EventSessionRegistry:` (`open_and_register`, `prune_closed`, `async def aclose(self) -> None`; calls `await handle.aclose()` on listener/replication handles and `await client.close()` on `AsyncPostgresClient`, which exposes `close()` rather than `aclose()`, and stays adapter-internal out of `postgres/__init__.py`)

### 2.3 Caller -> Seam -> Collaborator Map

- `BaseSyncDriver.execute_stack` / `BaseAsyncDriver.execute_stack` -> `driver.pipeline()` -> `SyncPostgresPipeline` / `AsyncPostgresPipeline` -> driver protocol adapter -> single native `client.pipeline_begin` / `pipeline_results` / `pipeline_finish`.
- Driver `load_from_arrow(table_name, data, ...)` -> `postgres/arrow.py::{load_arrow_sync, load_arrow_async}` -> native `client.copy_from_arrow` + `write_arrow_ipc` + `finish` + `close`/`aclose`.
- `PostgresNotificationSource` / `PostgresChangeSource` -> driver event hooks -> `PostgreSQLConfig.derive_direct_config()` + `EventSessionRegistry` + dedicated `AsyncPostgresClient` (`await client.close()`).

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| `sqlspec==0.71.1` `SQL` with real `sqlglot` AST transformer + `StatementFilter` + `TypedParameter` (single & `is_many=True`) | `base_config.statement_transformers` stays attached to transport copy and runs exactly once; filters apply once; `TypedParameter` unwraps to `.value`; offline and live SQLAlchemy `numeric_dollar` both emit `$1..$N`. |
| Overlapping `driver.pipeline()` entry, wrong `(thread_id, task_id)` ambient transaction, caught batch failure, or `driver.cancel()` mid-pipeline | Overlapping or cross-owner entry raises `InterfaceError`; zero duplicate Python `BEGIN` issued before `pipeline_begin()`; caught error still triggers `pipeline_finish(scope_id, False)` rollback and lease release; `cancel()` targets the active native pipeline call. |
| `select_to_arrow(threshold_rows=..., max_result_bytes=...)`, early `stream_arrow_batches(..., batch_size=65_536, columns=...)` exit, or `load_from_arrow` source error | Both `threshold_rows` and `max_result_bytes` forward to native `select_to_arrow`; stream wraps `vertebra._vertebra.ArrowCursor` in `vertebra.db.arrow.ArrowCursor` and closes on `break`/`aclose()`/cancel; `load_from_arrow` closes writer via `close()`/`await aclose()` while preserving the producer error. |
| `SyncPostgreSQLDriver._publish_notification` in active sync transaction or `SyncPostgreSQLDriver.close()` inside running `asyncio` loop | Inside sync transaction raises `InterfaceError` before `asyncio.to_thread`; `close()` drains `EventSessionRegistry.aclose()` via `PortalProvider.default().call` (`await handle.aclose()` then `await client.close()`). |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| Pipeline transaction delegation | Lazy adapter delegating `BEGIN`/`SAVEPOINT`/`COMMIT`/`ROLLBACK` to one native `pipeline_begin`/`pipeline_results`/`pipeline_finish` lifecycle | Issuing a Python-side `client.begin()` ahead of `client.pipeline_begin()`, which produces nested/duplicate `BEGIN` control frames and desynchronizes savepoint tracking. |
| `sqlspec==0.71.1` transformer handling | Preserve `base_config.statement_transformers` on unmutated `statement.copy(...)` and compile once | Setting `statement_transformers=()`, which silently strips caller AST predicates (such as tenant guards) when `SQL` has not yet been compiled. |
| Sync driver event cleanup | `PortalProvider.default().call` over `EventSessionRegistry.aclose` (`handle.aclose()` + `client.close()`) | `asyncio.run(...)` inside `SyncPostgreSQLDriver.close()`, which raises `RuntimeError` inside an active event loop. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch26]` (`[0/33 files documented]` across the full `36`-chapter / `129`-task flow consolidated in `ch30-user-and-workflow-docs`, `ch31-data-and-testing-okf-sync`, and `ch32-ffi-okf-and-review-routing-sync`; public hook docstrings in `_sync.py`, `_async.py`, and `events.py` document the sync vs async `_publish_notification` transaction boundary).

## 4. Requirement-to-Task Traceability Matrix

| Requirement | Owning Task | Verification Strategy | Canonical Test Gate |
| --- | --- | --- | --- |
| R2, R24 (R2.7, R24.4) | [t01](tasks/t01-sqlspec-compiler-boundaries.md) | `behavior_tdd` | `src/py/tests/unit/extensions/test_postgres_compiler_transport.py` |
| R13, R17, R24 (R24.5) | [t02](tasks/t02-driver-pipeline-transaction-binding.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py` |
| R20, R24 (R24.6) | [t03](tasks/t03-driver-arrow-and-owned-binds.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` |
| R21, R22, R23, R24 (R24.7) | [t04](tasks/t04-dedicated-driver-event-hooks.md) | `behavior_tdd` | `src/py/tests/unit/db/test_postgres_event_hooks.py` |

## 5. Implementation Plan

- [ ] native-postgres-driver:ch26:t01 — Prepare native PostgreSQL compiler boundaries without changing public defaults (tasks/t01-sqlspec-compiler-boundaries.md)
- [ ] native-postgres-driver:ch26:t02 — Bind permanent native driver pipeline factories to transaction ownership (tasks/t02-driver-pipeline-transaction-binding.md)
- [ ] native-postgres-driver:ch26:t03 — Complete native Arrow forwarding before public PostgreSQL activation (tasks/t03-driver-arrow-and-owned-binds.md)
- [ ] native-postgres-driver:ch26:t04 — Open and close dedicated native PostgreSQL event sessions from drivers (tasks/t04-dedicated-driver-event-hooks.md)

**Chapter Recovery Checkpoint:** `make dev && uv run --no-sync pytest src/py/tests/unit/extensions/test_postgres_compiler_transport.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_event_hooks.py src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q && uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions/sqlspec src/py/vertebra/extensions/sqlalchemy` exits `0`.

## 6. Current State & Remaining Plan Snapshot

- **Verified Plan Baseline**: `plan_revision: 8`, `plan_commit: d90d4625`; chapter `state: planned`, `planning_status: complete` (`36` chapters / `129` tasks; `ch01`–`ch21` and `ch22:t01`–`t02` (`80` tasks) are on `main` in `c8722c76`).
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch27-atomic-cutover-and-legacy-removal` ahead of `ch30`–`ch32` documentation/OKF sync (`[0/33 files documented]`) and `ch33-final-review-and-release-gate`.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — SQLSpec compiler boundary, single-lifecycle native pipelines, ArrowCursor Liskov signatures, and EventSessionRegistry hooks.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec BaseSyncDriver, BaseAsyncDriver, and PostgreSQLSyncDriver/PostgreSQLAsyncDriver adapters.
[^sqla-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy 2.1 +vtb/+vertebra PostgreSQL sync and async dialects.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Shared ArrowCursor wrapper over VTBARROW ShmCapsule and inline IPC buffers.
[^portal-py]: `src/py/vertebra/utils/portal.py::PortalProvider` — PortalProvider background event-loop bridge for synchronous driver cleanup.
[^pg-pipeline-py]: `src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline` — Adapter pipeline wrappers with no production caller on `main`.
[^pg-arrow-py]: `src/py/vertebra/db/adapters/postgres/arrow.py::load_arrow_sync` — Adapter Arrow COPY import helpers with no production caller on `main`.
