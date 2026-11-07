---
type: Spec
parent_flow: native-postgres-driver
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: "Chapter 26: SQLSpec/SQLAlchemy Compiler Boundaries, Pipeline Scopes, Arrow Forwarding, and Event Hooks"
description: Complete SQLSpec/SQLAlchemy numeric parameter compilation helpers, transaction-bound pipeline factories, Arrow/COPY forwarding, and dedicated event session hooks on the permanent PostgreSQL drivers.
status: stable
state: planned
planning_status: complete
depends_on_chapters:
  - ch25-dsn-bridge-lexer-and-permanent-drivers
replan_trigger: null
current_task: "native-postgres-driver:ch26:t01"
plan_revision: 6
plan_commit: 8833b064
state_revision: 0
last_verified_checkpoint: null
last_reviewed_commit: null
created_at: "2026-11-05T00:00:00Z"
updated_at: "2026-11-07T05:00:00Z"
generated:
  by: flow-revise
  at: "2026-11-07T05:00:00Z"
verified:
  - by: agent:plan-generator
    at: "2026-11-07T05:00:00Z"
    commit: dd432f18
sources:
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: SQLSpec compiler boundary, delayed pipeline commit, ArrowCursor row_count, and dedicated event session hooks
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
- **Pure Consumer Compiler Boundaries**: SQLSpec copies prepared statements to `ParameterStyle.NUMERIC` (`$1..$N`) via `SQL.copy(...)` without mutating caller `StatementConfig` or double-running transformers, and SQLAlchemy stages a `numeric_dollar` DBAPI/compiler profile while preserving legacy `pyformat` defaults until `ch27:t02` [^npd-contracts] [^sqlspec-py] [^sqla-py].
- **Transaction-Bound Single-`Sync` Pipelines**: `SyncPostgreSQLDriver.pipeline()` and `AsyncPostgreSQLDriver.pipeline()` wrap batches in an owned `BEGIN` or ambient savepoint, keep the transaction open across caller `results()` conversion and `execute_stack` cardinality checks, and commit/release only on clean context exit [^npd-contracts] [^sqlspec-py].
- **Zero-Fallback `VTBARROW` & Dedicated Event Registry**: Driver Arrow methods preserve inline/SHM `ArrowCursor` ownership (`256 MiB` default cap, `0`-row schema validation, zero `from_rows`/`to_pylist` fallback) [^arrow-py], and `_EventSessionRegistry` pairs dedicated `AsyncPostgresListener` / `AsyncPostgresReplication` handles with direct `AsyncPostgresClient` sockets pruned on `.closed` and drained via `CancelScope(shield=True)` / `PortalProvider.default().call` [^npd-contracts] [^portal-py].
<!-- truth: end -->

## 1. Specification, Context & Provenance

### 1.1 Problem Statement & Objectives

Before `ch27:t01`–`t02` atomically switches `driver.py` and consumer adapters to the native engine, the permanent `_sync.py` and `_async.py` driver classes must support every higher-level capability consumed by SQLSpec, SQLAlchemy, analytical `VTBARROW` pipelines, `PostgresNotificationSource`, and `PostgresChangeSource` without breaking the still-active legacy `driver.py` pair [^npd-contracts]. Chapter 26 implements pure SQLSpec `ParameterStyle.NUMERIC` and SQLAlchemy `numeric_dollar` compilation profiles (`ch26:t01`), binds `pipeline()` contexts to transaction/savepoint ownership (`ch26:t02`), completes native Arrow selection/streaming/COPY edge cases (`ch26:t03`), and wires dedicated `_EventSessionRegistry` notification and replication hooks across both permanent drivers (`ch26:t04`) [^sqlspec-py] [^sqla-py] [^arrow-py] [^portal-py].

### 1.2 Non-Goals

- Changing public `driver.py` exports, `PostgreSQLSyncDriver._execution_parameter_style`, `SyncPostgreSQLDialect.paramstyle`, or `embeddings.py` / `notify.py` SQL markers (all switched atomically in `ch27:t01`–`t02`).
- Removing `_LegacySyncPostgreSQLDriver` or `_LegacyAsyncPostgreSQLDriver` (`ch27:t03`–`t04`).

### 1.3 Upstream Baseline & Code Analysis

Parent roadmap: [spec.md](../../spec.md) and [contracts.md](../../contracts.md). Applicable skills and knowledge chapters: [python-framework](../../../../../skills/python-framework/SKILL.md), [pyo3-bridge](../../../../../skills/pyo3-bridge/SKILL.md), [query-pipeline-cdc-and-tenancy.md](../../../../knowledge/domains/data/query-pipeline-cdc-and-tenancy.md), and [database-drivers-and-dialers.md](../../../../knowledge/domains/data/database-drivers-and-dialers.md).

## 2. Requirements, Concrete Contracts & Architecture

### 2.1 Chapter Requirements

- **R2.7 & R24.4 (`ch26:t01`)**: Add permanent SQLSpec `ParameterStyle.NUMERIC` statement transport copy helpers in `src/py/vertebra/extensions/sqlspec/adapters.py` (preserving caller `StatementConfig` identity, `SQLResult.statement` provenance, filters, single-execution statement transformers, quotes, dollar-quotes, `::` casts, and `?`/`?|`/`?&` JSONB operators) and stage the PostgreSQL `numeric_dollar` DBAPI/compiler profile in `src/py/vertebra/extensions/sqlalchemy/dialects.py` without altering active legacy defaults.
- **R13, R17, R24.5 (`ch26:t02`)**: Implement `pipeline()` on `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` satisfying `SyncPipelineCapable` / `AsyncPipelineCapable` over `SyncPostgresPipeline` / `AsyncPostgresPipeline`, delaying transaction commit / savepoint release until clean context exit after caller result conversion and `execute_stack` validation.
- **R20, R24.6 (`ch26:t03`)**: Complete `select_to_arrow` (`max_result_bytes = 268_435_456`, inline vs SHM `ArrowCursor` wrapping, explicit `row_count`, empty-result columns), `stream_arrow_batches` (pinned lease through exhaustion/early close), and `load_from_arrow` (single in-flight `copy_from_arrow` IPC stream, `0`-row schema check, preserved producer error on abort) on both permanent drivers with zero Python-row fallback.
- **R21, R22, R23, R24.7 (`ch26:t04`)**: Implement `_EventSessionRegistry` in `events.py` and `_open_notification_listener`, `_replication_identity`, `_open_replication_stream`, and `_publish_notification` (`timeout=10.0`, sync ambient-scope `InterfaceError` preflight, async ambient-transaction participation, `PortalProvider.default().call` sync shutdown) on both permanent drivers.

### 2.2 Concrete Type & Signature Contracts

- `src/py/vertebra/extensions/sqlspec/adapters.py` & `sqlalchemy/dialects.py`:
  - `def _compile_postgres_numeric_transport(statement: SQL, parameters: Any = None) -> tuple[str, tuple[Any, ...]]` (using `SQL.copy(statement_config=...)` + `prepare_driver_parameters` with zero transformer re-execution)
  - Explicit `paramstyle="numeric_dollar"` PostgreSQL profile in `SyncDBAPIModule` / `AsyncDBAPIModule` / dialect helper.
- `src/py/vertebra/db/adapters/postgres/_sync.py` & `_async.py`:
  - `SyncPostgreSQLDriver.pipeline(self) -> SyncPipeline` and `AsyncPostgreSQLDriver.pipeline(self) -> AsyncPipeline`
  - `SyncPostgreSQLDriver.select_to_arrow(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, max_result_bytes: int = 268_435_456, **kwargs: Any) -> ArrowCursor` (and `async def` counterpart)
  - `async def _publish_notification(self, channel: str, payload: str) -> None`
  - `async def _open_notification_listener(self, channels: Sequence[str], *, queue_capacity: int = 1024, reconnect: bool = False) -> AsyncPostgresListener`
  - `async def _replication_identity(self, slot: str, publication: str) -> PostgresReplicationIdentity`
  - `async def _open_replication_stream(self, config: PostgresChangeConfig, *, start_lsn: int | None = None, reconnect: bool = False) -> AsyncPostgresReplication`
- `src/py/vertebra/db/adapters/postgres/events.py`:
  - `class _EventSessionRegistry:` (`register`, `prune_closed`, `async def aclose(self) -> None`)

### 2.3 Caller -> Seam -> Collaborator Map

- `BaseSyncDriver.execute_stack` / `BaseAsyncDriver.execute_stack` -> `driver.pipeline()` -> `SyncPostgresPipeline` / `AsyncPostgresPipeline` (pinned transaction/savepoint lease + single-`Sync` batch).
- `PostgresNotificationSource` / `PostgresChangeSource` -> `PostgresNotificationDriver` / `PostgresReplicationDriver` hooks on `_sync.py` & `_async.py` -> `PostgreSQLConfig.derive_direct_config()` + `_EventSessionRegistry` + dedicated `AsyncPostgresClient`.

### 2.4 Edge-Case & Failure-Mode Matrix

| Condition | Required Behavior |
| --- | --- |
| SQLSpec `SQL` with non-idempotent statement transformer compiled for numeric transport | Transformer executes once total; caller's original `SQL.statement_config` and `SQLResult.statement` remain unmutated. |
| Pipeline server error caught inside `with driver.pipeline():` or `execute_stack` cardinality/hydration error after `results()` | Scope remains marked failed; `__exit__` / `__aexit__` rolls back transaction or savepoint and releases lease cleanly. |
| `load_from_arrow` source generator raises mid-COPY while `abort()` also errors | Preserves and raises original source exception (chaining cleanup failure), aborts COPY, and balances pool leases. |
| `SyncPostgreSQLDriver._publish_notification` called inside active `with sync_driver.transaction():` | Raises `InterfaceError` immediately BEFORE `asyncio.to_thread` or network I/O; direct `sync_driver.execute("SELECT pg_notify($1, $2)", ...)` remains valid inside that transaction. |
| `SyncPostgreSQLDriver.close()` called from a thread with a running `asyncio` loop while dedicated event handles are open | Bridges `_event_registry.aclose()` via `PortalProvider.default().call` with GIL released (never calling `asyncio.run` on the active loop) and closes all handles and `AsyncPostgresClient`s. |

### 2.5 Mandatory Alternatives Comparison

| Decision | Chosen Design | Rejected Alternative & Why |
| --- | --- | --- |
| Pipeline commit timing | Commit/release savepoint in `__exit__` / `__aexit__` after caller conversion | Auto-committing inside `results()`, which commits partial writes before `execute_stack` validates row cardinality or hydrates `msgspec` schemas. |
| Sync driver event cleanup | `PortalProvider.default().call` over `_EventSessionRegistry.aclose` | `asyncio.run(...)` inside `SyncPostgreSQLDriver.close()`, which fails with `RuntimeError` whenever called inside an active event loop. |

## 3. Documentation & OKF Knowledge Plan

Progress: `[0/0 files documented in ch26]` (consolidated in `ch30:t01`–`ch30:t04`; public hook docstrings in `_sync.py`, `_async.py`, and `events.py` document the sync vs async `_publish_notification` transaction boundary).

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

- **Verified Baseline**: `HEAD @ dd432f18`; chapter `state: planned`, `planning_status: complete`.
- **Remaining Tasks in Chapter**: `t01`, `t02`, `t03`, `t04` (`4/4` pending).
- **Downstream Dependents**: Unblocks `ch27-atomic-cutover-and-legacy-removal` for atomic public barrel and extension cutover.

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — SQLSpec compiler boundary, delayed pipeline commit, ArrowCursor row_count, and dedicated event session hooks.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec BaseSyncDriver, BaseAsyncDriver, and PostgreSQLSyncDriver/PostgreSQLAsyncDriver adapters.
[^sqla-py]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncPostgreSQLDialect` — SQLAlchemy 2.1 +vtb/+vertebra PostgreSQL sync and async dialects.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Shared ArrowCursor wrapper over VTBARROW ShmCapsule and inline IPC buffers.
[^portal-py]: `src/py/vertebra/utils/portal.py::PortalProvider` — PortalProvider background event-loop bridge for synchronous driver cleanup.
