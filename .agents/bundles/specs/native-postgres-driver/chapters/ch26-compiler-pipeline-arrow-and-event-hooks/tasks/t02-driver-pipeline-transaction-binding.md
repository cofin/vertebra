---
type: Task
id: "native-postgres-driver:ch26:t02"
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: Bind permanent native driver pipeline factories to transaction ownership
description: Implement pipeline() factories on permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver binding SyncPostgresPipeline and AsyncPostgresPipeline to owned transaction or savepoint scopes.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch26:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/pipeline.py
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
files:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
tests:
  - src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
estimated_loc: 215
plan_revision: 6
plan_commit: 8833b064
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
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Single-Sync pipeline outcomes, delayed context commit, and caught-error rollback invariants
  - id: db-caps
    resource: src/py/vertebra/db/driver/capabilities.py::SyncPipelineCapable
    title: SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols
  - id: sqlspec-py
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver
    title: BaseSyncDriver.execute_stack and BaseAsyncDriver.execute_stack caller conversion inside pipeline context
tags:
  - postgres
  - pipeline
  - transactions
  - sqlspec
---

# Task t02: Bind Permanent Native Driver Pipeline Factories to Transaction Ownership

## Objective & Scope Boundary

Add `pipeline(self) -> SyncPipeline` to `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver` and `pipeline(self) -> AsyncPipeline` to `src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver`, binding `SyncPostgresPipeline` and `AsyncPostgresPipeline` to an owned `BEGIN` transaction or ambient savepoint so `results()` flushes a single-`Sync` batch without early commit and `__exit__` / `__aexit__` commits or rolls back after caller conversion and SQLSpec `execute_stack` validation [^npd-contracts] [^db-caps] [^sqlspec-py]. Public `driver.py` remains untouched until `ch27:t01`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Pipeline transaction/savepoint scope, `_project_public_row` select projection, `.outcomes` / `.failing_index`, and caught-error rollback rules [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/_sync.py` — Permanent `SyncPostgreSQLDriver` and `ContextVar` scope management from `ch25:t03`.
- `src/py/vertebra/db/adapters/postgres/_async.py` — Permanent `AsyncPostgreSQLDriver` and `ContextVar` scope management from `ch25:t04`.
- `src/py/vertebra/db/adapters/postgres/pipeline.py` — `SyncPostgresPipeline` and `AsyncPostgresPipeline` from `ch16:t02`.
- `src/py/vertebra/db/driver/capabilities.py` — `SyncPipeline`, `AsyncPipeline`, `SyncPipelineCapable`, and `AsyncPipelineCapable` signatures [^db-caps].
- `src/py/vertebra/extensions/sqlspec/adapters.py` — `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack` [^sqlspec-py].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/_sync.py`:
  - `def pipeline(self) -> SyncPipeline` on `SyncPostgreSQLDriver` (structurally satisfying `SyncPipelineCapable`):
    - Reuses `self._prepare_native_query(sql, parameters)` when `pipeline.execute(...)` / `pipeline.select(...)` queues operations (`None`-returning queue calls).
    - On `__enter__`: if no ambient `ContextVar` transaction exists, acquires one `SyncPostgresClient` lease and executes `BEGIN`; if an ambient transaction exists on the current `(thread_id, task_id)`, borrows its pinned lease and opens a native savepoint (rejecting wrong-thread/task inherited scopes or overlapping re-entrant pipeline entry while another pipeline is unflushed with `InterfaceError`).
    - On `results() -> list[Any]`: emits the queued batch as a single-`Sync` / single-logical-flush native pipeline call, projects `execute` entries as `int` (`affected_rows`) and `select` entries as `list[dict[str, Any]]` via `types._project_public_row` (mapping top-level `PgJson(None)` -> `None` and preserving rightmost duplicate column labels per Python dict construction), and leaves the transaction/savepoint OPEN so caller hydration and cardinality checks execute inside the atomic scope.
    - On `__exit__`: commits or releases the savepoint ONLY when `exc_type is None` AND the batch had zero server/validation errors; otherwise rolls back (preserving original exception) and releases any transient lease.
- `src/py/vertebra/db/adapters/postgres/_async.py`:
  - `def pipeline(self) -> AsyncPipeline` on `AsyncPostgreSQLDriver` (structurally satisfying `AsyncPipelineCapable`) with exact async counterparts (`__aenter__`, `execute`, `select`, `async def results(self) -> list[Any]`, shielded `__aexit__` rollback).

## Edge Cases & Error Semantics

- If a batch command fails on the server (raising first canonical `DatabaseError` subclass carrying `.outcomes` and `.failing_index`) and the caller catches that exception inside `with driver.pipeline():`, the pipeline scope remains marked failed and `__exit__` / `__aexit__` MUST roll back (never commit partial preceding statements).
- Calling `results()` a second time or calling `execute()` / `select()` after `results()` has flushed raises `InterfaceError` and marks an unexited scope failed on uncaught propagation.
- Exiting `with driver.pipeline():` without calling `results()` rolls back any owned `BEGIN` / savepoint without flushing queued statements unless documented auto-flush is absent (`results()` is explicit; un-flushed exit rolls back cleanly and releases the lease).
- Real `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack` over explicitly injected `_sync.SyncPostgreSQLDriver` / `_async.AsyncPostgreSQLDriver` roll back the entire batch when post-`results()` strict zip/cardinality check or `SQLResult` construction raises.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py` (direct-importing `SyncPostgreSQLDriver` from `_sync` and `AsyncPostgreSQLDriver` from `_async`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Mixed `execute` + `select` pipeline & delayed commit | Queue `INSERT` (`1`), `UPDATE` (`2`), `SELECT id, meta` (`PgJson(None)` + duplicate label); inspect store visibility before vs after `__exit__` | Queue methods return `None`; `results()` returns `[1, 2, [{"id": 1, "meta": None}]]` over `1` data `Sync`; uncommitted until `__exit__`, committed + lease released (`in_use == 0`) after `__exit__`. |
| Caught server failure & caller post-`results()` exception | (a) Batch op `1` succeeds, op `2` raises `IntegrityError` (`23505`), caller catches `IntegrityError` inside `with`; (b) `results()` succeeds then caller raises `ValueError("hydration failed")` before `__exit__` | Both (a) and (b) roll back op `1`'s mutation in observable store state and release the lease cleanly; (a) carries `.failing_index == 1` and ordered `.outcomes`. |
| Ambient transaction savepoint nesting & wrong-owner refusal | Enter `driver.transaction()`, write outer row, enter `driver.pipeline()` that fails, catch outside inner `with`, then commit outer transaction; separately access ambient scope from child task/thread | Failed inner pipeline rolls back only its savepoint so outer row commits cleanly; child task/thread `pipeline().__enter__` raises `InterfaceError`. |
| Second `results()` / queue-after-flush refusal | Call `pipe.results()` twice or `pipe.execute(...)` after `pipe.results()` | Raises `InterfaceError`. |
| SQLSpec `execute_stack` rollback on result-count / hydration error | Inject `_sync` / `_async` driver into `PostgreSQLSyncDriver` / `PostgreSQLAsyncDriver` (with `ch26:t01` numeric profile); induce post-`results()` mismatch/error, plus `continue_on_error=True` sequential fallback | Post-`results()` error rolls back batch transaction before lease release; `continue_on_error=True` preserves per-op behavior. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py -q`
- Final Green + SQLSpec Regression + Lint/Typecheck gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions/sqlspec
  ```

## Acceptance Criteria

- [ ] `_sync.py::SyncPostgreSQLDriver` and `_async.py::AsyncPostgreSQLDriver` satisfy `SyncPipelineCapable` and `AsyncPipelineCapable`, reusing `_prepare_native_query` and `SyncPostgresPipeline` / `AsyncPostgresPipeline` (R13, R17, R24).
- [ ] Pipeline transactions and nested savepoints stay pinned across caller `results()` conversion and `execute_stack` checks, rolling back on caught server errors or caller conversion exceptions (R13, R17, R24).
- [ ] Commit message follows `feat(postgres): bind native pipeline factories to owned scopes` once verified.

## Notes & Discoveries

- One data `Sync` and one logical flush cover all queued batch operations; `BEGIN` / `COMMIT` / `SAVEPOINT` control frames bracket the batch scope cleanly [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Single-Sync pipeline outcomes, delayed context commit, and caught-error rollback invariants.
[^db-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipelineCapable` — SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — BaseSyncDriver.execute_stack and BaseAsyncDriver.execute_stack caller conversion inside pipeline context.
