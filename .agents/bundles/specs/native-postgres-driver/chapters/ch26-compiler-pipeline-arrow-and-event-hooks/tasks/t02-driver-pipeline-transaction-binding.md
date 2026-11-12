---
type: Task
id: "native-postgres-driver:ch26:t02"
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: Bind permanent native driver pipeline factories to transaction ownership
description: Implement synchronous pipeline() factories on permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver lazily wrapping SyncPostgresPipeline and AsyncPostgresPipeline over a single native pipeline_begin/pipeline_results/pipeline_finish lifecycle.
status: draft
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
estimated_loc: 225
plan_revision: 8
plan_commit: 5eb75b2a
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
  - id: npd-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Single-lifecycle native pipeline delegation, delayed context commit, and caught-error rollback invariants
  - id: pg-pipe-py
    resource: src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline
    title: SyncPostgresPipeline, AsyncPostgresPipeline, SyncPipelineClientProtocol, and AsyncPipelineClientProtocol
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

Add synchronous `pipeline(self) -> SyncPipeline` to `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver` and synchronous `pipeline(self) -> AsyncPipeline` to `src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver`, wrapping `SyncPostgresPipeline` and `AsyncPostgresPipeline` over a lazy driver-owned `SyncPipelineClientProtocol` / `AsyncPipelineClientProtocol` adapter so pool leases acquire inside `__enter__` / `__aenter__`, `BEGIN` / `SAVEPOINT` / `COMMIT` / `ROLLBACK` execute via a single native `pipeline_begin` / `pipeline_results` / `pipeline_finish` lifecycle without duplicate Python-side `BEGIN`, active native calls register in `driver.cancel()`, and transient leases release cleanly after `pipeline_finish` [^npd-contracts] [^pg-pipe-py] [^db-caps] [^sqlspec-py]. Public `driver.py` remains untouched until `ch27:t01`.

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — Lazy lease acquisition, single native pipeline scope lifecycle, `_project_public_row` select projection, `.outcomes` / `.failing_index`, `cancel()` registration, and caught-error rollback rules [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/_sync.py` — Permanent `SyncPostgreSQLDriver`, `ContextVar` scope state, and active-operation `cancel()` registry from `ch25:t03`.
- `src/py/vertebra/db/adapters/postgres/_async.py` — Permanent `AsyncPostgreSQLDriver`, `ContextVar` scope state, and async `cancel()` registry from `ch25:t04`.
- `src/py/vertebra/db/adapters/postgres/pipeline.py` — `SyncPostgresPipeline`, `AsyncPostgresPipeline`, `SyncPipelineClientProtocol`, `AsyncPipelineClientProtocol`, `prepare`, and `release` hooks [^pg-pipe-py].
- `src/py/vertebra/db/driver/capabilities.py` — `SyncPipeline`, `AsyncPipeline`, `SyncPipelineCapable`, and `AsyncPipelineCapable` synchronous factory signatures [^db-caps].
- `src/py/vertebra/extensions/sqlspec/adapters.py` — `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack` [^sqlspec-py].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/_sync.py`:
  - `def pipeline(self) -> SyncPipeline` on `SyncPostgreSQLDriver` (synchronous zero-I/O factory structurally satisfying `SyncPipelineCapable`):
    - Returns `SyncPostgresPipeline(adapter, prepare=self._prepare_native_query, release=adapter.release)` backed by a private driver-owned `SyncPipelineClientProtocol` adapter.
    - Inside `__enter__` -> `adapter.pipeline_begin() -> int`: validates closed/failed driver state and `(thread_id, task_id)` `ContextVar` ownership (raising `InterfaceError` on cross-thread/task inherited ambient transactions OR overlapping/re-entrant active pipeline entry on the same scope); if no ambient transaction exists, lazily acquires one transient `SyncPostgresClient` pool lease, otherwise borrows the pinned ambient client; then delegates `BEGIN` (standalone) or `SAVEPOINT` (ambient transaction) directly to a **single** native `client.pipeline_begin()` call (**never** issuing a separate Python-side `client.begin()` / `BEGIN` ahead of `pipeline_begin()`).
    - Inside `results() -> list[Any]` -> `adapter.pipeline_results(scope_id, ops)`: runs `client.pipeline_results(scope_id, ops)` wrapped in the driver's active-operation `cancel()` registry, producing one single-`Sync` flush and keeping the native transaction/savepoint scope open across caller hydration and `execute_stack` cardinality checks (`select` rows map top-level `PgJson(None)` -> `None` via `types._project_public_row`, preserving rightmost duplicate column labels; ensure `pipeline.py` / adapter projection aligns with public `select` semantics).
    - Inside `__exit__` -> `adapter.pipeline_finish(scope_id, commit_ok)` + `adapter.release()`: registers `client.pipeline_finish(scope_id, commit_ok)` in the active-operation `cancel()` registry (`commit_ok=True` commits or releases savepoint only when zero batch/validation/caller errors occurred; `commit_ok=False` rolls back transaction or savepoint while preserving original exception), marks any ambient transaction failed if cancelled/corrupted, clears the active-pipeline flag, and releases any transient pool lease in `release()` after `pipeline_finish` completes.
- `src/py/vertebra/db/adapters/postgres/_async.py`:
  - `def pipeline(self) -> AsyncPipeline` on `AsyncPostgreSQLDriver` (synchronous zero-I/O factory structurally satisfying `AsyncPipelineCapable`, returning `AsyncPostgresPipeline(adapter, prepare=self._prepare_native_query, release=adapter.aclose_lease)`):
    - Exact async counterpart lazily acquiring any transient `AsyncPostgresClient` lease inside `__aenter__` -> `await adapter.pipeline_begin()`, delegating `BEGIN`/`SAVEPOINT`/`COMMIT`/`ROLLBACK` to a single native `await client.pipeline_begin()` / `await client.pipeline_results(...)` / shielded `await client.pipeline_finish(...)` lifecycle, enforcing `(thread_id, task_id)` ownership and overlapping-pipeline `InterfaceError`, registering each native call in `AsyncPostgreSQLDriver`'s active-operation `cancel()` registry, and releasing any transient lease after `pipeline_finish` inside `CancelScope(shield=True)`.

## Edge Cases & Error Semantics

- Constructing `pipe = driver.pipeline()` without entering `with pipe:` / `async with pipe:` acquires zero pool leases and performs zero I/O.
- Entering a second `with driver.pipeline():` inside an already-open `with driver.pipeline():` on the same driver scope raises `InterfaceError` without touching the active scope's socket.
- If `pipeline_begin()` fails after acquiring a transient pool lease, the adapter releases/retires that lease immediately before propagating the error so `in_use` never leaks.
- Calling `driver.cancel()` from a parent thread/task while `pipe.results()` is blocked in native wire I/O sends `1` `CancelRequest` to that pinned client, retires the socket, marks any ambient transaction failed until `rollback()`, and releases the pool slot once drained.
- Server error caught inside `with driver.pipeline():` or caller exception raised after `results()` before `__exit__`/`__aexit__` passes `commit_ok=False` to `pipeline_finish` and rolls back all batch mutations.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py` (direct-importing `SyncPostgreSQLDriver` from `_sync` and `AsyncPostgreSQLDriver` from `_async` over `postgres_ports.py`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Lazy factory, single `pipeline_begin`/`results`/`finish` lifecycle, & delayed commit | Call `p = driver.pipeline()` (`in_use == 0`), enter `with p:`, queue `%s`/`%(k)s`/`$1` `execute` + `select` (`PgJson(None)` + duplicate label), inspect calls & visibility before/after exit | Zero duplicate `client.begin()` calls (`pipeline_begin` called once on enter, `pipeline_results` once on `results()`, `pipeline_finish(..., True)` once on clean exit); uncommitted before exit, committed + `in_use == 0` after exit. |
| Caught server failure & post-`results()` caller exception | (a) Op `1` succeeds, op `2` raises `IntegrityError` (`23505`), caught inside `with`; (b) `results()` succeeds then caller raises `ValueError("hydration failed")` before exit | Both invoke `pipeline_finish(scope_id, False)`, roll back op `1`, and release lease; (a) carries `.failing_index == 1` and ordered `.outcomes`. |
| Ambient transaction savepoint, overlapping pipeline refusal, & cross-task/thread refusal | Enter `driver.transaction()`, write outer row, run failing `driver.pipeline()` caught outside inner `with`, commit outer; separately nest `with driver.pipeline():` inside `with driver.pipeline():` and access from child thread/task | Ambient client delegates savepoint via `pipeline_begin()` without acquiring a second lease; failed inner pipeline rolls back only savepoint so outer row commits; nested overlapping pipeline and wrong-owner access both raise `InterfaceError`. |
| Active-operation `driver.cancel()` during in-flight `pipeline_results` & failed `pipeline_begin` | Trigger `driver.cancel()` while `pipe.results()` awaits slow query; separately fail `client.pipeline_begin()` on transient lease | `cancel()` sends `1` `CancelRequest`, retires socket, and restores pool `in_use == 0`; failed `pipeline_begin` releases transient lease immediately (`in_use == 0`). |
| SQLSpec `execute_stack` rollback on post-`results()` cardinality/hydration error | Inject `_sync`/`_async` driver into `PostgreSQLSyncDriver`/`PostgreSQLAsyncDriver` (using `ch26:t01` numeric transport); induce post-`results()` error and test `continue_on_error=True` sequential path | Post-`results()` error triggers `pipeline_finish(..., False)` rolling back batch before lease release; `continue_on_error=True` preserves per-op results. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py -q`
- Final Green + SQLSpec Regression + Lint/Typecheck gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py src/py/tests/unit/db/test_postgres_pipeline.py src/py/tests/unit/extensions/test_sqlalchemy_and_sqlspec_vtb.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/test_postgres_native_adapter_pipeline.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres src/py/vertebra/extensions/sqlspec
  ```

## Acceptance Criteria

- [ ] `SyncPostgreSQLDriver.pipeline() -> SyncPipeline` and `AsyncPostgreSQLDriver.pipeline() -> AsyncPipeline` remain synchronous factories lazily acquiring transient leases in `__enter__`/`__aenter__` and delegating `BEGIN`/`SAVEPOINT`/`COMMIT`/`ROLLBACK` to a single native `pipeline_begin`/`pipeline_results`/`pipeline_finish` lifecycle (zero duplicate Python `BEGIN`) (R13, R17, R24).
- [ ] Overlapping pipelines and wrong-owner `(thread_id, task_id)` scopes raise `InterfaceError`, in-flight pipeline calls integrate with `driver.cancel()`, caught/caller errors roll back cleanly, and transient leases release after `pipeline_finish` (R13, R17, R24).
- [ ] Commit message follows `feat(postgres): bind native pipeline factories to owned scopes` once verified.

## Notes & Discoveries

- Delegating transaction/savepoint brackets directly to `client.pipeline_begin()` and `client.pipeline_finish()` aligns the Python adapter 1:1 with the Rust single-`Sync` pipeline state machine implemented in `ch16` [^npd-contracts] [^pg-pipe-py].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Single-lifecycle native pipeline delegation, delayed context commit, and caught-error rollback invariants.
[^pg-pipe-py]: `src/py/vertebra/db/adapters/postgres/pipeline.py::SyncPostgresPipeline` — SyncPostgresPipeline, AsyncPostgresPipeline, SyncPipelineClientProtocol, and AsyncPipelineClientProtocol.
[^db-caps]: `src/py/vertebra/db/driver/capabilities.py::SyncPipelineCapable` — SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols.
[^sqlspec-py]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — BaseSyncDriver.execute_stack and BaseAsyncDriver.execute_stack caller conversion inside pipeline context.
