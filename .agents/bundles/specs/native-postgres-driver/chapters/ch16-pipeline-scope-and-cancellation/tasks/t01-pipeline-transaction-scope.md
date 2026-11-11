---
type: Task
id: "native-postgres-driver:ch16:t01"
chapter_id: ch16-pipeline-scope-and-cancellation
title: "Implement retained pipeline transaction scope and Python pipeline context classes"
description: "Enforce SessionAccess::Pipeline transaction/savepoint scope across result conversion in vertebra-postgres and implement SyncPostgresPipeline and AsyncPostgresPipeline with enqueue-time snapshots."
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch15:t04"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch16-pipeline-scope-and-cancellation/spec.md
  - src/rs/vertebra-postgres/src/pipeline.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/py/vertebra/db/adapters/postgres/types.py
  - src/py/vertebra/db/driver/capabilities.py
files:
  - src/rs/vertebra-postgres/src/pipeline_scope.rs
  - src/rs/vertebra-postgres/src/session.rs
  - src/py/vertebra/db/adapters/postgres/pipeline.py
tests:
  - src/rs/vertebra-postgres/tests/pipeline_scope.rs
  - src/py/tests/unit/db/test_postgres_native_pipeline.py
estimated_loc: 250
plan_revision: 5
plan_commit: 0147d80c
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T19:32:00Z"
last_verified_commit: 1ae9ab3d
verification_evidence: "cargo test -p vertebra-postgres --test pipeline_scope (4 passed) + pytest test_postgres_native_pipeline.py (9 passed) + mypy/ruff exit 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-08T02:05:22Z"
commit: e8b397b4
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen pipeline transaction scope, caught-failure persistence, and capability context contracts
  - id: db-caps-py
    resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
    title: Public SyncPipeline and AsyncPipeline capability protocols
  - id: pg-driver-py
    resource: src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver
    title: Adapter _project_public_row projection convention for SELECT dictionaries
tags:
  - postgres
  - pipeline
  - transactions
  - python
  - rust
---

# Task t01: Implement Retained Pipeline Transaction Scope and Python Pipeline Context Classes

## Objective & Scope Boundary

Keep an explicit transaction or nested savepoint alive from pipeline context entry through caller result conversion in `vertebra-postgres` (`pipeline_scope.rs`, `session.rs`) while blocking all ordinary raw session methods under `SessionAccess::Pipeline(token)`, and implement adapter-owned `SyncPostgresPipeline` and `AsyncPostgresPipeline` (`src/py/vertebra/db/adapters/postgres/pipeline.py`) with enqueue-time deep parameter snapshots and persistent failure state [^pg-contracts] [^db-caps-py]. Exposing `pipeline_begin`, `pipeline_results`, and `pipeline_finish` on PyO3 `SyncPostgresClient` / `AsyncPostgresClient` and `_postgres.pyi` lands immediately next in `ch16:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 16 `SessionAccess::Pipeline`, persistent failure flags, and `SyncPostgresPipeline` / `AsyncPostgresPipeline` contracts.
- [contracts.md](../../../contracts.md) — Pipeline transaction/savepoint retention, caught-error rollback, and `_project_public_row` projection rules [^pg-contracts].
- `src/rs/vertebra-postgres/src/pipeline.rs` — `PipelineBatch`, `PipelineOutcome`, and `PipelineFailure` from `ch15:t03`–`t04`.
- `src/rs/vertebra-postgres/src/session.rs` — `SessionHandle`, `SessionOperationGuard` / `with_connection`, `TransactionScope` (`ch13:t01`), and `PooledConnection` pinning (`ch14:t02`).
- `src/py/vertebra/db/adapters/postgres/types.py` — `PostgresPipelineOperation`, `PostgresPipelineOutcome`, `PostgresErrorInfo`, and `_project_public_row` [^pg-driver-py].
- `src/py/vertebra/db/driver/capabilities.py::{SyncPipeline, AsyncPipeline}` — Capability context manager interface [^db-caps-py].

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/pipeline_scope.rs` and `src/rs/vertebra-postgres/src/session.rs` (mounted as canonical `crate::session::pipeline_scope` via `#[path = "pipeline_scope.rs"] pub mod pipeline_scope; pub use pipeline_scope::PipelineScopeToken;` inside `src/rs/vertebra-postgres/src/session.rs` — `rustc` 2021/2024 resolves `#[path = "pipeline_scope.rs"]` relative to `session.rs`'s parent `src/` directory, whereas unannotated `pub mod pipeline_scope;` looks in `src/session/` and `"../pipeline_scope.rs"` escapes `src/`; never declare a duplicate `mod pipeline_scope;` in `src/lib.rs`):
  - `#[derive(Clone, Copy, Debug, PartialEq, Eq)] pub struct PipelineScopeToken(pub u64);`
  - Centralize `SessionHandle` access checks via private `pub(crate) enum SessionAccess { Ordinary, Pipeline(PipelineScopeToken), Cancel, ForcedCleanup }`:
    - Route every ordinary raw entry (`with_connection` / `acquire_operation`, `query`, `query_multi`, `simple_query`, `begin`, `commit`, `rollback`, `savepoint`, `release_savepoint`, `rollback_to_savepoint`, `open_portal`, future COPY/Arrow sibling callers, and `SessionHandle::close` lease release) through `SessionAccess::Ordinary`. While any `PipelineScopeToken` is active, ALL `SessionAccess::Ordinary` calls fail immediately with `PostgresError::Interface` before touching the wire, even from the same client/thread/task.
  - `SessionHandle::pipeline_begin(&self) -> impl Future<Output = Result<PipelineScopeToken, PostgresError>> + Send`:
    - Refuses `TransactionStatus::Failed` (`'E'`), `PoolerMode::Statement`, and nested/concurrent `pipeline_begin` while a token is already active; opens `BEGIN` at `'I'` or `SAVEPOINT vtb_sp_<n>` at `'T'` via the trusted `SessionAccess::Pipeline(token)` path (outside the measured single-`Sync` data batch) and pins session/lease ownership.
  - `SessionHandle::pipeline_results(&self, token: PipelineScopeToken, batch: PipelineBatch) -> impl Future<Output = Result<Vec<PipelineOutcome>, PostgresError>> + Send`:
    - Validates exact `token`, refuses repeated `pipeline_results` invocations (`PostgresError::Interface`), and never commits/releases the scope.
    - On `ch15:t02` `PipelineValidation` preflight failure OR `ch15:t04` server `PipelineFailure`, persistently marks the active native scope `failed = true` and records the primary error BEFORE returning `Err(...)` so even if a caller catches the error, `pipeline_finish(token, true)` still rolls back.
  - `SessionHandle::pipeline_finish(&self, token: PipelineScopeToken, success: bool) -> impl Future<Output = Result<(), PostgresError>> + Send`:
    - Commits (`ExplicitTransaction`) or releases savepoint (`NestedSavepoint`) ONLY when `success == true` and native `failed == false`; otherwise rolls back the owned transaction or rolls back + releases only the pipeline savepoint (preserving ambient `'T'` work before the savepoint). Clearing an unclosed/dropped scope schedules bounded rollback/retirement via `ForcedCleanup`.
  - Include PyO3 conversion helpers (`extract_pipeline_batch`, `outcomes_to_python`, `pipeline_error_to_py`) in `pipeline_scope.rs` for `ch16:t02` client methods.
- `src/py/vertebra/db/adapters/postgres/pipeline.py`:
  - Define structural `Protocol`s (`SyncPipelineClientProtocol`, `AsyncPipelineClientProtocol`) and `type PreparePostgresQuery = Callable[[str, Sequence[PostgresValue | PgParameter] | Mapping[str, PostgresValue | PgParameter] | None], tuple[str, tuple[PostgresValue | PgParameter, ...]]]` with zero eager `vertebra._postgres` runtime import.
  - Default `prepare` callback accepts numeric `$N` SQL + `None` or non-`str`/`bytes` `Sequence` (normalizing `None` -> `()`), rejecting `Mapping` (`InterfaceError`) without rewriting `%s` / `%(name)s` placeholders.
  - `class SyncPostgresPipeline` and `class AsyncPostgresPipeline`:
    - `__init__(self, client, *, prepare: PreparePostgresQuery | None = None, release: Callable[[], None] | Callable[[], Awaitable[None]] | None = None) -> None`
    - `execute(self, sql: str, parameters: ... = None) -> None` and `select(self, sql: str, parameters: ... = None) -> None`: run `prepare` immediately and recursively deep-snapshot mutable containers/buffers (`dict`, `list`, `tuple`, `PgJson`, `PgArray`, `PgComposite`, `PgBitString`, `PgVector`, `PgTemporal`, `PgParameter`, `bytearray`/`memoryview` -> `bytes`, rejecting reference cycles/unsupported types) into owned queued `PostgresPipelineOperation` items; any `prepare`/snapshot/state error sets persistent `_failed = True` before raising.
    - `results(self) -> list[Any]` / `async def results(self) -> list[Any]`: submits queued operations once (`[]` returns `[]` without round trip), projects `"execute"` -> `int` (`affected_rows`) and `"select"` -> `list[dict[str, Any]]` via `_project_public_row`, checks outcome cardinality, marks `_failed = True` on any exception, and leaves the scope open.
    - `__exit__` / `__aexit__`: calls `pipeline_finish(scope_id, exc_type is None and not self._failed)` FIRST, then invokes `release` (if supplied) **once**, preserving any primary query/conversion exception and chaining cleanup errors.

## Edge Cases & Error Semantics

- **Mutating inputs after enqueue**: Mutating a passed `dict`, `list`, or `bytearray` after `pipe.execute(...)` and before `pipe.results()` never alters the queued snapshot.
- **Caught exceptions inside `with` body**: Catching a `prepare`, snapshot, or `pipeline_results` guard/server exception inside `with` still calls `pipeline_finish(scope_id, False)` because `_failed` (and native `failed`) is sticky.
- **Caller conversion error after `results()`**: Raising `ValueError` (e.g. `zip(strict=True)` or row schema mismatch) inside the `with` block after `results()` succeeded causes `__exit__`/`__aexit__` to call `pipeline_finish(scope_id, False)` and roll back all DML in the batch before `release()` returns the pooled lease.

## Public-Seam Test Blueprint

- **Canonical test files**: `src/rs/vertebra-postgres/tests/pipeline_scope.rs` (native `SessionHandle` scope + wire rollback) and `src/py/tests/unit/db/test_postgres_native_pipeline.py` (snapshot isolation, sticky `_failed` rollback, `_project_public_row` projection, and `release` ordering via structural client doubles; extended with live native PyO3 clients in `ch16:t02`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Post-`results()` caller exception rolls back | `pipeline_begin` at `'I'`, `pipeline_results([INSERT INTO t VALUES (1)])` succeeds (`'T'`), caller raises -> `pipeline_finish(token, false)` | Emits `"BEGIN"`, single-`Sync` `INSERT`, `"ROLLBACK"` -> `'I'`; separate connection observes `0` durable rows. |
| Ambient transaction savepoint survival | Caller `begin()` + `INSERT 1`, enters pipeline scope, `INSERT 2` + failing `SELECT 1/0`, `pipeline_finish(token, false)` | Scope opens `SAVEPOINT vtb_sp_<n>`, rolls back + releases only `vtb_sp_<n>`; caller stays in `'T'` with `INSERT 1` intact and uncommitted. |
| Same-owner ordinary API refusal | Active `PipelineScopeToken`; same owner calls `query`, `query_multi`, `begin`, `commit`, `rollback`, `savepoint`, `open_portal`, or `close` | Every call returns `Err(PostgresError::Interface)` with `0` wire bytes both before and after `pipeline_results`. |
| Caught preflight guard failure stays failed | `pipeline_results([INSERT, ";COMMIT"])` returns `Err(PipelineValidation)`; caller ignores error and calls `pipeline_finish(token, true)` | Native scope stayed `failed = true`; `pipeline_finish(token, true)` executes `"ROLLBACK"` and surfaces the primary `InterfaceError` (`outcomes = ()`, `failing_index = 1`). |
| Enqueue deep snapshot & sticky Python `_failed` | Pass `doc = {"a": [1]}` to `pipe.execute(..., [PgJson(doc)])`, mutate `doc["a"].append(2)` before `results()`; also catch `Mapping` `prepare` error inside `with` | Queued `PostgresPipelineOperation` retains `{"a": [1]}`; caught `prepare` error still calls `pipeline_finish(scope_id, False)` then `release()` once. |

## Verification

- Initial Red baseline:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_scope
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pipeline.py -q
  ```
- Final Green, Ruff, Mypy, Clippy, and diff gate:
  ```bash
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test pipeline_scope
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pipeline.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/pipeline.py src/py/tests/unit/db/test_postgres_native_pipeline.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres/pipeline.py
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `SessionHandle::{pipeline_begin, pipeline_results, pipeline_finish}` retains `BEGIN` (at `'I'`) or a savepoint (at `'T'`) across caller result conversion and blocks all `SessionAccess::Ordinary` calls while active (R13, R17).
- [ ] Caught preflight guard, preparation, snapshot, or server batch errors persist in both native and Python scope state so `finish(true)` / clean `with` exit still rolls back (R13, R17).
- [ ] `SyncPostgresPipeline` and `AsyncPostgresPipeline` snapshot nested mutable inputs at enqueue time, project `int` / `list[dict]`, and run `pipeline_finish` before invoking `release` once (R17).
- [ ] `pipeline_scope`, `test_postgres_native_pipeline.py`, Ruff, Mypy, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen pipeline transaction scope, `SessionAccess` exclusivity, and `SyncPostgresPipeline` / `AsyncPostgresPipeline` contracts.
[^db-caps-py]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — Public Vertebra pipeline capability interface.
[^pg-driver-py]: `src/py/vertebra/db/adapters/postgres/driver.py::SyncPostgreSQLDriver` — Adapter row dictionary projection convention.
