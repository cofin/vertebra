---
type: Task
id: "native-postgres-driver:ch16:t02"
chapter_id: ch16-pipeline-scope-and-cancellation
title: "Wire PyO3 client pipeline scope methods and end-to-end transactional pipeline tests"
description: "Expose pipeline_begin, pipeline_results, and pipeline_finish on SyncPostgresClient and AsyncPostgresClient, update _postgres.pyi, and verify SyncPostgresPipeline and AsyncPostgresPipeline over real wire sessions."
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch16:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - .agents/bundles/specs/native-postgres-driver/chapters/ch16-pipeline-scope-and-cancellation/spec.md
  - src/rs/vertebra-postgres/src/pipeline_scope.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/db/adapters/postgres/pipeline.py
files:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_pipeline.py
estimated_loc: 220
plan_revision: 8
plan_commit: 8c540809
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
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Frozen SyncPostgresClient and AsyncPostgresClient pipeline method signatures and exception attribute contracts
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Shared Tokio completion drainer for async PyO3 pipeline methods
  - id: db-errors-rs
    resource: src/rs/vertebra-py/src/errors.rs::DbExceptionTypes
    title: Canonical vertebra._vertebra DatabaseError, InterfaceError, and NotSupportedError classes
tags:
  - postgres
  - pipeline
  - pyo3
  - contexts
---

# Task t02: Wire PyO3 Client Pipeline Scope Methods and End-to-End Transactional Pipeline Tests

## Objective & Scope Boundary

Expose `pipeline_begin`, `pipeline_results`, and `pipeline_finish` on `SyncPostgresClient` and `AsyncPostgresClient` in `vertebra._postgres`, synchronize `src/py/vertebra/_postgres.pyi`, and verify `SyncPostgresPipeline` and `AsyncPostgresPipeline` end-to-end against real wire sessions [^pg-contracts] [^py-async-rs]. Driver-level `.pipeline()` factory binding with the SQL-aware pyformat bind compiler belongs to `ch26:t02`.

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 16 PyO3 pipeline client methods and `.outcomes` / `.failing_index` exception invariants.
- [contracts.md](../../../contracts.md) — `PostgresPipelineOperation`, `PostgresPipelineOutcome`, `PostgresErrorInfo`, and pipeline exception rules [^pg-contracts].
- `src/rs/vertebra-postgres/src/pipeline_scope.rs` (`crate::session::pipeline_scope`) — `SessionHandle::{pipeline_begin, pipeline_results, pipeline_finish}` and PyO3 conversion helpers from `ch16:t01`.
- `src/rs/vertebra-postgres/src/sync_client.rs` — `PySyncPostgresClient` `#[pymethods]`.
- `src/rs/vertebra-postgres/src/async_client.rs` — `PyAsyncPostgresClient` `#[pymethods]` and `_vertebra_postgres_completion_drainer` [^py-async-rs].
- `src/py/vertebra/db/adapters/postgres/pipeline.py` — `SyncPostgresPipeline` and `AsyncPostgresPipeline` from `ch16:t01`.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs`:
  - `fn pipeline_begin(&self, py: Python<'_>) -> PyResult<u64>`
  - `fn pipeline_results(&self, py: Python<'_>, scope_id: u64, operations: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>>` — extracts `Sequence[PostgresPipelineOperation]` while attached, runs single-`Sync` `SessionHandle::pipeline_results` inside `py.detach(|| block_on_db(...))`, and converts `Vec<PipelineOutcome>` -> `tuple[PostgresPipelineOutcome, ...]` (or raises the first canonical `vertebra._vertebra` `DatabaseError` / `InterfaceError` / `NotSupportedError` with immutable `.outcomes` and `.failing_index` via `pipeline_error_to_py`) [^db-errors-rs].
  - `fn pipeline_finish(&self, py: Python<'_>, scope_id: u64, success: bool) -> PyResult<()>` (rejecting non-`bool` integers or invalid `scope_id` values).
- `src/rs/vertebra-postgres/src/async_client.rs`:
  - `fn pipeline_begin(&self, py: Python<'_>) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` (`Awaitable[int]`)
  - `fn pipeline_results(&self, py: Python<'_>, scope_id: u64, operations: &Bound<'_, PyAny>) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` (`Awaitable[tuple[PostgresPipelineOutcome, ...]]`, extracting operations synchronously before spawning onto Tokio)
  - `fn pipeline_finish(&self, py: Python<'_>, scope_id: u64, success: bool) -> PyResult<Bound<'_, RustDbTaskAwaitable>>` (`Awaitable[None]`)
- `src/py/vertebra/_postgres.pyi`:
  - Import `PostgresPipelineOperation` and `PostgresPipelineOutcome` from `vertebra.db.adapters.postgres.types` and declare `pipeline_begin`, `pipeline_results`, and `pipeline_finish` on `SyncPostgresClient` and `AsyncPostgresClient`.

## Edge Cases & Error Semantics

- **Server-submitted failure vs preflight guard failure**:
  - Server error at index `k` raises the canonical `vertebra._vertebra` `DatabaseError` subclass (e.g., `IntegrityError` for `23505`, `DataError` for `22012`) with `exc.failing_index == k` and `exc.outcomes == (PostgresPipelineOutcome(0, "result", ...), PostgresPipelineOutcome(k, "error", ...), PostgresPipelineOutcome(k+1, "skipped", failed_index=k), ...)`.
  - Native SQL-head guard rejection at index `k` raises `InterfaceError` (transaction control) or `NotSupportedError` (`COPY`) with `exc.failing_index == k`, `exc.outcomes == ()`, `exc.sqlstate is None`, and zero data-batch frames emitted on the wire [^pg-contracts].
- **Duplicate column names & `PgJson(None)` projection**: `SyncPostgresPipeline.results()` / `AsyncPostgresPipeline.results()` projects `"select"` rows via `_project_public_row` (`dict` keyed by column names matching `driver.select` last-wins duplicate semantics and top-level `PgJson(None)` -> `None`) while raw `client.pipeline_results(...)` preserves lossless `PostgresQueryResult` tuples (`PgJson(None)` intact).

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_pipeline.py` exercising real `SyncPostgresClient` and `AsyncPostgresClient` wire sessions through `SyncPostgresPipeline` and `AsyncPostgresPipeline`.
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| End-to-end sync & async single-`Sync` pipeline | Queue `execute("INSERT ...", [1])`, `execute("UPDATE ...", [2])`, `select("SELECT id, payload FROM t")` inside `SyncPostgresPipeline` / `AsyncPostgresPipeline` | Emits `"BEGIN"`, 1 `Sync` for all 3 ops, then `"COMMIT"` on clean exit; `results()` returns `[1, 2, [{"id": 1, "payload": ...}]]`. |
| First canonical `DatabaseError` + `.outcomes` | 3-op batch where op `1` fails with SQLSTATE `"23505"` (`IntegrityError`) | Raises `vertebra._vertebra.IntegrityError` (`sqlstate == "23505"`, `failing_index == 1`), `outcomes` has `("result", "error", "skipped")`, and `__exit__`/`__aexit__` rolls back op `0`. |
| Caught SQL-head alias table inside `with` | Queue `INSERT` followed by `";/*c*/COMMIT"` / `"PREPARE TRANSACTION 'g'"` / `"COPY t TO STDOUT"`, catch `InterfaceError`/`NotSupportedError` inside `with` | `exc.outcomes == ()`, `exc.failing_index == 1`, `0` data-operation wire frames emitted, and `__exit__`/`__aexit__` still sends `"ROLLBACK"`. |
| Same-client raw escape & post-`results()` error | Call `client.commit()` or `client.query("SELECT 1")` inside `with` (before & after `results()`); separately raise `ValueError("bad shape")` after `results()` | Raw `commit`/`query` raises `InterfaceError`; post-`results()` `ValueError` rolls back `"BEGIN"` / savepoint before calling `release()` once. |

## Verification

- Initial Red baseline:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pipeline.py -q
  ```
- Final Green, stubtest, Ruff, Mypy, Clippy, and diff gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_pipeline.py -q
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/vertebra/db/adapters/postgres/pipeline.py src/py/tests/unit/db/test_postgres_native_pipeline.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
  cargo clippy --manifest-path src/rs/Cargo.toml -p vertebra-postgres --all-targets -- -D warnings
  git diff --check
  ```

## Acceptance Criteria

- [ ] `SyncPostgresClient` and `AsyncPostgresClient` expose `pipeline_begin`, `pipeline_results`, and `pipeline_finish` matching `_postgres.pyi` and `contracts.md` (R13, R17).
- [ ] `SyncPostgresPipeline` and `AsyncPostgresPipeline` execute real single-`Sync` wire batches, attach `.outcomes` and `.failing_index` to canonical `vertebra._vertebra` exceptions, block same-owner raw escapes, and roll back on caught guard errors or post-`results()` caller exceptions (R13, R17).
- [ ] `test_postgres_native_pipeline.py`, `mypy.stubtest vertebra._postgres`, Ruff, Mypy, Clippy `-D warnings`, and `git diff --check` pass with zero warnings or diff hygiene errors.

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Frozen PyO3 client pipeline methods and exception outcome invariants.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Shared Tokio completion ring.
[^db-errors-rs]: `src/rs/vertebra-py/src/errors.rs::DbExceptionTypes` — Canonical `vertebra._vertebra` exception hierarchy.
