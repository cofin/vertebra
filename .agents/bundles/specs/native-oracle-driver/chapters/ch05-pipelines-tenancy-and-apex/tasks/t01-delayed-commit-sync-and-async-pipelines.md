---
type: Task
id: "native-oracle-driver:ch05:t01"
chapter_id: ch05-pipelines-tenancy-and-apex
title: Implement OraclePipelineOutcome and delayed-commit SyncOraclePipeline and AsyncOraclePipeline
description: Implement ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED and OraclePipelineOutcome in vertebra._oracle and SyncOraclePipeline / AsyncOraclePipeline in vertebra.db.adapters.oracle.driver holding an open transaction or SAVEPOINT _vtb_pipe_<n> across results() and committing only on clean context exit.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-oracle-driver:ch04:t03"
read_set:
  - .agents/bundles/specs/native-oracle-driver/contracts.md
  - src/py/vertebra/db/driver/capabilities.py
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/extensions/sqlspec/adapters.py
  - src/rs/vertebra-db/src/py_async.rs
files:
  - src/rs/vertebra-oracle/src/client.rs
  - src/py/vertebra/db/adapters/oracle/driver.py
  - src/py/vertebra/_oracle.pyi
tests:
  - src/py/tests/unit/db/test_oracle_adapter.py
estimated_loc: 220
plan_revision: 3
plan_commit: 769302bc
state_revision: 0
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: null
last_verified_commit: null
verification_evidence: null
created_at: "2026-11-09T00:55:00Z"
updated_at: "2026-11-09T01:28:30Z"
commit: null
sources:
  - id: ora-contracts
    resource: .agents/bundles/specs/native-oracle-driver/contracts.md
    title: Public OraclePipelineOutcome and delayed-commit pipeline contracts
  - id: db-capabilities
    resource: src/py/vertebra/db/driver/capabilities.py::SyncPipeline
    title: SyncPipeline, AsyncPipeline, SyncPipelineCapable, and AsyncPipelineCapable protocols
  - id: ora-driver-py
    resource: src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver
    title: SyncOracleDriver and AsyncOracleDriver implementation module
  - id: sqlspec-stack
    resource: src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver
    title: SQLSpec execute_stack caller hydration inside driver.pipeline() context
  - id: crate-research
    resource: .agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md
    title: Primary-source analysis of oracledb =26.0.0-beta.5 synchronous TTC execution
tags:
  - oracle
  - pipeline
  - rust
  - python
  - transactions
---

# Task t01: Implement `OraclePipelineOutcome` and Delayed-Commit `SyncOraclePipeline` and `AsyncOraclePipeline`

## Objective & Scope Boundary

Implement `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool = False`, `OraclePipelineOutcome`, and `execute_pipeline_batch` on `SyncOracleClient` and `AsyncOracleClient` in `src/rs/vertebra-oracle/src/client.rs` (plus `src/py/vertebra/_oracle.pyi`), and implement `SyncOraclePipeline` (`SyncPipeline`) and `AsyncOraclePipeline` (`AsyncPipeline`) on `SyncOracleDriver` and `AsyncOracleDriver` in `src/py/vertebra/db/adapters/oracle/driver.py` [^ora-contracts] [^db-capabilities] [^ora-driver-py]. Entering `with driver.pipeline() as pipe:` or `async with driver.pipeline() as pipe:` pins a pooled worker session and opens a transaction (or issues `SAVEPOINT _vtb_pipe_<n>` when the driver is already inside an outer transaction). `pipe.results()` executes queued operations sequentially on the pinned worker thread while keeping the transaction/savepoint open so caller cardinality checks (`select_one`) and `msgspec` struct hydration inside the `with` block roll back the entire batch on error [^sqlspec-stack]. Also add a crate-capability tracking test detecting if a future `oracledb` crate adds non-blocking TTC pipelining [^crate-research]. VPD tenancy and SQLSpec/APEX wiring belong to `ch05:t02`–`t03` ([../spec.md](../spec.md)).

## Bounded Read Manifest

- `.agents/bundles/specs/native-oracle-driver/contracts.md` — `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED`, `OraclePipelineOutcome`, `SyncOraclePipeline`, and `AsyncOraclePipeline` contracts ([../../contracts.md](../../contracts.md)) [^ora-contracts].
- `src/py/vertebra/db/driver/capabilities.py` — `SyncPipeline`, `AsyncPipeline`, `SyncPipelineCapable`, and `AsyncPipelineCapable` runtime protocols [^db-capabilities].
- `src/py/vertebra/db/adapters/oracle/driver.py` — `SyncOracleDriver` and `AsyncOracleDriver` transaction and pool lease hooks [^ora-driver-py].
- `src/py/vertebra/extensions/sqlspec/adapters.py` — `BaseSyncDriver.execute_stack` and `BaseAsyncDriver.execute_stack` pipeline context lifecycle [^sqlspec-stack].
- `src/rs/vertebra-db/src/py_async.rs` — `spawn_rust_db_task_with_attr` async completion bridge.

## Symbol & Signature Contracts

- `src/rs/vertebra-oracle/src/client.rs` & `src/py/vertebra/_oracle.pyi`:
  - `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED: bool = False`
  - `#[pyclass(module = "vertebra._oracle", frozen)] pub struct OraclePipelineOutcome`:
    - `#[pyo3(get)] pub status: String` (`"completed"`, `"error"`, or `"skipped"`)
    - `#[pyo3(get)] pub result: Option<Py<OracleQueryResult>>`
    - `#[pyo3(get)] pub error: Option<Py<PyAny>>`
  - `SyncOracleClient.execute_pipeline_batch(&self, py: Python<'_>, operations: &Bound<'_, PySequence>, continue_on_error: bool) -> PyResult<Vec<OraclePipelineOutcome>>`
  - `AsyncOracleClient.execute_pipeline_batch<'py>(&self, py: Python<'py>, operations: &Bound<'py, PySequence>, continue_on_error: bool) -> PyResult<Bound<'py, PyAny>>`
- `src/py/vertebra/db/adapters/oracle/driver.py`:
  - `class SyncOraclePipeline`:
    - `def __init__(self, driver: SyncOracleDriver) -> None`
    - `def __enter__(self) -> Self`
    - `def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None`
    - `def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`
    - `def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`
    - `def results(self) -> list[Any]`
  - `class AsyncOraclePipeline`:
    - `def __init__(self, driver: AsyncOracleDriver) -> None`
    - `async def __aenter__(self) -> Self`
    - `async def __aexit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: TracebackType | None) -> None`
    - `def execute(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`
    - `def select(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None) -> None`
    - `async def results(self) -> list[Any]`
  - `SyncOracleDriver.pipeline(self) -> SyncOraclePipeline`
  - `AsyncOracleDriver.pipeline(self) -> AsyncOraclePipeline`

## Edge Cases & Error Semantics

- **Delayed Commit at Context Exit**: `results()` flushes queued `(sql, parameters, returns_rows)` items on the pinned worker thread and returns ordered row lists (for `select`) or `int` rowcounts (for `execute`), but does **not** commit. Clean `__exit__` / `__aexit__` (`exc_type is None` and `self._flushed` with zero pending queued operations) commits the pipeline-owned transaction.
- **Oracle `SAVEPOINT` Dialect Rules**: When `driver.in_transaction` is already `True` upon entering `pipeline()`, the pipeline issues `SAVEPOINT _vtb_pipe_<n>` (monotonic counter `_pipe_savepoint_seq`). On error (`exc_type is not None` or statement failure), it issues `ROLLBACK TO SAVEPOINT _vtb_pipe_<n>`. On clean exit, it leaves the outer transaction open without emitting `RELEASE SAVEPOINT` (which Oracle rejects with `ORA-00900`).
- **Mid-Batch Statement Failure & Unflushed Exit**: In fail-fast mode (`continue_on_error=False`), when operation `k` fails with a mapped `DatabaseError`, operations `k + 1 .. N` record `status="skipped"`, `results()` raises operation `k`'s `DatabaseError`, and `__exit__`/`__aexit__` rolls back the transaction or savepoint so operations `0 .. k - 1` are undone. Exiting the `with` block without calling `results()` when operations were queued also rolls back and raises `InterfaceError("Oracle pipeline exited with unexecuted operations")`.
- **Single Active Pipeline Guard**: Opening a second `driver.pipeline()` on the same driver while a pipeline is already active raises `InterfaceError("An Oracle pipeline is already active on this driver")`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_oracle_adapter.py`
- **Worked Test Matrix**:

| Test Name | Scenario & Input | Observable Assertion |
| :--- | :--- | :--- |
| `test_sync_and_async_pipeline_delayed_commit_on_clean_exit` | Queue `pipe.execute("INSERT INTO items VALUES (:1, :2)", (1, "a"))` and `pipe.select("SELECT id, label FROM items WHERE id = :1", (1,))`, call `pipe.results()` | `isinstance(driver, SyncPipelineCapable)` and `isinstance(async_driver, AsyncPipelineCapable)` are `True`; `results()` returns `[1, [{"id": 1, "label": "a"}]]` while transaction remains uncommitted until `__exit__`/`__aexit__`, after which `COMMIT` is recorded once. |
| `test_pipeline_rolls_back_when_caller_raises_after_results` | Call `pipe.results()` inside `with driver.pipeline() as pipe:`, then raise `ValueError("hydration failed")` before exiting `with` | Pipeline `__exit__`/`__aexit__` executes `ROLLBACK` (zero `COMMIT` calls) and propagates `ValueError("hydration failed")`. |
| `test_pipeline_nested_inside_transaction_uses_oracle_savepoint_without_release` | Inside `with driver.transaction():`, enter `with driver.pipeline() as pipe:`, run `pipe.results()`, and test both clean exit and error exit | Entry executes `SAVEPOINT _vtb_pipe_1`; error exit executes `ROLLBACK TO SAVEPOINT _vtb_pipe_1`; clean exit executes zero `RELEASE SAVEPOINT` and leaves outer transaction open until outer `__exit__`. |
| `test_pipeline_mid_batch_error_marks_subsequent_skipped_and_rolls_back` | Queue 3 statements where statement 2 raises `IntegrityError` (`ORA-00001`, `sqlstate="23505"`) | `OraclePipelineOutcome` entries have statuses `("completed", "error", "skipped")`; `results()` raises `IntegrityError`; `__exit__` executes `ROLLBACK`. |
| `test_oracledb_crate_ttc_pipeline_capability_probe` | Inspect `vertebra._oracle` capability constant `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED` | Asserts `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED is False` on `oracledb = "=26.0.0-beta.5"`, documenting the tracking probe if a future crate version exposes non-blocking TTC pipelining. |

## Verification

```bash
make dev
cargo test --manifest-path src/rs/Cargo.toml -p vertebra-oracle --locked
uv run --no-sync python -m mypy.stubtest vertebra._oracle
uv run --no-sync pytest src/py/tests/unit/db/test_oracle_adapter.py -k "pipeline" -q
uv run --no-sync mypy src/py/vertebra/db/adapters/oracle/driver.py src/py/tests/unit/db/test_oracle_adapter.py
```

Commit as `feat(oracle): implement delayed-commit sync and async pipelines`.

## Acceptance Criteria

- [ ] `ORACLESQL_NATIVE_TTC_PIPELINE_SUPPORTED`, `OraclePipelineOutcome`, and `execute_pipeline_batch` are exported on `vertebra._oracle` and typed in `src/py/vertebra/_oracle.pyi` (R9).
- [ ] `SyncOracleDriver` and `AsyncOracleDriver` satisfy `SyncPipelineCapable` and `AsyncPipelineCapable`, holding a transaction or `SAVEPOINT _vtb_pipe_<n>` across `results()` and committing only on clean `__exit__`/`__aexit__` without emitting `RELEASE SAVEPOINT` (R9).
- [ ] Post-`results()` caller exceptions and mid-batch `ORA-xxxxx` errors roll back the pipeline transaction/savepoint, and the `oracledb` crate TTC pipeline tracking test passes (R9).

## Notes & Discoveries

- Oracle SQL does not have a `RELEASE SAVEPOINT` statement; nested pipeline savepoints are retained on clean inner exit and released when the enclosing transaction commits or rolls back.

[^ora-contracts]: `.agents/bundles/specs/native-oracle-driver/contracts.md` — Public `OraclePipelineOutcome` and delayed-commit pipeline contracts.
[^db-capabilities]: `src/py/vertebra/db/driver/capabilities.py::SyncPipeline` — `SyncPipeline`, `AsyncPipeline`, `SyncPipelineCapable`, and `AsyncPipelineCapable` protocols.
[^ora-driver-py]: `src/py/vertebra/db/adapters/oracle/driver.py::SyncOracleDriver` — `SyncOracleDriver` and `AsyncOracleDriver` implementation module.
[^sqlspec-stack]: `src/py/vertebra/extensions/sqlspec/adapters.py::BaseSyncDriver` — SQLSpec `execute_stack` caller hydration inside `driver.pipeline()` context.
[^crate-research]: `.agents/bundles/specs/native-data-platform/research/rust-driver-crates_20261103.md` — Primary-source analysis of `oracledb =26.0.0-beta.5` synchronous TTC execution.
