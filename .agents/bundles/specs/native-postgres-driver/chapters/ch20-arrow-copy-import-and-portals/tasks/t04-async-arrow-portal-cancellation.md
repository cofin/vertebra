---
type: Task
id: "native-postgres-driver:ch20:t04"
chapter_id: ch20-arrow-copy-import-and-portals
title: Expose asynchronous select_to_arrow and cancellation-safe Arrow portal streams
description: Add AsyncPostgresClient.select_to_arrow and synchronous-factory AsyncPostgresClient.stream_arrow_batches returning AsyncPostgresArrowStream with pending-completion SHM and portal cancellation cleanup guards.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch20:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch20-arrow-copy-import-and-portals/spec.md
  - src/rs/vertebra-postgres/src/portal.rs
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
files:
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/portal.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_data.py
estimated_loc: 235
plan_revision: 6
plan_commit: c8722c76
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T19:25:26Z"
last_verified_commit: c8722c76
verification_evidence: "make lint-all, make dev, cargo test postgres_arrow_import_tests (10 passed) & postgres_arrow_tests (17 passed), mypy.stubtest vertebra._postgres, and pytest test_postgres_native_data.py (129 passed) exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T19:25:26Z"
commit: 83ebe4e1
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: c8722c76
sources:
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Async Arrow selection, parameter_oids/timeout, portal cancellation, and retained SHM view lifetime contracts
  - id: portal-rs
    resource: src/rs/vertebra-postgres/src/portal.rs::ArrowPortalStream
    title: Generation-fenced execute_select_to_arrow, ArrowPortalStream single-scope lifecycle, and mid-Execute CancelRequest retirement
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Event-loop AsyncCompletionQueue drainer and cancelled-future publication guard
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL portal CancelRequest drain and retirement on mid-stream cancellation
tags:
  - postgres
  - rust
  - pyo3
  - arrow
  - async
  - cancellation
---

# Task t04: Expose Asynchronous select_to_arrow and Cancellation-Safe Arrow Portal Streams

## Objective & Scope Boundary

Expose `AsyncPostgresClient.select_to_arrow` and synchronous-factory `AsyncPostgresClient.stream_arrow_batches` (`AsyncPostgresArrowStream`) in `src/rs/vertebra-postgres/src/{portal,async_client}.rs` and `src/py/vertebra/_postgres.pyi` over `_vertebra_postgres_completion_drainer`, guaranteeing zero leaked `VTBARROW` SHM regions or live portals when `asyncio` tasks cancel during portal setup, blocked or `8 MiB`-paused `Execute` fetches, or queued drainer publication [^pg-contracts] [^portal-rs] [^py-async-rs] [^pg-protocol-research].

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 20 cancellation-safe async Arrow selection and single-scope portal streaming contracts.
- `src/rs/vertebra-postgres/src/portal.rs` — `execute_select_to_arrow`, `ArrowPortalStream`, `Portal::type_catalog`, `SessionHandle::open_portal[_as]_with_generation`, and generation-fenced `CancelRequest` + `connect_timeout` bounded drain/socket retirement (`ch16:t03`, `ch20:t03`) [^portal-rs].
- `src/rs/vertebra-postgres/src/arrow.rs` — Existing `arrow_output_into_py_result` and `shm_output_into_py_cursor` (`ch19:t04`).
- `src/rs/vertebra-postgres/src/async_client.rs` — `AsyncPostgresClient`, `_vertebra_postgres_completion_drainer`, `OperationGeneration` cancel guards, and async portal patterns (`ch07:t01`, `ch17:t03`).
- `src/rs/vertebra-db/src/py_async.rs` — `DbCompletionDrainer` cancelled-future skip semantics (when `fut.cancelled()` is `true`, `produce` is dropped without being invoked, so `Drop` on captured Rust state cleans up deterministically) [^py-async-rs].
- `src/py/vertebra/_postgres.pyi` — Stubs for `AsyncPostgresClient` and `AsyncPostgresArrowStream`.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/portal.rs`:
  - Wire generation-fenced async entry points on `ArrowPortalStream` (`open_with_generation` delegating strictly to `SessionHandle::open_portal[_as]_with_generation` with zero duplicate outer `BEGIN`, `next_batch_shm_with_generation(op_gen)`, and `close_with_generation(op_gen)`) so cancelling an active `Bind`/`Execute` or calling `aclose()` while paused mid-`Execute` at the `8 MiB` byte cap issues `CancelRequest` + `connect_timeout` bounded drain/socket retirement [^portal-rs].
  - Add `PendingArrowBatchGuard` wrapping a produced `ArrowOutput::Shm` and shared `Arc<AsyncArrowPortalShared>` state inside the completion-queue `map_ok` closure:
    - When `map_ok` runs on the event loop thread (future was NOT cancelled), disarms the guard, transitions stream state from `Fetching` -> `Open`, and transfers ONLY the batch `Arc<ShmRegion>` into `vertebra._vertebra.ArrowCursor` via `crate::arrow::shm_output_into_py_cursor` (never transferring the portal lease).
    - When the `asyncio.Future` is cancelled before `DbCompletionDrainer` runs `map_ok` (so `CompletedDbTask` drops `produce` without calling it), `PendingArrowBatchGuard::drop` drops the undelivered `ArrowOutput::Shm` immediately and spawns bounded generation-fenced portal close/cancel/retirement on `db_runtime_handle()` so the stream transitions to `Closed` and never returns a live portal to the pool.
- `src/rs/vertebra-postgres/src/async_client.rs` & `src/py/vertebra/_postgres.pyi`:
  - `AsyncPostgresClient.select_to_arrow(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, parameter_oids: Sequence[int] | None = None, timeout: float | None = None, threshold_rows: int = 256, max_result_bytes: int = 268435456) -> Awaitable[PostgresArrowResult]` (delegates to `portal::execute_select_to_arrow` under a reserved `OperationGeneration`, ambient `TransactionOwner`, and `CommandBudget`, converting via `arrow::arrow_output_into_py_result`)
  - `AsyncPostgresClient.stream_arrow_batches(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, parameter_oids: Sequence[int] | None = None, batch_size: int = 65536, columns: Sequence[str] | None = None) -> AsyncPostgresArrowStream` (synchronous method returning `AsyncPostgresArrowStream` directly, NOT a coroutine; validates non-`bool` `batch_size > 0` and `parameter_oids` before returning)
  - `AsyncPostgresArrowStream` (`#[pyclass(name = "AsyncPostgresArrowStream", module = "vertebra._postgres", frozen, immutable_type)]`):
    - Explicit atomic/mutex state machine: `Unopened` -> `Fetching` -> `Open` -> `Closing` -> `Closed` (holding `std::sync::Mutex` only across non-blocking state transitions, never across `.await`).
    - `__aiter__(self) -> AsyncPostgresArrowStream`
    - `__anext__(self) -> Awaitable[vertebra._vertebra.ArrowCursor]` (reserves an `OperationGeneration` per open/fetch step; raises `InterfaceError` synchronously on overlapping `__anext__` while `Fetching` or `Closing`; raises `StopAsyncIteration` when `Closed` after normal exhaustion)
    - `aclose(self) -> Awaitable[None]` (idempotent before first `__anext__`, while paused mid-`Execute` at the `8 MiB` byte cap, after partial consumption, or after exhaustion; cancels any in-flight `Fetching` step or mid-`Execute` pause via generation-fenced `CancelRequest` + bounded drain/socket retirement under `connect_timeout`)
    - `__aenter__(self) -> Awaitable[AsyncPostgresArrowStream]` and `__aexit__(...) -> Awaitable[None]`.

## Edge Cases & Error Semantics

- Event loop responsiveness: blocked server `Execute` waits run entirely on Tokio (`db_runtime_handle()`), allowing concurrent `asyncio.sleep` heartbeats to tick without blocking the loop or GIL.
- Retained `vertebra._vertebra.ArrowCursor`, `memoryview`, and `pyarrow.Table` objects yielded from earlier batches remain valid and readable after `await stream.aclose()` or stream cancellation because `ShmCapsule` owns an independent `Arc<ShmRegion>` reference.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_data.py` (`test_async_arrow_*` suite)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Async `select_to_arrow` cold OIDs / `parameter_oids` / `timeout`, loop heartbeat, and overlapping `__anext__` | Cold custom OID + `INSERT ... RETURNING` query (`parameter_oids`, `timeout`), blocked peer fetch with concurrent `asyncio.sleep(0.005)` counter, and second `stream.__anext__()` while first is pending | Volatile SQL executes once; heartbeat ticks continuously while fetch blocks; second `__anext__()` raises `InterfaceError`; inline & SHM `PostgresArrowResult` match sync parity. |
| `aclose()` lifecycle (including mid-`Execute` `8 MiB` pause) & retained batch view lifetime | Call `await stream.aclose()` (a) before first `__anext__`, (b) after 1st `8 MiB`-capped batch while paused mid-`Execute` and retaining `mv = batch.buffer_view()` + `pa.ipc.open_stream(mv).read_all()`, and (c) twice after exhaustion | All `aclose()` paths succeed idempotently (fencing `CancelRequest` + bounded drain/retirement when mid-`Execute` with a single `BEGIN`/`ROLLBACK` scope); retained `mv` and PyArrow table remain readable after `aclose()` and `del stream`; `max_size = 1` pool serves next query. |
| Task cancellation during creation/fetch AND while completed batch sits queued on drainer | (a) Cancel `task` while peer holds `Bind`/`Execute` barrier; (b) pause loop drainer until worker pushes completed batch, then `task.cancel()` before drainer callback runs | Both paths raise `CancelledError`, drop any undelivered `ShmRegion` (`0` leaked `/dev/shm` files), send portal `Close`/`ROLLBACK` or generation-fenced `CancelRequest` + socket retirement, and unblock next query on `max_size = 1` pool. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k async_arrow -q`
- Final Green + lint + type-check + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_data.py
  uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/tests/unit/db/test_postgres_native_data.py
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k async_arrow -q
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests
  ```
- Planned local commit: `feat(postgres): preserve async Arrow stream ownership`

## Acceptance Criteria

- [x] `AsyncPostgresClient.select_to_arrow(..., *, parameter_oids=None, timeout=None, ...)` and synchronous-factory `AsyncPostgresClient.stream_arrow_batches(..., *, parameter_oids=None, batch_size=65536, columns=None)` pass `ruff`, `mypy`, and `mypy.stubtest vertebra._postgres` while keeping the `asyncio` loop unblocked during network waits (R16, R20).
- [x] `Unopened` / `Fetching` / `Open` / `Closing` / `Closed` state guards reject overlapping `__anext__` with `InterfaceError`, delegate strictly to `Portal`'s single transaction/savepoint scope, and keep `aclose()` idempotent across every state including mid-`Execute` `8 MiB` pauses (R16, R20).
- [x] Generation-fenced cancellation during portal open, in-flight fetch, mid-`Execute` pause, or queued drainer publication drops undelivered `ShmRegion` mappings and closes/retires the portal session while already-yielded `ArrowCursor` / `memoryview` views remain valid after stream closure (R16, R20).

## Notes & Discoveries

`PendingArrowBatchGuard` and `cancel_fetch` pair `SessionHandle::begin_portal_drop_cleanup` with `finish_portal_drop_cleanup` so immediate `await client.close()` after task cancellation waits for background portal rollback/retirement and returns clean leases to `max_size = 1` pools.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Async Arrow selection, `parameter_oids`/`timeout`, portal cancellation, and retained SHM view lifetime contracts.
[^portal-rs]: `src/rs/vertebra-postgres/src/portal.rs::ArrowPortalStream` — Generation-fenced `execute_select_to_arrow`, `ArrowPortalStream` single-scope lifecycle, and mid-`Execute` `CancelRequest` retirement.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue drainer and cancelled-future publication guard.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL portal CancelRequest drain and retirement on mid-stream cancellation.
