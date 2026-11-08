---
type: Task
id: "native-postgres-driver:ch20:t04"
chapter_id: ch20-arrow-copy-import-and-portals
title: Expose asynchronous select_to_arrow and cancellation-safe Arrow portal streams
description: Add AsyncPostgresClient.select_to_arrow and synchronous-factory AsyncPostgresClient.stream_arrow_batches returning AsyncPostgresArrowStream with pending-completion SHM and portal cancellation cleanup guards.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch20:t03"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch20-arrow-copy-import-and-portals/spec.md
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/rs/vertebra-postgres/src/cancel.rs
  - src/rs/vertebra-db/src/py_async.rs
  - src/py/vertebra/_postgres.pyi
files:
  - src/rs/vertebra-postgres/src/arrow.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_data.py
estimated_loc: 230
plan_revision: 9
plan_commit: b0de4dcb
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
    title: Async Arrow selection, portal cancellation, and retained SHM view lifetime contracts
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

Expose `AsyncPostgresClient.select_to_arrow` and synchronous-factory `AsyncPostgresClient.stream_arrow_batches` (`AsyncPostgresArrowStream`) in `src/rs/vertebra-postgres/src/{arrow,async_client}.rs` and `src/py/vertebra/_postgres.pyi` over `_vertebra_postgres_completion_drainer`, guaranteeing zero leaked `VTBARROW` SHM regions or live portals when `asyncio` tasks cancel during portal setup, blocked `Execute` fetches, or queued drainer publication [^pg-contracts] [^py-async-rs] [^pg-protocol-research].

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 20 cancellation-safe async Arrow selection and portal streaming contracts.
- `src/rs/vertebra-postgres/src/arrow.rs` — `execute_select_to_arrow`, `ArrowPortalStream`, `arrow_output_into_py_result`, and `shm_output_into_py_cursor` (`ch19:t04`, `ch20:t03`).
- `src/rs/vertebra-postgres/src/async_client.rs` — `AsyncPostgresClient`, `_vertebra_postgres_completion_drainer`, and async portal patterns (`ch07:t01`, `ch17:t03`).
- `src/rs/vertebra-postgres/src/cancel.rs` — Generation-fenced `CancelRequest` and bounded drain/retirement accessed via `crate::session::cancel` (`#[path = "cancel.rs"] pub mod cancel;` inside `session.rs` from `ch16:t03`; `lib.rs` leaves `cancel` unexported).
- `src/rs/vertebra-db/src/py_async.rs` — `DbCompletionDrainer` cancelled-future skip semantics (when `fut.cancelled()` is `true`, `produce` is not invoked, so `Drop` on captured Rust state must clean up!) [^py-async-rs].
- `src/py/vertebra/_postgres.pyi` — Stubs for `AsyncPostgresClient` and `AsyncPostgresArrowStream`.

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/arrow.rs`:
  - Add `PendingArrowBatchGuard` wrapping a produced `ArrowOutput::Shm` and shared `Arc<AsyncArrowPortalShared>` state inside the completion-queue `map_ok` closure:
    - When `map_ok` runs on the event loop thread (future was NOT cancelled), disarms the guard, transitions stream state from `Fetching` -> `Open`, and transfers ONLY the batch `Arc<ShmRegion>` into `vertebra._vertebra.ArrowCursor` via `shm_output_into_py_cursor` (never transferring the portal lease).
    - When the `asyncio.Future` is cancelled before `DbCompletionDrainer` runs `map_ok` (so `CompletedDbTask` drops `produce` without calling it), `PendingArrowBatchGuard::drop` drops the undelivered `ArrowOutput::Shm` immediately and spawns bounded portal close/cancel/retirement on `db_runtime_handle()` so the stream transitions to `Closed` and never returns a live portal to the pool.
- `src/rs/vertebra-postgres/src/async_client.rs` & `src/py/vertebra/_postgres.pyi`:
  - `AsyncPostgresClient.select_to_arrow(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, threshold_rows: int = 256, max_result_bytes: int = 268435456) -> Awaitable[PostgresArrowResult]`
  - `AsyncPostgresClient.stream_arrow_batches(self, sql: str, parameters: Sequence[PostgresValue | PgParameter] | None = None, *, batch_size: int = 65536, columns: Sequence[str] | None = None) -> AsyncPostgresArrowStream` (synchronous method returning `AsyncPostgresArrowStream` directly, NOT a coroutine; validates `batch_size > 0` before returning)
  - `AsyncPostgresArrowStream` (`#[pyclass(name = "AsyncPostgresArrowStream", module = "vertebra._postgres", frozen, immutable_type)]`):
    - Explicit atomic/mutex state machine: `Unopened` -> `Fetching` -> `Open` -> `Closing` -> `Closed` (holding `std::sync::Mutex` only across non-blocking state transitions, never across `.await`).
    - `__aiter__(self) -> AsyncPostgresArrowStream`
    - `__anext__(self) -> Awaitable[vertebra._vertebra.ArrowCursor]` (raises `InterfaceError` synchronously on overlapping `__anext__` while `Fetching` or `Closing`; raises `StopAsyncIteration` when `Closed` after normal exhaustion)
    - `aclose(self) -> Awaitable[None]` (idempotent before first `__anext__`, after partial consumption, or after exhaustion; cancels any in-flight `Fetching` step via protocol `CancelRequest` + bounded drain/retirement under `connect_timeout` deadline)
    - `__aenter__(self) -> Awaitable[AsyncPostgresArrowStream]` and `__aexit__(...) -> Awaitable[None]`.

## Edge Cases & Error Semantics

- Event loop responsiveness: blocked server `Execute` waits run entirely on Tokio (`db_runtime_handle()`), allowing concurrent `asyncio.sleep` heartbeats to tick without blocking the loop or GIL.
- Retained `vertebra._vertebra.ArrowCursor`, `memoryview`, and `pyarrow.Table` objects yielded from earlier batches remain valid and readable after `await stream.aclose()` or stream cancellation because `ShmCapsule` owns an independent `Arc<ShmRegion>` reference.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_data.py` (`test_async_arrow_*` suite)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Async `select_to_arrow`, loop heartbeat, and overlapping `__anext__` | Blocked peer fetch with concurrent `asyncio.sleep(0.005)` counter + second `stream.__anext__()` while first is pending | Heartbeat ticks continuously while fetch blocks; second `__anext__()` raises `InterfaceError`; inline & SHM `PostgresArrowResult` match sync parity. |
| `aclose()` lifecycle & retained batch view lifetime | Call `await stream.aclose()` (a) before first `__anext__`, (b) after 1st batch while retaining `mv = batch.buffer_view()` and `pa.ipc.open_stream(mv).read_all()`, and (c) twice after exhaustion | All `aclose()` paths succeed idempotently; retained `mv` and PyArrow table remain readable after `aclose()` and `del stream`; `max_size = 1` pool serves next query. |
| Task cancellation during creation/fetch AND while completed batch sits queued on drainer | (a) Cancel `task` while peer holds `Bind`/`Execute` barrier; (b) pause loop drainer until worker pushes completed batch, then `task.cancel()` before drainer callback runs | Both paths raise `CancelledError`, drop any undelivered `ShmRegion` (`0` leaked `/dev/shm` files), send portal `Close`/`ROLLBACK` or `CancelRequest` + socket retirement, and unblock next query on `max_size = 1` pool. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k async_arrow -q`
- Final Green + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k async_arrow -q
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_tests
  ```
- Planned local commit: `feat(postgres): preserve async Arrow stream ownership`

## Acceptance Criteria

- [ ] `AsyncPostgresClient.select_to_arrow` and synchronous-factory `AsyncPostgresClient.stream_arrow_batches` (`AsyncPostgresArrowStream`) pass `mypy.stubtest vertebra._postgres` and keep the `asyncio` loop unblocked during network waits (R16, R20).
- [ ] `Unopened` / `Fetching` / `Open` / `Closing` / `Closed` state guards reject overlapping `__anext__` with `InterfaceError` and keep `aclose()` idempotent across every state (R16, R20).
- [ ] Cancellation during portal open, in-flight fetch, or queued drainer publication drops undelivered `ShmRegion` mappings and closes/retires the portal session while already-yielded `ArrowCursor` / `memoryview` views remain valid after stream closure (R16, R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Async Arrow selection, portal cancellation, and retained SHM view lifetime contracts.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue drainer and cancelled-future publication guard.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL portal CancelRequest drain and retirement on mid-stream cancellation.
