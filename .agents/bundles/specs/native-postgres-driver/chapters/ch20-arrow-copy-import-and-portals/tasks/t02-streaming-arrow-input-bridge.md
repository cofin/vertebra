---
type: Task
id: "native-postgres-driver:ch20:t02"
chapter_id: ch20-arrow-copy-import-and-portals
title: Expose PyO3 COPY handles and stream Arrow IPC input buffers into native COPY
description: Expose SyncPostgresCopyOut, AsyncPostgresCopyOut, SyncPostgresCopyIn, AsyncPostgresCopyIn, copy_to, copy_from, copy_from_arrow, and write_arrow_ipc across sync_client.rs, async_client.rs, and _postgres.pyi.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch20:t01"
read_set:
  - .agents/bundles/specs/native-postgres-driver/chapters/ch20-arrow-copy-import-and-portals/spec.md
  - src/rs/vertebra-postgres/src/copy.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/vertebra/db/adapters/postgres/arrow.py
files:
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
tests:
  - src/py/tests/unit/db/test_postgres_native_data.py
estimated_loc: 240
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
  - id: pg-contracts
    resource: .agents/bundles/specs/native-postgres-driver/contracts.md
    title: Native PostgreSQL COPY, Arrow input, and free-threaded PyO3 bridge contracts
  - id: pg-protocol-research
    resource: .agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md
    title: PostgreSQL COPY TO CancelRequest drain and COPY FROM CopyFail abort semantics
  - id: py-async-rs
    resource: src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr
    title: Event-loop AsyncCompletionQueue bridge without worker GIL attachment
tags:
  - postgres
  - rust
  - pyo3
  - copy
  - arrow
---

# Task t02: Expose PyO3 COPY Handles and Stream Arrow IPC Input Buffers into Native COPY

## Objective & Scope Boundary

Expose PyO3 `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, and `AsyncPostgresCopyIn` plus `copy_to`, `copy_from`, and `copy_from_arrow` on `SyncPostgresClient` and `AsyncPostgresClient` in `src/rs/vertebra-postgres/src/{sync_client,async_client}.rs` and `src/py/vertebra/_postgres.pyi`, verifying end-to-end streaming with `vertebra.db.adapters.postgres.arrow` (`load_arrow_sync` / `load_arrow_async`) [^pg-contracts] [^pg-protocol-research] [^py-async-rs].

## Bounded Read Manifest

- [spec.md](../spec.md) — Chapter 20 PyO3 `COPY` handle, `16 MiB` IPC call cap, and caller-thread buffer handoff contracts.
- `src/rs/vertebra-postgres/src/copy.rs` — `CopyOut`, `CopyFromStream`, `SessionHandle::{copy_to, copy_from, copy_from_arrow}`, and `CopyFromStream::write_arrow_ipc` (`ch18:t02`–`t03`, `ch20:t01`).
- `src/rs/vertebra-postgres/src/sync_client.rs` — `SyncPostgresClient`, `py.detach` blocking bridge, and Python parameter/row extraction (`ch14:t03`–`t04`).
- `src/rs/vertebra-postgres/src/async_client.rs` — `AsyncPostgresClient` and `_vertebra_postgres_completion_drainer` task spawning (`ch07:t01`–`t03`).
- `src/py/vertebra/_postgres.pyi` — Public typing stub verified by `mypy.stubtest`.
- `src/py/vertebra/db/adapters/postgres/arrow.py` — `iter_arrow_ipc_batches`, `aiter_arrow_ipc_batches`, `load_arrow_sync`, and `load_arrow_async` (`ch20:t01`).

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs` & `src/py/vertebra/_postgres.pyi`:
  - `SyncPostgresClient.copy_to(self, sql: str) -> SyncPostgresCopyOut`
  - `SyncPostgresClient.copy_from(self, sql: str) -> SyncPostgresCopyIn`
  - `SyncPostgresClient.copy_from_arrow(self, table_name: str, first_batch_ipc: Buffer, *, columns: Sequence[str] | None = None) -> SyncPostgresCopyIn`
  - `SyncPostgresCopyOut` (`frozen, immutable_type`): `__iter__`, `__next__(self) -> tuple[bytes | None, ...]`, `close(self) -> None`, `__enter__`, `__exit__`.
  - `SyncPostgresCopyIn` (`frozen, immutable_type`): `write(self, rows: Sequence[Sequence[PostgresValue]], column_oids: Sequence[int]) -> None`, `write_arrow_ipc(self, buffer: Buffer) -> None`, `finish(self) -> int`, `close(self) -> None`, `__enter__` (commits via `close()` abort on unhandled `__exit__` exception or un-finished exit).
- `src/rs/vertebra-postgres/src/async_client.rs` & `src/py/vertebra/_postgres.pyi`:
  - `AsyncPostgresClient.copy_to(self, sql: str) -> Awaitable[AsyncPostgresCopyOut]`
  - `AsyncPostgresClient.copy_from(self, sql: str) -> Awaitable[AsyncPostgresCopyIn]`
  - `AsyncPostgresClient.copy_from_arrow(self, table_name: str, first_batch_ipc: Buffer, *, columns: Sequence[str] | None = None) -> Awaitable[AsyncPostgresCopyIn]`
  - `AsyncPostgresCopyOut` (`frozen, immutable_type`): `__aiter__`, `__anext__(self) -> Awaitable[tuple[bytes | None, ...]]`, `aclose(self) -> Awaitable[None]`, `__aenter__`, `__aexit__`.
  - `AsyncPostgresCopyIn` (`frozen, immutable_type`): `write(self, rows: Sequence[Sequence[PostgresValue]], column_oids: Sequence[int]) -> Awaitable[None]`, `write_arrow_ipc(self, buffer: Buffer) -> Awaitable[None]`, `finish(self) -> Awaitable[int]`, `aclose(self) -> Awaitable[None]`, `__aenter__`, `__aexit__`.
- Buffer & concurrency invariants:
  - Contiguous read-only `PyBuffer` bytes are validated (`len <= 16 MiB`, raising `DataError` before copying if exceeded) and copied into owned Rust `Bytes` while attached on the caller/loop thread; Tokio workers never hold `PyBuffer`, Python callbacks, or foreign C-stream release pointers.
  - Overlapping `next`/`write`/`write_arrow_ipc`/`finish` operations on a single `COPY` handle raise `InterfaceError` immediately before emitting wire frames.
  - Cancelled `copy_to`, `copy_from`, `copy_from_arrow`, `__anext__`, `write`, or `write_arrow_ipc` futures trigger bounded `CancelRequest` (`CopyOut`) or `CopyFail` (`CopyFromStream`) cleanup on `db_runtime_handle()` and never return an unrecovered lease to the pool.

## Edge Cases & Error Semantics

- Schema-only empty Arrow input (`0` rows): `load_arrow_sync` / `load_arrow_async` passes schema-only IPC to `copy_from_arrow`, validates target table/columns, opens and immediately `finish()`es a `0`-row binary `COPY`, and returns `0`.
- Producer error after batch `1` in a multi-batch `RecordBatchReader` / `__arrow_c_stream__`: `finally` calls `close()` / `aclose()` (`CopyFail` + rollback) and closes the input stream, leaving `0` committed rows.
- Missing `pyarrow` raises `MissingDependencyError` / `ImportError` before `copy_from_arrow` touches the network.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_data.py` (`test_load_arrow_*` and `test_native_copy_*` suites)
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `Table`, `RecordBatch`, `RecordBatchReader`, `__arrow_c_stream__`, and `ArrowCursor` with `to_pylist`/`to_pydict` raising | Monkeypatch `pa.Table.to_pylist` / `to_pydict` and `RecordBatch.to_pylist` / `to_pydict` to raise `AssertionError`; stream 3 batches into `load_arrow_sync` and `load_arrow_async` | All 5 input shapes succeed, returning exact server row count without invoking Python row/dict conversion. |
| Second-batch streaming barrier | Custom `__arrow_c_stream__` / `RecordBatchReader` whose batch-2 pull waits on an event set by the scripted PostgreSQL peer only after batch-1 `CopyData` arrives on the wire | Batch 1 transmits over TCP before batch 2 is read from the producer; reader `.close()` is observed on completion. |
| Empty schema-only input, `columns` subset, malformed/`> 16 MiB` IPC, and missing `pyarrow` | (a) `0`-row `pa.Table` with `columns=["b", "a"]`; (b) `b"not-arrow"` and `17 MiB` buffer to `write_arrow_ipc`; (c) blocked `pyarrow` import | (a) Quotes `("b", "a")`, finishes `0`-row `COPY`, returns `0`; (b) raises `DataError` before allocation/commit; (c) fails before any socket traffic. |
| Producer exception after batch 1 & `copy_to` early-close recovery on `max_size = 1` pool | (a) Reader raises `RuntimeError("boom")` on batch 2; (b) consumer breaks after row 1 of `copy_to` (sync `with` and async `async with` + task cancel) | (a) Peer receives `CopyFail` + `ROLLBACK`, `RuntimeError` propagates, next pooled query succeeds; (b) peer receives `CancelRequest`, drains/replaces session, and follow-up `client.query("SELECT 1")` returns `1`. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k load_arrow -q`
- Final Green + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k "load_arrow or native_copy" -q
  cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests
  ```
- Planned local commit: `feat(postgres): stream Arrow input buffers to COPY`

## Acceptance Criteria

- [ ] `SyncPostgresClient` and `AsyncPostgresClient` expose `copy_to`, `copy_from`, and `copy_from_arrow` with `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn`, and `AsyncPostgresCopyIn` passing `mypy.stubtest vertebra._postgres` (R19, R20).
- [ ] `load_arrow_sync` and `load_arrow_async` stream `Table`, `RecordBatch`, `RecordBatchReader`, `__arrow_c_stream__`, and `ArrowCursor` inputs one bounded IPC batch at a time without Python row/dict conversion, transmitting batch 1 before pulling batch 2 (R20).
- [ ] Oversize/malformed IPC, mid-stream source errors, server errors, early closes, and `asyncio` cancellations clean up via `CopyFail` / `CancelRequest` and close input readers without leaking pooled sessions or committing partial batches (R19, R20).

## Notes & Discoveries

None.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL COPY, Arrow input, and free-threaded PyO3 bridge contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL COPY TO CancelRequest drain and COPY FROM CopyFail abort semantics.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue bridge without worker GIL attachment.
