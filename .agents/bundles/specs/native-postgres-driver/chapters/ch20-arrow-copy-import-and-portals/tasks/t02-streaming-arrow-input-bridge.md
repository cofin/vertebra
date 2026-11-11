---
type: Task
id: "native-postgres-driver:ch20:t02"
chapter_id: ch20-arrow-copy-import-and-portals
title: Expose PyO3 COPY handles and stream Arrow IPC input buffers into native COPY
description: Expose SyncPostgresCopyOut, AsyncPostgresCopyOut, SyncPostgresCopyIn, AsyncPostgresCopyIn, copy_to, copy_from, copy_from_arrow, and write_arrow_ipc across sync_client.rs, async_client.rs, and _postgres.pyi.
status: stable
state: completed
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
  - src/rs/vertebra-postgres/src/lib.rs
  - src/rs/vertebra-postgres/src/sync_client.rs
  - src/rs/vertebra-postgres/src/async_client.rs
  - src/py/vertebra/_postgres.pyi
  - src/py/vertebra/db/adapters/postgres/arrow.py
tests:
  - src/py/tests/unit/db/test_postgres_native_data.py
estimated_loc: 240
plan_revision: 6
plan_commit: 8e9b324e
state_revision: 1
claimed_by: null
claimed_at: null
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-10T17:57:27Z"
last_verified_commit: c5b56452
verification_evidence: "make dev, ruff, mypy, mypy.stubtest vertebra._postgres, pytest test_postgres_native_data.py (57 passed), cargo test postgres_arrow_import_tests (10 passed), and cargo clippy exited 0"
created_at: "2026-11-05T18:25:07Z"
updated_at: "2026-11-10T17:57:27Z"
commit: c5b56452
verified:
  - by: agent:plan-generator
    at: "2026-11-10T14:45:00Z"
    commit: 8e9b324e
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
- `src/rs/vertebra-postgres/src/copy.rs` — `CopyOut`, `CopyFromStream`, `SessionHandle::{copy_to_with_generation_as, copy_from_with_generation_as, copy_from_arrow_with_generation_as}`, and `CopyFromStream::write_arrow_ipc` (`ch18:t02`–`t03`, `ch20:t01`).
- `src/rs/vertebra-postgres/src/sync_client.rs` — `SyncPostgresClient`, `py.detach` blocking bridge, ambient `TransactionOwner` wiring, and Python parameter/row extraction (`ch14:t03`–`t04`).
- `src/rs/vertebra-postgres/src/async_client.rs` — `AsyncPostgresClient`, `_vertebra_postgres_completion_drainer` task spawning, and `OperationGeneration` cancel guards (`ch07:t01`–`t03`, `ch16:t03`).
- `src/py/vertebra/_postgres.pyi` — Public typing stub verified by `mypy.stubtest` (importing `Buffer` from `collections.abc` and keeping `TYPE_CHECKING` imports clean for `ruff` and `mypy`).
- `src/py/vertebra/db/adapters/postgres/arrow.py` — `iter_arrow_ipc_batches`, `aiter_arrow_ipc_batches`, `load_arrow_sync(..., schema_name=...)`, and `load_arrow_async(..., schema_name=...)` (`ch20:t01`).

## Symbol & Signature Contracts

- `src/rs/vertebra-postgres/src/sync_client.rs` & `src/py/vertebra/_postgres.pyi` (matching `contracts.md` and `ch26:t03` 1:1):
  - `SyncPostgresClient.copy_to(self, sql: str) -> SyncPostgresCopyOut`
  - `SyncPostgresClient.copy_from(self, sql: str) -> SyncPostgresCopyIn`
  - `SyncPostgresClient.copy_from_arrow(self, table_name: str, first_batch_ipc: Buffer, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> SyncPostgresCopyIn`
  - `SyncPostgresCopyOut` (`frozen, immutable_type`): `__iter__`, `__next__(self) -> tuple[bytes | None, ...]`, `close(self) -> None`, `__enter__`, `__exit__`.
  - `SyncPostgresCopyIn` (`frozen, immutable_type`): `write(self, rows: Sequence[Sequence[PostgresValue]], column_oids: Sequence[int]) -> None`, `write_arrow_ipc(self, batch_ipc: Buffer) -> None`, `finish(self) -> int`, `close(self) -> None`, `__enter__`, `__exit__` (aborts via `close()` on unhandled `__exit__` exception or unfinished exit).
- `src/rs/vertebra-postgres/src/async_client.rs` & `src/py/vertebra/_postgres.pyi` (matching `contracts.md` and `ch26:t03` 1:1):
  - `AsyncPostgresClient.copy_to(self, sql: str) -> Awaitable[AsyncPostgresCopyOut]`
  - `AsyncPostgresClient.copy_from(self, sql: str) -> Awaitable[AsyncPostgresCopyIn]`
  - `AsyncPostgresClient.copy_from_arrow(self, table_name: str, first_batch_ipc: Buffer, *, columns: Sequence[str] | None = None, schema_name: str | None = None) -> Awaitable[AsyncPostgresCopyIn]`
  - `AsyncPostgresCopyOut` (`frozen, immutable_type`): `__aiter__`, `__anext__(self) -> Awaitable[tuple[bytes | None, ...]]`, `aclose(self) -> Awaitable[None]`, `__aenter__`, `__aexit__`.
  - `AsyncPostgresCopyIn` (`frozen, immutable_type`): `write(self, rows: Sequence[Sequence[PostgresValue]], column_oids: Sequence[int]) -> Awaitable[None]`, `write_arrow_ipc(self, batch_ipc: Buffer) -> Awaitable[None]`, `finish(self) -> Awaitable[int]`, `aclose(self) -> Awaitable[None]`, `__aenter__`, `__aexit__`.
- Buffer & concurrency invariants:
  - `_postgres.pyi` imports `Buffer` from `collections.abc` cleanly under `ruff` and `mypy`.
  - Contiguous read-only `PyBuffer` bytes (`first_batch_ipc`, `batch_ipc`) are validated (`len <= 16 MiB`, raising `DataError` before copying if exceeded) and copied into owned Rust `Bytes`/`Vec<u8>` while attached on the caller/loop thread; Tokio workers never hold `PyBuffer`, Python callbacks, or foreign C-stream release pointers.
  - Both `SyncPostgresClient.copy_from_arrow` and `AsyncPostgresClient.copy_from_arrow` route through `SessionHandle::copy_from_arrow_with_generation_as(op_gen, table_name, schema_name, first_batch_ipc, columns, owner)`, joining any ambient client `TransactionOwner` and fencing async task cancellation across both `pg_catalog` resolution and `CopySessionLease` checkout/`start_copy_in`.
  - Overlapping `next`/`write`/`write_arrow_ipc`/`finish` operations on a single `COPY` handle raise `InterfaceError` immediately before emitting wire frames.
  - Cancelled `copy_to`, `copy_from`, `copy_from_arrow`, `__anext__`, `write`, `write_arrow_ipc`, or `finish` futures trigger bounded `CancelRequest` (`CopyOut` / `pg_catalog` preflight) or `CopyFail` (`CopyFromStream`) cleanup on `db_runtime_handle()` and never return an unrecovered lease to the pool.

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
| Empty schema-only input, `columns` + `schema_name` forwarding, malformed/`> 16 MiB` IPC, and missing `pyarrow` | (a) `0`-row `pa.Table` with `columns=["b", "a"]` and `schema_name="analytics"` on `table_name="odd.table"` (plus ambient `client.transaction()` join); (b) `b"not-arrow"` and `17 MiB` `batch_ipc` to `write_arrow_ipc`; (c) blocked `pyarrow` import | (a) Quotes `COPY "analytics"."odd.table" ("b", "a")`, emits single `PGCOPY` header + `-1i16` trailer, returns `0` (and participates in ambient transaction); (b) raises `DataError` before allocation/commit; (c) fails before any socket traffic. |
| Producer exception after batch 1 & `copy_to`/`copy_from_arrow` cancel recovery on `max_size = 1` pool | (a) Reader raises `RuntimeError("boom")` on batch 2; (b) consumer breaks after row 1 of `copy_to` or cancels `copy_from_arrow(..., schema_name="analytics")` mid-preflight / mid-`start_copy_in` | (a) Peer receives `CopyFail` + `ROLLBACK`, `RuntimeError` propagates, next pooled query succeeds; (b) peer receives `CancelRequest` / `CopyFail`, drains/replaces session, and follow-up `client.query("SELECT 1")` returns `1`. |

## Verification

- Initial Red baseline: `uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k load_arrow -q`
- Final Green + lint + type-check + stubtest + Rust gate:
  ```bash
  make dev
  uv run --no-sync ruff check src/py/vertebra/_postgres.pyi src/py/vertebra/db/adapters/postgres/arrow.py src/py/tests/unit/db/test_postgres_native_data.py
  uv run --no-sync mypy src/py/vertebra/_postgres.pyi src/py/vertebra/db/adapters/postgres/arrow.py src/py/tests/unit/db/test_postgres_native_data.py
  uv run --no-sync python -m mypy.stubtest vertebra._postgres
  uv run --no-sync python -m pytest src/py/tests/unit/db/test_postgres_native_data.py -k "load_arrow or native_copy" -q
  PYTHONPATH="$(pwd)/src/py" cargo test --manifest-path src/rs/Cargo.toml -p vertebra-postgres --test postgres_arrow_import_tests
  ```
- Planned local commit: `feat(postgres): stream Arrow input buffers to COPY`

## Acceptance Criteria

- [x] `SyncPostgresClient` and `AsyncPostgresClient` expose `copy_to`, `copy_from`, and `copy_from_arrow(..., *, columns=None, schema_name=None)` with `SyncPostgresCopyOut`, `AsyncPostgresCopyOut`, `SyncPostgresCopyIn` (`write_arrow_ipc(self, batch_ipc: Buffer)`), and `AsyncPostgresCopyIn` (`write_arrow_ipc(self, batch_ipc: Buffer)`) passing `ruff`, `mypy`, and `mypy.stubtest vertebra._postgres` (R19, R20).
- [x] `load_arrow_sync` and `load_arrow_async` forward `columns` and `schema_name`, join ambient transactions, and stream `Table`, `RecordBatch`, `RecordBatchReader`, `__arrow_c_stream__`, and `ArrowCursor` inputs one bounded IPC batch at a time with a single `PGCOPY` header and zero Python row/dict conversion, transmitting batch 1 before pulling batch 2 (R20).
- [x] Oversize/malformed IPC, mid-stream source errors, server errors, early closes, and `asyncio` cancellations clean up via `CopyFail` / `CancelRequest` and close input readers without leaking pooled sessions or committing partial batches (R19, R20).

## Notes & Discoveries

`extract_ipc_buffer_bytes` uses `PyBytes` fast-path inspection and `PyObject_GetBuffer(..., PyBUF_CONTIG_RO)` with a stack-local RAII `PyBuffer_Release` guard so buffer mutation under free-threaded Python `3.14t`/`3.15t` cannot resize backing storage while checking the `16 MiB` call limit and copying into owned `Vec<u8>`.

[^pg-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — Native PostgreSQL COPY, Arrow input, and free-threaded PyO3 bridge contracts.
[^pg-protocol-research]: `.agents/bundles/specs/native-postgres-driver/research/protocol-and-execution_20261105.md` — PostgreSQL COPY TO CancelRequest drain and COPY FROM CopyFail abort semantics.
[^py-async-rs]: `src/rs/vertebra-db/src/py_async.rs::spawn_rust_db_task_with_attr` — Event-loop AsyncCompletionQueue bridge without worker GIL attachment.
