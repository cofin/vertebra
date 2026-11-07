---
type: Task
id: "native-postgres-driver:ch26:t03"
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: Complete native Arrow forwarding before public PostgreSQL activation
description: Complete native select_to_arrow, stream_arrow_batches, and load_from_arrow edge-case handling, threshold/row-count invariants, and lease/capsule lifetimes on permanent SyncPostgreSQLDriver and AsyncPostgreSQLDriver.
status: stable
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch26:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/arrow.py
  - .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
  - .agents/skills/pyo3-bridge/SKILL.md
files:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
tests:
  - src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
estimated_loc: 220
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
    title: PostgresArrowResult, max_result_bytes=256MiB, inline/SHM threshold, and ArrowCursor contracts
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public ArrowCursor wrapper, ARROW_SHM_THRESHOLD_ROWS, and IPC stream helpers
  - id: adr-004
    resource: .agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md
    title: Producer-owned ShmCapsule lifetime across optional database extensions
tags:
  - postgres
  - arrow
  - copy
  - shm
---

# Task t03: Complete Native Arrow Forwarding Before Public PostgreSQL Activation

## Objective & Scope Boundary

Complete edge-case handling, limit preflight validation, and lease/capsule ownership across `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` on `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver` and `src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver` with zero Python-row (`execute_with_description` / `from_rows` / `to_pylist`) fallback [^npd-contracts] [^arrow-py] [^adr-004]. Do NOT edit `embeddings.py`, `notify.py`, or `driver.py` in this task (migrated atomically in `ch27:t01`–`t02`).

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — `PostgresArrowResult`, `max_result_bytes = 268_435_456`, inline vs SHM threshold, and explicit `row_count` rules [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/_sync.py` — Thin synchronous Arrow forwarders from `ch25:t03`.
- `src/py/vertebra/db/adapters/postgres/_async.py` — Thin asynchronous Arrow forwarders from `ch25:t04`.
- `src/py/vertebra/db/arrow.py` — `ArrowCursor`, `ARROW_SHM_THRESHOLD_ROWS`, and IPC batch iteration helpers [^arrow-py].
- [adr-004-optional-driver-wheels-and-shm-ownership.md](../../../../../knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md) — Cross-extension `ShmCapsule` retention invariants [^adr-004].
- [SKILL.md](../../../../../../skills/pyo3-bridge/SKILL.md) — Zero-copy buffer lifetime discipline.

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/_sync.py` & `_async.py`:
  - `select_to_arrow(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, max_result_bytes: int = 268_435_456, **kwargs: Any) -> ArrowCursor` (and `async def` on `AsyncPostgreSQLDriver`):
    - Rejects `bool` or `max_result_bytes <= 0` with `ValueError` before acquiring a pool lease; compiles `(native_sql, native_params)` via `self._prepare_native_query(sql, parameters)`.
    - Calls native `client.select_to_arrow(native_sql, native_params, max_result_bytes=max_result_bytes)` returning `PostgresArrowResult(buffer, row_count, columns, used_shm)` (`used_shm=False` inline IPC bytes at/below `ARROW_SHM_THRESHOLD_ROWS`, `used_shm=True` `vertebra._vertebra.ArrowCursor` via `ShmCapsule` above threshold) and wraps it in public `vertebra.db.arrow.ArrowCursor(buffer, columns=res.columns, used_shm=res.used_shm, row_count=res.row_count)` so the returned cursor retains the backing buffer/capsule owner even after the transient socket lease returns to the pool.
  - `stream_arrow_batches(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, batch_size: int = 2048, **kwargs: Any)` (`Iterator[ArrowCursor]` on sync, `AsyncIterator[ArrowCursor]` on async):
    - Validates non-bool `batch_size >= 1` preflight; pins one native lease (or borrows ambient transaction lease) for the entire portal stream lifetime; yields SHM-backed `ArrowCursor` per batch; closes native stream and releases transient lease in `finally` on exhaustion, early `break` / `close()` / `aclose()`, or cancellation.
  - `load_from_arrow(self, table: str, source: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None, **kwargs: Any) -> int` (and `async def` on `AsyncPostgreSQLDriver`):
    - Validates `schema_name`, `table`, and optional `columns` identifiers preflight; streams serialized IPC batches through `iter_arrow_ipc_batches` / `aiter_arrow_ipc_batches` into native `client.copy_from_arrow(table=..., first_ipc=..., columns=..., schema_name=...)` followed by strictly sequential (`1` in-flight write at a time) `write_ipc(batch_ipc)` calls and `finish() -> int`.
    - Schema-only `0`-row Arrow source still executes native target validation and returns `0`; on producer/encoder error or cancellation, invokes `abort()` (shielded in async) and preserves the original producer exception if `abort()` or session reset also fails.

## Edge Cases & Error Semantics

- Native Arrow selection/streaming NEVER falls back to `execute_with_description`, `ArrowCursor.from_rows`, or `to_pylist()`.
- Empty `0`-row `select_to_arrow` preserves `columns` tuple and `len(cursor) == 0` (`cursor.row_count == 0`); inline IPC results with `> 0` rows report exact `len(cursor) == res.row_count` without materializing Python `rows`.
- Result exceeding `max_result_bytes` (`256 MiB` default) or a single cell exceeding the `8 MiB` native builder bound surfaces canonical `OperationalError` / `DataError` and releases/recovers the session cleanly.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` (direct-importing `SyncPostgreSQLDriver` from `_sync` and `AsyncPostgreSQLDriver` from `_async`).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| Inline vs SHM threshold, empty `0`-row schema, & capsule retention | `select_to_arrow` with `0` rows, `10` rows (`<= 256`), and `300` rows (`> 256`); close driver after obtaining `ArrowCursor` | `0` rows: `columns == ("id", "name")`, `len(cursor) == 0`, `used_shm is False`; `10` rows: `len(cursor) == 10`, `used_shm is False` (zero `execute_with_description` calls); `300` rows: `len(cursor) == 300`, `used_shm is True`; `memoryview(cursor)` remains readable after transient lease release and driver close. |
| Limit validation & `256 MiB` / `8 MiB` overflow errors | `select_to_arrow("SELECT 1", max_result_bytes=0)` or `max_result_bytes=-5` or `max_result_bytes=True`; simulate `> max_result_bytes` / `> 8 MiB` value | Invalid `max_result_bytes` raises `ValueError` with `0` pool acquisitions; overflow raises `OperationalError`/`DataError` and returns lease (`in_use == 0`). |
| `stream_arrow_batches` early exit & cancellation lease pin | Iterate `1` batch of a `3`-batch stream then `break` / `aclose()` / `task.cancel()` | Lease stays pinned (`in_use == 1`) while iterator is open, then native portal closes and lease returns (`in_use == 0`) immediately on `break` / `aclose()` / cancel. |
| `load_from_arrow` multi-batch backpressure, `0`-row import, & abort error preservation | (a) `0`-row schema-only Arrow table with `schema_name="analytics", columns=("id", "val")`; (b) `3`-batch iterator where batch `2` raises `RuntimeError("source boom")` and `copy.abort()` also raises `OperationalError` | (a) Validates target schema/columns and returns `0`; (b) enforces `1` in-flight native write at a time, calls `abort()`, releases/retires lease, and raises original `RuntimeError("source boom")`. |

## Verification

- Initial Red baseline: `uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_arrow.py -q`
- Final Green + Data/Legacy Regression + Lint/Typecheck gate:
  ```bash
  make dev
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_native_adapter_arrow.py src/py/tests/unit/db/test_postgres_native_data.py src/py/tests/unit/db/test_vector_wire_codecs.py -q
  uv run --no-sync pytest src/py/tests/unit/db/test_postgres_driver.py src/py/tests/unit/db/test_pg_dataplane_and_datafusion.py -q
  uv run --no-sync ruff check src/py/vertebra/db/adapters/postgres/_sync.py src/py/vertebra/db/adapters/postgres/_async.py src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
  uv run --no-sync mypy src/py/vertebra/db/adapters/postgres
  ```

## Acceptance Criteria

- [ ] `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` on `_sync.py` and `_async.py` enforce limit preflight, inline/SHM `ArrowCursor` `row_count` and `ShmCapsule` retention, pinned stream leases, `0`-row COPY validation, and original-exception preservation without Python-row fallbacks (R20, R24).
- [ ] CPU `pgcopy` / JSONB / vector helper suites and legacy `driver.py` tests remain untouched and green (R20, R24).
- [ ] Commit message follows `test(postgres): complete native Arrow forwarding contracts` once verified.

## Notes & Discoveries

- `embeddings.py` and `notify.py` keep their legacy SQL markers until `ch27:t01`–`t02` so the still-active legacy `SyncPostgreSQLDriver` (`psycopg` pyformat) remains coherent prior to atomic cutover [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresArrowResult, max_result_bytes=256MiB, inline/SHM threshold, and ArrowCursor contracts.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public ArrowCursor wrapper, ARROW_SHM_THRESHOLD_ROWS, and IPC stream helpers.
[^adr-004]: `.agents/bundles/knowledge/decisions/adr-004-optional-driver-wheels-and-shm-ownership.md` — Producer-owned ShmCapsule lifetime across optional database extensions.
