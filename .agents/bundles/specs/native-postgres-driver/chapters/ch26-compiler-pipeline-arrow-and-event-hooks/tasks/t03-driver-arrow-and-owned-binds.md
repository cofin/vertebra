---
type: Task
id: "native-postgres-driver:ch26:t03"
chapter_id: ch26-compiler-pipeline-arrow-and-event-hooks
title: Complete native Arrow forwarding before public PostgreSQL activation
description: Enforce exact SyncDatabaseDriver and AsyncDatabaseDriver Liskov Arrow signatures, threshold_rows/max_result_bytes forwarding, ArrowCursor batch wrapping, and postgres/arrow.py load_from_arrow delegation on permanent PostgreSQL drivers.
status: draft
state: pending
priority: P1
verification_strategy: behavior_tdd
depends_on:
  - "native-postgres-driver:ch26:t02"
read_set:
  - .agents/bundles/specs/native-postgres-driver/contracts.md
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
  - src/py/vertebra/db/adapters/postgres/arrow.py
  - src/py/vertebra/db/driver/_sync.py
  - src/py/vertebra/db/arrow.py
files:
  - src/py/vertebra/db/adapters/postgres/_sync.py
  - src/py/vertebra/db/adapters/postgres/_async.py
tests:
  - src/py/tests/unit/db/test_postgres_native_adapter_arrow.py
estimated_loc: 220
plan_revision: 7
plan_commit: c8722c76
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
    title: PostgresArrowResult, threshold_rows and max_result_bytes forwarding, stream_arrow_batches, and copy_from_arrow contracts
  - id: sync-base-py
    resource: src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver
    title: Canonical SyncDatabaseDriver Liskov signatures for select_to_arrow, stream_arrow_batches, and load_from_arrow
  - id: async-base-py
    resource: src/py/vertebra/db/driver/_async.py::AsyncDatabaseDriver
    title: Canonical AsyncDatabaseDriver Liskov signatures for select_to_arrow, stream_arrow_batches, and load_from_arrow
  - id: arrow-py
    resource: src/py/vertebra/db/arrow.py::ArrowCursor
    title: Public vertebra.db.arrow.ArrowCursor wrapper and ARROW_SHM_THRESHOLD_ROWS constant
tags:
  - postgres
  - arrow
  - copy
  - shm
---

# Task t03: Complete Native Arrow Forwarding Before Public PostgreSQL Activation

## Objective & Scope Boundary

Complete Liskov-exact signature alignment, preflight limit validation, `ArrowCursor` batch wrapping, and lease/capsule/writer lifecycle handling across `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` on `src/py/vertebra/db/adapters/postgres/_sync.py::SyncPostgreSQLDriver` and `src/py/vertebra/db/adapters/postgres/_async.py::AsyncPostgreSQLDriver`, delegating `load_from_arrow` directly to `src/py/vertebra/db/adapters/postgres/arrow.py` (`load_arrow_sync` / `load_arrow_async`) with zero Python-row (`execute_with_description` / `from_rows` / `to_pylist`) fallback [^npd-contracts] [^sync-base-py] [^async-base-py] [^arrow-py]. Leave `embeddings.py` and `driver.py` untouched until atomic activation in `ch27:t01` (`notify.py` was already migrated in `ch21:t03`).

## Bounded Read Manifest

- [spec.md](../spec.md) & [contracts.md](../../../contracts.md) — `PostgresArrowResult`, `threshold_rows` + `max_result_bytes = 268_435_456`, portal stream wrapping, and `copy_from_arrow` / `write_arrow_ipc` / `finish` / `close` / `aclose` contracts [^npd-contracts].
- `src/py/vertebra/db/adapters/postgres/_sync.py` — Synchronous Arrow forwarders from `ch25:t03`.
- `src/py/vertebra/db/adapters/postgres/_async.py` — Asynchronous Arrow forwarders from `ch25:t04`.
- `src/py/vertebra/db/adapters/postgres/arrow.py` — `load_arrow_sync` and `load_arrow_async` Arrow IPC batch import helpers implemented in `ch20`.
- `src/py/vertebra/db/driver/_sync.py` — Canonical `SyncDatabaseDriver` (`and _async.py::AsyncDatabaseDriver`) `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow(table_name, data, ...)` signatures [^sync-base-py] [^async-base-py].
- `src/py/vertebra/db/arrow.py` — `ArrowCursor` and `ARROW_SHM_THRESHOLD_ROWS` [^arrow-py].

## Symbol & Signature Contracts

- `src/py/vertebra/db/adapters/postgres/_sync.py` & `_async.py` (signatures matching `SyncDatabaseDriver` and `AsyncDatabaseDriver` 1:1 for `mypy` Liskov compliance):
  - `select_to_arrow(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, threshold_rows: int = ARROW_SHM_THRESHOLD_ROWS, max_result_bytes: int = 268_435_456, **kwargs: Any) -> ArrowCursor` (and `async def` on `AsyncPostgreSQLDriver`):
    - Rejects `bool`, `threshold_rows < 0`, or `max_result_bytes <= 0` with `ValueError` before acquiring a pool lease; compiles `(native_sql, native_params)` via `self._prepare_native_query(sql, parameters)`.
    - Extracts optional `timeout = kwargs.get("timeout")` and calls native `client.select_to_arrow(native_sql, native_params, timeout=timeout, threshold_rows=threshold_rows, max_result_bytes=max_result_bytes)` returning `PostgresArrowResult(buffer, row_count, columns, used_shm)`, wrapping it in `vertebra.db.arrow.ArrowCursor(res.buffer, columns=res.columns, used_shm=res.used_shm, row_count=res.row_count)` so the returned cursor retains the backing inline bytes or `ShmCapsule` even after the transient socket lease returns to the pool and the driver closes.
  - `stream_arrow_batches(self, sql: str, parameters: Sequence[Any] | Mapping[str, Any] | None = None, *, batch_size: int = 65_536, columns: Sequence[str] | None = None, **kwargs: Any)` (`Iterator[ArrowCursor]` on sync, `AsyncIterator[ArrowCursor]` on async):
    - Rejects `bool` or `batch_size < 1` with `ValueError` before lease acquisition; pins one native lease (or borrows the ambient `(thread_id, task_id)` transaction lease) across the full portal generator lifetime.
    - Opens native `client.stream_arrow_batches(native_sql, native_params, batch_size=batch_size, columns=columns)` (`SyncPostgresArrowStream` / `AsyncPostgresArrowStream`), wraps every yielded native `vertebra._vertebra.ArrowCursor` batch in `vertebra.db.arrow.ArrowCursor(native_batch, columns=tuple(columns) if columns is not None else (), used_shm=True)` (`vertebra.db.arrow` decodes schema column names directly from Arrow IPC when `columns=()`), and always runs `stream.close()` / shielded `await stream.aclose()` before releasing any transient lease in `finally` on exhaustion, early `break`, `.close()` / `.aclose()`, or task cancellation.
  - `load_from_arrow(self, table_name: str, data: Any, *, columns: Sequence[str] | None = None, schema_name: str | None = None, **kwargs: Any) -> int` (and `async def` on `AsyncPostgreSQLDriver`):
    - Acquires one native client lease (or borrows the ambient transaction client) and delegates directly to `src/py/vertebra/db/adapters/postgres/arrow.py` (`load_arrow_sync(client, table_name, data, columns=columns, schema_name=schema_name)` / `await load_arrow_async(client, table_name, data, columns=columns, schema_name=schema_name)`), which drives `client.copy_from_arrow(table_name, first_batch_ipc, columns=columns, schema_name=schema_name)`, sequential single-in-flight `copy.write_arrow_ipc(batch_ipc)` calls, `copy.finish() -> int`, and deterministic `copy.close()` / shielded `await copy.aclose()` in `finally`.
    - Schema-only `0`-row Arrow input still validates the target `schema_name`/`table_name`/`columns` via `copy_from_arrow` + `finish()` and returns `0`; producer iteration errors abort the unfinished writer via `copy.close()` / `await copy.aclose()` while preserving and raising the original producer exception even if cleanup also errors.

## Edge Cases & Error Semantics

- Native Arrow selection/streaming never falls back to `execute_with_description`, `ArrowCursor.from_rows`, or `to_pylist()`.
- Custom `threshold_rows` (e.g. `threshold_rows=4` vs default `ARROW_SHM_THRESHOLD_ROWS`) switches inline vs SHM materialization deterministically at `row_count > threshold_rows`; empty `0`-row `select_to_arrow` preserves `columns` and `len(cursor) == 0`.
- Active native calls inside `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` register with the driver's active-operation `cancel()` registry so `driver.cancel()` retires only the targeted session.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_postgres_native_adapter_arrow.py` (direct-importing `SyncPostgreSQLDriver` from `_sync` and `AsyncPostgreSQLDriver` from `_async`, using `@pytest.mark.parametrize` across threshold/limit/error cases).
- **Worked Test Matrix**:

| Case | Input | Expected Output / Error |
| --- | --- | --- |
| `timeout` & `threshold_rows` forwarding, `0`-row schema, & post-close `ShmCapsule` retention | `select_to_arrow` with `0` rows, `10` rows (`threshold_rows=256` -> inline vs `threshold_rows=4, timeout=2.5` -> SHM), and `300` rows; close driver after cursor returned | Exact `(timeout, threshold_rows, max_result_bytes)` forwarded to native client; `0` rows preserves `columns` + `len(cursor) == 0`; `vertebra.db.arrow.ArrowCursor` retains readable IPC view after lease release and `driver.close()`. |
| Limit validation & `256 MiB` / `8 MiB` overflow errors | `threshold_rows=-1`, `threshold_rows=True`, `max_result_bytes=0`, `batch_size=0`, `batch_size=False`; simulate `> max_result_bytes` / `> 8 MiB` cell | Invalid limits raise `ValueError` with `0` pool acquisitions; overflow raises `OperationalError`/`DataError` and restores `in_use == 0`. |
| `stream_arrow_batches(..., batch_size=65_536, columns=...)` wrapping & early close | Iterate `1` batch of a `3`-batch stream (with `columns=None` decoding IPC schema names via `columns=()` and custom `columns=("a", "b")`), then `break` / `aclose()` / `task.cancel()` | Forwards `(batch_size, columns)` to `client.stream_arrow_batches`; each yielded item is `isinstance(item, vertebra.db.arrow.ArrowCursor)` (`used_shm is True`, schema-decoded or explicit `columns`); `stream.close()`/`await stream.aclose()` fires and `in_use == 0` immediately on early exit or cancel. |
| `load_from_arrow(table_name, data, ...)` delegation via `postgres/arrow.py` | (a) `0`-row Arrow table with `schema_name="analytics", columns=("id", "val")`; (b) `3`-batch iterator raising `RuntimeError("source boom")` on batch `2` while `copy.close()`/`aclose()` raises `OperationalError` | (a) Calls `copy_from_arrow(table_name, first_batch_ipc, columns=..., schema_name=...)` + `finish()` -> `0`; (b) enforces `1` in-flight `write_arrow_ipc`, invokes `copy.close()`/`await copy.aclose()`, restores pool count, and raises original `RuntimeError("source boom")`. |

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

- [ ] `select_to_arrow`, `stream_arrow_batches`, and `load_from_arrow` on `_sync.py` and `_async.py` match `SyncDatabaseDriver`/`AsyncDatabaseDriver` Liskov signatures 1:1 (`threshold_rows`, `max_result_bytes`, `batch_size=65_536`, `columns`, `table_name`, `data`, `schema_name`), wrapping portal batches in `vertebra.db.arrow.ArrowCursor` and delegating COPY import to `postgres/arrow.py` (`copy_from_arrow`, `write_arrow_ipc`, `finish`, `close`/`aclose`) (R20, R24).
- [ ] CPU `pgcopy` / JSONB / vector helper suites and legacy `driver.py` tests remain untouched and green (R20, R24).
- [ ] Commit message follows `test(postgres): complete native Arrow forwarding contracts` once verified.

## Notes & Discoveries

- `notify.py` was already migrated to native `$1`/`$2` + `PostgresNotificationDriver` hooks in `ch21:t03`; `embeddings.py` and `driver.py` switch atomically in `ch27:t01` [^npd-contracts].

[^npd-contracts]: `.agents/bundles/specs/native-postgres-driver/contracts.md` — PostgresArrowResult, threshold_rows and max_result_bytes forwarding, stream_arrow_batches, and copy_from_arrow contracts.
[^sync-base-py]: `src/py/vertebra/db/driver/_sync.py::SyncDatabaseDriver` — Canonical SyncDatabaseDriver Liskov signatures for select_to_arrow, stream_arrow_batches, and load_from_arrow.
[^async-base-py]: `src/py/vertebra/db/driver/_async.py::AsyncDatabaseDriver` — Canonical AsyncDatabaseDriver Liskov signatures for select_to_arrow, stream_arrow_batches, and load_from_arrow.
[^arrow-py]: `src/py/vertebra/db/arrow.py::ArrowCursor` — Public vertebra.db.arrow.ArrowCursor wrapper and ARROW_SHM_THRESHOLD_ROWS constant.
