---
type: Task
id: "storage-remediation:ch04:t03"
chapter_id: ch04-streaming-arrow-parquet-and-shm
title: Make read_arrow_shm zero-copy over memoryview, keep ArrowCursor.__len__ lazy, and transpose _table_to_rows_and_cols column-wise
description: Retain memoryview slices via pyarrow.py_buffer in read_arrow_shm without bytes() copying, compute ArrowCursor.__len__ from the cached decoded IPC table without materializing tuples, and convert Arrow tables to row tuples via columnar to_pydict() transposition.
status: stable
state: completed
priority: P1
verification_strategy: behavior_tdd
depends_on: []
read_set:
  - src/py/vertebra/db/arrow.py
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
files:
  - src/py/vertebra/db/arrow.py
tests:
  - src/py/tests/unit/db/test_enterprise_olap_drivers.py
estimated_loc: 85
plan_revision: 1
plan_commit: fd571e3a
state_revision: 2
claimed_by: executor
claimed_at: "2026-11-09T00:46:30Z"
blocked_reason: null
unblock_condition: null
next_step: null
last_verified_at: "2026-11-09T00:52:30Z"
last_verified_commit: 113d25a5
verification_evidence: "pytest src/py/tests/unit/db (514 passed, 5 skipped); ruff check/format and mypy on db/arrow.py clean"
created_at: "2026-11-08T20:55:00Z"
updated_at: "2026-11-09T00:52:30Z"
commit: 113d25a5
generated:
  by: flow/0.28.0
  at: "2026-11-08T20:55:00Z"
verified:
  - by: executor
    at: "2026-11-09T00:52:30Z"
    commit: 113d25a5
sources:
  - id: py-db-arrow
    resource: src/py/vertebra/db/arrow.py::read_arrow_shm
    title: Shared-memory VTBARROW cursor hydration and columnar helpers
tags:
  - arrow
  - shm
  - database
  - performance
  - tdd
---

# Task t03: Zero-Copy `read_arrow_shm`, Lazy `ArrowCursor.__len__`, and Columnar `_table_to_rows_and_cols`

## Objective & Scope Boundary

Update `src/py/vertebra/db/arrow.py` [^py-db-arrow] (`1` file):
1. In `read_arrow_shm(region: Any, length: int | None = None) -> ArrowCursor`, preserve the optional `length` parameter, inspect `raw_view = memoryview(region).cast("B")`, auto-detect the 16-byte `VTBARROW` header (`raw_view[:8] == b"VTBARROW"` -> slice `payload_view = raw_view[16 : 16 + byte_len]`, validating `16 + byte_len <= len(raw_view)`) vs raw offset-0 Arrow IPC streams (`payload_view = raw_view`), validate/read schema over `pa.ipc.open_stream(pa.py_buffer(payload_view))` without copying `bytes(raw_view)` or calling `tbl.to_pylist()`, and retain `_shm_anchor = (region, raw_view, payload_view)` on `ArrowCursor` (falling back to `decode_json` only when `payload_view` is non-IPC JSON bytes).
2. Add `_row_count_cache: int | None` and `_shm_anchor: Any` to `ArrowCursor.__slots__`; in `ArrowCursor.__len__`, return `len(self._rows)` when `self._rows` is populated, `int(self._shm_cursor.row_count)` when positive, `_row_count_cache` when cached, or sum `batch.num_rows` across `pa.ipc.open_stream(pa.py_buffer(view))` (caching `_row_count_cache`) without populating `self._rows` or calling `to_pylist()`.
3. In `_table_to_rows_and_cols(data: Any, columns: Sequence[str] | None = None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`, accelerate `ArrowCursor` (when backed by Arrow IPC) and `pyarrow.Table` inputs by reading `tbl.to_pydict()` columns and transposing via `list(zip(*(col_dict.get(c, [None] * num_rows) for c in cols), strict=False))` instead of allocating `N` row dicts via `to_pylist()`, while preserving exact `Sequence` / `Mapping` / `Polars` fallback behavior and `(list[tuple[Any, ...]], tuple[str, ...])` return type.

Out of scope: Native Rust database drivers (`driver-fail-loud` and `native-postgres-driver`).

## Bounded Read Manifest

- `src/py/vertebra/db/arrow.py` — `ArrowCursor`, `write_arrow_shm`, `read_arrow_shm`, `_table_to_rows_and_cols` [^py-db-arrow].
- `src/py/tests/unit/db/test_enterprise_olap_drivers.py` — `VTBARROW` SHM round-trip and `ArrowCursor` tests.

## Symbol & Signature Contracts

- `ArrowCursor` (`__slots__` retaining `_shm_anchor` and `_row_count_cache`).
- `read_arrow_shm(region: Any, length: int | None = None) -> ArrowCursor`
- `_table_to_rows_and_cols(data: Any, columns: Sequence[str] | None = None) -> tuple[list[tuple[Any, ...]], tuple[str, ...]]`

## Edge Cases & Error Semantics

- `0`-row Arrow tables with `>= 1` columns return `([], ("id", "name"))` and `len(cursor) == 0`.
- `0`-column Arrow tables return `([], ())`.
- Truncated `VTBARROW` frame (`16 + byte_len > len(raw_view)`) raises `ValueError`.

## Public-Seam Test Blueprint

- **Canonical test file**: `src/py/tests/unit/db/test_enterprise_olap_drivers.py`.
- **Worked Test Matrix**:

| Case | Concrete Input | Expected Output / Error |
| --- | --- | --- |
| Zero-copy `read_arrow_shm` (both offset-0 IPC & `VTBARROW` header) + lazy `len(cursor)` | `region = write_arrow_shm(table)` and `VTBARROW`-prefixed `mmap` -> `cursor = read_arrow_shm(region)`; drop local `region` ref; inspect `len(cursor)` and `cursor.to_pyarrow()` | `len(cursor) == table.num_rows` while `cursor._rows == []`, and `cursor.to_pyarrow()` / `to_pylist()` returns exact rows even after caller drops `region`. |
| `_table_to_rows_and_cols` empty & populated tables | `0`-row typed `pa.Table` and `3`-row multi-column `pa.Table` | Returns `([], ("id", "name"))` and `([(1, "a"), (2, "b"), (3, "c")], ("id", "name"))`. |

## Verification

```bash
uv run --no-sync ruff check src/py/vertebra/db/arrow.py src/py/tests/unit/db/test_enterprise_olap_drivers.py
uv run --no-sync mypy src/py/vertebra/db/arrow.py
uv run --no-sync pytest src/py/tests/unit/db/test_enterprise_olap_drivers.py -k arrow -q
```

## Acceptance Criteria

- [x] [113d25a5] `read_arrow_shm` opens Arrow IPC streams directly over `pa.py_buffer(payload_view)` with an SHM lifetime anchor and zero `bytes(...)` copy (R4.3).
- [x] [113d25a5] `ArrowCursor.__len__` avoids `fetchall()` materialization and `_table_to_rows_and_cols` uses columnar `to_pydict()` transposition (R4.3).

## Notes & Discoveries

- [2026-11-09 01:10] `_table_to_rows_and_cols` returns schema column names for 0-row Arrow inputs, so `load_from_arrow` validates identifiers even on empty loads (fail-loud, consistent with non-empty input). `read_arrow_shm` cursors hold a buffer export of the caller's region; close the cursor before closing the `mmap`.
- [2026-11-08 20:55] Keep `_shm_anchor` typed cleanly on `ArrowCursor` so `mypy` passes in strict mode.

[^py-db-arrow]: `src/py/vertebra/db/arrow.py::read_arrow_shm` — Shared-memory `VTBARROW` cursor hydration and columnar helpers.
