---
type: Task
id: "native-cursor-data-path:cleanup:t19"
chapter_id: cleanup-and-checks
title: "Typed BigQuery client doubles"
description: "Clear thirteen mypy errors by giving the BigQuery client doubles the cursor signature of the client protocol and by making their cursor doubles satisfy the cursor protocol."
worksheet_format: compact
status: draft
state: completed
priority: P2
verification_strategy: static_validation
depends_on: []
read_set:
  - "src/py/vertebra/db/adapters/bigquery/_sync.py::SyncBigQueryCursorProtocol"
  - "src/py/vertebra/db/adapters/bigquery/_sync.py::SyncBigQueryClientProtocol"
  - "src/py/vertebra/db/adapters/bigquery/_async.py::AsyncBigQueryCursorProtocol"
  - "src/py/vertebra/db/adapters/bigquery/_async.py::AsyncBigQueryClientProtocol"
  - "src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py:10-18"
  - "src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py::_BigQueryToolClient.cursor"
  - "src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py::_BigQueryToolCursor"
  - "src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py::_AsyncBigQueryToolCursor"
  - "src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py::_AsyncBigQueryToolClient.cursor"
  - "src/py/tests/unit/extensions/test_sqlspec_bigquery.py:8-14"
  - "src/py/tests/unit/extensions/test_sqlspec_bigquery.py::_ArrowOptionsBigQueryClient"
  - "src/py/tests/unit/extensions/test_sqlspec_bigquery.py::_AsyncArrowOptionsBigQueryClient"
files: []
tests:
  - src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py
  - src/py/tests/unit/extensions/test_sqlspec_bigquery.py
estimated_loc: 80
plan_revision: 4
plan_commit: 06e99fb
state_revision: 1
claimed_by: null
claimed_at: null
commit: null
created_at: 2026-11-21T16:23:03Z
sources:
  - id: bigquery-protocol
    resource: src/py/vertebra/db/adapters/bigquery/_sync.py::SyncBigQueryClientProtocol
    title: "BigQuery client protocol with the cursor factory signature"
  - id: tool-doubles
    resource: src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py::_BigQueryToolClient
    title: "BigQuery tool client and cursor doubles"
  - id: arrow-doubles
    resource: src/py/tests/unit/extensions/test_sqlspec_bigquery.py::_ArrowOptionsBigQueryClient
    title: "Arrow options client doubles without a cursor factory"
tags: [testing, typing, bigquery]
---

# t19 — Typed BigQuery client doubles

## Objective & Scope Boundary

`mypy` reports no error in `test_gcp_adapters_and_sqlalchemy.py` and `test_sqlspec_bigquery.py`. It reports thirteen there today, all `arg-type` errors where a client double is passed to a BigQuery driver.[^bigquery-protocol][^tool-doubles][^arrow-doubles]

The doubles change. The protocols in `db/adapters/bigquery/` do not. No test changes what it asserts.

## Bounded Read Manifest

- The four protocols named in `read_set`, for the exact member names and parameter names.
- The import lines and the four double classes in `test_gcp_adapters_and_sqlalchemy.py`.
- The import lines and the two Arrow client doubles in `test_sqlspec_bigquery.py`.

## Contracts & Edge Cases

### `test_gcp_adapters_and_sqlalchemy.py`, eleven errors

The first conflict `mypy` names is the `labels` parameter of `cursor()`. Fixing only that exposes the next one: the cursor doubles do not satisfy the cursor protocols. Both are fixed here.

1. Import: `from collections.abc import Mapping, Sequence`.
2. `_BigQueryToolClient.cursor` and `_AsyncBigQueryToolClient.cursor`: the parameter becomes `labels: Mapping[str, str] | None = None`. The sync body keeps `labels=dict(labels) if labels is not None else None`.
3. `_BigQueryToolCursor`:
   - `__init__` also sets `self.arraysize: int = 1` and `self.lastrowid: int | None = None`.
   - `execute` becomes `def execute(self, sql: str, params: Any = ()) -> None`. The body passes `params` where it passed `parameters`.
   - `executemany` becomes `def executemany(self, sql: str, seq_of_params: Sequence[Any]) -> None`.
   - Add `def fetchone(self) -> tuple[Any, ...] | None`, which pops and returns the first buffered row, or returns `None`.
   - Add `def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]`, which removes and returns up to `size` rows, or `arraysize` rows when `size` is `None`.
4. `_AsyncBigQueryToolCursor`:
   - `__init__` also sets `self.arraysize: int = 1`. It is a plain attribute, because the protocol declares `arraysize` as a writable variable.
   - Add a read-only property `lastrowid` that returns the inner cursor's value.
   - `execute` becomes `async def execute(self, sql: str, params: Any = ()) -> None`. `executemany` takes `seq_of_params`.
   - Add `async def fetchone(self) -> tuple[Any, ...] | None` and `async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]`. Both call the inner cursor. `fetchmany` passes `self.arraysize` when `size` is `None`.
5. `FailingClient` inherits `cursor` from `_BigQueryToolClient` and needs no edit.

Before renaming `parameters`, run `git grep -n "parameters=" -- src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py`. A call that passes `parameters=` to one of these two cursor doubles changes to `params=`. There is none in the planning tree.

### `test_sqlspec_bigquery.py`, two errors

`_ArrowOptionsBigQueryClient` and `_AsyncArrowOptionsBigQueryClient` have no `cursor`. The Arrow read tests never open one. Each class gains the protocol's factory, placed before its `close` method:

```python
def cursor(
    self,
    *,
    session_id: str | None = None,
    destination_table: str | None = None,
    write_disposition: str | None = None,
    maximum_bytes_billed: int | None = None,
    job_timeout_ms: int | None = None,
    priority: str | None = None,
    labels: Mapping[str, str] | None = None,
    param_encoder: Callable[[str, Any], tuple[str, str | None]] | None = None,
    lazy_begin: bool = False,
) -> NoReturn:
    """Reject cursor creation; the Arrow read path never opens a cursor."""
    msg = "Arrow options client double does not serve cursors"
    raise AssertionError(msg)
```

- Import: `from typing import Any, NoReturn, cast`. `Callable` and `Mapping` are already imported.
- The method is a plain `def` in both classes. The async protocol's `cursor` is not a coroutine.

Edge cases:

- When `mypy` names another conflicting member on one of these doubles after the edits, give that member the protocol's parameter names and types in the double. A fix that needs a protocol change stops the task with the error text.
- No `# type: ignore` is added, and no `cast` is added at a call site that passes a double to a driver.
- Each new method has a one-line docstring. No inline comment.

## Test Matrix

| Check | Expected |
| --- | --- |
| `mypy` | No error in the two files |
| `pytest` on the two files | Same pass count as before the task; no new failure |
| `ruff check` on the two files | No finding |
| `git diff --stat -- src/py/vertebra` | Empty |

## Verification

```bash
uv run --no-sync --frozen mypy | grep "test_gcp_adapters_and_sqlalchemy.py\|test_sqlspec_bigquery.py"
uv run --no-sync --frozen pytest src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py -p no:xdist -q
uv run --no-sync --frozen ruff check src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py src/py/tests/unit/extensions/test_sqlspec_bigquery.py
git diff --stat -- src/py/vertebra src/rs
```

The `grep` prints nothing. `pytest` reports no failure. The last command prints nothing.

## Acceptance Criteria

- [ ] `mypy` reports no error in the two files.
- [ ] Both tool cursor doubles have every member of their cursor protocol, with the protocol's parameter names.
- [ ] Both Arrow client doubles have `cursor` with the protocol's keywords.
- [ ] No `# type: ignore`, and no production file has a diff from this task.

[^bigquery-protocol]: `src/py/vertebra/db/adapters/bigquery/_sync.py::SyncBigQueryClientProtocol`
[^tool-doubles]: `src/py/tests/unit/extensions/test_gcp_adapters_and_sqlalchemy.py::_BigQueryToolClient`
[^arrow-doubles]: `src/py/tests/unit/extensions/test_sqlspec_bigquery.py::_ArrowOptionsBigQueryClient`
