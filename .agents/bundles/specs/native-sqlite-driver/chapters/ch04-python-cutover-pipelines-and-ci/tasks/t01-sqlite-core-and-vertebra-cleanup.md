---
type: Task
id: "native-sqlite-driver:ch04:t01"
flow_id: native-sqlite-driver
chapter_id: ch04-python-cutover-pipelines-and-ci
title: "Fix SQLiteCore.initialization_pragmas and Remove Legacy _vertebra SQLite Binding"
description: "Fix 5-arg TypeError in SQLiteCore.initialization_pragmas and delete sqlite_initialization_pragmas from vertebra-py and _vertebra.pyi."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on:
  - "native-sqlite-driver:ch03:t03"
files:
  - src/py/vertebra/db/adapters/sqlite/core.py
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
tests:
  - src/py/tests/unit/db/test_sqlite_driver.py
read_set:
  - src/py/vertebra/db/adapters/sqlite/core.py
  - src/rs/vertebra-py/src/db.rs
  - src/py/vertebra/_vertebra.pyi
estimated_loc: 90
tags:
  - sqlite
  - core
  - debloat
sources:
  - id: sqlite-core
    resource: src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore
    title: "SQLiteCore pragma helper with 5-arg call defect"
  - id: vertebra-py-db
    resource: src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas
    title: "Legacy sqlite_initialization_pragmas binding in vertebra-py"
  - id: vertebra-pyi
    resource: src/py/vertebra/_vertebra.pyi
    title: "Core vertebra._vertebra stub declarations"
---

# Task `t01`: Fix SQLiteCore.initialization_pragmas and Remove Legacy _vertebra SQLite Binding

## Objective & Scope Boundary

Fix `SQLiteCore.initialization_pragmas` directly and delete `sqlite_initialization_pragmas` from `vertebra-py/src/db.rs` and `_vertebra.pyi` per [`../spec.md` §2](../spec.md) [^sqlite-core] [^vertebra-py-db] [^vertebra-pyi].

## Bounded Read Manifest

- `src/py/vertebra/db/adapters/sqlite/core.py` — Broken 5-arg `native_fn` call and `contextlib.suppress(TypeError)`.
- `src/rs/vertebra-py/src/db.rs` — `sqlite_initialization_pragmas` PyO3 function.
- `src/py/vertebra/_vertebra.pyi` — `sqlite_initialization_pragmas` stub.

## Contracts & Edge Cases

- Remove `contextlib.suppress(TypeError)` and `_vertebra.sqlite_initialization_pragmas` lookup from `SQLiteCore.initialization_pragmas`.
- Delete `sqlite_initialization_pragmas` from `vertebra-py/src/db.rs` and `src/py/vertebra/_vertebra.pyi`.
- **Edge Case**: Invalid `synchronous` values raise `ValueError` consistently.

## Test Matrix

- `@pytest.mark.parametrize` verifies `SQLiteCore.initialization_pragmas` for WAL/non-WAL, `:memory:`, and `foreign_keys` combinations; `mypy.stubtest vertebra._vertebra` passes.

## Verification

```bash
make dev && uv run --no-sync python -m mypy.stubtest vertebra._vertebra --concise && uv run --no-sync pytest src/py/tests/unit/db/test_sqlite_driver.py -q
```

## Acceptance Criteria

- [ ] `sqlite_initialization_pragmas` is removed from `_vertebra` and `SQLiteCore.initialization_pragmas` has zero silent `TypeError` fallbacks.
- [ ] Zero regressions in the chapter fast verification gate.

[^sqlite-core]: `src/py/vertebra/db/adapters/sqlite/core.py::SQLiteCore` — SQLiteCore pragma helper with 5-arg call defect.
[^vertebra-py-db]: `src/rs/vertebra-py/src/db.rs::sqlite_initialization_pragmas` — Legacy sqlite_initialization_pragmas binding in vertebra-py.
[^vertebra-pyi]: `src/py/vertebra/_vertebra.pyi` — Core vertebra._vertebra stub declarations.
