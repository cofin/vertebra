---
type: Task
id: native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t03
flow_id: native-sqlite-driver
chapter_id: ch03-python-cutover-pipelines-ci-and-docs
title: SQLAlchemy Native SQLite Dialect Alignment, Conformance Suite, and CI Gate
description: Remove stdlib sqlite3 shortcuts in dialects.py, register sqlite native_extension_module in conformance registry/gate, and wire CI.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 104fd6fa
depends_on:
- native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t02
files:
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/tests/conformance/db/registry.py
- src/py/tests/conformance/db/_gate.py
- .github/workflows/ci.yml
tests:
- src/py/tests/integration/db/test_driver_conformance.py
- src/py/tests/unit/db/test_sqlite_driver.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/tests/conformance/db/registry.py
- src/py/tests/conformance/db/_gate.py
- src/py/tests/integration/db/test_driver_conformance.py
- .github/workflows/ci.yml
estimated_loc: 110
tags:
- sqlite
- sqlalchemy
- conformance
- ci
sources:
- id: sa-dialects
  resource: src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect
  title: SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects
- id: conformance-suite
  resource: src/py/tests/integration/db/test_driver_conformance.py
  title: Database driver conformance suite
- id: ci-wf
  resource: .github/workflows/ci.yml
  title: Continuous integration test and wheel isolation workflow
---

# Task `t03`: SQLAlchemy Native SQLite Dialect Alignment, Conformance Suite, and CI Gate

## Objective & Scope Boundary

Route `sqlite+vertebra` / `sqlite+vertebra_async` cleanly through native SQLite drivers, enforce `vertebra._sqlite` in `test_driver_conformance.py`, and run `make sqlite-test` + `make sqlite-wheel-check` in `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^sa-dialects] [^conformance-suite] [^ci-wf].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncSQLiteDialect` and `AsyncSQLiteDialect`.
- `src/py/tests/conformance/db/registry.py` & `_gate.py` — `ENGINE_SPECS` (`sqlite`) and `ConformanceGate` (`VALID_ENGINES`).
- `src/py/tests/integration/db/test_driver_conformance.py` — SQLite conformance test entry points.
- `.github/workflows/ci.yml` — `duckdb-test` / `duckdb-wheel-check` CI steps.

## Contracts & Edge Cases

- Remove any direct `sqlite3` fallback in `dialects.py` (`sqlite+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async` remain intact).
- Set `native_extension_module="vertebra._sqlite"` on `ENGINE_SPECS["sqlite"]` in `src/py/tests/conformance/db/registry.py` and register `"sqlite"` in `ConformanceGate` (`src/py/tests/conformance/db/_gate.py`).
- **Edge Case**: Scalar conformance cases (`bool`, `JSONB` `dict`/`list`, `Decimal`, `UUID`, `datetime`, `bytes`) round-trip losslessly.

## Test Matrix

- Full `pytest src/py/tests/integration/db/test_driver_conformance.py -k sqlite` (`:memory:` and file WAL) passes with zero skips.

## Verification

```bash
uv run --no-sync pytest src/py/tests/integration/db/test_driver_conformance.py -k sqlite -q && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy
```

## Acceptance Criteria

- [ ] SQLite passes the full native driver conformance matrix and CI runs `sqlite-test` + `sqlite-wheel-check`.
- [ ] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect` — SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py` — Database driver conformance suite.
[^ci-wf]: `.github/workflows/ci.yml` — Continuous integration test and wheel isolation workflow.
