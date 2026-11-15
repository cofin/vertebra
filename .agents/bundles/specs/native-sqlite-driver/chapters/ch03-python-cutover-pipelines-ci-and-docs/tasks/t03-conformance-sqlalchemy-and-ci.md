---
type: Task
id: native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t03
flow_id: native-sqlite-driver
chapter_id: ch03-python-cutover-pipelines-ci-and-docs
title: SQLAlchemy Native SQLite Dialect Alignment, Conformance Suite, and CI Gate
description: Remove stdlib sqlite3 shortcuts in dialects.py, wire sqlite native_extension_module in ENGINE_SPECS and ConformanceGate, and add CI steps.
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 1b191388
depends_on:
- native-sqlite-driver:ch03-python-cutover-pipelines-ci-and-docs:t02
files:
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/tests/integration/conformance_gate.py
- src/py/tests/unit/db/test_conformance_gate.py
- src/py/tests/integration/db/test_driver_conformance.py
- .github/workflows/ci.yml
tests:
- src/py/tests/unit/db/test_conformance_gate.py
- src/py/tests/integration/db/test_driver_conformance.py
- src/py/tests/unit/db/test_sqlite_driver.py
read_set:
- src/py/vertebra/extensions/sqlalchemy/dialects.py
- src/py/tests/integration/conformance_gate.py
- src/py/tests/unit/db/test_conformance_gate.py
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
  resource: src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec
  title: Database driver conformance suite and ENGINE_SPECS
- id: conformance-gate
  resource: src/py/tests/integration/conformance_gate.py::ConformanceGate
  title: ConformanceGate SUPPORTED_ENGINES lane filter
- id: ci-wf
  resource: .github/workflows/ci.yml
  title: Continuous integration test and wheel isolation workflow
---

# Task `t03`: SQLAlchemy Native SQLite Dialect Alignment, Conformance Suite, and CI Gate

## Objective & Scope Boundary

Route `sqlite+vertebra` / `sqlite+vertebra_async` cleanly through native SQLite drivers, update `ENGINE_SPECS` and `ConformanceGate` to enforce `vertebra._sqlite` conformance with zero skips, and run `make sqlite-test` + `make sqlite-wheel-check` in `.github/workflows/ci.yml` per [`../spec.md` §2](../spec.md) [^sa-dialects] [^conformance-suite] [^conformance-gate] [^ci-wf].

## Bounded Read Manifest

- `src/py/vertebra/extensions/sqlalchemy/dialects.py` — `SyncSQLiteDialect` and `AsyncSQLiteDialect`.
- `src/py/tests/integration/conformance_gate.py` — `ConformanceGate.SUPPORTED_ENGINES`.
- `src/py/tests/unit/db/test_conformance_gate.py` — `ConformanceGate` supported-engine unit assertions.
- `src/py/tests/integration/db/test_driver_conformance.py` — `EngineConformanceSpec` and `ENGINE_SPECS` `"sqlite"` entry.
- `.github/workflows/ci.yml` — `duckdb-test` / `duckdb-wheel-check` CI steps.

## Contracts & Edge Cases

- Remove any direct `sqlite3` fallback in `dialects.py` (`sqlite+vertebra`, `+vtb`, `+vertebra_async`, `+vtb_async` remain intact).
- Update `ENGINE_SPECS` in `test_driver_conformance.py` for `"sqlite"` with `native_extension_module="vertebra._sqlite"`, `supports_pipeline=True`, and `pending_flow_id=None`, and add `"sqlite"` to `ConformanceGate.SUPPORTED_ENGINES` (`conformance_gate.py` + `test_conformance_gate.py`).
- Add CI steps running `make sqlite-test` and `make sqlite-wheel-check` in `.github/workflows/ci.yml`.
- **Edge Case**: Scalar conformance cases (`bool`, `JSONB` `dict`/`list`, `Decimal`, `UUID`, `datetime`, `bytes`) round-trip losslessly across `:memory:` and WAL files.

## Test Matrix

- `pytest src/py/tests/unit/db/test_conformance_gate.py` and `pytest src/py/tests/integration/db/test_driver_conformance.py -k sqlite` (`:memory:` and file WAL) pass with zero skips.
- `mypy src/py/vertebra/extensions/sqlalchemy` passes cleanly.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_conformance_gate.py src/py/tests/integration/db/test_driver_conformance.py -k "sqlite or conformance_gate" -q && uv run --no-sync mypy src/py/vertebra/extensions/sqlalchemy
```

## Acceptance Criteria

- [ ] SQLite passes the full native driver conformance matrix with `native_extension_module="vertebra._sqlite"` and CI runs `sqlite-test` + `sqlite-wheel-check`.
- [ ] Zero regressions in the fast verification gate.

[^sa-dialects]: `src/py/vertebra/extensions/sqlalchemy/dialects.py::SyncSQLiteDialect` — SQLAlchemy sqlite+vertebra and sqlite+vertebra_async dialects.
[^conformance-suite]: `src/py/tests/integration/db/test_driver_conformance.py::EngineConformanceSpec` — Database driver conformance suite and ENGINE_SPECS.
[^conformance-gate]: `src/py/tests/integration/conformance_gate.py::ConformanceGate` — ConformanceGate SUPPORTED_ENGINES lane filter.
[^ci-wf]: `.github/workflows/ci.yml` — Continuous integration test and wheel isolation workflow.
