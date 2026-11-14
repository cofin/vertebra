---
type: Task
id: "driver-conformance-ci:ch02:t01"
flow_id: driver-conformance-ci
chapter_id: ch02-conformance-harness-and-live-suites
title: "Implement ConformanceGate, ServiceVersionMatrix, and conftest zero-skip hooks"
description: "Create conformance_gate.py and wire engine selection, zero-skip session enforcement, and versioned pytest-databases fixtures in integration/conftest.py."
status: stable
state: pending
planning_quality: standard
planning_degraded_reason: null
worksheet_format: compact
strategy: regression_tdd
plan_commit: 18baadcd
depends_on: []
files:
  - src/py/tests/integration/conformance_gate.py
  - src/py/tests/integration/conftest.py
tests:
  - src/py/tests/unit/db/test_conformance_gate.py
read_set:
  - src/py/tests/integration/conftest.py
  - src/py/tests/integration/db/test_driver_conformance.py
  - pyproject.toml
estimated_loc: 225
tags:
  - conformance
  - pytest-databases
  - harness
sources:
  - id: int-conftest
    resource: src/py/tests/integration/conftest.py::vtb_mysql_config
    title: "Existing MySQL, MariaDB, SQL Server, and Oracle pytest-databases fixtures"
  - id: pyproject-pd
    resource: pyproject.toml
    title: "pytest-databases>=0.20.0b1 dependency declaration"
---

# Task `t01`: Implement `ConformanceGate`, `ServiceVersionMatrix`, and `conftest` Zero-Skip Hooks

## Objective & Scope Boundary

Create `src/py/tests/integration/conformance_gate.py` (`ConformanceGate`, `ServiceVersionMatrix`) and wire `pytest_configure`, `pytest_collection_modifyitems`, `pytest_runtest_makereport`, `pytest_sessionfinish`, and versioned `vtb_{mysql,mariadb,mssql,oracle}_config` fixtures in `src/py/tests/integration/conftest.py` per [`../spec.md` §2](../spec.md) [^int-conftest] [^pyproject-pd]. Out of scope: modifying PostgreSQL `vtb_postgres_config` or `VERTEBRA_CONFORMANCE_POSTGRES_REQUIRE_ZERO_SKIPS` hooks from `native-postgres-driver`.

## Bounded Read Manifest

- `src/py/tests/integration/conftest.py` — Existing integration hooks and `vtb_*_config` fixtures.
- `src/py/tests/integration/db/test_driver_conformance.py` — `EngineConformanceSpec.engine` parameterization seam.
- `pyproject.toml` — Marker registration (`driver_engine`) and `pytest-databases` configuration.

## Contracts & Edge Cases

- `ConformanceGate.from_environ` (Requires `/flow:revise`): Currently specifies raising `pytest.UsageError` on `"postgres"`, which conflicts with `.github/workflows/ci.yml` (`postgres-conformance` sets `VERTEBRA_CONFORMANCE_ENGINES=postgres`) and `test_driver_conformance.py` on `HEAD`; revise `t01` or `ci.yml` in coordination before executing this task. Unknown engines still raise `pytest.UsageError`; unset/blank env leaves the gate inactive after stripping whitespace and lowercasing tokens.
- `item_engines(item)`: unions `@pytest.mark.driver_engine("<name>")` args and `callspec.params.values()` string `.engine` attributes; `selects` never deselects unit tests outside `src/py/tests/integration`.
- `record` & `failures`: flags setup/call `skipped`, `wasxfail` (`xfailed`/`xpassed`), and any requested engine with `0` executed `call`-phase reports; `ServiceVersionMatrix.fixture_name` raises `pytest.UsageError` on unknown `VERTEBRA_CONFORMANCE_<FAMILY>_VERSION`.

## Test Matrix

- Parameterized unit tests in `src/py/tests/unit/db/test_conformance_gate.py` verifying unset/active parsing, `postgres` and unknown-engine `UsageError`, marker + `.engine` param extraction, unit-test non-deselection, setup-skip / xfail / `0`-executed failure reporting, and all `ServiceVersionMatrix` version mappings and error messages.

## Verification

```bash
uv run --no-sync pytest src/py/tests/unit/db/test_conformance_gate.py -q && uv run --no-sync ruff check src/py/tests/integration/conformance_gate.py src/py/tests/integration/conftest.py src/py/tests/unit/db/test_conformance_gate.py && uv run --no-sync mypy src/py/tests/integration/conformance_gate.py src/py/tests/integration/conftest.py src/py/tests/unit/db/test_conformance_gate.py
```

## Acceptance Criteria

- [ ] `ConformanceGate` and `ServiceVersionMatrix` enforce zero-skip lane execution and version resolution while remaining a no-op when `VERTEBRA_CONFORMANCE_ENGINES` is unset.
- [ ] Zero `.venv/...` paths appear anywhere in code, docstrings, or `sources:`.

[^int-conftest]: `src/py/tests/integration/conftest.py::vtb_mysql_config` — Existing MySQL, MariaDB, SQL Server, and Oracle `pytest-databases` fixtures.
[^pyproject-pd]: `pyproject.toml` — `pytest-databases>=0.20.0b1` dependency declaration.
